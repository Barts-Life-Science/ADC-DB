"""Silver specialist.registry_surgery_episode: 1 dataset(s).

- datasets: clinical_registry_surgery_episode
- moved verbatim from: 3_silver_specialist_care:2097, 3_silver_specialist_care:2726

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


# Clinical / registry surgery episode

# ==== journey_clinical.registry_surgery_episode ====
REGISTRY_SURGERY_EPISODE_EXPECTED_SCHEMA = [
    ("ENTRY_ID", "int"),
    ("MRN", "string"),
    ("SURNAME", "string"),
    ("FORENAME", "string"),
    ("NHS_NUMBER", "string"),
    ("GENDER", "string"),
    ("DATE_OF_BIRTH", "date"),
    ("DATE_OF_DEATH", "date"),
    ("ETHNIC_ORIGIN", "string"),
    ("DATE_OF_OPERATION", "timestamp"),
    ("AGE_AT_OPERATION", "int"),
    ("DATE_OF_ADMISSION", "timestamp"),
    ("ADMINISTRATIVE_CATEGORY", "string"),
    ("ANGINA_STATUS_PRE_SURGERY", "string"),
    ("DYSPNOEA_STATUS_PRE_SURGERY", "string"),
    ("NUMBER_OF_PREVIOUS_MIS", "string"),
    ("INTERVAL_BETWEEN_SURGERY_AND_LAST_MI", "string"),
    ("PREVIOUS_PCI", "string"),
    ("PREVIOUS_CARDIAC_SURGERYTXT", "string"),
    ("DATE_OF_LAST_CARDIAC_OPERATION", "timestamp"),
    ("DIABETES_MANAGEMENT", "string"),
    ("CIGARETTE_SMOKING_HISTORY", "string"),
    ("HISTORY_OF_HYPERTENSION", "string"),
    ("DIALYSIS", "string"),
    ("HISTORY_OF_PULMONARY_DISEASE", "string"),
    ("HISTORY_OF_NEUROLOGICAL_DISEASETXT", "string"),
    ("HISTORY_OF_NEUROLOGICAL_DYSFUNCTION", "string"),
    ("EXTRACARDIAC_ARTERIOPATHY", "string"),
    ("PRE_OPERATIVE_HEART_RHYTHM", "string"),
    ("LEFT_HEART_CATHETERISATION", "string"),
    ("DATE_OF_LAST_CATHETERISATION", "timestamp"),
    ("EXTENT_OF_CORONARY_VESSEL_DISEASE", "string"),
    ("EJECTION_FRACTION_CATEGORY", "string"),
    ("PA_SYSTOLIC", "int"),
    ("SEVERITY_OF_AORTIC_VALVE_STENOSIS_EOA", "double"),
    ("INTRAVENOUS_NITRATES_OR_ANY_HEPARIN", "string"),
    ("INTRAVENOUS_INOTROPES_PRIOR_TO_ANAESTHESIA", "string"),
    ("VENTILATED_PRE_OPERATION", "string"),
    ("CARDIOGENIC_SHOCK_PRE_OPERATION", "string"),
    ("DATE_AND_TIME_OF_OPERATION", "timestamp"),
    ("OPERATIVE_URGENCY", "string"),
    ("NUMBER_OF_PREVIOUS_HEART_OPERATIONS", "int"),
    ("RESPONSIBLE_CONSULTANT_ANAESTHETISTTXT", "string"),
    ("FIRST_OPERATORTXT", "string"),
    ("FIRST_OPERATOR_GRADE", "string"),
    ("FIRST_OPERATOR_CALMAN_YEAR_OF_TRAINEE", "string"),
    ("FIRST_ASSISTANTTXT", "string"),
    ("FIRST_ASSISTANT_GRADE", "string"),
    ("FIRST_ASSISTANT_CALMAN_YEAR_OF_TRAINEE", "string"),
    ("LEFT_MAIN_STEM_DISEASE", "string"),
    ("LEFT_VENTRICULAR_FUNCTION", "int"),
    ("CARDIAC_PROCEDURES", "string"),
    ("OTHER_ACTUAL_CARDIAC_PROCEDURESTXT", "string"),
    ("ACTUAL_CREATININE_AT_TIME_OF_SURGERY", "int"),
    ("SEVERITY_OF_AORTIC_VALVE_STENOSIS_GRADIENT", "int"),
    ("CATEGORY_OF_AORTIC_VALVE_STENOSIS", "string"),
    ("CABG", "string"),
    ("VALVE", "string"),
    ("MAJOR_AORTIC", "string"),
    ("OTHER_CARDIAC_PROCEDURES", "string"),
    ("PRESENTATIONTXT", "string"),
    ("OTHER_THORACIC_AND_VASCULAR_PROCEDURESTXT", "string"),
    ("DATE_AND_TIME_OF_OPERATION_FINISHED", "timestamp"),
    ("REASON_PRE_OPERATIVE_INTRA_AORTIC_BALLOON_PUMP_USED", "string"),
    ("PRE_OP_SUPPORT_DEVICES_USEDTXT", "string"),
    ("REASON_FOR_PRE_OPERATIVE_IMPELLER_DEVICE_USE", "string"),
    ("REASON_FOR_PRE_OPERATIVE_VENTRICULAR_ASSIST_DEVICE_USE", "string"),
    ("REASON_FOR_USE_OF_OTHER_SUPPORT_DEVICE_PRE_OPERATION", "string"),
    ("POSTCODE_OF_USUAL_ADDRESS", "string"),
    ("PULMONARY_HYPERTENSION", "string"),
    ("AETIOLOGYTXT", "string"),
    ("HEIGHT_IN_CM", "int"),
    ("POOR_MOBILITY_NON_CARDIAC_REASON", "string"),
    ("CREATININE_CLEARANCE", "double"),
    ("RENAL_IMPAIRMENT", "string"),
    ("HOSPITAL_IDENTIFIER_OLD", "string"),
    ("DIABETES_TYPE", "string"),
    ("CARDIOLOGY_CONSULTANTTXT", "string"),
    ("HYPERCHOLESTEROLAEMIA", "string"),
    ("MEAN_PAWPLA", "int"),
    ("ANTIBIOTIC_TREATMENT_FOR_ENDOCARDITIS", "string"),
    ("SECOND_ASSISTANTTXT", "string"),
    ("SECOND_ASSISTANT_GRADE", "string"),
    ("SECOND_ASSISTANT_CALMAN_YEAR_OF_TRAINEE", "string"),
    ("INCISION", "string"),
    ("ARTERIAL_CANNULATION_VERSION5PT2", "string"),
    ("VENOUS_CANNULATION", "string"),
    ("PERFUSIONISTTXT", "string"),
    ("OTHER_CARDIOLOGY_CONSULTANT", "string"),
    ("OTHER_CARDIOLOGY_CONSULTANT_GM_CNUMBER", "int"),
    ("WEIGHT_IN_KG", "double"),
    ("OTHER_CARDIAC_PROCEDURE_DETAILS", "string"),
    ("M_PRE_OPERATION_HISTORY", "string"),
    ("HISTORY_OF_POOR_MOBILITY", "string"),
    ("SURGICAL_INCISIONTXT", "string"),
    ("CONSULTANT_RESPONSIBLE_FOR_PROCEDURETXT", "string"),
    ("RESPONSIBLE_CONSULTANT_SURGEONTXT", "string"),
    ("PRIMARY_OPERATOR", "string"),
    ("HOSPITAL_IDENTIFIERTXT", "string"),
    ("MYECTOMY_OPTIONSTXT", "string"),
    ("CONVERSION_TO_STERNOTOMY", "string"),
    ("REFERRING_HOSPITALTXT", "string"),
    ("UNIT_COLLECTIVE_RESPONSIBILITY_CASE", "string"),
    ("SECONDARY_ASSISTING_CONSULTANT_SURGEONTXT", "string"),
    ("COVID19_DIAGNOSIS", "string"),
    ("COVID19_PT", "string"),
    ("HARLEY_STREET_CLINIC", "string"),
    ("DISCUSSED_AT_DOCUMENTED_QUORATE_PREOP_MDT", "string"),
    ("HAEMOGLOBIN_AT_TIME_OF_SURGERY", "int"),
    ("CLINICAL_PRESENTATIONTXT", "string"),
    ("CLINICAL_PRESENTATION_MALPERFUSIONTXT", "string"),
    ("CT_AORTA_DT", "timestamp"),
    ("CT_AORTA_DIAGNOSISTXT", "string"),
    ("CT_AORTA_OTHER_DX", "string"),
    ("CT_AORTA_EXTENT_OF_DISSECTIONTXT", "string"),
    ("CT_AORTA_PRIMARY_ENTRY_TEARTXT", "string"),
    ("CT_AORTA_SECONDARY_ENTRY_TEAR_ARCH", "string"),
    ("CT_AORTA_ROOT", "int"),
    ("CT_AORTA_STJ", "int"),
    ("CT_AORTA_MID_AA", "int"),
    ("CT_AORTA_DISTAL_AA", "int"),
    ("CT_AORTA_ARCH_LCCA", "int"),
    ("CT_AORTA_ARCH_LAS", "int"),
    ("CT_AORTA_PROXIMAL_DTA", "int"),
    ("CT_AORTA_MID_DTA", "int"),
    ("CT_AORTA_DISTAL_DT_ADIAPHRAGM", "int"),
    ("CT_AORTA_VISCERAL_LEVEL", "int"),
    ("CT_AORTA_INFRARENAL", "int"),
    ("FEV1", "int"),
    ("FEV1_PERCENT_PREDICTED", "int"),
    ("FEV1_FV_CRATIO", "double"),
    ("FEV1_FV_CPERCENT", "int"),
    ("DIFFUSION_CAPACITYMLMINKPA", "double"),
    ("DIFFUSION_CAPACITY_PERCENT_PREDICTED", "int"),
    ("CPET_ANAEROBIC_THRESHOLD", "double"),
    ("MAXIMAL_OXYGEN_UPTAKE", "double"),
    ("SURGICAL_INCISIONNON_NICOR_OPTIONSTXT", "string"),
    ("PREVIOUS_ABDOMINAL_DESCENDING_AORTIC_SURGTXT", "string"),
    ("PREVIOUS_ENDOVASCULAR_SURGERYTXT", "string"),
    ("PRIOR_RIGHT_CHEST_SURGERY_OR_RADIATION", "string"),
    ("SIGNIFICANT_CHEST_WALL_DEFORMITY", "string"),
    ("PREOPERATIVE_TRANSCATHETER_INTERVENTIONSTXT", "string"),
    ("RV_FRACTIONAL_AREA_CHANGE", "int"),
    ("TAPSE", "int"),
    ("CORONARY_CT", "string"),
    ("ABDOMINAL_OR_DESCENDING", "string"),
    ("ROCKWOOD_FRAILTY_SCORE", "string"),
    ("ABDOMINAL_OR_DESCENDING_AORTIC", "string"),
    ("M_OTHER_PREVIOUS_ENDOVASCULAR_SURGERY", "string"),
    ("AORTIC_VALVE_ANATOMY", "string"),
    ("OTHER_AORTIC_VALVE_ANATOMY", "string"),
    ("AORTIC_VALVE_DISEASETXT", "string"),
    ("AORTIC_VALVE_REGURGITATION", "string"),
    ("ERO_AAORTIC", "double"),
    ("VENA_CONTRACTA_WIDTH_AORTIC", "double"),
    ("AR_JET_DIRECTION", "string"),
    ("LV_END_SYSTOLIC_DIAMETER_AORTIC", "int"),
    ("LV_END_DIASTOLIC_DIAMETER_AORTIC", "int"),
    ("SEVERITY_OF_AORTIC_VALVE_STENOSIS_AVA", "double"),
    ("MITRAL_VALVE_DISEASETXT", "string"),
    ("MITRAL_VALVE_REGURGITATION", "string"),
    ("MECHANISM_OF_MITRAL_REGURGITATIONTXT", "string"),
    ("FLAIL", "string"),
    ("PROLAPSE", "string"),
    ("RESTRICTION", "string"),
    ("DEGREE_OF_MYXOMATOUS_CHANGE", "string"),
    ("ERO_AMITRAL", "double"),
    ("VENA_CONTRACTA_WIDTH_MITRAL", "double"),
    ("MR_JET_DIRECTION", "string"),
    ("LV_END_SYSTOLIC_DIAMETER_MITRAL", "int"),
    ("LV_END_DIASTOLIC_DIAMETER_MITRAL", "int"),
    ("L_ADIAMETER", "int"),
    ("R_VIMPAIRMENT", "string"),
    ("PERICARDIAL_EFFUSION", "string"),
    ("MITRAL_VALVE_STENOSIS", "string"),
    ("MITRAL_ANNULUS_CALCIFICATION", "string"),
    ("SEVERITY_OF_MITRAL_VALVE_STENOSIS_EOA", "double"),
    ("SEVERITY_OF_MITRAL_VALVE_STENOSIS_GRADIENT", "int"),
    ("DATE_LAST_CHANGED", "timestamp"),
    ("PRE1_ADC_UPDT", "timestamp"),
    ("NHS_NUMBER_VALID_IND", "boolean"),
    ("LINKAGE_STATUS", "string"),
    ("PERSON_ID", "bigint"),
    ("LINKAGE_METHOD", "string"),
    ("LINKAGE_HISTORICAL_FALLBACK_IND", "boolean"),
    ("LINKAGE_FALLBACK_CONFLICT_IND", "boolean"),
    ("PRE2_DATE_LAST_CHANGED", "timestamp"),
    ("PRE2_ADC_UPDT", "timestamp"),
    ("POST1_DATE_LAST_CHANGED", "timestamp"),
    ("POST1_ADC_UPDT", "timestamp"),
    ("POST2_DATE_LAST_CHANGED", "timestamp"),
    ("POST2_ADC_UPDT", "timestamp"),
    ("EPISODE_LAST_CHANGED", "timestamp"),
    ("ADC_UPDT", "timestamp"),
    ("SOURCE_TABLE", "string"),
    ("SOURCE_RECORD_KEY", "string"),
    ("PRE2_MODULE", "struct<MRN:string,SURNAME:string,FORENAME:string,NHS_NUMBER:string,GENDER:string,DATE_OF_BIRTH:date,DATE_OF_DEATH:date,ETHNIC_ORIGIN:string,DATE_OF_OPERATION:timestamp,HAEMATOCRIT:int,WBC:double,PLATELET_COUNT:int,MEAN_CORPUSCULAR_VOLUME:double,CREATININE:int,UREA:double,E_GFR:int,SERUM_ALBUMIN:int,BILIRUBIN:int,ALT:int,INR:double,APTT:int,PT:double,ERACS:string,HEMIARCH:string,PREVIOUS_PERCUTANEOUS_VALVE_IMPLANT_SITETXT:string,DATE_OF_LAST_PERCUTANEOUS_VALVE_IMPLANT:timestamp,PRESENCE_OF_PERICARDIAL_EFFUSIONPRE_OPERATION:string,IS_PRE_OPERATIVE_PULMONARY_ARTERY_PRESSURE_MEASURED:string,LIVER_CIRRHOSIS:string,LOWER_LIMB_STRENGTH:string,MINI_MENTAL_STATE_SCORE:string,TRICUSPID_VALVE_REGURGITATION:string,PATIENT_DIED_BEFORE_SURGERY:string,PREVIOUS_TAVI_OR_PERCUTANEOUS_VALVE_INTERVENTION:string,LOCATION_OF_PRIMARY_TEAR_OF_ACUTE_AORTIC_SYNDROME:string,AORTIC_PATHOLOGYTXT:string,CRAWFORD_CLASSIFICATION:string,AORTIC_PROCEDURETXT:string,AORTIC_SEGMENTS_OPERATED_ONTXT:string,TYPE_OF_ACUTE_AORTIC_SYNDROME:string,CLASSIFICATION_OF_AORTIC_SYNDROME_STANFORD:string,CLASSIFICATION_OF_AORTIC_SYNDROME_DEBAKEY:string,PATIENT_INTUBATEDVENTILATED_AT_TIME_OF_REFERRAL_F:string,DATETIME_OF_SYMPTOM_ONSET:timestamp,HOSPITAL_PATIENT_FIRST_PRESENTED_ATTXT:string,DATE_TIME_ARRIVAL_IN_ED_FIRST_PRESENTATION:timestamp,DATETIME_OF_CT_SCAN_OF_AORTA:timestamp,DID_PATIENT_REQUIRE_TRANSFER_TO_ANOTHER_HOSPITAL:string,DATE_TIME_OF_ARRIVAL_IN_SECOND_HOSPITAL_CT_UNIT:timestamp,DATETIME_OF_START_OF_ANAESTHESIA:timestamp,ARTERIAL_CANNULATION_NICO_RTXT:string,VENOUS_CANNULATION_NICO_RTXT:string,AORTIC_IMPLANT_LOCATIONTXT:string,AORTIC_IMPLANT_TYPE:string,OTHER_AORTIC_IMPLANT_TYPE:string,AORTIC_IMPLANT_BAR_CODE:string,AORTIC_IMPLANT_MANUFACTURER:string,AORTIC_IMPLANT_OTHER_MANUFACTURER:string,AORTIC_IMPLANT_SERIAL_NUMBER:string>"),
    ("POST1_MODULE", "struct<MRN:string,SURNAME:string,FORENAME:string,NUMBER_OF_DISTAL_CORONARY_ANASTOMOSES:int,NUMBER_OF_VALVES_REPLACED_OR_REPAIRED:int,NUMBER_OF_SEGMENTS_OF_THE_AORTA:int,CARDIOPULMONARY_BYPASS:string,PREDOMINANT_METHOD_OF_MYOCARDIAL_PROTECTION:string,CARDIOPLEGIA_TEMPERATURETXT:string,CARDIOPLEGIA_SOLUTION:string,CARDIOPLEGIA_INFUSION_MODETXT:string,CARDIOPLEGIA_TIMING:string,NON_CARDIOPLEGIC_MYOCARDIAL_PROTECTION:string,INTRA_AORTIC_BALLOON_PUMP_USEDTXT:string,HEIGHT_IN_CM:int,WEIGHT_IN_KG:double,CUMULATIVE_BYPASS_TIME:int,CUMULATIVE_CROSS_CLAMP_TIME:int,TOTAL_CIRCULATORY_ARREST_TIME:int,RETURN_TO_THEATRETXT:string,NEW_POST_OPERATIVE_NEUROLOGICAL_DYSFUNCTIONTXT:string,NEW_HAEMOFILTRATION_OR_DIALYSIS:string,DISCHARGE_DESTINATION_FROM_CARDIOTHORACIC_WARD:string,PATIENT_STATUS_AT_DISCHARGE:string,DATE_OF_DISCHARGE_OR_DEATH:timestamp,IMPELLER_DEVICE_USEDTXT:string,VENTRICULAR_ASSIST_DEVICE_USEDTXT:string,OTHER_SUPPORT_DEVICE_USEDTXT:string,DEEP_STERNAL_WOUND_INFECTION:string,DEEP_STERNAL_WOUND_INFECTION_TREATMENT:string,STERNOTOMYTXT:string,THORACOTOMYTXT:string,CANNULATIONTXT:string,WEANINGTXT:string,STERNAL_CLOSURETXT:string,THORACOTOMY_CLOSURETXT:string,NUMBER_OF_ARTERIAL_GRAFTS:int,NUMBER_OF_VENOUS_GRAFTS:int,PRE_OPERATIVE_STAY_IN_DAYS:int,POST_OPERATIVE_STAY_IN_DAYS:int,TOTAL_STAY_IN_DAYS:int,BODY_SURFACE_AREA:double,BODY_MASS_INDEX:double,ADDITIVE_EUROSCORE:int,LOGISTIC_EUROSCORE:double,SCTS_COMPLEX_CABG_BAYES_SCORE:double,NEUROPROTECTIONTXT:string,REASON_FOR_INTRA_OPERATIVE_INTRA_AORTIC_BALLOON_PUMP_USE:string,REASON_FOR_POST_OPERATIVE_INTRA_AORTIC_BALLOON_PUMP_USE:string,REASON_FOR_INTRA_OPERATIVE_IMPELLER_DEVICE_USE:string,REASON_FOR_POST_OPERATIVE_IMPELLER_DEVICE_USE:string,REASON_FOR_INTRA_OPERATIVE_VENTRICULAR_ASSIST_DEVICE_USE:string,REASON_FOR_POST_OPERATIVE_VENTRICULAR_ASSIST_DEVICE_USE:string,REASON_FOR_USE_OF_OTHER_SUPPORT_DEVICE_INTRA_OPERATIVELY:string,REASON_FOR_USE_OF_OTHER_SUPPORT_DEVICE_POST_OPERATION:string,DURATION_OF_OPERATION:int,DATASET_COMPLETE:string,SCTS_LOGISTIC_EURO_SCORE:double,EURO_SCORE2:double,MAJOR_AORTIC:string,M_PROCEDURE_FINDINGS:string,TRACHEOSTOMY:string,M_CLOSURE_DETAILS:string,M_POST_OPERATIVE_INSTRUCTIONS:string,BYPASS_TEMPERATURE:int,CAB_GCODINGTXT:string,VALVES_REPLACEMENT_CODINGTXT:string,MITRAL_VALVE_CODINGTXT:string,AORTIC_VALVE_CODINGTXT:string,TRICUSPID_VALVE_CODINGTXT:string,AORTIC_ROOT_AND_REPLACEMENT_CODINGTXT:string,ASD_CODINGTXT:string,VSD_CODINGTXT:string,COMMON_CODINGTXT:string,PROCEDURE_PERFORMEDTXT:string,PACING_WIRESTXT:string,MTO_EFINDINGS:string,NUMBER_OF_STERNAL_WIRES:int,STERNAL_WIRE_TYPE:string,POST_OP_CARETXT:string,TRANSFERREDTO_ITU:string,DRAINTXT:string,M_OPERATION_NOTE_HEADER:string,M_OPERATION_DETAILS:string,SURGEON:string,LEFT_PLEURAL_DRAINTXT:string,RIGHT_PLEURAL_DRAINTXT:string,MEDIASTINAL_DRAINTXT:string,PERICARDIAL_DRAINTXT:string,WOUNDSUBCUTANEOUS_TISSUE_DRAINTXT:string,DRAIN_SITETXT:string,STAGESTXT:string,PULMONARY_VALVE_CODINGTXT:string,PS_VAND_PAPV_DCODINGTXT:string,ATRIAL_APPENDAGE_CODINGTXT:string,OTHER_OPERATIONS_ON_VENTRICLES_OF_THE_HEARTTXT:string,DOR_VCODINGTXT:string,AVS_DCODINGTXT:string,HIGH_COST_DEVICESTXT:string,MILES_DEATH_QUESTION:string,CONSULTANT_EMAIL_ADDRESSTXT:string,STERNAL_WIRETXT:string,NUMBER_OF_SINGLE_STERNAL_WIRES:int,NUMBER_OF_PAIRED_STERNAL_WIRES:int,NUMBER_OF_MAYO_STERNAL_WIRES:int,R_APACING:string,R_VPACING:string,TEMPORARY_PACING:string,OTHER_PACING_WIRE_DETAILS:string,EPICARDIAL_PACING:string,LOCATION:string,SCRUB_NURSETXT:string,ASA:string,VEIN_HARVEST_TM:int,VEIN_HARVEST_SITETXT:string,VEIN_HARVEST_TYPE:string,UNNECESSARY_INCISION:string,CONCLUSION:string,SIZE:double,PSW_PERFORMED:string,PSW_REASON:string,PSW_CARRIED_OUT_BY:string,SSI_RISK_SCORE:string,SSI_FORM_COMPLETED:string,COLLATAMP:string,PICO:string,REDIVACSTERNUM:string,DIAMETER:double,PSW_NO_REASON:string,M_PSW_NO_OTHER_REASON:string,OTHER_SCRUB_NURSE:string,VEIN_HARVESTERTXT:string,OTHER_SCRUB_NURSE_IF_NOT_LISTED_ABOVE:string,CLASSIFICATION:string,SITE:string,M_SSI_FREE_TXT:string,BACTERIATXT:string,CULTURETXT:string,THEATRE_NO:string,CHEST_CLOSED_BYTXT:string,VH_OTHER_INFORMATION:string>"),
    ("POST2_MODULE", "struct<MRN:string,SURNAME:string,FORENAME:string,DRYING_TIME_COMPLIANT:string,POST_OPERATIVE_COMPLICATIONTXT:string,ATRIAL_FIBRILLATION:string,ANTICOAGULATION_COMMENCED:string,LRT_ITXT:string,POST_OPERATIVE_PYREXIA_TREATMENTTXT:string,STERNAL_WOUND_INFECTION_TREATMENTTXT:string,M_STERNAL_WOUND_INFECTION_REOPERATION_TYPE:string,HARVEST_WOUND_INFECTION_TREATMENTTXT:string,M_HARVEST_WOUND_INFECTION_REOPERATION_TYPE:string,POST_OPERATIVE_ILEUS:string,PERICARDIAL_EFFUSION:string,RESTERNOTOMY_FOR_BLEEDING_ON_DAY:int,INVESTIGATIONS_SATISFACTORY_PRIOR_TO_DISCHARGE:string,M_OTHER_INVESTIGATIONS_SATISFACTORY:string,M_LOCAL_CLINIC_DETAILS:string,CONNECTIVE_TISSUE_DISORDER:string,PEC_FLAP:string,PLASTIC_INTERVENTION:string,SSI:string,TYPE_OF_SSI_ADMISSION:string,SSI_SITETXT:string,DATE_OF_PEC_FLAP:timestamp,EXCESS_FLUID_OVERLOAD:string,PLEURAL_EFFUSION:string,PLEURAL_EFFUSION_MANAGEMENT:string,PNEUMOTHORAX:string,PNEUMOTHORAX_MANAGEMENT:string,MAXIMAL_DEPTHMM:double,INDICATION_FOR_PPM:string,NEW_CVA:string,FOLLOW_UP_ARRANGEMENTS:string,SEND_TO_NICORTXT:string,TOE:string,DOUBLE_LUMEN_TUBE:string,DOUBLE_LUMEN_TUBE_SIZE:int,CVP_INSERTION_SITE:string,PA_SHEATH_INSERTION_SITE:string,VASCATH_INSERTION_SITE:string,ARTERIAL_LINE_INSERTION_SITE:string,VENTILATION_MAX_INTRAOPERATIVE_FI_O2:double,MAX_INTRAOPERATIVE_PEEP:double,INTRAOPERATIVE_PEAK_LACTATE:double,END_OF_PROCEDURE_LACTATE:double,SPINAL_DRAIN_INSERTION_LEVEL:string,SPINAL_DRAIN_DEPTH_OF_SPACE:double,SPINAL_DRAIN_LENGTH_OF_CATHETER:double,SPINAL_DRAIN_NUMBER_OF_ATTEMPTS:int,SPINAL_DRAIN_INITIAL_PRESSURE:int,CSF:string,VASOPRESSIN_MAX_DOSE:double,MILRINONE_MAX_DOSE:double,ADRENALINE_MAX_DOSE:double,DOPAMINE_MAX_DOSE:double,DOBUTAMINE_MAX_DOSE:double,INTRAOPERATIVE_PACKED_RED_CELLS_GIVEN:int,INTRAOPERATIVE_FFP_RED_CELLS_GIVEN:int,INTRAOPERATIVE_CRYOPRECIPITATE_GIVEN:int,INTRAOPERATIVE_PLATELETS_GIVEN:int,INTRAOPERATIVE_PLATELET_RICH_PLASMA:int,INTRAOPERATIVE_TOTAL_CELL_SALVAGE:int,INTRAOPERATIVE_FIBRINOGEN_GIVEN:double,INTRAOPERATIVE4_FACTOR_PC_CGIVEN:int,INTRAOPERATIVE_FACTOR_VI_IGIVEN:int,INTRAOPERATIVE_TXA_TOTAL_DOSE:double,INTRAOPERATIVE_APROTININ_TOTAL_DOSE:int,BLOOD_PRODUCT_TRANSFUSIONINTRA_ANDOR_POST_OP:string,RED_BLOOD_CELLRBC_TRANSFUSIONTOTAL_UNITS_INTRA_AND_POST_OP:int,FRESH_FROZEN_PLASMAFFP_TRANSFUSIONTOTAL_UNITS_INTRA_AND_P:int,PLATELET_TRANSFUSIONTOTAL_UNITS_INTRA_AND_POST_OP:int,RB_CPOSTOP:int,FFP_RED_CELLS_POST_OP:int,PLATELETS_POST_OP:int,CRYOPRECIPITATE_POST_OP:int,FIBRINOGEN_POST_OP:double,FOUR_FACTOR_PC_CPOSTOP:int,FACTOR_VI_IPOSTOP:int,CK_RBASELINE:double,CKH_RBASELINE:double,CKM_ABASELINE:double,CRTM_ABASELINE:double,CFFM_ABASELINE:double,CKHM_ABASELINE:double,CRTLY30BASELINE:int,CK_RBYPASSEND:double,CKH_RBYPASSEND:double,CKM_ABYPASSEND:double,CRTM_ABYPASSEND:double,CFFM_ABYPASSEND:double,CKHM_ABYPASSEND:double,CRTLY30_BYPASS_END:int,CKR_POST_PROTAMINE1:double,CKHR_POST_PROTAMINE1:double,CKMA_POST_PROTAMINE1:double,CRTMA_POST_PROTAMINE1:double,CFFMA_POST_PROTAMINE1:double,CKHMA_POST_PROTAMINE1:double,CRTLY30_POST_PROTAMINE1:int,CKR_POST_PROTAMINE2:double,CKHR_POST_PROTAMINE2:double,CKMA_POST_PROTAMINE2:double,CRTMA_POST_PROTAMINE2:double,CFFMA_POST_PROTAMINE2:double,CKHMA_POST_PROTAMINE2:double,CRTLY30_POST_PROTAMINE2:int,LEFT_HEART_BYPASS:string,LEFT_HEART_BYPASS_TEMPERATURE:int,ARTERIAL_BYPASS:string,VENOUS_BYPASS:string,RENAL_PROTECTION:string,TOTAL_CEREBRAL_PERFUSION_TIME:int,LEFT_SUBCLAVIAN_ARTERY_TIME:int,INTERCOSTAL_ARTERIES_TIME:int,CELIAC_AXIS_TIME:int,SM_ATIME:int,LEFT_RENAL_TIME:int,RIGHT_RENAL_TIME:int,LEFT_ILIAC_TIME:int,RIGHT_ILIAC_TIME:int,RECIRCULATING_VOLUME_LTR:double,SPINAL_DRAIN_REMOVAL_DT:timestamp,OTHER_CVP_INSERTION_SITE:string,CHEST_DRAIN_OUTPUT_AT12_HOURS:string,CHEST_DRAIN_OUTPUT_AT24_HOURS:string,CHEST_DRAIN_OUTPUT_AT6_HOURS:string,M_POSTOPERATIVE_INVASIVE_PAIN_MANAGEMENT:string,POST_OP_L_VIMPAIRMENT:string,POST_OP_R_VIMPAIRMENT:string,PARAPLEGIA_ONSET_DT:timestamp,HAEMOFILTRATION_START_DT:timestamp,HAEMOFILTRATION_FINISH_DT:timestamp,IS_DIALYSIS_NEEDED_POST_DISCHARGE:string,TRACHEOSTOMY_DT:timestamp,M_WEANING_FROM_BYPASS:string,SS_IONSET_DT:timestamp,RENAL_PERFUSATETAAA_ONLY:string,AORTIC_IMPLANT_LOCATION2TXT:string,LEFT_HEART_BYPASS_TIME:int,CEREBRAL_PERFUSION_TIME:int,SYSTEMIC_TEMPERATURE:double,WAS_THE_STERNUM_CLOSED:string,M_REASON_STERNUM_WAS_NOT_CLOSED:string>"),
    ("ROW_HASH", "string"),
    ("PIPELINE_LOADED_AT", "timestamp"),
    ("IS_CURRENT_IN_SOURCE", "boolean"),
    ("SOURCE_PRESENT_IND", "boolean"),
    ("SOURCE_ABSENT_DETECTED_TS", "timestamp"),
]


@materialized_view(
    name=_n("journey_clinical.registry_surgery_episode"),
    refresh_policy="auto",
)
def registry_surgery_episode():
    # Build the declared dataset: Typed child of clinical_registry_entry for the surgery_episode
    # family. iWeb CS2010G adult cardiac surgery registry. Complete PRE1 fields remain top-
    # level; PRE2, POST1 and POST2 source fields are retained losslessly in typed module
    # structs. This preserves identifiers and clinical content while remaining within Unity
    # Catalog's 1,000-tag-per-table quota. Every governed bronze column is retained under its
    # lower-case name.
    s = read_source(IWEB_REGISTRY_SLOTS["surgery_episode"])
    actual_schema = [(field.name, field.dataType.simpleString()) for field in s.schema.fields]
    if actual_schema != REGISTRY_SURGERY_EPISODE_EXPECTED_SCHEMA:
        raise RuntimeError("surgery_episode: bronze schema drift" + f"; expected={REGISTRY_SURGERY_EPISODE_EXPECTED_SCHEMA!r} actual={actual_schema!r}")
    return s.select(
        stable_id("registry_entry:iweb", F.lit("surgery_episode"), F.lit("~"), s["ENTRY_ID"]).alias("patient_event_key"),
        s["ENTRY_ID"].alias("entry_id"),
        s["MRN"].alias("mrn"),
        s["SURNAME"].alias("surname"),
        s["FORENAME"].alias("forename"),
        s["NHS_NUMBER"].alias("nhs_number"),
        s["GENDER"].alias("gender"),
        s["DATE_OF_BIRTH"].alias("date_of_birth"),
        s["DATE_OF_DEATH"].alias("date_of_death"),
        s["ETHNIC_ORIGIN"].alias("ethnic_origin"),
        s["DATE_OF_OPERATION"].alias("date_of_operation"),
        s["AGE_AT_OPERATION"].alias("age_at_operation"),
        s["DATE_OF_ADMISSION"].alias("date_of_admission"),
        s["ADMINISTRATIVE_CATEGORY"].alias("administrative_category"),
        s["ANGINA_STATUS_PRE_SURGERY"].alias("angina_status_pre_surgery"),
        s["DYSPNOEA_STATUS_PRE_SURGERY"].alias("dyspnoea_status_pre_surgery"),
        s["NUMBER_OF_PREVIOUS_MIS"].alias("number_of_previous_mis"),
        s["INTERVAL_BETWEEN_SURGERY_AND_LAST_MI"].alias("interval_between_surgery_and_last_mi"),
        s["PREVIOUS_PCI"].alias("previous_pci"),
        s["PREVIOUS_CARDIAC_SURGERYTXT"].alias("previous_cardiac_surgerytxt"),
        s["DATE_OF_LAST_CARDIAC_OPERATION"].alias("date_of_last_cardiac_operation"),
        s["DIABETES_MANAGEMENT"].alias("diabetes_management"),
        s["CIGARETTE_SMOKING_HISTORY"].alias("cigarette_smoking_history"),
        s["HISTORY_OF_HYPERTENSION"].alias("history_of_hypertension"),
        s["DIALYSIS"].alias("dialysis"),
        s["HISTORY_OF_PULMONARY_DISEASE"].alias("history_of_pulmonary_disease"),
        s["HISTORY_OF_NEUROLOGICAL_DISEASETXT"].alias("history_of_neurological_diseasetxt"),
        s["HISTORY_OF_NEUROLOGICAL_DYSFUNCTION"].alias("history_of_neurological_dysfunction"),
        s["EXTRACARDIAC_ARTERIOPATHY"].alias("extracardiac_arteriopathy"),
        s["PRE_OPERATIVE_HEART_RHYTHM"].alias("pre_operative_heart_rhythm"),
        s["LEFT_HEART_CATHETERISATION"].alias("left_heart_catheterisation"),
        s["DATE_OF_LAST_CATHETERISATION"].alias("date_of_last_catheterisation"),
        s["EXTENT_OF_CORONARY_VESSEL_DISEASE"].alias("extent_of_coronary_vessel_disease"),
        s["EJECTION_FRACTION_CATEGORY"].alias("ejection_fraction_category"),
        s["PA_SYSTOLIC"].alias("pa_systolic"),
        s["SEVERITY_OF_AORTIC_VALVE_STENOSIS_EOA"].alias("severity_of_aortic_valve_stenosis_eoa"),
        s["INTRAVENOUS_NITRATES_OR_ANY_HEPARIN"].alias("intravenous_nitrates_or_any_heparin"),
        s["INTRAVENOUS_INOTROPES_PRIOR_TO_ANAESTHESIA"].alias("intravenous_inotropes_prior_to_anaesthesia"),
        s["VENTILATED_PRE_OPERATION"].alias("ventilated_pre_operation"),
        s["CARDIOGENIC_SHOCK_PRE_OPERATION"].alias("cardiogenic_shock_pre_operation"),
        s["DATE_AND_TIME_OF_OPERATION"].alias("date_and_time_of_operation"),
        s["OPERATIVE_URGENCY"].alias("operative_urgency"),
        s["NUMBER_OF_PREVIOUS_HEART_OPERATIONS"].alias("number_of_previous_heart_operations"),
        s["RESPONSIBLE_CONSULTANT_ANAESTHETISTTXT"].alias("responsible_consultant_anaesthetisttxt"),
        s["FIRST_OPERATORTXT"].alias("first_operatortxt"),
        s["FIRST_OPERATOR_GRADE"].alias("first_operator_grade"),
        s["FIRST_OPERATOR_CALMAN_YEAR_OF_TRAINEE"].alias("first_operator_calman_year_of_trainee"),
        s["FIRST_ASSISTANTTXT"].alias("first_assistanttxt"),
        s["FIRST_ASSISTANT_GRADE"].alias("first_assistant_grade"),
        s["FIRST_ASSISTANT_CALMAN_YEAR_OF_TRAINEE"].alias("first_assistant_calman_year_of_trainee"),
        s["LEFT_MAIN_STEM_DISEASE"].alias("left_main_stem_disease"),
        s["LEFT_VENTRICULAR_FUNCTION"].alias("left_ventricular_function"),
        s["CARDIAC_PROCEDURES"].alias("cardiac_procedures"),
        s["OTHER_ACTUAL_CARDIAC_PROCEDURESTXT"].alias("other_actual_cardiac_procedurestxt"),
        s["ACTUAL_CREATININE_AT_TIME_OF_SURGERY"].alias("actual_creatinine_at_time_of_surgery"),
        s["SEVERITY_OF_AORTIC_VALVE_STENOSIS_GRADIENT"].alias("severity_of_aortic_valve_stenosis_gradient"),
        s["CATEGORY_OF_AORTIC_VALVE_STENOSIS"].alias("category_of_aortic_valve_stenosis"),
        s["CABG"].alias("cabg"),
        s["VALVE"].alias("valve"),
        s["MAJOR_AORTIC"].alias("major_aortic"),
        s["OTHER_CARDIAC_PROCEDURES"].alias("other_cardiac_procedures"),
        s["PRESENTATIONTXT"].alias("presentationtxt"),
        s["OTHER_THORACIC_AND_VASCULAR_PROCEDURESTXT"].alias("other_thoracic_and_vascular_procedurestxt"),
        s["DATE_AND_TIME_OF_OPERATION_FINISHED"].alias("date_and_time_of_operation_finished"),
        s["REASON_PRE_OPERATIVE_INTRA_AORTIC_BALLOON_PUMP_USED"].alias("reason_pre_operative_intra_aortic_balloon_pump_used"),
        s["PRE_OP_SUPPORT_DEVICES_USEDTXT"].alias("pre_op_support_devices_usedtxt"),
        s["REASON_FOR_PRE_OPERATIVE_IMPELLER_DEVICE_USE"].alias("reason_for_pre_operative_impeller_device_use"),
        s["REASON_FOR_PRE_OPERATIVE_VENTRICULAR_ASSIST_DEVICE_USE"].alias("reason_for_pre_operative_ventricular_assist_device_use"),
        s["REASON_FOR_USE_OF_OTHER_SUPPORT_DEVICE_PRE_OPERATION"].alias("reason_for_use_of_other_support_device_pre_operation"),
        s["POSTCODE_OF_USUAL_ADDRESS"].alias("postcode_of_usual_address"),
        s["PULMONARY_HYPERTENSION"].alias("pulmonary_hypertension"),
        s["AETIOLOGYTXT"].alias("aetiologytxt"),
        s["HEIGHT_IN_CM"].alias("height_in_cm"),
        s["POOR_MOBILITY_NON_CARDIAC_REASON"].alias("poor_mobility_non_cardiac_reason"),
        s["CREATININE_CLEARANCE"].alias("creatinine_clearance"),
        s["RENAL_IMPAIRMENT"].alias("renal_impairment"),
        s["HOSPITAL_IDENTIFIER_OLD"].alias("hospital_identifier_old"),
        s["DIABETES_TYPE"].alias("diabetes_type"),
        s["CARDIOLOGY_CONSULTANTTXT"].alias("cardiology_consultanttxt"),
        s["HYPERCHOLESTEROLAEMIA"].alias("hypercholesterolaemia"),
        s["MEAN_PAWPLA"].alias("mean_pawpla"),
        s["ANTIBIOTIC_TREATMENT_FOR_ENDOCARDITIS"].alias("antibiotic_treatment_for_endocarditis"),
        s["SECOND_ASSISTANTTXT"].alias("second_assistanttxt"),
        s["SECOND_ASSISTANT_GRADE"].alias("second_assistant_grade"),
        s["SECOND_ASSISTANT_CALMAN_YEAR_OF_TRAINEE"].alias("second_assistant_calman_year_of_trainee"),
        s["INCISION"].alias("incision"),
        s["ARTERIAL_CANNULATION_VERSION5PT2"].alias("arterial_cannulation_version5pt2"),
        s["VENOUS_CANNULATION"].alias("venous_cannulation"),
        s["PERFUSIONISTTXT"].alias("perfusionisttxt"),
        s["OTHER_CARDIOLOGY_CONSULTANT"].alias("other_cardiology_consultant"),
        s["OTHER_CARDIOLOGY_CONSULTANT_GM_CNUMBER"].alias("other_cardiology_consultant_gm_cnumber"),
        s["WEIGHT_IN_KG"].alias("weight_in_kg"),
        s["OTHER_CARDIAC_PROCEDURE_DETAILS"].alias("other_cardiac_procedure_details"),
        s["M_PRE_OPERATION_HISTORY"].alias("m_pre_operation_history"),
        s["HISTORY_OF_POOR_MOBILITY"].alias("history_of_poor_mobility"),
        s["SURGICAL_INCISIONTXT"].alias("surgical_incisiontxt"),
        s["CONSULTANT_RESPONSIBLE_FOR_PROCEDURETXT"].alias("consultant_responsible_for_proceduretxt"),
        s["RESPONSIBLE_CONSULTANT_SURGEONTXT"].alias("responsible_consultant_surgeontxt"),
        s["PRIMARY_OPERATOR"].alias("primary_operator"),
        s["HOSPITAL_IDENTIFIERTXT"].alias("hospital_identifiertxt"),
        s["MYECTOMY_OPTIONSTXT"].alias("myectomy_optionstxt"),
        s["CONVERSION_TO_STERNOTOMY"].alias("conversion_to_sternotomy"),
        s["REFERRING_HOSPITALTXT"].alias("referring_hospitaltxt"),
        s["UNIT_COLLECTIVE_RESPONSIBILITY_CASE"].alias("unit_collective_responsibility_case"),
        s["SECONDARY_ASSISTING_CONSULTANT_SURGEONTXT"].alias("secondary_assisting_consultant_surgeontxt"),
        s["COVID19_DIAGNOSIS"].alias("covid19_diagnosis"),
        s["COVID19_PT"].alias("covid19_pt"),
        s["HARLEY_STREET_CLINIC"].alias("harley_street_clinic"),
        s["DISCUSSED_AT_DOCUMENTED_QUORATE_PREOP_MDT"].alias("discussed_at_documented_quorate_preop_mdt"),
        s["HAEMOGLOBIN_AT_TIME_OF_SURGERY"].alias("haemoglobin_at_time_of_surgery"),
        s["CLINICAL_PRESENTATIONTXT"].alias("clinical_presentationtxt"),
        s["CLINICAL_PRESENTATION_MALPERFUSIONTXT"].alias("clinical_presentation_malperfusiontxt"),
        s["CT_AORTA_DT"].alias("ct_aorta_dt"),
        s["CT_AORTA_DIAGNOSISTXT"].alias("ct_aorta_diagnosistxt"),
        s["CT_AORTA_OTHER_DX"].alias("ct_aorta_other_dx"),
        s["CT_AORTA_EXTENT_OF_DISSECTIONTXT"].alias("ct_aorta_extent_of_dissectiontxt"),
        s["CT_AORTA_PRIMARY_ENTRY_TEARTXT"].alias("ct_aorta_primary_entry_teartxt"),
        s["CT_AORTA_SECONDARY_ENTRY_TEAR_ARCH"].alias("ct_aorta_secondary_entry_tear_arch"),
        s["CT_AORTA_ROOT"].alias("ct_aorta_root"),
        s["CT_AORTA_STJ"].alias("ct_aorta_stj"),
        s["CT_AORTA_MID_AA"].alias("ct_aorta_mid_aa"),
        s["CT_AORTA_DISTAL_AA"].alias("ct_aorta_distal_aa"),
        s["CT_AORTA_ARCH_LCCA"].alias("ct_aorta_arch_lcca"),
        s["CT_AORTA_ARCH_LAS"].alias("ct_aorta_arch_las"),
        s["CT_AORTA_PROXIMAL_DTA"].alias("ct_aorta_proximal_dta"),
        s["CT_AORTA_MID_DTA"].alias("ct_aorta_mid_dta"),
        s["CT_AORTA_DISTAL_DT_ADIAPHRAGM"].alias("ct_aorta_distal_dt_adiaphragm"),
        s["CT_AORTA_VISCERAL_LEVEL"].alias("ct_aorta_visceral_level"),
        s["CT_AORTA_INFRARENAL"].alias("ct_aorta_infrarenal"),
        s["FEV1"].alias("fev1"),
        s["FEV1_PERCENT_PREDICTED"].alias("fev1_percent_predicted"),
        s["FEV1_FV_CRATIO"].alias("fev1_fv_cratio"),
        s["FEV1_FV_CPERCENT"].alias("fev1_fv_cpercent"),
        s["DIFFUSION_CAPACITYMLMINKPA"].alias("diffusion_capacitymlminkpa"),
        s["DIFFUSION_CAPACITY_PERCENT_PREDICTED"].alias("diffusion_capacity_percent_predicted"),
        s["CPET_ANAEROBIC_THRESHOLD"].alias("cpet_anaerobic_threshold"),
        s["MAXIMAL_OXYGEN_UPTAKE"].alias("maximal_oxygen_uptake"),
        s["SURGICAL_INCISIONNON_NICOR_OPTIONSTXT"].alias("surgical_incisionnon_nicor_optionstxt"),
        s["PREVIOUS_ABDOMINAL_DESCENDING_AORTIC_SURGTXT"].alias("previous_abdominal_descending_aortic_surgtxt"),
        s["PREVIOUS_ENDOVASCULAR_SURGERYTXT"].alias("previous_endovascular_surgerytxt"),
        s["PRIOR_RIGHT_CHEST_SURGERY_OR_RADIATION"].alias("prior_right_chest_surgery_or_radiation"),
        s["SIGNIFICANT_CHEST_WALL_DEFORMITY"].alias("significant_chest_wall_deformity"),
        s["PREOPERATIVE_TRANSCATHETER_INTERVENTIONSTXT"].alias("preoperative_transcatheter_interventionstxt"),
        s["RV_FRACTIONAL_AREA_CHANGE"].alias("rv_fractional_area_change"),
        s["TAPSE"].alias("tapse"),
        s["CORONARY_CT"].alias("coronary_ct"),
        s["ABDOMINAL_OR_DESCENDING"].alias("abdominal_or_descending"),
        s["ROCKWOOD_FRAILTY_SCORE"].alias("rockwood_frailty_score"),
        s["ABDOMINAL_OR_DESCENDING_AORTIC"].alias("abdominal_or_descending_aortic"),
        s["M_OTHER_PREVIOUS_ENDOVASCULAR_SURGERY"].alias("m_other_previous_endovascular_surgery"),
        s["AORTIC_VALVE_ANATOMY"].alias("aortic_valve_anatomy"),
        s["OTHER_AORTIC_VALVE_ANATOMY"].alias("other_aortic_valve_anatomy"),
        s["AORTIC_VALVE_DISEASETXT"].alias("aortic_valve_diseasetxt"),
        s["AORTIC_VALVE_REGURGITATION"].alias("aortic_valve_regurgitation"),
        s["ERO_AAORTIC"].alias("ero_aaortic"),
        s["VENA_CONTRACTA_WIDTH_AORTIC"].alias("vena_contracta_width_aortic"),
        s["AR_JET_DIRECTION"].alias("ar_jet_direction"),
        s["LV_END_SYSTOLIC_DIAMETER_AORTIC"].alias("lv_end_systolic_diameter_aortic"),
        s["LV_END_DIASTOLIC_DIAMETER_AORTIC"].alias("lv_end_diastolic_diameter_aortic"),
        s["SEVERITY_OF_AORTIC_VALVE_STENOSIS_AVA"].alias("severity_of_aortic_valve_stenosis_ava"),
        s["MITRAL_VALVE_DISEASETXT"].alias("mitral_valve_diseasetxt"),
        s["MITRAL_VALVE_REGURGITATION"].alias("mitral_valve_regurgitation"),
        s["MECHANISM_OF_MITRAL_REGURGITATIONTXT"].alias("mechanism_of_mitral_regurgitationtxt"),
        s["FLAIL"].alias("flail"),
        s["PROLAPSE"].alias("prolapse"),
        s["RESTRICTION"].alias("restriction"),
        s["DEGREE_OF_MYXOMATOUS_CHANGE"].alias("degree_of_myxomatous_change"),
        s["ERO_AMITRAL"].alias("ero_amitral"),
        s["VENA_CONTRACTA_WIDTH_MITRAL"].alias("vena_contracta_width_mitral"),
        s["MR_JET_DIRECTION"].alias("mr_jet_direction"),
        s["LV_END_SYSTOLIC_DIAMETER_MITRAL"].alias("lv_end_systolic_diameter_mitral"),
        s["LV_END_DIASTOLIC_DIAMETER_MITRAL"].alias("lv_end_diastolic_diameter_mitral"),
        s["L_ADIAMETER"].alias("l_adiameter"),
        s["R_VIMPAIRMENT"].alias("r_vimpairment"),
        s["PERICARDIAL_EFFUSION"].alias("pericardial_effusion"),
        s["MITRAL_VALVE_STENOSIS"].alias("mitral_valve_stenosis"),
        s["MITRAL_ANNULUS_CALCIFICATION"].alias("mitral_annulus_calcification"),
        s["SEVERITY_OF_MITRAL_VALVE_STENOSIS_EOA"].alias("severity_of_mitral_valve_stenosis_eoa"),
        s["SEVERITY_OF_MITRAL_VALVE_STENOSIS_GRADIENT"].alias("severity_of_mitral_valve_stenosis_gradient"),
        s["DATE_LAST_CHANGED"].alias("date_last_changed"),
        s["PRE1_ADC_UPDT"].alias("pre1_adc_updt"),
        s["NHS_NUMBER_VALID_IND"].alias("nhs_number_valid_ind"),
        s["LINKAGE_STATUS"].alias("linkage_status"),
        s["PERSON_ID"].alias("person_id"),
        s["LINKAGE_METHOD"].alias("linkage_method"),
        s["LINKAGE_HISTORICAL_FALLBACK_IND"].alias("linkage_historical_fallback_ind"),
        s["LINKAGE_FALLBACK_CONFLICT_IND"].alias("linkage_fallback_conflict_ind"),
        s["PRE2_DATE_LAST_CHANGED"].alias("pre2_date_last_changed"),
        s["PRE2_ADC_UPDT"].alias("pre2_adc_updt"),
        s["POST1_DATE_LAST_CHANGED"].alias("post1_date_last_changed"),
        s["POST1_ADC_UPDT"].alias("post1_adc_updt"),
        s["POST2_DATE_LAST_CHANGED"].alias("post2_date_last_changed"),
        s["POST2_ADC_UPDT"].alias("post2_adc_updt"),
        s["EPISODE_LAST_CHANGED"].alias("episode_last_changed"),
        s["ADC_UPDT"].alias("adc_updt"),
        s["SOURCE_TABLE"].alias("source_table"),
        s["SOURCE_RECORD_KEY"].alias("source_record_key"),
        s["PRE2_MODULE"].alias("pre2_module"),
        s["POST1_MODULE"].alias("post1_module"),
        s["POST2_MODULE"].alias("post2_module"),
        s["ROW_HASH"].alias("row_hash"),
        s["PIPELINE_LOADED_AT"].alias("pipeline_loaded_at"),
        s["IS_CURRENT_IN_SOURCE"].alias("is_current_in_source"),
        s["SOURCE_PRESENT_IND"].alias("source_present_ind"),
        s["SOURCE_ABSENT_DETECTED_TS"].alias("source_absent_detected_ts"),
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_registry_surgery_episode': {'columns': {'abdominal_or_descending': 'Coded pre-operative field '
                                                                              'relating to abdominal or '
                                                                              'descending aortic disease or '
                                                                              'prior surgery.',
                                                   'abdominal_or_descending_aortic': 'Coded pre-operative '
                                                                                     'field relating to '
                                                                                     'abdominal or '
                                                                                     'descending aortic '
                                                                                     'pathology or '
                                                                                     'intervention, held '
                                                                                     'alongside the related '
                                                                                     'abdominal or '
                                                                                     'descending field.',
                                                   'actual_creatinine_at_time_of_surgery': 'Serum creatinine '
                                                                                           'result recorded '
                                                                                           'at the time of '
                                                                                           'surgery, as '
                                                                                           'captured in the '
                                                                                           'registry.',
                                                   'adc_updt': 'Greatest landing timestamp across the four '
                                                               'source modules; excluded from ROW_HASH.',
                                                   'administrative_category': 'Administrative funding '
                                                                              'category of the patient for '
                                                                              'this episode, such as NHS or '
                                                                              'private care.',
                                                   'aetiologytxt': 'Coded aetiology option or options '
                                                                   'recorded for the valve or aortic '
                                                                   'pathology, stored as concatenated '
                                                                   'code-label text.',
                                                   'age_at_operation': 'Patient age in completed years at '
                                                                       'the date of the operation.',
                                                   'angina_status_pre_surgery': 'Pre-operative angina '
                                                                                'severity classification '
                                                                                'recorded before surgery.',
                                                   'antibiotic_treatment_for_endocarditis': 'Indicator of '
                                                                                            'whether the '
                                                                                            'patient was '
                                                                                            'receiving '
                                                                                            'antibiotic '
                                                                                            'treatment for '
                                                                                            'endocarditis '
                                                                                            'before surgery.',
                                                   'aortic_valve_anatomy': 'Coded description of aortic '
                                                                           'valve anatomy recorded at '
                                                                           'pre-operative assessment.',
                                                   'aortic_valve_diseasetxt': 'Text label of aortic valve '
                                                                              'disease selected from the '
                                                                              'pre-operative valve '
                                                                              'assessment option list.',
                                                   'aortic_valve_regurgitation': 'Coded severity grade of '
                                                                                 'aortic valve regurgitation '
                                                                                 'recorded pre-operatively.',
                                                   'ar_jet_direction': 'Coded direction of the aortic '
                                                                       'regurgitant jet observed on '
                                                                       'pre-operative echocardiography.',
                                                   'arterial_cannulation_version5pt2': 'Arterial cannulation '
                                                                                       'site or method '
                                                                                       'recorded using the '
                                                                                       'version 5.2 registry '
                                                                                       'option list.',
                                                   'cabg': 'Indicator of whether the episode included '
                                                           'coronary artery bypass grafting.',
                                                   'cardiac_procedures': 'Coded cardiac procedure category '
                                                                         'recorded in the pre-operative '
                                                                         'module for this episode.',
                                                   'cardiogenic_shock_pre_operation': 'Indicator of whether '
                                                                                      'the patient was in '
                                                                                      'cardiogenic shock '
                                                                                      'before the operation.',
                                                   'cardiology_consultanttxt': 'Recorded name of the '
                                                                               'cardiology consultant '
                                                                               'associated with the episode.',
                                                   'category_of_aortic_valve_stenosis': 'Coded category of '
                                                                                        'aortic valve '
                                                                                        'stenosis recorded '
                                                                                        'at pre-operative '
                                                                                        'assessment in the '
                                                                                        'CS2010G pre1 '
                                                                                        'module.',
                                                   'cigarette_smoking_history': 'Recorded cigarette smoking '
                                                                                'status history of the '
                                                                                'patient before surgery.',
                                                   'clinical_presentation_malperfusiontxt': 'Text label of '
                                                                                            'the recorded '
                                                                                            'malperfusion '
                                                                                            'component of '
                                                                                            'the clinical '
                                                                                            'presentation in '
                                                                                            'the iWeb '
                                                                                            'CS2010G '
                                                                                            'pre-operative '
                                                                                            'module.',
                                                   'clinical_presentationtxt': 'Text label of the recorded '
                                                                               'clinical presentation '
                                                                               'category for the episode, as '
                                                                               'captured in the iWeb CS2010G '
                                                                               'pre-operative module.',
                                                   'consultant_responsible_for_proceduretxt': 'Text label '
                                                                                              'naming the '
                                                                                              'consultant '
                                                                                              'responsible '
                                                                                              'for the '
                                                                                              'procedure, as '
                                                                                              'selected from '
                                                                                              'the iWeb '
                                                                                              'staff '
                                                                                              'picklist.',
                                                   'conversion_to_sternotomy': 'Coded indicator of whether '
                                                                               'the procedure was converted '
                                                                               'to a sternotomy.',
                                                   'coronary_ct': 'Coded indicator of whether pre-operative '
                                                                  'coronary CT imaging was performed.',
                                                   'covid19_diagnosis': 'Coded record of COVID-19 diagnosis '
                                                                        'status relating to the surgical '
                                                                        'episode.',
                                                   'covid19_pt': 'Coded indicator of whether the patient was '
                                                                 'a COVID-19 patient at the time of the '
                                                                 'episode.',
                                                   'cpet_anaerobic_threshold': 'Anaerobic threshold measured '
                                                                               'at pre-operative '
                                                                               'cardiopulmonary exercise '
                                                                               'testing, recorded in the '
                                                                               'CS2010G pre-operative '
                                                                               'module.',
                                                   'creatinine_clearance': 'Pre-operative creatinine '
                                                                           'clearance value recorded in the '
                                                                           'registry.',
                                                   'ct_aorta_arch_las': 'Aortic arch measurement at the left '
                                                                        'subclavian artery level recorded '
                                                                        'from the pre-operative CT aorta '
                                                                        'study.',
                                                   'ct_aorta_arch_lcca': 'Aortic arch measurement at the '
                                                                         'left common carotid artery '
                                                                         'recorded from the pre-operative CT '
                                                                         'aorta study.',
                                                   'ct_aorta_diagnosistxt': 'Text label of the diagnosis '
                                                                            'recorded from the pre-operative '
                                                                            'CT aorta study in the iWeb '
                                                                            'CS2010G pre-operative module.',
                                                   'ct_aorta_distal_aa': 'Distal ascending aorta measurement '
                                                                         'recorded from the pre-operative CT '
                                                                         'aorta study.',
                                                   'ct_aorta_distal_dt_adiaphragm': 'Distal descending '
                                                                                    'thoracic aorta '
                                                                                    'measurement at '
                                                                                    'diaphragm level '
                                                                                    'recorded from the '
                                                                                    'pre-operative CT aorta '
                                                                                    'study.',
                                                   'ct_aorta_dt': 'Date and time of the pre-operative CT '
                                                                  'aorta study recorded in the iWeb CS2010G '
                                                                  'pre-operative module.',
                                                   'ct_aorta_extent_of_dissectiontxt': 'Text label '
                                                                                       'describing the '
                                                                                       'extent of aortic '
                                                                                       'dissection seen on '
                                                                                       'the pre-operative CT '
                                                                                       'aorta study.',
                                                   'ct_aorta_infrarenal': 'Infrarenal aortic measurement '
                                                                          'recorded from the pre-operative '
                                                                          'CT aorta study.',
                                                   'ct_aorta_mid_aa': 'Mid ascending aorta measurement '
                                                                      'recorded from the pre-operative CT '
                                                                      'aorta study.',
                                                   'ct_aorta_mid_dta': 'Mid descending thoracic aorta '
                                                                       'measurement recorded from the '
                                                                       'pre-operative CT aorta study.',
                                                   'ct_aorta_other_dx': 'Free-text description of any other '
                                                                        'diagnosis identified on the '
                                                                        'pre-operative CT aorta study.',
                                                   'ct_aorta_primary_entry_teartxt': 'Text label describing '
                                                                                     'the location of the '
                                                                                     'primary entry tear '
                                                                                     'identified on the '
                                                                                     'pre-operative CT aorta '
                                                                                     'study.',
                                                   'ct_aorta_proximal_dta': 'Proximal descending thoracic '
                                                                            'aorta measurement recorded from '
                                                                            'the pre-operative CT aorta '
                                                                            'study.',
                                                   'ct_aorta_root': 'Aortic root measurement recorded from '
                                                                    'the pre-operative CT aorta study.',
                                                   'ct_aorta_secondary_entry_tear_arch': 'Recorded '
                                                                                         'indication of a '
                                                                                         'secondary entry '
                                                                                         'tear at the aortic '
                                                                                         'arch on the '
                                                                                         'pre-operative CT '
                                                                                         'aorta study.',
                                                   'ct_aorta_stj': 'Sinotubular junction measurement '
                                                                   'recorded from the pre-operative CT aorta '
                                                                   'study.',
                                                   'ct_aorta_visceral_level': 'Aortic measurement at the '
                                                                              'visceral segment level '
                                                                              'recorded from the '
                                                                              'pre-operative CT aorta study.',
                                                   'date_and_time_of_operation': 'Date and time at which the '
                                                                                 'surgical procedure started '
                                                                                 'for this episode.',
                                                   'date_and_time_of_operation_finished': 'Date and time at '
                                                                                          'which the '
                                                                                          'surgical '
                                                                                          'operation '
                                                                                          'finished.',
                                                   'date_last_changed': 'Timestamp of the most recent change '
                                                                        'to the pre1 module record in the '
                                                                        'source iWeb registry.',
                                                   'date_of_admission': 'Date the patient was admitted for '
                                                                        'the episode of care leading to this '
                                                                        'operation.',
                                                   'date_of_birth': 'Patient date of birth as recorded in '
                                                                    'the iWeb CS2010G pre-operative registry '
                                                                    'module.',
                                                   'date_of_death': 'Recorded date of patient death, null '
                                                                    'where no death has been recorded on the '
                                                                    'registry.',
                                                   'date_of_last_cardiac_operation': "Date of the patient's "
                                                                                     'most recent previous '
                                                                                     'cardiac operation, '
                                                                                     'null where there was '
                                                                                     'no prior surgery.',
                                                   'date_of_last_catheterisation': "Date of the patient's "
                                                                                   'most recent cardiac '
                                                                                   'catheterisation prior to '
                                                                                   'the surgical episode, as '
                                                                                   'recorded in the CS2010G '
                                                                                   'pre-operative module.',
                                                   'date_of_operation': 'Date of the cardiac surgical '
                                                                        'operation for this episode.',
                                                   'degree_of_myxomatous_change': 'Recorded degree of '
                                                                                  'myxomatous change of the '
                                                                                  'mitral valve on '
                                                                                  'pre-operative assessment.',
                                                   'diabetes_management': 'Method of diabetes management '
                                                                          'recorded before surgery, '
                                                                          'including a not-diabetic option.',
                                                   'diabetes_type': 'Coded type of diabetes recorded '
                                                                    'pre-operatively.',
                                                   'dialysis': 'Pre-operative dialysis status, including '
                                                               'type and timing of dialysis relative to '
                                                               'cardiac surgery.',
                                                   'diffusion_capacity_percent_predicted': 'Pre-operative '
                                                                                           'lung diffusion '
                                                                                           'capacity '
                                                                                           'expressed as a '
                                                                                           'percentage of '
                                                                                           'the predicted '
                                                                                           'value, from the '
                                                                                           'CS2010G '
                                                                                           'pre-operative '
                                                                                           'module.',
                                                   'diffusion_capacitymlminkpa': 'Pre-operative lung '
                                                                                 'diffusion capacity '
                                                                                 'measured in ml/min/kPa.',
                                                   'discussed_at_documented_quorate_preop_mdt': 'Coded '
                                                                                                'indicator '
                                                                                                'of whether '
                                                                                                'the case '
                                                                                                'was '
                                                                                                'discussed '
                                                                                                'at a '
                                                                                                'documented '
                                                                                                'quorate '
                                                                                                'pre-operative '
                                                                                                'multidisciplinary '
                                                                                                'team '
                                                                                                'meeting.',
                                                   'dyspnoea_status_pre_surgery': 'Pre-operative dyspnoea '
                                                                                  'severity classification '
                                                                                  'recorded before surgery.',
                                                   'ejection_fraction_category': 'Coded category of '
                                                                                 'pre-operative left '
                                                                                 'ventricular ejection '
                                                                                 'fraction used in cardiac '
                                                                                 'surgery risk assessment.',
                                                   'entry_id': 'Stable iWeb episode key and parent of '
                                                               'iweb_cardiac_surgery_procedure.',
                                                   'episode_last_changed': 'Greatest DateLastChanged '
                                                                           'timestamp across the four source '
                                                                           'modules.',
                                                   'ero_aaortic': 'Effective regurgitant orifice area for '
                                                                  'aortic regurgitation measured on '
                                                                  'pre-operative echocardiography.',
                                                   'ero_amitral': 'Effective regurgitant orifice area of the '
                                                                  'mitral valve measured on pre-operative '
                                                                  'echocardiography.',
                                                   'ethnic_origin': 'Coded ethnic origin of the patient as '
                                                                    'recorded in the pre-operative registry '
                                                                    'module.',
                                                   'extent_of_coronary_vessel_disease': 'Coded pre-operative '
                                                                                        'assessment of the '
                                                                                        'number of coronary '
                                                                                        'vessels with '
                                                                                        'significant '
                                                                                        'stenosis.',
                                                   'extracardiac_arteriopathy': 'Indicates whether '
                                                                                'extracardiac arteriopathy '
                                                                                'was recorded as a '
                                                                                'pre-operative risk factor.',
                                                   'fev1': 'Pre-operative forced expiratory volume in one '
                                                           'second recorded from lung function testing.',
                                                   'fev1_fv_cpercent': 'Pre-operative FEV1/FVC expressed as '
                                                                       'a percentage from lung function '
                                                                       'testing.',
                                                   'fev1_fv_cratio': 'Pre-operative ratio of FEV1 to forced '
                                                                     'vital capacity from lung function '
                                                                     'testing.',
                                                   'fev1_percent_predicted': 'Pre-operative FEV1 expressed '
                                                                             'as a percentage of the '
                                                                             'predicted value.',
                                                   'first_assistant_calman_year_of_trainee': 'Calman '
                                                                                             'training year '
                                                                                             'of the first '
                                                                                             'assistant when '
                                                                                             'that assistant '
                                                                                             'is a trainee.',
                                                   'first_assistant_grade': 'Coded staff grade of the first '
                                                                            'assistant in the operating '
                                                                            'team.',
                                                   'first_assistanttxt': 'Free-text name of the first '
                                                                         'assistant in the operating team.',
                                                   'first_operator_calman_year_of_trainee': 'Calman training '
                                                                                            'year of the '
                                                                                            'first operator '
                                                                                            'when that '
                                                                                            'operator is a '
                                                                                            'trainee.',
                                                   'first_operator_grade': 'Coded staff grade of the first '
                                                                           'operator performing the '
                                                                           'procedure, as recorded in the '
                                                                           'CS2010G pre-operative module.',
                                                   'first_operatortxt': 'Free-text name of the first '
                                                                        'operator performing the procedure.',
                                                   'flail': 'Indicator of mitral leaflet flail identified on '
                                                            'pre-operative valve assessment.',
                                                   'forename': 'Patient forename as recorded in the iWeb '
                                                               'CS2010G pre-operative registry module.',
                                                   'gender': 'Patient gender recorded on the pre-operative '
                                                             'CS2010G module for this surgical episode.',
                                                   'haemoglobin_at_time_of_surgery': 'Pre-operative '
                                                                                     'haemoglobin '
                                                                                     'concentration recorded '
                                                                                     'at the time of the '
                                                                                     'surgical episode, as '
                                                                                     'entered in the iWeb '
                                                                                     'CS2010G pre-operative '
                                                                                     'module.',
                                                   'harley_street_clinic': 'Coded flag indicating whether '
                                                                           'the episode was associated with '
                                                                           'the Harley Street Clinic.',
                                                   'height_in_cm': 'Patient height in centimetres recorded '
                                                                   'pre-operatively.',
                                                   'history_of_hypertension': 'Indicates whether the patient '
                                                                              'has a recorded history of '
                                                                              'hypertension prior to '
                                                                              'admission.',
                                                   'history_of_neurological_diseasetxt': 'Coded description '
                                                                                         'of any history of '
                                                                                         'neurological '
                                                                                         'disease recorded '
                                                                                         'before surgery.',
                                                   'history_of_neurological_dysfunction': 'Indicates whether '
                                                                                          'pre-operative '
                                                                                          'neurological '
                                                                                          'dysfunction '
                                                                                          'severely '
                                                                                          'affecting '
                                                                                          'ambulation or '
                                                                                          'day-to-day '
                                                                                          'functioning was '
                                                                                          'recorded.',
                                                   'history_of_poor_mobility': 'Coded indicator of '
                                                                               'pre-operative poor mobility '
                                                                               'used in cardiac surgical '
                                                                               'risk assessment.',
                                                   'history_of_pulmonary_disease': 'Recorded history of '
                                                                                   'chronic pulmonary '
                                                                                   'disease before surgery.',
                                                   'hospital_identifier_old': 'Legacy hospital or unit '
                                                                              'identifier code retained from '
                                                                              'the iWeb CS2010G '
                                                                              'pre-operative module.',
                                                   'hospital_identifiertxt': 'Text label of the hospital or '
                                                                             'trust identifier recorded for '
                                                                             'the episode in the iWeb '
                                                                             'CS2010G pre-operative module.',
                                                   'hypercholesterolaemia': 'Indicator of a recorded history '
                                                                            'of hypercholesterolaemia before '
                                                                            'surgery.',
                                                   'incision': 'Coded surgical incision type recorded for '
                                                               'the episode in the CS2010G pre-operative '
                                                               'module.',
                                                   'interval_between_surgery_and_last_mi': 'Categorised time '
                                                                                           'interval between '
                                                                                           'the most recent '
                                                                                           'myocardial '
                                                                                           'infarction and '
                                                                                           'the operation.',
                                                   'intravenous_inotropes_prior_to_anaesthesia': 'Indicator '
                                                                                                 'of whether '
                                                                                                 'intravenous '
                                                                                                 'inotropes '
                                                                                                 'were '
                                                                                                 'administered '
                                                                                                 'prior to '
                                                                                                 'induction '
                                                                                                 'of '
                                                                                                 'anaesthesia.',
                                                   'intravenous_nitrates_or_any_heparin': 'Indicator of '
                                                                                          'whether the '
                                                                                          'patient received '
                                                                                          'intravenous '
                                                                                          'nitrates or any '
                                                                                          'heparin before '
                                                                                          'operation.',
                                                   'is_current_in_source': 'Boolean flag indicating whether '
                                                                           'the episode is still present as '
                                                                           'a current record in the iWeb '
                                                                           'source.',
                                                   'l_adiameter': 'Left atrial diameter measured on '
                                                                  'pre-operative echocardiography.',
                                                   'left_heart_catheterisation': 'Indicates whether left '
                                                                                 'heart catheterisation was '
                                                                                 'performed before surgery.',
                                                   'left_main_stem_disease': 'Coded assessment of left main '
                                                                             'stem coronary artery disease '
                                                                             'severity before surgery.',
                                                   'left_ventricular_function': 'Recorded pre-operative left '
                                                                                'ventricular function value '
                                                                                'from cardiac imaging or '
                                                                                'catheterisation.',
                                                   'linkage_fallback_conflict_ind': 'Boolean flag indicating '
                                                                                    'that the fallback '
                                                                                    'linkage step returned '
                                                                                    'conflicting candidate '
                                                                                    'person matches.',
                                                   'linkage_historical_fallback_ind': 'Boolean flag '
                                                                                      'indicating that the '
                                                                                      'PERSON_ID linkage was '
                                                                                      'resolved using a '
                                                                                      'historical alias '
                                                                                      'fallback rather than '
                                                                                      'a current identifier '
                                                                                      'match.',
                                                   'linkage_method': 'Derived indicator of which identifier '
                                                                     'combination was used to resolve the '
                                                                     'patient linkage.',
                                                   'linkage_status': 'Derived status describing the outcome '
                                                                     'of linking the registry episode to the '
                                                                     'master patient index.',
                                                   'lv_end_diastolic_diameter_aortic': 'Left ventricular '
                                                                                       'end-diastolic '
                                                                                       'diameter recorded '
                                                                                       'within the '
                                                                                       'pre-operative aortic '
                                                                                       'valve assessment.',
                                                   'lv_end_diastolic_diameter_mitral': 'Left ventricular '
                                                                                       'end-diastolic '
                                                                                       'diameter recorded as '
                                                                                       'part of the '
                                                                                       'pre-operative mitral '
                                                                                       'valve assessment.',
                                                   'lv_end_systolic_diameter_aortic': 'Left ventricular '
                                                                                      'end-systolic diameter '
                                                                                      'recorded within the '
                                                                                      'pre-operative aortic '
                                                                                      'valve assessment.',
                                                   'lv_end_systolic_diameter_mitral': 'Left ventricular '
                                                                                      'end-systolic diameter '
                                                                                      'recorded as part of '
                                                                                      'the pre-operative '
                                                                                      'mitral valve '
                                                                                      'assessment.',
                                                   'm_other_previous_endovascular_surgery': 'Free-text '
                                                                                            'detail '
                                                                                            'describing '
                                                                                            'other previous '
                                                                                            'endovascular '
                                                                                            'surgery not '
                                                                                            'covered by the '
                                                                                            'coded options.',
                                                   'm_pre_operation_history': 'Free-text pre-operative '
                                                                              'clinical history narrative '
                                                                              'for the episode.',
                                                   'major_aortic': 'Indicator of whether the episode '
                                                                   'included a major aortic procedure.',
                                                   'maximal_oxygen_uptake': 'Maximal oxygen uptake recorded '
                                                                            'at pre-operative '
                                                                            'cardiopulmonary exercise '
                                                                            'testing in the CS2010G '
                                                                            'pre-operative module.',
                                                   'mean_pawpla': 'Mean pulmonary artery wedge or left '
                                                                  'atrial pressure recorded at pre-operative '
                                                                  'cardiac investigation.',
                                                   'mechanism_of_mitral_regurgitationtxt': 'Coded mechanism '
                                                                                           'of mitral '
                                                                                           'regurgitation '
                                                                                           'identified '
                                                                                           'pre-operatively, '
                                                                                           'held as the '
                                                                                           "registry's "
                                                                                           'decoded text '
                                                                                           'label.',
                                                   'mitral_annulus_calcification': 'Recorded presence or '
                                                                                   'degree of mitral annular '
                                                                                   'calcification on '
                                                                                   'pre-operative '
                                                                                   'assessment.',
                                                   'mitral_valve_diseasetxt': 'Coded description of '
                                                                              'pre-operative mitral valve '
                                                                              'disease, held as the '
                                                                              "registry's decoded text "
                                                                              'label.',
                                                   'mitral_valve_regurgitation': 'Pre-operative grade of '
                                                                                 'mitral valve regurgitation '
                                                                                 'recorded as a coded '
                                                                                 'category.',
                                                   'mitral_valve_stenosis': 'Pre-operative grade or presence '
                                                                            'of mitral valve stenosis '
                                                                            'recorded as a coded category.',
                                                   'mr_jet_direction': 'Direction of the mitral regurgitant '
                                                                       'jet recorded on pre-operative '
                                                                       'echocardiography.',
                                                   'mrn': 'Native source hospital number retained for '
                                                          'trace-back.',
                                                   'myectomy_optionstxt': 'Text label of the myectomy option '
                                                                          'recorded for the procedure.',
                                                   'nhs_number': 'Native source NHS number retained for '
                                                                 'governed trace-back.',
                                                   'nhs_number_valid_ind': 'Derived flag indicating whether '
                                                                           'the source NHS number passed '
                                                                           'validation, retained after the '
                                                                           'NHS number itself was dropped.',
                                                   'number_of_previous_heart_operations': 'Count of previous '
                                                                                          'cardiac '
                                                                                          'operations '
                                                                                          'undergone by the '
                                                                                          'patient before '
                                                                                          'this episode.',
                                                   'number_of_previous_mis': 'Categorised count of previous '
                                                                             'myocardial infarctions '
                                                                             'recorded before surgery.',
                                                   'operative_urgency': 'Coded urgency category of the '
                                                                        'cardiac surgical operation as '
                                                                        'recorded pre-operatively in the '
                                                                        'CS2010G registry.',
                                                   'other_actual_cardiac_procedurestxt': 'Text field listing '
                                                                                         'additional cardiac '
                                                                                         'procedures '
                                                                                         'performed, '
                                                                                         'generally holding '
                                                                                         'coded picklist '
                                                                                         'descriptions.',
                                                   'other_aortic_valve_anatomy': 'Text detail of aortic '
                                                                                 'valve anatomy when not '
                                                                                 'covered by the coded '
                                                                                 'anatomy options.',
                                                   'other_cardiac_procedure_details': 'Free-text detail '
                                                                                      'describing other '
                                                                                      'cardiac procedures '
                                                                                      'performed that are '
                                                                                      'not covered by the '
                                                                                      'coded procedure '
                                                                                      'fields.',
                                                   'other_cardiac_procedures': 'Indicator of whether other '
                                                                               'cardiac procedures were '
                                                                               'performed in addition to the '
                                                                               'main procedure categories.',
                                                   'other_cardiology_consultant': 'Additional cardiology '
                                                                                  'consultant recorded for '
                                                                                  'the patient where '
                                                                                  'different from the main '
                                                                                  'cardiology consultant.',
                                                   'other_cardiology_consultant_gm_cnumber': 'GMC '
                                                                                             'registration '
                                                                                             'number of the '
                                                                                             'other '
                                                                                             'cardiology '
                                                                                             'consultant '
                                                                                             'recorded for '
                                                                                             'the episode.',
                                                   'other_thoracic_and_vascular_procedurestxt': 'Coded '
                                                                                                'selection '
                                                                                                'of other '
                                                                                                'thoracic '
                                                                                                'and '
                                                                                                'vascular '
                                                                                                'procedures '
                                                                                                'associated '
                                                                                                'with the '
                                                                                                'episode, '
                                                                                                'stored as '
                                                                                                'concatenated '
                                                                                                'code-label '
                                                                                                'text.',
                                                   'pa_systolic': 'Pre-operative pulmonary artery systolic '
                                                                  'pressure as recorded in the registry.',
                                                   'patient_event_key': 'Deterministic SHA-256 key matching '
                                                                        'clinical_registry_entry.patient_event_key '
                                                                        'for the surgery_episode source '
                                                                        'family and native entry_id.',
                                                   'perfusionisttxt': 'Text label identifying the '
                                                                      'perfusionist selected for the case '
                                                                      'from the iWeb staff picklist.',
                                                   'pericardial_effusion': 'Indicator of pericardial '
                                                                           'effusion identified on '
                                                                           'pre-operative assessment.',
                                                   'person_id': 'Resolved independently through NHS number '
                                                                'and MRN; NULL on conflict or ambiguity.',
                                                   'pipeline_loaded_at': 'Timestamp recording when the '
                                                                         'pipeline loaded this row into the '
                                                                         'table.',
                                                   'poor_mobility_non_cardiac_reason': 'Indicator of whether '
                                                                                       'poor mobility is '
                                                                                       'attributable to a '
                                                                                       'non-cardiac reason.',
                                                   'post1_adc_updt': 'Technical timestamp of the last '
                                                                     'extract or load update of the post1 '
                                                                     'module row.',
                                                   'post1_date_last_changed': 'Timestamp of the last change '
                                                                              'to the post1 module record in '
                                                                              'the source iWeb registry.',
                                                   'post1_module': 'Typed lossless payload of '
                                                                   'reg_cs2010g_post1 fields except promoted '
                                                                   'module timestamps.',
                                                   'post2_adc_updt': 'Ingestion timestamp indicating when '
                                                                     'the post2 module row was last updated '
                                                                     'in the data platform.',
                                                   'post2_date_last_changed': 'Timestamp of the last '
                                                                              'recorded change to the post2 '
                                                                              'module record in the iWeb '
                                                                              'source system.',
                                                   'post2_module': 'Typed lossless payload of '
                                                                   'reg_cs2010g_post2 fields except promoted '
                                                                   'module timestamps.',
                                                   'postcode_of_usual_address': "Postcode of the patient's "
                                                                                'usual address as recorded '
                                                                                'in the iWeb CS2010G '
                                                                                'pre-operative registry '
                                                                                'module.',
                                                   'pre1_adc_updt': 'Technical ingestion/update timestamp '
                                                                    'for the pre1 source row within the data '
                                                                    'platform.',
                                                   'pre2_adc_updt': 'Technical timestamp of the last '
                                                                    'ingestion or update of the CS2010G pre2 '
                                                                    'row in the data platform.',
                                                   'pre2_date_last_changed': 'Timestamp of the last edit to '
                                                                             'the CS2010G pre2 record in the '
                                                                             'iWeb source system.',
                                                   'pre2_module': 'Typed lossless payload of '
                                                                  'reg_cs2010g_pre2 fields except promoted '
                                                                  'module timestamps.',
                                                   'pre_op_support_devices_usedtxt': 'Coded selection of '
                                                                                     'mechanical support '
                                                                                     'devices used before '
                                                                                     'the operation, stored '
                                                                                     'as concatenated '
                                                                                     'code-label text.',
                                                   'pre_operative_heart_rhythm': 'Coded cardiac rhythm '
                                                                                 'recorded at pre-operative '
                                                                                 'assessment in the CS2010G '
                                                                                 'pre-operative module.',
                                                   'preoperative_transcatheter_interventionstxt': 'Text '
                                                                                                  'label of '
                                                                                                  'pre-operative '
                                                                                                  'transcatheter '
                                                                                                  'interventions '
                                                                                                  'selected '
                                                                                                  'from the '
                                                                                                  'registry '
                                                                                                  'option '
                                                                                                  'list.',
                                                   'presentationtxt': 'Coded presentation option or options '
                                                                      'recorded for the valve or aortic '
                                                                      'pathology, stored as concatenated '
                                                                      'code-label text.',
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
                                                                                                   'from the '
                                                                                                   'pre-operative '
                                                                                                   'option '
                                                                                                   'list.',
                                                   'previous_cardiac_surgerytxt': 'Coded description of any '
                                                                                  'previous cardiac surgery '
                                                                                  'undertaken before this '
                                                                                  'episode.',
                                                   'previous_endovascular_surgerytxt': 'Text label recording '
                                                                                       'previous '
                                                                                       'endovascular surgery '
                                                                                       'selected from the '
                                                                                       'pre-operative option '
                                                                                       'list.',
                                                   'previous_pci': 'Indicates whether the patient had '
                                                                   'previous percutaneous coronary '
                                                                   'intervention and its timing category.',
                                                   'primary_operator': 'Primary operator recorded for the '
                                                                       'procedure in the CS2010G '
                                                                       'pre-operative module.',
                                                   'prior_right_chest_surgery_or_radiation': 'Coded '
                                                                                             'indicator of '
                                                                                             'prior right '
                                                                                             'chest surgery '
                                                                                             'or '
                                                                                             'radiotherapy '
                                                                                             'recorded '
                                                                                             'pre-operatively.',
                                                   'prolapse': 'Indicator of mitral leaflet prolapse '
                                                               'identified on pre-operative valve '
                                                               'assessment.',
                                                   'pulmonary_hypertension': 'Coded pre-operative pulmonary '
                                                                             'hypertension status.',
                                                   'r_vimpairment': 'Recorded degree of right ventricular '
                                                                    'impairment on pre-operative assessment.',
                                                   'reason_for_pre_operative_impeller_device_use': 'Coded '
                                                                                                   'reason '
                                                                                                   'for use '
                                                                                                   'of an '
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
                                                   'referring_hospitaltxt': 'Text label of the hospital that '
                                                                            'referred the patient for the '
                                                                            'cardiac surgical episode.',
                                                   'renal_impairment': 'Coded category of pre-operative '
                                                                       'renal impairment.',
                                                   'responsible_consultant_anaesthetisttxt': 'Text label '
                                                                                             'naming the '
                                                                                             'consultant '
                                                                                             'anaesthetist '
                                                                                             'responsible '
                                                                                             'for the case, '
                                                                                             'selected from '
                                                                                             'the iWeb staff '
                                                                                             'picklist.',
                                                   'responsible_consultant_surgeontxt': 'Text label naming '
                                                                                        'the consultant '
                                                                                        'surgeon responsible '
                                                                                        "for the patient's "
                                                                                        'care for this '
                                                                                        'episode.',
                                                   'restriction': 'Indicator of mitral leaflet restriction '
                                                                  'identified on pre-operative valve '
                                                                  'assessment.',
                                                   'rockwood_frailty_score': 'Rockwood clinical frailty '
                                                                             'scale category recorded at '
                                                                             'pre-operative assessment.',
                                                   'row_hash': "Deterministic hash of the merged row's "
                                                               'source column values used for change '
                                                               'detection.',
                                                   'rv_fractional_area_change': 'Right ventricular '
                                                                                'fractional area change '
                                                                                'measured on pre-operative '
                                                                                'echocardiography.',
                                                   'second_assistant_calman_year_of_trainee': 'Calman '
                                                                                              'training year '
                                                                                              'recorded for '
                                                                                              'the second '
                                                                                              'assistant '
                                                                                              'when that '
                                                                                              'assistant is '
                                                                                              'a trainee.',
                                                   'second_assistant_grade': 'Coded staff grade of the '
                                                                             'second assistant at the '
                                                                             'operation, as recorded in the '
                                                                             'iWeb CS2010G pre-operative '
                                                                             'module.',
                                                   'second_assistanttxt': 'Recorded name of the second '
                                                                          'assistant present at the '
                                                                          'operation.',
                                                   'secondary_assisting_consultant_surgeontxt': 'Text label '
                                                                                                'naming the '
                                                                                                'secondary '
                                                                                                'assisting '
                                                                                                'consultant '
                                                                                                'surgeon for '
                                                                                                'the '
                                                                                                'procedure.',
                                                   'severity_of_aortic_valve_stenosis_ava': 'Pre-operative '
                                                                                            'aortic valve '
                                                                                            'area measured '
                                                                                            'on '
                                                                                            'echocardiography, '
                                                                                            'recorded as a '
                                                                                            'marker of '
                                                                                            'aortic stenosis '
                                                                                            'severity.',
                                                   'severity_of_aortic_valve_stenosis_eoa': 'Aortic valve '
                                                                                            'effective '
                                                                                            'orifice area '
                                                                                            'recorded as a '
                                                                                            'measure of '
                                                                                            'aortic stenosis '
                                                                                            'severity before '
                                                                                            'surgery.',
                                                   'severity_of_aortic_valve_stenosis_gradient': 'Aortic '
                                                                                                 'valve '
                                                                                                 'pressure '
                                                                                                 'gradient '
                                                                                                 'recorded '
                                                                                                 'as a '
                                                                                                 'measure of '
                                                                                                 'aortic '
                                                                                                 'stenosis '
                                                                                                 'severity '
                                                                                                 'before '
                                                                                                 'surgery.',
                                                   'severity_of_mitral_valve_stenosis_eoa': 'Effective '
                                                                                            'orifice area of '
                                                                                            'the mitral '
                                                                                            'valve recorded '
                                                                                            'as a measure of '
                                                                                            'mitral stenosis '
                                                                                            'severity.',
                                                   'severity_of_mitral_valve_stenosis_gradient': 'Transmitral '
                                                                                                 'pressure '
                                                                                                 'gradient '
                                                                                                 'recorded '
                                                                                                 'as a '
                                                                                                 'measure of '
                                                                                                 'mitral '
                                                                                                 'stenosis '
                                                                                                 'severity.',
                                                   'significant_chest_wall_deformity': 'Coded indicator of '
                                                                                       'significant chest '
                                                                                       'wall deformity '
                                                                                       'identified '
                                                                                       'pre-operatively.',
                                                   'source_absent_detected_ts': 'Technical timestamp '
                                                                                'recording when the episode '
                                                                                'was first detected as no '
                                                                                'longer present in the iWeb '
                                                                                'source.',
                                                   'source_present_ind': 'Canonical boolean mirror of '
                                                                         'IS_CURRENT_IN_SOURCE; false rows '
                                                                         'are retained source tombstones.',
                                                   'source_record_key': 'Source system EntryId for the '
                                                                        'episode carried through as a string '
                                                                        'provenance key.',
                                                   'source_table': 'Label listing the iWeb CS2010G source '
                                                                   'modules contributing to the merged '
                                                                   'episode row.',
                                                   'surgical_incisionnon_nicor_optionstxt': 'Text label of '
                                                                                            'the surgical '
                                                                                            'incision '
                                                                                            'selected from '
                                                                                            'local non-NICOR '
                                                                                            'option list in '
                                                                                            'the '
                                                                                            'pre-operative '
                                                                                            'module.',
                                                   'surgical_incisiontxt': 'Text label of the surgical '
                                                                           'incision option selected for the '
                                                                           'procedure.',
                                                   'surname': 'Patient surname as recorded in the iWeb '
                                                              'CS2010G pre-operative registry module.',
                                                   'tapse': 'Tricuspid annular plane systolic excursion '
                                                            'measured on pre-operative echocardiography.',
                                                   'unit_collective_responsibility_case': 'Coded indicator '
                                                                                          'of whether the '
                                                                                          'case was '
                                                                                          'designated as a '
                                                                                          'unit collective '
                                                                                          'responsibility '
                                                                                          'case.',
                                                   'valve': 'Indicator of whether the episode included a '
                                                            'valve procedure.',
                                                   'vena_contracta_width_aortic': 'Vena contracta width of '
                                                                                  'the aortic regurgitant '
                                                                                  'jet measured on '
                                                                                  'pre-operative '
                                                                                  'echocardiography.',
                                                   'vena_contracta_width_mitral': 'Vena contracta width of '
                                                                                  'the mitral regurgitant '
                                                                                  'jet measured on '
                                                                                  'pre-operative '
                                                                                  'echocardiography.',
                                                   'venous_cannulation': 'Coded venous cannulation technique '
                                                                         'or cannula type used for '
                                                                         'cardiopulmonary bypass.',
                                                   'ventilated_pre_operation': 'Indicator of whether the '
                                                                               'patient was mechanically '
                                                                               'ventilated immediately '
                                                                               'before the operation.',
                                                   'weight_in_kg': 'Patient weight in kilograms recorded '
                                                                   'pre-operatively for the episode.'},
                                       'table': 'Typed child of clinical_registry_entry for the '
                                                'surgery_episode family. iWeb CS2010G adult cardiac surgery '
                                                'registry. Complete PRE1 fields remain top-level; PRE2, '
                                                'POST1 and POST2 source fields are retained losslessly in '
                                                'typed module structs. This preserves identifiers and '
                                                "clinical content while remaining within Unity Catalog's "
                                                '1,000-tag-per-table quota. Every governed bronze column is '
                                                'retained under its lower-case name.'}}
