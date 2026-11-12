"""Silver specialist.registry_eracs_episode: 1 dataset(s).

- datasets: clinical_registry_eracs_episode
- moved verbatim from: 3_silver_specialist_care:5166, 3_silver_specialist_care:5687

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


# Clinical / registry eracs episode

# ==== journey_clinical.registry_eracs_episode ====
REGISTRY_ERACS_EPISODE_EXPECTED_SCHEMA = [
    ("ENTRY_ID", "int"),
    ("MRN", "string"),
    ("GENDER", "string"),
    ("DATE_OF_DEATH", "date"),
    ("ETHNIC_ORIGIN", "string"),
    ("DATE_OF_ENTRY", "timestamp"),
    ("ERAC_SPATIENT", "string"),
    ("CONSULTANT_CARDIOTHORACICTXT", "string"),
    ("PRIMARY_OPERATORTXT", "string"),
    ("CARDIOPULMONARY_BYPASS_TIME", "int"),
    ("CROSS_CLAMP_TIME", "int"),
    ("COMPLICATIONSTXT", "string"),
    ("M_OTHER_COMPLICATIONS", "string"),
    ("BLOOD_PRODUCTS_ADMINISTEREDTXT", "string"),
    ("MEDICAL_HISTORYTXT", "string"),
    ("M_OTHER_MEDICAL_HISTORY", "string"),
    ("ECHO_POST_CB_PREPORT_ON_PACS", "string"),
    ("L_VFUNCTION_PRE", "string"),
    ("R_VFUNCTION_PRE", "string"),
    ("CHEST_DRAINSTXT", "string"),
    ("PACING_WIRES", "string"),
    ("M_UNDERLYING_RHYTHM_DETAILS", "string"),
    ("UNDERLYING_RHYTHMTXT", "string"),
    ("SETUP_PACING_MODE", "string"),
    ("SETUP_PACING_RATE", "int"),
    ("VCAP_THRESHOLD", "string"),
    ("ACAP_THRESHOLD", "string"),
    ("DRIPSTXT", "string"),
    ("PCA", "string"),
    ("VEIN_HARVEST_SITETXT", "string"),
    ("ANTIBIOTICSTXT", "string"),
    ("TIMING_OF2ND", "string"),
    ("TIMING_OF1ST", "string"),
    ("ADMISSION_DT_TM", "timestamp"),
    ("PATIENT_MEDICALLY_READY_DT_TM", "timestamp"),
    ("ACTUAL_DISCHARGE_DT_TM", "timestamp"),
    ("DISCHARGE_TO", "string"),
    ("FIRST_TIME_SITTING_DT_TM", "timestamp"),
    ("FIRST_MOBILISATION_DT_TM", "timestamp"),
    ("DRAINS_OUT_ON_ICU", "string"),
    ("DRAINS_REMOVED_DT", "timestamp"),
    ("TOTAL_VOLUME_DRAINED", "string"),
    ("CV_COUTON_ICU", "string"),
    ("TWO_COUTON_ICU", "string"),
    ("TEMPERATURE_AT_ARRIVAL_ON_ICU", "string"),
    ("P_WOUTON_ICU", "string"),
    ("BLOOD_PRODUCTS_IC_UTXT", "string"),
    ("PATIENT_PACED6HRS", "string"),
    ("PACING_MODE", "string"),
    ("ARRHYTHMIA_ON_IC_UTXT", "string"),
    ("PON_VSCORE", "string"),
    ("PAIN_SCORE", "string"),
    ("PC_AUSAGE12_HRS", "string"),
    ("TOTAL_AMOUNT_PCA", "string"),
    ("M_OTHER_MEDICAL_REASON_FOR_DELAY", "string"),
    ("M_OTHER_NON_MEDICAL_REASON_FOR_DELAY", "string"),
    ("NON_MEDICAL_REASONSTXT", "string"),
    ("FILTERTXT", "string"),
    ("M_OTHER_ARRHYTHMIA_ICU", "string"),
    ("L_VFUNCTION_POST_CPB", "string"),
    ("R_VFUNCTION_POST_CPB", "string"),
    ("ERACS_MEDICATION_PATHWAY", "string"),
    ("M_OTHER_DRIP_DETAILS", "string"),
    ("OTHER_REGIONAL_BLOCK", "string"),
    ("DISCHARGED_HDU_DT_TM", "timestamp"),
    ("PROCEDURE_PERFORMEDTXT", "string"),
    ("AORTIC_VALVE", "string"),
    ("MITRAL_VALVE", "string"),
    ("TRICUSPID_VALVE", "string"),
    ("AORTA", "string"),
    ("ACHD", "string"),
    ("M_OTHER_PROCEDURE_PERFORMED", "string"),
    ("OTHER_AORTIC_PROCEDURE", "string"),
    ("OTHER_ACH_DPROCEDURE", "string"),
    ("HEART_RATE", "int"),
    ("CROSSCLAMP_TIME120_MIN", "string"),
    ("BYPASSTIME180_MIN", "string"),
    ("PATIENT_ON_FIRST_FLOOR", "string"),
    ("ALLERGIESTXT", "string"),
    ("M_OTHER_ALLERGIES", "string"),
    ("MEDICAL_REASONS_FOR_DELAYTXT", "string"),
    ("ANTIEMETICTXT", "string"),
    ("ULCER_PROPHYLAXISTXT", "string"),
    ("I_VOPIATE_LOADING_AT_END", "string"),
    ("ANAESTHETIC_REGISTRAR_FELLOW", "string"),
    ("COMPLETE_REVASCULARISATION", "string"),
    ("MANADATORY", "string"),
    ("MEDICATION_PRESCRIBED_ON_POWERCHART", "string"),
    ("POST_OP_PLANTXT", "string"),
    ("M_OTHER_POST_OP_PLAN_DETAILS", "string"),
    ("P_MIN_BACKUP", "string"),
    ("BACKUP_RATE", "int"),
    ("CABG", "string"),
    ("BASELINE_ACT", "string"),
    ("POST_PROTAMINE_ACT", "string"),
    ("PLATELET_MAPPING_PERFORMED", "string"),
    ("TEG_POST_PROTAMINE_PERFORMED", "string"),
    ("SURGICAL_SITE_CHECKED", "string"),
    ("M_VOLUME_LOCAL_ANAESTHETIC", "string"),
    ("ADPMA35MM", "string"),
    ("AAMA35MM", "string"),
    ("RBC_NUMBER_OF_UNITS", "double"),
    ("FFP_NUMBER_OF_UNITS", "double"),
    ("CRYO_NUMBER_OF_UNITS_TRANSFUSED", "double"),
    ("OCTOPLEX_UNITS_TRANSFUSED", "double"),
    ("CELL_SALVAGE_TRANSFUSED", "double"),
    ("FIBRINOGEN_TRANSFUSED", "double"),
    ("AREAS_OF_CONCERN", "string"),
    ("M_TEG_RESULT_SUMMARY", "string"),
    ("TIMING_OF_ONDANSETRON", "string"),
    ("PARACETAMOL_ADMINISTERED", "string"),
    ("TIMING_OF_PARACETAMOL_ADMINISTERED", "string"),
    ("SUGGAMEDEX_ADMINISTERED", "string"),
    ("POST_OP_ANALGESIATXT", "string"),
    ("PLATELET_NUMBER_OF_UNITS", "double"),
    ("GRADE_OF_INTUBATION", "string"),
    ("ANTIPLATELETSTXT", "string"),
    ("OTHER_ANTIPLATELET", "string"),
    ("SPINAL_DRAIN_INSERTED", "string"),
    ("SPINAL_DRAINAGE_ABOVE", "int"),
    ("ISSUES_WITH_SPINAL_DRAIN", "string"),
    ("M_DETAILS_OF_SPINAL_DRAIN_ISSUES", "string"),
    ("M_THORACIC_PROCEDUREDETAILS", "string"),
    ("CARDIAC_OR_THORACIC", "string"),
    ("THORACIC_PROCEDURETXT", "string"),
    ("BRONCHOSCOPY", "string"),
    ("LVRS", "string"),
    ("LOBES_OPERATED_ONTXT", "string"),
    ("LYMPH_NODE_RESECTION", "string"),
    ("CONSULTANT_THORACICTXT", "string"),
    ("INTRAOPERATIVE_PAIN_CONTROLTXT", "string"),
    ("POSTOPERATIVE_PAIN_CONTROLTXT", "string"),
    ("TOF", "int"),
    ("INSTRUCTION", "string"),
    ("POST_OP_PLANTHORACIC_PATIENTSTXT", "string"),
    ("CONSULTANT_RESPONSIBLE_FOR_PROCEDURE", "string"),
    ("BELMONT_VOLUME", "double"),
    ("DATE_LAST_CHANGED", "timestamp"),
    ("ADC_UPDT", "timestamp"),
    ("NHS_NUMBER_VALID_IND", "boolean"),
    ("LINKAGE_STATUS", "string"),
    ("PERSON_ID", "bigint"),
    ("LINKAGE_METHOD", "string"),
    ("LINKAGE_HISTORICAL_FALLBACK_IND", "boolean"),
    ("LINKAGE_FALLBACK_CONFLICT_IND", "boolean"),
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
    ("TIMING_OF1ST_TIME", "string"),  # INSR_W2 W2.20
    ("TIMING_OF1ST_PARSE_STATUS", "string"),  # INSR_W2 W2.20
    ("TIMING_OF2ND_TIME", "string"),  # INSR_W2 W2.20
    ("TIMING_OF2ND_PARSE_STATUS", "string"),  # INSR_W2 W2.20
    ("TIMING_OF_ONDANSETRON_TIME", "string"),  # INSR_W2 W2.20
    ("TIMING_OF_ONDANSETRON_PARSE_STATUS", "string"),  # INSR_W2 W2.20
    ("TIMING_OF_PARACETAMOL_ADMINISTERED_TIME", "string"),  # INSR_W2 W2.20
    ("TIMING_OF_PARACETAMOL_ADMINISTERED_PARSE_STATUS", "string"),  # INSR_W2 W2.20
    ("BASELINE_ACT_NUM", "decimal(12,2)"),  # INSR_W2 W2.20
    ("BASELINE_ACT_PARSE_STATUS", "string"),  # INSR_W2 W2.20
    ("POST_PROTAMINE_ACT_NUM", "decimal(12,2)"),  # INSR_W2 W2.20
    ("POST_PROTAMINE_ACT_PARSE_STATUS", "string"),  # INSR_W2 W2.20
]


@materialized_view(
    name=_n("journey_clinical.registry_eracs_episode"),
    refresh_policy="auto",
)
def registry_eracs_episode():
    # Build the declared dataset: Typed child of clinical_registry_entry for the eracs_episode
    # family. Enhanced Recovery After Cardiac Surgery pathway registry, live since 2025: pathway
    # timestamps, pacing, TEG/platelet mapping, transfusion, mobilisation and discharge
    # milestones. Every governed bronze column is retained under its lower-case name.
    s = read_source(IWEB_REGISTRY_SLOTS["eracs_episode"])
    actual_schema = [(field.name, field.dataType.simpleString()) for field in s.schema.fields]
    if actual_schema != REGISTRY_ERACS_EPISODE_EXPECTED_SCHEMA:
        raise RuntimeError("eracs_episode: bronze schema drift" + f"; expected={REGISTRY_ERACS_EPISODE_EXPECTED_SCHEMA!r} actual={actual_schema!r}")
    return s.select(
        stable_id("registry_entry:iweb", F.lit("eracs_episode"), F.lit("~"), s["ENTRY_ID"]).alias("patient_event_key"),
        s["ENTRY_ID"].alias("entry_id"),
        s["MRN"].alias("mrn"),
        s["GENDER"].alias("gender"),
        s["DATE_OF_DEATH"].alias("date_of_death"),
        s["ETHNIC_ORIGIN"].alias("ethnic_origin"),
        s["DATE_OF_ENTRY"].alias("date_of_entry"),
        s["ERAC_SPATIENT"].alias("erac_spatient"),
        s["CONSULTANT_CARDIOTHORACICTXT"].alias("consultant_cardiothoracictxt"),
        s["PRIMARY_OPERATORTXT"].alias("primary_operatortxt"),
        s["CARDIOPULMONARY_BYPASS_TIME"].alias("cardiopulmonary_bypass_time"),
        s["CROSS_CLAMP_TIME"].alias("cross_clamp_time"),
        s["COMPLICATIONSTXT"].alias("complicationstxt"),
        s["M_OTHER_COMPLICATIONS"].alias("m_other_complications"),
        s["BLOOD_PRODUCTS_ADMINISTEREDTXT"].alias("blood_products_administeredtxt"),
        s["MEDICAL_HISTORYTXT"].alias("medical_historytxt"),
        s["M_OTHER_MEDICAL_HISTORY"].alias("m_other_medical_history"),
        s["ECHO_POST_CB_PREPORT_ON_PACS"].alias("echo_post_cb_preport_on_pacs"),
        s["L_VFUNCTION_PRE"].alias("l_vfunction_pre"),
        s["R_VFUNCTION_PRE"].alias("r_vfunction_pre"),
        s["CHEST_DRAINSTXT"].alias("chest_drainstxt"),
        s["PACING_WIRES"].alias("pacing_wires"),
        s["M_UNDERLYING_RHYTHM_DETAILS"].alias("m_underlying_rhythm_details"),
        s["UNDERLYING_RHYTHMTXT"].alias("underlying_rhythmtxt"),
        s["SETUP_PACING_MODE"].alias("setup_pacing_mode"),
        s["SETUP_PACING_RATE"].alias("setup_pacing_rate"),
        s["VCAP_THRESHOLD"].alias("vcap_threshold"),
        s["ACAP_THRESHOLD"].alias("acap_threshold"),
        s["DRIPSTXT"].alias("dripstxt"),
        s["PCA"].alias("pca"),
        s["VEIN_HARVEST_SITETXT"].alias("vein_harvest_sitetxt"),
        s["ANTIBIOTICSTXT"].alias("antibioticstxt"),
        s["TIMING_OF2ND"].alias("timing_of2nd"),
        s["TIMING_OF1ST"].alias("timing_of1st"),
        s["ADMISSION_DT_TM"].alias("admission_dt_tm"),
        s["PATIENT_MEDICALLY_READY_DT_TM"].alias("patient_medically_ready_dt_tm"),
        s["ACTUAL_DISCHARGE_DT_TM"].alias("actual_discharge_dt_tm"),
        s["DISCHARGE_TO"].alias("discharge_to"),
        s["FIRST_TIME_SITTING_DT_TM"].alias("first_time_sitting_dt_tm"),
        s["FIRST_MOBILISATION_DT_TM"].alias("first_mobilisation_dt_tm"),
        s["DRAINS_OUT_ON_ICU"].alias("drains_out_on_icu"),
        s["DRAINS_REMOVED_DT"].alias("drains_removed_dt"),
        s["TOTAL_VOLUME_DRAINED"].alias("total_volume_drained"),
        s["CV_COUTON_ICU"].alias("cv_couton_icu"),
        s["TWO_COUTON_ICU"].alias("two_couton_icu"),
        s["TEMPERATURE_AT_ARRIVAL_ON_ICU"].alias("temperature_at_arrival_on_icu"),
        s["P_WOUTON_ICU"].alias("p_wouton_icu"),
        s["BLOOD_PRODUCTS_IC_UTXT"].alias("blood_products_ic_utxt"),
        s["PATIENT_PACED6HRS"].alias("patient_paced6hrs"),
        s["PACING_MODE"].alias("pacing_mode"),
        s["ARRHYTHMIA_ON_IC_UTXT"].alias("arrhythmia_on_ic_utxt"),
        s["PON_VSCORE"].alias("pon_vscore"),
        s["PAIN_SCORE"].alias("pain_score"),
        s["PC_AUSAGE12_HRS"].alias("pc_ausage12_hrs"),
        s["TOTAL_AMOUNT_PCA"].alias("total_amount_pca"),
        s["M_OTHER_MEDICAL_REASON_FOR_DELAY"].alias("m_other_medical_reason_for_delay"),
        s["M_OTHER_NON_MEDICAL_REASON_FOR_DELAY"].alias("m_other_non_medical_reason_for_delay"),
        s["NON_MEDICAL_REASONSTXT"].alias("non_medical_reasonstxt"),
        s["FILTERTXT"].alias("filtertxt"),
        s["M_OTHER_ARRHYTHMIA_ICU"].alias("m_other_arrhythmia_icu"),
        s["L_VFUNCTION_POST_CPB"].alias("l_vfunction_post_cpb"),
        s["R_VFUNCTION_POST_CPB"].alias("r_vfunction_post_cpb"),
        s["ERACS_MEDICATION_PATHWAY"].alias("eracs_medication_pathway"),
        s["M_OTHER_DRIP_DETAILS"].alias("m_other_drip_details"),
        s["OTHER_REGIONAL_BLOCK"].alias("other_regional_block"),
        s["DISCHARGED_HDU_DT_TM"].alias("discharged_hdu_dt_tm"),
        s["PROCEDURE_PERFORMEDTXT"].alias("procedure_performedtxt"),
        s["AORTIC_VALVE"].alias("aortic_valve"),
        s["MITRAL_VALVE"].alias("mitral_valve"),
        s["TRICUSPID_VALVE"].alias("tricuspid_valve"),
        s["AORTA"].alias("aorta"),
        s["ACHD"].alias("achd"),
        s["M_OTHER_PROCEDURE_PERFORMED"].alias("m_other_procedure_performed"),
        s["OTHER_AORTIC_PROCEDURE"].alias("other_aortic_procedure"),
        s["OTHER_ACH_DPROCEDURE"].alias("other_ach_dprocedure"),
        s["HEART_RATE"].alias("heart_rate"),
        s["CROSSCLAMP_TIME120_MIN"].alias("crossclamp_time120_min"),
        s["BYPASSTIME180_MIN"].alias("bypasstime180_min"),
        s["PATIENT_ON_FIRST_FLOOR"].alias("patient_on_first_floor"),
        s["ALLERGIESTXT"].alias("allergiestxt"),
        s["M_OTHER_ALLERGIES"].alias("m_other_allergies"),
        s["MEDICAL_REASONS_FOR_DELAYTXT"].alias("medical_reasons_for_delaytxt"),
        s["ANTIEMETICTXT"].alias("antiemetictxt"),
        s["ULCER_PROPHYLAXISTXT"].alias("ulcer_prophylaxistxt"),
        s["I_VOPIATE_LOADING_AT_END"].alias("i_vopiate_loading_at_end"),
        s["ANAESTHETIC_REGISTRAR_FELLOW"].alias("anaesthetic_registrar_fellow"),
        s["COMPLETE_REVASCULARISATION"].alias("complete_revascularisation"),
        s["MANADATORY"].alias("manadatory"),
        s["MEDICATION_PRESCRIBED_ON_POWERCHART"].alias("medication_prescribed_on_powerchart"),
        s["POST_OP_PLANTXT"].alias("post_op_plantxt"),
        s["M_OTHER_POST_OP_PLAN_DETAILS"].alias("m_other_post_op_plan_details"),
        s["P_MIN_BACKUP"].alias("p_min_backup"),
        s["BACKUP_RATE"].alias("backup_rate"),
        s["CABG"].alias("cabg"),
        s["BASELINE_ACT"].alias("baseline_act"),
        s["POST_PROTAMINE_ACT"].alias("post_protamine_act"),
        s["PLATELET_MAPPING_PERFORMED"].alias("platelet_mapping_performed"),
        s["TEG_POST_PROTAMINE_PERFORMED"].alias("teg_post_protamine_performed"),
        s["SURGICAL_SITE_CHECKED"].alias("surgical_site_checked"),
        s["M_VOLUME_LOCAL_ANAESTHETIC"].alias("m_volume_local_anaesthetic"),
        s["ADPMA35MM"].alias("adpma35mm"),
        s["AAMA35MM"].alias("aama35mm"),
        s["RBC_NUMBER_OF_UNITS"].alias("rbc_number_of_units"),
        s["FFP_NUMBER_OF_UNITS"].alias("ffp_number_of_units"),
        s["CRYO_NUMBER_OF_UNITS_TRANSFUSED"].alias("cryo_number_of_units_transfused"),
        s["OCTOPLEX_UNITS_TRANSFUSED"].alias("octoplex_units_transfused"),
        s["CELL_SALVAGE_TRANSFUSED"].alias("cell_salvage_transfused"),
        s["FIBRINOGEN_TRANSFUSED"].alias("fibrinogen_transfused"),
        s["AREAS_OF_CONCERN"].alias("areas_of_concern"),
        s["M_TEG_RESULT_SUMMARY"].alias("m_teg_result_summary"),
        s["TIMING_OF_ONDANSETRON"].alias("timing_of_ondansetron"),
        s["PARACETAMOL_ADMINISTERED"].alias("paracetamol_administered"),
        s["TIMING_OF_PARACETAMOL_ADMINISTERED"].alias("timing_of_paracetamol_administered"),
        s["SUGGAMEDEX_ADMINISTERED"].alias("suggamedex_administered"),
        s["POST_OP_ANALGESIATXT"].alias("post_op_analgesiatxt"),
        s["PLATELET_NUMBER_OF_UNITS"].alias("platelet_number_of_units"),
        s["GRADE_OF_INTUBATION"].alias("grade_of_intubation"),
        s["ANTIPLATELETSTXT"].alias("antiplateletstxt"),
        s["OTHER_ANTIPLATELET"].alias("other_antiplatelet"),
        s["SPINAL_DRAIN_INSERTED"].alias("spinal_drain_inserted"),
        s["SPINAL_DRAINAGE_ABOVE"].alias("spinal_drainage_above"),
        s["ISSUES_WITH_SPINAL_DRAIN"].alias("issues_with_spinal_drain"),
        s["M_DETAILS_OF_SPINAL_DRAIN_ISSUES"].alias("m_details_of_spinal_drain_issues"),
        s["M_THORACIC_PROCEDUREDETAILS"].alias("m_thoracic_proceduredetails"),
        s["CARDIAC_OR_THORACIC"].alias("cardiac_or_thoracic"),
        s["THORACIC_PROCEDURETXT"].alias("thoracic_proceduretxt"),
        s["BRONCHOSCOPY"].alias("bronchoscopy"),
        s["LVRS"].alias("lvrs"),
        s["LOBES_OPERATED_ONTXT"].alias("lobes_operated_ontxt"),
        s["LYMPH_NODE_RESECTION"].alias("lymph_node_resection"),
        s["CONSULTANT_THORACICTXT"].alias("consultant_thoracictxt"),
        s["INTRAOPERATIVE_PAIN_CONTROLTXT"].alias("intraoperative_pain_controltxt"),
        s["POSTOPERATIVE_PAIN_CONTROLTXT"].alias("postoperative_pain_controltxt"),
        s["TOF"].alias("tof"),
        s["INSTRUCTION"].alias("instruction"),
        s["POST_OP_PLANTHORACIC_PATIENTSTXT"].alias("post_op_planthoracic_patientstxt"),
        s["CONSULTANT_RESPONSIBLE_FOR_PROCEDURE"].alias("consultant_responsible_for_procedure"),
        s["BELMONT_VOLUME"].alias("belmont_volume"),
        s["DATE_LAST_CHANGED"].alias("date_last_changed"),
        s["ADC_UPDT"].alias("adc_updt"),
        s["NHS_NUMBER_VALID_IND"].alias("nhs_number_valid_ind"),
        s["LINKAGE_STATUS"].alias("linkage_status"),
        s["PERSON_ID"].alias("person_id"),
        s["LINKAGE_METHOD"].alias("linkage_method"),
        s["LINKAGE_HISTORICAL_FALLBACK_IND"].alias("linkage_historical_fallback_ind"),
        s["LINKAGE_FALLBACK_CONFLICT_IND"].alias("linkage_fallback_conflict_ind"),
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
        s["TIMING_OF1ST_TIME"].alias("timing_of1st_time"),  # INSR_W2 W2.20
        s["TIMING_OF1ST_PARSE_STATUS"].alias("timing_of1st_parse_status"),  # INSR_W2 W2.20
        s["TIMING_OF2ND_TIME"].alias("timing_of2nd_time"),  # INSR_W2 W2.20
        s["TIMING_OF2ND_PARSE_STATUS"].alias("timing_of2nd_parse_status"),  # INSR_W2 W2.20
        s["TIMING_OF_ONDANSETRON_TIME"].alias("timing_of_ondansetron_time"),  # INSR_W2 W2.20
        s["TIMING_OF_ONDANSETRON_PARSE_STATUS"].alias("timing_of_ondansetron_parse_status"),  # INSR_W2 W2.20
        s["TIMING_OF_PARACETAMOL_ADMINISTERED_TIME"].alias("timing_of_paracetamol_administered_time"),  # INSR_W2 W2.20
        s["TIMING_OF_PARACETAMOL_ADMINISTERED_PARSE_STATUS"].alias("timing_of_paracetamol_administered_parse_status"),  # INSR_W2 W2.20
        s["BASELINE_ACT_NUM"].alias("baseline_act_num"),  # INSR_W2 W2.20
        s["BASELINE_ACT_PARSE_STATUS"].alias("baseline_act_parse_status"),  # INSR_W2 W2.20
        s["POST_PROTAMINE_ACT_NUM"].alias("post_protamine_act_num"),  # INSR_W2 W2.20
        s["POST_PROTAMINE_ACT_PARSE_STATUS"].alias("post_protamine_act_parse_status"),  # INSR_W2 W2.20
    )


VALUE_POLICIES = {
    'clinical_registry_eracs_episode': ValuePolicy(table='clinical_registry_eracs_episode', enabled=True),
}


WRAP_POLICY = {'clinical_registry_eracs_episode'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_registry_eracs_episode': {'columns': {'aama35mm': 'Indicator of whether the platelet mapping '
                                                             'arachidonic acid maximum amplitude reached the '
                                                             '35 mm threshold, recorded as a yes/no response '
                                                             'on the ERACS form.',
                                                 'acap_threshold': 'Atrial capture threshold recorded at the '
                                                                   'epicardial pacing wire check, held as a '
                                                                   'free-text value.',
                                                 'achd': 'Adult congenital heart disease procedure or '
                                                         'intervention type recorded for the episode.',
                                                 'actual_discharge_dt_tm': 'Date and time the patient was '
                                                                           'actually discharged.',
                                                 'adc_updt': 'Audit timestamp recording when the record was '
                                                             'last updated during ingestion from the source '
                                                             'registry.',
                                                 'admission_dt_tm': "Date and time of the patient's "
                                                                    'admission for the ERACS pathway '
                                                                    'episode.',
                                                 'adpma35mm': 'Yes/No indicator of whether the ADP maximum '
                                                              'amplitude (MA) on platelet mapping was above '
                                                              '35 mm.',
                                                 'allergiestxt': 'Coded picklist label(s) describing the '
                                                                 "patient's recorded allergies captured on "
                                                                 'the ERACS form.',
                                                 'anaesthetic_registrar_fellow': 'Name of the anaesthetic '
                                                                                 'registrar or fellow '
                                                                                 "involved in the patient's "
                                                                                 'procedure, entered as free '
                                                                                 'text.',
                                                 'antibioticstxt': 'Coded prophylactic antibiotic agents '
                                                                   'administered, stored as concatenated '
                                                                   'code and description pairs.',
                                                 'antiemetictxt': 'Coded picklist label(s) recording the '
                                                                  'antiemetic agent administered during the '
                                                                  'ERACS pathway.',
                                                 'antiplateletstxt': 'Antiplatelet agent(s) recorded for the '
                                                                     'episode, selected from the ERACS coded '
                                                                     'picklist.',
                                                 'aorta': 'Aortic procedure or intervention type recorded '
                                                          'for the episode.',
                                                 'aortic_valve': 'Aortic valve procedure or intervention '
                                                                 'type recorded for the episode.',
                                                 'areas_of_concern': 'Free-text note describing clinical '
                                                                     'areas of concern identified during the '
                                                                     'ERACS pathway episode.',
                                                 'arrhythmia_on_ic_utxt': 'Coded arrhythmias observed on '
                                                                          'ICU, stored as concatenated code '
                                                                          'and description pairs.',
                                                 'backup_rate': 'Numeric backup pacing rate set for the '
                                                                'patient post-operatively.',
                                                 'baseline_act': 'Baseline activated clotting time (ACT) '
                                                                 'result recorded before heparinisation, '
                                                                 'stored as text.',
                                                 'baseline_act_num': 'Leading number parsed from '
                                                                     'BASELINE_ACT (ERACS_TYPED_V1).',
                                                 'baseline_act_parse_status': 'PARSED, BLANK or UNPARSED for '
                                                                              'BASELINE_ACT.',
                                                 'belmont_volume': 'Volume recorded as delivered through the '
                                                                   'Belmont rapid infuser during the '
                                                                   'episode.',
                                                 'blood_products_administeredtxt': 'Coded list of blood and '
                                                                                   'salvage products '
                                                                                   'administered during the '
                                                                                   'episode.',
                                                 'blood_products_ic_utxt': 'Coded blood products '
                                                                           'administered on ICU, stored as '
                                                                           'concatenated code and '
                                                                           'description pairs.',
                                                 'bronchoscopy': 'Indicator of whether bronchoscopy was '
                                                                 'performed as part of the thoracic surgical '
                                                                 'episode.',
                                                 'bypasstime180_min': 'Yes/No indicator recording whether '
                                                                      'the cardiopulmonary bypass time '
                                                                      'exceeded 180 minutes for this ERACS '
                                                                      'episode.',
                                                 'cabg': 'Recorded detail of coronary artery bypass grafting '
                                                         'performed, including the number of grafts.',
                                                 'cardiac_or_thoracic': 'Categorisation of the episode as a '
                                                                        'cardiac or thoracic surgical '
                                                                        'pathway.',
                                                 'cardiopulmonary_bypass_time': 'Recorded cardiopulmonary '
                                                                                'bypass duration in minutes '
                                                                                'for the operation.',
                                                 'cell_salvage_transfused': 'Volume of cell-salvaged blood '
                                                                            'returned to the patient during '
                                                                            'the ERACS episode.',
                                                 'chest_drainstxt': 'Coded list of chest drain sites in '
                                                                    'place following the procedure.',
                                                 'complete_revascularisation': 'Yes/No indicator of whether '
                                                                               'complete surgical '
                                                                               'revascularisation was '
                                                                               'achieved for the coronary '
                                                                               'procedure.',
                                                 'complicationstxt': 'Coded list of peri-operative '
                                                                     'complications recorded for the '
                                                                     'episode, with any additional detail '
                                                                     'held in the other complications field.',
                                                 'consultant_cardiothoracictxt': 'Name of the responsible '
                                                                                 'cardiothoracic consultant '
                                                                                 'recorded for the episode.',
                                                 'consultant_responsible_for_procedure': 'Free-text entry '
                                                                                         'recording the '
                                                                                         'consultant '
                                                                                         'responsible for '
                                                                                         'the procedure.',
                                                 'consultant_thoracictxt': 'Decoded name of the responsible '
                                                                           'thoracic surgery consultant '
                                                                           'recorded for the episode.',
                                                 'cross_clamp_time': 'Recorded aortic cross-clamp duration '
                                                                     'in minutes for the operation.',
                                                 'crossclamp_time120_min': 'Yes/No indicator recording '
                                                                           'whether the aortic cross-clamp '
                                                                           'time exceeded 120 minutes for '
                                                                           'this ERACS episode.',
                                                 'cryo_number_of_units_transfused': 'Number of '
                                                                                    'cryoprecipitate units '
                                                                                    'transfused for the '
                                                                                    'ERACS episode.',
                                                 'cv_couton_icu': 'Indicator of whether the central venous '
                                                                  'catheter was removed while the patient '
                                                                  'was on ICU.',
                                                 'date_last_changed': 'Date and time the registry record was '
                                                                      'last changed in the source ERACS '
                                                                      'system.',
                                                 'date_of_birth': "Patient's date of birth as recorded on "
                                                                  'the source ERACS registry record.',
                                                 'date_of_death': 'Recorded date of death of the patient, '
                                                                  'populated only where a death has been '
                                                                  'registered against the record.',
                                                 'date_of_entry': 'Date and time the ERACS registry entry '
                                                                  'was recorded for the patient.',
                                                 'discharge_to': 'Recorded destination the patient was '
                                                                 'discharged to at the end of the episode.',
                                                 'discharged_hdu_dt_tm': 'Date and time the patient was '
                                                                         'discharged from the high '
                                                                         'dependency unit.',
                                                 'drains_out_on_icu': 'Indicator of whether the chest drains '
                                                                      'were removed while the patient was on '
                                                                      'ICU.',
                                                 'drains_removed_dt': 'Date and time the chest drains were '
                                                                      'removed.',
                                                 'dripstxt': 'Coded list of intravenous infusions running '
                                                             'for the episode, stored as concatenated code '
                                                             'and description pairs.',
                                                 'echo_post_cb_preport_on_pacs': 'Indicator of whether the '
                                                                                 'post-cardiopulmonary '
                                                                                 'bypass echocardiogram '
                                                                                 'report was available on '
                                                                                 'PACS.',
                                                 'entry_id': 'Internal surrogate identifier for the ERACS '
                                                             'registry entry, carried through from the '
                                                             'source registry record.',
                                                 'erac_spatient': 'Indicator of whether the patient was '
                                                                  'managed on the Enhanced Recovery After '
                                                                  'Cardiac Surgery pathway.',
                                                 'eracs_medication_pathway': 'Indicator of whether the ERACS '
                                                                             'medication pathway was '
                                                                             'followed for the episode.',
                                                 'ethnic_origin': 'Coded ethnic category recorded for the '
                                                                  'patient in the ERACS registry.',
                                                 'ffp_number_of_units': 'Number of fresh frozen plasma units '
                                                                        'transfused for the ERACS episode.',
                                                 'fibrinogen_transfused': 'Amount of fibrinogen concentrate '
                                                                          'transfused for the ERACS episode.',
                                                 'filtertxt': 'Decoded description of the filter option '
                                                              'selected on the ERACS peri-operative form.',
                                                 'first_mobilisation_dt_tm': "Date and time of the patient's "
                                                                             'first mobilisation after '
                                                                             'surgery.',
                                                 'first_time_sitting_dt_tm': 'Date and time the patient '
                                                                             'first sat out as part of the '
                                                                             'enhanced recovery mobilisation '
                                                                             'pathway.',
                                                 'forename': "Patient's forename as recorded on the source "
                                                             'ERACS registry record.',
                                                 'gender': 'Recorded gender of the patient associated with '
                                                           'the ERACS episode.',
                                                 'grade_of_intubation': 'Recorded laryngoscopy grade at '
                                                                        "intubation for the episode's airway "
                                                                        'management.',
                                                 'heart_rate': 'Recorded heart rate for the patient at the '
                                                               'relevant peri-operative assessment point.',
                                                 'i_vopiate_loading_at_end': 'Yes/No indicator of whether '
                                                                             'intravenous opiate loading was '
                                                                             'given at the end of the '
                                                                             'operation.',
                                                 'instruction': 'Form instruction or guidance text field on '
                                                                'the ERACS data-entry form.',
                                                 'intraoperative_pain_controltxt': 'Decoded description of '
                                                                                   'the intraoperative pain '
                                                                                   'control method or '
                                                                                   'methods used for the '
                                                                                   'episode.',
                                                 'is_current_in_source': 'Boolean flag indicating whether '
                                                                         'the record is still present as '
                                                                         'current in the source system.',
                                                 'issues_with_spinal_drain': 'Indicator of whether any '
                                                                             'issues occurred with the '
                                                                             'spinal drain during the '
                                                                             'episode.',
                                                 'l_vfunction_post_cpb': 'Assessed left ventricular function '
                                                                         'following cardiopulmonary bypass, '
                                                                         'recorded as a graded category.',
                                                 'l_vfunction_pre': 'Categorical assessment of left '
                                                                    'ventricular function recorded before '
                                                                    'the procedure.',
                                                 'linkage_fallback_conflict_ind': 'Boolean flag indicating '
                                                                                  'whether a conflict was '
                                                                                  'detected when a fallback '
                                                                                  'identifier was used for '
                                                                                  'person linkage.',
                                                 'linkage_historical_fallback_ind': 'Boolean flag indicating '
                                                                                    'whether the person '
                                                                                    'linkage relied on a '
                                                                                    'historical identifier '
                                                                                    'fallback.',
                                                 'linkage_method': 'Code indicating which identifiers were '
                                                                   'used to link the registry record to the '
                                                                   'person record.',
                                                 'linkage_status': 'Coded status describing the outcome of '
                                                                   'linking the registry record to a master '
                                                                   'person record.',
                                                 'lobes_operated_ontxt': 'Decoded description of the lung '
                                                                         'lobe or lobes operated on during '
                                                                         'the thoracic procedure.',
                                                 'lvrs': 'Indicator of whether lung volume reduction surgery '
                                                         '(LVRS) was performed for the thoracic episode.',
                                                 'lymph_node_resection': 'Indicator of whether lymph node '
                                                                         'resection was carried out during '
                                                                         'the thoracic procedure.',
                                                 'm_details_of_spinal_drain_issues': 'Free-text description '
                                                                                     'of issues encountered '
                                                                                     'with the spinal drain '
                                                                                     'during the episode.',
                                                 'm_other_allergies': 'Free-text description of an allergy '
                                                                      'selected as Other on the ERACS '
                                                                      'allergies question.',
                                                 'm_other_arrhythmia_icu': 'Free-text description of an '
                                                                           'other arrhythmia observed on the '
                                                                           'intensive care unit when not '
                                                                           'covered by the coded arrhythmia '
                                                                           'list.',
                                                 'm_other_complications': 'Free-text description of '
                                                                          'complications not covered by the '
                                                                          'coded complications list.',
                                                 'm_other_drip_details': 'Free-text details of other '
                                                                         'infusions (drips) administered '
                                                                         'that are not covered by the coded '
                                                                         'drips list.',
                                                 'm_other_medical_history': 'Free-text narrative of '
                                                                            'additional medical history not '
                                                                            'captured by the coded medical '
                                                                            'history list.',
                                                 'm_other_medical_reason_for_delay': 'Free-text description '
                                                                                     'of an other medical '
                                                                                     'reason for delayed '
                                                                                     'discharge or pathway '
                                                                                     'progression, captured '
                                                                                     'when the coded medical '
                                                                                     'reason list does not '
                                                                                     'apply.',
                                                 'm_other_non_medical_reason_for_delay': 'Free-text '
                                                                                         'description of an '
                                                                                         'other non-medical '
                                                                                         'reason for delayed '
                                                                                         'discharge or '
                                                                                         'pathway '
                                                                                         'progression, '
                                                                                         'captured when the '
                                                                                         'coded non-medical '
                                                                                         'reason list does '
                                                                                         'not apply.',
                                                 'm_other_post_op_plan_details': 'Free-text narrative '
                                                                                 'detailing additional '
                                                                                 'post-operative plan '
                                                                                 "instructions where 'Other' "
                                                                                 'was selected on the '
                                                                                 'post-operative plan '
                                                                                 'question.',
                                                 'm_other_procedure_performed': 'Free-text description of '
                                                                                'the procedure performed '
                                                                                'when the coded procedure '
                                                                                'list records an other '
                                                                                'procedure.',
                                                 'm_teg_result_summary': 'Free-text summary of '
                                                                         'thromboelastography (TEG) results '
                                                                         'recorded for the episode.',
                                                 'm_thoracic_proceduredetails': 'Free-text detail of the '
                                                                                'thoracic procedure '
                                                                                'performed where not fully '
                                                                                'captured by the coded '
                                                                                'procedure picklist.',
                                                 'm_underlying_rhythm_details': 'Free-text detail describing '
                                                                                "the patient's underlying "
                                                                                'cardiac rhythm where the '
                                                                                'coded value requires '
                                                                                'clarification.',
                                                 'm_volume_local_anaesthetic': 'Free-text entry recording '
                                                                               'the volume of local '
                                                                               'anaesthetic administered.',
                                                 'manadatory': 'Form control field (name misspelled from '
                                                               "'mandatory') used by the ERACS data-entry "
                                                               'form, with no populated values observed.',
                                                 'medical_historytxt': 'Coded list of relevant pre-existing '
                                                                       'medical conditions recorded for the '
                                                                       'patient, with additional detail held '
                                                                       'in the other medical history field.',
                                                 'medical_reasons_for_delaytxt': 'Coded picklist label(s) '
                                                                                 'recording medical reasons '
                                                                                 "for delay in the patient's "
                                                                                 'discharge or pathway '
                                                                                 'progression.',
                                                 'medication_prescribed_on_powerchart': 'Yes/No indicator of '
                                                                                        'whether the ERACS '
                                                                                        'pathway medication '
                                                                                        'was prescribed in '
                                                                                        'the PowerChart '
                                                                                        'electronic record.',
                                                 'mitral_valve': 'Mitral valve procedure or intervention '
                                                                 'type recorded for the episode.',
                                                 'mrn': 'Local hospital medical record number identifying '
                                                        'the patient for this ERACS registry entry.',
                                                 'nhs_number': 'NHS number recorded for the patient on the '
                                                               'source ERACS registry record, used for '
                                                               'national person identification and linkage.',
                                                 'nhs_number_valid_ind': 'Boolean flag indicating whether '
                                                                         'the NHS number held on the source '
                                                                         'registry record passed validation.',
                                                 'non_medical_reasonstxt': 'Decoded description of the '
                                                                           'non-medical reason(s) recorded '
                                                                           'for delay in discharge or '
                                                                           'pathway progression.',
                                                 'octoplex_units_transfused': 'Amount of Octaplex '
                                                                              'prothrombin complex '
                                                                              'concentrate transfused for '
                                                                              'the ERACS episode.',
                                                 'other_ach_dprocedure': 'Record of an other adult '
                                                                         'congenital heart disease procedure '
                                                                         'performed that is not covered by '
                                                                         'the standard options.',
                                                 'other_antiplatelet': 'Free-text detail of an antiplatelet '
                                                                       'agent not covered by the coded '
                                                                       'antiplatelet picklist.',
                                                 'other_aortic_procedure': 'Record of an other aortic '
                                                                           'procedure performed that is not '
                                                                           'covered by the standard aortic '
                                                                           'procedure options.',
                                                 'other_regional_block': 'Record of an other regional '
                                                                         'anaesthetic block used, where not '
                                                                         'covered by the standard block '
                                                                         'options.',
                                                 'p_min_backup': 'Yes/No indicator of whether backup (P-min) '
                                                                 'pacing was set for the patient '
                                                                 'post-operatively.',
                                                 'p_wouton_icu': 'Indicator of whether the temporary pacing '
                                                                 'wires were removed while the patient was '
                                                                 'on ICU.',
                                                 'pacing_mode': 'Pacing mode in use on ICU for the episode.',
                                                 'pacing_wires': 'Categorical record of which temporary '
                                                                 'epicardial pacing wires were placed.',
                                                 'pain_score': 'Recorded post-operative pain score for the '
                                                               'episode, held as a coded text value.',
                                                 'paracetamol_administered': 'Indicator of whether '
                                                                             'paracetamol was administered '
                                                                             'as part of the ERACS analgesia '
                                                                             'pathway.',
                                                 'patient_event_key': 'Deterministic SHA-256 key matching '
                                                                      'clinical_registry_entry.patient_event_key '
                                                                      'for the eracs_episode source family '
                                                                      'and native entry_id.',
                                                 'patient_medically_ready_dt_tm': 'Date and time the patient '
                                                                                  'was recorded as medically '
                                                                                  'ready for discharge.',
                                                 'patient_on_first_floor': 'Indicator recording whether the '
                                                                           'patient was located on the first '
                                                                           'floor (step-down ward area) as '
                                                                           'part of the ERACS pathway.',
                                                 'patient_paced6hrs': 'Indicator of whether the patient was '
                                                                      'paced during the first six hours on '
                                                                      'ICU.',
                                                 'pc_ausage12_hrs': 'Patient-controlled analgesia (PCA) '
                                                                    'usage recorded over the first 12 hours '
                                                                    'post-operatively.',
                                                 'pca': 'Indicator of whether patient-controlled analgesia '
                                                        'was used for the episode.',
                                                 'person_id': 'Internal surrogate identifier of the linked '
                                                              'person record in the source patient '
                                                              'administration system.',
                                                 'pipeline_loaded_at': 'Timestamp when the ingestion '
                                                                       'pipeline loaded the record into this '
                                                                       'table.',
                                                 'platelet_mapping_performed': 'Yes/No indicator of whether '
                                                                               'platelet mapping testing was '
                                                                               'performed for this episode.',
                                                 'platelet_number_of_units': 'Number of platelet units '
                                                                             'transfused for the ERACS '
                                                                             'episode.',
                                                 'pon_vscore': 'Recorded post-operative nausea and vomiting '
                                                               '(PONV) score for the patient following '
                                                               'cardiac or thoracic surgery, held as a coded '
                                                               'text value.',
                                                 'post_op_analgesiatxt': 'Post-operative analgesia option(s) '
                                                                         'recorded for the episode from the '
                                                                         'ERACS coded picklist.',
                                                 'post_op_planthoracic_patientstxt': 'Decoded description of '
                                                                                     'the agreed '
                                                                                     'post-operative '
                                                                                     'management plan for '
                                                                                     'thoracic surgery '
                                                                                     'patients.',
                                                 'post_op_plantxt': 'Coded picklist label(s) describing the '
                                                                    'agreed post-operative management plan '
                                                                    'for the patient.',
                                                 'post_protamine_act': 'Activated clotting time (ACT) result '
                                                                       'recorded after protamine '
                                                                       'administration, stored as text.',
                                                 'post_protamine_act_num': 'Leading number parsed from '
                                                                           'POST_PROTAMINE_ACT '
                                                                           '(ERACS_TYPED_V1).',
                                                 'post_protamine_act_parse_status': 'PARSED, BLANK or '
                                                                                    'UNPARSED for '
                                                                                    'POST_PROTAMINE_ACT.',
                                                 'postoperative_pain_controltxt': 'Decoded description of '
                                                                                  'the post-operative pain '
                                                                                  'control method or methods '
                                                                                  'used for the episode.',
                                                 'primary_operatortxt': 'Name of the clinician recorded as '
                                                                        'the primary operator for the '
                                                                        'procedure.',
                                                 'procedure_performedtxt': 'Decoded description of the '
                                                                           'cardiac surgical procedure '
                                                                           'performed for the episode.',
                                                 'r_vfunction_post_cpb': 'Assessed right ventricular '
                                                                         'function following cardiopulmonary '
                                                                         'bypass, recorded as a graded '
                                                                         'category.',
                                                 'r_vfunction_pre': 'Categorical assessment of right '
                                                                    'ventricular function recorded before '
                                                                    'the procedure.',
                                                 'rbc_number_of_units': 'Number of red blood cell units '
                                                                        'transfused for the ERACS episode.',
                                                 'row_hash': "Hash value computed across the source record's "
                                                             'field values to support change detection '
                                                             'during ingestion.',
                                                 'setup_pacing_mode': 'Pacing mode configured when temporary '
                                                                      'pacing was set up for the patient.',
                                                 'setup_pacing_rate': 'Pacing rate configured when temporary '
                                                                      'pacing was set up for the patient.',
                                                 'source_absent_detected_ts': 'Timestamp recording when the '
                                                                              'ingestion process first '
                                                                              'detected that the record was '
                                                                              'no longer present in the '
                                                                              'source registry.',
                                                 'source_present_ind': 'Canonical boolean mirror of '
                                                                       'IS_CURRENT_IN_SOURCE; false rows are '
                                                                       'retained source tombstones.',
                                                 'source_record_key': 'Key of the originating source '
                                                                      'registry record, derived from the '
                                                                      'source entry identifier.',
                                                 'source_table': 'Name of the source system table from which '
                                                                 'the record was ingested.',
                                                 'spinal_drain_inserted': 'Indicator of whether a spinal '
                                                                          '(cerebrospinal fluid) drain was '
                                                                          'inserted during the episode.',
                                                 'spinal_drainage_above': 'Numeric spinal drainage threshold '
                                                                          'value recorded in the ERACS '
                                                                          'spinal drain section.',
                                                 'suggamedex_administered': 'Indicator of whether sugammadex '
                                                                            'was administered for '
                                                                            'neuromuscular blockade reversal '
                                                                            'during the episode.',
                                                 'surgical_site_checked': 'Yes/No indicator of whether the '
                                                                          'surgical site was checked as part '
                                                                          'of the ERACS peri-operative '
                                                                          'compliance checks.',
                                                 'surname': "Patient's surname as recorded on the source "
                                                            'ERACS registry record.',
                                                 'teg_post_protamine_performed': 'Yes/No indicator of '
                                                                                 'whether a '
                                                                                 'thromboelastography (TEG) '
                                                                                 'test was performed after '
                                                                                 'protamine administration.',
                                                 'temperature_at_arrival_on_icu': 'Patient temperature '
                                                                                  'recorded on arrival on '
                                                                                  'ICU, held as free text.',
                                                 'thoracic_proceduretxt': 'Thoracic procedure recorded for '
                                                                          'the episode, selected from the '
                                                                          'ERACS coded picklist.',
                                                 'timing_of1st': 'Recorded timing of the first prophylactic '
                                                                 'antibiotic dose, held as free text.',
                                                 'timing_of1st_parse_status': 'PARSED, BLANK or UNPARSED for '
                                                                              'TIMING_OF1ST.',
                                                 'timing_of1st_time': "'HH:MM' parsed from TIMING_OF1ST "
                                                                      '(ERACS_TYPED_V1).',
                                                 'timing_of2nd': 'Recorded timing of the second prophylactic '
                                                                 'antibiotic dose, held as free text.',
                                                 'timing_of2nd_parse_status': 'PARSED, BLANK or UNPARSED for '
                                                                              'TIMING_OF2ND.',
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
                                                                                       'which paracetamol '
                                                                                       'was administered '
                                                                                       'during the episode.',
                                                 'timing_of_paracetamol_administered_parse_status': 'PARSED, '
                                                                                                    'BLANK '
                                                                                                    'or '
                                                                                                    'UNPARSED '
                                                                                                    'for '
                                                                                                    'TIMING_OF_PARACETAMOL_ADMINISTERED.',
                                                 'timing_of_paracetamol_administered_time': "'HH:MM' parsed "
                                                                                            'from '
                                                                                            'TIMING_OF_PARACETAMOL_ADMINISTERED '
                                                                                            '(ERACS_TYPED_V1).',
                                                 'tof': 'Recorded train-of-four (TOF) count from '
                                                        'neuromuscular blockade monitoring for the episode.',
                                                 'total_amount_pca': 'Total patient-controlled analgesia '
                                                                     'amount recorded for the episode, '
                                                                     'stored as text without a documented '
                                                                     'unit.',
                                                 'total_volume_drained': 'Total volume drained via the chest '
                                                                         'drains as recorded by the clinical '
                                                                         'team, held as free text.',
                                                 'tricuspid_valve': 'Tricuspid valve procedure or '
                                                                    'intervention type recorded for the '
                                                                    'episode.',
                                                 'two_couton_icu': 'Indicator of whether a trial without '
                                                                   'urinary catheter took place while the '
                                                                   'patient was on ICU.',
                                                 'ulcer_prophylaxistxt': 'Coded picklist label(s) recording '
                                                                         'the ulcer prophylaxis medication '
                                                                         'given during the ERACS pathway.',
                                                 'underlying_rhythmtxt': 'Coded underlying cardiac rhythm '
                                                                         'recorded for the patient.',
                                                 'vcap_threshold': 'Ventricular capture threshold recorded '
                                                                   'at the epicardial pacing wire check, '
                                                                   'held as a free-text value.',
                                                 'vein_harvest_sitetxt': 'Coded conduit vein harvest site or '
                                                                         'sites recorded for the operation, '
                                                                         'stored as concatenated code and '
                                                                         'description pairs.'},
                                     'table': 'Typed child of clinical_registry_entry for the eracs_episode '
                                              'family. Enhanced Recovery After Cardiac Surgery pathway '
                                              'registry, live since 2025: pathway timestamps, pacing, '
                                              'TEG/platelet mapping, transfusion, mobilisation and discharge '
                                              'milestones. Every governed bronze column is retained under '
                                              'its lower-case name.'}}
