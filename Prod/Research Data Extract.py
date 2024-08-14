# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *


# COMMAND ----------


@dlt.table(
    comment="Table that includes details of current patient demographics"
)
def rde_patient_demographics():
    pat = spark.table("4_prod.raw.mill_dir_person_patient").filter(col("ACTIVE_IND") > 0).alias("Pat")
    pers = spark.table("4_prod.raw.mill_dir_person").filter(col("ACTIVE_IND") > 0).alias("Pers")
    pinfo = spark.table("4_prod.raw.mill_dir_person_info").filter(col("ACTIVE_IND") > 0).alias("PInfo")
    address_lookup = spark.table("4_prod.support_tables.current_address").alias("A")
    nhs_lookup = spark.table("	4_prod.support_tables.patient_nhs").alias("NHS")
    mrn_lookup = spark.table("4_prod.support_tables.patient_mrn").alias("MRN")
    code_value_lookup = spark.table("4_prod.support_tables.code_value").alias("CV")
    
    return (
        pat
        .join(pers, (col("Pat.person_id") == col("Pers.person_id")) & (col("Pers.active_ind") == 1), "left")
        .join(pinfo, (col("PInfo.person_id") == col("Pat.person_id")) & (col("PInfo.active_ind") == 1), "left")
        .join(address_lookup, col("A.PARENT_ENTITY_ID") == col("Pat.PERSON_ID"), "left")
        .join(nhs_lookup, col("NHS.PERSON_ID") == col("Pat.PERSON_ID"), "left")
        .join(mrn_lookup, col("MRN.PERSON_ID") == col("Pat.PERSON_ID"), "left")
        .join(code_value_lookup.alias("Eth"), col("Pers.ETHNIC_GRP_CD") == col("Eth.CODE_VALUE"), "left")
        .join(code_value_lookup.alias("Gend"), col("Pers.SEX_CD") == col("Gend.CODE_VALUE"), "left")
        .join(code_value_lookup.alias("Mart"), col("Pers.MARITAL_TYPE_CD") == col("Mart.CODE_VALUE"), "left")
        .join(code_value_lookup.alias("lang"), col("Pers.LANGUAGE_CD") == col("lang.CODE_VALUE"), "left")
        .join(code_value_lookup.alias("Reli"), col("Pers.RELIGION_CD") == col("Reli.CODE_VALUE"), "left")
        .select(
            col("Pat.PERSON_ID").alias("PERSON_ID"),
            col("NHS.ALIAS").alias("NHS_Number"),
            col("MRN.ALIAS").alias("MRN"),
            col("Pers.BIRTH_DT_TM").alias("Date_of_Birth"),
            col("Gend.CODE_VALUE").alias("GENDER_CD"),
            col("Gend.DISPLAY").alias("Gender"),
            col("Eth.CODE_VALUE").alias("ETHNIC_CD"),
            col("Eth.DISPLAY").alias("Ethnicity"),
            col("Pers.DECEASED_DT_TM").alias("Date_of_Death"),
            col("A.ZIPCODE").alias("Postcode"),
            col("A.CITY").alias("City"),
            col("Pers.MARITAL_TYPE_CD").alias("MARITAL_STATUS_CD"),
            col("Mart.DISPLAY").alias("MARITAL_STATUS"),
            col("Pers.LANGUAGE_CD").alias("LANGUAGE_CD"),
            col("lang.DISPLAY").alias("LANGUAGE"),
            col("Pers.RELIGION_CD").alias("RELIGION_CD"),
            col("Reli.DISPLAY").alias("RELIGION"),
            greatest(
                col("Pat.ADC_UPDT"),
                col("Pers.ADC_UPDT"),
                col("PInfo.ADC_UPDT"),
                col("A.ADC_UPDT"),
                col("NHS.ADC_UPDT"),
                col("MRN.ADC_UPDT")
            ).alias("ADC_UPDT")
        )
    )

# COMMAND ----------


@dlt.table(
    comment="Table that includes details of patient encounters"
)
def rde_encounter():
    encounter = spark.table("4_prod.raw.mill_dir_encounter").alias("E")
    patient_demographics = dlt.read("rde_patient_demographics").alias("D")
    code_value_lookup = spark.table("4_prod.support_tables.code_value").alias("CV")
    encounter_alias = spark.table("4_prod.raw.mill_dir_encntr_alias")

    # Window function for selecting the first row of encounter alias
    window_spec = Window.partitionBy("EA.encntr_id").orderBy(desc("EA.beg_effective_dt_tm"))

    return (
        encounter
        .join(patient_demographics, col("E.person_id") == col("D.PERSON_ID"), "inner")
        .join(code_value_lookup.alias("etype"), col("E.ENCNTR_TYPE_CD") == col("etype.CODE_VALUE"), "left")
        .join(code_value_lookup.alias("estat"), col("E.ENCNTR_STATUS_CD") == col("estat.CODE_VALUE"), "left")
        .join(code_value_lookup.alias("ADM"), 
              when(col("E.FINANCIAL_CLASS_CD").isin(0, None), 
                   when(col("E.ACCOMMODATION_CD").isin(0, None), col("E.SERVICE_CATEGORY_CD"))
                   .otherwise(col("E.ACCOMMODATION_CD")))
              .otherwise(col("E.FINANCIAL_CLASS_CD")) == col("ADM.CODE_VALUE"), "left")
        .join(code_value_lookup.alias("TFC"), col("E.MED_SERVICE_CD") == col("TFC.CODE_VALUE"), "left")
        .join(encounter_alias.alias("EA"), (col("E.encntr_id") == col("EA.encntr_id")) & 
              (col("EA.encntr_alias_type_cd") == 1081) & (col("EA.active_ind") == 1), "left")
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .join(encounter_alias.alias("EF"), (col("E.encntr_id") == col("EF.encntr_id")) & 
              (col("EF.encntr_alias_type_cd") == 1077) & (col("EF.active_ind") == 1), "left")
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)      
        .select(
            col("E.PERSON_ID"),
            col("E.ENCNTR_ID"),
            col("D.NHS_Number"),
            col("E.REASON_FOR_VISIT").alias("REASON_FOR_VISIT_TXT"),
            col("D.MRN"),
            col("E.ENCNTR_TYPE_CD"),
            col("etype.DISPLAY").alias("ENC_TYPE"),
            col("E.ENCNTR_STATUS_CD").alias("ENC_STATUS_CD"),
            col("estat.DISPLAY").alias("ENC_STATUS"),
            col("EF.ALIAS").alias("FIN_NBR_ID"),
            when(col("E.FINANCIAL_CLASS_CD").isin(0, None), 
                 when(col("E.ACCOMMODATION_CD").isin(0, None), col("E.SERVICE_CATEGORY_CD"))
                 .otherwise(col("E.ACCOMMODATION_CD")))
            .otherwise(col("E.FINANCIAL_CLASS_CD")).alias("ADMIN_CATEGORY_CD"),
            col("ADM.DISPLAY").alias("ADMIN_DESC"),
            col("E.MED_SERVICE_CD").alias("TREATMENT_FUNCTION_CD"),
            col("TFC.DISPLAY").alias("TFC_DESC"),
            col("EA.ALIAS").alias("VISIT_ID"),
            col("E.REG_DT_TM").alias("CREATE_DT_TM"),
            greatest(col("E.ADC_UPDT"), col("D.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

# COMMAND ----------

@dlt.table(
    comment="Table that includes details of APC Diagnosis"
)
def rde_apc_diagnosis():
    cds_apc_icd_diag = spark.table("4_prod.raw.cds_apc_icd_diag").alias("Icd")
    cds_apc = spark.table("4_prod.raw.cds_apc").alias("Apc")
    lkp_icd_diag = spark.table("3_lookup.dwh.lkp_icd_diag").alias("ICDDESC")
    patient_demographics = dlt.read("rde_patient_demographics").alias("Pat")

    return (
        cds_apc_icd_diag
        .join(cds_apc, col("Icd.CDS_APC_ID") == col("Apc.CDS_APC_ID"), "inner")
        .join(patient_demographics, col("Pat.NHS_Number") == col("Apc.NHS_NUMBER"), "inner")
        .join(lkp_icd_diag, col("Icd.ICD_Diagnosis_Cd") == col("ICDDESC.ICD_Diag_Cd"), "left")
        .select(
            col("Icd.CDS_APC_ID").cast(StringType()).alias("CDS_APC_ID"),
            col("Pat.PERSON_ID").cast(StringType()).alias("PERSONID"),
            col("Pat.MRN").cast(StringType()).alias("MRN"),
            col("Icd.ICD_Diagnosis_Num").cast(IntegerType()).alias("ICD_Diagnosis_Num"),
            col("Icd.ICD_Diagnosis_Cd").cast(StringType()).alias("ICD_Diagnosis_Cd"),
            col("ICDDESC.ICD_Diag_Desc").cast(StringType()).alias("ICD_Diag_Desc"),
            col("Pat.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("Apc.Start_Dt").cast(StringType()).alias("Activity_date"),
            col("Apc.CDS_Activity_Dt").cast(StringType()).alias("CDS_Activity_Dt"),
            greatest(col("Icd.ADC_UPDT"), col("Apc.ADC_UPDT"), col("Pat.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

# COMMAND ----------

@dlt.table(
    comment="Table that includes details of APC OPCS Procedures"
)
def rde_apc_opcs():
    cds_apc_opcs_proc = spark.table("4_prod.raw.cds_apc_opcs_proc").alias("OPCS")
    cds_apc = spark.table("4_prod.raw.cds_apc").alias("Apc")
    lkp_opcs_410 = spark.table("3_lookup.dwh.opcs_410").alias("PDesc")
    patient_demographics = dlt.read("rde_patient_demographics").alias("Pat")

    return (
        cds_apc_opcs_proc
        .join(cds_apc, col("OPCS.CDS_APC_ID") == col("Apc.CDS_APC_ID"), "inner")
        .join(patient_demographics, col("Pat.NHS_Number") == col("Apc.NHS_NUMBER"), "inner")
        .join(lkp_opcs_410, col("OPCS.OPCS_Proc_Cd") == col("PDesc.Proc_Cd"), "left")
        .select(
            col("OPCS.CDS_APC_ID").cast(StringType()).alias("CDS_APC_ID"),
            col("Pat.PERSON_ID").cast(StringType()).alias("PERSONID"),
            col("Pat.MRN").cast(StringType()).alias("MRN"),
            col("OPCS.OPCS_Proc_Num").cast(IntegerType()).alias("OPCS_Proc_Num"),
            col("OPCS.OPCS_Proc_Scheme_Cd").cast(StringType()).alias("OPCS_Proc_Scheme_Cd"),
            col("OPCS.OPCS_Proc_Cd").cast(StringType()).alias("OPCS_Proc_Cd"),
            col("PDesc.Proc_Desc").cast(StringType()).alias("Proc_Desc"),
            col("OPCS.OPCS_Proc_Dt").cast(StringType()).alias("OPCS_Proc_Dt"),
            col("Pat.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("Apc.Start_Dt").cast(StringType()).alias("Activity_date"),
            col("Apc.CDS_Activity_Dt").cast(StringType()).alias("CDS_Activity_Dt"),
            greatest(col("OPCS.ADC_UPDT"), col("Apc.ADC_UPDT"), col("Pat.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

# COMMAND ----------

@dlt.table(
    comment="Table that includes details of Outpatient Diagnosis"
)
def rde_op_diagnosis():
    cds_opa_icd_diag = spark.table("4_prod.raw.cds_opa_icd_diag").alias("Icd")
    cds_op_all = spark.table("4_prod.raw.cds_op_all").alias("OP")
    lkp_icd_diag = spark.table("3_lookup.dwh.lkp_icd_diag").alias("ICDDESC")
    patient_demographics = dlt.read("rde_patient_demographics").alias("Pat")

    return (
        cds_opa_icd_diag
        .join(cds_op_all, col("Icd.CDS_OPA_ID") == col("OP.CDS_OPA_ID"), "inner")
        .join(patient_demographics, col("Pat.NHS_Number") == col("OP.NHS_NUMBER"), "inner")
        .join(lkp_icd_diag, col("Icd.ICD_Diag_Cd") == col("ICDDESC.ICD_Diag_Cd"), "left")
        .select(
            col("Icd.CDS_OPA_ID").cast(StringType()).alias("CDS_OPA_ID"),
            col("Pat.PERSON_ID").cast(StringType()).alias("PERSONID"),
            col("Pat.MRN").cast(StringType()).alias("MRN"),
            col("Icd.ICD_Diag_Num").cast(IntegerType()).alias("ICD_Diagnosis_Num"),
            col("Icd.ICD_Diag_Cd").cast(StringType()).alias("ICD_Diagnosis_Cd"),
            col("ICDDESC.ICD_Diag_Desc").cast(StringType()).alias("ICD_Diag_Desc"),
            col("Pat.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("OP.Att_Dt").cast(StringType()).alias("Activity_date"),
            col("OP.CDS_Activity_Dt").cast(StringType()).alias("CDS_Activity_Dt"),
            greatest(col("Icd.ADC_UPDT"), col("OP.ADC_UPDT"), col("Pat.ADC_UPDT")).alias("ADC_UPDT")
        )
        .filter(col("Icd.ICD_Diag_Cd").isNotNull())
    )

# COMMAND ----------

@dlt.table(
    comment="Table that includes details of Outpatient OPCS Procedures"
)
def rde_opa_opcs():
    cds_opa_opcs_proc = spark.table("4_prod.raw.cds_opa_opcs_proc").alias("OPCS")
    cds_op_all = spark.table("4_prod.raw.cds_op_all").alias("OP")
    lkp_opcs_410 = spark.table("3_lookup.dwh.opcs_410").alias("OPDesc")
    patient_demographics = dlt.read("rde_patient_demographics").alias("Pat")

    return (
        cds_opa_opcs_proc
        .join(cds_op_all, col("OPCS.CDS_OPA_ID") == col("OP.CDS_OPA_ID"), "inner")
        .join(patient_demographics, col("Pat.NHS_Number") == col("OP.NHS_NUMBER"), "inner")
        .join(lkp_opcs_410, col("OPCS.OPCS_Proc_Cd") == col("OPDesc.Proc_Cd"), "left")
        .select(
            col("OPCS.CDS_OPA_ID").cast(StringType()).alias("CDS_OPA_ID"),
            col("Pat.PERSON_ID").cast(StringType()).alias("PERSONID"),
            col("Pat.MRN").cast(StringType()).alias("MRN"),
            col("OPCS.OPCS_Proc_Num").cast(IntegerType()).alias("OPCS_Proc_Num"),
            col("OPCS.OPCS_Proc_Scheme_Cd").cast(StringType()).alias("OPCS_Proc_Scheme_Cd"),
            col("OPCS.OPCS_Proc_Cd").cast(StringType()).alias("OPCS_Proc_Cd"),
            col("OPDesc.Proc_Desc").cast(StringType()).alias("Proc_Desc"),
            coalesce(col("OPCS.OPCS_Proc_Dt"), col("OP.Att_Dt")).cast(StringType()).alias("OPCS_Proc_Dt"),
            col("Pat.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("OP.CDS_Activity_Dt").cast(StringType()).alias("CDS_Activity_Dt"),
            greatest(col("OPCS.ADC_UPDT"), col("OP.ADC_UPDT"), col("Pat.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

# COMMAND ----------

@dlt.table(
    comment="Table that includes details of Inpatient Attendance"
)
def rde_cds_apc():
    slam_apc_hrg_v4 = spark.table("4_prod.raw.slam_apc_hrg_v4")
    cds_apc = spark.table("4_prod.raw.cds_apc")
    lkp_hrg_v4 = spark.table("3_lookup.dwh.hrg_v4")
    lkp_cds_patient_class = spark.table("3_lookup.dwh.cds_patient_class")
    lkp_cds_admin_cat = spark.table("3_lookup.dwh.lkp_cds_admin_cat")
    lkp_cds_admiss_source = spark.table("3_lookup.dwh.lkp_cds_admiss_source")
    lkp_cds_disch_dest = spark.table("3_lookup.dwh.lkp_cds_disch_dest")
    cds_eal_tail = spark.table("4_prod.raw.cds_eal_tail")
    cds_eal_entry = spark.table("4_prod.raw.cds_eal_entry")
    lkp_cds_priority_type = spark.table("3_lookup.dwh.cds_priority_type")
    pi_cde_code_value_ref = spark.table("3_lookup.dwh.pi_cde_code_value_ref")
    patient_demographics = dlt.read("rde_patient_demographics")
    encounter = dlt.read("rde_encounter")

    return (
        slam_apc_hrg_v4.alias("HRG")
        .join(cds_apc.alias("APC"), slam_apc_hrg_v4.CDS_APC_Id == cds_apc.CDS_APC_ID)
        .join(patient_demographics.alias("Pat"), patient_demographics.NHS_Number == cds_apc.NHS_Number)
        .join(lkp_hrg_v4.alias("HRGDesc"), slam_apc_hrg_v4.Spell_HRG_Cd == lkp_hrg_v4.HRG_Cd, "left")
        .join(lkp_cds_patient_class.alias("PC"), cds_apc.Ptnt_Class_Cd == lkp_cds_patient_class.Patient_Class_Cd, "left")
        .join(lkp_cds_admin_cat.alias("AC"), cds_apc.Admin_Cat_Cd == lkp_cds_admin_cat.Admin_Cat_Cd, "left")
        .join(lkp_cds_admiss_source.alias("ASrce"), cds_apc.Admiss_Srce_Cd == lkp_cds_admiss_source.Admiss_Source_Cd, "left")
        .join(lkp_cds_disch_dest.alias("DS"), cds_apc.Disch_Dest == lkp_cds_disch_dest.Disch_Dest_Cd, "left")
        .join(cds_eal_tail.alias("EalTl"), (patient_demographics.PERSON_ID == cds_eal_tail.Encounter_ID) & (cds_eal_tail.Record_Type == '060'), "left")
        .join(cds_eal_entry.alias("WL"), cds_eal_entry.CDS_EAL_Id == cds_eal_tail.CDS_EAL_ID, "left")
        .join(lkp_cds_priority_type.alias("PT"), lkp_cds_priority_type.Priority_Type_Cd == cds_eal_entry.Priority_Type_Cd, "left")
        .join(encounter.alias("Enc"), patient_demographics.PERSON_ID == encounter.PERSON_ID, "left")
        .join(pi_cde_code_value_ref.alias("Descr"), encounter.ENCNTR_TYPE_CD == pi_cde_code_value_ref.CODE_VALUE_CD, "left")
        .select(
            col("APC.CDS_APC_ID").cast(StringType()).alias("CDS_APC_ID"),
            col("Pat.PERSON_ID").cast(StringType()).alias("PERSONID"),
            col("Pat.MRN").cast(StringType()).alias("MRN"),
            col("Pat.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("APC.Start_Dt").cast(StringType()).alias("Adm_Dt"),
            col("APC.Disch_Dt").cast(StringType()).alias("Disch_Dt"),
            datediff(col("APC.Disch_Dt"), col("APC.Start_Dt")).cast(StringType()).alias("LOS"),
            col("WL.Priority_Type_Cd").cast(StringType()).alias("Priority_Cd"),
            col("PT.Priority_Type_Desc").cast(StringType()).alias("Priority_Desc"),
            col("APC.Treat_Func_Cd").cast(StringType()).alias("Treat_Func_Cd"),
            col("HRG.Spell_HRG_Cd").cast(StringType()).alias("Spell_HRG_Cd"),
            col("HRGDesc.HRG_Desc").cast(StringType()).alias("HRG_Desc"),
            col("PC.Patient_Class_Desc").cast(StringType()).alias("Patient_Class_Desc"),
            split(col("PC.Patient_Class_Desc"), "-").getItem(0).cast(StringType()).alias("PatClass_Desc"),
            col("APC.Admin_Cat_Cd").cast(StringType()).alias("Admin_Cat_Cd"),
            col("AC.Admin_Cat_Desc").cast(StringType()).alias("Admin_Cat_Desc"),
            col("APC.Admiss_Srce_Cd").cast(StringType()).alias("Admiss_Srce_Cd"),
            col("ASrce.Admiss_Source_Desc").cast(StringType()).alias("Admiss_Source_Desc"),
            col("APC.Disch_Dest").cast(StringType()).alias("Disch_Dest"),
            col("DS.Disch_Dest_Desc").cast(StringType()).alias("Disch_Dest_Desc"),
            col("APC.Ep_Num").cast(StringType()).alias("Ep_Num"),
            col("APC.Ep_Start_Dt_tm").cast(StringType()).alias("Ep_Start_Dt"),
            col("APC.Ep_End_Dt_tm").cast(StringType()).alias("Ep_End_Dt"),
            col("APC.CDS_Activity_Dt").cast(StringType()).alias("CDS_Activity_Dt"),
            col("Descr.CODE_DESC_TXT").cast(StringType()).alias("ENC_DESC"),
            greatest(col("HRG.ADC_UPDT"), col("APC.ADC_UPDT"), col("Pat.ADC_UPDT"), col("Enc.ADC_UPDT")).alias("ADC_UPDT")
        )
    )
