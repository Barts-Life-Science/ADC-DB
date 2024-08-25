# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
from pyspark.sql.types import *


# COMMAND ----------

# Function to get the max ADC_UPDT or return a default date if the table doesn't exist
def get_max_adc_updt(table_name):
    try:
        result = spark.sql(f"SELECT MAX(ADC_UPDT) FROM {table_name}").collect()[0][0]
        return result if result is not None else datetime(1980, 1, 1)
    except:
        return datetime(1980, 1, 1)

# COMMAND ----------

@dlt.table(name="rde_patient_demographics_stream", temporary=True)
def patient_demographics_stream():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_patient_demographics")
    
    # Load tables
    pat = spark.table("4_prod.raw.mill_dir_person_patient").alias("Pat").filter(col("active_ind") == 1)
    pers = spark.table("4_prod.raw.mill_dir_person").alias("Pers").filter(col("active_ind") == 1)
    address_lookup = spark.table("4_prod.support_tables.current_address").alias("A")
    nhs_lookup = spark.table("4_prod.support_tables.patient_nhs").alias("NHS")
    mrn_lookup = spark.table("4_prod.support_tables.patient_mrn").alias("MRN")
    code_value_lookup = spark.table("4_prod.support_tables.code_value").alias("CV")
    
    # Filter tables based on ADC_UPDT
    pat_filtered = pat.filter(col("ADC_UPDT") > max_adc_updt)
    pers_filtered = pers.filter((col("ADC_UPDT") > max_adc_updt))
    address_filtered = address_lookup.filter(col("ADC_UPDT") > max_adc_updt)
    nhs_filtered = nhs_lookup.filter(col("ADC_UPDT") > max_adc_updt)
    mrn_filtered = mrn_lookup.filter(col("ADC_UPDT") > max_adc_updt)
    
    # Collect PERSON_IDs that need to be processed
    person_ids = (
        pat_filtered.select("PERSON_ID")
        .union(pers_filtered.select("PERSON_ID"))
        .union(address_filtered.select("PARENT_ENTITY_ID").alias("PERSON_ID"))
        .union(nhs_filtered.select("PERSON_ID"))
        .union(mrn_filtered.select("PERSON_ID"))
    ).distinct()
    
    # Filter pat based on collected PERSON_IDs
    pat_final = pat.join(person_ids, "PERSON_ID", "inner")
    
    new_data = (
        pat_final
        .join(pers, col("Pat.person_id") == col("Pers.person_id"), "left")
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
                col("A.ADC_UPDT"),
                col("NHS.ADC_UPDT"),
                col("MRN.ADC_UPDT")
            ).alias("ADC_UPDT")
        ).filter(col("ADC_UPDT") > max_adc_updt)
    )
    if not DeltaTable.isDeltaTable(spark, "rde_patient_demographics"):
        return new_data
    else:
        # Merge new_data with existing patient_demographics
        existing_data = dlt.read("rde_patient_demographics")
    
        merged_data = existing_data.alias("existing").merge(
            new_data.alias("new"),
            "existing.PERSON_ID = new.PERSON_ID"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll()

        return merged_data

@dlt.table(
    name="rde_patient_demographics",
    comment="Incrementally updated patient demographics",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def patient_demographics():
    return dlt.read("rde_patient_demographics_stream")

# COMMAND ----------


@dlt.table(name="rde_encounter_stream", temporary=True)
def encounter_stream():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_encounter")

    encounter = spark.table("4_prod.raw.mill_dir_encounter")
    patient_demographics = dlt.read("rde_patient_demographics").alias("D")
    code_value_lookup = spark.table("4_prod.support_tables.code_value").alias("CV")
    encounter_alias = spark.table("4_prod.raw.mill_dir_encntr_alias")

    person_ids = (
      encounter.filter(col("ADC_UPDT") > max_adc_updt).select("PERSON_ID")
      .union(patient_demographics.filter(col("ADC_UPDT") > max_adc_updt).select("PERSON_ID")
    )).distinct()

    encounter_final = encounter.join(person_ids, "PERSON_ID", "inner").alias("E")

    filtered_encounter_alias = encounter_alias.join(
        encounter_final.select("ENCNTR_ID"),
        encounter_alias.ENCNTR_ID == encounter_final.ENCNTR_ID,
        "inner"
    ).alias("EA")

    # Window function for selecting the first row of encounter alias
    window_spec = Window.partitionBy("EA.encntr_id").orderBy(desc("EA.beg_effective_dt_tm"))

    return (
        encounter_final
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

@dlt.table(
    name="rde_encounter",
    comment="Incrementally updated encounter data",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def encounter():
    if not DeltaTable.isDeltaTable(spark, "rde_encounter"):
        # If the table doesn't exist, create it from the stream
        return dlt.read("rde_encounter_stream")
    else:
        # If the table exists, apply changes
        return dlt.apply_changes(
            target = "rde_encounter",
            source = "rde_encounter_stream",
            keys = ["ENCNTR_ID"],
            sequence_by = "ADC_UPDT",
            apply_as_deletes = None,
            except_column_list = []
        )



# COMMAND ----------

@dlt.table(name="rde_apc_diagnosis_stream", temporary=True)
def apc_diagnosis_stream():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_apc_diagnosis")

    cds_apc_icd_diag = spark.table("4_prod.raw.cds_apc_icd_diag")
    cds_apc = spark.table("4_prod.raw.cds_apc").alias("Apc")
    lkp_icd_diag = spark.table("3_lookup.dwh.lkp_icd_diag").alias("ICDDESC")
    patient_demographics = dlt.read("rde_patient_demographics").alias("Pat")

    # Collect CDS_APC_IDs that need to be processed
    cds_apc_ids = (
        cds_apc_icd_diag.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_APC_ID")
        .union(cds_apc.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_APC_ID"))
    ).distinct()

    # Filter cds_apc_icd_diag based on collected CDS_APC_IDs
    cds_apc_icd_diag_final = cds_apc_icd_diag.join(cds_apc_ids, "CDS_APC_ID", "inner").alias("Icd")

    return (
        cds_apc_icd_diag_final
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
        ).filter(col("ADC_UPDT") > max_adc_updt)
    )

@dlt.table(
    name="rde_apc_diagnosis",
    comment="Incrementally updated APC diagnosis data",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def apc_diagnosis():
    if not DeltaTable.isDeltaTable(spark, "rde_apc_diagnosis"):
        return dlt.read("rde_apc_diagnosis_stream")
    else:
        return dlt.apply_changes(
            target = "rde_apc_diagnosis",
            source = "rde_apc_diagnosis_stream",
            keys = ["CDS_APC_ID", "ICD_Diagnosis_Num"],
            sequence_by = "ADC_UPDT",
            apply_as_deletes = None,
            except_column_list = []
        )






# COMMAND ----------

@dlt.table(name="rde_apc_opcs_stream", temporary=True)
def apc_opcs_stream():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_apc_opcs")

    cds_apc_opcs_proc = spark.table("4_prod.raw.cds_apc_opcs_proc")
    cds_apc = spark.table("4_prod.raw.cds_apc").alias("Apc")
    lkp_opcs_410 = spark.table("3_lookup.dwh.opcs_410").alias("PDesc")
    patient_demographics = dlt.read("rde_patient_demographics").alias("Pat")

    # Collect CDS_APC_IDs that need to be processed
    cds_apc_ids = (
        cds_apc_opcs_proc.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_APC_ID")
        .union(cds_apc.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_APC_ID"))
    ).distinct()

    # Filter cds_apc_opcs_proc based on collected CDS_APC_IDs
    cds_apc_opcs_proc_final = cds_apc_opcs_proc.join(cds_apc_ids, "CDS_APC_ID", "inner").alias("OPCS")

    return (
        cds_apc_opcs_proc_final
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
        ).filter(col("ADC_UPDT") > max_adc_updt)
    )

@dlt.table(
    name="rde_apc_opcs",
    comment="Incrementally updated APC OPCS data",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def apc_opcs():
    if not DeltaTable.isDeltaTable(spark, "rde_apc_opcs"):
        return dlt.read("rde_apc_opcs_stream")
    else:
        return dlt.apply_changes(
            target = "rde_apc_opcs",
            source = "rde_apc_opcs_stream",
            keys = ["CDS_APC_ID", "OPCS_Proc_Num"],
            sequence_by = "ADC_UPDT",
            apply_as_deletes = None,
            except_column_list = []
        )




# COMMAND ----------

@dlt.table(name="rde_op_diagnosis_stream", temporary=True)
def op_diagnosis_stream():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_op_diagnosis")

    cds_opa_icd_diag = spark.table("4_prod.raw.cds_opa_icd_diag")
    cds_op_all = spark.table("4_prod.raw.cds_op_all").alias("OP")
    lkp_icd_diag = spark.table("3_lookup.dwh.lkp_icd_diag").alias("ICDDESC")
    patient_demographics = dlt.read("rde_patient_demographics").alias("Pat")

    # Collect CDS_OPA_IDs that need to be processed
    cds_opa_ids = (
        cds_opa_icd_diag.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_OPA_ID")
        .union(cds_op_all.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_OPA_ID"))
    ).distinct()

    # Filter cds_opa_icd_diag based on collected CDS_OPA_IDs
    cds_opa_icd_diag_final = cds_opa_icd_diag.join(cds_opa_ids, "CDS_OPA_ID", "inner").alias("Icd")

    return (
        cds_opa_icd_diag_final
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
        ).filter(col("ADC_UPDT") > max_adc_updt)
        .filter(col("Icd.ICD_Diag_Cd").isNotNull())
    )

@dlt.table(
    name="rde_op_diagnosis",
    comment="Incrementally updated OP diagnosis data",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def op_diagnosis():
    if not DeltaTable.isDeltaTable(spark, "rde_op_diagnosis"):
        return dlt.read("rde_op_diagnosis_stream")
    else:
        return dlt.apply_changes(
            target = "rde_op_diagnosis",
            source = "rde_op_diagnosis_stream",
            keys = ["CDS_OPA_ID", "ICD_Diagnosis_Num"],
            sequence_by = "ADC_UPDT",
            apply_as_deletes = None,
            except_column_list = []
        )




# COMMAND ----------

@dlt.table(name="rde_opa_opcs_stream", temporary=True)
def opa_opcs_stream():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_opa_opcs")

    cds_opa_opcs_proc = spark.table("4_prod.raw.cds_opa_opcs_proc")
    cds_op_all = spark.table("4_prod.raw.cds_op_all").alias("OP")
    lkp_opcs_410 = spark.table("3_lookup.dwh.opcs_410").alias("OPDesc")
    patient_demographics = dlt.read("rde_patient_demographics").alias("Pat")

    # Collect CDS_OPA_IDs that need to be processed
    cds_opa_ids = (
        cds_opa_opcs_proc.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_OPA_ID")
        .union(cds_op_all.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_OPA_ID"))
    ).distinct()

    # Filter cds_opa_opcs_proc based on collected CDS_OPA_IDs
    cds_opa_opcs_proc_final = cds_opa_opcs_proc.join(cds_opa_ids, "CDS_OPA_ID", "inner").alias("OPCS")

    return (
        cds_opa_opcs_proc_final
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
        ).filter(col("ADC_UPDT") > max_adc_updt)
    )

@dlt.table(
    name="rde_opa_opcs",
    comment="Incrementally updated OPA OPCS data",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def opa_opcs():
    if not DeltaTable.isDeltaTable(spark, "rde_opa_opcs"):
        return dlt.read("rde_opa_opcs_stream")
    else:
        return dlt.apply_changes(
            target = "rde_opa_opcs",
            source = "rde_opa_opcs_stream",
            keys = ["CDS_OPA_ID", "OPCS_Proc_Num"],
            sequence_by = "ADC_UPDT",
            apply_as_deletes = None,
            except_column_list = []
        )





# COMMAND ----------

@dlt.table(name="rde_cds_apc_stream", temporary=True)
def cds_apc_stream():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_cds_apc")

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

    # Collect CDS_APC_IDs that need to be processed
    cds_apc_ids = (
        slam_apc_hrg_v4.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_APC_Id")
        .union(cds_apc.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_APC_ID"))
    ).distinct()

    # Filter slam_apc_hrg_v4 based on collected CDS_APC_IDs
    slam_apc_hrg_v4_final = slam_apc_hrg_v4.join(cds_apc_ids, slam_apc_hrg_v4.CDS_APC_Id == cds_apc_ids.CDS_APC_Id, "inner").select(slam_apc_hrg_v4["*"])

    return (
         slam_apc_hrg_v4_final.alias("HRG")
        .join(cds_apc.alias("APC"), col("HRG.CDS_APC_Id") == col("APC.CDS_APC_ID"))
        .join(patient_demographics.alias("Pat"), col("Pat.NHS_Number") == col("APC.NHS_Number"))
        .join(lkp_hrg_v4.alias("HRGDesc"), col("HRG.Spell_HRG_Cd") == col("HRGDesc.HRG_Cd"), "left")
        .join(lkp_cds_patient_class.alias("PC"), col("APC.Ptnt_Class_Cd") == col("PC.Patient_Class_Cd"), "left")
        .join(lkp_cds_admin_cat.alias("AC"), col("APC.Admin_Cat_Cd") == col("AC.Admin_Cat_Cd"), "left")
        .join(lkp_cds_admiss_source.alias("ASrce"), col("APC.Admiss_Srce_Cd") == col("ASrce.Admiss_Source_Cd"), "left")
        .join(lkp_cds_disch_dest.alias("DS"), col("APC.Disch_Dest") == col("DS.Disch_Dest_Cd"), "left")
        .join(cds_eal_tail.alias("EalTl"), (col("Pat.PERSON_ID") == col("EalTl.Encounter_ID")) & (col("EalTl.Record_Type") == '060'), "left")
        .join(cds_eal_entry.alias("WL"), col("WL.CDS_EAL_Id") == col("EalTl.CDS_EAL_ID"), "left")
        .join(lkp_cds_priority_type.alias("PT"), col("PT.Priority_Type_Cd") == col("WL.Priority_Type_Cd"), "left")
        .join(encounter.alias("Enc"), col("Pat.PERSON_ID") == col("Enc.PERSON_ID"), "left")
        .join(pi_cde_code_value_ref.alias("Descr"), col("Enc.ENCNTR_TYPE_CD") == col("Descr.CODE_VALUE_CD"), "left")
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
        ).filter(col("ADC_UPDT") > max_adc_updt)
    )

@dlt.table(
    name="rde_cds_apc",
    comment="Incrementally updated CDS APC data",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def cds_apc():
    if not DeltaTable.isDeltaTable(spark, "rde_cds_apc"):
        return dlt.read("rde_cds_apc_stream")
    else:
        return dlt.apply_changes(
            target = "rde_cds_apc",
            source = "rde_cds_apc_stream",
            keys = ["CDS_APC_ID"],
            sequence_by = "ADC_UPDT",
            apply_as_deletes = None,
            except_column_list = []
        )



       

# COMMAND ----------

@dlt.table(name="rde_cds_opa_stream", temporary=True)
def cds_opa_stream():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_cds_opa")

    cds_op_all = spark.table("4_prod.raw.cds_op_all")
    patient_demographics = dlt.read("rde_patient_demographics").alias("Pat")
    slam_op_hrg = spark.table("4_prod.raw.slam_op_hrg").alias("HRG")
    lkp_hrg_v4 = spark.table("3_lookup.dwh.hrg_v4").alias("HRGDesc")
    lkp_cds_first_attend = spark.table("3_lookup.dwh.cds_first_attend").alias("FA")
    lkp_cds_attended = spark.table("3_lookup.dwh.lkp_cds_attended").alias("AD")
    lkp_cds_attendance_outcome = spark.table("3_lookup.dwh.cds_attendance_outcome").alias("AO")
    pi_lkp_cde_code_value_ref = spark.table("3_lookup.dwh.pi_cde_code_value_ref").alias("AttType")
    cds_op_all_tail = spark.table("4_prod.raw.cds_op_all_tail").alias("OPATail")
    encounter = dlt.read("rde_encounter").alias("Enc")
    
    # Incorporating logic from PI_CDE_OP_ATTENDANCE
    mill_dir_cds_batch_content_hist = spark.table("4_prod.raw.mill_dir_cds_batch_content_hist").alias("BHIST")
    mill_dir_encounter = spark.table("4_prod.raw.mill_dir_encounter").alias("ENC")
    mill_dir_sch_appt = spark.table("4_prod.raw.mill_dir_sch_appt").alias("APPT")
    mill_dir_sch_event = spark.table("4_prod.raw.mill_dir_sch_event").alias("SCHE")
    mill_dir_encntr_alias = spark.table("4_prod.raw.mill_dir_encntr_alias").alias("EA")

    # Collect CDS_OPA_IDs that need to be processed
    cds_opa_ids = (
        cds_op_all.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_OPA_ID")
        .union(slam_op_hrg.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_OPA_Id"))
    ).distinct()

    # Filter cds_op_all based on collected CDS_OPA_IDs
    cds_op_all_final = cds_op_all.join(cds_opa_ids, cds_op_all.CDS_OPA_ID == cds_opa_ids.CDS_OPA_ID, "inner").select(cds_op_all["*"]).alias("OPALL")

    op_attendance = (
        cds_op_all_final
        .join(mill_dir_cds_batch_content_hist, 
              (regexp_replace(regexp_replace(col("OPALL.CDS_OPA_ID"), "BR1H00", ""), "BRNJ00", "") == col("BHIST.CDS_BATCH_CONTENT_ID").cast("string")) & 
              (to_date(col("OPALL.APPLICABLE_DT_TM")) == current_date()), 
              "left")
        .join(mill_dir_encounter, col("BHIST.ENCOUNTER_ID") == col("ENC.ENCNTR_ID"), "left")
        .join(mill_dir_sch_appt, (col("ENC.ENCNTR_ID") == col("APPT.ENCNTR_ID")) & (col("BHIST.PARENT_ENTITY_ID") == col("APPT.SCHEDULE_ID")), "left")
        .join(mill_dir_sch_event, col("APPT.SCH_EVENT_ID") == col("SCHE.SCH_EVENT_ID"), "left")
        .join(mill_dir_encntr_alias, (col("ENC.ENCNTR_ID") == col("EA.ENCNTR_ID")) & (col("EA.ENCNTR_ALIAS_TYPE_CD") == 1077) & (col("EA.ACTIVE_IND") == 1) & (col("EA.END_EFFECTIVE_DT_TM") > current_date()), "left")
        .filter((col("BHIST.UPDT_DT_TM") >= current_date()) & 
                (col("BHIST.CDS_TYPE_CD").isin(4446195, 14434936, 71834305)) & 
                (col("BHIST.CDS_BATCH_ID") != 0) & 
                (col("BHIST.ORGANIZATION_ID") == 8367658))
        .select(
            col("OPALL.CDS_OPA_ID").alias("CDS_BATCH_CONTENT_ID"),
            when(col("BHIST.CDS_TYPE_CD") == 71834305, col("BHIST.ACTIVITY_DT_TM")).otherwise(col("APPT.BEG_DT_TM")).alias("APPT_DT_TM"),
            coalesce(col("APPT.APPT_LOCATION_CD"), lit(0)).alias("APPT_LOCATION_CD"),
            when(col("ENC.REASON_FOR_VISIT") == "", None).otherwise(col("ENC.REASON_FOR_VISIT")).alias("REASON_FOR_VISIT_TXT"),
            coalesce(col("SCHE.APPT_TYPE_CD"), lit(0)).alias("APPT_TYPE_CD"),
            col("ENC.PERSON_ID"),
            col("EA.ALIAS").alias("FIN_NBR_ID"),
            when(col("OPALL.Att_Or_DNA_Cd") == -1, None).otherwise(col("OPALL.Att_Or_DNA_Cd")).alias("ATTENDED_DNA_NHS_CD_ALIAS"),
            coalesce(col("APPT.DURATION"), lit(0)).alias("EXPECTED_DUR_OF_APPT_NBR"),
            col("OPALL.Activity_Locn_Type_Cd").alias("ACTIVITY_LOC_TYPE_NHS_CD_ALIAS"),
            col("ENC.CREATE_PRSNL_ID").alias("ENCNTR_CREATE_PRSNL_ID"),
            col("ENC.UPDT_ID").alias("ENCNTR_UPDT_PRSNL_ID"),
            col("ENC.ACTIVE_IND").alias("ENCNTR_ACTIVE_IND"),
            col("BHIST.CDS_TYPE_CD"),
            col("BHIST.ACTIVITY_DT_TM")
        )
    )

    # Create a temporary view for op_attendance
    op_attendance.createOrReplaceTempView("op_attendance_view")

    # Rest of the code remains the same
    return (
        cds_op_all_final
        .join(patient_demographics, col("Pat.NHS_Number") == col("OPALL.NHS_NUMBER"), "inner")
        .join(slam_op_hrg, (col("HRG.MRN") == col("OPALL.MRN")) & (col("HRG.CDS_OPA_Id") == col("OPALL.CDS_OPA_ID")), "inner")
        .join(lkp_hrg_v4, col("HRG.NAC_HRG_Cd") == col("HRGDesc.HRG_Cd"), "left")
        .join(lkp_cds_first_attend, col("OPALL.First_Attend_Cd") == col("FA.First_Attend_Cd"), "left")
        .join(lkp_cds_attended, col("OPALL.Att_Or_DNA_Cd") == col("AD.Attended_Cd"), "left")
        .join(lkp_cds_attendance_outcome, col("OPALL.Outcome_Cd") == col("AO.Attendance_Outcome_Cd"), "left")
        .join(cds_op_all_tail, (col("OPATail.CDS_OPA_ID") == col("OPALL.CDS_OPA_ID")) & (col("OPALL.CDS_Activity_Dt") == to_date(col("OPATail.Activity_Dt_Tm"))), "left")
        .join(encounter, col("OPATail.Encounter_ID") == col("Enc.ENCNTR_ID"), "left")
        .join(pi_lkp_cde_code_value_ref, col("Enc.ENCNTR_TYPE_CD") == col("AttType.CODE_VALUE_CD"), "left")
        .join(spark.table("op_attendance_view"), col("OPALL.CDS_OPA_ID") == col("op_attendance_view.CDS_BATCH_CONTENT_ID"), "left")
        .select(
            col("OPALL.CDS_OPA_ID").cast(StringType()).alias("CDS_OPA_ID"),
            coalesce(col("Enc.ENC_TYPE"), lit("Outpatient")).cast(StringType()).alias("AttendanceType"),
            col("OPALL.CDS_Activity_Dt").cast(StringType()).alias("CDSDate"),
            col("OPALL.Att_Dt").cast(StringType()).alias("Att_Dt"),
            col("HRG.NAC_HRG_Cd").cast(StringType()).alias("HRG_Cd"),
            col("HRGDesc.HRG_Desc").cast(StringType()).alias("HRG_Desc"),
            col("OPALL.Treat_Func_Cd").cast(StringType()).alias("Treat_Func_Cd"),
            coalesce(col("AttType.CODE_DESC_TXT"), col("FA.First_Attend_Desc")).cast(StringType()).alias("Att_Type"),
            col("AD.Attended_Desc").cast(StringType()).alias("Attended_Desc"),
            col("AO.Attendance_Outcome_Desc").cast(StringType()).alias("Attendance_Outcome_Desc"),
            col("Pat.NHS_Number").cast(StringType()).alias("NHS_NUMBER"),
            col("op_attendance_view.APPT_DT_TM").cast(StringType()).alias("APPT_DT_TM"),
            col("op_attendance_view.APPT_LOCATION_CD").cast(StringType()).alias("APPT_LOCATION_CD"),
            col("op_attendance_view.REASON_FOR_VISIT_TXT").cast(StringType()).alias("REASON_FOR_VISIT_TXT"),
            col("op_attendance_view.APPT_TYPE_CD").cast(StringType()).alias("APPT_TYPE_CD"),
            col("op_attendance_view.FIN_NBR_ID").cast(StringType()).alias("FIN_NBR_ID"),
            col("op_attendance_view.ATTENDED_DNA_NHS_CD_ALIAS").cast(StringType()).alias("ATTENDED_DNA_NHS_CD_ALIAS"),
            col("op_attendance_view.EXPECTED_DUR_OF_APPT_NBR").cast(IntegerType()).alias("EXPECTED_DUR_OF_APPT_NBR"),
            col("op_attendance_view.ACTIVITY_LOC_TYPE_NHS_CD_ALIAS").cast(StringType()).alias("ACTIVITY_LOC_TYPE_NHS_CD_ALIAS"),
            greatest(col("OPALL.ADC_UPDT"), col("Pat.ADC_UPDT"), col("HRG.ADC_UPDT"), col("Enc.ADC_UPDT")).alias("ADC_UPDT")
        ).filter(col("ADC_UPDT") > max_adc_updt)
    )

@dlt.table(
    name="rde_cds_opa",
    comment="Incrementally updated CDS OPA data",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def cds_opa():
    if not DeltaTable.isDeltaTable(spark, "rde_cds_opa"):
        return dlt.read("rde_cds_opa_stream")
    else:
        return dlt.apply_changes(
            target = "rde_cds_opa",
            source = "rde_cds_opa_stream",
            keys = ["CDS_OPA_ID"],
            sequence_by = "ADC_UPDT",
            apply_as_deletes = None,
            except_column_list = []
        )



 

# COMMAND ----------


@dlt.table(name="rde_pathology_stream", temporary=True)
def pathology_stream():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_pathology")

    orders = spark.table("4_prod.raw.mill_dir_orders").alias("ORD")
    order_catalogue = spark.table("4_prod.raw.mill_dir_order_catalog").alias("CAT")
    clinical_event = spark.table("4_prod.raw.mill_dir_clinical_event")
    code_value_ref = spark.table("3_lookup.dwh.pi_cde_code_value_ref")
    blob_content = spark.table("4_prod.raw.pi_cde_blob_content").alias("d")
    encounter = dlt.read("rde_encounter").alias("ENC")

    clinical_event_final = clinical_event.filter(col("ADC_UPDT") > max_adc_updt).alias("EVE")


    joined_data = (
        clinical_event_final
        .join(encounter, col("EVE.ENCNTR_ID") == encounter.ENCNTR_ID)
        .join(orders, (encounter.ENCNTR_ID == orders.ENCNTR_ID) & (orders.ORDER_ID == col("EVE.ORDER_ID")))
        .join(order_catalogue, orders.CATALOG_CD == order_catalogue.CATALOG_CD)
        .join(code_value_ref.alias("Evres"), col("EVE.RESULT_UNITS_CD") == col("Evres.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("EVNTdes"), col("EVE.EVENT_CD") == col("EVNTdes.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ACTtype"), orders.ACTIVITY_TYPE_CD == col("ACTtype.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("RESFind"), col("EVE.NORMALCY_CD") == col("RESFind.CODE_VALUE_CD"), "left")
        .join(clinical_event.alias("EVNT2"), col("EVE.PARENT_EVENT_ID") == col("EVNT2.EVENT_ID"), "left")
        .join(code_value_ref.alias("TESTnm"), col("EVNT2.EVENT_CD") == col("TESTnm.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ORDStat"), orders.ORDER_STATUS_CD == col("ORDStat.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("RESstat"), col("EVE.RESULT_STATUS_CD") == col("RESstat.CODE_VALUE_CD"), "left")
        .join(blob_content, (col("EVE.EVENT_ID") == blob_content.EVENT_ID) | (col("EVNT2.EVENT_ID") == blob_content.EVENT_ID), "left")
    )

    return (
        joined_data
        .filter(
            (col("EVE.CONTRIBUTOR_SYSTEM_CD") == '6378204') & 
            (col("EVE.CONTRIBUTOR_SYSTEM_CD").isNotNull())
        )
        .select(
            encounter.ENCNTR_ID.cast(StringType()).alias("ENCNTR_ID"),
            encounter.PERSON_ID.cast(StringType()).alias("PERSONID"),
            encounter.MRN.cast(StringType()).alias("MRN"),
            encounter.NHS_Number.cast(StringType()).alias("NHS_Number"),
            orders.CURRENT_START_DT_TM.cast(StringType()).alias("RequestDate"),
            orders.ORDER_MNEMONIC.cast(StringType()).alias("TestCode"),
            col("TESTnm.CODE_DESC_TXT").cast(StringType()).alias("TestName"),
            col("EVNTdes.CODE_DESC_TXT").cast(StringType()).alias("TestDesc"),
            col("EVE.RESULT_VAL").cast(DoubleType()).alias("Result_nbr"),
            col("EVE.RESULT_VAL").cast(StringType()).alias("ResultTxt"),
            when(col("EVE.RESULT_VAL").cast("double").isNull(), lit(0)).otherwise(lit(1)).alias("ResultNumeric"),
            col("Evres.CODE_DESC_TXT").cast(StringType()).alias("ResultUnit"),
            col("EVE.NORMAL_HIGH").cast(StringType()).alias("ResUpper"),
            col("EVE.NORMAL_LOW").cast(StringType()).alias("ResLower"),
            col("RESFind.CODE_DESC_TXT").cast(StringType()).alias("Resultfinding"),
            col("EVE.EVENT_START_DT_TM").cast(StringType()).alias("ReportDate"),
            blob_content.BLOB_CONTENTS.cast(StringType()).alias("Report"),
            col("ORDStat.CODE_DESC_TXT").cast(StringType()).alias("OrderStatus"),
            col("RESstat.CODE_DESC_TXT").cast(StringType()).alias("ResStatus"),
            order_catalogue.CONCEPT_CKI.cast(StringType()).alias("SnomedCode"),
            col("EVE.EVENT_ID").cast(StringType()).alias("EventID"),
            substring(col("EVE.REFERENCE_NBR"), 1, 11).cast(StringType()).alias("LabNo"),
            greatest(col("EVE.ADC_UPDT"), orders.ADC_UPDT, encounter.ADC_UPDT, order_catalogue.ADC_UPDT).alias("ADC_UPDT")
        )
        .filter(col("ADC_UPDT") > max_adc_updt)
    )

@dlt.table(
    name="rde_pathology",
    comment="Incrementally updated pathology data",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def pathology():
    if not DeltaTable.isDeltaTable(spark, "rde_pathology"):
        return dlt.read("rde_pathology_stream")
    else:
        return dlt.apply_changes(
            target = "rde_pathology",
            source = "rde_pathology_stream",
            keys = ["ENCNTR_ID", "EventID"],
            sequence_by = "ADC_UPDT",
            apply_as_deletes = None,
            except_column_list = []
        )








# COMMAND ----------


@dlt.table(name="rde_raw_pathology_stream", temporary=True)
def raw_pathology_stream():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_raw_pathology")
    
    # Load source tables
    pres = spark.table("4_prod.raw.path_patient_resultlevel").alias("PRES")
    pmrt = spark.table("4_prod.raw.path_master_resultable").alias("PMRT")
    pmor = spark.table("4_prod.raw.path_master_orderables").alias("PMOR")
    psl = spark.table("4_prod.raw.path_patient_samplelevel").alias("PSL")
    
    # Filter pres based on ADC_UPDT
    filtered_pres = pres.filter(col("ADC_UPDT") > max_adc_updt)
    
    # Get distinct LabNo from filtered pres
    updated_lab_nos = filtered_pres.select("LabNo").distinct()
    
    # Filter psl based on LabNo from filtered pres using inner join
    filtered_psl = psl.join(updated_lab_nos, "LabNo", "inner")
    
    # Load patient demographics table
    patient_demographics = dlt.read("rde_patient_demographics").alias("PD")
    
    # Join the filtered and limited tables
    joined_data = (
        filtered_pres
        .join(filtered_psl, "LabNo")
        .join(pmrt, filtered_pres.TFCCode == pmrt.TFCCode, "left")
        .join(pmor, filtered_pres.TLCCode == pmor.TLCCode, "left")
        .join(patient_demographics, 
              (filtered_psl.NHSNo == patient_demographics.NHS_Number) | 
              (filtered_psl.MRN == patient_demographics.MRN), 
              "inner")
    )
    
    return (
        joined_data
        .select(
            patient_demographics.PERSON_ID,
            patient_demographics.NHS_Number,
            patient_demographics.MRN,
            filtered_pres.LabNo,
            filtered_pres.TLCCode,
            pmor.CSpecTypeCode.alias("Specimen"),
            pmor.SnomedCTCode.alias("TLCSnomed"),
            pmor.TLCDesc_Full.alias("TLCDesc"),
            filtered_pres.TFCCode,
            filtered_pres.LegTFCCode.alias("Subcode"),
            filtered_pres.WkgCode,
            when(filtered_pres.NotProcessed == 1, 0).otherwise(1).alias("Processed"),
            filtered_pres.Result1stLine.alias("Result"),
            when(
                filtered_pres.Result1stLine.isNotNull() & 
                (filtered_pres.Result1stLine != '.') & 
                filtered_pres.Result1stLine.cast("double").isNotNull(), 
                1
            ).otherwise(0).alias("ResultNumeric"),
            filtered_pres.ResultIDNo,
            pmrt.SectionCode,
            pmrt.TFCDesc_Full.alias("TFCDesc"),
            filtered_psl.RequestDT,
            filtered_psl.SampleDT,
            filtered_psl.ReportDate,
            filtered_psl.Fasting,
            filtered_psl.Pregnant,
            filtered_psl.RefClinCode,
            filtered_psl.RefSourceCode,
            filtered_psl.ClinicalDetails,
            greatest(col("PRES.ADC_UPDT"), col("PSL.ADC_UPDT")).alias("ADC_UPDT")
        ).distinct()
        .filter(col("ADC_UPDT") > max_adc_updt)
    )

@dlt.table(
    name="rde_raw_pathology",
    comment="Incrementally updated raw pathology data",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def raw_pathology():
    if not DeltaTable.isDeltaTable(spark, "rde_raw_pathology"):
        return dlt.read("rde_raw_pathology_stream")
    else:
        return dlt.apply_changes(
            target = "rde_raw_pathology",
            source = "rde_raw_pathology_stream",
            keys = ["LabNo", "TFCCode", "ResultIDNo"],
            sequence_by = "ADC_UPDT",
            apply_as_deletes = None,
            except_column_list = []
        )


     

# COMMAND ----------


@dlt.table(name="rde_ariapharmacy_stream", temporary=True)
def ariapharmacy_stream():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_ariapharmacy")
    
    pt_inst_key = spark.table("4_prod.raw.aria_pt_inst_key").alias("Ptkey")
    agt_rx = spark.table("4_prod.raw.aria_agt_rx").alias("Arx")
    rx = spark.table("4_prod.raw.aria_rx").alias("Rx")
    patient_demographics = dlt.read("rde_patient_demographics").alias("D")

    return (
        pt_inst_key.filter(col("ADC_UPDT") > max_adc_updt)
        .join(patient_demographics, 
              (col("D.NHS_Number") == regexp_replace(col("Ptkey.pt_key_value"), ' ', '')) &
              (col("Ptkey.pt_key_cd") == 24),
              "inner")
        .join(agt_rx, col("Arx.pt_id") == col("Ptkey.pt_id"), "inner")
        .join(rx, (col("Arx.pt_id") == col("Rx.pt_id")) & (col("Arx.rx_id") == col("Rx.rx_id")), "inner")
        .select(
            col("D.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("D.MRN").cast(StringType()).alias("MRN"),
            col("Arx.ADMN_START_DATE").cast(StringType()).alias("AdmnStartDate"),
            col("Arx.tp_name").cast(StringType()).alias("TreatPlan"),
            col("Arx.AGT_NAME").cast(StringType()).alias("ProductDesc"),
            col("Arx.dosage_form").cast(StringType()).alias("DosageForm"),
            col("Arx.rx_dose").cast(IntegerType()).alias("RxDose"),
            col("Arx.rx_total").cast(IntegerType()).alias("RxTotal"),
            col("Arx.set_date_tp_init").cast(StringType()).alias("SetDateTPInit"),
            col("Arx.dose_level").cast(IntegerType()).alias("DoseLevel"),
            col("Arx.admn_dosage_unit").cast(IntegerType()).alias("AdmnDosageUnit"),
            col("Arx.admn_route").cast(IntegerType()).alias("AdmnRoute"),
            col("Rx.pharm_appr_tstamp").cast(StringType()).alias("Pharmacist_Approved"),
            col("Ptkey.pt_inst_key_id").cast(StringType()).alias("pt_inst_key_id"),
            greatest(col("Ptkey.ADC_UPDT"), col("Arx.ADC_UPDT"), col("Rx.ADC_UPDT"), col("D.ADC_UPDT")).alias("ADC_UPDT")
        ).distinct()
        .filter(col("ADC_UPDT") > max_adc_updt)
    )

@dlt.table(
    name="rde_ariapharmacy",
    comment="Incrementally updated ARIA pharmacy data",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def ariapharmacy():
    if not DeltaTable.isDeltaTable(spark, "rde_ariapharmacy"):
        return dlt.read("rde_ariapharmacy_stream")
    else:
        return dlt.apply_changes(
            target = "rde_ariapharmacy",
            source = "rde_ariapharmacy_stream",
            keys = ["pt_inst_key_id"],
            sequence_by = "ADC_UPDT",
            apply_as_deletes = None,
            except_column_list = []
        )

# COMMAND ----------


@dlt.table(name="rde_iqemo_stream", temporary=True)
def iqemo_stream():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_iqemo")
    
    treatment_cycle = spark.table("4_prod.raw.iqemo_treatment_cycle").alias("TC")
    chemotherapy_course = spark.table("4_prod.raw.iqemo_chemotherapy_course").alias("CC")
    regimen = spark.table("4_prod.raw.iqemo_regimen").alias("RG")
    iqemo_patient = spark.table("4_prod.raw.iqemo_patient").alias("PT")
    patient_demographics = dlt.read("rde_patient_demographics").alias("DEM")

    return (
        treatment_cycle.filter(col("ADC_UPDT") > max_adc_updt)
        .join(chemotherapy_course, col("CC.ChemoTherapyCourseID") == col("TC.ChemoTherapyCourseID"), "left")
        .join(regimen, col("CC.RegimenID") == col("RG.RegimenID"), "left")
        .join(iqemo_patient, col("TC.PatientID") == col("PT.PatientID"), "left")
        .join(patient_demographics, col("PT.PrimaryIdentifier") == col("DEM.MRN"), "left")
        .select(
            col("DEM.PERSON_ID").cast(StringType()).alias("PERSON_ID"),
            col("PT.PrimaryIdentifier").cast(StringType()).alias("MRN"),
            col("PT.NHSNumber").cast(StringType()).alias("NHS_Number"),
            col("TC.TreatmentCycleID").cast(StringType()).alias("TreatmentCycleID"),
            col("TC.PrescribedDate").cast(StringType()).alias("PrescribedDate"),
            col("TC.TemplateName").cast(StringType()).alias("TemplateName"),
            col("RG.Name").cast(StringType()).alias("Name"),
            col("RG.DefaultCycles").cast(IntegerType()).alias("DefaultCycles"),
            col("RG.ChemoRadiation").cast(BooleanType()).alias("ChemoRadiation"),
            col("RG.OPCSProcurementCode").cast(StringType()).alias("OPCSProcurementCode"),
            col("RG.OPCSDeliveryCode").cast(StringType()).alias("OPCSDeliveryCode"),
            col("RG.SactName").cast(StringType()).alias("SactName"),
            col("RG.Indication").cast(StringType()).alias("Indication"),
            greatest(col("TC.ADC_UPDT"), col("CC.ADC_UPDT"), col("RG.ADC_UPDT"), col("PT.ADC_UPDT"), col("DEM.ADC_UPDT")).alias("ADC_UPDT")
        ).distinct()
        .filter(col("ADC_UPDT") > max_adc_updt)
    )

@dlt.table(
    name="rde_iqemo",
    comment="Incrementally updated iQEMO data",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def iqemo():
    if not DeltaTable.isDeltaTable(spark, "rde_iqemo"):
        return dlt.read("rde_iqemo_stream")
    else:
        return dlt.apply_changes(
            target = "rde_iqemo",
            source = "rde_iqemo_stream",
            keys = ["TreatmentCycleID"],
            sequence_by = "ADC_UPDT",
            apply_as_deletes = None,
            except_column_list = []
        )
