# Databricks notebook source
#https://learn.microsoft.com/en-us/azure/databricks/optimizations/incremental-refresh
#https://learn.microsoft.com/en-us/azure/databricks/delta/row-tracking

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

def get_max_adc_updt(table_name):
    try:
        default_date = lit(datetime(1980, 1, 1)).cast(DateType())
        result = spark.sql(f"SELECT MAX(ADC_UPDT) AS max_date FROM {table_name}")
        max_date = result.select(max("max_date").alias("max_date")).first()["max_date"]
        return max_date if max_date is not None else default_date
    except:
        return lit(datetime(1980, 1, 1)).cast(DateType())
    

def table_exists(table_name):
    try:
        result = spark.sql(f"SELECT 1 FROM {table_name} LIMIT 1")
        return result.first() is not None
    except:
        return False
    
def table_exists_with_rows(table_name):
    try:
        result = spark.sql(f"SELECT COUNT(*) AS row_count FROM {table_name}")
        row_count = result.first()["row_count"]
        return row_count > 0
    except:
        return False
    

# COMMAND ----------

@dlt.table(name="rde_patient_demographics_incr", temporary=True,
        table_properties={
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID",
        "skipChangeCommits": "true"
    })
def patient_demographics_incr():
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
    
    return (
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

@dlt.view(name="demographics_update")
def demographics_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_patient_demographics_incr")
    )

  
# Declare the target table
dlt.create_target_table(
    name = "rde_patient_demographics",
    comment="Incrementally updated patient demographics",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID"

    }
)

dlt.apply_changes(
  target = "rde_patient_demographics",
  source = "demographics_update",
   keys = ["PERSON_ID"],
        sequence_by = "ADC_UPDT",
        apply_as_deletes = None,
        except_column_list = [],
        stored_as_scd_type = 1
)

# COMMAND ----------


@dlt.table(name="rde_encounter_incr", table_properties={
        "pipelines.autoOptimize.zOrderCols": "ENCNTR_ID",
        "skipChangeCommits": "true"
    }, temporary=True)
def encounter_incr():
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

    
@dlt.view(name="encounter_update")
def encounter_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_encounter_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_encounter",
    comment="Incrementally updated encounter data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "ENCNTR_ID"

    }
)

dlt.apply_changes(
    target = "rde_encounter",
    source = "encounter_update",
    keys = ["ENCNTR_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)


# COMMAND ----------

@dlt.table(name="rde_apc_diagnosis_incr", table_properties={
        "skipChangeCommits": "true"
    }, temporary=True)
def apc_diagnosis_incr():
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

@dlt.view(name="apc_diagnosis_update")
def apc_diagnosis_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_apc_diagnosis_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_apc_diagnosis",
    comment="Incrementally updated APC diagnosis data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "CDS_APC_ID,ICD_Diagnosis_Num"

    }
)

dlt.apply_changes(
    target = "rde_apc_diagnosis",
    source = "apc_diagnosis_update",
    keys = ["CDS_APC_ID", "ICD_Diagnosis_Num"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)


# COMMAND ----------

@dlt.table(name="rde_apc_opcs_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def apc_opcs_incr():
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

@dlt.view(name="apc_opcs_update")
def apc_opcs_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_apc_opcs_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_apc_opcs",
    comment="Incrementally updated APC OPCS data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "CDS_APC_ID,OPCS_Proc_Num"

    }
)

dlt.apply_changes(
    target = "rde_apc_opcs",
    source = "apc_opcs_update",
    keys = ["CDS_APC_ID", "OPCS_Proc_Num"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)


# COMMAND ----------

@dlt.table(name="rde_op_diagnosis_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def op_diagnosis_incr():
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

@dlt.view(name="op_diagnosis_update")
def op_diagnosis_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_op_diagnosis_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_op_diagnosis",
    comment="Incrementally updated OP diagnosis data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "CDS_OPA_ID,ICD_Diagnosis_Num"

    }
)

dlt.apply_changes(
    target = "rde_op_diagnosis",
    source = "op_diagnosis_update",
    keys = ["CDS_OPA_ID", "ICD_Diagnosis_Num"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)


# COMMAND ----------

@dlt.table(name="rde_opa_opcs_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def opa_opcs_incr():
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

@dlt.view(name="opa_opcs_update")
def opa_opcs_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_opa_opcs_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_opa_opcs",
    comment="Incrementally updated OPA OPCS data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "CDS_OPA_ID,OPCS_Proc_Num"

    }
)

dlt.apply_changes(
    target = "rde_opa_opcs",
    source = "opa_opcs_update",
    keys = ["CDS_OPA_ID", "OPCS_Proc_Num"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)


# COMMAND ----------

@dlt.table(name="rde_cds_apc_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def cds_apc_incr():
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
        slam_apc_hrg_v4.filter(col("ADC_UPDT") > max_adc_updt).select(trim(col("CDS_APC_Id")).alias("CDS_APC_Id"))
        .union(cds_apc.filter(col("ADC_UPDT") > max_adc_updt).select(trim(col("CDS_APC_ID")).alias("CDS_APC_ID")))
    ).distinct()

    # Filter slam_apc_hrg_v4 based on collected CDS_APC_IDs
    slam_apc_hrg_v4_final = slam_apc_hrg_v4.join(cds_apc_ids, trim(slam_apc_hrg_v4.CDS_APC_Id) == trim(cds_apc_ids.CDS_APC_Id), "inner").select(slam_apc_hrg_v4["*"])


    return (
         slam_apc_hrg_v4_final.alias("HRG")
        .join(cds_apc.alias("APC"), trim(col("HRG.CDS_APC_Id")) == trim(col("APC.CDS_APC_ID")))
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

@dlt.view(name="cds_apc_update")
def cds_apc_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_cds_apc_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_cds_apc",
    comment="Incrementally updated CDS APC data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "CDS_APC_ID"
    }
)

dlt.apply_changes(
    target = "rde_cds_apc",
    source = "cds_apc_update",
    keys = ["CDS_APC_ID", "PERSONID", "EP_Num", "Adm_Dt"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)
       

# COMMAND ----------

@dlt.table(name="rde_cds_opa_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def cds_opa_incr():
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
            col("Pat.PERSON_ID").cast(StringType()).alias("PERSONID"),
            col("Pat.MRN").cast(StringType()).alias("MRN"),
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

@dlt.view(name="cds_opa_update")
def cds_opa_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_cds_opa_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_cds_opa",
    comment="Incrementally updated CDS OPA data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "CDS_OPA_ID"
    }
)

dlt.apply_changes(
    target = "rde_cds_opa",
    source = "cds_opa_update",
    keys = ["CDS_OPA_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)
 

# COMMAND ----------


@dlt.table(name="rde_pathology_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def pathology_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_pathology")

    orders = spark.table("4_prod.raw.mill_dir_orders").alias("ORD")
    order_catalogue = spark.table("4_prod.raw.mill_dir_order_catalog").alias("CAT")
    clinical_event = spark.table("4_prod.raw.mill_dir_clinical_event")
    code_value_ref = spark.table("3_lookup.dwh.pi_cde_code_value_ref")
    blob_content = spark.table("4_prod.raw.pi_cde_blob_content").alias("d")
    encounter = dlt.read("rde_encounter").alias("ENC")

    clinical_event_final = clinical_event.filter(F.col("VALID_UNTIL_DT_TM") > F.current_timestamp()).filter(col("ADC_UPDT") > max_adc_updt).alias("EVE")


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
    
@dlt.view(name="pathology_update")
def pathology_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_pathology_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_pathology",
    comment="Incrementally updated pathology data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "ENCNTR_ID,EventID"
    }
)

dlt.apply_changes(
    target = "rde_pathology",
    source = "pathology_update",
    keys = ["EventID", "LabNo", "TestCode"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------


@dlt.table(name="rde_raw_pathology_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def raw_pathology_incr():
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

@dlt.view(name="raw_pathology_update")
def raw_pathology_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_raw_pathology_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_raw_pathology",
    comment="Incrementally updated raw pathology data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "LabNo,TFCCode,ResultIDNo"

    }
)

dlt.apply_changes(
    target = "rde_raw_pathology",
    source = "raw_pathology_update",
    keys = ["PERSON_ID", "ResultIDNo"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

     

# COMMAND ----------


@dlt.table(name="rde_ariapharmacy_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def ariapharmacy_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_ariapharmacy")
    
    pt_inst_key = spark.table("4_prod.raw.aria_pt_inst_key").alias("Ptkey")
    agt_rx = spark.table("4_prod.raw.aria_agt_rx").alias("Arx")
    rx = spark.table("4_prod.raw.aria_rx").alias("Rx")
    patient_demographics = dlt.read("rde_patient_demographics").alias("D")
    person_alias = spark.table("4_prod.raw.mill_dir_person_alias").alias("PA")

    return (
        pt_inst_key.filter(col("ADC_UPDT") > max_adc_updt)
        .join(person_alias, 
              (regexp_replace(col("Ptkey.pt_key_value"), ' ', '') == col("PA.ALIAS")) &
              (col("PA.PERSON_ALIAS_TYPE_CD") == 18),
              "left")
        .join(patient_demographics, col("D.PERSON_ID") == col("PA.PERSON_ID"))
        .join(agt_rx, col("Arx.pt_id") == col("Ptkey.pt_id"), "inner")
        .join(rx, (col("Arx.pt_id") == col("Rx.pt_id")) & (col("Arx.rx_id") == col("Rx.rx_id")), "inner")
        .select(
            col("PA.PERSON_ID").cast(StringType()).alias("PERSON_ID"),
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
            greatest(col("Ptkey.ADC_UPDT"), col("Arx.ADC_UPDT"), col("Rx.ADC_UPDT"), col("D.ADC_UPDT"), col("PA.ADC_UPDT")).alias("ADC_UPDT")
        ).distinct()
        .filter(col("ADC_UPDT") > max_adc_updt)
        .filter(col("PERSON_ID").isNotNull())
    )

@dlt.view(name="ariapharmacy_update")
def ariapharmacy_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_ariapharmacy_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_ariapharmacy",
    comment="Incrementally updated ARIA pharmacy data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "pt_inst_key_id"

    }
)

dlt.apply_changes(
    target = "rde_ariapharmacy",
    source = "ariapharmacy_update",
    keys = ["NHS_Number", "AdmnStartDate", "ProductDesc", "RxDose", "RxTotal", "pt_inst_key_id"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------


@dlt.table(name="rde_iqemo_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def iqemo_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_iqemo")
   
    treatment_cycle = spark.table("4_prod.raw.iqemo_treatment_cycle").alias("TC")
    chemotherapy_course = spark.table("4_prod.raw.iqemo_chemotherapy_course").alias("CC")
    regimen = spark.table("4_prod.raw.iqemo_regimen").alias("RG")
    iqemo_patient = spark.table("4_prod.raw.iqemo_patient").alias("PT")
    patient_demographics = dlt.read("rde_patient_demographics").alias("DEM")
    person_alias = spark.table("4_prod.raw.mill_dir_person_alias").alias("PA")

    return (
        treatment_cycle.filter(col("ADC_UPDT") > max_adc_updt)
        .join(chemotherapy_course, col("CC.ChemoTherapyCourseID") == col("TC.ChemoTherapyCourseID"), "left")
        .join(regimen, col("CC.RegimenID") == col("RG.RegimenID"), "left")
        .join(iqemo_patient, col("TC.PatientID") == col("PT.PatientID"), "left")
        .join(
            person_alias.filter(col("PERSON_ALIAS_TYPE_CD") == 10),
            col("PT.PrimaryIdentifier") == col("PA.ALIAS"),
            "left"
        )
        .join(patient_demographics, col("PA.PERSON_ID") == col("DEM.PERSON_ID"), "left")
        .select(
            col("PA.PERSON_ID").cast(StringType()).alias("PERSON_ID"),
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
            greatest(col("TC.ADC_UPDT"), col("CC.ADC_UPDT"), col("RG.ADC_UPDT"), col("PT.ADC_UPDT"), col("DEM.ADC_UPDT"), col("PA.ADC_UPDT")).alias("ADC_UPDT")
        ).distinct()
        .filter(col("ADC_UPDT") > max_adc_updt)
        .filter(col("PERSON_ID").isNotNull())
    )

@dlt.view(name="iqemo_update")
def iqemo_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_iqemo_incr")
    )


# Declare the target table
dlt.create_target_table(
    name = "rde_iqemo",
    comment="Incrementally updated iQEMO data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "TreatmentCycleID"
    }
)

dlt.apply_changes(
    target = "rde_iqemo",
    source = "iqemo_update",
    keys = ["PERSON_ID", "TreatmentCycleID", "Indication", "Name"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------





@dlt.table(name="rde_powerforms_incr", table_properties={
        "pipelines.autoOptimize.zOrderCols": "ENCNTR_ID",
        "skipChangeCommits": "true"
    }, temporary=True)
def powerforms_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_powerforms")

    # Filter clinical_event based on max_adc_updt
#    filtered_clinical_event = (
#        spark.table("4_prod.raw.mill_dir_clinical_event")
#        .filter(F.col("ADC_UPDT") > max_adc_updt)
#        .filter(F.col("VALID_UNTIL_DT_TM") > F.current_timestamp())
#    )

    filtered_clinical_event = (
    spark.table("4_prod.raw.mill_dir_clinical_event")
    .filter((F.year(F.col("UPDT_DT_TM")) > 1900) & (F.year(F.col("UPDT_DT_TM")) < 2009))
    .filter(F.col("VALID_UNTIL_DT_TM") > F.current_timestamp())
    )

    # FormTemp
    form_temp = (
        spark.table("4_prod.raw.mill_dir_dcp_forms_activity")
        .join(
            spark.table("4_prod.raw.mill_dir_dcp_forms_ref"),
            "DCP_FORMS_REF_ID"
        )
        .join(
            filtered_clinical_event,
            (F.col("mill_dir_dcp_forms_activity.ENCNTR_ID") == filtered_clinical_event.ENCNTR_ID) &
            (F.col("mill_dir_dcp_forms_activity.DCP_FORMS_ACTIVITY_ID").cast("integer") == 
             F.when(filtered_clinical_event.REFERENCE_NBR.contains("!"),
                    F.split(filtered_clinical_event.REFERENCE_NBR, "!").getItem(0).cast("float").cast("integer"))
             .otherwise(filtered_clinical_event.REFERENCE_NBR.cast("float").cast("integer"))),
            "left_semi"
        )
        .select(
            F.col("mill_dir_dcp_forms_activity.DCP_FORMS_ACTIVITY_ID"),
            F.col("mill_dir_dcp_forms_activity.ENCNTR_ID"),
            F.col("mill_dir_dcp_forms_activity.FORM_STATUS_CD"),
            F.col("mill_dir_dcp_forms_ref.DCP_FORM_INSTANCE_ID"),
            F.greatest(F.col("mill_dir_dcp_forms_activity.ADC_UPDT"), F.col("mill_dir_dcp_forms_ref.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

    # DocResponseTemp
    doc_response_temp = (
        filtered_clinical_event.alias("CE")
        .join(form_temp, 
            (F.col("CE.ENCNTR_ID") == form_temp.ENCNTR_ID) &
            (F.when(F.col("CE.REFERENCE_NBR").contains("!"),
                    F.split(F.col("CE.REFERENCE_NBR"), "!").getItem(0).cast("float").cast("integer"))
             .otherwise(F.col("CE.REFERENCE_NBR").cast("float").cast("integer"))
             == form_temp.DCP_FORMS_ACTIVITY_ID.cast("integer")),
            "inner"
        )
        .join(spark.table("4_prod.raw.mill_dir_ce_date_result").alias("CEDR"), 
            F.col("CE.EVENT_ID") == F.col("CEDR.EVENT_ID"), "left")
        .join(spark.table("4_prod.raw.mill_dir_ce_string_result").alias("CESR"), 
            F.col("CE.EVENT_ID") == F.col("CESR.EVENT_ID"), "left")
        .join(spark.table("4_prod.raw.mill_dir_ce_coded_result").alias("CECR"), 
            F.col("CE.EVENT_ID") == F.col("CECR.EVENT_ID"), "left")
        .join(spark.table("3_lookup.dwh.mill_dir_code_value").alias("CV"), 
            (F.col("CECR.RESULT_CD") == F.col("CV.CODE_VALUE")) & (F.col("CV.ACTIVE_IND") == 1), "left")
        .join(filtered_clinical_event.alias("CE1"), 
            (F.col("CE.PARENT_EVENT_ID") == F.col("CE1.EVENT_ID")), "left")
        .filter(F.col("CE.TASK_ASSAY_CD") != 0)
        .select(
            F.col("CE.EVENT_ID").alias("DOC_RESPONSE_KEY"),
            F.col("CE.ENCNTR_ID"),
            F.col("CE.PERFORMED_DT_TM"),
            F.col("CE.EVENT_ID").alias("ELEMENT_EVENT_ID"),
            F.col("CE.PARENT_EVENT_ID").alias("SECTION_EVENT_ID"),
            F.col("CE.EVENT_ID").alias("FORM_EVENT_ID"),
            form_temp.DCP_FORM_INSTANCE_ID.alias("DOC_INPUT_ID"),
            form_temp.FORM_STATUS_CD,
            F.coalesce(F.col("CEDR.RESULT_DT_TM"), F.col("CESR.STRING_RESULT_TEXT"), F.col("CECR.DESCRIPTOR"), F.col("CV.DISPLAY")).alias("RESPONSE_VALUE_TXT"),
            F.when(F.col("CE1.EVENT_TITLE_TEXT").isin("TRACKING CONTROL", "Discrete Grid"), F.col("CE1.EVENT_ID")).otherwise("0").alias("GRID_EVENT_ID"),
            F.greatest(F.col("CE.ADC_UPDT"), form_temp.ADC_UPDT, F.coalesce(F.col("CEDR.ADC_UPDT"), F.col("CESR.ADC_UPDT"), F.col("CECR.ADC_UPDT"), F.col("CV.ADC_UPDT"), F.col("CE1.ADC_UPDT"))).alias("ADC_UPDT")
        )
    )

    # DocRefTemp
    doc_ref_temp = (
        spark.table("4_prod.raw.mill_dir_dcp_forms_ref").alias("FREF")
        .join(spark.table("4_prod.raw.mill_dir_dcp_forms_def").alias("FDEF"), 
            F.col("FREF.DCP_FORM_INSTANCE_ID") == F.col("FDEF.DCP_FORM_INSTANCE_ID"))
        .join(spark.table("4_prod.raw.mill_dir_dcp_section_ref").alias("SREF"), 
            F.col("FDEF.DCP_SECTION_REF_ID") == F.col("SREF.DCP_SECTION_REF_ID"))
        .join(spark.table("4_prod.raw.mill_dir_dcp_input_ref").alias("DIREF"), 
            F.col("SREF.DCP_SECTION_REF_ID") == F.col("DIREF.DCP_SECTION_REF_ID"))
        .join(spark.table("4_prod.raw.mill_dir_name_value_prefs").alias("NVP"), 
            (F.col("DIREF.DCP_INPUT_REF_ID") == F.col("NVP.PARENT_ENTITY_ID")) &
            (F.col("NVP.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
            (F.col("NVP.PVC_NAME").like("discrete_task_assay%")), "left")
        .join(spark.table("4_prod.raw.mill_dir_discrete_task_assay").alias("DTA"), 
            F.col("NVP.MERGE_ID") == F.col("DTA.TASK_ASSAY_CD"), "left")
        .join(spark.table("4_prod.raw.mill_dir_name_value_prefs").alias("NVP2"), 
            (F.col("DIREF.DCP_INPUT_REF_ID") == F.col("NVP2.PARENT_ENTITY_ID")) &
            (F.col("NVP2.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
            (F.col("NVP2.PVC_NAME") == "grid_event_cd"), "left")
        .join(spark.table("3_lookup.dwh.mill_dir_code_value").alias("CV"), 
            (F.col("NVP2.MERGE_ID") == F.col("CV.CODE_VALUE")) & (F.col("CV.ACTIVE_IND") == 1), "left")
        .select(
            F.col("FREF.DCP_FORM_INSTANCE_ID").alias("DOC_INPUT_KEY"),
            F.col("FREF.DESCRIPTION").alias("FORM_DESC_TXT"),
            F.col("SREF.DESCRIPTION").alias("SECTION_DESC_TXT"),
            F.col("DTA.MNEMONIC").alias("ELEMENT_LABEL_TXT"),
            F.col("CV.DISPLAY").alias("GRID_NAME_TXT"),
            F.col("DTA.DESCRIPTION").alias("GRID_COLUMN_DESC_TXT"),
            F.greatest(F.col("FREF.ADC_UPDT"), F.col("FDEF.ADC_UPDT"), F.col("SREF.ADC_UPDT"), F.col("DIREF.ADC_UPDT"), F.col("NVP.ADC_UPDT"), F.col("DTA.ADC_UPDT"), F.col("NVP2.ADC_UPDT"), F.col("CV.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

    return (
        doc_response_temp.alias("DRT")
        .join(dlt.read("rde_encounter").alias("ENC"), 
            doc_response_temp.ENCNTR_ID == F.col("ENC.ENCNTR_ID"))
        .join(doc_ref_temp.alias("FREF"), 
            doc_response_temp.DOC_INPUT_ID == doc_ref_temp.DOC_INPUT_KEY, "left")
        .join(spark.table("3_lookup.dwh.mill_dir_code_value").alias("CV"), 
            doc_response_temp.FORM_STATUS_CD == F.col("CV.CODE_VALUE"), "left")
        .select(
            F.col("ENC.NHS_Number").cast("string").alias("NHS_Number"),
            F.col("ENC.MRN").cast("string").alias("MRN"),
            F.col("ENC.PERSON_ID").cast(StringType()).alias("PERSON_ID"),
            F.col("ENC.ENCNTR_ID").cast("string").alias("ENCNTR_ID"), 
            F.date_format(F.col("PERFORMED_DT_TM"), "yyyy-MM-dd HH:mm").alias("PerformDate"),
            F.col("DOC_RESPONSE_KEY").cast("string").alias("DOC_RESPONSE_KEY"),
            F.col("FORM_DESC_TXT").cast("string").alias("Form"),
            F.col("FORM_EVENT_ID").cast("long").alias("FormID"),
            F.col("SECTION_DESC_TXT").cast("string").alias("Section"),
            F.col("SECTION_EVENT_ID").cast("long").alias("SectionID"),
            F.col("ELEMENT_LABEL_TXT").cast("string").alias("Element"),
            F.col("ELEMENT_EVENT_ID").cast("long").alias("ElementID"),
            F.col("GRID_NAME_TXT").cast("string").alias("Component"),
            F.col("GRID_COLUMN_DESC_TXT").cast("string").alias("ComponentDesc"),
            F.col("GRID_EVENT_ID").cast("long").alias("ComponentID"),
            F.col("RESPONSE_VALUE_TXT").cast("string").alias("Response"),
            F.when(F.col("RESPONSE_VALUE_TXT").rlike("^[0-9.]+$"), F.lit(1)).otherwise(F.lit(0)).cast("boolean").alias("ResponseNumeric"),
            F.col("CV.DESCRIPTION").cast("string").alias("Status"),
            F.greatest(
                F.col("ENC.ADC_UPDT"),
                F.col("DRT.ADC_UPDT"),
                F.col("FREF.ADC_UPDT"),
                F.col("CV.ADC_UPDT")).alias("ADC_UPDT")
        )
        #.filter(F.col("ADC_UPDT") > max_adc_updt)
    )

@dlt.view(name="powerforms_update")
def powerforms_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_powerforms_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_powerforms",
    comment="Incrementally updated powerforms data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "ENCNTR_ID,DOC_RESPONSE_KEY"
    }
)

dlt.apply_changes(
    target = "rde_powerforms",
    source = "powerforms_update",
    keys = ["ENCNTR_ID", "DOC_RESPONSE_KEY"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(name="rde_radiology_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def radiology_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_radiology")

    clinical_event = spark.table("4_prod.raw.mill_dir_clinical_event")
    orders = spark.table("4_prod.raw.mill_dir_orders").alias("ORD")
    code_value_ref = spark.table("3_lookup.dwh.pi_cde_code_value_ref")
    blob_content = spark.table("4_prod.raw.pi_cde_blob_content").alias("B")
    encounter = dlt.read("rde_encounter").alias("ENC")
    nhsi_exam_mapping = spark.table("4_prod.raw.tbl_nhsi_exam_mapping").alias("M")

    clinical_event_final = clinical_event.filter(F.col("VALID_UNTIL_DT_TM") > F.current_timestamp()).filter(col("ADC_UPDT") > max_adc_updt).alias("EVE")

    joined_data = (
        clinical_event_final
        .join(encounter, col("EVE.ENCNTR_ID") == encounter.ENCNTR_ID)
        .join(orders, (encounter.ENCNTR_ID == orders.ENCNTR_ID) & (orders.ORDER_ID == col("EVE.ORDER_ID")))
        .join(code_value_ref.alias("R"), col("EVE.RECORD_STATUS_CD") == col("R.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("EC"), col("EVE.ENTRY_MODE_CD") == col("EC.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ER"), col("EVE.RESULT_STATUS_CD") == col("ER.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ECLASS"), col("EVE.EVENT_CLASS_CD") == col("ECLASS.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("CD"), col("EVE.EVENT_CD") == col("CD.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("LO"), orders.ORDER_STATUS_CD == col("LO.CODE_VALUE_CD"), "left")
        .join(blob_content, col("EVE.EVENT_ID") == blob_content.EVENT_ID, "left")
        .join(nhsi_exam_mapping, 
              (col("EVE.EVENT_TITLE_TEXT") == col("M.ExaminationTypeName")) | 
              (col("EVE.EVENT_TAG") == col("M.ExaminationTypeName")), "left")
    )

    return (
        joined_data
        .filter(
            (col("EVE.CONTRIBUTOR_SYSTEM_CD") == '6141416') & 
            (col("EVE.CONTRIBUTOR_SYSTEM_CD").isNotNull())
        )
        .select(
            encounter.PERSON_ID.cast(StringType()).alias("PERSON_ID"),
            encounter.MRN.cast(StringType()).alias("MRN"),
            encounter.ENCNTR_ID.cast(StringType()).alias("ENCNTR_ID"),
            encounter.NHS_Number.cast(StringType()).alias("NHS_Number"),
            encounter.ENC_TYPE.cast(StringType()).alias("Acitvity_Type"),
            encounter.TREATMENT_FUNCTION_CD.cast(StringType()).alias("TFCode"),
            encounter.TFC_DESC.cast(StringType()).alias("TFCdesc"),
            orders.ORDER_MNEMONIC.cast(StringType()).alias("ExamName"),
            col("CD.CODE_DESC_TXT").cast(StringType()).alias("EventName"),
            col("EVE.EVENT_TAG").cast(StringType()).alias("EVENT_TAG_TXT"),
            when(col("EVE.RESULT_VAL").cast("double").isNull(), lit(0)).otherwise(lit(1)).alias("ResultNumeric"),
            col("EVE.EVENT_START_DT_TM").cast(StringType()).alias("ExamStart"),
            col("EVE.EVENT_END_DT_TM").cast(StringType()).alias("ExamEnd"),
            col("B.BLOB_CONTENTS").cast(StringType()).alias("ReportText"),
            col("LO.CODE_DESC_TXT").cast(StringType()).alias("LastOrderStatus"),
            col("R.CODE_DESC_TXT").cast(StringType()).alias("RecordStatus"),
            col("ER.CODE_DESC_TXT").cast(StringType()).alias("ResultStatus"),
            col("M.ExaminationTypecode").cast(StringType()).alias("ExaminationTypecode"),
            col("M.EX_Modality").cast(StringType()).alias("Modality"),
            col("M.EX_Sub_Modality").cast(StringType()).alias("SubModality"),
            col("M.ExaminationTypeName").cast(StringType()).alias("ExaminationTypeName"),
            col("EVE.EVENT_ID").cast(StringType()).alias("EventID"),
            greatest(col("EVE.ADC_UPDT"), orders.ADC_UPDT, encounter.ADC_UPDT, col("B.ADC_UPDT"), col("M.ADC_UPDT")).alias("ADC_UPDT")
        )
        .filter(col("ADC_UPDT") > max_adc_updt)
    )

@dlt.view(name="radiology_update")
def radiology_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_radiology_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_radiology",
    comment="Incrementally updated radiology data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "ENCNTR_ID,EventID"
    }
)

dlt.apply_changes(
    target = "rde_radiology",
    source = "radiology_update",
    keys = ["ENCNTR_ID", "EventID", "ExamName"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(name="rde_family_history_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def family_history_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_family_history")

    family_history = spark.table("4_prod.raw.pi_dir_family_history_activity").alias("F")
    patient_demographics = dlt.read("rde_patient_demographics").alias("E")
    person_patient_person_reltn = spark.table("4_prod.raw.mill_dir_person_person_reltn").alias("REL")
    nomenclature_ref = spark.table("3_lookup.dwh.mill_dir_nomenclature").alias("R")
    code_value_ref = spark.table("3_lookup.dwh.pi_cde_code_value_ref")

    return (
        family_history.filter(col("ADC_UPDT") > max_adc_updt)
        .join(patient_demographics, col("F.PERSON_ID") == col("E.PERSON_ID"), "inner")
        .join(person_patient_person_reltn, col("F.RELATED_PERSON_ID") == col("REL.RELATED_PERSON_ID"), "left")
        .join(nomenclature_ref, col("F.ACTIVITY_NOMEN") == col("R.NOMENCLATURE_ID"), "left")
        .join(code_value_ref.alias("REF"), col("REL.PERSON_RELTN_CD") == col("REF.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("RELTYPE"), col("REL.PERSON_RELTN_TYPE_CD") == col("RELTYPE.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("VOCAB"), col("R.SOURCE_VOCABULARY_CD") == col("VOCAB.CODE_VALUE_CD"), "left")
        .select(
            col("F.PERSON_ID").cast(StringType()).alias("PERSON_ID"),
            col("E.MRN").cast(StringType()).alias("MRN"),
            col("E.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("REL.PERSON_RELTN_CD").cast(StringType()).alias("RELATION_CD"),
            col("REF.CODE_DESC_TXT").cast(StringType()).alias("RelationDesc"),
            col("REL.PERSON_RELTN_TYPE_CD").cast(StringType()).alias("RELATION_TYPE"),
            col("RELTYPE.CODE_DESC_TXT").cast(StringType()).alias("RelationType"),
            col("F.ACTIVITY_NOMEN").cast(StringType()).alias("ACTIVITY_NOMEN"),
            col("R.SOURCE_STRING").cast(StringType()).alias("NomenDesc"),
            col("R.SOURCE_IDENTIFIER").cast(StringType()).alias("NomenVal"),
            col("R.SOURCE_VOCABULARY_CD").cast(StringType()).alias("VOCABULARY_CD"),
            col("VOCAB.CODE_DESC_TXT").cast(StringType()).alias("VocabDesc"),
            col("F.TYPE_MEAN").cast(StringType()).alias("TYPE"),
            col("REL.BEG_EFFECTIVE_DT_TM").cast(StringType()).alias("BegEffectDate"),
            col("REL.END_EFFECTIVE_DT_TM").cast(StringType()).alias("EndEffectDate"),
            col("F.FHX_VALUE_FLG").cast(StringType()).alias("FHX_VALUE_FLG"),
            col("REL.PERSON_PERSON_RELTN_ID").cast(StringType()).alias("REL_ID"),
            greatest(col("F.ADC_UPDT"), col("E.ADC_UPDT"), col("REL.ADC_UPDT"), col("R.ADC_UPDT")).alias("ADC_UPDT")
        )
        .filter(col("ADC_UPDT") > max_adc_updt)
    )

@dlt.view(name="family_history_update")
def family_history_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_family_history_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_family_history",
    comment="Incrementally updated family history data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID,RELATION_CD"
    }
)

dlt.apply_changes(
    target = "rde_family_history",
    source = "family_history_update",
    keys = ["PERSON_ID", "REL_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(name="rde_blobdataset_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def blobdataset_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_blobdataset")

    blob_content = spark.table("4_prod.raw.pi_cde_blob_content").alias("B")
    clinical_event = spark.table("4_prod.raw.mill_dir_clinical_event").filter(F.col("VALID_UNTIL_DT_TM") > F.current_timestamp()).alias("CE")
    encounter = dlt.read("rde_encounter").alias("E")
    code_value_ref = spark.table("3_lookup.dwh.pi_cde_code_value_ref")

    return (
        blob_content.filter(col("ADC_UPDT") > max_adc_updt)
        .join(clinical_event, col("B.EVENT_ID") == col("CE.EVENT_ID"), "inner")
        .join(encounter, col("CE.ENCNTR_ID") == col("E.ENCNTR_ID"), "inner")
        .join(clinical_event.alias("CE2"), col("CE.PARENT_EVENT_ID") == col("CE2.EVENT_ID"), "left")
        .join(code_value_ref.alias("PEvent"), col("CE2.EVENT_CD") == col("PEvent.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("Evntcd"), col("CE.EVENT_CD") == col("Evntcd.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("EReltn"), col("CE.EVENT_RELTN_CD") == col("EReltn.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("RecStat"), col("CE.RECORD_STATUS_CD") == col("RecStat.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ConSys"), col("CE.CONTRIBUTOR_SYSTEM_CD") == col("ConSys.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("EvntCls"), col("CE.EVENT_CLASS_CD") == col("EvntCls.CODE_VALUE_CD"), "left")
        .select(
            col("E.PERSON_ID").cast(StringType()).alias("PERSON_ID"),
            col("E.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("E.MRN").cast(StringType()).alias("MRN"),
            col("CE.CLINSIG_UPDT_DT_TM").cast(StringType()).alias("ClinicalSignificantDate"),
            col("PEvent.CODE_DESC_TXT").cast(StringType()).alias("MainEventDesc"),
            col("CE2.EVENT_TITLE_TEXT").cast(StringType()).alias("MainTitleText"),
            col("CE2.EVENT_TAG").cast(StringType()).alias("MainTagText"),
            col("CE.EVENT_TITLE_TEXT").cast(StringType()).alias("ChildEvent"),
            col("CE.EVENT_TAG").cast(StringType()).alias("ChildTagText"),
            col("B.BLOB_CONTENTS").cast(StringType()).alias("BlobContents"),
            col("Evntcd.CODE_DISP_TXT").cast(StringType()).alias("EventDesc"),
            col("CE.RESULT_VAL").cast(StringType()).alias("EventResultText"),
            col("CE.RESULT_VAL").cast(DoubleType()).alias("EventResultNBR"),
            col("EReltn.CODE_DESC_TXT").cast(StringType()).alias("EventReltnDesc"),
            col("RecStat.CODE_DESC_TXT").cast(StringType()).alias("Status"),
            col("ConSys.CODE_DESC_TXT").cast(StringType()).alias("SourceSys"),
            col("EvntCls.CODE_DESC_TXT").cast(StringType()).alias("ClassDesc"),
            col("CE.PARENT_EVENT_ID").cast(StringType()).alias("ParentEventID"),
            col("CE.EVENT_ID").cast(StringType()).alias("EventID"),
            greatest(col("B.ADC_UPDT"), col("CE.ADC_UPDT"), col("E.ADC_UPDT"), col("CE2.ADC_UPDT")).alias("ADC_UPDT")
        )
        .filter(col("ADC_UPDT") > max_adc_updt)
        .filter(col("BlobContents").isNotNull())
        .filter(col("NHS_Number").isNotNull())
    )

@dlt.view(name="blobdataset_update")
def blobdataset_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_blobdataset_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_blobdataset",
    comment="Incrementally updated BLOB dataset",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID,EventID"
    }
)

dlt.apply_changes(
    target = "rde_blobdataset",
    source = "blobdataset_update",
    keys = ["PERSON_ID", "EventID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(name="rde_pc_procedures_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def pc_procedures_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_pc_procedures")

    pc_procedures = spark.table("4_prod.raw.pc_procedures").alias("PCProc")
    patient_demographics = dlt.read("rde_patient_demographics").alias("E")
    person_alias = spark.table("4_prod.raw.mill_dir_person_alias").alias("PA")

    return (
        pc_procedures.filter(col("ADC_UPDT") > max_adc_updt)
        .join(person_alias, (pc_procedures.MRN == person_alias.ALIAS) & (person_alias.PERSON_ALIAS_TYPE_CD == 10), "inner")
        .join(patient_demographics, person_alias.PERSON_ID == patient_demographics.PERSON_ID, "inner")
        .select(
            col("PA.PERSON_ID").cast(StringType()).alias("PERSON_ID"),
            col("PCProc.MRN").cast(StringType()).alias("MRN"),
            col("E.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("PCProc.Admit_Dt_Tm").cast(StringType()).alias("AdmissionDT"),
            col("PCProc.Disch_Dt_Tm").cast(StringType()).alias("DischargeDT"),
            col("PCProc.Trtmt_Func").cast(StringType()).alias("TreatmentFunc"),
            col("PCProc.Specialty").cast(StringType()).alias("Specialty"),
            col("PCProc.Proc_Dt_Tm").cast(StringType()).alias("ProcDt"),
            col("PCProc.Proc_Txt").cast(StringType()).alias("ProcDetails"),
            col("PCProc.Proc_Cd").cast(StringType()).alias("ProcCD"),
            col("PCProc.Proc_Cd_Type").cast(StringType()).alias("ProcType"),
            col("PCProc.Encounter_Type").cast(StringType()).alias("EncType"),
            col("PCProc.Comment").cast(StringType()).alias("Comment"),
            greatest(col("PCProc.ADC_UPDT"), col("E.ADC_UPDT"), col("PA.ADC_UPDT")).alias("ADC_UPDT")
        )
        .filter(col("ADC_UPDT") > max_adc_updt)
    )

@dlt.view(name="pc_procedures_update")
def pc_procedures_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_pc_procedures_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_pc_procedures",
    comment="Incrementally updated PC procedures data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID,ProcCD"
    }
)

dlt.apply_changes(
    target = "rde_pc_procedures",
    source = "pc_procedures_update",
    keys = ["PERSON_ID", "ProcDt", "ProcCD"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(name="rde_all_procedures_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def all_procedures_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_all_procedures")

    mill_dir_procedure = spark.table("4_prod.raw.mill_dir_procedure").alias("mil")
    encounter = dlt.read("rde_encounter").alias("E")
    mill_dir_nomenclature = spark.table("3_lookup.dwh.mill_dir_nomenclature").alias("nom")

    return (
        mill_dir_procedure.filter(col("ADC_UPDT") > max_adc_updt)
        .join(encounter, col("mil.ENCNTR_id") == col("E.ENCNTR_ID"), "inner")
        .join(mill_dir_nomenclature, col("mil.NOMENCLATURE_ID") == col("nom.NOMENCLATURE_ID"), "left")
        .select(
            col("E.MRN").cast(StringType()).alias("MRN"),
            col("E.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("E.PERSON_ID").cast(StringType()).alias("Person_ID"),
            col("nom.SOURCE_IDENTIFIER").cast(StringType()).alias("Procedure_code"),
            split(col("nom.concept_cki"), "!").getItem(0).cast(StringType()).alias("Catalogue"),
            col("nom.source_string").cast(StringType()).alias("Code_text"),
            col("mil.PROCEDURE_NOTE").cast(StringType()).alias("Procedure_note"),
            col("mil.PROCEDURE_ID").cast(StringType()).alias("Procedure_ID"),
            coalesce(col("mil.PROC_Dt_Tm"), col("mil.ACTIVE_STATUS_DT_TM")).cast(StringType()).alias("Procedure_date"),
            greatest(col("mil.ADC_UPDT"), col("E.ADC_UPDT"), col("nom.ADC_UPDT")).alias("ADC_UPDT")
        )
        .distinct()
    )

@dlt.view(name="all_procedures_update")
def all_procedures_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_all_procedures_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_all_procedures",
    comment="Incrementally updated all procedures data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "Person_ID,Procedure_code"
    }
)

dlt.apply_changes(
    target = "rde_all_procedures",
    source = "all_procedures_update",
    keys = ["Person_ID", "Procedure_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(name="rde_pc_diagnosis_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def pc_diagnosis_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_pc_diagnosis")

    pc_diagnoses = spark.table("4_prod.raw.pc_diagnoses").alias("PR")
    patient_demographics = dlt.read("rde_patient_demographics").alias("E")
    person_alias = spark.table("4_prod.raw.mill_dir_person_alias").alias("PA")

    return (
        pc_diagnoses.filter(col("ADC_UPDT") > max_adc_updt)
        .join(person_alias, (pc_diagnoses.MRN == person_alias.ALIAS) & (person_alias.PERSON_ALIAS_TYPE_CD == 10), "inner")
        .join(patient_demographics, person_alias.PERSON_ID == patient_demographics.PERSON_ID, "inner")
        .select(
            col("PR.Diagnosis_Id").cast(StringType()).alias("DiagID"),
            col("PA.PERSON_ID").cast(StringType()).alias("Person_ID"),
            col("E.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("PR.MRN").cast(StringType()).alias("MRN"),
            col("PR.Diagnosis").cast(StringType()).alias("Diagnosis"),
            col("PR.Confirmation").cast(StringType()).alias("Confirmation"),
            col("PR.Diag_Dt").cast(StringType()).alias("DiagDt"),
            col("PR.Classification").cast(StringType()).alias("Classification"),
            col("PR.Clin_Service").cast(StringType()).alias("ClinService"),
            col("PR.Diag_Type").cast(StringType()).alias("DiagType"),
            col("PR.Diag_Code").cast(StringType()).alias("DiagCode"),
            col("PR.Vocab").cast(StringType()).alias("Vocab"),
            col("PR.Axis").cast(StringType()).alias("Axis"),
            greatest(col("PR.ADC_UPDT"), col("E.ADC_UPDT"), col("PA.ADC_UPDT")).alias("ADC_UPDT")
        )
        .filter(col("ADC_UPDT") > max_adc_updt)
    )

@dlt.view(name="pc_diagnosis_update")
def pc_diagnosis_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_pc_diagnosis_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_pc_diagnosis",
    comment="Incrementally updated PC diagnosis data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "Person_ID,DiagID"
    }
)

dlt.apply_changes(
    target = "rde_pc_diagnosis",
    source = "pc_diagnosis_update",
    keys = ["Person_ID", "DiagID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(name="rde_pc_problems_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def pc_problems_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_pc_problems")

    pc_problems = spark.table("4_prod.raw.pc_problems").alias("PCP")
    patient_demographics = dlt.read("rde_patient_demographics").alias("E")
    person_alias = spark.table("4_prod.raw.mill_dir_person_alias").alias("PA")

    return (
        pc_problems.filter(col("ADC_UPDT") > max_adc_updt)
        .join(person_alias, (pc_problems.MRN == person_alias.ALIAS) & (person_alias.PERSON_ALIAS_TYPE_CD == 10), "inner")
        .join(patient_demographics, person_alias.PERSON_ID == patient_demographics.PERSON_ID, "inner")
        .select(
            col("PCP.Problem_Id").cast(StringType()).alias("ProbID"),
            col("PA.PERSON_ID").cast(StringType()).alias("Person_ID"),
            col("PCP.MRN").cast(StringType()).alias("MRN"),
            col("E.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("PCP.Problem").cast(StringType()).alias("Problem"),
            col("PCP.Annotated_Disp").cast(StringType()).alias("Annot_Disp"),
            col("PCP.Confirmation").cast(StringType()).alias("Confirmation"),
            col("PCP.Classification").cast(StringType()).alias("Classification"),
            col("PCP.Onset_Date").cast(StringType()).alias("OnsetDate"),
            col("PCP.Status_Date").cast(StringType()).alias("StatusDate"),
            col("PCP.Status_Lifecycle").cast(StringType()).alias("Stat_LifeCycle"),
            col("PCP.Lifecycle_Cancelled_Rsn").cast(StringType()).alias("LifeCycleCancReson"),
            col("PCP.Vocab").cast(StringType()).alias("Vocab"),
            col("PCP.Axis").cast(StringType()).alias("Axis"),
            col("PCP.Secondary_Descriptions").cast(StringType()).alias("SecDesc"),
            col("PCP.Problem_Code").cast(StringType()).alias("ProbCode"),
            greatest(col("PCP.ADC_UPDT"), col("E.ADC_UPDT"), col("PA.ADC_UPDT")).alias("ADC_UPDT")
        )
        .filter(col("ADC_UPDT") > max_adc_updt)
    )

@dlt.view(name="pc_problems_update")
def pc_problems_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_pc_problems_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_pc_problems",
    comment="Incrementally updated PC problems data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "Person_ID,ProbID"
    }
)

dlt.apply_changes(
    target = "rde_pc_problems",
    source = "pc_problems_update",
    keys = ["Person_ID", "ProbID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(name="rde_msds_booking_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def msds_booking_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_msds_booking")

    mat_pregnancy = spark.table("4_prod.raw.mat_pregnancy").alias("PREG")
    patient_demographics = dlt.read("rde_patient_demographics").alias("DEM")
    msd101pregbook = spark.table("4_prod.raw.msds101pregbook").alias("MSDS")
    person_patient_address = spark.table("4_prod.raw.mill_dir_address").filter((col("PARENT_ENTITY_NAME") == "PERSON")).alias("ADDR")

    # Filter each table separately
    filtered_mat_pregnancy = mat_pregnancy.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_msd101pregbook = msd101pregbook.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_person_patient_address = person_patient_address.filter(col("ADC_UPDT") > max_adc_updt)

    # Get relevant pregnancy IDs
    relevant_pregnancy_ids = filtered_mat_pregnancy.select("PREGNANCY_ID").union(filtered_msd101pregbook.select("PREGNANCYID")).distinct()

    # Get person IDs associated with relevant pregnancy IDs
    relevant_person_ids_from_pregnancy = mat_pregnancy.join(relevant_pregnancy_ids, "PREGNANCY_ID").select("PERSON_ID")

    # Combine with person IDs from address changes
    all_relevant_person_ids = relevant_person_ids_from_pregnancy.union(filtered_person_patient_address.select("PARENT_ENTITY_ID")).distinct()

    return (
        mat_pregnancy
        .join(all_relevant_person_ids, mat_pregnancy.PERSON_ID == all_relevant_person_ids.PERSON_ID, "inner")
        .join(patient_demographics, col("PREG.PERSON_ID") == col("DEM.PERSON_ID"), "inner")
        .join(msd101pregbook, col("PREG.PREGNANCY_ID") == col("MSDS.PREGNANCYID"), "left")
        .join(person_patient_address, col("PREG.PERSON_ID") == col("ADDR.PARENT_ENTITY_ID"), "left")
        .select(
            col("PREG.PERSON_ID").cast(StringType()).alias("PERSON_ID"),
            col("PREG.PREGNANCY_ID").cast(StringType()).alias("PregnancyID"),
            col("DEM.MRN").cast(StringType()).alias("MRN"),
            col("DEM.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("PREG.FIRST_ANTENATAL_ASSESSMENT_DT_TM").cast(StringType()).alias("FirstAntenatalAPPTDate"),
            col("PREG.ALCOHOL_USE_NBR").cast(IntegerType()).alias("AlcoholUnitsPerWeek"),
            col("PREG.SMOKE_BOOKING_DESC").cast(StringType()).alias("SmokingStatusBooking"),
            col("PREG.SMOKING_STATUS_DEL_DESC").cast(StringType()).alias("SmokingStatusDelivery"),
            col("PREG.REC_SUB_USE_DESC").cast(StringType()).alias("SubstanceUse"),
            col("PREG.ROM_DT_TM").cast(StringType()).alias("DeliveryDate"),
            col("ADDR.ZIPCODE").cast(StringType()).alias("PostCode"),
            col("PREG.HT_BOOKING_CM").cast(FloatType()).alias("Height_CM"),
            col("PREG.WT_BOOKING_KG").cast(FloatType()).alias("Weight_KG"),
            col("PREG.BMI_BOOKING_DESC").cast(FloatType()).alias("BMI"),
            col("PREG.LAB_ONSET_METHOD_DESC").cast(StringType()).alias("LaborOnsetMethod"),
            col("PREG.AUGMENTATION_DESC").cast(StringType()).alias("Augmentation"),
            col("PREG.ANALGESIA_DEL_DESC").cast(StringType()).alias("AnalgesiaDelivery"),
            col("PREG.ANALGESIA_LAB_DESC").cast(StringType()).alias("AnalgesiaLabour"),
            col("PREG.ANAESTHESIA_DEL_DESC").cast(StringType()).alias("AnaesthesiaDelivery"),
            col("PREG.ANAESTHESIA_LAB_DESC").cast(StringType()).alias("AnaesthesiaLabour"),
            col("PREG.PERINEAL_TRAUMA_DESC").cast(StringType()).alias("PerinealTrauma"),
            col("PREG.EPISIOTOMY_DESC").cast(StringType()).alias("EpisiotomyDesc"),
            col("PREG.TOTAL_BLOOD_LOSS").cast(FloatType()).alias("BloodLoss"),
            col("MSDS.ANTENATALAPPDATE").cast(StringType()).alias("MSDS_AntenatalAPPTDate"),
            col("MSDS.COMPLEXSOCIALFACTORSIND").cast(StringType()).alias("MSDS_CompSocialFactor"),
            col("MSDS.DISABILITYINDMOTHER").cast(StringType()).alias("MSDS_DisabilityMother"),
            col("MSDS.DISCHARGEDATEMATSERVICE").cast(StringType()).alias("MSDS_MatDischargeDate"),
            col("MSDS.DISCHREASON").cast(StringType()).alias("MSDS_DischReason"),
            col("MSDS.EDDAGREED").cast(StringType()).alias("MSDS_EST_DELIVERYDATE_AGREED"),
            col("MSDS.EDDMETHOD").cast(StringType()).alias("MSDS_METH_OF_EST_DELIVERY_DATE_AGREED"),
            col("MSDS.FOLICACIDSUPPLEMENT").cast(StringType()).alias("MSDS_FolicAcidSupplement"),
            col("MSDS.LASTMENSTRUALPERIODDATE").cast(StringType()).alias("MSDS_LastMensturalPeriodDate"),
            col("MSDS.PREGFIRSTCONDATE").cast(StringType()).alias("MSDS_PregConfirmed"),
            col("MSDS.PREVIOUSCAESAREANSECTIONS").cast(StringType()).alias("MSDS_PrevC_Sections"),
            col("MSDS.PREVIOUSLIVEBIRTHS").cast(StringType()).alias("MSDS_PrevLiveBirths"),
            col("MSDS.PREVIOUSLOSSESLESSTHAN24WEEKS").cast(StringType()).alias("MSDS_PrevLossesLessThan24Weeks"),
            col("MSDS.PREVIOUSSTILLBIRTHS").cast(StringType()).alias("MSDS_PrevStillBirths"),
            col("MSDS.SUPPORTSTATUSINDMOTHER").cast(StringType()).alias("MSDS_MothSuppStatusIND"),
            greatest(col("PREG.ADC_UPDT"), col("DEM.ADC_UPDT"), col("MSDS.ADC_UPDT"), col("ADDR.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

# The rest of the code remains the same
@dlt.view(name="msds_booking_update")
def msds_booking_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_msds_booking_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_msds_booking",
    comment="Incrementally updated MSDS booking data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID,PregnancyID"
    }
)

dlt.apply_changes(
    target = "rde_msds_booking",
    source = "msds_booking_update",
    keys = ["PERSON_ID", "PregnancyID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(name="rde_msds_carecontact_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def msds_carecontact_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_msds_carecontact")

    msd201carecontactpreg = spark.table("4_prod.raw.msds201carecontactpreg").alias("CON")
    msds_booking = dlt.read("rde_msds_booking").alias("MB")

    # Filter the care contact table
    filtered_carecontact = msd201carecontactpreg.filter(col("ADC_UPDT") > max_adc_updt)

    # Get relevant pregnancy IDs
    relevant_pregnancy_ids = filtered_carecontact.select("PREGNANCYID").distinct()

    return (
        msd201carecontactpreg
        .join(relevant_pregnancy_ids, "PREGNANCYID")
        .join(msds_booking, col("CON.PREGNANCYID") == col("MB.PregnancyID"), "inner")
        .select(
            col("MB.PERSON_ID").cast(StringType()).alias("PERSON_ID"),
            col("MB.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("MB.MRN").cast(StringType()).alias("MRN"),
            col("CON.PREGNANCYID").cast(StringType()).alias("PregnancyID"),
            col("CON.CARECONID").cast(StringType()).alias("CareConID"),
            col("CON.CCONTACTDATETIME").cast(StringType()).alias("CareConDate"),
            col("CON.ADMINCATCODE").cast(StringType()).alias("AdminCode"),
            col("CON.CONTACTDURATION").cast(StringType()).alias("Duration"),
            col("CON.CONSULTTYPE").cast(StringType()).alias("ConsultType"),
            col("CON.CCSUBJECT").cast(StringType()).alias("Subject"),
            col("CON.MEDIUM").cast(StringType()).alias("Medium"),
            col("CON.GPTHERAPYIND").cast(StringType()).alias("GPTherapyIND"),
            col("CON.ATTENDCODE").cast(StringType()).alias("AttendCode"),
            col("CON.CANCELREASON").cast(StringType()).alias("CancelReason"),
            col("CON.CANCELDATE").cast(StringType()).alias("CancelDate"),
            col("CON.REPLAPPTOFFDATE").cast(StringType()).alias("RepAppOffDate"),
            greatest(col("CON.ADC_UPDT"), col("MB.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="msds_carecontact_update")
def msds_carecontact_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_msds_carecontact_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_msds_carecontact",
    comment="Incrementally updated MSDS care contact data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID,PregnancyID,CareConID"
    }
)

dlt.apply_changes(
    target = "rde_msds_carecontact",
    source = "msds_carecontact_update",
    keys = ["PERSON_ID", "PregnancyID", "CareConID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(name="rde_msds_delivery_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def msds_delivery_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_msds_delivery")

    mat_birth = spark.table("4_prod.raw.mat_birth").alias("BIRTH")
    mat_pregnancy = spark.table("4_prod.raw.mat_pregnancy").alias("MOTHER")
    patient_demographics = dlt.read("rde_patient_demographics").alias("DEM")
    msd301labdel = spark.table("4_prod.raw.msds301labdel").alias("MSDS")
    msd401babydemo = spark.table("4_prod.raw.msds401babydemo").alias("MSDBABY")

    # Filter each table separately
    filtered_mat_birth = mat_birth.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_mat_pregnancy = mat_pregnancy.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_msd301labdel = msd301labdel.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_msd401babydemo = msd401babydemo.filter(col("ADC_UPDT") > max_adc_updt)

    # Get relevant pregnancy IDs
    relevant_pregnancy_ids = (
        filtered_mat_birth.select("PREGNANCY_ID")
        .union(filtered_mat_pregnancy.select("PREGNANCY_ID"))
        .union(filtered_msd301labdel.select("PREGNANCYID"))
        .distinct()
    )

    # Get person IDs associated with relevant pregnancy IDs
    relevant_person_ids = mat_pregnancy.join(relevant_pregnancy_ids, "PREGNANCY_ID").select("PERSON_ID").distinct()

    return (
        mat_birth
        .join(relevant_pregnancy_ids, "PREGNANCY_ID", "inner")
        .join(mat_pregnancy, "PREGNANCY_ID", "left")
        .join(patient_demographics, col("MOTHER.PERSON_ID") == col("DEM.PERSON_ID"), "inner")
        .join(msd301labdel, col("BIRTH.PREGNANCY_ID") == col("MSDS.PREGNANCYID"), "left")
        .join(msd401babydemo, col("MSDS.LABOURDELIVERYID") == col("MSDBABY.LABOURDELIVERYID"), "left")
        .select(
            col("MOTHER.PERSON_ID").cast(StringType()).alias("Person_ID"),
            col("BIRTH.PREGNANCY_ID").cast(StringType()).alias("PregnancyID"),
            col("DEM.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("DEM.MRN").cast(StringType()).alias("MRN"),
            col("BIRTH.BABY_PERSON_ID").cast(StringType()).alias("BabyPerson_ID"),
            col("BIRTH.MRN").cast(StringType()).alias("Baby_MRN"),
            regexp_replace(col("BIRTH.NHS"), "-", "").cast(StringType()).alias("Baby_NHS"),
            col("BIRTH.BIRTH_ODR_NBR").cast(IntegerType()).alias("BirthOrder"),
            col("BIRTH.BIRTH_NBR").cast(IntegerType()).alias("BirthNumber"),
            col("BIRTH.BIRTH_LOC_DESC").cast(StringType()).alias("BirthLocation"),
            col("BIRTH.BIRTH_DT_TM").cast(StringType()).alias("BirthDateTime"),
            col("BIRTH.DEL_METHOD_DESC").cast(StringType()).alias("DeliveryMethod"),
            col("BIRTH.DEL_OUTCOME_DESC").cast(StringType()).alias("DeliveryOutcome"),
            col("BIRTH.NEO_OUTCOME_DESC").cast(StringType()).alias("NeonatalOutcome"),
            col("BIRTH.PREG_OUTCOME_DESC").cast(StringType()).alias("PregOutcome"),
            col("BIRTH.PRES_DEL_DESC").cast(StringType()).alias("PresDelDesc"),
            col("BIRTH.BIRTH_WT").cast(FloatType()).alias("BirthWeight"),
            col("BIRTH.NB_SEX_DESC").cast(StringType()).alias("BirthSex"),
            col("BIRTH.APGAR_1MIN").cast(IntegerType()).alias("APGAR1Min"),
            col("BIRTH.APGAR_5MIN").cast(IntegerType()).alias("APGAR5Min"),
            col("BIRTH.FEEDING_METHOD_DESC").cast(StringType()).alias("FeedingMethod"),
            col("BIRTH.MOTHER_COMPLICATION_DESC").cast(StringType()).alias("MotherComplications"),
            col("BIRTH.FETAL_COMPLICATION_DESC").cast(StringType()).alias("FetalComplications"),
            col("BIRTH.NEONATAL_COMPLICATION_DESC").cast(StringType()).alias("NeonatalComplications"),
            col("BIRTH.RESUS_METHOD_DESC").cast(StringType()).alias("ResMethod"),
            col("MSDS.LABOURDELIVERYID").cast(StringType()).alias("MSDS_LabourDelID"),
            col("MSDBABY.ORGSITEIDACTUALDELIVERY").cast(StringType()).alias("MSDS_DeliverySite"),
            col("MSDBABY.SETTINGPLACEBIRTH").cast(StringType()).alias("MSDS_BirthSetting"),
            col("MSDBABY.BABYFIRSTFEEDINDCODE").cast(StringType()).alias("MSDS_BabyFirstFeedCode"),
            col("MSDS.SETTINGINTRACARE").cast(StringType()).alias("MSDS_SettingIntraCare"),
            col("MSDS.REASONCHANGEDELSETTINGLAB").cast(StringType()).alias("MSDS_ReasonChangeDelSettingLab"),
            col("MSDS.LABOURONSETMETHOD").cast(StringType()).alias("MSDS_LabourOnsetMeth"),
            col("MSDS.LABOURONSETDATETIME").cast(StringType()).alias("MSDS_LabOnsetDate"),
            col("MSDS.CAESAREANDATETIME").cast(StringType()).alias("MSDS_CSectionDate"),
            col("MSDS.DECISIONTODELIVERDATETIME").cast(StringType()).alias("MSDS_DecDeliveryDate"),
            col("MSDS.ADMMETHCODEMOTHDELHSP").cast(StringType()).alias("MSDS_AdmMethCodeMothDelHSP"),
            col("MSDS.DISCHARGEDATETIMEMOTHERHSP").cast(StringType()).alias("MSDS_DischDate"),
            col("MSDS.DISCHMETHCODEMOTHPOSTDELHSP").cast(StringType()).alias("MSDS_DischMeth"),
            col("MSDS.DISCHDESTCODEMOTHPOSTDELHSP").cast(StringType()).alias("MSDS_DischDest"),
            col("MSDS.ROMDATETIME").cast(StringType()).alias("MSDS_RomDate"),
            col("MSDS.ROMMETHOD").cast(StringType()).alias("MSDS_RomMeth"),
            col("MSDS.ROMREASON").cast(StringType()).alias("MSDS_RomReason"),
            col("MSDS.EPISIOTOMYREASON").cast(StringType()).alias("MSDS_EpisiotomyReason"),
            col("MSDS.PLACENTADELIVERYMETHOD").cast(StringType()).alias("MSDS_PlancentaDelMeth"),
            col("MSDS.LABOURONSETPRESENTATION").cast(StringType()).alias("MSDS_LabOnsetPresentation"),
            greatest(col("BIRTH.ADC_UPDT"), col("MOTHER.ADC_UPDT"), col("DEM.ADC_UPDT"), col("MSDS.ADC_UPDT"), col("MSDBABY.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="msds_delivery_update")
def msds_delivery_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_msds_delivery_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_msds_delivery",
    comment="Incrementally updated MSDS delivery data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "Person_ID,PregnancyID"
    }
)

dlt.apply_changes(
    target = "rde_msds_delivery",
    source = "msds_delivery_update",
    keys = ["Person_ID", "PregnancyID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table(name="rde_msds_diagnosis_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def msds_diagnosis_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_msds_diagnosis")

    msd106diagnosispreg = spark.table("4_prod.raw.msds106diagnosispreg").alias("DIAG")
    msds_booking = dlt.read("rde_msds_booking").alias("PREG")
    lkp_mill_dir_snomed = spark.table("3_lookup.dwh.lkp_mill_dir_snomed").alias("S")
    mat_pregnancy = spark.table("4_prod.raw.mat_pregnancy").alias("MAT_PREG")
    patient_demographics = dlt.read("rde_patient_demographics").alias("DEM")

    # Filter each table separately
    filtered_msd106diagnosispreg = msd106diagnosispreg.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_mat_pregnancy = mat_pregnancy.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_lkp_mill_dir_snomed = lkp_mill_dir_snomed.filter(col("ADC_UPDT") > max_adc_updt)

    # Get relevant pregnancy IDs
    relevant_pregnancy_ids = (
        filtered_msd106diagnosispreg.select("PREGNANCYID")
        .union(filtered_mat_pregnancy.select("PREGNANCY_ID"))
        .distinct()
    ).alias("REL_PREG")

    # Window for getting the latest SNOMED record
    window_spec = Window.partitionBy("SNOMED_CD").orderBy(col("UPDT_DT_TM").desc())

    return (
        msd106diagnosispreg
        .join(relevant_pregnancy_ids, col("DIAG.PREGNANCYID") == col("REL_PREG.PREGNANCYID"), "inner")
        .join(mat_pregnancy, col("DIAG.PREGNANCYID") == col("MAT_PREG.PREGNANCY_ID"), "left")
        .join(patient_demographics, col("MAT_PREG.PERSON_ID") == col("DEM.PERSON_ID"), "inner")
        .join(
            lkp_mill_dir_snomed.withColumn("LastUpdt", row_number().over(window_spec)),
            (col("DIAG.DIAG") == col("S.SNOMED_CD")) & (col("LastUpdt") == 1),
            "left"
        )
        .select(
            col("MAT_PREG.PERSON_ID").cast(StringType()).alias("PERSON_ID"),
            col("DEM.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("DEM.MRN").cast(StringType()).alias("MRN"),
            col("DIAG.PREGNANCYID").cast(StringType()).alias("DiagPregID"),
            col("DIAG.DIAGSCHEME").cast(StringType()).alias("DiagScheme"),
            col("DIAG.DIAG").cast(StringType()).alias("Diagnosis"),
            col("DIAG.DIAGDATE").cast(StringType()).alias("DiagDate"),
            col("DIAG.LOCALFETALID").cast(StringType()).alias("LocalFetalID"),
            col("DIAG.FETALORDER").cast(StringType()).alias("FetalOrder"),
            col("S.SNOMED_CD").cast(StringType()).alias("SnomedCD"),
            col("S.SOURCE_STRING").cast(StringType()).alias("DiagDesc"),
            greatest(col("DIAG.ADC_UPDT"), col("MAT_PREG.ADC_UPDT"), col("DEM.ADC_UPDT"), col("S.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="msds_diagnosis_update")
def msds_diagnosis_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_msds_diagnosis_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_msds_diagnosis",
    comment="Incrementally updated MSDS diagnosis data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID,DiagPregID"
    }
)

dlt.apply_changes(
    target = "rde_msds_diagnosis",
    source = "msds_diagnosis_update",
    keys = ["PERSON_ID", "DiagPregID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)
