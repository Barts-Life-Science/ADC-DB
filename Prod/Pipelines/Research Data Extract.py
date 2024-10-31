# Databricks notebook source
#https://learn.microsoft.com/en-us/azure/databricks/optimizations/incremental-refresh
#https://learn.microsoft.com/en-us/azure/databricks/delta/row-tracking

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.functions import max as spark_max
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


@dlt.table(
    name="code_value",
    comment="Code Value Table",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.zOrderCols": "CODE_VALUE"
    }
)
def lookup_code_value():
    return (
        spark.table("3_lookup.mill.mill_code_value").filter(col("ACTIVE_IND") > 0)
)

@dlt.table(
    name="patient_nhs",
    comment="NHS table",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID"
    }
)
def lookup_nhs_number():
    window = Window.partitionBy("PERSON_ID").orderBy(desc("END_EFFECTIVE_DT_TM"))
    return (
        spark.table("4_prod.raw.mill_person_alias")
        .filter((col("PERSON_ALIAS_TYPE_CD") == 18) & (col("ACTIVE_IND") == 1))
        .withColumn("row", row_number().over(window))
        .filter(col("row") == 1)
        .select("PERSON_ID", "ALIAS", "ADC_UPDT")
    )

@dlt.table(
    name="patient_mrn",
    comment="MRN table",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID"
    }
)
def lookup_mrn():
    window = Window.partitionBy("PERSON_ID").orderBy(desc("END_EFFECTIVE_DT_TM"))
    return (
        spark.table("4_prod.raw.mill_person_alias")
        .filter((col("PERSON_ALIAS_TYPE_CD") == 10) & (col("ACTIVE_IND") == 1))
        .withColumn("row", row_number().over(window))
        .filter(col("row") == 1)
        .select("PERSON_ID", "ALIAS", "ADC_UPDT")
    )

@dlt.table(
    name="current_address",
    comment="Address table",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.zOrderCols": "PARENT_ENTITY_ID"
    }
)
def lookup_address():
    current_date_val = current_date()
    

    window = Window.partitionBy("PARENT_ENTITY_ID").orderBy(
        when(col("ZIPCODE").isNotNull() & (trim(col("ZIPCODE")) != ""), 0).otherwise(1),
        desc("BEG_EFFECTIVE_DT_TM")
    )
    
    return (
        spark.table("4_prod.raw.mill_address")
        .filter(
            (col("PARENT_ENTITY_NAME") == "PERSON") & 
            (col("ACTIVE_IND") == 1) & 
            (col("END_EFFECTIVE_DT_TM") > current_date_val)
        )
        .withColumn("row", row_number().over(window))
        .filter(col("row") == 1)
        .select("PARENT_ENTITY_ID", "ZIPCODE", "CITY", "ADC_UPDT")
    )

# COMMAND ----------

demographics_comment = "Contains the basic demographic details of each patient included in the extract." 

schema_rde_patient_demographics = StructType([
        StructField("PERSON_ID", DoubleType(), True, metadata={"comment": "This is the value of the unique primary identifier of the PERSON table. It is an internal system assigned number."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Hospital number, another unique identifier"}),
        StructField("Date_of_Birth", TimestampType(), True, metadata={"comment": "The date on which a PERSON was born or is officially deemed to have been born."}),
        StructField("GENDER_CD", DoubleType(), True, metadata={"comment": "Gender CD"}),
        StructField("Gender", StringType(), True, metadata={"comment": "Gender of the patient as text"}),
        StructField("ETHNIC_CD", DoubleType(), True, metadata={"comment": "Ethnicity CD"}),
        StructField("Ethnicity", StringType(), True, metadata={"comment": "The ethnicity of a PERSON, as specified by the PERSON."}),
        StructField("Date_of_Death", TimestampType(), True, metadata={"comment": "*Date of death of the patient  if deceased. May not be up to date if patient record has not been updated since the last spine lookup and patient died out of trust."}),
        StructField("Postcode", StringType(), True, metadata={"comment": "A code that is used to assist with finding or navigating to a specific location or delivery point. In some countries, this may provide better resolution than the standard postal address."}),
        StructField("City", StringType(), True, metadata={"comment": "The city field is the text name of the city associated with the address row."}),
        StructField("MARITAL_STATUS_CD", DoubleType(), True, metadata={"comment": "Marital status CD"}),
        StructField("MARITAL_STATUS", StringType(), True, metadata={"comment": "This field identifies the status of the person with regard to being married."}),
        StructField("LANGUAGE_CD", DoubleType(), True, metadata={"comment": "Language CD"}),
        StructField("LANGUAGE", StringType(), True, metadata={"comment": "The primary language spoken by the person."}),
        StructField("RELIGION_CD", DoubleType(), True, metadata={"comment": "Religion CD"}),
        StructField("RELIGION", StringType(), True, metadata={"comment": "A particular integrated system of belief in a supernatural power."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])


@dlt.table(name="rde_patient_demographics_incr", temporary=True,
        table_properties={
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID",
        "skipChangeCommits": "true"
    })
def patient_demographics_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_patient_demographics")
    
  
    pat = spark.table("4_prod.raw.mill_person_patient").alias("Pat").filter(col("active_ind") == 1)
    pers = spark.table("4_prod.raw.mill_person").alias("Pers").filter(col("active_ind") == 1)
    address_lookup = dlt.read("current_address").alias("A")
    nhs_lookup = dlt.read("patient_nhs").alias("NHS")
    mrn_lookup = dlt.read("patient_mrn").alias("MRN")
    code_value_lookup = dlt.read("code_value").alias("CV")
    
    
 
    pat_filtered = pat.filter(col("ADC_UPDT") > max_adc_updt)
    pers_filtered = pers.filter((col("ADC_UPDT") > max_adc_updt))
    address_filtered = address_lookup.filter(col("ADC_UPDT") > max_adc_updt)
    nhs_filtered = nhs_lookup.filter(col("ADC_UPDT") > max_adc_updt)
    mrn_filtered = mrn_lookup.filter(col("ADC_UPDT") > max_adc_updt)
    
  
    person_ids = (
        pat_filtered.select("PERSON_ID")
        .union(pers_filtered.select("PERSON_ID"))
        .union(address_filtered.select("PARENT_ENTITY_ID").alias("PERSON_ID"))
        .union(nhs_filtered.select("PERSON_ID"))
        .union(mrn_filtered.select("PERSON_ID"))
    ).distinct()
    
  
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

  

dlt.create_target_table(
    name = "rde_patient_demographics",
    comment=demographics_comment,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID"

    },
    schema = schema_rde_patient_demographics
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


encounter_comment = "Each encounter is a contact with a specific patient and clinical context"
schema_rde_encounter = StructType([
        StructField("PERSON_ID", DoubleType(), True, metadata={"comment": "Unique identifier for a person."}),
        StructField("ENCNTR_ID", DoubleType(), True, metadata={"comment": "Unique identifier for the encounter"}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "Patient NHS Number"}),
        StructField("REASON_FOR_VISIT_TXT", StringType(), True, metadata={"comment": "The free text description of reason for visit. Otherwise known as admitting symptom, presenting symptom, etc."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Patient Hospital Identifier"}),
        StructField("ENCNTR_TYPE_CD", DoubleType(), True, metadata={"comment": "Categorizes the encounter into a logical group or type. Examples may include inpatient, outpatient, etc."}),
        StructField("ENC_TYPE", StringType(), True, metadata={"comment": "Lookup text for the ENCNTR_TYPE_CD"}),
        StructField("ENC_STATUS_CD", DoubleType(), True, metadata={"comment": "Encounter status identifies the state of a particular encounter type from the time it is initiated until it is complete. (i.e., temporary, preliminary, active, discharged (complete), cancelled)."}),
        StructField("ENC_STATUS", StringType(), True, metadata={"comment": "Lookup text for the ENCNTR_STATUS_CD"}),
        StructField("FIN_NBR_ID", StringType(), True, metadata={"comment": "Financial Identifier for the encounter"}),
        StructField("ADMIN_CATEGORY_CD", DoubleType(), True, metadata={"comment": "Service category code."}),
        StructField("ADMIN_DESC", StringType(), True, metadata={"comment": "Lookup text for the ADMIN_CATEGORY_CD"}),
        StructField("TREATMENT_FUNCTION_CD", DoubleType(), True, metadata={"comment": "The type or category of medical service that the patient is receiving in relation to their encounter. The category may be of treatment type, surgery, general resources, or others."}),
        StructField("TFC_DESC", StringType(), True, metadata={"comment": "Lookup text for the TREATMENT_FUNCTION_CD"}),
        StructField("VISIT_ID", StringType(), True, metadata={"comment": "Linked visit ID for the encounter"}),
        StructField("CREATE_DT_TM", TimestampType(), True, metadata={"comment": "Date at which encounter was created."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_encounter_incr", table_properties={
        "pipelines.autoOptimize.zOrderCols": "ENCNTR_ID",
        "skipChangeCommits": "true"
    }, temporary=True)
def encounter_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_encounter")

    encounter = spark.table("4_prod.raw.mill_encounter")
    patient_demographics = dlt.read("rde_patient_demographics").alias("D")
    code_value_lookup = dlt.read("code_value").alias("CV")
    encounter_alias = spark.table("4_prod.raw.mill_encntr_alias")

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


dlt.create_target_table(
    name = "rde_encounter",
    comment=encounter_comment,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "ENCNTR_ID"

    },
    schema = schema_rde_encounter
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

apc_comment = "Diagnoses made in inpatient encounters"
schema_rde_apc_diagnosis = StructType([
        StructField("CDS_APC_ID", StringType(), True, metadata={"comment": "Uniquely identifies the inpatient attendence"}),
        StructField("PERSONID", StringType(), True, metadata={"comment": "Patient unique identifier"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Patient Local hospital number"}),
        StructField("ICD_Diagnosis_Num", IntegerType(), True, metadata={"comment": "Sequential index of the diagnosis"}),
        StructField("ICD_Diagnosis_Cd", StringType(), True, metadata={"comment": "ICD10 Code for the diagnosis."}),
        StructField("ICD_Diag_Desc", StringType(), True, metadata={"comment": "Text description of the diagnosis."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("Activity_date", StringType(), True, metadata={"comment": "Spell start date and time"}),
        StructField("CDS_Activity_Dt", StringType(), True, metadata={"comment": "\" Every CDS Type has a \"\"CDS Originating Date\"\" contained within the Commissioning Data Set data that must be used to populate the CDS ACTIVITY DATE\""}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_apc_diagnosis_incr", table_properties={
        "skipChangeCommits": "true"
    }, temporary=True)
def apc_diagnosis_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_apc_diagnosis")

    cds_apc_icd_diag = spark.table("4_prod.raw.cds_apc_icd_diag")
    cds_apc = spark.table("4_prod.raw.cds_apc").alias("Apc")
    lkp_icd_diag = spark.table("3_lookup.dwh.lkp_icd_diag").alias("ICDDESC")
    patient_demographics = dlt.read("rde_patient_demographics").alias("Pat")


    cds_apc_ids = (
        cds_apc_icd_diag.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_APC_ID")
        .union(cds_apc.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_APC_ID"))
    ).distinct()


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


dlt.create_target_table(
    name = "rde_apc_diagnosis",
    comment=apc_comment,
    schema = schema_rde_apc_diagnosis,
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

apc_opcs_comment = "Procedures from inpatient encounters"
schema_rde_apc_opcs = StructType([
        StructField("CDS_APC_ID", StringType(), True, metadata={"comment": "Uniquely identifies the inpatient attendence"}),
        StructField("PERSONID", StringType(), True, metadata={"comment": "Uniquely identifies the patient."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local hospital identifier"}),
        StructField("OPCS_Proc_Num", IntegerType(), True, metadata={"comment": "Sequential index of the procedures"}),
        StructField("OPCS_Proc_Scheme_Cd", StringType(), True, metadata={"comment": "CDS Procedure scheme in use."}),
        StructField("OPCS_Proc_Cd", StringType(), True, metadata={"comment": "OPCS Procedure code."}),
        StructField("Proc_Desc", StringType(), True, metadata={"comment": "Description of the procedure."}),
        StructField("OPCS_Proc_Dt", StringType(), True, metadata={"comment": "Date procedure occurred."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("Activity_date", StringType(), True, metadata={"comment": "Spell start date and time"}),
        StructField("CDS_Activity_Dt", StringType(), True, metadata={"comment": "\" Every CDS Type has a \"\"CDS Originating Date\"\" contained within the Commissioning Data Set data that must be used to populate the CDS ACTIVITY DATE\""}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])
    
@dlt.table(name="rde_apc_opcs_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def apc_opcs_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_apc_opcs")

    cds_apc_opcs_proc = spark.table("4_prod.raw.cds_apc_opcs_proc")
    cds_apc = spark.table("4_prod.raw.cds_apc").alias("Apc")
    lkp_opcs_410 = spark.table("3_lookup.dwh.opcs_410").alias("PDesc")
    patient_demographics = dlt.read("rde_patient_demographics").alias("Pat")


    cds_apc_ids = (
        cds_apc_opcs_proc.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_APC_ID")
        .union(cds_apc.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_APC_ID"))
    ).distinct()


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


dlt.create_target_table(
    name = "rde_apc_opcs",
    comment=apc_opcs_comment,
    schema = schema_rde_apc_opcs,
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

rde_op_diag_comment = "Diagnosis from outpatient encounters"
schema_rde_op_diagnosis = StructType([
        StructField("CDS_OPA_ID", StringType(), True, metadata={"comment": "Uniquely identifies each outpatient attendence"}),
        StructField("PERSONID", StringType(), True, metadata={"comment": "Unique identifier of the patient"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("ICD_Diagnosis_Num", IntegerType(), True, metadata={"comment": "Sequential number for the diagnoses"}),
        StructField("ICD_Diagnosis_Cd", StringType(), True, metadata={"comment": "ICD10 code for the diagnosis."}),
        StructField("ICD_Diag_Desc", StringType(), True, metadata={"comment": "Text description of the diagnosis"}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("Activity_date", StringType(), True, metadata={"comment": "Date of appointment"}),
        StructField("CDS_Activity_Dt", StringType(), True, metadata={"comment": "\" Every CDS Type has a \"\"CDS Originating Date\"\" contained within the Commissioning Data Set data that must be used to populate the CDS ACTIVITY DATE\""}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_op_diagnosis_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def op_diagnosis_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_op_diagnosis")

    cds_opa_icd_diag = spark.table("4_prod.raw.cds_opa_icd_diag")
    cds_op_all = spark.table("4_prod.raw.cds_op_all").alias("OP")
    lkp_icd_diag = spark.table("3_lookup.dwh.lkp_icd_diag").alias("ICDDESC")
    patient_demographics = dlt.read("rde_patient_demographics").alias("Pat")


    cds_opa_ids = (
        cds_opa_icd_diag.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_OPA_ID")
        .union(cds_op_all.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_OPA_ID"))
    ).distinct()


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


dlt.create_target_table(
    name = "rde_op_diagnosis",
    comment = rde_op_diag_comment,
    schema = schema_rde_op_diagnosis,
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

opa_opcs_comment = "Procedures from outpatient appointments"
schema_rde_opa_opcs = StructType([
        StructField("CDS_OPA_ID", StringType(), True, metadata={"comment": "Uniquely identifies each outpatient attendence"}),
        StructField("PERSONID", StringType(), True, metadata={"comment": "Unique identifier for the patient."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("OPCS_Proc_Num", IntegerType(), True, metadata={"comment": "Sequential number of the procedure for the patient."}),
        StructField("OPCS_Proc_Scheme_Cd", StringType(), True, metadata={"comment": "CDS Procedure schema in use."}),
        StructField("OPCS_Proc_Cd", StringType(), True, metadata={"comment": "OPCS code for the procedure."}),
        StructField("Proc_Desc", StringType(), True, metadata={"comment": "Text description of the procedure."}),
        StructField("OPCS_Proc_Dt", StringType(), True, metadata={"comment": "Date of the procedure."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("CDS_Activity_Dt", StringType(), True, metadata={"comment": "\" Every CDS Type has a \"\"CDS Originating Date\"\" contained within the Commissioning Data Set data that must be used to populate the CDS ACTIVITY DATE\""}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])
@dlt.table(name="rde_opa_opcs_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def opa_opcs_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_opa_opcs")

    cds_opa_opcs_proc = spark.table("4_prod.raw.cds_opa_opcs_proc")
    cds_op_all = spark.table("4_prod.raw.cds_op_all").alias("OP")
    lkp_opcs_410 = spark.table("3_lookup.dwh.opcs_410").alias("OPDesc")
    patient_demographics = dlt.read("rde_patient_demographics").alias("Pat")


    cds_opa_ids = (
        cds_opa_opcs_proc.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_OPA_ID")
        .union(cds_op_all.filter(col("ADC_UPDT") > max_adc_updt).select("CDS_OPA_ID"))
    ).distinct()


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


dlt.create_target_table(
    name = "rde_opa_opcs",
    comment = opa_opcs_comment,
    schema = schema_rde_opa_opcs,
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

cds_apc_comment = "Details of an inpatient attendence"
schema_rde_cds_apc = StructType([
        StructField("CDS_APC_ID", StringType(), True, metadata={"comment": "Uniquely identifies the inpatient attendence"}),
        StructField("PERSONID", StringType(), True, metadata={"comment": "Unique identifier for the person."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("Adm_Dt", StringType(), True, metadata={"comment": "The Start Date of the Hospital Provider Spell is the date of admission"}),
        StructField("Disch_Dt", StringType(), True, metadata={"comment": "DISCHARGE DATE (HOSPITAL PROVIDER SPELL) is the date a PATIENT was discharged from a Hospital Provider Spell.\
"}),
        StructField("LOS", StringType(), True, metadata={"comment": "A derived column from spell start and end date"}),
        StructField("Priority_Cd", StringType(), True, metadata={"comment": "CDS Priority code"}),
        StructField("Priority_Desc", StringType(), True, metadata={"comment": "Text description of the CDS Priority code"}),
        StructField("Treat_Func_Cd", StringType(), True, metadata={"comment": "A unique identifier for a TREATMENT FUNCTION.\
"}),
        StructField("Spell_HRG_Cd", StringType(), True, metadata={"comment": " Hospital provider spell healthcare resource group. This is derived from the Reference cost HRG grouper for completed spell activity. "}),
        StructField("HRG_Desc", StringType(), True, metadata={"comment": "Description of the HRG code"}),
        StructField("Patient_Class_Desc", StringType(), True, metadata={"comment": "A detailed description of the  classification of PATIENTS who have been admitted to a Hospital Provider Spell. "}),
        StructField("PatClass_Desc", StringType(), True, metadata={"comment": "A  description of the  of the patient classification code"}),
        StructField("Admin_Cat_Cd", StringType(), True, metadata={"comment": "Administrative category code"}),
        StructField("Admin_Cat_Desc", StringType(), True, metadata={"comment": "ADMINISTRATIVE CATEGORY CODE (ON ADMISSION) is used to record the ADMINISTRATIVE CATEGORY CODE at the start of the Hospital Provider Spell."}),
        StructField("Admiss_Srce_Cd", StringType(), True, metadata={"comment": "ADMISSION SOURCE (HOSPITAL PROVIDER SPELL) is the source of admission to a Hospital Provider Spell in a Hospital Site"}),
        StructField("Admiss_Source_Desc", StringType(), True, metadata={"comment": "ADMISSION SOURCE (HOSPITAL PROVIDER SPELL) is the source of admission to a Hospital Provider Spell in a Hospital Site"}),
        StructField("Disch_Dest", StringType(), True, metadata={"comment": "The destination of a PATIENT on completion of a Hospital Provider Spell"}),
        StructField("Disch_Dest_Desc", StringType(), True, metadata={"comment": "The destination of a PATIENT on completion of a Hospital Provider Spell"}),
        StructField("Ep_Num", StringType(), True, metadata={"comment": "A unique number or set of characters that is applicable to only one ACTIVITY for a PATIENT within an ORGANISATION"}),
        StructField("Ep_Start_Dt", StringType(), True, metadata={"comment": "Start date of the episode."}),
        StructField("Ep_End_Dt", StringType(), True, metadata={"comment": "End date of the episode."}),
        StructField("CDS_Activity_Dt", StringType(), True, metadata={"comment": "Date of the base cds activity."}),
        StructField("ENC_DESC", StringType(), True, metadata={"comment": "Text label for the type of encounter."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])
    
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


    cds_apc_ids = (
        slam_apc_hrg_v4.filter(col("ADC_UPDT") > max_adc_updt).select(trim(col("CDS_APC_Id")).alias("CDS_APC_Id"))
        .union(cds_apc.filter(col("ADC_UPDT") > max_adc_updt).select(trim(col("CDS_APC_ID")).alias("CDS_APC_ID")))
    ).distinct()


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


dlt.create_target_table(
    name = "rde_cds_apc",
    comment=cds_apc_comment,
    schema = schema_rde_cds_apc,
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
    keys = ["CDS_APC_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)
       

# COMMAND ----------

cds_opa_comment = "Details for each outpatient attendence"
schema_rde_cds_opa = StructType([
        StructField("CDS_OPA_ID", StringType(), True, metadata={"comment": "Uniquely identifies each outpatient attendence"}),
        StructField("AttendanceType", StringType(), True, metadata={"comment": "Type of attendance, Referall, pre-registration etc."}),
        StructField("CDSDate", StringType(), True, metadata={"comment": "\" Every CDS Type has a \"\"CDS Originating Date\"\" contained within the Commissioning Data Set data that must be used to populate the CDS ACTIVITY DATE\""}),
        StructField("Att_Dt", StringType(), True, metadata={"comment": "Date of appointment"}),
        StructField("HRG_Cd", StringType(), True, metadata={"comment": "HRG Code for this outpatient attendance"}),
        StructField("HRG_Desc", StringType(), True, metadata={"comment": "Description of the HRG code"}),
        StructField("Treat_Func_Cd", StringType(), True, metadata={"comment": "A unique identifier for a TREATMENT FUNCTION.\
"}),
        StructField("Att_Type", StringType(), True, metadata={"comment": "First attendance  type description "}),
        StructField("Attended_Desc", StringType(), True, metadata={"comment": "This indicates whether or not a patient attended for an appointment."}),
        StructField("Attendance_Outcome_Desc", StringType(), True, metadata={"comment": "Describes the outcome of an outpatient attendance."}),
        StructField("NHS_NUMBER", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("PERSONID", StringType(), True, metadata={"comment": "Unique identifier of the person."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("APPT_DT_TM", StringType(), True, metadata={"comment": "Date and time of the appointment"}),
        StructField("APPT_LOCATION_CD", StringType(), True, metadata={"comment": "Location code for the appointment."}),
        StructField("REASON_FOR_VISIT_TXT", StringType(), True, metadata={"comment": "Free text given reason for the encounter."}),
        StructField("APPT_TYPE_CD", StringType(), True, metadata={"comment": "Code for appointment type."}),
        StructField("FIN_NBR_ID", StringType(), True, metadata={"comment": "Financial ID for the appointment."}),
        StructField("ATTENDED_DNA_NHS_CD_ALIAS", StringType(), True, metadata={"comment": "attendance outcome code."}),
        StructField("EXPECTED_DUR_OF_APPT_NBR", IntegerType(), True, metadata={"comment": "Expected duration of appointment in minutes."}),
        StructField("ACTIVITY_LOC_TYPE_NHS_CD_ALIAS", StringType(), True, metadata={"comment": "Code for location type of the appointment."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

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
    mill_dir_cds_batch_content_hist = spark.table("4_prod.raw.mill_cds_batch_content_hist").alias("BHIST")
    mill_dir_encounter = spark.table("4_prod.raw.mill_encounter").alias("ENC")
    mill_dir_sch_appt = spark.table("4_prod.raw.mill_sch_appt").alias("APPT")
    mill_dir_sch_event = spark.table("4_prod.raw.mill_sch_event").alias("SCHE")
    mill_dir_encntr_alias = spark.table("4_prod.raw.mill_encntr_alias").alias("EA")

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


    op_attendance.createOrReplaceTempView("op_attendance_view")


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


dlt.create_target_table(
    name = "rde_cds_opa",
    comment=cds_opa_comment,
    schema = schema_rde_cds_opa,
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


pathology_comment = "Details of results and reports from pathology systems."
schema_rde_pathology = StructType([
        StructField("ENCNTR_ID", StringType(), True, metadata={"comment": "Unique identifier for the Encounter table."}),
        StructField("PERSONID", StringType(), True, metadata={"comment": "This is the value of the unique primary identifier of the person table.  It is an internal system assigned number."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("RequestDate", StringType(), True, metadata={"comment": "Pathology order requested date"}),
        StructField("TestCode", StringType(), True, metadata={"comment": "Any short code: like GLU for glucose or any arbitrary numeric id for the same.    This field has some shortcode related to text-not decoded"}),
        StructField("TestName", StringType(), True, metadata={"comment": "It is the code that identifies the most basic unit of the storage, i.e. RBC, discharge summary, image."}),
        StructField("TestDesc", StringType(), True, metadata={"comment": "Description of the test"}),
        StructField("Result_nbr", DoubleType(), True, metadata={"comment": "Numeric test result value "}),
        StructField("ResultTxt", StringType(), True, metadata={"comment": "Text test result  "}),
        StructField("ResultNumeric", IntegerType(), True, metadata={"comment": "A binary digit with 1 indicating that EVENT_RESULT_TXT is numeric; 0 otherwise."}),
        StructField("ResultUnit", StringType(), True, metadata={"comment": "This filed holds the unit "}),
        StructField("ResUpper", StringType(), True, metadata={"comment": "Normal High value"}),
        StructField("ResLower", StringType(), True, metadata={"comment": "Normal low value"}),
        StructField("Resultfinding", StringType(), True, metadata={"comment": "\"States whether the result is normal.  This can be used to determine whether to  display the event tag in different color on the flowsheet. For group results, this represents an \"\"overall\"\" normalcy. i.e. Is any result in the group     abnormal?  Also allows different purge criteria to be applied based on result."}),
        StructField("ReportDate", StringType(), True, metadata={"comment": "Optional clinical date time for the start of the event."}),
        StructField("Report", StringType(), True, metadata={"comment": "Detailed report from blob table "}),
        StructField("OrderStatus", StringType(), True, metadata={"comment": "Status of the order"}),
        StructField("ResStatus", StringType(), True, metadata={"comment": "Result status "}),
        StructField("SnomedCode", StringType(), True, metadata={"comment": "Snomed code for the pathology event if one exists."}),
        StructField("EventID", StringType(), True, metadata={"comment": "A unique ID that can be used to map with the blob report"}),
        StructField("LabNo", StringType(), True, metadata={"comment": "Reference number from pathology system"}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_pathology_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def pathology_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_pathology")

    orders = spark.table("4_prod.raw.mill_orders").alias("ORD")
    order_catalogue = spark.table("4_prod.raw.mill_order_catalog").alias("CAT")
    clinical_event = spark.table("4_prod.raw.mill_clinical_event")
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


dlt.create_target_table(
    name = "rde_pathology",
    comment=pathology_comment,
    schema = schema_rde_pathology,
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


raw_pathology_comment = "Pathology data direct from the pathology system's data warehouse"

schema_rde_raw_pathology = StructType([
        StructField("PERSON_ID", DoubleType(), True, metadata={"comment": "This is the value of the unique primary identifier of the person table.  It is an internal system assigned number."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("LabNo", StringType(), True, metadata={"comment": "Reference number from pathology system"}),
        #StructField("TLCCode", StringType(), True, metadata={"comment": "TLCCode"}),
        #StructField("Specimen", StringType(), True, metadata={"comment": "Type of specimen"}),
        #StructField("TLCSnomed", StringType(), True, metadata={"comment": "TLCSnomed"}),
        #StructField("TLCDesc", StringType(), True, metadata={"comment": "Description of TLCCode"}),
        StructField("TFCCode", StringType(), True, metadata={"comment": "TFCCode"}),
        StructField("Subcode", StringType(), True, metadata={"comment": "TFCCode subcode"}),
        StructField("WkgCode", StringType(), True, metadata={"comment": "WkgCode"}),
        #StructField("Processed", IntegerType(), True, metadata={"comment": "1 indicates that NotProcessed is 0; 0 when NotProcessed is 1"}),
        StructField("Result", StringType(), True, metadata={"comment": "Pathology result"}),
        StructField("ResultNumeric", IntegerType(), True, metadata={"comment": "1 indicates that TFCValue is numeric, not null, and not [.]; 0 otherwise. Note that in some cases where the value is for example [4.3  37%], it is identified as non-numeric. "}),
        StructField("TFCResultSeq", LongType(), True, metadata={"comment": "ID number from external system(s)"}),
        StructField("SectionCode", StringType(), True, metadata={"comment": "SectionCode"}),
        StructField("TFCDesc", StringType(), True, metadata={"comment": "Description of TFCCode"}),
        StructField("RequestDT", TimestampType(), True, metadata={"comment": "Request datetime"}),
        StructField("SampleDT", TimestampType(), True, metadata={"comment": "Sample datetime"}),
        StructField("ReportDate", TimestampType(), True, metadata={"comment": "Report datetime"}),
        StructField("Fasting", StringType(), True, metadata={"comment": "Fasting status"}),
        StructField("Pregnant", StringType(), True, metadata={"comment": "Pregnancy status"}),
        StructField("RefClinCode", StringType(), True, metadata={"comment": "RefClinCode"}),
        StructField("RefSourceCode", StringType(), True, metadata={"comment": "RefSourceCode"}),
        StructField("ClinicalDetails", StringType(), True, metadata={"comment": "Clinical information, or reason for requesting test"}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_raw_pathology_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def raw_pathology_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_raw_pathology")
    

    pres = spark.table("4_prod.raw.path_patient_resultlevel").alias("PRES")
    pmrt = spark.table("4_prod.raw.path_master_resultable").alias("PMRT")
    pmor = spark.table("4_prod.raw.path_master_orderables").alias("PMOR")
    psl = spark.table("4_prod.raw.path_patient_samplelevel").alias("PSL")

    filtered_pres = pres.filter(col("ADC_UPDT") > max_adc_updt)
    

    updated_lab_nos = filtered_pres.select("LabNo").distinct()
    

    filtered_psl = psl.join(updated_lab_nos, "LabNo", "inner")
    

    patient_demographics = dlt.read("rde_patient_demographics").alias("PD")
    
   
    joined_data = (
        filtered_pres
        .join(filtered_psl, "LabNo")
        .join(pmrt, filtered_pres.TFCCode == pmrt.TFCCode, "left")
        #.join(pmor, filtered_pres.TLCCode == pmor.TLCCode, "left")
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
            #filtered_pres.TLCCode,
            #pmor.CSpecTypeCode.alias("Specimen"),
            #pmor.SnomedCTCode.alias("TLCSnomed"),
            #pmor.TLCDesc_Full.alias("TLCDesc"),
            filtered_pres.TFCCode,
            filtered_pres.LegTFCCode.alias("Subcode"),
            filtered_pres.WkgCode,
            #when(filtered_pres.NotProcessed == 1, 0).otherwise(1).alias("Processed"),
            filtered_pres.TFCValue.alias("Result"),
            when(
                filtered_pres.TFCValue.isNotNull() & 
                (filtered_pres.TFCValue != '.') & 
                filtered_pres.TFCValue.cast("double").isNotNull(), 
                1
            ).otherwise(0).alias("ResultNumeric"),
            filtered_pres.TFCResultSeq,
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


dlt.create_target_table(
    name = "rde_raw_pathology",
    comment=raw_pathology_comment,
    schema=schema_rde_raw_pathology,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "LabNo,TFCCode,TFCResultSeq"

    }
)

dlt.apply_changes(
    target = "rde_raw_pathology",
    source = "raw_pathology_update",
    keys = ["PERSON_ID", "TFCResultSeq"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

     

# COMMAND ----------

aria_pharmacy_comment = "Data from the ARIA chemotherapy database, no longer in active use."

schema_rde_ariapharmacy = StructType([
        StructField("PERSON_ID", StringType(), True, metadata={"comment": "Unique identifier for the patient."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("AdmnStartDate", StringType(), True, metadata={"comment": "Date on which the first administration of the course is to be done."}),
        StructField("TreatPlan", StringType(), True, metadata={"comment": "Name of a treatment plan.  Combined with tp_vers_no, uniquely identifies a treatment plan."}),
        StructField("ProductDesc", StringType(), True, metadata={"comment": "Name of the agent."}),
        StructField("DosageForm", StringType(), True, metadata={"comment": "Code used to identify the dosage form of the agent."}),
        StructField("RxDose", IntegerType(), True, metadata={"comment": "The amount of the agent in this prescription to order from the pharmacy with the intent of administering it."}),
        StructField("RxTotal", IntegerType(), True, metadata={"comment": "The total amount of the agent which will be ordered for the patient according to the frequency and amount for this agent item."}),
        StructField("SetDateTPInit", StringType(), True, metadata={"comment": "Date of when the patient was registered on the plan, if the agent is part of a treatment plan."}),
        StructField("DoseLevel", IntegerType(), True, metadata={"comment": "Code to identify the dose level of the agent.  The field is used to create many forms of a specific agent normally based on the dosage to be given.  i.e. a pediatric form, high, medium, low, etc."}),
        StructField("AdmnDosageUnit", IntegerType(), True, metadata={"comment": "Dosage unit admninsterd"}),
        StructField("AdmnRoute", IntegerType(), True, metadata={"comment": "Code to identify the route which should be used to administer the agent."}),
        StructField("Pharmacist_Approved", StringType(), True, metadata={"comment": "pharmacist approved date time details"}),
        StructField("pt_inst_key_id", StringType(), True, metadata={"comment": "ID number for particular patient instance."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_ariapharmacy_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def ariapharmacy_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_ariapharmacy")
    
    pt_inst_key = spark.table("4_prod.raw.aria_pt_inst_key").alias("Ptkey")
    agt_rx = spark.table("4_prod.raw.aria_agt_rx").alias("Arx")
    rx = spark.table("4_prod.raw.aria_rx").alias("Rx")
    patient_demographics = dlt.read("rde_patient_demographics").alias("D")
    person_alias = spark.table("4_prod.raw.mill_person_alias").alias("PA")

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


dlt.create_target_table(
    name = "rde_ariapharmacy",
    comment=aria_pharmacy_comment,
    schema=schema_rde_ariapharmacy,
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

iqemo_comment="Data from the IQEMO chemotherapy system, replaced ARIA."

schema_rde_iqemo = StructType([
        StructField("PERSON_ID", StringType(), True, metadata={"comment": "This is the value of the unique primary identifier of the PERSON table. It is an internal system assigned number."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Primary identifier of PERSON in iQEMO system; joined with BHResearch Demographics table on MRN"}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("TreatmentCycleID", StringType(), True, metadata={"comment": "Unique ID in iQemo TreatmentCycle table"}),
        StructField("PrescribedDate", StringType(), True, metadata={"comment": "Date on which the treatment was prescribed"}),
        StructField("TemplateName", StringType(), True, metadata={"comment": "Treatment cycle template name"}),
        StructField("Name", StringType(), True, metadata={"comment": "Name for this Regimen. Must be unique within the organisation."}),
        StructField("DefaultCycles", IntegerType(), True, metadata={"comment": "The default number of cycles to create when booking a course of this regimen."}),
        StructField("ChemoRadiation", BooleanType(), True, metadata={"comment": "indicates if given with radiotherapy."}),
        StructField("OPCSProcurementCode", StringType(), True, metadata={"comment": "The NHS OPCS procurement code for this item.\
"}),
        StructField("OPCSDeliveryCode", StringType(), True, metadata={"comment": "The NHS OPCS delivery code for this item.\
"}),
        StructField("SactName", StringType(), True, metadata={"comment": "Name for the regimen matching those defined in the national SACT dataset."}),
        StructField("Indication", StringType(), True, metadata={"comment": "A free text description of the Indication for this regimen. Used to detail appropriate usage and displayed when booking courses of this regimen."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_iqemo_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def iqemo_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_iqemo")
   
    treatment_cycle = spark.table("4_prod.raw.iqemo_treatment_cycle").alias("TC")
    chemotherapy_course = spark.table("4_prod.raw.iqemo_chemotherapy_course").alias("CC")
    regimen = spark.table("4_prod.raw.iqemo_regimen").alias("RG")
    iqemo_patient = spark.table("4_prod.raw.iqemo_patient").alias("PT")
    patient_demographics = dlt.read("rde_patient_demographics").alias("DEM")
    person_alias = spark.table("4_prod.raw.mill_person_alias").alias("PA")

    return (
        treatment_cycle.filter(col("ADC_UPDT") > max_adc_updt)
        .join(chemotherapy_course, col("CC.ChemoTherapyCourseID") == col("TC.ChemoTherapyCourseID"), "left")
        .join(regimen, col("CC.RegimenID") == col("RG.RegimenID"), "left")
        .join(iqemo_patient, col("TC.PatientID") == col("PT.PatientID"), "left")
        .join(
            person_alias,
            (trim(iqemo_patient.PrimaryIdentifier) == trim(person_alias.ALIAS)) & 
             (person_alias.PERSON_ALIAS_TYPE_CD == 10),
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



dlt.create_target_table(
    name = "rde_iqemo",
    comment=iqemo_comment,
    schema=schema_rde_iqemo,
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



# COMMAND ----------

radiology_comment = "Results and reports from the radiology system."
schema_rde_radiology = StructType([
        StructField("PERSON_ID", StringType(), True, metadata={"comment": "This is the value of the unique primary identifier of the person table.  It is an internal system assigned number."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("ENCNTR_ID", StringType(), True, metadata={"comment": "This is the value of the unique primary identifier of the encounter table."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("Acitvity_Type", StringType(), True, metadata={"comment": "Inpatient/Outpatient/ED"}),
        StructField("TFCode", StringType(), True, metadata={"comment": "A unique identifier for a TREATMENT FUNCTION.\
"}),
        StructField("TFCdesc", StringType(), True, metadata={"comment": "Detailed description of the associated TFC code"}),
        StructField("ExamName", StringType(), True, metadata={"comment": "Name of the radiology exam"}),
        StructField("EventName", StringType(), True, metadata={"comment": "Description of event CD"}),
        StructField("EVENT_TAG_TXT", StringType(), True, metadata={"comment": "Brief text string to describe the event and to be displayed on the flowsheet. Calculated based on event class and status"}),
        StructField("ResultNumeric", IntegerType(), True, metadata={"comment": "1 indicates EVENT_RESULT_TXT is numeric; 0 otherwise"}),
        StructField("ExamStart", StringType(), True, metadata={"comment": "Optional clinical date time for the start of the event."}),
        StructField("ExamEnd", StringType(), True, metadata={"comment": "Clinical date time for the end of the event.  In the cases where results do not associate an Event Time range, then the event_start_dt_tm = event_end_dt_tm."}),
        StructField("ReportText", StringType(), True, metadata={"comment": "Detailed reoprt of the radiology examination "}),
        StructField("LastOrderStatus", StringType(), True, metadata={"comment": "Status code for the latest order associated with this event."}),
        StructField("RecordStatus", StringType(), True, metadata={"comment": "The lastest status of the order placed eg:Completed, cancelled etc"}),
        StructField("ResultStatus", StringType(), True, metadata={"comment": "This column is the decoded description of result status code.  Valid values: authenticated, unauthenticated, unknown, canceled, pending, in lab, active, modified, superseded, transcribed, not done."}),
        StructField("ExaminationTypecode", StringType(), True, metadata={"comment": "code used to uniquely identify examination "}),
        StructField("Modality", StringType(), True, metadata={"comment": "Medical imaging modalities, for example, includes magnetic resonance imaging (MRI), ultrasound, medical radiation, angiography and computed tomography (CT) scanning etc"}),
        StructField("SubModality", StringType(), True, metadata={"comment": "Submodalities are fine distinctions or the subsets of the Modalities "}),
        StructField("ExaminationTypeName", StringType(), True, metadata={"comment": "name of the examination"}),
        StructField("EventID", StringType(), True, metadata={"comment": "EventID is added to map to blob reports"}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_radiology_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def radiology_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_radiology")

    clinical_event = spark.table("4_prod.raw.mill_clinical_event")
    orders = spark.table("4_prod.raw.mill_orders").alias("ORD")
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


dlt.create_target_table(
    name = "rde_radiology",
    comment=radiology_comment,
    schema=schema_rde_radiology,
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

family_history_comment = "Details on family history such as family health conditions."

schema_rde_family_history = StructType([
        StructField("PERSON_ID", StringType(), True, metadata={"comment": "Unique identifier for the patient."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local hospital identifier."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "NHS Number of patient."}),
        StructField("RELATION_CD", StringType(), True, metadata={"comment": "Code for the type of relationship. e.g. Grandchild"}),
        StructField("RelationDesc", StringType(), True, metadata={"comment": "Text description of the relation_CD"}),
        StructField("RELATION_TYPE", StringType(), True, metadata={"comment": "Code for the type of relationship. e.g. family history"}),
        StructField("RelationType", StringType(), True, metadata={"comment": "Text description for RELATION_TYPE"}),
        StructField("ACTIVITY_NOMEN", StringType(), True, metadata={"comment": "Code for the nomenclature category."}),
        StructField("NomenDesc", StringType(), True, metadata={"comment": "Description of the condition."}),
        StructField("NomenVal", StringType(), True, metadata={"comment": "Code for the condition."}),
        StructField("VOCABULARY_CD", StringType(), True, metadata={"comment": "Code for the vocabulary being used."}),
        StructField("VocabDesc", StringType(), True, metadata={"comment": "Description of the vocabulary being used. e.g. SNOMED"}),
        StructField("TYPE", StringType(), True, metadata={"comment": "Type of family history event, e.g. condition."}),
        StructField("BegEffectDate", StringType(), True, metadata={"comment": "Start date."}),
        StructField("EndEffectDate", StringType(), True, metadata={"comment": "End date if applicable."}),
        StructField("FHX_VALUE_FLG", StringType(), True, metadata={"comment": "Indicates weather the condition for a Family member is positive, negative or unknown.  0 = Negative; 1 = Positive; 2 = Unknown;  3 = Unable to Obtain; 4 = Patient Adopted"}),
        StructField("REL_ID", StringType(), True, metadata={"comment": "Internal lookup ID for the relationship."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_family_history_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def family_history_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_family_history")

    family_history = spark.table("4_prod.raw.pi_dir_family_history_activity").alias("F")
    patient_demographics = dlt.read("rde_patient_demographics").alias("E")
    person_patient_person_reltn = spark.table("4_prod.raw.mill_person_person_reltn").alias("REL")
    nomenclature_ref = spark.table("3_lookup.mill.mill_nomenclature").alias("R")
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


dlt.create_target_table(
    name = "rde_family_history",
    comment=family_history_comment,
    schema= schema_rde_family_history,
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
    keys = ["PERSON_ID", "REL_ID", "RELATION_CD", "RELATION_TYPE", "RelationDesc", "ACTIVITY_NOMEN", "NomenVal"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

blob_comment = "Table containing all free text items for a given patient and encounter."

schema_rde_blobdataset = StructType([
        StructField("PERSON_ID", StringType(), True, metadata={"comment": "Unique Identifier for the patient."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("ClinicalSignificantDate", StringType(), True, metadata={"comment": "Date clinical event flagged as clinically significant, generally the date the event was created."}),
        StructField("MainEventDesc", StringType(), True, metadata={"comment": "Main event description(eg:ED Assessments, Coding Summary, Ophthalmology Clinic letter, ED Department Summary etc)"}),
        StructField("MainTitleText", StringType(), True, metadata={"comment": "Main event title"}),
        StructField("MainTagText", StringType(), True, metadata={"comment": "Main event tag text"}),
        StructField("ChildEvent", StringType(), True, metadata={"comment": "child event title"}),
        StructField("ChildTagText", StringType(), True, metadata={"comment": "child event tag text"}),
        StructField("BlobContents", StringType(), True, metadata={"comment": "Detailed description or report about the clinical event"}),
        StructField("EventDesc", StringType(), True, metadata={"comment": "Event description"}),
        StructField("EventResultText", StringType(), True, metadata={"comment": "Event result as text"}),
        StructField("EventResultNBR", DoubleType(), True, metadata={"comment": "Event result as numbers for numeric values"}),
        StructField("EventReltnDesc", StringType(), True, metadata={"comment": "event relation description like Child, root"}),
        StructField("Status", StringType(), True, metadata={"comment": "status of the record"}),
        StructField("SourceSys", StringType(), True, metadata={"comment": "details of the source system (eg: PowerChart, BLT none RAD or LAB , BLT_TIE_RAD etc)"}),
        StructField("ClassDesc", StringType(), True, metadata={"comment": "event class description(eg: Document)"}),
        StructField("ParentEventID", StringType(), True, metadata={"comment": "Lookup ID for the parent event of this one if any."}),
        StructField("EventID", StringType(), True, metadata={"comment": "EventID to map to radiology and pathology events."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_blobdataset_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def blobdataset_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_blobdataset")

    blob_content = spark.table("4_prod.raw.pi_cde_blob_content").alias("B")
    clinical_event = spark.table("4_prod.raw.mill_clinical_event").filter(F.col("VALID_UNTIL_DT_TM") > F.current_timestamp()).alias("CE")
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

dlt.create_target_table(
    name = "rde_blobdataset",
    comment=blob_comment,
    schema=schema_rde_blobdataset,
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

pc_procedures_comment = "Table of all procedures recorded in powerchart"

schema_rde_pc_procedures = StructType([
        StructField("PERSON_ID", StringType(), True, metadata={"comment": "Unique identifier for the patient"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("AdmissionDT", StringType(), True, metadata={"comment": "date of admission"}),
        StructField("DischargeDT", StringType(), True, metadata={"comment": "date of discharge"}),
        StructField("TreatmentFunc", StringType(), True, metadata={"comment": "treatment function description"}),
        StructField("Specialty", StringType(), True, metadata={"comment": "Main speciality"}),
        StructField("ProcDt", StringType(), True, metadata={"comment": "Date of procedure"}),
        StructField("ProcDetails", StringType(), True, metadata={"comment": "deatiled description of the procedure"}),
        StructField("ProcCD", StringType(), True, metadata={"comment": "ProcCD"}),
        StructField("ProcType", StringType(), True, metadata={"comment": "procedure type"}),
        StructField("EncType", StringType(), True, metadata={"comment": "Encounter type"}),
        StructField("Comment", StringType(), True, metadata={"comment": "Comments specific to the procedure if any"}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])


@dlt.table(name="rde_pc_procedures_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def pc_procedures_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_pc_procedures")

    pc_procedures = spark.table("4_prod.raw.pc_procedures").alias("PCProc")
    patient_demographics = dlt.read("rde_patient_demographics").alias("E")
    person_alias = spark.table("4_prod.raw.mill_person_alias").alias("PA")

    return (
        pc_procedures.filter(col("ADC_UPDT") > max_adc_updt)
        .join(
        person_alias,
        (trim(pc_procedures.MRN) == trim(person_alias.ALIAS)) & 
        (person_alias.PERSON_ALIAS_TYPE_CD == 10),
        "inner"
        )
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


dlt.create_target_table(
    name = "rde_pc_procedures",
    comment=pc_procedures_comment,
    schema=schema_rde_pc_procedures,
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

all_proc_comment = "Table of all procedures recorded in millenium procedure table."

schema_rde_all_procedures = StructType([
        StructField("MRN", StringType(), True, metadata={"comment": "Local hospital identifier."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "NHS Number for the patient."}),
        StructField("Person_ID", StringType(), True, metadata={"comment": "Unique identifier for the patient."}),
        StructField("Procedure_code", StringType(), True, metadata={"comment": "OPCS code for the procedure."}),
        StructField("Catalogue", StringType(), True, metadata={"comment": "Catalogue for the code, e.g. OPCS"}),
        StructField("Code_text", StringType(), True, metadata={"comment": "Text description of the procedure."}),
        StructField("Procedure_note", StringType(), True, metadata={"comment": "Any free text notes attached to the procedure."}),
        StructField("Procedure_ID", StringType(), True, metadata={"comment": "Internal ID for the procedure."}),
        StructField("Procedure_date", StringType(), True, metadata={"comment": "Date the procedure took place."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_all_procedures_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def all_procedures_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_all_procedures")

    mill_dir_procedure = spark.table("4_prod.raw.mill_procedure").alias("mil")
    encounter = dlt.read("rde_encounter").alias("E")
    mill_dir_nomenclature = spark.table("3_lookup.mill.mill_nomenclature").alias("nom")

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


dlt.create_target_table(
    name = "rde_all_procedures",
    comment=all_proc_comment,
    schema=schema_rde_all_procedures,
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

pc_diagnosis_comment = "Table of all diagnoses recorded in powerchart."

schema_rde_pc_diagnosis = StructType([
        StructField("DiagID", StringType(), True, metadata={"comment": "Unique identifier to identify the diagnosis"}),
        StructField("Person_ID", StringType(), True, metadata={"comment": "This is the value of the unique primary identifier of the person table.  It is an internal system assigned number."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("Diagnosis", StringType(), True, metadata={"comment": "Description of the diagnosis"}),
        StructField("Confirmation", StringType(), True, metadata={"comment": "Confirmation status"}),
        StructField("DiagDt", StringType(), True, metadata={"comment": "Date of diagnosis"}),
        StructField("Classification", StringType(), True, metadata={"comment": "Any classification flags applied to the diagnosis."}),
        StructField("ClinService", StringType(), True, metadata={"comment": "Any details on the clinical service wherin the diagnosis was made."}),
        StructField("DiagType", StringType(), True, metadata={"comment": "type of diagnosis e.g. Discharge"}),
        StructField("DiagCode", StringType(), True, metadata={"comment": "Diagnosis code"}),
        StructField("Vocab", StringType(), True, metadata={"comment": "Vocabulary of the code, e.g. SNOMED"}),
        StructField("Axis", StringType(), True, metadata={"comment": "Axis of the diagnosis, e.g. Finding, DIagnosis"}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])


@dlt.table(name="rde_pc_diagnosis_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def pc_diagnosis_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_pc_diagnosis")

    pc_diagnoses = spark.table("4_prod.raw.pc_diagnoses").alias("PR")
    patient_demographics = dlt.read("rde_patient_demographics").alias("E")
    person_alias = spark.table("4_prod.raw.mill_person_alias").alias("PA")

    return (
        pc_diagnoses.filter(col("ADC_UPDT") > max_adc_updt)
        .join(
        person_alias,
        (trim(pc_diagnoses.MRN) == trim(person_alias.ALIAS)) & 
        (person_alias.PERSON_ALIAS_TYPE_CD == 10),
        "inner"
        )
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


dlt.create_target_table(
    name = "rde_pc_diagnosis",
    comment=pc_diagnosis_comment,
    schema=schema_rde_pc_diagnosis,
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

pc_problems_comment = "Table of all problems recored in powerchart."

schema_rde_pc_problems = StructType([
        StructField("ProbID", StringType(), True, metadata={"comment": "Uniquely defines a problem within the problem table.  The problem_id can be  associated with multiple problem instances."}),
        StructField("Person_ID", StringType(), True, metadata={"comment": "This is the value of the unique primary identifier of the person table.  It is an internal system assigned number."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("Problem", StringType(), True, metadata={"comment": "Description of the problem"}),
        StructField("Annot_Disp", StringType(), True, metadata={"comment": "A de-normalized or annotated description of the problem.  This is defaulted from the display term of the selected codified problem, and can be extended (annotated) by the clinician.\
"}),
        StructField("Confirmation", StringType(), True, metadata={"comment": "Confirmation status eg:Complaining of ,Complaint of,      \
"}),
        StructField("Classification", StringType(), True, metadata={"comment": "Identifies the kind of problem.  Used to categorize the problem so that it may be managed and viewed independently within different applications.\
"}),
        StructField("OnsetDate", StringType(), True, metadata={"comment": "The date and time that the problem began.\
"}),
        StructField("StatusDate", StringType(), True, metadata={"comment": "Status last updated"}),
        StructField("Stat_LifeCycle", StringType(), True, metadata={"comment": "Indicate to what level of accuracy the life_cycle_dt_tm has been set\
"}),
        StructField("LifeCycleCancReson", StringType(), True, metadata={"comment": "reason for cancellation"}),
        StructField("Vocab", StringType(), True, metadata={"comment": "Vocabulary code identifier for the source concept"}),
        StructField("Axis", StringType(), True, metadata={"comment": "Axis of the concept, e.g. Finding, Body structure."}),
        StructField("SecDesc", StringType(), True, metadata={"comment": "secondary description"}),
        StructField("ProbCode", StringType(), True, metadata={"comment": "Problem code"}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])


@dlt.table(name="rde_pc_problems_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def pc_problems_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_pc_problems")

    pc_problems = spark.table("4_prod.raw.pc_problems").alias("PCP")
    patient_demographics = dlt.read("rde_patient_demographics").alias("E")
    person_alias = spark.table("4_prod.raw.mill_person_alias").alias("PA")

    return (
        pc_problems.filter(col("ADC_UPDT") > max_adc_updt)
        .join(
        person_alias,
        (trim(pc_problems.MRN) == trim(person_alias.ALIAS)) & 
        (person_alias.PERSON_ALIAS_TYPE_CD == 10),
        "inner"
        )
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


dlt.create_target_table(
    name = "rde_pc_problems",
    comment=pc_problems_comment,
    schema=schema_rde_pc_problems,
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

msds_booking_comment = "Table containing basic details about a pregnancy and delivery."
schema_rde_msds_booking = StructType([
        StructField("PERSON_ID", StringType(), True, metadata={"comment": "The unique identifier for each patient"}),
        StructField("PregnancyID", StringType(), True, metadata={"comment": "The unique identifier allocated to each Pregnancy Episode.\
"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Unique local identifier to identify the person"}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("FirstAntenatalAPPTDate", StringType(), True, metadata={"comment": "Date of first antenatal appointment."}),
        StructField("AlcoholUnitsPerWeek", IntegerType(), True, metadata={"comment": "Units of alcohol per week."}),
        StructField("SmokingStatusBooking", StringType(), True, metadata={"comment": "Smoking status at booking"}),
        StructField("SmokingStatusDelivery", StringType(), True, metadata={"comment": "Smoking status at delivery"}),
        StructField("SubstanceUse", StringType(), True, metadata={"comment": "Description of substance use"}),
        StructField("DeliveryDate", StringType(), True, metadata={"comment": "Expected delivery date."}),
        StructField("PostCode", StringType(), True, metadata={"comment": "Home address post code."}),
        StructField("Height_CM", FloatType(), True, metadata={"comment": "Height in cm."}),
        StructField("Weight_KG", FloatType(), True, metadata={"comment": "Weight in kg."}),
        StructField("BMI", FloatType(), True, metadata={"comment": "BMI"}),
        StructField("LaborOnsetMethod", StringType(), True, metadata={"comment": "Method for labor onset."}),
        StructField("Augmentation", StringType(), True, metadata={"comment": "If labor was augmented."}),
        StructField("AnalgesiaDelivery", StringType(), True, metadata={"comment": "Details of analgesia used during delivery"}),
        StructField("AnalgesiaLabour", StringType(), True, metadata={"comment": "Details of analgesia used during labor"}),
        StructField("AnaesthesiaDelivery", StringType(), True, metadata={"comment": "Details of anaesthesia used during delivery"}),
        StructField("AnaesthesiaLabour", StringType(), True, metadata={"comment": "Details of Anaesthesia used during labor."}),
        StructField("PerinealTrauma", StringType(), True, metadata={"comment": "Details of any perineal trauma from delivery."}),
        StructField("EpisiotomyDesc", StringType(), True, metadata={"comment": "Details of any episiotomy performed."}),
        StructField("BloodLoss", FloatType(), True, metadata={"comment": "Amount of blood lost."}),
        StructField("MSDS_AntenatalAPPTDate", StringType(), True, metadata={"comment": "Referred to as the Booking Appointment, the date on which the assessment for health and social care needs, risks and choices and arrangements made for antenatal care as part of the pregnancy episode was completed."}),
        StructField("MSDS_CompSocialFactor", StringType(), True, metadata={"comment": "Indicates if the mother is deemed to be subject to complex social factors, as defined by NICE guidance (CG110)."}),
        StructField("MSDS_DisabilityMother", StringType(), True, metadata={"comment": "An indication of whether a PERSON has been diagnosed as having a DISABILITY or perceives themselves to be disabled."}),
        StructField("MSDS_MatDischargeDate", StringType(), True, metadata={"comment": "Date on which mother ceased to be cared for in maternity services"}),
        StructField("MSDS_DischReason", StringType(), True, metadata={"comment": "The reason that the mother was discharged from maternity services."}),
        StructField("MSDS_EST_DELIVERYDATE_AGREED", StringType(), True, metadata={"comment": "The Estimated Date of Delivery, as agreed by ultrasound scan, LMP or Clinical Assessment."}),
        StructField("MSDS_METH_OF_EST_DELIVERY_DATE_AGREED", StringType(), True, metadata={"comment": "The method by which the Agreed Estimated Date of Delivery was calculated."}),
        StructField("MSDS_FolicAcidSupplement", StringType(), True, metadata={"comment": "Code for if mother given folic acid supplement."}),
        StructField("MSDS_LastMensturalPeriodDate", StringType(), True, metadata={"comment": "Date on which last menstrual period began"}),
        StructField("MSDS_PregConfirmed", StringType(), True, metadata={"comment": "Date pregenancy confirmed"}),
        StructField("MSDS_PrevC_Sections", StringType(), True, metadata={"comment": "The number of previous pregnancies where a baby was delivered via a caesarean (this is not the same as number of babies delivered via caesarean)."}),
        StructField("MSDS_PrevLiveBirths", StringType(), True, metadata={"comment": "The number live births from previous pregnancies"}),
        StructField("MSDS_PrevLossesLessThan24Weeks", StringType(), True, metadata={"comment": "The number of terminations and losses before 24 weeks of pregnancy (i.e. within <=23 weeks + 6 days)"}),
        StructField("MSDS_PrevStillBirths", StringType(), True, metadata={"comment": "The number stillbirths from previous pregnancies"}),
        StructField("MSDS_MothSuppStatusIND", StringType(), True, metadata={"comment": "As identified at the Booking Appointment, whether or not the mother feels she is supported in pregnancy and looking after a baby, from partner, family or friends."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_msds_booking_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def msds_booking_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_msds_booking")

    mat_pregnancy = spark.table("4_prod.raw.mat_pregnancy").alias("PREG").filter((col("DELETE_IND") == 0))
    patient_demographics = dlt.read("rde_patient_demographics").alias("DEM")
    msd101pregbook = spark.table("4_prod.raw.msds101pregbook").alias("MSDS")

    current_date_val = current_date()

    person_patient_address = (
        spark.table("4_prod.raw.mill_address")
        .filter((col("PARENT_ENTITY_NAME") == "PERSON") & (col("END_EFFECTIVE_DT_TM") > current_date_val))
        .withColumn("max_beg_effective_dt_tm", 
                spark_max("BEG_EFFECTIVE_DT_TM").over(Window.partitionBy("PARENT_ENTITY_ID")))
        .filter(col("BEG_EFFECTIVE_DT_TM") == col("max_beg_effective_dt_tm"))
        .drop("max_beg_effective_dt_tm")
        .alias("ADDR")
    )
    



    filtered_mat_pregnancy = mat_pregnancy.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_msd101pregbook = msd101pregbook.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_person_patient_address = person_patient_address.filter(col("ADC_UPDT") > max_adc_updt)


    relevant_pregnancy_ids = filtered_mat_pregnancy.select("PREGNANCY_ID").union(filtered_msd101pregbook.select("PREGNANCYID")).distinct()


    relevant_person_ids_from_pregnancy = mat_pregnancy.join(relevant_pregnancy_ids, "PREGNANCY_ID").select("PERSON_ID")


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


@dlt.view(name="msds_booking_update")
def msds_booking_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_msds_booking_incr")
    )


dlt.create_target_table(
    name = "rde_msds_booking",
    comment=msds_booking_comment,
    schema=schema_rde_msds_booking,
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

msds_carecontact_comment="Table containing details of each contact with care services for given pregnant person."
schema_rde_msds_carecontact = StructType([
        StructField("PERSON_ID", StringType(), True, metadata={"comment": "Uniquely identifies the patient."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Unique local identifier to identify the person"}),
        StructField("PregnancyID", StringType(), True, metadata={"comment": "The unique identifier allocated to each Pregnancy Episode.\
"}),
        StructField("CareConID", StringType(), True, metadata={"comment": "The CARE CONTACT IDENTIFIER is used to uniquely identify the CARE CONTACT within the Health Care Provider."}),
        StructField("CareConDate", StringType(), True, metadata={"comment": "The date on which a Care Contact took place, or, if cancelled, was scheduled to take place.\
"}),
        StructField("AdminCode", StringType(), True, metadata={"comment": "The PATIENT's ADMINISTRATIVE CATEGORY CODE may change during an episode or spell."}),
        StructField("Duration", StringType(), True, metadata={"comment": "CLINICAL CONTACT DURATION OF CARE CONTACT includes the time spent on the different CARE ACTIVITIES that may be performed in a single CARE CONTACT. The duration of each CARE ACTIVITY is recorded in CLINICAL CONTACT DURATION OF CARE ACTIVITY."}),
        StructField("ConsultType", StringType(), True, metadata={"comment": "This indicates the type of consultation for a SERVICE."}),
        StructField("Subject", StringType(), True, metadata={"comment": "The person who was the subject of the Care Contact."}),
        StructField("Medium", StringType(), True, metadata={"comment": "Identifies the communication mechanism used to relay information between the CARE PROFESSIONAL and the PERSON who is the subject of the consultation, during a CARE ACTIVITY."}),
        StructField("GPTherapyIND", StringType(), True, metadata={"comment": "An indicator of whether a Care Activity was delivered as Group Therapy."}),
        StructField("AttendCode", StringType(), True, metadata={"comment": "Indicates whether an APPOINTMENT for a CARE CONTACT took place and if the APPOINTMENT did not take place it whether advanced warning was given."}),
        StructField("CancelReason", StringType(), True, metadata={"comment": "The reason that a Care Contact was cancelled."}),
        StructField("CancelDate", StringType(), True, metadata={"comment": "The date that a Care Contact was cancelled by the Provider or Patient."}),
        StructField("RepAppOffDate", StringType(), True, metadata={"comment": "The replacement appointment date offered by the provider to the patient following the cancellation of an appointment by the SERVICE."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_msds_carecontact_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def msds_carecontact_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_msds_carecontact")

    msd201carecontactpreg = spark.table("4_prod.raw.msds201carecontactpreg").alias("CON")
    msds_booking = dlt.read("rde_msds_booking").alias("MB")


    filtered_carecontact = msd201carecontactpreg.filter(col("ADC_UPDT") > max_adc_updt)


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


dlt.create_target_table(
    name = "rde_msds_carecontact",
    comment=msds_carecontact_comment,
    schema=schema_rde_msds_carecontact,
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

msds_comment = "Details of a specific birth/delivery"

schema_rde_msds_delivery = StructType([
        StructField("Person_ID", StringType(), True, metadata={"comment": "The internal identifier for the person"}),
        StructField("PregnancyID", StringType(), True, metadata={"comment": "The unique identifier allocated to each Pregnancy Episode.\
"}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Unique local identifier to identify the person"}),
        StructField("BabyPerson_ID", StringType(), True, metadata={"comment": "Internal identifier for the baby."}),
        StructField("Baby_MRN", StringType(), True, metadata={"comment": "MRN for the baby."}),
        StructField("Baby_NHS", StringType(), True, metadata={"comment": "NHS Number for the baby."}),
        StructField("BirthOrder", IntegerType(), True, metadata={"comment": "Order in which this baby was born for this labor."}),
        StructField("BirthNumber", IntegerType(), True, metadata={"comment": "Total number of babies born this labor."}),
        StructField("BirthLocation", StringType(), True, metadata={"comment": "Location code for the birth."}),
        StructField("BirthDateTime", StringType(), True, metadata={"comment": "Date and time of the birth."}),
        StructField("DeliveryMethod", StringType(), True, metadata={"comment": "Method of delivery."}),
        StructField("DeliveryOutcome", StringType(), True, metadata={"comment": "Outcome of delivery."}),
        StructField("NeonatalOutcome", StringType(), True, metadata={"comment": "Outcome of neonatal care."}),
        StructField("PregOutcome", StringType(), True, metadata={"comment": "Outcome of pregnancy."}),
        StructField("PresDelDesc", StringType(), True, metadata={"comment": "Description of the presentation at delivery, e.g. vertex."}),
        StructField("BirthWeight", FloatType(), True, metadata={"comment": "Weight of baby."}),
        StructField("BirthSex", StringType(), True, metadata={"comment": "Sex of baby."}),
        StructField("APGAR1Min", IntegerType(), True, metadata={"comment": "APGAR score at 1 minute."}),
        StructField("APGAR5Min", IntegerType(), True, metadata={"comment": "APGAR score at 1 minute."}),
        StructField("FeedingMethod", StringType(), True, metadata={"comment": "Method of feeding."}),
        StructField("MotherComplications", StringType(), True, metadata={"comment": "Details of any complications for the mother."}),
        StructField("FetalComplications", StringType(), True, metadata={"comment": "Details of any comlplications of the baby."}),
        StructField("NeonatalComplications", StringType(), True, metadata={"comment": "Details of any neonatal complications."}),
        StructField("ResMethod", StringType(), True, metadata={"comment": "Resusitation method if applicatble."}),
        StructField("MSDS_LabourDelID", StringType(), True, metadata={"comment": "The unique identifier for a specific labour/ delivery. "}),
        StructField("MSDS_DeliverySite", StringType(), True, metadata={"comment": "Site code for the delivery."}),
        StructField("MSDS_BirthSetting", StringType(), True, metadata={"comment": "Code for place of birth."}),
        StructField("MSDS_BabyFirstFeedCode", StringType(), True, metadata={"comment": "Code for details of baby's first feeding."}),
        StructField("MSDS_SettingIntraCare", StringType(), True, metadata={"comment": "Code for intra care setting."}),
        StructField("MSDS_ReasonChangeDelSettingLab", StringType(), True, metadata={"comment": "Season for changing setting if applicable."}),
        StructField("MSDS_LabourOnsetMeth", StringType(), True, metadata={"comment": "Labour onset method"}),
        StructField("MSDS_LabOnsetDate", StringType(), True, metadata={"comment": "Date when established labour is confirmed - regular painful contractions and progressive cervical dilatation"}),
        StructField("MSDS_CSectionDate", StringType(), True, metadata={"comment": "The date of the caesarean section (i.e. date of Knife to skin)"}),
        StructField("MSDS_DecDeliveryDate", StringType(), True, metadata={"comment": "The date on which the decision was made to deliver the baby (where an emergency caesarean or other assisted delivery is required)."}),
        StructField("MSDS_AdmMethCodeMothDelHSP", StringType(), True, metadata={"comment": ""}),
        StructField("MSDS_DischDate", StringType(), True, metadata={"comment": "Date on which mother was discharged from hospital following completion of labour and delivery."}),
        StructField("MSDS_DischMeth", StringType(), True, metadata={"comment": "Discharge method"}),
        StructField("MSDS_DischDest", StringType(), True, metadata={"comment": "discharge destination"}),
        StructField("MSDS_RomDate", StringType(), True, metadata={"comment": "Rupture of membranes date if applicable."}),
        StructField("MSDS_RomMeth", StringType(), True, metadata={"comment": "Method for rupture of membranes."}),
        StructField("MSDS_RomReason", StringType(), True, metadata={"comment": "Text for reason for rupture of membranes."}),
        StructField("MSDS_EpisiotomyReason", StringType(), True, metadata={"comment": "Any reason provided for sugical episiotomy."}),
        StructField("MSDS_PlancentaDelMeth", StringType(), True, metadata={"comment": "Whether placenta was removed through physiological, active or manual means.  Where more than one method is used, the final method is used"}),
        StructField("MSDS_LabOnsetPresentation", StringType(), True, metadata={"comment": "The presentation of the fetus at onset of labour"}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_msds_delivery_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def msds_delivery_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_msds_delivery")

    mat_birth = spark.table("4_prod.raw.mat_birth").alias("BIRTH")
    mat_pregnancy = spark.table("4_prod.raw.mat_pregnancy").alias("MOTHER")
    patient_demographics = dlt.read("rde_patient_demographics").alias("DEM")
    msd301labdel = spark.table("4_prod.raw.msds301labdel").alias("MSDS")
    msd401babydemo = spark.table("4_prod.raw.msds401babydemo").alias("MSDBABY")


    filtered_mat_birth = mat_birth.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_mat_pregnancy = mat_pregnancy.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_msd301labdel = msd301labdel.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_msd401babydemo = msd401babydemo.filter(col("ADC_UPDT") > max_adc_updt)

    relevant_pregnancy_ids = (
        filtered_mat_birth.select("PREGNANCY_ID")
        .union(filtered_mat_pregnancy.select("PREGNANCY_ID"))
        .union(filtered_msd301labdel.select("PREGNANCYID"))
        .distinct()
    )

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


dlt.create_target_table(
    name = "rde_msds_delivery",
    comment=msds_comment,
    schema=schema_rde_msds_delivery,
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

msds_diagnosis_comment="Table containing details of any diagnosis of the mother."
schema_rde_msds_diagnosis = StructType([
        StructField("PERSON_ID", StringType(), True, metadata={"comment": "Unique identifier of the patient."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Unique local identifier to identify the person"}),
        StructField("DiagPregID", StringType(), True, metadata={"comment": "The unique identifier allocated to each Pregnancy Episode.\
"}),
        StructField("DiagScheme", StringType(), True, metadata={"comment": "The code scheme basis of a diagnosis."}),
        StructField("Diagnosis", StringType(), True, metadata={"comment": "This is the DIAGNOSIS of the person, from a specific classification or clinical terminology, for the main condition treated or investigated during the relevant episode of healthcare."}),
        StructField("DiagDate", StringType(), True, metadata={"comment": "The date of the primary diagnosis."}),
        StructField("LocalFetalID", StringType(), True, metadata={"comment": "The unique identifier allocated to a fetus, which remains consistent throughout the pregnancy."}),
        StructField("FetalOrder", StringType(), True, metadata={"comment": "The order or sequence in which the Fetus was assessed. The FETAL ORDER is represented by a single numeric value, with 1 indicating the first or only Fetus assessed in the sequence, 2 indicating the second, and so on."}),
        StructField("SnomedCD", StringType(), True, metadata={"comment": "Snomed diagnosis code"}),
        StructField("DiagDesc", StringType(), True, metadata={"comment": "Description of primary diagnosis"}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_msds_diagnosis_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def msds_diagnosis_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_msds_diagnosis")

    msd106diagnosispreg = spark.table("4_prod.raw.msds106diagnosispreg").alias("DIAG")
    msds_booking = dlt.read("rde_msds_booking").alias("PREG")
    lkp_mill_dir_snomed = spark.table("3_lookup.dwh.lkp_mill_dir_snomed").alias("S")
    mat_pregnancy = spark.table("4_prod.raw.mat_pregnancy").alias("MAT_PREG")
    patient_demographics = dlt.read("rde_patient_demographics").alias("DEM")


    filtered_msd106diagnosispreg = msd106diagnosispreg.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_mat_pregnancy = mat_pregnancy.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_lkp_mill_dir_snomed = lkp_mill_dir_snomed.filter(col("ADC_UPDT") > max_adc_updt)


    relevant_pregnancy_ids = (
        filtered_msd106diagnosispreg.select("PREGNANCYID")
        .union(filtered_mat_pregnancy.select("PREGNANCY_ID"))
        .distinct()
    ).alias("REL_PREG")


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


dlt.create_target_table(
    name = "rde_msds_diagnosis",
    comment=msds_diagnosis_comment,
    schema=schema_rde_msds_diagnosis,
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
    keys = ["PERSON_ID", "DiagPregID", "Diagnosis", "LocalFetalID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

allergy_comment = "Table containing any known allergies of the patient."

schema_rde_allergydetails = StructType([
        StructField("AllergyID", StringType(), True, metadata={"comment": "Uniquely defines an allergy/adverse reaction within the allergy table. The allergy_id can be associated with multiple allergy instances. When  allergy is added to the allergy table the allergy_id is assigned to allergy_instance_id"}),
        StructField("PERSON_ID", StringType(), True, metadata={"comment": "Unique identifier of the patient."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("SubstanceFTDesc", StringType(), True, metadata={"comment": "A free text description of the substance decoded on SUBSTANCE_NOM_ID"}),
        StructField("SubstanceDesc", StringType(), True, metadata={"comment": "A free text description of the substance"}),
        StructField("SubstanceDispTxt", StringType(), True, metadata={"comment": "A free display text of the substance "}),
        StructField("SubstanceValueTxt", StringType(), True, metadata={"comment": "value/code of  substance "}),
        StructField("SubstanceType", StringType(), True, metadata={"comment": "Identifies the type of substance, eg. drug, food, contrast, environment"}),
        StructField("ReactionType", StringType(), True, metadata={"comment": "Identifies the type of reaction, eg. Allergy, adverse drug reaction."}),
        StructField("Severity", StringType(), True, metadata={"comment": "Indicates the general severity of the allergy or reaction."}),
        StructField("SourceInfo", StringType(), True, metadata={"comment": "Identifies the source of the information regarding the reaction, eg. Provider, parent, chart, interface."}),
        StructField("OnsetDT", StringType(), True, metadata={"comment": "The date that the reaction was identified."}),
        StructField("ReactionStatus", StringType(), True, metadata={"comment": "The status of the reaction, eg. confirmed, cancelled, proposed, working, suspected."}),
        StructField("CreatedDT", StringType(), True, metadata={"comment": "The date and time that the allergy/adverse reaction was entered on the allergy profile."}),
        StructField("CancelReason", StringType(), True, metadata={"comment": "Identifies the reason why a reaction has been cancelled."}),
        StructField("CancelDT", StringType(), True, metadata={"comment": "The date and time that the allergy was set to a status of cancelled."}),
        StructField("ActiveStatus", StringType(), True, metadata={"comment": "Indicates the status of the row itself (not the data in the row) such as active, inactive, combined away, pending purge,etc."}),
        StructField("ActiveDT", StringType(), True, metadata={"comment": "The date and time that the active_status_cd was set."}),
        StructField("BegEffecDT", StringType(), True, metadata={"comment": "The date and time for which this table row becomes effective. Normally, this will be the date and time the row is added, but could be a past or date and time."}),
        StructField("EndEffecDT", StringType(), True, metadata={"comment": "The date/time after which the row is no longer valid as active current data. This may be valued with the date that the row became inactive. The date that the reaction was identified."}),
        StructField("DataStatus", StringType(), True, metadata={"comment": "Data status indicates a level of authenticity of the row data. Typically this will either be AUTHENTICATED or UNAUTHENTICATED."}),
        StructField("DataStatusDT", StringType(), True, metadata={"comment": "The date and time that the data_status_cd was set."}),
        StructField("VocabDesc", StringType(), True, metadata={"comment": "Original source vocabulary that the allergy was received with from an interface."}),
        StructField("PrecisionDesc", StringType(), True, metadata={"comment": "Indicates to what precision (not entered, age, about, before, after, unknown) the onset_dt_tm has been set."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])
    
@dlt.table(name="rde_allergydetails_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def allergydetails_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_allergydetails")

    allergy = spark.table("4_prod.raw.mill_allergy").alias("A")
    encounter = dlt.read("rde_encounter").alias("ENC")
    code_value_ref = spark.table("3_lookup.dwh.pi_cde_code_value_ref")
    nomenclature_ref = spark.table("3_lookup.mill.mill_nomenclature").alias("Det")

    return (
        allergy.filter(col("ADC_UPDT") > max_adc_updt)
        .join(encounter, (col("A.ENCNTR_ID") == col("ENC.ENCNTR_ID")) & (col("A.PERSON_ID") == col("ENC.PERSON_ID")), "inner")
        .join(code_value_ref.alias("Stat"), col("A.DATA_STATUS_CD") == col("Stat.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("Prec"), col("A.ONSET_PRECISION_CD") == col("Prec.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("Reac"), col("A.REACTION_CLASS_CD") == col("Reac.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ReacStat"), col("A.REACTION_STATUS_CD") == col("ReacStat.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("Vocab"), col("A.REC_SRC_VOCAB_CD") == col("Vocab.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("Sub"), col("A.SUBSTANCE_TYPE_CD") == col("Sub.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("Seve"), col("A.SEVERITY_CD") == col("Seve.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("Sorc"), col("A.SOURCE_OF_INFO_CD") == col("Sorc.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("Creas"), col("A.CANCEL_REASON_CD") == col("Creas.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("Activ"), col("A.ACTIVE_STATUS_CD") == col("Activ.CODE_VALUE_CD"), "left")
        .join(nomenclature_ref, col("A.SUBSTANCE_NOM_ID") == col("Det.NOMENCLATURE_ID"), "left")
        .select(
            col("A.ALLERGY_ID").cast(StringType()).alias("AllergyID"),
            col("ENC.PERSON_ID").cast(StringType()).alias("PERSON_ID"),
            col("ENC.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("ENC.MRN").cast(StringType()).alias("MRN"),
            col("A.SUBSTANCE_FTDESC").cast(StringType()).alias("SubstanceFTDesc"),
            col("Det.SOURCE_STRING").cast(StringType()).alias("SubstanceDesc"),
            col("Det.SHORT_STRING").cast(StringType()).alias("SubstanceDispTxt"),
            col("Det.SOURCE_IDENTIFIER").cast(StringType()).alias("SubstanceValueTxt"),
            col("Sub.CODE_DESC_TXT").cast(StringType()).alias("SubstanceType"),
            col("Reac.CODE_DESC_TXT").cast(StringType()).alias("ReactionType"),
            col("Seve.CODE_DESC_TXT").cast(StringType()).alias("Severity"),
            col("Sorc.CODE_DESC_TXT").cast(StringType()).alias("SourceInfo"),
            col("A.ONSET_DT_TM").cast(StringType()).alias("OnsetDT"),
            col("ReacStat.CODE_DESC_TXT").cast(StringType()).alias("ReactionStatus"),
            col("A.CREATED_DT_TM").cast(StringType()).alias("CreatedDT"),
            col("Creas.CODE_DESC_TXT").cast(StringType()).alias("CancelReason"),
            col("A.CANCEL_DT_TM").cast(StringType()).alias("CancelDT"),
            col("Activ.CODE_DESC_TXT").cast(StringType()).alias("ActiveStatus"),
            col("A.ACTIVE_STATUS_DT_TM").cast(StringType()).alias("ActiveDT"),
            col("A.BEG_EFFECTIVE_DT_TM").cast(StringType()).alias("BegEffecDT"),
            col("A.END_EFFECTIVE_DT_TM").cast(StringType()).alias("EndEffecDT"),
            col("Stat.CODE_DESC_TXT").cast(StringType()).alias("DataStatus"),
            col("A.DATA_STATUS_DT_TM").cast(StringType()).alias("DataStatusDT"),
            col("Vocab.CODE_DESC_TXT").cast(StringType()).alias("VocabDesc"),
            col("Prec.CODE_DESC_TXT").cast(StringType()).alias("PrecisionDesc"),
            greatest(col("A.ADC_UPDT"), col("ENC.ADC_UPDT"), col("Det.ADC_UPDT")).alias("ADC_UPDT")
        )
        .filter(col("ADC_UPDT") > max_adc_updt)
    )

@dlt.view(name="allergydetails_update")
def allergydetails_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_allergydetails_incr")
    )


dlt.create_target_table(
    name = "rde_allergydetails",
    comment=allergy_comment,
    schema=schema_rde_allergydetails,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "NHS_Number,AllergyID"
    }
)

dlt.apply_changes(
    target = "rde_allergydetails",
    source = "allergydetails_update",
    keys = ["NHS_Number", "AllergyID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

scr_demo_comment = "Table containing demographic details of individuals in the Sommerset Cancer Registry"
schema_rde_scr_demographics = StructType([
        StructField("PATIENTID", StringType(), True, metadata={"comment": "Unique identifier for patients used in SCR."}),
        StructField("PERSON_ID", StringType(), True, metadata={"comment": "Unique Barts identifier for the patient."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "NHS Number of the patient."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local Barts hospital identifier for the patient."}),
        StructField("DeathDate", StringType(), True, metadata={"comment": "Date of death if applicable."}),
        StructField("DeathCause", StringType(), True, metadata={"comment": "Text on cause of death if known."}),
        StructField("PT_AT_RISK", StringType(), True, metadata={"comment": "True false if patient considered at risk."}),
        StructField("REASON_RISK", StringType(), True, metadata={"comment": "If patient at risk, free text comment as to reason."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_scr_demographics_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def scr_demographics_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_scr_demographics")

    scr_demographics = spark.table("4_prod.ancil_scr.scr_tbldemographics").alias("SCR")
    patient_demographics = dlt.read("rde_patient_demographics").alias("PAT")
    person_alias = spark.table("4_prod.raw.mill_person_alias").alias("ALIAS")

    return (
        scr_demographics.filter(col("ADC_UPDT") > max_adc_updt)
        .join(person_alias, 
              (col("SCR.N1_1_NHS_NUMBER") == col("ALIAS.alias")) |
              (col("SCR.N1_2_HOSPITAL_NUMBER") == col("ALIAS.alias")),
              "left")
        .join(patient_demographics, 
              (col("ALIAS.PERSON_ID") == col("PAT.PERSON_ID")),
              "inner")
        .select(
            col("SCR.PATIENT_ID").cast(StringType()).alias("PATIENTID"),
            col("PAT.PERSON_ID").cast(StringType()).alias("PERSON_ID"),
            col("PAT.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("PAT.MRN").cast(StringType()).alias("MRN"),
            col("SCR.N15_1_DATE_DEATH").cast(StringType()).alias("DeathDate"),
            col("SCR.N15_3_DEATH_CAUSE").cast(StringType()).alias("DeathCause"),
            col("SCR.PT_AT_RISK").cast(StringType()).alias("PT_AT_RISK"),
            col("SCR.REASON_RISK").cast(StringType()).alias("REASON_RISK"),
            greatest(col("SCR.ADC_UPDT"), col("PAT.ADC_UPDT")).alias("ADC_UPDT")
        )
        .filter(col("ADC_UPDT") > max_adc_updt)
    )

# The rest of the code remains the same
@dlt.view(name="scr_demographics_update")
def scr_demographics_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_scr_demographics_incr")
    )


dlt.create_target_table(
    name = "rde_scr_demographics",
    comment=scr_demo_comment,
    schema=schema_rde_scr_demographics,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "NHS_Number,PATIENTID"
    }
)

dlt.apply_changes(
    target = "rde_scr_demographics",
    source = "scr_demographics_update",
    keys = ["PATIENTID", "PERSON_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

scr_referrals_comment = "Referrals for individuals in the Sommerset Cancer Registry"
schema_rde_scr_referrals = StructType([
        StructField("CareID", StringType(), True, metadata={"comment": "Unique SCR identifier for care pathway."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local Barts hospital number."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "Patient NHS Number"}),
        StructField("PATIENT_ID", StringType(), True, metadata={"comment": "Unique Barts Patient ID"}),
        StructField("CancerSite", StringType(), True, metadata={"comment": "Site of the cancer, e.g. skin"}),
        StructField("PriorityDesc", StringType(), True, metadata={"comment": "Priority description, e.g. Routine"}),
        StructField("DecisionDate", StringType(), True, metadata={"comment": "Date decision was made."}),
        StructField("ReceiptDate", StringType(), True, metadata={"comment": "Date of receipt of decision."}),
        StructField("DateSeenFirst", StringType(), True, metadata={"comment": "Date person first seen by specialist."}),
        StructField("CancerType", StringType(), True, metadata={"comment": "Details on type of cancer, e.g. suspected skin cancers."}),
        StructField("StatusDesc", StringType(), True, metadata={"comment": "Description of status, e.g. Non-Cancer"}),
        StructField("FirstAppt", StringType(), True, metadata={"comment": "1 if first appointment, 2 if not."}),
        StructField("DiagDate", StringType(), True, metadata={"comment": "Date of diagnosis if any."}),
        StructField("DiagCode", StringType(), True, metadata={"comment": "ICD10 code of diagnosis."}),
        StructField("DiagDesc", StringType(), True, metadata={"comment": "Text description of the diagnosis."}),
        StructField("OtherDiagDate", StringType(), True, metadata={"comment": "Date for additional diagnosis"}),
        StructField("Laterality", StringType(), True, metadata={"comment": "Laterality of cancer if known, e.g. Right."}),
        StructField("DiagBasis", StringType(), True, metadata={"comment": "SCR code for basis of diagnosis."}),
        StructField("Histology", StringType(), True, metadata={"comment": "Code for histology results."}),
        StructField("Differentiation", StringType(), True, metadata={"comment": "Coded result for cancer differentiation"}),
        StructField("ClinicalTStage", StringType(), True, metadata={"comment": "Number for clinical stage."}),
        StructField("ClinicalTCertainty", StringType(), True, metadata={"comment": "Code for clinical certainty."}),
        StructField("ClinicalNStage", StringType(), True, metadata={"comment": "Code for clinical N stage."}),
        StructField("ClinicalNCertainty", StringType(), True, metadata={"comment": "Code for clinical N certainty."}),
        StructField("ClinicalMStage", StringType(), True, metadata={"comment": "Code for clinical M stage"}),
        StructField("ClinicalMCertainty", StringType(), True, metadata={"comment": "Code for clinical M certainty."}),
        StructField("PathologicalTCertainty", StringType(), True, metadata={"comment": "Code for pathological T certainty."}),
        StructField("PathologicalTStage", StringType(), True, metadata={"comment": "Code for pathological T stage."}),
        StructField("PathologicalNCertainty", StringType(), True, metadata={"comment": "Code for pathological N certainty."}),
        StructField("PathologicalNStage", StringType(), True, metadata={"comment": "Code for pathological N stage."}),
        StructField("PathologicalMCertainty", StringType(), True, metadata={"comment": "Code for pathological M certainty."}),
        StructField("PathologicalMStage", StringType(), True, metadata={"comment": "Code for pathological M stage."}),
        StructField("TumourStatus", StringType(), True, metadata={"comment": "Tumor status code."}),
        StructField("TumourDesc", StringType(), True, metadata={"comment": "Description of the tumor."}),
        StructField("NonCancer", StringType(), True, metadata={"comment": "Free text description of non cancer referral details."}),
        StructField("CRecurrence", StringType(), True, metadata={"comment": "Any text descriptio of recurrence."}),
        StructField("RefComments", StringType(), True, metadata={"comment": "Any comments attached to the referral."}),
        StructField("DecisionReason", StringType(), True, metadata={"comment": "Any text provided to support descision."}),
        StructField("TreatReason", StringType(), True, metadata={"comment": "Any text provided to support treatment reason."}),
        StructField("RecSiteID", StringType(), True, metadata={"comment": "ID for rec site."}),
        StructField("NewTumourSite", StringType(), True, metadata={"comment": "Any text about new tumor site."}),
        StructField("ActionID", StringType(), True, metadata={"comment": "SCR ID fo action."}),
        StructField("SnomedCD", StringType(), True, metadata={"comment": "SCR ID for snomed code."}),
        StructField("SubSiteID", StringType(), True, metadata={"comment": "SCR ID for subsite."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_scr_referrals_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def scr_referrals_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_scr_referrals")

    scr_referrals = spark.table("4_prod.ancil_scr.scr_tblmain_referrals").alias("REF")
    scr_demographics = dlt.read("rde_scr_demographics").alias("D")
    priority_type = spark.table("4_prod.ancil_scr.scr_ltblpriority_type").alias("PT")
    ca_status = spark.table("4_prod.ancil_scr.scr_ltblca_status").alias("CA")
    cancer_type = spark.table("4_prod.ancil_scr.scr_ltblcancer_type").alias("TYP")
    diagnosis = spark.table("4_prod.ancil_scr.scr_ltbldiagnosis").alias("DIA")
    laterality = spark.table("4_prod.ancil_scr.scr_ltbllaterality").alias("LA")
    differentiation = spark.table("4_prod.ancil_scr.scr_ltbldifferentiation").alias("DIF")
    tumour_status = spark.table("4_prod.ancil_scr.scr_ltbltumour_status").alias("TS")


    scr_data = (
        scr_referrals
        .join(scr_demographics, col("REF.PATIENT_ID") == col("D.PATIENTID"), "inner")
        .filter((col("REF.ADC_UPDT") > max_adc_updt) | (col("D.ADC_UPDT") > max_adc_updt))
    )

    return (
        scr_data
        .join(priority_type, col("REF.N2_4_PRIORITY_TYPE") == col("PT.PRIORITY_CODE"), "left")
        .join(ca_status, col("REF.N2_13_CANCER_STATUS") == col("CA.STATUS_CODE"), "left")
        .join(cancer_type, col("REF.N2_12_CANCER_TYPE") == col("TYP.CANCER_TYPE_CODE"), "left")
        .join(diagnosis, col("REF.N4_2_DIAGNOSIS_CODE") == col("DIA.DIAG_CODE"), "left")
        .join(laterality, col("REF.N4_3_LATERALITY") == col("LA.LAT_CODE"), "left")
        .join(differentiation, col("REF.N4_6_DIFFERENTIATION") == col("DIF.GRADE_CODE"), "left")
        .join(tumour_status, col("REF.L_TUMOUR_STATUS") == col("TS.TUMOUR_CODE"), "left")
        .select(
            col("REF.CARE_ID").cast(StringType()).alias("CareID"),
            col("D.MRN").cast(StringType()).alias("MRN"),
            col("D.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("REF.PATIENT_ID").cast(StringType()).alias("PATIENT_ID"),
            col("REF.L_CANCER_SITE").cast(StringType()).alias("CancerSite"),
            col("PT.PRIORITY_DESC").cast(StringType()).alias("PriorityDesc"),
            col("REF.N2_5_DECISION_DATE").cast(StringType()).alias("DecisionDate"),
            col("REF.N2_6_RECEIPT_DATE").cast(StringType()).alias("ReceiptDate"),
            col("REF.N2_9_FIRST_SEEN_DATE").cast(StringType()).alias("DateSeenFirst"),
            col("TYP.CANCER_TYPE_DESC").cast(StringType()).alias("CancerType"),
            col("CA.STATUS_DESC").cast(StringType()).alias("StatusDesc"),
            col("REF.L_FIRST_APPOINTMENT").cast(StringType()).alias("FirstAppt"),
            col("REF.N4_1_DIAGNOSIS_DATE").cast(StringType()).alias("DiagDate"),
            col("REF.N4_2_DIAGNOSIS_CODE").cast(StringType()).alias("DiagCode"),
            col("DIA.DIAG_DESC").cast(StringType()).alias("DiagDesc"),
            col("REF.L_OTHER_DIAG_DATE").cast(StringType()).alias("OtherDiagDate"),
            concat_ws("- ", col("REF.N4_3_LATERALITY"), col("LA.LAT_DESC")).cast(StringType()).alias("Laterality"),
            col("REF.N4_4_BASIS_DIAGNOSIS").cast(StringType()).alias("DiagBasis"),
            col("REF.N4_5_HISTOLOGY").cast(StringType()).alias("Histology"),
            concat_ws("- ", col("REF.N4_6_DIFFERENTIATION"), col("DIF.GRADE_DESC")).cast(StringType()).alias("Differentiation"),
            col("REF.ClinicalTStage").cast(StringType()).alias("ClinicalTStage"),
            col("REF.ClinicalTCertainty").cast(StringType()).alias("ClinicalTCertainty"),
            col("REF.ClinicalNStage").cast(StringType()).alias("ClinicalNStage"),
            col("REF.ClinicalNCertainty").cast(StringType()).alias("ClinicalNCertainty"),
            col("REF.ClinicalMStage").cast(StringType()).alias("ClinicalMStage"),
            col("REF.ClinicalMCertainty").cast(StringType()).alias("ClinicalMCertainty"),
            col("REF.PathologicalTCertainty").cast(StringType()).alias("PathologicalTCertainty"),
            col("REF.PathologicalTStage").cast(StringType()).alias("PathologicalTStage"),
            col("REF.PathologicalNCertainty").cast(StringType()).alias("PathologicalNCertainty"),
            col("REF.PathologicalNStage").cast(StringType()).alias("PathologicalNStage"),
            col("REF.PathologicalMCertainty").cast(StringType()).alias("PathologicalMCertainty"),
            col("REF.PathologicalMStage").cast(StringType()).alias("PathologicalMStage"),
            col("REF.L_TUMOUR_STATUS").cast(StringType()).alias("TumourStatus"),
            col("TS.TUMOUR_DESC").cast(StringType()).alias("TumourDesc"),
            col("REF.L_NON_CANCER").cast(StringType()).alias("NonCancer"),
            col("REF.L_RECURRENCE").cast(StringType()).alias("CRecurrence"),
            col("REF.L_COMMENTS").cast(StringType()).alias("RefComments"),
            col("REF.N16_7_DECISION_REASON").cast(StringType()).alias("DecisionReason"),
            col("REF.N16_8_TREATMENT_REASON").cast(StringType()).alias("TreatReason"),
            col("REF.RECURRENCE_CANCER_SITE_ID").cast(StringType()).alias("RecSiteID"),
            col("REF.TUMOUR_SITE_NEW").cast(StringType()).alias("NewTumourSite"),
            col("REF.ACTION_ID").cast(StringType()).alias("ActionID"),
            col("REF.SNOMed_CT").cast(StringType()).alias("SnomedCD"),
            col("REF.SubsiteID").cast(StringType()).alias("SubSiteID"),
            greatest(col("REF.ADC_UPDT"), col("D.ADC_UPDT")).alias("ADC_UPDT")
        )
        .filter(col("ADC_UPDT") > max_adc_updt)
    )


@dlt.view(name="scr_referrals_update")
def scr_referrals_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_scr_referrals_incr")
    )


dlt.create_target_table(
    name = "rde_scr_referrals",
    comment=scr_referrals_comment,
    schema=schema_rde_scr_referrals,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "NHS_Number,CareID"
    }
)

dlt.apply_changes(
    target = "rde_scr_referrals",
    source = "scr_referrals_update",
    keys = ["CareID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

scr_comments_comment = "Comments for patients in the Sommerset Cancer Registry"
schema_rde_scr_trackingcomments = StructType([
        StructField("MRN", StringType(), True, metadata={"comment": "Local Barts hospital ID."}),
        StructField("COM_ID", StringType(), True, metadata={"comment": "SCR comment ID."}),
        StructField("CareID", StringType(), True, metadata={"comment": "SCR care pathway ID"}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "NHS NUmber for patient."}),
        StructField("Date_Time", StringType(), True, metadata={"comment": "Date and time of comment."}),
        StructField("Comments", StringType(), True, metadata={"comment": "Free text of the comment."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_scr_trackingcomments_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def scr_trackingcomments_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_scr_trackingcomments")

    scr_tracking_comments = spark.table("4_prod.ancil_scr.scr_tbltracking_comments").alias("C")
    scr_referrals = dlt.read("rde_scr_referrals").alias("R")

    return (
        scr_tracking_comments
        .join(scr_referrals, col("C.CARE_ID") == col("R.CareID"), "inner")
        .filter((col("C.ADC_UPDT") > max_adc_updt) | (col("R.ADC_UPDT") > max_adc_updt))
        .select(
            col("R.MRN").cast(StringType()).alias("MRN"),
            col("C.COM_ID").cast(StringType()).alias("COM_ID"),
            col("C.CARE_ID").cast(StringType()).alias("CareID"),
            col("R.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("C.DATE_TIME").cast(StringType()).alias("Date_Time"),
            col("C.COMMENTS").cast(StringType()).alias("Comments"),
            greatest(col("C.ADC_UPDT"), col("R.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="scr_trackingcomments_update")
def scr_trackingcomments_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_scr_trackingcomments_incr")
    )


dlt.create_target_table(
    name = "rde_scr_trackingcomments",
    comment=scr_comments_comment,
    schema=schema_rde_scr_trackingcomments,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "NHS_Number,CareID,COM_ID"
    }
)

dlt.apply_changes(
    target = "rde_scr_trackingcomments",
    source = "scr_trackingcomments_update",
    keys = ["CareID", "COM_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

scr_careplan_comment = "Details on the agreed care plan for patients in the Sommerset Cancer Registry"
schema_rde_scr_careplan = StructType([
        StructField("PlanID", StringType(), True, metadata={"comment": "SCR ID for the care plan."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Barts Local hospital ID."}),
        StructField("CareID", StringType(), True, metadata={"comment": "SCR ID for the care pathway."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "NHS NUmber of patient."}),
        StructField("MDTDate", StringType(), True, metadata={"comment": "Date of cancer multi disciplinary team meeting."}),
        StructField("CareIntent", StringType(), True, metadata={"comment": "Code for care intention."}),
        StructField("TreatType", StringType(), True, metadata={"comment": "Code for tareatment type"}),
        StructField("WHOStatus", StringType(), True, metadata={"comment": "Code for WHOStatus"}),
        StructField("PlanType", StringType(), True, metadata={"comment": "Code for Plan type"}),
        StructField("Network", StringType(), True, metadata={"comment": "Yes/no for network."}),
        StructField("NetworkDate", StringType(), True, metadata={"comment": "Date of network"}),
        StructField("AgreedCarePlan", StringType(), True, metadata={"comment": "Y/n for agreed care plan."}),
        StructField("MDTSite", StringType(), True, metadata={"comment": "SCR code for the MDT site."}),
        StructField("MDTComments", StringType(), True, metadata={"comment": "Any free text comments from the MDT meeting."}),
        StructField("NetworkFeedback", StringType(), True, metadata={"comment": "Any freetext feedback from network."}),
        StructField("NetworkComments", StringType(), True, metadata={"comment": "Any freetext comments from network."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_scr_careplan_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def scr_careplan_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_scr_careplan")

    scr_careplan = spark.table("4_prod.ancil_scr.scr_tblmain_care_plan").alias("CP")
    scr_referrals = dlt.read("rde_scr_referrals").alias("R")

    return (
        scr_careplan
        .join(scr_referrals, col("CP.CARE_ID") == col("R.CareID"), "inner")
        .filter((col("CP.ADC_UPDT") > max_adc_updt) | (col("R.ADC_UPDT") > max_adc_updt))
        .select(
            col("CP.PLAN_ID").cast(StringType()).alias("PlanID"),
            col("R.MRN").cast(StringType()).alias("MRN"),
            col("CP.CARE_ID").cast(StringType()).alias("CareID"),
            col("R.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("CP.N5_2_MDT_DATE").cast(StringType()).alias("MDTDate"),
            col("CP.N5_5_CARE_INTENT").cast(StringType()).alias("CareIntent"),
            col("CP.N5_6_TREATMENT_TYPE_1").cast(StringType()).alias("TreatType"),
            col("CP.N5_10_WHO_STATUS").cast(StringType()).alias("WHOStatus"),
            col("CP.N_L28_PLAN_TYPE").cast(StringType()).alias("PlanType"),
            col("CP.L_NETWORK").cast(StringType()).alias("Network"),
            col("CP.L_DATE_NETWORK_MEETING").cast(StringType()).alias("NetworkDate"),
            col("CP.L_CARE_PLAN_AGREED").cast(StringType()).alias("AgreedCarePlan"),
            col("CP.L_MDT_SITE").cast(StringType()).alias("MDTSite"),
            col("CP.L_MDT_COMMENTS").cast(StringType()).alias("MDTComments"),
            col("CP.L_NETWORK_FEEDBACK").cast(StringType()).alias("NetworkFeedback"),
            col("CP.L_NETWORK_COMMENTS").cast(StringType()).alias("NetworkComments"),
            greatest(col("CP.ADC_UPDT"), col("R.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="scr_careplan_update")
def scr_careplan_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_scr_careplan_incr")
    )


dlt.create_target_table(
    name = "rde_scr_careplan",
    comment=scr_careplan_comment,
    schema=schema_rde_scr_careplan,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "NHS_Number,CareID,PlanID"
    }
)

dlt.apply_changes(
    target = "rde_scr_careplan",
    source = "scr_careplan_update",
    keys = ["CareID", "PlanID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

scr_treatment_comment = "Details on cancer treaments for individuals in the Sommerset Cancer Registry"
schema_rde_scr_deftreatment = StructType([
        StructField("TreatmentID", StringType(), True, metadata={"comment": "SCR id for the treatment."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local Barts hospital identifier."}),
        StructField("CareID", StringType(), True, metadata={"comment": "SCR ID For the care plan."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "NHS Number for the patient."}),
        StructField("DecisionDate", StringType(), True, metadata={"comment": "Date of treatment decision."}),
        StructField("StartDate", StringType(), True, metadata={"comment": "Date of start of treatment."}),
        StructField("Treatment", StringType(), True, metadata={"comment": "code for treatment type."}),
        StructField("TreatEvent", StringType(), True, metadata={"comment": "Code for type of treatment event."}),
        StructField("TreatSetting", StringType(), True, metadata={"comment": "Code for type of treatment setting."}),
        StructField("TPriority", StringType(), True, metadata={"comment": "Code for treatment priority."}),
        StructField("Intent", StringType(), True, metadata={"comment": "Code for treatment intent."}),
        StructField("TreatNo", StringType(), True, metadata={"comment": "Sequential number of treatment."}),
        StructField("TreatID", StringType(), True, metadata={"comment": "SCR ID for treatment."}),
        StructField("ChemoRT", StringType(), True, metadata={"comment": "Code for chemotherapy."}),
        StructField("DelayComments", StringType(), True, metadata={"comment": "Any comments about any delay."}),
        StructField("DEPRECATEDComments", StringType(), True, metadata={"comment": "Any attached comments."}),
        StructField("DEPRECATEDAllComments", StringType(), True, metadata={"comment": "Concatenation of all comments."}),
        StructField("RootTCIComments", StringType(), True, metadata={"comment": "Free text of any root ci comments."}),
        StructField("ROOT_DATE_COMMENTS", StringType(), True, metadata={"comment": "Date of root ci comments."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_scr_deftreatment_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def scr_deftreatment_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_scr_deftreatment")

    scr_deftreatment = spark.table("4_prod.ancil_scr.scr_tbldefinitive_treatment").alias("DT")
    scr_referrals = dlt.read("rde_scr_referrals").alias("R")

    return (
        scr_deftreatment
        .join(scr_referrals, col("DT.CARE_ID") == col("R.CareID"), "inner")
        .filter((col("DT.ADC_UPDT") > max_adc_updt) | (col("R.ADC_UPDT") > max_adc_updt))
        .select(
            col("DT.TREATMENT_ID").cast(StringType()).alias("TreatmentID"),
            col("R.MRN").cast(StringType()).alias("MRN"),
            col("DT.CARE_ID").cast(StringType()).alias("CareID"),
            col("R.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("DT.DECISION_DATE").cast(StringType()).alias("DecisionDate"),
            col("DT.START_DATE").cast(StringType()).alias("StartDate"),
            col("DT.TREATMENT").cast(StringType()).alias("Treatment"),
            col("DT.TREATMENT_EVENT").cast(StringType()).alias("TreatEvent"),
            col("DT.TREATMENT_SETTING").cast(StringType()).alias("TreatSetting"),
            col("DT.RT_PRIORITY").cast(StringType()).alias("TPriority"),
            col("DT.RT_INTENT").cast(StringType()).alias("Intent"),
            col("DT.TREAT_NO").cast(StringType()).alias("TreatNo"),
            col("DT.TREAT_ID").cast(StringType()).alias("TreatID"),
            col("DT.CHEMO_RT").cast(StringType()).alias("ChemoRT"),
            col("DT.DELAY_COMMENTS").cast(StringType()).alias("DelayComments"),
            col("DT.DEPRECATED_21_01_COMMENTS").cast(StringType()).alias("DEPRECATEDComments"),
            col("DT.DEPRECATED_21_01_ALL_COMMENTS").cast(StringType()).alias("DEPRECATEDAllComments"),
            col("DT.ROOT_TCI_COMMENTS").cast(StringType()).alias("RootTCIComments"),
            col("DT.ROOT_DTT_DATE_COMMENTS").cast(StringType()).alias("ROOT_DATE_COMMENTS"),
            greatest(col("DT.ADC_UPDT"), col("R.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="scr_deftreatment_update")
def scr_deftreatment_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_scr_deftreatment_incr")
    )


dlt.create_target_table(
    name = "rde_scr_deftreatment",
    comment=scr_treatment_comment,
    schema=schema_rde_scr_deftreatment,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "NHS_Number,CareID,TreatmentID"
    }
)

dlt.apply_changes(
    target = "rde_scr_deftreatment",
    source = "scr_deftreatment_update",
    keys = ["CareID", "TreatmentID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

scr_diagnosis_comment = "Diagnoses for individuals in the Sommerset Cancer Registry"
schema_rde_scr_diagnosis = StructType([
        StructField("CareID", StringType(), True, metadata={"comment": "SCR ID for care plan."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Barts Local Hospital ID"}),
        StructField("CancerSite", StringType(), True, metadata={"comment": "Site of cancer, e.g. Skin"}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "Nuhs number of patient."}),
        StructField("HospitalNumber", StringType(), True, metadata={"comment": "Barts Local Hospital ID"}),
        StructField("PatientStatus", StringType(), True, metadata={"comment": "Free text for status of patient."}),
        StructField("TumourStatus", StringType(), True, metadata={"comment": "Text status of tumour, e.g. Primary"}),
        StructField("NewTumourSite", StringType(), True, metadata={"comment": "Text details of new tumour site."}),
        StructField("DiagDate", StringType(), True, metadata={"comment": "Date of diagnosis."}),
        StructField("DatePatInformed", StringType(), True, metadata={"comment": "Date patient was informed."}),
        StructField("PrimDiagICD", StringType(), True, metadata={"comment": "ICD10 code for primary diagnosis."}),
        StructField("PrimDiagSnomed", StringType(), True, metadata={"comment": "SNOMED code for primary diagnosis."}),
        StructField("SecDiag", StringType(), True, metadata={"comment": "ICD10 code for secondary diagnosis."}),
        StructField("Laterality", StringType(), True, metadata={"comment": "Laterality of the cancer. e.g. Right"}),
        StructField("NonCancerdet", StringType(), True, metadata={"comment": "Any free text details of diagnosis being non cancer or other patient inadmissability."}),
        StructField("DiagBasis", StringType(), True, metadata={"comment": "Basis for the diagnosis, e.g. Clinical investigation."}),
        StructField("Histology", StringType(), True, metadata={"comment": "Code for Histology of the tumour."}),
        StructField("Differentiation", StringType(), True, metadata={"comment": "Any text details about differentiation."}),
        StructField("Comments", StringType(), True, metadata={"comment": "Any free text commands on the diagnosis."}),
        StructField("PathwayEndFaster", StringType(), True, metadata={"comment": "Date for end of pathway."}),
        StructField("PathwayEndReason", StringType(), True, metadata={"comment": "Reason for the end of the pathway."}),
        StructField("PrimCancerSite", StringType(), True, metadata={"comment": "Location of the primary cancer, e.g. Lower Gastroinestinal."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_scr_diagnosis_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def scr_diagnosis_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_scr_diagnosis")

    scr_diagnosis = spark.table("4_prod.ancil_scr.scr_bivwdiagnosis").alias("DIAG")
    scr_demographics = dlt.read("rde_scr_demographics").alias("D")

    return (
        scr_diagnosis
        .join(scr_demographics, col("DIAG.NHS_Number") == col("D.NHS_Number"), "inner")
        .filter((col("DIAG.ADC_UPDT") > max_adc_updt) | (col("D.ADC_UPDT") > max_adc_updt))
        .select(
            col("DIAG.CARE_ID").cast(StringType()).alias("CareID"),
            col("D.MRN").cast(StringType()).alias("MRN"),
            col("DIAG.Cancer_Site").cast(StringType()).alias("CancerSite"),
            col("D.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("DIAG.Hospital_Number").cast(StringType()).alias("HospitalNumber"),
            col("DIAG.Patient_Status").cast(StringType()).alias("PatientStatus"),
            col("DIAG.Tumour_Status").cast(StringType()).alias("TumourStatus"),
            col("DIAG.New_Tumour_Site").cast(StringType()).alias("NewTumourSite"),
            col("DIAG.Date_of_Diagnosis").cast(StringType()).alias("DiagDate"),
            col("DIAG.Date_Patient_Informed").cast(StringType()).alias("DatePatInformed"),
            col("DIAG.Primary_Diagnosis__ICD_").cast(StringType()).alias("PrimDiagICD"),
            col("DIAG.Primary_Diagnosis__SNOMED_").cast(StringType()).alias("PrimDiagSnomed"),
            col("DIAG.Secondary_Diagnosis").cast(StringType()).alias("SecDiag"),
            col("DIAG.Laterality").cast(StringType()).alias("Laterality"),
            col("DIAG.Non_cancer_details").cast(StringType()).alias("NonCancerdet"),
            col("DIAG.Basis_of_Diagnosis").cast(StringType()).alias("DiagBasis"),
            col("DIAG.Histology").cast(StringType()).alias("Histology"),
            col("DIAG.Grade_of_Differentiation").cast(StringType()).alias("Differentiation"),
            col("DIAG.Comments").cast(StringType()).alias("Comments"),
            col("DIAG.Pathway_End_Date__Faster_Diagnosis_").cast(StringType()).alias("PathwayEndFaster"),
            col("DIAG.Pathway_End_Reason__Faster_Diagnosis_").cast(StringType()).alias("PathwayEndReason"),
            col("DIAG.Primary_Cancer_Site").cast(StringType()).alias("PrimCancerSite"),
            greatest(col("DIAG.ADC_UPDT"), col("D.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="scr_diagnosis_update")
def scr_diagnosis_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_scr_diagnosis_incr")
    )


dlt.create_target_table(
    name = "rde_scr_diagnosis",
    comment=scr_diagnosis_comment,
    schema=schema_rde_scr_diagnosis,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "NHS_Number,CareID"
    }
)

dlt.apply_changes(
    target = "rde_scr_diagnosis",
    source = "scr_diagnosis_update",
    keys = ["CareID", "NHS_Number"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

scr_investigations_comment = "Details of investigations undertaken for patients in the Sommerset Cancer Registry"
schema_rde_scr_investigations = StructType([
        StructField("CareID", StringType(), True, metadata={"comment": "SCR ID for care plan"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local Barts Hospital ID"}),
        StructField("CancerSite", StringType(), True, metadata={"comment": "Site of cancer, e.g. Lung"}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "NHS number of patient."}),
        StructField("HospitalNumber", StringType(), True, metadata={"comment": "Local Barts Hospital ID"}),
        StructField("DiagInvestigation", StringType(), True, metadata={"comment": "Method of investigation, e.g. PET scan."}),
        StructField("ReqDate", StringType(), True, metadata={"comment": "Requested date of investigation"}),
        StructField("DatePerformed", StringType(), True, metadata={"comment": "Date investigation performed"}),
        StructField("DateReported", StringType(), True, metadata={"comment": "Date report submitted"}),
        StructField("BiopsyTaken", StringType(), True, metadata={"comment": "Yes/No on if biopsy taken."}),
        StructField("Outcome", StringType(), True, metadata={"comment": "Outcome status of the investigation, e.g. Performed"}),
        StructField("Comments", StringType(), True, metadata={"comment": "Any free text comments attached to the investigation."}),
        StructField("NICIPCode", StringType(), True, metadata={"comment": "NICIP procedure code for the investiation. "}),
        StructField("SnomedCT", StringType(), True, metadata={"comment": "Any associated SNOMED code."}),
        StructField("AnotomicalSite", StringType(), True, metadata={"comment": "Site of the investigation, e.g. Whole Body Imaging"}),
        StructField("AnatomicalSide", StringType(), True, metadata={"comment": "Side of investigation if applicable, e.g. Left"}),
        StructField("ImagingReport", StringType(), True, metadata={"comment": "Text of the report from the investigation."}),
        StructField("StagingLaproscopyPerformed", StringType(), True, metadata={"comment": "Yes/No on if staging laproscopy performed."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_scr_investigations_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def scr_investigations_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_scr_investigations")

    scr_investigations = spark.table("4_prod.ancil_scr.scr_bivwinvestigations").alias("INV")
    scr_demographics = dlt.read("rde_scr_demographics").alias("D")

    return (
        scr_investigations
        .join(scr_demographics, col("INV.NHS_Number") == col("D.NHS_Number"), "inner")
        .filter((col("INV.ADC_UPDT") > max_adc_updt) | (col("D.ADC_UPDT") > max_adc_updt))
        .select(
            col("INV.CARE_ID").cast(StringType()).alias("CareID"),
            col("D.MRN").cast(StringType()).alias("MRN"),
            col("INV.Cancer_Site").cast(StringType()).alias("CancerSite"),
            col("D.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("INV.Hospital_Number").cast(StringType()).alias("HospitalNumber"),
            col("INV.Diagnostic_Investigation").cast(StringType()).alias("DiagInvestigation"),
            col("INV.Date_Requested").cast(StringType()).alias("ReqDate"),
            col("INV.Date_Performed").cast(StringType()).alias("DatePerformed"),
            col("INV.Reported_Date").cast(StringType()).alias("DateReported"),
            col("INV.Biopsy_Taken").cast(StringType()).alias("BiopsyTaken"),
            col("INV.Outcome").cast(StringType()).alias("Outcome"),
            col("INV.Comments").cast(StringType()).alias("Comments"),
            col("INV.Imaging_Code_NICIP_").cast(StringType()).alias("NICIPCode"),
            col("INV.Imaging_Code__SNOMed_CT_").cast(StringType()).alias("SnomedCT"),
            col("INV.Anatomical_Site_1").cast(StringType()).alias("AnotomicalSite"),
            col("INV.Anatomical_Side").cast(StringType()).alias("AnatomicalSide"),
            col("INV.Imaging_Report_Text").cast(StringType()).alias("ImagingReport"),
            col("INV.Staging_Laparoscopy_Performed").cast(StringType()).alias("StagingLaproscopyPerformed"),
            greatest(col("INV.ADC_UPDT"), col("D.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="scr_investigations_update")
def scr_investigations_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_scr_investigations_incr")
    )


dlt.create_target_table(
    name = "rde_scr_investigations",
    comment=scr_investigations_comment,
    schema=schema_rde_scr_investigations,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "NHS_Number,CareID"
    }
)

dlt.apply_changes(
    target = "rde_scr_investigations",
    source = "scr_investigations_update",
    keys = ["CareID", "NHS_Number", "DiagInvestigation"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

scr_pathology_comment = "Details of pathology performed for patients in the Sommerset Cancer Registry"
schema_rde_scr_pathology = StructType([
        StructField("PathologyID", StringType(), True, metadata={"comment": "SCR pathology result identifier."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local Barts Hospital ID"}),
        StructField("CareID", StringType(), True, metadata={"comment": "SCR care plan identifier."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "NHS Number for the patient."}),
        StructField("PathologyType", StringType(), True, metadata={"comment": "Code for type of pathology."}),
        StructField("ResultDate", StringType(), True, metadata={"comment": "Date for receiging pathology results."}),
        StructField("ExcisionMargins", StringType(), True, metadata={"comment": "Code for excision margins"}),
        StructField("Nodes", StringType(), True, metadata={"comment": "Number of nodes."}),
        StructField("PositiveNodes", StringType(), True, metadata={"comment": "Number of positive nodes."}),
        StructField("PathTstage", StringType(), True, metadata={"comment": "Number for stage in the T pathology."}),
        StructField("PathNstage", StringType(), True, metadata={"comment": "Number for the stage in the N pathology."}),
        StructField("PathMstage", StringType(), True, metadata={"comment": "Number for the stage in the M pathology."}),
        StructField("Comments", StringType(), True, metadata={"comment": "Any attached free text comment."}),
        StructField("SampleDate", StringType(), True, metadata={"comment": "Date the sample was taken."}),
        StructField("PathologyReport", StringType(), True, metadata={"comment": "Text of the report"}),
        StructField("SNomedCT", StringType(), True, metadata={"comment": "SCR ID for snomed concept."}),
        StructField("SNomedID", StringType(), True, metadata={"comment": "Code for snomed oncept."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_scr_pathology_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def scr_pathology_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_scr_pathology")

    scr_pathology = spark.table("4_prod.ancil_scr.scr_tblmain_pathology").alias("P")
    scr_referrals = dlt.read("rde_scr_referrals").alias("R")

    return (
        scr_pathology
        .join(scr_referrals, col("P.CARE_ID") == col("R.CareID"), "inner")
        .filter((col("P.ADC_UPDT") > max_adc_updt) | (col("R.ADC_UPDT") > max_adc_updt))
        .select(
            col("P.PATHOLOGY_ID").cast(StringType()).alias("PathologyID"),
            col("R.MRN").cast(StringType()).alias("MRN"),
            col("P.CARE_ID").cast(StringType()).alias("CareID"),
            col("R.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("P.N8_1_PATHOLOGY_TYPE").cast(StringType()).alias("PathologyType"),
            col("P.N8_3_RESULT_DATE").cast(StringType()).alias("ResultDate"),
            col("P.N8_13_EXCISION_MARGINS").cast(StringType()).alias("ExcisionMargins"),
            col("P.N8_14_NODES").cast(StringType()).alias("Nodes"),
            col("P.N8_15_POSITIVE_NODES").cast(StringType()).alias("PositiveNodes"),
            col("P.N8_16_PATH_T_STAGE").cast(StringType()).alias("PathTstage"),
            col("P.N8_17_PATH_N_STAGE").cast(StringType()).alias("PathNstage"),
            col("P.N8_18_PATH_M_STAGE").cast(StringType()).alias("PathMstage"),
            col("P.L_COMMENTS").cast(StringType()).alias("Comments"),
            col("P.SAMPLE_DATE").cast(StringType()).alias("SampleDate"),
            col("P.L_PATHOLOGY_TEXT").cast(StringType()).alias("PathologyReport"),
            col("P.SNOMedCT").cast(StringType()).alias("SNomedCT"),
            col("P.SNOMEDDiagnosisID").cast(StringType()).alias("SNomedID"),
            greatest(col("P.ADC_UPDT"), col("R.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="scr_pathology_update")
def scr_pathology_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_scr_pathology_incr")
    )


dlt.create_target_table(
    name = "rde_scr_pathology",
    comment=scr_pathology_comment,
    schema=schema_rde_scr_pathology,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "NHS_Number,CareID,PathologyID"
    }
)

dlt.apply_changes(
    target = "rde_scr_pathology",
    source = "scr_pathology_update",
    keys = ["CareID", "PathologyID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

powerforms_comment = "Contains details of differnet types of powerforms completed for a particular patient."
schema_rde_powerforms = StructType([
        StructField("PERSON_ID", StringType(), True, metadata={"comment": "Unique identifer of the patient."}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("ENCNTR_ID", StringType(), True, metadata={"comment": "This is the value of the unique primary identifier of the encounter table."}),
        StructField("PerformDate", StringType(), True, metadata={"comment": "Activity performed date and time"}),
        StructField("DOC_RESPONSE_KEY", StringType(), True, metadata={"comment": "Unique identifier for this powerform event."}),
        StructField("Form", StringType(), True, metadata={"comment": "Name of the form."}),
        StructField("FormID", LongType(), True, metadata={"comment": "This is the event_id from CLINICAL_EVENT. For imported data (for which there actually is no HNAM form/event) it?s a negative number corresponding to an external index system."}),
        StructField("Section", StringType(), True, metadata={"comment": "Name of the section"}),
        StructField("SectionID", LongType(), True, metadata={"comment": "Identifies a documentation set section"}),
        StructField("Element", StringType(), True, metadata={"comment": "Name of the element"}),
        StructField("ElementID", LongType(), True, metadata={"comment": "Identifies a documentation set element"}),
        StructField("Component", StringType(), True, metadata={"comment": "Name of the component."}),
        StructField("ComponentDesc", StringType(), True, metadata={"comment": "Description of the component"}),
        StructField("ComponentID", LongType(), True, metadata={"comment": "Identifies a documentation set component"}),
        StructField("Response", StringType(), True, metadata={"comment": "Response input into the form for the specific component, element, section, form."}),
        StructField("ResponseNumeric", IntegerType(), True, metadata={"comment": "1 if response is a number, otherwise 0."}),
        StructField("Status", StringType(), True, metadata={"comment": "Status of the results on the form"}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_powerforms_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def powerforms_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_powerforms")

    doc_response = spark.table("4_prod.raw.pi_cde_doc_response").alias("DOC")
    encounter = dlt.read("rde_encounter").alias("Enc")
    doc_ref = spark.table("3_lookup.dwh.pi_lkp_cde_doc_ref").alias("Dref")
    code_value_ref = spark.table("3_lookup.dwh.pi_cde_code_value_ref").alias("Cref")


    filtered_doc_response = doc_response.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_doc_ref = doc_ref.filter(col("ADC_UPDT") > max_adc_updt)
    filtered_code_value_ref = code_value_ref.filter(col("ADC_UPDT") > max_adc_updt)

    relevant_encounter_ids = filtered_doc_response.select("ENCNTR_ID").distinct()

    return (
        doc_response
        .join(relevant_encounter_ids, "ENCNTR_ID", "inner")
        .join(encounter, "ENCNTR_ID", "inner")
        .join(doc_ref, col("DOC.DOC_INPUT_ID") == col("Dref.DOC_INPUT_KEY"), "left")
        .join(code_value_ref, col("DOC.FORM_STATUS_CD") == col("Cref.CODE_VALUE_CD"), "left")
        .select(
            col("Enc.PERSON_ID").cast(StringType()).alias("PERSON_ID"),
            col("Enc.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("Enc.MRN").cast(StringType()).alias("MRN"),
            col("Enc.ENCNTR_ID").cast(StringType()).alias("ENCNTR_ID"),
            col("DOC.PERFORMED_DT_TM").cast(StringType()).alias("PerformDate"),
            col("DOC.DOC_RESPONSE_KEY").cast(StringType()).alias("DOC_RESPONSE_KEY"),
            col("Dref.FORM_DESC_TXT").cast(StringType()).alias("Form"),
            col("DOC.FORM_EVENT_ID").cast(LongType()).alias("FormID"),
            col("Dref.SECTION_DESC_TXT").cast(StringType()).alias("Section"),
            col("DOC.SECTION_EVENT_ID").cast(LongType()).alias("SectionID"),
            col("Dref.ELEMENT_LABEL_TXT").cast(StringType()).alias("Element"),
            col("DOC.ELEMENT_EVENT_ID").cast(LongType()).alias("ElementID"),
            col("Dref.GRID_NAME_TXT").cast(StringType()).alias("Component"),
            col("Dref.GRID_COLUMN_DESC_TXT").cast(StringType()).alias("ComponentDesc"),
            col("DOC.GRID_EVENT_ID").cast(LongType()).alias("ComponentID"),
            col("DOC.RESPONSE_VALUE_TXT").cast(StringType()).alias("Response"),
            when(col("DOC.RESPONSE_VALUE_TXT").cast("double").isNotNull(), lit(1)).otherwise(lit(0)).alias("ResponseNumeric"),
            col("Cref.CODE_DESC_TXT").cast(StringType()).alias("Status"),
            greatest(col("DOC.ADC_UPDT"), col("Enc.ADC_UPDT"), col("Dref.ADC_UPDT"), col("Cref.ADC_UPDT")).alias("ADC_UPDT")
        )
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


dlt.create_target_table(
    name = "rde_powerforms",
    comment=powerforms_comment,
    schema=schema_rde_powerforms,
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

powertrials_comment = "Table of details of individuals registered on a study in powertrials."
schema_rde_mill_powertrials = StructType([
        StructField("PERSONID", StringType(), True, metadata={"comment": "Uniquely identifies individual in millenium."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("NHS_NUMBER", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("Study_Code", StringType(), True, metadata={"comment": "Code for study in powertrials"}),
        StructField("Study_Name", StringType(), True, metadata={"comment": "Name of study in powertrials"}),
        StructField("Study_Participant_ID", StringType(), True, metadata={"comment": "ID for participant within the given study."}),
        StructField("On_Study_Date", StringType(), True, metadata={"comment": "Date participant joined the study."}),
        StructField("Off_Study_Date", StringType(), True, metadata={"comment": "Date participant left the study."}),
        StructField("Off_Study_Code", StringType(), True, metadata={"comment": "Code for reason for leaving the study if applicabl.e"}),
        StructField("Off_Study_Reason", StringType(), True, metadata={"comment": "Text for the code for leaving the study if applicable."}),
        StructField("Off_Study_Comment", StringType(), True, metadata={"comment": "Free text comment for reason for leaving the study."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_mill_powertrials_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def mill_powertrials_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_mill_powertrials")

    mill_pt_prot_reg = spark.table("4_prod.raw.mill_pt_prot_reg").alias("RES")
    mill_pt_prot_master = spark.table("4_prod.raw.mill_prot_master").alias("STUDYM")
    code_value_ref = spark.table("3_lookup.dwh.pi_cde_code_value_ref").alias("LOOK")
    patient_demographics = dlt.read("rde_patient_demographics").alias("PDEM")


    window_spec = Window.partitionBy("RES.PROT_MASTER_ID", "RES.PERSON_ID").orderBy(desc("RES.BEG_EFFECTIVE_DT_TM"))

    return (
        mill_pt_prot_reg
        .join(patient_demographics, col("RES.PERSON_ID") == col("PDEM.PERSON_ID"), "inner")
        .join(mill_pt_prot_master, col("RES.PROT_MASTER_ID") == col("STUDYM.PROT_MASTER_ID"), "left")
        .join(code_value_ref, col("RES.REMOVAL_REASON_CD").cast("string") == col("LOOK.CODE_VALUE_CD").cast("string"), "left")
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .filter((col("RES.ADC_UPDT") > max_adc_updt) | (col("PDEM.ADC_UPDT") > max_adc_updt) | (col("STUDYM.ADC_UPDT") > max_adc_updt))
        .select(
            col("PDEM.PERSON_ID").cast(StringType()).alias("PERSONID"),
            col("PDEM.MRN").cast(StringType()).alias("MRN"),
            col("PDEM.NHS_Number").cast(StringType()).alias("NHS_NUMBER"),
            col("RES.PROT_MASTER_ID").cast(StringType()).alias("Study_Code"),
            col("STUDYM.PRIMARY_MNEMONIC").cast(StringType()).alias("Study_Name"),
            col("RES.PROT_ACCESSION_NBR").cast(StringType()).alias("Study_Participant_ID"),
            col("RES.ON_STUDY_DT_TM").cast(StringType()).alias("On_Study_Date"),
            col("RES.OFF_STUDY_DT_TM").cast(StringType()).alias("Off_Study_Date"),
            col("RES.REMOVAL_REASON_CD").cast(StringType()).alias("Off_Study_Code"),
            col("LOOK.CODE_DESC_TXT").cast(StringType()).alias("Off_Study_Reason"),
            col("RES.REMOVAL_REASON_DESC").cast(StringType()).alias("Off_Study_Comment"),
            greatest(col("RES.ADC_UPDT"), col("PDEM.ADC_UPDT"), col("STUDYM.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="mill_powertrials_update")
def mill_powertrials_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_mill_powertrials_incr")
    )


dlt.create_target_table(
    name = "rde_mill_powertrials",
    comment=powertrials_comment,
    schema=schema_rde_mill_powertrials,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSONID,Study_Code"
    }
)

dlt.apply_changes(
    target = "rde_mill_powertrials",
    source = "mill_powertrials_update",
    keys = ["PERSONID", "Study_Code"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

aliases_comment = "Table of alternative identifiers such as NHS Number or MRN for all the patients in the cohort."
schema_rde_aliases = StructType([
        StructField("PERSONID", StringType(), True, metadata={"comment": "Uniquely identifies individual in millenium."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("NHS_NUMBER", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("CodeType", StringType(), True, metadata={"comment": "Type of code, NHS Number or MRN"}),
        StructField("Code", StringType(), True, metadata={"comment": "Alphanumeric of code."}),
        StructField("IssueDate", StringType(), True, metadata={"comment": "Date the code started being used."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_aliases_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def aliases_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_aliases")

    person_alias = spark.table("4_prod.raw.mill_person_alias").alias("AL")
    patient_demographics = dlt.read("rde_patient_demographics").alias("PAT")

    return (
        person_alias
        .join(patient_demographics, col("AL.PERSON_ID") == col("PAT.PERSON_ID"), "inner")
        .filter((col("AL.ADC_UPDT") > max_adc_updt) | (col("PAT.ADC_UPDT") > max_adc_updt))
        .filter(col("AL.ACTIVE_IND") == 1)  # Only include active aliases
        .filter((col("AL.PERSON_ALIAS_TYPE_CD") == 18) | (col("AL.PERSON_ALIAS_TYPE_CD") == 10))  # NHS Number or MRN
        .filter((col("AL.ALIAS") != col("PAT.MRN")) & (col("AL.ALIAS") != col("PAT.NHS_Number")))  # Exclude current MRN and NHS Number
        .select(
            col("AL.PERSON_ID").cast(StringType()).alias("PERSONID"),
            col("PAT.MRN").cast(StringType()).alias("MRN"),
            col("PAT.NHS_Number").cast(StringType()).alias("NHS_NUMBER"),
            when(col("AL.PERSON_ALIAS_TYPE_CD") == 18, "NHS_Number")
            .when(col("AL.PERSON_ALIAS_TYPE_CD") == 10, "MRN")
            .otherwise(None).cast(StringType()).alias("CodeType"),
            col("AL.ALIAS").cast(StringType()).alias("Code"),
            col("AL.BEG_EFFECTIVE_DT_TM").cast(StringType()).alias("IssueDate"),
            greatest(col("AL.ADC_UPDT"), col("PAT.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="aliases_update")
def aliases_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_aliases_incr")
    )


dlt.create_target_table(
    name = "rde_aliases",
    comment=aliases_comment,
    schema=schema_rde_aliases,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSONID,CodeType,Code"
    }
)

dlt.apply_changes(
    target = "rde_aliases",
    source = "aliases_update",
    keys = ["PERSONID", "CodeType", "Code"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

critactivity_comment = "Table of activities occuring in the critical care pathway for a given patient."
schema_rde_critactivity = StructType([
        StructField("PERSONID", StringType(), True, metadata={"comment": "The unique identifier for each patient"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local Identifier"}),
        StructField("NHS_NUMBER", StringType(), True, metadata={"comment": "NHS Number"}),
        StructField("Period_ID", StringType(), True, metadata={"comment": "ID for the crit care period."}),
        StructField("CDS_APC_ID", StringType(), True, metadata={"comment": "ID for the CDS"}),
        StructField("ActivityDate", StringType(), True, metadata={"comment": "Date of the activity."}),
        StructField("ActivityCode", IntegerType(), True, metadata={"comment": "Code for the activity."}),
        StructField("ActivityDesc", StringType(), True, metadata={"comment": "Text description of the code."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_critactivity_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def critactivity_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_critactivity")

    crit_care_activity = spark.table("4_prod.raw.crit_care_activity").alias("a")
    nhs_data_dict_ref = spark.table("3_lookup.dwh.nhs_data_dct_ref_deprecated").alias("ref")
    patient_demographics = dlt.read("rde_patient_demographics").alias("DEM")

    return (
        crit_care_activity
        .join(patient_demographics, col("a.mrn") == col("DEM.MRN"), "inner")
        .join(nhs_data_dict_ref, 
              (col("a.Activity_Code") == col("ref.NHS_DATA_DICT_NHS_CD_ALIAS")) & 
              (col("ref.NHS_DATA_DICT_ELEMENT_NAME_KEY_TXT") == 'CRITICALCAREACTIVITY'),
              "left")
        .filter((col("a.ADC_UPDT") > max_adc_updt) | (col("DEM.ADC_UPDT") > max_adc_updt))
        .select(
            col("DEM.PERSON_ID").cast(StringType()).alias("PERSONID"),
            col("DEM.MRN").cast(StringType()).alias("MRN"),
            col("DEM.NHS_Number").cast(StringType()).alias("NHS_NUMBER"),
            col("a.CC_Period_Local_Id").cast(StringType()).alias("Period_ID"),
            col("a.CDS_APC_ID").cast(StringType()).alias("CDS_APC_ID"),
            col("a.Activity_Date").cast(StringType()).alias("ActivityDate"),
            col("a.Activity_Code").cast(IntegerType()).alias("ActivityCode"),
            col("ref.NHS_DATA_DICT_DESCRIPTION_TXT").cast(StringType()).alias("ActivityDesc"),
            greatest(col("a.ADC_UPDT"), col("DEM.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="critactivity_update")
def critactivity_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_critactivity_incr")
    )


dlt.create_target_table(
    name = "rde_critactivity",
    comment=critactivity_comment,
    schema=schema_rde_critactivity,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSONID,Period_ID,ActivityDate,ActivityCode"
    }
)

dlt.apply_changes(
    target = "rde_critactivity",
    source = "critactivity_update",
    keys = ["PERSONID", "Period_ID", "ActivityDate", "ActivityCode"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

critperiod_comment = "Table of critical care periods for a given patient."
schema_rde_critperiod = StructType([
        StructField("PERSONID", StringType(), True, metadata={"comment": "The unique identifier for each patient"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local Identifier"}),
        StructField("NHS_NUMBER", StringType(), True, metadata={"comment": "NHS Number"}),
        StructField("Period_ID", StringType(), True, metadata={"comment": "ID For the crit care period."}),
        StructField("StartDate", StringType(), True, metadata={"comment": "Date the period starts."}),
        StructField("DischargeDate", StringType(), True, metadata={"comment": "Date of discharge from critical care."}),
        StructField("Level_2_Days", IntegerType(), True, metadata={"comment": "Days in level 2."}),
        StructField("Level_3_Days", IntegerType(), True, metadata={"comment": "Days in level 3."}),
        StructField("Dischage_Dest_CD", IntegerType(), True, metadata={"comment": "Code for the discharge destination."}),
        StructField("Discharge_destination", StringType(), True, metadata={"comment": "Text description of the discharge destination."}),
        StructField("Adv_Cardio_Days", IntegerType(), True, metadata={"comment": "Days in Advanced cardio unit."}),
        StructField("Basic_Cardio_Days", IntegerType(), True, metadata={"comment": "Days in basic cardio unit."}),
        StructField("Adv_Resp_Days", IntegerType(), True, metadata={"comment": "Days in advanced respiritory unit."}),
        StructField("Basic_Resp_Days", IntegerType(), True, metadata={"comment": "Days in basic respitory unit."}),
        StructField("Renal_Days", IntegerType(), True, metadata={"comment": "Days in Renal Unit."}),
        StructField("Neuro_Days", IntegerType(), True, metadata={"comment": "Days in Neuro Unit."}),
        StructField("Gastro_Days", IntegerType(), True, metadata={"comment": "Days in Gastro unit."}),
        StructField("Derm_Days", IntegerType(), True, metadata={"comment": "Days in dermatology Unit."}),
        StructField("Liver_Days", IntegerType(), True, metadata={"comment": "Days in Liver unit."}),
        StructField("No_Organ_Systems", IntegerType(), True, metadata={"comment": "Number of organ systems."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_critperiod_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def critperiod_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_critperiod")

    crit_care_period = spark.table("4_prod.raw.crit_care_period").alias("a")
    nhs_data_dict_ref = spark.table("3_lookup.dwh.nhs_data_dct_ref_deprecated").alias("ref")
    patient_demographics = dlt.read("rde_patient_demographics").alias("DEM")

    return (
        crit_care_period
        .join(patient_demographics, col("a.mrn") == col("DEM.MRN"), "inner")
        .join(nhs_data_dict_ref, 
              (col("a.CC_Disch_Dest_Cd") == col("ref.NHS_DATA_DICT_NHS_CD_ALIAS")) & 
              (col("ref.NHS_DATA_DICT_ELEMENT_NAME_KEY_TXT") == 'CRITICALCAREDISCHDESTINATION'),
              "left")
        .filter((col("a.ADC_UPDT") > max_adc_updt) | (col("DEM.ADC_UPDT") > max_adc_updt))
        .select(
            col("DEM.PERSON_ID").cast(StringType()).alias("PERSONID"),
            col("DEM.MRN").cast(StringType()).alias("MRN"),
            col("DEM.NHS_Number").cast(StringType()).alias("NHS_NUMBER"),
            col("a.CC_Period_Local_Id").cast(StringType()).alias("Period_ID"),
            col("a.CC_Period_Start_Dt_Tm").cast(StringType()).alias("StartDate"),
            col("a.CC_Period_Disch_Dt_Tm").cast(StringType()).alias("DischargeDate"),
            col("a.CC_Level2_Days").cast(IntegerType()).alias("Level_2_Days"),
            col("a.CC_Level3_Days").cast(IntegerType()).alias("Level_3_Days"),
            col("a.CC_Disch_Dest_Cd").cast(IntegerType()).alias("Dischage_Dest_CD"),
            col("ref.NHS_DATA_DICT_DESCRIPTION_TXT").cast(StringType()).alias("Discharge_destination"),
            col("a.CC_Adv_Cardio_Days").cast(IntegerType()).alias("Adv_Cardio_Days"),
            col("a.CC_Basic_Cardio_Days").cast(IntegerType()).alias("Basic_Cardio_Days"),
            col("a.CC_Adv_Resp_Days").cast(IntegerType()).alias("Adv_Resp_Days"),
            col("a.CC_Basic_Resp_Days").cast(IntegerType()).alias("Basic_Resp_Days"),
            col("a.CC_Renal_Days").cast(IntegerType()).alias("Renal_Days"),
            col("a.CC_Neuro_Days").cast(IntegerType()).alias("Neuro_Days"),
            col("a.CC_Gastro_Days").cast(IntegerType()).alias("Gastro_Days"),
            col("a.CC_Derm_Days").cast(IntegerType()).alias("Derm_Days"),
            col("a.CC_Liver_Days").cast(IntegerType()).alias("Liver_Days"),
            col("a.CC_No_Organ_Systems").cast(IntegerType()).alias("No_Organ_Systems"),
            greatest(col("a.ADC_UPDT"), col("DEM.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="critperiod_update")
def critperiod_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_critperiod_incr")
    )


dlt.create_target_table(
    name = "rde_critperiod",
    comment=critperiod_comment,
    schema=schema_rde_critperiod,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSONID,Period_ID"
    }
)

dlt.apply_changes(
    target = "rde_critperiod",
    source = "critperiod_update",
    keys = ["PERSONID", "Period_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

critopcs_comment = "Details of procedures undertaken in the critical care pathway."
schema_rde_critopcs = StructType([
        StructField("PERSONID", StringType(), True, metadata={"comment": "The unique identifier for each patient"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local Identifier"}),
        StructField("NHS_NUMBER", StringType(), True, metadata={"comment": "NHS Number"}),
        StructField("Period_ID", StringType(), True, metadata={"comment": "ID For the crit care period."}),
        StructField("ProcDate", StringType(), True, metadata={"comment": "Date of the procedure."}),
        StructField("ProcCode", StringType(), True, metadata={"comment": "OPCS Code of the procedure."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_critopcs_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def critopcs_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_critopcs")

    crit_care_opcs = spark.table("4_prod.raw.crit_care_opcs").alias("a")
    patient_demographics = dlt.read("rde_patient_demographics").alias("DEM")

    return (
        crit_care_opcs
        .join(patient_demographics, col("a.mrn") == col("DEM.MRN"), "inner")
        .filter((col("a.ADC_UPDT") > max_adc_updt) | (col("DEM.ADC_UPDT") > max_adc_updt))
        .select(
            col("DEM.PERSON_ID").cast(StringType()).alias("PERSONID"),
            col("DEM.MRN").cast(StringType()).alias("MRN"),
            col("DEM.NHS_Number").cast(StringType()).alias("NHS_NUMBER"),
            col("a.CC_Period_Local_Id").cast(StringType()).alias("Period_ID"),
            col("a.OPCS_Proc_Dt").cast(StringType()).alias("ProcDate"),
            col("a.OPCS_Proc_Code").cast(StringType()).alias("ProcCode"),
            greatest(col("a.ADC_UPDT"), col("DEM.ADC_UPDT")).alias("ADC_UPDT")
        )
        .distinct()  
    )

@dlt.view(name="critopcs_update")
def critopcs_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_critopcs_incr")
    )


dlt.create_target_table(
    name = "rde_critopcs",
    comment=critopcs_comment,
    schema=schema_rde_critopcs,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSONID,Period_ID,ProcDate,ProcCode"
    }
)

dlt.apply_changes(
    target = "rde_critopcs",
    source = "critopcs_update",
    keys = ["PERSONID", "Period_ID", "ProcDate", "ProcCode"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

measurements_comment = "Table of all measurements taken for a given patient."
schema_rde_measurements = StructType([
        StructField("PERSONID", StringType(), True, metadata={"comment": "Uniquely identifies individual in millenium."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("NHS_NUMBER", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("SystemLookup", StringType(), True, metadata={"comment": "Lookup for the system the measurement comes from."}),
        StructField("ClinicalSignificanceDate", StringType(), True, metadata={"comment": "The date of clinical significance for the measurement."}),
        StructField("ResultNumeric", BooleanType(), True, metadata={"comment": "1 indicates EVENT_RESULT_TXT is numeric; 0 otherwise."}),
        StructField("EventResult", StringType(), True, metadata={"comment": "The measurement."}),
        StructField("UnitsCode", IntegerType(), True, metadata={"comment": "Code for the units."}),
        StructField("UnitsDesc", StringType(), True, metadata={"comment": "Lookup of the code for the units."}),
        StructField("NormalCode", IntegerType(), True, metadata={"comment": "Code for the normalcy value."}),
        StructField("NormalDesc", StringType(), True, metadata={"comment": "Lookup of the code for the normalcy value."}),
        StructField("LowValue", StringType(), True, metadata={"comment": "Lower threshold for normal results."}),
        StructField("HighValue", StringType(), True, metadata={"comment": "Higher threshold for normal results."}),
        StructField("EventText", StringType(), True, metadata={"comment": "Text for the event."}),
        StructField("EventType", StringType(), True, metadata={"comment": "Type lookup for the event."}),
        StructField("EventParent", StringType(), True, metadata={"comment": "Lookup details for the parent of this measurement event."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])


@dlt.table(name="rde_measurements_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def measurements_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_measurements")

    clinical_event = spark.table("4_prod.raw.mill_clinical_event").alias("cce")
    encounter = dlt.read("rde_encounter").alias("ENC")
    code_value_ref = spark.table("3_lookup.dwh.pi_cde_code_value_ref")

    return (
        clinical_event.filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
        .join(encounter, col("cce.ENCNTR_ID") == col("ENC.ENCNTR_ID"), "inner")
        .join(code_value_ref.alias("ref"), col("cce.event_cd") == col("ref.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("urf"), col("cce.result_units_cd") == col("urf.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("nrf"), col("cce.normalcy_cd") == col("nrf.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("srf"), col("cce.contributor_system_cd") == col("srf.CODE_VALUE_CD"), "left")
        .join(clinical_event.alias("pev"), col("cce.PARENT_EVENT_ID") == col("pev.EVENT_ID"), "left")
        .join(code_value_ref.alias("TESTnm"), col("pev.EVENT_CD") == col("TESTnm.CODE_VALUE_CD"), "left")
        .filter((col("cce.ADC_UPDT") > max_adc_updt) | (col("ENC.ADC_UPDT") > max_adc_updt))
        .filter((col("cce.RESULT_UNITS_CD") > 0) |
                ((col("cce.RESULT_VAL") == '0') & 
                 (~col("cce.RESULT_VAL").cast("double").isNull()) &
                 (col("cce.RESULT_STATUS_CD") == 25) & (col("cce.ORDER_ID") != '0') &
                 (~col("cce.RESULT_VAL").like("%Comment%"))))
        .filter((col("cce.CONTRIBUTOR_SYSTEM_CD").isNull()) | 
                ((col("cce.CONTRIBUTOR_SYSTEM_CD") != '6378204') & (col("cce.CONTRIBUTOR_SYSTEM_CD") != '6141416')))
        .select(
            col("ENC.PERSON_ID").cast(StringType()).alias("PERSONID"),
            col("ENC.MRN").cast(StringType()).alias("MRN"),
            col("ENC.NHS_Number").cast(StringType()).alias("NHS_NUMBER"),
            col("srf.CODE_DESC_TXT").cast(StringType()).alias("SystemLookup"),
            col("cce.CLINSIG_UPDT_DT_TM").cast(StringType()).alias("ClinicalSignificanceDate"),
            when(col("cce.RESULT_VAL").cast("double").isNull(), lit(0)).otherwise(lit(1)).cast("boolean").alias("ResultNumeric"),
            col("cce.RESULT_VAL").cast(StringType()).alias("EventResult"),
            col("cce.RESULT_UNITS_CD").cast(IntegerType()).alias("UnitsCode"),
            col("urf.CODE_DESC_TXT").cast(StringType()).alias("UnitsDesc"),
            col("cce.normalcy_cd").cast(IntegerType()).alias("NormalCode"),
            col("nrf.CODE_DESC_TXT").cast(StringType()).alias("NormalDesc"),
            col("cce.NORMAL_LOW").cast(StringType()).alias("LowValue"),
            col("cce.NORMAL_HIGH").cast(StringType()).alias("HighValue"),
            col("cce.EVENT_TAG").cast(StringType()).alias("EventText"),
            col("ref.CODE_DESC_TXT").cast(StringType()).alias("EventType"),
            col("TESTnm.CODE_DESC_TXT").cast(StringType()).alias("EventParent"),
            greatest(col("cce.ADC_UPDT"), col("ENC.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="measurements_update")
def measurements_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_measurements_incr")
    )


dlt.create_target_table(
    name = "rde_measurements",
    comment=measurements_comment,
    schema=schema_rde_measurements,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSONID,ClinicalSignificanceDate,EventType"
    }
)

dlt.apply_changes(
    target = "rde_measurements",
    source = "measurements_update",
    keys = ["PERSONID", "ClinicalSignificanceDate", "EventType", "EventResult"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

emergency_comment = "Details of an admission to the emergency department."

schema_rde_emergencyd = StructType([
        StructField("PERSONID", StringType(), True, metadata={"comment": "Uniquely identifies individual in millenium."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("NHS_NUMBER", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("Arrival_Dt_Tm", StringType(), True, metadata={"comment": "Date of arrival in emergency department"}),
        StructField("Departure_Dt_Tm", StringType(), True, metadata={"comment": "Date of departure from department."}),
        StructField("Dischage_Status_CD", StringType(), True, metadata={"comment": "Code for discharge status."}),
        StructField("Discharge_Status_Desc", StringType(), True, metadata={"comment": "Description of discharge status code."}),
        StructField("Discharge_Dest_CD", StringType(), True, metadata={"comment": "Code for discharge destination."}),
        StructField("Discharge_Dest_Desc", StringType(), True, metadata={"comment": "Description of discharge destination code."}),
        StructField("Diag_Code", StringType(), True, metadata={"comment": "Code for diagnosis."}),
        StructField("SNOMED_CD", StringType(), True, metadata={"comment": "SNOMED code for diagnosis."}),
        StructField("SNOMED_Desc", StringType(), True, metadata={"comment": "Description of snomed code."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_emergencyd_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def emergencyd_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_emergencyd")

    cds_aea = spark.table("4_prod.raw.cds_aea").alias("AEA")
    patient_demographics = dlt.read("rde_patient_demographics").alias("DEM")
    discharge_destination = spark.table("3_lookup.dwh.cds_ecd_ref_discharge_destination").alias("e")
    discharge_status = spark.table("3_lookup.dwh.cds_ecd_map_att_disp_disch_stat").alias("d")
    treatment_site = spark.table("3_lookup.dwh.site").alias("ts")
    cds_aea_diag = spark.table("4_prod.raw.cds_aea_diag").alias("DIA")
    ecd_ref_diagnosis = spark.table("3_lookup.dwh.cds_ecd_ref_diagnosis").alias("REF")

    return (
        cds_aea
        .join(patient_demographics, col("AEA.mrn") == col("DEM.MRN"), "inner")
        .join(discharge_destination, col("AEA.Discharge_Destination_Cd") == col("e.Discharge_Destination_Snomed_Cd"), "left")
        .join(discharge_status, col("AEA.Discharge_Status_Cd") == col("d.Discharge_Status_ECD_Cd"), "left")
        .join(treatment_site, col("AEA.treatment_site_code") == col("ts.site_cd"), "left")
        .join(cds_aea_diag, col("AEA.cds_aea_id") == col("DIA.cds_aea_id"), "left")
        .join(ecd_ref_diagnosis, col("DIA.Diag_ECD_Cd") == col("REF.diagnosis_Snomed_cd"), "left")
        .filter((col("AEA.ADC_UPDT") > max_adc_updt) | (col("DEM.ADC_UPDT") > max_adc_updt))
        .select(
            col("DEM.PERSON_ID").cast(StringType()).alias("PERSONID"),
            col("DEM.MRN").cast(StringType()).alias("MRN"),
            col("DEM.NHS_Number").cast(StringType()).alias("NHS_NUMBER"),
            col("AEA.ARRIVAL_DT_TM").cast(StringType()).alias("Arrival_Dt_Tm"),
            col("AEA.DEPARTURE_TM").cast(StringType()).alias("Departure_Dt_Tm"),
            col("AEA.DISCHARGE_STATUS_CD").cast(StringType()).alias("Dischage_Status_CD"),
            col("d.DISCHARGE_STATUS_DESC").cast(StringType()).alias("Discharge_Status_Desc"),
            col("AEA.Discharge_destination_Cd").cast(StringType()).alias("Discharge_Dest_CD"),
            col("e.DISCHARGE_DESTINATION_DESC").cast(StringType()).alias("Discharge_Dest_Desc"),
            col("DIA.DIAG_CD").cast(StringType()).alias("Diag_Code"),
            col("REF.Diagnosis_Snomed_Cd").cast(StringType()).alias("SNOMED_CD"),
            col("REF.Diagnosis_Snomed_Desc").cast(StringType()).alias("SNOMED_Desc"),
            greatest(col("AEA.ADC_UPDT"), col("DEM.ADC_UPDT"), col("DIA.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="emergencyd_update")
def emergencyd_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_emergencyd_incr")
    )


dlt.create_target_table(
    name = "rde_emergencyd",
    comment=emergency_comment,
    schema=schema_rde_emergencyd,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSONID,Arrival_Dt_Tm,Diag_Code"
    }
)

dlt.apply_changes(
    target = "rde_emergencyd",
    source = "emergencyd_update",
    keys = ["PERSONID", "Arrival_Dt_Tm", "Diag_Code"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

medadmin_comment = "Table include all administration events for medicines."

schema_rde_medadmin = StructType([
        StructField("PERSONID", StringType(), True, metadata={"comment": "Uniquely identifies individual in millenium."}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("NHS_NUMBER", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("EVENT_ID", StringType(), True, metadata={"comment": "ID for the admin event."}),
        StructField("ORDER_ID", StringType(), True, metadata={"comment": "ID of the order linked to the admin event."}),
        StructField("EVENT_TYPE", StringType(), True, metadata={"comment": "Code for the type of event."}),
        StructField("ORDER_SYNONYM_ID", StringType(), True, metadata={"comment": "Millenium identifier of the drug."}),
        StructField("ORDER_MULTUM", StringType(), True, metadata={"comment": "MULTUM drug ontology code, maps to rxnorm."}),
        StructField("Order_Desc", StringType(), True, metadata={"comment": "Description of the drug."}),
        StructField("Order_Detail", StringType(), True, metadata={"comment": "Detailed notes about the order."}),
        StructField("ORDER_STRENGTH", FloatType(), True, metadata={"comment": "Strength for the order."}),
        StructField("ORDER_STRENGTH_UNIT", StringType(), True, metadata={"comment": "Units used in the strength of order."}),
        StructField("ORDER_VOLUME", FloatType(), True, metadata={"comment": "Volume for the order."}),
        StructField("ORDER_VOLUME_UNIT", StringType(), True, metadata={"comment": "Units used in the volume of the order."}),
        StructField("ORDER_ACTION_SEQUENCE", IntegerType(), True, metadata={"comment": "If part of a sequence, the order in which the sequence takes place."}),
        StructField("ADMIN_ROUTE", StringType(), True, metadata={"comment": "Administration route."}),
        StructField("ADMIN_METHOD", StringType(), True, metadata={"comment": "Method of administration"}),
        StructField("ADMIN_INITIAL_DOSAGE", FloatType(), True, metadata={"comment": "Initial dosage if different from total dosage."}),
        StructField("ADMIN_DOSAGE", FloatType(), True, metadata={"comment": "Dosage for the specific administration"}),
        StructField("ADMIN_DOSAGE_UNIT", StringType(), True, metadata={"comment": "Units used for the admin dosage."}),
        StructField("ADMIN_INITIAL_VOLUME", FloatType(), True, metadata={"comment": "Initial volume"}),
        StructField("ADMIN_TOTAL_INTAKE_VOLUME", FloatType(), True, metadata={"comment": "Total intake volume"}),
        StructField("ADMIN_DILUENT_TYPE", StringType(), True, metadata={"comment": "Diluent type if used."}),
        StructField("ADMIN_INFUSION_RATE", FloatType(), True, metadata={"comment": "Infusion rate if used."}),
        StructField("ADMIN_INFUSION_UNIT", StringType(), True, metadata={"comment": "Units for infusion."}),
        StructField("ADMIN_INFUSION_TIME", StringType(), True, metadata={"comment": "Time over which infusion takes place."}),
        StructField("ADMIN_MEDICATION_FORM", StringType(), True, metadata={"comment": "Form of medication administered if required."}),
        StructField("ADMIN_STRENGTH", FloatType(), True, metadata={"comment": "Strength of drug administered."}),
        StructField("ADMIN_STRENGTH_UNIT", StringType(), True, metadata={"comment": "Units for strength of drug."}),
        StructField("ADMIN_INFUSED_VOLUME", FloatType(), True, metadata={"comment": "Volume infused for the administration."}),
        StructField("ADMIN_INFUSED_VOLUME_UNIT", StringType(), True, metadata={"comment": "Units for the volume infused."}),
        StructField("ADMIN_REMAINING_VOLUME", FloatType(), True, metadata={"comment": "Remaining volume if any."}),
        StructField("ADMIN_REMAINING_VOLUME_UNIT", StringType(), True, metadata={"comment": "Units for remaining volume."}),
        StructField("ADMIN_IMMUNIZATION_TYPE", StringType(), True, metadata={"comment": "Type of aimmunization if relevent."}),
        StructField("ADMIN_REFUSAL", StringType(), True, metadata={"comment": "Code for an administration refusal if relevent."}),
        StructField("ADMIN_IV_EVENT", StringType(), True, metadata={"comment": "Code for an IV event if required."}),
        StructField("ADMIN_SYNONYM_ID", StringType(), True, metadata={"comment": "Internal code for the drug administered."}),
        StructField("ADMIN_MULTUM", StringType(), True, metadata={"comment": "MULTUM drug ontology code for drug administred."}),
        StructField("Admin_Desc", StringType(), True, metadata={"comment": "Text description of the drug administred"}),
        StructField("ADMINISTRATOR", StringType(), True, metadata={"comment": "Role of the member of staff administering the drug."}),
        StructField("EVENT_DESC", StringType(), True, metadata={"comment": "Clinical event description for the event code."}),
        StructField("EVENT_DATE", StringType(), True, metadata={"comment": "Date of the clinical event for the administration."}),
        StructField("ADMIN_START_DATE", StringType(), True, metadata={"comment": "Date and time administration of drug started."}),
        StructField("ADMIN_END_DATE", StringType(), True, metadata={"comment": "Date and time administration of drug concluded."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_medadmin_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def medadmin_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_medadmin")

    clinical_event = spark.table("4_prod.raw.mill_clinical_event").alias("CE")
    med_admin_event = spark.table("4_prod.raw.mill_med_admin_event").alias("MAE")
    encounter = dlt.read("rde_encounter").alias("ENC")
    ce_med_result = spark.table("4_prod.raw.mill_ce_med_result").alias("MR")
    order_ingredient = spark.table("4_prod.raw.mill_order_ingredient").alias("OI")
    order_catalog_synonym = spark.table("3_lookup.mill.mill_order_catalog_synonym").alias("OSYN")
    order_catalog = spark.table("3_lookup.mill.mill_order_catalog").alias("OCAT")
    code_value_ref = spark.table("3_lookup.dwh.pi_cde_code_value_ref")

    window_spec = Window.partitionBy("EVENT_ID").orderBy(desc("VALID_FROM_DT_TM"))

    return (
        clinical_event.filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
        .join(med_admin_event, col("CE.EVENT_ID") == col("MAE.EVENT_ID"), "inner")
        .join(encounter, col("CE.ENCNTR_ID") == col("ENC.ENCNTR_ID"), "inner")
        .join(ce_med_result.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1), 
              col("MAE.EVENT_ID") == col("MR.EVENT_ID"), "left")
        .join(order_ingredient.withColumn("rn", row_number().over(Window.partitionBy("ORDER_ID").orderBy(asc("ACTION_SEQUENCE")))).filter(col("rn") == 1), 
              col("MAE.TEMPLATE_ORDER_ID") == col("OI.ORDER_ID"), "left")
        .join(order_catalog_synonym.alias("OSYN"), col("OI.SYNONYM_ID") == col("OSYN.SYNONYM_ID"), "left")
        .join(order_catalog_synonym.alias("ASYN"), col("MR.SYNONYM_ID") == col("ASYN.SYNONYM_ID"), "left")
        .join(order_catalog.alias("OCAT"), col("OSYN.CATALOG_CD") == col("OCAT.CATALOG_CD"), "left")
        .join(order_catalog.alias("ACAT"), col("ASYN.CATALOG_CD") == col("ACAT.CATALOG_CD"), "left")
        .join(code_value_ref.alias("EVENT_TYPE"), col("MAE.EVENT_TYPE_CD") == col("EVENT_TYPE.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ADMIN_ROUTE"), col("MR.ADMIN_ROUTE_CD") == col("ADMIN_ROUTE.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ADMIN_METHOD"), col("MR.ADMIN_METHOD_CD") == col("ADMIN_METHOD.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ADMIN_DOSAGE_UNIT"), col("MR.DOSAGE_UNIT_CD") == col("ADMIN_DOSAGE_UNIT.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ADMIN_DILUENT_TYPE"), col("MR.DILUENT_TYPE_CD") == col("ADMIN_DILUENT_TYPE.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ADMIN_INFUSION_UNIT"), col("MR.INFUSION_UNIT_CD") == col("ADMIN_INFUSION_UNIT.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ADMIN_INFUSION_TIME"), col("MR.INFUSION_TIME_CD") == col("ADMIN_INFUSION_TIME.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ADMIN_MEDICATION_FORM"), col("MR.MEDICATION_FORM_CD") == col("ADMIN_MEDICATION_FORM.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ADMIN_STRENGTH_UNIT"), col("MR.ADMIN_STRENGTH_UNIT_CD") == col("ADMIN_STRENGTH_UNIT.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ADMIN_INFUSED_VOLUME_UNIT"), col("MR.INFUSED_VOLUME_UNIT_CD") == col("ADMIN_INFUSED_VOLUME_UNIT.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ADMIN_REMAINING_VOLUME_UNIT"), col("MR.REMAINING_VOLUME_UNIT_CD") == col("ADMIN_REMAINING_VOLUME_UNIT.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ADMIN_IMMUNIZATION_TYPE"), col("MR.IMMUNIZATION_TYPE_CD") == col("ADMIN_IMMUNIZATION_TYPE.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ADMIN_REFUSAL"), col("MR.REFUSAL_CD") == col("ADMIN_REFUSAL.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ADMIN_IV_EVENT"), col("MR.IV_EVENT_CD") == col("ADMIN_IV_EVENT.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ADMINISTRATOR"), col("MAE.POSITION_CD") == col("ADMINISTRATOR.CODE_VALUE_CD"), "left")
        .filter((col("CE.ADC_UPDT") > max_adc_updt) | (col("ENC.ADC_UPDT") > max_adc_updt) | (col("MAE.ADC_UPDT") > max_adc_updt))
        .filter(col("MAE.EVENT_ID") > 0)
        .select(
            col("ENC.PERSON_ID").cast(StringType()).alias("PERSONID"),
            col("ENC.MRN").cast(StringType()).alias("MRN"),
            col("ENC.NHS_Number").cast(StringType()).alias("NHS_NUMBER"),
            col("CE.EVENT_ID").cast(StringType()).alias("EVENT_ID"),
            col("CE.ORDER_ID").cast(StringType()).alias("ORDER_ID"),
            col("EVENT_TYPE.CODE_DESC_TXT").cast(StringType()).alias("EVENT_TYPE"),
            col("OI.SYNONYM_ID").cast(StringType()).alias("ORDER_SYNONYM_ID"),
            substring(col("OCAT.CKI"), -6, 6).cast(StringType()).alias("ORDER_MULTUM"),
            col("OSYN.MNEMONIC").cast(StringType()).alias("Order_Desc"),
            col("OI.ORDER_DETAIL_DISPLAY_LINE").cast(StringType()).alias("Order_Detail"),
            col("OI.STRENGTH").cast(FloatType()).alias("ORDER_STRENGTH"),
            col("OI.STRENGTH_UNIT").cast(StringType()).alias("ORDER_STRENGTH_UNIT"),
            col("OI.VOLUME").cast(FloatType()).alias("ORDER_VOLUME"),
            col("OI.VOLUME_UNIT").cast(StringType()).alias("ORDER_VOLUME_UNIT"),
            col("OI.ACTION_SEQUENCE").cast(IntegerType()).alias("ORDER_ACTION_SEQUENCE"),
            col("ADMIN_ROUTE.CODE_DESC_TXT").cast(StringType()).alias("ADMIN_ROUTE"),
            col("ADMIN_METHOD.CODE_DESC_TXT").cast(StringType()).alias("ADMIN_METHOD"),
            col("MR.INITIAL_DOSAGE").cast(FloatType()).alias("ADMIN_INITIAL_DOSAGE"),
            col("MR.ADMIN_DOSAGE").cast(FloatType()).alias("ADMIN_DOSAGE"),
            col("ADMIN_DOSAGE_UNIT.CODE_DESC_TXT").cast(StringType()).alias("ADMIN_DOSAGE_UNIT"),
            col("MR.INITIAL_VOLUME").cast(FloatType()).alias("ADMIN_INITIAL_VOLUME"),
            col("MR.TOTAL_INTAKE_VOLUME").cast(FloatType()).alias("ADMIN_TOTAL_INTAKE_VOLUME"),
            col("ADMIN_DILUENT_TYPE.CODE_DESC_TXT").cast(StringType()).alias("ADMIN_DILUENT_TYPE"),
            col("MR.INFUSION_RATE").cast(FloatType()).alias("ADMIN_INFUSION_RATE"),
            col("ADMIN_INFUSION_UNIT.CODE_DESC_TXT").cast(StringType()).alias("ADMIN_INFUSION_UNIT"),
            col("ADMIN_INFUSION_TIME.CODE_DESC_TXT").cast(StringType()).alias("ADMIN_INFUSION_TIME"),
            col("ADMIN_MEDICATION_FORM.CODE_DESC_TXT").cast(StringType()).alias("ADMIN_MEDICATION_FORM"),
            col("MR.ADMIN_STRENGTH").cast(FloatType()).alias("ADMIN_STRENGTH"),
            col("ADMIN_STRENGTH_UNIT.CODE_DESC_TXT").cast(StringType()).alias("ADMIN_STRENGTH_UNIT"),
            col("MR.INFUSED_VOLUME").cast(FloatType()).alias("ADMIN_INFUSED_VOLUME"),
            col("ADMIN_INFUSED_VOLUME_UNIT.CODE_DESC_TXT").cast(StringType()).alias("ADMIN_INFUSED_VOLUME_UNIT"),
            col("MR.REMAINING_VOLUME").cast(FloatType()).alias("ADMIN_REMAINING_VOLUME"),
            col("ADMIN_REMAINING_VOLUME_UNIT.CODE_DESC_TXT").cast(StringType()).alias("ADMIN_REMAINING_VOLUME_UNIT"),
            col("ADMIN_IMMUNIZATION_TYPE.CODE_DESC_TXT").cast(StringType()).alias("ADMIN_IMMUNIZATION_TYPE"),
            col("ADMIN_REFUSAL.CODE_DESC_TXT").cast(StringType()).alias("ADMIN_REFUSAL"),
            col("ADMIN_IV_EVENT.CODE_DESC_TXT").cast(StringType()).alias("ADMIN_IV_EVENT"),
            col("MR.SYNONYM_ID").cast(StringType()).alias("ADMIN_SYNONYM_ID"),
            substring(col("ACAT.CKI"), -6, 6).cast(StringType()).alias("ADMIN_MULTUM"),
            col("ASYN.MNEMONIC").cast(StringType()).alias("Admin_Desc"),
            col("ADMINISTRATOR.CODE_DESC_TXT").cast(StringType()).alias("ADMINISTRATOR"),
            col("CE.EVENT_TAG").cast(StringType()).alias("EVENT_DESC"),
            col("CE.CLINSIG_UPDT_DT_TM").cast(StringType()).alias("EVENT_DATE"),
            col("MR.ADMIN_START_DT_TM").cast(StringType()).alias("ADMIN_START_DATE"),
            col("MR.ADMIN_END_DT_TM").cast(StringType()).alias("ADMIN_END_DATE"),
            greatest(col("CE.ADC_UPDT"), col("ENC.ADC_UPDT"), col("MAE.ADC_UPDT"), col("OI.ADC_UPDT")).alias("ADC_UPDT")
        )
    )

@dlt.view(name="medadmin_update")
def medadmin_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_medadmin_incr")
    )


dlt.create_target_table(
    name = "rde_medadmin",
    comment=medadmin_comment,
    schema=schema_rde_medadmin,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSONID,EVENT_ID"
    }
)

dlt.apply_changes(
    target = "rde_medadmin",
    source = "medadmin_update",
    keys = ["EVENT_ID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

pharmacy_order_comment = "Table of the details of all pharmacy orders."
schema_rde_pharmacyorders = StructType([
        StructField("OrderID", StringType(), True, metadata={"comment": "Unique ID to identify orders"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local identifier to identify a person"}),
        StructField("NHS_Number", StringType(), True, metadata={"comment": "The NHS NUMBER, the primary identifier of a PERSON, is a unique identifier for a PATIENT within the NHS in England and Wales. Based on this field we identify the COHORT patients from the DWH"}),
        StructField("ENCNTRID", StringType(), True, metadata={"comment": "Identifier for the encounter linked to this order."}),
        StructField("EncType", StringType(), True, metadata={"comment": "Type of the encounter linked to this order."}),
        StructField("PERSONID", StringType(), True, metadata={"comment": "Unique identifier."}),
        StructField("OrderDate", StringType(), True, metadata={"comment": "Date and time the order is placed"}),
        StructField("LastOrderStatusDateTime", StringType(), True, metadata={"comment": "Date and time in which the status was updated last"}),
        StructField("ReqStartDateTime", StringType(), True, metadata={"comment": "Start date requested for the order."}),
        StructField("OrderText", StringType(), True, metadata={"comment": "Medicine details/prescirption "}),
        StructField("Comments", StringType(), True, metadata={"comment": "Any free text comments attached to the order."}),
        StructField("OrderDetails", StringType(), True, metadata={"comment": "Includes details like dosage, form ect"}),
        StructField("LastOrderStatus", StringType(), True, metadata={"comment": "Description of last order status include Ordered, Cancelled, Completed, Voided Without Results, Discontinued, Future, Incomplete, InProcess, Ordered, Suspended, Pending Complete"}),
        StructField("ClinicalCategory", StringType(), True, metadata={"comment": "This field describes the clinical category which  includes Activity\
"}),
        StructField("ActivityDesc", StringType(), True, metadata={"comment": "description of Activity_CD like Consults, Communication Orders, Diets, General Assessments, Infectious Diseases Consults, Microbiology, Patient Activity, Pharmacy, Hospital At Night, Appointment Consults OM, Diagnostic Cardiology, Non Theatre Procedures"}),
        StructField("OrderableType", StringType(), True, metadata={"comment": "Description of Orderable_CD like Dietary, Discern Rule Order, Laboratory, Patient Care, Pharmacy"}),
        StructField("PriorityDesc", StringType(), True, metadata={"comment": "Description of Priority_CD like ROUTINE, STAT, Urgent, Within 2 wks"}),
        StructField("CancelledReason", StringType(), True, metadata={"comment": "reason for cancellation of this order"}),
        StructField("CancelledDT", StringType(), True, metadata={"comment": "date of cancellation of this order"}),
        StructField("CompletedDT", StringType(), True, metadata={"comment": "completed date"}),
        StructField("DiscontinuedDT", StringType(), True, metadata={"comment": "discountinued date"}),
        StructField("ConceptIdent", StringType(), True, metadata={"comment": "Identifier for the drug ordered."}),
        StructField("PRIORITY_CD", StringType(), True, metadata={"comment": "Priority code for the order."}),
        StructField("ADC_UPDT", TimestampType(), True, metadata={"comment": ""})
    ])

@dlt.table(name="rde_pharmacyorders_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def pharmacyorders_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_pharmacyorders")

    orders = spark.table("4_prod.raw.mill_orders").alias("O")
    encounter = dlt.read("rde_encounter").alias("ENC")
    code_value_ref = spark.table("3_lookup.dwh.pi_cde_code_value_ref")
    order_detail = spark.table("4_prod.raw.mill_order_detail").alias("OD")
    order_comment = spark.table("4_prod.raw.mill_order_comment").alias("COM")
    long_text = spark.table("4_prod.raw.mill_long_text").alias("LNG")
    order_action = spark.table("4_prod.raw.mill_order_action").alias("ACT")

    # Window specs for order_detail
    window_spec = Window.partitionBy("ORDER_ID", "OE_FIELD_ID").orderBy(col("ACTION_SEQUENCE").desc())

    priority_detail = (
        order_detail.filter(col("OE_FIELD_ID") == 12657)
        .withColumn("row", row_number().over(window_spec))
        .filter(col("row") == 1)
        .select(col("ORDER_ID").alias("PRIORITY_ORDER_ID"), col("OE_FIELD_VALUE").alias("PRIORITY_CD"))
    )

    canceled_reason_detail = (
        order_detail.filter(col("OE_FIELD_ID") == 12664)
        .withColumn("row", row_number().over(window_spec))
        .filter(col("row") == 1)
        .select(col("ORDER_ID").alias("CANCELED_ORDER_ID"), col("OE_FIELD_VALUE").alias("CANCELED_REASON_CD"))
    )

    # Window spec for order comments
    comment_window_spec = Window.partitionBy("COM.ORDER_ID").orderBy(col("COM.ACTION_SEQUENCE").desc(), col("LNG.UPDT_DT_TM").desc())

    order_comments = (
        order_comment
        .join(long_text, (col("COM.ORDER_ID") == col("LNG.PARENT_ENTITY_ID")) & (col("COM.LONG_TEXT_ID") == col("LNG.LONG_TEXT_ID")))
        .filter((col("LNG.PARENT_ENTITY_NAME") == "ORDER_COMMENT") & (col("LNG.ACTIVE_IND") == 1) & (col("COM.COMMENT_TYPE_CD") == 66))
        .withColumn("row", row_number().over(comment_window_spec))
        .filter(col("row") == 1)
        .select(col("COM.ORDER_ID").alias("COMMENT_ORDER_ID"), col("LNG.LONG_TEXT"))
    )

    cancel_dt = (
    order_action.filter(col("ACTION_TYPE_CD") == 2526)  # Cancel Order
    .join(orders, col("ACT.ORDER_ID") == col("O.ORDER_ID"))
    .withColumn("row_num", row_number().over(Window.partitionBy("O.ORDER_ID").orderBy(col("ACT.ACTION_SEQUENCE").desc())))
    .filter(col("row_num") == 1)
    .select(
        col("O.ORDER_ID").alias("CANCEL_ORDER_ID"),
        col("ACT.ACTION_DT_TM").alias("CANCELED_DT_TM"),
        col("ACT.ACTION_PERSONNEL_ID").alias("CANCELED_TRAN_PRSNL_ID")
    )
)

    completed_dt = (
        order_action.filter(col("ACTION_TYPE_CD") == 2529)  # Completed Order
        .join(orders, col("ACT.ORDER_ID") == col("O.ORDER_ID"))
        .withColumn("row_num", row_number().over(Window.partitionBy("O.ORDER_ID").orderBy(col("ACT.ACTION_SEQUENCE").desc())))
        .filter(col("row_num") == 1)
        .select(
            col("O.ORDER_ID").alias("COMPLETED_ORDER_ID"),
            col("ACT.ACTION_DT_TM").alias("COMPLETED_DT_TM")
        )
    )

    # New subquery for DISCONTINUE_DT_TM
    discontinue_dt = (
        order_action.filter(col("ACTION_TYPE_CD") == 2532)  # Discontinue Order
        .join(orders, col("ACT.ORDER_ID") == col("O.ORDER_ID"))
        .withColumn("row_num", row_number().over(Window.partitionBy("O.ORDER_ID").orderBy(col("ACT.ACTION_SEQUENCE").desc())))
        .filter(col("row_num") == 1)
        .select(
            col("O.ORDER_ID").alias("DISCONTINUE_ORDER_ID"),
            col("ACT.ACTION_DT_TM").alias("DISCONTINUE_DT_TM")
        )
    )

    return (
        orders
        .join(encounter, col("O.ENCNTR_ID") == col("ENC.ENCNTR_ID"), "inner")
        .join(code_value_ref.alias("Activity"), col("O.ACTIVITY_TYPE_CD") == col("Activity.CODE_VALUE_CD"), "left")
        .join(priority_detail, col("O.ORDER_ID") == col("PRIORITY_ORDER_ID"), "left")
        .join(canceled_reason_detail, col("O.ORDER_ID") == col("CANCELED_ORDER_ID"), "left")
        .join(code_value_ref.alias("Cancel"), col("CANCELED_REASON_CD") == col("Cancel.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("ClinicCat"), col("O.DCP_CLIN_CAT_CD") == col("ClinicCat.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("OrderTyp"), col("O.CATALOG_TYPE_CD") == col("OrderTyp.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("LastOStat"), col("O.ORDER_STATUS_CD") == col("LastOStat.CODE_VALUE_CD"), "left")
        .join(code_value_ref.alias("Prio"), col("PRIORITY_CD") == col("Prio.CODE_VALUE_CD"), "left")
        .join(order_comments, col("O.ORDER_ID") == col("COMMENT_ORDER_ID"), "left")
        .join(cancel_dt, col("O.ORDER_ID") == col("CANCEL_ORDER_ID"), "left")
        .join(completed_dt, col("O.ORDER_ID") == col("COMPLETED_ORDER_ID"), "left")
        .join(discontinue_dt, col("O.ORDER_ID") == col("DISCONTINUE_ORDER_ID"), "left")
        .filter((col("O.ADC_UPDT") > max_adc_updt) | (col("ENC.ADC_UPDT") > max_adc_updt))
        .filter((col("O.CATALOG_TYPE_CD") == 2516) & 
                (col("O.ORDER_STATUS_CD").isin(2543, 2545, 2547, 2548, 2550, 2552, 643466)) &
                (col("O.DCP_CLIN_CAT_CD") == 10577) & 
                (col("O.ACTIVITY_TYPE_CD") == "705") &
                (col("O.ACTIVE_IND") == 1))
        .select(
            col("O.ORDER_ID").cast(StringType()).alias("OrderID"),
            col("ENC.MRN").cast(StringType()).alias("MRN"),
            col("ENC.NHS_Number").cast(StringType()).alias("NHS_Number"),
            col("O.ENCNTR_ID").cast(StringType()).alias("ENCNTRID"),
            col("ENC.ENC_TYPE").cast(StringType()).alias("EncType"),
            col("ENC.PERSON_ID").cast(StringType()).alias("PERSONID"),
            col("O.ORIG_ORDER_DT_TM").cast(StringType()).alias("OrderDate"),
            col("O.STATUS_DT_TM").cast(StringType()).alias("LastOrderStatusDateTime"),
            col("O.CURRENT_START_DT_TM").cast(StringType()).alias("ReqStartDateTime"),
            col("O.ORDER_MNEMONIC").cast(StringType()).alias("OrderText"),
            col("LNG.LONG_TEXT").cast(StringType()).alias("Comments"),
            col("O.ORDER_DETAIL_DISPLAY_LINE").cast(StringType()).alias("OrderDetails"),
            col("LastOStat.CODE_DESC_TXT").cast(StringType()).alias("LastOrderStatus"),
            col("ClinicCat.CODE_DESC_TXT").cast(StringType()).alias("ClinicalCategory"),
            col("Activity.CODE_DESC_TXT").cast(StringType()).alias("ActivityDesc"),
            col("OrderTyp.CODE_DESC_TXT").cast(StringType()).alias("OrderableType"),
            col("Prio.CODE_DESC_TXT").cast(StringType()).alias("PriorityDesc"),
            col("Cancel.CODE_DESC_TXT").cast(StringType()).alias("CancelledReason"),
            col("CANCELED_DT_TM").cast(StringType()).alias("CancelledDT"),
            coalesce(col("COMPLETED_DT_TM"), col("O.CLIN_RELEVANT_UPDT_DT_TM")).cast(StringType()).alias("CompletedDT"),
            col("DISCONTINUE_DT_TM").cast(StringType()).alias("DiscontinuedDT"),
            col("O.CKI").cast(StringType()).alias("ConceptIdent"),
            col("PRIORITY_CD").cast(StringType()).alias("PRIORITY_CD"),
            greatest(col("O.ADC_UPDT"), col("ENC.ADC_UPDT")).alias("ADC_UPDT")
        )
    )



@dlt.view(name="pharmacyorders_update")
def pharmacyorders_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_pharmacyorders_incr")
    )

# Declare the target table
dlt.create_target_table(
    name = "rde_pharmacyorders",
    comment=pharmacy_order_comment,
    schema=schema_rde_pharmacyorders,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSONID,OrderID"
    }
)

dlt.apply_changes(
    target = "rde_pharmacyorders",
    source = "pharmacyorders_update",
    keys = ["OrderID"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

nnu_exam_comment = "Details of Neonatal Unit Routine Examination."

schema_rde_mat_nnu_exam = StructType([
    StructField("PERSON_ID", StringType(), True, metadata={'comment': "a unique identifier assigned to each patient."}),
    StructField("MRN", StringType(), True, metadata={'comment': "the Medical Record Number"}),
    StructField("NHS_Number", StringType(), True, metadata={'comment': "The NHS_Number column represents the unique National Health Service (NHS) number assigned to each patient"}),
    StructField("ExamDate", TimestampType(), True, metadata={'comment': "the date and time when a particular examination of the neonate (newborn baby) was performed in the Neonatal Intensive Care Unit (NICU)."}),
    StructField("HeadCircumference", StringType(), True, metadata={'comment': "the circumference measurement of a newborn's head, typically measured in centimeters. Head circumference is an important indicator of brain growth and development, and is routinely measured during neonatal examinations to monitor for potential issues or abnormalities."}),
    StructField("Skin", StringType(), True, metadata={'comment': "the overall condition or appearance of the neonate's skin during the examination, with possible values indicating if the skin appears normal, abnormal, or was not examined."}),
    StructField("SkinComments", StringType(), True, metadata={'comment': "descriptions or notes related to any observations, abnormalities, or markings on the neonate's skin during the examination. This could include rashes, birthmarks, jaundice, dryness, or other skin conditions that the examiner deems relevant to document."}),
    StructField("Cranium", StringType(), True, metadata={'comment': "observations or findings related to the skull or cranial region of the newborn infant during a physical examination."}),
    StructField("CraniumComments", StringType(), True, metadata={'comment': "notes or observations made by healthcare professionals during examinations related to the shape, appearance, and any abnormalities or conditions affecting the baby's cranium (skull) or head region."}),
    StructField("Fontanelle", StringType(), True, metadata={'comment': "the examination findings related to the soft spots or gaps between the skull bones in a newborn baby's head, which help accommodate the growth of the infant's brain during the early stages of development."}),
    StructField("FontanelleComments", StringType(), True, metadata={'comment': "free-text notes or observations made by the examiner regarding the condition, size, tension, or any abnormalities detected in the fontanelles (soft spots on an infant's skull) during the neonatal examination."}),
    StructField("Sutures", StringType(), True, metadata={'comment': "examination and assessment of the cranial sutures or fibrous joints between the bones of a newborn's skull, which can provide insights into the infant's neurological development and potential cranial abnormalities."}),
    StructField("SuturesComments", StringType(), True, metadata={'comment': "notes and observations made by healthcare professionals during the examination regarding the appearance and condition of the cranial sutures (fibrous joints that connect the bones of an infant's skull) and related features like the fontanelles (soft spots on a baby's head). This information helps assess normal skull development and identify any potential abnormalities or concerns related to the skull shape, suture width, fontanelle tension, or presence of swelling or ridges."}),
    StructField("RedReflex", StringType(), True, metadata={'comment': "examination findings related to the red reflex test, which is a screening tool used to assess the clarity and transparency of the eye's media (cornea, lens, vitreous humor) and the retina in newborns."}),
    StructField("RedReflexComments", StringType(), True, metadata={'comment': "notes and observations related to the red reflex examination of a newborn's eyes. The red reflex test is performed to assess the clarity of the ocular media (cornea, lens, vitreous) and detect potential abnormalities or conditions like cataracts, retinopathy of prematurity (ROP), or other eye problems in neonates."}),
    StructField("RedReflexRight", StringType(), True, metadata={'comment': "the observation or assessment of the red reflex in the right eye of a neonate, which is an important examination to detect potential abnormalities or issues related to the eye's structures or the presence of cataracts."}),
    StructField("RedReflexCommentsRight", StringType(), True, metadata={'comment': "comments or observations related to the red reflex examination of the right eye in newborn infants. The red reflex test is performed to assess the transparency of the eye's media (cornea, lens, vitreous) and detect any abnormalities or opacities that may obstruct the normal reddish-orange reflection from the retina when a light source is shone into the eye. This column allows clinicians to document any specific findings, concerns, or limitations encountered during the examination of the right eye's red reflex."}),
    StructField("Ears", StringType(), True, metadata={'comment': "the findings or observations related to the physical examination of the newborn's ears, which is an essential part of the neonatal assessment."}),
    StructField("EarsComments", StringType(), True, metadata={'comment': "free-text notes or observations made by healthcare professionals during the neonatal examination regarding the appearance, position, or any abnormalities related to the infant's ears, such as low-set ears, ear tags, or issues with the ear canal. This column allows for documenting any relevant comments or concerns about the ears that may require further evaluation or follow-up."}),
    StructField("PalateSuck", StringType(), True, metadata={'comment': "the assessment or observation of the neonate's ability to suck or create a seal with their palate while feeding or during an examination, which can indicate normal or abnormal sucking reflex and oral-motor function."}),
    StructField("PalateSuckComments", StringType(), True, metadata={'comment': "comments or observations made by the examiner regarding the neonate's ability to suck and the condition of their palate during feeding or examination. It may include details about the strength and coordination of the sucking reflex, presence of any anatomical abnormalities like cleft palate or tongue-tie, and any difficulties or concerns related to feeding or sucking."}),
    StructField("Spine", StringType(), True, metadata={'comment': "the physical examination findings related to the spine or spinal cord of a newborn infant, which is a critical assessment in neonatal care to detect any congenital abnormalities or injuries that may require immediate medical intervention."}),
    StructField("SpineComments", StringType(), True, metadata={'comment': "free-text notes or observations made by healthcare professionals during the neonatal examination regarding any abnormalities, findings, or comments related to the infant's spine, such as the presence of sacral dimples, skin discoloration, or concerns that may warrant further evaluation or imaging of the spine."}),
    StructField("Breath", StringType(), True, metadata={'comment': "the clinician's assessment of the neonate's respiratory function or breathing pattern during the examination."}),
    StructField("BreathComments", StringType(), True, metadata={'comment': "notes or observations about the neonate's breathing patterns or respiratory status during the examination. These comments may include descriptions of abnormal breathing sounds, rates, or signs of respiratory distress, as well as any interventions or treatments related to the neonate's respiratory condition."}),
    StructField("Heart", StringType(), True, metadata={'comment': "findings or assessment of the neonate's heart during a physical examination, which is a crucial part of evaluating the overall health and potential congenital abnormalities in newborn infants."}),
    StructField("HeartComments", StringType(), True, metadata={'comment': "comments or notes related to any abnormal findings, observations, or diagnoses associated with the heart or cardiac function of the newborn infant during the examination. It allows healthcare providers to document details about heart murmurs, congenital heart defects, abnormal heart sounds, or any other relevant cardiac observations made during the physical assessment."}),
    StructField("Femoral", StringType(), True, metadata={'comment': "assessment or examination findings related to the femoral pulse or femoral artery of the neonate during the physical examination, where values such as 'normal', 'abnormal', 'unsure', 'not examined', or 'None' may indicate the status or presence of the femoral pulse or any abnormalities detected."}),
    StructField("FemoralComments", StringType(), True, metadata={'comment': "comments related to the assessment of the femoral pulses in a newborn during a physical examination. The femoral pulses are felt at the groin area and provide information about the peripheral circulation and cardiovascular status of the neonate."}),
    StructField("FemoralRight", StringType(), True, metadata={'comment': "examination findings related to the femoral pulse or artery on the right side of the neonate, which is an important indicator of cardiac function and peripheral perfusion in neonatal care."}),
    StructField("FemoralCommentsRight", StringType(), True, metadata={'comment': "comments or notes about the examination or assessment of the right femoral (upper thigh) pulse of a neonate, which can provide insights into the cardiovascular health and perfusion status of the newborn."}),
    StructField("Abdomen", StringType(), True, metadata={'comment': "observations and findings related to the physical examination of a newborn's abdomen during a neonatal assessment."}),
    StructField("AbdomenComments", StringType(), True, metadata={'comment': "notes and observations made by medical professionals regarding the condition and appearance of the newborn's abdomen during examinations. These comments may include details about distension, umbilical abnormalities (e.g., granulomas, hernias), surgical scars, presence of stomas or tubes, liver size, bowel movements, and any other notable findings related to the abdominal area."}),
    StructField("Genitalia", StringType(), True, metadata={'comment': "observations or findings related to the external genitalia of a neonate (newborn infant) during a physical examination, which is an important aspect of neonatal care and assessment."}),
    StructField("GenitaliaComments", StringType(), True, metadata={'comment': "notes and observations related to the examination and assessment of the newborn's external genitalia, including any abnormalities, conditions, or procedures performed related to the genitalia."}),
    StructField("Testicles", StringType(), True, metadata={'comment': "observations or findings related to the condition or presence of the newborn's testicles during a physical examination, which is an important aspect of assessing the infant's health and development."}),
    StructField("TesticlesComments", StringType(), True, metadata={'comment': "comments or observations related to the examination and positioning of the testicles in newborn male infants. This information is relevant for detecting potential congenital abnormalities or conditions affecting the proper descent and location of the testicles, which may require further monitoring or treatment."}),
    StructField("Anus", StringType(), True, metadata={'comment': "examination and assessment of the newborn's anal opening, which is an important aspect of evaluating the baby's gastrointestinal and genitourinary system during routine neonatal examinations."}),
    StructField("AnusComments", StringType(), True, metadata={'comment': "contains notes or comments made by healthcare professionals during the examination of a newborn's anus and surrounding area. These comments may include observations about the appearance, position, patency (openness), or any abnormalities or malformations related to the anus, as well as any planned or performed surgical interventions."}),
    StructField("Hands", StringType(), True, metadata={'comment': "the evaluation of a newborn's hands during a physical examination, with possible values indicating whether their hands appear normal, were not examined, show some abnormality, or if there is no information recorded."}),
    StructField("HandsComments", StringType(), True, metadata={'comment': "contains free text notes or observations made by healthcare professionals during neonatal examinations regarding any abnormalities, conditions, or remarks related to the hands of newborn infants, such as congenital deformities, injuries, circulation issues, or presence of medical devices."}),
    StructField("Feet", StringType(), True, metadata={'comment': "examination findings related to the physical appearance and condition of the newborn's feet during a neonatal assessment."}),
    StructField("FeetComments", StringType(), True, metadata={'comment': "free text comments or observations related to any abnormalities, conditions, or notable findings regarding the feet of the newborn infant during the physical examination. This can include congenital deformities like clubfoot (talipes), extra digits (polydactyly), webbed toes, toe positioning or flexibility issues, swelling, discoloration, and notes on reflexes or muscle tone in the feet."}),
    StructField("Hips", StringType(), True, metadata={'comment': "the clinical assessment of the neonate's hip joints, indicating if they appear normal, abnormal, or if the examiner is unsure or did not examine them during that particular examination."}),
    StructField("HipsComments", StringType(), True, metadata={'comment': "comments or notes related to the examination and assessment of the hips and hip joints in newborn babies. The comments often mention whether there were any abnormalities detected, such as clicking or restricted movement, which may indicate conditions like developmental dysplasia of the hip (DDH). Recommendations for further evaluation through ultrasound scans or follow-up are also commonly noted, especially for babies born via breech delivery, as they have a higher risk of hip dysplasia."}),
    StructField("HipsRight", StringType(), True, metadata={'comment': "the clinical examination findings or assessment of the right hip joint of a newborn infant, indicating whether the hip appears normal, abnormal, or if the examiner was unsure or did not examine it during the assessment."}),
    StructField("HipRightComments", StringType(), True, metadata={'comment': "contains comments related to any abnormalities, concerns, or follow-up actions required for the right hip of the neonate, particularly in cases of breech delivery or suspected hip dysplasia, which may require further examination or imaging (e.g., hip ultrasound) to rule out any congenital hip issues."}),
    StructField("Tone", StringType(), True, metadata={'comment': "the overall muscle tone or tension observed in the neonate during the physical examination, indicating whether it is normal, abnormal, or not examined."}),
    StructField("ToneComments", StringType(), True, metadata={'comment': "comments or notes regarding the neonate's muscle tone during the examination. Muscle tone refers to the tension or resistance of muscles to stretch, which can indicate neurological or muscular conditions in newborns. The comments may describe observations such as hypotonia (decreased muscle tone), hypertonia (increased muscle tone), floppiness, lethargy, abnormal posturing, or the effects of medications on muscle tone."}),
    StructField("Movement", StringType(), True, metadata={'comment': "observations or assessments of the neonate's overall body movements during the physical examination, with values indicating whether the movements were normal, abnormal, not examined, or unsure."}),
    StructField("MovementComments", StringType(), True, metadata={'comment': "comments or observations made by the medical staff regarding the movements and muscle tone of the newborn baby during the examination. It allows healthcare professionals to document any abnormalities, limitations, or notable observations related to the baby's spontaneous movements, reflexes, muscle tone, or activity level."}),
    StructField("Moro", StringType(), True, metadata={'comment': "observations or results of the Moro reflex test, which assesses the startle reflex and neurological responses in a neonate by evaluating the extension and abduction of the limbs after eliciting the reflex through a sudden change in body position or loud sound."}),
    StructField("MoroComments", StringType(), True, metadata={'comment': "comments or observations made by the medical professional during the examination of the Moro reflex in newborn infants. The Moro reflex is an involuntary motor response observed in newborns, which involves extending the arms and legs and then bringing them back together in response to a sudden noise or movement. This reflex is an important indicator of neurological development and function in neonates."}),
    StructField("Overall", StringType(), True, metadata={'comment': "an overall assessment or summary of the neonate's physical examination findings, indicating whether the overall examination was normal, abnormal, not examined, or if the examiner was unsure about the overall status based on the various individual components evaluated."}),
    StructField("OverallComments", StringType(), True, metadata={'comment': "general observations, remarks, and notes made by the examiner regarding the overall condition, presentation, and any notable findings related to the neonate during the examination. It serves as a catch-all field for documenting pertinent information that may not fit into the other specific columns, such as family history, planned follow-up tests or procedures, suspected diagnoses, and any other relevant comments about the neonate's overall health status."}),
    StructField("NameOfExaminer", StringType(), True, metadata={'comment': "the name of the medical professional (doctor, nurse, etc.) who performed the examination on the newborn baby."}),
    StructField("Palate", StringType(), True, metadata={'comment': "observations or findings related to the structure and appearance of the palate (roof of the mouth) during a physical examination of a newborn infant in the neonatal intensive care unit."}),
    StructField("PalateComments", StringType(), True, metadata={'comment': "text notes or observations made by the examiner regarding any abnormalities, conditions, or remarks related to the palate of the neonate during the examination."}),
    StructField("SuckingReflex", StringType(), True, metadata={'comment': "assessment or observation of the sucking reflex in a newborn baby, which is an innate behavior necessary for feeding. The values suggest varying states or results of examining this reflex during a neonatal examination."}),
    StructField("SuckingReflexComments", StringType(), True, metadata={'comment': "free-text comments or observations related to the neonate's sucking reflex, which is an essential component of feeding and swallowing. This column allows clinicians to document any abnormalities, concerns, or relevant notes regarding the neonate's ability to effectively suck and coordinate the sucking action, which is crucial for adequate oral intake and development."}),
    StructField("EarsLeft", StringType(), True, metadata={'comment': "findings or observations related to the left ear of a newborn baby during a neonatal examination, indicating whether the left ear appears normal, abnormal, or was not examined."}),
    StructField("EarsCommentsLeft", StringType(), True, metadata={'comment': "any findings, abnormalities, or observations related to the left ear of a neonate during a physical examination in the neonatal intensive care unit."}),
    StructField("EarsRight", StringType(), True, metadata={'comment': "examination findings or observations specific to the right ear of a neonate or newborn baby during a neonatal assessment or examination."}),
    StructField("EarsCommentsRight", StringType(), True, metadata={'comment': "any observations, abnormalities, or relevant remarks specifically regarding the right ear during the neonatal examination."}),
    StructField("Eyes", StringType(), True, metadata={'comment': "the overall condition or observations made during the examination of the neonate's eyes, indicating whether they appeared normal, abnormal, or were not examined during that particular assessment."}),
    StructField("EyesComments", StringType(), True, metadata={'comment': "free-text notes or observations made by the examiner regarding the appearance, condition, or any abnormalities related to the neonate's eyes during the physical examination."}),
    StructField("Chest_NZ", StringType(), True, metadata={'comment': "observations or findings related to the chest or respiratory system of the neonate."}),
    StructField("ChestComments_NZ", StringType(), True, metadata={'comment': "any additional comments or observations related to the examination of the neonate's chest or respiratory system."}),
    StructField("Mouth_NZ", StringType(), True, metadata={'comment': "represents observations or findings related to the mouth or oral cavity of a newborn baby during a physical examination in New Zealand, with 'normal' indicating no abnormalities were noted and 'None' suggesting no data was recorded for that field."}),
    StructField("MouthComments_NZ", StringType(), True, metadata={'comment': "stores free-text comments or observations related to the examination of a neonate's mouth."}),
    StructField("Growth_NZ", StringType(), True, metadata={'comment': "observations or measurements related to the growth or developmental milestones of a newborn infant."}),
    StructField("GrowthComments_NZ", StringType(), True, metadata={'comment': "contains any additional comments or observations related to the growth and development of the neonate (newborn baby) during the examination"}),
    StructField("Grasp", StringType(), True, metadata={'comment': "an assessment or observation of the newborn's grasping reflex or ability, which is an important milestone in neurological development and can indicate potential issues if found to be abnormal."}),
    StructField("GraspComments", StringType(), True, metadata={'comment': "contains comments or observations related to the grasp reflex or the ability of the neonate to grasp or hold objects, which is an important milestone in neurological development and assessment of the newborn."}),
    StructField("Femorals_NZ", StringType(), True, metadata={'comment': "contains observations or notes regarding the examination of the femoral pulses or arteries in a newborn, which are important for assessing the cardiovascular health and peripheral circulation in neonates."}),
    StructField("FemoralsComments_NZ", StringType(), True, metadata={'comment': "stores comments or observations related to the examination of the femoral pulses or femoral artery in newborns, which is an important part of the cardiovascular assessment in neonatal care."}),
    StructField("InguinalHernia", StringType(), True, metadata={'comment': "the presence or absence of an inguinal hernia in a neonate (newborn baby) during a physical examination, which is a condition where abdominal contents protrude through the inguinal canal in the groin area."}),
    StructField("InguinalHerniaComments", StringType(), True, metadata={'comment': "ontains textual comments or notes related to the presence or absence of inguinal hernias (protrusions near the groin area) observed during the neonatal examination."}),
    StructField("GeneralComments", StringType(), True, metadata={'comment': "contains free text notes or remarks made by healthcare professionals regarding the overall health status, observations, or additional relevant information about the newborn's examination that may not fit into the other specific columns. It serves as a catch-all field to capture any general comments, impressions, or additional details pertinent to the neonatal examination."}),
    StructField("SyncScope", StringType(), True, metadata={'comment': "whether a particular examination or assessment was performed using a specialized device called a SyncScope, which is used to monitor vital signs and other parameters in neonatal intensive care units."}),
    StructField("ADC_UPDT", TimestampType(), True, metadata={'comment': "Timestamp for when row was last updated."})
])

@dlt.table(name="rde_mat_nnu_exam_incr", table_properties={"skipChangeCommits": "true"}, temporary=True)
def mat_nnu_exam_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_mat_nnu_exam")

    nnu_exam = spark.table("4_prod.raw.nnu_routineexamination").alias("NNUExam")
    patient_demographics = dlt.read("rde_patient_demographics").alias("PDEM")
    nnu_nationalid = spark.table("4_prod.raw.nnu_tblnationalidepidx").alias("NIE")

    return (
        nnu_exam
        .join(nnu_nationalid, col("NIE.EntityID") == col("NNUExam.entityid"), "left")
        .join(patient_demographics, col("NIE.nationalid") == col("PDEM.NHS_Number"), "left")
        .filter(col("PDEM.PERSON_ID").isNotNull())
        .select(
            col("PDEM.PERSON_ID").cast(StringType()).alias("PERSON_ID"),
            col("PDEM.MRN").cast(StringType()).alias("MRN"),
            col("NIE.nationalid").cast(StringType()).alias("NHS_Number"),
            coalesce(col("NNUExam.DateOfExamination"), col("NNUExam.RecordTimestamp")).cast(TimestampType()).alias("ExamDate"),
            col("NNUExam.HeadCircumference").cast(StringType()),
            col("NNUExam.Skin").cast(StringType()),
            col("NNUExam.SkinComments").cast(StringType()),
            col("NNUExam.Cranium").cast(StringType()),
            col("NNUExam.CraniumComments").cast(StringType()),
            col("NNUExam.Fontanelle").cast(StringType()),
            col("NNUExam.FontanelleComments").cast(StringType()),
            col("NNUExam.Sutures").cast(StringType()),
            col("NNUExam.SuturesComments").cast(StringType()),
            col("NNUExam.RedReflex").cast(StringType()),
            col("NNUExam.RedReflexComments").cast(StringType()),
            col("NNUExam.RedReflexRight").cast(StringType()),
            col("NNUExam.RedReflexCommentsRight").cast(StringType()),
            col("NNUExam.Ears").cast(StringType()),
            col("NNUExam.EarsComments").cast(StringType()),
            col("NNUExam.PalateSuck").cast(StringType()),
            col("NNUExam.PalateSuckComments").cast(StringType()),
            col("NNUExam.Spine").cast(StringType()),
            col("NNUExam.SpineComments").cast(StringType()),
            col("NNUExam.Breath").cast(StringType()),
            col("NNUExam.BreathComments").cast(StringType()),
            col("NNUExam.Heart").cast(StringType()),
            col("NNUExam.HeartComments").cast(StringType()),
            col("NNUExam.Femoral").cast(StringType()),
            col("NNUExam.FemoralComments").cast(StringType()),
            col("NNUExam.FemoralRight").cast(StringType()),
            col("NNUExam.FemoralCommentsRight").cast(StringType()),
            col("NNUExam.Abdomen").cast(StringType()),
            col("NNUExam.AbdomenComments").cast(StringType()),
            col("NNUExam.Genitalia").cast(StringType()),
            col("NNUExam.GenitaliaComments").cast(StringType()),
            col("NNUExam.Testicles").cast(StringType()),
            col("NNUExam.TesticlesComments").cast(StringType()),
            col("NNUExam.Anus").cast(StringType()),
            col("NNUExam.AnusComments").cast(StringType()),
            col("NNUExam.Hands").cast(StringType()),
            col("NNUExam.HandsComments").cast(StringType()),
            col("NNUExam.Feet").cast(StringType()),
            col("NNUExam.FeetComments").cast(StringType()),
            col("NNUExam.Hips").cast(StringType()),
            col("NNUExam.HipsComments").cast(StringType()),
            col("NNUExam.HipsRight").cast(StringType()),
            col("NNUExam.HipRightComments").cast(StringType()),
            col("NNUExam.Tone").cast(StringType()),
            col("NNUExam.ToneComments").cast(StringType()),
            col("NNUExam.Movement").cast(StringType()),
            col("NNUExam.MovementComments").cast(StringType()),
            col("NNUExam.Moro").cast(StringType()),
            col("NNUExam.MoroComments").cast(StringType()),
            col("NNUExam.Overall").cast(StringType()),
            col("NNUExam.OverallComments").cast(StringType()),
            col("NNUExam.NameOfExaminer").cast(StringType()),
            col("NNUExam.Palate").cast(StringType()),
            col("NNUExam.PalateComments").cast(StringType()),
            col("NNUExam.SuckingReflex").cast(StringType()),
            col("NNUExam.SuckingReflexComments").cast(StringType()),
            col("NNUExam.EarsLeft").cast(StringType()),
            col("NNUExam.EarsCommentsLeft").cast(StringType()),
            col("NNUExam.EarsRight").cast(StringType()),
            col("NNUExam.EarsCommentsRight").cast(StringType()),
            col("NNUExam.Eyes").cast(StringType()),
            col("NNUExam.EyesComments").cast(StringType()),
            col("NNUExam.Chest_NZ").cast(StringType()),
            col("NNUExam.ChestComments_NZ").cast(StringType()),
            col("NNUExam.Mouth_NZ").cast(StringType()),
            col("NNUExam.MouthComments_NZ").cast(StringType()),
            col("NNUExam.Growth_NZ").cast(StringType()),
            col("NNUExam.GrowthComments_NZ").cast(StringType()),
            col("NNUExam.Grasp").cast(StringType()),
            col("NNUExam.GraspComments").cast(StringType()),
            col("NNUExam.Femorals_NZ").cast(StringType()),
            col("NNUExam.FemoralsComments_NZ").cast(StringType()),
            col("NNUExam.InguinalHernia").cast(StringType()),
            col("NNUExam.InguinalHerniaComments").cast(StringType()),
            col("NNUExam.GeneralComments").cast(StringType()),
            col("NNUExam.SyncScope").cast(StringType()),
            greatest(col("NNUExam.ADC_UPDT"), col("PDEM.ADC_UPDT")).alias("ADC_UPDT")
        )
        .filter((col("NNUExam.ADC_UPDT") > max_adc_updt) | (col("PDEM.ADC_UPDT") > max_adc_updt) | (col("NIE.ADC_UPDT") > max_adc_updt))
    )

@dlt.view(name="mat_nnu_exam_update")
def mat_nnu_exam_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_mat_nnu_exam_incr")
    )

dlt.create_target_table(
    name = "rde_mat_nnu_exam",
    comment = nnu_exam_comment,
    schema = schema_rde_mat_nnu_exam,
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID,ExamDate,NHS_Number"
    }
)

dlt.apply_changes(
    target = "rde_mat_nnu_exam",
    source = "mat_nnu_exam_update",
    keys = ["PERSON_ID", "ExamDate", "NHS_Number"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

nnu_episodes_comment = "Details of Neonatal Unit Episodes."

schema_rde_mat_nnu_episodes = StructType([
     StructField("Person_ID", StringType(), True, metadata={'comment': "The Person_ID column represents a unique identifier"}),
    StructField("NHS_Number", StringType(), True, metadata={'comment': "The NHS_Number column represents the unique National Health Service number assigned to each patient in the United Kingdom for identifying individuals across the healthcare system and linking their medical records."}),
    StructField("MRN", StringType(), True, metadata={'comment': "The MRN (Medical Record Number) column represents a unique identifier assigned to each patient in the hospital's medical records system, which helps track and associate all medical information for that individual across different visits or episodes of care."}),
    StructField("CareLocationName", StringType(), True, metadata={'comment': "The CareLocationName column represents the names of the neonatal care units or hospital wards where the baby received care during their admission or episode of care."}),
    StructField("EpisodeType", StringType(), True, metadata={'comment': "The EpisodeType column represents the type of care episode or admission for the neonate, such as a neonatal intensive care unit (NICU) stay denoted by nnu (neonatal unit)."}),
    StructField("Sex", StringType(), True, metadata={'comment': "This column represents the assigned sex of the neonate or newborn baby, typically recorded as 'M' for male, 'F' for female, or 'I' for intersex/indeterminate in cases where the sex is ambiguous or undetermined at birth."}),
    StructField("BirthTimeBaby", TimestampType(), True, metadata={'comment': "The BirthTimeBaby column represents the date and time when the baby was born or delivered, which is a critical timestamp in neonatal care for tracking the baby's age, gestational period, and various developmental milestones and health interventions."}),
    StructField("GestationWeeks", IntegerType(), True, metadata={'comment': "The GestationWeeks column represents the number of completed weeks of gestation or pregnancy for the newborn baby at the time of birth."}),
    StructField("GestationDays", IntegerType(), True, metadata={'comment': "GestationDays represents the number of days in addition to the completed weeks of gestation for a newborn baby, providing more precise information about the gestational age at the time of birth."}),
    StructField("Birthweight", FloatType(), True, metadata={'comment': "The Birthweight column represents the weight of the newborn baby in grams at the time of birth, which is an important indicator of the baby's health and potential complications associated with low or high birth weight."}),
    StructField("BirthLength", FloatType(), True, metadata={'comment': "BirthLength represents the length of the baby measured at birth, typically in centimeters, which is an important indicator of the baby's overall size and development at the time of delivery in the neonatal intensive care unit."}),
    StructField("BirthHeadCircumference", FloatType(), True, metadata={'comment': "The BirthHeadCircumference column represents the circumference of the baby's head measured in centimeters at the time of birth, which is an important indicator of fetal growth and potential developmental issues."}),
    StructField("BirthOrder", IntegerType(), True, metadata={'comment': "The BirthOrder column represents the order in which the baby was born in the case of a multiple birth, such as twins, triplets, etc. For singleton births, the value is typically 1. The values in the sample data (4, 1, 3, None, 2, 6, 9, 5, 7) indicate the birth order of the baby in a multiple birth situation, with 'None' likely representing a missing or unknown value."}),
    StructField("FetusNumber", IntegerType(), True, metadata={'comment': "FetusNumber represents the numerical identifier assigned to each fetus in a multiple pregnancy or birth, indicating the order or number of the fetus among the total fetuses delivered."}),
    StructField("BirthSummary", StringType(), True, metadata={'comment': "The BirthSummary column provides a brief summary of key details about the baby's birth, typically including the baby's gender, whether it was a singleton or multiple birth, the date and time of birth, the gestational age at birth (in weeks and days), and the birth weight of the baby."}),
    StructField("EpisodeNumber", IntegerType(), True, metadata={'comment': "The EpisodeNumber column represents a unique identifier or sequence number assigned to each episode or admission of a baby in the Neonatal Intensive Care Unit (NICU), allowing for tracking and distinguishing multiple episodes of care for the same baby."}),
    StructField("AdmitTime", TimestampType(), True, metadata={'comment': "The AdmitTime column represents the date and time when a newborn baby was admitted to the neonatal intensive care unit (NICU) for medical care and observation."}),
    StructField("AdmitFromName", StringType(), True, metadata={'comment': "The AdmitFromName column represents the name of the hospital, facility or location from which the neonate (newborn baby) was admitted or transferred to the current healthcare institution providing neonatal intensive care unit (NICU) services."}),
    StructField("AdmitFromNHSCode", StringType(), True, metadata={'comment': "The AdmitFromNHSCode column represents the NHS code or identifier of the healthcare facility or location from which the baby was admitted or transferred to the current neonatal intensive care unit."}),
    StructField("ProviderName", StringType(), True, metadata={'comment': "The ProviderName column represents the name of the healthcare facility or provider that is responsible for delivering neonatal care to the patient during a particular episode or admission."}),
    StructField("ProviderNHSCode", StringType(), True, metadata={'comment': "The ProviderNHSCode column represents a unique code assigned to the healthcare provider or facility responsible for delivering neonatal care during the episode recorded in the database."}),
    StructField("NetworkName", StringType(), True, metadata={'comment': "NetworkName represents the name of the neonatal network or regional healthcare system that the hospital or care location belongs to, which helps coordinate and manage neonatal intensive care services across different hospitals and facilities within a specific geographic area."}),
    StructField("AdmitTemperature", FloatType(), True, metadata={'comment': "represents the body temperature of the neonate measured at the time of admission to the neonatal intensive care unit, recorded in degrees Celsius or Fahrenheit."}),
    StructField("AdmitTemperatureTime", TimestampType(), True, metadata={'comment': "The AdmitTemperatureTime column represents the date and time when the baby's temperature was recorded upon admission to the neonatal intensive care unit."}),
    StructField("AdmitBloodPressure", StringType(), True, metadata={'comment': "represents the systolic blood pressure (in mmHg) of the neonate at the time of admission to the neonatal intensive care unit."}),
    StructField("AdmitHeartRate", IntegerType(), True, metadata={'comment': "represents the heart rate of the neonate (newborn baby) measured at the time of admission to the neonatal intensive care unit (NICU), recorded in beats per minute."}),
    StructField("AdmitRespiratoryRate", IntegerType(), True, metadata={'comment': "represents the respiratory rate (breaths per minute) of the neonate at the time of admission to the Neonatal Intensive Care Unit (NICU)."}),
    StructField("AdmitSaO2", FloatType(), True, metadata={'comment': "The AdmitSaO2 column represents the oxygen saturation level of the neonate's blood measured at the time of admission to the neonatal intensive care unit, which is an important vital sign for assessing the baby's respiratory and circulatory status."}),
    StructField("AdmitBloodGlucose", FloatType(), True, metadata={'comment': "The AdmitBloodGlucose column represents the blood glucose level (in mmol/L or mg/dL) of the neonate at the time of admission to the neonatal intensive care unit (NICU). Blood glucose levels are closely monitored in newborns, especially preterm infants, as abnormal levels can indicate metabolic disorders or other underlying conditions that require prompt management."}),
    StructField("AdmitWeight", FloatType(), True, metadata={'comment': "The AdmitWeight column represents the weight of the baby in grams at the time of admission to the neonatal intensive care unit (NICU)."}),
    StructField("AdmitHeadCircumference", FloatType(), True, metadata={'comment': "The AdmitHeadCircumference column represents the measurement of the baby's head circumference in centimeters at the time of admission to the neonatal intensive care unit."}),
    StructField("DischTime", TimestampType(), True, metadata={'comment': "DischTime represents the date and time when the neonate (newborn baby) was discharged or released from the neonatal intensive care unit (NICU) after receiving care and treatment."}),
    StructField("DischargeHospitalName", StringType(), True, metadata={'comment': "The DischargeHospitalName column represents the name of the hospital or healthcare facility where the neonate was discharged or transferred to after receiving care in the neonatal intensive care unit."}),
    StructField("DischargeHospitalCode", StringType(), True, metadata={'comment': "The DischargeHospitalCode column contains unique codes representing the hospitals or healthcare facilities where the neonatal patient was discharged to after their stay at the neonatal intensive care unit."}),
    StructField("DischargeWeight", FloatType(), True, metadata={'comment': "represents the weight (in grams) of the neonate at the time of discharge from the neonatal intensive care unit or hospital, which is an important indicator of the baby's growth and overall health status prior to going home."}),
    StructField("DischargeHeadCircumference", FloatType(), True, metadata={'comment': "records the measurement of the baby's head circumference (in cm or inches) at the time of discharge from the neonatal intensive care unit (NICU). Head circumference is an important growth parameter monitored in newborns and infants, as it can indicate potential issues with brain growth or development."}),
    StructField("DischargeMilk", StringType(), True, metadata={'comment': "The DischargeMilk column represents the type of milk the baby was receiving at the time of discharge from the neonatal intensive care unit, with the coded values potentially indicating breast milk (1), formula (2), a combination of breast milk and formula (3), and other types of milk or feeding (4)."}),
    StructField("DischargeFeeding", StringType(), True, metadata={'comment': "The DischargeFeeding column represents the type of feeding method(s) used for the infant at the time of discharge from the neonatal unit, coded as a combination of different feeding types such as breast milk, formula, tube feeding, etc."}),
    StructField("HomeTubeFeeding", StringType(), True, metadata={'comment': "The HomeTubeFeeding column indicates whether the baby was discharged from the hospital requiring tube feeding at home, where a value of 1 means tube feeding was required at home, and 0 or None means no home tube feeding was necessary."}),
    StructField("DischargeOxygen", StringType(), True, metadata={'comment': "The DischargeOxygen column represents whether the newborn infant required supplemental oxygen at the time of discharge from the neonatal intensive care unit."}),
    StructField("EpisodeSummary", StringType(), True, metadata={'comment': "The EpisodeSummary column provides a brief narrative summary of each baby's admission episode in the Neonatal Intensive Care Unit (NICU), including the admission date and time, the source location (e.g., labor ward, another hospital), the discharge date and time, and the discharge destination (e.g., home, another hospital, postnatal ward)."}),
    StructField("VentilationDays", IntegerType(), True, metadata={'comment': "The VentilationDays column represents the number of days the neonate (newborn baby) required mechanical ventilation support during their stay in the Neonatal Intensive Care Unit (NICU)."}),
    StructField("CPAPDays", IntegerType(), True, metadata={'comment': "CPAPDays represents the number of days a newborn infant in a Neonatal Intensive Care Unit (NICU) received respiratory support through Continuous Positive Airway Pressure (CPAP), a non-invasive ventilation technique that delivers constant air pressure into the baby's airways to keep the lungs partially inflated, facilitating breathing and gas exchange."}),
    StructField("OxygenDays", IntegerType(), True, metadata={'comment': "represents the total number of days a neonate (newborn) required supplemental oxygen therapy during their hospital stay in the Neonatal Intensive Care Unit (NICU)."}),
    StructField("OxygenDaysNoVent", IntegerType(), True, metadata={'comment': "The OxygenDaysNoVent column represents the number of days a neonate received supplemental oxygen therapy without mechanical ventilation during their hospital stay in the Neonatal Intensive Care Unit (NICU)."}),
    StructField("OxygenLastTime", TimestampType(), True, metadata={'comment': "The OxygenLastTime column represents the date and time when oxygen supplementation was last provided to the neonate during their hospital stay in the Neonatal Intensive Care Unit (NICU)."}),
    StructField("ICCareDays", IntegerType(), True, metadata={'comment': "The ICCareDays column represents the number of days the neonate (newborn baby) spent in the Intensive Care Unit (ICU) during their hospital stay for neonatal care."}),
    StructField("HDCareDays", IntegerType(), True, metadata={'comment': "The HDCareDays column represents the number of days the neonate (newborn baby) spent receiving high dependency care during their hospital stay in the Neonatal Intensive Care Unit (NICU)."}),
    StructField("SCCareDays", IntegerType(), True, metadata={'comment': "SCCareDays represents the number of days the neonate (newborn baby) spent receiving special care during their hospital stay in the Neonatal Intensive Care Unit (NICU)."}),
    StructField("ICCareDays2011", IntegerType(), True, metadata={'comment': "ICCareDays2011 represents the number of days an infant spent receiving intensive care during their hospital stay in the year 2011, which typically involves close monitoring and advanced medical interventions for critically ill or premature newborns."}),
    StructField("HDCareDays2011", IntegerType(), True, metadata={'comment': "HDCareDays2011 represents the number of days the neonate spent in high dependency care during the year 2011 for that particular episode or admission to the Neonatal Intensive Care Unit."}),
    StructField("SCCareDays2011", IntegerType(), True, metadata={'comment': "represents the number of days a neonate (newborn baby) spent in Special Care during the year 2011 for a particular episode of care in the Neonatal Intensive Care Unit (NICU)."}),
    StructField("NormalCareDays2011", IntegerType(), True, metadata={'comment': "NormalCareDays2011 represents the number of days a newborn baby spent in a normal care setting within a neonatal unit during the year 2011."}),
    StructField("HRG1", StringType(), True, metadata={'comment': "represents the first Healthcare Resource Group (HRG) code assigned to the patient's episode, which is used for reimbursement purposes and reflects the complexity and resource requirements of the care provided."}),
    StructField("HRG2", StringType(), True, metadata={'comment': "represents the second Healthcare Resource Group (HRG) code assigned to the neonatal episode, which is used for categorizing healthcare activities and associated costs for reimbursement purposes in neonatal care."}),
    StructField("HRG3", StringType(), True, metadata={'comment': "represents the third level of Healthcare Resource Group (HRG) code, which is a grouping used in the National Health Service (NHS) for categorizing hospital cases and associated costs for reimbursement purposes within neonatal intensive care units."}),
    StructField("HRG4", StringType(), True, metadata={'comment': "a column that represents the fourth Healthcare Resource Group (HRG) code assigned to the neonatal episode, which is used for categorizing and reimbursing hospital care based on the complexity of the case and the resources required."}),
    StructField("HRG5", StringType(), True, metadata={'comment': "a field related to Healthcare Resource Groups (HRGs), which are standardized groups used for categorizing and costing hospital episodes or treatments in the UK's National Health Service. This specific column likely represents the fifth HRG code associated with the neonatal care episode, but the lack of diverse sample values makes it difficult to provide a more detailed explanation."}),
    StructField("LocnNNUDays", IntegerType(), True, metadata={'comment': "LocnNNUDays represents the number of days the neonate spent in the Neonatal Intensive Care Unit (NICU) during their hospital stay."}),
    StructField("LocnTCDays", IntegerType(), True, metadata={'comment': "LocnTCDays represents the number of days the neonate spent in the Transitional Care (TC) location during the hospital episode."}),
    StructField("LocnPNWDays", IntegerType(), True, metadata={'comment': "LocnPNWDays represents the number of days a newborn spent in the Postnatal Ward (PNW) location during their hospital stay in the neonatal intensive care unit."}),
    StructField("LocnOBSDays", IntegerType(), True, metadata={'comment': "LocnOBSDays represents the number of days the neonate spent in the Obstetric (OBS) care location or unit during the episode of care."}),
    StructField("LocnNNUPortion", FloatType(), True, metadata={'comment': "LocnNNUPortion represents the percentage or portion of the total hospital stay that the neonate spent in the Neonatal Intensive Care Unit (NICU) during a particular episode of care."}),
    StructField("LocnTCPoriton", FloatType(), True, metadata={'comment': "LocnTCPoriton represents the portion or percentage of time during the neonatal care episode that the baby spent in the Transitional Care (TC) location or unit."}),
    StructField("LocnPNWPortion", FloatType(), True, metadata={'comment': "represents the portion or fraction of a neonatal episode that was spent in a Post-Natal Ward (PNW) location within the neonatal unit, which typically provides lower levels of care compared to Intensive Care or High Dependency units."}),
    StructField("DrugsDuringStay", StringType(), True, metadata={'comment': "This column lists the drugs and medications administered to the newborn baby during their hospital stay in the Neonatal Intensive Care Unit (NICU). It includes antibiotics, pain relievers, vitamins, nutritional supplements, respiratory medications, cardiovascular drugs, and other therapeutic agents used in the treatment and management of various neonatal conditions and complications."}),
    StructField("DiagnosisDuringStay", StringType(), True, metadata={'comment': "This column contains the various medical diagnoses or conditions identified during the neonate's hospital stay in the Neonatal Intensive Care Unit (NICU). It documents the different health issues, complications, and diseases that the newborn infant experienced or was at risk for during their NICU admission."}),
    StructField("NationalIDMother", StringType(), True, metadata={'comment': "represents a unique national identifier or number assigned to each mother, used for tracking and identifying records across healthcare systems in the context of neonatal care."}),
    StructField("BloodGroupMother", StringType(), True, metadata={'comment': "The BloodGroupMother column represents the blood group and Rh factor of the mother, which is important information for neonatal care as it can help identify potential blood incompatibility issues between the mother and baby that may require treatment or precautions."}),
    StructField("BirthDateMother", DateType(), True, metadata={'comment': "The BirthDateMother column represents the date of birth of the mother who gave birth to the baby admitted to the neonatal intensive care unit."}),
    StructField("AgeMother", IntegerType(), True, metadata={'comment': "The AgeMother column represents the age of the mother in years at the time of giving birth to the baby admitted to the Neonatal Intensive Care Unit (NICU)."}),
    StructField("HepBMother", StringType(), True, metadata={'comment': "The HepBMother column indicates whether the mother has tested positive or negative for the Hepatitis B virus during pregnancy, which is important information for determining the appropriate neonatal care and preventive measures for the newborn."}),
    StructField("HepBMotherHighRisk", StringType(), True, metadata={'comment': "HepBMotherHighRisk indicates whether the mother of the newborn is considered at high risk for hepatitis B virus (HBV) infection, which can potentially transmit the virus to the baby during childbirth."}),
    StructField("HivMother", StringType(), True, metadata={'comment': "The HivMother column indicates the HIV status of the mother, which is an important factor to consider for neonatal care and potential transmission to the baby."}),
    StructField("RubellaScreenMother", StringType(), True, metadata={'comment': "The RubellaScreenMother column indicates the rubella immunity status of the mother, which is an important factor in assessing the risk of congenital rubella syndrome in the newborn. The values include 'Unknown', 'None' (not immune), 'Immune', 'Not tested', and 'Non-immune', reflecting whether the mother was screened for rubella immunity during pregnancy and the result of that screening."}),
    StructField("SyphilisScreenMother", StringType(), True, metadata={'comment': "SyphilisScreenMother represents the result of a screening test performed on the mother during pregnancy to detect the presence of syphilis, a sexually transmitted infection that can be passed from mother to baby during pregnancy or delivery."}),
    StructField("MumHCV", StringType(), True, metadata={'comment': "The MumHCV column indicates the hepatitis C virus (HCV) status of the baby's mother during pregnancy, which is an important factor in neonatal care as HCV can potentially be transmitted from mother to child during pregnancy or childbirth."}),
    StructField("HepCPCRMother", StringType(), True, metadata={'comment': "HepCPCRMother represents the Hepatitis C PCR test result for the mother, indicating whether she has an active Hepatitis C viral infection during the pregnancy, which could potentially be transmitted to the newborn."}),
    StructField("MumVDRL", StringType(), True, metadata={'comment': "MumVDRL represents the result of a blood test for syphilis performed on the mother during pregnancy, which is important for detecting potential congenital syphilis infection in the newborn."}),
    StructField("MumTPHA", StringType(), True, metadata={'comment': "MumTPHA represents the result of the Treponema pallidum haemagglutination assay (TPHA) test for syphilis performed on the mother during pregnancy or around the time of delivery."}),
    StructField("MaternalPyrexiaInLabour38c", StringType(), True, metadata={'comment': "indicates whether the mother experienced a fever (pyrexia) above 38°C during labor, which can be a risk factor for adverse neonatal outcomes."}),
    StructField("IntrapartumAntibioticsGiven", StringType(), True, metadata={'comment': "The IntrapartumAntibioticsGiven column indicates whether antibiotics were administered to the mother during labor (intrapartum period) prior to the baby's delivery, with values of 0 representing no antibiotics given, 1 representing antibiotics given, and None indicating missing data."}),
    StructField("MeconiumStainedLiquor", StringType(), True, metadata={'comment': "The MeconiumStainedLiquor column indicates whether the amniotic fluid (the liquid surrounding the baby in the womb) was stained with meconium (the baby's first stool) during labor and delivery, which can be a sign of fetal distress and may require additional monitoring or interventions for the newborn."}),
    StructField("MembraneRuptureDate", DateType(), True, metadata={'comment': "The MembraneRuptureDate column records the date when the amniotic membrane surrounding the fetus ruptured or broke, which typically signals the onset of labor and delivery."}),
    StructField("MembranerupturedDuration", FloatType(), True, metadata={'comment': "represents the duration in minutes or hours between when the mother's membranes (amniotic sac) ruptured and the baby's birth, which is an important factor in assessing the risk of infection or complications during labor and delivery."}),
    StructField("ParentsConsanguinous", StringType(), True, metadata={'comment': "The ParentsConsanguinous column indicates whether the parents of the neonate are related by blood, which could increase the risk of genetic disorders or congenital anomalies in the child."}),
    StructField("DrugsAbusedMother", StringType(), True, metadata={'comment': "The DrugsAbusedMother column records information about any drugs or substances that were abused or misused by the mother during pregnancy, which could potentially impact the health and development of the newborn baby."}),
    StructField("SmokingMother", StringType(), True, metadata={'comment': "The SmokingMother column indicates whether the mother of the baby was smoking during the pregnancy, with a value of '1' indicating that the mother was a smoker, and '0' indicating that the mother was a non-smoker. This information is relevant for neonatal care as maternal smoking during pregnancy can have adverse effects on the developing fetus and impact the health of the newborn."}),
    StructField("CigarettesMother", IntegerType(), True, metadata={'comment': "The CigarettesMother column represents the number of cigarettes smoked by the mother during pregnancy, which is an important risk factor for various adverse neonatal outcomes and complications."}),
    StructField("AlcoholMother", StringType(), True, metadata={'comment': "The AlcoholMother column indicates whether the mother of the neonate consumed alcohol during pregnancy, with possible values being 1 (yes), 2 (unknown or not recorded), 0 (no), or None (missing data)."}),
    StructField("PreviousPregnanciesNumber", IntegerType(), True, metadata={'comment': "The PreviousPregnanciesNumber column represents the number of previous pregnancies the mother has had before the current pregnancy and birth recorded in this neonatal episode."}),
    StructField("AgeFather", IntegerType(), True, metadata={'comment': "The AgeFather column represents the age of the baby's father at the time of the baby's birth, which is an important demographic detail for neonatal care records."}),
    StructField("EthnicityFather", StringType(), True, metadata={'comment': "The EthnicityFather column represents the ethnic background or racial classification of the father of the newborn baby admitted to the neonatal intensive care unit, which may be relevant for understanding potential genetic or cultural factors influencing the baby's health and care."}),
    StructField("GestationWeeksCalculated", IntegerType(), True, metadata={'comment': "represents the calculated gestational age of the baby in weeks, based on various factors such as the mother's last menstrual period, ultrasound measurements, or other clinical assessments during the pregnancy."}),
    StructField("GestationDaysCalculated", IntegerType(), True, metadata={'comment': "GestationDaysCalculated represents the calculated number of days of gestation for the baby, along with the GestationWeeksCalculated column, which indicates the precise gestational age at the time of birth."}),
    StructField("BookingName", StringType(), True, metadata={'comment': "The BookingName column represents the name of the hospital or maternity unit where the mother initially booked or registered for antenatal care and planned to give birth."}),
    StructField("BookingNHSCode", StringType(), True, metadata={'comment': "contains unique codes or identifiers assigned to the healthcare facilities or hospitals where the mother's pregnancy was initially booked or registered for prenatal care and delivery."}),
    StructField("SteroidsAntenatalGiven", StringType(), True, metadata={'comment': "The SteroidsAntenatalGiven column indicates whether antenatal steroid medications were given to the mother before delivery to help mature the baby's lungs and reduce respiratory complications for premature infants."}),
    StructField("SteroidsName", StringType(), True, metadata={'comment': "represents the name or type of steroid medication administered to the mother and/or baby as part of antenatal or neonatal care, with the sample values indicating either no steroid given (None) or potentially a code referring to a specific steroid drug."}),
    StructField("SteroidsAntenatalCourses", IntegerType(), True, metadata={'comment': "The SteroidsAntenatalCourses column represents the number of courses of antenatal steroid treatments given to the mother before delivery, which can help promote fetal lung maturity and reduce complications in preterm infants."}),
    StructField("PlaceOfBirthName", StringType(), True, metadata={'comment': "The PlaceOfBirthName column represents the name of the hospital or facility where the baby was born."}),
    StructField("PlaceOfBirthNHSCode", StringType(), True, metadata={'comment': "contains codes representing the specific NHS hospital or facility where the baby was born. These codes likely correspond to unique identifiers assigned to different healthcare providers or locations within the National Health Service (NHS) system in the UK."}),
    StructField("Apgar1", IntegerType(), True, metadata={'comment': "The Apgar1 column represents the Apgar score assessed at 1 minute after birth, which is a quick method for evaluating a newborn's physical condition and need for medical attention."}),
    StructField("Apgar5", IntegerType(), True, metadata={'comment': "The Apgar5 column represents the Apgar score assigned to a newborn baby at 5 minutes after birth, which is a standardized assessment of the baby's overall health and well-being based on factors such as heart rate, respiratory effort, muscle tone, reflex irritability, and color."}),
    StructField("Apgar10", IntegerType(), True, metadata={'comment': "Apgar10 represents the Apgar score assigned to the newborn baby at 10 minutes after birth, which is a quantitative measure of the baby's overall condition and well-being, evaluated based on factors such as heart rate, respiratory effort, muscle tone, reflex irritability, and color."}),
    StructField("BabyBloodType", StringType(), True, metadata={'comment': "The BabyBloodType column represents the blood type or blood group of the newborn baby, which is an important factor in neonatal care for potential blood transfusions, monitoring for hemolytic disease, and identifying any incompatibilities with the mother's blood type."}),
    StructField("Crib2Score", FloatType(), True, metadata={'comment': "The Crib2Score column represents the Clinical Risk Index for Babies (CRIB-II) score, which is a tool used to assess the initial risk of mortality and need for intensive care for premature or critically ill newborns based on factors such as birth weight, gestational age, congenital malformations, and clinical measurements at birth."}),
    StructField("FinalNNUOutcome", StringType(), True, metadata={'comment': "The FinalNNUOutcome column represents the ultimate outcome or discharge status of the neonate from the Neonatal Intensive Care Unit (NICU), such as whether the baby was discharged home, transferred to a regular hospital ward, died during the NICU stay, or if the outcome is unknown or not recorded."}),
    StructField("VitaminKGiven", StringType(), True, metadata={'comment': "VitaminKGiven represents whether or not vitamin K was administered to the newborn baby, likely to prevent vitamin K deficiency bleeding (a potentially serious condition in newborns). The values indicate if vitamin K was given (1) or not (0 or None), with 7 possibly indicating a specific dosage or other related information."}),
    StructField("CordArterialpH", FloatType(), True, metadata={'comment': "The CordArterialpH column represents the pH value measured from the umbilical cord artery at the time of birth. This value provides important information about the baby's acid-base balance and oxygenation status during the intrapartum period."}),
    StructField("CordVenouspH", FloatType(), True, metadata={'comment': "The CordVenouspH column contains the pH value measured from the umbilical vein of the newborn baby after delivery, which indicates the acid-base balance in the baby's venous blood. A normal range for cord venous pH is typically between 7.25 and 7.45. Lower values may indicate fetal distress or metabolic acidosis."}),
    StructField("CordPcO2Arterial", FloatType(), True, metadata={'comment': "CordPcO2Arterial represents the partial pressure of carbon dioxide (CO2) measured in the arterial blood from the umbilical cord at the time of birth. It is an important indicator of the baby's respiratory status and acid-base balance in the womb."}),
    StructField("CordPcO2Venous", FloatType(), True, metadata={'comment': "CordPcO2Venous represents the partial pressure of carbon dioxide (CO2) measured in the venous blood from the umbilical cord of a newborn baby. This measurement provides information about the baby's acid-base status and respiratory condition at birth."}),
    StructField("CordArterialBE", FloatType(), True, metadata={'comment': "The CordArterialBE column represents the base excess (a measure of the pH balance or metabolic state) in the umbilical cord arterial blood of a newborn baby at the time of delivery. This value helps assess the baby's condition and potential need for interventions related to metabolic acidosis or other pH imbalances."}),
    StructField("CordVenousBE", FloatType(), True, metadata={'comment': "CordVenousBE represents the base excess value measured from the venous umbilical cord blood at the time of delivery, which provides information about the newborn's acid-base status and can indicate potential complications or distress during the birthing process."}),
    StructField("CordClamping", StringType(), True, metadata={'comment': "The CordClamping column represents whether the umbilical cord was clamped after birth, and potentially the timing of when it was clamped, for a newborn baby admitted to the neonatal intensive care unit."}),
    StructField("CordClampingTimeMinute", IntegerType(), True, metadata={'comment': "The CordClampingTimeMinute column represents the number of minutes after the birth when the umbilical cord was clamped and cut, separating the baby from the placenta."}),
    StructField("CordClampingTimeSecond", IntegerType(), True, metadata={'comment': "The CordClampingTimeSecond column represents the duration in seconds after birth that the umbilical cord was clamped, which is an important factor in neonatal care as it can affect the transfer of blood and nutrients from the placenta to the newborn."}),
    StructField("CordStripping", StringType(), True, metadata={'comment': "indicates whether cord stripping was performed during the delivery process, which involves milking the blood from the umbilical cord toward the baby after birth to increase the baby's blood volume."}),
    StructField("ResusSurfactant", StringType(), True, metadata={'comment': "The ResusSurfactant column indicates whether a preterm infant required administration of exogenous surfactant to aid lung function and improve respiratory distress syndrome after resuscitation at birth."}),
    StructField("Seizures", StringType(), True, metadata={'comment': "indicates whether the neonate (newborn baby) experienced seizures during their stay in the Neonatal Intensive Care Unit (NICU), with a value of '1' representing the occurrence of seizures and potentially 'None' or a null value indicating no seizures were observed."}),
    StructField("HIEGrade", StringType(), True, metadata={'comment': "HIEGrade represents the grade or severity of Hypoxic-Ischemic Encephalopathy (HIE), a condition caused by lack of oxygen and blood flow to the brain around the time of birth, with values ranging from 1 to 3 indicating mild, moderate, or severe HIE, respectively, and 'None' indicating the absence of HIE."}),
    StructField("Anticonvulsants", StringType(), True, metadata={'comment': "The Anticonvulsants column represents the anticonvulsant medications administered to the neonate (newborn baby) for the treatment or prevention of seizures, which can occur in certain neonatal conditions or complications."}),
    StructField("Pneumothorax", StringType(), True, metadata={'comment': "The Pneumothorax column indicates whether the neonate (newborn baby) suffered from a pneumothorax, which is a condition where air leaks into the space between the lungs and the chest wall, during their stay in the Neonatal Intensive Care Unit (NICU)."}),
    StructField("NecrotisingEnterocolitis", StringType(), True, metadata={'comment': "indicates whether the neonate (newborn baby) developed necrotizing enterocolitis (NEC), a serious gastrointestinal condition that primarily affects premature infants, with the values representing different levels of severity or stages of the condition."}),
    StructField("NeonatalAbstinence", StringType(), True, metadata={'comment': "The NeonatalAbstinence column indicates whether a newborn baby experienced symptoms of withdrawal or abstinence due to the mother's use of addictive substances (such as opioids, alcohol, or other drugs) during pregnancy, which can cause neonatal abstinence syndrome (NAS) in the infant."}),
    StructField("ROPScreenDate", DateType(), True, metadata={'comment': "The ROPScreenDate column represents the date when the baby was screened for retinopathy of prematurity (ROP), a potentially blinding eye disorder that primarily affects premature infants."}),
    StructField("ROPSurgeryDate", DateType(), True, metadata={'comment': "ROPSurgeryDate represents the date when a premature infant underwent surgery to treat Retinopathy of Prematurity (ROP), a potentially blinding eye disorder caused by abnormal blood vessel growth in the retina of premature babies."}),
    StructField("Dexamethasone", StringType(), True, metadata={'comment': "The Dexamethasone column indicates whether the preterm infant received dexamethasone, a corticosteroid medication used to help promote lung maturity and reduce respiratory distress in premature babies."}),
    StructField("PDAIndomethacin", StringType(), True, metadata={'comment': "The PDAIndomethacin column indicates whether or not the neonate received indomethacin, a medication used to treat patent ductus arteriosus (PDA), a condition where a blood vessel called the ductus arteriosus fails to close properly after birth."}),
    StructField("PDAIbuprofen", StringType(), True, metadata={'comment': "The PDAIbuprofen column indicates whether the neonate received ibuprofen medication for treatment of a Patent Ductus Arteriosus (PDA), a common congenital heart condition in premature infants where the ductus arteriosus fails to close after birth."}),
    StructField("PDASurgery", StringType(), True, metadata={'comment': "The PDASurgery column indicates whether the neonate underwent surgical intervention for a patent ductus arteriosus (PDA), which is a congenital heart defect where the ductus arteriosus fails to close after birth, allowing oxygenated blood to flow from the aorta to the pulmonary artery."}),
    StructField("PDADischarge", StringType(), True, metadata={'comment': "PDADischarge indicates whether the neonate had a patent ductus arteriosus (PDA) at the time of discharge from the neonatal intensive care unit, which is a common condition in premature infants where the blood vessel connecting the pulmonary artery and aorta fails to close properly after birth."}),
    StructField("UACTime", TimestampType(), True, metadata={'comment': "UACTime represents the date and time when an umbilical arterial catheter (UAC) was inserted in the neonate, which is commonly used for continuous blood pressure monitoring and blood sampling in critically ill neonates."}),
    StructField("UVCTime", TimestampType(), True, metadata={'comment': "UVCTime represents the date and time when an umbilical venous catheter (UVC) was inserted into the newborn's umbilical vein for delivering fluids, medications, or monitoring purposes during neonatal intensive care."}),
    StructField("LongLineTime", TimestampType(), True, metadata={'comment': "The LongLineTime column represents the date and time when a long intravenous line was inserted into the neonate for administering medications, fluids or nutrients. Long lines are commonly used in neonatal intensive care units to provide critical care to premature or ill newborns."}),
    StructField("PeripheralArterialLineTime", TimestampType(), True, metadata={'comment': "The PeripheralArterialLineTime column represents the date and time when a peripheral arterial line was inserted into the neonate for continuous blood pressure monitoring and blood sampling during their stay in the Neonatal Intensive Care Unit."}),
    StructField("SurgicalLineTime", TimestampType(), True, metadata={'comment': "The SurgicalLineTime column likely represents the date and time when a surgical line (such as a central venous catheter or other indwelling catheter) was inserted into the neonate during their hospital stay for medical purposes like administering medications or fluids."}),
    StructField("ParenteralNutritionDays", IntegerType(), True, metadata={'comment': "ParenteralNutritionDays represents the number of days a neonate (newborn baby) received parenteral nutrition, which is the intravenous administration of nutrients, during their hospital stay in the Neonatal Intensive Care Unit (NICU)."}),
    StructField("HeadScanFirstTime", TimestampType(), True, metadata={'comment': "HeadScanFirstTime represents the date and time when the first head scan or cranial imaging procedure (such as an ultrasound or MRI) was performed on the neonate during their hospital stay in the Neonatal Intensive Care Unit (NICU)."}),
    StructField("HeadScanFirstResult", StringType(), True, metadata={'comment': "HeadScanFirstResult represents the result of the first head scan or cranial imaging (e.g., ultrasound, MRI) performed on the neonate, which can help detect potential abnormalities or conditions related to the brain and skull during the neonatal intensive care admission."}),
    StructField("HeadScanLastTime", TimestampType(), True, metadata={'comment': "HeadScanLastTime represents the date and time of the most recent head scan performed on the neonate during their hospital stay, which is crucial for monitoring and diagnosing potential neurological issues or intracranial complications in newborns admitted to the neonatal intensive care unit."}),
    StructField("HeadScanLastResult", StringType(), True, metadata={'comment': "HeadScanLastResult represents the findings or interpretation of the most recent cranial ultrasound or other neuroimaging examination performed on the neonate, indicating whether the scan showed any abnormalities or was normal."}),
    StructField("CongenitalAnomalies", StringType(), True, metadata={'comment': "This column represents any congenital anomalies or birth defects present in the newborn baby, such as structural malformations, genetic disorders, or developmental abnormalities detected during the neonatal period."}),
    StructField("VPShuntTime", TimestampType(), True, metadata={'comment': "VPShuntTime represents the date and time when a ventriculoperitoneal (VP) shunt was inserted in the baby to treat conditions like hydrocephalus or intraventricular hemorrhage, which are common complications in premature or critically ill neonates."}),
    StructField("BloodCultureFirstTime", TimestampType(), True, metadata={'comment': "The column records the date and time when the first blood culture test was performed on the neonate during their hospital stay to check for the presence of bacteria or fungi in the bloodstream."}),
    StructField("BloodCultureFirstResult", StringType(), True, metadata={'comment': "The column represents the microorganism(s) identified in the initial blood culture test performed on the neonate during the hospital stay, which helps diagnose bloodstream infections and guide appropriate antimicrobial treatment."}),
    StructField("CSFCultureFirstTime", TimestampType(), True, metadata={'comment': "CSFCultureFirstTime represents the date and time when the first cerebrospinal fluid (CSF) culture was taken from the baby during their hospital stay, which is an important diagnostic test to check for potential infections in the central nervous system."}),
    StructField("CSFCultureFirstResult", StringType(), True, metadata={'comment': "The column represents the result of the initial cerebrospinal fluid (CSF) culture test performed on the neonate, which detects the presence of any bacterial or fungal pathogens that may indicate a central nervous system infection."}),
    StructField("UrineCultureFirstTime", TimestampType(), True, metadata={'comment': "This column stores the date and time when the first urine culture test was performed on the baby during the hospital stay, which helps detect urinary tract infections or other relevant microbiological findings."}),
    StructField("UrineCultureFirstResult", StringType(), True, metadata={'comment': "The column represents the result of the first urine culture test performed on the neonate during their stay in the Neonatal Intensive Care Unit (NICU), which helps identify the presence of any bacterial or viral infections in the urinary tract."}),
    StructField("ExchangeTransfusion", StringType(), True, metadata={'comment': "The ExchangeTransfusion column indicates whether the newborn received an exchange transfusion procedure, which involves replacing the infant's blood with donor blood to treat conditions such as severe jaundice or hemolytic disease."}),
    StructField("Tracheostomy", StringType(), True, metadata={'comment': "The Tracheostomy column indicates whether the neonate (newborn baby) required a surgical procedure to create an opening in the neck into the trachea to assist with breathing during their hospital stay in the Neonatal Intensive Care Unit."}),
    StructField("PulmonaryVasodilatorTime", TimestampType(), True, metadata={'comment': "PulmonaryVasodilatorTime represents the date and time when pulmonary vasodilator medication, used to treat pulmonary hypertension in newborns, was administered or started during the neonatal intensive care unit stay."}),
    StructField("PulmonaryVasodilatorDrugs", StringType(), True, metadata={'comment': "The column represents the drugs administered to the neonate to dilate the pulmonary blood vessels and improve blood flow and oxygenation in the lungs, which is often required in cases of pulmonary hypertension or respiratory distress."}),
    StructField("Inotropes", StringType(), True, metadata={'comment': "The Inotropes column records the types of inotropic drugs administered to the neonate during their hospital stay, which are medications used to support cardiovascular function and improve blood flow by increasing the force of contraction of the heart muscle."}),
    StructField("InotropesFirstTime", TimestampType(), True, metadata={'comment': "InotropesFirstTime represents the date and time when inotropic medications, which are drugs used to support the heart's pumping ability, were first administered to the neonate during their hospital stay in the neonatal intensive care unit."}),
    StructField("PeritonealDialysis", StringType(), True, metadata={'comment': "The  column indicates whether the neonate underwent peritoneal dialysis, a procedure where fluid is introduced into the peritoneal cavity to remove waste products from the blood when the kidneys are not functioning properly."}),
    StructField("DischargeApnoeaCardioSat", StringType(), True, metadata={'comment': "The column indicates whether the neonate experienced apnea (temporary cessation of breathing) or cardiorespiratory desaturation (drop in blood oxygen levels) at the time of discharge from the neonatal intensive care unit."}),
    StructField("gastroschisis", StringType(), True, metadata={'comment': "The gastroschisis column represents whether the newborn infant has a congenital abdominal wall defect, where the intestines protrude outside the body through a small opening beside the umbilical cord. The values '1' and 'None' may indicate the presence or absence of this condition, respectively."}),
    StructField("Cooled", StringType(), True, metadata={'comment': "The 'Cooled' column indicates whether the baby received therapeutic hypothermia (cooling treatment) during their stay in the Neonatal Intensive Care Unit (NICU). Therapeutic hypothermia is a treatment strategy used for newborns with hypoxic-ischemic encephalopathy (HIE), a type of brain injury caused by lack of oxygen or blood flow, to help reduce the risk of long-term neurological damage."}),
    StructField("FirstConsultationWithParents", TimestampType(), True, metadata={'comment': "This column records the date and time of the first consultation or meeting between the healthcare providers and the parents of the neonate (newborn baby) during the hospital admission."}),
    StructField("ReceivedMothersMilkDuringAdmission", StringType(), True, metadata={'comment': "The column indicates whether the neonate received breast milk from their mother during their admission to the Neonatal Intensive Care Unit (NICU), with values such as 1 for yes, 0 for no, and potentially other codes like 7 or 9 for unknown or not applicable cases."}),
    StructField("DischargeLength", FloatType(), True, metadata={'comment': "The column represents the length (in centimeters) of the infant at the time of discharge from the neonatal intensive care unit or hospital stay."}),
    StructField("PrincipalDiagnosisAtDischarge", StringType(), True, metadata={'comment': "The  column represents the primary medical diagnosis or condition that was identified for the neonate during their hospital stay, which was recorded at the time of discharge from the neonatal intensive care unit."}),
    StructField("ActiveProblemsAtDischarge", StringType(), True, metadata={'comment': "This column contains text descriptions of any active medical problems or issues that the baby was still facing at the time of discharge from the neonatal care unit. These could include ongoing needs for respiratory support, feeding difficulties, diagnosed medical conditions, pending follow-up appointments or tests, and other unresolved health concerns that will require continued monitoring or treatment after leaving the hospital."}),
    StructField("PrincipleProceduresDuringStay", StringType(), True, metadata={'comment': "The column lists the major medical procedures, interventions, and diagnostic tests performed on the neonate during their stay in the Neonatal Intensive Care Unit (NICU). This can include procedures like intubation, mechanical ventilation, catheter insertions, administration of medications or treatments (e.g. surfactant, phototherapy), imaging tests (X-rays, ultrasounds, MRIs), and other diagnostic or therapeutic interventions necessary for the care of the newborn infant."}),
    StructField("RespiratoryDiagnoses", StringType(), True, metadata={'comment': "This column represents the various respiratory diagnoses or conditions observed in newborn babies admitted to the neonatal intensive care unit. It captures a range of respiratory issues, including respiratory distress syndrome, persistent pulmonary hypertension, pneumothorax, meconium aspiration syndrome, apnea, chronic lung disease, and other respiratory complications that may require specialized care and monitoring."}),
    StructField("CardiovascularDiagnoses", StringType(), True, metadata={'comment': "This column contains cardiovascular diagnoses or conditions related to the heart and blood vessels that were identified in the neonate during the admission. It includes congenital heart defects, vascular conditions, hypertension, hypotension, arrhythmias, and other cardiovascular abnormalities or issues observed in the newborn."}),
    StructField("GastrointestinalDiagnoses", StringType(), True, metadata={'comment': "This column represents various gastrointestinal diagnoses or conditions that a neonate may have, including jaundice, feeding problems, gastroesophageal reflux, necrotizing enterocolitis, congenital malformations of the gastrointestinal tract, and other digestive system disorders."}),
    StructField("NeurologyDiagnoses", StringType(), True, metadata={'comment': "This column represents various neurological diagnoses or conditions affecting the brain and nervous system in neonates or newborns, such as intraventricular hemorrhage, hypoxic-ischemic encephalopathy (HIE), seizures, congenital malformations of the brain, hydrocephalus, and other neurological disorders or abnormalities identified during the neonatal period."}),
    StructField("ROPDiagnosis", StringType(), True, metadata={'comment': "The column records any diagnosed eye or vision-related conditions or abnormalities in the preterm or newborn infant, including different grades of retinopathy of prematurity (ROP) as well as other congenital eye malformations, eye infections, or vision impairments."}),
    StructField("HaemDiagnoses", StringType(), True, metadata={'comment': "This column represents the various hematological (blood-related) diagnoses or conditions that a neonate may have, such as bleeding disorders, anemia, jaundice, polycythemia, thrombocytopenia, and other disorders related to blood cells, clotting, or hemolysis (breakdown of red blood cells)."}),
    StructField("RenalDiagnoses", StringType(), True, metadata={'comment': "The  column records various congenital or acquired renal and urological conditions diagnosed in neonates/infants during their hospital stay or at discharge, such as hypospadias, hydronephrosis, renal dysplasia, urethral valves, renal failure, dehydration, and other kidney or urinary tract abnormalities."}),
    StructField("SkinDiagnoses", StringType(), True, metadata={'comment': "The column appears to record various skin-related conditions, injuries, or anomalies diagnosed or observed in neonatal patients during their hospital stay or at birth. This could include conditions like jaundice, rashes, lacerations, burns, birthmarks, congenital skin disorders, and other dermatological issues relevant for neonatal care."}),
    StructField("MetabolicDiagnoses", StringType(), True, metadata={'comment': "The column represents any metabolic disorders or conditions diagnosed in the neonate during their stay in the Neonatal Intensive Care Unit (NICU), such as metabolic diseases, electrolyte imbalances, or other metabolic abnormalities."}),
    StructField("InfectionsDiagnoses", StringType(), True, metadata={'comment': "This column represents the different types of infections diagnosed in neonates (newborn infants) during their hospital stay or care episode. It includes bacterial infections like sepsis, meningitis, pneumonia, as well as viral infections like cytomegalovirus (CMV) and fungal infections like candidiasis. The values capture specific organisms causing the infections when known, as well as more general descriptions like 'suspected sepsis' or 'infection risk' when the causative agent is unknown or unconfirmed."}),
    StructField("SocialIssues", StringType(), True, metadata={'comment': "The SocialIssues column documents any social concerns, challenges, or circumstances surrounding the baby's family situation that may impact the baby's care or well-being. This could include issues like parental substance abuse, homelessness, child protective services involvement, young or unsupported parents, potential foster care placement, or other significant social factors that healthcare providers should be aware of during the baby's hospitalization and discharge planning."}),
    StructField("DayOneLocationOfCare", StringType(), True, metadata={'comment': "The DayOneLocationOfCare column represents the location or care setting where the neonate was cared for on the first day after birth, such as the neonatal intensive care unit (NICU, or 'nnu'), transitional care ('tc'), postnatal ward ('pnw'), or observation unit ('obs')."}),
    StructField("BirthCareLocationName", StringType(), True, metadata={'comment': "BirthCareLocationName represents the name of the hospital or healthcare facility where the baby was born and initially received care after birth."}),
    StructField("UnitResponsibleFor2YearFollowUp", StringType(), True, metadata={'comment': "This column indicates the hospital or healthcare unit responsible for performing the 2-year follow-up assessment and care for the neonate after discharge from the neonatal intensive care unit (NICU)."}),
    StructField("CordLactate", FloatType(), True, metadata={'comment': "CordLactate represents the level of lactate measured in the umbilical cord blood at the time of birth, which can indicate the degree of fetal hypoxia or metabolic acidosis experienced by the newborn during labor and delivery."}),
    StructField("ROPScreenFirstDateDueStart", DateType(), True, metadata={'comment': "The column represents the earliest date when the first screening for Retinopathy of Prematurity (ROP) was scheduled or expected to be performed for a premature infant admitted to the neonatal intensive care unit."}),
    StructField("ROPScreenFirstDateDueEnd", DateType(), True, metadata={'comment': "The column represents the end date by which the first screening for Retinopathy of Prematurity (ROP), a potentially blinding eye disorder affecting premature infants, should be performed according to the screening guidelines for that particular neonatal care unit."}),
    StructField("ROPFirstScreenStart", DateType(), True, metadata={'comment': "The column represents the date when the initial screening for Retinopathy of Prematurity (ROP), a potentially blinding eye disorder primarily affecting premature infants, was performed for the neonate during their hospital stay in the Neonatal Intensive Care Unit."}),
    StructField("ROPFirstScreenEnd", DateType(), True, metadata={'comment': "ROPFirstScreenEnd represents the end date of the first screening for Retinopathy of Prematurity (ROP), a potentially blinding eye disease that affects premature infants."}),
    StructField("LSOA", StringType(), True, metadata={'comment': "LSOA stands for Lower Layer Super Output Area, which is a geographic area used for small area statistics in the UK. It likely represents the LSOA code or region where the baby was born or where the neonatal care took place, providing information about the local area or population demographics."}),
    StructField("DateOfFirstExamination", DateType(), True, metadata={'comment': "The column represents the date when the first physical examination of the newborn baby was conducted after admission to the neonatal intensive care unit."}),
    StructField("DateOfRoutineNeonatalExamination", DateType(), True, metadata={'comment': "The column represents the date when a routine physical examination of the newborn baby was performed in the neonatal intensive care unit."}),
    StructField("MotherIntendToBreastFeed", StringType(), True, metadata={'comment': "The column indicates whether the mother intended to breastfeed her baby during the neonatal care episode, with 1 representing an intent to breastfeed and 0 indicating no intent to breastfeed."}),
    StructField("MagnesiumSulphate", StringType(), True, metadata={'comment': "The column indicates whether magnesium sulfate was administered to the mother during pregnancy or labor, which can help prevent or treat certain complications such as preterm labor or preeclampsia, and may provide neuroprotective effects for the newborn."}),
    StructField("ReasonMagnesiumSulphateNotGiven", StringType(), True, metadata={'comment': "The column records the reason why magnesium sulfate, a drug used to prevent or treat certain pregnancy complications, was not administered to the mother during the pregnancy or labor process for the particular neonatal episode."}),
    StructField("LabourWardDeath", StringType(), True, metadata={'comment': "The LabourWardDeath column indicates whether the baby died in the labor ward before being admitted to the Neonatal Intensive Care Unit (NICU)."}),
    StructField("AdmitPrincipalReason_Other", StringType(), True, metadata={'comment': "The column appears to capture additional or specific reasons for admission to the neonatal intensive care unit that do not fall under common or pre-defined categories. It allows for documenting various medical conditions, complications, or concerns that necessitated the admission of the newborn, such as suspected necrotizing enterocolitis, respiratory distress, congenital anomalies, or other neonatal issues requiring specialized care."}),
    StructField("TwoYearFollowUpPerformedAnyEpisode", StringType(), True, metadata={'comment': "The column indicates whether a follow-up assessment was conducted at around 2 years of age for the neonate during any of their admission episodes in the neonatal intensive care unit."}),
    StructField("CauseOfDeath1A", StringType(), True, metadata={'comment': "This column represents the primary or underlying cause of death for neonates who did not survive, as recorded on their death certificate under Part 1A. The sample values indicate various conditions like neonatal encephalopathy, necrotizing enterocolitis, bronchopulmonary dysplasia, sepsis, congenital anomalies, and prematurity-related complications that can lead to neonatal mortality."}),
    StructField("CauseOfDeath1B", StringType(), True, metadata={'comment': "This column records the underlying or contributory causes of death for babies who did not survive, providing details on the medical conditions or complications that led to the infant's demise, such as extreme prematurity, respiratory distress syndrome, sepsis, pulmonary hemorrhage, congenital anomalies, or other life-threatening neonatal illnesses or events."}),
    StructField("CauseOfDeath2", StringType(), True, metadata={'comment': "This column represents the secondary or contributory cause of death for a neonate admitted to the neonatal intensive care unit (NICU). It supplements the primary cause of death recorded in a separate column, providing additional information on underlying medical conditions, complications, or events that may have contributed to the infant's demise."}),
    StructField("DateTimeLeftHospital", TimestampType(), True, metadata={'comment': "The DateTimeLeftHospital column represents the date and time when a neonate (newborn baby) was discharged or left the hospital after receiving neonatal care."}),
    StructField("ReasonMagnesiumSulphateGiven", StringType(), True, metadata={'comment': "The ReasonMagnesiumSulphateGiven column represents the primary reason for administering magnesium sulfate to the mother, such as neuroprotection for the baby or other medical indications, during the pregnancy or labor process in neonatal care."}),
    StructField("TwoYearFollowUpPerformedAnyEpisode_Date", DateType(), True, metadata={'comment': "TwoYearFollowUpPerformedAnyEpisode_Date represents the date when a follow-up assessment was performed for the infant around 2 years after their initial neonatal intensive care unit (NICU) admission, likely to evaluate long-term developmental outcomes and any ongoing medical concerns."}),
    StructField("MaternalMedicalNotes", StringType(), True, metadata={'comment': "The MaternalMedicalNotes column contains free-text notes documenting any relevant medical conditions, diagnoses, medications, or other medical information pertaining to the mother during pregnancy or before delivery. This information can provide valuable context for understanding potential risk factors, complications, or medical considerations related to the mother's health that may have impacted the pregnancy or the neonate's condition."}),
    StructField("AnomalyScanComments", StringType(), True, metadata={'comment': "The AnomalyScanComments column contains free-text notes or comments regarding any abnormalities or findings detected during prenatal ultrasound scans or fetal anomaly scans performed during the pregnancy. These comments may include details about fetal growth patterns, placental issues, structural abnormalities or defects identified in the fetus, concerns raised from the scans, and any other relevant information recorded by the healthcare provider conducting the prenatal scans."}),
    StructField("ReceivedAntenatalCare", StringType(), True, metadata={'comment': "The ReceivedAntenatalCare column indicates whether the mother received prenatal care during her pregnancy, with possible values such as 0 for no antenatal care received, 1 for antenatal care received, or other coded values representing different levels or types of antenatal care."}),
    StructField("DateFirstUltrasound", DateType(), True, metadata={'comment': "The DateFirstUltrasound column represents the date when the first ultrasound scan was performed during the mother's pregnancy for the recorded baby."}),
    StructField("FollowUp", StringType(), True, metadata={'comment': "The FollowUp column appears to contain information about follow-up appointments or referrals scheduled for the baby after discharge from the Neonatal Intensive Care Unit (NICU). The values indicate the type of follow-up care needed, such as neonatal/pediatric clinic visits, specialist hospital appointments (e.g., ophthalmology, cleft palate clinic), referrals to other specialist clinics (e.g., physiotherapy, speech and language therapy, dietician), or home visits by the neonatal community team. The entries also specify the time frame for the follow-up, typically within a few weeks or months after discharge."}),
    StructField("timeReady", TimestampType(), True, metadata={'comment': "The timeReady column represents the date and time when the neonate (newborn baby) was ready for transfer or discharge from the neonatal intensive care unit (NICU)."}),
    StructField("BabyAwaiting", StringType(), True, metadata={'comment': "The BabyAwaiting column indicates the current status or situation the baby is awaiting, such as discharge from the hospital or transfer to another healthcare facility."}),
    StructField("TransferDestinationHospital", StringType(), True, metadata={'comment': "This column represents the unique identifier or code for the hospital or healthcare facility to which the neonate (newborn baby) was transferred for continued care or treatment. It tracks the destination hospital when a transfer out of the current care location is required, possibly due to a higher level of care needs or other reasons."}),
    StructField("EPOCDischargeLetterSent", StringType(), True, metadata={'comment': "EPOCDischargeLetterSent is a column that indicates whether a discharge letter or summary was sent to the Effective Perinatal and Outpatient Care (EPOC) team, which typically coordinates follow-up care and support services for infants discharged from the neonatal intensive care unit."}),
    StructField("ParentEducationHandExpress", StringType(), True, metadata={'comment': "The ParentEducationHandExpress column contains timestamps indicating when parents were educated or trained on hand expressing breast milk for their baby in the Neonatal Intensive Care Unit."}),
    StructField("ParentEducationBreastPump", StringType(), True, metadata={'comment': "The ParentEducationBreastPump column contains timestamps indicating when parents of a newborn baby received education or training on how to use a breast pump for expressing breast milk during their baby's stay in the neonatal intensive care unit (NICU)."}),
    StructField("DischargeSummaryReferredToOutreachTeam", StringType(), True, metadata={'comment': "This column indicates whether the patient was referred to an outreach team upon discharge from the neonatal intensive care unit (NICU), which typically involves coordinating follow-up care and support services for high-risk infants after leaving the hospital."}),
    StructField("DischargeSummaryReferredToOutreachTeam_Date", DateType(), True, metadata={'comment': "DischargeSummaryReferredToOutreachTeam_Date represents the date on which a referral was made to an outreach team for post-discharge care or follow-up when the baby was discharged from the neonatal intensive care unit."}),
    StructField("NECDiagnosis", StringType(), True, metadata={'comment': "NECDiagnosis is a column that indicates whether a neonate (newborn baby) was diagnosed with necrotizing enterocolitis (NEC), a serious gastrointestinal disease that primarily affects premature infants, with possible values of 1 for diagnosed with NEC, 0 for not diagnosed with NEC, or None if the diagnosis status is unknown or not recorded."}),
    StructField("NECDiagBasedOn", StringType(), True, metadata={'comment': "The NECDiagBasedOn column represents the criteria or diagnostic methods used to confirm a diagnosis of necrotizing enterocolitis (NEC) in a neonate, such as clinical features, radiographic findings, or a combination of both."}),
    StructField("clinicalFeatures", StringType(), True, metadata={'comment': "The `clinicalFeatures` column contains text or coded information describing the clinical signs and symptoms observed in a neonate diagnosed with necrotizing enterocolitis (NEC), a serious gastrointestinal condition that affects premature infants."}),
    StructField("radiographicFeatures", StringType(), True, metadata={'comment': "The radiographicFeatures column contains information about any radiographic (X-ray or imaging) findings or features observed that contributed to the diagnosis of necrotizing enterocolitis (NEC), a serious intestinal disease that can affect premature infants."}),
    StructField("FinalSummaryText", StringType(), True, metadata={'comment': "The FinalSummaryText column contains a narrative summary of the baby's clinical course, diagnosis, treatment, and discharge/transfer plan during their stay in the neonatal intensive care unit (NICU). This summary provides an overview of the key events, interventions, and outcomes for the NICU admission episode. It allows the care team to communicate important details about the baby's condition, management, and follow-up needs in a concise yet comprehensive manner."}),
    StructField("DateTimeOfDeath", TimestampType(), True, metadata={'comment': "The DateTimeOfDeath column records the date and time when a newborn baby passed away during their care in the neonatal intensive care unit."}),
    StructField("SteroidsLastDose", TimestampType(), True, metadata={'comment': "The SteroidsLastDose column represents the date and time when the last dose of steroid medication was administered to the neonate during their stay in the neonatal intensive care unit (NICU)."}),
    StructField("WaterBirth", StringType(), True, metadata={'comment': "The WaterBirth column indicates whether the baby was born through a water birth, which is a delivery method where the mother gives birth while partially or fully immersed in a tub of warm water."}),
    StructField("BCGImmunisationIndicated", StringType(), True, metadata={'comment': "This column indicates whether the newborn baby was recommended to receive the Bacillus Calmette-Guérin (BCG) vaccine, which is typically given to protect against tuberculosis, especially in high-risk populations or areas with a high prevalence of the disease."}),
    StructField("BCGGivenDuringStay", StringType(), True, metadata={'comment': "The BCGGivenDuringStay column indicates whether the Bacillus Calmette-Guerin (BCG) vaccine, which protects against tuberculosis, was administered to the neonate during their stay in the Neonatal Intensive Care Unit."}),
    StructField("MotherFirstLanguage", StringType(), True, metadata={'comment': "The MotherFirstLanguage column represents the native or first language spoken by the mother of the baby admitted to the neonatal intensive care unit, which can be useful for effective communication and understanding cultural considerations during the baby's care."}),
    StructField("MetabolicDiagnoses1", StringType(), True, metadata={'comment': "This column records various metabolic disorders or conditions diagnosed in the newborn baby during their stay in the neonatal intensive care unit. It includes conditions related to abnormal levels of glucose, electrolytes, acids, and other metabolic disturbances that may require medical intervention or monitoring."}),
    StructField("MaternalCoronaVirusAtBirth", StringType(), True, metadata={'comment': "The MaternalCoronaVirusAtBirth column indicates whether the mother had a confirmed, suspected, or no infection with the coronavirus during the time of giving birth, which could potentially impact the neonate's health and care requirements."}),
    StructField("EthnicityBaby", StringType(), True, metadata={'comment': "The EthnicityBaby column represents the ethnic background or race of the baby admitted to the neonatal intensive care unit, likely using a standardized coding system to categorize different ethnicities."}),
    StructField("SyncScope", StringType(), True, metadata={'comment': "The SyncScope column represents the scope or extent of synchronization for data records related to a particular neonatal patient or episode in the BadgerNet database, potentially indicating which systems or components need to be synchronized with the latest data updates."}),
    StructField("GestationWeeksCorrected_NowOrAtDisch", IntegerType(), True, metadata={'comment': "The GestationWeeksCorrected_NowOrAtDisch column represents the corrected gestational age in weeks of the neonate, either at the current time or at the time of discharge from the neonatal intensive care unit (NICU). This value is used to assess the baby's development and growth relative to their expected due date, accounting for premature or late birth."}),
    StructField("GestationDaysCorrected_NowOrAtDisch", IntegerType(), True, metadata={'comment': "The GestationDaysCorrected_NowOrAtDisch column represents the number of days added or subtracted from the gestational age (in days) to account for prematurity or post-term birth, either at the current time or at the time of discharge from the neonatal intensive care unit."}),
    StructField("ADC_UPDT", TimestampType(), True, metadata={'comment': "Timestamp of last update."})

])

@dlt.table(name="rde_mat_nnu_episodes_incr", table_properties={"skipChangeCommits": "true"}, temporary=True)
def mat_nnu_episodes_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_mat_nnu_episodes")

    nnu_episodes = spark.table("4_prod.raw.nnu_episodes").alias("Nep")
    patient_demographics = dlt.read("rde_patient_demographics").alias("PDEM")

    return (
        nnu_episodes
        .join(patient_demographics, col("Nep.NationalIDBaby") == col("PDEM.NHS_Number"), "left")
        .filter(col("PDEM.PERSON_ID").isNotNull())
        .select(
            col("PDEM.PERSON_ID").cast(StringType()).alias("Person_ID"),
            col("Nep.NationalIDBaby").cast(StringType()).alias("NHS_Number"),
            col("PDEM.MRN").cast(StringType()).alias("MRN"),
            col("Nep.CareLocationName").cast(StringType()),
            col("Nep.EpisodeType").cast(StringType()),
            col("Nep.Sex").cast(StringType()),
            col("Nep.BirthTimeBaby").cast(TimestampType()),
            col("Nep.GestationWeeks").cast(IntegerType()),
            col("Nep.GestationDays").cast(IntegerType()),
            col("Nep.Birthweight").cast(FloatType()),
            col("Nep.BirthLength").cast(FloatType()),
            col("Nep.BirthHeadCircumference").cast(FloatType()),
            col("Nep.BirthOrder").cast(IntegerType()),
            col("Nep.FetusNumber").cast(IntegerType()),
            col("Nep.BirthSummary").cast(StringType()),
            col("Nep.EpisodeNumber").cast(IntegerType()),
            col("Nep.AdmitTime").cast(TimestampType()),
            col("Nep.AdmitFromName").cast(StringType()),
            col("Nep.AdmitFromNHSCode").cast(StringType()),
            col("Nep.ProviderName").cast(StringType()),
            col("Nep.ProviderNHSCode").cast(StringType()),
            col("Nep.NetworkName").cast(StringType()),
            col("Nep.AdmitTemperature").cast(FloatType()),
            col("Nep.AdmitTemperatureTime").cast(TimestampType()),
            col("Nep.AdmitBloodPressure").cast(StringType()),
            col("Nep.AdmitHeartRate").cast(IntegerType()),
            col("Nep.AdmitRespiratoryRate").cast(IntegerType()),
            col("Nep.AdmitSaO2").cast(FloatType()),
            col("Nep.AdmitBloodGlucose").cast(FloatType()),
            col("Nep.AdmitWeight").cast(FloatType()),
            col("Nep.AdmitHeadCircumference").cast(FloatType()),
            col("Nep.DischTime").cast(TimestampType()),
            col("Nep.DischargeHospitalName").cast(StringType()),
            col("Nep.DischargeHospitalCode").cast(StringType()),
            col("Nep.DischargeWeight").cast(FloatType()),
            col("Nep.DischargeHeadCircumference").cast(FloatType()),
            col("Nep.DischargeMilk").cast(StringType()),
            col("Nep.DischargeFeeding").cast(StringType()),
            col("Nep.HomeTubeFeeding").cast(StringType()),
            col("Nep.DischargeOxygen").cast(StringType()),
            col("Nep.EpisodeSummary").cast(StringType()),
            col("Nep.VentilationDays").cast(IntegerType()),
            col("Nep.CPAPDays").cast(IntegerType()),
            col("Nep.OxygenDays").cast(IntegerType()),
            col("Nep.OxygenDaysNoVent").cast(IntegerType()),
            col("Nep.OxygenLastTime").cast(TimestampType()),
            col("Nep.ICCareDays").cast(IntegerType()),
            col("Nep.HDCareDays").cast(IntegerType()),
            col("Nep.SCCareDays").cast(IntegerType()),
            col("Nep.ICCareDays2011").cast(IntegerType()),
            col("Nep.HDCareDays2011").cast(IntegerType()),
            col("Nep.SCCareDays2011").cast(IntegerType()),
            col("Nep.NormalCareDays2011").cast(IntegerType()),
            col("Nep.HRG1").cast(StringType()),
            col("Nep.HRG2").cast(StringType()),
            col("Nep.HRG3").cast(StringType()),
            col("Nep.HRG4").cast(StringType()),
            col("Nep.HRG5").cast(StringType()),
            col("Nep.LocnNNUDays").cast(IntegerType()),
            col("Nep.LocnTCDays").cast(IntegerType()),
            col("Nep.LocnPNWDays").cast(IntegerType()),
            col("Nep.LocnOBSDays").cast(IntegerType()),
            col("Nep.LocnNNUPortion").cast(FloatType()),
            col("Nep.LocnTCPoriton").cast(FloatType()),
            col("Nep.LocnPNWPortion").cast(FloatType()),
            col("Nep.DrugsDuringStay").cast(StringType()),
            col("Nep.DiagnosisDuringStay").cast(StringType()),
            col("Nep.NationalIDMother").cast(StringType()),
            col("Nep.BloodGroupMother").cast(StringType()),
            col("Nep.BirthDateMother").cast(DateType()),
            col("Nep.AgeMother").cast(IntegerType()),
            col("Nep.HepBMother").cast(StringType()),
            col("Nep.HepBMotherHighRisk").cast(StringType()),
            col("Nep.HivMother").cast(StringType()),
            col("Nep.RubellaScreenMother").cast(StringType()),
            col("Nep.SyphilisScreenMother").cast(StringType()),
            col("Nep.MumHCV").cast(StringType()),
            col("Nep.HepCPCRMother").cast(StringType()),
            col("Nep.MumVDRL").cast(StringType()),
            col("Nep.MumTPHA").cast(StringType()),
            col("Nep.MaternalPyrexiaInLabour38c").cast(StringType()),
            col("Nep.IntrapartumAntibioticsGiven").cast(StringType()),
            col("Nep.MeconiumStainedLiquor").cast(StringType()),
            col("Nep.MembraneRuptureDate").cast(DateType()),
            col("Nep.MembranerupturedDuration").cast(FloatType()),
            col("Nep.ParentsConsanguinous").cast(StringType()),
            col("Nep.DrugsAbusedMother").cast(StringType()),
            col("Nep.SmokingMother").cast(StringType()),
            col("Nep.CigarettesMother").cast(IntegerType()),
            col("Nep.AlcoholMother").cast(StringType()),
            col("Nep.PreviousPregnanciesNumber").cast(IntegerType()),
            col("Nep.AgeFather").cast(IntegerType()),
            col("Nep.EthnicityFather").cast(StringType()),
            col("Nep.GestationWeeksCalculated").cast(IntegerType()),
            col("Nep.GestationDaysCalculated").cast(IntegerType()),
            col("Nep.BookingName").cast(StringType()),
            col("Nep.BookingNHSCode").cast(StringType()),
            col("Nep.SteroidsAntenatalGiven").cast(StringType()),
            col("Nep.SteroidsName").cast(StringType()),
            col("Nep.SteroidsAntenatalCourses").cast(IntegerType()),
            col("Nep.PlaceOfBirthName").cast(StringType()),
            col("Nep.PlaceOfBirthNHSCode").cast(StringType()),
            col("Nep.Apgar1").cast(IntegerType()),
            col("Nep.Apgar5").cast(IntegerType()),
            col("Nep.Apgar10").cast(IntegerType()),
            col("Nep.BabyBloodType").cast(StringType()),
            col("Nep.Crib2Score").cast(FloatType()),
            col("Nep.FinalNNUOutcome").cast(StringType()),
            col("Nep.VitaminKGiven").cast(StringType()),
            col("Nep.CordArterialpH").cast(FloatType()),
            col("Nep.CordVenouspH").cast(FloatType()),
            col("Nep.CordPcO2Arterial").cast(FloatType()),
            col("Nep.CordPcO2Venous").cast(FloatType()),
            col("Nep.CordArterialBE").cast(FloatType()),
            col("Nep.CordVenousBE").cast(FloatType()),
            col("Nep.CordClamping").cast(StringType()),
            col("Nep.CordClampingTimeMinute").cast(IntegerType()),
            col("Nep.CordClampingTimeSecond").cast(IntegerType()),
            col("Nep.CordStripping").cast(StringType()),
            col("Nep.ResusSurfactant").cast(StringType()),
            col("Nep.Seizures").cast(StringType()),
            col("Nep.HIEGrade").cast(StringType()),
            col("Nep.Anticonvulsants").cast(StringType()),
            col("Nep.Pneumothorax").cast(StringType()),
            col("Nep.NecrotisingEnterocolitis").cast(StringType()),
            col("Nep.NeonatalAbstinence").cast(StringType()),
            col("Nep.ROPScreenDate").cast(DateType()),
            col("Nep.ROPSurgeryDate").cast(DateType()),
            col("Nep.Dexamethasone").cast(StringType()),
            col("Nep.PDAIndomethacin").cast(StringType()),
            col("Nep.PDAIbuprofen").cast(StringType()),
            col("Nep.PDASurgery").cast(StringType()),
            col("Nep.PDADischarge").cast(StringType()),
            col("Nep.UACTime").cast(TimestampType()),
            col("Nep.UVCTime").cast(TimestampType()),
            col("Nep.LongLineTime").cast(TimestampType()),
            col("Nep.PeripheralArterialLineTime").cast(TimestampType()),
            col("Nep.SurgicalLineTime").cast(TimestampType()),
            col("Nep.ParenteralNutritionDays").cast(IntegerType()),
            col("Nep.HeadScanFirstTime").cast(TimestampType()),
            col("Nep.HeadScanFirstResult").cast(StringType()),
            col("Nep.HeadScanLastTime").cast(TimestampType()),
            col("Nep.HeadScanLastResult").cast(StringType()),
            col("Nep.CongenitalAnomalies").cast(StringType()),
            col("Nep.VPShuntTime").cast(TimestampType()),
            col("Nep.BloodCultureFirstTime").cast(TimestampType()),
            col("Nep.BloodCultureFirstResult").cast(StringType()),
            col("Nep.CSFCultureFirstTime").cast(TimestampType()),
            col("Nep.CSFCultureFirstResult").cast(StringType()),
            col("Nep.UrineCultureFirstTime").cast(TimestampType()),
            col("Nep.UrineCultureFirstResult").cast(StringType()),
            col("Nep.ExchangeTransfusion").cast(StringType()),
            col("Nep.Tracheostomy").cast(StringType()),
            col("Nep.PulmonaryVasodilatorTime").cast(TimestampType()),
            col("Nep.PulmonaryVasodilatorDrugs").cast(StringType()),
            col("Nep.Inotropes").cast(StringType()),
            col("Nep.InotropesFirstTime").cast(TimestampType()),
            col("Nep.PeritonealDialysis").cast(StringType()),
            col("Nep.DischargeApnoeaCardioSat").cast(StringType()),
            col("Nep.gastroschisis").cast(StringType()),
            col("Nep.Cooled").cast(StringType()),
            col("Nep.FirstConsultationWithParents").cast(TimestampType()),
            col("Nep.ReceivedMothersMilkDuringAdmission").cast(StringType()),
            col("Nep.DischargeLength").cast(FloatType()),
            col("Nep.PrincipalDiagnosisAtDischarge").cast(StringType()),
            col("Nep.ActiveProblemsAtDischarge").cast(StringType()),
col("Nep.PrincipleProceduresDuringStay").cast(StringType()),
            col("Nep.RespiratoryDiagnoses").cast(StringType()),
            col("Nep.CardiovascularDiagnoses").cast(StringType()),
            col("Nep.GastrointestinalDiagnoses").cast(StringType()),
            col("Nep.NeurologyDiagnoses").cast(StringType()),
            col("Nep.ROPDiagnosis").cast(StringType()),
            col("Nep.HaemDiagnoses").cast(StringType()),
            col("Nep.RenalDiagnoses").cast(StringType()),
            col("Nep.SkinDiagnoses").cast(StringType()),
            col("Nep.MetabolicDiagnoses").cast(StringType()),
            col("Nep.InfectionsDiagnoses").cast(StringType()),
            col("Nep.SocialIssues").cast(StringType()),
            col("Nep.DayOneLocationOfCare").cast(StringType()),
            col("Nep.BirthCareLocationName").cast(StringType()),
            col("Nep.UnitResponsibleFor2YearFollowUp").cast(StringType()),
            col("Nep.CordLactate").cast(FloatType()),
            col("Nep.ROPScreenFirstDateDueStart").cast(DateType()),
            col("Nep.ROPScreenFirstDateDueEnd").cast(DateType()),
            col("Nep.ROPFirstScreenStart").cast(DateType()),
            col("Nep.ROPFirstScreenEnd").cast(DateType()),
            col("Nep.LSOA").cast(StringType()),
            col("Nep.DateOfFirstExamination").cast(DateType()),
            col("Nep.DateOfRoutineNeonatalExamination").cast(DateType()),
            col("Nep.MotherIntendToBreastFeed").cast(StringType()),
            col("Nep.MagnesiumSulphate").cast(StringType()),
            col("Nep.ReasonMagnesiumSulphateNotGiven").cast(StringType()),
            col("Nep.LabourWardDeath").cast(StringType()),
            col("Nep.AdmitPrincipalReason_Other").cast(StringType()),
            col("Nep.TwoYearFollowUpPerformedAnyEpisode").cast(StringType()),
            col("Nep.CauseOfDeath1A").cast(StringType()),
            col("Nep.CauseOfDeath1B").cast(StringType()),
            col("Nep.CauseOfDeath2").cast(StringType()),
            col("Nep.DateTimeLeftHospital").cast(TimestampType()),
            col("Nep.ReasonMagnesiumSulphateGiven").cast(StringType()),
            col("Nep.TwoYearFollowUpPerformedAnyEpisode_Date").cast(DateType()),
            col("Nep.MaternalMedicalNotes").cast(StringType()),
            col("Nep.AnomalyScanComments").cast(StringType()),
            col("Nep.ReceivedAntenatalCare").cast(StringType()),
            col("Nep.DateFirstUltrasound").cast(DateType()),
            col("Nep.FollowUp").cast(StringType()),
            col("Nep.timeReady").cast(TimestampType()),
            col("Nep.BabyAwaiting").cast(StringType()),
            col("Nep.TransferDestinationHospital").cast(StringType()),
            col("Nep.EPOCDischargeLetterSent").cast(StringType()),
            col("Nep.ParentEducationHandExpress").cast(StringType()),
            col("Nep.ParentEducationBreastPump").cast(StringType()),
            col("Nep.DischargeSummaryReferredToOutreachTeam").cast(StringType()),
            col("Nep.DischargeSummaryReferredToOutreachTeam_Date").cast(DateType()),
            col("Nep.NECDiagnosis").cast(StringType()),
            col("Nep.NECDiagBasedOn").cast(StringType()),
            col("Nep.clinicalFeatures").cast(StringType()),
            col("Nep.radiographicFeatures").cast(StringType()),
            col("Nep.FinalSummaryText").cast(StringType()),
            col("Nep.DateTimeOfDeath").cast(TimestampType()),
            col("Nep.SteroidsLastDose").cast(TimestampType()),
            col("Nep.WaterBirth").cast(StringType()),
            col("Nep.BCGImmunisationIndicated").cast(StringType()),
            col("Nep.BCGGivenDuringStay").cast(StringType()),
            col("Nep.MotherFirstLanguage").cast(StringType()),
            col("Nep.MetabolicDiagnoses1").cast(StringType()),
            col("Nep.MaternalCoronaVirusAtBirth").cast(StringType()),
            col("Nep.EthnicityBaby").cast(StringType()),
            col("Nep.SyncScope").cast(StringType()),
            col("Nep.GestationWeeksCorrected_NowOrAtDisch").cast(IntegerType()),
            col("Nep.GestationDaysCorrected_NowOrAtDisch").cast(IntegerType()),
            greatest(col("Nep.ADC_UPDT"), col("PDEM.ADC_UPDT")).alias("ADC_UPDT")
        )
        .filter((col("Nep.ADC_UPDT") > max_adc_updt) | (col("PDEM.ADC_UPDT") > max_adc_updt))
    )

@dlt.view(name="mat_nnu_episodes_update")
def mat_nnu_episodes_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_mat_nnu_episodes_incr")
    )

dlt.create_target_table(
    name = "rde_mat_nnu_episodes",
    comment = nnu_episodes_comment,
    schema = schema_rde_mat_nnu_episodes,
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "Person_ID,NHS_Number,BirthTimeBaby"
    }
)

dlt.apply_changes(
    target = "rde_mat_nnu_episodes",
    source = "mat_nnu_episodes_update",
    keys = ["Person_ID", "NHS_Number", "BirthTimeBaby"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)

# COMMAND ----------

nnu_nccmds_comment = "Details of Neonatal Critical Care Minimum Data Set."

schema_rde_mat_nnu_nccmds = StructType([
    StructField("Person_ID", StringType(), True, metadata={'comment': "The Person_ID column represents a unique identifier assigned to each patient"}),
    StructField("NHS_Number", StringType(), True, metadata={'comment': "the unique National Health Service number assigned to each patient in the UK healthcare system, which serves as a national identifier for individuals receiving medical care."}),
    StructField("MRN", StringType(), True, metadata={'comment': "unique identification numbers assigned to each patient by the healthcare facility for maintaining their medical records and facilitating efficient data management and tracking within the neonatal intensive care unit."}),
    StructField("WardLocation", StringType(), True, metadata={'comment': "the ward or unit within the hospital where the neonate (newborn baby) is receiving care, such as the Transitional Care (tc) unit, Observation (obs) unit, Postnatal Ward (pnw), or Neonatal Intensive Care Unit (nnu)."}),
    StructField("DOB", DateType(), True, metadata={'comment': "the date when a newborn baby was born and admitted to the Neonatal Intensive Care Unit (NICU) for critical care."}),
    StructField("CriticalCareStartDate", DateType(), True, metadata={'comment': "the date when a newborn patient was admitted to the critical care unit or neonatal intensive care unit (NICU) for specialized medical treatment and monitoring."}),
    StructField("CriticalCareStartTime", TimestampType(), True, metadata={'comment': "the time of day when a patient was admitted to the neonatal intensive care unit for critical care treatment."}),
    StructField("CriticalCareDischargeDate", DateType(), True, metadata={'comment': "the date when a neonate (newborn baby) was discharged or released from the Neonatal Intensive Care Unit (NICU) after receiving critical care treatment."}),
    StructField("CriticalCareDischargeTime", TimestampType(), True, metadata={'comment': "the date and time when a neonate was discharged from the critical care unit after receiving intensive neonatal care."}),
    StructField("Gestation", IntegerType(), True, metadata={'comment': "the gestational age in weeks at the time of birth for each neonate (newborn baby) admitted to the Neonatal Intensive Care Unit."}),
    StructField("PersonWeight", FloatType(), True, metadata={'comment': "the weight of the newborn baby in grams at the time of admission to the Neonatal Intensive Care Unit."}),
    StructField("CCAC1", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC2", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC3", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC4", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC5", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC6", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC7", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC8", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC9", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC10", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC11", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC12", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC13", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC14", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC15", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC16", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC17", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC18", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC19", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("CCAC20", StringType(), True, metadata={'comment': "diagnosis for admitting the neonate to the critical care unit, where each code corresponds to a specific neonatal condition or complication."}),
    StructField("HCDRUG1", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG2", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG3", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG4", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG5", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG6", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG7", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG8", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG9", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG10", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG11", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG12", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG13", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG14", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG15", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG16", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG17", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG18", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG19", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("HCDRUG20", StringType(), True, metadata={'comment': "codes or identifiers for specific drugs or medications administered to neonatal patients in the critical care unit"}),
    StructField("ADC_UPDT", TimestampType(), True, metadata={'comment': "Timestamp for row being updated."})
])

@dlt.table(name="rde_mat_nnu_nccmds_incr", table_properties={"skipChangeCommits": "true"}, temporary=True)
def mat_nnu_nccmds_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_mat_nnu_nccmds")

    nnu_nccmds = spark.table("4_prod.raw.nnu_nccmds").alias("MDS")
    patient_demographics = dlt.read("rde_patient_demographics").alias("PDEM")

    return (
        nnu_nccmds
        .join(patient_demographics, col("MDS.NHSNumberBaby") == col("PDEM.NHS_Number"), "left")
        .filter(col("PDEM.PERSON_ID").isNotNull())
        .select(
            col("PDEM.PERSON_ID").cast(StringType()).alias("Person_ID"),
            col("MDS.NHSNumberBaby").cast(StringType()).alias("NHS_Number"),
            col("PDEM.MRN").cast(StringType()).alias("MRN"),
            col("MDS.WardLocation").cast(StringType()),
            col("MDS.DOB").cast(DateType()),
            col("MDS.CriticalCareStartDate").cast(DateType()),
            col("MDS.CriticalCareStartTime").cast(TimestampType()),
            col("MDS.CriticalCareDischargeDate").cast(DateType()),
            col("MDS.CriticalCareDischargeTime").cast(TimestampType()),
            col("MDS.Gestation").cast(IntegerType()),
            col("MDS.PersonWeight").cast(FloatType()),
            col("MDS.CCAC1").cast(StringType()),
            col("MDS.CCAC2").cast(StringType()),
            col("MDS.CCAC3").cast(StringType()),
            col("MDS.CCAC4").cast(StringType()),
            col("MDS.CCAC5").cast(StringType()),
            col("MDS.CCAC6").cast(StringType()),
            col("MDS.CCAC7").cast(StringType()),
            col("MDS.CCAC8").cast(StringType()),
            col("MDS.CCAC9").cast(StringType()),
            col("MDS.CCAC10").cast(StringType()),
            col("MDS.CCAC11").cast(StringType()),
            col("MDS.CCAC12").cast(StringType()),
            col("MDS.CCAC13").cast(StringType()),
            col("MDS.CCAC14").cast(StringType()),
            col("MDS.CCAC15").cast(StringType()),
            col("MDS.CCAC16").cast(StringType()),
            col("MDS.CCAC17").cast(StringType()),
            col("MDS.CCAC18").cast(StringType()),
            col("MDS.CCAC19").cast(StringType()),
            col("MDS.CCAC20").cast(StringType()),
            col("MDS.HCDRUG1").cast(StringType()),
            col("MDS.HCDRUG2").cast(StringType()),
            col("MDS.HCDRUG3").cast(StringType()),
            col("MDS.HCDRUG4").cast(StringType()),
            col("MDS.HCDRUG5").cast(StringType()),
            col("MDS.HCDRUG6").cast(StringType()),
            col("MDS.HCDRUG7").cast(StringType()),
            col("MDS.HCDRUG8").cast(StringType()),
            col("MDS.HCDRUG9").cast(StringType()),
            col("MDS.HCDRUG10").cast(StringType()),
            col("MDS.HCDRUG11").cast(StringType()),
            col("MDS.HCDRUG12").cast(StringType()),
            col("MDS.HCDRUG13").cast(StringType()),
            col("MDS.HCDRUG14").cast(StringType()),
            col("MDS.HCDRUG15").cast(StringType()),
            col("MDS.HCDRUG16").cast(StringType()),
            col("MDS.HCDRUG17").cast(StringType()),
            col("MDS.HCDRUG18").cast(StringType()),
            col("MDS.HCDRUG19").cast(StringType()),
            col("MDS.HCDRUG20").cast(StringType()),
            greatest(col("MDS.ADC_UPDT"), col("PDEM.ADC_UPDT")).alias("ADC_UPDT")
        )
        .filter((col("MDS.ADC_UPDT") > max_adc_updt) | (col("PDEM.ADC_UPDT") > max_adc_updt))
    )

@dlt.view(name="mat_nnu_nccmds_update")
def mat_nnu_nccmds_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
        .option("ignoreChanges", "true")
        .table("LIVE.rde_mat_nnu_nccmds_incr")
    )

dlt.create_target_table(
    name = "rde_mat_nnu_nccmds",
    comment = nnu_nccmds_comment,
    schema = schema_rde_mat_nnu_nccmds,
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "Person_ID,NHS_Number,CriticalCareStartDate"
    }
)

dlt.apply_changes(
    target = "rde_mat_nnu_nccmds",
    source = "mat_nnu_nccmds_update",
    keys = ["Person_ID", "NHS_Number", "CriticalCareStartDate"],
    sequence_by = "ADC_UPDT",
    apply_as_deletes = None,
    except_column_list = [],
    stored_as_scd_type = 1
)
