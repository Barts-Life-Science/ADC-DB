# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import *


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

@dlt.table(name="rde_patient_demographics_incr", #temporary=True,
        table_properties={
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID",
        "skipChangeCommits": "true"
    })
def patient_demographics_incr():
    max_adc_updt = get_max_adc_updt("8_dev.rde_2.rde_patient_demographics")
    
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

@dlt.table(name="demographics_update")
def demographics_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
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
    })#, temporary=True)
def encounter_incr():
    max_adc_updt = get_max_adc_updt("8_dev.rde_2.rde_encounter")

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

    
@dlt.table(name="encounter_update")
def encounter_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
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


@dlt.table(name="rde_ariapharmacy_incr", table_properties={
        "skipChangeCommits": "true"})#, temporary=True)
def ariapharmacy_incr():
    max_adc_updt = get_max_adc_updt("8_dev.rde_2.rde_ariapharmacy")
    
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
        .join(patient_demographics, 
              (col("D.NHS_Number") == coalesce(col("PA.PERSON_ID"), regexp_replace(col("Ptkey.pt_key_value"), ' ', ''))) &
              (col("Ptkey.pt_key_cd") == 24),
              "inner")
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

@dlt.table(name="ariapharmacy_update")
def ariapharmacy_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("ignoreDeletes", "true")
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
