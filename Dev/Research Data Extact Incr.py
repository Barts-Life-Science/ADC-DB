# Databricks notebook source
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

demo_schema = StructType([
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
        StructField("ADC_UPDT", TimestampType(), True)
    ])

@dlt.table(name="rde_patient_demographics_incr", temporary=True,
        table_properties={
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID",
        "skipChangeCommits": "true"
    },
    schema = demo_schema)
def patient_demographics_incr():
    max_adc_updt = get_max_adc_updt("8_dev.rde.rde_patient_demographics")
    
    # Load tables
    pat = spark.table("4_prod.raw.mill_person_patient").alias("Pat").filter(col("active_ind") == 1)
    pers = spark.table("4_prod.raw.mill_person").alias("Pers").filter(col("active_ind") == 1)
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


@dlt.table(name="demographics_update", temporary=True)
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
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID",
        "skipChangeCommits": "true"

    },
    schema = demo_schema
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
    max_adc_updt = get_max_adc_updt("8_dev.rde.rde_encounter")

    encounter = spark.table("4_prod.raw.mill_encounter")
    patient_demographics = dlt.read("rde_patient_demographics").alias("D")
    code_value_lookup = spark.table("4_prod.support_tables.code_value").alias("CV")
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

    
@dlt.table(name="encounter_update")
def encounter_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("skipChangeCommits", "true")
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
    max_adc_updt = get_max_adc_updt("8_dev.rde.rde_ariapharmacy")
    
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

@dlt.table(name="ariapharmacy_update")
def ariapharmacy_update():
    return (
        spark.readStream
        .option("forceDeleteReadCheckpoint", "true")
        .option("skipChangeCommits", "true")
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



# COMMAND ----------

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

# Declare the target table
dlt.create_target_table(
    name = "rde_allergydetails",
    comment="Incrementally updated allergy details data",
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



# COMMAND ----------

schema_rde_critactivity = StructType([
        StructField("PERSONID", StringType(), True, metadata={"comment": "The unique identifier for each patient"}),
        StructField("MRN", StringType(), True, metadata={"comment": "Local Identifier"}),
        StructField("NHS_NUMBER", StringType(), True, metadata={"comment": "NHS Number"}),
        StructField("Period_ID", StringType(), True, metadata={"comment": "ID for the crit care period."}),
        StructField("CDS_APC_ID", StringType(), True, metadata={"comment": "ID for the CDS"}),
        StructField("ActivityDate", StringType(), True, metadata={"comment": "Date of the activity."}),
        StructField("ActivityCode", IntegerType(), True, metadata={"comment": "Code for the activity."}),
        StructField("ActivityDesc", StringType(), True, metadata={"comment": "Text description of the code."}),
        StructField("ADC_UPDT", TimestampType(), True)
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

# Declare the target table
dlt.create_target_table(
    name = "rde_critactivity",
    comment="Incrementally updated critical care activity data",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "PERSONID,Period_ID,ActivityDate,ActivityCode"
    },
    schema = schema_rde_critactivity
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


@dlt.table(name="rde_pharmacyorders_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def pharmacyorders_incr():
    max_adc_updt = get_max_adc_updt("8_dev.rde.rde_pharmacyorders")

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
                #(col("O.ORDER_STATUS_CD").isin("2543", "2545", "2547", "2548", "2550", "2552", "643466")) &
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
    comment="Incrementally updated pharmacy orders data",
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
