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

@dlt.table(name="rde_patient_demographics_incr", temporary=True,
        table_properties={
        "pipelines.autoOptimize.zOrderCols": "PERSON_ID",
        "skipChangeCommits": "true"
    })
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
