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

    allergy = spark.table("4_prod.raw.mill_dir_allergy").alias("A")
    encounter = dlt.read("rde_encounter").alias("ENC")
    code_value_ref = spark.table("3_lookup.dwh.pi_cde_code_value_ref")
    nomenclature_ref = spark.table("3_lookup.dwh.mill_dir_nomenclature").alias("Det")

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

@dlt.table(name="rde_mill_powertrials_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def mill_powertrials_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_mill_powertrials")

    mill_pt_prot_reg = spark.table("4_prod.raw.mill_dir_prot_reg").alias("RES")
    mill_pt_prot_master = spark.table("4_prod.raw.mill_dir_prot_master").alias("STUDYM")
    code_value_ref = spark.table("3_lookup.dwh.pi_cde_code_value_ref").alias("LOOK")
    patient_demographics = dlt.read("rde_patient_demographics").alias("PDEM")

    # Window function to get the latest record for each patient and study
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

# Declare the target table
dlt.create_target_table(
    name = "rde_mill_powertrials",
    comment="Incrementally updated Powertrials data",
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

# Declare the target table
dlt.create_target_table(
    name = "rde_critperiod",
    comment="Incrementally updated critical care period data",
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
        .distinct()  # Ensure uniqueness of records
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

# Declare the target table
dlt.create_target_table(
    name = "rde_critopcs",
    comment="Incrementally updated critical care OPCS data",
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

# Declare the target table
dlt.create_target_table(
    name = "rde_emergencyd",
    comment="Incrementally updated emergency department data",
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

@dlt.table(name="rde_medadmin_incr", table_properties={
        "skipChangeCommits": "true"}, temporary=True)
def medadmin_incr():
    max_adc_updt = get_max_adc_updt("4_prod.rde.rde_medadmin")

    clinical_event = spark.table("4_prod.raw.mill_dir_clinical_event").alias("CE")
    med_admin_event = spark.table("4_prod.raw.mill_dir_med_admin_event").alias("MAE")
    encounter = dlt.read("rde_encounter").alias("ENC")
    ce_med_result = spark.table("4_prod.raw.mill_dir_ce_med_result").alias("MR")
    order_ingredient = spark.table("4_prod.raw.mill_dir_order_ingredient").alias("OI")
    order_catalog_synonym = spark.table("3_lookup.dwh.mill_dir_order_catalog_synonym").alias("OSYN")
    order_catalog = spark.table("3_lookup.dwh.mill_dir_order_catalog").alias("OCAT")
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
            greatest(col("CE.ADC_UPDT"), col("ENC.ADC_UPDT"), col("MAE.ADC_UPDT"), col("MR.ADC_UPDT"), col("OI.ADC_UPDT")).alias("ADC_UPDT")
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

# Declare the target table
dlt.create_target_table(
    name = "rde_medadmin",
    comment="Incrementally updated medication administration data",
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
