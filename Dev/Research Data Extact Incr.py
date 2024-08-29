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
       
