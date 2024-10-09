# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date
from delta.tables import DeltaTable

# Get the maximum ADC_UPDT from the target table
target_table = DeltaTable.forName(spark, "4_prod.bronze.mill_form_activity")
max_adc_updt = target_table.toDF().agg(F.max("ADC_UPDT")).collect()[0][0]

# If the target table is empty, use a default date
if max_adc_updt is None:
    max_adc_updt = to_date(F.lit("1900-01-01"))

# Filter clinical_event as in the original code
filtered_clinical_event = (
    spark.table("4_prod.raw.mill_clinical_event")
    .filter(F.col("VALID_UNTIL_DT_TM") > F.current_timestamp())
)

# Create the new table with updated filter
mill_form_activity = (
    spark.table("4_prod.raw.mill_dcp_forms_activity")
    .filter(F.col("ADC_UPDT") > max_adc_updt)
    .join(
        spark.table("4_prod.raw.mill_dcp_forms_ref"),
        "DCP_FORMS_REF_ID"
    )
    .join(
        filtered_clinical_event,
        (F.col("mill_dcp_forms_activity.ENCNTR_ID") == filtered_clinical_event.ENCNTR_ID) &
        (F.col("mill_dcp_forms_activity.DCP_FORMS_ACTIVITY_ID").cast("integer") == 
         F.when(filtered_clinical_event.REFERENCE_NBR.contains("!"),
                F.split(filtered_clinical_event.REFERENCE_NBR, "!").getItem(0).cast("float").cast("integer"))
         .otherwise(filtered_clinical_event.REFERENCE_NBR.cast("float").cast("integer"))),
        "inner"
    )
    .join(
        spark.table("4_prod.raw.mill_ce_date_result"),
        filtered_clinical_event.EVENT_ID == F.col("mill_ce_date_result.EVENT_ID"),
        "left"
    )
    .join(
        spark.table("4_prod.raw.mill_ce_string_result"),
        filtered_clinical_event.EVENT_ID == F.col("mill_ce_string_result.EVENT_ID"),
        "left"
    )
    .join(
        spark.table("4_prod.raw.mill_ce_coded_result"),
        filtered_clinical_event.EVENT_ID == F.col("mill_ce_coded_result.EVENT_ID"),
        "left"
    )
    .select(
        F.col("mill_dcp_forms_activity.DCP_FORMS_ACTIVITY_ID"),
        F.col("mill_dcp_forms_activity.FORM_STATUS_CD"),
        F.col("mill_dcp_forms_activity.ENCNTR_ID"),
        F.col("mill_dcp_forms_activity.DCP_FORMS_REF_ID"),
        F.col("mill_dcp_forms_activity.FORM_DT_TM"),
        F.col("mill_dcp_forms_activity.TASK_ID"),
        F.col("mill_clinical_event.EVENT_ID"),
        F.col("mill_clinical_event.PARENT_EVENT_ID"),
        F.col("mill_clinical_event.PERFORMED_DT_TM"),
        F.col("mill_dcp_forms_ref.DCP_FORM_INSTANCE_ID"),
        F.col("mill_ce_date_result.RESULT_DT_TM").alias("DATE_RESULT"),
        F.col("mill_ce_string_result.STRING_RESULT_TEXT").alias("STRING_RESULT"),
        F.col("mill_ce_string_result.STRING_RESULT_FORMAT_CD").cast("long").alias("RESULT_TYPE"),
        F.col("mill_ce_coded_result.DESCRIPTOR").alias("CODED_RESULT"),
        F.col("mill_ce_coded_result.RESULT_CD").cast("long").alias("CODED_CD"),
        F.col("mill_ce_coded_result.NOMENCLATURE_ID").cast("long").alias("CODED_NOM"),
        F.greatest(
            F.col("mill_dcp_forms_activity.ADC_UPDT"),
            F.col("mill_dcp_forms_ref.ADC_UPDT"),
            F.col("mill_clinical_event.ADC_UPDT"),
            F.col("mill_ce_date_result.ADC_UPDT"),
            F.col("mill_ce_string_result.ADC_UPDT"),
            F.col("mill_ce_coded_result.ADC_UPDT")
        ).alias("ADC_UPDT")
    )
)

# Perform the merge operation
(target_table.alias("target")
 .merge(
     mill_form_activity.alias("source"),
     "target.DCP_FORMS_ACTIVITY_ID = source.DCP_FORMS_ACTIVITY_ID"
 )
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute()
)
