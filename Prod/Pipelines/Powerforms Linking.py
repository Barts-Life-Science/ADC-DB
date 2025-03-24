# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date
from delta.tables import DeltaTable
from pyspark.sql.window import Window


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
# Create a UDF to check if a string is a valid number
is_valid_number = F.udf(lambda x: x is not None and x.replace('.', '').isdigit() if x else False)

# Modify the join condition
mill_form_activity = (
    spark.table("4_prod.raw.mill_dcp_forms_activity")
    .filter(F.col("ADC_UPDT") > max_adc_updt)
    .join(
        spark.table("4_prod.raw.mill_dcp_forms_ref"),
        "DCP_FORMS_REF_ID"
    )
    .join(
        filtered_clinical_event.withColumn(
            "parsed_reference",
            F.when(F.col("REFERENCE_NBR").contains("!"),
                  F.split(F.col("REFERENCE_NBR"), "!").getItem(0))
            .otherwise(F.col("REFERENCE_NBR"))
        ).filter(
            (F.length(F.col("parsed_reference")) <= 10) &  # Add reasonable length check
            F.col("parsed_reference").rlike("^[0-9]+(\\.[0-9]+)?$")  # Only allow valid numbers
        ),
        (F.col("mill_dcp_forms_activity.ENCNTR_ID") == filtered_clinical_event.ENCNTR_ID) &
        (F.col("mill_dcp_forms_activity.DCP_FORMS_ACTIVITY_ID") == 
         F.col("parsed_reference").cast("double").cast("long")),
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
mill_form_activity = mill_form_activity.repartition("DCP_FORMS_ACTIVITY_ID")

row_count = mill_form_activity.count()
print(f"Number of rows in mill_form_activity: {row_count}")

# Get the list of DCP_FORMS_ACTIVITY_ID to be deleted
ids_to_delete = mill_form_activity.select("DCP_FORMS_ACTIVITY_ID").distinct()

# Delete matching rows from the target table
window = Window.partitionBy(F.lit(1))  # Global window
ids_to_delete_set = ids_to_delete.select(F.collect_set("DCP_FORMS_ACTIVITY_ID").over(window).alias("id_set")).first()
if ids_to_delete_set is not None:
    ids_to_delete_set = ids_to_delete_set.id_set
else:
    ids_to_delete_set = []

target_table.delete(F.col("DCP_FORMS_ACTIVITY_ID").isin(ids_to_delete_set))

# Append the new data to the target table
mill_form_activity.write.format("delta").mode("append").saveAsTable("4_prod.bronze.mill_form_activity")

# Optimize the target table
#target_table.optimize().executeZOrderBy("DCP_FORMS_ACTIVITY_ID")
