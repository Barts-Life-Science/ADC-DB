# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window


@dlt.table(
    name="code_value",
    comment="Code Value Table"
)
def lookup_code_value():
    return (
        spark.table("3_lookup.dwh.mill_dir_code_value").filter(col("ACTIVE_IND") > 0)
)

@dlt.table(
    name="patient_nhs",
    comment="NHS table"
)
def lookup_nhs_number():
    window = Window.partitionBy("PERSON_ID").orderBy(desc("END_EFFECTIVE_DT_TM"))
    return (
        spark.table("4_prod.raw.mill_dir_person_alias")
        .filter((col("PERSON_ALIAS_TYPE_CD") == 18) & (col("ACTIVE_IND") == 1))
        .withColumn("row", row_number().over(window))
        .filter(col("row") == 1)
        .select("PERSON_ID", "ALIAS", "ADC_UPDT")
    )

@dlt.table(
    name="patient_mrn",
    comment="MRN table"
)
def lookup_mrn():
    window = Window.partitionBy("PERSON_ID").orderBy(desc("END_EFFECTIVE_DT_TM"))
    return (
        spark.table("4_prod.raw.mill_dir_person_alias")
        .filter((col("PERSON_ALIAS_TYPE_CD") == 10) & (col("ACTIVE_IND") == 1))
        .withColumn("row", row_number().over(window))
        .filter(col("row") == 1)
        .select("PERSON_ID", "ALIAS", "ADC_UPDT")
    )

@dlt.table(
    name="current_address",
    comment="Address table"
)
def lookup_address():
    window = Window.partitionBy("PARENT_ENTITY_ID").orderBy(desc("END_EFFECTIVE_DT_TM"))
    return (
        spark.table("4_prod.raw.mill_dir_address")
        .filter((col("PARENT_ENTITY_NAME") == "PERSON") & (col("ACTIVE_IND") == 1))
        .withColumn("row", row_number().over(window))
        .filter(col("row") == 1)
        .select("PARENT_ENTITY_ID", "ZIPCODE", "CITY", "ADC_UPDT")
    )

# COMMAND ----------


