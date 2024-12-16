# Databricks notebook source

def pacs_sql_MillRefToAccessionNbr():
    spark.sql(
    """
    CREATE OR REPLACE FUNCTION pacs_MillRefToAccessionNbr(MillRefNbr STRING)
    RETURNS STRING
    RETURN 
    SELECT   
        CASE
            WHEN LEFT(MillRefNbr, 16) RLIKE r'^\d{16}$'
            THEN LEFT(MillRefNbr, 16)
            WHEN LEFT(MillRefNbr, 7) RLIKE r'^\d{7}$'
            THEN LEFT(MillRefNbr, 7)
            WHEN LEFT(MillRefNbr, 6) RLIKE r'^\d{6}$'
            THEN LEFT(MillRefNbr, 6)
            WHEN LEFT(MillRefNbr, 6) = 'RNH098'
            THEN 'RNH098'
            WHEN SUBSTRING(MillRefNbr, 15, 1) RLIKE r'\D'
            THEN LEFT(MillRefNbr, 14)
            ELSE LEFT(MillRefNbr, 16)
        END
    """)

# COMMAND ----------

from pyspark.sql import functions as F

def pacs_py_MillRefToAccessionNbr(column):
    return \
        F.when(
            column.rlike(r'^\d{16}$'), 
            F.left(column, F.lit(16))
        ).when(
            column.rlike(r'^\d{7}$'),
            F.left(column, F.lit(7))
        ).when(
            column.rlike(r'^\d{6}$'),
            F.left(column, F.lit(6))
        ).when(
            column.like('RNH098'),
            F.lit('RNH098')
        ).when(
            F.substring(column, 15, 1).rlike(r'\D'),
            F.left(column, F.lit(14))
        ).otherwise(
            F.left(column, F.lit(16))
        )
    
    

spark.udf.register("pacs_py_MillRefAccessionNbr", pacs_py_MillRefToAccessionNbr)

# COMMAND ----------

import time

start = time.time()
df = spark.sql(
        """
        WITH ce1 AS (
            SELECT
                COALESCE(SERIES_REF_NBR, REFERENCE_NBR) AS MillRefNbr
            FROM 4_prod.raw.mill_clinical_event
            WHERE 
                CONTRIBUTOR_SYSTEM_CD = 6141416 -- BLT_TIE_RAD 
                AND VALID_UNTIL_DT_TM > CURRENT_TIMESTAMP()
        )
        SELECT
            pacs_MillRefToAccessionNbr(MillRefNbr) AS MillAccessionNbr
        FROM ce1
    """
)
df.collect()
end = time.time()
print(end-start)

# COMMAND ----------

start = time.time()
df = spark.sql(
    """
    SELECT
        COALESCE(SERIES_REF_NBR, REFERENCE_NBR) AS MillRefNbr
    FROM 4_prod.raw.mill_clinical_event
    WHERE 
        CONTRIBUTOR_SYSTEM_CD = 6141416 -- BLT_TIE_RAD 
        AND VALID_UNTIL_DT_TM > CURRENT_TIMESTAMP()
    """
)
df = df.withColumn("MillAccessionNbr", pacs_py_MillRefToAccessionNbr(F.col("MillRefNbr")))
df.collect()
end = time.time()
print(end-start)
