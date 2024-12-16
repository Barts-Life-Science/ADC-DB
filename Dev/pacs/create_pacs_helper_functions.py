# Databricks notebook source

def pacs_MillRefToAccessionNbr():
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
