# Databricks notebook source
import dlt
from pyspark.sql import functions as F


# COMMAND ----------

import sys, os

sys.path.append("/Workspace/Shared/ADC-DB/Dev/pacs/utils")
import pacs_data_transformations as DT

# COMMAND ----------

spark.udf.register("identifyMillRefPattern", DT.identifyMillRefPattern)
spark.udf.register("millRefAccessionNbr", DT.millRefToAccessionNbr)
spark.udf.register("millRefToExamCode", DT.millRefToExamCode)
spark.udf.register("transformExamAccessionNumber", DT.transformExamAccessionNumber)

# COMMAND ----------


    
    



# COMMAND ----------


    
    



# COMMAND ----------





# COMMAND ----------



# COMMAND ----------

@dlt.table(
    name="pacs_patient_alias",
    comment="PacsPatientId, MillPersonId, Mrn, NhsNumber",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"#,
        #"pipelines.autoOptimize.zOrderCols": "MillPersonId"
    }
)
def pacs_patient_alias():
    return spark.sql(
        """
        SELECT 
            PatientId AS PacsPatientId, 
            CAST(mrn.PERSON_ID AS BIGINT) AS MillPersonId, 
            mrn.ALIAS AS Mrn,
            nhs.ALIAS AS NhsNumber
        FROM 4_prod.raw.pacs_patients AS p
        LEFT JOIN 4_prod.raw.mill_person_alias AS mrn
        ON p.PatientPersonalId = mrn.ALIAS
        LEFT JOIN 4_prod.raw.mill_person_alias AS nhs
        ON mrn.PERSON_ID = nhs.PERSON_ID
        WHERE 
            mrn.PERSON_ALIAS_TYPE_CD = 10 -- MRN
            AND nhs.PERSON_ALIAS_TYPE_CD = 18 -- NHS
            AND mrn.active_ind = 1
            AND nhs.active_ind = 1
        """
    )
    



# COMMAND ----------




# COMMAND ----------



@dlt.table(
    name="stag_mill_clinical_event_pacs",
    comment="staging mill_clinical_event data for pacs",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "temporary":"true"
    }
)
def stag_mill_clinical_event_pacs():
    df = spark.sql(
        """
        SELECT
            ce.*,
            COALESCE(SERIES_REF_NBR, REFERENCE_NBR) AS MillRefNbr,
            CASE
                WHEN EVENT_TAG = 'RADRPT'
                THEN 1
                ELSE 0
            END AS MillIsRadrpt,
            CASE
                WHEN EVENT_START_DT_TM < '2000-01-01'
                THEN NULL
                ELSE EVENT_START_DT_TM
            END AS MillEventDate,
            event_class_cd.DESCRIPTION AS MillEventClass,
            event_reltn_cd.DESCRIPTION AS MillEventReltn
        FROM 4_prod.raw.mill_clinical_event AS ce
        LEFT JOIN 3_lookup.mill.mill_code_value AS event_class_cd
        ON ce.EVENT_CLASS_CD = event_class_cd.CODE_VALUE
        LEFT JOIN 3_lookup.mill.mill_code_value AS event_reltn_cd
        ON ce.EVENT_RELTN_CD = event_reltn_cd.CODE_VALUE
        WHERE 
            CONTRIBUTOR_SYSTEM_CD = 6141416 -- BLT_TIE_RAD 
            AND VALID_UNTIL_DT_TM > CURRENT_TIMESTAMP()
        """
    )
    patterns = DT.createMillRefRegexPatternList()
    df = df.withColumn("MillAccessionNbr", DT.millRefToAccessionNbr(patterns, F.col("MillRefNbr")))
    df = df.withColumn("MillRefNbrPattern", DT.identifyMillRefPattern(patterns, F.col("MillRefNbr")))
    df = df.withColumn("MillExamCode", DT.millRefToExamCode(F.col("MillRefNbr"), F.col("MillAccessionNbr")))
    df = df.withColumn("MillEventDate", F.coalesce(F.col("MillEventDate"), F.col("PERFORMED_DT_TM")))
    return df


# COMMAND ----------

@dlt.table(
    name="stag_pacs_examinations_examcode",
    comment="Map examcode to exam modality for staging pacs_examinations",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "temporary":"true"
    }
)
def stag_pacs_examinations_examcode():
    return spark.sql(
        """
        SELECT
            ExaminationCode, 
            MODE(ExaminationModality) AS ExaminationModality
        FROM 4_prod.raw.pacs_examinations
        GROUP BY ExaminationCode
        """
    )


# COMMAND ----------



# COMMAND ----------

@dlt.table(
    name="stag_pacs_examinations",
    comment="staging pacs_examinations",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
        #"temporary":"true"
    }
)
def stag_pacs_examinations():
    df = spark.sql(
        """
        WITH er AS (
            SELECT
                ExaminationReportExaminationId, 
                ExaminationReportRequestId,
                COUNT(ExaminationReportReportId) AS ExaminationReportCount
            FROM 4_prod.raw.pacs_examinationreports AS er
            GROUP BY ExaminationReportExaminationId, ExaminationReportRequestId
        )
        SELECT 
            e.*,
            er.*
        FROM 4_prod.raw.pacs_examinations AS e
        LEFT JOIN er
        ON er.examinationreportexaminationid = e.examinationid
        WHERE ADC_Deleted IS NULL
        --LEFT JOIN LIVE.stag_pacs_examinations_examcode AS excd
        --ON excd.ExaminationCode = e.ExaminationCode
        """
    )
    df = df.withColumn('ExaminationAccessionNumber_t', DT.transformExamAccessionNumber(F.col('ExaminationAccessionNumber'), F.col('ExaminationIdString')))
    df = df.withColumn('ExamRefNbr', F.coalesce(F.col('ExaminationAccessionNumber_t'), F.col('ExaminationIdString'), F.col('ExaminationText1')))
    return df

# COMMAND ----------

@dlt.table(
    name="pacs_exam",
    comment="extended pacs exam table",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
    }
)
def pacs_exam():
    df = spark.sql(
        """
        With ce AS (
            SELECT 
                MillAccessionNbr,
                MAX(CLINICAL_EVENT_ID) AS MillClinicalEventId,
                MAX(EVENT_ID) AS MillEventId,
                MAX(PERSON_ID) AS MillPersonId
            FROM LIVE.stag_mill_clinical_event_pacs
            WHERE LOWER(MillEventReltn) = 'root'
            GROUP BY MillAccessionNbr
        )
        SELECT 
            e.ExaminationId,
            e.ExaminationModality,
            e.ExaminationBodyPart,
            e.ExaminationScheduledDate,
            e.ExaminationReportCount,
            r.RequestId,
            r.RequestIdString,
            ce.MillAccessionNbr,
            e.ExamRefNbr,
            ce.MillClinicalEventId,
            ce.MillEventId,
            ce.MillPersonId
        FROM LIVE.stag_pacs_examinations AS e
        LEFT JOIN 4_prod.raw.pacs_requests AS r
        ON e.ExaminationReportRequestId = r.RequestId
        LEFT JOIN ce
        ON 
            ce.MillAccessionNbr = r.RequestIdString
            --ce.MillAccessionNbr = e.ExamAccessionNbr 
        WHERE 
            e.ADC_Deleted IS NULL
            AND r.ADC_Deleted IS NULL
        --LEFT JOIN LIVE.stag_pacs_examinations_examcode AS excd
        --ON excd.ExaminationCode = e.ExaminationCode
        """)
    
    return df


# COMMAND ----------

'''
@dlt.table(
    name="pacs_clinical_event",
    comment="mill_clinical_event joined with pacs_requests",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
#@dlt.expect("valid timestamp", "MillEventStartDtTm > '2000-01-01'")
def pacs_clinical_event():
    return spark.sql(
        """
        SELECT 
            ce3.CLINICAL_EVENT_ID AS MillClinicalEventId,
            rq.RequestId,
            ex.ExaminationId,
            ex.ExaminationReportCount,
            ce3.MillRefNbr AS MillPacsRefNbr,
            COALESCE(rq.RequestIdString, ce3.MillAccessionNbr) AS AccessionNbr,
            ce3.MillExamCode,
            lkp_ex.id AS LkpExamCodeId,
            ex.ExaminationBodyPart,
            COALESCE(ex.ExaminationModality, pexcd.ExaminationModality) AS ExaminationModality,
            ce3.ENCNTR_ID AS MillEncntrId,
            ce3.EVENT_ID AS MillEventId,
            ce3.PERSON_ID AS MillPersonId,
            ce3.EVENT_TITLE_TEXT AS MillEventTitle,
            ce3.MillIsRadrpt,
            ce3.MillEventClass,
            ce3.MillEventReltn,
            ex.ExaminationScheduledDate,
            CASE
                WHEN ce3.MillEventDate IS NULL
                THEN ex.ExaminationScheduledDate
                ELSE ce3.MillEventDate
            END AS MillEventDate,
            CASE
                WHEN MillEventDate IS NULL
                THEN 0
                ELSE DATEDIFF(ex.ExaminationScheduledDate, ce3.MillEventDate)
            END AS ExamDateDiff,
            ce3.UPDT_DT_TM AS MillUpdtDtTm
        FROM LIVE.stag_mill_clinical_event_pacs AS ce3
        LEFT JOIN (SELECT * FROM 4_prod.raw.pacs_requests WHERE ADC_Deleted IS NULL) AS rq
        ON rq.RequestIdString = ce3.MillAccessionNbr
        LEFT JOIN LIVE.stag_pacs_examinations AS ex
        ON 
            rq.RequestId = ex.examinationreportrequestid
            AND ce3.MillExamCode = ex.ExaminationCode
        LEFT JOIN LIVE.pacs_lkp_examcode AS lkp_ex
        ON ce3.MillExamCode = lkp_ex.short_code
        LEFT JOIN LIVE.stag_pacs_examinations_examcode AS pexcd
        ON ce3.MillExamCode = pexcd.examinationcode
        """
    )

    
'''


# COMMAND ----------

@dlt.table(
    name="pacs_lkp_examcode",
    comment="mill_clinical_event joined with pacs_requests",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def pacs_lkp_examcode():
    return spark.read.format("csv") \
                     .option("header", "true") \
                     .load("/Volumes/4_prod/pacs/base/Annex-1-DID_lookup_group.csv")

# COMMAND ----------

@dlt.table(
    name="pacs_blob_content",
    comment="mill_clinical_event joined with pacs_requests",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def pacs_blob_content():
    return spark.sql(
        """
        WITH milleventid AS (
            SELECT DISTINCT EVENT_ID
            FROM LIVE.stag_mill_clinical_event_pacs AS ce
        )
        SELECT
            blob.EVENT_ID,
            blob.BLOB_CONTENTS AS ReportText
        FROM 4_prod.raw.pi_cde_blob_content AS blob
        INNER JOIN milleventid
        ON blob.EVENT_ID = milleventid.EVENT_ID
        """
    )

    


