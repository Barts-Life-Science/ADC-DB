# Databricks notebook source
import dlt
from pyspark.sql import functions as F


# COMMAND ----------


def pacs_MillRefToAccessionNbr(column):
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
    
    

spark.udf.register("pacs_MillRefAccessionNbr", pacs_MillRefToAccessionNbr)

# COMMAND ----------

def pacs_MillRefToExamCode(mill_ref_col, accession_nbr_col):
    mill_ref_last_char = F.right(mill_ref_col, F.lit(1))
    mill_item_code = F.replace(mill_ref_col, accession_nbr_col, F.lit(''))
    mill_item_code = F.replace(mill_item_code, mill_ref_last_char, F.lit(''))
    mill_ref_left = F.left(mill_item_code, F.length(mill_item_code))
    mill_ref_right = F.right(mill_item_code, F.length(mill_item_code))
    return F.when(mill_ref_left.eqNullSafe(mill_ref_right), mill_ref_left).otherwise(mill_item_code)

spark.udf.register("pacs_MillRefToExamCode", pacs_MillRefToExamCode)

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
            *,
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
            END AS MillEventDate
        FROM 4_prod.raw.mill_clinical_event
        WHERE 
            CONTRIBUTOR_SYSTEM_CD = 6141416 -- BLT_TIE_RAD 
            AND VALID_UNTIL_DT_TM > CURRENT_TIMESTAMP()
        """
    )
    df = df.withColumn("MillAccessionNbr", pacs_MillRefToAccessionNbr(F.col("MillRefNbr")))
    df = df.withColumn("MillExamCode", pacs_MillRefToExamCode(F.col("MillRefNbr"), F.col("MillAccessionNbr")))
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

@dlt.table(
    name="stag_pacs_examinations",
    comment="staging pacs_examinations",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "temporary":"true"
    }
)
def stag_pacs_examinations():
    return spark.sql(
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
            --COALESCE(e.ExaminationModality, excd.ExaminationModality) AS ImputedExaminationModality
        FROM 4_prod.raw.pacs_examinations AS e
        LEFT JOIN er
        ON er.examinationreportexaminationid = e.examinationid
        --LEFT JOIN LIVE.stag_pacs_examinations_examcode AS excd
        --ON excd.ExaminationCode = e.ExaminationCode
        """
    )

# COMMAND ----------

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
            ce3.PERSON_ID AS MillPersonId,
            ce3.EVENT_TITLE_TEXT AS MillEventTitle,
            ce3.MillIsRadrpt,
            ex.ExaminationScheduledDate,
            CASE
                WHEN ce3.MillEventDate IS NULL
                THEN ex.ExaminationScheduledDate
                ELSE ce3.MillEventDate
            END AS MillEventDate,
            ce3.UPDT_DT_TM AS MillUpdtDtTm
            --blob.BLOB_CONTENTS AS ReportText
        FROM LIVE.stag_mill_clinical_event_pacs AS ce3
        LEFT JOIN 4_prod.raw.pacs_requests AS rq
        ON rq.RequestIdString = ce3.MillAccessionNbr
        LEFT JOIN LIVE.stag_pacs_examinations AS ex
        ON 
            rq.RequestId = ex.examinationreportrequestid
            AND ce3.MillExamCode = ex.ExaminationCode
        LEFT JOIN LIVE.pacs_lkp_examcode AS lkp_ex
        ON ce3.MillExamCode = lkp_ex.short_code
        LEFT JOIN LIVE.stag_pacs_examinations_examcode AS pexcd
        ON ce3.MillExamCode = pexcd.examinationcode
        --LEFT JOIN 4_prod.raw.pi_cde_blob_content AS blob
        --ON ce3.EVENT_ID = blob.EVENT_ID -- TODO: Check if EVENT_ID is one-to-one mapping
        """
    )

    



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
