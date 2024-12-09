# Databricks notebook source
import dlt

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
    name="pacs_clinical_event",
    comment="mill_clinical_event joined with pacs_requests",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def pacs_clinical_event():
    return spark.sql(
        """
        WITH rq AS (
            SELECT
                *,
                REPLACE(
                    REPLACE(SUBSTRING_INDEX(RequestQuestion,'-',6),'-',''),
                    ' ',
                    ''
                ) AS RequestShortcode
            FROM 4_prod.raw.pacs_requests
        ),
        ce1 AS (
            SELECT
                *,
                COALESCE(SERIES_REF_NBR, REFERENCE_NBR) AS MillRefNbr,
                CASE
                    WHEN EVENT_TAG = 'RADRPT'
                    THEN 1
                    ELSE 0
                END AS MillIsRadrpt
            FROM 4_prod.raw.mill_clinical_event
            WHERE 
                CONTRIBUTOR_SYSTEM_CD = 6141416 -- BLT_TIE_RAD 
                AND VALID_UNTIL_DT_TM > CURRENT_TIMESTAMP()
        ),
        ce2 AS (
            SELECT
                *,
                CASE
                    WHEN LEFT(MillRefNbr, 1) RLIKE '[0-9]'
                    THEN LEFT(MillRefNbr, 7)
                    ELSE LEFT(MillRefNbr, 16)
                END AS MillAccessionNbr,
                CASE
                    WHEN LEFT(MillRefNbr, 1) RLIKE '[0-9]'
                    THEN SUBSTRING(MillRefNbr, 8, LEN(MillRefNbr)-8)                     
                    ELSE SUBSTRING(MillRefNbr, 17, LEN(MillRefNbr)-17) 
                END AS MillRefNbrItemCode,
                RIGHT(MillRefNbr, 1) AS MillRefNbrLastDigit
            FROM ce1
        ),
        ce3 AS (
            SELECT
                *,
                CASE
                    WHEN MillRefNbrLastDigit = '0' AND EVENT_TAG != 'RADRPT'
                    THEN LEFT(MillRefNbrItemCode, LEN(MillRefNbrItemCode)/2)
                    ELSE MillRefNbrItemCode
                END AS MillPacsExamCode
            FROM ce2
        ),
        er AS (
            SELECT
                ExaminationReportExaminationId, 
                ExaminationReportRequestId,
                COUNT(DISTINCT ExaminationReportReportId) AS ReportIdCount
            FROM 4_prod.raw.pacs_examinationreports AS er
            GROUP BY
                ExaminationReportExaminationId, 
                ExaminationReportRequestId
        ),
        ex AS (
            SELECT *
            FROM er
            LEFT JOIN 4_prod.raw.pacs_examinations AS e
            ON er.examinationreportexaminationid = e.examinationid
        )
        SELECT 
            ce3.CLINICAL_EVENT_ID AS MillClinicalEventId,
            rq.RequestId,
            ex.ExaminationId,
            ce3.MillRefNbr AS MillPacsRefNbr,
            rq.RequestIdString AS RequestAccessionNbr,
            --ex.ExaminationReportReportId,
            ce3.MillPacsExamCode,
            ex.ExaminationBodyPart,
            ex.ExaminationModality,
            --ce2.PARENT_EVENT_ID AS MillParentEventId,
            ce3.ENCNTR_ID AS MillEncntrId,
            ce3.PERSON_ID AS MillPersonId,
            ce3.EVENT_TITLE_TEXT AS MillEventTitle,
            ce3.MillIsRadrpt,
            ex.ExaminationScheduledDate,
            ce3.EVENT_START_DT_TM AS MillEventStartDtTm, -- ReportDate?
            ce3.UPDT_DT_TM AS MillUpdtDtTm
        FROM ce3
        LEFT JOIN rq
        ON rq.RequestIdString = ce3.MillAccessionNbr
        LEFT JOIN ex
        ON 
            rq.RequestId = ex.examinationreportrequestid
            AND ce3.MillPacsExamCode = ex.ExaminationCode
            --AND DATE_FORMAT(ce3.EVENT_START_DT_TM, 'y-m') = DATE_FORMAT(ex.ExaminationScheduledDate, 'y-m')
        """
    )

    



# COMMAND ----------


