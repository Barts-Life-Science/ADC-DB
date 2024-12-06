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
        SELECT 
            ce.CLINICAL_EVENT_ID AS MillClinicalEventId,
            ce.ENCNTR_ID AS MillEncntrId,
            ce.PERSON_ID As MillPersonId,
            rq.RequestIdString AS RequestAccessionNumber,
            ce.SERIES_REF_NBR AS MillSeriesRefNbr,
            ce.REFERENCE_NBR AS MillReferenceNbr,
            ce.EVENT_TITLE_TEXT AS MillEventTitle,
            REPLACE(REPLACE(SUBSTRING_INDEX(rq.RequestQuestion, '-', 6), '-', ''), ' ', '') AS RequestShortcode,
            ce.EVENT_START_DT_TM AS MillEventStartDtTm,
            ce.UPDT_DT_TM AS MillUpdtDtTm
        FROM 4_prod.raw.pacs_requests AS rq
        LEFT JOIN 4_prod.raw.mill_clinical_event AS ce
        ON rq.RequestIdString = LEFT(COALESCE(ce.SERIES_REF_NBR, ce.REFERENCE_NBR), 16)
        WHERE 
            ce.CONTRIBUTOR_SYSTEM_CD = 6141416 -- BLT_TIE_RAD
        """
    )

    



# COMMAND ----------


