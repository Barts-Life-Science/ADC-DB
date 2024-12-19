# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from collections import OrderedDict

# COMMAND ----------

def createMillRefRegexPatternList():
    patterns = []
    # Each item is a 3-element tuple (preprocess, regex_pattern, extraction)
    patterns.append((lambda col:F.left(col, F.lit(16)), r'\d{16}', lambda col:F.left(col, F.lit(16))))
    patterns.append((lambda col:F.left(col, F.lit(7)), r'\d{7}', lambda col:F.left(col, F.lit(7))))
    patterns.append((lambda col:F.left(col, F.lit(6)), r'\d{6}', lambda col:F.left(col, F.lit(6))))
    patterns.append((lambda col:F.left(col, F.lit(6)), r'RNH098', lambda col:F.lit('RNH098')))
    patterns.append((lambda col:F.substring(col, 15, 1), r'\D', lambda col:F.left(col, F.lit(14))))
    patterns.append((lambda col:col, r'.+', lambda col:F.left(col, F.lit(16))))
    return patterns


# COMMAND ----------




def pacs_IdentifyMillRefPattern(pattern_list, column):    
    for i, pat in enumerate(pattern_list[::-1]):
        if i == 0:
            nested = F.when(pat[0](column).rlike(pat[1]), F.lit(pat[1])).otherwise(F.lit(None))
        else:
            nested = F.when(pat[0](column).rlike(pat[1]), F.lit(pat[1])).otherwise(nested)

    return nested
    
    

spark.udf.register("pacs_IdentifyMillRefPattern", pacs_IdentifyMillRefPattern)

# COMMAND ----------


def pacs_MillRefToAccessionNbr(pattern_list, column):
    for i, pat in enumerate(pattern_list[::-1]):
        if i == 0:
            nested = F.when(pat[0](column).rlike(pat[1]), pat[2](column)).otherwise(F.lit(None))
        else:
            nested = F.when(pat[0](column).rlike(pat[1]), pat[2](column)).otherwise(nested)

    return nested
    
    

spark.udf.register("pacs_MillRefAccessionNbr", pacs_MillRefToAccessionNbr)

# COMMAND ----------

def pacs_MillRefToExamCode(mill_ref_col, accession_nbr_col):
    mill_item_code = F.replace(F.left(mill_ref_col, F.length(mill_ref_col)-1), accession_nbr_col, F.lit(''))
    mill_item_code = F.replace(mill_item_code, F.lit('_SECTRA'), F.lit(''))
    mill_ref_left = F.left(mill_item_code, F.length(mill_item_code)/2)
    mill_ref_right = F.right(mill_item_code, F.length(mill_item_code)/2)
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
    patterns = createMillRefRegexPatternList()
    df = df.withColumn("MillAccessionNbr", pacs_MillRefToAccessionNbr(patterns, F.col("MillRefNbr")))
    df = df.withColumn("MillRefNbrPattern", pacs_IdentifyMillRefPattern(patterns, F.col("MillRefNbr")))
    df = df.withColumn("MillExamCode", pacs_MillRefToExamCode(F.col("MillRefNbr"), F.col("MillAccessionNbr")))
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
        "delta.enableRowTracking": "true",
        "temporary":"true"
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
            --COALESCE(e.ExaminationModality, excd.ExaminationModality) AS ImputedExaminationModality
        FROM 4_prod.raw.pacs_examinations AS e
        LEFT JOIN er
        ON er.examinationreportexaminationid = e.examinationid
        WHERE ADC_Deleted IS NULL
        --LEFT JOIN LIVE.stag_pacs_examinations_examcode AS excd
        --ON excd.ExaminationCode = e.ExaminationCode
        """
    )
    df = df.withColumn('ExaminationAccessionNumber', F.when(F.col('ExaminationAccessionNumber').eqNullSafe(F.lit('VALUE_TOO_LONG')), F.lit(None)))
    df = df.withColumn('ExaminationLongIdString', F.when(F.length(F.col('ExaminationIdString'))<6, F.lit(None)))
    df = df.withColumn('ExamIdOrAccessionNbr', F.coalesce(F.col('ExaminationAccessionNumber'), F.col('ExaminationLongIdString'), F.col('ExaminationText1')))
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
            e.ExamIdOrAccessionNbr,
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

    


