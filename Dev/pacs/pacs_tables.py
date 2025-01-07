# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import *

# COMMAND ----------

import sys, os

sys.path.append("/Workspace/Shared/ADC-DB/Dev/pacs/utils")
import pacs_data_transformations as DT

# COMMAND ----------

#spark.udf.register("identifyMillRefPattern", DT.identifyMillRefPattern)
#spark.udf.register("millRefAccessionNbr", DT.millRefToAccessionNbr)
#spark.udf.register("millRefToExamCode", DT.millRefToExamCode)
#spark.udf.register("transformExamAccessionNumber", DT.transformExamAccessionNumber)

# COMMAND ----------


    
    



# COMMAND ----------


    
    



# COMMAND ----------





# COMMAND ----------

@dlt.table(
    name="stag_patient_alias",
    comment="PacsPatientId, MillPersonId, Mrn, NhsNumber",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"#,
        #"pipelines.autoOptimize.zOrderCols": "MillPersonId"
    }
)
def stag_patient_alias():
    return spark.sql(
        """
        WITH alias AS (
            SELECT
                ALIAS,
                MAX(PERSON_ID) AS PERSON_ID,
                MAX(PERSON_ALIAS_TYPE_CD) AS PERSON_ALIAS_TYPE_CD
            FROM 4_prod.raw.mill_person_alias
            WHERE 
                ACTIVE_IND = 1
                AND PERSON_ID IS NOT NULL
            GROUP BY ALIAS
        ),
        mrn AS (
            SELECT
                MAX(ALIAS) AS ALIAS,
                PERSON_ID
            FROM 4_prod.raw.mill_person_alias
            WHERE 
                ACTIVE_IND = 1
                AND PERSON_ALIAS_TYPE_CD = 10 -- MRN
            GROUP BY PERSON_ID
        ),
        nhs AS (
            SELECT
                MAX(ALIAS) AS ALIAS,
                PERSON_ID
            FROM 4_prod.raw.mill_person_alias
            WHERE 
                PERSON_ALIAS_TYPE_CD = 18 -- NHS
                AND ACTIVE_IND = 1
            GROUP BY PERSON_ID
        )
        SELECT 
            PatientId AS PacsPatientId, 
            PatientPersonalId,
            alias.ALIAS AS MillAlias,
            alias.PERSON_ALIAS_TYPE_CD AS MillAliasType,
            CAST(alias.PERSON_ID AS BIGINT) AS MillPersonId,
            mrn.ALIAS AS Mrn,
            nhs.ALIAS AS NhsNumber
        FROM 4_prod.raw.pacs_patients AS p
        LEFT JOIN alias
        ON p.PatientPersonalId = alias.ALIAS
        LEFT JOIN mrn
        ON alias.PERSON_ID = mrn.PERSON_ID
        LEFT JOIN nhs
        ON alias.PERSON_ID = nhs.PERSON_ID
        """
    )
    



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
        WITH exam AS (
            SELECT
                ExaminationPatientId,
                MAX(ExaminationDate) AS LatestExamDate
            FROM LIVE.stag_pacs_examinations
            GROUP BY ExaminationPatientId
        )
        SELECT 
            p.*,
            exam.LatestExamDate
        FROM LIVE.stag_patient_alias AS p
        LEFT JOIN exam
        ON p.PacsPatientId = exam.ExaminationPatientId
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
        -- These are wrong person_id with name like 'TESTDONOTUSE'
        WITH bad_pid AS (
            SELECT DISTINCT PERSON_ID
            FROM 4_prod.raw.mill_person
            WHERE
                BIRTH_DT_TM < '1900-01-10'
                AND LOWER(NAME_FIRST) LIKE '%test%'
                AND LOWER(NAME_LAST) LIKE '%test%'
        )
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
            event_reltn_cd.DESCRIPTION AS MillEventReltn,
            CASE
                WHEN bad_pid.PERSON_ID IS NULL
                THEN ce.PERSON_ID
                ELSE NULL
            END AS MillPersonId
        FROM 4_prod.raw.mill_clinical_event AS ce
        LEFT JOIN 3_lookup.mill.mill_code_value AS event_class_cd
        ON ce.EVENT_CLASS_CD = event_class_cd.CODE_VALUE
        LEFT JOIN 3_lookup.mill.mill_code_value AS event_reltn_cd
        ON ce.EVENT_RELTN_CD = event_reltn_cd.CODE_VALUE
        LEFT JOIN bad_pid
        ON ce.PERSON_ID = bad_pid.PERSON_ID
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
        SELECT 
            e.*
        FROM 4_prod.raw.pacs_examinations AS e
        WHERE ADC_Deleted IS NULL
        --LEFT JOIN LIVE.stag_pacs_examinations_examcode AS excd
        --ON excd.ExaminationCode = e.ExaminationCode
        """
    )
    df = df.withColumn('ExaminationAccessionNumber_t', DT.transformExamAccessionNumber(F.col('ExaminationAccessionNumber'), F.col('ExaminationIdString')))
    df = df.withColumn('ExaminationIdString_t', DT.nullExamIdStrByList(F.col('ExaminationIdString'), DT.createExamIdStrBlacklist()))
    df = df.withColumn('ExamRefNbr', F.coalesce(F.col('ExaminationAccessionNumber_t'), F.col('ExaminationIdString_t'), F.col('ExaminationText1')))
    return df

# COMMAND ----------

@dlt.table(
    name="intmd_pacs_examinations",
    comment="intermediate pacs_examinations",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
        #"temporary":"true"
    }
)
def intmd_pacs_examinations():
    df = spark.sql(
        """
        WITH er AS (
            SELECT
                ExaminationReportExaminationId, 
                ExaminationReportRequestId,
                COUNT(ExaminationReportReportId) AS ExaminationReportCount
            FROM 4_prod.raw.pacs_examinationreports AS er
            GROUP BY ExaminationReportExaminationId, ExaminationReportRequestId
        ),
        ce AS (
            SELECT 
                MillAccessionNbr,
                MAX(CLINICAL_EVENT_ID) AS MillClinicalEventId,
                MAX(EVENT_ID) AS MillEventId,
                MAX(MillPersonId) AS MillPersonId
            FROM LIVE.stag_mill_clinical_event_pacs
            WHERE LOWER(MillEventReltn) = 'root'
            GROUP BY MillAccessionNbr
        ),
        pa AS (
            SELECT
                PacsPatientId,
                MAX(MillPersonId) AS MillPersonId
            FROM LIVE.stag_patient_alias
            GROUP BY PacsPatientId
        ),
        r AS (
            SELECT 
                RequestId,
                MAX(RequestIdString) AS RequestIdString
            FROM 4_prod.raw.pacs_requests
            WHERE ADC_Deleted IS NULL
            GROUP BY RequestId
        )
        SELECT 
            e.*,
            er.*,
            r.RequestId,
            r.RequestIdString,
            ce.MillAccessionNbr,
            ce.MillClinicalEventId,
            ce.MillEventId,
            ce.MillPersonId,
            pa.MillPersonId AS PacsMillPersonId
        FROM LIVE.stag_pacs_examinations AS e
        LEFT JOIN er
        ON er.examinationreportexaminationid = e.examinationid
        LEFT JOIN r
        ON er.ExaminationReportRequestId = r.RequestId
        LEFT JOIN ce
        ON ce.MillAccessionNbr = r.RequestIdString
        LEFT JOIN pa
        ON e.ExaminationPatientId = pa.PacsPatientId
        --LEFT JOIN LIVE.stag_pacs_examinations_examcode AS excd
        --ON excd.ExaminationCode = e.ExaminationCode
        WHERE 
            e.ADC_Deleted IS NULL
        """
    )
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
        SELECT 
            ExaminationId,
            ExaminationCode,
            ExaminationDescription,
            ExaminationModality,
            ExaminationBodyPart,
            ExaminationScheduledDate,
            ExaminationReportCount,
            RequestId,
            RequestIdString,
            MillAccessionNbr,
            ExamRefNbr,
            MillClinicalEventId,
            MillEventId,
            COALESCE(PacsMillPersonId, MillPersonId) AS MillPersonId
        FROM LIVE.intmd_pacs_examinations AS e
        --LEFT JOIN LIVE.stag_pacs_examinations_examcode AS excd
        --ON excd.ExaminationCode = e.ExaminationCode
        """)
    
    return df


# COMMAND ----------



# COMMAND ----------

@dlt.table(
    name="stag_pacs_requestquestion",
    comment="staging pacs requests table",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
    }
)
def stag_pacs_requestquestion():
    df = spark.sql(
        """
        SELECT
            RequestId,
            RequestQuestion,
            EXPLODE_OUTER(
                SPLIT(
                    RequestQuestion, 
                    r'----- '
                )
            ) AS SplitRequestQuestion,
            REGEXP_COUNT(
                RequestQuestion, 
                --r'----- ([A-Z0-9]{4,8}) ------'
                r'----- '
            ) AS RequestQuestionSplitCount
        FROM 4_prod.raw.pacs_requests
        WHERE 
            ADC_Deleted IS NULL
        """)
    df = df.filter("LENGTH(SplitRequestQuestion) > 0 OR LENGTH(REPLACE(RequestQuestion, '----- ', '')) = 0")
    df = df.withColumn("RequestQuestionExamCode", F.regexp_extract(F.col("SplitRequestQuestion"), r'(.+) ------', 1))
    df = df.withColumn("RequestQuestionExamCodeSeq", F.row_number().over(Window.partitionBy("RequestId", "RequestQuestionExamCode").orderBy("RequestId")))
    # add another col to show whether examcode is in the right format
    # Separate 
    return df


# COMMAND ----------

@dlt.table(
    name="stag_pacs_requestanamnesis",
    comment="staging pacs requests table",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
    }
)
def stag_pacs_requestanamnesis():
    df = spark.sql(
        """
        SELECT
            RequestId,
            RequestAnamnesis,
            EXPLODE_OUTER(
                SPLIT( -- split order is not guaranteed
                    RequestAnamnesis,
                    r'----- '
                )
            ) AS SplitRequestAnamnesis,
            REGEXP_COUNT(
                RequestAnamnesis, 
                --r'----- ([A-Z0-9]{4,8}) ------'
                r'----- '
            ) AS RequestAnamnesisSplitCount
        FROM 4_prod.raw.pacs_requests
        WHERE 
            ADC_Deleted IS NULL
        """)
    # Attempt to add examcode seq order info
    '''
    df = df.withColumn("RequestAnamnesisSplitCount", F.size(F.col("SplitRequestAnamnesisArray")))
    # Each array element maps to a new row
    df = df.select(df["*"], F.explode_outer(F.col("SplitRequestAnamnesisArray")).alias("SplitRequestAnamnesis"))
    '''

    # Drop empty output SplitRequestAnamnesis unless the input RequestAnamnesis is empty
    df = df.filter("LENGTH(SplitRequestAnamnesis) > 0 OR LENGTH(RequestAnamnesis) = 0")
    df = df.withColumn("RequestAnamnesisExamCode", F.regexp_extract(F.col("SplitRequestAnamnesis"), r'(.+) ------', 1))
    # TODO: Add index to array after regexp instead of this
    df = df.withColumn("RequestAnamnesisExamCodeSeq", F.row_number().over(Window.partitionBy("RequestId", "RequestAnamnesisExamCode").orderBy("RequestId")))
    # add another col to show whether examcode is in the right format
    return df


# COMMAND ----------

@dlt.table(
    name="intmd_pacs_requestexam",
    comment="intermediate pacs requests table",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
    }
)
def intmd_pacs_requestexam():
    df = spark.sql(
        """
        WITH pa AS (
            SELECT
                PacsPatientId,
                MAX(MillPersonId) AS MillPersonId
            FROM LIVE.stag_patient_alias
            GROUP BY PacsPatientId
        ),
        r AS (
            SELECT *
            FROM 4_prod.raw.pacs_requests
            WHERE ADC_Deleted IS NULL
        ),
        uni AS (
            SELECT
                RequestId,
                RequestQuestionExamCode AS RequestExamCode,
                RequestQuestionExamCodeSeq AS RequestExamCodeSeq
            FROM LIVE.stag_pacs_requestquestion

            UNION

            SELECT
                RequestId,
                RequestAnamnesisExamCode AS RequestExamCode,
                RequestAnamnesisExamCodeSeq AS RequestExamCodeSeq
            FROM LIVE.stag_pacs_requestanamnesis
            
        )
        SELECT
            r.*,
            uni.RequestExamCode,
            uni.RequestExamCodeSeq,
            rq.SplitRequestQuestion,
            ra.SplitRequestAnamnesis,
            pa.MillPersonId
        FROM uni
        LEFT JOIN LIVE.stag_pacs_requestquestion AS rq
        ON uni.RequestId = rq.RequestId
        AND uni.RequestExamCode = rq.RequestQuestionExamCode
        AND uni.RequestExamCodeSeq = rq.RequestQuestionExamCodeSeq
        LEFT JOIN LIVE.stag_pacs_requestanamnesis AS ra
        ON uni.RequestId = ra.RequestId
        AND uni.RequestExamCode = ra.RequestAnamnesisExamCode
        AND uni.RequestExamCodeSeq = ra.RequestAnamnesisExamCodeSeq
        LEFT JOIN r
        ON uni.RequestId = r.RequestId
        LEFT JOIN pa
        ON r.RequestPatientId = pa.PacsPatientId
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

    


