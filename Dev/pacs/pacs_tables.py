# Databricks notebook source


# COMMAND ----------

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
    name="intmd_pacs_patient_alias",
    comment="PacsPatientId, MillPersonId, Mrn, NhsNumber",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"#,
        #"pipelines.autoOptimize.zOrderCols": "MillPersonId"
    }
)
def intmd_pacs_patient_alias():
    return spark.sql(
        """
        WITH exam AS (
            SELECT
                ExaminationPatientId,
                MAX(ExaminationDate) AS LatestExamDate
            FROM LIVE.stag_pacs_examinations
            GROUP BY ExaminationPatientId
        ),
        millpersonid AS (
            SELECT
                MillAccessionNbr,
                MAX(MillPersonId) AS MillPersonId
            FROM LIVE.stag_mill_clinical_event_pacs
            GROUP BY MillAccessionNbr
        ),
        rq_personid AS (
            SELECT
                RequestPatientId,
                MAX(MillPersonId) AS MillPersonId
            FROM 4_prod.raw.pacs_requests AS r
            INNER JOIN millpersonid AS m
            ON r.RequestIdString = m.MillAccessionNbr
            WHERE ADC_Deleted IS NULL
            GROUP BY RequestPatientId
        ),
        ex_personid AS (
            SELECT
                ExaminationPatientId,
                MAX(MillPersonId) AS MillPersonId
            FROM LIVE.stag_pacs_examinations AS e
            INNER JOIN millpersonid AS m
            ON m.MillAccessionNbr = e.ExamRefNbr
            GROUP BY ExaminationPatientId
        ),
        ce_personid AS (
            SELECT DISTINCT 
                MillPersonId
            FROM LIVE.stag_mill_clinical_event_pacs
        )
        SELECT 
            p.*,
            rq_personid.MillPersonId AS RequestMillPersonId,
            ex_personid.MillPersonId AS ExamMillPersonId,
            COALESCE(p.MillPersonId, rq_personid.MillPersonId, ex_personid.MillPersonId) AS MillPersonId_t,
            exam.LatestExamDate,
            CASE
                WHEN ce_personid.MillPersonId IS NULL
                THEN FALSE
                ELSE TRUE
            END AS PatientIdCouldBePersonId
            
        FROM LIVE.stag_patient_alias AS p
        LEFT JOIN exam
        ON p.PacsPatientId = exam.ExaminationPatientId
        LEFT JOIN rq_personid
        ON p.PacsPatientId = rq_personid.RequestPatientId
        LEFT JOIN ex_personid
        ON p.PacsPatientId = ex_personid.ExaminationPatientId
        LEFT JOIN ce_personid
        ON p.PacsPatientId = ce_personid.MillPersonId
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
                WHEN EVENT_START_DT_TM < '2000-01-01'
                THEN NULL
                ELSE EVENT_START_DT_TM
            END AS MillEventDate,
            CASE
                WHEN bad_pid.PERSON_ID IS NULL
                THEN ce.PERSON_ID
                ELSE NULL
            END AS MillPersonId,
            event_class_cd.DESCRIPTION AS MillEventClass,
            event_reltn_cd.DESCRIPTION AS MillEventReltn
        FROM 4_prod.raw.mill_clinical_event AS ce
        LEFT JOIN bad_pid
        ON ce.PERSON_ID = bad_pid.PERSON_ID
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
    name="intmd_mill_clinical_event_pacs",
    comment="intermediate mill_clinical_event data for pacs",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "temporary":"true"
    }
)
def intmd_mill_clinical_event_pacs():
    df = spark.sql(
        """
        WITH pacs_pid AS (
            SELECT
                MillPersonId,
                MAX(PacsPatientId) AS PacsPatientId
            FROM LIVE.stag_patient_alias
            WHERE MillPersonId IS NOT NULL
            GROUP BY MillPersonId
        )
        SELECT
            CLINICAL_EVENT_ID,
            PARENT_EVENT_ID,
            EVENT_ID,
            ENCNTR_ID,
            EVENT_TITLE_TEXT AS MillEventTitleText,
            ce.MillPersonId,
            PacsPatientId,
            MillAccessionNbr,
            MillExamCode,
            MillEventDate,
            EVENT_TAG,
            MillEventClass,
            MillEventReltn,
            COALESCE(SERIES_REF_NBR, REFERENCE_NBR) AS MillRefNbr
        FROM LIVE.stag_mill_clinical_event_pacs AS ce
        LEFT JOIN pacs_pid
        ON ce.MillPersonId = pacs_pid.MillPersonId
        """)
    return df

# COMMAND ----------



@dlt.table(
    name="intmd_pacs_examcode",
    comment="Map examcode to exam modality for staging pacs_examinations",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "temporary":"true"
    }
)
def intmd_pacs_examcode():
    df = spark.sql(
        """
        WITH uni AS (
            SELECT DISTINCT
                ExaminationCode AS RawExamCode
            FROM LIVE.stag_pacs_examinations
            UNION
            SELECT DISTINCT
                RequestQuestionExamCode AS RawExamCode
            FROM LIVE.stag_pacs_requestquestion
            UNION
            SELECT DISTINCT
                RequestAnamnesisExamCode AS RawExamCode
            FROM LIVE.stag_pacs_requestanamnesis
            UNION
            SELECT DISTINCT
                MillExamCode AS RawExamCode
            FROM LIVE.stag_mill_clinical_event_pacs
        ),
        exam AS (
            SELECT
                ExaminationCode AS ExamCode,
                MODE(ExaminationModality) AS ExamModality,
                MODE(ExaminationBodyPart) AS ExamBodyPart,
                COUNT(*) AS NumRowsInPacsExam
            FROM LIVE.stag_pacs_examinations
            GROUP BY ExaminationCode
        ),
        exam2 AS (
            SELECT
                ExaminationCode AS ExamCode,
                MODE(ExaminationModality) AS ExamModality2
            FROM LIVE.stag_pacs_examinations AS e
            LEFT JOIN exam
            ON e.ExaminationCode = exam.ExamCode
            WHERE exam.ExamModality != ExaminationModality
            GROUP BY ExaminationCode
        ),
        ce1 AS (
            SELECT
                EVENT_TITLE_TEXT AS MillEventTitleText,
                MODE(MillExamCode) AS MillExamCode
            FROM LIVE.stag_mill_clinical_event_pacs
            GROUP BY EVENT_TITLE_TEXT
        ),
        ce2 AS (
            SELECT
                MillExamCode,
                MODE(EVENT_TITLE_TEXT) AS MillEventTitleText,
                COUNT(*) AS NumRowsInMillCE
            FROM LIVE.stag_mill_clinical_event_pacs
            GROUP BY MillExamCode
        ),
        rqq AS (
            SELECT
                RequestQuestionExamCode,
                COUNT(*) AS NumRowsInRequestQuestion
            FROM LIVE.stag_pacs_requestquestion
            GROUP BY RequestQuestionExamCode
        ),
        rqa AS (
            SELECT
                RequestAnamnesisExamCode,
                COUNT(*) AS NumRowsInRequestAnamnesis
            FROM LIVE.stag_pacs_requestanamnesis
            GROUP BY RequestAnamnesisExamCode
        ),
        ai_ec AS (
            SELECT vs_input, MAX(ValidatedAIExamCode) AS AIExamCode
            FROM 4_prod.pacs_ai.predicted_pacs_examcode
            GROUP BY vs_input
        )
        SELECT
            RawExamCode,
            ce1.MillExamCode,
            ai_ec.AIExamCode,
            COALESCE(ai_ec.AIExamCode, ce1.MillExamCode, uni.RawExamCode) AS ExamCode,
            ExamModality,
            ExamModality2,
            COALESCE(ce1.MillEventTitleText, ce2.MillEventTitleText) AS MillEventTitleText,
            ExamBodyPart,
            rqq.NumRowsInRequestQuestion,
            rqa.NumRowsInRequestAnamnesis,
            exam.NumRowsInPacsExam,
            ce2.NumRowsInMillCE
        FROM uni
        LEFT JOIN ce1
        ON uni.RawExamCode = ce1.MillEventTitleText
        LEFT JOIN ce2
        ON uni.RawExamCode = ce2.MillExamCode
        LEFT JOIN exam
        ON COALESCE(ce1.MillExamCode, uni.RawExamCode) = exam.ExamCode
        LEFT JOIN rqq
        ON uni.RawExamCode = rqq.RequestQuestionExamCode
        LEFT JOIN rqa
        ON uni.RawExamCode = rqa.RequestAnamnesisExamCode
        LEFT JOIN exam2
        ON exam2.ExamCode = uni.RawExamCode
        LEFT JOIN ai_ec
        ON ai_ec.vs_input = Uni.RawExamCode
        """
    )
    df = df.withColumn("ExamCode", F.when(F.col("ExamCode").like("Z%"), F.right(F.col("ExamCode"), F.length(F.col("ExamCode"))-1)).otherwise(F.col("ExamCode")))
    lkp = spark.read.table("LIVE.pacs_examcode_dict").select("short_code")
    df = df.join(lkp.alias("lkp"), F.col("ExamCode") == F.col("lkp.short_code"), "left")
    df = df.withColumn("IsExamCodeInLkp", F.when(F.col("short_code").isNotNull(), F.lit(True)).otherwise(F.lit(False)))
    df = df.drop("short_code")
    return df

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
    #df = df.withColumn('ExaminationIdString_t', DT.nullExamIdStrByList(F.col('ExaminationIdString'), DT.createExamIdStrBlacklist()))
    df = df.withColumn('ExaminationIdString_t', F.col('ExaminationIdString'))
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
        ce_examcode AS (
            SELECT
                EVENT_TITLE_TEXT,
                MAX(MillExamCode) AS MillExamCode
            FROM LIVE.stag_mill_clinical_event_pacs
            GROUP BY EVENT_TITLE_TEXT
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
            COALESCE(ce.MillAccessionNbr, ce2.MillAccessionNbr) AS MillAccessionNbr,
            COALESCE(ce.MillClinicalEventId, ce2.MillClinicalEventId) AS MillClinicalEventId,
            COALESCE(ce.MillEventId, ce2.MillEventId) AS MillEventId,
            pa.MillPersonId,
            COALESCE(pa.MillPersonId, ce.MillPersonId, ce2.MillPersonId) AS MillPersonId_t,
            COALESCE(ce_examcode.MillExamCode, e.ExaminationCode) AS ExaminationCode_t
        FROM LIVE.stag_pacs_examinations AS e
        LEFT JOIN er
        ON er.examinationreportexaminationid = e.examinationid
        LEFT JOIN r
        ON er.ExaminationReportRequestId = r.RequestId
        LEFT JOIN ce -- TODO: this should join with ExamRefNbr too
        ON ce.MillAccessionNbr = r.RequestIdString
        LEFT JOIN ce AS ce2
        ON ce2.MillAccessionNbr = e.ExamRefNbr
        LEFT JOIN pa
        ON e.ExaminationPatientId = pa.PacsPatientId
        LEFT JOIN ce_examcode
        ON ce_examcode.EVENT_TITLE_TEXT = e.ExaminationCode
        --LEFT JOIN LIVE.stag_pacs_examinations_examcode AS excd
        --ON excd.ExaminationCode = e.ExaminationCode
        WHERE 
            e.ADC_Deleted IS NULL
        """
    )

    return df

# COMMAND ----------

'''
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
            MillPersonId_t AS MillPersonId
        FROM LIVE.intmd_pacs_examinations AS e
        --LEFT JOIN LIVE.stag_pacs_examinations_examcode AS excd
        --ON excd.ExaminationCode = e.ExaminationCode
        """)
    
    return df
'''

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
            --SPLIT(
            --    RequestQuestion, 
            --    r'----- '
            --) AS SplitRequestQuestionArray,
            SPLIT(
                RequestQuestion, 
                r'\n----- '
            ) AS SplitRequestQuestionArray,
            REGEXP_COUNT(
                RequestQuestion,
                r'----- ([^\n\-\,\.]{1,40}) ------'
            ) AS RequestQuestionExamCodeCount
        FROM 4_prod.raw.pacs_requests
        WHERE 
            ADC_Deleted IS NULL
        """)
    df = df.withColumn("RequestQuestionSplitCount", F.size(F.col("SplitRequestQuestionArray")))
    #df = df.withColumn("RequestQuestionSplitCount", F.coalesce(F.col("RequestQuestionSplitCount"), F.lit(-1)))
    # Each array element maps to a new row
    df = df.select(df["*"], F.posexplode_outer(F.col("SplitRequestQuestionArray")).alias("SplitRequestQuestionSeqNum", "SplitRequestQuestion"))
    df = df.withColumn("SplitRequestQuestion", F.when(F.col("SplitRequestQuestionSeqNum")>0, F.concat(F.lit("----- "), F.col("SplitRequestQuestion"))).otherwise(F.col("SplitRequestQuestion")))

    #df = df.filter("LENGTH(SplitRequestQuestion) > 0 OR RequestQuestionSplitCount <= 1")
    df = df.withColumn("RequestQuestionExamCode", F.regexp_extract(F.col("SplitRequestQuestion"), r'----- ([^\n\-\,\.]{1,40}) ------', 1))
    df = df.withColumn("RequestQuestionExamCode", F.when(F.col("RequestQuestionExamCodeCount")>0, F.rtrim(F.ltrim(F.col("RequestQuestionExamCode")))).otherwise(F.lit(None)))
    df = df.withColumn("RequestQuestionExamCodeSeq", F.row_number().over(Window.partitionBy("RequestId", "RequestQuestionExamCode").orderBy("SplitRequestQuestionSeqNum")))
    

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
            --SPLIT( -- split order is not guaranteed
            --    RequestAnamnesis,
            --    r'----- '
            --) AS SplitRequestAnamnesisArray
            SPLIT(
                RequestAnamnesis, 
                r'\n----- '
            ) AS SplitRequestAnamnesisArray,
            REGEXP_COUNT(
                RequestAnamnesis,
                r'----- ([^\n\-\,\.]{1,40}) ------'
            ) AS RequestAnamnesisExamCodeCount
        FROM 4_prod.raw.pacs_requests
        WHERE 
            ADC_Deleted IS NULL
        """)

    df = df.withColumn("RequestAnamnesisSplitCount", F.size(F.col("SplitRequestAnamnesisArray")))
    df = df.withColumn("RequestAnamnesisSplitCount", F.coalesce(F.col("RequestAnamnesisSplitCount"), F.lit(-1)))
    # Each array element maps to a new row
    df = df.select(df["*"], F.posexplode_outer(F.col("SplitRequestAnamnesisArray")).alias("SplitRequestAnamnesisSeqNum","SplitRequestAnamnesis"))
    df = df.withColumn("SplitRequestAnamnesis", F.when(F.col("SplitRequestAnamnesisSeqNum")>0, F.concat(F.lit("----- "), F.col("SplitRequestAnamnesis"))).otherwise(F.col("SplitRequestAnamnesis")))
    # Drop empty output SplitRequestAnamnesis unless the input RequestAnamnesis is empty
    #df = df.filter("LENGTH(SplitRequestAnamnesis) > 0 OR RequestAnamnesisSplitCount <= 1")
    df = df.withColumn("RequestAnamnesisExamCode", F.regexp_extract(F.col("SplitRequestAnamnesis"), r'----- ([^\n\-\,\.]{1,40}) ------', 1))
    df = df.withColumn("RequestAnamnesisExamCode", F.when(F.col("RequestAnamnesisExamCodeCount")>0, F.rtrim(F.ltrim(F.col("RequestAnamnesisExamCode")))).otherwise(F.lit(None)))
    # TODO: Add index to array after regexp instead of this
    df = df.withColumn("RequestAnamnesisExamCodeSeq", F.row_number().over(Window.partitionBy("RequestId", "RequestAnamnesisExamCode").orderBy("SplitRequestAnamnesisSeqNum")))
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
            
        ),
        ce AS (
            SELECT
                MillAccessionNbr,
                MAX(MillPersonId) AS MillPersonId,
                MAX(MillEventDate) AS MillEventDate,
                Max(MillExamCode) AS MillExamCode,
                MAX(Clinical_Event_Id) AS MillClinicalEventId
            FROM LIVE.stag_mill_clinical_event_pacs
            GROUP BY MillAccessionNbr
        ),
        ce_examcode AS (
            SELECT
                EVENT_TITLE_TEXT,
                MAX(MillExamCode) AS MillExamCode
            FROM LIVE.stag_mill_clinical_event_pacs
            GROUP BY EVENT_TITLE_TEXT
        ),
        exam AS (
            SELECT
                ExaminationReportRequestId,
                MAX(ExaminationDate) AS ExaminationDate,
                MAX(ExaminationCode) AS ExaminationCode,
                MAX(ExaminationId) AS ExaminationId
            FROM 4_prod.raw.pacs_examinationreports AS er
            LEFT JOIN LIVE.stag_pacs_examinations AS e
            ON er.examinationreportexaminationid = e.examinationid
            WHERE e.ADC_Deleted IS NULL
            GROUP BY ExaminationReportRequestId
        ),
        erp AS (
            SELECT
                ExaminationReportRequestId,
                MAX(ReportId) AS ReportId,
                MAX(ReportDate) AS ReportDate
            FROM 4_prod.raw.pacs_examinationreports AS er
            LEFT JOIN 4_prod.raw.pacs_reports AS rp
            ON er.examinationreportreportid = rp.reportid
            WHERE rp.ADC_Deleted IS NULL
            GROUP BY ExaminationReportRequestId
        )
        SELECT
            r.*,
            CASE
                WHEN LENGTH(uni.RequestExamCode) = 0
                THEN NULL
                ELSE uni.RequestExamCode
            END AS RequestExamCode,
            exam.ExaminationCode,
            ce.MillExamCode,
            ce_examcode.MillExamCode AS MillExamCode2,
            uni.RequestExamCodeSeq,
            rq.SplitRequestQuestion,
            ra.SplitRequestAnamnesis,
            pa.MillPersonId,
            COALESCE(pa.MillPersonId, ce.MillPersonId) AS MillPersonId_t,
            ce.MillEventDate,
            exam.ExaminationDate,
            COALESCE(exam.ExaminationDate, ce.MillEventDate, erp.ReportDate) AS ExamDate,
            ce.MillClinicalEventId,
            exam.ExaminationId,
            erp.ReportId
        FROM uni
        LEFT JOIN LIVE.stag_pacs_requestquestion AS rq
        ON uni.RequestId = rq.RequestId
        AND (
            uni.RequestExamCode = rq.RequestQuestionExamCode 
            OR (uni.RequestExamCode IS NULL AND rq.RequestQuestionExamCode IS NULL))
        AND uni.RequestExamCodeSeq = rq.RequestQuestionExamCodeSeq
        LEFT JOIN LIVE.stag_pacs_requestanamnesis AS ra
        ON uni.RequestId = ra.RequestId
        AND (
            uni.RequestExamCode = ra.RequestAnamnesisExamCode
            OR (uni.RequestExamCode IS NULL AND ra.RequestAnamnesisExamCode IS NULL)
        )
        AND uni.RequestExamCodeSeq = ra.RequestAnamnesisExamCodeSeq
        LEFT JOIN r
        ON uni.RequestId = r.RequestId
        LEFT JOIN pa
        ON r.RequestPatientId = pa.PacsPatientId
        LEFT JOIN ce
        ON r.RequestIdString = ce.MillAccessionNbr
        LEFT JOIN exam
        ON uni.RequestId = exam.examinationreportrequestid
        LEFT JOIN erp
        ON uni.RequestId = erp.examinationreportrequestid
        LEFT JOIN ce_examcode
        ON uni.RequestExamCode = ce_examcode.EVENT_TITLE_TEXT
        """)
    
    #df = df.withColumn("RequestExamCode_t", F.when(F.length(F.col("RequestExamCode"))>0, F.col("RequestExamCode")).otherwise(F.coalesce(F.col("ExaminationCode"), F.col("MillExamCode"))))
    df = df.withColumn("RequestExamCode_t", F.coalesce(F.col("MillExamCode2"), F.col("RequestExamCode"), F.col("ExaminationCode"), F.col("MillExamCode")))
    

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
    name="pacs_examcode_dict",
    comment="mill_clinical_event joined with pacs_requests",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def pacs_examcode_dict():
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
        WITH millevent AS (
            SELECT DISTINCT
                EVENT_ID,
                --MAX(MillAccessionNbr) AS MillAccessionNbr
                MillAccessionNbr
            FROM LIVE.stag_mill_clinical_event_pacs AS ce
            --GROUP BY EVENT_ID
        )
        SELECT
            blob.*,
            millevent.MillAccessionNbr
        FROM 4_prod.raw.pi_cde_blob_content AS blob
        INNER JOIN millevent
        ON blob.EVENT_ID = millevent.EVENT_ID
        """
    )

    



# COMMAND ----------

@dlt.table(
    name="all_pacs_ref_nbr",
    comment="mill_clinical_event joined with pacs_requests",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def all_pacs_ref_nbr():
    df = spark.sql("""
        SELECT DISTINCT
            MillAccessionNbr AS RefNbr,
            'mill_clinical_event' AS SrcTable,
            MillExamCode AS ExamCode,
            MillPersonId AS MillPersonId,
            PacsPatientId,
            MillEventDate AS ExamDate
        FROM LIVE.intmd_mill_clinical_event_pacs

        UNION ALL
        SELECT DISTINCT
            RequestIdString AS RefNbr,
            'pacs_requests' AS SrcTable,
            RequestExamCode_t AS ExamCode,
            MillPersonId_t AS MillPersonId,
            RequestPatientId,
            ExamDate
        FROM LIVE.intmd_pacs_requestexam

        UNION ALL
        SELECT DISTINCT
            ExamRefNbr AS RefNbr,
            'pacs_examinations' AS SrcTable,
            ExaminationCode AS ExamCode,
            MillPersonId_t AS MillPersonId,
            ExaminationPatientId,
            ExaminationDate
        FROM LIVE.intmd_pacs_examinations
            
    """)
    return df

# COMMAND ----------

@dlt.table(
    name="stag_extracted_accession_nbr",
    comment="Accession number successfully extracted from PACS",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def stag_extracted_accession_nbr():
    files = dbutils.fs.ls("/Volumes/4_prod/pacs/base/extracted_accession_numbers/")
    
    for i, f in enumerate(files):
        if i == 0:
            df = spark.read.format("csv").option("header", "true").load(f.path).select(F.col("AccessionNbr"))
        else:
            df = df.union(spark.read.format("csv").option("header", "true").load(f.path).select(F.col("AccessionNbr")))

    return df

# COMMAND ----------

@dlt.table(
    name="stag_requested_accession_nbr",
    comment="Accession number requested from PACS",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def stag_requested_accession_nbr():
    files = dbutils.fs.ls("/Volumes/4_prod/pacs/base/requested_accession_numbers/")
    
    for i, f in enumerate(files):
        if i == 0:
            df = spark.read.format("csv").option("header", "true").load(f.path).select(F.col("Accession Number").alias("AccessionNbr"))
        else:
            df = df.union(spark.read.format("csv").option("header", "true").load(f.path).select(F.col("Accession Number").alias("AccessionNbr")))

    return df

# COMMAND ----------

@dlt.table(
    name="all_pacs_ref_stat",
    comment="mill_clinical_event joined with pacs_requests",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def all_pacs_ref_stat():
    df = spark.sql("""
        WITH m AS (
            SELECT DISTINCT
                MillAccessionNbr AS RefNbr,
                'mill_clinical_event' AS SrcTable
            FROM LIVE.intmd_mill_clinical_event_pacs
        ),
        r AS (
            SELECT DISTINCT
                RequestIdString AS RefNbr,
                'pacs_requests' AS SrcTable
            FROM LIVE.intmd_pacs_requestexam
        ),
        e AS (
            SELECT DISTINCT
                ExamRefNbr AS RefNbr,
                'pacs_examinations' AS SrcTable
            FROM LIVE.intmd_pacs_examinations
        ),
        full AS (
            SELECT
                COALESCE(m.RefNbr, r.RefNbr, e.RefNbr) AS RefNbr,
                IFF(m.SrcTable IS NULL, 0, 1) AS IsRecordedInMillCE,
                IFF(r.SrcTable IS NULL, 0, 1) AS IsRecordedInPacsRequest,
                IFF(e.SrcTable IS NULL, 0, 1) AS IsRecordedInPacsExam
            FROM m
            FULL OUTER JOIN r
            ON m.RefNbr = r.RefNbr
            FULL OUTER JOIN e
            ON m.RefNbr = e.RefNbr
        ),
        ext AS (
            SELECT DISTINCT
                AccessionNbr AS RefNbr,
                'extracted' AS SrcTable
            FROM LIVE.stag_extracted_accession_nbr
        ),
        ne AS (
            SELECT DISTINCT
                AccessionNbr AS RefNbr,
                'requestedButNotExtracted' AS SrcTable
            FROM LIVE.stag_requested_accession_nbr AS rq
            LEFT JOIN ext
            ON rq.AccessionNbr = ext.RefNbr
            WHERE ext.RefNbr IS NULL

        )
        SELECT
            full.*,
            IFF(ext.SrcTable IS NULL, 0, 1) AS IsExtracted,
            IFF(ne.SrcTable IS NULL, 0, 1) AS IsNotExtracted
        FROM full
        LEFT JOIN ext
        ON full.RefNbr = ext.RefNbr
        LEFT JOIN ne
        ON full.RefNbr = ne.RefNbr
    """)
    return df

# COMMAND ----------

@dlt.table(
    name="joined_pacs_data",
    comment="mill_clinical_event joined with pacs_requests",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def joined_pacs_data():
    df = spark.sql("""
        WITH ce AS (
            SELECT
                MillAccessionNbr AS AccessionNbr,
                Clinical_Event_ID,
                EVENT_ID,
                MillExamCode AS ExamCode,
                ROW_NUMBER() OVER (
                    PARTITION BY MillAccessionNbr, MillExamCode
                    ORDER BY Clinical_Event_Id ASC
                ) AS ExamCodeSeq,
                MillPersonId,
                PacsPatientId,
                MillEventDate
            FROM LIVE.intmd_mill_clinical_event_pacs
            WHERE MillEventClass = 'Radiology'
        ),
        req AS (
            SELECT
                RequestIdString AS AccessionNbr,
                RequestExamCode_t AS ExamCode,
                RequestExamCodeSeq AS ExamCodeSeq,
                RequestId,
                SplitRequestQuestion AS RequestQuestion,
                SplitRequestAnamnesis AS RequestAnamnesis
            FROM LIVE.intmd_pacs_requestexam
        ),
        exa AS (
            SELECT
                ExaminationRequestId,
                ExaminationCode_t AS ExaminationCode,
                ExaminationIdString,
                ROw_NUMBER() OVER (
                    PARTITION BY ExaminationRequestId, ExaminationCode_t
                    ORDER BY ExaminationId ASC
                ) AS ExamCodeSeq
            FROM LIVE.intmd_pacs_examinations
            WHERE ExaminationRequestId IS NOT NULL
        ),
        uni AS (
            SELECT
                AccessionNbr,
                ExamCode,
                ExamCodeSeq
            FROM ce

            UNION

            SELECT
                AccessionNbr,
                ExamCode,
                ExamCodeSeq
            FROM req
            WHERE ExamCode IS NOT NULL
        )
        SELECT
            u.AccessionNbr,
            exa.ExaminationIdString,
            u.ExamCode,
            u.ExamCodeSeq,
            ce.Clinical_Event_ID,
            ce.EVENT_ID,
            ce.MillPersonId,
            req.RequestId,
            req.RequestQuestion,
            req.RequestAnamnesis
        FROM (SELECT DISTINCT * FROM uni) AS u
        LEFT JOIN ce
        ON 
            u.AccessionNbr = ce.AccessionNbr
            AND (
                u.ExamCode = ce.ExamCode OR
                (
                    u.ExamCode IS NULL AND ce.ExamCode IS NULL
                )
            )
            AND u.ExamCodeSeq = ce.ExamCodeSeq
        LEFT JOIN req
        ON
            u.AccessionNbr = req.AccessionNbr
            AND (
                u.ExamCode = req.ExamCode OR req.ExamCode IS NULL
            )
            AND u.ExamCodeSeq = req.ExamCodeSeq
        LEFT JOIN exa
        ON
            exa.ExaminationRequestId = req.RequestId
            AND (
                exa.ExaminationCode = req.ExamCode OR req.ExamCode IS NULL
            )
            AND exa.ExamCodeSeq = req.ExamCodeSeq

    """)
    return df

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType

schema = StructType([
    StructField(
        "AccessionNbr", StringType(), True,
        {'comment': "PACS Accession number for retrieving image data and linking records across systems."}
    ),
    StructField(
        "Clinical_Event_ID", LongType(), True,
        {'comment': "Identifiers to link with clinical event records in the Millenium EHR system."}
    ),
    StructField(
        "EVENT_ID", LongType(), True,
        {'comment': "Identifiers to link with event records in the Millenium EHR system."}
    ),
    StructField(
        "ENCNTR_ID", LongType(), True,
        {'comment': "Identifiers to link with encounter records in the Millenium EHR system."}
    ),
    StructField(
        "PARENT_EVENT_ID", LongType(), True,
        {'comment': "Identifiers to link with parent event records in the Millenium EHR system."}
    ),
    StructField(
        "ExamCode", StringType(), True,
        {'comment': "Standardized short code representing the type of imaging examination or procedure performed."}
    ),
    StructField(
        "ExamCodeSeq", StringType(), True,
        {'comment': "Sequence number or index for the exam code, distinguishing multiple exams of the same type within a single accession or event."}
    ),
    StructField(
        "MillRefNbr", StringType(), True,
        {'comment': "Reference number recorded in Mill_Clinical_Event, often a concatenation of accession and exam code, used for cross-referencing and traceability."}
    ),
    StructField(
        "PersonId", LongType(), True,
        {'comment': "Identifiers for patients in the Millennium EHR system, enabling linkage to demographic and clinical data."}
    ),
    StructField(
        "PacsPatientId", LongType(), True,
        {'comment': "Unique patient identifier in the PACS system."}
    ),
    StructField(
        "MillEventDate", TimestampType(), True,
        {'comment': "Timestamp of the imaging examination event in the Millennium EHR system, typically representing when the clinical event or imaging procedure occurred."}
    ),
    StructField(
        "RequestId", LongType(), True,
        {'comment': "Unique identifier for the imaging request in the PACS."}
    ),
    StructField(
        "RequestQuestion", StringType(), True,
        {'comment': "Text of the clinical question or reason for the imaging request, providing context for the examination."}
    ),
    StructField(
        "RequestAnamnesis", StringType(), True,
        {'comment': "Detailed patient history or clinical background provided with the imaging request, supporting diagnostic interpretation."}
    ),
    StructField(
        "ExaminationIdString", StringType(), True,
        {'comment': "String representation of the examination ID, uniquely identifying a specific imaging examination instance."}
    ),
    StructField(
        "ExaminationStudyUid", StringType(), True,
        {'comment': "Unique identifier (UID) for the imaging study, typically a DICOM Study Instance UID, used for interoperability and image retrieval."}
    ),
    StructField(
        "ExaminationDescription", StringType(), True,
        {'comment': "Free-text or standardized description of the examination, summarizing the procedure or body part imaged."}
    ),
    StructField(
        "ExaminationModality", StringType(), True,
        {'comment': "Type of imaging modality used (e.g., CT, MR, US), indicating the technology or equipment for the examination."}
    ),
    StructField(
        "ExaminationBodyPart", StringType(), True,
        {'comment': "Anatomical region or body part examined during the imaging procedure, aiding in clinical context and reporting."}
    ),
    StructField(
        "Laterality", StringType(), True,
        {'comment': "Laterality of examined body part."}
    ),
    StructField(
        "Mrn", StringType(), True,
        {'comment': "Patient MRN identifier."}
    ),
    StructField(
        "NhsNumber", StringType(), True,
        {'comment': "Patient NHS number."}
    )

])

# COMMAND ----------

@dlt.table(
    name="barts_imaging_metadata",
    comment="Medical imaging metadata available at Barts Health NHS Trust.",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    },
    schema=schema
)
def mill_pacs_data_expanded():
    df = spark.sql("""
        WITH ce AS (
            SELECT
                MillAccessionNbr AS AccessionNbr,
                Clinical_Event_ID,
                EVENT_ID,
                ENCNTR_ID,
                PARENT_EVENT_ID,
                MillExamCode AS ExamCode,
                REPLACE(MillRefNbr, CONCAT(MillAccessionNbr, ExamCode), '')  AS ExamCodeSeq,
                MillRefNbr,
                MillPersonId AS PersonId,
                CAST(PacsPatientId AS BIGINT) AS PacsPatientId,
                MillEventDate
            FROM LIVE.intmd_mill_clinical_event_pacs
            WHERE MillEventClass = 'Radiology'
        ),
        req AS (
            SELECT
                RequestIdString AS AccessionNbr,
                RequestExamCode_t AS ExamCode,
                RequestExamCodeSeq AS ExamCodeSeq,
                CAST(MAX(RequestId) AS BIGINT) AS RequestId,
                MAX(SplitRequestQuestion) AS RequestQuestion,
                MAX(SplitRequestAnamnesis) AS RequestAnamnesis
            FROM LIVE.intmd_pacs_requestexam
            GROUP BY AccessionNbr, ExamCode, ExamCodeSeq
        ),
        exa AS (
            SELECT
                ExaminationRequestId,
                ExaminationCode_t AS ExaminationCode,
                ExaminationIdString,
                ExaminationDescription,
                COALESCE(ExaminationModality, ec.ExamModality) AS ExaminationModality,
                COALESCE(ExaminationBodyPart, ec.ExamBodyPart) AS ExaminationBodyPart,
                ExaminationStudyUid,
                ROW_NUMBER() OVER (
                    PARTITION BY ExaminationRequestId, ExaminationCode_t
                    ORDER BY ExaminationId ASC
                ) AS ExamCodeSeq
            FROM LIVE.intmd_pacs_examinations AS e
            LEFT JOIN LIVE.intmd_pacs_examcode AS ec
            ON e.ExaminationCode_t = ec.RawExamCode
            WHERE ExaminationRequestId IS NOT NULL
        ),
        ali AS (
            SELECT
                MillPersonId,
                MAX(Mrn) AS Mrn,
                MAX(NhsNumber) AS NhsNumber
            FROM LIVE.intmd_pacs_patient_alias
            GROUP BY MillPersonId
        )
        --,
        --rep AS (
        --    SELECT
        --        MillAccessionNbr,
        --        COUNT(DISTINCT CE_BLOB_CONTENT_KEY) AS PacsReportCount
        --    FROM LIVE.pacs_blob_content
        --    GROUP BY MillAccessionNbr
        --)
        SELECT
            ce.*,
            req.RequestId,
            req.RequestQuestion,
            req.RequestAnamnesis,
            exa.ExaminationIdString,
            exa.ExaminationStudyUid,
            exa.ExaminationDescription, -- Can use Mill Event Title Text instead
            exa.ExaminationModality,
            exa.ExaminationBodyPart,
            REPLACE(dc.laterality, ' (qualifier value)', '') AS laterality,
            ali.Mrn,
            ali.NhsNumber
        FROM ce
        LEFT JOIN req
        ON
            ce.AccessionNbr = req.AccessionNbr
            AND (ce.ExamCode = req.ExamCode OR req.ExamCode IS NULL)
            AND ce.ExamCodeSeq = req.ExamCodeSeq
        LEFT JOIN exa
        ON 
            exa.ExaminationRequestId = req.RequestId
            AND exa.ExaminationCode = ce.ExamCode
            AND exa.ExamCodeSeq = ce.ExamCodeSeq
        LEFT JOIN LIVE.pacs_examcode_dict AS dc
        ON ce.ExamCode = dc.short_code
        LEFT JOIN ali
        ON
            ce.PersonId = ali.MillPersonId
    """)

    return df

# COMMAND ----------


schema = StructType([
    StructField(
        "PersonId", LongType(), True,
        {'comment': "Identifiers for patients in the Millennium EHR system, enabling linkage to demographic and clinical data."}
    ), 
    StructField(
        "ReportEventId", LongType(), True,
        {'comment': "Identifiers to link with event records in the Millenium EHR system."}
    ),
    StructField(
        "ReportEncntrId", LongType(), True,
        {'comment': "Identifiers to link with encounter records in the Millenium EHR system."}
    ),
    StructField(
        "AccessionNbr", StringType(), True,
        {'comment': "PACS Accession number for retrieving image data and linking records across systems."}
    ),
    StructField(
        "ExamCode", StringType(), True,
        {'comment': "Standardized short code representing the type of imaging examination or procedure performed."}
    ),
     StructField(
        "Mrn", StringType(), True,
        {'comment': "Patient MRN identifier."}
    ),
    StructField(
        "Nhs_Number", StringType(), True,
        {'comment': "Patient NHS number."}
    ),
    StructField(
        "BlobContents", StringType(), True,
        {'comment': "Imaging report text."}
    ),
    StructField(
        "AnonymizedText", StringType(), True,
        {'comment': "Anonymized imaging report text."}
    )

])

# COMMAND ----------

@dlt.table(
    name="imaging_report",
    comment="mill_clinical_event joined with pacs_requests",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    },
    schema=schema
)
def mill_pacs_data_expanded_report():
    df = spark.sql("""
        SELECT DISTINCT
            b.PERSON_ID AS PersonId,
            b.EVENTID AS ReportEventId,
            b.ENCNTR_ID AS ReportEncntrId,
            c.MillAccessionNbr AS AccessionNbr,
            c.MillExamCode AS ExamCode,
            b.Mrn,
            b.Nhs_Number,
            NULL AS BlobContents, -- placeholder: fetch data from rde_blobdataset
            NULL AS AnonymizedText -- placeholder: fetch data from rde_blobdataset
        FROM 4_prod.rde.rde_blobdataset AS b
        INNER JOIN LIVE.intmd_mill_clinical_event_pacs AS c
        ON b.EVENTID = c.EVENT_ID AND b.PERSON_ID = c.MillPersonId
        WHERE
            b.MainEventDesc = 'RADRPT'
            AND c.MillEventClass = 'Document'
    """)
    return df

# COMMAND ----------

@dlt.table(
    name="mill_pacs_data_expanded_quality",
    comment="mill_clinical_event joined with pacs_requests",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true"
    }
)
def mill_pacs_data_expanded_quality():
    df = spark.sql("""
        SELECT
            SUM(CAST(ISNULL(RequestId) AS INT))/COUNT(*) AS RequestIdNullPercentage,
            SUM(CAST(ISNULL(ExaminationIdString) AS INT))/COUNT(*) AS ExaminationIdNullPercentage
        FROM LIVE.barts_imaging_metadata
    """)
    return df
