# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Staging

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   'Total count' AS item,
# MAGIC   'Total' AS tag,
# MAGIC   COUNT(*) AS value
# MAGIC FROM 4_prod.pacs.stag_pacs_examinations
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Missing ExaminationDate' AS item,
# MAGIC   'Date' AS tag,
# MAGIC   1-COUNT(ExaminationDate)/COUNT(*) AS value
# MAGIC FROM 4_prod.pacs.stag_pacs_examinations
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Missing ExaminationScheduledDate' AS item,
# MAGIC   'Date' AS tag,
# MAGIC   1-COUNT(ExaminationScheduledDate)/COUNT(*) AS value
# MAGIC FROM 4_prod.pacs.stag_pacs_examinations
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Missing ExaminationPatientId' AS item,
# MAGIC   'PatientId' AS tag,
# MAGIC   1-COUNT(ExaminationPatientId)/COUNT(*) AS value
# MAGIC FROM 4_prod.pacs.stag_pacs_examinations
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Missing ExaminationCode' AS item,
# MAGIC   'ExamCode' AS tag,
# MAGIC   1-COUNT(ExaminationCode)/COUNT(*) AS value
# MAGIC FROM 4_prod.pacs.stag_pacs_examinations
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Missing ExamRefNbr' AS item,
# MAGIC   'ExamNbr' AS tag,
# MAGIC   1-COUNT(ExamRefNbr)/COUNT(*) AS value
# MAGIC FROM 4_prod.pacs.stag_pacs_examinations
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Missing ExaminationAccessionNumber' AS item,
# MAGIC   'ExamNbr' AS tag,
# MAGIC   (
# MAGIC     1-COUNT(ExaminationAccessionNumber)/(SELECT COUNT(*) FROM 4_prod.pacs.stag_pacs_examinations)
# MAGIC   ) AS value
# MAGIC FROM 4_prod.pacs.stag_pacs_examinations
# MAGIC WHERE ExaminationAccessionNumber != 'VALUE_TOO_LONG'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   'Total count' AS item,
# MAGIC   'Total' AS tag,
# MAGIC   COUNT(*) AS value
# MAGIC FROM 4_prod.pacs.stag_mill_clinical_event_pacs
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Missing EVENT_START_DT_TM' AS item,
# MAGIC   'Date' AS tag,
# MAGIC   1-COUNT(EVENT_START_DT_TM)/(SELECT COUNT(*) FROM 4_prod.pacs.stag_mill_clinical_event_pacs) AS value
# MAGIC FROM 4_prod.pacs.stag_mill_clinical_event_pacs AS m
# MAGIC WHERE EVENT_START_DT_TM > '2000-01-01'
# MAGIC
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Missing PERFORMED_DT_TM' AS item,
# MAGIC   'Date' AS tag,
# MAGIC   1-COUNT(PERFORMED_DT_TM)/COUNT(*) AS value
# MAGIC FROM 4_prod.pacs.stag_mill_clinical_event_pacs
# MAGIC
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Missing MillEventDate' AS item,
# MAGIC   'Date' AS tag,
# MAGIC   1-COUNT(MillEventDate)/COUNT(*) AS value
# MAGIC FROM 4_prod.pacs.stag_mill_clinical_event_pacs
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC   'Missing PERSON_ID' AS item,
# MAGIC   'PatientId' AS tag,
# MAGIC   1-COUNT(PERSON_ID)/COUNT(*) AS value
# MAGIC FROM 4_prod.pacs.stag_mill_clinical_event_pacs
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Missing MillAccessionNbr' AS item,
# MAGIC   'ExamNbr' AS tag,
# MAGIC   1-COUNT(MillAccessionNbr)/COUNT(*) AS value
# MAGIC FROM 4_prod.pacs.stag_mill_clinical_event_pacs
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Missing MillExamCode' AS item,
# MAGIC   'ExamCode' AS tag,
# MAGIC   1-COUNT(MillExamCode)/COUNT(*) AS value
# MAGIC FROM 4_prod.pacs.stag_mill_clinical_event_pacs

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   'Total count' AS item,
# MAGIC   'Total' AS tag,
# MAGIC   COUNT(*) AS value
# MAGIC FROM 4_prod.raw.pacs_requests
# MAGIC WHERE ADC_Deleted IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Missing RequestPatientId' AS item,
# MAGIC   'PatientId' AS tag,
# MAGIC   1-COUNT(RequestPatientId)/COUNT(*) AS value
# MAGIC FROM 4_prod.raw.pacs_requests
# MAGIC WHERE ADC_Deleted IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Missing RequestIdString' AS item,
# MAGIC   'ExamRefNbr' AS tag,
# MAGIC   1-COUNT(RequestIdString)/COUNT(*) AS value
# MAGIC FROM 4_prod.raw.pacs_requests
# MAGIC WHERE ADC_Deleted IS NULL
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   'Total count' AS item,
# MAGIC   'Total' AS tag,
# MAGIC   COUNT(*) AS value
# MAGIC FROM 4_prod.pacs.stag_pacs_requestquestion
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Missing RequestQuestionExamCode' AS item,
# MAGIC   'ExamCode' AS tag,
# MAGIC   1-COUNT(RequestQuestionExamCode)/(SELECT COUNT(*) FROM 4_prod.pacs.stag_pacs_requestquestion WHERE LENGTH(RequestQuestion)>0) AS value
# MAGIC FROM 4_prod.pacs.stag_pacs_requestquestion
# MAGIC WHERE LENGTH(RequestQuestionExamCode) > 0 
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'RequestId in pacs_requests but not in stag_pacs_requestquestion' AS item,
# MAGIC   'Id' AS tag,
# MAGIC   1-COUNT(DISTINCT RequestId)/(SELECT COUNT(DISTINCT RequestId) FROM 4_prod.raw.pacs_requests WHERE ADC_Deleted IS NULL)  AS value
# MAGIC FROM 4_prod.pacs.stag_pacs_requestquestion
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Missing RequestQuestion' AS item,
# MAGIC   'Input' AS tag,
# MAGIC   1-COUNT(RequestQuestion)/(SELECT COUNT(*) FROM 4_prod.raw.pacs_requests) AS value
# MAGIC FROM 4_prod.pacs.stag_pacs_requestquestion
# MAGIC WHERE LENGTH(RequestQuestion) > 0

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Intermediate

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   'Total count' AS item,
# MAGIC   'Total' AS tag,
# MAGIC   COUNT(*) AS value
# MAGIC FROM 4_prod.pacs.intmd_pacs_requestexam
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Missing MillPersonId' AS item,
# MAGIC   'PatientId' AS tag,
# MAGIC   1-COUNT(MillPersonId)/COUNT(*) AS value
# MAGIC FROM 4_prod.pacs.intmd_pacs_requestexam
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Missing MillPersonId_t' AS item,
# MAGIC   'PatientId' AS tag,
# MAGIC   1-COUNT(MillPersonId_t)/COUNT(*) AS value
# MAGIC FROM 4_prod.pacs.intmd_pacs_requestexam
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'RequestId in pacs_requests but not in intmd_pacs_requestexam' AS item,
# MAGIC   'Id' AS tag,
# MAGIC   1-COUNT(DISTINCT RequestId)/(SELECT COUNT(DISTINCT RequestId) FROM 4_prod.raw.pacs_requests WHERE ADC_Deleted IS NULL)  AS value
# MAGIC FROM 4_prod.pacs.intmd_pacs_requestexam
# MAGIC
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Missing MillEventDate' AS item,
# MAGIC   'ExamDate' AS tag,
# MAGIC   1-COUNT(MillEventDate)/COUNT(*)  AS value
# MAGIC FROM 4_prod.pacs.intmd_pacs_requestexam
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Missing ExaminationDate' AS item,
# MAGIC   'ExamDate' AS tag,
# MAGIC   1-COUNT(ExaminationDate)/COUNT(*)  AS value
# MAGIC FROM 4_prod.pacs.intmd_pacs_requestexam
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Missing ExamDate' AS item,
# MAGIC   'ExamDate' AS tag,
# MAGIC   1-COUNT(ExamDate)/COUNT(*)  AS value
# MAGIC FROM 4_prod.pacs.intmd_pacs_requestexam
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Missing ExamCode' AS item,
# MAGIC   'ExamCode' AS tag,
# MAGIC   1-COUNT(RequestExamCode)/COUNT(*)  AS value
# MAGIC FROM 4_prod.pacs.intmd_pacs_requestexam
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Missing ExamCode_t' AS item,
# MAGIC   'ExamCode' AS tag,
# MAGIC   1-COUNT(RequestExamCode_t)/COUNT(*)  AS value
# MAGIC FROM 4_prod.pacs.intmd_pacs_requestexam
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Space char in ExamCode_t' AS item,
# MAGIC   'ExamCode' AS tag,
# MAGIC   COUNT(RequestExamCode_t)/15897572  AS value
# MAGIC FROM 4_prod.pacs.intmd_pacs_requestexam
# MAGIC WHERE RequestExamCode_t LIKE '% %'
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Missing ExaminationId' AS item,
# MAGIC   'ExamCode' AS tag,
# MAGIC   1-COUNT(ExaminationId)/COUNT(*)  AS value
# MAGIC FROM 4_prod.pacs.intmd_pacs_requestexam
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Missing MillClinicalEventId' AS item,
# MAGIC   'ExamCode' AS tag,
# MAGIC   1-COUNT(MillClinicalEventId)/COUNT(*)  AS value
# MAGIC FROM 4_prod.pacs.intmd_pacs_requestexam
# MAGIC
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Missing ReportId' AS item,
# MAGIC   'ExamCode' AS tag,
# MAGIC   1-COUNT(ReportId)/COUNT(*)  AS value
# MAGIC FROM 4_prod.pacs.intmd_pacs_requestexam

# COMMAND ----------



# COMMAND ----------


total = spark.sql("""
    SELECT COUNT(*) AS total
    FROM 4_prod.pacs.intmd_pacs_examinations
""").collect()[0]["total"]
print("total count:", total)

df = spark.sql(f"""
    SELECT
        'Missing ExaminationDate' AS item,
        1-COUNT(ExaminationDate)/{total} AS value
    FROM 4_prod.pacs.intmd_pacs_examinations

    UNION ALL

    SELECT
        'Missing MillPersonId' AS item,
        1-COUNT(MillPersonId)/{total} AS value
    FROM 4_prod.pacs.intmd_pacs_examinations

    UNION ALL

    SELECT
        'Missing MillPersonId after 2014' AS item,
        1-COUNT(MillPersonId)/COUNT(*) AS value
    FROM 4_prod.pacs.intmd_pacs_examinations
    WHERE ExaminationDate > '2014-01-01'

    UNION ALL

    SELECT
        'Missing MillPersonId_t' AS item,
        1-COUNT(MillPersonId_t)/{total} AS value
    FROM 4_prod.pacs.intmd_pacs_examinations

    UNION ALL

    SELECT
        'Missing ExaminationCode' AS item,
        1-COUNT(ExaminationCode)/{total} AS value
    FROM 4_prod.pacs.intmd_pacs_examinations


    UNION ALL

    SELECT
        'Space char in ExaminationCode' AS item,
        COUNT(ExaminationCode)/{total} AS value
    FROM 4_prod.pacs.intmd_pacs_examinations
    WHERE ExaminationCode LIKE '% %'
""")

display(df)

# COMMAND ----------


table = "4_prod.pacs.intmd_mill_clinical_event_pacs"

total = spark.sql(f"""
    SELECT COUNT(*) AS total
    FROM {table}
""").collect()[0]["total"]
print("total count:", total)

df = spark.sql(f"""
    SELECT
        'Missing MillPersonId' AS item,
        1-COUNT(MillPersonId)/{total} AS value
    FROM {table}

""")

display(df)

# COMMAND ----------


table = "4_prod.pacs.intmd_pacs_patient_alias"

total = spark.sql(f"""
    SELECT COUNT(*) AS total
    FROM {table}
""").collect()[0]["total"]
print("total count:", total)

df = spark.sql(f"""
    SELECT
        'Missing MillPersonId' AS item,
        1-COUNT(MillPersonId)/COUNT(*) AS value
    FROM {table}

    UNION ALL

    SELECT
        'Missing MillPersonId_t' AS item,
        1-COUNT(MillPersonId_t)/COUNT(*) AS value
    FROM {table}

    UNION ALL


    SELECT
        'Missing MillPersonId after 2014' AS item,
        1-COUNT(MillPersonId)/COUNT(*) AS value
    FROM {table}
    WHERE LatestExamDate > '2014-01-01'

    UNION ALL


    SELECT
        'Missing MillPersonId_t after 2014' AS item,
        1-COUNT(MillPersonId_t)/COUNT(*) AS value
    FROM {table}
    WHERE LatestExamDate >'2014-01-01'

    
    UNION ALL


    SELECT
        'PaitentIdCouldBePersonId' AS item,
        COUNT(*)/{total} AS value
    FROM {table}
    WHERE PatientIdCouldBePersonId = TRUE

""")

display(df)

# COMMAND ----------



# COMMAND ----------


table = "4_prod.pacs.all_pacs_ref_nbr"

total = spark.sql(f"""
    SELECT COUNT(*) AS total
    FROM {table}
""").collect()[0]["total"]
print("total count:", total)

df = spark.sql(f"""
    SELECT
        'Missing MillPersonId' AS item,
        1-COUNT(MillPersonId)/{total} AS value
    FROM {table}


    UNION ALL


    SELECT
        'Missing ExamDate' AS item,
        1-COUNT(ExamDate)/{total} AS value
    FROM {table}

    UNION ALL

    SELECT
        'Missing MillPersonId after 2014' AS item,
        1-COUNT(MillPersonId)/COUNT(*) AS value
    FROM {table}
    WHERE ExamDate > '2014-01-01'

    UNION ALL

    SELECT
        'Missing MillPersonId before 2014' AS item,
        1-COUNT(MillPersonId)/COUNT(*) AS value
    FROM {table}
    WHERE ExamDate < '2014-01-01'


    UNION ALL

    SELECT
        'Missing ExamCode' AS item,
        1- COUNT(ExamCode)/{total} AS value
    FROM {table}
    WHERE ExamCode IS NOT NULL

""")

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(DISTINCT MillPersonId)
# MAGIC FROM 4_prod.pacs.intmd_mill_clinical_event_pacs
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT COUNT(DISTINCT MillPersonId)
# MAGIC FROM 4_prod.pacs.intmd_mill_clinical_event_pacs
# MAGIC WHERE PacsPatientId IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.intmd_pacs_requestexam
# MAGIC WHERE ExamDate > '2022-01-01'
# MAGIC AND MillPersonId IS NULL
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.intmd_pacs_requestexam
# MAGIC WHERE requestexamcode_t NOT RLIKE '^[a-zA-Z0-9]{4,8}$' AND MillExamCode IS NULL
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ce AS (
# MAGIC   SELECT
# MAGIC     EVENT_TITLE_TEXT,
# MAGIC     MAX(MillExamCode) AS MillExamCode
# MAGIC   FROM 4_prod.pacs.stag_mill_clinical_event_pacs
# MAGIC   GROUP BY EVENT_TITLE_TEXT
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.stag_pacs_requestquestion AS rq
# MAGIC LEFT JOIN ce
# MAGIC ON rq.RequestQuestionExamCOde = ce.EVENT_TITLE_TEXT
# MAGIC WHERE --requestquestionexamcode NOT RLIKE '^[a-zA-Z0-9]{4,8}$'
# MAGIC ce.EVENT_TITLE_TEXT IS NOT NULL
# MAGIC LIMIT 100

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.all_pacs_ref_nbr
# MAGIC WHERE MillPersonId IS NULL
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.intmd_pacs_examcode
# MAGIC WHERE ExamCode_t LIKE '% %' AND LinkedExamCodeFromPacsExam IS NOT NULL
# MAGIC LIMIT 100
# MAGIC
