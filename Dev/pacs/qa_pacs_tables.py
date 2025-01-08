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
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'RequestId in pacs_requests but not in intmd_pacs_requestexam' AS item,
# MAGIC   'Id' AS tag,
# MAGIC   1-COUNT(DISTINCT RequestId)/(SELECT COUNT(DISTINCT RequestId) FROM 4_prod.raw.pacs_requests WHERE ADC_Deleted IS NULL)  AS value
# MAGIC FROM 4_prod.pacs.intmd_pacs_requestexam
