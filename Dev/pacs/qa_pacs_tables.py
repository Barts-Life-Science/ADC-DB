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
