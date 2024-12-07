# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT DISTINCT PERSON_ALIAS_TYPE_CD, DISPLAY
# MAGIC FROM 4_prod.raw.mill_person_alias AS a
# MAGIC LEFT JOIN 3_lookup.mill.mill_code_value AS c
# MAGIC ON a.PERSON_ALIAS_TYPE_CD = c.CODE_VALUE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT CONTRIBUTOR_SYSTEM_CD, DISPLAY
# MAGIC FROM 4_prod.raw.mill_clinical_event AS e
# MAGIC LEFT JOIN 3_lookup.mill.mill_code_value AS c
# MAGIC ON e.CONTRIBUTOR_SYSTEM_CD = c.CODE_VALUE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT CONTRIBUTOR_SYSTEM_CD, DISPLAY
# MAGIC FROM 4_prod.raw.mill_encounter AS e
# MAGIC LEFT JOIN 3_lookup.mill.mill_code_value AS c
# MAGIC ON e.CONTRIBUTOR_SYSTEM_CD = c.CODE_VALUE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 4_prod.raw.pacs_examinations
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 4_prod.raw.mill_clinical_event
# MAGIC WHERE CONTRIBUTOR_SYSTEM_CD = 6141416 -- BLT_TIE_RAD
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 4_prod.raw.pacs_Requests AS rq
# MAGIC LEFT JOIN 4_prod.raw.pacs_ExaminationReports AS erp
# MAGIC ON rq.RequestId = erp.ExaminationReportRequestId
# MAGIC WHERE RequestIdString = 'UKWXH--------'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 4_prod.raw.mill_clinical_event
# MAGIC WHERE SERIES_REF_NBR = ''

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY FUNCTION extract_pacs_shortcode(RequestQuestion STRING)
# MAGIC   RETURNS STRING
# MAGIC   RETURN REPLACE(REPLACE(SUBSTRING_INDEX(RequestQuestion, '-', 6), '-', ''), ' ', '')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT extract_pacs_shortcode(RequestQuestion) AS pacs_shortcode FROM 4_prod.raw.pacs_requests
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 4_prod.pacs.pacs_clinical_event
# MAGIC WHERE RequestAccessionNumber = 'UKRLH02009396361'
