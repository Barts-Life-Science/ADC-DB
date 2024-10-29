# Databricks notebook source
# MAGIC %sql
# MAGIC -- This query decodes the contributor system cd in mill_clinical_event
# MAGIC -- CODE_VALUE = 6141416 : BLT_TIE_RAD
# MAGIC
# MAGIC SELECT cv.*
# MAGIC FROM 3_lookup.mill.mill_code_value AS cv
# MAGIC INNER JOIN (SELECT DISTINCT CONTRIBUTOR_SYSTEM_CD FROM 4_prod.raw.mill_clinical_event) AS ce
# MAGIC ON cv.CODE_VALUE = ce.CONTRIBUTOR_SYSTEM_CD
# MAGIC --WHERE DESCRIPTION ILIKE '%Calcifi%'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 4_prod.raw.mill_encounter AS en
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 4_prod.raw.mill_clinical_event AS ce
# MAGIC INNER JOIN 4_prod.raw.pacs_examinations AS pex
# MAGIC ON LOWER(ce.Reference_nbr) = LOWER(pex.examinationaccessionnumber)
# MAGIC WHERE ce.CONTRIBUTOR_SYSTEM_CD = '6141416' -- BLT_TIE_RAD
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 4_prod.raw.mill_clinical_event AS ce
# MAGIC WHERE ce.CONTRIBUTOR_SYSTEM_CD = '6141416' -- BLT_TIE_RAD
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 4_prod.raw.pacs_Examinations
# MAGIC WHERE ExaminationAccessionNumber != 'VALUE_TOO_LONG'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- was not able to get a match using examination accession num from Sectra
# MAGIC
# MAGIC SELECT * FROM 4_prod.raw.mill_clinical_event
# MAGIC WHERE Reference_Nbr ILIKE '%{redacted_examinationaccessionnumber}%'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT p.PERSON_ID, ce.* FROM 4_prod.raw.mill_clinical_event AS ce
# MAGIC INNER JOIN (SELECT DISTINCT ENCNTR_ID, PERSON_ID FROM 4_prod.raw.mill_encounter) AS en
# MAGIC ON ce.ENCNTR_ID = en.ENCNTR_ID
# MAGIC INNER JOIN (SELECT DISTINCT PERSON_ID FROM 6_mgmt.cohorts.pdac009) AS p
# MAGIC ON en.PERSON_ID = p.PERSON_ID
# MAGIC WHERE 
# MAGIC ce.CONTRIBUTOR_SYSTEM_CD = '6141416' -- BLT_TIE_RAD
# MAGIC AND EVENT_TITLE_TEXT ILIKE '%CT%'
# MAGIC AND EVENT_START_DT_TM BETWEEN '2010-01-01' AND '2024-01-25'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(DISTINCT REFERENCE_NBR) 
# MAGIC FROM 4_prod.raw.mill_clinical_event AS ce
# MAGIC INNER JOIN (
# MAGIC   SELECT DISTINCT ENCNTR_ID, PERSON_ID 
# MAGIC   FROM 4_prod.raw.mill_encounter) AS en
# MAGIC ON ce.ENCNTR_ID = en.ENCNTR_ID
# MAGIC INNER JOIN (
# MAGIC   SELECT DISTINCT PERSON_ID 
# MAGIC   FROM 6_mgmt.cohorts.pdac009) AS p
# MAGIC ON en.PERSON_ID = p.PERSON_ID
# MAGIC INNER JOIN (
# MAGIC   SELECT DISTINCT ExaminationAccessionNumber AS access_nbr
# MAGIC   FROM 4_prod.raw.pacs_Examinations
# MAGIC ) AS pex
# MAGIC ON LOWER(pex.access_nbr) = LOWER(ce.reference_nbr)
# MAGIC WHERE 
# MAGIC ce.CONTRIBUTOR_SYSTEM_CD = '6141416' -- BLT_TIE_RAD
# MAGIC AND EVENT_TITLE_TEXT ILIKE 'CT%'
# MAGIC AND EVENT_START_DT_TM BETWEEN '2010-01-01' AND '2024-01-25'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 4_prod.raw.mill_clinical_event
# MAGIC WHERE 
# MAGIC CONTRIBUTOR_SYSTEM_CD = '6141416' -- BLT_TIE_RAD
# MAGIC AND EVENT_TITLE_TEXT ILIKE '%calcifi%'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 4_prod.raw.mill_clinical_event
# MAGIC WHERE 
# MAGIC CONTRIBUTOR_SYSTEM_CD = '6141416' -- BLT_TIE_RAD
# MAGIC AND EVENT_TITLE_TEXT ILIKE '%calcifi%'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 4_prod.raw.mill_clinical_event
# MAGIC WHERE 
# MAGIC CONTRIBUTOR_SYSTEM_CD = '6141416' -- BLT_TIE_RAD
# MAGIC AND EVENT_TITLE_TEXT ILIKE '%leison%'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 4_prod.raw.mill_clinical_event
# MAGIC WHERE 
# MAGIC CONTRIBUTOR_SYSTEM_CD = '6141416' -- BLT_TIE_RAD
# MAGIC AND EVENT_TITLE_TEXT ILIKE '%density%'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 4_prod.raw.mill_clinical_event
# MAGIC WHERE 
# MAGIC --CONTRIBUTOR_SYSTEM_CD = '6141416' -- BLT_TIE_RAD
# MAGIC EVENT_TITLE_TEXT ILIKE '%BI-RAD%'
# MAGIC LIMIT 10
