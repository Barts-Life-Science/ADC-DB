# Databricks notebook source
# MAGIC %sql
# MAGIC -- This is the patient cohort
# MAGIC SELECT * FROM  6_mgmt.cohorts.pdac009 LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the number of patients who had any imaging exams
# MAGIC CREATE TEMPORARY VIEW breast_cancer_patients AS
# MAGIC SELECT p.PatientId, p.PatientPersonalId, c.PERSON_ID, e.ExaminationId
# MAGIC FROM 6_mgmt.cohorts.pdac009 AS c
# MAGIC INNER JOIN 4_prod.raw.mill_person_alias AS a
# MAGIC ON c.PERSON_ID = a.PERSON_ID
# MAGIC INNER JOIN 4_prod.raw.pacs_patients AS p
# MAGIC ON a.ALIAS = p.PatientPersonalId
# MAGIC INNER JOIN 4_prod.raw.pacs_examinations AS e
# MAGIC ON e.ExaminationPatientId = p.PatientId
# MAGIC WHERE a.PERSON_ALIAS_TYPE_CD = 10
# MAGIC AND e.ExaminationDate BETWEEN '2010-01-01' AND '2024-01-25'
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examinations

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT e.* FROM 4_prod.raw.pacs_examinations AS e
# MAGIC INNER JOIN 4_prod.raw.pacs_examinationreports AS er
# MAGIC ON e.examinationid = er.examinationreportexaminationid
# MAGIC LIMIT 10
# MAGIC
# MAGIC -- ExaminationBodyPart and ExaminationDescription contain info about the imaging body part
# MAGIC -- ExaminationModality, ExaminationCode, and ExaminationDescription contain info about the imaging modality

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Guess the meaning of ExaminationStatus
# MAGIC
# MAGIC SELECT ExaminationStatus, COUNT(*)
# MAGIC FROM 4_prod.raw.pacs_examinations AS e
# MAGIC INNER JOIN 4_prod.raw.pacs_examinationreports AS er
# MAGIC ON e.examinationid = er.examinationreportexaminationid
# MAGIC WHERE ExaminationDate BETWEEN '2010-01-01' AND '2024-01-25'
# MAGIC GROUP BY ExaminationStatus

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Examinations - Body Part

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT ExaminationBodyPart
# MAGIC FROM 4_prod.raw.pacs_Examinations

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check NULL values in these two columns
# MAGIC -- ExaminationDescription has a lot fewer NULL values 
# MAGIC
# MAGIC SELECT 'ExaminationBodyPart', COUNT(*)
# MAGIC from 4_prod.raw.pacs_examinations
# MAGIC WHERE ExaminationBodyPart IS NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 'ExaminationDescription', COUNT(*)
# MAGIC FROM 4_prod.raw.pacs_examinations
# MAGIC WHERE ExaminationDescription IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT ExaminationDescription 
# MAGIC from 4_prod.raw.pacs_examinations
# MAGIC WHERE ExaminationDescription LIKE '%breast%' OR ExaminationDescription LIKE '%mammogram%'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT ExaminationCode, ExaminationDescription 
# MAGIC from 4_prod.raw.pacs_examinations
# MAGIC WHERE ExaminationDescription LIKE '%contrast%'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Only 10 distinct ExaminationCode don't have a description
# MAGIC -- These don't seem relevant to breast
# MAGIC SELECT DISTINCT ExaminationCode, ExaminationDescription 
# MAGIC from 4_prod.raw.pacs_examinations
# MAGIC WHERE ExaminationDescription IS NULL

# COMMAND ----------


body_parts = {
    "abdomen":["%abdom%"],
    "axilla":["%axilla%"],
    "bone":["%bone%"],
    "brain":["%brain%"],
    #"head":["%head%"],
    "breast":["%breast%", "%mammogram%"],
    "chest":["%chest%", "%thora%"],
    "liver": ["%liver%"],
    "neck": ["%neck%"],
    "pelvis": ["%pelvis%"],
    "sentinel node": ["%sentinel node%"],
    "spine": ["%spine%", "%spinal%"],
    "whole body": ["%whole body%"]
}

q_template_main = """
SELECT '__KEY__', COUNT(DISTINCT e.ExaminationId)
FROM 4_prod.raw.pacs_examinations AS e
INNER JOIN breast_cancer_patients AS p
ON e.ExaminationId = p.ExaminationId
INNER JOIN 4_prod.raw.pacs_examinationreports AS er
ON e.ExaminationId = er.ExaminationReportExaminationId
WHERE 
"""

q_template_cond = """LOWER(e.ExaminationDescription) LIKE LOWER('__VAL__') OR LOWER(e.ExaminationBodyPart) LIKE LOWER('__VAL__')\n"""

q = ""
for i, key_val in enumerate(body_parts.items()):
    if i > 0:
        q += "\nUNION\n"

    q_cond = ""
    for j, val in enumerate(key_val[1]):
        if j > 0:
            q_cond += " OR "
        
        q_cond +=q_template_cond.replace("__VAL__", val)

    q += q_template_main.replace("__KEY__", key_val[0])+q_cond


print(q)





# COMMAND ----------

tmp_df = spark.sql(q)

display(tmp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Use short code table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT short_code
# MAGIC from 4_prod.raw.pacs_examinations AS e
# MAGIC LEFT JOIN 1_inland.evan_demo.pacs_shortcodes AS sc
# MAGIC ON e.ExaminationCode = sc.short_code OR e.ExaminationCode = sc.short_code + 'C'
# MAGIC WHERE sc.short_code IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- ExaminationCode is not standardised; there could be some weird values.
# MAGIC
# MAGIC SELECT DISTINCT ExaminationCode
# MAGIC from 4_prod.raw.pacs_examinations AS e
# MAGIC LEFT JOIN 1_inland.evan_demo.pacs_shortcodes AS sc
# MAGIC ON e.ExaminationCode = sc.short_code OR e.ExaminationCode = sc.short_code + 'C'
# MAGIC WHERE sc.short_code IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT ExaminationId, RequestId, e.ExaminationModality, e.ExaminationCode, e.ExaminationDescription, r.RequestQuestion
# MAGIC FROM 4_prod.raw.pacs_examinations AS e
# MAGIC INNER JOIN 4_prod.raw.pacs_requests AS r
# MAGIC ON e.ExaminationRequestId = r.RequestId
# MAGIC WHERE ExaminationDate > '2010-01-01'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Examinations - Modalities

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT ExaminationModality FROM 4_prod.raw.pacs_examinations
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check NULL values in these two columns
# MAGIC -- ExaminationDescription has a lot fewer NULL values 
# MAGIC
# MAGIC SELECT 'ExaminationModality', COUNT(*)
# MAGIC from 4_prod.raw.pacs_examinations
# MAGIC WHERE ExaminationModality IS NULL
# MAGIC AND ExaminationStatus=100
# MAGIC AND ExaminationDate BETWEEN '2010-01-01' AND '2024-01-25'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 'ExaminationDescription', COUNT(*)
# MAGIC FROM 4_prod.raw.pacs_examinations
# MAGIC WHERE ExaminationDescription IS NULL
# MAGIC AND ExaminationStatus=100
# MAGIC AND ExaminationDate BETWEEN '2010-01-01' AND '2024-01-25'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT ExaminationDescription
# MAGIC FROM 4_prod.raw.pacs_examinations AS e
# MAGIC WHERE ExaminationDate BETWEEN '2010-01-01' AND '2024-01-25'
# MAGIC AND LOWER(ExaminationDescription) LIKE 'mg%' OR LOWER(ExaminationDescription) LIKE '%mammogram%' 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT ExaminationDescription
# MAGIC FROM 4_prod.raw.pacs_examinations AS e
# MAGIC WHERE ExaminationDate BETWEEN '2010-01-01' AND '2024-01-25'
# MAGIC AND LOWER(ExaminationDescription) LIKE 'us%' OR LOWER(ExaminationDescription) LIKE '%ultraso%' OR LOWER(ExaminationDescription) LIKE '%sonograph%' OR LOWER(ExaminationDescription) LIKE '%echo%'

# COMMAND ----------


modalities = {
    "mammogram":["%mammogram%", "MG%"],
    "ultrasound":["%ultraso%", "US%", "%echo", "%sonograph%"],
    "mri":["% MRI %", 'MR%'],
    "pet":["% pet %", 'PT%'],
    "ct":["ct%"],
    "nuclear medicine":["%nuclear%", "NM%"],
    "x-ray": ["%x ray%", "%x-ray%", "XR%"],
    "tomosynthesis": ["%tomosynthesis%"],
    "contrast": ["%contrast%"]
}

q_template_main = """
SELECT '__KEY__', COUNT(DISTINCT e.ExaminationId)
FROM 4_prod.raw.pacs_examinations AS e
INNER JOIN breast_cancer_patients AS p
ON e.ExaminationId = p.ExaminationId
INNER JOIN 4_prod.raw.pacs_examinationreports AS er
ON e.ExaminationId = er.ExaminationReportExaminationId
WHERE
ExaminationDate BETWEEN '2010-01-01' AND '2024-01-25'
AND (__COND__)
"""

q_template_cond = """LOWER(e.ExaminationDescription) LIKE LOWER('__VAL__') OR LOWER(e.ExaminationModality) LIKE LOWER('__VAL__')\n"""

q = ""
for i, key_val in enumerate(modalities.items()):
    if i > 0:
        q += "\nUNION\n"

    q_cond = ""
    for j, val in enumerate(key_val[1]):
        if j > 0:
            q_cond += " OR "
        
        q_cond +=q_template_cond.replace("__VAL__", val)

    q += q_template_main.replace("__KEY__", key_val[0]).replace("__COND__", q_cond)


print(q)





# COMMAND ----------

tmp_df = spark.sql(q)

display(tmp_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC from 4_prod.raw.pacs_Requests
# MAGIC WHERE RequestQuestion LIKE '%post treatment%'
# MAGIC LIMIT 10

# COMMAND ----------


keywords = {
  "Screening": ["%screen%"],
  "Diagnostic": ["%diagnostic%"],
  "Follow-up": ["%follow up%", "%follow-up%", "%F/up%"],
  "On-treatment": ["%on-treatment%", "%on treatment%", "%on-tx%", "%on tx%"],
  "Post-treatment": ["%post-treatment%", "%post treatment%", "%post-tx%", "%post tx%"]
}

q_template_main = """
SELECT '__KEY__', COUNT(DISTINCT e.ExaminationId)
FROM 4_prod.raw.pacs_examinations AS e
INNER JOIN breast_cancer_patients AS p
ON e.ExaminationId = p.ExaminationId
INNER JOIN 4_prod.raw.pacs_examinationreports AS er
ON e.ExaminationId = er.ExaminationReportExaminationId
LEFT JOIN 4_prod.raw.pacs_requests AS r
ON e.ExaminationRequestId = r.RequestId
WHERE 
ExaminationDate BETWEEN '2010-01-01' AND '2024-01-25'
AND (__COND__)
"""

q_template_cond = """LOWER(e.ExaminationDescription) LIKE LOWER('__VAL__') OR LOWER(r.RequestQuestion) LIKE LOWER('__VAL__')\n"""

q = ""
for i, key_val in enumerate(keywords.items()):
    if i > 0:
        q += "\nUNION\n"

    q_cond = ""
    for j, val in enumerate(key_val[1]):
        if j > 0:
            q_cond += " OR "
        
        q_cond += q_template_cond.replace("__VAL__", val)

    q += q_template_main.replace("__KEY__", key_val[0]).replace("__COND__", q_cond)


print(q)





# COMMAND ----------

tmp_df = spark.sql(q)

display(tmp_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC from 4_prod.raw.pacs_Reports
# MAGIC LIMIT 10

# COMMAND ----------


keywords = {
  "Investigation, Assessment or Planning": ["%investigation%", "%assessment%", "%planning%"],
  "Family History Screening": ["%family history screen%", "%family hx screen%"],
  "Follow-up": ["%follow up%", "%follow-up%", "%F/up%"],
  "High risk": ["high risk%"],
  "Routine Screening": ["%routine screen%"],
  "Staging": ["%Staging%"],
  "Treatment Response": ["%treatment response%", "%tx response%"]
}

q_template_main = """
SELECT '__KEY__', COUNT(DISTINCT e.ExaminationId)
FROM 4_prod.raw.pacs_examinations AS e
INNER JOIN breast_cancer_patients AS p
ON e.ExaminationId = p.ExaminationId
INNER JOIN 4_prod.raw.pacs_examinationreports AS er
ON e.ExaminationId = er.ExaminationReportExaminationId
LEFT JOIN 4_prod.raw.pacs_requests AS r
ON e.ExaminationRequestId = r.RequestId
WHERE 
ExaminationDate BETWEEN '2010-01-01' AND '2024-01-25'
AND (__COND__)
"""

q_template_cond = """LOWER(e.ExaminationDescription) LIKE LOWER('__VAL__') OR LOWER(r.RequestQuestion) LIKE LOWER('__VAL__')\n"""

q = ""
for i, key_val in enumerate(keywords.items()):
    if i > 0:
        q += "\nUNION\n"

    q_cond = ""
    for j, val in enumerate(key_val[1]):
        if j > 0:
            q_cond += " OR "
        
        q_cond += q_template_cond.replace("__VAL__", val)

    q += q_template_main.replace("__KEY__", key_val[0]).replace("__COND__", q_cond)


print(q)





# COMMAND ----------

tmp_df = spark.sql(q)

display(tmp_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 4_prod.raw.pacs_reports AS r
# MAGIC INNER JOIN 4_prod.raw.pacs_examinationreports AS er
# MAGIC ON r.ReportId = er.ExaminationReportReportId
# MAGIC INNER JOIN 4_prod.raw.pacs_examinations AS e
# MAGIC ON er.ExaminationReportExaminationId = e.ExaminationId
# MAGIC INNER JOIN breast_cancer_patients AS bcp
# MAGIC ON e.ExaminationPatientId = bcp.PatientId
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 4_prod.raw.pacs_reports AS r
# MAGIC INNER JOIN 4_prod.raw.pacs_examinationreports AS er
# MAGIC ON r.ReportId = er.ExaminationReportReportId
# MAGIC INNER JOIN 4_prod.raw.pacs_examinations AS e
# MAGIC ON er.ExaminationReportExaminationId = e.ExaminationId
# MAGIC INNER JOIN breast_cancer_patients AS bcp
# MAGIC ON e.ExaminationPatientId = bcp.PatientId
# MAGIC WHERE LOWER(ReportText) LIKE '%partial response%' AND LOWER(ReportText) NOT LIKE '%partial response to treatment%'
# MAGIC LIMIT 10

# COMMAND ----------


keywords = {
  "Partial": ["%partial response%", "%partial treatment response%", "%partial tx response%"],
  "Mixed": ["%mixed response%", "%mixed treatment response%", "%mixed tx response%"],
  "Complete": ["%complete response%", "%complete treatment response%", "%complete tx response%"],
  "Stable disease": ["%stable disease%"],
  "Progressive disease": ["%progressive disease%"],
  "Unknown": ["%unknown response%"]
}

q_template_main = """
SELECT '__KEY__', COUNT(DISTINCT rp.reportid)
FROM 4_prod.raw.pacs_reports AS rp
INNER JOIN 4_prod.raw.pacs_examinationreports AS er
ON rp.ReportId = er.ExaminationReportReportId
INNER JOIN 4_prod.raw.pacs_examinations AS e
ON e.ExaminationId = er.ExaminationReportExaminationId
INNER JOIN breast_cancer_patients AS bcp
ON e.ExaminationId = bcp.ExaminationId
WHERE 
ExaminationDate BETWEEN '2010-01-01' AND '2024-01-25'
AND (__COND__)
"""

q_template_cond = """LOWER(rp.ReportText) LIKE LOWER('__VAL__')\n"""

q = ""
for i, key_val in enumerate(keywords.items()):
    if i > 0:
        q += "\nUNION\n"

    q_cond = ""
    for j, val in enumerate(key_val[1]):
        if j > 0:
            q_cond += " OR "
        
        q_cond += q_template_cond.replace("__VAL__", val)

    q += q_template_main.replace("__KEY__", key_val[0]).replace("__COND__", q_cond)


print(q)





# COMMAND ----------

tmp_df = spark.sql(q)

display(tmp_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(DISTINCT REPORTId) FROM 4_prod.raw.pacs_reports AS r
# MAGIC INNER JOIN 4_prod.raw.pacs_examinationreports AS er
# MAGIC ON r.ReportId = er.ExaminationReportReportId
# MAGIC INNER JOIN 4_prod.raw.pacs_examinations AS e
# MAGIC ON er.ExaminationReportExaminationId = e.ExaminationId
# MAGIC INNER JOIN breast_cancer_patients AS bcp
# MAGIC ON e.ExaminationPatientId = bcp.PatientId
# MAGIC WHERE LOWER(ReportText) LIKE '%bi-rads%' 
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(DISTINCT REPORTId) FROM 4_prod.raw.pacs_reports AS r
# MAGIC INNER JOIN 4_prod.raw.pacs_examinationreports AS er
# MAGIC ON r.ReportId = er.ExaminationReportReportId
# MAGIC INNER JOIN 4_prod.raw.pacs_examinations AS e
# MAGIC ON er.ExaminationReportExaminationId = e.ExaminationId
# MAGIC INNER JOIN breast_cancer_patients AS bcp
# MAGIC ON e.ExaminationPatientId = bcp.PatientId
# MAGIC WHERE LOWER(ReportText) LIKE '%focus%' 
# MAGIC LIMIT 10
