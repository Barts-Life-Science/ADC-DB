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

# MAGIC %md
# MAGIC
# MAGIC ### Examinations - Body Part

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 4_prod.raw.pacs_examinations LIMIT 10
# MAGIC
# MAGIC -- ExaminationBodyPart and ExaminationDescription contain info about the imaging body part

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
# MAGIC SELECT DISTINCT ExaminationCode, ExaminationDescription 
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
    "breast":["%breast%", "%mammogram%"],
    "chest":["%chest%", "%thora%"],
    "liver": ["%liver%"],
    "neck": ["%neck%"],
    "pelvis": ["%pelvis%"],
    "sentinel node": ["%sentinel node%"],
    "spine": ["%spin%"],
    "whole body": ["%whole body%"]
}

q_template_main = """
SELECT '__KEY__', COUNT(DISTINCT e.ExaminationId)
FROM 4_prod.raw.pacs_examinations AS e
INNER JOIN breast_cancer_patients AS p
ON e.ExaminationId = p.ExaminationId
WHERE 
"""

q_template_cond = """e.ExaminationDescription LIKE '__VAL__'\n"""

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


modalities = {
    "mammogram":["%mammogram%"],
    "ultrasound":["%ultrasound%", "US%", "%echo", "%sonograph%"],
    "mri":["%mri%"],
    "pet":["%pet%"],
    "ct":["%ct%"],
    "nuclear medicine":["%nuclear%", "%nm%"],
    "x-ray": ["%x ray%", "%x-ray%"],
    "tomosynthesis": ["%tomosynthesis%"]
}

q_template_main = """
SELECT '__KEY__', COUNT(DISTINCT e.ExaminationId)
FROM 4_prod.raw.pacs_examinations AS e
INNER JOIN breast_cancer_patients AS p
ON e.ExaminationId = p.ExaminationId
WHERE 
"""

q_template_cond = """e.ExaminationDescription LIKE '__VAL__'\n"""

q = ""
for i, key_val in enumerate(modalities.items()):
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

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC from 4_prod.raw.pacs_Requests
# MAGIC WHERE RequestQuestion LIKE '%post treatment%'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC from 4_prod.raw.pacs_Reports
# MAGIC LIMIT 10
