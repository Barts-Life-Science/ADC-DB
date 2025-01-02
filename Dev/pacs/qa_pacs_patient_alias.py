# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT 'Total count' AS item, COUNT(*) AS num_rows
# MAGIC FROM 4_prod.pacs.pacs_patient_alias
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 'Total count since 2014' AS item, COUNT(*) AS num_rows
# MAGIC FROM 4_prod.pacs.pacs_patient_alias
# MAGIC WHERE latestExamDate > '2014-01-01'
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC
# MAGIC SELECT 'Distinct PacsPatientId' AS item, COUNT(DISTINCT PacsPatientId) AS num_rows
# MAGIC FROM 4_prod.pacs.pacs_patient_alias
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 'Missing MillPersonId' AS item, COUNT(*) AS num_rows
# MAGIC FROM 4_prod.pacs.pacs_patient_alias
# MAGIC WHERE MillPersonId IS NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 'Missing MillPersonId since 2014' AS item, COUNT(*) AS num_rows
# MAGIC FROM 4_prod.pacs.pacs_patient_alias
# MAGIC WHERE MillPersonId IS NULL
# MAGIC AND latestExamDate > '2014-01-01'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.pacs_patient_alias
# MAGIC WHERE MillPersonId IS NULL
# MAGIC AND LatestExamDate > '2022-01-01'
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM 4_prod.raw.mill_person_alias
# MAGIC WHERE alias = '7059359672'
