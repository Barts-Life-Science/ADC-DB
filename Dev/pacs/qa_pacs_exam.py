# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Source tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4_prod.raw.pacs_examinations

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Intermediate tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check duplicates on pacs_exam
# MAGIC
# MAGIC WITH dup as (
# MAGIC   SELECT ExaminationId
# MAGIC   FROM 4_prod.pacs.pacs_exam
# MAGIC   GROUP BY ExaminationId
# MAGIC   HAVING COUNT(*) > 1
# MAGIC   LIMIT 100
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.intmd_pacs_examinations AS e
# MAGIC INNER JOIN dup
# MAGIC ON e.ExaminationId = dup.ExaminationId

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.intmd_pacs_examinations
# MAGIC WHERE PacsMillPersonId IS NULL
# MAGIC LIMIT 100

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check consistency on MillPersonId between mill_clinical_event and pacs_examinations tables
# MAGIC
# MAGIC SELECT COUNT(*)
# MAGIC FROM 4_prod.pacs.intmd_pacs_examinations
# MAGIC WHERE MillPersonId != PacsMillPersonId

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.intmd_pacs_examinations
# MAGIC WHERE MillPersonId != PacsMillPersonId
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SELECT 'Missing MillAccessionNbr', COUNT(*)
# MAGIC FROM 4_prod.pacs.pacs_exam
# MAGIC WHERE MillAccessionNbr IS NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 'Missing MillAccessionNbr and ExamRefNbr', COUNT(*)
# MAGIC FROM 4_prod.pacs.pacs_exam
# MAGIC WHERE MillAccessionNbr IS NULL AND ExamRefNbr IS NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 'Missing MillPersonId', COUNT(*)
# MAGIC FROM 4_prod.pacs.pacs_exam
# MAGIC WHERE MillPersonId IS NULL 
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 'Total', COUNT(*)
# MAGIC FROM 4_prod.pacs.pacs_exam
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.pacs_exam
# MAGIC WHERE MillPersonId IS NULL 
# MAGIC LIMIT 100

# COMMAND ----------


