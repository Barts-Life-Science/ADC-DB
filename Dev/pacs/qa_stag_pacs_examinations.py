# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Upstream tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4_prod.raw.pacs_examinations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cases that could cause errors
# MAGIC
# MAGIC SELECT 
# MAGIC   'ExaminationAccessionNumber does not end with 0 despite ExaminationIdString=0' AS description,
# MAGIC   COUNT(*) AS num_rows,
# MAGIC   0 AS expected_num_rows
# MAGIC FROM 4_prod.raw.pacs_examinations
# MAGIC WHERE 
# MAGIC   ExaminationAccessionNumber NOT LIKE '%0'
# MAGIC   AND ExaminationIdString == '0'
# MAGIC   AND ExaminationAccessionNumber != 'VALUE_TOO_LONG'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current table
