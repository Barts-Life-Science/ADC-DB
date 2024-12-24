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
# MAGIC   'ExaminationAccessionNumber does not end ExaminationIdString' AS description,
# MAGIC   COUNT(*) AS num_rows,
# MAGIC   0 AS expected_num_rows
# MAGIC FROM 4_prod.raw.pacs_examinations
# MAGIC WHERE 
# MAGIC   ExaminationAccessionNumber NOT LIKE CONCAT('%', ExaminationIdString)
# MAGIC   AND ExaminationAccessionNumber != 'VALUE_TOO_LONG'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM 4_prod.raw.pacs_examinations
# MAGIC WHERE 
# MAGIC   ExaminationAccessionNumber NOT LIKE CONCAT('%', ExaminationIdString)
# MAGIC   AND ExaminationAccessionNumber != 'VALUE_TOO_LONG'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current table
