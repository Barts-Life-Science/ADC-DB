# Databricks notebook source
# MAGIC %md
# MAGIC ## Pacs_Clinical_Event

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check duplicates
# MAGIC -- Expect zero row in result
# MAGIC SELECT MillClinicalEventId, COUNT(*)
# MAGIC FROM 4_prod.pacs.pacs_clinical_event
# MAGIC GROUP BY MillClinicalEventId, RequestId, ExaminationId
# MAGIC HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check invalid timestamp
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.pacs_clinical_event
# MAGIC WHERE MillEventDate < '2000-01-01'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check which column is best for imputing Mill_Clinical_Event.Event_Start_Dt_Tm
# MAGIC -- Lower value is better
# MAGIC WITH cte_examid AS(
# MAGIC   SELECT DISTINCT MillClinicalEventId, ExaminationId
# MAGIC   FROM 4_prod.pacs.pacs_clinical_event AS pce
# MAGIC )
# MAGIC SELECT 
# MAGIC   AVG(ABS(DATEDIFF(Event_Start_Dt_Tm, e.ExaminationScheduledDate))) AS examiantionscheduleddate,
# MAGIC   AVG(ABS(DATEDIFF(Event_Start_Dt_Tm, e.ExaminationDate))) AS examiantiondate,
# MAGIC   AVG(ABS(DATEDIFF(Event_Start_Dt_Tm, e.ExaminationImportDateTime))) AS examiantionimportdatetime
# MAGIC FROM 4_prod.raw.mill_clinical_event AS mce
# MAGIC INNER JOIN cte_examid
# MAGIC ON mce.clinical_event_id = cte_examid.millclinicaleventid
# MAGIC INNER JOIN 4_prod.raw.pacs_examinations AS e
# MAGIC ON cte_examid.examinationid = e.examinationid
# MAGIC WHERE 
# MAGIC   mce.Event_Start_Dt_Tm > '2010-01-01'
# MAGIC   AND mce.Contributor_System_Cd = 6141416	--BLT_TIE_RAD

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check which column is best for imputing Mill_Clinical_Event.Event_Start_Dt_Tm
# MAGIC -- Lower value is better
# MAGIC WITH cte_examid AS(
# MAGIC   SELECT DISTINCT MillClinicalEventId, ExaminationId
# MAGIC   FROM 4_prod.pacs.pacs_clinical_event AS pce
# MAGIC )
# MAGIC SELECT 
# MAGIC   AVG(ABS(DATEDIFF(Performed_Dt_Tm, e.ExaminationScheduledDate))) AS examiantionscheduleddate,
# MAGIC   AVG(ABS(DATEDIFF(Performed_Dt_Tm, e.ExaminationDate))) AS examiantiondate,
# MAGIC   AVG(ABS(DATEDIFF(Performed_Dt_Tm, e.ExaminationImportDateTime))) AS examiantionimportdatetime
# MAGIC FROM 4_prod.raw.mill_clinical_event AS mce
# MAGIC INNER JOIN cte_examid
# MAGIC ON mce.clinical_event_id = cte_examid.millclinicaleventid
# MAGIC INNER JOIN 4_prod.raw.pacs_examinations AS e
# MAGIC ON cte_examid.examinationid = e.examinationid
# MAGIC WHERE 
# MAGIC   mce.Event_Start_Dt_Tm > '2010-01-01'
# MAGIC   AND mce.Contributor_System_Cd = 6141416	--BLT_TIE_RAD

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC   'Total' AS condition,
# MAGIC   COUNT(*) AS num_rows
# MAGIC FROM 4_prod.pacs.pacs_clinical_event
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 
# MAGIC   'Duplicates' AS condition,
# MAGIC   COUNT(MillClinicalEventId, RequestId, ExaminationId) - COUNT(DISTINCT MillClinicalEventId, RequestId, ExaminationId) AS num_rows
# MAGIC FROM 4_prod.pacs.pacs_clinical_event
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT
# MAGIC   'No ExaminationId' AS condition,
# MAGIC   COUNT(*) AS num_rows
# MAGIC FROM 4_prod.pacs.pacs_clinical_event
# MAGIC WHERE ExaminationId IS NULL
# MAGIC
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT
# MAGIC   'No RequestId' AS condition,
# MAGIC   COUNT(*) AS num_rows
# MAGIC FROM 4_prod.pacs.pacs_clinical_event
# MAGIC WHERE RequestId IS NULL
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT
# MAGIC   'No AccessionNbr' AS condition,
# MAGIC   COUNT(*) AS num_rows
# MAGIC FROM 4_prod.pacs.pacs_clinical_event
# MAGIC WHERE AccessionNbr IS NULL
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.pacs_clinical_event
# MAGIC WHERE ExaminationModality IS NULL
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.pacs_clinical_event
# MAGIC WHERE MillPacsRefNbr LIKE 'RNH0%' AND ExaminationModality IS NOT NULL
# MAGIC AND MillPacsRefNbr NOT LIKE 'RNH0XR%' AND MillPacsRefNbr NOT LIKE 'RNH0CT%'
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- check if EventId is unique
# MAGIC SELECT COUNT(DISTINCT MillEventId), COUNT(*)
# MAGIC FROM 4_prod.pacs.pacs_clinical_event
# MAGIC WHERE MillEventId IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(DISTINCT EVENT_ID), COUNT(*)
# MAGIC FROM 4_prod.raw.pi_cde_blob_content AS blob
# MAGIC INNER JOIN (SELECT DISTINCT MillEventId FROM 4_prod.pacs.pacs_clinical_event) AS e
# MAGIC ON blob.EVENT_ID = e.MillEventId

# COMMAND ----------


