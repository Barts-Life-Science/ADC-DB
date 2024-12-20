# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## STAG_PACS_EXAMINATIONS

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Query: Check number of exams in ExaminationReports if ExaminationAccessionNumber is not null
# MAGIC -- Observation: 
# MAGIC
# MAGIC SELECT
# MAGIC   COUNT(*) AS total,
# MAGIC   COUNT(RequestId) AS matched_request_count,
# MAGIC   SUM(CASE
# MAGIC     WHEN 
# MAGIC       RequestIdString IS NOT NULL
# MAGIC     THEN 1
# MAGIC     ELSE 0
# MAGIC   END) AS nonnull_reqeust_id_str_count,
# MAGIC   SUM(CASE
# MAGIC     WHEN 
# MAGIC       ExaminationAccessionNumber IS NOT NULL
# MAGIC     THEN 1
# MAGIC     ELSE 0
# MAGIC   END) AS nonnull_exam_accession_nbr_count,
# MAGIC   SUM(CASE
# MAGIC     WHEN 
# MAGIC       RequestIdString = ExaminationAccessionNumber
# MAGIC       AND RequestIdString IS NOT NULL
# MAGIC       AND ExaminationAccessionNumber IS NOT NULL
# MAGIC     THEN 1
# MAGIC     ELSE 0
# MAGIC   END) AS matched_exam_accession_nbr_count,
# MAGIC   SUM(CASE
# MAGIC     WHEN 
# MAGIC       RequestIdString != ExaminationAccessionNumber
# MAGIC       AND RequestIdString IS NOT NULL
# MAGIC       AND ExaminationAccessionNumber IS NOT NULL
# MAGIC     THEN 1
# MAGIC     ELSE 0
# MAGIC   END) AS unmatched_exam_accession_nbr_count,
# MAGIC   SUM(CASE
# MAGIC     WHEN 
# MAGIC       RequestIdString = ExaminationIdString
# MAGIC       AND RequestIdString IS NOT NULL
# MAGIC       AND ExaminationIdString IS NOT NULL
# MAGIC     THEN 1
# MAGIC     ELSE 0
# MAGIC   END) AS matched_exam_id_str_count,
# MAGIC   SUM(CASE
# MAGIC     WHEN 
# MAGIC       RequestIdString != ExaminationIdString
# MAGIC       AND RequestIdString IS NOT NULL
# MAGIC       AND ExaminationIdString IS NOT NULL
# MAGIC     THEN 1
# MAGIC     ELSE 0
# MAGIC   END) AS unmatched_exam_id_str_count,
# MAGIC   SUM(CASE
# MAGIC     WHEN 
# MAGIC       RequestIdString = ExaminationText1
# MAGIC       AND RequestIdString IS NOT NULL
# MAGIC       AND ExaminationText1 IS NOT NULL
# MAGIC     THEN 1
# MAGIC     ELSE 0
# MAGIC   END) AS matched_exam_text1_count,
# MAGIC   SUM(CASE
# MAGIC     WHEN 
# MAGIC       RequestIdString != ExaminationText1
# MAGIC       AND RequestIdString IS NOT NULL
# MAGIC       AND ExaminationText1 IS NOT NULL
# MAGIC     THEN 1
# MAGIC     ELSE 0
# MAGIC   END) AS unmatched_exam_text1_count
# MAGIC FROM 4_prod.pacs.stag_pacs_examinations AS ex
# MAGIC LEFT JOIN 4_prod.raw.pacs_requests AS rq
# MAGIC ON ex.ExaminationReportRequestId = rq.RequestId
# MAGIC
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Query: Check number of exams in ExaminationReports if ExaminationAccessionNumber is not null
# MAGIC -- Observation: 
# MAGIC
# MAGIC SELECT
# MAGIC   ExaminationIdString,
# MAGIC   ExaminationAccessionNumber,
# MAGIC   ExaminationText1,
# MAGIC   RequestIdString
# MAGIC FROM 4_prod.pacs.stag_pacs_examinations AS ex
# MAGIC LEFT JOIN 4_prod.raw.pacs_requests AS rq
# MAGIC ON ex.ExaminationReportRequestId = rq.RequestId
# MAGIC WHERE ExaminationAccessionNumber IS NOT NULL
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   COUNT(*), 
# MAGIC   COUNT(ExaminationIdString), 
# MAGIC   COUNT(ExaminationText1), 
# MAGIC   COUNT(ExaminationAccessionNumber)
# MAGIC FROM 4_prod.raw.pacs_examinations

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT ExamIdOrAccessionNbr
# MAGIC FROM `4_prod`.pacs.stag_pacs_examinations
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## PACS_EXAM

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.pacs_exam
# MAGIC WHERE RequestIdString IS NULL
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC   'Total count' AS issue,
# MAGIC   COUNT(*) AS count
# MAGIC FROM 4_prod.pacs.pacs_exam
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT
# MAGIC   'Duplicates' AS issue,
# MAGIC   COUNT(*) - COUNT(DISTINCT ExaminationId) AS count
# MAGIC FROM 4_prod.pacs.pacs_exam
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT
# MAGIC   'No RequestId' AS issue,
# MAGIC   COUNT(*) - COUNT(RequestId) AS count
# MAGIC FROM 4_prod.pacs.pacs_exam
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT
# MAGIC   'No MillAccessionNbr' AS issue,
# MAGIC   COUNT(*) - COUNT(MillAccessionNbr) AS count
# MAGIC FROM 4_prod.pacs.pacs_exam

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.pacs_exam
# MAGIC WHERE ExamAccessionNbr != MillAccessionNbr
# MAGIC AND MillAccessionNbr IS NOT NULL
# MAGIC AND ExamAccessionNbr IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.pacs_exam_ext
# MAGIC WHERE MillAccessionNbr IS NULL
# MAGIC LIMIT 100

# COMMAND ----------

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
# MAGIC -- Check which column is best for imputing Mill_Clinical_Event.Performed_Dt_Tm
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
# MAGIC -- Check if at least 1 row exists with MillEventReltn = 'root' per MillAccessionNbr group
# MAGIC
# MAGIC SELECT DISTINCT MillAccessionNbr, MillRefNbrPattern
# MAGIC FROM 4_prod.pacs.stag_mill_clinical_event_pacs
# MAGIC
# MAGIC EXCEPT
# MAGIC
# MAGIC SELECT DISTINCT MillAccessionNbr, MillRefNbrPattern
# MAGIC FROM 4_prod.pacs.stag_mill_clinical_event_pacs
# MAGIC WHERE  Lower(MillEventReltn) = 'root'
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM 4_prod.pacs.stag_mill_clinical_event_pacs
# MAGIC LIMIT 100

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
# MAGIC   'Duplicates (MillClinicalEventId, RequestId, ExaminationId)' AS condition,
# MAGIC   COUNT(MillClinicalEventId, RequestId, ExaminationId) - COUNT(DISTINCT MillClinicalEventId, RequestId, ExaminationId) AS num_rows
# MAGIC FROM 4_prod.pacs.pacs_clinical_event
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC SELECT 
# MAGIC   'Duplicates (MillClinicalEventId)' AS condition,
# MAGIC   COUNT(MillClinicalEventId) - COUNT(DISTINCT MillClinicalEventId) AS num_rows
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
# MAGIC WHERE ExaminationModality IS NOT NULL
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


