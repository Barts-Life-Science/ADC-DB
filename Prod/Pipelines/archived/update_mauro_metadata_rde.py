# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY FUNCTION to_col_json_str(
# MAGIC   col_name STRING, data_type STRING, sensitive STRING, description STRING
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC /*
# MAGIC RETURN CONCAT(
# MAGIC   '{', '''name'':''', col_name, ''',',
# MAGIC   '''values'':null,',
# MAGIC   '''dataType'':''', data_type, ''',',
# MAGIC   '''sensitive'':', sensitive, ',',
# MAGIC   '''description'':''', description, '''','}')*/
# MAGIC
# MAGIC RETURN TO_JSON(named_struct(
# MAGIC   'label', col_name,
# MAGIC   'description', description,
# MAGIC   'index', 0,
# MAGIC   'dataType', named_struct(
# MAGIC     'label', data_type,
# MAGIC     'description', data_type,
# MAGIC     'index', 0,
# MAGIC     'domainType', 'PrimitiveType'
# MAGIC   )))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE 1_inland.evan_demo.mauro_metadata_col_rde(
# MAGIC   table_catalog VARCHAR(50),
# MAGIC   table_schema VARCHAR(50),
# MAGIC   table_name VARCHAR(100),
# MAGIC   column_name VARCHAR(200),
# MAGIC   ordinal_position INT,
# MAGIC   dataType VARCHAR(50),
# MAGIC   ig_risk INT,
# MAGIC   ig_severity INT,
# MAGIC   sensitive BOOLEAN,
# MAGIC   column_description STRING,
# MAGIC   json_str STRING,
# MAGIC   json_struct STRUCT<label STRING, description STRING, index INT, dataType STRUCT<label STRING, description STRING, index INT, domainType STRING>>
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH ig_risk AS (
# MAGIC   SELECT *
# MAGIC   FROM system.information_schema.column_tags AS t
# MAGIC   WHERE LOWER(tag_name) = 'ig_risk'
# MAGIC ),
# MAGIC ig_severity AS (
# MAGIC   SELECT *
# MAGIC   FROM system.information_schema.column_tags AS t
# MAGIC   WHERE LOWER(tag_name) = 'ig_severity'
# MAGIC ),
# MAGIC ig_sensitive AS (
# MAGIC   SELECT
# MAGIC     r.catalog_name,
# MAGIC     r.schema_name,
# MAGIC     r.table_name,
# MAGIC     r.column_name,
# MAGIC     r.tag_value AS ig_risk,
# MAGIC     s.tag_value AS ig_severity,
# MAGIC     CASE 
# MAGIC       WHEN UPPER(r.column_name) = 'ADC_UPDT'
# MAGIC         THEN FALSE
# MAGIC       WHEN r.tag_value IS NULL OR s.tag_value IS NULL
# MAGIC         THEN NULL
# MAGIC       WHEN r.tag_value >= 3 OR s.tag_value >= 2
# MAGIC         THEN TRUE
# MAGIC       ELSE FALSE
# MAGIC     END AS sensitive
# MAGIC   FROM ig_risk AS r
# MAGIC  INNER JOIN ig_severity AS s
# MAGIC   ON
# MAGIC     r.catalog_name = s.catalog_name
# MAGIC     AND r.schema_name = s.schema_name
# MAGIC     AND r.table_name = s.table_name
# MAGIC     AND r.column_name = s.column_name
# MAGIC )
# MAGIC INSERT INTO 1_inland.evan_demo.mauro_metadata_col_rde
# MAGIC SELECT 
# MAGIC   c.table_catalog,
# MAGIC   c.table_schema,
# MAGIC   c.table_name,
# MAGIC   c.column_name AS name,
# MAGIC   c.ordinal_position,
# MAGIC   c.data_type AS dataType,
# MAGIC   s.ig_risk,
# MAGIC   s.ig_severity,
# MAGIC   s.sensitive,
# MAGIC   c.comment AS description,
# MAGIC   NULL,
# MAGIC   NULL
# MAGIC FROM system.information_schema.columns AS c
# MAGIC LEFT JOIN ig_sensitive AS s
# MAGIC ON
# MAGIC   c.table_catalog = s.catalog_name
# MAGIC   AND c.table_schema = s.schema_name
# MAGIC   AND c.table_name = s.table_name
# MAGIC   AND c.column_name = s.column_name
# MAGIC WHERE
# MAGIC   (
# MAGIC     c.table_catalog = '4_prod'
# MAGIC     AND c.table_schema = 'rde'
# MAGIC     AND c.table_name ILIKE 'rde_%'
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO 1_inland.evan_demo.mauro_metadata_col_rde AS target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     table_name,
# MAGIC     column_name, 
# MAGIC     CASE
# MAGIC       WHEN sensitive IS NULL AND column_description IS NULL
# MAGIC         THEN to_col_json_str(column_name, dataType, 'null', 'null')
# MAGIC       WHEN sensitive IS NULL
# MAGIC         THEN to_col_json_str(column_name, dataType, 'null', column_description)
# MAGIC       WHEN column_description IS NULL
# MAGIC         THEN to_col_json_str(column_name, dataType, sensitive, 'null')
# MAGIC       ELSE to_col_json_str(column_name, dataType, sensitive, column_description)
# MAGIC     END AS json_str
# MAGIC   FROM 1_inland.evan_demo.mauro_metadata_col_rde
# MAGIC   WHERE
# MAGIC   (
# MAGIC     table_catalog = '4_prod'
# MAGIC     AND table_schema = 'rde'
# MAGIC     AND table_name ILIKE 'rde_%'
# MAGIC   )
# MAGIC ) AS source
# MAGIC ON target.column_name = source.column_name
# MAGIC AND target.table_name = source.table_name
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET target.json_str = source.json_str

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO 1_inland.evan_demo.mauro_metadata_col_rde AS target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     table_name,
# MAGIC     column_name, 
# MAGIC     from_json(json_str, 'label STRING, description STRING, index INT, dataType STRUCT<label STRING, description STRING, index INT, domainType STRING>') AS json_struct
# MAGIC   FROM 1_inland.evan_demo.mauro_metadata_col_rde
# MAGIC   WHERE
# MAGIC   (
# MAGIC     table_catalog = '4_prod'
# MAGIC     AND table_schema = 'rde'
# MAGIC     AND table_name ILIKE 'rde_%'
# MAGIC   )
# MAGIC ) AS source
# MAGIC ON target.column_name = source.column_name
# MAGIC AND target.table_name = source.table_name
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET target.json_struct = source.json_struct

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if any json_str is null
# MAGIC SELECT * 
# MAGIC FROM 1_inland.evan_demo.mauro_metadata_col_rde
# MAGIC WHERE json_str IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE 1_inland.evan_demo.mauro_metadata_tab_rde(
# MAGIC   `label` VARCHAR(100),
# MAGIC   description STRING,
# MAGIC   --`columns` STRING,
# MAGIC   `index` INT,
# MAGIC   `dataElements` ARRAY<STRUCT<label STRING, description STRING, index INT, dataType STRUCT<label STRING, description STRING, index INT, domainType STRING>>>
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO 1_inland.evan_demo.mauro_metadata_tab_rde
# MAGIC SELECT
# MAGIC   c.table_name,
# MAGIC   NULL,
# MAGIC   --CONCAT('[', array_join(collect_set(c.json_str), ','), ']'),
# MAGIC   0,
# MAGIC   collect_set(c.json_struct)
# MAGIC FROM 1_inland.evan_demo.mauro_metadata_col_rde AS c
# MAGIC GROUP BY c.table_name

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO 1_inland.evan_demo.mauro_metadata_tab_rde AS target
# MAGIC USING (
# MAGIC   SELECT table_name, comment
# MAGIC   FROM system.information_schema.tables
# MAGIC   WHERE
# MAGIC   (
# MAGIC     table_catalog = '4_prod'
# MAGIC     AND table_schema = 'rde'
# MAGIC     AND table_name ILIKE 'rde_%'
# MAGIC   )
# MAGIC ) AS source
# MAGIC ON target.label = source.table_name
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET target.description = source.comment

# COMMAND ----------

df = spark.sql("""
select
  collect_list(
    struct(label, description, index, dataElements)
  ) AS childDataClasses
from 1_inland.evan_demo.mauro_metadata_tab_rde
""")

# COMMAND ----------

df.write.mode("overwrite").json("/Volumes/1_inland/sectra/vone/rde_mauro.json")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


