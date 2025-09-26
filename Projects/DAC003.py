# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS 5_projects.dac003_breastonestop.breast_patient_codes;
# MAGIC
# MAGIC CREATE TABLE 5_projects.dac003_breastonestop.breast_patient_codes(
# MAGIC     code_type VARCHAR(100),
# MAGIC     code_value VARCHAR(400)
# MAGIC );
# MAGIC
# MAGIC
# MAGIC INSERT INTO 5_projects.dac003_breastonestop.breast_patient_codes
# MAGIC VALUES
# MAGIC ('ICD-10_prefix', 'N60'),
# MAGIC ('ICD-10_prefix', 'N61'),
# MAGIC ('ICD-10_prefix', 'N62'),
# MAGIC ('ICD-10_prefix', 'N63'),
# MAGIC ('ICD-10_prefix', 'C50'),
# MAGIC ('ICD-10_prefix', 'D24'),
# MAGIC ('ICD-10_prefix', 'D05'),
# MAGIC ('ICD-10_prefix', 'D486'),
# MAGIC ('ICD-10_prefix', 'Z123'),
# MAGIC ('ICD-10_prefix', 'Z901'),
# MAGIC ('ICD-10_prefix', 'Z853'),
# MAGIC ('OPCS-4_prefix', 'B27'),
# MAGIC ('OPCS-4_prefix', 'B28'),
# MAGIC ('OPCS-4_prefix', 'B29'),
# MAGIC ('OPCS-4_prefix', 'B30'),
# MAGIC ('OPCS-4_prefix', 'B31'),
# MAGIC ('OPCS-4_prefix', 'B32'),
# MAGIC ('OPCS-4_prefix', 'B33'),
# MAGIC ('OPCS-4_prefix', 'B34'),
# MAGIC ('OPCS-4_prefix', 'B35'),
# MAGIC ('OPCS-4_prefix', 'B36'),
# MAGIC ('OPCS-4_prefix', 'B37'),
# MAGIC ('OPCS-4_prefix', 'B38'),
# MAGIC ('OPCS-4_prefix', 'B39'),
# MAGIC ('OPCS-4_prefix', 'B40'),
# MAGIC ('OPCS-4_prefix', 'B41'),
# MAGIC ('OPCS-4_prefix', 'U18'),
# MAGIC ('OPCS-4_prefix', 'Z15');

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS 5_projects.dac003_breastonestop.breast_patient_order_list_202406;
# MAGIC
# MAGIC
# MAGIC CREATE TABLE 5_projects.dac003_breastonestop.breast_patient_order_list_202406(
# MAGIC     PERSON_ID BIGINT,
# MAGIC     ORDER_ID BIGINT,
# MAGIC     CATALOG_CD BIGINT,
# MAGIC     ORDER_MNEMONIC VARCHAR(400),
# MAGIC     ORDER_DT_TM TIMESTAMP,
# MAGIC     SRC_TABLE VARCHAR(100)
# MAGIC );
# MAGIC
# MAGIC
# MAGIC
# MAGIC WITH cd AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         VALUES
# MAGIC         (6180043), -- US Breast Rt
# MAGIC         (6181432), -- US Guided core biopsy breast Lt
# MAGIC         (6182217), -- US Breast Lt
# MAGIC         (6183363), -- US Guided core biopsy breast Rt
# MAGIC         (6183590) -- US Breast Both
# MAGIC     ) AS tmp(CATALOG_CD)
# MAGIC )
# MAGIC INSERT INTO 5_projects.dac003_breastonestop.breast_patient_order_list_202406
# MAGIC SELECT
# MAGIC     PERSON_ID,
# MAGIC     ORDER_ID,
# MAGIC     o.CATALOG_CD,
# MAGIC     ORDER_MNEMONIC,
# MAGIC     o.ORIG_ORDER_DT_TM,
# MAGIC     'MILL_DIR_ORDERS'
# MAGIC FROM 4_prod.raw.MILL_ORDERS AS o
# MAGIC INNER JOIN cd
# MAGIC     ON o.CATALOG_CD = cd.CATALOG_CD
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC /**
# MAGIC WITH cd AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         VALUES
# MAGIC         ('US Breast Rt'),
# MAGIC         ('US Guided core biopsy breast Lt'),
# MAGIC         ('US Breast Lt'),
# MAGIC         ('US Guided core biopsy breast Rt'),
# MAGIC         ('US Breast Both')
# MAGIC     ) AS tmp(order_txt)
# MAGIC )
# MAGIC INSERT INTO 5_projects.dac003_breastonestop.breast_patient_order_list_202406
# MAGIC SELECT
# MAGIC     PERSON_ID,
# MAGIC     ORDER_ID,
# MAGIC     0 AS catalog_cd,
# MAGIC     ORDER_MNEM_TXT,
# MAGIC     ORDER_DT_TM,
# MAGIC     'PI_CDE_ORDER' AS src_table
# MAGIC FROM 4_prod.raw.PI_CDE_ORDER AS o
# MAGIC INNER JOIN cd
# MAGIC     ON LOWER(LTRIM(RTRIM(o.ORDER_MNEM_TXT))) = LOWER(LTRIM(RTRIM(cd.order_txt)))
# MAGIC ;
# MAGIC
# MAGIC **/
# MAGIC
# MAGIC SELECT 
# MAGIC     ORDER_MNEMONIC, 
# MAGIC     COUNT(DISTINCT PERSON_ID) AS patient_count
# MAGIC FROM 5_projects.dac003_breastonestop.breast_patient_order_list_202406
# MAGIC WHERE order_dt_tm BETWEEN '2010-01-01' AND '2024-01-25'
# MAGIC GROUP BY ORDER_MNEMONIC
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Total num of patients' AS ORDER_MNEMONIC, 
# MAGIC     COUNT(DISTINCT PERSON_ID) AS patient_count
# MAGIC FROM 5_projects.dac003_breastonestop.breast_patient_order_list_202406
# MAGIC WHERE order_dt_tm BETWEEN '2010-01-01' AND '2024-01-25'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS 5_projects.dac003_breastonestop.breast_patient_opa_list_202406;
# MAGIC
# MAGIC
# MAGIC CREATE TABLE 5_projects.dac003_breastonestop.breast_patient_opa_list_202406(
# MAGIC     person_id BIGINT,
# MAGIC     src_table VARCHAR(200),
# MAGIC     src_id VARCHAR(200),
# MAGIC     src_id_col VARCHAR(100),
# MAGIC     src_event_dt_tm TIMESTAMP,
# MAGIC     src_code_value VARCHAR(100)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO 5_projects.dac003_breastonestop.breast_patient_opa_list_202406
# MAGIC SELECT
# MAGIC   ce.person_id,
# MAGIC   'MILL_SCH_APPT',
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   COALESCE(ce.EVENT_START_DT_TM, ce.EVENT_END_DT_TM),
# MAGIC   ce.EVENT_CD
# MAGIC FROM 4_prod.raw.mill_sch_appt AS sa 
# MAGIC INNER JOIN 4_prod.raw.mill_clinical_event AS ce 
# MAGIC ON ce.EVENT_ID = sa.SCH_EVENT_ID
# MAGIC WHERE 
# MAGIC   sa.DESCRIPTION ILIKE '%breast%'
# MAGIC   AND COALESCE(ce.EVENT_START_DT_TM, ce.EVENT_END_DT_TM) BETWEEN '2010-01-01' AND '2024-01-25';
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS 5_projects.dac003_breastonestop.breast_patient_list_202406;
# MAGIC
# MAGIC CREATE TABLE 5_projects.dac003_breastonestop.breast_patient_list_202406(
# MAGIC     person_id BIGINT,
# MAGIC     ehr_code_type VARCHAR(100),
# MAGIC     ehr_code_value VARCHAR(100),
# MAGIC     src_table VARCHAR(200),
# MAGIC     src_id VARCHAR(200),
# MAGIC     src_id_col VARCHAR(100),
# MAGIC     src_event_dt_tm TIMESTAMP,
# MAGIC     src_code_value VARCHAR(100)
# MAGIC );
# MAGIC
# MAGIC /**
# MAGIC WITH icd AS (
# MAGIC     SELECT *
# MAGIC     FROM 5_projects.dac003_breastonestop.breast_patient_codes
# MAGIC     WHERE code_type LIKE 'ICD-10%'
# MAGIC )
# MAGIC INSERT INTO 5_projects.dac003_breastonestop.breast_patient_list_202406
# MAGIC SELECT
# MAGIC     e.person_id,
# MAGIC     c.code_type AS ehr_code_type,
# MAGIC     c.code_value,
# MAGIC     'PI_CDE_DIAGNOSIS' AS src_table,
# MAGIC     p.diagnosis_id,
# MAGIC     'DIAGNOSIS_ID' AS src_id_col,
# MAGIC     COALESCE(p.diagnosis_dt_tm, e.BEG_EFFECTIVE_DT_TM),
# MAGIC     p.CONCEPT_CKI_IDENT
# MAGIC FROM BH_DATAWAREHOUSE.dbo.PI_CDE_DIAGNOSIS AS p
# MAGIC INNER JOIN icd AS c
# MAGIC     ON p.CONCEPT_CKI_IDENT LIKE 'ICD10WHO!'+LTRIM(RTRIM(c.code_value))+'%'
# MAGIC INNER JOIN BH_DATAWAREHOUSE.dbo.PI_CDE_ENCOUNTER AS pe
# MAGIC     ON p.ENCNTR_ID = pe.ENCNTR_ID
# MAGIC INNER JOIN BH_DATAWAREHOUSE.dbo.MILL_DIR_ENCOUNTER AS e
# MAGIC     ON p.ENCNTR_ID = e.ENCNTR_ID AND pe.PERSON_ID = e.PERSON_ID
# MAGIC WHERE
# MAGIC     p.ACTIVE_IND = 1
# MAGIC     AND e.ACTIVE_IND = 1
# MAGIC     AND e.BEG_EFFECTIVE_DT_TM IS NOT NULL
# MAGIC
# MAGIC **/
# MAGIC
# MAGIC WITH opcs AS (
# MAGIC     SELECT *
# MAGIC     FROM 5_projects.dac003_breastonestop.breast_patient_codes
# MAGIC     WHERE code_type ILIKE 'OPCS-4%'
# MAGIC )
# MAGIC INSERT INTO 5_projects.dac003_breastonestop.breast_patient_list_202406
# MAGIC SELECT
# MAGIC     e.person_id,
# MAGIC     c.code_type AS ehr_code_type,
# MAGIC     c.code_value,
# MAGIC     'MILL_DIR_PROCEDURE' AS src_table,
# MAGIC     p.procedure_id,
# MAGIC     'PROCEDURE_ID' AS src_id_col,
# MAGIC     e.BEG_EFFECTIVE_DT_TM,
# MAGIC     COALESCE(n.CONCEPT_CKI, n.SOURCE_IDENTIFIER)
# MAGIC FROM 4_prod.raw.MILL_PROCEDURE AS p
# MAGIC INNER JOIN 3_lookup.mill.MILL_NOMENCLATURE AS n
# MAGIC     ON p.NOMENCLATURE_ID = n.NOMENCLATURE_ID
# MAGIC INNER JOIN opcs AS c
# MAGIC     ON LTRIM(RTRIM(SOURCE_IDENTIFIER)) ILIKE CONCAT(LTRIM(RTRIM(c.code_value)),'%')
# MAGIC INNER JOIN 4_prod.raw.MILL_ENCOUNTER AS e
# MAGIC     ON p.ENCNTR_ID = e.ENCNTR_ID
# MAGIC WHERE 
# MAGIC     SOURCE_VOCABULARY_CD = 685812 -- OPCS4
# MAGIC     AND e.BEG_EFFECTIVE_DT_TM IS NOT NULL
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC WITH icd AS (
# MAGIC     SELECT *
# MAGIC     FROM 5_projects.dac003_breastonestop.breast_patient_codes
# MAGIC     WHERE code_type ILIKE 'ICD-10%'
# MAGIC )
# MAGIC INSERT INTO 5_projects.dac003_breastonestop.breast_patient_list_202406
# MAGIC SELECT
# MAGIC     e.person_id,
# MAGIC     c.code_type AS ehr_code_type,
# MAGIC     c.code_value,
# MAGIC     'MILL_DIR_DIAGNOSIS' AS src_table,
# MAGIC     d.diagnosis_id,
# MAGIC     'DIAGNOSIS_ID' AS src_id_col,
# MAGIC     e.BEG_EFFECTIVE_DT_TM,
# MAGIC     COALESCE(n.CONCEPT_CKI, n.SOURCE_IDENTIFIER)
# MAGIC FROM 4_prod.raw.MILL_DIAGNOSIS AS d
# MAGIC INNER JOIN 3_lookup.mill.MILL_NOMENCLATURE AS n
# MAGIC     ON d.NOMENCLATURE_ID = n.NOMENCLATURE_ID
# MAGIC INNER JOIN 5_projects.dac003_breastonestop.breast_patient_codes AS c
# MAGIC     ON LTRIM(RTRIM(n.SOURCE_IDENTIFIER)) ILIKE CONCAT(LTRIM(RTRIM(c.code_value)),'%')
# MAGIC INNER JOIN 4_prod.raw.MILL_ENCOUNTER AS e
# MAGIC     ON d.ENCNTR_ID = e.ENCNTR_ID
# MAGIC WHERE 
# MAGIC     c.code_type = 'ICD-10_prefix' 
# MAGIC     AND CONCEPT_CKI ILIKE 'ICD10WHO%'
# MAGIC     AND e.BEG_EFFECTIVE_DT_TM IS NOT NULL
# MAGIC ;
# MAGIC
# MAGIC SELECT 
# MAGIC     COUNT(DISTINCT PERSON_ID) AS person_count
# MAGIC FROM 5_projects.dac003_breastonestop.breast_patient_list_202406
# MAGIC WHERE src_event_dt_tm BETWEEN '2010-01-01' AND '2024-01-25'
# MAGIC

# COMMAND ----------

#Breast cancer one stop

project_identifier = 'dac003'

rde_tables = ['rde_aliases', 'rde_all_procedures', 'rde_blobdataset', 'rde_all_diagnosis', 'rde_allergydetails', 'rde_apc_diagnosis', 'rde_apc_opcs', 'rde_ariapharmacy', 'rde_blobdataset', 'rde_cds_apc', 'rde_cds_opa', 'rde_critactivity', 'rde_critopcs', 'rde_critperiod', 'rde_emergencyd', 'rde_encounter', 'rde_family_history', 'rde_iqemo', 'rde_measurements', 'rde_medadmin', 'rde_op_diagnosis', 'rde_opa_opcs', 'rde_pathology', 'rde_patient_demographics', 'rde_pc_diagnosis', 'rde_pc_problems', 'rde_pc_procedures', 'rde_pharmacyorders', 'rde_radiology', 'rde_raw_pathology']

map_tables = ['map_address']

max_ig_risk = 3
max_ig_severity = 2
columns_to_exclude = ['ADC_UPDT', 'full_street_address', 'UPRN', 'LATITUDE', 'LONGITUDE', 'match_algorithm', 'match_confidence', 'match_quality']

cohort_sql = f"""
CREATE OR REPLACE VIEW 6_mgmt.cohorts.dac003 AS
WITH cte AS (

    -- Extracted from MILL_SCH_APPT (instead of PI_CDE_OP_ATTENDANCE)
    SELECT DISTINCT PERSON_ID
    FROM 5_projects.dac003_breastonestop.breast_patient_opa_list_202406
    WHERE src_event_dt_tm BETWEEN '2010-01-01' AND '2024-01-25'

    UNION

    -- Extracted from MILL_DIR_DIAGNOSIS, MILL_DIR_PROCEDURE
    SELECT DISTINCT PERSON_ID
    FROM 5_projects.dac003_breastonestop.breast_patient_list_202406
    WHERE src_event_dt_tm BETWEEN '2010-01-01' AND '2024-01-25'

    UNION

    -- Extracted from MILL_DIR_ORDERS
    SELECT DISTINCT PERSON_ID
    FROM 5_projects.dac003_breastonestop.breast_patient_order_list_202406
    WHERE order_dt_tm BETWEEN '2010-01-01' AND '2024-01-25'




)
SELECT DISTINCT PERSON_ID
FROM cte;
"""
spark.sql(cohort_sql)



spark.sql("USE CATALOG 5_projects")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS 5_projects.{project_identifier}")

# Get list of existing views in the target schema
existing_views_df = spark.sql(f"""
    SHOW VIEWS IN 5_projects.{project_identifier}
""")

# Drop all existing views in the schema
if existing_views_df.count() > 0:
    for row in existing_views_df.collect():
        view_name = row.viewName
        spark.sql(f"DROP VIEW IF EXISTS {project_identifier}.{view_name}")
        print(f"Dropped view: {project_identifier}.{view_name}")


def get_columns_with_high_tags(schema_name, table_name):
    """
    Get columns with high ig_risk or ig_severity tags for any schema/table.
    """
    # Get columns with high ig_risk
    high_risk_columns = spark.sql(f"""
        SELECT column_name
        FROM 4_prod.information_schema.column_tags
        WHERE schema_name = '{schema_name}'
        AND table_name = '{table_name}'
        AND tag_name = 'ig_risk'
        AND tag_value > {max_ig_risk}
    """).toPandas()['column_name'].tolist()

    # Get columns with high ig_severity
    high_severity_columns = spark.sql(f"""
        SELECT column_name
        FROM 4_prod.information_schema.column_tags
        WHERE schema_name = '{schema_name}'
        AND table_name = '{table_name}'
        AND tag_name = 'ig_severity'
        AND tag_value > {max_ig_severity}
    """).toPandas()['column_name'].tolist()

    return high_risk_columns + high_severity_columns



def get_columns_except_excluded(catalog, schema, table_name, use_alias=None):
    """
    Get column names excluding specified columns and columns with high tags.
    If use_alias is provided, prepend it to column names (e.g., 'm.column_name').
    """
    # Get all columns from the table
    full_table_path = f"{catalog}.{schema}.{table_name}"
    all_columns = spark.table(full_table_path).columns
    
    # Get columns with high risk or severity tags
    high_tag_columns = get_columns_with_high_tags(schema, table_name)
    
    all_excluded_columns = high_tag_columns + columns_to_exclude
    
    # Filter out excluded columns using set difference
    filtered_columns = list(set(all_columns) - set(all_excluded_columns))
    
    # Sort and format columns
    sorted_columns = sorted(filtered_columns)
    
    if use_alias:
        return ", ".join([f"{use_alias}.{col}" for col in sorted_columns])
    else:
        return ", ".join(sorted_columns)




def find_person_id_column(full_table_path):
    """
    Finds the person ID column in a table given its full path.
    Searches for common variations of the person identifier column name.
    """
    columns = spark.table(full_table_path).columns
    # Comprehensive list of potential person ID column names
    potential_columns = [
        'PERSON_ID', 'person_id', 'Person_ID', 'personid', 
        'PERSONID', 'PersonID', 'participant_id', 'PARENT_ENTITY_ID'
    ]
    
    for col in potential_columns:
        if col in columns:
            return col
            
    # Fallback: fuzzy match for any column containing 'person' and 'id'
    for col in columns:
        col_lower = col.lower()
        if 'person' in col_lower and 'id' in col_lower:
            return col
            
    return None


#----------- RDE Table Processing -----------

for table in rde_tables:
    person_id_col = find_person_id_column(f"4_prod.rde.{table}")
    
    # Get filtered columns (excluding high IG tags and specified columns)
    columns = get_columns_except_excluded('4_prod', 'rde', table, use_alias='s')
    
    if person_id_col:
        view_sql = f"""
        CREATE OR REPLACE VIEW 5_projects.{project_identifier}.{table}
        AS
        SELECT {columns}
        FROM 4_prod.rde.{table} s
        INNER JOIN 6_mgmt.cohorts.{project_identifier} c
        ON s.{person_id_col} = c.PERSON_ID
        """
    else:
        view_sql = f"""
        CREATE OR REPLACE VIEW 5_projects.{project_identifier}.{table}
        AS
        SELECT {columns}
        FROM 4_prod.rde.{table} s
        """
        print(f"Warning: No person ID column found in rde.{table}. Creating view without cohort filtering.")
    
    spark.sql(view_sql)
    print(f"Created view: 5_projects.{project_identifier}.{table}")

#----------- Generic Table Processing Function for bronze, raw, omop, etc. -----------
def process_and_create_views(tables, source_catalog, source_schema, project_identifier):
    """
    Generic function to create cohort-filtered views for a list of tables
    from a specific source location with column exclusion and IG tag filtering.
    """
    for table in tables:
        full_table_path = f"{source_catalog}.{source_schema}.{table}"
        person_id_col = find_person_id_column(full_table_path)
        
        # Get filtered columns (excluding high IG tags and specified columns)
        columns = get_columns_except_excluded(source_catalog, source_schema, table, use_alias='m')
        
        if person_id_col:
            view_sql = f"""
            CREATE OR REPLACE VIEW 5_projects.{project_identifier}.{table}
            AS
            SELECT {columns}
            FROM {full_table_path} m
            INNER JOIN 6_mgmt.cohorts.{project_identifier} c
            ON m.{person_id_col} = c.PERSON_ID
            """
            spark.sql(view_sql)
            print(f"Created view: 5_projects.{project_identifier}.{table}")
        else:
            # Still create view but without cohort filtering if no person ID column
            view_sql = f"""
            CREATE OR REPLACE VIEW 5_projects.{project_identifier}.{table}
            AS
            SELECT {columns}
            FROM {full_table_path} m
            """
            spark.sql(view_sql)
            print(f"Warning: No person ID column found in {full_table_path}. Creating view without cohort filtering.")

#----------- Process map, mill, and omop Tables using the Generic Function -----------

process_and_create_views(map_tables, '4_prod', 'bronze', project_identifier)



#----------- Create Final Schema View -----------

schema_sql = f"""
CREATE OR REPLACE VIEW 5_projects.{project_identifier}.schema AS
SELECT 
    table_name,
    column_name,
    COALESCE(comment, '') as column_comment
FROM 5_projects.information_schema.columns
WHERE table_catalog = '5_projects'
AND table_schema = '{project_identifier}'
AND table_name != 'schema'
ORDER BY table_name, column_name
"""
spark.sql(schema_sql)
print(f"Created schema view: 5_projects.{project_identifier}.schema")
