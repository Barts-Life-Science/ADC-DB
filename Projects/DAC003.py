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



# COMMAND ----------

#ADAMS Project

project_identifier = 'dac003'

rde_tables = ['rde_aliases', 'rde_all_procedures', 'rde_allergydetails', 'rde_apc_diagnosis', 'rde_apc_opcs', 'rde_ariapharmacy', 'rde_blobdataset', 'rde_cds_apc', 'rde_cds_opa', 'rde_critactivity', 'rde_critopcs', 'rde_critperiod', 'rde_emergencyd', 'rde_encounter', 'rde_family_history', 'rde_iqemo', 'rde_measurements', 'rde_medadmin', 'rde_op_diagnosis', 'rde_opa_opcs', 'rde_pathology', 'rde_patient_demographics', 'rde_pc_diagnosis', 'rde_pc_problems', 'rde_pc_procedures', 'rde_pharmacyorders', 'rde_radiology', 'rde_raw_pathology']

max_ig_risk = 4
max_ig_severity = 3
columns_to_exclude = ['ADC_UPDT']

cohort_sql = f"""
CREATE OR REPLACE VIEW 6_mgmt.cohorts.dac003 AS
WITH matched_aliases AS (
  SELECT DISTINCT l.Identifier, pa.PERSON_ID
  FROM 6_mgmt.cohort_lookup.dac003_lookup l
  JOIN 4_prod.raw.mill_person_alias pa
  ON REGEXP_REPLACE(l.Identifier, '[ -]', '') = REGEXP_REPLACE(pa.ALIAS, '[ -]', '')
),
matched_persons AS (
  SELECT DISTINCT 
    l.Identifier,
    p.PERSON_ID
  FROM 6_mgmt.cohort_lookup.dac003_lookup l
  JOIN 4_prod.raw.mill_person p
  ON UPPER(l.Name) = CONCAT(p.NAME_FIRST_KEY, ' ', p.NAME_LAST_KEY)
  AND TO_DATE(l.dob, 'dd/MM/yyyy') = DATE(p.BIRTH_DT_TM)
)
SELECT DISTINCT
  COALESCE(ma.PERSON_ID, mp.PERSON_ID) as PERSON_ID,
  CAST(NULL as STRING) as subcohort
FROM 6_mgmt.cohort_lookup.dac003_lookup l
LEFT JOIN matched_aliases ma ON l.Identifier = ma.Identifier
LEFT JOIN matched_persons mp ON l.Identifier = mp.Identifier
WHERE COALESCE(ma.PERSON_ID, mp.PERSON_ID) IS NOT NULL;
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

def get_columns_with_high_tags(table_name):
    # Get columns with high ig_risk
    high_risk_columns = spark.sql(f"""
        SELECT column_name
        FROM 4_prod.information_schema.column_tags
        WHERE schema_name = 'rde'
        AND table_name = '{table_name}'
        AND tag_name = 'ig_risk'
        AND tag_value > {max_ig_risk}
    """).toPandas()['column_name'].tolist()

    # Get columns with high ig_severity
    high_severity_columns = spark.sql(f"""
        SELECT column_name
        FROM 4_prod.information_schema.column_tags
        WHERE schema_name = 'rde'
        AND table_name = '{table_name}'
        AND tag_name = 'ig_severity'
        AND tag_value > {max_ig_severity}
    """).toPandas()['column_name'].tolist()

    # Convert the combined list to a set before returning
    return high_risk_columns + high_severity_columns

# Function to get column names excluding specified columns and columns with high tags
def get_columns_except_excluded(table_name):
    # Get all columns from the table
    all_columns = spark.table(f"4_prod.rde.{table_name}").columns
    
    # Get columns with high risk or severity tags
    high_tag_columns = get_columns_with_high_tags(table_name)

    all_exluded_columns = high_tag_columns + columns_to_exclude
    
    # Filter out excluded columns using set difference
    filtered_columns = list(set(all_columns) - set(all_exluded_columns))
    
    # Convert back to sorted list and join
    return ", ".join(sorted(filtered_columns))

# Function to determine the person ID column name
def get_person_id_column(table_name):
    columns = spark.table(f"4_prod.rde.{table_name}").columns
    if 'PERSON_ID' in columns:
        return 'PERSON_ID'
    elif 'PERSONID' in columns:
        return 'PERSONID'
    elif 'Person_ID' in columns:
        return 'Person_ID'
    else:
        return None

for table in rde_tables:
    # Get columns string
    columns = get_columns_except_excluded(table)
    
    # Get the appropriate person ID column name
    person_id_col = get_person_id_column(table)
    
    if person_id_col:
        # Create view SQL with cohort filtering
        view_sql = f"""
        CREATE OR REPLACE VIEW 5_projects.{project_identifier}.{table}
        AS
        WITH source_data AS (
            SELECT {columns}
            FROM 4_prod.rde.{table}
        )
        SELECT s.*
        FROM source_data s
        INNER JOIN 6_mgmt.cohorts.{project_identifier} c
        ON s.{person_id_col} = c.PERSON_ID
        """
    else:
        # If no person ID column exists, create view without filtering
        view_sql = f"""
        CREATE OR REPLACE VIEW 5_projects.{project_identifier}.{table}
        AS
        SELECT {columns}
        FROM 4_prod.rde.{table}
        """
        print(f"Warning: No person ID column found in {table}. Creating view without cohort filtering.")
    
    # Execute the SQL
    spark.sql(view_sql)
    
    print(f"Created view: 5_projects.{project_identifier}.{table}")


# Create schema view
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
