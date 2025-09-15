# Databricks notebook source
#CLAUDE Breast Cancer Biobank Project

project_identifier = 'dac023'

rde_tables = ['rde_aliases', 'rde_all_procedures', 'rde_all_diagnosis', 'rde_allergydetails', 'rde_apc_diagnosis', 'rde_apc_opcs', 'rde_ariapharmacy', 'rde_blobdataset', 'rde_cds_apc', 'rde_cds_opa', 'rde_critactivity', 'rde_critopcs', 'rde_critperiod', 'rde_emergencyd', 'rde_emergency_findings', 'rde_encounter', 'rde_family_history', 'rde_iqemo', 'rde_mat_nnu_episodes', 'rde_mat_nnu_exam', 'rde_mat_nnu_nccmds', 'rde_measurements', 'rde_medadmin', 'rde_mill_powertrials', 'rde_msds_booking', 'rde_msds_carecontact', 'rde_msds_delivery', 'rde_msds_diagnosis', 'rde_op_diagnosis', 'rde_opa_opcs', 'rde_pathology', 'rde_patient_demographics', 'rde_pc_diagnosis', 'rde_pc_problems', 'rde_pc_procedures', 'rde_pharmacyorders', 'rde_powerforms', 'rde_radiology', 'rde_raw_pathology', 'rde_scr_careplan', 'rde_scr_deftreatment', 'rde_scr_demographics', 'rde_scr_diagnosis', 'rde_scr_investigations', 'rde_scr_pathology', 'rde_scr_referrals', 'rde_scr_trackingcomments']

max_ig_risk = 4
max_ig_severity = 4
columns_to_exclude = ['ADC_UPDT']

cohort_sql = f"""
CREATE OR REPLACE VIEW 6_mgmt.cohorts.dac023 AS
WITH matched_aliases AS (
  SELECT DISTINCT 
    l.nhs_number, 
    TRY_CAST(pa.PERSON_ID AS BIGINT) as PERSON_ID  -- Returns NULL for non-numeric
  FROM 6_mgmt.cohort_lookup.dac023_lookup l
  JOIN 4_prod.raw.mill_person_alias pa
  ON REGEXP_REPLACE(l.nhs_number, '[ -]', '') = REGEXP_REPLACE(pa.ALIAS, '[ -]', '')
)
SELECT DISTINCT
  ma.PERSON_ID as PERSON_ID,
  CAST(NULL as STRING) as subcohort
FROM 6_mgmt.cohort_lookup.dac023_lookup l
JOIN matched_aliases ma ON l.nhs_number = ma.nhs_number
WHERE ma.PERSON_ID IS NOT NULL;
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
