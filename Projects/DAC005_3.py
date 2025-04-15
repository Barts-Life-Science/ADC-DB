# Databricks notebook source
#Bloodcounts Cohort 3, ferritin

project_identifier = 'dar005_3'

rde_tables = ['rde_all_procedures', 'rde_allergydetails', 'rde_apc_diagnosis', 'rde_apc_opcs', 'rde_cds_apc', 'rde_cds_opa', 'rde_critactivity', 'rde_critopcs', 'rde_critperiod', 'rde_emergencyd', 'rde_family_history', 'rde_measurements', 'rde_medadmin', 'rde_op_diagnosis', 'rde_opa_opcs', 'rde_patient_demographics', 'rde_pc_diagnosis', 'rde_pc_problems', 'rde_pc_procedures', 'rde_pharmacyorders', 'rde_pathology', 'rde_raw_pathology', 'rde_all_diagnosis', 'rde_msds_booking']

bloodcount_tables = ['action_message', 'ana_internal1', 'ana_internal2', 'bb', 'bb_f', 'cal', 'cls_param', 'dst_data', 'dst_data_2', 'err', 'flag_sus2', 'flagging', 'flg_gate', 'giveup', 'hsa', 'hsa_f', 'ig', 'ipu_csv1', 'ipu_csv2', 'jo_aflas', 'jo_aflas01', 'jo_aflas03', 'jo_aflas04', 'lyact', 'mix_issue', 'mt_data', 'neonew_hpc', 'neuteoissue', 'otherinfo', 'outputdata', 'plt_abn_sus', 'plt_clumps_wnr_gate', 'plt_clumps_wnr_v17_18', 'plt_clumps_wnr_v21', 'plt_dst_raw_data', 'plt_swt', 'rbc_abn_sus', 'rbc_dst_raw_data', 'reportable', 'reportable_f', 'research', 'research_f', 'sampling_plt', 'sampling_pltf', 'sampling_rbc', 'sampling_ret', 'sampling_wdf', 'sampling_wdf_2times', 'sampling_wnr', 'sampling_wpc', 'sampling_wpc_2times', 'sampling_wpc_3times', 'sampling_wpc_4times', 'service_in', 'service_out', 'servicesettinglog', 't_data', 'wbc_abn_sct_bf', 'wbc_abn_sus', 'wdf_low_sfl', 'wnr_aggl', 'wp_reana', 'xn_sample', 'sct_ret', 'sct_wdf', 'sct_wnr']

map_tables = ['map_diagnosis', 'map_problem', 'map_procedure', 'map_med_amin']

max_ig_risk = 3
max_ig_severity = 2
columns_to_exclude = ['ADC_UPDT']

# Function to get the sample number column for xn_sample
def get_xn_sample_column():
    columns = spark.table("4_prod.ancil_bloodcounts.xn_sample").columns
    if 'sample_no_' in columns:
        return 'sample_no_'
    elif 'sample_no' in columns:
        return 'sample_no'
    elif 'sampleid' in columns:
        return 'sampleid'
    else:
        return None

# Function to get the date column for xn_sample
def get_xn_sample_date_column():
    columns = spark.table("4_prod.ancil_bloodcounts.xn_sample").columns
    if 'date' in columns:
        return 'date'
    elif 'Date' in columns:
        return 'Date'
    elif 'DATE' in columns:
        return 'DATE'
    else:
        # Try to find any column with 'date' in its name
        for col in columns:
            if 'date' in col.lower():
                return col
        return None

# Get the correct column names for xn_sample
xn_sample_col = get_xn_sample_column()
xn_date_col = get_xn_sample_date_column()

# Create the cohort view SQL with ferritin filter
cohort_sql = f"""
CREATE OR REPLACE VIEW 6_mgmt.cohorts.{project_identifier} AS
WITH xn_samples AS (
    SELECT 
        xs.{xn_sample_col} AS lab_no,
        TO_DATE(xs.{xn_date_col}, 'yyyy/MM/dd') AS sample_date,
        rp.PERSON_ID
    FROM 
        4_prod.ancil_bloodcounts.xn_sample xs
    JOIN 
        4_prod.rde.rde_raw_pathology rp ON xs.{xn_sample_col} = rp.LabNo
)
SELECT DISTINCT
    xs.PERSON_ID,
    NULL AS subcohort
FROM 
    xn_samples xs
WHERE 
    EXISTS (
        SELECT 1
        FROM 4_prod.rde.rde_raw_pathology rp
        WHERE rp.PERSON_ID = xs.PERSON_ID
        AND rp.TFCCode IN ('FER', 'FER2', 'FERC')
AND (
            DATEDIFF(
                COALESCE(rp.SampleDt, rp.ReportDate),
                xs.sample_date
            ) BETWEEN 0 AND 14
        )
    )
"""

# Execute the SQL to create the view
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

def get_sample_number_column(table_name):
    columns = spark.table(f"4_prod.ancil_bloodcounts.{table_name}").columns
    if 'sample_no_' in columns:
        return 'sample_no_'
    elif 'sample_no' in columns:
        return 'sample_no'
    elif 'sampleid' in columns:
        return 'sampleid'
    else:
        return None

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

# Process bloodcount tables
for table in bloodcount_tables:
    # Get the appropriate sample number column name
    sample_no_col = get_sample_number_column(table)
    
    if sample_no_col:
        # Create view SQL with cohort filtering through rde_raw_pathology
        view_sql = f"""
        CREATE OR REPLACE VIEW 5_projects.{project_identifier}.{table}
        AS
        SELECT b.*
        FROM 4_prod.ancil_bloodcounts.{table} b
        INNER JOIN 4_prod.rde.rde_raw_pathology p ON b.{sample_no_col} = p.LabNo
        INNER JOIN 6_mgmt.cohorts.{project_identifier} c ON p.PERSON_ID = c.PERSON_ID
        """
    
    # Execute the SQL
    spark.sql(view_sql)
    
    print(f"Created view: 5_projects.{project_identifier}.{table}")



def get_bronze_person_id_column(table_name):
    columns = spark.table(f"4_prod.bronze.{table_name}").columns
    potential_columns = ['person_id', 'PERSON_ID', 'personid', 'PersonID', 'PERSONID', 'Person_ID']
    
    for col in potential_columns:
        if col in columns:
            return col
    
    # If no exact match, try to find any column with 'person' and 'id' in its name
    for col in columns:
        if 'person' in col.lower() and 'id' in col.lower():
            return col
    
    return None

# Process bronze map tables
for table in map_tables:
    # Get the appropriate person ID column name
    person_id_col = get_bronze_person_id_column(table)
    
    if person_id_col:
        # Create view SQL with cohort filtering
        view_sql = f"""
        CREATE OR REPLACE VIEW 5_projects.{project_identifier}.{table}
        AS
        SELECT m.*
        FROM 4_prod.bronze.{table} m
        INNER JOIN 6_mgmt.cohorts.{project_identifier} c
        ON m.{person_id_col} = c.PERSON_ID
        """
        
        # Execute the SQL
        spark.sql(view_sql)
        
        print(f"Created view: 5_projects.{project_identifier}.{table}")
    else:
        print(f"Warning: No person ID column found in {table}. Skipping view creation.")


def get_bronze_person_id_column(table_name):
    columns = spark.table(f"4_prod.bronze.{table_name}").columns
    potential_columns = ['person_id', 'PERSON_ID', 'personid', 'PersonID', 'PERSONID', 'Person_ID']
    
    for col in potential_columns:
        if col in columns:
            return col
    
    # If no exact match, try to find any column with 'person' and 'id' in its name
    for col in columns:
        if 'person' in col.lower() and 'id' in col.lower():
            return col
    
    return None

# Process bronze map tables
for table in map_tables:
    # Get the appropriate person ID column name
    person_id_col = get_bronze_person_id_column(table)
    
    if person_id_col:
        # Create view SQL with cohort filtering
        view_sql = f"""
        CREATE OR REPLACE VIEW 5_projects.{project_identifier}.{table}
        AS
        SELECT m.*
        FROM 4_prod.bronze.{table} m
        INNER JOIN 6_mgmt.cohorts.{project_identifier} c
        ON m.{person_id_col} = c.PERSON_ID
        """
        
        # Execute the SQL
        spark.sql(view_sql)
        
        print(f"Created view: 5_projects.{project_identifier}.{table}")
    else:
        print(f"Warning: No person ID column found in {table}. Skipping view creation.")


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
