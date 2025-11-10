# Databricks notebook source
#UTI

project_identifier = 'dac047'

rde_tables = ['rde_all_diagnosis', 'rde_critactivity', 'rde_emergencyd', 'rde_emergency_findings', 'rde_encounter', 'rde_medadmin', 'rde_pathology', 'rde_patient_demographics', 'rde_powerforms', 'rde_radiology', 'rde_blobdataset']

map_tables = []

mill_tables = [] 

omop_tables = [] 

max_ig_risk = 3
max_ig_severity = 2
columns_to_exclude = ['ADC_UPDT']




# Create the cohort view SQL

cohort_sql = """
CREATE OR REPLACE VIEW 6_mgmt.cohorts.dac047 AS
WITH eligible_encounters AS (
  -- Step 1: Filter encounters by type first (cheap filter)
  SELECT 
    ENCNTR_ID,
    PERSON_ID,
    encntr_type_desc,
    ARRIVE_DT_TM,
    DEPART_DT_TM
  FROM 4_prod.bronze.map_encounter
  WHERE encntr_type_desc IN ('Inpatient', 'Emergency Department')
),
eligible_patients AS (
  -- Step 2: Get patients 65+ (cheap calculation, done once per patient)
  SELECT 
    PERSON_ID,
    Date_of_Birth
  FROM 4_prod.rde.rde_patient_demographics
  WHERE Date_of_Birth IS NOT NULL
    AND YEAR(CURRENT_DATE()) - YEAR(Date_of_Birth) >= 65
),
relevant_events AS (
  -- Step 3: Get only clinical events for eligible encounters and patients
  SELECT 
    ce.EVENT_ID,
    ce.PERSON_ID,
    ce.ENCNTR_ID,
    ce.EVENT_END_DT_TM,
    ee.encntr_type_desc,
    ee.ARRIVE_DT_TM,
    ee.DEPART_DT_TM,
    ep.Date_of_Birth
  FROM 4_prod.raw.mill_clinical_event ce
  INNER JOIN eligible_encounters ee
    ON ce.ENCNTR_ID = ee.ENCNTR_ID
    AND ce.PERSON_ID = ee.PERSON_ID
  INNER JOIN eligible_patients ep
    ON ce.PERSON_ID = ep.PERSON_ID
),
age_verified AS (
  -- Step 4: Verify age at actual event time
  SELECT 
    re.*,
    COALESCE(
      re.EVENT_END_DT_TM,
      re.ARRIVE_DT_TM
    ) AS event_date
  FROM relevant_events re
),
age_at_event_filtered AS (
  SELECT *
  FROM age_verified
  WHERE event_date IS NOT NULL
    AND YEAR(event_date) - YEAR(Date_of_Birth) >= 65
),
uti_matched_blobs AS (
  -- Step 5: NOW scan blob text - only for pre-filtered EVENT_IDs
  SELECT 
    bd.EventID,
    bd.PERSON_ID,
    aef.ENCNTR_ID
  FROM 4_prod.rde.rde_blobdataset bd
  INNER JOIN age_at_event_filtered aef
    ON bd.EventID = aef.EVENT_ID
    AND bd.PERSON_ID = aef.PERSON_ID
  WHERE bd.BlobContents IS NOT NULL
    AND (
      LOWER(bd.BlobContents) rlike '(^|[^a-z])urinary tract infection([^a-z]|$)'
      OR LOWER(bd.BlobContents) rlike '(^|[^a-z])pyelonephritis([^a-z]|$)'
      OR LOWER(bd.BlobContents) rlike '(^|[^a-z])cystitis([^a-z]|$)'
    )
)
SELECT DISTINCT
  CAST(PERSON_ID AS BIGINT) AS PERSON_ID,
  CAST(NULL AS STRING) AS subcohort
FROM uti_matched_blobs
WHERE PERSON_ID IS NOT NULL;
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




def find_person_id_column(full_table_path):
    """
    Finds the person ID column in a table given its full path.
    Searches for common variations of the person identifier column name.
    """
    columns = spark.table(full_table_path).columns
    # Comprehensive list of potential person ID column names
    potential_columns = [
        'PERSON_ID', 'person_id', 'Person_ID', 'personid', 
        'PERSONID', 'PersonID', 'participant_id'
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
    columns = get_columns_except_excluded(table)
    person_id_col = find_person_id_column(f"4_prod.rde.{table}")
    
    if person_id_col:
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
        view_sql = f"""
        CREATE OR REPLACE VIEW 5_projects.{project_identifier}.{table}
        AS
        SELECT {columns}
        FROM 4_prod.rde.{table}
        """
        print(f"Warning: No person ID column found in rde.{table}. Creating view without cohort filtering.")
    
    spark.sql(view_sql)
    print(f"Created view: 5_projects.{project_identifier}.{table}")

#----------- Generic Table Processing Function for bronze, raw, omop, etc. -----------

def process_and_create_views(tables, source_catalog, source_schema, project_identifier):
    """
    Generic function to create cohort-filtered views for a list of tables
    from a specific source location. It selects all columns (*).
    """
    for table in tables:
        full_table_path = f"{source_catalog}.{source_schema}.{table}"
        person_id_col = find_person_id_column(full_table_path)
        
        if person_id_col:
            view_sql = f"""
            CREATE OR REPLACE VIEW 5_projects.{project_identifier}.{table}
            AS
            SELECT m.*
            FROM {full_table_path} m
            INNER JOIN 6_mgmt.cohorts.{project_identifier} c
            ON m.{person_id_col} = c.PERSON_ID
            """
            spark.sql(view_sql)
            print(f"Created view: 5_projects.{project_identifier}.{table}")
        else:
            print(f"Warning: No person ID column found in {full_table_path}. Skipping view creation for {table}.")

#----------- Process map, mill, and omop Tables using the Generic Function -----------

process_and_create_views(map_tables, '4_prod', 'bronze', project_identifier)
process_and_create_views(mill_tables, '4_prod', 'raw', project_identifier)
process_and_create_views(omop_tables, '4_prod', 'omop', project_identifier)


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

