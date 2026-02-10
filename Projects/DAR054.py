# Databricks notebook source
# MAGIC %md
# MAGIC DAR054 - Preventing serious complications in patients with haematological malignancies
# MAGIC Partner: Pulse AI (Commercial)
# MAGIC
# MAGIC Purpose: ML model development for predicting complications (neutropenic sepsis, TLS, CRS, etc.)
# MAGIC
# MAGIC Cohort: Patients with specified haematological malignancy ICD-10 codes:
# MAGIC
# MAGIC Category	ICD-10 Range	Description
# MAGIC Lymphoma	C81-C85	Hodgkin and Non-Hodgkin lymphomas
# MAGIC AML	C92.0-C92.9	Acute Myeloid Leukaemia
# MAGIC Multiple Myeloma	C90	Plasma cell disorders
# MAGIC MDS	D46	Myelodysplastic Syndromes
# MAGIC CLL/Lymphoid	C91	CLL and related lymphoid leukaemias
# MAGIC CML	C92.1-C92.2	Chronic Myeloid Leukaemia
# MAGIC SUBCOHORT: Available via subcohort view with:
# MAGIC
# MAGIC CATEGORY: Disease category (Lymphoma, AML, Multiple Myeloma, MDS, CLL/Lymphoid Leukaemia, CML)
# MAGIC TIER: Risk tier (Tier 1 = High early complication risk, Tier 2 = Lower risk)
# MAGIC SUBTYPE: Specific ICD-10 code (e.g., C83.3)

# COMMAND ----------

# Configuration
project_identifier = 'dar054'

# RDE tables to include (33 tables)
rde_tables = [
    # Demographics
    'rde_patient_demographics', 
    # Diagnoses
    'rde_all_diagnosis', 
    'rde_all_problems', 
    # Procedures
    'rde_all_procedures',
    # Encounters
    'rde_encounter', 
    'rde_emergencyd', 
    'rde_emergency_findings',
    # Labs/Measurements
    'rde_pathology', 
    'rde_raw_pathology', 
    'rde_measurements',
    # Medications
    'rde_medadmin', 
    'rde_pharmacyorders', 
    'rde_ariapharmacy', 
    'rde_iqemo',
    # Critical Care
    'rde_critperiod', 
    'rde_critactivity', 
    'rde_critopcs',
    # Radiology
    'rde_radiology',
    # Free Text 
    'rde_blobdataset',
    # Additional clinical data
    'rde_allergydetails',
    'rde_family_history',
    'rde_mill_powertrials',
    # Reference
    'rde_powerforms'
]

# Bronze/map tables to include (from 4_prod.bronze)
map_tables = ['map_patient_journey']

# IG thresholds - STRICT (confirmed for external commercial access)
# max_ig_risk = 3 excludes: MRN, NHS_Number, Postcode, DOB, DOD, raw BlobContents
# max_ig_severity = 2 excludes: unanonymized free text
max_ig_risk = 3
max_ig_severity = 2

columns_to_exclude = ['ADC_UPDT']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Cohort View
# MAGIC
# MAGIC Select patients with haematological malignancy diagnoses (ICD-10 C81-C96).
# MAGIC This view contains only distinct PERSON_IDs for joining to other tables.

# COMMAND ----------

cohort_codes = [
    # Lymphoma (C81-C85) - Tier 1
    'C835', 'C844', 'C833', 'C831', 'C851', 'C859', 'C817',
    # Lymphoma (C81-C85) - Tier 2
    'C823', 'C830', 'C822', 'C821', 'C820', 'C840', 
    'C810', 'C811', 'C812', 'C813', 'C814',
    # AML (C92) - Tier 1
    'C924', 'C926', 'C925', 'C920',
    # AML (C92) - Tier 2
    'C928', 'C929', 'C923', 'C927',
    # Multiple Myeloma (C90) - Tier 1
    'C902', 'C900', 'C909',
    # Multiple Myeloma (C90) - Tier 2
    'C901', 'C903',
    # MDS (D46) - Tier 1
    'D462', 'D469',
    # MDS (D46) - Tier 2
    'D460', 'D461', 'D464',
    # CLL/Lymphoid (C91) - Tier 1
    'C912', 'C913', 'C910',
    # CLL/Lymphoid (C91) - Tier 2
    'C911', 'C914', 'C915', 'C917', 'C919',
    # CML (C92.1-C92.2) - Tier 1
    'C922',
    # CML (C92.1-C92.2) - Tier 2
    'C921'
]

# Create SQL-friendly list
cohort_codes_sql = ", ".join([f"'{code}'" for code in cohort_codes])

# Create cohort view in 6_mgmt.cohorts (distinct patients only)
cohort_sql = f"""
CREATE OR REPLACE VIEW 6_mgmt.cohorts.{project_identifier} AS
SELECT DISTINCT Person_ID AS PERSON_ID
FROM 4_prod.bronze.map_diagnosis
WHERE (
    SUBSTRING(REPLACE(ICD10_CODE, '.', '') , 1, 5) IN ({cohort_codes_sql})
    OR SUBSTRING(REPLACE(ICD10_CODE, '.', '') , 1, 4) IN ({cohort_codes_sql})
)
"""

spark.sql(cohort_sql)
print(f"Created cohort view: 6_mgmt.cohorts.{project_identifier}")

# Display cohort statistics
cohort_stats = spark.sql(f"""
    SELECT COUNT(DISTINCT PERSON_ID) as total_patients
    FROM 6_mgmt.cohorts.{project_identifier}
""")
display(cohort_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Project Schema and Filtered Views
# MAGIC
# MAGIC Create views in 5_projects.dar054 that:
# MAGIC 1. Filter to cohort patients only
# MAGIC 2. Exclude columns with IG risk > 3 or IG severity > 2

# COMMAND ----------

# Create project schema
spark.sql("USE CATALOG 5_projects")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS 5_projects.{project_identifier}")

# Drop all existing views in the schema
existing_views_df = spark.sql(f"SHOW VIEWS IN 5_projects.{project_identifier}")

if existing_views_df.count() > 0:
    for row in existing_views_df.collect():
        view_name = row.viewName
        spark.sql(f"DROP VIEW IF EXISTS {project_identifier}.{view_name}")
        print(f"Dropped view: {project_identifier}.{view_name}")

# COMMAND ----------

def get_columns_with_high_tags(table_name):
    """Get columns that exceed IG risk or severity thresholds."""
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

    return high_risk_columns + high_severity_columns


def get_columns_except_excluded(table_name):
    """Get column names excluding specified columns and columns with high IG tags."""
    # Get all columns from the table
    all_columns = spark.table(f"4_prod.rde.{table_name}").columns
    
    # Get columns with high risk or severity tags
    high_tag_columns = get_columns_with_high_tags(table_name)

    all_excluded_columns = high_tag_columns + columns_to_exclude
    
    # Filter out excluded columns using set difference
    filtered_columns = list(set(all_columns) - set(all_excluded_columns))
    
    # Convert back to sorted list and join
    return ", ".join(sorted(filtered_columns))


def get_person_id_column(table_name):
    """Determine the person ID column name for a table."""
    columns = spark.table(f"4_prod.rde.{table_name}").columns
    if 'PERSON_ID' in columns:
        return 'PERSON_ID'
    elif 'PERSONID' in columns:
        return 'PERSONID'
    elif 'Person_ID' in columns:
        return 'Person_ID'
    else:
        return None


def find_person_id_column(full_table_path):
    """
    Find the person ID column in a table given its full path.
    Searches for common variations of the person identifier column name.
    """
    columns = spark.table(full_table_path).columns
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


def process_and_create_views(tables, source_catalog, source_schema, project_id):
    """
    Generic function to create cohort-filtered views for a list of tables
    from a specific source location. Selects all columns (*).
    """
    for table in tables:
        full_table_path = f"{source_catalog}.{source_schema}.{table}"
        person_id_col = find_person_id_column(full_table_path)
        
        if person_id_col:
            view_sql = f"""
            CREATE OR REPLACE VIEW 5_projects.{project_id}.{table}
            AS
            SELECT m.*
            FROM {full_table_path} m
            INNER JOIN 6_mgmt.cohorts.{project_id} c
            ON m.{person_id_col} = c.PERSON_ID
            """
            spark.sql(view_sql)
            created_views.append(table)
            print(f"Created view: 5_projects.{project_id}.{table}")
        else:
            print(f"Warning: No person ID column found in {full_table_path}. Skipping view creation for {table}.")

# COMMAND ----------

# Create filtered views for each RDE table
created_views = []
failed_tables = []

for table in rde_tables:
    try:
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
            SELECT DISTINCT s.*
            FROM source_data s
            INNER JOIN 6_mgmt.cohorts.{project_identifier} c
            ON s.{person_id_col} = c.PERSON_ID
            """
        else:
            # If no person ID column exists, create view without filtering (reference tables)
            view_sql = f"""
            CREATE OR REPLACE VIEW 5_projects.{project_identifier}.{table}
            AS
            SELECT {columns}
            FROM 4_prod.rde.{table}
            """
            print(f"Note: No person ID column found in {table}. Creating view without cohort filtering (reference table).")
        
        # Execute the SQL
        spark.sql(view_sql)
        created_views.append(table)
        print(f"Created view: 5_projects.{project_identifier}.{table}")
        
    except Exception as e:
        failed_tables.append((table, str(e)))
        print(f"ERROR creating view for {table}: {e}")

# COMMAND ----------

# Process map tables from bronze schema
process_and_create_views(map_tables, '4_prod', 'bronze', project_identifier)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Subcohort Lookup View
# MAGIC
# MAGIC This view maps each patient to their haematological malignancy ICD-10 categories.
# MAGIC Patients may have multiple subcohorts if they have multiple qualifying diagnoses.

# COMMAND ----------

subcohort_sql = f"""
CREATE OR REPLACE VIEW 5_projects.{project_identifier}.subcohort AS
WITH coded_diagnoses AS (
    SELECT
        PERSON_ID,
        REPLACE(ICD10_CODE, '.', '') AS Diagnosis_code,
        SUBSTRING(Diagnosis_code, 1, 5) AS code_5char,
        SUBSTRING(Diagnosis_code, 1, 4) AS code_4char,
        DIAG_DT_TM AS DIAGNOSIS_DATE
    FROM 4_prod.bronze.map_diagnosis
    WHERE Person_ID IN (SELECT PERSON_ID FROM 6_mgmt.cohorts.{project_identifier})
),
categorized AS (
    SELECT
        PERSON_ID,
        Diagnosis_code,
        DIAGNOSIS_DATE,
        -- Determine SUBTYPE (the specific code)
        CASE
            WHEN code_5char IN ('C835','C844','C833','C831','C851','C859','C817',
                                'C823','C830','C822','C821','C820','C840',
                                'C810','C811','C812','C813','C814') THEN code_5char
            WHEN code_5char IN ('C924','C926','C925','C920','C928','C929','C923','C927','C922','C921') THEN code_5char
            WHEN code_5char IN ('C902','C900','C909','C901','C903') THEN code_5char
            WHEN code_5char IN ('D462','D469','D460','D461','D464') THEN code_5char
            WHEN code_5char IN ('C912','C913','C910','C911','C914','C915','C917','C919') THEN code_5char
            WHEN code_4char IN ('C835','C844','C833','C831','C851','C859','C817',
                                'C823','C830','C822','C821','C820','C840',
                                'C810','C811','C812','C813','C814') THEN code_4char
            WHEN code_4char IN ('C924','C926','C925','C920','C928','C929','C923','C927','C922','C921') THEN code_4char
            WHEN code_4char IN ('C902','C900','C909','C901','C903') THEN code_4char
            WHEN code_4char IN ('D462','D469','D460','D461','D464') THEN code_4char
            WHEN code_4char IN ('C912','C913','C910','C911','C914','C915','C917','C919') THEN code_4char
            ELSE NULL
        END AS SUBTYPE,
        -- Determine CATEGORY
        CASE
            WHEN code_5char LIKE 'C81%' OR code_5char LIKE 'C82%' OR code_5char LIKE 'C83%' 
                 OR code_5char LIKE 'C84%' OR code_5char LIKE 'C85%'
                 OR code_4char LIKE 'C81%' OR code_4char LIKE 'C82%' OR code_4char LIKE 'C83%' 
                 OR code_4char LIKE 'C84%' OR code_4char LIKE 'C85%' THEN 'Lymphoma'
            WHEN (code_5char IN ('C924','C926','C925','C920','C928','C929','C923','C927')
                  OR code_4char IN ('C924','C926','C925','C920','C928','C929','C923','C927')) THEN 'AML'
            WHEN (code_5char IN ('C921','C922') OR code_4char IN ('C921','C922')) THEN 'CML'
            WHEN code_5char LIKE 'C90%' OR code_4char LIKE 'C90%' THEN 'Multiple Myeloma'
            WHEN code_5char LIKE 'D46%' OR code_4char LIKE 'D46%' THEN 'MDS'
            WHEN code_5char LIKE 'C91%' OR code_4char LIKE 'C91%' THEN 'CLL/Lymphoid Leukaemia'
            ELSE NULL
        END AS CATEGORY,
        -- Determine TIER
        CASE
            -- Lymphoma Tier 1
            WHEN code_5char IN ('C835','C844','C833','C831','C851','C859','C817')
                 OR code_4char IN ('C835','C844','C833','C831','C851','C859','C817') THEN 'Tier 1'
            -- Lymphoma Tier 2
            WHEN code_5char IN ('C823','C830','C822','C821','C820','C840','C810','C811','C812','C813','C814')
                 OR code_4char IN ('C823','C830','C822','C821','C820','C840','C810','C811','C812','C813','C814') THEN 'Tier 2'
            -- AML Tier 1
            WHEN code_5char IN ('C924','C926','C925','C920')
                 OR code_4char IN ('C924','C926','C925','C920') THEN 'Tier 1'
            -- AML Tier 2
            WHEN code_5char IN ('C928','C929','C923','C927')
                 OR code_4char IN ('C928','C929','C923','C927') THEN 'Tier 2'
            -- Multiple Myeloma Tier 1
            WHEN code_5char IN ('C902','C900','C909')
                 OR code_4char IN ('C902','C900','C909') THEN 'Tier 1'
            -- Multiple Myeloma Tier 2
            WHEN code_5char IN ('C901','C903')
                 OR code_4char IN ('C901','C903') THEN 'Tier 2'
            -- MDS Tier 1
            WHEN code_5char IN ('D462','D469')
                 OR code_4char IN ('D462','D469') THEN 'Tier 1'
            -- MDS Tier 2
            WHEN code_5char IN ('D460','D461','D464')
                 OR code_4char IN ('D460','D461','D464') THEN 'Tier 2'
            -- CLL/Lymphoid Tier 1
            WHEN code_5char IN ('C912','C913','C910')
                 OR code_4char IN ('C912','C913','C910') THEN 'Tier 1'
            -- CLL/Lymphoid Tier 2
            WHEN code_5char IN ('C911','C914','C915','C917','C919')
                 OR code_4char IN ('C911','C914','C915','C917','C919') THEN 'Tier 2'
            -- CML Tier 1
            WHEN code_5char = 'C922' OR code_4char = 'C922' THEN 'Tier 1'
            -- CML Tier 2
            WHEN code_5char = 'C921' OR code_4char = 'C921' THEN 'Tier 2'
            ELSE NULL
        END AS TIER
    FROM coded_diagnoses
)
SELECT
    PERSON_ID,
    CATEGORY,
    TIER,
    SUBTYPE,
    Diagnosis_code AS DIAGNOSIS_CODE,
    MIN(DIAGNOSIS_DATE) AS FIRST_DIAGNOSIS_DATE
FROM categorized
WHERE SUBTYPE IS NOT NULL
GROUP BY PERSON_ID, CATEGORY, TIER, SUBTYPE, Diagnosis_code
ORDER BY PERSON_ID, CATEGORY, TIER, FIRST_DIAGNOSIS_DATE
"""
spark.sql(subcohort_sql)
print(f"Created subcohort view: 5_projects.{project_identifier}.subcohort")

# Display subcohort breakdown by category and tier
subcohort_breakdown = spark.sql(f"""
    SELECT 
        CATEGORY,
        TIER,
        SUBTYPE,
        COUNT(DISTINCT PERSON_ID) as patient_count
    FROM 5_projects.{project_identifier}.subcohort
    GROUP BY CATEGORY, TIER, SUBTYPE
    ORDER BY CATEGORY, TIER, SUBTYPE
""")
display(subcohort_breakdown)

# COMMAND ----------

# Create schema view for documentation
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Print summary
print("=" * 60)
print(f"DAR054 Project Setup Complete")
print("=" * 60)
print(f"\nCohort view: 6_mgmt.cohorts.{project_identifier}")
print(f"Project schema: 5_projects.{project_identifier}")
print(f"\nIG Thresholds Applied:")
print(f"  - max_ig_risk = {max_ig_risk}")
print(f"  - max_ig_severity = {max_ig_severity}")
print(f"\nViews created: {len(created_views)} data views + subcohort + schema")
if failed_tables:
    print(f"\nFailed tables: {len(failed_tables)}")
    for table, error in failed_tables:
        print(f"  - {table}: {error}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification Queries

# COMMAND ----------

# Verify cohort count
print("1. Cohort size:")
display(spark.sql(f"SELECT COUNT(DISTINCT PERSON_ID) as cohort_size FROM 6_mgmt.cohorts.{project_identifier}"))

# COMMAND ----------

# Verify views created
print("2. Views in project schema:")
display(spark.sql(f"SHOW VIEWS IN 5_projects.{project_identifier}"))

# COMMAND ----------

# Verify subcohort view
print("4. Subcohort distribution:")
display(spark.sql(f"""
    SELECT 
        SUBTYPE,
        COUNT(DISTINCT PERSON_ID) as patients,
        COUNT(*) as diagnosis_records
    FROM 5_projects.{project_identifier}.subcohort
    GROUP BY SUBTYPE
    ORDER BY SUBTYPE
"""))
