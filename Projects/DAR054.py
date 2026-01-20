# Databricks notebook source
# MAGIC %md
# MAGIC # DAR054 - Preventing serious complications in patients with haematological malignancies
# MAGIC
# MAGIC **Partner:** Pulse AI (Commercial)
# MAGIC
# MAGIC **Purpose:** ML model development for predicting complications (neutropenic sepsis, TLS, CRS, etc.)
# MAGIC
# MAGIC **Cohort:** Patients with ICD-10 codes C81-C96 (haematological malignancies)
# MAGIC - C81: Hodgkin lymphoma
# MAGIC - C82: Follicular lymphoma
# MAGIC - C83: Non-follicular lymphoma
# MAGIC - C84: Mature T/NK-cell lymphomas
# MAGIC - C85: Other non-Hodgkin lymphoma
# MAGIC - C86: Other T/NK-cell lymphomas
# MAGIC - C88: Malignant immunoproliferative diseases
# MAGIC - C90: Multiple myeloma and plasmacytomas
# MAGIC - C91: Lymphoid leukaemia
# MAGIC - C92: Myeloid leukaemia
# MAGIC - C93: Monocytic leukaemia
# MAGIC - C94: Other leukaemias
# MAGIC - C95: Leukaemia unspecified
# MAGIC - C96: Other malignant neoplasms of lymphoid/haematopoietic tissue
# MAGIC
# MAGIC **SUBCOHORT:** ICD-10 category (C81, C82, etc.) for disease subtyping - available via `subcohort` view
# MAGIC
# MAGIC ---
# MAGIC **Note:** This notebook should be moved to `/Workspace/Shared/ADC-DB/Projects/DAR054` for production use.

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
    # Free Text (NLP)
    'rde_blobdataset',
    # Cancer Registry
    'rde_scr_diagnosis', 
    'rde_scr_careplan', 
    'rde_scr_deftreatment', 
    'rde_scr_investigations', 
    'rde_scr_pathology', 
    'rde_scr_referrals',
    'rde_scr_demographics',
    'rde_scr_trackingcomments',
    # Additional clinical data
    'rde_allergydetails',
    'rde_family_history',
    'rde_mill_powertrials',
    # Reference
    'rde_powerforms'
]

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

# Create cohort view in 6_mgmt.cohorts (distinct patients only)
cohort_sql = f"""
CREATE OR REPLACE VIEW 6_mgmt.cohorts.{project_identifier} AS
SELECT DISTINCT Person_ID AS PERSON_ID
FROM 4_prod.rde.rde_all_diagnosis
WHERE Catalogue = 'ICD10WHO'
AND Diagnosis_code RLIKE '^C(8[1-6]|88|9[0-6])'
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

# MAGIC %md
# MAGIC ## Create Subcohort Lookup View
# MAGIC
# MAGIC This view maps each patient to their haematological malignancy ICD-10 categories.
# MAGIC Patients may have multiple subcohorts if they have multiple qualifying diagnoses.

# COMMAND ----------

# Create subcohort lookup view
subcohort_sql = f"""
CREATE OR REPLACE VIEW 5_projects.{project_identifier}.subcohort AS
SELECT
    Person_ID AS PERSON_ID,
    SUBSTRING(Diagnosis_code, 1, 3) AS SUBCOHORT,
    Diagnosis_code AS DIAGNOSIS_CODE,
    Code_text AS DIAGNOSIS_DESCRIPTION,
    MIN(Diagnosis_date) AS FIRST_DIAGNOSIS_DATE
FROM 4_prod.rde.rde_all_diagnosis
WHERE Catalogue = 'ICD10WHO'
AND Diagnosis_code RLIKE '^C(8[1-6]|88|9[0-6])'
AND Person_ID IN (SELECT PERSON_ID FROM 6_mgmt.cohorts.{project_identifier})
GROUP BY Person_ID, SUBSTRING(Diagnosis_code, 1, 3), Diagnosis_code, Code_text
ORDER BY Person_ID, FIRST_DIAGNOSIS_DATE
"""
spark.sql(subcohort_sql)
print(f"Created subcohort view: 5_projects.{project_identifier}.subcohort")

# Display subcohort breakdown
subcohort_breakdown = spark.sql(f"""
    SELECT 
        SUBCOHORT as icd10_category,
        COUNT(DISTINCT PERSON_ID) as patient_count
    FROM 5_projects.{project_identifier}.subcohort
    GROUP BY SUBCOHORT
    ORDER BY SUBCOHORT
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

# Verify IG filtering on demographics (check excluded columns)
print("3. Demographics columns (after IG filtering):")
display(spark.sql(f"DESCRIBE 5_projects.{project_identifier}.rde_patient_demographics"))

# COMMAND ----------

# Verify subcohort view
print("4. Subcohort distribution:")
display(spark.sql(f"""
    SELECT 
        SUBCOHORT,
        COUNT(DISTINCT PERSON_ID) as patients,
        COUNT(*) as diagnosis_records
    FROM 5_projects.{project_identifier}.subcohort
    GROUP BY SUBCOHORT
    ORDER BY SUBCOHORT
"""))

# COMMAND ----------

# Sample data availability
print("5. Sample data counts:")
data_counts = spark.sql(f"""
    SELECT 
        'Demographics' as data_type, COUNT(*) as record_count 
    FROM 5_projects.{project_identifier}.rde_patient_demographics
    UNION ALL
    SELECT 'Diagnoses', COUNT(*) FROM 5_projects.{project_identifier}.rde_all_diagnosis
    UNION ALL
    SELECT 'Encounters', COUNT(*) FROM 5_projects.{project_identifier}.rde_encounter
    UNION ALL
    SELECT 'Pathology', COUNT(*) FROM 5_projects.{project_identifier}.rde_pathology
    UNION ALL
    SELECT 'Allergies', COUNT(*) FROM 5_projects.{project_identifier}.rde_allergydetails
    UNION ALL
    SELECT 'Clinical Trials', COUNT(*) FROM 5_projects.{project_identifier}.rde_mill_powertrials
""")
display(data_counts)
