# Databricks notebook source
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

project_identifier = 'phar001'

# IG thresholds: columns must have BOTH ig_risk <= max_ig_risk AND ig_severity <= max_ig_severity to be included.
# Untagged columns are excluded unless listed in columns_to_include.
max_ig_risk = 3
max_ig_severity = 2

# Always exclude these columns regardless of tags.
columns_to_exclude = ['ADC_UPDT']

# Always include these columns regardless of tags (e.g. untagged columns you've manually reviewed).
# Format: {'catalog.schema.table': ['col1', 'col2']} or use '*' as table key for all tables.
columns_to_include = {}

# Tables that should bypass IG filtering entirely (e.g. reference/vocabulary tables with no patient data).
# These get SELECT * with no column filtering. Still cohort-filtered if a person_id column exists.
unfiltered_tables = []

# ---------------------------------------------------------------------------
# Table lists
# ---------------------------------------------------------------------------

rde_tables = [
    'rde_aliases', 'rde_all_procedures', 'rde_all_diagnosis', 'rde_allergydetails',
    'rde_apc_diagnosis', 'rde_apc_opcs', 'rde_ariapharmacy', 'rde_cds_apc', 'rde_cds_opa',
    'rde_critactivity', 'rde_critopcs', 'rde_critperiod', 'rde_emergencyd',
    'rde_emergency_findings', 'rde_encounter', 'rde_family_history', 'rde_iqemo',
    'rde_mat_nnu_episodes', 'rde_mat_nnu_exam', 'rde_mat_nnu_nccmds', 'rde_measurements',
    'rde_medadmin', 'rde_mill_powertrials', 'rde_msds_booking', 'rde_msds_carecontact',
    'rde_msds_delivery', 'rde_msds_diagnosis', 'rde_op_diagnosis', 'rde_opa_opcs',
    'rde_pathology', 'rde_patient_demographics', 'rde_pc_diagnosis', 'rde_pc_problems',
    'rde_pc_procedures', 'rde_pharmacyorders', 'rde_powerforms', 'rde_radiology',
    'rde_raw_pathology', 'rde_blobdataset',
]

map_tables = [
    'map_diagnosis', 'map_problem', 'map_procedure', 'map_med_admin',
    'map_coded_events', 'map_encounter', 'map_nomen_events',
    'map_numeric_events', 'map_person',
]

# Tables with standard PERSON_ID columns
mill_tables = [
    'mat_pregnancy', 'mill_allergy', 'mill_encounter', 'mill_episode',
    'mill_orders', 'mill_problem', 'mill_procedure', 'pc_diagnoses', 'pc_problems',
]

omop_tables = [
    'person', 'condition_occurrence', 'death', 'device_exposure',
    'drug_exposure', 'measurement', 'observation', 'procedure_occurrence',
    'visit_occurrence',
]

# Pharos-specific silver tables (from the Pharos pipeline in 4_prod.silver)
pharos_tables = [
    'pharos_person', 'pharos_medical_history', 'pharos_tumour',
    'pharos_imaging', 'pharos_pathology', 'pharos_treatment',
    'pharos_followup', 'pharos_comorbidities',
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cohort

# COMMAND ----------

cohort_sql = f"""
CREATE OR REPLACE VIEW 6_mgmt.cohorts.{project_identifier} AS
WITH matched_aliases AS (
  SELECT DISTINCT 
    l.NHS_NUMBER, 
    TRY_CAST(pa.PERSON_ID AS BIGINT) AS PERSON_ID
  FROM 6_mgmt.cohort_lookup.phar001_lookup l
  JOIN 4_prod.raw.mill_person_alias pa
    ON REGEXP_REPLACE(l.NHS_NUMBER, '[ -]', '') = REGEXP_REPLACE(pa.ALIAS, '[ -]', '')
)
SELECT DISTINCT
  PERSON_ID,
  CAST(NULL AS STRING) AS SUBCOHORT
FROM matched_aliases
WHERE PERSON_ID IS NOT NULL
"""
spark.sql(cohort_sql)
print(f"Created cohort view: 6_mgmt.cohorts.{project_identifier}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Setup

# COMMAND ----------

spark.sql("USE CATALOG 5_projects")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS 5_projects.{project_identifier}")

existing_views_df = spark.sql(f"SHOW VIEWS IN 5_projects.{project_identifier}")
if existing_views_df.count() > 0:
    for row in existing_views_df.collect():
        view_name = row.viewName
        spark.sql(f"DROP VIEW IF EXISTS 5_projects.{project_identifier}.{view_name}")
        print(f"Dropped view: 5_projects.{project_identifier}.{view_name}")

existing_tables_df = spark.sql(f"SHOW TABLES IN 5_projects.{project_identifier}")
existing_views_set = set()
if existing_views_df.count() > 0:
    existing_views_set = {row.viewName for row in existing_views_df.collect()}
for row in existing_tables_df.collect():
    if row.tableName not in existing_views_set and row.tableName != project_identifier:
        spark.sql(f"DROP TABLE IF EXISTS 5_projects.{project_identifier}.{row.tableName}")
        print(f"Dropped table: 5_projects.{project_identifier}.{row.tableName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_safe_columns(catalog, schema, table):
    """
    Return a list of column names that pass IG filtering for the given table.
    """
    full_path = f"{catalog}.{schema}.{table}"
    all_columns = spark.table(full_path).columns

    safe_df = spark.sql(f"""
        SELECT r.column_name
        FROM (
            SELECT column_name, CAST(tag_value AS INT) AS risk_val
            FROM {catalog}.information_schema.column_tags
            WHERE schema_name = '{schema}'
              AND table_name = '{table}'
              AND tag_name = 'ig_risk'
        ) r
        JOIN (
            SELECT column_name, CAST(tag_value AS INT) AS sev_val
            FROM {catalog}.information_schema.column_tags
            WHERE schema_name = '{schema}'
              AND table_name = '{table}'
              AND tag_name = 'ig_severity'
        ) s ON r.column_name = s.column_name
        WHERE r.risk_val <= {max_ig_risk}
          AND s.sev_val <= {max_ig_severity}
    """)
    safe_columns = set(safe_df.toPandas()['column_name'].tolist())

    includes = set(columns_to_include.get(full_path, []))
    includes |= set(columns_to_include.get('*', []))
    safe_columns |= includes

    safe_columns -= set(columns_to_exclude)

    return [c for c in all_columns if c in safe_columns]


def find_person_id_column(catalog, schema, table):
    """Find the person ID column in a table, checking common name variations."""
    full_path = f"{catalog}.{schema}.{table}"
    columns = spark.table(full_path).columns

    for candidate in ['PERSON_ID', 'person_id', 'Person_ID', 'PERSONID', 'PersonID', 'participant_id']:
        if candidate in columns:
            return candidate

    for col in columns:
        if 'person' in col.lower() and 'id' in col.lower():
            return col

    return None


def create_cohort_filtered_view(catalog, schema, table, project_id, column_list=None, alias=None):
    """
    Create a cohort-filtered view in 5_projects.{project_id}.
    """
    full_path = f"{catalog}.{schema}.{table}"
    view_name = alias or table

    if column_list is None:
        column_list = get_safe_columns(catalog, schema, table)
        if not column_list:
            print(f"WARNING: No columns passed IG filter for {full_path}. Skipping.")
            return False

    person_id_col = find_person_id_column(catalog, schema, table)

    if column_list == ['*']:
        columns_sql = "t.*"
    else:
        columns_sql = ", ".join([f"t.`{c}`" for c in column_list])

    if person_id_col:
        view_sql = f"""
        CREATE OR REPLACE VIEW 5_projects.{project_id}.{view_name} AS
        SELECT {columns_sql}
        FROM {full_path} t
        INNER JOIN 6_mgmt.cohorts.{project_id} c
            ON t.{person_id_col} = c.PERSON_ID
        """
    else:
        print(f"Note: No person ID column in {full_path}. Creating view without cohort filtering.")
        view_sql = f"""
        CREATE OR REPLACE VIEW 5_projects.{project_id}.{view_name} AS
        SELECT {columns_sql}
        FROM {full_path} t
        """

    spark.sql(view_sql)
    print(f"Created view: 5_projects.{project_id}.{view_name} ({len(column_list) if column_list != ['*'] else 'all'} columns)")
    return True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Views

# COMMAND ----------

created = []
failed = []

for table in rde_tables:
    try:
        if create_cohort_filtered_view('4_prod', 'rde', table, project_identifier):
            created.append(f"rde.{table}")
    except Exception as e:
        failed.append((f"rde.{table}", str(e)))
        print(f"ERROR: rde.{table}: {e}")

for table in map_tables:
    try:
        if create_cohort_filtered_view('4_prod', 'bronze', table, project_identifier):
            created.append(f"bronze.{table}")
    except Exception as e:
        failed.append((f"bronze.{table}", str(e)))
        print(f"ERROR: bronze.{table}: {e}")

for table in omop_tables:
    try:
        if create_cohort_filtered_view('4_prod', 'omop', table, project_identifier):
            created.append(f"omop.{table}")
    except Exception as e:
        failed.append((f"omop.{table}", str(e)))
        print(f"ERROR: omop.{table}: {e}")

for table in mill_tables:
    try:
        if create_cohort_filtered_view('4_prod', 'raw', table, project_identifier):
            created.append(f"raw.{table}")
    except Exception as e:
        failed.append((f"raw.{table}", str(e)))
        print(f"ERROR: raw.{table}: {e}")

# --- Pharos silver tables (cohort-filtered, no IG filtering - these are project-specific curated tables) ---
for table in pharos_tables:
    try:
        if create_cohort_filtered_view('4_prod', 'silver', table, project_identifier, column_list=['*']):
            created.append(f"silver.{table}")
    except Exception as e:
        failed.append((f"silver.{table}", str(e)))
        print(f"ERROR: silver.{table}: {e}")

for table in unfiltered_tables:
    try:
        if '.' in table:
            parts = table.split('.')
            cat, sch, tbl = parts[0], parts[1], parts[2]
        else:
            cat, sch, tbl = '4_prod', 'omop', table
        if create_cohort_filtered_view(cat, sch, tbl, project_identifier, column_list=['*']):
            created.append(f"{sch}.{tbl}")
    except Exception as e:
        failed.append((f"{table}", str(e)))
        print(f"ERROR: {table}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bespoke Views
# MAGIC Tables with non-standard patient identifiers (PatientID, NHSNumber, MRN) that need custom join logic.

# COMMAND ----------

# --- iqemo tables: joined via iqemo_patient.PatientID → NHSNumber → cohort ---

# iqemo_patient: join on NHSNumber via cohort lookup
spark.sql(f"""
    CREATE OR REPLACE VIEW 5_projects.{project_identifier}.iqemo_patient AS
    SELECT t.*
    FROM 4_prod.raw.iqemo_patient t
    INNER JOIN 6_mgmt.cohort_lookup.phar001_lookup l
        ON REGEXP_REPLACE(CAST(t.NHSNumber AS STRING), '[ -]', '') = REGEXP_REPLACE(l.NHS_NUMBER, '[ -]', '')
""")
created.append("raw.iqemo_patient")
print(f"Created view: 5_projects.{project_identifier}.iqemo_patient")

# iqemo_chemotherapy_course, iqemo_treatment_cycle: join via iqemo_patient.PatientID
for tbl in ['iqemo_chemotherapy_course', 'iqemo_treatment_cycle']:
    spark.sql(f"""
        CREATE OR REPLACE VIEW 5_projects.{project_identifier}.{tbl} AS
        SELECT t.*
        FROM 4_prod.raw.{tbl} t
        INNER JOIN 5_projects.{project_identifier}.iqemo_patient ip
            ON t.PatientID = ip.PatientID
    """)
    created.append(f"raw.{tbl}")
    print(f"Created view: 5_projects.{project_identifier}.{tbl}")

# iqemo_regimen: reference table, filter to regimens used by cohort
spark.sql(f"""
    CREATE OR REPLACE VIEW 5_projects.{project_identifier}.iqemo_regimen AS
    SELECT t.*
    FROM 4_prod.raw.iqemo_regimen t
    WHERE t.RegimenID IN (
        SELECT DISTINCT RegimenID FROM 5_projects.{project_identifier}.iqemo_chemotherapy_course
    )
""")
created.append("raw.iqemo_regimen")
print(f"Created view: 5_projects.{project_identifier}.iqemo_regimen")

# COMMAND ----------

# --- mat_birth and pc_procedures: join on NHS number ---

spark.sql(f"""
    CREATE OR REPLACE VIEW 5_projects.{project_identifier}.mat_birth AS
    SELECT t.*
    FROM 4_prod.raw.mat_birth t
    INNER JOIN 6_mgmt.cohort_lookup.phar001_lookup l
        ON REGEXP_REPLACE(CAST(t.NHS AS STRING), '[ -]', '') = REGEXP_REPLACE(l.NHS_NUMBER, '[ -]', '')
""")
created.append("raw.mat_birth")
print(f"Created view: 5_projects.{project_identifier}.mat_birth")

spark.sql(f"""
    CREATE OR REPLACE VIEW 5_projects.{project_identifier}.pc_procedures AS
    SELECT t.*
    FROM 4_prod.raw.pc_procedures t
    INNER JOIN 6_mgmt.cohort_lookup.phar001_lookup l
        ON REGEXP_REPLACE(CAST(t.NHS_Number AS STRING), '[ -]', '') = REGEXP_REPLACE(l.NHS_NUMBER, '[ -]', '')
""")
created.append("raw.pc_procedures")
print(f"Created view: 5_projects.{project_identifier}.pc_procedures")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Documentation View

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW 5_projects.{project_identifier}.schema AS
SELECT 
    table_name,
    column_name,
    COALESCE(comment, '') AS column_comment
FROM 5_projects.information_schema.columns
WHERE table_catalog = '5_projects'
  AND table_schema = '{project_identifier}'
  AND table_name != 'schema'
ORDER BY table_name, column_name
""")
print(f"Created schema view: 5_projects.{project_identifier}.schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print(f"Project Setup Complete: {project_identifier}")
print("=" * 60)
print(f"\nCohort:  6_mgmt.cohorts.{project_identifier}")
print(f"Lookup:  6_mgmt.cohort_lookup.phar001_lookup")
print(f"Schema:  5_projects.{project_identifier}")
print(f"\nIG Thresholds: ig_risk <= {max_ig_risk}, ig_severity <= {max_ig_severity}")
print(f"Policy: untagged columns are EXCLUDED (treated as max risk)")
print(f"\nViews created: {len(created)}")
for v in created:
    print(f"  + {v}")
if failed:
    print(f"\nFailed: {len(failed)}")
    for t, e in failed:
        print(f"  x {t}: {e}")
