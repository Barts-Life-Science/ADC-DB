# Databricks notebook source
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

project_identifier = 'darXXX'  # <-- set this

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
# Table lists: populate with the tables you need for your project.
# ---------------------------------------------------------------------------

# RDE tables (from 4_prod.rde)
rde_tables = [
    # 'rde_patient_demographics',
    # 'rde_all_diagnosis',
]

# Bronze/map tables (from 4_prod.bronze)
map_tables = [
    # 'map_patient_journey',
]

# OMOP tables (from 4_prod.omop)
omop_tables = [
    # 'death',
]

# Raw/mill tables (from 4_prod.raw)
mill_tables = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cohort

# COMMAND ----------

cohort_sql = f"""
CREATE OR REPLACE VIEW 6_mgmt.cohorts.{project_identifier} AS
-- TODO: define your cohort here
SELECT DISTINCT
    person_id AS PERSON_ID,
    CAST(NULL AS STRING) AS SUBCOHORT
FROM 4_prod.bronze.map_person
WHERE 1 = 0  -- placeholder
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

# Also drop any existing tables (e.g. materialised results from previous runs)
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

    A column is included only if:
      1. It has ig_risk <= max_ig_risk AND ig_severity <= max_ig_severity, OR
      2. It is in the columns_to_include override for this table.

    A column is excluded if:
      - It has no ig_risk/ig_severity tags (treated as max risk), OR
      - Its tag values exceed the thresholds, OR
      - It is in columns_to_exclude.
    """
    full_path = f"{catalog}.{schema}.{table}"
    all_columns = spark.table(full_path).columns

    # Get columns that are within both IG thresholds (must have BOTH tags within limits)
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

    # Add any explicit includes for this table
    includes = set(columns_to_include.get(full_path, []))
    includes |= set(columns_to_include.get('*', []))
    safe_columns |= includes

    # Remove explicit excludes
    safe_columns -= set(columns_to_exclude)

    # Preserve original column order
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

    Args:
        catalog: Source catalog (e.g. '4_prod')
        schema: Source schema (e.g. 'rde', 'bronze', 'omop')
        table: Source table name
        project_id: Project identifier for the target schema and cohort
        column_list: Explicit list of columns to select. If None, uses get_safe_columns().
                     Pass ['*'] to select all columns (for unfiltered reference tables).
        alias: Optional view name override. Defaults to the source table name.
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

# --- RDE tables ---
for table in rde_tables:
    try:
        if create_cohort_filtered_view('4_prod', 'rde', table, project_identifier):
            created.append(f"rde.{table}")
    except Exception as e:
        failed.append((f"rde.{table}", str(e)))
        print(f"ERROR: rde.{table}: {e}")

# --- Bronze/map tables ---
for table in map_tables:
    try:
        if create_cohort_filtered_view('4_prod', 'bronze', table, project_identifier):
            created.append(f"bronze.{table}")
    except Exception as e:
        failed.append((f"bronze.{table}", str(e)))
        print(f"ERROR: bronze.{table}: {e}")

# --- OMOP tables ---
for table in omop_tables:
    try:
        if create_cohort_filtered_view('4_prod', 'omop', table, project_identifier):
            created.append(f"omop.{table}")
    except Exception as e:
        failed.append((f"omop.{table}", str(e)))
        print(f"ERROR: omop.{table}: {e}")

# --- Raw/mill tables ---
for table in mill_tables:
    try:
        if create_cohort_filtered_view('4_prod', 'raw', table, project_identifier):
            created.append(f"raw.{table}")
    except Exception as e:
        failed.append((f"raw.{table}", str(e)))
        print(f"ERROR: raw.{table}: {e}")

# --- Unfiltered reference tables (bypass IG, still cohort-filtered if person_id exists) ---
for table in unfiltered_tables:
    try:
        # Determine catalog/schema from table name or default to omop
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
# MAGIC Add any project-specific views here (e.g. custom joins, calculated columns).

# COMMAND ----------

# Example: map_address with EPC tenure join
# 
# addr_cols = get_safe_columns('4_prod', 'bronze', 'map_address')
# addr_cols_sql = ", ".join([f"m.`{c}`" for c in addr_cols])
# spark.sql(f"""
#     CREATE OR REPLACE VIEW 5_projects.{project_identifier}.map_address AS
#     SELECT {addr_cols_sql}, e.TENURE
#     FROM 4_prod.bronze.map_address m
#     LEFT JOIN 4_prod.bronze.map_address_epc e ON m.ADDRESS_ID = e.ADDRESS_ID
#     INNER JOIN 6_mgmt.cohorts.{project_identifier} c ON m.PARENT_ENTITY_ID = c.PERSON_ID
#     WHERE m.PARENT_ENTITY_NAME = 'PERSON'
# """)

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
