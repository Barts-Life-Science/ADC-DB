# Databricks notebook source
# MAGIC %md
# MAGIC # _slam_finance_common
# MAGIC Reusable mechanics for the SLAM/finance bronze pipeline: config, alias
# MAGIC resolution CTEs, watermark/version state, MERGE helpers, comment baking.
# MAGIC No side effects on load beyond defining names.

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql import functions as F
from delta.tables import DeltaTable


def bronze_control_schema(target_schema: str = "4_prod.bronze") -> str:
    """Control-plane schema for state and audit tables."""
    target = str(target_schema).strip()
    return "6_mgmt.bronze" if target.lower() == "4_prod.bronze" else target


SRC_APC = "4_prod.raw.slam_apc_hrg_v4"
SRC_OP = "4_prod.raw.slam_op_hrg"
SRC_FIN = "4_prod.raw.finance_slr_hcdr_expenditure_report"
SRC_PLICS = {
    "PE2103": (
        "4_prod.slam.pe2103r1hreport_dbo_episodectp",
        "4_prod.slam.pe2103r1hreport_dbo_episodecostctp",
    ),
    "PE2122NCC": (
        "4_prod.slam.pe2122nccr1hreport_dbo_episodectp",
        "4_prod.slam.pe2122nccr1hreport_dbo_episodecostctp",
    ),
}
LKP_HRG = "3_lookup.dwh.hrg_v4"
LKP_ICD = "3_lookup.dwh.lkp_icd_diag"
LKP_OPCS = "3_lookup.dwh.opcs_410"
LKP_ADMISS_SOURCE = "3_lookup.dwh.lkp_cds_admiss_source"
LKP_DISCH_DEST = "3_lookup.dwh.lkp_cds_disch_dest"
LKP_PATIENT_CLASS = "3_lookup.dwh.cds_patient_class"
LKP_FIRST_ATTEND = "3_lookup.dwh.cds_first_attend"
OMOP_CONCEPT = "3_lookup.omop.concept"
OMOP_CONCEPT_REL = "3_lookup.omop.concept_relationship"
PERSON_ALIAS = "4_prod.raw.mill_person_alias"

ALIAS_TYPE_MRN = 10
ALIAS_TYPE_NHS = 18

# Curated finance site map (approved short codes only; CSS/admin left null)
FINANCE_SITE_MAP = {
    "RLH": "Royal London Hospital",
    "SBH": "St Bartholomew's Hospital",
    "WXH": "Whipps Cross Hospital",
    "NUH": "Newham University Hospital",
    "NGH": "Newham General Hospital",
}

def finance_site_case(col):
    clauses = []
    for k, v in FINANCE_SITE_MAP.items():
        safe_k = k.replace("'", "''")
        safe_v = v.replace("'", "''")
        clauses.append(
            f"WHEN upper(trim({col})) = '{safe_k}' THEN '{safe_v}'"
        )
    return f"CASE {' '.join(clauses)} ELSE NULL END"

# COMMAND ----------

def sf_widget(name, default, choices=None, label=None):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        if choices:
            dbutils.widgets.dropdown(name, default, choices, label or name)
        else:
            dbutils.widgets.text(name, default, label or name)
        return dbutils.widgets.get(name)

def sf_bool(name, default=False):
    try:
        return dbutils.widgets.get(name).strip().lower() == "true"
    except Exception:
        return default

def sf_utc_now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# COMMAND ----------

def person_alias_cte(alias_type_cd, cte_name):
    """CTE resolving one PERSON_ID per alias value, deterministically.
    Inlined per statement (serverless-safe: no cross-cell temp views)."""
    return f"""
    {cte_name} AS (
        SELECT ALIAS, PERSON_ID FROM (
            SELECT trim(ALIAS) AS ALIAS, CAST(PERSON_ID AS BIGINT) AS PERSON_ID,
                   ROW_NUMBER() OVER (
                       PARTITION BY trim(ALIAS)
                       ORDER BY ACTIVE_IND DESC,
                                END_EFFECTIVE_DT_TM DESC NULLS LAST,
                                BEG_EFFECTIVE_DT_TM DESC NULLS LAST,
                                PERSON_ID DESC
                   ) AS rn
            FROM {PERSON_ALIAS}
            WHERE PERSON_ALIAS_TYPE_CD = {alias_type_cd}
              AND ALIAS IS NOT NULL AND trim(ALIAS) <> ''
        ) WHERE rn = 1
    )"""

def compact_code_array(prefix, n):
    """Ordered array of non-blank codes from wide cols prefix1..prefixN
    (element 1 = primary position)."""
    cols = ", ".join(f"trim({prefix}{i})" for i in range(1, n + 1))
    return f"filter(array({cols}), x -> x IS NOT NULL AND x <> '')"

# COMMAND ----------

def get_max_timestamp(table_name, ts_column="ADC_UPDT",
                      default_date=datetime(1980, 1, 1)):
    try:
        row = spark.sql(f"SELECT MAX({ts_column}) AS m FROM {table_name}").collect()[0]
        return row["m"] or default_date
    except Exception:
        return default_date

def table_exists(table_name):
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        return False

def source_version(table_name):
    """Current Delta version of a source table."""
    return spark.sql(
        f"DESCRIBE HISTORY {table_name} LIMIT 1"
    ).collect()[0]["version"]

def merge_upsert(updates_df, target_table, keys):
    cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    (
        DeltaTable.forName(spark, target_table)
        .alias("t")
        .merge(updates_df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

def enable_cdf(table_name):
    spark.sql(
        f"ALTER TABLE {table_name} SET TBLPROPERTIES "
        "(delta.enableChangeDataFeed = true)"
    )

# COMMAND ----------

# Pipeline state: one row per (target_table, source_table) with the last
# successfully processed source Delta version. Written only after validation.
def ensure_state_tables(schema):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema}.map_slam_finance_pipeline_state (
            target_table STRING, source_table STRING,
            last_source_version BIGINT, updated_at TIMESTAMP
        )""")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema}.map_slam_finance_pipeline_audit (
            run_id STRING, target_table STRING, mode STRING,
            rows_after BIGINT, validation JSON := STRING, run_at TIMESTAMP
        )""".replace("JSON := STRING", "STRING"))
    state_table = f"{schema}.map_slam_finance_pipeline_state"
    audit_table = f"{schema}.map_slam_finance_pipeline_audit"
    enable_cdf(state_table)
    enable_cdf(audit_table)
    apply_table_comment(
        state_table,
        "Successful-source checkpoint state for the SLAM and finance bronze pipeline; "
        "updated only after the validation gate passes."
    )
    apply_column_comments(state_table, {
        "target_table": "Fully qualified bronze target table governed by this checkpoint.",
        "source_table": "Fully qualified Delta source table whose version is checkpointed.",
        "last_source_version": "Latest source Delta version successfully processed and validated.",
        "updated_at": "Timestamp when the successful checkpoint was committed.",
    })
    apply_table_comment(
        audit_table,
        "Run-level audit rows for the SLAM and finance bronze pipeline, including "
        "execution mode, resulting row count and serialized validation results."
    )
    apply_column_comments(audit_table, {
        "run_id": "Pipeline run identifier supplied by the caller or generated by the notebook.",
        "target_table": "Fully qualified target represented by this audit row.",
        "mode": "Execution mode such as FULL, MERGE, SNAPSHOT_MERGE, UNCHANGED_SKIP or SKIPPED.",
        "rows_after": "Target row count after the run; zero for skipped or unavailable targets.",
        "validation": "JSON string containing the validation checks shared by the run.",
        "run_at": "Timestamp when the audit row was written.",
    })

def get_state_version(schema, target_table, source_table):
    rows = spark.sql(f"""
        SELECT last_source_version
        FROM {schema}.map_slam_finance_pipeline_state
        WHERE target_table = '{target_table}' AND source_table = '{source_table}'
    """).collect()
    return rows[0][0] if rows else None

def set_state_version(schema, target_table, source_table, version):
    spark.sql(f"""
        MERGE INTO {schema}.map_slam_finance_pipeline_state t
        USING (SELECT '{target_table}' target_table, '{source_table}' source_table,
                      CAST({version} AS BIGINT) v) s
        ON t.target_table = s.target_table AND t.source_table = s.source_table
        WHEN MATCHED THEN UPDATE SET
            last_source_version = s.v, updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT
            (target_table, source_table, last_source_version, updated_at)
            VALUES (s.target_table, s.source_table, s.v, current_timestamp())
    """)

def write_audit(schema, run_id, target_table, mode, rows_after, validation):
    v = json.dumps(validation, default=str).replace("'", "''")
    spark.sql(f"""
        INSERT INTO {schema}.map_slam_finance_pipeline_audit VALUES
        ('{run_id}', '{target_table}', '{mode}', {rows_after},
         '{v}', current_timestamp())
    """)

# COMMAND ----------

def apply_column_comments(table_name, comments):
    existing = {
        r["col_name"]: r["comment"]
        for r in spark.sql(f"DESCRIBE TABLE {table_name}").collect()
        if r["col_name"] and not r["col_name"].startswith("#")
    }
    for col, comment in comments.items():
        if existing.get(col) == comment:
            continue
        safe = comment.replace("'", "''")
        spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN `{col}` COMMENT '{safe}'")

def apply_table_comment(table_name, comment):
    current = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]["description"]
    if current == comment:
        return
    safe = comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {table_name} IS '{safe}'")

def assert_no_direct_identifiers(table_name):
    """Release gate: published bronze tables carry no direct identifiers."""
    banned = {"MRN", "NHS_NUMBER", "NHSNUMBER", "NHSNO", "DOB",
              "DATE_OF_BIRTH", "POSTCODE", "POSTCD"}
    cols = {c.upper() for c in spark.table(table_name).columns}
    hit = banned & cols
    assert not hit, f"{table_name} publishes direct identifiers: {hit}"



