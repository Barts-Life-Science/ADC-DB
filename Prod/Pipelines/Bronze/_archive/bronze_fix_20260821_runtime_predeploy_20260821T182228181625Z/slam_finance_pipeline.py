# Databricks notebook source
# release: bronze_completeness_20260816_v1 — production-safe combined drop-in
# s6_dropin_slam_finance — S6 B2 self-contained drop-in.
# Rebased on the live 2026-08-13 slam_finance_pipeline + _slam_finance_common.
# Pure Python and self-contained. Production writes require the explicit release guard.

# COMMAND ----------

import json
import statistics
import time

def _preflight_widget(name, default, choices=None):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        if choices:
            dbutils.widgets.dropdown(name, default, choices, name)
        else:
            dbutils.widgets.text(name, default, name)
        return dbutils.widgets.get(name)

_preflight_widget("run_plics", "true", ["true", "false"])
_preflight_widget("run_new_plics_landings", "false", ["false", "true"])

NEW_CANONICAL_PLICS = {
    "PE1920": (
        "4_prod.slam.pe1920r1hreport_dbo_episodectp",
        "4_prod.slam.pe1920r1hreport_dbo_episodecostctp",
    ),
    "PE2223NCC": (
        "4_prod.slam.pe2223nccr1hreport_dbo_episodectp",
        "4_prod.slam.pe2223nccr1hreport_dbo_episodecostctp",
    ),
    "PE2324NCC": (
        "4_prod.slam.pe2324nccr1hreport_dbo_episodectp",
        "4_prod.slam.pe2324nccr1hreport_dbo_episodecostctp",
    ),
    "PE2425NCC": (
        "4_prod.slam.pe2425nccr1hreport_dbo_episodectp",
        "4_prod.slam.pe2425nccr1hreport_dbo_episodecostctp",
    ),
    "PE2526NCC": (
        "4_prod.slam.pe2526nccr1hreport_dbo_episodectp",
        "4_prod.slam.pe2526nccr1hreport_dbo_episodecostctp",
    ),
}

GENERATOR_ONLY_RUNBOOK = {
    "definition_of_record": "~/build_cwt_repaired.py",
    "generated_notebook": "/Workspace/Shared/ADC-DF/databricks/IncrementalUpdateV2/Setup/CreateWatermarkTable",
    "grant_flip": {
        "edit": (
            "Move PE1920R1HReport, PE2223NCCR1HReport, PE2324NCCR1HReport, "
            "PE2425NCCR1HReport and PE2526NCCR1HReport from "
            "SLAM_UNGRANTED_CANONICAL to SLAM_GRANTED_CANONICAL."
        ),
        "required_generated_policy": {
            "copy_trigger": "weekly", "copy_query_timeout": "02:00:00",
            "active_ind": 1, "upsert_task": "wt_updt",
        },
        "forbidden": "No in-place UPDATE of 6_mgmt.incr_updt_v2.watermark.",
    },
    "create_watermark": {
        "target_catalog": "6_mgmt",
        "forbidden_target_catalog": "8_dev",
        "steps": [
            "Drain ingestion backlogs and snapshot existing watermarks.",
            "Run python3 ~/build_cwt_repaired.py and publish the generated canonical notebook.",
            "Run Setup/CreateWatermarkTable once; it truncates/rebuilds the registry.",
            "Reconcile every pre-existing watermark and verify all ten canonical rows.",
        ],
    },
    "adf": {
        "pipeline": "IncrUpdtV2Pipeline",
        "one_watermark_id_at_a_time": True,
        "parameters": {"trigger_name": "<minted watermark_id>", "dev_mode": "0", "run_rde": "0"},
        "landing_gate": "SQL Server COUNT_BIG must equal the landed Delta count for every table.",
    },
    "frozen_deactivation_after_verified_bronze": {
        "generator_edit": (
            "Introduce SLAM_FROZEN_CANONICAL containing PE1920R1HReport, PE2103R1HReport, "
            "PE2122NCCR1HReport, PE2223NCCR1HReport, PE2324NCCR1HReport and "
            "PE2425NCCR1HReport. Generate those rows with copy_trigger='error', "
            "copy_query_timeout='02:00:00', active_ind=0. Rerun CreateWatermarkTable."
        ),
        "weekly_exception": (
            "PE2526NCCR1HReport remains in SLAM_GRANTED_CANONICAL with weekly/02:00:00/active_ind=1."
        ),
        "reason": "A weekly wt_updt overwrite bumps Delta version and would force needless large rebuilds.",
    },
    "pe2605": {
        "current_policy": "Remain in SLAM_DUPLICATE_OR_PROVISIONAL, trigger='error', active_ind=0.",
        "promotion_gate": (
            "Only reclassify if source probe proves both EpisodeCtp and EpisodeCostCtp exist "
            "and the owner confirms PE2605 is canonical; then add it explicitly to this drop-in."
        ),
    },
}
print(json.dumps({"generator_runbook": GENERATOR_ONLY_RUNBOOK}, sort_keys=True))

_missing_new_plics = [
    table_name
    for pair in NEW_CANONICAL_PLICS.values()
    for table_name in pair
    if not spark.catalog.tableExists(table_name)
]
if (dbutils.widgets.get("run_plics").strip().lower() == "true"
        and dbutils.widgets.get("run_new_plics_landings").strip().lower() == "true"
        and _missing_new_plics):
    dbutils.notebook.exit(json.dumps({
        "pipeline": "s6_dropin_slam_finance",
        "result": "BLOCKED_ON_LANDING",
        "missing_sources": _missing_new_plics,
        "required_action": GENERATOR_ONLY_RUNBOOK,
        "writes": [],
    }, sort_keys=True))

# COMMAND ----------


# ==== S6 BLOCK v1 (SYNC-WITH _completeness_common) ====
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import TimestampType, DateType

def table_version(tbl):
    """Current Delta commit version (metadata-only read)."""
    return spark.sql(f"DESCRIBE HISTORY {tbl} LIMIT 1").collect()[0]["version"]

def due_check(control_table, pipeline, sources):
    """Master plan S2.3 rule-4 due-check. Returns (due, current_versions): due=False iff
    EVERY source table's Delta version matches the last recorded successful run.
    Per-source rows (rule 5) - never a combined high-watermark."""
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {control_table}
        (pipeline STRING, source STRING, version BIGINT, updated_at TIMESTAMP)""")
    cur = {t: table_version(t) for t in sources}
    seen = {r["source"]: r["version"] for r in spark.sql(
        f"SELECT source, version FROM {control_table} WHERE pipeline = '{pipeline}'").collect()}
    return any(seen.get(t) != v for t, v in cur.items()), cur

def record_versions(control_table, pipeline, versions):
    """Call ONLY after a successful publish - a crashed run must re-run in full."""
    for t, v in versions.items():
        spark.sql(f"""MERGE INTO {control_table} c
            USING (SELECT '{pipeline}' pipeline, '{t}' source, CAST({v} AS BIGINT) version) s
            ON c.pipeline = s.pipeline AND c.source = s.source
            WHEN MATCHED THEN UPDATE SET c.version = s.version, c.updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (pipeline, source, version, updated_at)
                 VALUES (s.pipeline, s.source, s.version, current_timestamp())""")

def dq_all_clinical(df, admin_stamps):
    """S2.2 date-quality standard, v2 rule: flag EVERY retained temporal column except the
    product's NAMED admin/system stamps (the declared contract) and derived *_CLEAN columns.
    Returns (df_with_flags, flagged_column_list) - log the list in the session log."""
    cols = [f.name for f in df.schema.fields
            if isinstance(f.dataType, (TimestampType, DateType))
            and f.name not in admin_stamps and not f.name.endswith("_CLEAN")]
    return dq_columns(df, cols), cols

def replace_with_tombstones(df, target, key_cols):
    """Deterministic replace with NO silent hard deletes (S2.2 lifecycle): rows present in
    the prior published version but absent from the fresh build are re-appended with
    SOURCE_PRESENT_IND=false, retaining their previous column values and stamps.
    A key that reappears at source is resurrected as present (its tombstone drops out)."""
    fresh = df.withColumn("SOURCE_PRESENT_IND", F.lit(True))
    v_prev = table_version(target) if spark.catalog.tableExists(target) else None
    (fresh.write.format("delta").mode("overwrite")
          .option("overwriteSchema", "true").saveAsTable(target))
    if v_prev is not None:
        prior = spark.read.option("versionAsOf", v_prev).table(target)
        gone = (prior.join(spark.table(target).select(*key_cols).distinct(),
                           key_cols, "left_anti")
                     .withColumn("SOURCE_PRESENT_IND", F.lit(False)))
        gone.write.format("delta").mode("append").saveAsTable(target)

def table_fingerprint(tbl, exclude=("PIPELINE_UPDT_DT_TM",)):
    """Canonical whole-row fingerprint: order-independent sum of xxhash64 over the JSON of
    every column except volatile stamps. Equal fingerprint == identical published content."""
    cols = [c for c in spark.table(tbl).columns if c not in exclude]
    return (spark.table(tbl)
            .select(F.sum(F.xxhash64(F.to_json(F.struct(*[F.col(c) for c in cols])))
                          .cast("decimal(38,0)")).alias("fp"))
            .collect()[0]["fp"])
S6_SOURCE_VERSIONS = None  # legacy inlined-common constant; unused by this drop-in

def lookup_counterpart_tags(col_name):
    """Modal (ig_risk, ig_severity) for this column name across 4_prod.bronze — copied, never guessed.
    Returns None when no counterpart exists (caller must then decide explicitly)."""
    col_lit = col_name.replace("'", "''")
    rows = (spark.sql(f"""
        SELECT MAX(CASE WHEN tag_name='ig_risk' THEN tag_value END) r,
               MAX(CASE WHEN tag_name='ig_severity' THEN tag_value END) s, COUNT(*) n
        FROM `4_prod`.information_schema.column_tags
        WHERE schema_name='bronze' AND upper(column_name)=upper('{col_lit}')
        GROUP BY table_name""")
        .groupBy("r", "s").count()
        .orderBy(F.desc("count"), F.asc("r"), F.asc("s"))
        .collect())
    return (rows[0]["r"], rows[0]["s"]) if rows else None

def ig_tag_table(table, tag_map, default=('0', '0')):
    """Apply complete IG tags, skipping columns whose two tags are already correct."""
    cat, sch, tbl = table.split(".")
    existing_rows = spark.sql(f"""
        SELECT column_name,
               MAX(CASE WHEN tag_name='ig_risk' THEN tag_value END) AS risk,
               MAX(CASE WHEN tag_name='ig_severity' THEN tag_value END) AS severity
        FROM `{cat}`.information_schema.column_tags
        WHERE schema_name='{sch}' AND table_name='{tbl}'
          AND tag_name IN ('ig_risk','ig_severity')
        GROUP BY column_name
    """).collect()
    existing = {
        r["column_name"]: (str(r["risk"]) if r["risk"] is not None else None,
                           str(r["severity"]) if r["severity"] is not None else None)
        for r in existing_rows
    }
    cols = [r.col_name for r in spark.sql(f"DESCRIBE {table}").collect()
            if r.col_name and not r.col_name.startswith('#')]
    skipped = 0
    for c in cols:
        if c in tag_map:
            risk, sev = tag_map[c]
        elif all(value is not None for value in existing.get(c, (None, None))):
            skipped += 1
            continue
        else:
            found = lookup_counterpart_tags(c)
            risk, sev = found if found else default
            if not found:
                print(f"IG-TAG DEFAULTED {table}.{c} -> {default} — REVIEW")
        assert risk is not None and sev is not None, (
            f"Incomplete counterpart tags for {table}.{c}: ig_risk={risk}, ig_severity={sev}")
        desired = (str(risk), str(sev))
        if existing.get(c) == desired:
            skipped += 1
            continue
        col_ident = c.replace("`", "``")
        risk_lit = desired[0].replace("'", "''")
        sev_lit = desired[1].replace("'", "''")
        spark.sql(
            f"ALTER TABLE {table} ALTER COLUMN `{col_ident}` "
            f"SET TAGS ('ig_risk'='{risk_lit}','ig_severity'='{sev_lit}')")
    print(f"IG-TAG {table}: skipped={skipped}, total={len(cols)}")

def ig_tag_gate(table):
    """Fail when any table column is missing either required IG tag."""
    cat, sch, tbl = table.split('.')
    sch_lit = sch.replace("'", "''")
    tbl_lit = tbl.replace("'", "''")
    cat_ident = cat.replace("`", "``")
    row = spark.sql(f"""
        WITH cols AS (
          SELECT column_name
          FROM `{cat_ident}`.information_schema.columns
          WHERE table_schema='{sch_lit}' AND table_name='{tbl_lit}'
        ),
        risk_tagged AS (
          SELECT DISTINCT column_name
          FROM `{cat_ident}`.information_schema.column_tags
          WHERE schema_name='{sch_lit}' AND table_name='{tbl_lit}' AND tag_name='ig_risk'
        ),
        severity_tagged AS (
          SELECT DISTINCT column_name
          FROM `{cat_ident}`.information_schema.column_tags
          WHERE schema_name='{sch_lit}' AND table_name='{tbl_lit}' AND tag_name='ig_severity'
        )
        SELECT
          COALESCE(SUM(CASE WHEN r.column_name IS NULL THEN 1 ELSE 0 END), 0) AS missing_risk,
          COALESCE(SUM(CASE WHEN s.column_name IS NULL THEN 1 ELSE 0 END), 0) AS missing_severity,
          COALESCE(SUM(CASE WHEN r.column_name IS NULL OR s.column_name IS NULL THEN 1 ELSE 0 END), 0)
            AS missing_either
        FROM cols c
        LEFT JOIN risk_tagged r ON c.column_name = r.column_name
        LEFT JOIN severity_tagged s ON c.column_name = s.column_name
        """).collect()[0]
    missing_risk = int(row["missing_risk"])
    missing_severity = int(row["missing_severity"])
    missing_either = int(row["missing_either"])
    assert missing_either == 0, (
        f"{missing_either} columns on {table} missing ig_risk and/or ig_severity "
        f"({missing_risk} missing ig_risk; {missing_severity} missing ig_severity)")

# ==== END S6 BLOCK v1 ====

# COMMAND ----------


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
    "PE1920": (
        "4_prod.slam.pe1920r1hreport_dbo_episodectp",
        "4_prod.slam.pe1920r1hreport_dbo_episodecostctp",
    ),
    "PE2103": (
        "4_prod.slam.pe2103r1hreport_dbo_episodectp",
        "4_prod.slam.pe2103r1hreport_dbo_episodecostctp",
    ),
    "PE2122NCC": (
        "4_prod.slam.pe2122nccr1hreport_dbo_episodectp",
        "4_prod.slam.pe2122nccr1hreport_dbo_episodecostctp",
    ),
    "PE2223NCC": (
        "4_prod.slam.pe2223nccr1hreport_dbo_episodectp",
        "4_prod.slam.pe2223nccr1hreport_dbo_episodecostctp",
    ),
    "PE2324NCC": (
        "4_prod.slam.pe2324nccr1hreport_dbo_episodectp",
        "4_prod.slam.pe2324nccr1hreport_dbo_episodecostctp",
    ),
    "PE2425NCC": (
        "4_prod.slam.pe2425nccr1hreport_dbo_episodectp",
        "4_prod.slam.pe2425nccr1hreport_dbo_episodecostctp",
    ),
    "PE2526NCC": (
        "4_prod.slam.pe2526nccr1hreport_dbo_episodectp",
        "4_prod.slam.pe2526nccr1hreport_dbo_episodecostctp",
    ),
}
S6B_EXISTING_PLICS_EXTRACTS = {"PE2103", "PE2122NCC"}
if dbutils.widgets.get("run_new_plics_landings").strip().lower() != "true":
    SRC_PLICS = {
        extract_cd: pair
        for extract_cd, pair in SRC_PLICS.items()
        if extract_cd in S6B_EXISTING_PLICS_EXTRACTS
    }
PE2605_PLICS = (
    "4_prod.slam.pe2605r1hreport_dbo_episodectp",
    "4_prod.slam.pe2605r1hreport_dbo_episodecostctp",
)
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

def align_updates_to_target(updates_df, target_table):
    target_fields = spark.table(target_table).schema.fields
    target_columns = [field.name for field in target_fields]
    source_columns = set(updates_df.columns)
    target_column_set = set(target_columns)

    missing_from_source = target_column_set - source_columns
    unexpected_missing = missing_from_source - {"SOURCE_ABSENT_DETECTED_TS"}
    assert not unexpected_missing, (
        f"{target_table}: target columns missing from merge source: "
        f"{sorted(unexpected_missing)}"
    )
    unexpected_source = source_columns - target_column_set
    assert not unexpected_source, (
        f"{target_table}: merge source columns missing from target: "
        f"{sorted(unexpected_source)}"
    )
    if "SOURCE_ABSENT_DETECTED_TS" in missing_from_source:
        updates_df = updates_df.withColumn(
            "SOURCE_ABSENT_DETECTED_TS", F.lit(None).cast("timestamp")
        )
    return updates_df.select(*target_columns)


def merge_upsert(updates_df, target_table, keys):
    cond = " AND ".join(
        f"t.`{k}` <=> s.`{k}`" for k in keys
    )
    aligned_updates = align_updates_to_target(updates_df, target_table)
    (
        DeltaTable.forName(spark, target_table)
        .alias("t")
        .merge(aligned_updates.alias("s"), cond)
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



# COMMAND ----------


# COMMAND ----------


# COMMAND ----------

TARGET_SCHEMA = sf_widget("target_schema", "8_dev.bronze", label="Target schema")
sf_widget("allow_production_write", "false", ["false", "true"], "Allow production write")
sf_widget("force_full_refresh", "false", ["false", "true"], "Force full refresh")
sf_widget("run_slam_hrg", "true", ["true", "false"], "Run SLAM APC/OP HRG")
sf_widget("run_finance", "true", ["true", "false"], "Run finance HCD")
sf_widget("run_plics", "true", ["true", "false"], "Run PLICS")
sf_widget("run_new_plics_landings", "false", ["false", "true"], "Run future landed PLICS extracts")
sf_widget("pipeline_run_id", "", label="Pipeline run id (optional)")
sf_widget("include_pe2605", "false", ["false", "true"], "Include owner-confirmed PE2605")
sf_widget("pe2605_owner_confirmed", "false", ["false", "true"], "PE2605 canonical owner confirmation")
sf_widget("benchmark_mode", "false", ["false", "true"], "Benchmark selected extract")
sf_widget("benchmark_extract", "PE2526NCC", label="Benchmark extract")
sf_widget("process_extract", "ALL", label="Process one PLICS extract or ALL")
sf_widget("benchmark_interrupt_after_stage", "", label="Interrupt after validated stage (test only)")
sf_widget("apply_plics_ig_tags", "false", ["false", "true"], "Apply and gate PLICS IG tags")
sf_widget("gates_only", "false", ["false", "true"], "Run S6b gates before build")
sf_widget("run_s6b_full_gates", "true", ["true", "false"], "Run expensive S6b parity gates")


ALLOW_PROD_WRITE = sf_bool("allow_production_write", False)
FORCE_FULL = sf_bool("force_full_refresh", False)
RUN_SLAM_HRG = sf_bool("run_slam_hrg", True)
RUN_FINANCE = sf_bool("run_finance", True)
RUN_PLICS = sf_bool("run_plics", True)
INCLUDE_PE2605 = sf_bool("include_pe2605", False)
PE2605_OWNER_CONFIRMED = sf_bool("pe2605_owner_confirmed", False)
BENCHMARK_MODE = sf_bool("benchmark_mode", False)
BENCHMARK_EXTRACT = dbutils.widgets.get("benchmark_extract").strip().upper()
PROCESS_EXTRACT = dbutils.widgets.get("process_extract").strip().upper() or "ALL"
BENCHMARK_INTERRUPT_AFTER_STAGE = dbutils.widgets.get("benchmark_interrupt_after_stage").strip().upper()
APPLY_PLICS_IG_TAGS = sf_bool("apply_plics_ig_tags", False)
GATES_ONLY = sf_bool("gates_only", False)
RUN_S6B_FULL_GATES = sf_bool("run_s6b_full_gates", True)

RUN_ID = dbutils.widgets.get("pipeline_run_id") or \
    datetime.utcnow().strftime("slamfin_%Y%m%d_%H%M%S")

assert TARGET_SCHEMA.count(".") == 1, "target_schema must be catalog.schema"
assert TARGET_SCHEMA.startswith("8_dev.") or ALLOW_PROD_WRITE, (
    f"Refusing to write {TARGET_SCHEMA} without allow_production_write=true")
if INCLUDE_PE2605:
    assert PE2605_OWNER_CONFIRMED, (
        "PE2605 remains conditional: set include_pe2605=true only with owner confirmation")
    missing_pe2605 = [t for t in PE2605_PLICS if not table_exists(t)]
    assert not missing_pe2605, f"PE2605 requested but source tables are absent: {missing_pe2605}"
    SRC_PLICS["PE2605"] = PE2605_PLICS

CONTROL_SCHEMA = bronze_control_schema(TARGET_SCHEMA)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CONTROL_SCHEMA}")
ensure_state_tables(CONTROL_SCHEMA)

T_APC = f"{TARGET_SCHEMA}.map_slam_apc_hrg"
T_OP = f"{TARGET_SCHEMA}.map_slam_op_hrg"
T_ACT = f"{TARGET_SCHEMA}.map_slam_costed_activity"
T_COST = f"{TARGET_SCHEMA}.map_slam_cost_line_item"
T_FIN = f"{TARGET_SCHEMA}.map_finance_hcd_expenditure"


# ==== S6b A14 ====
S6B_SOURCE_VERSIONS = f"{CONTROL_SCHEMA}.s6b_source_versions"
_pending_s6b_hrg_versions = []


def present_filtered_count(table):
    return spark.sql(
        f"SELECT COUNT(*) c FROM {table} WHERE SOURCE_PRESENT_IND"
    ).collect()[0]["c"]


def _ensure_present_column(table):
    if not table_exists(table):
        return
    columns = set(spark.table(table).columns)
    if "SOURCE_PRESENT_IND" not in columns:
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS (SOURCE_PRESENT_IND BOOLEAN)")
        spark.sql(
            f"UPDATE {table} SET SOURCE_PRESENT_IND = true "
            "WHERE SOURCE_PRESENT_IND IS NULL"
        )
    if "SOURCE_ABSENT_DETECTED_TS" not in columns:
        spark.sql(
            f"ALTER TABLE {table} ADD COLUMNS ("
            "SOURCE_ABSENT_DETECTED_TS TIMESTAMP COMMENT "
            "'First timestamp when a previously published source row was absent; "
            "NULL while present.'"
            ")"
        )

def _enable_s6b_table_contract(table):
    spark.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES ("
        "delta.enableChangeDataFeed=true,"
        "delta.enableRowTracking=true,"
        "delta.enableDeletionVectors=true,"
        "delta.appendOnly=false)"
    )


def _reconcile_hrg_snapshot(target, source, target_key, source_key_expr):
    spark.sql(f"""
        MERGE INTO {target} t
        USING (
            SELECT DISTINCT {source_key_expr} AS K
            FROM {source}
            WHERE {source_key_expr} IS NOT NULL AND {source_key_expr} <> ''
        ) s
        ON t.{target_key} = s.K
        WHEN MATCHED AND NOT COALESCE(t.SOURCE_PRESENT_IND, false) THEN UPDATE SET
            t.SOURCE_PRESENT_IND = true,
            t.SOURCE_ABSENT_DETECTED_TS = NULL,
            t.ADC_UPDT = current_timestamp()
        WHEN NOT MATCHED BY SOURCE AND COALESCE(t.SOURCE_PRESENT_IND, true) THEN UPDATE SET
            t.SOURCE_PRESENT_IND = false,
            t.SOURCE_ABSENT_DETECTED_TS = COALESCE(
                t.SOURCE_ABSENT_DETECTED_TS, current_timestamp()
            ),
            t.ADC_UPDT = current_timestamp()
    """)

def _stable_business_fingerprint(table, columns, present_only=False):
    df = spark.table(table)
    if present_only:
        df = df.where("SOURCE_PRESENT_IND")
    return df.select(
        F.count("*").alias("n"),
        F.sum(
            F.xxhash64(F.to_json(F.struct(*[F.col(c) for c in columns])))
             .cast("decimal(38,0)")
        ).alias("fp"),
    ).collect()[0].asDict()


def _slam_tag_tables():
    for table in (T_APC, T_OP, T_ACT, T_COST):
        explicit = {}
        for col_name, dtype in spark.table(table).dtypes:
            upper = col_name.upper()
            if upper == "PERSON_ID":
                explicit[col_name] = lookup_counterpart_tags(col_name) or ("2", "2")
            elif upper.endswith("_ID") or upper.endswith("_CD"):
                explicit[col_name] = lookup_counterpart_tags(col_name) or ("2", "2")
            elif dtype == "string":
                explicit[col_name] = lookup_counterpart_tags(col_name) or ("1", "1")
        ig_tag_table(table, explicit)
        ig_tag_gate(table)


def _s6b_gate_diag(stage):
    safe_run = str(RUN_ID).replace("'", "''")
    safe_stage = str(stage).replace("'", "''")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.slam_release_diagnostics (
            run_id STRING, stage STRING, recorded_at TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(
        f"INSERT INTO {CONTROL_SCHEMA}.slam_release_diagnostics "
        f"VALUES ('{safe_run}', '{safe_stage}', current_timestamp())"
    )


def run_s6b_slam_gates(prepare=False):
    _s6b_gate_diag("start")
    blockers = {
        T_APC: ["CDS_APC_ID"],
        T_OP: ["CDS_OPA_ID"],
        T_ACT: ["EXTRACT_CD", "ACTIVITY_RECORD_ID"],
        T_COST: ["EXTRACT_CD", "ACTIVITY_RECORD_ID", "LINE_HASH"],
    }
    if prepare:
        for table in blockers:
            _enable_s6b_table_contract(table)
        _slam_tag_tables()

    results = {}
    for dev, keys in blockers.items():
        prod = dev.replace("8_dev.bronze.", "4_prod.bronze.")
        _s6b_gate_diag(f"{dev}:start")
        assert table_exists(dev), f"GATE EXPECTED TABLE MISSING: {dev}"
        field = next((f for f in spark.table(dev).schema.fields if f.name == "SOURCE_PRESENT_IND"), None)
        assert field is not None and isinstance(field.dataType, T.BooleanType), (
            f"GATE EXPECTED BOOLEAN SOURCE_PRESENT_IND: {dev}"
        )
        _s6b_gate_diag(f"{dev}:type")
        dev_present = present_filtered_count(dev)
        prod_rows = spark.table(prod).count()
        present_count_drift = dev_present - prod_rows
        source_key_count = None
        if dev == T_APC:
            source_key_count = spark.sql(
                f"SELECT COUNT(DISTINCT trim(CDS_APC_Id)) c FROM {SRC_APC} "
                "WHERE CDS_APC_Id IS NOT NULL AND trim(CDS_APC_Id) <> ''"
            ).collect()[0]["c"]
            assert dev_present == source_key_count, (dev, dev_present, source_key_count)
        elif dev == T_OP:
            source_key_count = spark.sql(
                f"SELECT COUNT(DISTINCT trim(CDS_OPA_Id)) c FROM {SRC_OP} "
                "WHERE CDS_OPA_Id IS NOT NULL AND trim(CDS_OPA_Id) <> ''"
            ).collect()[0]["c"]
            assert dev_present == source_key_count, (dev, dev_present, source_key_count)

        _s6b_gate_diag(f"{dev}:count")
        null_key_expr = F.col(keys[0]).isNull()
        for key in keys[1:]:
            null_key_expr = null_key_expr | F.col(key).isNull()
        key_stats = spark.table(dev).agg(
            F.count("*").alias("n"),
            F.countDistinct(F.struct(*[F.col(k) for k in keys])).alias("d"),
            F.sum(F.when(null_key_expr, 1).otherwise(0)).alias("nulls"),
        ).collect()[0]
        assert key_stats["n"] == key_stats["d"] and int(key_stats["nulls"] or 0) == 0
        _s6b_gate_diag(f"{dev}:keys")

        dev_cols = set(spark.table(dev).columns)
        prod_cols = set(spark.table(prod).columns)
        business_cols = sorted(
            (dev_cols & prod_cols)
            - {"ADC_UPDT", "SOURCE_PRESENT_IND", "PERSON_ID", "PERSON_LINK_METHOD"}
        )
        person_drift = None
        if present_count_drift != 0:
            # Different live populations make row-level prod parity non-comparable.
            dev_fp = {"n": dev_present, "fp": None}
            prod_fp = {"n": prod_rows, "fp": None}
            business_fingerprint_matches_prod = False
            _s6b_gate_diag(f"{dev}:fingerprint_skipped")
        else:
            if dev == T_COST:
                # LINE_HASH covers all cost-line business fields and avoids serialising 181M wide rows.
                dev_fp = spark.table(dev).where("SOURCE_PRESENT_IND").agg(
                    F.count("*").alias("n"),
                    F.sum(F.xxhash64("LINE_HASH").cast("decimal(38,0)")).alias("fp"),
                ).collect()[0].asDict()
                prod_fp = spark.table(prod).agg(
                    F.count("*").alias("n"),
                    F.sum(F.xxhash64("LINE_HASH").cast("decimal(38,0)")).alias("fp"),
                ).collect()[0].asDict()
            else:
                dev_fp = _stable_business_fingerprint(dev, business_cols, present_only=True)
                prod_fp = _stable_business_fingerprint(prod, business_cols, present_only=False)
            business_fingerprint_matches_prod = dev_fp == prod_fp
            _s6b_gate_diag(f"{dev}:fingerprint")

            if {"PERSON_ID", "PERSON_LINK_METHOD"} <= (dev_cols & prod_cols):
                d = spark.table(dev).where("SOURCE_PRESENT_IND").select(
                    *keys, "PERSON_ID", "PERSON_LINK_METHOD"
                ).alias("d")
                p = spark.table(prod).select(*keys, "PERSON_ID", "PERSON_LINK_METHOD").alias("p")
                cond = None
                for key in keys:
                    term = F.col(f"d.{key}").eqNullSafe(F.col(f"p.{key}"))
                    cond = term if cond is None else cond & term
                person_drift = d.join(p, cond, "inner").where(
                    ~F.col("d.PERSON_ID").eqNullSafe(F.col("p.PERSON_ID"))
                    | ~F.col("d.PERSON_LINK_METHOD").eqNullSafe(F.col("p.PERSON_LINK_METHOD"))
                ).count()

        _s6b_gate_diag(f"{dev}:person")
        ig_tag_gate(dev)
        _s6b_gate_diag(f"{dev}:tags")
        results[dev] = {
            "prod_rows": prod_rows,
            "dev_present": dev_present,
            "present_count_drift_vs_prod": present_count_drift,
            "source_key_count": source_key_count,
            "business_fingerprint": str(dev_fp["fp"]),
            "business_fingerprint_matches_prod": business_fingerprint_matches_prod,
            "person_link_drift_rows": person_drift,
            "version": int(table_version(dev)),
        }

    represented = spark.table(T_COST).where("SOURCE_PRESENT_IND").agg(
        F.sum("SOURCE_DUPLICATE_COUNT").alias("n")
    ).collect()[0]["n"]
    prod_represented = spark.table("4_prod.bronze.map_slam_cost_line_item").agg(
        F.sum("SOURCE_DUPLICATE_COUNT").alias("n")
    ).collect()[0]["n"]
    assert represented == prod_represented
    _s6b_gate_diag("represented")
    results[T_COST]["represented_source_rows"] = int(represented)

    print("[S6B_SLAM_GATES] " + json.dumps(results, sort_keys=True, default=str))
    return results


def _s6b_hrg_lifecycle_fixture():
    source = f"{TARGET_SCHEMA}.s6b_hrg_source_fixture"
    target = f"{TARGET_SCHEMA}.s6b_hrg_target_fixture"
    spark.sql(f"DROP TABLE IF EXISTS {source}")
    spark.sql(f"DROP TABLE IF EXISTS {target}")
    spark.createDataFrame([("A",), ("B",)], "K STRING").write.saveAsTable(source)
    (
        spark.createDataFrame(
            [("A", True), ("B", True)],
            "K STRING, SOURCE_PRESENT_IND BOOLEAN",
        )
        .withColumn("SOURCE_ABSENT_DETECTED_TS", F.lit(None).cast("timestamp"))
        .withColumn("ADC_UPDT", F.current_timestamp())
        .write.saveAsTable(target)
    )
    spark.sql(f"DELETE FROM {source} WHERE K='B'")
    _reconcile_hrg_snapshot(target, source, "K", "K")
    dropped = spark.table(target).where("K='B'").collect()[0]
    assert dropped["SOURCE_PRESENT_IND"] is False
    assert dropped["SOURCE_ABSENT_DETECTED_TS"] is not None
    spark.sql(f"INSERT INTO {source} VALUES ('B')")
    _reconcile_hrg_snapshot(target, source, "K", "K")
    restored = spark.table(target).where("K='B'").collect()[0]
    assert restored["SOURCE_PRESENT_IND"] is True
    assert restored["SOURCE_ABSENT_DETECTED_TS"] is None
    spark.sql(f"DROP TABLE IF EXISTS {source}")
    spark.sql(f"DROP TABLE IF EXISTS {target}")
    print("[S6B_SLAM] HRG tombstone/resurrection fixture PASS")


if GATES_ONLY:
    run_s6b_slam_gates(prepare=False)
    dbutils.notebook.exit(json.dumps({"mode": "GATES_ONLY", "status": "PASS"}))


_results = []
_started_at = sf_utc_now()
print(f"[SLAM-FIN] run={RUN_ID} target={TARGET_SCHEMA} force_full={FORCE_FULL}")

# COMMAND ----------


# COMMAND ----------

def _apc_select_sql(watermark=None):
    wm_sql = watermark.strftime("%Y-%m-%d %H:%M:%S.%f") if watermark else None
    inc = f"AND s.ADC_UPDT > timestamp'{wm_sql}'" if wm_sql else ""
    return f"""
    WITH {person_alias_cte(ALIAS_TYPE_MRN, "mrn_person")},
    src AS (
        SELECT s.*,
               ROW_NUMBER() OVER (
                   PARTITION BY trim(s.CDS_APC_Id)
                   ORDER BY s.Record_Updated_Dt DESC NULLS LAST,
                            s.ADC_UPDT DESC NULLS LAST,
                            s.Ep_End_Dt DESC NULLS LAST
               ) AS rn
        FROM {SRC_APC} s
        WHERE s.CDS_APC_Id IS NOT NULL AND trim(s.CDS_APC_Id) <> '' {inc}
    )
    SELECT
        p.PERSON_ID,
        CASE WHEN p.PERSON_ID IS NOT NULL THEN 'MRN_ALIAS' END AS PERSON_LINK_METHOD,
        trim(s.CDS_APC_Id)              AS CDS_APC_ID,
        trim(s.Hosp_Prov_Spell_Num)     AS HOSP_PROV_SPELL_NUM,
        s.Org_Code                      AS PROVIDER_ORG_CD,
        s.Adm_Dt                        AS ADMISSION_DT_TM,
        s.Disch_Dt                      AS DISCHARGE_DT_TM,
        s.Ep_St_Dt                      AS EPISODE_START_DT_TM,
        s.Ep_End_Dt                     AS EPISODE_END_DT_TM,
        s.Ep_Order                      AS EPISODE_ORDER,
        s.Ep_Duration                   AS EPISODE_DURATION_DAYS,
        s.Main_Spec_cd                  AS MAIN_SPECIALTY_CD,
        s.Treat_Spec_Cd                 AS TREATMENT_FUNCTION_CD,
        s.Admiss_Method                 AS ADMISSION_METHOD_CD,
        CAST(s.Admiss_Source AS STRING) AS ADMISSION_SOURCE_CD,
        asrc.Admiss_Source_Desc         AS ADMISSION_SOURCE_DESC,
        CAST(s.Disch_Method AS STRING)  AS DISCHARGE_METHOD_CD,
        CAST(s.Disch_Destin AS STRING)  AS DISCHARGE_DEST_CD,
        dd.Disch_Dest_Desc              AS DISCHARGE_DEST_DESC,
        CAST(s.Ptnt_Class AS STRING)    AS PATIENT_CLASS_CD,
        pc.Patient_Class_Desc           AS PATIENT_CLASS_DESC,
        s.Age                           AS SOURCE_AGE,
        s.Sex                           AS SOURCE_SEX_CD,
        s.Neonatal_Care_Level_Cd        AS NEONATAL_CARE_LEVEL_CD,
        s.CC_days                       AS CRITICAL_CARE_DAYS,
        s.RH_days                       AS REHAB_DAYS,
        {compact_code_array("s.ICD_Diag", 50)}  AS ICD_DIAG_CODES,
        {compact_code_array("s.OPCS_Proc", 50)} AS OPCS_PROC_CODES,
        s.FCE_HRG_Cd                    AS FCE_HRG_CD,
        hf.HRG_Desc                     AS FCE_HRG_DESC,
        s.FCE_Grouping_Method_Flag      AS FCE_GROUPING_METHOD_FLAG,
        s.FCE_Dom_proc                  AS FCE_DOMINANT_PROC_CD,
        opf.Proc_Desc                   AS FCE_DOMINANT_PROC_DESC,
        s.FCE_PBC                       AS FCE_PBC_CD,
        s.FCE_Calc_Epi_dur              AS FCE_CALC_EPISODE_DUR,
        s.FCE_Reporting_EPI_DUR         AS FCE_REPORTING_EPISODE_DUR,
        s.Dom_Ep_Flag                   AS DOMINANT_EPISODE_FLAG,
        s.Spell_HRG_Cd                  AS SPELL_HRG_CD,
        hs.HRG_Desc                     AS SPELL_HRG_DESC,
        s.Spell_Group_Method_Flag       AS SPELL_GROUPING_METHOD_FLAG,
        s.Spell_Dom_Proc                AS SPELL_DOMINANT_PROC_CD,
        ops.Proc_Desc                   AS SPELL_DOMINANT_PROC_DESC,
        s.Spell_PDiag                   AS SPELL_PRIMARY_DIAG_CD,
        icdp.ICD_Diag_Desc              AS SPELL_PRIMARY_DIAG_DESC,
        s.Spell_SDiag                   AS SPELL_SECONDARY_DIAG_CD,
        icds.ICD_Diag_Desc              AS SPELL_SECONDARY_DIAG_DESC,
        s.Spell_Ep_Count                AS SPELL_EPISODE_COUNT,
        s.Spell_LOS                     AS SPELL_LOS,
        s.Spell_Reporting_LOS           AS SPELL_REPORTING_LOS,
        s.Spell_CCDays                  AS SPELL_CRITICAL_CARE_DAYS,
        s.Spell_SSC                     AS SPELL_SSC_CD,
        s.Spell_BP                      AS SPELL_BEST_PRACTICE_CD,
        s.Errors                        AS GROUPER_ERRORS,
        s.Record_Updated_Dt             AS SOURCE_RECORD_UPDATED_DT,
        true                               AS SOURCE_PRESENT_IND,
        s.ADC_UPDT
    FROM src s
    LEFT JOIN mrn_person p ON trim(s.MRN) = p.ALIAS
    LEFT JOIN {LKP_HRG} hf ON s.FCE_HRG_Cd = hf.HRG_Cd
    LEFT JOIN {LKP_HRG} hs ON s.Spell_HRG_Cd = hs.HRG_Cd
    LEFT JOIN {LKP_OPCS} opf ON replace(trim(s.FCE_Dom_proc), '.', '') = opf.Proc_Cd
    LEFT JOIN {LKP_OPCS} ops ON replace(trim(s.Spell_Dom_Proc), '.', '') = ops.Proc_Cd
    LEFT JOIN {LKP_ICD} icdp ON replace(trim(s.Spell_PDiag), '.', '') = icdp.ICD_Diag_Cd
    LEFT JOIN {LKP_ICD} icds ON replace(trim(s.Spell_SDiag), '.', '') = icds.ICD_Diag_Cd
    LEFT JOIN {LKP_ADMISS_SOURCE} asrc
           ON CAST(s.Admiss_Source AS STRING) = CAST(asrc.Admiss_Source_Cd AS STRING)
    LEFT JOIN {LKP_DISCH_DEST} dd
           ON CAST(s.Disch_Destin AS STRING) = CAST(dd.Disch_Dest_Cd AS STRING)
    LEFT JOIN {LKP_PATIENT_CLASS} pc
           ON CAST(s.Ptnt_Class AS STRING) = CAST(pc.Patient_Class_Cd AS STRING)
    WHERE s.rn = 1
    """

def _relink_unresolved(target_table, key_col, src_table, src_key_expr,
                       src_mrn_expr):
    """Re-attempt MRN linkage for rows that are still unlinked (picks up
    alias-table changes without full CDF plumbing)."""
    spark.sql(f"""
        MERGE INTO {target_table} t
        USING (
            WITH {person_alias_cte(ALIAS_TYPE_MRN, "mrn_person")},
            src AS (
                SELECT {src_key_expr} AS K, {src_mrn_expr} AS M,
                       ROW_NUMBER() OVER (PARTITION BY {src_key_expr}
                           ORDER BY ADC_UPDT DESC NULLS LAST) rn
                FROM {src_table}
                WHERE {src_key_expr} IS NOT NULL
            )
            SELECT s.K, p.PERSON_ID
            FROM src s JOIN mrn_person p ON s.M = p.ALIAS
            WHERE s.rn = 1
        ) s
        ON t.{key_col} = s.K AND t.PERSON_ID IS NULL
        WHEN MATCHED THEN UPDATE SET
            t.PERSON_ID = s.PERSON_ID,
            t.PERSON_LINK_METHOD = 'MRN_ALIAS',
            t.ADC_UPDT = current_timestamp()
    """)

if RUN_SLAM_HRG:
    _ensure_present_column(T_APC)
    if FORCE_FULL or not table_exists(T_APC):
        spark.sql(f"CREATE OR REPLACE TABLE {T_APC} "
                  f"TBLPROPERTIES (delta.enableChangeDataFeed = true) "
                  f"AS {_apc_select_sql()}")
        mode = "FULL"
    else:
        wm = get_max_timestamp(T_APC)
        merge_upsert(spark.sql(_apc_select_sql(watermark=wm)), T_APC, ["CDS_APC_ID"])
        _relink_unresolved(T_APC, "CDS_APC_ID", SRC_APC,
                           "trim(CDS_APC_Id)", "trim(MRN)")
        mode = f"MERGE>{wm}"
    _ensure_present_column(T_APC)
    apc_due, apc_versions = due_check(
        S6B_SOURCE_VERSIONS, "slam_hrg_apc_reconcile", [SRC_APC])
    if FORCE_FULL or apc_due:
        _reconcile_hrg_snapshot(T_APC, SRC_APC, "CDS_APC_ID", "trim(CDS_APC_Id)")
        _pending_s6b_hrg_versions.append(
            ("slam_hrg_apc_reconcile", apc_versions))
    n = spark.table(T_APC).count()
    _results.append({"table": T_APC, "mode": mode, "rows": n})
    print(f"[SLAM-FIN] {T_APC} {mode} rows={n}")
else:
    _results.append({"table": T_APC, "mode": "SKIPPED"})

# COMMAND ----------

if RUN_SLAM_HRG:
    assert_no_direct_identifiers(T_APC)
    apply_table_comment(T_APC, (
        "SLAM HRG4+ grouper output at finished-consultant-episode grain (one "
        "row per trimmed CDS_APC_ID) from 4_prod.raw.slam_apc_hrg_v4. Carries "
        "FCE- and spell-level HRG assignment, grouper metadata and mapped "
        "descriptions; person-resolved via MRN (mill_person_alias type 10) "
        "with the identifier itself not published. ICD/OPCS grouper inputs "
        "kept as ordered arrays (element 1 = primary); coded diagnosis/"
        "procedure of record lives in map_diagnosis/map_procedure."
    ))
    apply_column_comments(T_APC, {
        "PERSON_ID": "Millennium PERSON_ID resolved from source MRN via mill_person_alias type 10 (deterministic tiebreak); null when unresolved — source MRN is retained only in raw.",
        "PERSON_LINK_METHOD": "How PERSON_ID was resolved: MRN_ALIAS, or null when unlinked.",
        "CDS_APC_ID": "Trimmed CDS admitted-patient-care episode identifier; unique key; joins back to raw.",
        "HOSP_PROV_SPELL_NUM": "Hospital provider spell number grouping episodes into spells.",
        "SOURCE_AGE": "NON-CANONICAL grouper input age as submitted to SLAM; demographics of record are in map_person.",
        "SOURCE_SEX_CD": "NON-CANONICAL grouper input sex code as submitted to SLAM; demographics of record are in map_person.",
        "TREATMENT_FUNCTION_CD": "NHS Treatment Function Code (national standard).",
        "ICD_DIAG_CODES": "Ordered ICD-10 codes as submitted to the grouper; element 1 = primary diagnosis.",
        "OPCS_PROC_CODES": "Ordered OPCS-4 codes as submitted to the grouper; element 1 = primary procedure.",
        "FCE_HRG_CD": "HRG4+ code assigned to this finished consultant episode.",
        "SPELL_HRG_CD": "HRG4+ code assigned at hospital-spell level (repeated on every episode of the spell).",
        "GROUPER_ERRORS": "Grouper error/quality messages.",
        "ADC_UPDT": "Load/update timestamp; incremental watermark column.",
    })

# COMMAND ----------


# COMMAND ----------

def _op_select_sql(watermark=None):
    wm_sql = watermark.strftime("%Y-%m-%d %H:%M:%S.%f") if watermark else None
    inc = f"AND s.ADC_UPDT > timestamp'{wm_sql}'" if wm_sql else ""
    return f"""
    WITH {person_alias_cte(ALIAS_TYPE_MRN, "mrn_person")},
    src AS (
        SELECT s.*,
               ROW_NUMBER() OVER (
                   PARTITION BY trim(s.CDS_OPA_Id)
                   ORDER BY s.Record_Updated_Dt DESC NULLS LAST,
                            s.ADC_UPDT DESC NULLS LAST,
                            s.Att_Dt DESC NULLS LAST
               ) AS rn
        FROM {SRC_OP} s
        WHERE s.CDS_OPA_Id IS NOT NULL AND trim(s.CDS_OPA_Id) <> '' {inc}
    )
    SELECT
        p.PERSON_ID,
        CASE WHEN p.PERSON_ID IS NOT NULL THEN 'MRN_ALIAS' END AS PERSON_LINK_METHOD,
        trim(s.CDS_OPA_Id)         AS CDS_OPA_ID,
        s.Att_Dt                   AS ATTENDANCE_DT_TM,
        s.Main_Spec_cd             AS MAIN_SPECIALTY_CD,
        s.Treat_Spec_Cd            AS TREATMENT_FUNCTION_CD,
        CAST(s.First_Attend_Cd AS STRING) AS FIRST_ATTEND_CD,
        fa.First_Attend_Desc       AS FIRST_ATTEND_DESC,
        s.Age                      AS SOURCE_AGE,
        s.Sex                      AS SOURCE_SEX_CD,
        {compact_code_array("s.OPCS_Proc", 12)} AS OPCS_PROC_CODES,
        s.NAC_HRG_Cd               AS HRG_CD,
        h.HRG_Desc                 AS HRG_DESC,
        s.Grouping_Method_Flag     AS GROUPING_METHOD_FLAG,
        s.Dom_proc                 AS DOMINANT_PROC_CD,
        op.Proc_Desc               AS DOMINANT_PROC_DESC,
        s.Errors                   AS GROUPER_ERRORS,
        s.Record_Updated_Dt        AS SOURCE_RECORD_UPDATED_DT,
        true                          AS SOURCE_PRESENT_IND,
        s.ADC_UPDT
    FROM src s
    LEFT JOIN mrn_person p ON trim(s.MRN) = p.ALIAS
    LEFT JOIN {LKP_HRG} h ON s.NAC_HRG_Cd = h.HRG_Cd
    LEFT JOIN {LKP_OPCS} op ON replace(trim(s.Dom_proc), '.', '') = op.Proc_Cd
    LEFT JOIN {LKP_FIRST_ATTEND} fa
           ON CAST(s.First_Attend_Cd AS STRING) = CAST(fa.First_Attend_Cd AS STRING)
    WHERE s.rn = 1
    """

if RUN_SLAM_HRG:
    _ensure_present_column(T_OP)
    if FORCE_FULL or not table_exists(T_OP):
        spark.sql(f"CREATE OR REPLACE TABLE {T_OP} "
                  f"TBLPROPERTIES (delta.enableChangeDataFeed = true) "
                  f"AS {_op_select_sql()}")
        mode = "FULL"
    else:
        wm = get_max_timestamp(T_OP)
        merge_upsert(spark.sql(_op_select_sql(watermark=wm)), T_OP, ["CDS_OPA_ID"])
        _relink_unresolved(T_OP, "CDS_OPA_ID", SRC_OP,
                           "trim(CDS_OPA_Id)", "trim(MRN)")
        mode = f"MERGE>{wm}"
    _ensure_present_column(T_OP)
    op_due, op_versions = due_check(
        S6B_SOURCE_VERSIONS, "slam_hrg_op_reconcile", [SRC_OP])
    if FORCE_FULL or op_due:
        _reconcile_hrg_snapshot(T_OP, SRC_OP, "CDS_OPA_ID", "trim(CDS_OPA_Id)")
        _pending_s6b_hrg_versions.append(
            ("slam_hrg_op_reconcile", op_versions))
    n = spark.table(T_OP).count()
    _results.append({"table": T_OP, "mode": mode, "rows": n})
    print(f"[SLAM-FIN] {T_OP} {mode} rows={n}")
else:
    _results.append({"table": T_OP, "mode": "SKIPPED"})


# COMMAND ----------

if RUN_SLAM_HRG:
    assert_no_direct_identifiers(T_OP)
    apply_table_comment(T_OP, (
        "SLAM HRG4+ non-admitted-consultation grouper output at outpatient-"
        "attendance grain (one row per trimmed CDS_OPA_ID) from "
        "4_prod.raw.slam_op_hrg; repeated source keys deduplicated to latest "
        "Record_Updated_Dt/ADC_UPDT. Person-resolved via MRN alias type 10 "
        "(identifier not published); OPCS inputs retained as an ordered array."
    ))
    apply_column_comments(T_OP, {
        "PERSON_ID": "Millennium PERSON_ID resolved from source MRN via mill_person_alias type 10 (deterministic tiebreak); null when unresolved — source MRN is retained only in raw.",
        "PERSON_LINK_METHOD": "How PERSON_ID was resolved: MRN_ALIAS, or null when unlinked.",
        "CDS_OPA_ID": "Trimmed CDS outpatient attendance identifier; unique key; joins back to raw.",
        "SOURCE_AGE": "NON-CANONICAL grouper input age as submitted to SLAM; demographics of record are in map_person.",
        "SOURCE_SEX_CD": "NON-CANONICAL grouper input sex code as submitted to SLAM; demographics of record are in map_person.",
        "HRG_CD": "HRG4+ non-admitted-consultation HRG assigned to the outpatient attendance.",
        "OPCS_PROC_CODES": "Ordered OPCS-4 codes as submitted to the grouper; element 1 = primary procedure.",
        "ATTENDANCE_DT_TM": "Attendance date/time as supplied; sentinel outliers 1800/2100/2999/5643 are retained losslessly — see validation baselines.",
        "ADC_UPDT": "Load/update timestamp; incremental watermark column.",
    })

# COMMAND ----------


# COMMAND ----------

_FIN_BUSINESS_COLS = [
    "TransactionID", "MRN", "NHSNumber", "EffectiveDate", "FinancialYear",
    "FinancialMonth", "ReportingYear", "ReportingMonth", "OrgIDProviderCode",
    "SiteCode", "SpecCode", "ConsultantCode", "PatientType", "POD",
    "ChargeableItem", "AdditionalInfo", "DMD", "DMDTaxonomyCode",
    "RouteOfAdministration", "Strength", "Volume", "PackSize", "Quantity",
    "UnitOfMeasure", "DispensingRoute", "DispensingLocation", "Indication",
    "FundingReference", "HRGCode", "HRGDesc", "CCGResidence", "CCGGP",
    "CommissionerCode", "CommissionerType", "ServiceLine",
    "ServiceCategoryCode", "UnitPrice_Supplier", "UnitPrice_Commissioner",
    "VAT", "VATCode", "Income", "Cost", "Margin", "Lloyds_Dispensing_Fee",
    "Production_Fee", "Fixed_Patient_Income", "CostCentreDesc", "DrugFeed",
    "DataSet", "DrugCategory", "LedgerCode", "ExclusionFlag",
    "ExclusionReason", "Lv3_Code", "Lv3_Description", "Lv6_Code",
    "Lv6_Description", "Lv7_Code", "Lv7_Description", "Lv9_Code",
    "Lv9_Description", "SLR_Code", "Diabetic_Flag", "IMCoE_Flag",
]
_FIN_HASH = "sha2(concat_ws('||', " + ", ".join(
    f"coalesce(CAST(f.{c} AS STRING), '')" for c in _FIN_BUSINESS_COLS
) + "), 256)"

_FIN_SQL = f"""
WITH {person_alias_cte(ALIAS_TYPE_MRN, "mrn_person")},
     {person_alias_cte(ALIAS_TYPE_NHS, "nhs_person")},
dmd_map AS (
    SELECT c.concept_code, c.concept_id, c.concept_name,
           MIN(std.concept_id) AS std_concept_id,
           MIN(std.concept_name) AS std_concept_name,
           COUNT(DISTINCT std.concept_id) AS std_n
    FROM {OMOP_CONCEPT} c
    LEFT JOIN {OMOP_CONCEPT_REL} r
           ON r.concept_id_1 = c.concept_id AND r.relationship_id = 'Maps to'
          AND (r.invalid_reason IS NULL OR r.invalid_reason = '')
    LEFT JOIN {OMOP_CONCEPT} std
           ON std.concept_id = r.concept_id_2 AND std.standard_concept = 'S'
    WHERE c.vocabulary_id = 'dm+d'
    GROUP BY c.concept_code, c.concept_id, c.concept_name
),
src AS (
    SELECT f.*,
        CASE WHEN trim(f.DMD) RLIKE '^[0-9]+$' THEN trim(f.DMD) END AS DMD_CODE_STRICT,
        {_FIN_HASH} AS ROW_HASH
    FROM {SRC_FIN} f
),
collapsed AS (
    SELECT *, COUNT(*) OVER (PARTITION BY ROW_HASH) AS SOURCE_DUPLICATE_COUNT,
           ROW_NUMBER() OVER (PARTITION BY ROW_HASH ORDER BY ROW_HASH) AS rn
    FROM src
)
SELECT
    coalesce(pm.PERSON_ID, pn.PERSON_ID) AS PERSON_ID,
    CASE WHEN pm.PERSON_ID IS NOT NULL THEN 'MRN_ALIAS'
         WHEN pn.PERSON_ID IS NOT NULL THEN 'NHS_ALIAS' END AS PERSON_LINK_METHOD,
    k.ROW_HASH, k.SOURCE_DUPLICATE_COUNT,
    true AS SOURCE_PRESENT_IND,
    nullif(trim(k.TransactionID), '') AS TRANSACTION_ID,
    k.FinancialYear   AS FINANCIAL_YEAR,
    k.FinancialMonth  AS FINANCIAL_MONTH,
    k.ReportingYear   AS REPORTING_YEAR,
    k.ReportingMonth  AS REPORTING_MONTH,
    k.EffectiveDate   AS EFFECTIVE_DT_TM,
    k.OrgIDProviderCode AS PROVIDER_ORG_CD,
    k.SiteCode        AS SITE_CD,
    {finance_site_case("k.SiteCode")} AS SITE_NAME,
    k.SpecCode        AS SPECIALTY_CD,
    k.ConsultantCode  AS CONSULTANT_CD,
    k.PatientType     AS PATIENT_TYPE,
    k.POD             AS POD_CD,
    k.ChargeableItem  AS CHARGEABLE_ITEM,
    k.AdditionalInfo  AS ADDITIONAL_INFO,
    k.DMD             AS DMD_RAW,
    k.DMD_CODE_STRICT AS DMD_CODE,
    d.concept_id      AS DMD_CONCEPT_ID,
    d.concept_name    AS DMD_CONCEPT_NAME,
    CASE WHEN d.std_n = 1 THEN d.std_concept_id END AS DRUG_STANDARD_CONCEPT_ID,
    CASE WHEN d.std_n = 1 THEN d.std_concept_name END AS DRUG_STANDARD_CONCEPT_NAME,
    CASE WHEN k.DMD_CODE_STRICT IS NULL THEN 'NO_CODE'
         WHEN d.concept_id IS NULL THEN 'CODE_UNMAPPED'
         WHEN d.std_n = 1 THEN 'MAPPED_STANDARD'
         ELSE 'MAPPED_DMD_ONLY' END AS DMD_MAPPING_STATUS,
    k.DMDTaxonomyCode AS DMD_TAXONOMY_CD,
    k.RouteOfAdministration AS ROUTE_OF_ADMINISTRATION,
    k.Strength AS STRENGTH, k.Volume AS VOLUME, k.PackSize AS PACK_SIZE,
    k.Quantity AS QUANTITY, k.UnitOfMeasure AS UNIT_OF_MEASURE,
    k.DispensingRoute AS DISPENSING_ROUTE,
    k.DispensingLocation AS DISPENSING_LOCATION,
    k.Indication AS INDICATION, k.FundingReference AS FUNDING_REFERENCE,
    k.HRGCode AS HCDR_CATEGORY_CD, k.HRGDesc AS HCDR_CATEGORY_DESC,
    k.CCGResidence AS CCG_RESIDENCE_CD, k.CCGGP AS CCG_GP_CD,
    k.CommissionerCode AS COMMISSIONER_CD, k.CommissionerType AS COMMISSIONER_TYPE,
    k.ServiceLine AS SERVICE_LINE, k.ServiceCategoryCode AS SERVICE_CATEGORY_CD,
    k.UnitPrice_Supplier AS UNIT_PRICE_SUPPLIER,
    k.UnitPrice_Commissioner AS UNIT_PRICE_COMMISSIONER,
    k.VAT, k.VATCode AS VAT_CD,
    k.Income AS INCOME, k.Cost AS COST, k.Margin AS MARGIN,
    k.Lloyds_Dispensing_Fee AS LLOYDS_DISPENSING_FEE,
    k.Production_Fee AS PRODUCTION_FEE,
    k.Fixed_Patient_Income AS FIXED_PATIENT_INCOME,
    k.CostCentreDesc AS COST_CENTRE_DESC,
    k.DrugFeed AS DRUG_FEED, k.DataSet AS DATA_SET,
    k.DrugCategory AS DRUG_CATEGORY, k.LedgerCode AS LEDGER_CD,
    k.ExclusionFlag AS EXCLUSION_FLAG, k.ExclusionReason AS EXCLUSION_REASON,
    k.Lv3_Code AS LEDGER_LV3_CD, k.Lv3_Description AS LEDGER_LV3_DESC,
    k.Lv6_Code AS LEDGER_LV6_CD, k.Lv6_Description AS LEDGER_LV6_DESC,
    k.Lv7_Code AS LEDGER_LV7_CD, k.Lv7_Description AS LEDGER_LV7_DESC,
    k.Lv9_Code AS LEDGER_LV9_CD, k.Lv9_Description AS LEDGER_LV9_DESC,
    k.SLR_Code AS SLR_CD, k.Diabetic_Flag AS DIABETIC_FLAG,
    k.IMCoE_Flag AS IMCOE_FLAG,
    current_timestamp() AS ADC_UPDT
FROM collapsed k
LEFT JOIN mrn_person pm ON trim(k.MRN) = pm.ALIAS
LEFT JOIN nhs_person pn
       ON regexp_replace(k.NHSNumber, ' ', '') = pn.ALIAS
LEFT JOIN dmd_map d ON k.DMD_CODE_STRICT = d.concept_code
WHERE k.rn = 1
"""

if RUN_FINANCE:
    fin_ver = source_version(SRC_FIN)
    prev_ver = get_state_version(CONTROL_SCHEMA, T_FIN, SRC_FIN)
    if not FORCE_FULL and prev_ver == fin_ver and table_exists(T_FIN):
        mode = "UNCHANGED_SKIP"
    elif FORCE_FULL or not table_exists(T_FIN):
        spark.sql(f"CREATE OR REPLACE TABLE {T_FIN} "
                  f"TBLPROPERTIES (delta.enableChangeDataFeed = true) "
                  f"AS {_FIN_SQL}")
        mode = "FULL"
    else:
        snap = spark.sql(_FIN_SQL)
        merge_upsert(snap, T_FIN, ["ROW_HASH"])
        # soft-delete: rows no longer present in the current snapshot
        spark.sql(f"""
            MERGE INTO {T_FIN} t
            USING (SELECT ROW_HASH FROM ({_FIN_SQL})) s
            ON t.ROW_HASH = s.ROW_HASH
            WHEN NOT MATCHED BY SOURCE AND t.SOURCE_PRESENT_IND THEN UPDATE SET
                t.SOURCE_PRESENT_IND = false, t.ADC_UPDT = current_timestamp()
        """)
        mode = "SNAPSHOT_MERGE"
    n = spark.table(T_FIN).count() if table_exists(T_FIN) else 0
    _results.append({"table": T_FIN, "mode": mode, "rows": n,
                     "source_version": fin_ver})
    print(f"[SLAM-FIN] {T_FIN} {mode} rows={n} src_ver={fin_ver}")
else:
    _results.append({"table": T_FIN, "mode": "SKIPPED"})


# COMMAND ----------

if RUN_FINANCE and table_exists(T_FIN):
    assert_no_direct_identifiers(T_FIN)
    apply_table_comment(T_FIN, (
        "Patient-level SLR high-cost drug/device reimbursement expenditure from "
        "4_prod.raw.finance_slr_hcdr_expenditure_report. No natural source key "
        "(TransactionID is blank on a material share of rows): ROW_HASH is a "
        "content hash over all business columns, with exact duplicates collapsed "
        "using SOURCE_DUPLICATE_COUNT (reconstruct totals as measure × count). "
        "The source is a full snapshot: rows absent from the latest snapshot are "
        "retained with SOURCE_PRESENT_IND=false, never deleted. Person-resolved "
        "via MRN alias type 10 then NHS-number alias type 18; identifiers are not "
        "published. HCDR_CATEGORY_CD is the source HRGCode, a local high-cost-"
        "drug category and not a standard HRG. dm+d is strictly matched using "
        "all-numeric codes only, with a standard hop only for one valid Maps-to "
        "target. HomeDeliveryCharge is omitted because it was blank on all rows "
        "at design time."
    ))
    apply_column_comments(T_FIN, {
        "ROW_HASH": "SHA-256 content hash over every source business column; primary key for snapshot reconciliation.",
        "SOURCE_DUPLICATE_COUNT": "Number of exact-identical source rows represented by this published row; reconstruct additive measures as measure × count.",
        "SOURCE_PRESENT_IND": "True when present in the latest full source snapshot; false marks a restated/disappeared row retained for audit.",
        "PERSON_LINK_METHOD": "How PERSON_ID was resolved: MRN_ALIAS first, NHS_ALIAS fallback, or null when unresolved.",
        "DMD_RAW": "Verbatim source dm+d value; includes junk values such as No SNOMED.",
        "DMD_CODE": "Strict trimmed all-numeric dm+d code; null for blank or non-numeric source values.",
        "DMD_MAPPING_STATUS": "dm+d mapping state: NO_CODE, CODE_UNMAPPED, MAPPED_STANDARD, or MAPPED_DMD_ONLY.",
        "DRUG_STANDARD_CONCEPT_ID": "OMOP standard concept reached only when exactly one valid Maps-to target exists.",
        "HCDR_CATEGORY_CD": "Local high-cost-drug reimbursement category from source HRGCode; not an HRG4+ code.",
        "SITE_NAME": "Curated mapping for RLH, SBH, WXH, NUH and NGH only; other values are unresolved by design.",
        "EFFECTIVE_DT_TM": "Effective date/time as supplied; two known rows dated 2122 are retained losslessly.",
        "ADC_UPDT": "Pipeline insert/update timestamp.",
    })

# COMMAND ----------


# COMMAND ----------

COMMUNITY_SOURCE_COLUMNS = {
    "PARTCOST": ("e.PartCost", "PARTIAL_COSTING_IND", "STRING"),
    "CAREDTE": ("e.CareDte", "CARE_DT_TM", "TIMESTAMP"),
    "CAREID": ("e.CareId", "CARE_ID", "STRING"),
    "CLINDURATION": ("e.ClinDuration", "CLINICAL_CONTACT_DURATION", "INT"),
    "CHSCCY": ("e.ChsCcy", "CHS_CURRENCY_CD", "STRING"),
    "TEAMTYPE": ("e.TeamType", "TEAM_TYPE_CD", "INT"),
    "CCSUBJECT": ("e.CCSubject", "CONTACT_SUBJECT_CD", "STRING"),
    "CONSULTTYPE": ("e.ConsultType", "CONSULT_TYPE_CD", "STRING"),
    "CMEDIUM": ("e.CMedium", "CONSULT_MEDIUM_CD", "STRING"),
    "LOCCODE": ("e.LocCode", "LOCATION_CD", "STRING"),
    "GPTHERAPYIND": ("e.GPTherapyInd", "GP_THERAPY_IND", "STRING"),
    "SERREQID": ("e.SerReqID", "SERVICE_REQUEST_ID", "STRING"),
}

PLIC_EP_REQUIRED = {
    "ACTIVITYRECORDID", "FEEDTYPE", "PLEMI", "NHSNO", "NHSST", "CDSID", "ATTID",
    "ACTIVITYSTARTDATE", "ACTIVITYENDDATE", "ARRDATE", "ARRTIME", "DEPDATE", "DEPTIME",
    "DEPTYP", "ORGID", "PATORGID", "PATHID", "POD", "TFC", "ALOS", "CFBAND",
    "EPINO", "EPSTDTE", "EPENDTE", "EPTYPE", "HSPELLNO", "HRG", "HRGFCE", "HRGSPL",
    "APPDATE", "APPTIME", "CCUF", "ORGSSUPP", "CCPERTYPE", "CCLI", "UNACTDATE",
    "UNACT", "UNHRG",
}
PLIC_COST_REQUIRED = {
    "ACTIVITYRECORDID", "ACTCSTID", "RESCSTID", "ACTCNT", "CTPCSIU",
    "UNCUR", "CTPUNCURDATE", "TOTCST", "TOTOCST",
}

# COMMAND ----------

def _schema_columns(table_name):
    return {c.upper().replace("_", "") for c in spark.table(table_name).columns}

def _require_plics_schema(ep_table, cost_table):
    ep_cols = _schema_columns(ep_table)
    cost_cols = _schema_columns(cost_table)
    missing_ep = sorted(PLIC_EP_REQUIRED - ep_cols)
    missing_cost = sorted(PLIC_COST_REQUIRED - cost_cols)
    assert not missing_ep, f"{ep_table}: missing EpisodeCtp columns {missing_ep}"
    assert not missing_cost, f"{cost_table}: missing EpisodeCostCtp columns {missing_cost}"

def community_cols_present(src_table):
    """Schema-driven community-column switch; no extract-name hardcode."""
    cols = _schema_columns(src_table)
    present = sorted(set(COMMUNITY_SOURCE_COLUMNS) & cols)
    if present and len(present) != len(COMMUNITY_SOURCE_COLUMNS):
        missing = sorted(set(COMMUNITY_SOURCE_COLUMNS) - cols)
        raise AssertionError(
            f"{src_table}: partial community schema is unsafe; present={present}, missing={missing}")
    return len(present) == len(COMMUNITY_SOURCE_COLUMNS)

def _community_select(src_table):
    if community_cols_present(src_table):
        return ",\n        ".join(
            (
                f"CAST({source_expr} AS {dtype}) AS {target_col}"
                if target_col == "TEAM_TYPE_CD"
                else f"{source_expr} AS {target_col}"
            )
            for source_expr, target_col, dtype in COMMUNITY_SOURCE_COLUMNS.values()
        )
    return ",\n        ".join(
        f"CAST(NULL AS {dtype}) AS {target_col}"
        for _, target_col, dtype in COMMUNITY_SOURCE_COLUMNS.values()
    )

def _plics_ep_branch(extract_cd, src_table, version):
    community = _community_select(src_table)
    return f"""
    SELECT '{extract_cd}' AS EXTRACT_CD, e.ActivityRecordID AS ACTIVITY_RECORD_ID,
        e.FeedType AS FEED_TYPE, e.PLEMI AS PLEMI,
        e.NHSNo AS _NHSNO, e.NhsSt AS NHS_NUMBER_STATUS_CD,
        trim(e.CDSID) AS CDS_ID, e.AttID AS ATTENDANCE_ID,
        e.ActivityStartDate AS ACTIVITY_START_DT_TM,
        e.ActivityEndDate AS ACTIVITY_END_DT_TM,
        e.ArrDate AS ARRIVAL_DT, e.ArrTime AS ARRIVAL_TM,
        e.DepDate AS DEPARTURE_DT, e.DepTime AS DEPARTURE_TM,
        e.DepTyp AS DEPARTURE_TYPE_CD,
        e.OrgId AS PROVIDER_ORG_CD, e.PatOrgId AS PATIENT_ORG_CD,
        e.PathID AS PATHWAY_ID, e.Pod AS POD_CD,
        e.Tfc AS TREATMENT_FUNCTION_CD, e.Alos AS SOURCE_LOS,
        e.CFBand AS CF_BAND_CD, e.EpiNo AS EPISODE_NUMBER,
        e.EpStDte AS EPISODE_START_DT_TM, e.EpEnDte AS EPISODE_END_DT_TM,
        e.EpType AS EPISODE_TYPE_CD, e.HSpellNo AS HOSP_SPELL_ID,
        e.HRG AS HRG_CD, e.HrgFce AS FCE_HRG_CD, e.HrgSpl AS SPELL_HRG_CD,
        e.AppDate AS APPOINTMENT_DT, e.AppTime AS APPOINTMENT_TM,
        e.CCUF AS CRITICAL_CARE_UNIT_FUNCTION_CD, e.OrgsSupp AS ORGANS_SUPPORTED,
        e.CCPerType AS CRITICAL_CARE_PERIOD_TYPE_CD, e.CCLI AS CRITICAL_CARE_LEVEL_IND,
        e.UnActDate AS UNBUNDLED_ACTIVITY_DT_TM, e.UnAct AS UNBUNDLED_ACTIVITY_CD,
        e.UnHRG AS UNBUNDLED_HRG_CD,
        {community}
    FROM {src_table} VERSION AS OF {version} e
    """

def _plics_cost_branch(extract_cd, src_table, version):
    return f"""
    SELECT '{extract_cd}' AS EXTRACT_CD, c.ActivityRecordID AS ACTIVITY_RECORD_ID,
        c.ActCstID AS ACTIVITY_COST_ITEM_CD, c.ResCstID AS RESOURCE_COST_ITEM_CD,
        c.ActCnt AS ACTIVITY_COUNT, c.CTPCSIU AS UNBUNDLED_SUBTYPE_CD,
        c.UnCur AS UNBUNDLED_CURRENCY_CD, c.CTPUnCurDate AS UNBUNDLED_CURRENCY_DT_TM,
        c.TotCst AS TOTAL_COST, c.TotOCst AS TOTAL_O_COST
    FROM {src_table} VERSION AS OF {version} c
    """

def _stage_tables(extract_cd):
    slug = "".join(c.lower() if c.isalnum() else "_" for c in extract_cd).strip("_")
    return (
        f"{TARGET_SCHEMA}.s6_plics_stage_{slug}_activity",
        f"{TARGET_SCHEMA}.s6_plics_stage_{slug}_cost",
    )

def _df_fingerprint(df, exclude=("ADC_UPDT",)):
    cols = [c for c in df.columns if c not in exclude]
    return (
        df.select(F.sum(
            F.xxhash64(F.to_json(F.struct(*[F.col(c) for c in cols])))
             .cast("decimal(38,0)")).alias("fp"))
        .collect()[0]["fp"]
    )

def _history_metrics(table_name):
    row = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()[0]
    return {
        "version": int(row["version"]),
        "operation": row["operation"],
        "operationMetrics": dict(row["operationMetrics"] or {}),
    }

def _source_count_at(table_name, version):
    return spark.read.option("versionAsOf", version).table(table_name).count()

# COMMAND ----------

def _build_plics_stage(extract_cd, ep_table, cost_table, ep_version, cost_version):
    stage_act, stage_cost = _stage_tables(extract_cd)
    _require_plics_schema(ep_table, cost_table)
    cost_branch = _plics_cost_branch(extract_cd, cost_table, cost_version)
    spark.sql(f"""
        CREATE OR REPLACE TABLE {stage_cost}
        TBLPROPERTIES (delta.enableChangeDataFeed = true) AS
        WITH unioned AS ({cost_branch})
        SELECT EXTRACT_CD, ACTIVITY_RECORD_ID,
            sha2(concat_ws('||',
                coalesce(ACTIVITY_COST_ITEM_CD,''), coalesce(RESOURCE_COST_ITEM_CD,''),
                coalesce(ACTIVITY_COUNT,''), coalesce(UNBUNDLED_SUBTYPE_CD,''),
                coalesce(UNBUNDLED_CURRENCY_CD,''),
                coalesce(CAST(UNBUNDLED_CURRENCY_DT_TM AS STRING),''),
                coalesce(CAST(TOTAL_COST AS STRING),''),
                coalesce(CAST(TOTAL_O_COST AS STRING),'')
            ), 256) AS LINE_HASH,
            ACTIVITY_COST_ITEM_CD, RESOURCE_COST_ITEM_CD, ACTIVITY_COUNT,
            UNBUNDLED_SUBTYPE_CD, UNBUNDLED_CURRENCY_CD, UNBUNDLED_CURRENCY_DT_TM,
            TOTAL_COST, TOTAL_O_COST,
            COUNT(*) AS SOURCE_DUPLICATE_COUNT,
            current_timestamp() AS ADC_UPDT,
            true AS SOURCE_PRESENT_IND
        FROM unioned
        GROUP BY ALL
    """)

    ep_branch = _plics_ep_branch(extract_cd, ep_table, ep_version)
    spark.sql(f"""
        CREATE OR REPLACE TABLE {stage_act}
        TBLPROPERTIES (delta.enableChangeDataFeed = true) AS
        WITH {person_alias_cte(ALIAS_TYPE_NHS, "nhs_person")},
        {person_alias_cte(ALIAS_TYPE_MRN, "mrn_person")},
        cds_mrn AS (
            SELECT K, M FROM (
                SELECT trim(CDS_APC_Id) AS K, trim(MRN) AS M,
                       ROW_NUMBER() OVER (PARTITION BY trim(CDS_APC_Id)
                           ORDER BY ADC_UPDT DESC NULLS LAST) rn
                FROM {SRC_APC} WHERE CDS_APC_Id IS NOT NULL
            ) WHERE rn = 1
            UNION ALL
            SELECT K, M FROM (
                SELECT trim(CDS_OPA_Id) AS K, trim(MRN) AS M,
                       ROW_NUMBER() OVER (PARTITION BY trim(CDS_OPA_Id)
                           ORDER BY ADC_UPDT DESC NULLS LAST) rn
                FROM {SRC_OP} WHERE CDS_OPA_Id IS NOT NULL
            ) WHERE rn = 1
        ),
        unioned AS ({ep_branch}),
        costs AS (
            SELECT EXTRACT_CD, ACTIVITY_RECORD_ID,
                   SUM(SOURCE_DUPLICATE_COUNT) AS COST_LINE_COUNT,
                   SUM(CAST(TOTAL_COST AS DECIMAL(31,18)) *
                       CAST(SOURCE_DUPLICATE_COUNT AS DECIMAL(6,0))) AS TOTAL_COST_SUM,
                   SUM(CAST(TOTAL_O_COST AS DECIMAL(31,18)) *
                       CAST(SOURCE_DUPLICATE_COUNT AS DECIMAL(6,0))) AS TOTAL_O_COST_SUM
            FROM {stage_cost} GROUP BY 1, 2
        )
        SELECT
            coalesce(pn.PERSON_ID, pm2.PERSON_ID) AS PERSON_ID,
            CASE WHEN pn.PERSON_ID IS NOT NULL THEN 'NHS_ALIAS'
                 WHEN pm2.PERSON_ID IS NOT NULL THEN 'CDS_MRN' END AS PERSON_LINK_METHOD,
            u.EXTRACT_CD, u.ACTIVITY_RECORD_ID, u.FEED_TYPE, u.PLEMI,
            u.NHS_NUMBER_STATUS_CD, u.CDS_ID, u.ATTENDANCE_ID,
            u.ACTIVITY_START_DT_TM, u.ACTIVITY_END_DT_TM,
            u.ARRIVAL_DT, u.ARRIVAL_TM, u.DEPARTURE_DT, u.DEPARTURE_TM,
            u.DEPARTURE_TYPE_CD, u.PROVIDER_ORG_CD, u.PATIENT_ORG_CD,
            u.PATHWAY_ID, u.POD_CD, u.TREATMENT_FUNCTION_CD, u.SOURCE_LOS,
            u.CF_BAND_CD, u.EPISODE_NUMBER, u.EPISODE_START_DT_TM,
            u.EPISODE_END_DT_TM, u.EPISODE_TYPE_CD, u.HOSP_SPELL_ID,
            u.HRG_CD, h.HRG_Desc AS HRG_DESC,
            u.FCE_HRG_CD, hf.HRG_Desc AS FCE_HRG_DESC,
            u.SPELL_HRG_CD, hs.HRG_Desc AS SPELL_HRG_DESC,
            u.APPOINTMENT_DT, u.APPOINTMENT_TM,
            u.CRITICAL_CARE_UNIT_FUNCTION_CD, u.ORGANS_SUPPORTED,
            u.CRITICAL_CARE_PERIOD_TYPE_CD, u.CRITICAL_CARE_LEVEL_IND,
            u.UNBUNDLED_ACTIVITY_DT_TM, u.UNBUNDLED_ACTIVITY_CD,
            u.UNBUNDLED_HRG_CD, hu.HRG_Desc AS UNBUNDLED_HRG_DESC,
            u.PARTIAL_COSTING_IND, u.CARE_DT_TM, u.CARE_ID,
            u.CLINICAL_CONTACT_DURATION, u.CHS_CURRENCY_CD, u.TEAM_TYPE_CD,
            u.CONTACT_SUBJECT_CD, u.CONSULT_TYPE_CD, u.CONSULT_MEDIUM_CD,
            u.LOCATION_CD, u.GP_THERAPY_IND, u.SERVICE_REQUEST_ID,
            c.COST_LINE_COUNT, c.TOTAL_COST_SUM, c.TOTAL_O_COST_SUM,
            current_timestamp() AS ADC_UPDT,
            true AS SOURCE_PRESENT_IND
        FROM unioned u
        LEFT JOIN nhs_person pn ON regexp_replace(u._NHSNO, ' ', '') = pn.ALIAS
        LEFT JOIN cds_mrn cm ON u.CDS_ID = cm.K
        LEFT JOIN mrn_person pm2 ON cm.M = pm2.ALIAS
        LEFT JOIN costs c ON u.EXTRACT_CD = c.EXTRACT_CD
                         AND u.ACTIVITY_RECORD_ID = c.ACTIVITY_RECORD_ID
        LEFT JOIN {LKP_HRG} h  ON u.HRG_CD = h.HRG_Cd
        LEFT JOIN {LKP_HRG} hf ON u.FCE_HRG_CD = hf.HRG_Cd
        LEFT JOIN {LKP_HRG} hs ON u.SPELL_HRG_CD = hs.HRG_Cd
        LEFT JOIN {LKP_HRG} hu ON u.UNBUNDLED_HRG_CD = hu.HRG_Cd
    """)
    return stage_act, stage_cost

# COMMAND ----------

def _validate_plics_stage(extract_cd, ep_table, cost_table,
                          ep_version, cost_version, stage_act, stage_cost):
    ep_source_rows = _source_count_at(ep_table, ep_version)
    cost_source_rows = _source_count_at(cost_table, cost_version)
    assert ep_source_rows > 0, f"{extract_cd}: EpisodeCtp source is empty"
    assert cost_source_rows > 0, f"{extract_cd}: EpisodeCostCtp source is empty"

    act = spark.table(stage_act)
    cost = spark.table(stage_cost)
    act_stats = act.agg(
        F.count("*").alias("rows"),
        F.count("ACTIVITY_RECORD_ID").alias("filled"),
        F.countDistinct("ACTIVITY_RECORD_ID").alias("keys"),
    ).collect()[0]
    cost_stats = cost.agg(
        F.count("*").alias("rows"),
        F.count("ACTIVITY_RECORD_ID").alias("activity_filled"),
        F.count("LINE_HASH").alias("hash_filled"),
        F.countDistinct(F.struct(
            "EXTRACT_CD", "ACTIVITY_RECORD_ID", "LINE_HASH")).alias("keys"),
        F.sum("SOURCE_DUPLICATE_COUNT").alias("represented_rows"),
    ).collect()[0]
    no_orphans = (
        cost.select("EXTRACT_CD", "ACTIVITY_RECORD_ID").distinct()
        .join(act.select("EXTRACT_CD", "ACTIVITY_RECORD_ID").distinct(),
              ["EXTRACT_CD", "ACTIVITY_RECORD_ID"], "left_anti").count()
    )
    costless_activities = (
        act.select("EXTRACT_CD", "ACTIVITY_RECORD_ID").distinct()
        .join(cost.select("EXTRACT_CD", "ACTIVITY_RECORD_ID").distinct(),
              ["EXTRACT_CD", "ACTIVITY_RECORD_ID"], "left_anti").count()
    )
    spans = act.agg(
        F.min("ACTIVITY_START_DT_TM").alias("min_activity_start"),
        F.max("ACTIVITY_START_DT_TM").alias("max_activity_start"),
        F.min("ACTIVITY_END_DT_TM").alias("min_activity_end"),
        F.max("ACTIVITY_END_DT_TM").alias("max_activity_end"),
        F.min("EPISODE_START_DT_TM").alias("min_episode_start"),
        F.max("EPISODE_END_DT_TM").alias("max_episode_end"),
    ).collect()[0].asDict()
    for lo, hi in (
        ("min_activity_start", "max_activity_start"),
        ("min_activity_end", "max_activity_end"),
        ("min_episode_start", "max_episode_end"),
    ):
        if spans[lo] is not None and spans[hi] is not None:
            assert spans[lo] <= spans[hi], f"{extract_cd}: invalid observed span {lo}>{hi}"

    raw_cost = (
        spark.read.option("versionAsOf", cost_version).table(cost_table)
        .agg(
            F.round(F.sum(F.col("TotCst").cast("decimal(38,18)")), 2).alias("total"),
            F.round(F.sum(F.col("TotOCst").cast("decimal(38,18)")), 2).alias("ototal"))
        .collect()[0]
    )
    stage_cost_total = cost.agg(
        F.round(F.sum(
            F.col("TOTAL_COST").cast("decimal(38,18)") *
            F.col("SOURCE_DUPLICATE_COUNT").cast("decimal(38,0)")), 2).alias("total"),
        F.round(F.sum(
            F.col("TOTAL_O_COST").cast("decimal(38,18)") *
            F.col("SOURCE_DUPLICATE_COUNT").cast("decimal(38,0)")), 2).alias("ototal"),
    ).collect()[0]
    # Source cost fields are floating-point in these extracts. Recasting and summing tens of
    # millions of rows can move the aggregate by sub-pound amounts; use £1 or 1 ppb, whichever
    # is larger, while duplicate accounting remains exact.
    total_diff = abs(float(raw_cost["total"]) - float(stage_cost_total["total"]))
    ototal_diff = abs(float(raw_cost["ototal"]) - float(stage_cost_total["ototal"]))
    total_tolerance = max(1.0, abs(float(raw_cost["total"])) * 1e-9)
    ototal_tolerance = max(1.0, abs(float(raw_cost["ototal"])) * 1e-9)

    cross_extract_overlap = 0
    if table_exists(T_ACT):
        cross_extract_overlap = (
            act.select("ACTIVITY_RECORD_ID").distinct().alias("s")
            .join(
                spark.table(T_ACT).where(F.col("EXTRACT_CD") != extract_cd)
                     .select("ACTIVITY_RECORD_ID").distinct().alias("t"),
                "ACTIVITY_RECORD_ID", "inner").count()
        )

    checks = [
        {"check": f"{extract_cd}_activity_pk", "ok":
            act_stats["rows"] == act_stats["filled"] == act_stats["keys"] == ep_source_rows,
         "stage_rows": int(act_stats["rows"]), "source_rows": int(ep_source_rows)},
        {"check": f"{extract_cd}_cost_pk", "ok":
            cost_stats["rows"] == cost_stats["activity_filled"] ==
            cost_stats["hash_filled"] == cost_stats["keys"],
         "stage_rows": int(cost_stats["rows"]), "keys": int(cost_stats["keys"])},
        {"check": f"{extract_cd}_duplicate_accounting", "ok":
            int(cost_stats["represented_rows"] or 0) == cost_source_rows,
         "represented_rows": int(cost_stats["represented_rows"] or 0),
         "source_rows": int(cost_source_rows)},
        {"check": f"{extract_cd}_no_orphans", "ok": no_orphans == 0,
         "orphans": int(no_orphans)},
        {"check": f"{extract_cd}_cost_conserved", "ok":
            total_diff <= total_tolerance and ototal_diff <= ototal_tolerance,
         "raw_total": str(raw_cost["total"]), "stage_total": str(stage_cost_total["total"]),
         "total_diff": total_diff, "total_tolerance": total_tolerance,
         "raw_ototal": str(raw_cost["ototal"]), "stage_ototal": str(stage_cost_total["ototal"]),
         "ototal_diff": ototal_diff, "ototal_tolerance": ototal_tolerance},
    ]
    assert all(c["ok"] for c in checks), (
        f"{extract_cd}: staging validation failed {[c for c in checks if not c['ok']]}")
    evidence = {
        "extract_cd": extract_cd,
        "source_versions": {ep_table: int(ep_version), cost_table: int(cost_version)},
        "community_columns_present": community_cols_present(ep_table),
        "source_rows": {"activity": int(ep_source_rows), "cost": int(cost_source_rows)},
        "stage_rows": {"activity": int(act_stats["rows"]), "cost": int(cost_stats["rows"])},
        "costless_activities": int(costless_activities),
        "observed_date_spans": {k: str(v) if v is not None else None for k, v in spans.items()},
        "cross_extract_activity_id_overlap": int(cross_extract_overlap),
        "fingerprints": {
            "activity": str(_df_fingerprint(act)),
            "cost": str(_df_fingerprint(cost)),
        },
        "stage_history": {
            "activity": _history_metrics(stage_act),
            "cost": _history_metrics(stage_cost),
        },
    }
    return checks, evidence

# COMMAND ----------

def _ensure_target_from_stage(target, stage):
    if not table_exists(target):
        spark.sql(f"""
            CREATE TABLE {target}
            TBLPROPERTIES (delta.enableChangeDataFeed = true)
            AS SELECT * FROM {stage} WHERE 1 = 0
        """)
    _ensure_present_column(target)

    target_fields = spark.table(target).schema.fields
    target_columns = [field.name for field in target_fields]
    stage_df = spark.table(stage)
    if "SOURCE_ABSENT_DETECTED_TS" not in stage_df.columns:
        stage_df = stage_df.withColumn(
            "SOURCE_ABSENT_DETECTED_TS", F.lit(None).cast("timestamp")
        )

    target_schema = [
        (field.name, field.dataType.simpleString()) for field in target_fields
    ]
    stage_schema = [
        (field.name, field.dataType.simpleString())
        for field in stage_df.select(*target_columns).schema.fields
    ]
    assert target_schema == stage_schema, (
        f"{target}: stage schema drift; target={target_schema}, "
        f"stage={stage_schema}"
    )
    return stage_df.select(*target_columns)


def _replace_validated_slice(target, stage, extract_cd):
    stage_df = _ensure_target_from_stage(target, stage)
    before_other = spark.table(target).where(
        F.col("EXTRACT_CD") != extract_cd
    ).count()
    keys = (
        ["EXTRACT_CD", "ACTIVITY_RECORD_ID", "LINE_HASH"]
        if "LINE_HASH" in stage_df.columns
        else ["EXTRACT_CD", "ACTIVITY_RECORD_ID"]
    )
    prior = spark.table(target).where(F.col("EXTRACT_CD") == extract_cd)
    gone = (
        prior.join(stage_df.select(*keys).distinct(), keys, "left_anti")
        .withColumn("SOURCE_PRESENT_IND", F.lit(False))
        .withColumn(
            "SOURCE_ABSENT_DETECTED_TS",
            F.coalesce(
                F.col("SOURCE_ABSENT_DETECTED_TS"),
                F.current_timestamp(),
            ),
        )
        .withColumn("ADC_UPDT", F.current_timestamp())
    )
    publish = stage + "__s6b_publish"
    (
        stage_df.unionByName(gone, allowMissingColumns=False)
        .write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(publish)
    )
    spark.sql(f"""
        INSERT INTO {target}
        REPLACE WHERE EXTRACT_CD = '{extract_cd}'
        SELECT * FROM {publish}
    """)
    after_other = spark.table(target).where(
        F.col("EXTRACT_CD") != extract_cd
    ).count()
    assert before_other == after_other, (
        f"{target}: REPLACE WHERE changed another extract "
        f"({before_other}->{after_other})"
    )
    target_present = spark.table(target).where(
        (F.col("EXTRACT_CD") == extract_cd) & F.col("SOURCE_PRESENT_IND")
    )
    assert stage_df.count() == target_present.count(), (
        f"{target}: present slice row-count mismatch"
    )
    assert _df_fingerprint(stage_df) == _df_fingerprint(target_present), (
        f"{target}: present slice fingerprint differs from validated stage"
    )
    spark.sql(f"DROP TABLE IF EXISTS {publish}")


# COMMAND ----------

def _s6b_plics_lifecycle_fixture():
    target = f"{TARGET_SCHEMA}.s6b_plics_target_fixture"
    stage = f"{TARGET_SCHEMA}.s6b_plics_stage_fixture"
    spark.sql(f"DROP TABLE IF EXISTS {target}")
    spark.sql(f"DROP TABLE IF EXISTS {stage}")
    base = spark.createDataFrame(
        [("X", "A", True), ("X", "B", True)],
        "EXTRACT_CD STRING, ACTIVITY_RECORD_ID STRING, SOURCE_PRESENT_IND BOOLEAN",
    ).withColumn("ADC_UPDT", F.current_timestamp())
    base.write.saveAsTable(target)
    base.where("ACTIVITY_RECORD_ID <> 'B'").write.saveAsTable(stage)
    _replace_validated_slice(target, stage, "X")
    assert spark.table(target).where("ACTIVITY_RECORD_ID='B'").collect()[0]["SOURCE_PRESENT_IND"] is False
    base.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(stage)
    _replace_validated_slice(target, stage, "X")
    assert spark.table(target).where("ACTIVITY_RECORD_ID='B'").collect()[0]["SOURCE_PRESENT_IND"] is True
    spark.sql(f"DROP TABLE IF EXISTS {target}")
    spark.sql(f"DROP TABLE IF EXISTS {stage}")
    print("[S6B_SLAM] PLICS tombstone/resurrection fixture PASS")

def _slice_exists(target, extract_cd):
    return table_exists(target) and (
        spark.table(target).where(F.col("EXTRACT_CD") == extract_cd).limit(1).count() == 1)

def _extract_versions(ep_table, cost_table):
    return {ep_table: source_version(ep_table), cost_table: source_version(cost_table)}

def _extract_due(extract_cd, ep_table, cost_table, versions, ignore_force=False):
    return (
        (FORCE_FULL and not ignore_force)
        or (table_exists(T_ACT) and "SOURCE_PRESENT_IND" not in spark.table(T_ACT).columns)
        or (table_exists(T_COST) and "SOURCE_PRESENT_IND" not in spark.table(T_COST).columns)
        or not _slice_exists(T_ACT, extract_cd)
        or not _slice_exists(T_COST, extract_cd)
        or get_state_version(CONTROL_SCHEMA, T_ACT, ep_table) != versions[ep_table]
        or get_state_version(CONTROL_SCHEMA, T_COST, cost_table) != versions[cost_table]
    )

def _apply_plics_tags():
    for table_name in (T_ACT, T_COST):
        explicit = {}
        for col, dtype in spark.table(table_name).dtypes:
            if col == "PERSON_ID" or col.endswith("_ID") or dtype == "string":
                explicit[col] = ("4", "2")
        ig_tag_table(table_name, explicit)
        ig_tag_gate(table_name)

_plics_stage_validation = []

_plics_evidence = []
_pending_plics_state_commits = []
_processed_plics_extracts = []

if RUN_PLICS:
    assert PROCESS_EXTRACT == "ALL" or PROCESS_EXTRACT in SRC_PLICS, (
        f"Unknown process_extract={PROCESS_EXTRACT}; choose ALL or one of {sorted(SRC_PLICS)}")
    for extract_cd, (ep_table, cost_table) in SRC_PLICS.items():
        if PROCESS_EXTRACT not in {"ALL", extract_cd}:
            _results.append({"table": T_ACT, "extract_cd": extract_cd, "mode": "NOT_SELECTED", "rows": 0})
            _results.append({"table": T_COST, "extract_cd": extract_cd, "mode": "NOT_SELECTED", "rows": 0})
            continue
        versions = _extract_versions(ep_table, cost_table)
        if not _extract_due(extract_cd, ep_table, cost_table, versions):
            _results.append({"table": T_ACT, "extract_cd": extract_cd,
                             "mode": "UNCHANGED_SKIP",
                             "rows": spark.table(T_ACT).where(
                                 F.col("EXTRACT_CD") == extract_cd).count()})
            _results.append({"table": T_COST, "extract_cd": extract_cd,
                             "mode": "UNCHANGED_SKIP",
                             "rows": spark.table(T_COST).where(
                                 F.col("EXTRACT_CD") == extract_cd).count()})
            continue

        repeats = 3 if BENCHMARK_MODE and BENCHMARK_EXTRACT in {extract_cd, "ALL"} else 1
        repeat_evidence = []
        for repeat_no in range(1, repeats + 1):
            started = time.perf_counter()
            stage_act, stage_cost = _build_plics_stage(
                extract_cd, ep_table, cost_table,
                versions[ep_table], versions[cost_table])
            checks, evidence = _validate_plics_stage(
                extract_cd, ep_table, cost_table,
                versions[ep_table], versions[cost_table], stage_act, stage_cost)
            evidence["repeat_no"] = repeat_no
            evidence["elapsed_seconds"] = round(time.perf_counter() - started, 3)
            repeat_evidence.append(evidence)
            if repeat_no > 1:
                assert evidence["fingerprints"] == repeat_evidence[0]["fingerprints"], (
                    f"{extract_cd}: repeated stage fingerprints differ")
        if repeats == 3:
            durations = [e["elapsed_seconds"] for e in repeat_evidence]
            repeat_evidence[-1]["warm_median_seconds_x3"] = statistics.median(durations)

        _plics_stage_validation.extend(checks)
        _plics_evidence.extend(repeat_evidence)
        if BENCHMARK_INTERRUPT_AFTER_STAGE == extract_cd:
            raise RuntimeError(
                f"INTENTIONAL_RETRY_TEST {extract_cd}: validated stage retained; "
                "target slices and state are untouched")

        _replace_validated_slice(T_COST, stage_cost, extract_cd)
        _replace_validated_slice(T_ACT, stage_act, extract_cd)
        _pending_plics_state_commits.extend([
            (T_ACT, ep_table, versions[ep_table]),
            (T_COST, cost_table, versions[cost_table]),
        ])
        _processed_plics_extracts.append(extract_cd)
        _results.append({"table": T_ACT, "extract_cd": extract_cd,
                         "mode": "STAGE_VALIDATE_REPLACE",
                         "rows": spark.table(T_ACT).where(
                             F.col("EXTRACT_CD") == extract_cd).count()})
        _results.append({"table": T_COST, "extract_cd": extract_cd,
                         "mode": "STAGE_VALIDATE_REPLACE",
                         "rows": spark.table(T_COST).where(
                             F.col("EXTRACT_CD") == extract_cd).count()})
else:
    _results.append({"table": T_ACT, "mode": "SKIPPED"})
    _results.append({"table": T_COST, "mode": "SKIPPED"})

if RUN_PLICS and table_exists(T_ACT) and table_exists(T_COST):
    assert_no_direct_identifiers(T_ACT)
    assert_no_direct_identifiers(T_COST)
    extracts = ", ".join(SRC_PLICS)
    apply_table_comment(T_ACT, (
        "PLICS patient-level costed activity across canonical R1H extracts: "
        f"{extracts}. Key (EXTRACT_CD, ACTIVITY_RECORD_ID); IDs are reused across "
        "extracts and must never be joined alone. Each changed extract is built into "
        "an isolated s6_plics_stage table, fully validated, then atomically replaced "
        "with INSERT INTO REPLACE WHERE. Person linkage remains NHS alias then "
        "CDS-to-MRN fallback. Community columns are selected only when the source "
        "schema contains the complete community field set."
    ))
    apply_table_comment(T_COST, (
        "PLICS cost lines across canonical R1H extracts; join on "
        "(EXTRACT_CD, ACTIVITY_RECORD_ID). Exact duplicates are collapsed with "
        "SOURCE_DUPLICATE_COUNT and LINE_HASH. Each changed extract is staged and "
        "validated before atomic REPLACE WHERE; state advances only after all run "
        "validation succeeds."
    ))
    apply_column_comments(T_ACT, {
        "EXTRACT_CD": "Source costing extract discriminator; required in every activity join.",
        "PLEMI": "PLICS matching identifier reused across extracts; never join without EXTRACT_CD.",
        "NHS_NUMBER_STATUS_CD": "Non-identifying source quality/status code used during linkage.",
        "ADC_UPDT": "Per-extract stage/publish timestamp.",
        "SOURCE_PRESENT_IND": "True when present in the current extract; false rows are retained source tombstones.",
    })
    apply_column_comments(T_COST, {
        "EXTRACT_CD": "Source costing extract discriminator; required in every cost-line join.",
        "SOURCE_DUPLICATE_COUNT": "Exact source rows represented by this collapsed line.",
        "ADC_UPDT": "Per-extract stage/publish timestamp.",
    })
    if APPLY_PLICS_IG_TAGS:
        _apply_plics_tags()

# Benchmark evidence contract:
# - benchmark_mode=true + benchmark_extract=<extract> performs three complete stage builds,
#   validates each, and asserts stable fingerprints excluding ADC_UPDT.
# - benchmark_interrupt_after_stage=<extract> deliberately stops after validated staging;
#   the retry must reuse/rebuild the stage, leave target slices unchanged, and commit no state.
# - after execution, export the Spark SQL final adaptive plans (isFinalPlan=true) for both CTAS
#   statements and both REPLACE WHERE statements. Record scan bytes/files, output rows, shuffle
#   read/write, spill, skew max-vs-median, task count and AQE coalescing. Do not infer an
#   optimization from source code or the initial plan.
# - run two subsequent unchanged notebook executions; both must report UNCHANGED_SKIP and no
#   target Delta version change. The in-run due-check proof below is supplemental, not a substitute.

# COMMAND ----------

def _check(name, sql, ok_fn):
    row = spark.sql(sql).collect()[0].asDict()
    ok = ok_fn(row)
    print(f"[VALIDATE] {'PASS' if ok else 'FAIL'} {name}: {row}")
    return {"check": name, "ok": ok, **row}

_validation = []
_validation.extend(_plics_stage_validation)
if RUN_SLAM_HRG:
    _validation.append(_check("apc_keys_unique",
        f"SELECT COUNT(*) c, COUNT(DISTINCT CDS_APC_ID) k FROM {T_APC}",
        lambda r: r["c"] == r["k"] and r["c"] > 0))
    _validation.append(_check("op_keys_unique",
        f"SELECT COUNT(*) c, COUNT(DISTINCT CDS_OPA_ID) k FROM {T_OP}",
        lambda r: r["c"] == r["k"] and r["c"] > 0))
if RUN_FINANCE and table_exists(T_FIN):
    _validation.append(_check("fin_source_reconcile",
        f"""SELECT (SELECT SUM(SOURCE_DUPLICATE_COUNT) FROM {T_FIN}
                    WHERE SOURCE_PRESENT_IND) a,
                   (SELECT COUNT(*) FROM {SRC_FIN}) b""",
        lambda r: r["a"] == r["b"]))
    _validation.append(_check("fin_cost_conserved",
        f"""SELECT round((SELECT SUM(COST * SOURCE_DUPLICATE_COUNT) FROM {T_FIN}
                    WHERE SOURCE_PRESENT_IND), 2) a,
                   round((SELECT SUM(Cost) FROM {SRC_FIN}), 2) b""",
        lambda r: r["a"] == r["b"]))
if RUN_PLICS and table_exists(T_COST):
    _validation.append(_check("plics_no_orphans",
        f"""SELECT COUNT(*) c FROM (SELECT * FROM {T_COST} WHERE SOURCE_PRESENT_IND) c
            LEFT ANTI JOIN (SELECT * FROM {T_ACT} WHERE SOURCE_PRESENT_IND) e
              ON c.EXTRACT_CD = e.EXTRACT_CD
             AND c.ACTIVITY_RECORD_ID = e.ACTIVITY_RECORD_ID""",
        lambda r: r["c"] == 0))

_all_ok = all(v["ok"] for v in _validation)
assert _all_ok, f"Validation failed: {[v['check'] for v in _validation if not v['ok']]}"

# state commit — only reached when validation passed
for pipeline_name, versions in _pending_s6b_hrg_versions:
    record_versions(S6B_SOURCE_VERSIONS, pipeline_name, versions)
if RUN_FINANCE and table_exists(T_FIN):
    set_state_version(CONTROL_SCHEMA, T_FIN, SRC_FIN, source_version(SRC_FIN))
if RUN_PLICS:
    for target_table, source_table, version in _pending_plics_state_commits:
        set_state_version(CONTROL_SCHEMA, target_table, source_table, version)
    # Supplemental NO_OP proof: the same source versions must be due=False twice after commit.
    for extract_cd in _processed_plics_extracts:
        ep_table, cost_table = SRC_PLICS[extract_cd]
        versions = _extract_versions(ep_table, cost_table)
        due_1 = _extract_due(extract_cd, ep_table, cost_table, versions, ignore_force=True)
        due_2 = _extract_due(extract_cd, ep_table, cost_table, versions, ignore_force=True)
        assert not due_1 and not due_2, f"{extract_cd}: post-commit NO_OP proof failed"
        _plics_evidence.append({
            "extract_cd": extract_cd,
            "no_op_due_check_1": due_1,
            "no_op_due_check_2": due_2,
            "note": "Two full unchanged notebook reruns are still required for promotion evidence.",
        })
S6B_SLAM_RESULTS = None
if RUN_S6B_FULL_GATES:
    S6B_SLAM_RESULTS = run_s6b_slam_gates(prepare=True)
    _s6b_hrg_lifecycle_fixture()
    _s6b_plics_lifecycle_fixture()

for r in _results:
    write_audit(CONTROL_SCHEMA, RUN_ID, r["table"], r.get("mode", "?"),
                r.get("rows", 0), _validation)

_summary = {
    "status": "SUCCESS", "result": "BUILT", "target": TARGET_SCHEMA, "pipeline": "slam_finance_pipeline",
    "run_id": RUN_ID, "target_schema": TARGET_SCHEMA,
    "started_at": _started_at, "completed_at": sf_utc_now(),
    "force_full_refresh": FORCE_FULL,
    "results": _results, "validation": _validation,
    "plics_evidence": _plics_evidence,
    "s6b_slam_results": S6B_SLAM_RESULTS,
}
print(json.dumps(_summary, indent=2, default=str))
dbutils.notebook.exit(json.dumps(_summary, default=str))

# COMMAND ----------

# BENCHMARK / PROMOTION EVIDENCE — human-gated, no production action in this notebook.
PROMOTION_EVIDENCE_REQUIRED = {
    "rebase": {
        "live_pipeline": "/Workspace/Shared/ADC-DB/Prod/Pipelines/Bronze/slam_finance_pipeline",
        "live_common": "/Workspace/Shared/ADC-DB/Prod/Pipelines/Bronze/_slam_finance_common",
        "requirement": "Rebase immediately before swap and re-run source-section equivalence checks.",
    },
    "benchmark": [
        "Pin all PLICS source Delta versions.",
        "Run benchmark_mode=true for a representative large extract; record warm median x3.",
        "Export final AQE plans (isFinalPlan=true) and scan/shuffle/spill/skew/task metrics.",
        "Run benchmark_interrupt_after_stage once; prove targets/state unchanged, then retry.",
        "Run two full unchanged executions; require UNCHANGED_SKIP and unchanged target versions.",
    ],
    "correctness": [
        "Per-extract activity and cost PK gates pass.",
        "No orphan cost lines; duplicate accounting and cost conservation pass.",
        "Observed date spans and cross-extract overlap are recorded, never boundary-deduped.",
        "Stage and published-slice fingerprints match.",
        "State rows commit only after all stage and global validations pass.",
    ],
    "promotion": [
        "Production execution requires allow_production_write=true and the release plan hash.",
        "Human swaps the reviewed drop-in over the live pipeline using the S4 drop-in pattern.",
        "First prod run retries=0 and only after landing/generator/deactivation gates.",
        "Apply column tags to every promoted column and run ig_tag_gate.",
        "Record weekly runtime/cost delta; PE2526 remains weekly and frozen extracts inactive.",
        "A14 promotes in the same atomic swap as S6 B2; restore rowTracking/CDF/deletion vectors and notify Journey per blocker.",
    ],
}
print(json.dumps({"promotion_evidence_required": PROMOTION_EVIDENCE_REQUIRED}, sort_keys=True))



