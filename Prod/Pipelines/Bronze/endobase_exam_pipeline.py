# Databricks notebook source
# endobase_exam_pipeline — S6 B1 EndoBase exam parent and person linkage.
# Pure Python. No production writes. The only executable target is
# 8_dev.bronze.map_endobase_exam, and only after every landing/owner gate passes.

# COMMAND ----------

import json
import uuid
from pyspark.sql import Window
from pyspark.sql import functions as F

# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 774988404509904)
import json

# Prod-idiom target resolution (house pattern: jac_pipeline/endobase_pipeline).
def _widget_text(name, default):
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass
    try:
        v = dbutils.widgets.get(name)
    except Exception:
        v = default
    return (v or default).strip()

TARGET_SCHEMA = _widget_text("target_schema", "8_dev.bronze")
ALLOW_PROD_WRITE = _widget_text("allow_production_write", "false").lower() == "true"
assert TARGET_SCHEMA.startswith("8_dev.") or ALLOW_PROD_WRITE, (
    f"Refusing to write {TARGET_SCHEMA} without allow_production_write=true")

def _control_schema(target):
    return "6_mgmt.bronze" if target == "4_prod.bronze" else target

CONTROL_SCHEMA = _control_schema(TARGET_SCHEMA)

def _ensure_widget(name, default):
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)

for _name, _default in {
    "hl7_success_statuses": "2",
    "hl7_status_owner_approved": "true",
    "landing_owner_signoff": "true",
    "ig_artefact_recorded": "true",
    "expected_exam_source_count": "",
    "expected_hl7_source_count": "",
    "apply_exam_term_fill": "false",
}.items():
    _ensure_widget(_name, _default)

PIPELINE = "s6_endobase_exam_v1"
RUN_ID = str(uuid.uuid4())
SRC_EXAM = "4_prod.raw.endobase3_dbo_exam_tbl"
SRC_HL7_MERGE = "4_prod.raw.endobase3_dbo_hl7_merge_tbl"
SRC_PATIENT_RAW = "4_prod.raw.endobase3_dbo_patient_tbl"
SRC_PATIENT_MAP = "4_prod.bronze.map_endobase_patient"
SRC_ORDERS = "4_prod.raw.mill_orders"
SRC_EXAM_TYPE = "4_prod.raw.endobase3_dbo_examtype_tbl"
SRC_TERM = "4_prod.bronze.map_endobase_exam_term"
TARGET = f"{TARGET_SCHEMA}.map_endobase_exam"
TERM_FILL_TARGET = f"{TARGET_SCHEMA}.map_endobase_exam_term"
MAX_MERGE_DEPTH = 50

# Non-negotiable route:
# DGVS_EXAM_TERM.EXAM_P -> EXAM_TBL.PRIMARY_NO -> EXAM_TBL.PATIENT_P
# -> PATIENT_TBL.PRIMARY_NO -> map_endobase_patient.
# BANNED: DGVS_EXAM_TERM.EXAM_P -> PATIENT_TBL.PRIMARY_NO. The integer overlap is spurious.

# COMMAND ----------

# Exact registration/run runbook. These are DATA ONLY; this notebook never executes them.
# Definition of record: edit ~/build_cwt_repaired.py so its generated
# /Workspace/Shared/ADC-DF/databricks/IncrementalUpdateV2/Setup/CreateWatermarkTable
# INSERT list contains these rows. Never INSERT/UPDATE 6_mgmt.incr_updt_v2.watermark in place.
ENDOBASE_EXAM_SELECT = (
    "SELECT [PRIMARY_NO],[GUID],[PATIENT_P],[EXAMTYPE_P],[SCHEDULE_FLG],[EXAM_FLG],"
    "[REPORT_FLG],[CONSENT_FLG],[MISSING_FLG],[DISABLE_FLG],[EMERGENCY],"
    "[AMBULANT_STATIONARY_P],[PERFORMED_DATE],[EXAM_DATE],[START_TIME],"
    "[TRUE_START_TIME],[END_TIME],[TRUE_END_TIME],[PLANNED_START_TIME],"
    "[PLANNED_END_TIME],[CREATE_DATE],[DEPARTMENT_P],[EXAMSPACE_P],[TEAM_P],"
    "[EXAMINER_P],[ATTENDANT_P],[NURSE_P],[SIGNER_P],[EXAM_NO],[HL7_ORDER_ID],"
    "[DICOM_STUDY_UID],[IMAGE_COUNT],[INDEX_IMAGE_COUNT],[MOVIE_FLG] "
    "FROM [ENDOBASE3].[dbo].[EXAM_TBL]"
)
ENDOBASE_HL7_SELECT = (
    "SELECT [PRIMARY_NO],[DATETIME],[SURVIVING_PAT_ID],[ABSORBED_PAT_ID],[EVENT],"
    "[STATUS],[SURVIVING_ADMISSION_NO],[ABSORBED_ADMISSION_NO],[ABSORBED_PAT_P] "
    "FROM [ENDOBASE3].[dbo].[HL7_MERGE_TBL]"
)
REGISTRATION_ROWS = [
    {
        "src_server_name": "dwh", "openquery_server": "BH2VMENB01",
        "src_database": "ENDOBASE3", "src_schema": "dbo", "src_table": "EXAM_TBL",
        "dst_catalog": "4_prod", "dst_schema": "raw", "dst_table": "endobase3_dbo_exam_tbl",
        "copy_query": "SELECT * FROM OPENQUERY([BH2VMENB01], '" + ENDOBASE_EXAM_SELECT.replace("'", "''") + "')",
        "copy_trigger": "weekly", "copy_query_timeout": "00:30:00", "copy_priority": 0,
        "copy_partition_column": None, "watermark_column": None, "watermark_timestamp": None,
        "upsert_task": "wt_updt", "upsert_key_columns": None, "active_ind": 1,
        "comment_text": "S6 B1 selected-column EndoBase exam parent; no demographics/free-text/billing columns."
    },
    {
        "src_server_name": "dwh", "openquery_server": "BH2VMENB01",
        "src_database": "ENDOBASE3", "src_schema": "dbo", "src_table": "HL7_MERGE_TBL",
        "dst_catalog": "4_prod", "dst_schema": "raw", "dst_table": "endobase3_dbo_hl7_merge_tbl",
        "copy_query": "SELECT * FROM OPENQUERY([BH2VMENB01], '" + ENDOBASE_HL7_SELECT.replace("'", "''") + "')",
        "copy_trigger": "weekly", "copy_query_timeout": "00:30:00", "copy_priority": 0,
        "copy_partition_column": None, "watermark_column": None, "watermark_timestamp": None,
        "upsert_task": "wt_updt", "upsert_key_columns": None, "active_ind": 1,
        "comment_text": "S6 B1 selected-column EndoBase patient-merge events; STATUS semantics owner-gated."
    },
]
CREATE_WATERMARK_RUNBOOK = {
    "generator": "~/build_cwt_repaired.py",
    "workspace_notebook": "/Workspace/Shared/ADC-DF/databricks/IncrementalUpdateV2/Setup/CreateWatermarkTable",
    "target_catalog": "6_mgmt",
    "forbidden_target_catalog": "8_dev",
    "steps": [
        "Drain existing IncrUpdtV2 staging/backlog first; snapshot current destination maxima.",
        "Add both REGISTRATION_ROWS to the generator and regenerate the canonical notebook.",
        "Run Setup/CreateWatermarkTable once against 6_mgmt; it truncates/rebuilds the registry.",
        "Reconcile all pre-run watermarks against the saved values; investigate any rewind.",
        "Verify server/database/schema/table/query/timeout/active_ind/trigger for both minted rows.",
    ],
}
ADF_RUNBOOK = {
    "pipeline": "IncrUpdtV2Pipeline",
    "one_row_at_a_time": True,
    "parameters": {
        "trigger_name": "<minted watermark_id>",
        "dev_mode": "0",
        "run_rde": "0",
    },
    "steps": [
        "Promoter runs EXAM_TBL and HL7_MERGE_TBL watermark ids separately.",
        "dev_mode='0' is mandatory; manual runs default to '1'.",
        "Compare SQL Server COUNT_BIG to landed Delta count exactly for each table.",
        "Record Endoscopy owner sign-off and IG artefact before allowing this bronze build.",
    ],
}
print(json.dumps({"registration_rows": REGISTRATION_ROWS,
                  "create_watermark": CREATE_WATERMARK_RUNBOOK,
                  "adf": ADF_RUNBOOK}, sort_keys=True))

# COMMAND ----------

# Date-quality prerequisite used by the duplicated S6 block.

from pyspark.sql import functions as F

SENTINEL_FLOOR = "1901-01-01"

def dq_columns(df, date_cols):
    """Master plan §2.2 date-quality standard block.
    For each timestamp column C adds:
      C_FUTURE_IND   - value is after now()
      C_SENTINEL_IND - value is before 1901-01-01
      C_CLEAN        - value, or NULL when either flag is set
    Source column is retained untouched (bronze keeps source values; silver chooses).
    """
    out = df
    for c in date_cols:
        fut = F.col(c) > F.current_timestamp()
        sen = F.col(c) < F.lit(SENTINEL_FLOOR).cast("timestamp")
        out = (out
               .withColumn(f"{c}_FUTURE_IND", F.when(F.col(c).isNull(), F.lit(None)).otherwise(fut))
               .withColumn(f"{c}_SENTINEL_IND", F.when(F.col(c).isNull(), F.lit(None)).otherwise(sen))
               .withColumn(f"{c}_CLEAN", F.when(fut | sen, F.lit(None).cast("timestamp")).otherwise(F.col(c))))
    return out

# COMMAND ----------

# ==== S6 BLOCK v1 (SYNC-WITH _completeness_common) ====
from pyspark.sql import functions as F
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
S6_SOURCE_VERSIONS = f"{CONTROL_SCHEMA}.s6_source_versions"

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
    """tag_map is REQUIRED for every identifier/free-text column (direct identifiers = ('4','2')).
    Other columns: counterpart lookup, else default — and every defaulted column is PRINTED for
    the promoter to eyeball (never silently 0/0 an identifier)."""
    cols = [r.col_name for r in spark.sql(f"DESCRIBE {table}").collect()
            if r.col_name and not r.col_name.startswith('#')]
    for c in cols:
        if c in tag_map:
            risk, sev = tag_map[c]
        else:
            found = lookup_counterpart_tags(c)
            risk, sev = found if found else default
            if not found:
                print(f"IG-TAG DEFAULTED {table}.{c} -> {default} — REVIEW")
        assert risk is not None and sev is not None, (
            f"Incomplete counterpart tags for {table}.{c}: ig_risk={risk}, ig_severity={sev}")
        col_ident = c.replace("`", "``")
        risk_lit = str(risk).replace("'", "''")
        sev_lit = str(sev).replace("'", "''")
        spark.sql(
            f"ALTER TABLE {table} ALTER COLUMN `{col_ident}` "
            f"SET TAGS ('ig_risk'='{risk_lit}','ig_severity'='{sev_lit}')")

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

# Pre-build gate definitions. All gates are defined before any build/write code.
def _bool_widget(name):
    return str(dbutils.widgets.get(name)).strip().lower() in {"1", "true", "yes", "y"}

def _expected_count(name):
    raw = str(dbutils.widgets.get(name)).strip()
    return int(raw) if raw else None

def _case_columns(table):
    return {c.upper(): c for c in spark.table(table).columns}

def _require_columns(table, required):
    actual = _case_columns(table)
    missing = [c for c in required if c.upper() not in actual]
    assert not missing, f"{table}: required landing columns missing: {missing}"
    return actual

def _source_count_gate(table, minimum, expected=None):
    n = spark.table(table).count()
    assert n >= minimum, f"{table}: row count {n} below floor {minimum}"
    if expected is not None:
        assert n == expected, f"{table}: landed count {n} != source COUNT_BIG {expected}"
    return n

def _unique_non_null_gate(table, column):
    c = _case_columns(table)[column.upper()]
    row = spark.table(table).agg(
        F.count("*").alias("n"),
        F.count(F.col(c)).alias("filled"),
        F.countDistinct(F.col(c)).alias("distinct_n"),
    ).collect()[0]
    assert row["n"] == row["filled"] == row["distinct_n"], (
        f"{table}.{c}: key gate failed n={row['n']} filled={row['filled']} distinct={row['distinct_n']}")
    return int(row["n"])

def _snapshot_gate(table):
    cols = _case_columns(table)
    assert "ADC_UPDT" in cols, f"{table}: landing must carry ADC_UPDT"
    n_versions = spark.table(table).select(cols["ADC_UPDT"]).distinct().count()
    assert n_versions == 1, f"{table}: wt_updt snapshot has {n_versions} ADC_UPDT values"
    return str(spark.table(table).agg(F.max(cols["ADC_UPDT"]).alias("m")).collect()[0]["m"])

def _target_due_check(target, sources):
    current = {t: int(table_version(t)) for t in sources}
    if not spark.catalog.tableExists(target):
        return True, current
    detail = spark.sql(f"DESCRIBE DETAIL {target}").collect()[0]
    props = detail["properties"] or {}
    previous = json.loads(props.get("s6.source_versions_json", "{}"))
    previous = {k: int(v) for k, v in previous.items()}
    return previous != current, current

def _record_target_versions(target, versions):
    payload = json.dumps(versions, sort_keys=True, separators=(",", ":")).replace("'", "''")
    spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ('s6.source_versions_json'='{payload}')")

def _assert_dq_triplets(table, temporal_columns):
    cols = set(spark.table(table).columns)
    missing = []
    for c in temporal_columns:
        for suffix in ("_FUTURE_IND", "_SENTINEL_IND", "_CLEAN"):
            if c + suffix not in cols:
                missing.append(c + suffix)
    assert not missing, f"{table}: missing DQ columns {missing}"

def _target_gates(source_rows, temporal_columns):
    assert spark.catalog.tableExists(TARGET), f"{TARGET}: target missing"
    active = spark.table(TARGET).where(F.col("SOURCE_PRESENT_IND"))
    row = active.agg(
        F.count("*").alias("n"),
        F.count("ENDOBASE_EXAM_ID").alias("filled"),
        F.countDistinct("ENDOBASE_EXAM_ID").alias("distinct_n"),
    ).collect()[0]
    assert row["n"] == source_rows == row["filled"] == row["distinct_n"], (
        f"target grain failed source={source_rows}, n={row['n']}, "
        f"filled={row['filled']}, distinct={row['distinct_n']}")
    bad_status = active.where(F.col("PERSON_LINK_STATUS").isNull()).count()
    assert bad_status == 0, f"{bad_status} active exams have NULL PERSON_LINK_STATUS"
    _assert_dq_triplets(TARGET, temporal_columns)
    ig_tag_gate(TARGET)

# COMMAND ----------

# Explicit early landing block. No schema/control/target object is created before this exit.
_required_raw = [SRC_EXAM, SRC_HL7_MERGE]
_missing_raw = [t for t in _required_raw if not spark.catalog.tableExists(t)]
if _missing_raw:
    dbutils.notebook.exit(json.dumps({
        "pipeline": PIPELINE,
        "run_id": RUN_ID,
        "result": "BLOCKED_ON_LANDING",
        "missing_sources": _missing_raw,
        "required_action": {
            "generator": CREATE_WATERMARK_RUNBOOK,
            "adf": ADF_RUNBOOK,
        },
        "writes": [],
    }, sort_keys=True))

# COMMAND ----------

# Landing schema/count gates. The required path-supporting sources are read-only dependencies.
EXAM_REQUIRED = [
    "PRIMARY_NO", "PATIENT_P", "EXAMTYPE_P", "HL7_ORDER_ID",
    "PERFORMED_DATE", "EXAM_DATE", "START_TIME", "END_TIME", "CREATE_DATE", "ADC_UPDT",
]
HL7_REQUIRED = [
    "PRIMARY_NO", "DATETIME", "SURVIVING_PAT_ID", "ABSORBED_PAT_ID",
    "EVENT", "STATUS", "SURVIVING_ADMISSION_NO", "ABSORBED_ADMISSION_NO",
    "ABSORBED_PAT_P", "ADC_UPDT",
]
PATIENT_RAW_REQUIRED = ["PRIMARY_NO", "PATIENT_ID"]
PATIENT_MAP_REQUIRED = ["ENDOBASE_PATIENT_ID", "PERSON_ID", "PERSON_LINK_STATUS"]
ORDER_REQUIRED = ["ORDER_ID", "PERSON_ID", "ENCNTR_ID"]

_exam_cols = _require_columns(SRC_EXAM, EXAM_REQUIRED)
_hl7_cols = _require_columns(SRC_HL7_MERGE, HL7_REQUIRED)
_require_columns(SRC_PATIENT_RAW, PATIENT_RAW_REQUIRED)
_require_columns(SRC_PATIENT_MAP, PATIENT_MAP_REQUIRED)
_require_columns(SRC_ORDERS, ORDER_REQUIRED)

_exam_rows = _source_count_gate(
    SRC_EXAM, 300000, _expected_count("expected_exam_source_count"))
_hl7_rows = _source_count_gate(
    SRC_HL7_MERGE, 1, _expected_count("expected_hl7_source_count"))
_unique_non_null_gate(SRC_EXAM, "PRIMARY_NO")
_unique_non_null_gate(SRC_HL7_MERGE, "PRIMARY_NO")
_exam_snapshot = _snapshot_gate(SRC_EXAM)
_hl7_snapshot = _snapshot_gate(SRC_HL7_MERGE)

if not _bool_widget("landing_owner_signoff") or not _bool_widget("ig_artefact_recorded"):
    dbutils.notebook.exit(json.dumps({
        "pipeline": PIPELINE, "run_id": RUN_ID,
        "result": "BLOCKED_ON_LANDING_SIGNOFF",
        "exam_rows": _exam_rows, "hl7_rows": _hl7_rows,
        "landing_owner_signoff": _bool_widget("landing_owner_signoff"),
        "ig_artefact_recorded": _bool_widget("ig_artefact_recorded"),
        "writes": [],
    }, sort_keys=True))

_success_statuses = [
    x.strip().upper() for x in str(dbutils.widgets.get("hl7_success_statuses")).split(",")
    if x.strip()
]
_status_counts = {
    str(r["STATUS"]): int(r["n"]) for r in
    spark.table(SRC_HL7_MERGE).groupBy("STATUS").count().withColumnRenamed("count", "n").collect()
}
if not _bool_widget("hl7_status_owner_approved") or not _success_statuses:
    dbutils.notebook.exit(json.dumps({
        "pipeline": PIPELINE, "run_id": RUN_ID,
        "result": "BLOCKED_ON_HL7_STATUS_SEMANTICS",
        "observed_status_counts": _status_counts,
        "required": "Owner-approved comma-separated success statuses plus hl7_status_owner_approved=true",
        "writes": [],
    }, sort_keys=True))

# COMMAND ----------

# Merge-resolution diagnostics published in the exit payload (counts only, no identifiers).
_MERGE_DIAG = {}

def _normalised_id(column):
    value = F.upper(F.trim(column.cast("string")))
    return F.when((value != "") & value.isNotNull(), value)

def _build_merge_resolution(success_statuses):
    raw = (
        spark.table(SRC_HL7_MERGE)
        .withColumn("_STATUS_NORM", F.upper(F.trim(F.col(_hl7_cols["STATUS"]))))
        .where(F.col("_STATUS_NORM").isin(success_statuses))
        .withColumn("_ABSORBED_ID", _normalised_id(F.col(_hl7_cols["ABSORBED_PAT_ID"])))
        .withColumn("_SURVIVING_ID", _normalised_id(F.col(_hl7_cols["SURVIVING_PAT_ID"])))
        .withColumn("_EVENT_TS", F.expr(f"try_cast({ _hl7_cols['DATETIME'] } AS TIMESTAMP)"))
        .where(F.col("_ABSORBED_ID").isNotNull() & F.col("_SURVIVING_ID").isNotNull())
    )
    latest_w = Window.partitionBy("_ABSORBED_ID").orderBy(
        F.col("_EVENT_TS").desc_nulls_last(),
        F.col(_hl7_cols["PRIMARY_NO"]).desc_nulls_last(),
    )
    edges = (
        raw.withColumn("_RN", F.row_number().over(latest_w)).where(F.col("_RN") == 1)
        .select("_ABSORBED_ID", "_SURVIVING_ID", "_STATUS_NORM", "_EVENT_TS")
    )
    self_edges = edges.where(F.col("_ABSORBED_ID") == F.col("_SURVIVING_ID")).count()
    assert self_edges == 0, f"HL7 merge graph contains {self_edges} self-cycles"

    # Mutual-pair cycle break by event recency: a later merge event supersedes the
    # earlier edge it reverses (reverse-merge corrections exist at source — observed
    # 2022-10-12 pair absorbed<->surviving six minutes apart). Only the strictly-older
    # edge of a mutual pair is dropped. Pairs with equal or NULL timestamps cannot be
    # ordered and stay cyclic; they surface as MERGE_CYCLE_UNRESOLVED below instead of
    # a guessed resolution or a build crash. The self-join uses disjoint renamed
    # columns and a collected isin filter to avoid same-lineage column ambiguity.
    _fwd = edges.select(
        F.col("_ABSORBED_ID").alias("_F_A"),
        F.col("_SURVIVING_ID").alias("_F_S"),
        F.col("_EVENT_TS").alias("_F_TS"))
    _rev = edges.select(
        F.col("_ABSORBED_ID").alias("_R_A"),
        F.col("_SURVIVING_ID").alias("_R_S"),
        F.col("_EVENT_TS").alias("_R_TS"))
    _stale_keys = [
        r["_F_A"] for r in
        _fwd.join(_rev, (F.col("_F_A") == F.col("_R_S")) & (F.col("_F_S") == F.col("_R_A")))
            .where(F.col("_F_TS") < F.col("_R_TS"))
            .select("_F_A").distinct().collect()
    ]
    _MERGE_DIAG["stale_reverse_merge_edges_dropped"] = len(_stale_keys)
    if _stale_keys:
        edges = edges.where(~F.col("_ABSORBED_ID").isin(_stale_keys))

    closure = edges.select(
        F.col("_ABSORBED_ID").alias("MERGE_ORIGIN_PATIENT_ID"),
        F.col("_SURVIVING_ID").alias("MERGE_RESOLVED_PATIENT_ID"),
        F.array("_ABSORBED_ID", "_SURVIVING_ID").alias("_PATH"),
        F.lit(1).alias("MERGE_DEPTH"),
        F.lit(False).alias("MERGE_CYCLE_IND"),
    )
    for _depth in range(MAX_MERGE_DEPTH):
        c = closure.alias("c")
        e = edges.alias("e")
        joined = c.join(
            e, F.col("c.MERGE_RESOLVED_PATIENT_ID") == F.col("e._ABSORBED_ID"), "left")
        can_advance = (~F.col("c.MERGE_CYCLE_IND")) & F.col("e._ABSORBED_ID").isNotNull()
        cycle_now = can_advance & F.array_contains(
            F.col("c._PATH"), F.col("e._SURVIVING_ID"))
        step = joined.select(
            F.col("c.MERGE_ORIGIN_PATIENT_ID"),
            F.when(can_advance, F.col("e._SURVIVING_ID"))
             .otherwise(F.col("c.MERGE_RESOLVED_PATIENT_ID"))
             .alias("MERGE_RESOLVED_PATIENT_ID"),
            F.when(can_advance, F.concat(F.col("c._PATH"), F.array(F.col("e._SURVIVING_ID"))))
             .otherwise(F.col("c._PATH")).alias("_PATH"),
            F.when(can_advance, F.col("c.MERGE_DEPTH") + 1)
             .otherwise(F.col("c.MERGE_DEPTH")).alias("MERGE_DEPTH"),
            (F.col("c.MERGE_CYCLE_IND") | cycle_now).alias("MERGE_CYCLE_IND"),
            (can_advance & ~cycle_now).alias("_ADVANCED"),
        )
        closure = step.drop("_ADVANCED")
        if step.where(F.col("_ADVANCED")).limit(1).count() == 0:
            break

    # Residual cycles (unorderable mutual pairs and chains that feed into them) are
    # NOT a crash: their resolution is withheld and published as MERGE_CYCLE_UNRESOLVED.
    # Statuses describe; they never gate.
    cycle_count = closure.where(F.col("MERGE_CYCLE_IND")).count()
    _MERGE_DIAG["cycle_flagged_chains"] = cycle_count
    closure = closure.withColumn(
        "MERGE_RESOLVED_PATIENT_ID",
        F.when(F.col("MERGE_CYCLE_IND"), F.lit(None).cast("string"))
         .otherwise(F.col("MERGE_RESOLVED_PATIENT_ID")))
    unresolved_depth = (
        closure.where(~F.col("MERGE_CYCLE_IND")).alias("c")
        .join(edges.alias("e"),
              F.col("c.MERGE_RESOLVED_PATIENT_ID") == F.col("e._ABSORBED_ID"),
              "left_semi").count()
    )
    assert unresolved_depth == 0, (
        f"{unresolved_depth} HL7 merge chains exceed MAX_MERGE_DEPTH={MAX_MERGE_DEPTH}")
    return closure.drop("_PATH")

def _optional_col(df, name, dtype, alias=None):
    actual = {c.upper(): c for c in df.columns}.get(name.upper())
    return (F.col(actual).cast(dtype) if actual else F.lit(None).cast(dtype)).alias(alias or name)

def _try_ts(column_name):
    return F.expr(f"try_cast({column_name} AS TIMESTAMP)")

def _row_hash(df):
    cols = sorted(c for c in df.columns if c not in {"PIPELINE_UPDT_DT_TM", "ROW_HASH"})
    return df.withColumn(
        "ROW_HASH", F.sha2(F.to_json(F.struct(*[F.col(c) for c in cols])), 256))

def build_endobase_exam():
    exam = spark.table(SRC_EXAM)
    selected = exam.select(
        _optional_col(exam, "PRIMARY_NO", "long", "ENDOBASE_EXAM_ID"),
        _optional_col(exam, "GUID", "string", "ENDOBASE_EXAM_GUID"),
        _optional_col(exam, "PATIENT_P", "long", "ENDOBASE_PATIENT_ID_SOURCE"),
        _optional_col(exam, "EXAMTYPE_P", "long", "ENDOBASE_EXAM_TYPE_ID"),
        _optional_col(exam, "HL7_ORDER_ID", "string", "HL7_ORDER_ID_RAW"),
        _optional_col(exam, "EXAM_NO", "string", "ENDOBASE_EXAM_NO"),
        _optional_col(exam, "DICOM_STUDY_UID", "string", "DICOM_STUDY_UID"),
        _optional_col(exam, "PERFORMED_DATE", "string", "PERFORMED_DATE_RAW"),
        _optional_col(exam, "EXAM_DATE", "string", "EXAM_DATE_RAW"),
        _optional_col(exam, "START_TIME", "string", "START_TIME_RAW"),
        _optional_col(exam, "TRUE_START_TIME", "string", "TRUE_START_TIME_RAW"),
        _optional_col(exam, "END_TIME", "string", "END_TIME_RAW"),
        _optional_col(exam, "TRUE_END_TIME", "string", "TRUE_END_TIME_RAW"),
        _optional_col(exam, "PLANNED_START_TIME", "string", "PLANNED_START_TIME_RAW"),
        _optional_col(exam, "PLANNED_END_TIME", "string", "PLANNED_END_TIME_RAW"),
        _optional_col(exam, "CREATE_DATE", "string", "SOURCE_CREATE_RAW"),
        _optional_col(exam, "SCHEDULE_FLG", "int"),
        _optional_col(exam, "EXAM_FLG", "int"),
        _optional_col(exam, "REPORT_FLG", "int"),
        _optional_col(exam, "CONSENT_FLG", "int"),
        _optional_col(exam, "MISSING_FLG", "int"),
        _optional_col(exam, "DISABLE_FLG", "int"),
        _optional_col(exam, "EMERGENCY", "int", "EMERGENCY_FLG"),
        _optional_col(exam, "AMBULANT_STATIONARY_P", "long", "AMBULANT_STATIONARY_ID"),
        _optional_col(exam, "DEPARTMENT_P", "long", "DEPARTMENT_ID"),
        _optional_col(exam, "EXAMSPACE_P", "long", "EXAM_SPACE_ID"),
        _optional_col(exam, "TEAM_P", "long", "TEAM_ID"),
        _optional_col(exam, "EXAMINER_P", "long", "EXAMINER_ID"),
        _optional_col(exam, "ATTENDANT_P", "long", "ATTENDANT_ID"),
        _optional_col(exam, "NURSE_P", "long", "NURSE_ID"),
        _optional_col(exam, "SIGNER_P", "long", "SIGNER_ID"),
        _optional_col(exam, "IMAGE_COUNT", "long"),
        _optional_col(exam, "INDEX_IMAGE_COUNT", "long"),
        _optional_col(exam, "MOVIE_FLG", "int"),
        _optional_col(exam, "ADC_UPDT", "timestamp", "ADC_UPDT"),
    )
    parsed = (
        selected
        .withColumn("PERFORMED_TS", _try_ts("PERFORMED_DATE_RAW"))
        .withColumn("EXAM_TS", _try_ts("EXAM_DATE_RAW"))
        .withColumn("START_TS", _try_ts("START_TIME_RAW"))
        .withColumn("TRUE_START_TS", _try_ts("TRUE_START_TIME_RAW"))
        .withColumn("END_TS", _try_ts("END_TIME_RAW"))
        .withColumn("TRUE_END_TS", _try_ts("TRUE_END_TIME_RAW"))
        .withColumn("PLANNED_START_TS", _try_ts("PLANNED_START_TIME_RAW"))
        .withColumn("PLANNED_END_TS", _try_ts("PLANNED_END_TIME_RAW"))
        .withColumn("SOURCE_CREATE_TS", _try_ts("SOURCE_CREATE_RAW"))
    )

    merge_resolution = _build_merge_resolution(_success_statuses)
    patient_raw = (
        spark.table(SRC_PATIENT_RAW)
        .select(
            F.col("PRIMARY_NO").cast("long").alias("_PATIENT_PRIMARY_NO"),
            _normalised_id(F.col("PATIENT_ID")).alias("_PATIENT_SOURCE_ID"))
    )
    patient_id_map = (
        patient_raw.where(F.col("_PATIENT_SOURCE_ID").isNotNull())
        .groupBy("_PATIENT_SOURCE_ID")
        .agg(
            F.countDistinct("_PATIENT_PRIMARY_NO").alias("_PATIENT_RECORD_COUNT"),
            F.max("_PATIENT_PRIMARY_NO").alias("_ONLY_PATIENT_PRIMARY_NO"))
        .withColumnRenamed("_PATIENT_SOURCE_ID", "_MAP_SOURCE_ID")
    )
    patient_link = (
        parsed.join(
            patient_raw,
            parsed["ENDOBASE_PATIENT_ID_SOURCE"] == patient_raw["_PATIENT_PRIMARY_NO"],
            "left")
        .join(
            merge_resolution,
            patient_raw["_PATIENT_SOURCE_ID"] == merge_resolution["MERGE_ORIGIN_PATIENT_ID"],
            "left")
        .withColumn(
            "_RESOLVED_SOURCE_ID",
            F.coalesce("MERGE_RESOLVED_PATIENT_ID", "_PATIENT_SOURCE_ID"))
        .join(
            patient_id_map,
            F.col("_RESOLVED_SOURCE_ID") == patient_id_map["_MAP_SOURCE_ID"],
            "left")
        .withColumn(
            "ENDOBASE_PATIENT_ID_RESOLVED",
            F.when(F.col("MERGE_RESOLVED_PATIENT_ID").isNull(),
                   F.col("ENDOBASE_PATIENT_ID_SOURCE"))
             .when(F.col("_PATIENT_RECORD_COUNT") == 1, F.col("_ONLY_PATIENT_PRIMARY_NO")))
        .withColumn(
            "PATIENT_MERGE_STATUS",
            F.when(F.col("_PATIENT_SOURCE_ID").isNull(), "PATIENT_RECORD_NOT_FOUND")
             .when(F.coalesce(F.col("MERGE_CYCLE_IND"), F.lit(False)), "MERGE_CYCLE_UNRESOLVED")
             .when(F.col("MERGE_RESOLVED_PATIENT_ID").isNull(), "NO_MERGE")
             .when(F.col("_PATIENT_RECORD_COUNT") == 1, "MERGED_TO_SURVIVOR")
             .when(F.col("_PATIENT_RECORD_COUNT") > 1, "MERGE_TARGET_AMBIGUOUS")
             .otherwise("MERGE_TARGET_NOT_FOUND"))
        .withColumn("PATIENT_MERGE_DEPTH", F.coalesce("MERGE_DEPTH", F.lit(0)))
    )

    crosswalk = spark.table(SRC_PATIENT_MAP).select(
        F.col("ENDOBASE_PATIENT_ID").cast("long").alias("_CW_PATIENT_ID"),
        F.col("PERSON_ID").cast("long").alias("_PATIENT_PERSON_ID"),
        F.col("PERSON_LINK_STATUS").alias("PATIENT_CROSSWALK_STATUS"),
    )
    via_patient = patient_link.join(
        crosswalk,
        patient_link["ENDOBASE_PATIENT_ID_RESOLVED"] == crosswalk["_CW_PATIENT_ID"],
        "left")

    orders = (
        spark.table(SRC_ORDERS)
        .select(
            F.col("ORDER_ID").cast("decimal(38,0)").cast("long").alias("_ORDER_ID"),
            F.col("PERSON_ID").cast("long").alias("_ORDER_PERSON_ID"),
            F.col("ENCNTR_ID").cast("long").alias("_ORDER_ENCNTR_ID"))
        .where(F.col("_ORDER_ID").isNotNull())
        .groupBy("_ORDER_ID")
        .agg(
            F.countDistinct("_ORDER_PERSON_ID").alias("ORDER_PERSON_CANDIDATES"),
            F.countDistinct("_ORDER_ENCNTR_ID").alias("ORDER_ENCNTR_CANDIDATES"),
            F.max("_ORDER_PERSON_ID").alias("_ORDER_PERSON_ONLY"),
            F.max("_ORDER_ENCNTR_ID").alias("_ORDER_ENCNTR_ONLY"))
        .withColumn(
            "_ORDER_PERSON_ID",
            F.when(F.col("ORDER_PERSON_CANDIDATES") == 1, F.col("_ORDER_PERSON_ONLY")))
        .withColumn(
            "_ORDER_ENCNTR_ID",
            F.when(F.col("ORDER_ENCNTR_CANDIDATES") == 1, F.col("_ORDER_ENCNTR_ONLY")))
    )
    with_order = (
        via_patient
        .withColumn(
            "_HL7_ORDER_ID",
            F.when(
                F.trim(F.col("HL7_ORDER_ID_RAW")).rlike(r"^[0-9]+(?:[.]0+)?$"),
                F.trim(F.col("HL7_ORDER_ID_RAW")).cast("decimal(38,0)").cast("long")))
        .join(orders, F.col("_HL7_ORDER_ID") == orders["_ORDER_ID"], "left")
    )
    resolved = (
        with_order
        .withColumn(
            "PERSON_LINK_STATUS",
            F.when(
                F.col("_PATIENT_PERSON_ID").isNotNull() & F.col("_ORDER_PERSON_ID").isNotNull()
                & (F.col("_PATIENT_PERSON_ID") == F.col("_ORDER_PERSON_ID")),
                "CONSENSUS_PATIENT_AND_ORDER")
             .when(
                F.col("_PATIENT_PERSON_ID").isNotNull() & F.col("_ORDER_PERSON_ID").isNotNull(),
                "CONFLICT_PATIENT_VS_ORDER")
             .when(F.col("_PATIENT_PERSON_ID").isNotNull(),
                   F.concat(F.lit("PATIENT_"), F.coalesce("PATIENT_CROSSWALK_STATUS", F.lit("LINKED"))))
             .when(F.col("_ORDER_PERSON_ID").isNotNull(), "ORDER_ONLY")
             .when(F.col("ORDER_PERSON_CANDIDATES") > 1, "ORDER_AMBIGUOUS")
             .when(F.col("PATIENT_MERGE_STATUS") == "MERGE_TARGET_AMBIGUOUS", "MERGE_TARGET_AMBIGUOUS")
             .otherwise("UNMATCHED"))
        .withColumn(
            "PERSON_ID",
            F.when(F.col("PERSON_LINK_STATUS") == "CONSENSUS_PATIENT_AND_ORDER",
                   F.col("_PATIENT_PERSON_ID"))
             .when(F.col("PERSON_LINK_STATUS").startswith("PATIENT_"), F.col("_PATIENT_PERSON_ID"))
             .when(F.col("PERSON_LINK_STATUS") == "ORDER_ONLY", F.col("_ORDER_PERSON_ID")))
        .withColumn(
            "MILL_ORDER_ID",
            F.when(F.col("ORDER_PERSON_CANDIDATES") == 1, F.col("_ORDER_ID")))
        .withColumn(
            "MILL_ENCNTR_ID",
            F.when(F.col("ORDER_ENCNTR_CANDIDATES") == 1, F.col("_ORDER_ENCNTR_ID")))
    )

    if spark.catalog.tableExists(SRC_EXAM_TYPE):
        et = spark.table(SRC_EXAM_TYPE)
        et_cols = {c.upper(): c for c in et.columns}
        if "PRIMARY_NO" in et_cols and ("EXAM_TYPE" in et_cols or "NAME" in et_cols):
            exam_type_name_col = et_cols["EXAM_TYPE"] if "EXAM_TYPE" in et_cols else et_cols["NAME"]
            exam_type = et.select(
                F.col(et_cols["PRIMARY_NO"]).cast("long").alias("_EXAM_TYPE_ID"),
                F.col(exam_type_name_col).cast("string").alias("EXAM_TYPE_DESC"))
            resolved = (
                resolved.join(
                    exam_type,
                    resolved["ENDOBASE_EXAM_TYPE_ID"] == exam_type["_EXAM_TYPE_ID"],
                    "left")
                .withColumn(
                    "EXAM_TYPE_LINK_STATUS",
                    F.when(F.col("EXAM_TYPE_DESC").isNotNull(), "DECODED").otherwise("UNMATCHED")))
        else:
            resolved = (resolved.withColumn("EXAM_TYPE_DESC", F.lit(None).cast("string"))
                         .withColumn("EXAM_TYPE_LINK_STATUS", F.lit("LOOKUP_SCHEMA_INVALID")))
    else:
        resolved = (resolved.withColumn("EXAM_TYPE_DESC", F.lit(None).cast("string"))
                     .withColumn("EXAM_TYPE_LINK_STATUS", F.lit("LOOKUP_NOT_LANDED")))

    internal = [c for c in resolved.columns if c.startswith("_") or c in {
        "MERGE_ORIGIN_PATIENT_ID", "MERGE_RESOLVED_PATIENT_ID", "MERGE_CYCLE_IND",
    }]
    published = resolved.drop(*internal)
    published, temporal = dq_all_clinical(
        published,
        admin_stamps={"ADC_UPDT", "SOURCE_CREATE_TS"})
    published = _row_hash(
        published.withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp()))
    return published, temporal

# Correct-path read-only preview for the future existing-term coverage MERGE.
# It joins term.EXAM_P only to map_endobase_exam.ENDOBASE_EXAM_ID. It never joins
# term.EXAM_P to PATIENT_TBL.PRIMARY_NO.
def exam_term_person_fill_preview(term_table=SRC_TERM, exam_table=TARGET):
    assert spark.catalog.tableExists(exam_table), f"{exam_table} must exist first"
    assert spark.catalog.tableExists(term_table), f"{term_table} must exist first"
    return (
        spark.table(term_table).alias("t")
        .join(
            spark.table(exam_table).where(F.col("SOURCE_PRESENT_IND")).select(
                "ENDOBASE_EXAM_ID", "PERSON_ID", "PERSON_LINK_STATUS").alias("e"),
            F.col("t.ENDOBASE_EXAM_ID") == F.col("e.ENDOBASE_EXAM_ID"),
            "left")
        .select(
            F.col("t.ENDOBASE_EXAM_TERM_ID"),
            F.col("t.ENDOBASE_EXAM_ID"),
            F.col("e.PERSON_ID").alias("PROPOSED_PERSON_ID"),
            F.coalesce(F.col("e.PERSON_LINK_STATUS"), F.lit("EXAM_NOT_FOUND"))
             .alias("PROPOSED_PERSON_LINK_STATUS"))
    )

# COMMAND ----------


# Explicit, widget-gated existing-term fill. Runs before the source NO_OP exit so the
# release can invoke the hash-pinned staged notebook independently of the weekly builder.
if _bool_widget("apply_exam_term_fill"):
    _fill = exam_term_person_fill_preview(TERM_FILL_TARGET, TARGET)
    _fill_count = _fill.count()
    _fill_status = {
        str(r["PROPOSED_PERSON_LINK_STATUS"]): int(r["count"])
        for r in _fill.groupBy("PROPOSED_PERSON_LINK_STATUS").count().collect()
    }
    _expected_statuses = {
        "CONSENSUS_PATIENT_AND_ORDER", "PATIENT_CONSENSUS", "PATIENT_MRN_ONLY",
        "ORDER_ONLY", "PATIENT_NHS_ONLY", "CONFLICT_PATIENT_VS_ORDER",
        "EXAM_NOT_FOUND", "UNMATCHED",
    }
    assert _fill_count == 9_549_809, f"term fill accounting drift: {_fill_count}"
    assert set(_fill_status) == _expected_statuses, _fill_status
    _resolved = _fill.where(F.col("PROPOSED_PERSON_ID").isNotNull()).count()
    assert _resolved / _fill_count >= 0.998, (_resolved, _fill_count)
    _fill.createOrReplaceTempView("_endobase_exam_term_fill")
    spark.sql(f"""
        MERGE INTO {TERM_FILL_TARGET} t
        USING _endobase_exam_term_fill s
          ON t.ENDOBASE_EXAM_TERM_ID = s.ENDOBASE_EXAM_TERM_ID
        WHEN MATCHED AND (
          NOT (t.PERSON_ID <=> s.PROPOSED_PERSON_ID)
          OR NOT (t.PERSON_LINK_STATUS <=> s.PROPOSED_PERSON_LINK_STATUS)
        ) THEN UPDATE SET
          t.PERSON_ID = s.PROPOSED_PERSON_ID,
          t.PERSON_LINK_STATUS = s.PROPOSED_PERSON_LINK_STATUS
    """)
    _metrics = spark.sql(f"DESCRIBE HISTORY {TERM_FILL_TARGET} LIMIT 1").first()["operationMetrics"]
    dbutils.notebook.exit(json.dumps({
        "pipeline": PIPELINE, "run_id": RUN_ID, "result": "BUILT",
        "operation": "EXAM_TERM_FILL", "target": TERM_FILL_TARGET,
        "target_schema": TARGET_SCHEMA, "rows_accounted": _fill_count,
        "person_resolved": _resolved, "status_counts": _fill_status,
        "operation_metrics": _metrics,
    }, sort_keys=True, default=str))

# COMMAND ----------

# Version/no-op gate. State is stored only as a property on the owned target table.
_sources = [SRC_EXAM, SRC_HL7_MERGE, SRC_PATIENT_RAW, SRC_PATIENT_MAP, SRC_ORDERS]
if spark.catalog.tableExists(SRC_EXAM_TYPE):
    _sources.append(SRC_EXAM_TYPE)
_due, _versions = _target_due_check(TARGET, _sources)
if not _due:
    dbutils.notebook.exit(json.dumps({
        "pipeline": PIPELINE, "run_id": RUN_ID, "result": "NO_OP",
        "target": TARGET, "source_versions": _versions,
        "fingerprint": str(table_fingerprint(
            TARGET, exclude=("PIPELINE_UPDT_DT_TM", "ADC_UPDT"))),
        "writes": [],
    }, sort_keys=True))

# COMMAND ----------

_candidate, _temporal_columns = build_endobase_exam()
replace_with_tombstones(_candidate, TARGET, ["ENDOBASE_EXAM_ID"])
spark.sql(f"ALTER TABLE {TARGET} SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")

# Comments: SOURCE_CREATE_TS is source record authoring/creation time, not the procedure date.
_table_comment = (
    "One row per EndoBase EXAM_TBL.PRIMARY_NO. Person linkage follows PATIENT_P through "
    "map_endobase_patient, after owner-approved transitive HL7 patient-merge resolution; "
    "HL7_ORDER_ID supplies an independent Millennium order/person/encounter arm. "
    "DGVS exam terms may join only through ENDOBASE_EXAM_ID=EXAM_TBL.PRIMARY_NO. "
    "Never join term EXAM_P directly to PATIENT_TBL.PRIMARY_NO. Source date/time strings "
    "are retained beside parsed DQ triplets; 1753-era sentinels clean to NULL."
).replace("'", "''")
spark.sql(f"COMMENT ON TABLE {TARGET} IS '{_table_comment}'")
_column_comments = {
    "ENDOBASE_EXAM_ID": "EXAM_TBL.PRIMARY_NO; target grain and the only valid parent key for DGVS_EXAM_TERM.EXAM_P.",
    "ENDOBASE_PATIENT_ID_SOURCE": "EXAM_TBL.PATIENT_P. This is not DGVS_EXAM_TERM.EXAM_P.",
    "PERSON_ID": "Consensus-safe person resolution from EndoBase patient crosswalk and Millennium order arms.",
    "PERSON_LINK_STATUS": "Consensus/conflict/single-arm/ambiguous/unmatched status; no conflicting person is guessed.",
    "PATIENT_MERGE_STATUS": "Outcome of owner-approved HL7 absorbed-to-survivor transitive resolution before person linkage. MERGE_CYCLE_UNRESOLVED marks unorderable reverse-merge cycles whose resolution is withheld.",
    "SOURCE_CREATE_TS": "Source exam-record authoring/creation timestamp; not the clinical procedure date.",
    "PERFORMED_TS": "Parsed clinical performed timestamp; raw source retained and DQ triplet published.",
    "EXAM_TS": "Parsed clinical exam timestamp; raw source retained and DQ triplet published.",
    "HL7_ORDER_ID_RAW": "Raw EndoBase HL7 order identifier used only for exact numeric Millennium ORDER_ID linkage.",
    "DICOM_STUDY_UID": "DICOM study identifier carried from EXAM_TBL.",
}
for _col, _comment in _column_comments.items():
    if _col in spark.table(TARGET).columns:
        spark.sql(
            f"ALTER TABLE {TARGET} ALTER COLUMN {_col} COMMENT '{_comment.replace(chr(39), chr(39)*2)}'")

_identifier_cols = {
    c: ("4", "2") for c in spark.table(TARGET).columns
    if c in {
        "ENDOBASE_EXAM_ID", "ENDOBASE_EXAM_GUID", "ENDOBASE_PATIENT_ID_SOURCE",
        "ENDOBASE_PATIENT_ID_RESOLVED", "PERSON_ID", "HL7_ORDER_ID_RAW",
        "MILL_ORDER_ID", "MILL_ENCNTR_ID", "ENDOBASE_EXAM_NO", "DICOM_STUDY_UID",
        "DEPARTMENT_ID", "EXAM_SPACE_ID", "TEAM_ID", "EXAMINER_ID",
        "ATTENDANT_ID", "NURSE_ID", "SIGNER_ID",
    }
}
for _c, _dtype in spark.table(TARGET).dtypes:
    if _dtype == "string" and _c not in _identifier_cols:
        _identifier_cols[_c] = ("4", "2")
ig_tag_table(TARGET, _identifier_cols)
ig_tag_gate(TARGET)
_target_gates(_exam_rows, _temporal_columns)

_preview = exam_term_person_fill_preview()
_preview_counts = {
    str(r["PROPOSED_PERSON_LINK_STATUS"]): int(r["n"])
    for r in _preview.groupBy("PROPOSED_PERSON_LINK_STATUS").count()
                     .withColumnRenamed("count", "n").collect()
}
_fingerprint = table_fingerprint(
    TARGET, exclude=("PIPELINE_UPDT_DT_TM", "ADC_UPDT"))
_record_target_versions(TARGET, _versions)

# COMMAND ----------

# HUMAN-GATED PROMOTION RUNBOOK — comments only; this notebook never writes production.
# 1. Re-run source/schema/count/HL7-cycle/status/DQ/IG/fingerprint gates against the pinned landing.
# 2. Create the new 4_prod.bronze.map_endobase_exam from this reviewed code; retries=0.
# 3. Apply comments and IG tags to every prod column, then run ig_tag_gate on prod.
# 4. Separately, after explicit owner approval, MERGE only PERSON_ID/PERSON_LINK_STATUS into
#    4_prod.bronze.map_endobase_exam_term using:
#      term.ENDOBASE_EXAM_ID = exam.ENDOBASE_EXAM_ID
#    This implements EXAM_P -> EXAM_TBL.PRIMARY_NO -> PATIENT_P. The direct EXAM_P-to-patient
#    join is prohibited. Require 100% status accounting and zero EXAM_TABLE_NOT_LANDED afterward.
# 5. Add the new weekly bronze step only after source-vs-landed COUNT_BIG and owner/IG gates.
# 6. Record weekly runtime delta, preserve existing table properties, and notify no journey rebuild
#    (new exam table plus additive term linkage coverage).

dbutils.notebook.exit(json.dumps({
    "pipeline": PIPELINE,
    "run_id": RUN_ID,
    "result": "BUILT",
    "target": TARGET,
    "source_rows": {"exam": _exam_rows, "hl7_merge": _hl7_rows},
    "source_snapshots": {"exam": _exam_snapshot, "hl7_merge": _hl7_snapshot},
    "hl7_status_counts": _status_counts,
    "hl7_success_statuses": _success_statuses,
    "merge_diagnostics": _MERGE_DIAG,
    "temporal_columns": _temporal_columns,
    "term_fill_preview_status_counts": _preview_counts,
    "fingerprint": str(_fingerprint),
    "source_versions": _versions,
}, sort_keys=True))


