# Databricks notebook source
# iweb_coronary_pipeline — S6 B10 iWeb coronary Pre+Post parent.
# Pure Python. No production writes. The only executable target is
# 8_dev.bronze.iweb_coronary_procedure, and only after every landing gate passes.

# COMMAND ----------

import json
import re
import uuid
from collections import defaultdict
from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, TimestampType

# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 380158996159783)
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
    "landing_owner_signoff": "true",
    "ig_artefact_recorded": "true",
    "expected_pre_source_count": "",
    "expected_post_source_count": "",
}.items():
    _ensure_widget(_name, _default)

PIPELINE = "s6_iweb_coronary_parent_v1"
RUN_ID = str(uuid.uuid4())
SRC_PRE = "4_prod.iweb.reg_coronary_pre"
SRC_POST = "4_prod.iweb.reg_coronary_postprocedure"
SRC_LESION = "4_prod.bronze.iweb_coronary_lesion"
PERSON_ALIAS = "4_prod.raw.mill_person_alias"
TARGET = f"{TARGET_SCHEMA}.iweb_coronary_procedure"
MRN_ALIAS_TYPE = 10
NHS_ALIAS_TYPE = 18

# COMMAND ----------

# Exact generator/CreateWatermarkTable/ADF runbook. DATA ONLY; never executed here.
REGISTRATION_ROWS = [
    {
        "src_server_name": "dwh", "openquery_server": "RL1VMSQL04",
        "src_database": "iWeb", "src_schema": "dbo", "src_table": "Reg_CORONARY_Pre",
        "dst_catalog": "4_prod", "dst_schema": "iweb", "dst_table": "reg_coronary_pre",
        "copy_query": "##openquery_query##", "copy_trigger": "weekly",
        "copy_query_timeout": "00:10:00", "copy_priority": 0,
        "copy_partition_column": None, "watermark_column": None, "watermark_timestamp": None,
        "upsert_task": "wt_updt", "upsert_key_columns": None, "active_ind": 1,
        "comment_text": "S6 B10 iWeb coronary procedure Pre parent; EntryId grain; NHS/MRN retained for governed bronze linkage."
    },
    {
        "src_server_name": "dwh", "openquery_server": "RL1VMSQL04",
        "src_database": "iWeb", "src_schema": "dbo", "src_table": "Reg_CORONARY_PostProcedure",
        "dst_catalog": "4_prod", "dst_schema": "iweb", "dst_table": "reg_coronary_postprocedure",
        "copy_query": "##openquery_query##", "copy_trigger": "weekly",
        "copy_query_timeout": "00:10:00", "copy_priority": 0,
        "copy_partition_column": None, "watermark_column": None, "watermark_timestamp": None,
        "upsert_task": "wt_updt", "upsert_key_columns": None, "active_ind": 1,
        "comment_text": "S6 B10 iWeb coronary procedure Post parent; paired by EntryId; DateOfDischarge quality-gated downstream."
    },
]
CREATE_WATERMARK_RUNBOOK = {
    "generator": "~/build_cwt_repaired.py",
    "workspace_notebook": "/Workspace/Shared/ADC-DF/databricks/IncrementalUpdateV2/Setup/CreateWatermarkTable",
    "target_catalog": "6_mgmt",
    "forbidden_target_catalog": "8_dev",
    "steps": [
        "Drain existing IncrUpdtV2 staging/backlog and snapshot all current destination maxima.",
        "Add both REGISTRATION_ROWS to the generator beside Reg_CORONARY_SubProcedure.",
        "Regenerate and run Setup/CreateWatermarkTable against 6_mgmt only.",
        "Reconcile pre-run watermarks; the setup notebook truncates/rebuilds the registry.",
        "Verify dwh/RL1VMSQL04/iWeb/dbo, destinations, weekly, wt_updt, timeout, active_ind=1.",
    ],
}
ADF_RUNBOOK = {
    "pipeline": "IncrUpdtV2Pipeline",
    "parameters": {
        "trigger_name": "<minted watermark_id>",
        "dev_mode": "0",
        "run_rde": "0",
    },
    "steps": [
        "Promoter runs the Pre and Post watermark ids separately, never the broad weekly lane.",
        "dev_mode='0' is mandatory; manual ADF runs default to '1'.",
        "Compare source COUNT_BIG and landed Delta counts exactly for both tables.",
        "Record source-owner sign-off and IG artefact before enabling the bronze build.",
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

# Pre-build gates. These definitions precede every build/write.
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
    versions = spark.table(table).select(cols["ADC_UPDT"]).distinct().count()
    assert versions == 1, f"{table}: wt_updt snapshot has {versions} ADC_UPDT values"
    return str(spark.table(table).agg(F.max(cols["ADC_UPDT"]).alias("m")).collect()[0]["m"])

def _documented_pre_gate(table):
    names = [re.sub(r"[^A-Z0-9]", "", c.upper()) for c in spark.table(table).columns]
    groups = {
        "clinical_syndrome": ("SYNDROME",),
        "urgency": ("URGENCY",),
        "acs_timing": ("ACS", "SYMPTOMONSET", "ONSETTIME"),
        "lvef": ("LVEF", "EJECTIONFRACTION", "LVFUNCTION"),
    }
    missing = [
        label for label, tokens in groups.items()
        if not any(any(token in name for token in tokens) for name in names)
    ]
    assert not missing, (
        f"{table}: documented PCI Pre content groups missing {missing}; review landing schema before build")

def _target_due_check(target, sources):
    current = {t: int(table_version(t)) for t in sources}
    if not spark.catalog.tableExists(target):
        return True, current
    props = spark.sql(f"DESCRIBE DETAIL {target}").collect()[0]["properties"] or {}
    previous = {k: int(v) for k, v in json.loads(
        props.get("s6.source_versions_json", "{}")).items()}
    return previous != current, current

def _record_target_versions(target, versions):
    payload = json.dumps(versions, sort_keys=True, separators=(",", ":")).replace("'", "''")
    spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ('s6.source_versions_json'='{payload}')")

def _assert_dq_triplets(table, temporal_columns):
    cols = set(spark.table(table).columns)
    missing = [
        c + suffix for c in temporal_columns
        for suffix in ("_FUTURE_IND", "_SENTINEL_IND", "_CLEAN")
        if c + suffix not in cols
    ]
    assert not missing, f"{table}: missing DQ triplets {missing}"

def _target_gates(expected_rows, source_pre_rows, source_post_rows, lesion_rows, temporal_columns):
    assert spark.catalog.tableExists(TARGET), f"{TARGET}: target missing"
    active = spark.table(TARGET).where(F.col("SOURCE_PRESENT_IND"))
    row = active.agg(
        F.count("*").alias("n"),
        F.count("ENTRY_ID").alias("filled"),
        F.countDistinct("ENTRY_ID").alias("distinct_n"),
        F.count("PERSON_ID").alias("linked"),
        F.sum("LESION_CHILD_COUNT").alias("child_sum"),
    ).collect()[0]
    assert row["n"] == expected_rows == row["filled"] == row["distinct_n"], (
        f"target key gate failed expected={expected_rows}, n={row['n']}, "
        f"filled={row['filled']}, distinct={row['distinct_n']}")
    link_rate = float(row["linked"]) / float(row["n"]) if row["n"] else 0.0
    assert link_rate >= 0.98, f"person link rate {link_rate:.4%} below 98% precedent floor"
    assert int(row["child_sum"] or 0) == lesion_rows, (
        f"child accounting failed sum={row['child_sum']} lesion_rows={lesion_rows}")
    assert active.where(F.col("NHS_NUMBER").isNotNull() | F.col("MRN").isNotNull()).count() > 0
    _assert_dq_triplets(TARGET, temporal_columns)
    ig_tag_gate(TARGET)
    return link_rate

# COMMAND ----------

# Explicit early block. No target/control/schema object is created before this exit.
_required_raw = [SRC_PRE, SRC_POST]
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

PRE_REQUIRED = ["EntryId", "NHSNumber", "MRN", "DateLastChanged", "ADC_UPDT"]
POST_REQUIRED = [
    "EntryId", "NHSNumber", "MRN", "DateOfDischarge", "DateLastChanged", "ADC_UPDT",
]
LESION_REQUIRED = ["PARENT_ENTRY_ID", "ENTRY_ID", "PERSON_ID"]
ALIAS_REQUIRED = [
    "ALIAS", "PERSON_ID", "PERSON_ALIAS_TYPE_CD", "ACTIVE_IND",
    "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM", "ADC_UPDT",
]
_pre_cols = _require_columns(SRC_PRE, PRE_REQUIRED)
_post_cols = _require_columns(SRC_POST, POST_REQUIRED)
_require_columns(SRC_LESION, LESION_REQUIRED)
_require_columns(PERSON_ALIAS, ALIAS_REQUIRED)
_documented_pre_gate(SRC_PRE)

_pre_rows = _source_count_gate(SRC_PRE, 50000, _expected_count("expected_pre_source_count"))
_post_rows = _source_count_gate(SRC_POST, 50000, _expected_count("expected_post_source_count"))
_unique_non_null_gate(SRC_PRE, "EntryId")
_unique_non_null_gate(SRC_POST, "EntryId")
_pre_snapshot = _snapshot_gate(SRC_PRE)
_post_snapshot = _snapshot_gate(SRC_POST)

if not _bool_widget("landing_owner_signoff") or not _bool_widget("ig_artefact_recorded"):
    dbutils.notebook.exit(json.dumps({
        "pipeline": PIPELINE, "run_id": RUN_ID,
        "result": "BLOCKED_ON_LANDING_SIGNOFF",
        "pre_rows": _pre_rows, "post_rows": _post_rows,
        "landing_owner_signoff": _bool_widget("landing_owner_signoff"),
        "ig_artefact_recorded": _bool_widget("ig_artefact_recorded"),
        "writes": [],
    }, sort_keys=True))

# COMMAND ----------

def _column(value):
    return F.col(value) if isinstance(value, str) else value

def nhs_norm(value):
    digits = F.regexp_replace(_column(value).cast("string"), r"[^0-9]", "")
    return F.when(F.length(digits) == 10, digits)

def nhs_valid(norm_value):
    norm_col = _column(norm_value)
    weighted = reduce(
        lambda left, right: left + right,
        [
            F.substring(norm_col, index + 1, 1).cast("int") * (10 - index)
            for index in range(9)
        ],
    )
    check = F.lit(11) - (weighted % 11)
    expected = F.when(check == 11, F.lit(0)).otherwise(check)
    return (
        norm_col.isNotNull()
        & (check != 10)
        & (F.substring(norm_col, 10, 1).cast("int") == expected)
    )

def mrn_norm(value):
    digits = F.regexp_replace(_column(value).cast("string"), r"[^0-9]", "")
    stripped = F.regexp_replace(digits, r"^0+", "")
    return F.when(F.length(digits).between(1, 20) & (stripped != ""), stripped)

def _alias_lookup(alias_type_cd, prefix, historical_latest=False):
    alias_digits = F.regexp_replace(F.col("ALIAS").cast("string"), r"[^0-9]", "")
    alias_norm = (
        F.regexp_replace(alias_digits, r"^0+", "")
        if alias_type_cd == MRN_ALIAS_TYPE
        else F.when(F.length(alias_digits) == 10, alias_digits)
    )
    source = spark.table(PERSON_ALIAS).where(
        (F.col("ACTIVE_IND") == 1)
        & (F.col("PERSON_ALIAS_TYPE_CD") == alias_type_cd)
        & (F.col("BEG_EFFECTIVE_DT_TM").isNull()
           | (F.col("BEG_EFFECTIVE_DT_TM") <= F.current_timestamp()))
    )
    if not historical_latest:
        source = source.where(
            F.col("END_EFFECTIVE_DT_TM").isNull()
            | (F.col("END_EFFECTIVE_DT_TM") > F.current_timestamp()))
    source = source.withColumn("ALIAS_NORM", alias_norm).where(
        F.col("ALIAS_NORM").isNotNull() & (F.col("ALIAS_NORM") != ""))
    if historical_latest:
        recency = Window.partitionBy("ALIAS_NORM").orderBy(
            F.col("BEG_EFFECTIVE_DT_TM").desc_nulls_last(),
            F.col("ADC_UPDT").desc_nulls_last(),
        )
        source = source.withColumn(
            "_ALIAS_RECENCY_RANK", F.dense_rank().over(recency)).where(
                F.col("_ALIAS_RECENCY_RANK") == 1)
    return (
        source.groupBy("ALIAS_NORM")
        .agg(
            F.countDistinct("PERSON_ID").cast("long").alias(f"{prefix}_PERSON_COUNT"),
            F.max("PERSON_ID").cast("long").alias(f"{prefix}_PERSON_ID_RAW"),
        )
        .withColumn(
            f"{prefix}_PERSON_ID",
            F.when(F.col(f"{prefix}_PERSON_COUNT") == 1,
                   F.col(f"{prefix}_PERSON_ID_RAW")))
        .drop(f"{prefix}_PERSON_ID_RAW")
    )

# Copied from the live iWeb resolver contract: current aliases first, consensus-safe
# historical fallback, ambiguity retained, no disagreement guessed.
def resolve_persons(df, mrn_col=None, nhs_col=None):
    out = df
    if mrn_col and mrn_col in out.columns:
        out = out.withColumn("_MRN_NORM", mrn_norm(mrn_col))
        for prefix, historical in (("MRN_CUR", False), ("MRN_HIST", True)):
            lookup = _alias_lookup(MRN_ALIAS_TYPE, prefix, historical)
            out = out.join(
                lookup, out["_MRN_NORM"] == lookup["ALIAS_NORM"], "left"
            ).drop(lookup["ALIAS_NORM"])
    else:
        for name in (
            "MRN_CUR_PERSON_ID", "MRN_CUR_PERSON_COUNT",
            "MRN_HIST_PERSON_ID", "MRN_HIST_PERSON_COUNT",
        ):
            out = out.withColumn(name, F.lit(None).cast("long"))

    if nhs_col and nhs_col in out.columns:
        out = (out.withColumn("_NHS_NORM", nhs_norm(nhs_col))
                  .withColumn("NHS_NUMBER_VALID_IND", nhs_valid(F.col("_NHS_NORM"))))
        for prefix, historical in (("NHS_CUR", False), ("NHS_HIST", True)):
            lookup = _alias_lookup(NHS_ALIAS_TYPE, prefix, historical)
            out = out.join(
                lookup,
                out["NHS_NUMBER_VALID_IND"] & (out["_NHS_NORM"] == lookup["ALIAS_NORM"]),
                "left").drop(lookup["ALIAS_NORM"])
    else:
        for name in (
            "NHS_CUR_PERSON_ID", "NHS_CUR_PERSON_COUNT",
            "NHS_HIST_PERSON_ID", "NHS_HIST_PERSON_COUNT",
        ):
            out = out.withColumn(name, F.lit(None).cast("long"))
        out = out.withColumn("NHS_NUMBER_VALID_IND", F.lit(None).cast("boolean"))

    mrn_current, nhs_current = F.col("MRN_CUR_PERSON_ID"), F.col("NHS_CUR_PERSON_ID")
    mrn_history, nhs_history = F.col("MRN_HIST_PERSON_ID"), F.col("NHS_HIST_PERSON_ID")
    historical_agreement = (
        mrn_history.isNotNull() & nhs_history.isNotNull() & (mrn_history == nhs_history))

    mrn_person = (
        F.when(mrn_current.isNotNull(), mrn_current)
         .when(nhs_current.isNotNull() & (mrn_history == nhs_current), mrn_history)
         .when(nhs_current.isNull() & (nhs_history.isNull() | historical_agreement), mrn_history)
    )
    nhs_person = (
        F.when(nhs_current.isNotNull(), nhs_current)
         .when(mrn_current.isNotNull() & (nhs_history == mrn_current), nhs_history)
         .when(mrn_current.isNull() & (mrn_history.isNull() | historical_agreement), nhs_history)
    )
    fallback_used = (
        (mrn_current.isNull() & mrn_person.isNotNull())
        | (nhs_current.isNull() & nhs_person.isNotNull()))
    fallback_conflict = (
        (nhs_current.isNotNull() & mrn_history.isNotNull() & (nhs_current != mrn_history))
        | (mrn_current.isNotNull() & nhs_history.isNotNull() & (mrn_current != nhs_history))
        | (mrn_current.isNull() & nhs_current.isNull()
           & mrn_history.isNotNull() & nhs_history.isNotNull()
           & (mrn_history != nhs_history))
    )
    status = (
        F.when(mrn_person.isNotNull() & nhs_person.isNotNull() & (mrn_person == nhs_person),
               "MATCHED_BOTH")
         .when(mrn_person.isNotNull() & nhs_person.isNotNull() & (mrn_person != nhs_person),
               "CONFLICT")
         .when(mrn_person.isNotNull(), "MATCHED_MRN")
         .when(nhs_person.isNotNull(), "MATCHED_NHS")
         .when(
             (F.coalesce(F.col("MRN_CUR_PERSON_COUNT"), F.lit(0)) > 1)
             | (F.coalesce(F.col("MRN_HIST_PERSON_COUNT"), F.lit(0)) > 1)
             | (F.coalesce(F.col("NHS_CUR_PERSON_COUNT"), F.lit(0)) > 1)
             | (F.coalesce(F.col("NHS_HIST_PERSON_COUNT"), F.lit(0)) > 1),
             "AMBIGUOUS")
         .otherwise("UNMATCHED")
    )
    return (
        out.withColumn("LINKAGE_STATUS", status)
        .withColumn(
            "PERSON_ID",
            F.when(
                F.col("LINKAGE_STATUS").isin("MATCHED_BOTH", "MATCHED_MRN", "MATCHED_NHS"),
                F.coalesce(mrn_person, nhs_person)).cast("long"))
        .withColumn(
            "LINKAGE_METHOD",
            F.when(F.col("LINKAGE_STATUS").isin("MATCHED_BOTH", "CONFLICT"), "MRN+NHS")
             .when(F.col("LINKAGE_STATUS") == "MATCHED_MRN", "MRN")
             .when(F.col("LINKAGE_STATUS") == "MATCHED_NHS", "NHS"))
        .withColumn("LINKAGE_HISTORICAL_FALLBACK_IND",
                    F.coalesce(fallback_used, F.lit(False)))
        .withColumn("LINKAGE_FALLBACK_CONFLICT_IND",
                    F.coalesce(fallback_conflict, F.lit(False)))
        .drop(
            "_MRN_NORM", "_NHS_NORM",
            "MRN_CUR_PERSON_ID", "MRN_CUR_PERSON_COUNT",
            "MRN_HIST_PERSON_ID", "MRN_HIST_PERSON_COUNT",
            "NHS_CUR_PERSON_ID", "NHS_CUR_PERSON_COUNT",
            "NHS_HIST_PERSON_ID", "NHS_HIST_PERSON_COUNT",
        )
    )

def _snake(name):
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", str(name))
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"[^A-Za-z0-9]+", "_", value)
    return re.sub(r"_+", "_", value).strip("_").upper()

def _patient_pii_excluded(name):
    norm = re.sub(r"[^a-z0-9]", "", name.lower())
    if norm in {"nhsnumber", "mrn"}:
        return False
    exact = {
        "surname", "forename", "firstname", "lastname", "patientname",
        "dateofbirth", "dob", "nameofkin", "phoneofkin",
    }
    return (
        norm in exact or "nextofkin" in norm or "postcode" in norm
        or "address" in norm or "telephone" in norm or "phone" in norm
        or "mobile" in norm or "email" in norm
    )

def _prefix_source(table, prefix, is_post=False):
    df = spark.table(table)
    case = {c.upper(): c for c in df.columns}
    specials = {
        "ENTRYID", "NHSNUMBER", "MRN", "DATELASTCHANGED", "ADC_UPDT", "ADCUPDT",
    }
    if is_post:
        specials.add("DATEOFDISCHARGE")
    exprs = [
        F.col(case["ENTRYID"]).cast("long").alias("ENTRY_ID"),
        F.lit(True).alias(f"_{prefix}_ROW_PRESENT"),
        F.col(case["NHSNUMBER"]).cast("string").alias(f"_{prefix}_NHS_NUMBER"),
        F.col(case["MRN"]).cast("string").alias(f"_{prefix}_MRN"),
        F.col(case["DATELASTCHANGED"]).cast("timestamp").alias(f"{prefix}_DATE_LAST_CHANGED"),
        F.col(case["ADC_UPDT"]).cast("timestamp").alias(f"{prefix}_ADC_UPDT"),
    ]
    if is_post:
        discharge = case["DATEOFDISCHARGE"]
        exprs.extend([
            F.col(discharge).cast("string").alias("POST_DATE_OF_DISCHARGE_RAW"),
            F.expr(f"try_cast(`{discharge}` AS TIMESTAMP)").alias("POST_DATE_OF_DISCHARGE_TS"),
        ])
    used = {"ENTRY_ID"}
    for field in df.schema.fields:
        norm = re.sub(r"[^A-Z0-9_]", "", field.name.upper())
        norm_no_underscore = norm.replace("_", "")
        if norm_no_underscore in specials or _patient_pii_excluded(field.name):
            continue
        alias = f"{prefix}_{_snake(field.name)}"
        assert alias not in used, f"{table}: prefixed column collision {alias}"
        used.add(alias)
        if isinstance(field.dataType, StringType) and (
            "DATE" in alias or "TIME" in alias or alias.endswith("_TS")
        ):
            exprs.append(F.col(field.name).cast("string").alias(alias + "_RAW"))
            quoted = field.name.replace("`", "``")
            exprs.append(F.expr(f"try_cast(`{quoted}` AS TIMESTAMP)").alias(alias + "_TS"))
        else:
            exprs.append(F.col(field.name).alias(alias))
    return df.select(*exprs)

def _row_hash(df):
    cols = sorted(c for c in df.columns if c not in {"PIPELINE_UPDT_DT_TM", "ROW_HASH"})
    return df.withColumn(
        "ROW_HASH", F.sha2(F.to_json(F.struct(*[F.col(c) for c in cols])), 256))

def build_iweb_coronary_procedure():
    pre = _prefix_source(SRC_PRE, "PRE", is_post=False)
    post = _prefix_source(SRC_POST, "POST", is_post=True)
    joined = (
        pre.alias("pre").join(post.alias("post"), "ENTRY_ID", "full")
        .withColumn(
            "SOURCE_JOIN_STATUS",
            F.when(F.col("_PRE_ROW_PRESENT") & F.col("_POST_ROW_PRESENT"), "PRE_AND_POST")
             .when(F.col("_PRE_ROW_PRESENT"), "PRE_ONLY")
             .otherwise("POST_ONLY"))
        .withColumn("NHS_NUMBER", F.coalesce("_PRE_NHS_NUMBER", "_POST_NHS_NUMBER"))
        .withColumn("MRN", F.coalesce("_PRE_MRN", "_POST_MRN"))
        .withColumn(
            "IDENTIFIER_CONCORDANCE_IND",
            F.when(
                F.col("_PRE_NHS_NUMBER").isNull() | F.col("_POST_NHS_NUMBER").isNull()
                | F.col("_PRE_MRN").isNull() | F.col("_POST_MRN").isNull(),
                F.lit(None).cast("boolean"))
             .otherwise(
                 (F.trim(F.col("_PRE_NHS_NUMBER")) == F.trim(F.col("_POST_NHS_NUMBER")))
                 & (F.trim(F.col("_PRE_MRN")) == F.trim(F.col("_POST_MRN")))))
        .withColumn(
            "DATE_LAST_CHANGED",
            F.greatest("PRE_DATE_LAST_CHANGED", "POST_DATE_LAST_CHANGED"))
        .withColumn("ADC_UPDT", F.greatest("PRE_ADC_UPDT", "POST_ADC_UPDT"))
        .drop(
            "_PRE_ROW_PRESENT", "_POST_ROW_PRESENT",
            "_PRE_NHS_NUMBER", "_POST_NHS_NUMBER", "_PRE_MRN", "_POST_MRN")
    )
    linked = resolve_persons(joined, mrn_col="MRN", nhs_col="NHS_NUMBER")

    lesion = spark.table(SRC_LESION)
    if "IS_CURRENT_IN_SOURCE" in lesion.columns:
        lesion = lesion.where(F.col("IS_CURRENT_IN_SOURCE"))
    child_counts = (
        lesion.where(F.col("PARENT_ENTRY_ID").isNotNull())
        .groupBy(F.col("PARENT_ENTRY_ID").cast("long").alias("_PARENT_ENTRY_ID"))
        .agg(F.count("*").cast("long").alias("LESION_CHILD_COUNT"))
    )
    with_children = (
        linked.join(
            child_counts, linked["ENTRY_ID"] == child_counts["_PARENT_ENTRY_ID"], "left")
        .withColumn("LESION_CHILD_COUNT", F.coalesce("LESION_CHILD_COUNT", F.lit(0).cast("long")))
        .drop("_PARENT_ENTRY_ID")
    )
    published, temporal = dq_all_clinical(
        with_children,
        admin_stamps={
            "PRE_DATE_LAST_CHANGED", "POST_DATE_LAST_CHANGED", "DATE_LAST_CHANGED",
            "PRE_ADC_UPDT", "POST_ADC_UPDT", "ADC_UPDT",
        })
    published = _row_hash(
        published.withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp()))
    return published, temporal, lesion

# COMMAND ----------

_sources = [SRC_PRE, SRC_POST, SRC_LESION, PERSON_ALIAS]
_due, _versions = _target_due_check(TARGET, _sources)
if not _due:
    dbutils.notebook.exit(json.dumps({
        "pipeline": PIPELINE, "run_id": RUN_ID, "result": "NO_OP",
        "target": TARGET, "source_versions": _versions,
        "fingerprint": str(table_fingerprint(
            TARGET, exclude=("PIPELINE_UPDT_DT_TM", "ADC_UPDT",
                             "PRE_ADC_UPDT", "POST_ADC_UPDT"))),
        "writes": [],
    }, sort_keys=True))

# COMMAND ----------

_candidate, _temporal_columns, _lesion = build_iweb_coronary_procedure()
_union_key_count = (
    spark.table(SRC_PRE).select(F.col(_pre_cols["ENTRYID"]).cast("long").alias("ENTRY_ID"))
    .unionByName(
        spark.table(SRC_POST).select(F.col(_post_cols["ENTRYID"]).cast("long").alias("ENTRY_ID")))
    .distinct().count()
)
_parent_keys = _candidate.select("ENTRY_ID").distinct()
_orphan_children = (
    _lesion.where(F.col("PARENT_ENTRY_ID").isNotNull())
    .select(F.col("PARENT_ENTRY_ID").cast("long").alias("ENTRY_ID")).distinct()
    .join(_parent_keys, "ENTRY_ID", "left_anti").count()
)
assert _orphan_children == 0, (
    f"{_orphan_children} current lesion parent ids absent from Pre+Post parent key set")

replace_with_tombstones(_candidate, TARGET, ["ENTRY_ID"])
spark.sql(f"ALTER TABLE {TARGET} SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")

_table_comment = (
    "One row per unioned Reg_CORONARY_Pre/Reg_CORONARY_PostProcedure EntryId, with PRE_ and "
    "POST_ source fields kept separate. NHS number and MRN are deliberately published and "
    "column-tagged ig_risk=4/ig_severity=2 under the 2026-08-13 identifier doctrine. "
    "Person linkage copies the live iWeb current-alias plus consensus-safe historical fallback. "
    "DateOfDischarge raw text is retained beside parsed future/sentinel/clean fields; known source "
    "maximum 2026-12-10 is a future typo, never silently accepted. DATE_LAST_CHANGED is an edit "
    "timestamp, not the procedure date. LESION_CHILD_COUNT counts current iweb_coronary_lesion "
    "children; parent procedures with zero lesions are legitimate and retained."
).replace("'", "''")
spark.sql(f"COMMENT ON TABLE {TARGET} IS '{_table_comment}'")
_comments = {
    "ENTRY_ID": "Reg_CORONARY_Pre/PostProcedure EntryId; parent key for lesion PARENT_ENTRY_ID.",
    "NHS_NUMBER": "Source NHS number published under identifier doctrine; ig_risk=4 and ig_severity=2.",
    "MRN": "Source local medical record number published under identifier doctrine; ig_risk=4 and ig_severity=2.",
    "PERSON_ID": "Millennium PERSON_ID from the live iWeb consensus-safe MRN/NHS resolver.",
    "LINKAGE_STATUS": "MATCHED_BOTH, MATCHED_MRN, MATCHED_NHS, CONFLICT, AMBIGUOUS, or UNMATCHED.",
    "POST_DATE_OF_DISCHARGE_RAW": "Verbatim source discharge value; retained even when future-dated.",
    "POST_DATE_OF_DISCHARGE_TS": "Parsed discharge timestamp; use its DQ triplet and CLEAN value.",
    "DATE_LAST_CHANGED": "Greatest Pre/Post source edit timestamp; not the clinical procedure date.",
    "LESION_CHILD_COUNT": "Count of current iweb_coronary_lesion rows whose PARENT_ENTRY_ID equals this EntryId.",
}
for _col, _comment in _comments.items():
    if _col in spark.table(TARGET).columns:
        spark.sql(
            f"ALTER TABLE {TARGET} ALTER COLUMN {_col} COMMENT '{_comment.replace(chr(39), chr(39)*2)}'")

_tag_map = {}
for _c, _dtype in spark.table(TARGET).dtypes:
    if (
        _c in {"ENTRY_ID", "NHS_NUMBER", "MRN", "PERSON_ID"}
        or _c.endswith("_ID")
        or _c.endswith("_UID")
        or _dtype == "string"
    ):
        _tag_map[_c] = ("4", "2")
ig_tag_table(TARGET, _tag_map)
ig_tag_gate(TARGET)

_lesion_rows = _lesion.where(F.col("PARENT_ENTRY_ID").isNotNull()).count()
_link_rate = _target_gates(
    _union_key_count, _pre_rows, _post_rows, _lesion_rows, _temporal_columns)
_active = spark.table(TARGET).where(F.col("SOURCE_PRESENT_IND"))
_join_status_counts = {
    str(r["SOURCE_JOIN_STATUS"]): int(r["n"])
    for r in _active.groupBy("SOURCE_JOIN_STATUS").count().withColumnRenamed("count", "n").collect()
}
_parents_without_lesions = _active.where(F.col("LESION_CHILD_COUNT") == 0).count()
_fingerprint = table_fingerprint(
    TARGET, exclude=("PIPELINE_UPDT_DT_TM", "ADC_UPDT", "PRE_ADC_UPDT", "POST_ADC_UPDT"))
_record_target_versions(TARGET, _versions)

# COMMAND ----------

# HUMAN-GATED PROMOTION RUNBOOK — comments only; this notebook never writes production.
# 1. Confirm both generator rows exist after a fresh Setup/CreateWatermarkTable regeneration.
# 2. Run each minted watermark id through IncrUpdtV2Pipeline with dev_mode='0', run_rde='0'.
# 3. Pin source COUNT_BIG, landed counts, schemas and Delta versions; rerun all key, child,
#    linkage, DQ, fingerprint and IG gates with retries=0.
# 4. Create the new 4_prod.bronze.iweb_coronary_procedure only after review; apply every
#    table/column comment, tag every column, and run ig_tag_gate against prod.
# 5. NHS_NUMBER and MRN remain published at ig_risk=4/ig_severity=2. Do not copy the old
#    lesion-table dropped-NHS precedent.
# 6. Verify zero orphan current lesion children; record parents without lesions as legitimate.
# 7. Add the new weekly bronze step beside the other iWeb feeds and record weekly runtime delta.
#    This is a new table, so no journey notify-first rebuild is required.

dbutils.notebook.exit(json.dumps({
    "pipeline": PIPELINE,
    "run_id": RUN_ID,
    "result": "BUILT",
    "target": TARGET,
    "target_schema": TARGET_SCHEMA,
    "source_rows": {"pre": _pre_rows, "post": _post_rows},
    "source_snapshots": {"pre": _pre_snapshot, "post": _post_snapshot},
    "union_parent_rows": _union_key_count,
    "source_join_status_counts": _join_status_counts,
    "lesion_rows_accounted": _lesion_rows,
    "orphan_children": _orphan_children,
    "parents_without_lesions": _parents_without_lesions,
    "person_link_rate": _link_rate,
    "temporal_columns": _temporal_columns,
    "fingerprint": str(_fingerprint),
    "source_versions": _versions,
}, sort_keys=True))


