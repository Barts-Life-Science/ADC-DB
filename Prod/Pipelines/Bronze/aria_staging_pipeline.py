# Databricks notebook source
# aria_staging_pipeline — S6c B11 ARIA diagnosis and structured staging.
# Pure Python. Writes only 8_dev.bronze.map_aria_diagnosis_staging.

# COMMAND ----------

import json
import uuid
from pyspark.sql import Window
from pyspark.sql import functions as F

# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 586267715838127)
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
    "expected_source_count": "",
}.items():
    _ensure_widget(_name, _default)

PIPELINE = "s6_aria_diagnosis_staging_v1"
RUN_ID = str(uuid.uuid4())
SRC_DX = "4_prod.raw.aria_pt_dx"
SRC_PTKEY = "4_prod.raw.aria_pt_inst_key"
SRC_ALIAS = "4_prod.raw.mill_person_alias"
SRC_PERSON = "4_prod.raw.mill_person"
SRC_CONCEPT = "3_lookup.omop.concept"
SRC_REL = "3_lookup.omop.concept_relationship"
TARGET = f"{TARGET_SCHEMA}.map_aria_diagnosis_staging"
MRN_KEY_CD = 2
MRN_ALIAS_TYPE_CD = 10

# The DW mirror is frozen at 2024-07-22. Native ARIA radiotherapy tables are absent
# from this mirror and remain a separate governance-lane request.


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


# COMMAND ----------

_required = [SRC_DX, SRC_PTKEY, SRC_ALIAS, SRC_PERSON, SRC_CONCEPT, SRC_REL]
_missing = [t for t in _required if not spark.catalog.tableExists(t)]
if _missing:
    dbutils.notebook.exit(json.dumps({
        "pipeline": PIPELINE, "run_id": RUN_ID, "result": "BLOCKED_ON_LANDING",
        "missing_sources": _missing, "writes": [],
    }, sort_keys=True))

DX_REQUIRED = [
    "pt_id", "dx_id", "icd_cd", "stage_of_disease", "onset_date", "confirm_dx",
    "date_approx_ind", "stg_crit_desc", "cur_entry_ind", "cs_of_dth_ind", "hx_of_ind",
    "ADC_UPDT",
]
PTKEY_REQUIRED = ["pt_id", "pt_key_cd", "pt_key_value", "ADC_UPDT"]
ALIAS_REQUIRED = ["PERSON_ID", "ALIAS", "PERSON_ALIAS_TYPE_CD", "ACTIVE_IND", "ADC_UPDT"]
_require_columns(SRC_DX, DX_REQUIRED)
_require_columns(SRC_PTKEY, PTKEY_REQUIRED)
_require_columns(SRC_ALIAS, ALIAS_REQUIRED)
_require_columns(SRC_PERSON, ["PERSON_ID"])
_require_columns(SRC_CONCEPT, [
    "concept_id", "concept_name", "concept_code", "vocabulary_id", "domain_id",
    "standard_concept", "invalid_reason",
])
_require_columns(SRC_REL, ["concept_id_1", "concept_id_2", "relationship_id", "invalid_reason"])

_source_rows = spark.table(SRC_DX).count()
_expected_text = str(dbutils.widgets.get("expected_source_count")).strip()
if _expected_text:
    assert _source_rows == int(_expected_text), (
        f"{SRC_DX}: expected {int(_expected_text)} rows, found {_source_rows}")
assert _source_rows == 134818, f"ARIA_PT_DX landed row count drifted: {_source_rows}"

_grain = spark.table(SRC_DX).agg(
    F.count("*").alias("n"),
    F.count("pt_id").alias("pt_filled"),
    F.count("dx_id").alias("dx_filled"),
    F.countDistinct(F.struct("pt_id", "dx_id")).alias("k"),
).first()
assert _grain["n"] == _grain["pt_filled"] == _grain["dx_filled"] == _grain["k"], (
    f"ARIA grain must be unique non-null (pt_id,dx_id): {_grain.asDict()}")

_profile = spark.table(SRC_DX).agg(
    F.countDistinct("pt_id").alias("patients"),
    F.sum(F.when(F.col("icd_cd").isNotNull(), 1).otherwise(0)).alias("icd_nonnull"),
    F.sum(F.when(F.trim(F.coalesce("icd_cd", F.lit(""))) != "", 1).otherwise(0)).alias("icd_nonblank"),
    F.sum(F.when(F.trim(F.coalesce("stage_of_disease", F.lit(""))) != "", 1).otherwise(0)).alias("stage_nonblank"),
    F.sum(F.when(F.col("cur_entry_ind") == "Y", 1).otherwise(0)).alias("current_rows"),
    F.min("onset_date").alias("onset_min_raw"),
    F.max("onset_date").alias("onset_max_raw"),
).first()
assert int(_profile["patients"]) == 31056
assert int(_profile["icd_nonnull"]) == 33959
assert int(_profile["icd_nonblank"]) == 33276
assert int(_profile["stage_nonblank"]) == 21286
assert int(_profile["current_rows"]) == 134771

if not _bool_widget("landing_owner_signoff") or not _bool_widget("ig_artefact_recorded"):
    dbutils.notebook.exit(json.dumps({
        "pipeline": PIPELINE, "run_id": RUN_ID,
        "result": "BLOCKED_ON_LANDING_SIGNOFF",
        "source_rows": _source_rows,
        "source_profile": {k: str(v) for k, v in _profile.asDict().items()},
        "landing_owner_signoff": _bool_widget("landing_owner_signoff"),
        "ig_artefact_recorded": _bool_widget("ig_artefact_recorded"),
        "writes": [],
    }, sort_keys=True))

# COMMAND ----------

def _norm_id(c):
    return F.regexp_replace(F.trim(c.cast("string")), " ", "")

def build_patient_link():
    any_key = (
        spark.table(SRC_PTKEY)
        .select(F.col("pt_id").cast("string").alias("ARIA_PT_ID"))
        .where(F.col("ARIA_PT_ID").isNotNull())
        .distinct()
        .withColumn("PT_INST_KEY_ANY_IND", F.lit(True))
    )
    mrn_keys = (
        spark.table(SRC_PTKEY)
        .where(F.col("pt_key_cd") == MRN_KEY_CD)
        .select(
            F.col("pt_id").cast("string").alias("ARIA_PT_ID"),
            _norm_id(F.col("pt_key_value")).alias("_MRN"),
        )
        .where(F.col("_MRN").isNotNull() & (F.col("_MRN") != ""))
        .distinct()
    )
    aliases = (
        spark.table(SRC_ALIAS)
        .where((F.col("ACTIVE_IND") == 1) & (F.col("PERSON_ALIAS_TYPE_CD") == MRN_ALIAS_TYPE_CD))
        .select(
            _norm_id(F.col("ALIAS")).alias("_MRN"),
            F.col("PERSON_ID").cast("long").alias("_PERSON_ID"),
        )
        .where(F.col("_MRN").isNotNull() & (F.col("_MRN") != ""))
    )
    linked = mrn_keys.join(aliases, "_MRN", "left")
    per_patient = (
        linked.groupBy("ARIA_PT_ID")
        .agg(
            F.countDistinct("_MRN").cast("long").alias("ARIA_MRN_KEY_COUNT"),
            F.min("_MRN").alias("ARIA_MRN"),
            F.countDistinct("_PERSON_ID").cast("long").alias("PERSON_CANDIDATE_COUNT"),
            F.min("_PERSON_ID").cast("long").alias("_PERSON_ID"),
        )
        .withColumn(
            "PERSON_LINK_STATUS",
            F.when(F.col("PERSON_CANDIDATE_COUNT") == 1, "LINKED_MRN")
             .when(F.col("PERSON_CANDIDATE_COUNT") > 1, "AMBIGUOUS_MRN")
             .otherwise("MRN_NO_ALIAS_MATCH"))
        .withColumn(
            "PERSON_ID",
            F.when(F.col("PERSON_CANDIDATE_COUNT") == 1, F.col("_PERSON_ID")))
        .drop("_PERSON_ID")
    )
    return (
        any_key.join(per_patient, "ARIA_PT_ID", "full")
        .withColumn("PT_INST_KEY_ANY_IND", F.coalesce("PT_INST_KEY_ANY_IND", F.lit(False)))
        .withColumn("ARIA_MRN_KEY_COUNT", F.coalesce("ARIA_MRN_KEY_COUNT", F.lit(0).cast("long")))
        .withColumn("PERSON_CANDIDATE_COUNT", F.coalesce("PERSON_CANDIDATE_COUNT", F.lit(0).cast("long")))
        .withColumn(
            "PERSON_LINK_STATUS",
            F.coalesce(
                "PERSON_LINK_STATUS",
                F.when(F.col("PT_INST_KEY_ANY_IND"), F.lit("NO_MRN_KEY"))
                 .otherwise(F.lit("NO_PT_INST_KEY"))))
    )

def build_icd_map():
    codes = (
        spark.table(SRC_DX)
        .select(F.upper(F.trim("icd_cd")).alias("ICD_CODE"))
        .where(F.col("ICD_CODE").isNotNull() & (F.col("ICD_CODE") != ""))
        .distinct()
        .withColumn("_ICD_NORM", F.regexp_replace("ICD_CODE", "[.]", ""))
    )
    concept = (
        spark.table(SRC_CONCEPT)
        .where(F.col("invalid_reason").isNull() & F.col("vocabulary_id").isin("ICD10", "ICD10CM"))
        .select(
            F.col("concept_id").cast("long").alias("_SOURCE_CONCEPT_ID"),
            F.upper(F.regexp_replace(F.trim("concept_code"), "[.]", "")).alias("_ICD_NORM"),
            F.col("standard_concept").alias("_SOURCE_STANDARD"),
        )
    )
    std = (
        spark.table(SRC_CONCEPT)
        .where(F.col("invalid_reason").isNull() & (F.col("standard_concept") == "S"))
        .select(
            F.col("concept_id").cast("long").alias("DIAGNOSIS_CONCEPT_ID"),
            F.col("concept_name").alias("DIAGNOSIS_CONCEPT_NAME"),
            F.col("vocabulary_id").alias("DIAGNOSIS_VOCABULARY_ID"),
            F.col("domain_id").alias("DIAGNOSIS_DOMAIN_ID"),
        )
    )
    rel = (
        spark.table(SRC_REL)
        .where((F.col("relationship_id") == "Maps to") & F.col("invalid_reason").isNull())
        .select(
            F.col("concept_id_1").cast("long").alias("_SOURCE_CONCEPT_ID"),
            F.col("concept_id_2").cast("long").alias("DIAGNOSIS_CONCEPT_ID"),
        )
    )
    src = codes.join(concept, "_ICD_NORM", "left")
    direct = (
        src.where(F.col("_SOURCE_STANDARD") == "S")
        .select("ICD_CODE", F.col("_SOURCE_CONCEPT_ID").alias("DIAGNOSIS_CONCEPT_ID"))
    )
    mapped = (
        src.where(F.col("_SOURCE_CONCEPT_ID").isNotNull())
        .join(rel, "_SOURCE_CONCEPT_ID", "inner")
        .select("ICD_CODE", "DIAGNOSIS_CONCEPT_ID")
    )
    candidates = direct.unionByName(mapped).distinct()
    agg = (
        candidates.groupBy("ICD_CODE")
        .agg(
            F.countDistinct("DIAGNOSIS_CONCEPT_ID").cast("int").alias("ICD_STANDARD_CANDIDATE_COUNT"),
            F.min("DIAGNOSIS_CONCEPT_ID").cast("long").alias("_ONLY_CONCEPT_ID"),
        )
    )
    source_match = (
        src.groupBy("ICD_CODE")
        .agg(F.countDistinct("_SOURCE_CONCEPT_ID").cast("int").alias("ICD_SOURCE_CANDIDATE_COUNT"))
    )
    return (
        codes.drop("_ICD_NORM")
        .join(source_match, "ICD_CODE", "left")
        .join(agg, "ICD_CODE", "left")
        .withColumn("ICD_SOURCE_CANDIDATE_COUNT", F.coalesce("ICD_SOURCE_CANDIDATE_COUNT", F.lit(0)))
        .withColumn("ICD_STANDARD_CANDIDATE_COUNT", F.coalesce("ICD_STANDARD_CANDIDATE_COUNT", F.lit(0)))
        .withColumn(
            "ICD_MAPPING_STATUS",
            F.when(F.col("ICD_SOURCE_CANDIDATE_COUNT") == 0, "UNMAPPED_CODE")
             .when(F.col("ICD_STANDARD_CANDIDATE_COUNT") == 0, "NO_STANDARD_MAP")
             .when(F.col("ICD_STANDARD_CANDIDATE_COUNT") == 1, "MAPPED_STANDARD")
             .otherwise("AMBIGUOUS_STANDARD_MAP"))
        .withColumn(
            "DIAGNOSIS_CONCEPT_ID",
            F.when(F.col("ICD_STANDARD_CANDIDATE_COUNT") == 1, F.col("_ONLY_CONCEPT_ID")))
        .drop("_ONLY_CONCEPT_ID")
        .join(std, "DIAGNOSIS_CONCEPT_ID", "left")
    )

def build_target():
    source = spark.table(SRC_DX)
    source_cols = []
    for field in source.schema.fields:
        if field.name == "pt_id":
            source_cols.append(F.col(field.name).cast("string").alias("ARIA_PT_ID"))
        elif field.name == "dx_id":
            source_cols.append(F.col(field.name).cast("long").alias("ARIA_DX_ID"))
        elif field.name == "ADC_UPDT":
            source_cols.append(F.col(field.name).cast("timestamp").alias("ADC_UPDT"))
        else:
            source_cols.append(F.col(field.name).alias(field.name.upper()))
    base = source.select(*source_cols)
    patient = build_patient_link()
    icd = build_icd_map()
    joined = (
        base.join(patient, "ARIA_PT_ID", "left")
        .withColumn("PT_INST_KEY_ANY_IND", F.coalesce("PT_INST_KEY_ANY_IND", F.lit(False)))
        .withColumn("ARIA_MRN_KEY_COUNT", F.coalesce("ARIA_MRN_KEY_COUNT", F.lit(0).cast("long")))
        .withColumn("PERSON_CANDIDATE_COUNT", F.coalesce("PERSON_CANDIDATE_COUNT", F.lit(0).cast("long")))
        .withColumn("PERSON_LINK_STATUS", F.coalesce("PERSON_LINK_STATUS", F.lit("NO_PT_INST_KEY")))
        .withColumn("ICD_CODE", F.upper(F.trim("ICD_CD")))
        .join(icd, "ICD_CODE", "left")
    )
    published, temporal = dq_all_clinical(
        joined,
        admin_stamps={
            "TRANS_LOG_TSTAMP", "TRANS_LOG_MTSTAMP", "TRANS_TRF_TSTAMP", "ADC_UPDT",
        },
    )
    published = (
        published
        .withColumn(
            "ARIA_DIAGNOSIS_KEY",
            F.sha2(F.concat_ws("||", F.col("ARIA_PT_ID"), F.col("ARIA_DX_ID").cast("string")), 256))
        .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp())
    )
    return published, temporal

# COMMAND ----------

_sources = [SRC_DX, SRC_PTKEY, SRC_ALIAS, SRC_PERSON, SRC_CONCEPT, SRC_REL]
_due, _versions = _target_due_check(TARGET, _sources)
if not _due:
    dbutils.notebook.exit(json.dumps({
        "pipeline": PIPELINE, "run_id": RUN_ID, "result": "NO_OP",
        "target": TARGET, "source_versions": _versions,
        "fingerprint": str(table_fingerprint(
            TARGET, exclude=("PIPELINE_UPDT_DT_TM", "ADC_UPDT"))),
        "writes": [],
    }, sort_keys=True))

_candidate, _temporal_columns = build_target()
replace_with_tombstones(_candidate, TARGET, ["ARIA_PT_ID", "ARIA_DX_ID"])
spark.sql(f"ALTER TABLE {TARGET} SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")

_comment = (
    "One row per source (pt_id, dx_id) from the frozen 2024-07-22 ARIA_PT_DX DW mirror. "
    "Publishes the estate's first structured diagnosis/staging arm including stage_of_disease, "
    "stg_crit_desc, cs_of_dth_ind, hx_of_ind and date_approx_ind verbatim. PERSON_ID uses the "
    "established ARIA pt_key_cd=2 MRN lane against active Millennium MRN aliases; ambiguity and "
    "missing-key states are explicit. ICD codes map only where a unique valid standard OMOP "
    "concept is reached by exact ICD10/ICD10CM code and Maps-to semantics. Native radiotherapy "
    "tables are absent from the DW mirror and remain governance-scoped."
).replace("'", "''")
spark.sql(f"COMMENT ON TABLE {TARGET} IS '{_comment}'")

_column_comments = {
    "ARIA_PT_ID": "ARIA internal patient identifier from ARIA_PT_DX.pt_id.",
    "ARIA_DX_ID": "ARIA diagnosis sequence; unique only with ARIA_PT_ID.",
    "PERSON_ID": "Consensus-safe Millennium PERSON_ID from the ARIA MRN key lane; NULL for ambiguous or unresolved patients.",
    "PERSON_LINK_STATUS": "LINKED_MRN, AMBIGUOUS_MRN, MRN_NO_ALIAS_MATCH, NO_MRN_KEY, or NO_PT_INST_KEY.",
    "STAGE_OF_DISEASE": "Verbatim structured ARIA stage_of_disease value; the estate's first structured staging asset.",
    "STG_CRIT_DESC": "Verbatim ARIA staging-criteria description.",
    "DATE_APPROX_IND": "Verbatim indicator that the clinical date is approximate.",
    "ONSET_DATE": "Verbatim source onset timestamp; use the DQ triplet and ONSET_DATE_CLEAN.",
    "DIAGNOSIS_CONCEPT_ID": "Unique valid standard OMOP concept reached from exact ICD code semantics; NULL when absent or ambiguous.",
}
for _col, _text in _column_comments.items():
    if _col in spark.table(TARGET).columns:
        spark.sql(
            f"ALTER TABLE {TARGET} ALTER COLUMN {_col} COMMENT '{_text.replace(chr(39), chr(39)*2)}'")

_tag_map = {}
for _col, _dtype in spark.table(TARGET).dtypes:
    if (
        _col in {"ARIA_PT_ID", "ARIA_DX_ID", "ARIA_DIAGNOSIS_KEY", "ARIA_MRN", "PERSON_ID"}
        or _col.endswith("_ID") or _col.endswith("_KEY") or _dtype == "string"
    ):
        _tag_map[_col] = ("4", "2")
ig_tag_table(TARGET, _tag_map)
ig_tag_gate(TARGET)

_active = spark.table(TARGET).where(F.col("SOURCE_PRESENT_IND"))
_gate = _active.agg(
    F.count("*").alias("n"),
    F.countDistinct(F.struct("ARIA_PT_ID", "ARIA_DX_ID")).alias("k"),
    F.countDistinct("ARIA_PT_ID").alias("patients"),
    F.countDistinct(F.when(F.col("PT_INST_KEY_ANY_IND"), F.col("ARIA_PT_ID"))).alias("patients_with_any_key"),
    F.countDistinct(F.when(F.col("PERSON_ID").isNotNull(), F.col("ARIA_PT_ID"))).alias("patients_linked"),
    F.sum(F.when(F.trim(F.coalesce("STAGE_OF_DISEASE", F.lit(""))) != "", 1).otherwise(0)).alias("stage_nonblank"),
    F.sum(F.when(F.col("CUR_ENTRY_IND") == "Y", 1).otherwise(0)).alias("current_rows"),
    F.min("ONSET_DATE_CLEAN").alias("onset_min_clean"),
    F.max("ONSET_DATE_CLEAN").alias("onset_max_clean"),
).first()
assert _gate["n"] == _gate["k"] == _source_rows
assert int(_gate["patients"]) == 31056
assert int(_gate["patients_with_any_key"]) == 30995
assert float(_gate["patients_with_any_key"]) / float(_gate["patients"]) >= 0.998
assert int(_gate["stage_nonblank"]) == 21286
assert int(_gate["current_rows"]) == 134771
assert str(_gate["onset_min_clean"]).startswith("1951-")
assert str(_gate["onset_max_clean"]).startswith("2023-10-25")
_assert_dq_triplets(TARGET, _temporal_columns)

_status_counts = {
    str(r["PERSON_LINK_STATUS"]): int(r["n"])
    for r in _active.groupBy("PERSON_LINK_STATUS").count().withColumnRenamed("count", "n").collect()
}
_icd_counts = {
    str(r["ICD_MAPPING_STATUS"]): int(r["n"])
    for r in _active.groupBy("ICD_MAPPING_STATUS").count().withColumnRenamed("count", "n").collect()
}
_fingerprint = table_fingerprint(TARGET, exclude=("PIPELINE_UPDT_DT_TM", "ADC_UPDT"))
_record_target_versions(TARGET, _versions)

# COMMAND ----------

# HUMAN-GATED PROMOTION RUNBOOK — comments only; this notebook never writes production.
# 1. Re-pin the frozen source, pt-key, alias, person and OMOP lookup versions; rerun all gates.
# 2. Create 4_prod.bronze.map_aria_diagnosis_staging from reviewed code with retries=0.
# 3. Re-apply comments and both IG tags to every prod column; run ig_tag_gate on prod.
# 4. Keep aria_pt_dx inactive in the canonical CreateWatermarkTable generator because the mirror
#    is frozen. Do not add a weekly bronze step until the source is formally revived.
# 5. Native ARIA radiotherapy remains a separate governance request; this promotion does not imply it.

dbutils.notebook.exit(json.dumps({
    "pipeline": PIPELINE,
    "run_id": RUN_ID,
    "result": "BUILT",
    "target": TARGET,
    "target_schema": TARGET_SCHEMA,
    "source_rows": _source_rows,
    "source_profile": {k: str(v) for k, v in _profile.asDict().items()},
    "gate_profile": {k: str(v) for k, v in _gate.asDict().items()},
    "person_link_status_counts": _status_counts,
    "icd_mapping_status_counts": _icd_counts,
    "temporal_columns": _temporal_columns,
    "fingerprint": str(_fingerprint),
    "source_versions": _versions,
    "frozen_source": "2024-07-22",
    "rt_exploration": "CLOSED_SOURCE_ABSENT_FROM_DW_MIRROR_NATIVE_SYBASE_IS_GOVERNANCE_LANE",
}, sort_keys=True))


