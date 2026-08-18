# Databricks notebook source
# radiology_event_pipeline — S3/A9.
# Grain: one resolved latest row per EVENT_ID from contributor systems 6141416 and 1198958523.
# Resolution re-reads every raw version for affected events; it never compares an increment
# only with the previously published row. REFERENCE_NBR parsing is lookup-anchored and
# regex-quotes every code, preserving digit-bearing and underscore-bearing codes.

# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 700525436450654)
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

SCHEMA = TARGET_SCHEMA
CONTROL = CONTROL_SCHEMA
MODE = _widget_text("mode", "prod" if TARGET_SCHEMA == "4_prod.bronze" else "dev")
ACTION = _widget_text("action", "build").lower()
EVENT_SOURCE = _widget_text("event_source", "4_prod.raw.mill_clinical_event")
BLOB_SOURCE = _widget_text("blob_source", "4_prod.bronze.mill_blob_text")
_nhsi_default = (
    "3_lookup.dwh.nhsi_exam_mapping"
    if TARGET_SCHEMA == "4_prod.bronze"
    else "8_dev.s4_bronze.nhsi_exam_mapping"
)
NHSI_LOOKUP = _widget_text("nhsi_lookup", _nhsi_default)
assert ACTION in ("fixture", "pre_gates", "build", "gates")
assert not (TARGET_SCHEMA == "4_prod.bronze" and NHSI_LOOKUP.startswith("8_dev.")), (
    "prod may not read staged NHSI lookup")
WATERMARK_TABLE = f"{CONTROL}.s3_watermarks"
TARGET = f"{SCHEMA}.map_radiology_event"
PIPE = "a9_radiology_event"
CONTRIBUTORS = [6141416, 1198958523]

SOURCE_STATE_PROPERTY = "bronze_completeness.source_versions_json"

def _source_versions(sources):
    return {source: int(table_version(source)) for source in sources}

def _target_state_current(target, versions):
    if not spark.catalog.tableExists(target):
        return False
    properties = spark.sql(f"DESCRIBE DETAIL {target}").first()["properties"] or {}
    previous = {k: int(v) for k, v in json.loads(
        properties.get(SOURCE_STATE_PROPERTY, "{}")).items()}
    return previous == versions

def _record_source_versions(target, versions):
    payload = json.dumps(versions, sort_keys=True, separators=(",", ":")).replace("'", "''")
    spark.sql(
        f"ALTER TABLE {target} SET TBLPROPERTIES "
        f"('{SOURCE_STATE_PROPERTY}'='{payload}')"
    )

# COMMAND ----------

# ==== COMMON BLOCK v1 (SYNC-WITH _completeness_common) ====
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

def get_watermark(control_table, source_name, default="1980-01-01"):
    """Per-source watermark (master plan §2.3 rule 5 - one row per source, never GREATEST across sources)."""
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {control_table} (
        source_name STRING, watermark TIMESTAMP, updated_at TIMESTAMP)""")
    rows = spark.sql(f"""SELECT watermark FROM {control_table}
                         WHERE source_name = '{source_name}'""").collect()
    return rows[0]["watermark"] if rows else spark.sql(
        f"SELECT CAST('{default}' AS TIMESTAMP) w").collect()[0]["w"]

def set_watermark(control_table, source_name, new_wm):
    """new_wm must be the SOURCE MAX(ADC_UPDT) observed this run (source-change clock, never build clock)."""
    if new_wm is None:
        return
    spark.sql(f"""MERGE INTO {control_table} t
        USING (SELECT '{source_name}' source_name, CAST('{new_wm}' AS TIMESTAMP) watermark) s
        ON t.source_name = s.source_name
        WHEN MATCHED AND s.watermark > t.watermark
             THEN UPDATE SET t.watermark = s.watermark, t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (source_name, watermark, updated_at)
             VALUES (s.source_name, s.watermark, current_timestamp())""")
# ==== END COMMON BLOCK v1 ====

# COMMAND ----------

# ==== S2 BLOCK v1 (SYNC-WITH _completeness_common) ====
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
# ==== END S2 BLOCK v1 ====

# COMMAND ----------

# ==== S3 BLOCK v1 (SYNC-WITH _completeness_common) ====
from pyspark.sql import functions as F

WATERMARK_TABLE = None  # set by each pipeline's config cell, e.g. "8_dev.bronze.s3_watermarks"

def wm_get(pipeline, source):
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE}
        (pipeline STRING, source STRING, max_adc_updt TIMESTAMP, updated_at TIMESTAMP)""")
    r = spark.sql(f"""SELECT max_adc_updt FROM {WATERMARK_TABLE}
                      WHERE pipeline = '{pipeline}' AND source = '{source}'""").collect()
    return r[0]["max_adc_updt"] if r else None

def wm_set(pipeline, source, new_max):
    # recorded ONLY after a successful publish (retry-safe: a crashed run re-reads the old mark)
    spark.sql(f"""MERGE INTO {WATERMARK_TABLE} t
        USING (SELECT '{pipeline}' pipeline, '{source}' source,
                      TIMESTAMP'{new_max}' max_adc_updt, current_timestamp() updated_at) s
        ON t.pipeline = s.pipeline AND t.source = s.source
        WHEN MATCHED THEN UPDATE SET t.max_adc_updt = s.max_adc_updt, t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *""")

def incr_slice(source_table, pipeline, source_key, lookback_hours=24):
    """Watermark increment on the SOURCE arrival clock (ADC_UPDT), per §2.3 rule 5:
    per-source marks, never GREATEST across sources; lookback absorbs late stragglers
    (MERGE idempotence makes re-reads harmless). First run (no mark) = full source.
    BOUNDED: the slice is pinned to a run-start snapshot (Delta VERSION AS OF) and an
    upper ADC_UPDT boundary computed ON that snapshot — rows arriving between Spark
    actions cannot fall between the slice and the committed mark. Returns (df, boundary);
    the caller passes EXACTLY this boundary to wm_set after publish."""
    v = spark.sql(f"DESCRIBE HISTORY {source_table} LIMIT 1").collect()[0]["version"]
    snap = spark.read.option("versionAsOf", v).table(source_table)
    boundary = snap.agg(F.max("ADC_UPDT")).collect()[0][0]
    wm = wm_get(pipeline, source_key)
    df = snap
    if wm is not None:
        df = df.where(F.col("ADC_UPDT") > F.expr(f"TIMESTAMP'{wm}' - INTERVAL {lookback_hours} HOURS"))
    if boundary is not None:
        df = df.where(F.col("ADC_UPDT") <= F.lit(boundary))
    return df, boundary

# SECONDARY-SOURCE DOCTRINE (binding for every S3 pipeline): any published column whose
# value depends on a table OTHER than the primary source (med-family membership, blob/report
# children, course attributes, map links) gets its OWN wm_get/wm_set checkpoint; each run's
# work set = primary increment ∪ AFFECTED KEYS derived from every secondary increment
# (e.g. parents of changed children, cycles of changed courses). A secondary change with no
# primary change must still rewrite the affected primary rows — the ROW_HASH guard keeps
# the rewrite cheap and idempotent.

def keyed_upsert(target, key_cols, df):
    """ROW_HASH-guarded keyed MERGE: one code path for initial build (empty target)
    and weekly increments; unchanged rows are never rewritten (§2.3 rule 3).
    df MUST already carry ROW_HASH = xxhash64(to_json(struct(<all published non-admin cols>)))."""
    df.createOrReplaceTempView("s3_upsert_src")
    on = " AND ".join(f"t.{c} = s.{c}" for c in key_cols)
    spark.sql(f"""MERGE INTO {target} t USING s3_upsert_src s ON {on}
        WHEN MATCHED AND t.ROW_HASH <> s.ROW_HASH THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")
    return spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").collect()[0]["operationMetrics"]

def latest_per_key(df, key_cols, order_exprs):
    """Deterministic latest-row-per-key: order_exprs is a list of Column expressions
    ALREADY carrying desc()/asc() + NULLS LAST (house dedup rule)."""
    from pyspark.sql.window import Window
    w = Window.partitionBy(*key_cols).orderBy(*order_exprs)
    return (df.withColumn("_rn", F.row_number().over(w))
              .where(F.col("_rn") == 1).drop("_rn"))
# ==== END S3 BLOCK v1 ====
# Pipeline config must override the shared block placeholder.
WATERMARK_TABLE = f"{CONTROL}.s3_watermarks"

# COMMAND ----------

from pyspark.sql import functions as F

if ACTION=="fixture":
    spark.sql("DROP TABLE IF EXISTS 8_dev.s3_bronze.fix_ris")
    spark.sql("""CREATE TABLE 8_dev.s3_bronze.fix_ris AS
      SELECT * FROM 4_prod.raw.mill_clinical_event VERSION AS OF 6656
      WHERE CAST(CONTRIBUTOR_SYSTEM_CD AS BIGINT) IN (6141416,1198958523)""")
    r=spark.sql("""SELECT COUNT(*) n,COUNT(DISTINCT CAST(EVENT_ID AS BIGINT)) d,
                   MAX(ADC_UPDT) max_adc FROM 8_dev.s3_bronze.fix_ris""").collect()[0]
    assert r["n"]==41231835 and r["d"]==33818713
    print(r.asDict());dbutils.notebook.exit("fixture ready")

def pinned_event_slice():
    v=table_version(EVENT_SOURCE)
    snap=spark.read.option("versionAsOf",v).table(EVENT_SOURCE)
    snap=snap.where(F.col("CONTRIBUTOR_SYSTEM_CD").cast("bigint").isin(CONTRIBUTORS))
    boundary=snap.agg(F.max("ADC_UPDT")).collect()[0][0]
    wm=wm_get(PIPE,EVENT_SOURCE)
    inc=snap
    if wm is not None:
        inc=inc.where(F.col("ADC_UPDT")>F.expr(f"TIMESTAMP'{wm}' - INTERVAL 24 HOURS"))
    if boundary is not None:
        inc=inc.where(F.col("ADC_UPDT")<=F.lit(boundary))
    return snap,inc,boundary,v

def incr_slice_clock(source_table,clock_col):
    v=table_version(source_table);snap=spark.read.option("versionAsOf",v).table(source_table)
    boundary=snap.agg(F.max(clock_col)).collect()[0][0];wm=wm_get(PIPE,source_table);inc=snap
    if wm is not None:
        inc=inc.where(F.col(clock_col)>F.expr(f"TIMESTAMP'{wm}' - INTERVAL 24 HOURS"))
    if boundary is not None:inc=inc.where(F.col(clock_col)<=F.lit(boundary))
    return inc,boundary

def run_gates():
    assert spark.catalog.tableExists(TARGET),f"TABLE_OR_VIEW_NOT_FOUND: {TARGET}"
    d=spark.table(TARGET)
    a=d.agg(F.count("*").alias("n"),F.countDistinct("EVENT_ID").alias("du"),
            F.sum((F.col("VERSION_STATUS")=="MULTI_OPEN_RESOLVED").cast("long")).alias("multi"),
            F.sum((F.col("VERSION_STATUS")=="CLOSED_ONLY").cast("long")).alias("closed"),
            F.sum((F.col("IN_ERROR_IND")).cast("long")).alias("in_error")).collect()[0]
    if EVENT_SOURCE=="8_dev.s3_bronze.fix_ris":
        assert a["n"]==33818713 and a["multi"]==985 and a["closed"]==45184,f"G1 {a}"
    assert a["n"]==a["du"],f"G1 duplicate EVENT_ID {a}"
    class234=d.where("EVENT_CLASS_CD=234")
    parse=class234.where("EXAM_TYPE_CODE IS NOT NULL").count()/max(class234.count(),1)
    assert parse>=0.85,f"G2 parse coverage {parse}"
    assert d.where("EXAM_TYPE_CODE IS NOT NULL AND NHSI_MODALITY_CATEGORY IS NULL").limit(1).count()==0,         "G2 parsed code absent lookup"
    c224=d.where("EVENT_CLASS_CD=224")
    blob_rate=c224.where("BLOB_TEXT_IND").count()/max(c224.count(),1)
    assert blob_rate>=0.970,f"G3 DOC blob rate {blob_rate}"
    assert d.where("EVENT_CLASS_CD<>224 AND BLOB_TEXT_IND").count()/max(d.where("EVENT_CLASS_CD<>224").count(),1)<0.001,         "G3 unexpected non-DOC blobs"
    assert d.where("REPORT_LINK_STATUS='LINKED' AND RADIOLOGY_REPORT_EVENT_ID IS NULL").limit(1).count()==0
    assert d.where("RESULT_STATUS_DESC IS NULL").limit(1).count()==0,"G5 result-status decode"
    assert 0<a["in_error"]<=147,f"G5 in-error scale {a['in_error']}"
    for c in ("EVENT_START_DT_TM","EVENT_END_DT_TM","PERFORMED_DT_TM"):
        for s in ("_FUTURE_IND","_SENTINEL_IND","_CLEAN"):
            assert c+s in d.columns,f"G6 missing {c+s}"
    person=spark.sql(f"""
      WITH horizon AS (SELECT MAX(ADC_UPDT) h FROM 4_prod.bronze.map_person),
      sampled AS (
        SELECT PERSON_ID, MIN(SOURCE_ADC_UPDT) first_event_adc
        FROM {TARGET}
        WHERE PERSON_ID IS NOT NULL AND pmod(xxhash64(PERSON_ID),6)=0
        GROUP BY PERSON_ID
      )
      SELECT COUNT(*) n,
             SUM(CASE WHEN p.PERSON_ID IS NOT NULL THEN 1 ELSE 0 END) ok,
             SUM(CASE WHEN x.first_event_adc <= h THEN 1 ELSE 0 END) synchronized_n,
             SUM(CASE WHEN x.first_event_adc <= h AND p.PERSON_ID IS NOT NULL THEN 1 ELSE 0 END) synchronized_ok,
             MAX(h) map_person_horizon
      FROM sampled x CROSS JOIN horizon
      LEFT JOIN 4_prod.bronze.map_person p USING (PERSON_ID)
    """).collect()[0]
    assert person["ok"]/person["n"]>=0.992,f"G4 overall person linkage {person}"
    assert person["synchronized_ok"]/person["synchronized_n"]>=0.993,(
        f"G4 synchronized person linkage {person}")
    orders=spark.sql(f"""SELECT COUNT(*) n,SUM(CASE WHEN o.ORDER_ID IS NOT NULL THEN 1 ELSE 0 END) ok
      FROM (SELECT ORDER_ID FROM {TARGET} WHERE ORDER_ID IS NOT NULL AND pmod(xxhash64(ORDER_ID),331)=0) x
      LEFT JOIN 4_prod.raw.mill_orders o ON x.ORDER_ID=CAST(o.ORDER_ID AS BIGINT)""").collect()[0]
    assert orders["ok"]/orders["n"]>=0.996,f"G4 order linkage {orders}"
    print({"rows":a["n"],"multi_open":a["multi"],"closed_only":a["closed"],"parse_234":parse,
           "doc_blob_rate":blob_rate,"in_error":a["in_error"],"person_link":person.asDict(),
           "order_link":orders.asDict()})
    print("A9 gates PASS")

if ACTION=="pre_gates":
    run_gates();dbutils.notebook.exit("unexpected pre-gate pass")
if ACTION=="gates":
    run_gates();dbutils.notebook.exit("gates pass")

# COMMAND ----------

cv_table="3_lookup.mill.mill_code_value"
SOURCE_TABLES=[EVENT_SOURCE,BLOB_SOURCE,NHSI_LOOKUP,cv_table]
CURRENT_SOURCE_VERSIONS=_source_versions(SOURCE_TABLES)
if _target_state_current(TARGET,CURRENT_SOURCE_VERSIONS):
    run_gates()
    dbutils.notebook.exit(json.dumps({
        "result":"NO_OP","target":TARGET,"target_schema":TARGET_SCHEMA,
        "source_versions":CURRENT_SOURCE_VERSIONS,
    },sort_keys=True))

marks={}
snap,event_inc,event_boundary,event_version=pinned_event_slice()
marks[EVENT_SOURCE]=wm_get(PIPE,EVENT_SOURCE)
blob_inc,blob_boundary=incr_slice(BLOB_SOURCE,PIPE,BLOB_SOURCE)
marks[BLOB_SOURCE]=wm_get(PIPE,BLOB_SOURCE)
lookup_inc,lookup_boundary=incr_slice_clock(NHSI_LOOKUP,"SOURCE_LOADED_AT")
marks[NHSI_LOOKUP]=wm_get(PIPE,NHSI_LOOKUP)
cv_inc,cv_boundary=incr_slice(cv_table,PIPE,cv_table)
marks[cv_table]=wm_get(PIPE,cv_table)

inc_keys=event_inc.select(F.col("EVENT_ID").cast("bigint").alias("EVENT_ID")).distinct()
parent_keys=(event_inc.where(F.col("EVENT_CLASS_CD").cast("bigint").isin([224,231]))
 .select(F.abs(F.col("PARENT_EVENT_ID").cast("bigint")).alias("EVENT_ID")).where("EVENT_ID IS NOT NULL").distinct())
blob_keys=blob_inc.select(F.col("EVENT_ID").cast("bigint").alias("EVENT_ID")).distinct()
affected=inc_keys.union(parent_keys).union(blob_keys).distinct()
if spark.catalog.tableExists(TARGET):
    old_parents=(spark.table(TARGET).join(inc_keys,"EVENT_ID","inner")
                 .where(F.col("EVENT_CLASS_CD").isin([224,231]))
                 .select(F.col("PARENT_EVENT_ID_NORM").alias("EVENT_ID")).where("EVENT_ID IS NOT NULL"))
    affected=affected.union(old_parents).distinct()
    if lookup_inc.limit(1).count()>0:
        affected=affected.union(spark.table(TARGET).select("EVENT_ID")).distinct()
    changed_cv=cv_inc.select(F.col("CODE_VALUE").cast("bigint").alias("_CD")).distinct()
    cv_events=(spark.table(TARGET).join(changed_cv,
      (F.col("CONTRIBUTOR_SYSTEM_CD")==F.col("_CD"))|(F.col("EVENT_CLASS_CD")==F.col("_CD"))|
      (F.col("EVENT_CD")==F.col("_CD"))|(F.col("RESULT_STATUS_CD")==F.col("_CD")),"inner")
      .select("EVENT_ID"))
    affected=affected.union(cv_events).distinct()

raw=(snap.select(
 F.col("EVENT_ID").cast("bigint").alias("EVENT_ID"),
 F.col("PARENT_EVENT_ID").cast("bigint").alias("PARENT_EVENT_ID"),
 F.col("PERSON_ID").cast("bigint").alias("PERSON_ID"),
 F.col("ENCNTR_ID").cast("bigint").alias("ENCNTR_ID"),
 F.when(F.col("ORDER_ID").cast("bigint")==0,F.lit(None)).otherwise(F.col("ORDER_ID").cast("bigint")).alias("ORDER_ID"),
 F.col("CONTRIBUTOR_SYSTEM_CD").cast("bigint").alias("CONTRIBUTOR_SYSTEM_CD"),
 F.col("EVENT_CLASS_CD").cast("bigint").alias("EVENT_CLASS_CD"),
 F.col("EVENT_CD").cast("bigint").alias("EVENT_CD"),
 "EVENT_TAG","EVENT_TITLE_TEXT","REFERENCE_NBR","EVENT_START_DT_TM","EVENT_END_DT_TM","PERFORMED_DT_TM",
 F.col("RESULT_STATUS_CD").cast("bigint").alias("RESULT_STATUS_CD"),
 "VALID_FROM_DT_TM","VALID_UNTIL_DT_TM",F.col("UPDT_CNT").cast("bigint").alias("UPDT_CNT"),
 "UPDT_DT_TM",F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"))
 .join(affected,"EVENT_ID","inner")
 .withColumn("_OPEN",(F.col("VALID_UNTIL_DT_TM")>=F.lit("2035-01-01").cast("timestamp")).cast("int")))
open_counts=raw.groupBy("EVENT_ID").agg(F.sum("_OPEN").alias("_OPEN_N"))
resolved=(latest_per_key(raw,["EVENT_ID"],[
 F.col("_OPEN").desc(),F.col("VALID_UNTIL_DT_TM").desc_nulls_last(),F.col("UPDT_CNT").desc_nulls_last(),
 F.col("UPDT_DT_TM").desc_nulls_last()])
 .join(open_counts,"EVENT_ID","left")
 .withColumn("VERSION_STATUS",F.when(F.col("_OPEN_N")==0,"CLOSED_ONLY")
             .when(F.col("_OPEN_N")>1,"MULTI_OPEN_RESOLVED").otherwise("OPEN"))
 .withColumn("PARENT_EVENT_ID_NORM",F.abs("PARENT_EVENT_ID"))
 .drop("_OPEN","_OPEN_N"))

cv=spark.table(cv_table).select(F.col("CODE_VALUE").cast("bigint").alias("_CD"),F.col("DISPLAY").alias("_DESC"))
for c,o in [("CONTRIBUTOR_SYSTEM_CD","CONTRIBUTOR_SYSTEM_DESC"),("EVENT_CLASS_CD","EVENT_CLASS_DESC"),
            ("EVENT_CD","EVENT_DESC"),("RESULT_STATUS_CD","RESULT_STATUS_DESC")]:
    lk=cv.withColumnRenamed("_CD",f"_{o}_CD").withColumnRenamed("_DESC",o)
    resolved=resolved.join(lk,F.col(c)==F.col(f"_{o}_CD"),"left").drop(f"_{o}_CD")
resolved=resolved.withColumn("IN_ERROR_IND",F.col("RESULT_STATUS_CD")==31)

# Exact suffix parser: lookup codes have only six lengths. Generate 0/1 trailing-sequence
# variants only for class-234 exam events, then use one broadcast equality join.
_code_lengths=sorted(
    [int(r["L"]) for r in spark.table(NHSI_LOOKUP)
     .select(F.length("EXAM_TYPE_CODE").alias("L")).distinct().collect()],
    reverse=True,
)
lk=F.broadcast(spark.table(NHSI_LOOKUP).select(
    F.upper("EXAM_TYPE_CODE").alias("_CODE"),F.col("NHSI_MAPPING").alias("_NHSI")))
exam=resolved.where(F.col("EVENT_CLASS_CD")==234).select(
    "EVENT_ID",F.upper("REFERENCE_NBR").alias("_REF"))
_candidate_structs=[
    F.struct(F.lit(length).alias("_LEN"),F.lit(trail).alias("_TRAIL"),
             F.substring(F.col("_REF"),F.length("_REF")-F.lit(trail+length)+F.lit(1),length).alias("_CAND"))
    for length in _code_lengths for trail in (0,1)
]
matches=(exam.withColumn("_C",F.explode(F.array(*_candidate_structs)))
         .select("EVENT_ID",F.col("_C._LEN").alias("_LEN"),F.col("_C._TRAIL").alias("_TRAIL"),
                 F.col("_C._CAND").alias("_CAND"))
         .join(lk,F.col("_CAND")==F.col("_CODE"),"inner"))
exam_map=(latest_per_key(matches,["EVENT_ID"],[
            F.col("_LEN").desc(),F.col("_TRAIL").asc(),F.col("_CODE").asc()])
          .select("EVENT_ID",F.col("_CODE").alias("EXAM_TYPE_CODE"),
                  F.col("_NHSI").alias("NHSI_MODALITY_CATEGORY"))
          .withColumn("EXAM_CODE_PARSE_METHOD",F.lit("CODE_TAIL_MATCH")))
best=(resolved.join(exam_map,"EVENT_ID","left")
      .withColumn("EXAM_CODE_PARSE_METHOD",
                  F.coalesce("EXAM_CODE_PARSE_METHOD",F.lit("NONE"))))

blob_keys_all=spark.table(BLOB_SOURCE).select(F.col("EVENT_ID").cast("bigint").alias("_BLOB_EVENT_ID")).distinct()
best=(best.join(blob_keys_all,F.col("EVENT_ID")==F.col("_BLOB_EVENT_ID"),"left")
      .withColumn("BLOB_TEXT_IND",F.col("_BLOB_EVENT_ID").isNotNull()).drop("_BLOB_EVENT_ID"))

parent_events=best.where("EVENT_CLASS_CD=234").select(F.col("EVENT_ID").alias("_PARENT")).distinct()
new_children=(best.where("EVENT_CLASS_CD=224")
              .select(F.col("PARENT_EVENT_ID_NORM").alias("_PARENT"),
                      F.col("EVENT_ID").alias("_REPORT")))
if spark.catalog.tableExists(TARGET):
    old_children=(spark.table(TARGET).where("EVENT_CLASS_CD=224")
                  .select(F.col("PARENT_EVENT_ID_NORM").alias("_PARENT"),
                          F.col("EVENT_ID").alias("_REPORT")).join(parent_events,"_PARENT","inner"))
    children=old_children.unionByName(new_children)
else:
    children=new_children
report=(children.groupBy("_PARENT").agg(F.countDistinct("_REPORT").alias("_REPORT_N"),
                                        F.min("_REPORT").alias("_ONLY_REPORT")))
best=(best.join(report,F.col("EVENT_ID")==F.col("_PARENT"),"left")
 .withColumn("RADIOLOGY_REPORT_EVENT_ID",
   F.when((F.col("EVENT_CLASS_CD")==234)&(F.col("_REPORT_N")==1),F.col("_ONLY_REPORT")))
 .withColumn("REPORT_LINK_STATUS",
   F.when(F.col("EVENT_CLASS_CD")!=234,"NONE")
    .when(F.col("_REPORT_N")==1,"LINKED").when(F.col("_REPORT_N")>1,"AMBIGUOUS").otherwise("NONE"))
 .drop("_PARENT","_REPORT_N","_ONLY_REPORT"))
best,flagged=dq_all_clinical(best,admin_stamps={"UPDT_DT_TM","SOURCE_ADC_UPDT"})
admin={"UPDT_DT_TM","SOURCE_ADC_UPDT","PIPELINE_UPDT_DT_TM","ROW_HASH"}
hash_cols=[c for c in best.columns if c not in admin and not c.endswith(("_FUTURE_IND","_SENTINEL_IND","_CLEAN"))]
out=(best.withColumn("ROW_HASH",F.xxhash64(F.to_json(F.struct(*[F.col(c) for c in hash_cols]))))
     .withColumn("PIPELINE_UPDT_DT_TM",F.current_timestamp()))
if not spark.catalog.tableExists(TARGET):
    out.limit(0).write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed","true").saveAsTable(TARGET)
metrics=keyed_upsert(TARGET,["EVENT_ID"],out)
unmatched=(spark.table(TARGET).where("EVENT_CLASS_CD=234 AND EXAM_TYPE_CODE IS NULL")
           .groupBy(F.regexp_extract(F.upper("REFERENCE_NBR"),"([A-Z0-9_]+)[0-9]*$",1).alias("STEM"))
           .count().orderBy(F.desc("count")).limit(30).collect())
print({"event_source_version":event_version,"affected_events":affected.count(),"dq_flagged":flagged,
       "metrics":metrics,"unmatched_head":[r.asDict() for r in unmatched]})
run_gates()
for source,boundary in [(EVENT_SOURCE,event_boundary),(BLOB_SOURCE,blob_boundary),
                        (NHSI_LOOKUP,lookup_boundary),(cv_table,cv_boundary)]:
    old=marks[source]
    if boundary is not None and (old is None or boundary>old):wm_set(PIPE,source,boundary)
_record_source_versions(TARGET,CURRENT_SOURCE_VERSIONS)
spark.sql(f"""COMMENT ON TABLE {TARGET} IS
'Grain: one latest resolved EVENT_ID from BLT_TIE_RAD plus BHR_TIE_SECTRA_RAD. Resolution prefers open versions and deterministically handles multi-open and closed-only events. REFERENCE_NBR is matched against regex-quoted NHSI codes plus optional trailing sequence digits; no code is invented. Report links use DOC child PARENT_EVENT_ID and record ambiguity. In-error results and implausible dates are retained and flagged. No EVENT_STATUS_CD exists; RESULT_STATUS_CD is the status source.'""")
print("A9 build complete")

# COMMAND ----------

# PROMOTION RUNBOOK — HUMAN GATED
# Production precondition: publish 3_lookup.dwh.nhsi_exam_mapping; never use the dev staging table.
# Initial build uses approved classic compute with contributor filtering first. Set prod target/control,
# mode=prod, raw event/blob sources and the production lookup. Rerun all gates, register with retries=0,
# and record the warm-median weekly cost. NICIP concept columns remain a follow-up after S4 Task 5b.

dbutils.notebook.exit(json.dumps({"result": "BUILT", "target": TARGET, "target_schema": TARGET_SCHEMA}, sort_keys=True))


