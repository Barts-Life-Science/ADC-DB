# Databricks notebook source
# order_comment_pipeline — S3/A8b.
# Grain: (ORDER_ID, ACTION_SEQUENCE, COMMENT_TYPE_CD), after removing exact ingestion duplicates.
# Every comment type and revision is retained. LATEST_IND is ranked only within the RDE lane:
# type 66 + available text + active long_text. Missing post-stall text is explicit and self-healing.

# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 543833752575381)
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
COMMENT_SOURCE = _widget_text("comment_source", "4_prod.raw.mill_order_comment")
TEXT_SOURCE = _widget_text("text_source", "4_prod.raw.mill_long_text")
assert ACTION in ("pre_gates", "build", "gates")
WATERMARK_TABLE = f"{CONTROL}.s3_watermarks"
TARGET = f"{SCHEMA}.map_order_comment"
PIPE = "a8b_order_comment"

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

COMMENT_BASE=["ORDER_ID","ACTION_SEQUENCE","COMMENT_TYPE_CD","LONG_TEXT_ID","COMMENT_UPDT_DT_TM",
              "COMMENT_UPDT_CNT","COMMENT_DT_TM","SOURCE_COMMENT_ADC_UPDT"]

def run_gates():
    assert spark.catalog.tableExists(TARGET),f"TABLE_OR_VIEW_NOT_FOUND: {TARGET}"
    d=spark.table(TARGET)
    a=d.agg(F.count("*").alias("n"),
            F.countDistinct(F.struct("ORDER_ID","ACTION_SEQUENCE","COMMENT_TYPE_CD")).alias("keys"),
            F.sum((F.col("COMMENT_TYPE_CD")==66).cast("long")).alias("type66"),
            F.sum(((F.col("COMMENT_TYPE_CD")==66)&F.col("TEXT_AVAILABLE_IND")&
                   (F.col("TEXT_ACTIVE_IND")==1)).cast("long")).alias("lane"),
            F.countDistinct(F.when((F.col("COMMENT_TYPE_CD")==66)&F.col("TEXT_AVAILABLE_IND")&
                   (F.col("TEXT_ACTIVE_IND")==1),F.col("ORDER_ID"))).alias("lane_orders"),
            F.sum((~F.col("TEXT_AVAILABLE_IND")).cast("long")).alias("no_text"),
            F.sum(F.col("TEXT_TRUNCATED_IND").cast("long")).alias("truncated")).collect()[0]
    if COMMENT_SOURCE=="8_dev.s3_bronze.fix_mill_order_comment" and TEXT_SOURCE=="8_dev.s3_bronze.fix_mill_long_text":
        assert a["n"]==138066159,f"G1 rows {a['n']}"
        assert a["lane"]==104261112 and a["lane_orders"]==102995105,f"G2 lane {a}"
        assert a["no_text"]==28762054,f"G3 no-text {a['no_text']}"
    assert a["n"]==a["keys"],f"G1 duplicate keys {a}"
    eligible=(d.where((F.col("COMMENT_TYPE_CD")==66)&F.col("TEXT_AVAILABLE_IND")&
                      (F.col("TEXT_ACTIVE_IND")==1))
              .groupBy("ORDER_ID").agg(F.sum(F.col("LATEST_IND").cast("long")).alias("w")))
    assert eligible.where("w<>1").limit(1).count()==0,"G2 latest winner cardinality"
    assert d.where("LATEST_IND AND NOT (COMMENT_TYPE_CD=66 AND TEXT_AVAILABLE_IND AND TEXT_ACTIVE_IND=1)").limit(1).count()==0,         "G2 winner outside parity lane"
    assert d.where("TEXT_AVAILABLE_IND AND COMMENT_TEXT IS NULL").limit(1).count()==0,"G3 available-null"
    for c in ("COMMENT_DT_TM","COMMENT_UPDT_DT_TM","TEXT_UPDT_DT_TM"):
        for s in ("_FUTURE_IND","_SENTINEL_IND","_CLEAN"):
            assert c+s in d.columns,f"G5 missing {c+s}"
    oracle=spark.sql(f"""WITH r AS (
      SELECT ORDER_ID,Comments FROM 4_prod.rde.rde_pharmacyorders
      WHERE Comments IS NOT NULL AND TRIM(Comments)<>'' LIMIT 20000),
    b AS (SELECT ORDER_ID,COMMENT_TEXT FROM {TARGET} WHERE LATEST_IND)
    SELECT COUNT(*) n,
      SUM(CASE WHEN UPPER(TRIM(REGEXP_REPLACE(r.Comments,'[\\s]+',' '))) =
                    UPPER(TRIM(REGEXP_REPLACE(b.COMMENT_TEXT,'[\\s]+',' '))) THEN 1 ELSE 0 END) ok
    FROM r JOIN b USING (ORDER_ID)""").collect()[0]
    assert oracle["n"]>0 and oracle["ok"]/oracle["n"]>=0.99,f"G4 oracle {oracle}"
    print({"rows":a["n"],"lane_rows":a["lane"],"lane_orders":a["lane_orders"],
           "no_text":a["no_text"],"truncated":a["truncated"],"oracle":oracle.asDict()})
    print("A8b gates PASS")

if ACTION=="pre_gates":
    run_gates(); dbutils.notebook.exit("unexpected pre-gate pass")
if ACTION=="gates":
    run_gates(); dbutils.notebook.exit("gates pass")

# COMMAND ----------

cv_table="3_lookup.mill.mill_code_value"
SOURCE_TABLES=[COMMENT_SOURCE,TEXT_SOURCE,cv_table]
CURRENT_SOURCE_VERSIONS=_source_versions(SOURCE_TABLES)
if _target_state_current(TARGET,CURRENT_SOURCE_VERSIONS):
    run_gates()
    dbutils.notebook.exit(json.dumps({
        "result":"NO_OP","target":TARGET,"target_schema":TARGET_SCHEMA,
        "source_versions":CURRENT_SOURCE_VERSIONS,
    },sort_keys=True))

marks={}
com_inc,com_boundary=incr_slice(COMMENT_SOURCE,PIPE,COMMENT_SOURCE)
marks[COMMENT_SOURCE]=wm_get(PIPE,COMMENT_SOURCE)
text_inc,text_boundary=incr_slice(TEXT_SOURCE,PIPE,TEXT_SOURCE)
marks[TEXT_SOURCE]=wm_get(PIPE,TEXT_SOURCE)
cv_inc,cv_boundary=incr_slice(cv_table,PIPE,cv_table)
marks[cv_table]=wm_get(PIPE,cv_table)

inc=(com_inc.select(
 F.col("ORDER_ID").cast("bigint").alias("ORDER_ID"),
 F.col("ACTION_SEQUENCE").cast("bigint").alias("ACTION_SEQUENCE"),
 F.col("COMMENT_TYPE_CD").cast("bigint").alias("COMMENT_TYPE_CD"),
 F.col("LONG_TEXT_ID").cast("bigint").alias("LONG_TEXT_ID"),
 F.col("UPDT_DT_TM").alias("COMMENT_UPDT_DT_TM"),
 F.col("UPDT_CNT").cast("bigint").alias("COMMENT_UPDT_CNT"),
 "COMMENT_DT_TM",F.col("ADC_UPDT").alias("SOURCE_COMMENT_ADC_UPDT"))
 .dropDuplicates())

affected=inc.select("ORDER_ID").union(
 text_inc.where(F.upper(F.trim("PARENT_ENTITY_NAME"))=="ORDER_COMMENT")
 .select(F.col("PARENT_ENTITY_ID").cast("bigint").alias("ORDER_ID"))).distinct()
if spark.catalog.tableExists(TARGET):
    changed_cv=cv_inc.select(F.col("CODE_VALUE").cast("bigint").alias("COMMENT_TYPE_CD")).distinct()
    cv_orders=spark.table(TARGET).join(changed_cv,"COMMENT_TYPE_CD","inner").select("ORDER_ID")
    affected=affected.union(cv_orders).distinct()
    existing=spark.table(TARGET).join(affected,"ORDER_ID","inner").select(*COMMENT_BASE)
    base=existing.unionByName(inc)
else:
    base=inc
base=latest_per_key(base,["ORDER_ID","ACTION_SEQUENCE","COMMENT_TYPE_CD"],[
    F.col("SOURCE_COMMENT_ADC_UPDT").desc_nulls_last(),F.col("COMMENT_UPDT_CNT").desc_nulls_last(),
    F.col("COMMENT_UPDT_DT_TM").desc_nulls_last(),F.col("LONG_TEXT_ID").desc()])

cv=(spark.table(cv_table).select(F.col("CODE_VALUE").cast("bigint").alias("_COMMENT_TYPE_CD"),
                                 F.col("DISPLAY").alias("COMMENT_TYPE_DESC")))
base=base.join(cv,F.col("COMMENT_TYPE_CD")==F.col("_COMMENT_TYPE_CD"),"left").drop("_COMMENT_TYPE_CD")
text=(spark.table(TEXT_SOURCE)
 .where(F.upper(F.trim("PARENT_ENTITY_NAME"))=="ORDER_COMMENT")
 .select(F.col("LONG_TEXT_ID").cast("bigint").alias("_TEXT_ID"),
         F.col("PARENT_ENTITY_ID").cast("bigint").alias("_TEXT_ORDER_ID"),
         F.col("LONG_TEXT").alias("_LONG_TEXT"),F.col("ACTIVE_IND").cast("bigint").alias("TEXT_ACTIVE_IND"),
         F.col("UPDT_DT_TM").alias("TEXT_UPDT_DT_TM"),F.col("UPDT_CNT").cast("bigint").alias("_TEXT_UPDT_CNT"),
         F.col("ADC_UPDT").alias("SOURCE_TEXT_ADC_UPDT")))
text=latest_per_key(text,["_TEXT_ID","_TEXT_ORDER_ID"],[
    F.col("SOURCE_TEXT_ADC_UPDT").desc_nulls_last(),F.col("_TEXT_UPDT_CNT").desc_nulls_last(),
    F.col("TEXT_UPDT_DT_TM").desc_nulls_last()])
base=(base.join(text,(F.col("LONG_TEXT_ID")==F.col("_TEXT_ID"))&
                      (F.col("ORDER_ID")==F.col("_TEXT_ORDER_ID")),"left")
 .withColumn("TEXT_AVAILABLE_IND",F.col("_TEXT_ID").isNotNull())
 .withColumn("TEXT_TRUNCATED_IND",F.length("_LONG_TEXT")>1048576)
 .withColumn("COMMENT_TEXT",F.substring("_LONG_TEXT",1,1048576))
 .drop("_TEXT_ID","_TEXT_ORDER_ID","_LONG_TEXT","_TEXT_UPDT_CNT"))
lane=base.where((F.col("COMMENT_TYPE_CD")==66)&F.col("TEXT_AVAILABLE_IND")&(F.col("TEXT_ACTIVE_IND")==1))
winner=(latest_per_key(lane,["ORDER_ID"],[
    F.col("ACTION_SEQUENCE").desc(),F.col("TEXT_UPDT_DT_TM").desc_nulls_last(),
    F.col("COMMENT_UPDT_DT_TM").desc_nulls_last(),F.col("LONG_TEXT_ID").desc()])
    .select("ORDER_ID","ACTION_SEQUENCE","COMMENT_TYPE_CD").withColumn("LATEST_IND",F.lit(True)))
base=(base.join(winner,["ORDER_ID","ACTION_SEQUENCE","COMMENT_TYPE_CD"],"left")
      .fillna(False,["LATEST_IND","TEXT_AVAILABLE_IND","TEXT_TRUNCATED_IND"]))
base,flagged=dq_all_clinical(base,admin_stamps={"SOURCE_COMMENT_ADC_UPDT","SOURCE_TEXT_ADC_UPDT"})
admin={"SOURCE_COMMENT_ADC_UPDT","SOURCE_TEXT_ADC_UPDT","PIPELINE_UPDT_DT_TM","ROW_HASH"}
hash_cols=[c for c in base.columns if c not in admin and not c.endswith(("_FUTURE_IND","_SENTINEL_IND","_CLEAN"))]
out=(base.withColumn("ROW_HASH",F.xxhash64(F.to_json(F.struct(*[F.col(c) for c in hash_cols]))))
     .withColumn("PIPELINE_UPDT_DT_TM",F.current_timestamp()))
if not spark.catalog.tableExists(TARGET):
    out.limit(0).write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed","true").saveAsTable(TARGET)
metrics=keyed_upsert(TARGET,["ORDER_ID","ACTION_SEQUENCE","COMMENT_TYPE_CD"],out)
print({"affected_orders":affected.count(),"dq_flagged":flagged,"metrics":metrics})
run_gates()
for source,boundary in [(COMMENT_SOURCE,com_boundary),(TEXT_SOURCE,text_boundary),(cv_table,cv_boundary)]:
    old=marks[source]
    if boundary is not None and (old is None or boundary>old):
        wm_set(PIPE,source,boundary)
_record_source_versions(TARGET,CURRENT_SOURCE_VERSIONS)
spark.sql(f"""COMMENT ON TABLE {TARGET} IS
'Grain: one deduplicated (ORDER_ID, ACTION_SEQUENCE, COMMENT_TYPE_CD) source comment. All nine comment types and all revisions are retained. LATEST_IND ranks only type-66 rows whose ORDER_COMMENT long text exists and is active, matching the RDE lane. TEXT_AVAILABLE_IND=false honestly represents missing text, including the mill_long_text feed frozen since 2025-09-23; secondary-source increments self-heal rows when text arrives. Text is capped at 1,048,576 characters with TEXT_TRUNCATED_IND; raw retains full values.'""")
print("A8b build complete")

# COMMAND ----------

# PROMOTION RUNBOOK — HUMAN GATED
# Requalify both shallow clones after any source VACUUM or seven days. Initial build uses approved
# classic compute; set prod target/control/mode and raw source paths. Rerun all prod gates, register
# as an independent Bronze_Pipeline step with retries=0, and record warm-median weekly cost.
# The mill_long_text repair is an operations dependency but does not block honest promotion.

dbutils.notebook.exit(json.dumps({"result": "BUILT", "target": TARGET, "target_schema": TARGET_SCHEMA}, sort_keys=True))


