# Databricks notebook source
# orders_spine_pipeline — S3/A8a.
# Grain: one ORDER_ID. Lossless generic Millennium orders spine with a medication-family
# membership pointer. MED_FAMILY_IND is an actual map_medication_order membership test,
# never ACTIVITY_TYPE_CD=705. Display-line, action/review/verify, protocol/template,
# and cost/billing families are named exclusions.

# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 1008566672745369)
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
ORDERS_SOURCE = _widget_text("orders_source", "4_prod.raw.mill_orders")
MED_SOURCE = _widget_text("med_source", "4_prod.bronze.map_medication_order")
assert ACTION in ("pre_gates", "build", "gates")
WATERMARK_TABLE = f"{CONTROL}.s3_watermarks"
TARGET = f"{SCHEMA}.map_orders"
PIPE = "a8a_orders_spine"

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

def incr_slice_clock(source_table,pipeline,source_key,clock_col,lookback_hours=24):
    v=spark.sql(f"DESCRIBE HISTORY {source_table} LIMIT 1").collect()[0]["version"]
    snap=spark.read.option("versionAsOf",v).table(source_table)
    boundary=snap.agg(F.max(clock_col)).collect()[0][0]
    wm=wm_get(pipeline,source_key)
    df=snap
    if wm is not None:
        df=df.where(F.col(clock_col)>F.expr(f"TIMESTAMP'{wm}' - INTERVAL {lookback_hours} HOURS"))
    if boundary is not None:
        df=df.where(F.col(clock_col)<=F.lit(boundary))
    return df,boundary

BASE_COLS=["ORDER_ID","PERSON_ID","ENCNTR_ID","ACTIVITY_TYPE_CD","CATALOG_CD","CATALOG_TYPE_CD",
           "ORDER_MNEMONIC","HNA_ORDER_MNEMONIC","ORDERED_AS_MNEMONIC","ORDER_STATUS_CD",
           "DEPT_STATUS_CD","ACTIVE_IND","ORIG_ORDER_DT_TM","CURRENT_START_DT_TM","STATUS_DT_TM",
           "PROJECTED_STOP_DT_TM","DISCONTINUE_EFFECTIVE_DT_TM","SOURCE_ADC_UPDT"]

def run_gates():
    assert spark.catalog.tableExists(TARGET),f"TABLE_OR_VIEW_NOT_FOUND: {TARGET}"
    d=spark.table(TARGET)
    agg=d.agg(F.count("*").alias("n"),F.countDistinct("ORDER_ID").alias("du"),
              F.sum(F.col("MED_FAMILY_IND").cast("long")).alias("med"),
              F.sum(((F.col("ACTIVITY_TYPE_CD")==705)&(~F.col("MED_FAMILY_IND"))).cast("long")).alias("pharm_gap"),
              F.sum(F.col("ORIG_ORDER_DT_TM_FUTURE_IND").cast("long")).alias("future"),
              F.sum(F.col("ORIG_ORDER_DT_TM_SENTINEL_IND").cast("long")).alias("sentinel")).collect()[0]
    assert agg["n"]>=637536104 and agg["n"]==agg["du"],f"G1 rows/key {agg}"
    med_n=spark.table(MED_SOURCE).select(F.col("ORDER_ID").cast("bigint")).distinct().count()
    assert agg["med"]==med_n,f"G2 med pointer {agg['med']} != {med_n}"
    if ORDERS_SOURCE=="8_dev.s3_bronze.fix_mill_orders" and MED_SOURCE=="8_dev.s3_bronze.fix_map_medication_order":
        assert agg["pharm_gap"]==392640,f"G2 baseline pharmacy gap {agg['pharm_gap']}"
    for c in ("ACTIVITY_TYPE_DESC","ORDER_STATUS_DESC","DEPT_STATUS_DESC","CATALOG_DISPLAY","CATALOG_TYPE_DESC"):
        cov=d.where(F.col(c).isNotNull()).count()/agg["n"]
        assert cov>=0.9999,f"G3 {c} coverage {cov}"
    for c in ("ORIG_ORDER_DT_TM","CURRENT_START_DT_TM","STATUS_DT_TM","PROJECTED_STOP_DT_TM",
              "DISCONTINUE_EFFECTIVE_DT_TM"):
        for s in ("_FUTURE_IND","_SENTINEL_IND","_CLEAN"):
            assert c+s in d.columns,f"G4 missing {c+s}"
    # Bounded RDE semantics: resolve sampled event ORDER_IDs and compare decoded status text.
    path=spark.sql(f"""SELECT COUNT(*) n,
      SUM(CASE WHEN UPPER(TRIM(r.OrderStatus))=UPPER(TRIM(o.ORDER_STATUS_DESC)) THEN 1 ELSE 0 END) ok
      FROM (SELECT EventID,OrderStatus FROM 4_prod.rde.rde_pathology WHERE OrderStatus IS NOT NULL LIMIT 10000) r
      JOIN 4_prod.raw.mill_clinical_event e ON r.EventID=CAST(e.EVENT_ID AS BIGINT)
      JOIN {TARGET} o ON CAST(e.ORDER_ID AS BIGINT)=o.ORDER_ID""").collect()[0]
    rad=spark.sql(f"""SELECT COUNT(*) n,
      SUM(CASE WHEN UPPER(TRIM(r.LastOrderStatus))=UPPER(TRIM(o.ORDER_STATUS_DESC)) THEN 1 ELSE 0 END) ok
      FROM (SELECT EventID,LastOrderStatus FROM 4_prod.rde.rde_radiology WHERE LastOrderStatus IS NOT NULL LIMIT 10000) r
      JOIN 4_prod.raw.mill_clinical_event e ON r.EventID=CAST(e.EVENT_ID AS BIGINT)
      JOIN {TARGET} o ON CAST(e.ORDER_ID AS BIGINT)=o.ORDER_ID""").collect()[0]
    assert path["n"]>0 and path["ok"]/path["n"]>=0.99,f"G6 pathology status {path}"
    assert rad["n"]>0 and rad["ok"]/rad["n"]>=0.99,f"G6 radiology status {rad}"
    print({"target":TARGET,"rows":agg["n"],"med_family":agg["med"],"pharmacy_705_not_med":agg["pharm_gap"],
           "future_orig":agg["future"],"sentinel_orig":agg["sentinel"],
           "rde_pathology":path.asDict(),"rde_radiology":rad.asDict()})
    print("A8a gates PASS")

if ACTION=="pre_gates":
    run_gates(); dbutils.notebook.exit("unexpected pre-gate pass")
if ACTION=="gates":
    run_gates(); dbutils.notebook.exit("gates pass")

# COMMAND ----------

catalog_table="3_lookup.mill.mill_order_catalog"
cv_table="3_lookup.mill.mill_code_value"
SOURCE_TABLES=[ORDERS_SOURCE,MED_SOURCE,catalog_table,cv_table]
CURRENT_SOURCE_VERSIONS=_source_versions(SOURCE_TABLES)
if _target_state_current(TARGET,CURRENT_SOURCE_VERSIONS):
    run_gates()
    dbutils.notebook.exit(json.dumps({
        "result":"NO_OP","target":TARGET,"target_schema":TARGET_SCHEMA,
        "source_versions":CURRENT_SOURCE_VERSIONS,
    },sort_keys=True))

marks={}
orders_inc,orders_boundary=incr_slice(ORDERS_SOURCE,PIPE,ORDERS_SOURCE)
marks[ORDERS_SOURCE]=wm_get(PIPE,ORDERS_SOURCE)
med_clock="SOURCE_ADC_UPDT" if "SOURCE_ADC_UPDT" in spark.table(MED_SOURCE).columns else "ADC_UPDT"
med_inc,med_boundary=incr_slice_clock(MED_SOURCE,PIPE,MED_SOURCE,med_clock)
marks[MED_SOURCE]=wm_get(PIPE,MED_SOURCE)
cat_inc,cat_boundary=incr_slice(catalog_table,PIPE,catalog_table)
marks[catalog_table]=wm_get(PIPE,catalog_table)
cv_inc,cv_boundary=incr_slice(cv_table,PIPE,cv_table)
marks[cv_table]=wm_get(PIPE,cv_table)

inc=(orders_inc.select(
 F.col("ORDER_ID").cast("bigint").alias("ORDER_ID"),
 F.col("PERSON_ID").cast("bigint").alias("PERSON_ID"),
 F.col("ENCNTR_ID").cast("bigint").alias("ENCNTR_ID"),
 F.col("ACTIVITY_TYPE_CD").cast("bigint").alias("ACTIVITY_TYPE_CD"),
 F.col("CATALOG_CD").cast("bigint").alias("CATALOG_CD"),
 F.col("CATALOG_TYPE_CD").cast("bigint").alias("CATALOG_TYPE_CD"),
 "ORDER_MNEMONIC","HNA_ORDER_MNEMONIC","ORDERED_AS_MNEMONIC",
 F.col("ORDER_STATUS_CD").cast("bigint").alias("ORDER_STATUS_CD"),
 F.col("DEPT_STATUS_CD").cast("bigint").alias("DEPT_STATUS_CD"),
 F.col("ACTIVE_IND").cast("bigint").alias("ACTIVE_IND"),
 "ORIG_ORDER_DT_TM","CURRENT_START_DT_TM","STATUS_DT_TM","PROJECTED_STOP_DT_TM",
 "DISCONTINUE_EFFECTIVE_DT_TM",F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT")))

affected=inc.select("ORDER_ID").union(
 med_inc.select(F.col("ORDER_ID").cast("bigint").alias("ORDER_ID"))).distinct()
if spark.catalog.tableExists(TARGET):
    cat_codes=cat_inc.select(F.col("CATALOG_CD").cast("bigint").alias("CATALOG_CD")).distinct()
    cat_orders=spark.table(TARGET).join(cat_codes,"CATALOG_CD","inner").select("ORDER_ID")
    changed_cv=cv_inc.select(F.col("CODE_VALUE").cast("bigint").alias("_CD")).distinct()
    cv_orders=(spark.table(TARGET).join(changed_cv,
       (F.col("ACTIVITY_TYPE_CD")==F.col("_CD"))|(F.col("ORDER_STATUS_CD")==F.col("_CD"))|
       (F.col("DEPT_STATUS_CD")==F.col("_CD"))|(F.col("CATALOG_TYPE_CD")==F.col("_CD")),"inner")
       .select("ORDER_ID"))
    affected=affected.union(cat_orders).union(cv_orders).distinct()
    existing=spark.table(TARGET).join(affected,"ORDER_ID","inner").select(*BASE_COLS)
    base=existing.unionByName(inc)
else:
    base=inc
base=latest_per_key(base,["ORDER_ID"],[F.col("SOURCE_ADC_UPDT").desc_nulls_last()])

catalog=(spark.table(catalog_table)
 .select(F.col("CATALOG_CD").cast("bigint").alias("_CATALOG_CD"),
         F.col("DESCRIPTION").alias("CATALOG_DISPLAY")))
base=base.join(catalog,F.col("CATALOG_CD")==F.col("_CATALOG_CD"),"left").drop("_CATALOG_CD")
cv=spark.table(cv_table).select(F.col("CODE_VALUE").cast("bigint").alias("_CD"),F.col("DISPLAY").alias("_DESC"))
for code_col,out_col in [("ACTIVITY_TYPE_CD","ACTIVITY_TYPE_DESC"),("ORDER_STATUS_CD","ORDER_STATUS_DESC"),
                         ("DEPT_STATUS_CD","DEPT_STATUS_DESC"),("CATALOG_TYPE_CD","CATALOG_TYPE_DESC")]:
    lk=cv.withColumnRenamed("_CD",f"_{out_col}_CD").withColumnRenamed("_DESC",out_col)
    base=base.join(lk,F.col(code_col)==F.col(f"_{out_col}_CD"),"left").drop(f"_{out_col}_CD")
med_keys=spark.table(MED_SOURCE).select(F.col("ORDER_ID").cast("bigint").alias("_MED_ORDER_ID")).distinct()
base=(base.join(med_keys,F.col("ORDER_ID")==F.col("_MED_ORDER_ID"),"left")
      .withColumn("MED_FAMILY_IND",F.col("_MED_ORDER_ID").isNotNull()).drop("_MED_ORDER_ID"))
base,flagged=dq_all_clinical(base,admin_stamps={"SOURCE_ADC_UPDT"})
admin={"SOURCE_ADC_UPDT","PIPELINE_UPDT_DT_TM","ROW_HASH"}
hash_cols=[c for c in base.columns if c not in admin and not c.endswith(("_FUTURE_IND","_SENTINEL_IND","_CLEAN"))]
out=(base.withColumn("ROW_HASH",F.xxhash64(F.to_json(F.struct(*[F.col(c) for c in hash_cols]))))
     .withColumn("PIPELINE_UPDT_DT_TM",F.current_timestamp()))
if not spark.catalog.tableExists(TARGET):
    out.limit(0).write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed","true").saveAsTable(TARGET)
metrics=keyed_upsert(TARGET,["ORDER_ID"],out)
print({"affected_orders":affected.count(),"dq_flagged":flagged,"metrics":metrics})
run_gates()
for source,boundary in [(ORDERS_SOURCE,orders_boundary),(MED_SOURCE,med_boundary),
                        (catalog_table,cat_boundary),(cv_table,cv_boundary)]:
    old=marks[source]
    if boundary is not None and (old is None or boundary>old):
        wm_set(PIPE,source,boundary)

_record_source_versions(TARGET,CURRENT_SOURCE_VERSIONS)

spark.sql(f"""COMMENT ON TABLE {TARGET} IS
'Grain: one mill_orders ORDER_ID; lossless generic order spine. MED_FAMILY_IND is membership in map_medication_order, not an ACTIVITY_TYPE_CD inference. Bare-FK status/activity decodes are unfiltered. Clinical source dates are retained with future/sentinel/clean triplets. Named exclusions: display-line text families, order detail/action/review/verify plumbing, protocol/template internals, and cost/billing codes; those remain in raw or domain products.'""")
print("A8a build complete")

# COMMAND ----------

# PROMOTION RUNBOOK — HUMAN GATED
# Requalify the shallow-clone evidence if source VACUUM occurs or more than seven days elapse.
# Set target_schema=4_prod.bronze, production control schema, mode=prod, and raw/prod source paths.
# Initial build uses approved classic compute; rerun every gate on prod. Register after
# map_medication_order so membership changes are visible. retries=0. Record the warm-median
# seven-day increment cost and integrated weekly-job delta before promotion.

dbutils.notebook.exit(json.dumps({"result": "BUILT", "target": TARGET, "target_schema": TARGET_SCHEMA}, sort_keys=True))


