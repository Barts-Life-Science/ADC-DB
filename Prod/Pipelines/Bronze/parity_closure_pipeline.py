# Databricks notebook source
# parity_closure_pipeline — S3/A11 new products.
# Builds map_cancer_treatment_cycle at true (course,cycle) grain and map_blob_succession
# from mill_ce_blob_result only. mill_ce_blob chunk duplication is explicitly out of scope.

# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 607859911067070)
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
SCOPE = _widget_text("scope", "all").lower()
CYCLE_SOURCE = _widget_text("cycle_source", "4_prod.raw.iqemo_treatment_cycle")
COURSE_SOURCE = _widget_text("course_source", "4_prod.raw.iqemo_chemotherapy_course")
CANCER_MAP = _widget_text("cancer_map", "4_prod.bronze.map_cancer_treatment")
BLOB_SOURCE = _widget_text("blob_source", "4_prod.raw.mill_ce_blob_result")
assert ACTION in ("pre_gates", "build", "gates") and SCOPE in ("all", "cycle", "blob")
WATERMARK_TABLE = f"{CONTROL}.s3_watermarks"
CYCLE_TARGET = f"{SCHEMA}.map_cancer_treatment_cycle"
BLOB_TARGET = f"{SCHEMA}.map_blob_succession"

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

def pinned_slice(source,pipeline,source_key,clock="ADC_UPDT"):
    v=table_version(source);snap=spark.read.option("versionAsOf",v).table(source)
    boundary=snap.agg(F.max(clock)).collect()[0][0];wm=wm_get(pipeline,source_key);inc=snap
    if wm is not None:inc=inc.where(F.col(clock)>F.expr(f"TIMESTAMP'{wm}' - INTERVAL 24 HOURS"))
    if boundary is not None:inc=inc.where(F.col(clock)<=F.lit(boundary))
    return snap,inc,boundary,v

def cycle_gates():
    assert spark.catalog.tableExists(CYCLE_TARGET),f"TABLE_OR_VIEW_NOT_FOUND: {CYCLE_TARGET}"
    d=spark.table(CYCLE_TARGET)
    a=d.agg(F.count("*").alias("n"),
            F.countDistinct(F.struct("CHEMOTHERAPY_COURSE_ID","TREATMENT_CYCLE_ID")).alias("du"),
            F.sum((F.col("MAP_CANCER_TREATMENT_LINK_STATUS")=="LINKED").cast("long")).alias("linked")).collect()[0]
    assert a["n"]>=293391 and a["n"]==a["du"],f"7b key/lossless {a}"
    assert a["linked"]/a["n"]>=0.84,f"7b raw-to-map link {a}"
    back=spark.sql(f"""SELECT COUNT(*) n,SUM(CASE WHEN c.CHEMOTHERAPY_COURSE_ID IS NOT NULL THEN 1 ELSE 0 END) ok
      FROM (SELECT DISTINCT CAST(iqemo_chemotherapy_course_id AS INT) id FROM {CANCER_MAP}
            WHERE iqemo_chemotherapy_course_id IS NOT NULL) m
      LEFT JOIN (SELECT DISTINCT CHEMOTHERAPY_COURSE_ID FROM {CYCLE_TARGET}) c
        ON m.id=c.CHEMOTHERAPY_COURSE_ID""").collect()[0]
    assert back["ok"]/back["n"]>=0.998,f"7b map-to-raw course coverage {back}"
    for c in ("PRESCRIBED_DATE","PHARMACY_CONFIRMED_DATE","CANCELLATION_DATE","START_DATE"):
        for s in ("_FUTURE_IND","_SENTINEL_IND","_CLEAN"):
            assert c+s in d.columns,f"7b dq missing {c+s}"
    print({"cycle_rows":a["n"],"linked":a["linked"],"map_to_raw":back.asDict(),
           "rde_iqemo_context":289793})

def blob_gates():
    assert spark.catalog.tableExists(BLOB_TARGET),f"TABLE_OR_VIEW_NOT_FOUND: {BLOB_TARGET}"
    d=spark.table(BLOB_TARGET)
    a=d.agg(F.count("*").alias("n"),F.countDistinct("EVENT_ID").alias("du"),
            F.sum((F.col("SUCCESSION_TYPE_CD")==371).cast("long")).alias("interim"),
            F.sum((F.col("SUCCESSION_TYPE_CD")==370).cast("long")).alias("final"),
            F.sum((F.col("SUCCESSION_TYPE_CD")==377).cast("long")).alias("unknown")).collect()[0]
    if BLOB_SOURCE=="8_dev.s3_bronze.fix_mill_ce_blob_result":
        # S3-A11 correction: gate at the product grain (latest open row per EVENT_ID),
        # not at the source-version grain used by the original recon percentages.
        assert a["n"]==206548261,f"7d baseline rows {a['n']}"
        assert (a["interim"],a["final"],a["unknown"])==(110744318,85184143,10619800),a
    assert a["n"]==a["du"],f"7d duplicate event {a}"
    shares=[a["interim"]/a["n"],a["final"]/a["n"],a["unknown"]/a["n"]]
    expected=[0.5362,0.4124,0.0514]
    assert all(abs(x-y)<=0.005 for x,y in zip(shares,expected)),f"7d latest-event succession shares {shares}"
    for s in ("_FUTURE_IND","_SENTINEL_IND","_CLEAN"):
        assert "VALID_FROM_DT_TM"+s in d.columns
    print({"blob_rows":a["n"],"shares":shares})

def run_gates():
    if SCOPE in ("all","cycle"):cycle_gates()
    if SCOPE in ("all","blob"):blob_gates()
    print("A11 new-product gates PASS")

if ACTION=="pre_gates":
    run_gates();dbutils.notebook.exit("unexpected pre-gate pass")
if ACTION=="gates":
    run_gates();dbutils.notebook.exit("gates pass")

# COMMAND ----------

cv_table="3_lookup.mill.mill_code_value"
CYCLE_SOURCE_VERSIONS=_source_versions([CYCLE_SOURCE,COURSE_SOURCE,CANCER_MAP])
BLOB_SOURCE_VERSIONS=_source_versions([BLOB_SOURCE,cv_table])
selected_current = (
    (SCOPE not in ("all","cycle") or _target_state_current(CYCLE_TARGET,CYCLE_SOURCE_VERSIONS))
    and (SCOPE not in ("all","blob") or _target_state_current(BLOB_TARGET,BLOB_SOURCE_VERSIONS))
)
if selected_current:
    run_gates()
    dbutils.notebook.exit(json.dumps({
        "result":"NO_OP","target":TARGET_SCHEMA,"target_schema":TARGET_SCHEMA,
        "source_versions":{
            "map_cancer_treatment_cycle":CYCLE_SOURCE_VERSIONS,
            "map_blob_succession":BLOB_SOURCE_VERSIONS,
        },
    },sort_keys=True))

def build_cycle():
    pipe="a11_cancer_cycle";marks={}
    cycle_snap,cycle_inc,cycle_boundary,cycle_v=pinned_slice(CYCLE_SOURCE,pipe,CYCLE_SOURCE)
    marks[CYCLE_SOURCE]=wm_get(pipe,CYCLE_SOURCE)
    course_snap,course_inc,course_boundary,course_v=pinned_slice(COURSE_SOURCE,pipe,COURSE_SOURCE)
    marks[COURSE_SOURCE]=wm_get(pipe,COURSE_SOURCE)
    map_clock="SRC_ADC_UPDT" if "SRC_ADC_UPDT" in spark.table(CANCER_MAP).columns else "ADC_UPDT"
    map_snap,map_inc,map_boundary,map_v=pinned_slice(CANCER_MAP,pipe,CANCER_MAP,map_clock)
    marks[CANCER_MAP]=wm_get(pipe,CANCER_MAP)

    affected=(cycle_inc.select(F.col("ChemotherapyCourseID").cast("int").alias("CHEMOTHERAPY_COURSE_ID"))
      .union(course_inc.select(F.col("ChemotherapyCourseID").cast("int").alias("CHEMOTHERAPY_COURSE_ID")))
      .union(map_inc.where("iqemo_chemotherapy_course_id IS NOT NULL")
             .select(F.col("iqemo_chemotherapy_course_id").cast("int").alias("CHEMOTHERAPY_COURSE_ID")))
      .distinct())
    c=(cycle_snap.join(affected,F.col("ChemotherapyCourseID")==F.col("CHEMOTHERAPY_COURSE_ID"),"inner")
       .drop("CHEMOTHERAPY_COURSE_ID")
       .select(
        F.col("ChemotherapyCourseID").cast("int").alias("CHEMOTHERAPY_COURSE_ID"),
        F.col("TreatmentCycleID").cast("int").alias("TREATMENT_CYCLE_ID"),
        F.col("PatientID").cast("int").alias("IQEMO_PATIENT_ID"),
        F.col("EpisodeID").cast("int").alias("EPISODE_ID"),"SurfaceArea","SurfaceAreaCapped",
        F.col("PrescribedDate").alias("PRESCRIBED_DATE"),
        F.col("PharmacyConfirmedDate").alias("PHARMACY_CONFIRMED_DATE"),
        "CycleFrequency","Cost","LockStatus","CycleStatus","CalculatedSurfaceArea","RegimenCycleID",
        "CancellationReasonID",F.col("CancellationDate").alias("CANCELLATION_DATE"),
        "PerformanceStatusAdultID","PerformanceStatusYoungPersonID","PrescriptionVersionNumber",
        "AllowOverdue","MinimumCycleFrequency","AllowEarlyPrescriptionConfirmation","PharmacyChanged",
        "TreatmentResponseID","OutcomeComments","CaseTypeID","PreCancelStatus","WeightChanged",
        "TreatmentCycleCode","SerumCreatinineChanged","Reason","OrganisationID","TemplateName",
        F.col("StartDate").alias("START_DATE"),"HasReadyToConfirmNotes","PharmacyUnitID",
        "BaselineCreatininePathologyResultID","CustomerOrderNumber","OverrideScreeningLock","ReasonID",
        F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT")))
    course=course_snap.select(F.col("ChemotherapyCourseID").cast("int").alias("CHEMOTHERAPY_COURSE_ID"),
                              F.col("LineOfTreatmentID").cast("int").alias("LINE_OF_TREATMENT_ID"),
                              F.col("RegimenNumber").cast("int").alias("REGIMEN_NUMBER"))
    map_courses=map_snap.where("iqemo_chemotherapy_course_id IS NOT NULL").select(
        F.col("iqemo_chemotherapy_course_id").cast("int").alias("_MAP_COURSE")).distinct()
    out=(c.join(course,"CHEMOTHERAPY_COURSE_ID","left")
         .join(map_courses,F.col("CHEMOTHERAPY_COURSE_ID")==F.col("_MAP_COURSE"),"left")
         .withColumn("MAP_CANCER_TREATMENT_LINK_STATUS",
                     F.when(F.col("_MAP_COURSE").isNotNull(),"LINKED").otherwise("UNLINKED_COURSE"))
         .drop("_MAP_COURSE"))
    out,flagged=dq_all_clinical(out,admin_stamps={"SOURCE_ADC_UPDT"})
    admin={"SOURCE_ADC_UPDT","PIPELINE_UPDT_DT_TM","ROW_HASH"}
    hash_cols=[x for x in out.columns if x not in admin and not x.endswith(("_FUTURE_IND","_SENTINEL_IND","_CLEAN"))]
    out=(out.withColumn("ROW_HASH",F.xxhash64(F.to_json(F.struct(*[F.col(x) for x in hash_cols]))))
         .withColumn("PIPELINE_UPDT_DT_TM",F.current_timestamp()))
    if not spark.catalog.tableExists(CYCLE_TARGET):
        out.limit(0).write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed","true").saveAsTable(CYCLE_TARGET)
    metrics=keyed_upsert(CYCLE_TARGET,["CHEMOTHERAPY_COURSE_ID","TREATMENT_CYCLE_ID"],out)
    print({"cycle_versions":[cycle_v,course_v,map_v],"affected_courses":affected.count(),
           "dq_flagged":flagged,"metrics":metrics})
    cycle_gates()
    for source,boundary in [(CYCLE_SOURCE,cycle_boundary),(COURSE_SOURCE,course_boundary),(CANCER_MAP,map_boundary)]:
        old=marks[source]
        if boundary is not None and (old is None or boundary>old):wm_set(pipe,source,boundary)
    _record_source_versions(CYCLE_TARGET,CYCLE_SOURCE_VERSIONS)
    spark.sql(f"""COMMENT ON TABLE {CYCLE_TARGET} IS
    'Grain: one (CHEMOTHERAPY_COURSE_ID, TREATMENT_CYCLE_ID); TreatmentCycleID alone is only a per-course 0..326 sequence. Course-level line-of-treatment and regimen number come from iqemo_chemotherapy_course. Link status is course-level and unlinked cycles are retained. Audit plumbing excluded: source user IDs, lock/cancel user IDs, ChangeID, VersionTimeStamp, and DateUpdated. Clinical cycle payload including outcome/reason text is retained.'""")

def build_blob():
    pipe="a11_blob_succession";marks={}
    snap,inc,boundary,source_v=pinned_slice(BLOB_SOURCE,pipe,BLOB_SOURCE)
    marks[BLOB_SOURCE]=wm_get(pipe,BLOB_SOURCE)
    cv_snap,cv_inc,cv_boundary,cv_v=pinned_slice(cv_table,pipe,cv_table)
    marks[cv_table]=wm_get(pipe,cv_table)
    affected=inc.select(F.col("EVENT_ID").cast("bigint").alias("EVENT_ID")).distinct()
    if spark.catalog.tableExists(BLOB_TARGET):
        changed=cv_inc.select(F.col("CODE_VALUE").cast("bigint").alias("SUCCESSION_TYPE_CD")).distinct()
        affected=affected.union(spark.table(BLOB_TARGET).join(changed,"SUCCESSION_TYPE_CD","inner").select("EVENT_ID")).distinct()
    raw=(snap.select(F.col("EVENT_ID").cast("bigint").alias("EVENT_ID"),
          F.col("SUCCESSION_TYPE_CD").cast("bigint").alias("SUCCESSION_TYPE_CD"),
          "VALID_FROM_DT_TM","VALID_UNTIL_DT_TM",F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"))
         .join(affected,"EVENT_ID","inner")
         .withColumn("_OPEN",(F.col("VALID_UNTIL_DT_TM")>=F.lit("2035-01-01").cast("timestamp")).cast("int")))
    out=latest_per_key(raw,["EVENT_ID"],[F.col("_OPEN").desc(),F.col("VALID_UNTIL_DT_TM").desc_nulls_last(),
                                        F.col("VALID_FROM_DT_TM").desc_nulls_last()]).drop("_OPEN")
    cv=cv_snap.select(F.col("CODE_VALUE").cast("bigint").alias("_CD"),
                      F.col("DISPLAY").alias("SUCCESSION_TYPE_DESC"))
    out=out.join(cv,F.col("SUCCESSION_TYPE_CD")==F.col("_CD"),"left").drop("_CD","VALID_UNTIL_DT_TM")
    out,flagged=dq_all_clinical(out,admin_stamps={"SOURCE_ADC_UPDT"})
    admin={"SOURCE_ADC_UPDT","PIPELINE_UPDT_DT_TM","ROW_HASH"}
    hash_cols=[x for x in out.columns if x not in admin and not x.endswith(("_FUTURE_IND","_SENTINEL_IND","_CLEAN"))]
    out=(out.withColumn("ROW_HASH",F.xxhash64(F.to_json(F.struct(*[F.col(x) for x in hash_cols]))))
         .withColumn("PIPELINE_UPDT_DT_TM",F.current_timestamp()))
    if not spark.catalog.tableExists(BLOB_TARGET):
        out.limit(0).write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed","true").saveAsTable(BLOB_TARGET)
    metrics=keyed_upsert(BLOB_TARGET,["EVENT_ID"],out)
    print({"blob_source_version":source_v,"affected_events":affected.count(),"dq_flagged":flagged,"metrics":metrics})
    blob_gates()
    for source,b in [(BLOB_SOURCE,boundary),(cv_table,cv_boundary)]:
        old=marks[source]
        if b is not None and (old is None or b>old):wm_set(pipe,source,b)
    _record_source_versions(BLOB_TARGET,BLOB_SOURCE_VERSIONS)
    spark.sql(f"""COMMENT ON TABLE {BLOB_TARGET} IS
    'Grain: one latest open mill_ce_blob_result row per EVENT_ID, carrying succession metadata. Source mill_ce_blob_result is stale since 2026-04-20. This table reads mill_ce_blob_result only; the active (EVENT_ID, BLOB_SEQ_NUM) version-duplication defect belongs to the different mill_ce_blob chunk table and is not hidden or repaired here.'""")

if SCOPE in ("all","cycle"):build_cycle()
if SCOPE in ("all","blob"):build_blob()
print("A11 new products complete")

# COMMAND ----------

# CONSOLIDATED PROMOTION RUNBOOK — HUMAN GATED
# map_cancer_treatment_cycle and map_blob_succession are independent. Use production
# target/control schema and production sources; initial blob build uses approved classic compute.
# Rerun gates on prod, preserve CDF/row-tracking/DV policy, retries=0, and record weekly-budget delta.
# A9 requires S4 NHSI publication. A11 med-admin and cancer builder extensions ride the pending S4
# swaps. Restricted identifiers require grants before Task 8. Every production action is promoter-run.

dbutils.notebook.exit(json.dumps({"result": "BUILT", "target": TARGET_SCHEMA, "target_schema": TARGET_SCHEMA}, sort_keys=True))


