# Databricks notebook source
# person_encntr_attribute_pipeline — S3/A7.
# Grain: one source EAV row per SOURCE_PK; CURRENT_IND marks the deterministic latest active
# row per (ENTITY_ID, INFO_SUB_TYPE_CD). All allowlisted history is retained.
# Long-text attributes are excluded: Accessibility Information, Social Worker Name,
# Ambulance ID, INFO_TYPE 1169 comments, and operational routing/contact groupers.
# No interpreter subtype exists. HOUSING_ACCOMMODATION_STATUS is distinct from
# map_patient_journey ward/bed ACCOMMODATION_CD and ACCOMMODATION_REASON_CD.

# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 1104562556424143)
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
assert ACTION in ("pre_gates", "build", "gates")
WATERMARK_TABLE = f"{CONTROL}.s3_watermarks"

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

PERSON_ATTRS = {
  3843283:    ("NO_FIXED_ABODE", "CODE", 100599),
  3843309:    ("CHILD_IN_PUBLIC_CARE", "CODE", 100599),
  3843308:    ("UK_RESIDENT_12_MONTHS", "CODE", None),  # S3-A7 mixed code sets 100014/100599
  1064499297: ("COMMUNICATION_METHOD", "CODE", 100109),
  1064501701: ("LEARNING_DIFFICULTIES", "CODE", 100112),
  1064501385: ("HEARING_IMPAIRMENT", "CODE", 100110),
  1064499383: ("VISION_IMPAIRMENT", "CODE", 100114),
}
ENCNTR_ATTRS = {
  3843321:    ("CANCER_REFERRAL", "CODE", 100599),
  4464972:    ("ED_OBSERVATION_PATIENT", "CODE", 100599),
  469460613:  ("HOUSING_ACCOMMODATION_STATUS", "CODE", 100095),
  469460719:  ("DISCHARGE_FOLLOW_UP", "CODE", 100098),
  3843370:    ("OVERSEAS_VISITOR_STATUS", "CODE", 271570),
  1201705421: ("DISCHARGE_TO_HOSPITAL_AT_HOME", "CODE", 14167),  # S3-A7 live code-set correction
  3886336:    ("DELAYED_DISCHARGE_REASON", "CODE", 100120),  # S3-A7 live code-set confirmation
}
BASELINE_ACTIVE = {
 "map_person_attribute": {3843283:2208193,3843308:6671250,3843309:679604,1064499297:5290,
                          1064499383:148,1064501385:408,1064501701:646},
 "map_encounter_attribute": {3843321:27632971,3843370:42587819,3886336:13131,4464972:6532468,
                             469460613:4270052,469460719:3754495,1201705421:1521623},
}
BASE_COLS = [
 "ENTITY_ID","INFO_TYPE_CD","INFO_SUB_TYPE_CD","SOURCE_PK","ATTRIBUTE_NAME","VALUE_KIND",
 "EXPECTED_CODE_SET","VALUE_CD","VALUE_DT_TM","VALUE_NUMERIC","VALUE_ANSWERED_IND","ACTIVE_IND",
 "BEG_EFFECTIVE_DT_TM","END_EFFECTIVE_DT_TM","UPDT_DT_TM","SOURCE_ADC_UPDT"
]

def gate_target(name, attrs, link_floor):
    target=f"{SCHEMA}.{name}"
    assert spark.catalog.tableExists(target), f"TABLE_OR_VIEW_NOT_FOUND: {target}"
    d=spark.table(target)
    active={int(r["INFO_SUB_TYPE_CD"]):int(r["n"]) for r in
            d.where("ACTIVE_IND=1").groupBy("INFO_SUB_TYPE_CD").count()
             .withColumnRenamed("count","n").collect()}
    for k,n in BASELINE_ACTIVE[name].items():
        assert active.get(k,0) >= n, f"{name} G1 {k}: {active.get(k,0)} < baseline {n}"
    dup=d.groupBy("SOURCE_PK").count().where("count<>1").limit(1).count()
    assert dup==0, f"{name} G2 duplicate SOURCE_PK"
    bad_current=d.where("CURRENT_IND AND ACTIVE_IND<>1").limit(1).count()
    assert bad_current==0, f"{name} G2 inactive CURRENT_IND"
    grp=(d.groupBy("ENTITY_ID","INFO_SUB_TYPE_CD")
          .agg(F.sum(F.when(F.col("ACTIVE_IND")==1,1).otherwise(0)).alias("active_n"),
               F.sum(F.when(F.col("CURRENT_IND"),1).otherwise(0)).alias("current_n")))
    assert grp.where("(active_n>0 AND current_n<>1) OR (active_n=0 AND current_n<>0)").limit(1).count()==0,         f"{name} G2 current cardinality"
    assert d.where("NOT VALUE_ANSWERED_IND AND VALUE_DISPLAY IS NOT NULL").limit(1).count()==0,         f"{name} G3 unanswered decode"
    assert d.where("VALUE_ANSWERED_IND AND VALUE_DISPLAY IS NULL").limit(1).count()==0,         f"{name} G3 unresolved code"
    for k,(_,_,cs) in attrs.items():
        if cs is not None:
            assert d.where((F.col("INFO_SUB_TYPE_CD")==k)&F.col("VALUE_ANSWERED_IND")&
                           (F.col("VALUE_CODE_SET")!=cs)).limit(1).count()==0, f"{name} G3 code set {k}"
    yesno=[k for k,v in attrs.items() if v[2]==100599]
    assert d.where(F.col("INFO_SUB_TYPE_CD").isin(yesno)&F.col("VALUE_ANSWERED_IND")&
                   (~F.col("VALUE_DISPLAY").isin("Yes","No"))).limit(1).count()==0, f"{name} G3 yes/no"
    total=d.count()
    linked=d.where("LINK_STATUS='LINKED'").count()
    assert linked/max(total,1)>=link_floor, f"{name} G4 link rate {linked/max(total,1):.5f}"
    synchronized_link_rate = None
    if name == "map_encounter_attribute":
        # 2026-08-14 release re-proof: mill_encntr_info can land ahead of map_encounter.
        # Preserve the original 99.9% gate at the downstream map's synchronized ADC horizon,
        # while the overall 99.8% floor still catches a material unresolved-tail regression.
        link_horizon = spark.table("4_prod.bronze.map_encounter").agg(
            F.max("ADC_UPDT").alias("h")
        ).first()["h"]
        synchronized = d.where(F.col("SOURCE_ADC_UPDT") <= F.lit(link_horizon))
        synchronized_total = synchronized.count()
        synchronized_linked = synchronized.where("LINK_STATUS='LINKED'").count()
        synchronized_link_rate = synchronized_linked / max(synchronized_total, 1)
        assert synchronized_link_rate >= 0.999, (
            f"{name} G4 synchronized link rate {synchronized_link_rate:.5f} "
            f"at map_encounter horizon {link_horizon}"
        )
    for c in ("VALUE_DT_TM","BEG_EFFECTIVE_DT_TM","END_EFFECTIVE_DT_TM"):
        for suffix in ("_FUTURE_IND","_SENTINEL_IND","_CLEAN"):
            assert c+suffix in d.columns, f"{name} G5 missing {c+suffix}"
    print({"target":target,"rows":total,"linked":linked,"link_rate":linked/max(total,1),
           "synchronized_link_rate":synchronized_link_rate,"active":active})
    return True

def run_gates():
    gate_target("map_person_attribute",PERSON_ATTRS,0.989)
    gate_target("map_encounter_attribute",ENCNTR_ATTRS,0.998)
    print("A7 gates PASS")

if ACTION=="pre_gates":
    run_gates()
    dbutils.notebook.exit("unexpected pre-gate pass")
if ACTION=="gates":
    run_gates()
    dbutils.notebook.exit("gates pass")

# COMMAND ----------

def transform_product(name, source_table, attrs, entity_col, pk_col, link_table, link_col, unlinked_status):
    pipe=f"a7_{name}"
    target=f"{SCHEMA}.{name}"
    marks_before={}
    source_inc, source_boundary=incr_slice(source_table,pipe,source_table)
    marks_before[source_table]=wm_get(pipe,source_table)
    link_inc, link_boundary=incr_slice(link_table,pipe,link_table)
    marks_before[link_table]=wm_get(pipe,link_table)
    cv_table="3_lookup.mill.mill_code_value"
    cv_inc, cv_boundary=incr_slice(cv_table,pipe,cv_table)
    marks_before[cv_table]=wm_get(pipe,cv_table)

    keys=list(attrs.keys())
    name_pairs=[x for k,v in attrs.items() for x in (F.lit(k),F.lit(v[0]))]
    kind_pairs=[x for k,v in attrs.items() for x in (F.lit(k),F.lit(v[1]))]
    cs_pairs=[x for k,v in attrs.items() for x in (F.lit(k),F.lit(v[2]).cast("bigint"))]
    name_map=F.create_map(*name_pairs); kind_map=F.create_map(*kind_pairs); cs_map=F.create_map(*cs_pairs)

    inc=(source_inc.where(F.col("INFO_SUB_TYPE_CD").cast("bigint").isin(keys))
         .select(F.col(entity_col).cast("bigint").alias("ENTITY_ID"),
                 F.col("INFO_TYPE_CD").cast("bigint").alias("INFO_TYPE_CD"),
                 F.col("INFO_SUB_TYPE_CD").cast("bigint").alias("INFO_SUB_TYPE_CD"),
                 F.col(pk_col).cast("bigint").alias("SOURCE_PK"),
                 F.col("VALUE_CD").cast("bigint").alias("VALUE_CD"),
                 "VALUE_DT_TM","VALUE_NUMERIC",
                 F.col("ACTIVE_IND").cast("bigint").alias("ACTIVE_IND"),
                 "BEG_EFFECTIVE_DT_TM","END_EFFECTIVE_DT_TM","UPDT_DT_TM",
                 F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"))
         .withColumn("ATTRIBUTE_NAME",name_map[F.col("INFO_SUB_TYPE_CD")])
         .withColumn("VALUE_KIND",kind_map[F.col("INFO_SUB_TYPE_CD")])
         .withColumn("EXPECTED_CODE_SET",cs_map[F.col("INFO_SUB_TYPE_CD")])
         .withColumn("VALUE_ANSWERED_IND",F.col("VALUE_CD").isNotNull()&(F.col("VALUE_CD")!=0))
         .select(*BASE_COLS))
    inc=inc.withColumn("_S3_REFRESH_PRIORITY",F.lit(1))

    changed_pks=inc.select("SOURCE_PK").distinct()
    affected=inc.select("ENTITY_ID","INFO_SUB_TYPE_CD").distinct()
    if spark.catalog.tableExists(target):
        old_groups=(spark.table(target).join(changed_pks,"SOURCE_PK","inner")
                    .select("ENTITY_ID","INFO_SUB_TYPE_CD").distinct())
        link_entities=link_inc.select(F.col(link_col).cast("bigint").alias("ENTITY_ID")).distinct()
        link_groups=(spark.table(target).join(link_entities,"ENTITY_ID","inner")
                     .select("ENTITY_ID","INFO_SUB_TYPE_CD").distinct())
        changed_codes=cv_inc.select(F.col("CODE_VALUE").cast("bigint").alias("VALUE_CD")).distinct()
        code_groups=(spark.table(target).join(changed_codes,"VALUE_CD","inner")
                     .select("ENTITY_ID","INFO_SUB_TYPE_CD").distinct())
        affected=affected.union(old_groups).union(link_groups).union(code_groups).distinct()
        existing=(spark.table(target).join(affected,["ENTITY_ID","INFO_SUB_TYPE_CD"],"inner")
                  .select(*BASE_COLS).withColumn("_S3_REFRESH_PRIORITY",F.lit(0)))
        base=existing.unionByName(inc)
    else:
        base=inc

    base=latest_per_key(base,["SOURCE_PK"],[
        F.col("_S3_REFRESH_PRIORITY").desc(),F.col("SOURCE_ADC_UPDT").desc_nulls_last(),
        F.col("UPDT_DT_TM").desc_nulls_last()]).drop("_S3_REFRESH_PRIORITY")
    cv=(spark.table(cv_table)
        .select(F.col("CODE_VALUE").cast("bigint").alias("_CV"),
                F.col("DISPLAY").alias("VALUE_DISPLAY"),
                F.col("CODE_SET").cast("bigint").alias("VALUE_CODE_SET")))
    base=(base.withColumn("_JOIN_CD",F.when(F.col("VALUE_ANSWERED_IND"),F.col("VALUE_CD")))
          .join(cv,F.col("_JOIN_CD")==F.col("_CV"),"left").drop("_JOIN_CD","_CV"))
    link=(spark.table(link_table).select(F.col(link_col).cast("bigint").alias("_LINK_ID")).distinct())
    base=(base.join(link,F.col("ENTITY_ID")==F.col("_LINK_ID"),"left")
          .withColumn("LINK_STATUS",F.when(F.col("_LINK_ID").isNotNull(),"LINKED").otherwise(unlinked_status))
          .drop("_LINK_ID"))
    cur=(latest_per_key(base.where("ACTIVE_IND=1"),["ENTITY_ID","INFO_SUB_TYPE_CD"],[
          F.col("UPDT_DT_TM").desc_nulls_last(),F.col("BEG_EFFECTIVE_DT_TM").desc_nulls_last(),
          F.col("SOURCE_PK").desc()])
         .select("SOURCE_PK").withColumn("CURRENT_IND",F.lit(True)))
    out=base.join(cur,"SOURCE_PK","left").fillna(False,["CURRENT_IND"])
    out,flagged=dq_all_clinical(out,admin_stamps={"UPDT_DT_TM","SOURCE_ADC_UPDT"})
    admin={"UPDT_DT_TM","SOURCE_ADC_UPDT","PIPELINE_UPDT_DT_TM","ROW_HASH"}
    hash_cols=[c for c in out.columns if c not in admin and not c.endswith(("_FUTURE_IND","_SENTINEL_IND","_CLEAN"))]
    out=(out.withColumn("ROW_HASH",F.xxhash64(F.to_json(F.struct(*[F.col(c) for c in hash_cols]))))
            .withColumn("PIPELINE_UPDT_DT_TM",F.current_timestamp()))
    if not spark.catalog.tableExists(target):
        (out.limit(0).write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed","true")
         .saveAsTable(target))
    metrics=keyed_upsert(target,["SOURCE_PK"],out)
    print({"target":target,"affected_groups":affected.count(),"dq_flagged":flagged,"metrics":metrics})
    return [(pipe,source_table,source_boundary,marks_before[source_table]),
            (pipe,link_table,link_boundary,marks_before[link_table]),
            (pipe,cv_table,cv_boundary,marks_before[cv_table])]

PERSON_STATE_SOURCES=["4_prod.raw.mill_person_info","4_prod.bronze.map_person","3_lookup.mill.mill_code_value"]
ENCNTR_STATE_SOURCES=["4_prod.raw.mill_encntr_info","4_prod.bronze.map_encounter","3_lookup.mill.mill_code_value"]
PERSON_SOURCE_VERSIONS=_source_versions(PERSON_STATE_SOURCES)
ENCNTR_SOURCE_VERSIONS=_source_versions(ENCNTR_STATE_SOURCES)
if (
    _target_state_current(f"{SCHEMA}.map_person_attribute",PERSON_SOURCE_VERSIONS)
    and _target_state_current(f"{SCHEMA}.map_encounter_attribute",ENCNTR_SOURCE_VERSIONS)
):
    run_gates()
    dbutils.notebook.exit(json.dumps({
        "result":"NO_OP","target":TARGET_SCHEMA,"target_schema":TARGET_SCHEMA,
        "source_versions":{
            "map_person_attribute":PERSON_SOURCE_VERSIONS,
            "map_encounter_attribute":ENCNTR_SOURCE_VERSIONS,
        },
    },sort_keys=True))

commits=[]
commits += transform_product("map_person_attribute","4_prod.raw.mill_person_info",PERSON_ATTRS,
                             "PERSON_ID","PERSON_INFO_ID","4_prod.bronze.map_person","PERSON_ID",
                             "UNLINKED_PERSON_TYPE")
commits += transform_product("map_encounter_attribute","4_prod.raw.mill_encntr_info",ENCNTR_ATTRS,
                             "ENCNTR_ID","ENCNTR_INFO_ID","4_prod.bronze.map_encounter","ENCNTR_ID",
                             "UNLINKED")
run_gates()
for pipe,source,boundary,old in commits:
    if boundary is not None and (old is None or boundary>old):
        wm_set(pipe,source,boundary)

_record_source_versions(f"{SCHEMA}.map_person_attribute",PERSON_SOURCE_VERSIONS)
_record_source_versions(f"{SCHEMA}.map_encounter_attribute",ENCNTR_SOURCE_VERSIONS)

spark.sql(f"""COMMENT ON TABLE {SCHEMA}.map_person_attribute IS
'Grain: one allowlisted mill_person_info SOURCE_PK per row; history retained and CURRENT_IND is the latest active row per person/subtype. VALUE_CD=0 is unanswered and never decoded. Bare-FK code decodes are unfiltered. Excludes LONG_TEXT attributes including Accessibility Information and Social Worker Name, operational contact/routing groupers, and free-text address flags. No interpreter subtype exists; language remains on map_person.'""")
spark.sql(f"""COMMENT ON TABLE {SCHEMA}.map_encounter_attribute IS
'Grain: one allowlisted mill_encntr_info SOURCE_PK per row; history retained and CURRENT_IND is the latest active row per encounter/subtype. HOUSING_ACCOMMODATION_STATUS is self-reported housing and is distinct from map_patient_journey ward/bed accommodation and accommodation reason. Excludes Ambulance ID and comments LONG_TEXT plus operational routing groupers. VALUE_CD=0 is unanswered; bare-FK decodes are unfiltered.'""")
print("A7 build and gates complete")

# COMMAND ----------

# PROMOTION RUNBOOK — HUMAN GATED
# Set target_schema=4_prod.bronze, control_schema to the production bronze-control schema,
# mode=prod. Run on approved compute; rerun all gates against prod. Register the two products
# as one Bronze_Pipeline step after map_person/map_encounter; retries=0. Preserve the comments,
# excluded-attribute list, and package A7 classification. The 237M encounter path requires the
# recorded pinned-fixture benchmark pack before promotion.

dbutils.notebook.exit(json.dumps({"result": "BUILT", "target": TARGET_SCHEMA, "target_schema": TARGET_SCHEMA}, sort_keys=True))


