# Databricks notebook source
# identifier_pipeline — S3/A10.
# Bronze carries source identifiers. De-identification/pseudonymisation is a serve-time concern.
# Every published column receives the estate ig_risk and ig_severity tags.

# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 732464874501890)
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

TARGET_SCHEMA = _widget_text("target_schema", "8_dev.s3_bronze")
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
assert ACTION in ("build", "gates")

# COMMAND ----------

from datetime import timedelta
from pyspark.sql import functions as F

PERSON_SOURCE = "4_prod.raw.mill_person_alias"
ENCNTR_SOURCE = "4_prod.raw.mill_encntr_alias"
PERSON_TARGET = f"{SCHEMA}.map_patient_identifier"
ENCNTR_TARGET = f"{SCHEMA}.map_encounter_identifier"
WATERMARK_TABLE = f"{CONTROL}.s3_watermarks"
RUN_AS_OF = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]

PERSON_TYPES = {10: "MRN", 18: "NHS"}
ENCNTR_TYPES = {1077: "FIN", 1079: "MRN", 1081: "VISIT"}


def table_version(table_name):
    return int(spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()[0]["version"])


SOURCE_STATE_PROPERTY = "bronze_completeness.source_versions_json"

def _source_versions(sources):
    return {source: int(table_version(source)) for source in sources}

def _target_state_current(target, versions):
    if not spark.catalog.tableExists(target):
        return False
    properties = spark.sql(f"DESCRIBE DETAIL {target}").first()["properties"] or {}
    previous = {k: int(v) for k, v in json.loads(
        properties.get(SOURCE_STATE_PROPERTY, "{}")).items()}
    if previous != versions:
        return False
    expired_current = spark.table(target).where(
        F.col("CURRENT_IND") & F.col("END_EFFECTIVE_DT_TM").isNotNull()
        & (F.col("END_EFFECTIVE_DT_TM") < F.lit(RUN_AS_OF))
    ).limit(1).count()
    return expired_current == 0

def _record_source_versions(target, versions):
    payload = json.dumps(versions, sort_keys=True, separators=(",", ":")).replace("'", "''")
    spark.sql(
        f"ALTER TABLE {target} SET TBLPROPERTIES "
        f"('{SOURCE_STATE_PROPERTY}'='{payload}')"
    )


def wm_get(pipeline, source):
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
        pipeline STRING, source STRING, max_adc_updt TIMESTAMP, updated_at TIMESTAMP)""")
    rows = spark.sql(f"""SELECT max_adc_updt FROM {WATERMARK_TABLE}
                         WHERE pipeline='{pipeline}' AND source='{source}'""").collect()
    return rows[0]["max_adc_updt"] if rows else None


def wm_set(pipeline, source, boundary):
    if boundary is None:
        return
    spark.sql(f"""MERGE INTO {WATERMARK_TABLE} t
      USING (SELECT '{pipeline}' pipeline, '{source}' source,
                    TIMESTAMP'{boundary}' max_adc_updt, current_timestamp() updated_at) s
      ON t.pipeline=s.pipeline AND t.source=s.source
      WHEN MATCHED THEN UPDATE SET t.max_adc_updt=s.max_adc_updt,t.updated_at=s.updated_at
      WHEN NOT MATCHED THEN INSERT *""")


def pinned_increment(source, pipeline):
    version = table_version(source)
    snap = spark.read.option("versionAsOf", version).table(source)
    boundary = snap.agg(F.max("ADC_UPDT").alias("boundary")).collect()[0]["boundary"]
    old = wm_get(pipeline, source)
    inc = snap
    if old is not None:
        inc = inc.where(F.col("ADC_UPDT") > F.lit(old - timedelta(hours=24)))
    if boundary is not None:
        inc = inc.where(F.col("ADC_UPDT") <= F.lit(boundary))
    return snap, inc, boundary, old, version


def latest_per_key(df, keys, ordering):
    from pyspark.sql.window import Window
    return (df.withColumn("_RN", F.row_number().over(Window.partitionBy(*keys).orderBy(*ordering)))
              .where("_RN=1").drop("_RN"))


def dq_dates(df, columns):
    out = df
    for c in columns:
        future = F.col(c).isNotNull() & (F.col(c) > (F.lit(RUN_AS_OF) + F.expr("INTERVAL 1 DAY")))
        sentinel = F.col(c).isNotNull() & (F.col(c) < F.lit("1901-01-01").cast("timestamp"))
        out = (out.withColumn(c + "_FUTURE_IND", future)
                  .withColumn(c + "_SENTINEL_IND", sentinel)
                  .withColumn(c + "_CLEAN", F.when(~future & ~sentinel, F.col(c))))
    return out


def keyed_upsert(target, key, df):
    df.createOrReplaceTempView("s3_identifier_upsert_src")
    return spark.sql(f"""MERGE INTO {target} t USING s3_identifier_upsert_src s
      ON t.{key}=s.{key}
      WHEN MATCHED AND t.ROW_HASH<>s.ROW_HASH THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")


def delete_removed_allowed_rows(target, changed_pks, allowed_snapshot_pks):
    removed = changed_pks.join(allowed_snapshot_pks, "SOURCE_PK", "left_anti")
    if removed.limit(1).count() == 0:
        return
    removed.createOrReplaceTempView("s3_identifier_removed_src")
    spark.sql(f"""MERGE INTO {target} t USING s3_identifier_removed_src s
      ON t.SOURCE_PK=s.SOURCE_PK WHEN MATCHED THEN DELETE""")


def apply_ig_tags(target):
    catalog, schema, table = target.split(".")
    direct = {"ALIAS_VALUE": (4, 2)}
    internal = {"PERSON_ID": (0, 1), "ENCNTR_ID": (0, 1)}
    lifecycle = {"BEG_EFFECTIVE_DT_TM": (1, 1), "END_EFFECTIVE_DT_TM": (1, 1)}
    cols = [r["col_name"] for r in spark.sql(f"DESCRIBE {target}").collect()
            if r["col_name"] and not r["col_name"].startswith("#")]
    for col in cols:
        risk, severity = direct.get(col, internal.get(col, lifecycle.get(col, (0, 0))))
        spark.sql(f"""ALTER TABLE {target} ALTER COLUMN `{col}`
                      SET TAGS ('ig_risk'='{risk}', 'ig_severity'='{severity}')""")
    tagged = spark.sql(f"""SELECT column_name, COUNT(DISTINCT tag_name) n
      FROM {catalog}.information_schema.column_tags
      WHERE schema_name='{schema}' AND table_name='{table}'
        AND tag_name IN ('ig_risk','ig_severity')
      GROUP BY column_name""")
    assert tagged.count() == len(cols), f"not all columns tagged on {target}"
    assert tagged.where("n<>2").limit(1).count() == 0, f"incomplete IG tags on {target}"


def transform(source, target, pipeline, pk_col, entity_col, type_col, type_map):
    snap, inc, boundary, old_wm, version = pinned_increment(source, pipeline)
    allowed = list(type_map)
    typed_snap = snap.where(F.col(type_col).cast("bigint").isin(allowed))
    changed_pks = inc.select(F.col(pk_col).cast("bigint").alias("SOURCE_PK")).distinct()
    affected = (inc.where(F.col(type_col).cast("bigint").isin(allowed))
                  .select(F.col(entity_col).cast("bigint").alias("ENTITY_ID"),
                          F.col(type_col).cast("bigint").alias("ALIAS_TYPE_CD")).distinct())
    if spark.catalog.tableExists(target):
        old_groups = (spark.table(target).join(changed_pks, "SOURCE_PK", "inner")
                      .select(F.col(entity_col).cast("bigint").alias("ENTITY_ID"),
                              F.col("ALIAS_TYPE_CD")).distinct())
        expired_groups = (spark.table(target)
            .where(F.col("CURRENT_IND") & F.col("END_EFFECTIVE_DT_TM").isNotNull()
                   & (F.col("END_EFFECTIVE_DT_TM") < F.lit(RUN_AS_OF)))
            .select(F.col(entity_col).cast("bigint").alias("ENTITY_ID"),
                    F.col("ALIAS_TYPE_CD")).distinct())
        affected = affected.union(old_groups).union(expired_groups).distinct()

    type_pairs = []
    for code, label in type_map.items():
        type_pairs.extend([F.lit(code), F.lit(label)])
    type_expr = F.create_map(*type_pairs)

    base = (
        typed_snap
        .select(F.col(pk_col).cast("bigint").alias("SOURCE_PK"),
                F.col(entity_col).cast("bigint").alias(entity_col),
                F.col(type_col).cast("bigint").alias("ALIAS_TYPE_CD"),
                F.col("ALIAS").alias("ALIAS_VALUE"),
                F.col("ALIAS_POOL_CD").cast("bigint").alias("ALIAS_POOL_CD"),
                F.col("ACTIVE_IND").cast("bigint").alias("ACTIVE_IND"),
                F.col("BEG_EFFECTIVE_DT_TM"), F.col("END_EFFECTIVE_DT_TM"),
                F.col("UPDT_CNT").cast("bigint").alias("UPDT_CNT"),
                F.col("UPDT_DT_TM"), F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"))
        .withColumn("ENTITY_ID", F.col(entity_col))
        .join(affected, ["ENTITY_ID", "ALIAS_TYPE_CD"], "inner")
        .drop("ENTITY_ID")
        .withColumn("ALIAS_TYPE", type_expr[F.col("ALIAS_TYPE_CD")])
        .withColumn("CURRENT_IND",
            (F.col("ACTIVE_IND") == 1) &
            (F.col("END_EFFECTIVE_DT_TM").isNull() |
             (F.col("END_EFFECTIVE_DT_TM") >= F.lit(RUN_AS_OF))))
    )

    base = latest_per_key(base, ["SOURCE_PK"], [
        F.col("SOURCE_ADC_UPDT").desc_nulls_last(),
        F.col("UPDT_CNT").desc_nulls_last(),
        F.col("UPDT_DT_TM").desc_nulls_last()])
    group_cols = [entity_col, "ALIAS_TYPE_CD"]
    multi = (base.where("CURRENT_IND")
        .groupBy(*group_cols)
        .agg(F.countDistinct("ALIAS_VALUE").alias("_CURRENT_DISTINCT_N"))
        .where("_CURRENT_DISTINCT_N>1")
        .select(*group_cols).withColumn("MULTI_ACTIVE_IND", F.lit(True)))
    out = (base.join(multi, group_cols, "left")
        .fillna(False, ["MULTI_ACTIVE_IND"]))
    out = dq_dates(out, ["BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM"])
    admin = {"UPDT_DT_TM", "SOURCE_ADC_UPDT", "PIPELINE_UPDT_DT_TM", "ROW_HASH"}
    hash_cols = [c for c in out.columns
                 if c not in admin and not c.endswith(("_FUTURE_IND", "_SENTINEL_IND", "_CLEAN"))]
    out = (out.withColumn("ROW_HASH", F.xxhash64(F.to_json(F.struct(*[F.col(c) for c in hash_cols]))))
              .withColumn("PIPELINE_UPDT_DT_TM", F.current_timestamp()))

    if not spark.catalog.tableExists(target):
        (out.limit(0).write.format("delta").mode("overwrite")
            .option("delta.enableChangeDataFeed", "true").saveAsTable(target))
    metrics = keyed_upsert(target, "SOURCE_PK", out)
    allowed_snapshot_pks = typed_snap.select(F.col(pk_col).cast("bigint").alias("SOURCE_PK")).distinct()
    delete_removed_allowed_rows(target, changed_pks, allowed_snapshot_pks)
    apply_ig_tags(target)
    print({"target": target, "source_version": version, "affected_groups": affected.count(),
           "source_rows": typed_snap.count(), "metrics": metrics})
    return {"source": source, "target": target, "pipeline": pipeline, "boundary": boundary,
            "old_wm": old_wm, "version": version, "entity_col": entity_col,
            "type_col": type_col, "type_map": type_map}


def gate_product(meta):
    source, target = meta["source"], meta["target"]
    entity_col, type_col, type_map = meta["entity_col"], meta["type_col"], meta["type_map"]
    snap = spark.read.option("versionAsOf", meta["version"]).table(source)
    expected = (snap.where(F.col(type_col).cast("bigint").isin(list(type_map)))
        .groupBy(F.col(type_col).cast("bigint").alias("typ")).count())
    actual = spark.table(target).groupBy(F.col("ALIAS_TYPE_CD").alias("typ")).count()
    mismatch = (expected.alias("e").join(actual.alias("a"), "typ", "full")
        .where(F.coalesce(F.col("e.count"), F.lit(-1)) != F.coalesce(F.col("a.count"), F.lit(-1))))
    assert mismatch.limit(1).count() == 0, f"row-count mismatch {target}"
    d = spark.table(target)
    assert d.groupBy("SOURCE_PK").count().where("count<>1").limit(1).count() == 0, "duplicate source key"
    assert d.where("CURRENT_IND AND (ACTIVE_IND<>1 OR (END_EFFECTIVE_DT_TM IS NOT NULL AND END_EFFECTIVE_DT_TM < current_timestamp()))").limit(1).count() == 0, "invalid CURRENT_IND"
    group_cols = [entity_col, "ALIAS_TYPE_CD"]
    expected_multi = (d.where("CURRENT_IND").groupBy(*group_cols)
        .agg(F.countDistinct("ALIAS_VALUE").alias("n")).where("n>1").select(*group_cols))
    actual_multi = d.where("MULTI_ACTIVE_IND").select(*group_cols).distinct()
    assert expected_multi.exceptAll(actual_multi).limit(1).count() == 0, "missing MULTI_ACTIVE_IND"
    assert actual_multi.exceptAll(expected_multi).limit(1).count() == 0, "spurious MULTI_ACTIVE_IND"
    link_table = "4_prod.bronze.map_person" if entity_col == "PERSON_ID" else "4_prod.bronze.map_encounter"
    link_keys = (spark.table(link_table).select(entity_col).distinct()
                 .withColumn("_LINKED", F.lit(1)))
    links = (d.select(entity_col).distinct()
        .join(link_keys, entity_col, "left")
        .agg(F.count("*").alias("n"), F.sum(F.coalesce("_LINKED", F.lit(0))).alias("ok"))
        .collect()[0])
    assert links["ok"] / links["n"] >= 0.99, f"link rate below 99%: {links}"
    for c in ("BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM"):
        for suffix in ("_FUTURE_IND", "_SENTINEL_IND", "_CLEAN"):
            assert c + suffix in d.columns, f"missing DQ column {c+suffix}"
    props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()}
    assert props.get("delta.enableChangeDataFeed") == "true", "CDF disabled"
    apply_ig_tags(target)
    summary = d.agg(F.count("*").alias("rows"),
                    F.sum(F.col("CURRENT_IND").cast("long")).alias("current_rows"),
                    F.countDistinct(F.when(F.col("MULTI_ACTIVE_IND"), F.col(entity_col))).alias("multi_entities")).collect()[0]
    print({"target": target, **summary.asDict(), "linked": links["ok"], "entities": links["n"]})


def current_meta(source, target, pipeline, entity_col, type_col, type_map):
    return {"source": source, "target": target, "pipeline": pipeline,
            "version": table_version(source), "entity_col": entity_col,
            "type_col": type_col, "type_map": type_map}


if ACTION == "gates":
    gate_product(current_meta(PERSON_SOURCE, PERSON_TARGET, "a10_patient_identifier",
                              "PERSON_ID", "PERSON_ALIAS_TYPE_CD", PERSON_TYPES))
    gate_product(current_meta(ENCNTR_SOURCE, ENCNTR_TARGET, "a10_encounter_identifier",
                              "ENCNTR_ID", "ENCNTR_ALIAS_TYPE_CD", ENCNTR_TYPES))
    dbutils.notebook.exit("A10 gates PASS")

PERSON_SOURCE_VERSIONS=_source_versions([PERSON_SOURCE])
ENCNTR_SOURCE_VERSIONS=_source_versions([ENCNTR_SOURCE])
if (
    _target_state_current(PERSON_TARGET,PERSON_SOURCE_VERSIONS)
    and _target_state_current(ENCNTR_TARGET,ENCNTR_SOURCE_VERSIONS)
):
    gate_product(current_meta(PERSON_SOURCE, PERSON_TARGET, "a10_patient_identifier",
                              "PERSON_ID", "PERSON_ALIAS_TYPE_CD", PERSON_TYPES))
    gate_product(current_meta(ENCNTR_SOURCE, ENCNTR_TARGET, "a10_encounter_identifier",
                              "ENCNTR_ID", "ENCNTR_ALIAS_TYPE_CD", ENCNTR_TYPES))
    dbutils.notebook.exit(json.dumps({
        "result":"NO_OP","target":TARGET_SCHEMA,"target_schema":TARGET_SCHEMA,
        "source_versions":{
            "map_patient_identifier":PERSON_SOURCE_VERSIONS,
            "map_encounter_identifier":ENCNTR_SOURCE_VERSIONS,
        },
    },sort_keys=True))

person_meta = transform(PERSON_SOURCE, PERSON_TARGET, "a10_patient_identifier",
                        "PERSON_ALIAS_ID", "PERSON_ID", "PERSON_ALIAS_TYPE_CD", PERSON_TYPES)
encntr_meta = transform(ENCNTR_SOURCE, ENCNTR_TARGET, "a10_encounter_identifier",
                        "ENCNTR_ALIAS_ID", "ENCNTR_ID", "ENCNTR_ALIAS_TYPE_CD", ENCNTR_TYPES)
gate_product(person_meta)
gate_product(encntr_meta)
for meta in (person_meta, encntr_meta):
    if meta["boundary"] is not None and (meta["old_wm"] is None or meta["boundary"] > meta["old_wm"]):
        wm_set(meta["pipeline"], meta["source"], meta["boundary"])

_record_source_versions(PERSON_TARGET,PERSON_SOURCE_VERSIONS)
_record_source_versions(ENCNTR_TARGET,ENCNTR_SOURCE_VERSIONS)

spark.sql(f"""COMMENT ON TABLE {PERSON_TARGET} IS
'Grain: one Millennium person-alias source row for MRN or NHS identifier types. Bronze retains direct identifiers; serve-time governance removes or transforms them for researcher provision. CURRENT_IND requires ACTIVE_IND=1 and an open effective end. MULTI_ACTIVE_IND exposes concurrent identifiers without an arbitrary winner. Every column carries ig_risk and ig_severity tags.'""")
spark.sql(f"""COMMENT ON TABLE {ENCNTR_TARGET} IS
'Grain: one Millennium encounter-alias source row for FIN, encounter-MRN or visit identifier types. Bronze retains direct identifiers; serve-time governance removes or transforms them for researcher provision. CURRENT_IND requires ACTIVE_IND=1 and an open effective end. MULTI_ACTIVE_IND exposes concurrent identifiers without an arbitrary winner. Every column carries ig_risk and ig_severity tags.'""")
print("A10 build and gates complete")

# COMMAND ----------

# PROMOTION RUNBOOK — HUMAN GATED
# Promote to 4_prod.bronze, not a special restricted schema. Preserve all column IG tags.
# Run the >100M benchmark protocol on the encounter source, rerun gates against production,
# register after map_person/map_encounter with retries=0, and verify serve-time policies consume
# ig_risk/ig_severity before researcher provision.

dbutils.notebook.exit(json.dumps({"result": "BUILT", "target": TARGET_SCHEMA, "target_schema": TARGET_SCHEMA}, sort_keys=True))


