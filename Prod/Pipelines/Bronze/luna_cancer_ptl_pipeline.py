# Databricks notebook source
"""B3 LUNA Pathfinder cancer PTL and narrow linkage bronze builder (dev only)."""

# COMMAND ----------

import json
import re
import traceback
import uuid
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType


# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 1116545487244952)
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


_ensure_widget("run_as_of", "")
_run_as_of_input = dbutils.widgets.get("run_as_of").strip()
RUN_AS_OF = _run_as_of_input or str(spark.sql("SELECT current_timestamp() ts").first()["ts"])
RUN_AS_OF_COL = F.lit(RUN_AS_OF).cast("timestamp")
RUN_ID = str(uuid.uuid4())
PIPELINE = "b3_luna_cancer_ptl_v1_1"
# TARGET_SCHEMA resolved by the release guard above.

SRC_PATHFINDER = "4_prod.raw.luna_pathfinder_ptl"
SRC_LINKAGE = "4_prod.raw.luna_cancer_ptl"
SRC_PATIENT = "4_prod.raw.luna_patient_core"
SRC_RTT = "4_prod.raw.luna_rtt_core"
SRC_REFERRAL = "4_prod.raw.luna_referral_core"
SRC_REFERRAL_MAP = "4_prod.bronze.map_referral"
SRC_PATHWAY_MAP = "4_prod.bronze.map_rtt_pathway"
PERSON_ALIAS = "4_prod.raw.mill_person_alias"
SRC_PERSON = "4_prod.raw.mill_person"

TARGET_PATHFINDER = f"{TARGET_SCHEMA}.map_cancer_ptl"
TARGET_LINKAGE = f"{TARGET_SCHEMA}.map_cancer_ptl_linkage"

MRN_ALIAS_TYPE = 10
NHS_ALIAS_TYPE = 18
SENTINEL_FLOOR = "1901-01-01"

PROMOTION_RUNBOOK = {
    "human_gated": True,
    "steps": [
        "Promote both reviewed tables to 4_prod.bronze with retries=0.",
        "Reapply IG tags in production and run ig_tag_gate there.",
        "Add the weekly LUNA PTL bronze step only after the measured 16.2M-row runtime review.",
        "Notify journey consumers that map_cancer_ptl is the authoritative cancer-pathway plane.",
        "Do not execute any 6_mgmt, 4_prod, or 3_lookup write from this notebook.",
    ],
}


# COMMAND ----------

def qname(name):
    return ".".join("`" + part.replace("`", "``") + "`" for part in name.split("."))


def table_version(table):
    return int(spark.sql(f"DESCRIBE HISTORY {qname(table)} LIMIT 1").first()["version"])


def capture_versions(tables):
    return {table: table_version(table) for table in tables}


def read_pinned(table, versions):
    return spark.read.option("versionAsOf", versions[table]).table(table)


def _target_due_check(target, versions):
    if not spark.catalog.tableExists(target):
        return True
    props = spark.sql(f"DESCRIBE DETAIL {qname(target)}").first()["properties"] or {}
    previous = {k: int(v) for k, v in json.loads(
        props.get("s6.source_versions_json", "{}")).items()}
    return previous != {k: int(v) for k, v in versions.items()}


def _record_target_versions(target, versions):
    payload = json.dumps(versions, sort_keys=True, separators=(",", ":")).replace("'", "''")
    spark.sql(
        f"ALTER TABLE {qname(target)} SET TBLPROPERTIES "
        f"('s6.source_versions_json'='{payload}', 'b3.run_as_of'='{RUN_AS_OF}')"
    )


def _snake(name):
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", str(name))
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"[^A-Za-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_").upper()
    value = value.replace("_DATE_TIME", "_DT_TM")
    return value


def source_select(df, overrides=None):
    overrides = overrides or {}
    return df.select(*[
        F.col(c).alias(overrides.get(c, _snake(c))) for c in df.columns
    ])


def dq_columns(df, date_cols):
    types = {field.name: field.dataType for field in df.schema.fields}
    out = df
    for c in date_cols:
        dtype = types[c]
        value = F.col(c)
        future = value.cast("timestamp") > RUN_AS_OF_COL
        sentinel = value.cast("timestamp") < F.lit(SENTINEL_FLOOR).cast("timestamp")
        out = (
            out.withColumn(
                f"{c}_FUTURE_IND",
                F.when(value.isNull(), F.lit(None).cast("boolean")).otherwise(future),
            )
            .withColumn(
                f"{c}_SENTINEL_IND",
                F.when(value.isNull(), F.lit(None).cast("boolean")).otherwise(sentinel),
            )
            .withColumn(
                f"{c}_CLEAN",
                F.when(future | sentinel, F.lit(None).cast(dtype)).otherwise(value),
            )
        )
    return out


def dq_all_clinical(df, admin_stamps):
    cols = [
        field.name for field in df.schema.fields
        if isinstance(field.dataType, (TimestampType, DateType))
        and field.name not in admin_stamps
        and not field.name.endswith("_CLEAN")
    ]
    return dq_columns(df, cols), cols


def measure_future_counts(df, columns):
    if not columns:
        return {}
    row = df.agg(*[
        F.sum(F.when(F.col(c).cast("timestamp") > RUN_AS_OF_COL, 1).otherwise(0)).alias(c)
        for c in columns
    ]).first().asDict()
    return {c: int(row[c] or 0) for c in columns}


HASH_EXCLUDE = {
    "ROW_HASH", "PIPELINE_UPDT_DT_TM", "SOURCE_PRESENT_IND",
    "SOURCE_ABSENT_DETECTED_TS", "ADC_UPDT",
}


def _row_hash(df):
    cols = sorted(c for c in df.columns if c not in HASH_EXCLUDE)
    payload = F.struct(*[
        F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")).alias(c) for c in cols
    ])
    return df.withColumn("ROW_HASH", F.sha2(F.to_json(payload), 256))


def verify_unique_key(df, keys):
    null_condition = reduce(lambda a, b: a | b, [F.col(k).isNull() for k in keys])
    row = df.agg(
        F.count("*").alias("n"),
        F.countDistinct(F.struct(*[F.col(k) for k in keys])).alias("d"),
        F.sum(F.when(null_condition, 1).otherwise(0)).alias("nulls"),
    ).first()
    assert int(row["n"]) == int(row["d"]) and int(row["nulls"] or 0) == 0, (
        f"key violation {keys}: n={row['n']} distinct={row['d']} null_rows={row['nulls']}"
    )
    return int(row["n"])


def verify_joinable_key_unique(df, keys):
    joinable = df
    for key in keys:
        joinable = joinable.where(F.col(key).isNotNull())
    row = joinable.agg(
        F.count("*").alias("n"),
        F.countDistinct(F.struct(*[F.col(k) for k in keys])).alias("d"),
    ).first()
    assert int(row["n"]) == int(row["d"]), (
        f"joinable key duplicates {keys}: n={row['n']} distinct={row['d']}"
    )
    return int(row["n"])


def _latest_merge_metrics(target):
    row = spark.sql(
        f"SELECT operation, operationMetrics FROM (DESCRIBE HISTORY {qname(target)}) "
        "WHERE operation IN ('MERGE','CREATE OR REPLACE TABLE AS SELECT','WRITE') "
        "ORDER BY version DESC LIMIT 1"
    ).first()
    return {"operation": row["operation"], **dict(row["operationMetrics"] or {})} if row else {}


def merge_with_tombstones(df, target, key_cols):
    source = (
        _row_hash(df)
        .withColumn("PIPELINE_UPDT_DT_TM", RUN_AS_OF_COL)
        .withColumn("SOURCE_PRESENT_IND", F.lit(True))
        .withColumn("SOURCE_ABSENT_DETECTED_TS", F.lit(None).cast("timestamp"))
    )
    source_rows = verify_unique_key(source, key_cols)
    if not spark.catalog.tableExists(target):
        (
            source.write.format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .option("overwriteSchema", "true")
            .mode("overwrite")
            .saveAsTable(target)
        )
        spark.sql(
            f"ALTER TABLE {qname(target)} SET TBLPROPERTIES "
            "('delta.enableChangeDataFeed'='true','delta.appendOnly'='false')"
        )
        metrics = {"operation": "CREATE", "numOutputRows": str(source_rows)}
    else:
        target_schema = spark.table(target).schema
        target_cols = {field.name for field in target_schema.fields}
        additions = [field for field in source.schema.fields if field.name not in target_cols]
        if additions:
            ddl = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in additions)
            spark.sql(f"ALTER TABLE {qname(target)} ADD COLUMNS ({ddl})")
            target_schema = spark.table(target).schema
        aligned = source
        for field in target_schema.fields:
            if field.name not in aligned.columns:
                aligned = aligned.withColumn(field.name, F.lit(None).cast(field.dataType))
        aligned = aligned.select(*[field.name for field in target_schema.fields])
        condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in key_cols)
        (
            DeltaTable.forName(spark, target).alias("t")
            .merge(aligned.alias("s"), condition)
            .whenMatchedUpdateAll(
                condition=(
                    "NOT (t.`ROW_HASH` <=> s.`ROW_HASH`) "
                    "OR NOT COALESCE(t.`SOURCE_PRESENT_IND`, false)"
                )
            )
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceUpdate(
                condition="COALESCE(t.`SOURCE_PRESENT_IND`, true) = true",
                set={
                    "SOURCE_PRESENT_IND": "false",
                    "SOURCE_ABSENT_DETECTED_TS": f"CAST('{RUN_AS_OF}' AS TIMESTAMP)",
                    "PIPELINE_UPDT_DT_TM": f"CAST('{RUN_AS_OF}' AS TIMESTAMP)",
                },
            )
            .execute()
        )
        metrics = _latest_merge_metrics(target)
    return {"target": target, "source_rows": source_rows, **metrics}


def table_fingerprint(table, exclude=("PIPELINE_UPDT_DT_TM", "ADC_UPDT")):
    cols = [c for c in spark.table(table).columns if c not in exclude]
    return str(
        spark.table(table).select(
            F.sum(F.xxhash64(F.to_json(F.struct(*[F.col(c) for c in cols])))
                  .cast("decimal(38,0)")).alias("fp")
        ).first()["fp"]
    )


_TAG_CACHE = {}


def lookup_counterpart_tags(col_name):
    if col_name in _TAG_CACHE:
        return _TAG_CACHE[col_name]
    col_lit = col_name.replace("'", "''")
    rows = (
        spark.sql(f"""
            SELECT MAX(CASE WHEN tag_name='ig_risk' THEN tag_value END) r,
                   MAX(CASE WHEN tag_name='ig_severity' THEN tag_value END) s
            FROM `4_prod`.information_schema.column_tags
            WHERE schema_name='bronze' AND upper(column_name)=upper('{col_lit}')
            GROUP BY table_name
        """)
        .groupBy("r", "s").count()
        .orderBy(F.desc("count"), F.asc("r"), F.asc("s"))
        .collect()
    )
    result = (rows[0]["r"], rows[0]["s"]) if rows else None
    _TAG_CACHE[col_name] = result
    return result


def ig_tag_table(table, tag_map, default=("0", "0")):
    for c in spark.table(table).columns:
        if c in tag_map:
            risk, severity = tag_map[c]
        else:
            found = lookup_counterpart_tags(c)
            risk, severity = found if found else default
            if not found:
                print(f"IG-TAG DEFAULTED {table}.{c} -> {default} — REVIEW")
        spark.sql(
            f"ALTER TABLE {qname(table)} ALTER COLUMN `{c.replace('`','``')}` "
            f"SET TAGS ('ig_risk'='{risk}','ig_severity'='{severity}')"
        )


def ig_tag_gate(table):
    cat, schema, name = table.split(".")
    row = spark.sql(f"""
        WITH cols AS (
          SELECT column_name FROM `{cat}`.information_schema.columns
          WHERE table_schema='{schema}' AND table_name='{name}'
        ), risk AS (
          SELECT DISTINCT column_name FROM `{cat}`.information_schema.column_tags
          WHERE schema_name='{schema}' AND table_name='{name}' AND tag_name='ig_risk'
        ), severity AS (
          SELECT DISTINCT column_name FROM `{cat}`.information_schema.column_tags
          WHERE schema_name='{schema}' AND table_name='{name}' AND tag_name='ig_severity'
        )
        SELECT SUM(CASE WHEN r.column_name IS NULL OR s.column_name IS NULL THEN 1 ELSE 0 END) missing
        FROM cols c LEFT JOIN risk r USING(column_name) LEFT JOIN severity s USING(column_name)
    """).first()
    assert int(row["missing"] or 0) == 0, f"{table}: columns missing IG tags"


def apply_comments(table, table_comment, derived_comments=None):
    table_text = table_comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {qname(table)} IS '{table_text}'")
    derived_comments = derived_comments or {}
    for c in spark.table(table).columns:
        comment = derived_comments.get(c, f"B3 bronze field {c}; source value retained unless documented as derived.")
        spark.sql(
            f"ALTER TABLE {qname(table)} ALTER COLUMN `{c.replace('`','``')}` "
            f"COMMENT '{comment.replace(chr(39), chr(39)*2)}'"
        )


def assert_dq_triplets(table, temporal_columns):
    cols = set(spark.table(table).columns)
    missing = [
        c + suffix for c in temporal_columns
        for suffix in ("_FUTURE_IND", "_SENTINEL_IND", "_CLEAN")
        if c + suffix not in cols
    ]
    assert not missing, f"{table}: missing DQ triplets {missing}"


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
        [F.substring(norm_col, index + 1, 1).cast("int") * (10 - index) for index in range(9)],
    )
    check = F.lit(11) - (weighted % 11)
    expected = F.when(check == 11, F.lit(0)).otherwise(check)
    return norm_col.isNotNull() & (check != 10) & (
        F.substring(norm_col, 10, 1).cast("int") == expected
    )


def mrn_norm(value):
    digits = F.regexp_replace(_column(value).cast("string"), r"[^0-9]", "")
    stripped = F.regexp_replace(digits, r"^0+", "")
    return F.when(F.length(digits).between(1, 20) & (stripped != ""), stripped)


PERSON_ALIAS_PINNED = None


def _alias_lookup(alias_type_cd, prefix, historical_latest=False):
    alias_digits = F.regexp_replace(F.col("ALIAS").cast("string"), r"[^0-9]", "")
    alias_norm = (
        F.regexp_replace(alias_digits, r"^0+", "")
        if alias_type_cd == MRN_ALIAS_TYPE
        else F.when(F.length(alias_digits) == 10, alias_digits)
    )
    source = PERSON_ALIAS_PINNED.where(
        (F.col("ACTIVE_IND") == 1)
        & (F.col("PERSON_ALIAS_TYPE_CD") == alias_type_cd)
        & (F.col("BEG_EFFECTIVE_DT_TM").isNull()
           | (F.col("BEG_EFFECTIVE_DT_TM") <= RUN_AS_OF_COL))
    )
    if not historical_latest:
        source = source.where(
            F.col("END_EFFECTIVE_DT_TM").isNull()
            | (F.col("END_EFFECTIVE_DT_TM") > RUN_AS_OF_COL)
        )
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
            F.when(F.col(f"{prefix}_PERSON_COUNT") == 1, F.col(f"{prefix}_PERSON_ID_RAW")),
        )
        .drop(f"{prefix}_PERSON_ID_RAW")
    )


def resolve_persons(df, mrn_col=None, nhs_col=None):
    out = df
    if mrn_col and mrn_col in out.columns:
        out = out.withColumn("_MRN_NORM", mrn_norm(mrn_col))
        for prefix, historical in (("MRN_CUR", False), ("MRN_HIST", True)):
            lookup = _alias_lookup(MRN_ALIAS_TYPE, prefix, historical)
            out = out.join(lookup, out["_MRN_NORM"] == lookup["ALIAS_NORM"], "left").drop(
                lookup["ALIAS_NORM"])
    else:
        for name in (
            "MRN_CUR_PERSON_ID", "MRN_CUR_PERSON_COUNT",
            "MRN_HIST_PERSON_ID", "MRN_HIST_PERSON_COUNT",
        ):
            out = out.withColumn(name, F.lit(None).cast("long"))

    if nhs_col and nhs_col in out.columns:
        out = out.withColumn("_NHS_NORM", nhs_norm(nhs_col)).withColumn(
            "NHS_NUMBER_VALID_IND", nhs_valid(F.col("_NHS_NORM")))
        for prefix, historical in (("NHS_CUR", False), ("NHS_HIST", True)):
            lookup = _alias_lookup(NHS_ALIAS_TYPE, prefix, historical)
            out = out.join(
                lookup,
                out["NHS_NUMBER_VALID_IND"] & (out["_NHS_NORM"] == lookup["ALIAS_NORM"]),
                "left",
            ).drop(lookup["ALIAS_NORM"])
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
            "AMBIGUOUS",
        )
        .otherwise("UNMATCHED")
    )
    return (
        out.withColumn("LINKAGE_STATUS", status)
        .withColumn(
            "PERSON_ID",
            F.when(
                F.col("LINKAGE_STATUS").isin("MATCHED_BOTH", "MATCHED_MRN", "MATCHED_NHS"),
                F.coalesce(mrn_person, nhs_person),
            ).cast("long"),
        )
        .withColumn(
            "LINKAGE_METHOD",
            F.when(F.col("LINKAGE_STATUS").isin("MATCHED_BOTH", "CONFLICT"), "MRN+NHS")
            .when(F.col("LINKAGE_STATUS") == "MATCHED_MRN", "MRN")
            .when(F.col("LINKAGE_STATUS") == "MATCHED_NHS", "NHS"),
        )
        .withColumn("LINKAGE_HISTORICAL_FALLBACK_IND", F.coalesce(fallback_used, F.lit(False)))
        .withColumn("LINKAGE_FALLBACK_CONFLICT_IND", F.coalesce(fallback_conflict, F.lit(False)))
        .drop(
            "_MRN_NORM", "_NHS_NORM",
            "MRN_CUR_PERSON_ID", "MRN_CUR_PERSON_COUNT",
            "MRN_HIST_PERSON_ID", "MRN_HIST_PERSON_COUNT",
            "NHS_CUR_PERSON_ID", "NHS_CUR_PERSON_COUNT",
            "NHS_HIST_PERSON_ID", "NHS_HIST_PERSON_COUNT",
        )
    )



def published_person_arms(referral_map, pathway_map):
    referral = (
        referral_map.where(F.coalesce(F.col("SOURCE_PRESENT_IND"), F.lit(True)))
        .groupBy("SOURCE_SYSTEM_OID", "REFERRAL_OID")
        .agg(
            F.countDistinct("PERSON_ID").alias("_L_REF_COUNT"),
            F.max("PERSON_ID").cast("long").alias("_L_REF_RAW"),
            F.max("PERSON_LINK_METHOD").alias("_L_REF_METHOD"),
        )
        .withColumn("_L_REF_PERSON", F.when(F.col("_L_REF_COUNT") == 1, F.col("_L_REF_RAW")))
        .drop("_L_REF_RAW")
    )
    pathway = (
        pathway_map.where(F.coalesce(F.col("SOURCE_PRESENT_IND"), F.lit(True)))
        .groupBy("SOURCE_SYSTEM_OID", "PATHWAY_OID")
        .agg(
            F.countDistinct("PERSON_ID").alias("_L_PW_COUNT"),
            F.max("PERSON_ID").cast("long").alias("_L_PW_RAW"),
            F.max("PERSON_LINK_METHOD").alias("_L_PW_METHOD"),
        )
        .withColumn("_L_PW_PERSON", F.when(F.col("_L_PW_COUNT") == 1, F.col("_L_PW_RAW")))
        .drop("_L_PW_RAW")
    )
    return referral, pathway



# COMMAND ----------

def add_person_consensus(df):
    ref, path, ident = F.col("_L_REF_PERSON"), F.col("_L_PW_PERSON"), F.col("_I_PERSON")
    arm_count = (
        F.when(ref.isNotNull(), 1).otherwise(0)
        + F.when(path.isNotNull(), 1).otherwise(0)
        + F.when(ident.isNotNull(), 1).otherwise(0)
    )
    conflict = (
        (ref.isNotNull() & path.isNotNull() & (ref != path))
        | (ref.isNotNull() & ident.isNotNull() & (ref != ident))
        | (path.isNotNull() & ident.isNotNull() & (path != ident))
    )
    agreed = F.coalesce(ref, path, ident)
    status = (
        F.when(conflict, "CONFLICT_ARMS")
        .when(arm_count >= 2, "CONSENSUS_ALL")
        .when(ref.isNotNull(), "LINKED_REFERRAL")
        .when(path.isNotNull(), "LINKED_PATHWAY")
        .when(ident.isNotNull(), "LINKED_IDENTIFIER")
        .when(F.col("_I_STATUS").isin("AMBIGUOUS", "CONFLICT"), "IDENTIFIER_AMBIGUOUS")
        .when(F.col("SOURCE_SYSTEM_OID").isNull(), "NAMESPACE_MISSING_UNLINKED")
        .when(~F.coalesce(F.col("_PATIENT_SPINE_MATCH"), F.lit(False)), "PATIENT_SPINE_MISS")
        .otherwise("UNMATCHED")
    )
    method = F.when(conflict, F.lit("conflict")).otherwise(F.concat_ws(
        "+",
        F.when(ref.isNotNull(), F.lit("referral")),
        F.when(path.isNotNull(), F.lit("pathway")),
        F.when(ident.isNotNull(), F.concat(F.lit("identifier:"), F.col("_I_STATUS"))),
    ))
    return (
        df.withColumn("PERSON_LINK_STATUS", status)
        .withColumn("PERSON_ID", F.when(~conflict & (arm_count > 0), agreed).cast("long"))
        .withColumn("PERSON_LINK_METHOD", F.when(F.length(method) > 0, method).otherwise(F.lit("none")))
        .withColumn("IDENTIFIER_LINK_STATUS", F.col("_I_STATUS"))
        .drop(
            "_L_REF_PERSON", "_L_PW_PERSON", "_I_PERSON", "_I_STATUS", "_I_METHOD",
            "_L_REF_COUNT", "_L_PW_COUNT", "_L_REF_METHOD", "_L_PW_METHOD",
            "_PATIENT_SPINE_MATCH", "_I_MRN", "_I_NHS",
        )
    )


def physical_key_gate(pathfinder_raw):
    row = pathfinder_raw.agg(
        F.count("*").alias("n"),
        F.count("PTLUniqueID").alias("filled"),
        F.countDistinct("PTLUniqueID").alias("d"),
    ).first()
    assert row["n"] == row["filled"] == row["d"], (
        f"PTLUniqueID physical key violated: n={row['n']} filled={row['filled']} "
        f"distinct={row['d']} — a namespace-duplicated PTLUniqueID would corrupt "
        "tombstone identity. STOP; rekey on the composite."
    )
    return int(row["n"])


def spine_link_status(namespace, key, matched):
    return (
        F.when(namespace.isNull(), "NAMESPACE_MISSING")
        .when(key.isNull(), "KEY_MISSING")
        .when(F.coalesce(matched, F.lit(False)), "MATCHED")
        .otherwise("UNMATCHED")
    )


def pathfinder_gates(source_rows, temporal_columns, future_counts):
    active = spark.table(TARGET_PATHFINDER).where(F.col("SOURCE_PRESENT_IND"))
    assert verify_unique_key(active, ["PTL_UNIQUE_ID"]) == source_rows
    status_nulls = active.where(F.col("PERSON_LINK_STATUS").isNull()).count()
    assert status_nulls == 0, f"{TARGET_PATHFINDER}: {status_nulls} null person statuses"
    assert_dq_triplets(TARGET_PATHFINDER, temporal_columns)
    for c, expected in future_counts.items():
        actual = active.where(F.col(f"{c}_FUTURE_IND") == True).count()
        assert actual == expected, f"{c}: published future={actual}, pinned source future={expected}"
    ig_tag_gate(TARGET_PATHFINDER)
    rows = int(active.count())
    return {
        "rows": rows,
        "source_key_status": {
            str(r["SOURCE_KEY_STATUS"]): int(r["count"])
            for r in active.groupBy("SOURCE_KEY_STATUS").count().collect()
        },
        "person_status_counts": {
            str(r["PERSON_LINK_STATUS"]): int(r["count"])
            for r in active.groupBy("PERSON_LINK_STATUS").count().collect()
        },
        "conflict_arms": active.where(F.col("PERSON_LINK_STATUS") == "CONFLICT_ARMS").count(),
        "parent_orphans": active.where(
            F.col("PARENT_PTL_UNIQUE_ID").isNotNull()
            & ~F.coalesce(F.col("PARENT_PRESENT_IND"), F.lit(False))
        ).count(),
        "patient_spine_matches": active.where(F.col("PATIENT_SPINE_IND") == True).count(),
        "pathway_spine_matches": active.where(F.col("PATHWAY_SPINE_IND") == True).count(),
        "referral_spine_matches": active.where(F.col("REFERRAL_SPINE_IND") == True).count(),
        "future_counts": future_counts,
        "fingerprint": table_fingerprint(TARGET_PATHFINDER),
    }


def build_pathfinder(versions):
    global PERSON_ALIAS_PINNED
    raw = read_pinned(SRC_PATHFINDER, versions)
    source_rows = physical_key_gate(raw)
    due = _target_due_check(TARGET_PATHFINDER, versions)
    if not due:
        active = spark.table(TARGET_PATHFINDER).where(F.col("SOURCE_PRESENT_IND"))
        temporal = sorted(
            c[:-len("_FUTURE_IND")] for c in active.columns if c.endswith("_FUTURE_IND")
        )
        future_counts = {
            c: active.where(F.col(f"{c}_FUTURE_IND") == True).count()
            for c in temporal
        }
        return {
            "mode": "NO_OP",
            "write": None,
            "gate": pathfinder_gates(source_rows, temporal, future_counts),
        }

    selected = source_select(raw).withColumn(
        "SOURCE_KEY_STATUS",
        F.when(F.col("SOURCE_SYSTEM_OID").isNull(), "NAMESPACE_MISSING").otherwise("COMPLETE"),
    )

    parent_keys = selected.select(
        F.col("PTL_UNIQUE_ID").alias("_PARENT_KEY"),
        F.lit(True).alias("_PARENT_MATCH"),
    )
    selected = (
        selected.join(
            parent_keys,
            selected["PARENT_PTL_UNIQUE_ID"] == parent_keys["_PARENT_KEY"],
            "left",
        )
        .withColumn(
            "PARENT_PRESENT_IND",
            F.when(F.col("PARENT_PTL_UNIQUE_ID").isNull(), F.lit(None).cast("boolean"))
            .otherwise(F.coalesce(F.col("_PARENT_MATCH"), F.lit(False))),
        )
        .drop("_PARENT_KEY", "_PARENT_MATCH")
    )
    parent_orphans = selected.where(
        F.col("PARENT_PTL_UNIQUE_ID").isNotNull() & ~F.col("PARENT_PRESENT_IND")
    ).count()

    patient_raw = read_pinned(SRC_PATIENT, versions)
    verify_joinable_key_unique(patient_raw, ["SourceSystemOID", "PatientOID"])
    patient = patient_raw.select(
        F.col("SourceSystemOID").cast("long").alias("SOURCE_SYSTEM_OID"),
        F.col("PatientOID").cast("long").alias("PATIENT_OID"),
        F.col("LocalPatientID").cast("string").alias("_I_MRN"),
        F.col("NHSNumber").cast("string").alias("_I_NHS"),
        F.lit(True).alias("_PATIENT_SPINE_MATCH"),
    )
    rtt = read_pinned(SRC_RTT, versions).select(
        F.col("SourceSystemOID").cast("long").alias("SOURCE_SYSTEM_OID"),
        F.col("PathwayOID").cast("long").alias("PATHWAY_OID"),
    ).distinct().withColumn("_PATHWAY_SPINE_MATCH", F.lit(True))
    referral = read_pinned(SRC_REFERRAL, versions).select(
        F.col("SourceSystemOID").cast("long").alias("SOURCE_SYSTEM_OID"),
        F.col("ReferralOID").cast("long").alias("REFERRAL_OID"),
    ).distinct().withColumn("_REFERRAL_SPINE_MATCH", F.lit(True))

    referral_map = read_pinned(SRC_REFERRAL_MAP, versions)
    pathway_map = read_pinned(SRC_PATHWAY_MAP, versions)
    ref_arm, path_arm = published_person_arms(referral_map, pathway_map)
    PERSON_ALIAS_PINNED = read_pinned(PERSON_ALIAS, versions)

    joined = (
        selected.join(patient, ["SOURCE_SYSTEM_OID", "PATIENT_OID"], "left")
        .join(rtt, ["SOURCE_SYSTEM_OID", "PATHWAY_OID"], "left")
        .join(referral, ["SOURCE_SYSTEM_OID", "REFERRAL_OID"], "left")
        .join(ref_arm, ["SOURCE_SYSTEM_OID", "REFERRAL_OID"], "left")
        .join(path_arm, ["SOURCE_SYSTEM_OID", "PATHWAY_OID"], "left")
        .withColumn(
            "PATIENT_SPINE_IND",
            F.when(F.col("SOURCE_SYSTEM_OID").isNull(), F.lit(None).cast("boolean"))
            .otherwise(F.coalesce(F.col("_PATIENT_SPINE_MATCH"), F.lit(False))),
        )
        .withColumn(
            "PATHWAY_SPINE_IND",
            F.when(F.col("SOURCE_SYSTEM_OID").isNull(), F.lit(None).cast("boolean"))
            .otherwise(F.coalesce(F.col("_PATHWAY_SPINE_MATCH"), F.lit(False))),
        )
        .withColumn(
            "REFERRAL_SPINE_IND",
            F.when(F.col("SOURCE_SYSTEM_OID").isNull(), F.lit(None).cast("boolean"))
            .otherwise(F.coalesce(F.col("_REFERRAL_SPINE_MATCH"), F.lit(False))),
        )
        .withColumn(
            "PATIENT_SPINE_LINK_STATUS",
            spine_link_status(F.col("SOURCE_SYSTEM_OID"), F.col("PATIENT_OID"),
                              F.col("_PATIENT_SPINE_MATCH")),
        )
        .withColumn(
            "PATHWAY_SPINE_LINK_STATUS",
            spine_link_status(F.col("SOURCE_SYSTEM_OID"), F.col("PATHWAY_OID"),
                              F.col("_PATHWAY_SPINE_MATCH")),
        )
        .withColumn(
            "REFERRAL_SPINE_LINK_STATUS",
            spine_link_status(F.col("SOURCE_SYSTEM_OID"), F.col("REFERRAL_OID"),
                              F.col("_REFERRAL_SPINE_MATCH")),
        )
        .drop("_PATHWAY_SPINE_MATCH", "_REFERRAL_SPINE_MATCH")
    )
    resolved = (
        resolve_persons(joined, "_I_MRN", "_I_NHS")
        .withColumnRenamed("PERSON_ID", "_I_PERSON")
        .withColumnRenamed("LINKAGE_STATUS", "_I_STATUS")
        .withColumnRenamed("LINKAGE_METHOD", "_I_METHOD")
    )
    consensus = add_person_consensus(resolved)
    dq_df, temporal = dq_all_clinical(consensus, {"ADC_UPDT"})
    future_counts = measure_future_counts(consensus, temporal)

    write = merge_with_tombstones(dq_df, TARGET_PATHFINDER, ["PTL_UNIQUE_ID"])
    apply_comments(
        TARGET_PATHFINDER,
        "Authoritative LUNA cancer pathway tracker under the 2026-08-14 owner ruling. "
        "One physical row per PTL_UNIQUE_ID; logical source key is "
        "(SOURCE_SYSTEM_OID, PTL_UNIQUE_ID), with NAMESPACE_MISSING rows retained. "
        "ARCHIVED_PATHWAY is descriptive source state, never a filter. Weekly snapshot "
        "tombstones and hash-gated MERGE provide delta-only CDF semantics.",
        {
            "SOURCE_KEY_STATUS": "COMPLETE or NAMESPACE_MISSING; missing namespaces publish and are never dropped.",
            "ARCHIVED_PATHWAY": "Source archived-pathway state retained verbatim; never used as a publication gate.",
            "PARENT_PRESENT_IND": "Whether a non-null parent PTL_UNIQUE_ID exists in the pinned source snapshot.",
            "PERSON_LINK_STATUS": "Consensus-safe published linkage status; conflicting arms never publish PERSON_ID.",
            "SOURCE_PRESENT_IND": "False only when a formerly published physical key disappears from the weekly snapshot.",
        },
    )
    explicit = {}
    for c in spark.table(TARGET_PATHFINDER).columns:
        if c == "PERSON_ID" or c.endswith("_OID") or c.endswith("_PRID") or c in {
            "PTL_UNIQUE_ID", "PARENT_PTL_UNIQUE_ID", "SPECIALTY", "TREATMENT_FUNCTION",
            "SITE", "SITE_GROUP", "LEAD_CLINICIAN", "DIVISION",
        }:
            explicit[c] = ("4", "2")
    ig_tag_table(TARGET_PATHFINDER, explicit)
    gate = pathfinder_gates(source_rows, temporal, future_counts)
    assert gate["parent_orphans"] == parent_orphans
    _record_target_versions(TARGET_PATHFINDER, versions)
    return {"mode": "BUILD", "write": write, "gate": gate}


def linkage_gates(source_rows, temporal_columns):
    active = spark.table(TARGET_LINKAGE).where(F.col("SOURCE_PRESENT_IND"))
    assert verify_unique_key(active, ["ROW_ID"]) == source_rows
    invalid = active.where(
        F.col("PERSON_ID").isNotNull() & ~F.col("PERSON_VALID_IND")
    ).count()
    assert_dq_triplets(TARGET_LINKAGE, temporal_columns)
    ig_tag_gate(TARGET_LINKAGE)
    return {
        "rows": source_rows,
        "invalid_person_ids": invalid,
        "fingerprint": table_fingerprint(TARGET_LINKAGE),
    }


def build_linkage(versions):
    raw = read_pinned(SRC_LINKAGE, versions)
    source_rows = verify_unique_key(raw, ["RowID"])
    due = _target_due_check(TARGET_LINKAGE, versions)
    if not due:
        active = spark.table(TARGET_LINKAGE).where(F.col("SOURCE_PRESENT_IND"))
        temporal = sorted(
            c[:-len("_FUTURE_IND")] for c in active.columns if c.endswith("_FUTURE_IND")
        )
        return {
            "mode": "NO_OP",
            "write": None,
            "gate": linkage_gates(source_rows, temporal),
        }
    selected = source_select(raw)
    people = read_pinned(SRC_PERSON, versions).select(
        F.col("PERSON_ID").cast("long").alias("_VALID_PERSON_ID")
    ).distinct().withColumn("_PERSON_MATCH", F.lit(True))
    selected = (
        selected.join(people, selected["PERSON_ID"] == people["_VALID_PERSON_ID"], "left")
        .withColumn(
            "PERSON_VALID_IND",
            F.when(F.col("PERSON_ID").isNull(), F.lit(None).cast("boolean"))
            .otherwise(F.coalesce(F.col("_PERSON_MATCH"), F.lit(False))),
        )
        .drop("_VALID_PERSON_ID", "_PERSON_MATCH")
    )
    invalid = selected.where(
        F.col("PERSON_ID").isNotNull() & ~F.col("PERSON_VALID_IND")
    ).count()
    dq_df, temporal = dq_all_clinical(selected, {"ADC_UPDT"})
    write = merge_with_tombstones(dq_df, TARGET_LINKAGE, ["ROW_ID"])
    apply_comments(
        TARGET_LINKAGE,
        "Six-column linkage extract (RowID grain). NOT a 2WW/31/62-day tracker — "
        "the authoritative cancer PTL is map_cancer_ptl (Pathfinder). Use only for "
        "person/referral-date linkage.",
        {
            "PERSON_ID": "Source-supplied Millennium PERSON_ID, validated against pinned mill_person.",
            "PERSON_VALID_IND": "True when source PERSON_ID exists in pinned mill_person; null when source PERSON_ID is null.",
        },
    )
    ig_tag_table(
        TARGET_LINKAGE,
        {"HOSPITAL_NUMBER": ("4", "2"), "NHS_NUMBER": ("4", "2"), "PERSON_ID": ("4", "2")},
    )
    gate = linkage_gates(source_rows, temporal)
    assert gate["invalid_person_ids"] == invalid
    _record_target_versions(TARGET_LINKAGE, versions)
    return {"mode": "BUILD", "write": write, "gate": gate}


# COMMAND ----------

try:
    BASE_DEPENDENCIES = [
        SRC_PATHFINDER, SRC_LINKAGE, SRC_PATIENT, SRC_RTT, SRC_REFERRAL,
        SRC_REFERRAL_MAP, SRC_PATHWAY_MAP, PERSON_ALIAS, SRC_PERSON,
    ]
    BASE_VERSIONS = capture_versions(BASE_DEPENDENCIES)

    pathfinder_versions = {
        table: BASE_VERSIONS[table]
        for table in [
            SRC_PATHFINDER, SRC_PATIENT, SRC_RTT, SRC_REFERRAL,
            SRC_REFERRAL_MAP, SRC_PATHWAY_MAP, PERSON_ALIAS,
        ]
    }
    linkage_versions = {
        SRC_LINKAGE: BASE_VERSIONS[SRC_LINKAGE],
        SRC_PERSON: BASE_VERSIONS[SRC_PERSON],
    }

    pathfinder_result = build_pathfinder(pathfinder_versions)
    linkage_result = build_linkage(linkage_versions)

    payload = {
        "pipeline": PIPELINE,
        "run_id": RUN_ID,
        "run_as_of": RUN_AS_OF,
        "result": "BUILT",
        "target": TARGET_SCHEMA,
        "target_schema": TARGET_SCHEMA,
        "source_versions": BASE_VERSIONS,
        "products": {
            TARGET_PATHFINDER: pathfinder_result,
            TARGET_LINKAGE: linkage_result,
        },
        "promotion_runbook": PROMOTION_RUNBOOK,
    }
except Exception as exc:
    payload = {
        "pipeline": PIPELINE,
        "run_id": RUN_ID,
        "run_as_of": RUN_AS_OF,
        "result": "FAILED",
        "error": repr(exc),
        "traceback": traceback.format_exc(),
    }
    print("[B3_LUNA_CANCER_PTL] " + json.dumps(payload, sort_keys=True, default=str))
    raise

print("[B3_LUNA_CANCER_PTL] " + json.dumps(payload, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(payload, sort_keys=True, default=str))


