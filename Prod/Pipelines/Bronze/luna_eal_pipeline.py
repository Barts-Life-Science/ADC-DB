# Databricks notebook source
"""B3 LUNA elective-access-list bronze builder (dev only).

Builds the parent, EAV attribute, and procedure products from pinned Delta snapshots.
Production promotion and watermark changes remain human-gated runbook actions.
"""

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


# release: bronze_completeness_20260816_v1 — prod-idiom refactor; behavior-identical (NO_OP re-proof run 142792538546812)
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
PIPELINE = "b3_luna_eal_v1_1"
# TARGET_SCHEMA resolved by the release guard above.

SRC_CORE = "4_prod.raw.luna_eal_core"
SRC_ATTRIBUTE = "4_prod.raw.luna_eal_additional"
SRC_PROCEDURE = "4_prod.raw.luna_eal_procedures"
SRC_PATIENT = "4_prod.raw.luna_patient_core"
SRC_REFERRAL_MAP = "4_prod.bronze.map_referral"
SRC_PATHWAY_MAP = "4_prod.bronze.map_rtt_pathway"
PERSON_ALIAS = "4_prod.raw.mill_person_alias"

TARGET_PARENT = f"{TARGET_SCHEMA}.map_elective_access_list"
TARGET_ATTRIBUTE = f"{TARGET_SCHEMA}.map_elective_access_list_attribute"
TARGET_PROCEDURE = f"{TARGET_SCHEMA}.map_elective_access_list_procedure"

MRN_ALIAS_TYPE = 10
NHS_ALIAS_TYPE = 18
SENTINEL_FLOOR = "1901-01-01"

PROMOTION_RUNBOOK = {
    "human_gated": True,
    "steps": [
        "Promote the three reviewed tables to 4_prod.bronze with retries=0.",
        "Reapply IG tags in production and run ig_tag_gate there.",
        "Add the weekly LUNA PTL bronze step only after runtime-budget review.",
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


# COMMAND ----------

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
        .when(~F.coalesce(F.col("_PATIENT_SPINE_MATCH"), F.lit(False)), "PATIENT_SPINE_MISS")
        .otherwise("UNMATCHED")
    )
    method = F.when(
        conflict, F.lit("conflict")
    ).otherwise(F.concat_ws(
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


def parent_gates(source_rows, temporal_columns, future_counts):
    active = spark.table(TARGET_PARENT).where(F.col("SOURCE_PRESENT_IND"))
    assert verify_unique_key(active, ["SOURCE_SYSTEM_OID", "WAITING_LIST_OID"]) == source_rows
    status_nulls = active.where(F.col("PERSON_LINK_STATUS").isNull()).count()
    assert status_nulls == 0, f"{TARGET_PARENT}: {status_nulls} null person statuses"
    assert_dq_triplets(TARGET_PARENT, temporal_columns)
    for c, expected in future_counts.items():
        actual = active.where(F.col(f"{c}_FUTURE_IND") == True).count()
        assert actual == expected, f"{c}: published future={actual}, pinned source future={expected}"
    ig_tag_gate(TARGET_PARENT)
    conflicts = active.where(F.col("PERSON_LINK_STATUS") == "CONFLICT_ARMS").count()
    return {
        "rows": source_rows,
        "conflict_arms": conflicts,
        "person_status_counts": {
            str(r["PERSON_LINK_STATUS"]): int(r["count"])
            for r in active.groupBy("PERSON_LINK_STATUS").count().collect()
        },
        "future_counts": future_counts,
        "fingerprint": table_fingerprint(TARGET_PARENT),
    }


def build_parent(versions):
    global PERSON_ALIAS_PINNED
    due = _target_due_check(TARGET_PARENT, versions)
    if not due:
        active = spark.table(TARGET_PARENT).where(F.col("SOURCE_PRESENT_IND"))
        temporal = sorted(
            c[:-len("_FUTURE_IND")] for c in active.columns if c.endswith("_FUTURE_IND")
        )
        future_counts = {
            c: active.where(F.col(f"{c}_FUTURE_IND") == True).count()
            for c in temporal
        }
        gate = parent_gates(active.count(), temporal, future_counts)
        return {"mode": "NO_OP", "write": None, "gate": gate}
    core_raw = read_pinned(SRC_CORE, versions)
    source_rows = verify_unique_key(core_raw, ["SourceSystemOID", "WaitingListOID"])
    selected = source_select(core_raw, {
        "WaitingListStausChangeDateTime": "WAITING_LIST_STATUS_CHANGE_DT_TM",
    })
    referral_map = read_pinned(SRC_REFERRAL_MAP, versions)
    pathway_map = read_pinned(SRC_PATHWAY_MAP, versions)
    patient = read_pinned(SRC_PATIENT, versions)
    verify_joinable_key_unique(patient, ["SourceSystemOID", "PatientOID"])
    PERSON_ALIAS_PINNED = read_pinned(PERSON_ALIAS, versions)
    ref_arm, path_arm = published_person_arms(referral_map, pathway_map)
    patient_ids = patient.select(
        F.col("SourceSystemOID").cast("long").alias("SOURCE_SYSTEM_OID"),
        F.col("PatientOID").cast("long").alias("PATIENT_OID"),
        F.col("LocalPatientID").cast("string").alias("_I_MRN"),
        F.col("NHSNumber").cast("string").alias("_I_NHS"),
        F.lit(True).alias("_PATIENT_SPINE_MATCH"),
    )
    linked = (
        selected.join(ref_arm, ["SOURCE_SYSTEM_OID", "REFERRAL_OID"], "left")
        .join(path_arm, ["SOURCE_SYSTEM_OID", "PATHWAY_OID"], "left")
        .join(patient_ids, ["SOURCE_SYSTEM_OID", "PATIENT_OID"], "left")
    )
    resolved = (
        resolve_persons(linked, "_I_MRN", "_I_NHS")
        .withColumnRenamed("PERSON_ID", "_I_PERSON")
        .withColumnRenamed("LINKAGE_STATUS", "_I_STATUS")
        .withColumnRenamed("LINKAGE_METHOD", "_I_METHOD")
    )
    consensus = add_person_consensus(resolved)
    dq_df, temporal = dq_all_clinical(
        consensus,
        {"ADC_UPDT", "CREATED_DT_TM", "MODIFIED_DT_TM", "TCI_CREATED_DT_TM"},
    )
    future_counts = measure_future_counts(consensus, temporal)
    write = None
    write = merge_with_tombstones(dq_df, TARGET_PARENT, ["SOURCE_SYSTEM_OID", "WAITING_LIST_OID"])
    comments = {
            "ACTIVE_IND": "Source closure state retained verbatim; false does not mean source absence.",
            "SOURCE_PRESENT_IND": "False only when a formerly published key disappears from the weekly full snapshot.",
            "SOURCE_ABSENT_DETECTED_TS": "Pinned RUN_AS_OF when source absence was first detected.",
            "ROW_HASH": "Content hash excluding volatile pipeline/source-load stamps; gates keyed MERGE updates.",
            "PERSON_LINK_STATUS": "Cross-arm consensus status; conflicts never publish PERSON_ID.",
    }
    apply_comments(
            TARGET_PARENT,
            "One row per (SOURCE_SYSTEM_OID, WAITING_LIST_OID) from weekly LUNA EAL Core. "
            "ActiveInd is source closure state; SOURCE_PRESENT_IND is snapshot tombstone state. "
            "Hash-gated MERGE means CDF contains genuine deltas rather than full-snapshot churn.",
            comments,
    )
    explicit = {c: ("4", "2") for c in {
            "PERSON_ID", "WAITING_LIST_ID", "LEGACY_WAITING_LIST_ID", "PATIENT_OID",
            "REFERRAL_OID", "PATHWAY_OID", "WAITING_LIST_OID", "LEAD_CLINICIAN_PRID",
            "CREATED_BY_PRID", "MODIFIED_BY_PRID", "TCI_CREATED_BY", "COMMENTS",
            "WAITING_LIST_STATUS_REASON", "WAITING_LIST_NAME",
    } if c in spark.table(TARGET_PARENT).columns}
    ig_tag_table(TARGET_PARENT, explicit)
    gate = parent_gates(source_rows, temporal, future_counts)
    _record_target_versions(TARGET_PARENT, versions)
    return {"mode": "BUILD", "write": write, "gate": gate}


def attribute_gates(source_rows, temporal_columns, multi_count):
    active = spark.table(TARGET_ATTRIBUTE).where(F.col("SOURCE_PRESENT_IND"))
    assert verify_unique_key(
        active, ["SOURCE_SYSTEM_OID", "WAITING_LIST_OID", "FIELD_OID"]
    ) == source_rows
    missing = active.where(F.col("PARENT_PRESENT_IND").isNull()).count()
    assert missing == 0, f"{TARGET_ATTRIBUTE}: {missing} parent lookups absent"
    assert active.where(F.col("FIELD_VALUE_KIND") == "multi").count() == multi_count
    assert_dq_triplets(TARGET_ATTRIBUTE, temporal_columns)
    ig_tag_gate(TARGET_ATTRIBUTE)
    return {
        "rows": source_rows,
        "multi_value_rows": multi_count,
        "fingerprint": table_fingerprint(TARGET_ATTRIBUTE),
    }


def build_attribute(source_versions, parent_version):
    versions = {**source_versions, TARGET_PARENT: parent_version}
    due = _target_due_check(TARGET_ATTRIBUTE, versions)
    if not due:
        active = spark.table(TARGET_ATTRIBUTE).where(F.col("SOURCE_PRESENT_IND"))
        temporal = sorted(
            c[:-len("_FUTURE_IND")] for c in active.columns if c.endswith("_FUTURE_IND")
        )
        multi_count = active.where(F.col("FIELD_VALUE_KIND") == "multi").count()
        gate = attribute_gates(active.count(), temporal, multi_count)
        return {"mode": "NO_OP", "write": None, "gate": gate}
    raw = read_pinned(SRC_ATTRIBUTE, versions)
    source_rows = verify_unique_key(raw, ["SourceSystemOID", "WaitingListOID", "FieldOID"])
    selected = source_select(raw)
    value_count = (
        F.when(F.col("FIELD_VALUE_RVID").isNotNull(), 1).otherwise(0)
        + F.when(F.col("FIELD_VALUE_DATE").isNotNull(), 1).otherwise(0)
        + F.when(F.col("FIELD_VALUE_VAR").isNotNull(), 1).otherwise(0)
        + F.when(F.col("FIELD_VALUE_INT").isNotNull(), 1).otherwise(0)
    )
    selected = selected.withColumn(
        "FIELD_VALUE_KIND",
        F.when(value_count > 1, "multi")
        .when(F.col("FIELD_VALUE_RVID").isNotNull(), "rvid")
        .when(F.col("FIELD_VALUE_DATE").isNotNull(), "date")
        .when(F.col("FIELD_VALUE_VAR").isNotNull(), "var")
        .when(F.col("FIELD_VALUE_INT").isNotNull(), "int")
        .otherwise("empty"),
    )
    multi_count = selected.where(F.col("FIELD_VALUE_KIND") == "multi").count()
    parent = read_pinned(TARGET_PARENT, versions).select(
        "SOURCE_SYSTEM_OID", "WAITING_LIST_OID",
        F.col("SOURCE_PRESENT_IND").alias("PARENT_PRESENT_IND"),
        F.lit(True).alias("_PARENT_MATCH"),
    )
    joined = selected.join(parent, ["SOURCE_SYSTEM_OID", "WAITING_LIST_OID"], "left")
    missing = joined.where(F.col("_PARENT_MATCH").isNull()).count()
    assert missing == 0, f"{missing} EAL attribute rows have no parent"
    joined = joined.drop("_PARENT_MATCH")
    dq_df, temporal = dq_all_clinical(joined, {"ADC_UPDT"})
    write = None
    write = merge_with_tombstones(
            dq_df, TARGET_ATTRIBUTE, ["SOURCE_SYSTEM_OID", "WAITING_LIST_OID", "FIELD_OID"]
    )
    apply_comments(
            TARGET_ATTRIBUTE,
            "One row per (SOURCE_SYSTEM_OID, WAITING_LIST_OID, FIELD_OID) from weekly LUNA EAL Additional. "
            "Polymorphic values are retained verbatim; FIELD_VALUE_KIND describes but never collapses them.",
            {
                "FIELD_VALUE_KIND": "rvid, date, var, int, empty, or multi when multiple source value columns are populated.",
                "PARENT_PRESENT_IND": "Snapshot-presence state inherited from the parent waiting-list product.",
            },
    )
    ig_tag_table(TARGET_ATTRIBUTE, {"FIELD_VALUE_VAR": ("4", "2")})
    gate = attribute_gates(source_rows, temporal, multi_count)
    _record_target_versions(TARGET_ATTRIBUTE, versions)
    return {"mode": "BUILD", "write": write, "gate": gate}


def procedure_source_gates(procedure_raw, core_raw):
    ns = procedure_raw.where(
        F.col("SourceSystemOID").isNotNull() | F.col("ProcedureOID").isNotNull()
    ).count()
    assert ns == 0, (
        f"EAL Procedures namespace populated on {ns} rows — the bare-key ruling no longer holds. "
        "STOP: rekey on the composite and re-run the containment analysis before publishing."
    )
    source_rows = verify_unique_key(
        procedure_raw, ["WaitingListOID", "ProcedureTypeSeq", "ProcedureSeq"]
    )
    multi = (
        core_raw.groupBy("WaitingListOID")
        .agg(F.countDistinct("SourceSystemOID").alias("n"))
        .where(F.col("n") > 1)
        .count()
    )
    assert multi == 0, (
        f"{multi} WaitingListOIDs span source systems — bare parent join is no longer safe"
    )
    return source_rows


def procedure_gates(source_rows):
    active = spark.table(TARGET_PROCEDURE).where(F.col("SOURCE_PRESENT_IND"))
    assert verify_unique_key(
        active, ["WAITING_LIST_OID", "PROCEDURE_TYPE_SEQ", "PROCEDURE_SEQ"]
    ) == source_rows
    missing = active.where(F.col("PARENT_PRESENT_IND").isNull()).count()
    assert missing == 0, f"{TARGET_PROCEDURE}: {missing} parent lookups absent"
    inherited_false = active.where(~F.col("SOURCE_SYSTEM_OID_INHERITED_IND")).count()
    assert inherited_false == 0, f"{TARGET_PROCEDURE}: inherited namespace flag failed"
    ig_tag_gate(TARGET_PROCEDURE)
    return {"rows": source_rows, "fingerprint": table_fingerprint(TARGET_PROCEDURE)}


def build_procedure(source_versions, parent_version):
    versions = {**source_versions, TARGET_PARENT: parent_version}
    raw = read_pinned(SRC_PROCEDURE, versions)
    core_raw = read_pinned(SRC_CORE, versions)
    source_rows = procedure_source_gates(raw, core_raw)
    due = _target_due_check(TARGET_PROCEDURE, versions)
    if not due:
        gate = procedure_gates(source_rows)
        return {"mode": "NO_OP", "write": None, "gate": gate}
    selected = source_select(raw, {"SourceSystemOID": "SOURCE_SYSTEM_OID_SOURCE"})
    parent = read_pinned(TARGET_PARENT, versions).select(
        "WAITING_LIST_OID", "SOURCE_SYSTEM_OID",
        F.col("SOURCE_PRESENT_IND").alias("PARENT_PRESENT_IND"),
        F.lit(True).alias("_PARENT_MATCH"),
    )
    joined = selected.join(parent, ["WAITING_LIST_OID"], "left")
    missing = joined.where(F.col("_PARENT_MATCH").isNull()).count()
    assert missing == 0, f"{missing} EAL procedure rows have no parent"
    joined = (
        joined.drop("_PARENT_MATCH")
        .withColumn("SOURCE_SYSTEM_OID_INHERITED_IND", F.lit(True))
    )
    write = None
    write = merge_with_tombstones(
            joined, TARGET_PROCEDURE,
            ["WAITING_LIST_OID", "PROCEDURE_TYPE_SEQ", "PROCEDURE_SEQ"],
    )
    apply_comments(
            TARGET_PROCEDURE,
            "One row per bare (WAITING_LIST_OID, PROCEDURE_TYPE_SEQ, PROCEDURE_SEQ). "
            "SOURCE_SYSTEM_OID is inherited from the collision-free parent under the 2026-08-14 owner ruling; "
            "the notebook fails loudly if source namespace fields populate or parent OIDs collide.",
            {
                "SOURCE_SYSTEM_OID_SOURCE": "Raw Procedures.SourceSystemOID retained verbatim; required to remain NULL by the bare-key ruling.",
                "SOURCE_SYSTEM_OID": "Namespace inherited from the unique EAL parent waiting-list row.",
                "SOURCE_SYSTEM_OID_INHERITED_IND": "Always true on present rows; proves namespace provenance is the parent.",
            },
    )
    explicit = {c: ("4", "2") for c in ["WAITING_LIST_OID", "PROCEDURE_OID"]}
    ig_tag_table(TARGET_PROCEDURE, explicit)
    gate = procedure_gates(source_rows)
    _record_target_versions(TARGET_PROCEDURE, versions)
    return {"mode": "BUILD", "write": write, "gate": gate}


# COMMAND ----------

try:
    BASE_DEPENDENCIES = [
        SRC_CORE, SRC_ATTRIBUTE, SRC_PROCEDURE, SRC_PATIENT,
        SRC_REFERRAL_MAP, SRC_PATHWAY_MAP, PERSON_ALIAS,
    ]
    BASE_VERSIONS = capture_versions(BASE_DEPENDENCIES)

    parent_versions = {
        table: BASE_VERSIONS[table]
        for table in [SRC_CORE, SRC_PATIENT, SRC_REFERRAL_MAP, SRC_PATHWAY_MAP, PERSON_ALIAS]
    }
    parent_result = build_parent(parent_versions)
    parent_version = table_version(TARGET_PARENT)

    attribute_result = build_attribute({SRC_ATTRIBUTE: BASE_VERSIONS[SRC_ATTRIBUTE]}, parent_version)
    procedure_result = build_procedure(
        {SRC_PROCEDURE: BASE_VERSIONS[SRC_PROCEDURE], SRC_CORE: BASE_VERSIONS[SRC_CORE]},
        parent_version,
    )

    payload = {
        "pipeline": PIPELINE,
        "run_id": RUN_ID,
        "run_as_of": RUN_AS_OF,
        "result": "BUILT",
        "target": TARGET_SCHEMA,
        "target_schema": TARGET_SCHEMA,
        "source_versions": BASE_VERSIONS,
        "products": {
            TARGET_PARENT: parent_result,
            TARGET_ATTRIBUTE: attribute_result,
            TARGET_PROCEDURE: procedure_result,
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
    print("[B3_LUNA_EAL] " + json.dumps(payload, sort_keys=True, default=str))
    raise

print("[B3_LUNA_EAL] " + json.dumps(payload, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(payload, sort_keys=True, default=str))


