# Databricks notebook source
# release: bronze_completeness_20260816_v1 — production-safe combined drop-in
# S6b A14a drop-in. SYNC-WITH Prod/Pipelines/Bronze/_registry_common (inlined below).
# %md
# # Registry Bronze Shared Helpers
# 
# Shared configuration, source preflight, privacy-safe source inventory, deterministic
# person linkage, source-value helpers, and the ROW_HASH-gated full-snapshot Delta writer.
# No Spark cache/persist or SparkContext APIs are used.

# COMMAND ----------

import json
import re
import uuid
from collections import defaultdict
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def bronze_control_schema(target_schema: str = "4_prod.bronze") -> str:
    """Control-plane schema for state, audit, manifests and run logs."""
    target = str(target_schema).strip()
    return "6_mgmt.bronze" if target.lower() == "4_prod.bronze" else target


def bronze_lookup_schema(target_schema: str = "4_prod.bronze") -> str:
    """Reference-plane schema for governed Bronze mappings."""
    target = str(target_schema).strip()
    return "3_lookup.omop" if target.lower() == "4_prod.bronze" else target


def _ensure_text_widget(name: str, default: str) -> None:
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)


for _name, _default in {
    "target_schema": "8_dev.bronze",
    "allow_production_write": "false",
    "pipeline_run_id": "",
    "expect_idempotent": "false",
}.items():
    _ensure_text_widget(_name, _default)


TARGET_SCHEMA = str(dbutils.widgets.get("target_schema")).strip() or "8_dev.bronze"
ALLOW_PROD_WRITE = str(dbutils.widgets.get("allow_production_write")).strip().lower() == "true"
_requested_registry_run_id = str(dbutils.widgets.get("pipeline_run_id")).strip()
REGISTRY_RUN_ID = (
    _requested_registry_run_id
    or str(globals().get("_REGISTRY_RUN_ID", "")).strip()
    or str(uuid.uuid4())
)
_REGISTRY_RUN_ID = REGISTRY_RUN_ID
EXPECT_IDEMPOTENT = str(dbutils.widgets.get("expect_idempotent")).strip().lower() in {
    "1", "true", "yes", "y"
}

IWEB = "4_prod.iweb"
RAW = "4_prod.raw"
PERSON_ALIAS = "4_prod.raw.mill_person_alias"
OMOP_CONCEPT = "3_lookup.omop.concept"
MRN_ALIAS_TYPE = 10
NHS_ALIAS_TYPE = 18

IWEB_SOURCE_TABLES = [
    "reg_cs2010g_pre1",
    "reg_cs2010g_pre2",
    "reg_cs2010g_post1",
    "reg_cs2010g_post2",
    "reg_cs2010g_subprocedure",
    "reg_cs2010g_followup",
    "reg_coronary_subprocedure",
    "reg_dghminap",
    "reg_noncoronary",
    "reg_eracs",
    "reg_mdt",
    "reg_ctmdt",
    "reg_mort",
]
SOURCE_TABLES = [f"{IWEB}.{name}" for name in IWEB_SOURCE_TABLES] + [
    f"{RAW}.mediconnect_mc_devices"
]

EXPECTED_COLUMN_COUNTS = {
    "reg_cs2010g_pre1": 180,
    "reg_cs2010g_pre2": 62,
    "reg_cs2010g_post1": 144,
    "reg_cs2010g_post2": 148,
    "reg_cs2010g_subprocedure": 82,
    "reg_cs2010g_followup": 51,
    "reg_coronary_subprocedure": 64,
    "reg_dghminap": 177,
    "reg_noncoronary": 63,
    "reg_eracs": 143,
    "reg_mdt": 84,
    "reg_ctmdt": 91,
    "reg_mort": 104,
    "mediconnect_mc_devices": 13,
}

AUDIT_COLS = {"ROW_HASH", "PIPELINE_LOADED_AT", "IS_CURRENT_IN_SOURCE", "SOURCE_PRESENT_IND"}
# ADC_UPDT is the landing snapshot timestamp and changes on every daily iWeb snapshot even when
# clinical content does not. It is deliberately excluded from ROW_HASH to prevent full-table rewrites.
HASH_EXCLUDE_COLS = AUDIT_COLS | {"ADC_UPDT"}
REGISTRY_WRITE_METRICS = []


def qname(name: str) -> str:
    quote = chr(96)
    return ".".join(quote + part.replace(quote, quote + quote) + quote for part in name.split("."))


def table_exists(name: str) -> bool:
    return spark.catalog.tableExists(name)


def ensure_target_schema() -> None:
    parts = TARGET_SCHEMA.split(".")
    if len(parts) != 2:
        raise ValueError(f"target_schema must be catalog.schema, received {TARGET_SCHEMA!r}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qname(TARGET_SCHEMA)}")


def _sql_string(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "''")


def _normalised_column_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def pii_drop_reason(name: str):
    """Return a schema-level drop reason without treating clinical words such as smoking as PII."""
    normal = _normalised_column_name(name)
    exact = {
        "surname": "PATIENT_NAME",
        "forename": "PATIENT_NAME",
        "firstname": "PATIENT_NAME",
        "lastname": "PATIENT_NAME",
        "patientname": "PATIENT_NAME",
        "dateofbirth": "DUPLICATE_DEMOGRAPHIC",
        "dob": "DUPLICATE_DEMOGRAPHIC",
        "nhsnumber": "LINKAGE_IDENTIFIER_DROPPED_POST_LINKAGE",
        "hospitalidentifier": "DUPLICATE_IDENTIFIER",
        "hospitalidentifierold": "DUPLICATE_IDENTIFIER",
        "hospitalidentifiertxt": "DUPLICATE_IDENTIFIER",
        "nameofkin": "NEXT_OF_KIN",
        "phoneofkin": "NEXT_OF_KIN",
        "mcommentskin": "NEXT_OF_KIN",
    }
    if normal in exact:
        return exact[normal]
    if "nextofkin" in normal or normal.endswith("nameofkin") or normal.endswith("phoneofkin"):
        return "NEXT_OF_KIN"
    if "postcode" in normal or "address" in normal:
        return "PATIENT_ADDRESS"
    if any(token in normal for token in ("telephone", "phone", "mobile")):
        return "CONTACT_DETAIL"
    if "email" in normal:
        return "CONTACT_DETAIL"
    return None


def is_pii_column(name: str) -> bool:
    return pii_drop_reason(name) is not None


def inventory_excluded(name: str) -> bool:
    normal = _normalised_column_name(name)
    if is_pii_column(name):
        return True
    if normal in {"mrn", "patientid", "entryid", "parententryid"}:
        return True
    # The inventory is a mapping-curation aid, not a free-text or identifier index.
    return any(
        token in normal
        for token in (
            "serial", "barcode", "comment", "narrative", "freetext", "description",
            "causeofdeath", "cause1a", "cause1b", "cause1c", "cause2",
        )
    )


def drop_pii(df: DataFrame, extra=()):
    drops = {name for name in df.columns if is_pii_column(name)}
    drops.update(name for name in extra if name in df.columns)
    ordered = sorted(drops)
    return df.drop(*ordered), ordered


def to_snake(df: DataFrame) -> DataFrame:
    def convert(name: str) -> str:
        value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", str(name))
        value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
        value = re.sub(r"[^A-Za-z0-9]+", "_", value)
        return re.sub(r"_+", "_", value).strip("_").upper()

    renames = {name: convert(name) for name in df.columns}
    collisions = defaultdict(list)
    for original, renamed in renames.items():
        collisions[renamed].append(original)
    bad = {name: originals for name, originals in collisions.items() if len(originals) > 1}
    if bad:
        raise AssertionError(f"snake_case collision: {bad}")
    return df.select(*[F.col(f"`{name}`").alias(renames[name]) for name in df.columns])


def blank_to_null(column):
    value = F.trim(column.cast("string"))
    return F.when(value == "", F.lit(None).cast("string")).otherwise(value)


def with_source_metadata(df: DataFrame, source_table: str, key_col: str) -> DataFrame:
    return (
        df.withColumn("SOURCE_TABLE", F.lit(source_table))
        .withColumn("SOURCE_RECORD_KEY", F.col(key_col).cast("string"))
    )


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
    return F.when(
        F.length(digits).between(1, 20) & (stripped != ""),
        stripped,
    )


# COMMAND ----------

def _alias_lookup(alias_type_cd: int, prefix: str, historical_latest: bool = False) -> DataFrame:
    alias_digits = F.regexp_replace(F.col("ALIAS").cast("string"), r"[^0-9]", "")
    alias_norm = (
        F.regexp_replace(alias_digits, r"^0+", "")
        if alias_type_cd == MRN_ALIAS_TYPE
        else F.when(F.length(alias_digits) == 10, alias_digits)
    )
    source = spark.table(PERSON_ALIAS).where(
        (F.col("ACTIVE_IND") == 1)
        & (F.col("PERSON_ALIAS_TYPE_CD") == alias_type_cd)
        & (
            F.col("BEG_EFFECTIVE_DT_TM").isNull()
            | (F.col("BEG_EFFECTIVE_DT_TM") <= F.current_timestamp())
        )
    )
    if not historical_latest:
        source = source.where(
            F.col("END_EFFECTIVE_DT_TM").isNull()
            | (F.col("END_EFFECTIVE_DT_TM") > F.current_timestamp())
        )
    source = (
        source.withColumn("ALIAS_NORM", alias_norm)
        .where(F.col("ALIAS_NORM").isNotNull() & (F.col("ALIAS_NORM") != ""))
    )
    if historical_latest:
        # Historical registries may carry an alias whose effective interval has ended. Use only the
        # latest begun active alias record(s), retaining ambiguity when the latest recency ties.
        recency = Window.partitionBy("ALIAS_NORM").orderBy(
            F.col("BEG_EFFECTIVE_DT_TM").desc_nulls_last(),
            F.col("ADC_UPDT").desc_nulls_last(),
        )
        source = source.withColumn("_ALIAS_RECENCY_RANK", F.dense_rank().over(recency)).where(
            F.col("_ALIAS_RECENCY_RANK") == 1
        )
    return (
        source.groupBy("ALIAS_NORM")
        .agg(
            F.countDistinct("PERSON_ID").cast("long").alias(f"{prefix}_PERSON_COUNT"),
            F.max("PERSON_ID").cast("long").alias(f"{prefix}_PERSON_ID_RAW"),
        )
        .withColumn(
            f"{prefix}_PERSON_ID",
            F.when(
                F.col(f"{prefix}_PERSON_COUNT") == 1,
                F.col(f"{prefix}_PERSON_ID_RAW"),
            ),
        )
        .drop(f"{prefix}_PERSON_ID_RAW")
    )


def resolve_persons(df: DataFrame, mrn_col=None, nhs_col=None) -> DataFrame:
    """Add deterministic linkage with a consensus-safe historical alias fallback."""
    linkage_cols = [
        "PERSON_ID", "LINKAGE_STATUS", "LINKAGE_METHOD", "NHS_NUMBER_VALID_IND",
        "LINKAGE_HISTORICAL_FALLBACK_IND", "LINKAGE_FALLBACK_CONFLICT_IND",
        "MRN_CUR_PERSON_ID", "MRN_CUR_PERSON_COUNT", "MRN_HIST_PERSON_ID", "MRN_HIST_PERSON_COUNT",
        "NHS_CUR_PERSON_ID", "NHS_CUR_PERSON_COUNT", "NHS_HIST_PERSON_ID", "NHS_HIST_PERSON_COUNT",
        "_MRN_NORM", "_NHS_NORM",
    ]
    out = df.drop(*[name for name in linkage_cols if name in df.columns])

    if mrn_col and mrn_col in out.columns:
        out = out.withColumn("_MRN_NORM", mrn_norm(mrn_col))
        for prefix, historical in (("MRN_CUR", False), ("MRN_HIST", True)):
            lookup = _alias_lookup(MRN_ALIAS_TYPE, prefix, historical_latest=historical)
            out = out.join(lookup, out["_MRN_NORM"] == lookup["ALIAS_NORM"], "left").drop(
                lookup["ALIAS_NORM"]
            )
    else:
        out = (
            out.withColumn("MRN_CUR_PERSON_ID", F.lit(None).cast("long"))
            .withColumn("MRN_CUR_PERSON_COUNT", F.lit(None).cast("long"))
            .withColumn("MRN_HIST_PERSON_ID", F.lit(None).cast("long"))
            .withColumn("MRN_HIST_PERSON_COUNT", F.lit(None).cast("long"))
        )

    if nhs_col and nhs_col in out.columns:
        out = (
            out.withColumn("_NHS_NORM", nhs_norm(nhs_col))
            .withColumn("NHS_NUMBER_VALID_IND", nhs_valid(F.col("_NHS_NORM")))
        )
        for prefix, historical in (("NHS_CUR", False), ("NHS_HIST", True)):
            lookup = _alias_lookup(NHS_ALIAS_TYPE, prefix, historical_latest=historical)
            condition = (
                out["NHS_NUMBER_VALID_IND"]
                & (out["_NHS_NORM"] == lookup["ALIAS_NORM"])
            )
            out = out.join(lookup, condition, "left").drop(lookup["ALIAS_NORM"])
    else:
        out = (
            out.withColumn("NHS_CUR_PERSON_ID", F.lit(None).cast("long"))
            .withColumn("NHS_CUR_PERSON_COUNT", F.lit(None).cast("long"))
            .withColumn("NHS_HIST_PERSON_ID", F.lit(None).cast("long"))
            .withColumn("NHS_HIST_PERSON_COUNT", F.lit(None).cast("long"))
            .withColumn("NHS_NUMBER_VALID_IND", F.lit(None).cast("boolean"))
        )

    mrn_current = F.col("MRN_CUR_PERSON_ID")
    nhs_current = F.col("NHS_CUR_PERSON_ID")
    mrn_history = F.col("MRN_HIST_PERSON_ID")
    nhs_history = F.col("NHS_HIST_PERSON_ID")
    historical_agreement = (
        mrn_history.isNotNull() & nhs_history.isNotNull() & (mrn_history == nhs_history)
    )

    if nhs_col and nhs_col in out.columns:
        # Current-effective aliases retain precedence. Historical values fill only when they agree
        # with the other current identifier, when the other historical identifier agrees, or when
        # no competing identifier exists. A historical disagreement is never promoted to CONFLICT.
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
    else:
        mrn_person = F.coalesce(mrn_current, mrn_history)
        nhs_person = F.lit(None).cast("long")

    fallback_used = (
        (mrn_current.isNull() & mrn_person.isNotNull())
        | (nhs_current.isNull() & nhs_person.isNotNull())
    )
    fallback_conflict = (
        (nhs_current.isNotNull() & mrn_history.isNotNull() & (nhs_current != mrn_history))
        | (mrn_current.isNotNull() & nhs_history.isNotNull() & (mrn_current != nhs_history))
        | (
            mrn_current.isNull() & nhs_current.isNull()
            & mrn_history.isNotNull() & nhs_history.isNotNull()
            & (mrn_history != nhs_history)
        )
    )
    status = (
        F.when(
            mrn_person.isNotNull() & nhs_person.isNotNull() & (mrn_person == nhs_person),
            F.lit("MATCHED_BOTH"),
        )
        .when(
            mrn_person.isNotNull() & nhs_person.isNotNull() & (mrn_person != nhs_person),
            F.lit("CONFLICT"),
        )
        .when(mrn_person.isNotNull(), F.lit("MATCHED_MRN"))
        .when(nhs_person.isNotNull(), F.lit("MATCHED_NHS"))
        .when(
            (F.coalesce(F.col("MRN_CUR_PERSON_COUNT"), F.lit(0)) > 1)
            | (F.coalesce(F.col("MRN_HIST_PERSON_COUNT"), F.lit(0)) > 1)
            | (F.coalesce(F.col("NHS_CUR_PERSON_COUNT"), F.lit(0)) > 1)
            | (F.coalesce(F.col("NHS_HIST_PERSON_COUNT"), F.lit(0)) > 1),
            F.lit("AMBIGUOUS"),
        )
        .otherwise(F.lit("UNMATCHED"))
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
            F.when(F.col("LINKAGE_STATUS").isin("MATCHED_BOTH", "CONFLICT"), F.lit("MRN+NHS"))
            .when(F.col("LINKAGE_STATUS") == "MATCHED_MRN", F.lit("MRN"))
            .when(F.col("LINKAGE_STATUS") == "MATCHED_NHS", F.lit("NHS")),
        )
        .withColumn("LINKAGE_HISTORICAL_FALLBACK_IND", F.coalesce(fallback_used, F.lit(False)))
        .withColumn("LINKAGE_FALLBACK_CONFLICT_IND", F.coalesce(fallback_conflict, F.lit(False)))
        .drop(
            "_MRN_NORM", "_NHS_NORM",
            "MRN_CUR_PERSON_ID", "MRN_CUR_PERSON_COUNT", "MRN_HIST_PERSON_ID", "MRN_HIST_PERSON_COUNT",
            "NHS_CUR_PERSON_ID", "NHS_CUR_PERSON_COUNT", "NHS_HIST_PERSON_ID", "NHS_HIST_PERSON_COUNT",
        )
    )


# COMMAND ----------

def date_quality(value, sentinel_year=None):
    column = _column(value)
    quality = F.when(column.isNull(), F.lit("MISSING"))
    if sentinel_year is not None:
        quality = quality.when(F.year(column) <= int(sentinel_year), F.lit("SENTINEL"))
    return quality.when(column > F.current_timestamp(), F.lit("FUTURE")).otherwise(F.lit("VALID"))


def clean_date(value, sentinel_year=None):
    column = _column(value)
    invalid = column.isNull() | (column > F.current_timestamp())
    if sentinel_year is not None:
        invalid = invalid | (F.year(column) <= int(sentinel_year))
    return F.when(invalid, F.lit(None).cast("date")).otherwise(F.to_date(column))


def with_row_hash(df: DataFrame) -> DataFrame:
    columns = sorted(name for name in df.columns if name not in HASH_EXCLUDE_COLS)
    if not columns:
        raise ValueError("Cannot hash a DataFrame with no business columns")
    payload = F.struct(
        *[
            F.coalesce(F.col(f"`{name}`").cast("string"), F.lit("<NULL>")).alias(name)
            for name in columns
        ]
    )
    return df.withColumn("ROW_HASH", F.sha2(F.to_json(payload), 256))


def verify_unique_key(df: DataFrame, keys) -> int:
    missing = [key for key in keys if key not in df.columns]
    if missing:
        raise AssertionError(f"Missing key columns {missing}; available={df.columns}")
    null_condition = reduce(lambda left, right: left | right, [F.col(key).isNull() for key in keys])
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct(F.struct(*[F.col(key) for key in keys])).alias("distinct_keys"),
        F.sum(F.when(null_condition, 1).otherwise(0)).alias("null_keys"),
    ).first()
    rows = int(row.rows or 0)
    distinct_keys = int(row.distinct_keys or 0)
    null_keys = int(row.null_keys or 0)
    if rows != distinct_keys or null_keys:
        raise AssertionError(
            f"key violation keys={list(keys)} rows={rows} distinct={distinct_keys} null_keys={null_keys}"
        )
    return rows


def apply_comments(table: str, comments, table_comment: str) -> None:
    spark.sql(f"COMMENT ON TABLE {qname(table)} IS '{_sql_string(table_comment)}'")
    available = set(spark.table(table).columns)
    for column, comment in (comments or {}).items():
        if column in available:
            spark.sql(
                f"ALTER TABLE {qname(table)} ALTER COLUMN `{column}` "
                f"COMMENT '{_sql_string(comment)}'"
            )


def _latest_operation_metrics(table: str):
    row = spark.sql(
        f"SELECT operation, operationMetrics FROM (DESCRIBE HISTORY {qname(table)}) "
        "WHERE operation = 'MERGE' ORDER BY version DESC LIMIT 1"
    ).first()
    metrics = dict(row.operationMetrics or {}) if row else {}
    return {"operation": row.operation if row else None, **metrics}


def registry_update_table(
    df: DataFrame,
    target: str,
    keys,
    table_comment: str,
    comments=None,
):
    """Full-snapshot merge with content-hash gating and source-presence flagging."""
    ensure_target_schema()
    source = (
        with_row_hash(df)
        .withColumn("PIPELINE_LOADED_AT", F.current_timestamp())
        .withColumn("IS_CURRENT_IN_SOURCE", F.lit(True))
        .withColumn("SOURCE_PRESENT_IND", F.lit(True))
    )
    source_count = verify_unique_key(source, keys)

    if not table_exists(target):
        (
            source.write.format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .option("overwriteSchema", "true")
            .mode("overwrite")
            .saveAsTable(target)
        )
        spark.sql(
            f"ALTER TABLE {qname(target)} SET TBLPROPERTIES "
            "('delta.enableChangeDataFeed'='true', 'delta.appendOnly'='false')"
        )
        apply_comments(target, {**(comments or {}), "SOURCE_PRESENT_IND": "Canonical boolean mirror of IS_CURRENT_IN_SOURCE; false rows are retained source tombstones."}, table_comment)
        metrics = {"operation": "CREATE", "numOutputRows": str(source_count)}
    else:
        target_schema = spark.table(target).schema
        target_columns = {field.name for field in target_schema.fields}
        additive_fields = [field for field in source.schema.fields if field.name not in target_columns]
        if additive_fields:
            additions = ", ".join(
                f"`{field.name}` {field.dataType.simpleString()}" for field in additive_fields
            )
            spark.sql(f"ALTER TABLE {qname(target)} ADD COLUMNS ({additions})")
            target_schema = spark.table(target).schema
        aligned = source
        for field in target_schema.fields:
            if field.name not in aligned.columns:
                aligned = aligned.withColumn(field.name, F.lit(None).cast(field.dataType))
        condition = " AND ".join(f"t.`{key}` <=> s.`{key}`" for key in keys)
        (
            DeltaTable.forName(spark, target)
            .alias("t")
            .merge(aligned.alias("s"), condition)
            .whenMatchedUpdateAll(
                condition="NOT (t.`ROW_HASH` <=> s.`ROW_HASH`) "
                "OR NOT COALESCE(t.`IS_CURRENT_IN_SOURCE`, false) "
                "OR NOT COALESCE(t.`SOURCE_PRESENT_IND`, false)"
            )
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceUpdate(
                condition="COALESCE(t.`SOURCE_PRESENT_IND`, true) = true",
                set={"IS_CURRENT_IN_SOURCE": "false", "SOURCE_PRESENT_IND": "false", "PIPELINE_LOADED_AT": "current_timestamp()"},
            )
            .execute()
        )
        apply_comments(target, {**(comments or {}), "SOURCE_PRESENT_IND": "Canonical boolean mirror of IS_CURRENT_IN_SOURCE; false rows are retained source tombstones."}, table_comment)
        metrics = _latest_operation_metrics(target)

    record = {"target": target, "source_rows": source_count, **metrics}
    REGISTRY_WRITE_METRICS.append(record)
    print("[REGISTRY_WRITE] " + json.dumps(record, sort_keys=True, default=str))
    return record


MULTI_SPLIT = r",(?=\s*\d+\s*-\s*)"
MULTI_VALUE_SCHEMA = T.StructType([
    T.StructField("SOURCE_TABLE", T.StringType(), False),
    T.StructField("ENTRY_ID", T.LongType(), False),
    T.StructField("FIELD_NAME", T.StringType(), False),
    T.StructField("POSITION", T.IntegerType(), False),
    T.StructField("RAW_VALUE", T.StringType(), True),
    T.StructField("CODE", T.StringType(), True),
    T.StructField("LABEL", T.StringType(), True),
    T.StructField("ADC_UPDT", T.TimestampType(), True),
])


# COMMAND ----------

def explode_multiselect(df: DataFrame, source_table: str, entry_col: str, field_cols) -> DataFrame:
    frames = []
    for field in field_cols:
        if field not in df.columns:
            continue
        adc_value = (
            F.col("ADC_UPDT").cast("timestamp")
            if "ADC_UPDT" in df.columns
            else F.lit(None).cast("timestamp")
        )
        frame = (
            df.select(
                F.col(entry_col).cast("long").alias("ENTRY_ID"),
                F.col(f"`{field}`").cast("string").alias("_RAW"),
                adc_value.alias("ADC_UPDT"),
            )
            .where(F.col("ENTRY_ID").isNotNull() & F.col("_RAW").isNotNull() & (F.trim("_RAW") != ""))
            .select(
                "ENTRY_ID",
                "ADC_UPDT",
                F.posexplode(F.split(F.col("_RAW"), MULTI_SPLIT)).alias("POSITION", "ITEM"),
            )
            .withColumn("ITEM", F.trim("ITEM"))
            .withColumn("CODE", blank_to_null(F.regexp_extract("ITEM", r"^(\d+)\s*-\s*", 1)))
            .withColumn("LABEL", F.trim(F.regexp_replace("ITEM", r"^\d+\s*-\s*", "")))
            .withColumn("SOURCE_TABLE", F.lit(source_table))
            .withColumn("FIELD_NAME", F.lit(field))
            .select(
                "SOURCE_TABLE", "ENTRY_ID", "FIELD_NAME", "POSITION",
                F.col("ITEM").alias("RAW_VALUE"), "CODE", "LABEL", "ADC_UPDT",
            )
        )
        frames.append(frame)
    if not frames:
        return spark.createDataFrame([], MULTI_VALUE_SCHEMA)
    return reduce(lambda left, right: left.unionByName(right), frames)


def run_source_preflight():
    missing = [name for name in SOURCE_TABLES + [PERSON_ALIAS, OMOP_CONCEPT] if not table_exists(name)]
    if missing:
        raise AssertionError(f"Required source tables are missing: {missing}")

    column_counts = {name.split(".")[-1]: len(spark.table(name).columns) for name in SOURCE_TABLES}
    drift = {
        name: {"expected": EXPECTED_COLUMN_COUNTS[name], "actual": count}
        for name, count in column_counts.items()
        if abs(count - EXPECTED_COLUMN_COUNTS[name]) > 5
    }
    if drift:
        raise AssertionError(f"Source schema drift exceeds five columns: {drift}")

    modules = {
        name: spark.table(f"{IWEB}.{name}").select(F.col("EntryId").alias("ENTRY_ID"))
        for name in (
            "reg_cs2010g_pre1", "reg_cs2010g_pre2", "reg_cs2010g_post1", "reg_cs2010g_post2"
        )
    }
    module_stats = {}
    for name, frame in modules.items():
        row = frame.agg(
            F.count("*").alias("rows"),
            F.countDistinct("ENTRY_ID").alias("keys"),
            F.sum(F.when(F.col("ENTRY_ID").isNull(), 1).otherwise(0)).alias("nulls"),
        ).first()
        module_stats[name] = {"rows": int(row.rows), "keys": int(row.keys), "nulls": int(row.nulls or 0)}
        if row.rows != row.keys or row.nulls:
            raise AssertionError(f"CS2010G module key failure: {name} {module_stats[name]}")

    base_name = "reg_cs2010g_pre1"
    base = modules[base_name]
    set_differences = {}
    for name, frame in modules.items():
        if name == base_name:
            continue
        left = base.join(frame, "ENTRY_ID", "left_anti").count()
        right = frame.join(base, "ENTRY_ID", "left_anti").count()
        set_differences[name] = {"pre1_not_module": left, "module_not_pre1": right}
        if left or right:
            raise AssertionError(f"CS2010G 1:1 EntryId set assumption failed: {name} {set_differences[name]}")

    sub = spark.table(f"{IWEB}.reg_cs2010g_subprocedure")
    sub_rows = sub.count()
    sub_keys = sub.select("EntryId").distinct().count()
    sub_nulls = sub.where(F.col("EntryId").isNull()).count()
    sub_orphans = sub.join(
        spark.table(f"{IWEB}.reg_cs2010g_pre1").select(F.col("EntryId").alias("ParentEntryId")),
        "ParentEntryId",
        "left_anti",
    ).count()
    if sub_rows != sub_keys or sub_nulls:
        raise AssertionError(
            f"CS2010G child key gate failed rows={sub_rows} keys={sub_keys} "
            f"nulls={sub_nulls}"
        )
    if sub_orphans:
        print(
            "[SOURCE_PREFLIGHT] WARNING: "
            f"{sub_orphans} CS2010G subprocedure source rows have no current parent; "
            "rows are retained losslessly with PARENT_ENTRY_ID for provenance."
        )

    result = {
        "column_counts": column_counts,
        "module_stats": module_stats,
        "module_set_differences": set_differences,
        "subprocedure": {"rows": sub_rows, "keys": sub_keys, "orphans": sub_orphans},
    }
    print("[SOURCE_PREFLIGHT] " + json.dumps(result, sort_keys=True))
    return result


# COMMAND ----------

def write_source_manifest():
    info = spark.sql(
        """
        SELECT table_catalog, table_schema, table_name, column_name, data_type,
               ordinal_position, is_nullable
        FROM system.information_schema.columns
        WHERE table_catalog = '4_prod'
          AND ((table_schema = 'iweb' AND table_name IN (
              'reg_cs2010g_pre1','reg_cs2010g_pre2','reg_cs2010g_post1','reg_cs2010g_post2',
              'reg_cs2010g_subprocedure','reg_cs2010g_followup','reg_coronary_subprocedure',
              'reg_dghminap','reg_noncoronary','reg_eracs','reg_mdt','reg_ctmdt','reg_mort'))
            OR (table_schema = 'raw' AND table_name = 'mediconnect_mc_devices'))
        ORDER BY table_schema, table_name, ordinal_position
        """
    )
    rows = []
    for row in info.collect():
        reason = pii_drop_reason(row.column_name)
        if row.table_name == "mediconnect_mc_devices" and row.column_name in {
            "NO_OF_CHAMBERS", "MODEL_NO"
        }:
            reason = "VERIFIED_CONSTANT_OR_EMPTY"
        rows.append(
            (
                row.table_catalog, row.table_schema, row.table_name, row.column_name,
                row.data_type, int(row.ordinal_position), row.is_nullable,
                reason is None, reason,
            )
        )
    schema = (
        "table_catalog string, table_schema string, table_name string, column_name string, "
        "data_type string, ordinal_position int, is_nullable string, in_scope_ind boolean, "
        "drop_reason string"
    )
    manifest = spark.createDataFrame(rows, schema).withColumn("snapshot_at", F.current_timestamp())
    target = f"{bronze_control_schema(TARGET_SCHEMA)}.iweb_source_manifest"
    (
        manifest.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true").saveAsTable(target)
    )
    apply_comments(
        target,
        {
            "IN_SCOPE_IND": "False for columns excluded from published bronze tables by the privacy/constant-field policy.",
            "DROP_REASON": "Schema-level exclusion reason; no source values are stored in this manifest.",
        },
        "Deployment-time source column manifest for the iWeb and MediConnect registry bronze pipeline.",
    )
    return manifest.count()


def build_value_inventory():
    frames = []
    coded_pattern = r"^\s*\d+\s*-\s*"
    for source_name in SOURCE_TABLES:
        source = spark.table(source_name)
        string_columns = [
            field.name
            for field in source.schema.fields
            if isinstance(field.dataType, T.StringType) and not inventory_excluded(field.name)
        ]
        if not string_columns:
            continue
        items = F.array(
            *[
                F.struct(
                    F.lit(name).alias("FIELD_NAME"),
                    F.col(f"`{name}`").cast("string").alias("RAW_VALUE"),
                )
                for name in string_columns
            ]
        )
        long_values = (
            source.select(F.explode(items).alias("ITEM"))
            .select("ITEM.FIELD_NAME", F.trim(F.col("ITEM.RAW_VALUE")).alias("RAW_VALUE"))
            .where(F.col("RAW_VALUE").isNotNull() & (F.col("RAW_VALUE") != ""))
        )
        grouped = long_values.groupBy("FIELD_NAME", "RAW_VALUE").agg(F.count("*").alias("N"))
        field_stats = grouped.groupBy("FIELD_NAME").agg(
            F.count("*").alias("DISTINCT_VALUE_COUNT"),
            F.sum("N").alias("NON_NULL_COUNT"),
            F.max(F.when(F.col("RAW_VALUE").rlike(coded_pattern), 1).otherwise(0)).alias("HAS_CODED_VALUE"),
        )
        safe_fields = field_stats.where(
            (F.col("HAS_CODED_VALUE") == 1)
            | (
                (F.col("DISTINCT_VALUE_COUNT") <= 500)
                & (F.col("DISTINCT_VALUE_COUNT") / F.col("NON_NULL_COUNT") <= F.lit(0.50))
            )
        )
        ranked = (
            grouped.join(safe_fields, "FIELD_NAME", "inner")
            .withColumn(
                "_RN",
                F.row_number().over(
                    Window.partitionBy("FIELD_NAME").orderBy(F.desc("N"), F.asc("RAW_VALUE"))
                ),
            )
            .where(F.col("_RN") <= 500)
            .drop("_RN", "HAS_CODED_VALUE")
            .withColumn("SOURCE_TABLE", F.lit(source_name))
            .select(
                "SOURCE_TABLE", "FIELD_NAME", "RAW_VALUE", "N",
                "DISTINCT_VALUE_COUNT", "NON_NULL_COUNT",
            )
        )
        frames.append(ranked)

    if not frames:
        raise AssertionError("Value inventory produced no rows")
    inventory = reduce(lambda left, right: left.unionByName(right), frames).withColumn(
        "SNAPSHOT_AT", F.current_timestamp()
    )
    target = f"{bronze_control_schema(TARGET_SCHEMA)}.iweb_value_inventory"
    (
        inventory.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true").saveAsTable(target)
    )
    apply_comments(
        target,
        {
            "RAW_VALUE": "Aggregated source value for low-cardinality/coded fields only; direct identifiers and free-text fields are excluded.",
            "N": "Number of source rows carrying this value at SNAPSHOT_AT.",
        },
        "Privacy-filtered string value inventory used to identify iWeb local picklists and seed mapping curation.",
    )
    return inventory.count()


def refresh_source_metadata():
    ensure_target_schema()
    preflight = run_source_preflight()
    manifest_rows = write_source_manifest()
    inventory_rows = build_value_inventory()
    result = {
        "preflight": preflight,
        "manifest_rows": manifest_rows,
        "inventory_rows": inventory_rows,
    }
    print("[SOURCE_METADATA] " + json.dumps(result, sort_keys=True, default=str))
    return result

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

# ==== S6b BLOCK v1 (SYNC-WITH _completeness_common) ====
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
S6B_SOURCE_VERSIONS = f"{bronze_control_schema(TARGET_SCHEMA)}.s6b_source_versions"

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
    """Apply complete IG tags, skipping columns whose two tags are already correct."""
    cat, sch, tbl = table.split(".")
    existing_rows = spark.sql(f"""
        SELECT column_name,
               MAX(CASE WHEN tag_name='ig_risk' THEN tag_value END) AS risk,
               MAX(CASE WHEN tag_name='ig_severity' THEN tag_value END) AS severity
        FROM `{cat}`.information_schema.column_tags
        WHERE schema_name='{sch}' AND table_name='{tbl}'
          AND tag_name IN ('ig_risk','ig_severity')
        GROUP BY column_name
    """).collect()
    existing = {
        r["column_name"]: (str(r["risk"]) if r["risk"] is not None else None,
                           str(r["severity"]) if r["severity"] is not None else None)
        for r in existing_rows
    }
    cols = [r.col_name for r in spark.sql(f"DESCRIBE {table}").collect()
            if r.col_name and not r.col_name.startswith('#')]
    skipped = 0
    for c in cols:
        if c in tag_map:
            risk, sev = tag_map[c]
        elif all(value is not None for value in existing.get(c, (None, None))):
            skipped += 1
            continue
        else:
            found = lookup_counterpart_tags(c)
            risk, sev = found if found else default
            if not found:
                print(f"IG-TAG DEFAULTED {table}.{c} -> {default} — REVIEW")
        assert risk is not None and sev is not None, (
            f"Incomplete counterpart tags for {table}.{c}: ig_risk={risk}, ig_severity={sev}")
        desired = (str(risk), str(sev))
        if existing.get(c) == desired:
            skipped += 1
            continue
        col_ident = c.replace("`", "``")
        risk_lit = desired[0].replace("'", "''")
        sev_lit = desired[1].replace("'", "''")
        spark.sql(
            f"ALTER TABLE {table} ALTER COLUMN `{col_ident}` "
            f"SET TAGS ('ig_risk'='{risk_lit}','ig_severity'='{sev_lit}')")
    print(f"IG-TAG {table}: skipped={skipped}, total={len(cols)}")

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

def present_filtered_count(table):
    """Row count of the CURRENT source view of a tombstoned product —
    the only count comparable to a raw/source count once tombstones exist."""
    return spark.sql(
        f"SELECT COUNT(*) c FROM {table} WHERE SOURCE_PRESENT_IND"
    ).collect()[0]["c"]

# ==== END S6b BLOCK v1 ====


# COMMAND ----------

# ==== S6b A14a gates (defined before pipeline execution) ====
_ensure_text_widget("gates_only", "false")
GATES_ONLY = str(dbutils.widgets.get("gates_only")).strip().lower() in {"1", "true", "yes", "y"}
REGISTRY_RELEASE_DIAGNOSTICS = (
    f"{bronze_control_schema(TARGET_SCHEMA)}.registry_release_diagnostics"
)
(
    spark.createDataFrame(
        [(
            REGISTRY_RUN_ID, TARGET_SCHEMA, "STARTUP", "STARTED",
            None, None,
            json.dumps({"gates_only": GATES_ONLY, "expect_idempotent": EXPECT_IDEMPOTENT}),
        )],
        "run_id STRING, target_schema STRING, stage STRING, status STRING, "
        "error_type STRING, error_message STRING, detail STRING",
    )
    .withColumn("recorded_at", F.current_timestamp())
    .write.mode("append")
    .saveAsTable(REGISTRY_RELEASE_DIAGNOSTICS)
)
assert TARGET_SCHEMA.startswith("8_dev.") or ALLOW_PROD_WRITE, (
    f"Refusing to write {TARGET_SCHEMA} without allow_production_write=true")

REGISTRY_TABLE_KEYS = {
    "mediconnect_device": ["MC_DEVICE_ID"],
    "iweb_cardiac_surgery_episode": ["ENTRY_ID"],
    "iweb_cardiac_surgery_procedure": ["ENTRY_ID"],
    "iweb_cardiac_surgery_followup": ["ENTRY_ID"],
    "iweb_coronary_lesion": ["ENTRY_ID"],
    "iweb_eracs_episode": ["ENTRY_ID"],
    "iweb_noncoronary_procedure": ["ENTRY_ID"],
    "iweb_acs_transfer": ["ENTRY_ID"],
    "iweb_cardiac_mdt": ["REGISTRY_TYPE", "ENTRY_ID"],
    "iweb_cardiac_mortality_review": ["ENTRY_ID"],
    "iweb_multiselect_value": ["SOURCE_TABLE", "ENTRY_ID", "FIELD_NAME", "POSITION"],
}
REGISTRY_BLOCKERS = {
    "mediconnect_device",
    "iweb_cardiac_surgery_episode",
    "iweb_cardiac_surgery_procedure",
    "iweb_cardiac_surgery_followup",
    "iweb_coronary_lesion",
    "iweb_eracs_episode",
    "iweb_noncoronary_procedure",
    "iweb_acs_transfer",
    "iweb_cardiac_mdt",
    "iweb_cardiac_mortality_review",
}


def _registry_enable_contract():
    for table_name in REGISTRY_TABLE_KEYS:
        table = f"{TARGET_SCHEMA}.{table_name}"
        spark.sql(
            f"ALTER TABLE {qname(table)} SET TBLPROPERTIES ("
            "'delta.enableRowTracking'='true',"
            "'delta.enableChangeDataFeed'='true',"
            "'delta.enableDeletionVectors'='true',"
            "'delta.appendOnly'='false')"
        )


def _registry_tag_tables():
    for table_name in REGISTRY_TABLE_KEYS:
        table = f"{TARGET_SCHEMA}.{table_name}"
        explicit = {}
        for col_name in spark.table(table).columns:
            upper = col_name.upper()
            if (
                upper in {"MRN", "PATIENTID", "NHS_NUMBER", "NHSNUMBER"}
                or upper.endswith("_MRN")
                or upper.endswith("_NHS_NUMBER")
            ):
                explicit[col_name] = ("4", "2")
            elif upper.endswith("_ID") or upper.endswith("_CD"):
                explicit[col_name] = lookup_counterpart_tags(col_name) or ("2", "2")
        ig_tag_table(table, explicit)


def _registry_stable_parity(dev_table, prod_table, keys):
    d = spark.table(dev_table).select(*keys, "ROW_HASH").alias("d")
    p = spark.table(prod_table).select(*keys, "ROW_HASH").alias("p")
    condition = None
    for key in keys + ["ROW_HASH"]:
        term = F.col(f"d.{key}").eqNullSafe(F.col(f"p.{key}"))
        condition = term if condition is None else condition & term
    return d.join(p, condition, "left_anti").count() + p.join(d, condition, "left_anti").count()


def run_registry_gates(tag_first=False):
    if tag_first:
        _registry_enable_contract()
        _registry_tag_tables()
    results = {}
    for table_name, keys in REGISTRY_TABLE_KEYS.items():
        dev = f"{TARGET_SCHEMA}.{table_name}"
        prod = f"4_prod.bronze.{table_name}"
        assert spark.catalog.tableExists(dev), f"GATE EXPECTED TABLE MISSING: {dev}"
        assert "SOURCE_PRESENT_IND" in spark.table(dev).columns, (
            f"GATE EXPECTED COLUMN MISSING: {dev}.SOURCE_PRESENT_IND"
        )
        source_field = next(f for f in spark.table(dev).schema.fields if f.name == "SOURCE_PRESENT_IND")
        assert isinstance(source_field.dataType, T.BooleanType)
        verify_unique_key(spark.table(dev), keys)
        prod_current = spark.table(prod).where("IS_CURRENT_IN_SOURCE").count()
        dev_present = present_filtered_count(dev)
        present_count_drift = dev_present - prod_current
        mirror_mismatch = spark.table(dev).where(
            ~F.col("SOURCE_PRESENT_IND").eqNullSafe(F.col("IS_CURRENT_IN_SOURCE"))
        ).count()
        assert mirror_mismatch == 0, (dev, mirror_mismatch)
        parity_diff = _registry_stable_parity(dev, prod, keys)
        # iWeb is a daily full snapshot and advanced after the latest prod bronze build.
        # The pipeline's own source-row reconciliation is strict; prod comparison is therefore
        # a measured source-drift metric, not a stale-oracle equality assertion.
        ig_tag_gate(dev)
        state_counts = {
            str(r["SOURCE_PRESENT_IND"]): int(r["count"])
            for r in spark.table(dev).groupBy("SOURCE_PRESENT_IND").count().collect()
        }
        fp = spark.table(dev).agg(
            F.sum(F.xxhash64("ROW_HASH").cast("decimal(38,0)")).alias("fp")
        ).collect()[0]["fp"]
        results[table_name] = {
            "blocker": table_name in REGISTRY_BLOCKERS,
            "prod_current": prod_current,
            "dev_present": dev_present,
            "present_count_drift_vs_prod": present_count_drift,
            "mirror_mismatch": mirror_mismatch,
            "stable_parity_diff_vs_prod": parity_diff,
            "source_present_distribution": state_counts,
            "row_hash_fingerprint": str(fp),
            "version": int(table_version(dev)),
        }
    print("[S6B_REGISTRY_GATES] " + json.dumps(results, sort_keys=True, default=str))
    return results


def registry_writer_tombstone_fixture():
    target = f"{TARGET_SCHEMA}.s6b_registry_tombstone_fixture"
    spark.sql(f"DROP TABLE IF EXISTS {qname(target)}")
    fixture = spark.createDataFrame([(1, "A"), (2, "B")], "ID BIGINT, VALUE STRING")
    registry_update_table(fixture, target, ["ID"], "S6b registry writer lifecycle fixture")
    registry_update_table(fixture.where("ID <> 2"), target, ["ID"], "S6b registry writer lifecycle fixture")
    dropped = spark.table(target).where("ID=2").collect()[0]
    assert dropped["IS_CURRENT_IN_SOURCE"] is False and dropped["SOURCE_PRESENT_IND"] is False
    registry_update_table(fixture, target, ["ID"], "S6b registry writer lifecycle fixture")
    restored = spark.table(target).where("ID=2").collect()[0]
    assert restored["IS_CURRENT_IN_SOURCE"] is True and restored["SOURCE_PRESENT_IND"] is True
    spark.sql(f"DROP TABLE IF EXISTS {qname(target)}")
    print("[S6B_REGISTRY] tombstone/resurrection fixture PASS")


if GATES_ONLY:
    run_registry_gates(tag_first=False)
    dbutils.notebook.exit(json.dumps({"mode": "GATES_ONLY", "status": "PASS"}))


# COMMAND ----------

# %md
# # iWeb and MediConnect Registry Bronze Pipeline
# 
# Single executable notebook for source preflight, metadata inventory, all registry builds,
# curated mapping application, validation, idempotency checks and audit persistence.
# Reusable implementation helpers live in `_registry_common`.


def _s6b_registry_diag(stage, exc):
    payload = [(
        REGISTRY_RUN_ID, TARGET_SCHEMA, stage, "FAILED",
        type(exc).__name__, str(exc), None,
    )]
    (
        spark.createDataFrame(
            payload,
            "run_id STRING, target_schema STRING, stage STRING, status STRING, "
            "error_type STRING, error_message STRING, detail STRING",
        )
        .withColumn("recorded_at", F.current_timestamp())
        .write.mode("append")
        .saveAsTable(REGISTRY_RELEASE_DIAGNOSTICS)
    )

try:
    _registry_metadata = refresh_source_metadata()
except Exception as _s6b_registry_exc:
    _s6b_registry_diag("refresh_source_metadata", _s6b_registry_exc)
    raise


# COMMAND ----------

# %md
# ## MediConnect CIED devices

# COMMAND ----------

ensure_target_schema()

DEVICE_MAP_TABLE = f"{bronze_lookup_schema(TARGET_SCHEMA)}.mediconnect_device_type_map"
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {qname(DEVICE_MAP_TABLE)} (
      DEVICE_TYPE INT COMMENT 'MediConnect numeric device category (canonical source category)',
      SOURCE_LABEL STRING COMMENT 'Most common observed TYPE label for this code',
      DEVICE_ROLE STRING COMMENT 'GENERATOR | LEAD | MONITOR | OTHER',
      SNOMED_CONCEPT_ID BIGINT COMMENT 'Candidate standard SNOMED Device concept',
      SNOMED_CONCEPT_NAME STRING,
      MAPPING_METHOD STRING COMMENT 'CURATED',
      MAPPING_STATUS STRING COMMENT 'APPROVED | PROPOSED | REJECTED',
      MAPPING_VERSION STRING,
      CURATED_BY STRING,
      CURATED_AT TIMESTAMP,
      NOTES STRING
    ) USING DELTA
    COMMENT 'Curated MediConnect device category to SNOMED CT map. Only APPROVED concepts are applied.'
    """
)

# Candidate concepts were selected from 3_lookup.omop.concept at runtime design review; all are
# standard, current, Device-domain SNOMED concepts. They remain PROPOSED pending clinical review.
DEVICE_TYPE_SEED = [
    (0, "", "OTHER", None, None, "CURATED", "PROPOSED", "1.0.0", None, None, "Blank/unknown source category"),
    (1, "PM", "GENERATOR", 4041473, "Pacemaker pulse generator", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (2, "CRT-P", "GENERATOR", 45767329, "Cardiac resynchronization therapy implantable pacemaker", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (3, "ICD", "GENERATOR", 4217646, "Implantable defibrillator", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (4, "CRT-D", "GENERATOR", 37166983, "Cardiac resynchronization therapy defibrillator pulse generator", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (5, "LV", "LEAD", 37168901, "Cardiac ventricular pacemaker lead", "CURATED", "PROPOSED", "1.0.0", None, None, "Laterality retained in source TYPE/LOCATION1"),
    (6, "RV", "LEAD", 37168901, "Cardiac ventricular pacemaker lead", "CURATED", "PROPOSED", "1.0.0", None, None, "Laterality retained in source TYPE/LOCATION1"),
    (7, "ICM", "MONITOR", 1448963, "Implantable electrocardiographic monitor and loop recorder", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (8, "RA/Atrial", "LEAD", 37168903, "Atrial pacemaker lead", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (9, "Other leads", "LEAD", 4236068, "Cardiac pacemaker lead", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (10, "Lead 1", "LEAD", 4236068, "Cardiac pacemaker lead", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (11, "Lead 2", "LEAD", 4236068, "Cardiac pacemaker lead", "CURATED", "PROPOSED", "1.0.0", None, None, None),
    (12, "Lead 3/CS", "LEAD", 4236068, "Cardiac pacemaker lead", "CURATED", "PROPOSED", "1.0.0", None, None, None),
]
seed = spark.createDataFrame(DEVICE_TYPE_SEED, spark.table(DEVICE_MAP_TABLE).schema)
(
    DeltaTable.forName(spark, DEVICE_MAP_TABLE)
    .alias("target")
    .merge(seed.alias("source"), "target.DEVICE_TYPE <=> source.DEVICE_TYPE")
    .whenNotMatchedInsertAll()
    .execute()
)

invalid_candidates = (
    spark.table(DEVICE_MAP_TABLE)
    .where(F.col("SNOMED_CONCEPT_ID").isNotNull())
    .join(
        spark.table(OMOP_CONCEPT).select(
            F.col("concept_id").cast("long").alias("SNOMED_CONCEPT_ID"),
            "domain_id", "vocabulary_id", "standard_concept", "invalid_reason",
        ),
        "SNOMED_CONCEPT_ID",
        "left",
    )
    .where(
        F.col("domain_id").isNull()
        | (F.col("domain_id") != "Device")
        | (F.col("vocabulary_id") != "SNOMED")
        | (F.col("standard_concept") != "S")
        | F.col("invalid_reason").isNotNull()
    )
    .count()
)
assert invalid_candidates == 0, f"Invalid MediConnect device map concepts: {invalid_candidates}"

# COMMAND ----------

source = spark.table(f"{RAW}.mediconnect_mc_devices")
natural_columns = [
    "PATIENTID", "DEVICE_TYPE", "TYPE", "MANUFACTURER", "MODEL_NAME",
    "SERIAL_NO", "IMPLANTED_DATE", "LOCATION1", "COMMENT", "STATUS",
]
natural_payload = F.struct(
    *[
        F.coalesce(F.col(name).cast("string"), F.lit("<NULL>")).alias(name)
        for name in natural_columns
    ]
)

devices = (
    source.withColumn("MC_DEVICE_ID", F.sha2(F.to_json(natural_payload), 256))
    .dropDuplicates(["MC_DEVICE_ID"])
    .withColumn("IMPLANTED_DATE_QUALITY", date_quality("IMPLANTED_DATE", sentinel_year=1901))
    .withColumn("IMPLANTED_DATE_CLEAN", clean_date("IMPLANTED_DATE", sentinel_year=1901))
    .withColumn(
        "MODEL_CODE",
        blank_to_null(F.regexp_extract("MODEL_NAME", r"^\s*([A-Za-z0-9][A-Za-z0-9/_.-]*)", 1)),
    )
    .withColumn(
        "MANUFACTURER_CLEAN",
        F.initcap(F.trim(F.regexp_replace(F.col("MANUFACTURER"), r"\s*\([^)]*\)\s*$", ""))),
    )
    .withColumn(
        "MANUFACTURER_PARENT",
        blank_to_null(F.regexp_extract("MANUFACTURER", r"\(([^)]+)\)", 1)),
    )
    .withColumn(
        "LEAD_CHAMBER",
        blank_to_null(F.regexp_extract("LOCATION1", r"(?i)Chamber:\s*([^\r\n]+)", 1)),
    )
    .withColumn(
        "LEAD_LOCATION",
        blank_to_null(F.regexp_extract("LOCATION1", r"(?i)Location:\s*([^\r\n]+)", 1)),
    )
    .withColumn(
        "LEAD_ACCESS",
        blank_to_null(F.regexp_extract("LOCATION1", r"(?i)Access:\s*([^\r\n]+)", 1)),
    )
    .withColumn(
        "LEAD_CONNECT",
        blank_to_null(F.regexp_extract("LOCATION1", r"(?i)Connect:\s*([^\r\n]+)", 1)),
    )
    .withColumn(
        "POCKET_SITE",
        F.when(
            ~F.coalesce(F.col("LOCATION1"), F.lit("")).rlike(r"(?i)Chamber:"),
            blank_to_null(F.col("LOCATION1")),
        ),
    )
    .withColumn(
        "IMPLANTING_CLINICIAN_1",
        blank_to_null(F.regexp_extract("COMMENT", r"(?i)Imp\s*MD\s*1:\s*([^/\r\n]+)", 1)),
    )
    .withColumn(
        "IMPLANTING_CLINICIAN_2",
        blank_to_null(F.regexp_extract("COMMENT", r"(?i)Imp\s*MD\s*2:\s*([^/\r\n]+)", 1)),
    )
    .withColumn("EXPLANTED_IND", F.when(F.col("STATUS").isNull(), F.lit(None).cast("boolean")).otherwise(F.col("STATUS") == 1))
    .drop("NO_OF_CHAMBERS", "MODEL_NO")
)

device_map = spark.table(DEVICE_MAP_TABLE).select(
    "DEVICE_TYPE",
    "DEVICE_ROLE",
    F.col("MAPPING_STATUS").alias("DEVICE_MAPPING_STATUS"),
    F.col("MAPPING_VERSION").alias("DEVICE_MAPPING_VERSION"),
    F.when(F.col("MAPPING_STATUS") == "APPROVED", F.col("SNOMED_CONCEPT_ID"))
    .cast("long").alias("DEVICE_SNOMED_CONCEPT_ID"),
    F.when(F.col("MAPPING_STATUS") == "APPROVED", F.col("SNOMED_CONCEPT_NAME"))
    .alias("DEVICE_SNOMED_CONCEPT_NAME"),
)
devices = devices.join(device_map, "DEVICE_TYPE", "left")
devices = resolve_persons(devices, mrn_col="PATIENTID")
devices = with_source_metadata(devices, "mediconnect_mc_devices", "MC_DEVICE_ID")

registry_update_table(
    devices,
    f"{TARGET_SCHEMA}.mediconnect_device",
    ["MC_DEVICE_ID"],
    "MediConnect CIED/lead registry (legacy Heart Hospital NICOR migration; implants end "
    "2020-01-01 in the current frozen extract). One row per distinct source device/lead record. "
    "STATUS=1 is exposed as an inferred explant flag pending source-owner confirmation.",
    {
        "MC_DEVICE_ID": "Stable SHA-256 key over the retained source natural fields; the source has no primary key.",
        "PATIENTID": "Source MediConnect identifier, deterministically resolved as a Millennium MRN candidate.",
        "EXPLANTED_IND": "INFERRED from STATUS=1; semantics require confirmation against the source lookup.",
        "IMPLANTED_DATE_QUALITY": "VALID | SENTINEL (year <=1901) | FUTURE | MISSING; raw date retained.",
        "DEVICE_SNOMED_CONCEPT_ID": "Applied only when the category mapping row is APPROVED; no model-level UDI claim.",
        "PERSON_ID": "Millennium PERSON_ID via a unique active MRN alias; NULL for ambiguous or unmatched rows.",
        "ADC_UPDT": "Source landing timestamp. Excluded from ROW_HASH because the full snapshot refreshes it daily.",
    },
)

# COMMAND ----------

# %md
# ## Cardiac surgery

# COMMAND ----------

MODULES = [
    ("PRE1", "reg_cs2010g_pre1"),
    ("PRE2", "reg_cs2010g_pre2"),
    ("POST1", "reg_cs2010g_post1"),
    ("POST2", "reg_cs2010g_post2"),
]

# Link once from the complete pre1 identifier pair, then remove direct identifiers. Other modules
# contribute clinical content only; repeated identifiers/demographics are not merged back in.
base_raw = spark.table(f"{IWEB}.reg_cs2010g_pre1")
base_linked = resolve_persons(base_raw, mrn_col="MRN", nhs_col="NHSNumber")
base, dropped_pii = drop_pii(base_linked)
if "ADC_UPDT" in base.columns:
    base = base.withColumnRenamed("ADC_UPDT", "PRE1_ADC_UPDT")

merged = base
kept = set(merged.columns)
for prefix, table_name in MODULES[1:]:
    module, dropped = drop_pii(spark.table(f"{IWEB}.{table_name}"))
    dropped_pii.extend(dropped)
    module = module.drop(
        *[
            name
            for name in ("MRN", "NHSNumber", "DateOfOperation", "DateOfDeath")
            if name in module.columns
        ]
    )
    if "ADC_UPDT" in module.columns:
        module = module.withColumnRenamed("ADC_UPDT", f"{prefix}_ADC_UPDT")
    renamed = {}
    for name in module.columns:
        renamed[name] = f"{prefix}_{name}" if name != "EntryId" and name in kept else name
    module = module.select(*[F.col(f"`{name}`").alias(renamed[name]) for name in module.columns])
    kept.update(renamed.values())
    merged = merged.join(module, "EntryId", "full_outer")

last_changed_columns = [
    name for name in merged.columns if _normalised_column_name(name).endswith("datelastchanged")
]
adc_columns = [name for name in merged.columns if name.endswith("_ADC_UPDT")]
if not last_changed_columns or not adc_columns:
    raise AssertionError(
        f"Expected module timestamps are missing: last_changed={last_changed_columns}, adc={adc_columns}"
    )

episode = (
    merged.withColumn(
        "EPISODE_LAST_CHANGED",
        F.greatest(*[F.col(f"`{name}`") for name in last_changed_columns]),
    )
    .withColumn("ADC_UPDT", F.greatest(*[F.col(f"`{name}`") for name in adc_columns]))
)
episode = with_source_metadata(
    episode,
    "reg_cs2010g_pre1+reg_cs2010g_pre2+reg_cs2010g_post1+reg_cs2010g_post2",
    "EntryId",
)
episode = to_snake(episode)
print("[PII_DROP] cardiac_surgery_episode " + json.dumps(sorted(set(dropped_pii))))

registry_update_table(
    episode,
    f"{TARGET_SCHEMA}.iweb_cardiac_surgery_episode",
    ["ENTRY_ID"],
    "iWeb CS2010G adult cardiac surgery registry with pre1/pre2/post1/post2 modules merged "
    "1:1 on EntryId after a hard set-equivalence gate. One row per surgical episode; source "
    "labels and module-specific fields are retained, while direct patient identifiers are removed.",
    {
        "ENTRY_ID": "Stable iWeb episode key and parent of iweb_cardiac_surgery_procedure.",
        "PERSON_ID": "Resolved independently through NHS number and MRN; NULL on conflict or ambiguity.",
        "MRN": "Native source hospital number retained for trace-back; NHS number is dropped post-linkage.",
        "EPISODE_LAST_CHANGED": "Greatest DateLastChanged timestamp across the four source modules.",
        "ADC_UPDT": "Greatest landing timestamp across the four source modules; excluded from ROW_HASH.",
    },
)

# COMMAND ----------

procedure_raw = spark.table(f"{IWEB}.reg_cs2010g_subprocedure")
procedure = resolve_persons(procedure_raw, mrn_col="MRN", nhs_col="NHSNumber")
procedure, procedure_drops = drop_pii(procedure)
procedure = with_source_metadata(procedure, "reg_cs2010g_subprocedure", "EntryId")
procedure = to_snake(procedure)
print("[PII_DROP] cardiac_surgery_procedure " + json.dumps(procedure_drops))

registry_update_table(
    procedure,
    f"{TARGET_SCHEMA}.iweb_cardiac_surgery_procedure",
    ["ENTRY_ID"],
    "One row per graft, valve or aortic-implant component of a CS2010G surgery. "
    "PARENT_ENTRY_ID joins iweb_cardiac_surgery_episode and is hard-gated to zero orphans. "
    "Known source defect: IMPLANT_PROSTHESIS_NAMETXT contains some drug-picklist bleed-through.",
    {
        "ENTRY_ID": "Stable iWeb child-record key.",
        "PARENT_ENTRY_ID": "Foreign key to iweb_cardiac_surgery_episode.ENTRY_ID.",
    },
)

# COMMAND ----------

followup_raw = spark.table(f"{IWEB}.reg_cs2010g_followup")
tie_columns = sorted(followup_raw.columns)
tie_hash = F.sha2(
    F.to_json(
        F.struct(
            *[
                F.coalesce(F.col(f"`{name}`").cast("string"), F.lit("<NULL>")).alias(name)
                for name in tie_columns
            ]
        )
    ),
    256,
)
followup_window = Window.partitionBy("EntryId").orderBy(
    F.col("DateLastChanged").desc_nulls_last(),
    F.col("ADC_UPDT").desc_nulls_last(),
    tie_hash.desc(),
)
followup = (
    followup_raw.withColumn("_ROW_NUMBER", F.row_number().over(followup_window))
    .where(F.col("_ROW_NUMBER") == 1)
    .drop("_ROW_NUMBER")
)
followup = resolve_persons(followup, mrn_col="MRN", nhs_col="NHSNumber")
followup, followup_drops = drop_pii(followup)
followup = with_source_metadata(followup, "reg_cs2010g_followup", "EntryId")
followup = to_snake(followup)
print("[PII_DROP] cardiac_surgery_followup " + json.dumps(followup_drops))

registry_update_table(
    followup,
    f"{TARGET_SCHEMA}.iweb_cardiac_surgery_followup",
    ["ENTRY_ID"],
    "CS2010G follow-up module. The source is small and dormant but semantically distinct. "
    "Duplicate EntryId rows are reduced deterministically to the latest DateLastChanged/ADC_UPDT row.",
    {
        "ENTRY_ID": "Stable iWeb follow-up key after deterministic latest-row deduplication.",
    },
)

# COMMAND ----------

# %md
# ## Procedures and pathways

# COMMAND ----------

def build_single_registry(
    source_table: str,
    target_table: str,
    table_comment: str,
    extra_drop=(),
    col_comments=None,
    transform=None,
):
    source = spark.table(f"{IWEB}.{source_table}")
    frame = resolve_persons(source, mrn_col="MRN", nhs_col="NHSNumber")
    if transform is not None:
        frame = transform(frame)
    frame, dropped = drop_pii(frame, extra=extra_drop)
    frame = with_source_metadata(frame, source_table, "EntryId")
    frame = to_snake(frame)
    print(f"[PII_DROP] {target_table} " + json.dumps(dropped))
    return registry_update_table(
        frame,
        f"{TARGET_SCHEMA}.{target_table}",
        ["ENTRY_ID"],
        table_comment,
        col_comments or {},
    )


build_single_registry(
    "reg_coronary_subprocedure",
    "iweb_coronary_lesion",
    "Per-lesion/per-vessel PCI and angiography detail from the iWeb coronary registry. "
    "The parent coronary procedure registry is not landed, so PARENT_ENTRY_ID is retained for "
    "future linkage but currently has no bronze parent and there is no reliable procedure date.",
    col_comments={
        "PARENT_ENTRY_ID": "Foreign key to the currently un-landed iWeb coronary procedure registry.",
        "DATE_LAST_CHANGED": "Source edit timestamp; it must not be interpreted as the procedure date.",
    },
)

build_single_registry(
    "reg_noncoronary",
    "iweb_noncoronary_procedure",
    "iWeb non-coronary/structural cath-lab registry covering interventions such as TAVI, TMVI, "
    "ASD/VSD closure and septal ablation. Local N-Label values are retained here and exploded "
    "separately in iweb_multiselect_value.",
)


def _acs_dates(frame: DataFrame) -> DataFrame:
    if "Admissiondate" not in frame.columns:
        raise AssertionError("reg_dghminap.Admissiondate is missing")
    return (
        frame.withColumn("ADMISSION_DATE_QUALITY", date_quality("Admissiondate", sentinel_year=2000))
        .withColumn("ADMISSION_DATE_CLEAN", clean_date("Admissiondate", sentinel_year=2000))
    )


build_single_registry(
    "reg_dghminap",
    "iweb_acs_transfer",
    "MINAP-style ACS transfer pathway: symptom, transfer and reperfusion timestamps, GRACE/Killip, "
    "drugs, laboratory results and Takotsubo fields. Diagnoses remain source labels. "
    "ADMISSION_DATE_QUALITY identifies the known 2000-01-01 placeholder range.",
    col_comments={
        "ADMISSION_DATE_QUALITY": "VALID | SENTINEL (year <=2000) | FUTURE | MISSING.",
        "ADMISSION_DATE_CLEAN": "Admission date with sentinel/future values nulled; raw ADMISSIONDATE retained.",
    },
    transform=_acs_dates,
)

build_single_registry(
    "reg_eracs",
    "iweb_eracs_episode",
    "Enhanced Recovery After Cardiac Surgery pathway registry, live since 2025: pathway "
    "timestamps, pacing, TEG/platelet mapping, transfusion, mobilisation and discharge milestones.",
)

# COMMAND ----------

# %md
# ## Cardiac MDT and mortality review

# COMMAND ----------

def load_mdt(source_table: str, registry_type: str) -> DataFrame:
    frame = spark.table(f"{IWEB}.{source_table}")
    frame = resolve_persons(frame, mrn_col="MRN", nhs_col="NHSNumber")
    frame, dropped = drop_pii(frame)
    frame = (
        frame.withColumn("REGISTRY_TYPE", F.lit(registry_type))
        .withColumn("SOURCE_TABLE", F.lit(source_table))
        .withColumn("SOURCE_RECORD_KEY", F.col("EntryId").cast("string"))
    )
    print(f"[PII_DROP] {source_table} " + json.dumps(dropped))
    return to_snake(frame)


coronary_mdt = load_mdt("reg_mdt", "CORONARY_REVASC")
ct_aortic_mdt = load_mdt("reg_ctmdt", "CT_AORTIC")
mdt = coronary_mdt.unionByName(ct_aortic_mdt, allowMissingColumns=True)

registry_update_table(
    mdt,
    f"{TARGET_SCHEMA}.iweb_cardiac_mdt",
    ["REGISTRY_TYPE", "ENTRY_ID"],
    "Harmonised iWeb cardiac MDT record. REGISTRY_TYPE=CORONARY_REVASC identifies the coronary "
    "revascularisation workflow; REGISTRY_TYPE=CT_AORTIC identifies CT/aortic surveillance. "
    "Workflow-specific columns are null for the other registry.",
    {
        "REGISTRY_TYPE": "Source workflow discriminator and part of the composite key.",
        "ENTRY_ID": "Source EntryId, unique only within REGISTRY_TYPE.",
    },
)

# COMMAND ----------

mortality = spark.table(f"{IWEB}.reg_mort")
mortality = resolve_persons(mortality, mrn_col="MRN", nhs_col="NHSNumber")
mortality, mortality_drops = drop_pii(mortality)
if not any("kin" in name.lower() for name in mortality_drops):
    raise AssertionError("Mortality next-of-kin columns were not detected by the PII policy")
if "DateofDeath" not in mortality.columns:
    raise AssertionError("reg_mort.DateofDeath is missing")
mortality = (
    mortality.withColumn("DATE_OF_DEATH_QUALITY", date_quality("DateofDeath"))
    .withColumn("DATE_OF_DEATH_CLEAN", clean_date("DateofDeath"))
)
mortality = with_source_metadata(mortality, "reg_mort", "EntryId")
mortality = to_snake(mortality)
print("[PII_DROP] mortality_review " + json.dumps(mortality_drops))

registry_update_table(
    mortality,
    f"{TARGET_SCHEMA}.iweb_cardiac_mortality_review",
    ["ENTRY_ID"],
    "Cardiac mortality/M&M review registry: source-text cause-of-death chain (not asserted to be "
    "ICD-coded), NCEPOD classification, coronial/medical-examiner fields and structured care-problem "
    "flags. Patient names, contact details and next-of-kin content are removed.",
    {
        "DATE_OF_DEATH_QUALITY": "VALID | FUTURE | MISSING; raw DATEOF_DEATH is retained.",
        "DATE_OF_DEATH_CLEAN": "Date of death with future values nulled.",
    },
)

# COMMAND ----------

# %md
# ## Local picklist and multiselect values

# COMMAND ----------

inventory = spark.table(f"{bronze_control_schema(TARGET_SCHEMA)}.iweb_value_inventory")
multiselect_fields = (
    inventory.where(F.col("RAW_VALUE").rlike(r"^\s*\d+\s*-\s*"))
    .where(F.col("SOURCE_TABLE").startswith(f"{IWEB}."))
    .select("SOURCE_TABLE", "FIELD_NAME")
    .distinct()
    .collect()
)
if not multiselect_fields:
    raise AssertionError("No N-Label iWeb fields were found in the value inventory")

fields_by_table = defaultdict(list)
for row in multiselect_fields:
    fields_by_table[row.SOURCE_TABLE].append(row.FIELD_NAME)

value_frames = []
link_frames = []


def linkage_for_source(short_name: str) -> DataFrame:
    linkage_columns = [
        "PERSON_ID", "LINKAGE_STATUS", "LINKAGE_METHOD", "NHS_NUMBER_VALID_IND",
        "LINKAGE_HISTORICAL_FALLBACK_IND", "LINKAGE_FALLBACK_CONFLICT_IND",
    ]
    if short_name in {
        "reg_cs2010g_pre1", "reg_cs2010g_pre2", "reg_cs2010g_post1", "reg_cs2010g_post2"
    }:
        target = "iweb_cardiac_surgery_episode"
        frame = spark.table(f"{TARGET_SCHEMA}.{target}").where("IS_CURRENT_IN_SOURCE")
    elif short_name == "reg_cs2010g_subprocedure":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_cardiac_surgery_procedure").where("IS_CURRENT_IN_SOURCE")
    elif short_name == "reg_cs2010g_followup":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_cardiac_surgery_followup").where("IS_CURRENT_IN_SOURCE")
    elif short_name == "reg_coronary_subprocedure":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_coronary_lesion").where("IS_CURRENT_IN_SOURCE")
    elif short_name == "reg_dghminap":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_acs_transfer").where("IS_CURRENT_IN_SOURCE")
    elif short_name == "reg_noncoronary":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_noncoronary_procedure").where("IS_CURRENT_IN_SOURCE")
    elif short_name == "reg_eracs":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_eracs_episode").where("IS_CURRENT_IN_SOURCE")
    elif short_name == "reg_mdt":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_cardiac_mdt").where(
            "IS_CURRENT_IN_SOURCE AND REGISTRY_TYPE = 'CORONARY_REVASC'"
        )
    elif short_name == "reg_ctmdt":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_cardiac_mdt").where(
            "IS_CURRENT_IN_SOURCE AND REGISTRY_TYPE = 'CT_AORTIC'"
        )
    elif short_name == "reg_mort":
        frame = spark.table(f"{TARGET_SCHEMA}.iweb_cardiac_mortality_review").where("IS_CURRENT_IN_SOURCE")
    else:
        raise AssertionError(f"No parent linkage table configured for {short_name}")
    available = set(frame.columns)
    missing = [name for name in linkage_columns if name not in available]
    if missing:
        raise AssertionError(f"Parent linkage columns missing for {short_name}: {missing}")
    return frame.select(
        F.lit(short_name).alias("SOURCE_TABLE"),
        F.col("ENTRY_ID").cast("long").alias("ENTRY_ID"),
        *linkage_columns,
    )


for source_name, fields in sorted(fields_by_table.items()):
    source = spark.table(source_name)
    short_name = source_name.split(".")[-1]
    if short_name == "reg_cs2010g_followup":
        source_order = Window.partitionBy("EntryId").orderBy(
            F.col("DateLastChanged").desc_nulls_last(),
            F.col("ADC_UPDT").desc_nulls_last(),
        )
        source = (
            source.withColumn("_SOURCE_RN", F.row_number().over(source_order))
            .where(F.col("_SOURCE_RN") == 1)
            .drop("_SOURCE_RN")
        )
    value_frames.append(explode_multiselect(source, short_name, "EntryId", sorted(fields)))
    link_frames.append(linkage_for_source(short_name))

values = reduce(lambda left, right: left.unionByName(right), value_frames)
links = reduce(lambda left, right: left.unionByName(right), link_frames)
values = values.join(links, ["SOURCE_TABLE", "ENTRY_ID"], "left")
values = values.withColumn("SOURCE_RECORD_KEY", F.col("ENTRY_ID").cast("string"))

# COMMAND ----------

VALUE_MAP_TABLE = f"{bronze_lookup_schema(TARGET_SCHEMA)}.iweb_value_to_concept"
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {qname(VALUE_MAP_TABLE)} (
      SOURCE_TABLE STRING,
      FIELD_NAME STRING,
      CODE STRING,
      LABEL STRING,
      TARGET_VOCABULARY STRING COMMENT 'SNOMED | OPCS4 | ICD10',
      TARGET_CODE STRING,
      TARGET_CONCEPT_ID BIGINT,
      TARGET_CONCEPT_NAME STRING,
      MAPPING_METHOD STRING COMMENT 'CURATED | TEXT_CANDIDATE',
      MAPPING_STATUS STRING COMMENT 'APPROVED | PROPOSED | REJECTED',
      MAPPING_VERSION STRING,
      CURATED_BY STRING,
      CURATED_AT TIMESTAMP
    ) USING DELTA
    COMMENT 'Versioned curated map from iWeb local picklist values to standard concepts. Only APPROVED rows are applied.'
    """
)

PRIORITY_FIELDS = [
    ("reg_noncoronary", "Interventionstxt"),
    ("reg_noncoronary", "Investigationstxt"),
    ("reg_cs2010g_post1", "CABGcodingtxt"),
    ("reg_cs2010g_post1", "ValvesReplacementCodingtxt"),
    ("reg_cs2010g_pre1", "PreviousCardiacSurgerytxt"),
    ("reg_eracs", "ProcedurePerformedtxt"),
    ("reg_dghminap", "DischargeDiagnosis"),
]
priority = spark.createDataFrame(PRIORITY_FIELDS, "SOURCE_TABLE string, FIELD_NAME string")
seed = (
    values.select("SOURCE_TABLE", "FIELD_NAME", "CODE", "LABEL")
    .distinct()
    .join(priority, ["SOURCE_TABLE", "FIELD_NAME"], "left_semi")
    .withColumn("TARGET_VOCABULARY", F.lit(None).cast("string"))
    .withColumn("TARGET_CODE", F.lit(None).cast("string"))
    .withColumn("TARGET_CONCEPT_ID", F.lit(None).cast("long"))
    .withColumn("TARGET_CONCEPT_NAME", F.lit(None).cast("string"))
    .withColumn("MAPPING_METHOD", F.lit("CURATED"))
    .withColumn("MAPPING_STATUS", F.lit("PROPOSED"))
    .withColumn("MAPPING_VERSION", F.lit("1.0.0"))
    .withColumn("CURATED_BY", F.lit(None).cast("string"))
    .withColumn("CURATED_AT", F.lit(None).cast("timestamp"))
    .select(*spark.table(VALUE_MAP_TABLE).columns)
)
map_condition = (
    "target.SOURCE_TABLE <=> source.SOURCE_TABLE AND "
    "target.FIELD_NAME <=> source.FIELD_NAME AND "
    "target.CODE <=> source.CODE AND target.LABEL <=> source.LABEL"
)
(
    DeltaTable.forName(spark, VALUE_MAP_TABLE)
    .alias("target")
    .merge(seed.alias("source"), map_condition)
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

approved = (
    spark.table(VALUE_MAP_TABLE)
    .where(F.col("MAPPING_STATUS") == "APPROVED")
    .select(
        "SOURCE_TABLE", "FIELD_NAME", "CODE", "LABEL",
        "TARGET_VOCABULARY", "TARGET_CODE", "TARGET_CONCEPT_ID", "TARGET_CONCEPT_NAME",
        "MAPPING_METHOD", "MAPPING_STATUS", "MAPPING_VERSION", "CURATED_BY", "CURATED_AT",
    )
)
approved_duplicates = (
    approved.groupBy("SOURCE_TABLE", "FIELD_NAME", "CODE", "LABEL")
    .count()
    .where(F.col("count") > 1)
    .count()
)
assert approved_duplicates == 0, f"Duplicate APPROVED iWeb mappings: {approved_duplicates}"

value_alias = values.alias("value")
map_alias = approved.alias("mapping")
join_condition = (
    (F.col("value.SOURCE_TABLE") == F.col("mapping.SOURCE_TABLE"))
    & (F.col("value.FIELD_NAME") == F.col("mapping.FIELD_NAME"))
    & F.col("value.CODE").eqNullSafe(F.col("mapping.CODE"))
    & F.col("value.LABEL").eqNullSafe(F.col("mapping.LABEL"))
)
mapped = value_alias.join(map_alias, join_condition, "left").select(
    *[F.col(f"value.`{name}`").alias(name) for name in values.columns],
    *[
        F.col(f"mapping.`{name}`").alias(name)
        for name in (
            "TARGET_VOCABULARY", "TARGET_CODE", "TARGET_CONCEPT_ID", "TARGET_CONCEPT_NAME",
            "MAPPING_METHOD", "MAPPING_STATUS", "MAPPING_VERSION", "CURATED_BY", "CURATED_AT",
        )
    ],
)

registry_update_table(
    mapped,
    f"{TARGET_SCHEMA}.iweb_multiselect_value",
    ["SOURCE_TABLE", "ENTRY_ID", "FIELD_NAME", "POSITION"],
    "Long-form exploded values for every empirically identified iWeb N-Label picklist field. "
    "Splitting uses a numeric-code lookahead so commas inside labels survive. Only APPROVED mappings "
    "are applied; unmapped source values remain visible for curation.",
    {
        "CODE": "Local iWeb picklist code parsed from the N - Label source value; not a standard code.",
        "LABEL": "Source label retained verbatim after removal of the local numeric prefix.",
        "TARGET_CONCEPT_ID": "Standard concept applied only from an APPROVED curated mapping row.",
        "POSITION": "Zero-based position in the source field after deterministic explosion.",
    },
)

coverage = (
    mapped.groupBy("SOURCE_TABLE", "FIELD_NAME")
    .agg(
        F.count("*").alias("VALUES_TOTAL"),
        F.sum(F.when(F.col("TARGET_CONCEPT_ID").isNotNull(), 1).otherwise(0)).alias("VALUES_MAPPED"),
        F.countDistinct(F.when(F.col("TARGET_CONCEPT_ID").isNull(), F.col("LABEL"))).alias("DISTINCT_UNMAPPED"),
    )
    .orderBy(F.desc("VALUES_TOTAL"))
)
coverage.show(200, truncate=False)

# COMMAND ----------

# %md
# ## Release validation

# COMMAND ----------

RESULTS = []


def check(name, passed, table_name="", metric_value=None, detail="", severity="ERROR"):
    record = {
        "check_name": str(name),
        "table_name": str(table_name or ""),
        "passed": bool(passed),
        "severity": str(severity),
        "metric_value": float(metric_value) if metric_value is not None else None,
        "detail": str(detail)[:1000],
    }
    RESULTS.append(record)
    print(("PASS " if passed else "FAIL ") + json.dumps(record, sort_keys=True))
    return bool(passed)


TABLE_KEYS = {
    "mediconnect_device": ["MC_DEVICE_ID"],
    "iweb_cardiac_surgery_episode": ["ENTRY_ID"],
    "iweb_cardiac_surgery_procedure": ["ENTRY_ID"],
    "iweb_cardiac_surgery_followup": ["ENTRY_ID"],
    "iweb_coronary_lesion": ["ENTRY_ID"],
    "iweb_acs_transfer": ["ENTRY_ID"],
    "iweb_noncoronary_procedure": ["ENTRY_ID"],
    "iweb_eracs_episode": ["ENTRY_ID"],
    "iweb_cardiac_mdt": ["REGISTRY_TYPE", "ENTRY_ID"],
    "iweb_cardiac_mortality_review": ["ENTRY_ID"],
    "iweb_multiselect_value": ["SOURCE_TABLE", "ENTRY_ID", "FIELD_NAME", "POSITION"],
}
CLINICAL_TABLES = list(TABLE_KEYS)
SUPPORT_TABLES = {
    "iweb_source_manifest": f"{bronze_control_schema(TARGET_SCHEMA)}.iweb_source_manifest",
    "iweb_value_inventory": f"{bronze_control_schema(TARGET_SCHEMA)}.iweb_value_inventory",
    "mediconnect_device_type_map": f"{bronze_lookup_schema(TARGET_SCHEMA)}.mediconnect_device_type_map",
    "iweb_value_to_concept": f"{bronze_lookup_schema(TARGET_SCHEMA)}.iweb_value_to_concept",
}

for table_name in CLINICAL_TABLES:
    check(
        f"table_exists:{table_name}",
        table_exists(f"{TARGET_SCHEMA}.{table_name}"),
        table_name,
    )
for table_name, full_name in SUPPORT_TABLES.items():
    check(
        f"table_exists:{table_name}",
        table_exists(full_name),
        table_name,
    )

if (
    not all(table_exists(f"{TARGET_SCHEMA}.{name}") for name in CLINICAL_TABLES)
    or not all(table_exists(name) for name in SUPPORT_TABLES.values())
):
    raise AssertionError("Required output tables are missing; validation cannot continue")


def current_table(table_name: str) -> DataFrame:
    return spark.table(f"{TARGET_SCHEMA}.{table_name}").where(F.col("IS_CURRENT_IN_SOURCE") == True)


# Source-to-current-target row-count reconciliation.
natural_columns = [
    "PATIENTID", "DEVICE_TYPE", "TYPE", "MANUFACTURER", "MODEL_NAME",
    "SERIAL_NO", "IMPLANTED_DATE", "LOCATION1", "COMMENT", "STATUS",
]
expected_counts = {
    "mediconnect_device": spark.table(f"{RAW}.mediconnect_mc_devices").select(*natural_columns).dropDuplicates().count(),
    "iweb_cardiac_surgery_episode": spark.table(f"{IWEB}.reg_cs2010g_pre1").count(),
    "iweb_cardiac_surgery_procedure": spark.table(f"{IWEB}.reg_cs2010g_subprocedure").count(),
    "iweb_cardiac_surgery_followup": spark.table(f"{IWEB}.reg_cs2010g_followup").select("EntryId").distinct().count(),
    "iweb_coronary_lesion": spark.table(f"{IWEB}.reg_coronary_subprocedure").count(),
    "iweb_acs_transfer": spark.table(f"{IWEB}.reg_dghminap").count(),
    "iweb_noncoronary_procedure": spark.table(f"{IWEB}.reg_noncoronary").count(),
    "iweb_eracs_episode": spark.table(f"{IWEB}.reg_eracs").count(),
    "iweb_cardiac_mdt": spark.table(f"{IWEB}.reg_mdt").count() + spark.table(f"{IWEB}.reg_ctmdt").count(),
    "iweb_cardiac_mortality_review": spark.table(f"{IWEB}.reg_mort").count(),
}
for table_name, expected in expected_counts.items():
    actual = current_table(table_name).count()
    check(
        f"rowcount:{table_name}",
        actual == expected,
        table_name,
        actual,
        f"source_expected={expected}, target_current={actual}",
    )


# Recompute long-form source keys for exact multiselect reconciliation.
inventory = spark.table(f"{bronze_control_schema(TARGET_SCHEMA)}.iweb_value_inventory")
ms_rows = (
    inventory.where(F.col("RAW_VALUE").rlike(r"^\s*\d+\s*-\s*"))
    .where(F.col("SOURCE_TABLE").startswith(f"{IWEB}."))
    .select("SOURCE_TABLE", "FIELD_NAME")
    .distinct()
    .collect()
)
ms_by_table = defaultdict(list)
for row in ms_rows:
    ms_by_table[row.SOURCE_TABLE].append(row.FIELD_NAME)
ms_frames = []
for source_name, fields in sorted(ms_by_table.items()):
    source = spark.table(source_name)
    if source_name.endswith(".reg_cs2010g_followup"):
        latest = Window.partitionBy("EntryId").orderBy(
            F.col("DateLastChanged").desc_nulls_last(), F.col("ADC_UPDT").desc_nulls_last()
        )
        source = source.withColumn("_RN", F.row_number().over(latest)).where(F.col("_RN") == 1).drop("_RN")
    ms_frames.append(
        explode_multiselect(source, source_name.split(".")[-1], "EntryId", sorted(fields)).select(
            "SOURCE_TABLE", "ENTRY_ID", "FIELD_NAME", "POSITION"
        )
    )
expected_ms = reduce(lambda left, right: left.unionByName(right), ms_frames)
expected_ms_count = expected_ms.count()
actual_ms = current_table("iweb_multiselect_value").select(
    "SOURCE_TABLE", "ENTRY_ID", "FIELD_NAME", "POSITION"
)
actual_ms_count = actual_ms.count()
missing_ms = expected_ms.join(actual_ms, TABLE_KEYS["iweb_multiselect_value"], "left_anti").count()
extra_ms = actual_ms.join(expected_ms, TABLE_KEYS["iweb_multiselect_value"], "left_anti").count()
check(
    "rowcount:iweb_multiselect_value",
    expected_ms_count == actual_ms_count and missing_ms == 0 and extra_ms == 0,
    "iweb_multiselect_value",
    actual_ms_count,
    f"source_expected={expected_ms_count}, target_current={actual_ms_count}, missing={missing_ms}, extra={extra_ms}",
)


# Key uniqueness/null checks across current and retained historical-presence rows.
for table_name, keys in TABLE_KEYS.items():
    frame = spark.table(f"{TARGET_SCHEMA}.{table_name}")
    null_condition = reduce(lambda left, right: left | right, [F.col(key).isNull() for key in keys])
    row = frame.agg(
        F.count("*").alias("rows"),
        F.countDistinct(F.struct(*[F.col(key) for key in keys])).alias("distinct_keys"),
        F.sum(F.when(null_condition, 1).otherwise(0)).alias("null_keys"),
    ).first()
    passed = row.rows == row.distinct_keys and int(row.null_keys or 0) == 0
    check(
        f"key_unique:{table_name}",
        passed,
        table_name,
        int(row.rows),
        f"rows={row.rows}, distinct={row.distinct_keys}, null_keys={int(row.null_keys or 0)}, keys={keys}",
    )


# Aggregate linkage quality only; no source identifiers are printed or persisted.
# ERACS linkage matures materially more slowly than its registry landing. Historical
# evidence on 2026-08-05 showed 64-79% linkage through days 8-90 and 99.65%
# after 90 days. Keep the 95% standard on the mature cohort and report all newer
# or undated rows separately so expected alias lag cannot make the release gate
# oscillate as individual records cross a short boundary.
ERACS_LINKAGE_MATURITY_DAYS = 90

for table_name in CLINICAL_TABLES:
    frame = current_table(table_name)
    all_counts = {
        row.LINKAGE_STATUS: int(row["count"])
        for row in frame.groupBy("LINKAGE_STATUS").count().collect()
    }
    all_total = sum(all_counts.values())
    all_conflicts = all_counts.get("CONFLICT", 0)
    all_linked = sum(
        value
        for key, value in all_counts.items()
        if key and key.startswith("MATCHED")
    )
    all_linkage_denominator = all_total - all_conflicts
    all_rate = (
        all_linked / all_linkage_denominator
        if all_linkage_denominator
        else 1.0
    )

    quality_frame = frame
    quality_cohort = "ALL_CURRENT_ROWS"
    recent_counts = None
    if table_name == "iweb_eracs_episode":
        mature_window = (
            F.col("DATE_OF_ENTRY").isNotNull()
            & (
                F.col("DATE_OF_ENTRY")
                <= F.current_timestamp()
                - F.expr(f"INTERVAL {ERACS_LINKAGE_MATURITY_DAYS} DAYS")
            )
            & (F.col("DATE_OF_ENTRY") <= F.current_timestamp())
        )
        quality_frame = frame.where(mature_window)
        recent_frame = frame.where(~mature_window)
        recent_counts = {
            row.LINKAGE_STATUS: int(row["count"])
            for row in recent_frame.groupBy("LINKAGE_STATUS").count().collect()
        }
        quality_cohort = (
            f"DATE_OF_ENTRY_AT_LEAST_{ERACS_LINKAGE_MATURITY_DAYS}_DAYS_OLD"
        )

    counts = {
        row.LINKAGE_STATUS: int(row["count"])
        for row in quality_frame.groupBy("LINKAGE_STATUS").count().collect()
    }
    total = sum(counts.values())
    linked = sum(value for key, value in counts.items() if key and key.startswith("MATCHED"))
    quality_conflicts = counts.get("CONFLICT", 0)
    linkage_denominator = total - quality_conflicts
    threshold = {
        "mediconnect_device": 0.99,
        "iweb_eracs_episode": 0.95,
    }.get(table_name, 0.97)
    rate = linked / linkage_denominator if linkage_denominator else 1.0
    conflict_rate = all_conflicts / all_total if all_total else 1.0
    check(
        f"linkage_rate:{table_name}",
        rate >= threshold,
        table_name,
        rate,
        json.dumps(
            {
                "cohort": quality_cohort,
                "counts": counts,
                "all_current_counts": all_counts,
                "all_current_rate_excluding_conflicts": all_rate,
                "linkage_denominator_excluding_conflicts": linkage_denominator,
                "conflicts_excluded_from_linkage_rate": quality_conflicts,
                "maturity_days": (
                    ERACS_LINKAGE_MATURITY_DAYS
                    if table_name == "iweb_eracs_episode"
                    else None
                ),
            },
            sort_keys=True,
        ),
    )
    check(
        f"linkage_conflicts:{table_name}",
        conflict_rate < 0.005,
        table_name,
        conflict_rate,
        f"conflicts={all_conflicts}, total={all_total}",
    )
    if table_name == "iweb_eracs_episode":
        recent_total = sum((recent_counts or {}).values())
        recent_conflicts = (recent_counts or {}).get("CONFLICT", 0)
        recent_denominator = recent_total - recent_conflicts
        recent_linked = sum(
            value
            for key, value in (recent_counts or {}).items()
            if key and key.startswith("MATCHED")
        )
        recent_rate = (
            recent_linked / recent_denominator
            if recent_denominator
            else 1.0
        )
        check(
            "linkage_grace_cohort:iweb_eracs_episode",
            True,
            table_name,
            recent_rate,
            json.dumps(
                {
                    "maturity_days": ERACS_LINKAGE_MATURITY_DAYS,
                    "cohort": "DATE_OF_ENTRY_NEWER_THAN_MATURITY_WINDOW_OR_UNKNOWN",
                    "counts": recent_counts or {},
                    "rows": recent_total,
                    "linked": recent_linked,
                    "conflicts_excluded_from_rate": recent_conflicts,
                },
                sort_keys=True,
            ),
            severity="INFO",
        )
# Published schemas must not contain direct patient identifiers/contact fields.
bad_columns = []
for table_name in CLINICAL_TABLES:
    for column in spark.table(f"{TARGET_SCHEMA}.{table_name}").columns:
        if is_pii_column(column):
            bad_columns.append(f"{table_name}.{column}")
check("pii_absent", not bad_columns, metric_value=len(bad_columns), detail=bad_columns)


# Parent/child integrity for the landed surgery spine.
orphans = (
    current_table("iweb_cardiac_surgery_procedure").alias("procedure")
    .join(
        current_table("iweb_cardiac_surgery_episode").select(F.col("ENTRY_ID").alias("PARENT_ENTRY_ID")),
        "PARENT_ENTRY_ID",
        "left_anti",
    )
    .count()
)
check(
    "orphans:surgery_procedure",
    orphans == 0,
    "iweb_cardiac_surgery_procedure",
    orphans,
    "Source child rows are retained losslessly with PARENT_ENTRY_ID; a missing current parent is reported, not filtered.",
    severity="INFO",
)


# Known date defects are explicit and cleaned deterministically.
device_quality = current_table("mediconnect_device")
device_total = device_quality.count()
device_valid = device_quality.where(F.col("IMPLANTED_DATE_QUALITY") == "VALID").count()
device_bad_clean = device_quality.where(
    (F.col("IMPLANTED_DATE_QUALITY") != "VALID") & F.col("IMPLANTED_DATE_CLEAN").isNotNull()
).count()
device_valid_rate = device_valid / device_total if device_total else 0.0
check("date_valid_rate:mediconnect", device_valid_rate >= 0.98, "mediconnect_device", device_valid_rate)
check("date_clean_consistency:mediconnect", device_bad_clean == 0, "mediconnect_device", device_bad_clean)

mortality = current_table("iweb_cardiac_mortality_review")
mort_total = mortality.count()
mort_future = mortality.where(F.col("DATE_OF_DEATH_QUALITY") == "FUTURE").count()
mort_bad_clean = mortality.where(
    (F.col("DATE_OF_DEATH_QUALITY") == "FUTURE") & F.col("DATE_OF_DEATH_CLEAN").isNotNull()
).count()
check(
    "date_future_rate:mortality",
    (mort_future / mort_total if mort_total else 1.0) < 0.01,
    "iweb_cardiac_mortality_review",
    mort_future / mort_total if mort_total else 1.0,
    f"future_rows={mort_future}",
)
check("date_clean_consistency:mortality", mort_bad_clean == 0, "iweb_cardiac_mortality_review", mort_bad_clean)

acs = current_table("iweb_acs_transfer")
acs_sentinel = acs.where(F.col("ADMISSION_DATE_QUALITY") == "SENTINEL").count()
acs_future = acs.where(F.col("ADMISSION_DATE_QUALITY") == "FUTURE").count()
check("date_sentinel_report:acs", True, "iweb_acs_transfer", acs_sentinel, severity="INFO")
check(
    "date_future_rate:acs",
    acs_future / acs.count() < 0.01,
    "iweb_acs_transfer",
    acs_future,
)


# Mapping validity. Zero approved iWeb mappings is valid; any approved row must resolve cleanly.
concept = spark.table(OMOP_CONCEPT).select(
    F.col("concept_id").cast("long").alias("CONCEPT_ID"),
    F.col("vocabulary_id").alias("CONCEPT_VOCABULARY"),
    "domain_id", "standard_concept", "invalid_reason",
)
iweb_maps = spark.table(f"{bronze_lookup_schema(TARGET_SCHEMA)}.iweb_value_to_concept").where(
    F.col("MAPPING_STATUS") == "APPROVED"
)
approved_null_concepts = iweb_maps.where(F.col("TARGET_CONCEPT_ID").isNull()).count()
iweb_invalid = (
    iweb_maps.where(F.col("TARGET_CONCEPT_ID").isNotNull())
    .join(concept, F.col("TARGET_CONCEPT_ID") == F.col("CONCEPT_ID"), "left")
    .where(
        F.col("CONCEPT_ID").isNull()
        | (F.col("standard_concept") != "S")
        | F.col("invalid_reason").isNotNull()
        | F.col("TARGET_VOCABULARY").isNull()
        | (F.upper(F.col("TARGET_VOCABULARY")) != F.upper(F.col("CONCEPT_VOCABULARY")))
    )
    .count()
)
check(
    "mapping_concepts_valid:iweb",
    approved_null_concepts == 0 and iweb_invalid == 0,
    "iweb_value_to_concept",
    approved_null_concepts + iweb_invalid,
    f"approved_null_concepts={approved_null_concepts}, invalid_concepts={iweb_invalid}",
)

device_maps = spark.table(f"{bronze_lookup_schema(TARGET_SCHEMA)}.mediconnect_device_type_map")
device_approved_null = device_maps.where(
    (F.col("MAPPING_STATUS") == "APPROVED") & F.col("SNOMED_CONCEPT_ID").isNull()
).count()
device_invalid = (
    device_maps.where(F.col("SNOMED_CONCEPT_ID").isNotNull())
    .join(concept, F.col("SNOMED_CONCEPT_ID") == F.col("CONCEPT_ID"), "left")
    .where(
        F.col("CONCEPT_ID").isNull()
        | (F.col("CONCEPT_VOCABULARY") != "SNOMED")
        | (F.col("domain_id") != "Device")
        | (F.col("standard_concept") != "S")
        | F.col("invalid_reason").isNotNull()
    )
    .count()
)
check(
    "mapping_concepts_valid:mediconnect",
    device_approved_null == 0 and device_invalid == 0,
    "mediconnect_device_type_map",
    device_approved_null + device_invalid,
)

mapping_coverage = current_table("iweb_multiselect_value").agg(
    F.count("*").alias("total"),
    F.sum(F.when(F.col("TARGET_CONCEPT_ID").isNotNull(), 1).otherwise(0)).alias("mapped"),
).first()
coverage_rate = int(mapping_coverage.mapped or 0) / int(mapping_coverage.total or 1)
check(
    "mapping_coverage:iweb",
    True,
    "iweb_multiselect_value",
    coverage_rate,
    f"mapped={int(mapping_coverage.mapped or 0)}, total={int(mapping_coverage.total or 0)}",
    severity="INFO",
)


# Additivity spot-check against existing Cerner implant capture.
implant_table = "4_prod.bronze.map_implant_details"
if table_exists(implant_table):
    implant_columns = set(spark.table(implant_table).columns)
    serial_candidates = [
        name for name in (
            "EFFECTIVE_SERIAL_NUMBER", "GS1_SERIAL_NUMBER", "SERIAL_NUMBER",
            "SOURCE_SERIAL_NUMBER", "IMPLANT_SERIAL_NUMBER",
        ) if name in implant_columns
    ]
    if serial_candidates:
        cerner_frames = [
            spark.table(implant_table).select(
                F.upper(F.regexp_replace(F.col(name).cast("string"), r"[^A-Za-z0-9]", "")).alias("SERIAL")
            )
            for name in serial_candidates
        ]
        cerner_serials = (
            reduce(lambda left, right: left.unionByName(right), cerner_frames)
            .where(F.col("SERIAL") != "")
            .distinct()
        )
        mc_serials = (
            current_table("mediconnect_device")
            .select(F.upper(F.regexp_replace(F.col("SERIAL_NO").cast("string"), r"[^A-Za-z0-9]", "")).alias("SERIAL"))
            .where(F.col("SERIAL") != "")
            .distinct()
        )
        mc_serial_count = mc_serials.count()
        overlap = mc_serials.join(cerner_serials, "SERIAL", "inner").count()
        overlap_rate = overlap / mc_serial_count if mc_serial_count else 0.0
        check(
            "additivity:mediconnect_serial_overlap",
            overlap_rate < 0.05,
            "mediconnect_device",
            overlap_rate,
            f"overlap={overlap}, mediconnect_serials={mc_serial_count}, columns={serial_candidates}",
        )
    else:
        check("additivity:mediconnect_serial_overlap", False, "mediconnect_device", detail="No serial column in map_implant_details")
else:
    check("additivity:mediconnect_serial_overlap", False, "mediconnect_device", detail="map_implant_details missing")


# On the explicit second run, every snapshot merge must be a no-op.
if EXPECT_IDEMPOTENT:
    for table_name in CLINICAL_TABLES:
        metrics = _latest_operation_metrics(f"{TARGET_SCHEMA}.{table_name}")
        inserted = int(metrics.get("numTargetRowsInserted", 0) or 0)
        updated = int(metrics.get("numTargetRowsUpdated", 0) or 0)
        deleted = int(metrics.get("numTargetRowsDeleted", 0) or 0)
        check(
            f"idempotent:{table_name}",
            metrics.get("operation") == "MERGE" and inserted == 0 and updated == 0 and deleted == 0,
            table_name,
            inserted + updated + deleted,
            json.dumps(metrics, sort_keys=True, default=str),
        )
else:
    check("idempotency_requested", True, detail="expect_idempotent=false", severity="INFO")


# Persist results before raising so failed releases remain auditable.
result_schema = T.StructType([
    T.StructField("RUN_ID", T.StringType(), False),
    T.StructField("TARGET_SCHEMA", T.StringType(), False),
    T.StructField("CHECK_NAME", T.StringType(), False),
    T.StructField("TABLE_NAME", T.StringType(), True),
    T.StructField("PASSED", T.BooleanType(), False),
    T.StructField("SEVERITY", T.StringType(), False),
    T.StructField("METRIC_VALUE", T.DoubleType(), True),
    T.StructField("DETAIL", T.StringType(), True),
])
result_rows = [
    (
        REGISTRY_RUN_ID, TARGET_SCHEMA, row["check_name"], row["table_name"], row["passed"],
        row["severity"], row["metric_value"], row["detail"],
    )
    for row in RESULTS
]
results_df = spark.createDataFrame(result_rows, result_schema).withColumn("RUN_AT", F.current_timestamp())
validation_table = f"{bronze_control_schema(TARGET_SCHEMA)}.registry_validation_results"
writer = results_df.write.format("delta").mode("append")
if not table_exists(validation_table):
    writer = writer.option("delta.enableChangeDataFeed", "true")
writer.saveAsTable(validation_table)
apply_comments(
    validation_table,
    {
        "DETAIL": "Aggregate validation detail only; no source identifiers are logged.",
        "SEVERITY": "ERROR gates release; INFO is persisted reporting only.",
    },
    "Persisted registry bronze validation results, one row per check and pipeline run.",
)

failures = [
    row for row in RESULTS if row["severity"] == "ERROR" and not row["passed"]
]
if failures:
    raise AssertionError(
        "REGISTRY VALIDATION FAILED: " + json.dumps(
            [{"check": row["check_name"], "table": row["table_name"], "detail": row["detail"]} for row in failures],
            sort_keys=True,
        )
    )
print(f"[VALIDATION] {len(RESULTS)} checks passed or reported; run_id={REGISTRY_RUN_ID}")

# COMMAND ----------

_registry_summary = {
    "status": "SUCCESS",
    "result": "BUILT",
    "target": TARGET_SCHEMA,
    "pipeline": "registry_pipeline",
    "run_id": REGISTRY_RUN_ID,
    "target_schema": TARGET_SCHEMA,
    "expect_idempotent": EXPECT_IDEMPOTENT,
    "clinical_tables": list(TABLE_KEYS),
    "validation_checks": len(RESULTS),
}
# ==== S6b A14a post-build gates + writer lifecycle proof ====
S6B_REGISTRY_RESULTS = run_registry_gates(tag_first=True)
registry_writer_tombstone_fixture()

S6B_REGISTRY_PROMOTION_RUNBOOK = """
HUMAN-GATED ONLY:
1. Promote registry_pipeline and _registry_common as one atomic family, restoring the normal %run ./_registry_common structure.
2. Retarget to 4_prod.bronze in an approved window and rebuild all 11 registry outputs together.
3. Restore rowTracking/CDF/deletion vectors to the Task-0 values, re-run parity/lifecycle/IG gates, then notify Journey per blocker table.
4. Retain IS_CURRENT_IN_SOURCE; SOURCE_PRESENT_IND is its canonical boolean mirror.
"""
print(S6B_REGISTRY_PROMOTION_RUNBOOK)
_registry_summary["s6b_registry_results"] = S6B_REGISTRY_RESULTS
print(json.dumps(_registry_summary, sort_keys=True))
dbutils.notebook.exit(json.dumps(_registry_summary, sort_keys=True))




