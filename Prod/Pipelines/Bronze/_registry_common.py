# Databricks notebook source
# MAGIC %md
# MAGIC # Registry Bronze Shared Helpers
# MAGIC
# MAGIC Shared configuration, source preflight, privacy-safe source inventory, deterministic
# MAGIC person linkage, source-value helpers, and the ROW_HASH-gated full-snapshot Delta writer.
# MAGIC No Spark cache/persist or SparkContext APIs are used.

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
    "pipeline_run_id": "",
    "expect_idempotent": "false",
}.items():
    _ensure_text_widget(_name, _default)


TARGET_SCHEMA = str(dbutils.widgets.get("target_schema")).strip() or "8_dev.bronze"
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

AUDIT_COLS = {"ROW_HASH", "PIPELINE_LOADED_AT", "IS_CURRENT_IN_SOURCE"}
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
        apply_comments(target, comments or {}, table_comment)
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
                "OR NOT COALESCE(t.`IS_CURRENT_IN_SOURCE`, false)"
            )
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceUpdate(
                condition="t.`IS_CURRENT_IN_SOURCE` = true",
                set={"IS_CURRENT_IN_SOURCE": "false", "PIPELINE_LOADED_AT": "current_timestamp()"},
            )
            .execute()
        )
        apply_comments(target, comments or {}, table_comment)
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
    if sub_rows != sub_keys or sub_nulls or sub_orphans:
        raise AssertionError(
            f"CS2010G child gate failed rows={sub_rows} keys={sub_keys} "
            f"nulls={sub_nulls} orphans={sub_orphans}"
        )

    result = {
        "column_counts": column_counts,
        "module_stats": module_stats,
        "module_set_differences": set_differences,
        "subprocedure": {"rows": sub_rows, "keys": sub_keys, "orphans": sub_orphans},
    }
    print("[SOURCE_PREFLIGHT] " + json.dumps(result, sort_keys=True))
    return result


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
