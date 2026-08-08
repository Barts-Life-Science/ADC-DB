# Databricks notebook source
# MAGIC %md
# MAGIC # JAC Pharmacy Bronze Pipeline
# MAGIC
# MAGIC Brings JAC pharmacy supply data into bronze:
# MAGIC - map_pharmacy_issue - OPD + PHM daily issue/return/credit transactions
# MAGIC - map_homecare_request - homecare requests at item grain
# MAGIC - map_pharmacy_drug - JAC drug dictionary with deterministic dm+d VTM mapping
# MAGIC
# MAGIC jac_phm_patient_basic is used only as an identifier bridge to PERSON_ID; it is not copied.
# MAGIC Loading is snapshot-aware: source Delta version gating, row-hash MERGE,
# MAGIC SOURCE_PRESENT_IND soft-deletes, and checkpoints in jac_pipeline_state.

# COMMAND ----------

# MAGIC %run ./_bronze_common

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from functools import reduce

try:
    dbutils.widgets.text("target_schema", "8_dev.bronze")
except Exception:
    pass
try:
    dbutils.widgets.text("enable_dmd_name_mapping", "true")
except Exception:
    pass

TARGET_SCHEMA = bronze_value("target_schema", "8_dev.bronze")
JAC_RUN_ID = bronze_run_id()
FORCE_FULL_REFRESH = bronze_bool("force_full_refresh", False)
ENABLE_DMD = bronze_bool("enable_dmd_name_mapping", True)
_STARTED_AT = bronze_utc_now()

RAW = "4_prod.raw"
SRC_ISSUES_PHM = f"{RAW}.jac_phm_xdailyissues"
SRC_ISSUES_OPD = f"{RAW}.jac_opd_xdailyissues"
SRC_PATIENT = f"{RAW}.jac_phm_patient_basic"
SRC_HC_HDR = f"{RAW}.jac_phm_homecare_request_header"
SRC_HC_ITEM = f"{RAW}.jac_phm_homecare_request_item"
PERSON_ALIAS = f"{RAW}.mill_person_alias"
OMOP_CONCEPT = "3_lookup.omop.concept"
OMOP_CONCEPT_SYNONYM = "3_lookup.omop.concept_synonym"
CARE_SITE = "4_prod.bronze.map_care_site"
MRN_ALIAS_TYPE, NHS_ALIAS_TYPE = 10, 18

ISSUE_TABLE = f"{TARGET_SCHEMA}.map_pharmacy_issue"
HOMECARE_TABLE = f"{TARGET_SCHEMA}.map_homecare_request"
DRUG_TABLE = f"{bronze_lookup_schema(TARGET_SCHEMA)}.map_pharmacy_drug"
STATE_TABLE = f"{bronze_control_schema(TARGET_SCHEMA)}.jac_pipeline_state"

JAC_SOURCES = [SRC_ISSUES_PHM, SRC_ISSUES_OPD, SRC_PATIENT, SRC_HC_HDR, SRC_HC_ITEM]
_missing = [
    table
    for table in JAC_SOURCES + [PERSON_ALIAS, OMOP_CONCEPT]
    if not bronze_table_exists(table)
]
assert not _missing, f"Missing sources: {_missing}"


def qident(name: str) -> str:
    q = chr(96)
    return q + name.replace(q, q + q) + q


def qname(name: str) -> str:
    return ".".join(qident(part) for part in name.split("."))


def blank_to_null(column):
    value = F.trim(column.cast("string"))
    return F.when(value == "", F.lit(None).cast("string")).otherwise(value)


def decode_map(column, mapping, default="OTHER"):
    expression = F.lit(default).cast("string")
    for key, value in mapping.items():
        expression = F.when(column == key, F.lit(value)).otherwise(expression)
    return expression


ISSUE_CATEGORY_MAP = {
    "ONESTOP": "ISSUE",
    "INP": "ISSUE",
    "TTA": "ISSUE",
    "OUT": "ISSUE",
    "CLINIC": "ISSUE",
    "DAYCASE": "ISSUE",
    "A&E": "ISSUE",
    "CRED": "RETURN_CREDIT",
    "RET": "RETURN_CREDIT",
    "REQ": "REQUEST",
    "KADJ": "ADJUSTMENT",
    "QUAR": "ADJUSTMENT",
    "XFR": "TRANSFER",
    "RGO": "OTHER",
    "MRAW": "OTHER",
    "TRADE": "OTHER",
}
HC_REQUEST_STATUS = {
    "C": "COMPLETE",
    "R": "RELEASED",
    "I": "IN_PROGRESS",
    "!": "CANCELLED_OR_ERROR",
}
HC_ITEM_STATUS = {
    "C": "COMPLETE",
    "I": "INCOMPLETE",
    "!": "CANCELLED_OR_ERROR",
}


def parse_quantity(df: DataFrame, col="quantity") -> DataFrame:
    quantity = F.trim(F.col(col).cast("string"))
    containers = F.regexp_extract(quantity, r"^(-?\d+)\s*x\s*", 1)
    units = F.regexp_extract(quantity, r"^-?\d+\s*x\s*(\d+(?:\.\d+)?)", 1)
    leading = F.regexp_extract(quantity, r"^(-?\d+(?:\.\d+)?)\s", 1)
    has_pack_pattern = (containers != "") & (units != "")
    return (
        df.withColumn("QUANTITY_RAW", blank_to_null(quantity))
        .withColumn(
            "QUANTITY_PARSE_METHOD",
            F.when(has_pack_pattern, F.lit("PACK_PATTERN")).when(
                leading != "", F.lit("LEADING_NUMBER")
            ),
        )
        .withColumn(
            "ISSUED_CONTAINERS",
            F.when(has_pack_pattern, containers.cast("decimal(18,2)")),
        )
        .withColumn(
            "UNITS_PER_CONTAINER",
            F.when(has_pack_pattern, units.cast("decimal(18,2)")),
        )
        .withColumn(
            "TOTAL_UNITS",
            F.when(
                has_pack_pattern,
                containers.cast("decimal(18,2)") * units.cast("decimal(18,2)"),
            ).when(leading != "", leading.cast("decimal(18,2)")),
        )
    )


def parse_bnf(df: DataFrame, col="bnf_code") -> DataFrame:
    raw = F.col(col).cast("string")
    normalised = F.upper(F.regexp_replace(F.trim(raw), r"[^0-9A-Za-z.]", ""))
    return (
        df.withColumn("BNF_CODE_RAW", raw)
        .withColumn("BNF_CODE_NORMALISED", blank_to_null(normalised))
        .withColumn(
            "BNF_CHAPTER",
            F.when(normalised.rlike(r"^[0-9A-Z]{2}"), F.substring(normalised, 1, 2)),
        )
        .withColumn(
            "BNF_SECTION",
            F.when(
                normalised.rlike(r"^[0-9A-Z]{2}\.[0-9A-Z]{2}"),
                F.substring(normalised, 1, 5),
            ),
        )
        .withColumn(
            "BNF_PARAGRAPH",
            F.when(
                normalised.rlike(r"^[0-9A-Z]{2}\.[0-9A-Z]{2}\.[0-9A-Z]{2}"),
                F.substring(normalised, 1, 8),
            ),
        )
        .withColumn(
            "BNF_SUBPARAGRAPH",
            F.when(
                normalised.rlike(
                    r"^[0-9A-Z]{2}\.[0-9A-Z]{2}\.[0-9A-Z]{2}\.[0-9A-Z]{2}"
                ),
                F.substring(normalised, 1, 11),
            ),
        )
    )


_quantity_tests = {
    row["QUANTITY_RAW"]: row
    for row in parse_quantity(
        spark.createDataFrame(
            [
                ("4 x 1 Vial Pack",),
                ("99 Tablet",),
                ("0 x 0.75 mL Pack",),
                ("garbage",),
                (None,),
            ],
            "quantity string",
        )
    ).collect()
    if row["QUANTITY_RAW"] is not None
}
assert _quantity_tests["4 x 1 Vial Pack"]["QUANTITY_PARSE_METHOD"] == "PACK_PATTERN"
assert float(_quantity_tests["4 x 1 Vial Pack"]["TOTAL_UNITS"]) == 4.0
assert _quantity_tests["99 Tablet"]["QUANTITY_PARSE_METHOD"] == "LEADING_NUMBER"
assert float(_quantity_tests["99 Tablet"]["TOTAL_UNITS"]) == 99.0
assert float(_quantity_tests["0 x 0.75 mL Pack"]["UNITS_PER_CONTAINER"]) == 0.75
assert _quantity_tests["garbage"]["QUANTITY_PARSE_METHOD"] is None
assert _quantity_tests["garbage"]["TOTAL_UNITS"] is None

_bnf_tests = {
    row["BNF_CODE_RAW"]: row
    for row in parse_bnf(
        spark.createDataFrame(
            [("02.05.05.01",), ("08.02",), ("99",), ("AA.00",)],
            "bnf_code string",
        )
    ).collect()
}
assert _bnf_tests["02.05.05.01"]["BNF_PARAGRAPH"] == "02.05.05"
assert _bnf_tests["02.05.05.01"]["BNF_SUBPARAGRAPH"] == "02.05.05.01"
assert _bnf_tests["08.02"]["BNF_CHAPTER"] == "08"
assert _bnf_tests["08.02"]["BNF_SECTION"] == "08.02"
assert _bnf_tests["99"]["BNF_SECTION"] is None
assert _bnf_tests["AA.00"]["BNF_CHAPTER"] == "AA"
print("[JAC] parser tests passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snapshot-aware loading

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qname(TARGET_SCHEMA)}")
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {qname(STATE_TABLE)} (
      target_table STRING NOT NULL,
      source_table STRING NOT NULL,
      source_version BIGINT NOT NULL,
      source_rows BIGINT,
      run_id STRING NOT NULL,
      committed_at TIMESTAMP NOT NULL
    ) USING DELTA
    COMMENT 'JAC bronze pipeline checkpoints: last processed Delta version per target and source.'
    """
)

HASH_EXCLUDE = {
    "ROW_HASH",
    "ADC_UPDT",
    "SOURCE_PRESENT_IND",
    "CURATED_DMD_CONCEPT_ID",
    "CURATED_STATUS",
    "CURATED_NOTES",
}


def source_version(table: str) -> int:
    row = spark.sql(f"DESCRIBE HISTORY {qname(table)} LIMIT 1").collect()[0]
    return int(row["version"])


def last_committed_version(target: str, source: str):
    row = (
        spark.table(STATE_TABLE)
        .where(
            (F.col("target_table") == target)
            & (F.col("source_table") == source)
        )
        .agg(F.max("source_version").alias("version"))
        .collect()[0]
    )
    return row["version"]


def needs_run(target: str, sources: list) -> bool:
    if FORCE_FULL_REFRESH or not bronze_table_exists(target):
        return True
    for source in sources:
        previous = last_committed_version(target, source)
        if previous is None or source_version(source) > previous:
            return True
    return False


def with_row_hash(df: DataFrame) -> DataFrame:
    columns = sorted(column for column in df.columns if column not in HASH_EXCLUDE)
    payload = F.concat_ws(
        "\u0001",
        *[
            F.coalesce(F.col(column).cast("string"), F.lit("<NULL>"))
            for column in columns
        ],
    )
    return df.withColumn("ROW_HASH", F.sha2(payload, 256))


def verify_unique_key(df: DataFrame, keys) -> int:
    return (
        df.groupBy(*keys)
        .count()
        .where(F.col("count") > 1)
        .count()
    )


def assert_unique_non_null(df: DataFrame, keys, label: str) -> None:
    null_condition = reduce(
        lambda left, right: left | right,
        [F.col(key).isNull() for key in keys],
    )
    null_count = df.where(null_condition).count()
    duplicate_count = verify_unique_key(df, keys)
    assert null_count == 0, f"{label}: {null_count} rows have NULL key values"
    assert duplicate_count == 0, f"{label}: {duplicate_count} duplicate keys"


def record_checkpoints(
    target: str,
    versions: dict,
    output_rows: int,
) -> None:
    rows = [
        (
            target,
            source,
            int(version),
            output_rows if index == 0 else None,
            JAC_RUN_ID,
        )
        for index, (source, version) in enumerate(versions.items())
    ]
    updates = (
        spark.createDataFrame(
            rows,
            """
            target_table string,
            source_table string,
            source_version long,
            source_rows long,
            run_id string
            """,
        )
        .withColumn("committed_at", F.current_timestamp())
    )
    (
        DeltaTable.forName(spark, STATE_TABLE)
        .alias("t")
        .merge(
            updates.alias("s"),
            "t.target_table = s.target_table AND t.source_table = s.source_table",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def jac_update_table(
    df: DataFrame,
    target: str,
    keys: list,
    sources: list,
    preserve_columns=None,
) -> dict:
    """Full-snapshot upsert with row hashes, soft deletes, and source checkpoints."""
    preserve_columns = set(preserve_columns or [])
    versions = {source: source_version(source) for source in sources}
    output_rows = df.count()
    staged = (
        with_row_hash(df)
        .withColumn("SOURCE_PRESENT_IND", F.lit(True))
        .withColumn("ADC_UPDT", F.current_timestamp())
    )
    if not bronze_table_exists(target):
        staged.write.format("delta").saveAsTable(target)
        metrics = {"operation": "CREATE", "rows": output_rows}
    else:
        condition = " AND ".join(
            f"t.{qident(key)} <=> s.{qident(key)}" for key in keys
        )
        update_columns = [
            column
            for column in staged.columns
            if column not in preserve_columns
        ]
        update_values = {
            column: f"s.{qident(column)}" for column in update_columns
        }
        insert_values = {
            column: f"s.{qident(column)}" for column in staged.columns
        }
        (
            DeltaTable.forName(spark, target)
            .alias("t")
            .merge(staged.alias("s"), condition)
            .whenMatchedUpdate(
                condition=(
                    "t.ROW_HASH <> s.ROW_HASH "
                    "OR t.SOURCE_PRESENT_IND = false"
                ),
                set=update_values,
            )
            .whenNotMatchedInsert(values=insert_values)
            .whenNotMatchedBySourceUpdate(
                condition="t.SOURCE_PRESENT_IND = true",
                set={
                    "SOURCE_PRESENT_IND": "false",
                    "ADC_UPDT": "current_timestamp()",
                },
            )
            .execute()
        )
        history = spark.sql(
            f"DESCRIBE HISTORY {qname(target)} LIMIT 1"
        ).collect()[0]
        operation_metrics = history["operationMetrics"] or {}
        metrics = {
            "operation": history["operation"],
            **{
                key: value
                for key, value in operation_metrics.items()
                if key
                in (
                    "numTargetRowsInserted",
                    "numTargetRowsUpdated",
                    "numTargetRowsDeleted",
                    "numSourceRows",
                )
            },
        }
    record_checkpoints(target, versions, output_rows)
    return metrics


def apply_comments(table: str, comments: dict, table_comment: str) -> None:
    def escape(value):
        return str(value).replace("\\", "\\\\").replace("'", "''")

    spark.sql(
        f"COMMENT ON TABLE {qname(table)} IS '{escape(table_comment)}'"
    )
    columns = set(spark.table(table).columns)
    for column, comment in comments.items():
        if column in columns:
            spark.sql(
                f"ALTER TABLE {qname(table)} ALTER COLUMN {qident(column)} "
                f"COMMENT '{escape(comment)}'"
            )


def dmd_mode_changed(table: str) -> bool:
    if not bronze_table_exists(table):
        return False
    methods = {
        row["DRUG_MAPPING_METHOD"]
        for row in spark.table(table)
        .select("DRUG_MAPPING_METHOD")
        .where(F.col("DRUG_MAPPING_METHOD").isNotNull())
        .distinct()
        .collect()
    }
    return ("DISABLED" in methods) if ENABLE_DMD else any(
        method != "DISABLED" for method in methods
    )


print(
    f"[JAC] target={TARGET_SCHEMA}, run_id={JAC_RUN_ID}, "
    f"force_full_refresh={FORCE_FULL_REFRESH}, dmd_mapping={ENABLE_DMD}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Person bridge

# COMMAND ----------


def _digits(column):
    return F.regexp_replace(column.cast("string"), r"[^0-9]", "")


def mrn_norm(column):
    value = F.upper(
        F.regexp_replace(F.trim(column.cast("string")), r"[^0-9A-Za-z]", "")
    )
    return F.when(F.length(value) > 0, value)


def nhs_norm(column):
    digits = _digits(column)
    return F.when(F.length(digits) == 10, digits)


def nhs_checksum_ok(normalised_column):
    weighted = reduce(
        lambda accumulator, index: accumulator
        + F.substring(normalised_column, index + 1, 1).cast("int")
        * (10 - index),
        range(9),
        F.lit(0),
    )
    check_digit = 11 - (weighted % 11)
    check_digit = F.when(check_digit == 11, F.lit(0)).otherwise(check_digit)
    return (check_digit != 10) & (
        check_digit == F.substring(normalised_column, 10, 1).cast("int")
    )


def _alias_bridge(alias_type: int, out_person: str, out_count: str) -> DataFrame:
    normalised = (
        mrn_norm(F.col("ALIAS"))
        if alias_type == MRN_ALIAS_TYPE
        else nhs_norm(F.col("ALIAS"))
    )
    aliases = (
        spark.table(PERSON_ALIAS)
        .where(
            (F.col("PERSON_ALIAS_TYPE_CD") == alias_type)
            & (F.col("ACTIVE_IND") == 1)
        )
        .withColumn("ALIAS_NORM", normalised)
        .where(F.col("ALIAS_NORM").isNotNull())
        .groupBy("ALIAS_NORM")
        .agg(
            F.countDistinct("PERSON_ID").alias(out_count),
            F.first("PERSON_ID", ignorenulls=True).alias("_person_id"),
        )
    )
    return (
        aliases.withColumn(
            out_person,
            F.when(F.col(out_count) == 1, F.col("_person_id")),
        )
        .drop("_person_id")
    )


def build_person_bridge() -> DataFrame:
    patients = spark.table(SRC_PATIENT).select(
        F.col("lnkpid").alias("LNKPID"),
        mrn_norm(F.col("hospital_no")).alias("_mrn"),
        nhs_norm(F.col("nhs_no")).alias("_nhs"),
    )
    assert_unique_non_null(patients, ["LNKPID"], SRC_PATIENT)
    return (
        patients.withColumn(
            "_nhs",
            F.when(nhs_checksum_ok(F.col("_nhs")), F.col("_nhs")),
        )
        .join(
            _alias_bridge(
                MRN_ALIAS_TYPE, "_mrn_person_id", "MRN_CANDIDATES"
            ).withColumnRenamed("ALIAS_NORM", "_mrn"),
            "_mrn",
            "left",
        )
        .join(
            _alias_bridge(
                NHS_ALIAS_TYPE, "_nhs_person_id", "NHS_CANDIDATES"
            ).withColumnRenamed("ALIAS_NORM", "_nhs"),
            "_nhs",
            "left",
        )
        .withColumn(
            "PERSON_MATCH_METHOD",
            F.when(
                F.col("_mrn_person_id").isNotNull()
                & (F.col("_mrn_person_id") == F.col("_nhs_person_id")),
                "MRN_NHS_AGREE",
            )
            .when(
                F.col("_mrn_person_id").isNotNull()
                & F.col("_nhs_person_id").isNotNull(),
                "CONFLICT",
            )
            .when(
                F.col("_mrn_person_id").isNotNull()
                & F.col("_nhs_person_id").isNull()
                & (
                    F.coalesce(F.col("NHS_CANDIDATES"), F.lit(0))
                    <= 1
                ),
                "MRN_ONLY",
            )
            .when(
                F.col("_nhs_person_id").isNotNull()
                & F.col("_mrn_person_id").isNull()
                & (
                    F.coalesce(F.col("MRN_CANDIDATES"), F.lit(0))
                    <= 1
                ),
                "NHS_ONLY",
            )
            .when(
                (F.coalesce(F.col("MRN_CANDIDATES"), F.lit(0)) > 1)
                | (F.coalesce(F.col("NHS_CANDIDATES"), F.lit(0)) > 1),
                "AMBIGUOUS",
            )
            .otherwise("NONE"),
        )
        .withColumn(
            "PERSON_ID",
            F.when(
                F.col("PERSON_MATCH_METHOD").isin(
                    "MRN_NHS_AGREE", "MRN_ONLY"
                ),
                F.col("_mrn_person_id"),
            ).when(
                F.col("PERSON_MATCH_METHOD") == "NHS_ONLY",
                F.col("_nhs_person_id"),
            ),
        )
        .withColumn(
            "PERSON_MATCH_STATUS",
            F.when(F.col("PERSON_ID").isNotNull(), "MATCHED").otherwise(
                F.col("PERSON_MATCH_METHOD")
            ),
        )
        .select(
            "LNKPID",
            "PERSON_ID",
            "PERSON_MATCH_METHOD",
            "PERSON_MATCH_STATUS",
            "MRN_CANDIDATES",
            "NHS_CANDIDATES",
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drug dictionary and dm+d VTM mapping

# COMMAND ----------


def normalise_drug_name(column):
    value = F.lower(F.trim(column.cast("string")))
    value = F.split(value, r"\(").getItem(0)
    value = F.regexp_replace(value, r"\s+", " ")
    return F.trim(F.regexp_replace(value, r"[\s,;/-]+$", ""))


def normalise_dmd_name(column):
    return F.lower(
        F.trim(F.regexp_replace(column.cast("string"), r"\s+", " "))
    )


def dmd_vtm_pool() -> DataFrame:
    concepts = (
        spark.table(OMOP_CONCEPT)
        .where(
            (F.col("vocabulary_id") == "dm+d")
            & (F.col("concept_class_id") == "VTM")
            & F.col("invalid_reason").isNull()
        )
        .select("concept_id", "concept_code", "concept_name")
    )
    pool = concepts.select(
        normalise_dmd_name(F.col("concept_name")).alias("NAME_KEY"),
        "concept_id",
        "concept_code",
        "concept_name",
        F.lit("VTM_NAME").alias("MATCH_SOURCE"),
        F.lit(1).alias("MATCH_RANK"),
    )
    if bronze_table_exists(OMOP_CONCEPT_SYNONYM):
        synonyms = (
            spark.table(OMOP_CONCEPT_SYNONYM)
            .join(concepts, "concept_id")
            .select(
                normalise_dmd_name(
                    F.col("concept_synonym_name")
                ).alias("NAME_KEY"),
                "concept_id",
                "concept_code",
                "concept_name",
                F.lit("VTM_SYNONYM").alias("MATCH_SOURCE"),
                F.lit(2).alias("MATCH_RANK"),
            )
        )
        pool = pool.unionByName(synonyms)
    pool = pool.where(
        F.col("NAME_KEY").isNotNull() & (F.col("NAME_KEY") != "")
    )
    unambiguous = (
        pool.groupBy("NAME_KEY")
        .agg(F.countDistinct("concept_id").alias("_concept_count"))
        .where(F.col("_concept_count") == 1)
        .select("NAME_KEY")
    )
    window = Window.partitionBy("NAME_KEY").orderBy(
        F.col("MATCH_RANK"), F.col("concept_id")
    )
    return (
        pool.join(unambiguous, "NAME_KEY")
        .withColumn("_row_number", F.row_number().over(window))
        .where(F.col("_row_number") == 1)
        .drop("_row_number", "MATCH_RANK")
    )


def dmd_sources():
    sources = [OMOP_CONCEPT]
    if bronze_table_exists(OMOP_CONCEPT_SYNONYM):
        sources.append(OMOP_CONCEPT_SYNONYM)
    return sources


def build_drug_dictionary() -> DataFrame:
    source_frames = [
        spark.table(source)
        .groupBy(F.col("lnkdid").alias("JAC_DRUG_ID"))
        .agg(
            F.max("drug_name").alias("DRUG_NAME"),
            F.max("drug_form").alias("DRUG_FORM"),
            F.max("drug_strength").alias("DRUG_STRENGTH"),
            F.max("drugfull").alias("DRUG_FULL"),
            F.max("bnf_code").alias("BNF_CODE_RAW"),
            F.count(F.lit(1)).alias("_source_rows"),
        )
        for source in (SRC_ISSUES_PHM, SRC_ISSUES_OPD)
    ]
    drugs = (
        source_frames[0]
        .unionByName(source_frames[1])
        .groupBy("JAC_DRUG_ID")
        .agg(
            F.max("DRUG_NAME").alias("DRUG_NAME"),
            F.max("DRUG_FORM").alias("DRUG_FORM"),
            F.max("DRUG_STRENGTH").alias("DRUG_STRENGTH"),
            F.max("DRUG_FULL").alias("DRUG_FULL"),
            F.max("BNF_CODE_RAW").alias("BNF_CODE_RAW"),
            F.sum("_source_rows").cast("long").alias("SOURCE_ROW_COUNT"),
        )
        .withColumn("NAME_KEY", normalise_drug_name(F.col("DRUG_NAME")))
    )
    if ENABLE_DMD:
        drugs = (
            drugs.join(dmd_vtm_pool(), "NAME_KEY", "left")
            .select(
                "JAC_DRUG_ID",
                "DRUG_NAME",
                "DRUG_FORM",
                "DRUG_STRENGTH",
                "DRUG_FULL",
                "BNF_CODE_RAW",
                "NAME_KEY",
                "SOURCE_ROW_COUNT",
                F.col("concept_id")
                .cast("long")
                .alias("DMD_VTM_CONCEPT_ID"),
                F.col("concept_code").alias("DMD_VTM_CODE"),
                F.col("concept_name").alias("DMD_VTM_NAME"),
                F.coalesce(
                    F.col("MATCH_SOURCE"), F.lit("UNMAPPED")
                ).alias("DRUG_MAPPING_METHOD"),
            )
        )
    else:
        drugs = (
            drugs.withColumn(
                "DMD_VTM_CONCEPT_ID", F.lit(None).cast("long")
            )
            .withColumn("DMD_VTM_CODE", F.lit(None).cast("string"))
            .withColumn("DMD_VTM_NAME", F.lit(None).cast("string"))
            .withColumn("DRUG_MAPPING_METHOD", F.lit("DISABLED"))
        )
    return (
        drugs.withColumn(
            "CURATED_DMD_CONCEPT_ID", F.lit(None).cast("long")
        )
        .withColumn("CURATED_STATUS", F.lit(None).cast("string"))
        .withColumn("CURATED_NOTES", F.lit(None).cast("string"))
    )


DRUG_COMMENTS = {
    "JAC_DRUG_ID": "JAC drug dimension identifier from lnkdid.",
    "DRUG_NAME": "Representative source drug name for this JAC drug id.",
    "DRUG_FORM": "Representative source drug form.",
    "DRUG_STRENGTH": "Representative source drug strength.",
    "DRUG_FULL": "Representative full source drug description.",
    "BNF_CODE_RAW": "BNF classification exactly as supplied; not a presentation code.",
    "NAME_KEY": "Lowercase deterministic source-name key used for exact dm+d matching.",
    "SOURCE_ROW_COUNT": "Issue rows carrying this drug at the latest processed snapshot.",
    "DMD_VTM_CONCEPT_ID": "Automatically matched valid dm+d VTM OMOP concept_id.",
    "DMD_VTM_CODE": "SNOMED CT identifier from the automatically matched dm+d VTM.",
    "DMD_VTM_NAME": "Preferred name of the automatically matched dm+d VTM.",
    "DRUG_MAPPING_METHOD": "VTM_NAME, VTM_SYNONYM, UNMAPPED, or DISABLED; exact equality only.",
    "CURATED_DMD_CONCEPT_ID": "Human override, effective only when CURATED_STATUS is APPROVED.",
    "CURATED_STATUS": "Curation status: APPROVED, PROPOSED, or REJECTED.",
    "CURATED_NOTES": "Human curation notes; preserved by pipeline refreshes.",
    "ROW_HASH": "SHA-256 of pipeline-managed business columns; curation columns excluded.",
    "SOURCE_PRESENT_IND": "False when the drug id disappeared from both current issue snapshots.",
    "ADC_UPDT": "Timestamp this row was last inserted or changed by jac_pipeline.",
}

_drug_sources = [SRC_ISSUES_PHM, SRC_ISSUES_OPD] + (
    dmd_sources() if ENABLE_DMD else []
)
_drug_needed = needs_run(DRUG_TABLE, _drug_sources) or dmd_mode_changed(
    DRUG_TABLE
)
_drug_metrics = {"operation": "SKIP"}
if _drug_needed:
    _drugs = build_drug_dictionary()
    assert_unique_non_null(_drugs, ["JAC_DRUG_ID"], "drug dictionary")
    _drug_metrics = jac_update_table(
        _drugs,
        DRUG_TABLE,
        ["JAC_DRUG_ID"],
        _drug_sources,
        preserve_columns={
            "CURATED_DMD_CONCEPT_ID",
            "CURATED_STATUS",
            "CURATED_NOTES",
        },
    )
    print(f"[JAC] map_pharmacy_drug: {_drug_metrics}")

    _valid_vtms = (
        spark.table(OMOP_CONCEPT)
        .where(
            (F.col("vocabulary_id") == "dm+d")
            & (F.col("concept_class_id") == "VTM")
            & F.col("invalid_reason").isNull()
        )
        .select(F.col("concept_id").cast("long").alias("_effective_concept_id"))
    )
    _bad_drug_mappings = (
        spark.table(DRUG_TABLE)
        .where(F.col("SOURCE_PRESENT_IND"))
        .withColumn(
            "_effective_concept_id",
            F.coalesce(
                F.when(
                    F.col("CURATED_STATUS") == "APPROVED",
                    F.col("CURATED_DMD_CONCEPT_ID"),
                ),
                F.col("DMD_VTM_CONCEPT_ID"),
            ),
        )
        .where(F.col("_effective_concept_id").isNotNull())
        .join(_valid_vtms, "_effective_concept_id", "left_anti")
        .count()
    )
    assert (
        _bad_drug_mappings == 0
    ), f"Invalid dm+d VTM mappings in {DRUG_TABLE}: {_bad_drug_mappings}"

    if ENABLE_DMD:
        _drug_coverage = (
            spark.table(DRUG_TABLE)
            .where(F.col("SOURCE_PRESENT_IND"))
            .agg(
                (
                    F.sum(
                        F.when(
                            F.col("DMD_VTM_CONCEPT_ID").isNotNull(),
                            F.col("SOURCE_ROW_COUNT"),
                        ).otherwise(F.lit(0))
                    )
                    / F.sum("SOURCE_ROW_COUNT")
                ).alias("coverage")
            )
            .collect()[0]["coverage"]
        )
        print(f"[JAC] drug row-weighted dm+d coverage: {_drug_coverage:.4f}")
        assert _drug_coverage >= 0.75

    apply_comments(
        DRUG_TABLE,
        DRUG_COMMENTS,
        "JAC drug dictionary with deterministic exact-name dm+d VTM mapping and curation columns.",
    )
else:
    print("[JAC] map_pharmacy_drug: sources unchanged, skipping")


def effective_drug_mapping() -> DataFrame:
    curated_concepts = (
        spark.table(OMOP_CONCEPT)
        .where(
            (F.col("vocabulary_id") == "dm+d")
            & (F.col("concept_class_id") == "VTM")
            & F.col("invalid_reason").isNull()
        )
        .select(
            F.col("concept_id").cast("long").alias("_curated_concept_id"),
            F.col("concept_code").alias("_curated_code"),
            F.col("concept_name").alias("_curated_name"),
        )
    )
    drug = (
        spark.table(DRUG_TABLE)
        .where(F.col("SOURCE_PRESENT_IND"))
        .join(
            curated_concepts,
            F.col("CURATED_DMD_CONCEPT_ID")
            == F.col("_curated_concept_id"),
            "left",
        )
        .withColumn(
            "_use_curated",
            (F.col("CURATED_STATUS") == "APPROVED")
            & F.col("_curated_concept_id").isNotNull(),
        )
    )
    return drug.select(
        "JAC_DRUG_ID",
        F.when(
            F.col("_use_curated"), F.col("_curated_concept_id")
        )
        .otherwise(F.col("DMD_VTM_CONCEPT_ID"))
        .cast("long")
        .alias("DMD_VTM_CONCEPT_ID"),
        F.when(F.col("_use_curated"), F.col("_curated_code"))
        .otherwise(F.col("DMD_VTM_CODE"))
        .alias("DMD_VTM_CODE"),
        F.when(F.col("_use_curated"), F.col("_curated_name"))
        .otherwise(F.col("DMD_VTM_NAME"))
        .alias("DMD_VTM_NAME"),
        F.when(F.col("_use_curated"), F.lit("CURATED_APPROVED"))
        .otherwise(F.col("DRUG_MAPPING_METHOD"))
        .alias("DRUG_MAPPING_METHOD"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dependency gating and shared bridges

# COMMAND ----------


def build_care_site_bridge() -> DataFrame:
    return (
        spark.table(CARE_SITE)
        .select(
            F.upper(F.trim(F.col("care_site_name"))).alias("_CARE_SITE_NAME"),
            F.col("care_site_cd").cast("long").alias("CARE_SITE_CD"),
        )
        .where(F.col("_CARE_SITE_NAME").isNotNull())
        .groupBy("_CARE_SITE_NAME")
        .agg(
            F.countDistinct("CARE_SITE_CD").alias("_candidate_count"),
            F.first("CARE_SITE_CD").alias("CARE_SITE_CD"),
        )
        .where(F.col("_candidate_count") == 1)
        .drop("_candidate_count")
    )


_issue_sources = [
    SRC_ISSUES_PHM,
    SRC_ISSUES_OPD,
    SRC_PATIENT,
    PERSON_ALIAS,
    DRUG_TABLE,
]
if bronze_table_exists(CARE_SITE):
    _issue_sources.append(CARE_SITE)

_homecare_sources = [
    SRC_HC_HDR,
    SRC_HC_ITEM,
    SRC_PATIENT,
    PERSON_ALIAS,
]
if ENABLE_DMD:
    _homecare_sources.extend(dmd_sources())
if bronze_table_exists(CARE_SITE):
    _homecare_sources.append(CARE_SITE)

_issue_needed = needs_run(ISSUE_TABLE, _issue_sources)
_homecare_needed = needs_run(
    HOMECARE_TABLE, _homecare_sources
) or dmd_mode_changed(HOMECARE_TABLE)

person_bridge = None
if _issue_needed or _homecare_needed:
    person_bridge = build_person_bridge()
    assert_unique_non_null(person_bridge, ["LNKPID"], "person bridge")
    _bridge_stats = person_bridge.groupBy("PERSON_MATCH_STATUS").count().collect()
    print(
        "[JAC] person bridge statuses:",
        {
            row["PERSON_MATCH_STATUS"]: row["count"]
            for row in _bridge_stats
        },
    )
    _bridge_counts = person_bridge.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(
            F.when(F.col("PERSON_ID").isNotNull(), 1).otherwise(0)
        ).alias("matched"),
    ).collect()[0]
    _bridge_rate = _bridge_counts["matched"] / _bridge_counts["total"]
    print(f"[JAC] patient-level match rate: {_bridge_rate:.4f}")
    assert _bridge_rate >= 0.75

_care_site = (
    build_care_site_bridge()
    if (_issue_needed or _homecare_needed)
    and bronze_table_exists(CARE_SITE)
    else None
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pharmacy issues

# COMMAND ----------


def build_issue_branch(source_table: str, branch: str) -> DataFrame:
    issue = spark.table(source_table).select(
        F.col("dailyissues").alias("DAILYISSUES_KEY"),
        F.lit(branch).alias("SOURCE_BRANCH"),
        F.col("lnkpid").alias("LNKPID"),
        F.col("lnkdid").alias("JAC_DRUG_ID"),
        F.col("issue_date").alias("ISSUE_DATE"),
        F.col("issue_time").alias("ISSUE_DTTM"),
        blank_to_null(F.col("issue_type")).alias("ISSUE_TYPE"),
        blank_to_null(F.col("return")).alias("RETURN_MARKER"),
        blank_to_null(F.col("NFD_issue_reason")).alias("NFD_ISSUE_REASON"),
        F.col("issue_value").alias("ISSUE_VALUE_GBP"),
        blank_to_null(F.col("expenditure_category")).alias(
            "EXPENDITURE_CATEGORY"
        ),
        "quantity",
        F.col("doseunit_per_pack").alias("DOSEUNIT_PER_PACK"),
        blank_to_null(F.col("drug_doseunit")).alias("DRUG_DOSEUNIT"),
        blank_to_null(F.col("drug_packsize")).alias("DRUG_PACKSIZE"),
        F.col("container_returns").alias("CONTAINER_RETURNS"),
        F.col("doseunit_returns").alias("DOSEUNIT_RETURNS"),
        blank_to_null(F.col("drug_name")).alias("DRUG_NAME"),
        blank_to_null(F.col("drug_form")).alias("DRUG_FORM"),
        blank_to_null(F.col("drug_strength")).alias("DRUG_STRENGTH"),
        blank_to_null(F.col("drugfull")).alias("DRUG_FULL"),
        "bnf_code",
        blank_to_null(F.col("location_code")).alias("LOCATION_CODE"),
        blank_to_null(F.col("location")).alias("LOCATION_NAME"),
        blank_to_null(F.col("costcentre_code")).alias("COSTCENTRE_CODE"),
        blank_to_null(F.col("costcentre")).alias("COSTCENTRE_NAME"),
        blank_to_null(F.col("costcentre_fcode")).alias("COSTCENTRE_FCODE"),
        blank_to_null(F.col("costcentre_lcode")).alias("COSTCENTRE_LCODE"),
        blank_to_null(F.col("stock_type")).alias("STOCK_TYPE"),
        blank_to_null(F.col("bulk_issue_section")).alias(
            "BULK_ISSUE_SECTION"
        ),
        F.col("Record_Updated_Dt").alias("SOURCE_RECORD_UPDATED_DT"),
    )
    return (
        parse_bnf(parse_quantity(issue).drop("quantity"))
        .drop("bnf_code")
    )


ISSUE_COMMENTS = {
    "PHARMACY_ISSUE_ID": "Deterministic PK: SHA-256 of SOURCE_BRANCH and DAILYISSUES_KEY.",
    "DAILYISSUES_KEY": "Raw JAC dailyissues composite key retained for traceability.",
    "SOURCE_BRANCH": "PHM is inpatient pharmacy; OPD is outpatient pharmacy.",
    "LNKPID": "JAC internal patient link key; not a direct patient identifier.",
    "JAC_DRUG_ID": "JAC drug dimension identifier from lnkdid.",
    "ISSUE_DATE": "Source issue calendar date.",
    "ISSUE_DTTM": "Source issue timestamp.",
    "ISSUE_TYPE": "Raw JAC issue type code.",
    "ISSUE_CATEGORY": "Derived category: ISSUE, RETURN_CREDIT, REQUEST, ADJUSTMENT, TRANSFER, or OTHER.",
    "RETURN_MARKER": "Raw JAC return marker.",
    "NFD_ISSUE_REASON": "Source reason an item was not dispensed or fulfilled.",
    "ISSUE_VALUE_GBP": "Source-recorded monetary value; credits can be negative.",
    "EXPENDITURE_CATEGORY": "Source expenditure category.",
    "DOSEUNIT_PER_PACK": "Source-recorded number of dose units per pack.",
    "DRUG_DOSEUNIT": "Source dose-unit description.",
    "DRUG_PACKSIZE": "Source pack-size description.",
    "CONTAINER_RETURNS": "Source-recorded returned container count.",
    "DOSEUNIT_RETURNS": "Source-recorded returned dose-unit count.",
    "DRUG_NAME": "Source drug name.",
    "DRUG_FORM": "Source drug form.",
    "DRUG_STRENGTH": "Source drug strength.",
    "DRUG_FULL": "Source full drug description.",
    "LOCATION_CODE": "Raw JAC pharmacy location code.",
    "LOCATION_NAME": "Raw JAC pharmacy location name.",
    "COSTCENTRE_CODE": "Raw JAC cost-centre code.",
    "COSTCENTRE_NAME": "Raw JAC cost-centre name.",
    "COSTCENTRE_FCODE": "Raw JAC cost-centre finance code.",
    "COSTCENTRE_LCODE": "Raw JAC cost-centre local code.",
    "STOCK_TYPE": "Raw JAC stock type.",
    "BULK_ISSUE_SECTION": "Raw JAC bulk-issue section.",
    "SOURCE_RECORD_UPDATED_DT": "Upstream mirror timestamp; snapshot-level, not a reliable row watermark.",
    "QUANTITY_RAW": "Verbatim source quantity text.",
    "QUANTITY_PARSE_METHOD": "PACK_PATTERN, LEADING_NUMBER, or NULL when unparsed.",
    "ISSUED_CONTAINERS": "Parsed container count for PACK_PATTERN quantities.",
    "UNITS_PER_CONTAINER": "Parsed units per container for PACK_PATTERN quantities.",
    "TOTAL_UNITS": "Best-effort parsed quantity; use only with QUANTITY_PARSE_METHOD.",
    "BNF_CODE_RAW": "BNF classification exactly as supplied; not a presentation code.",
    "BNF_CODE_NORMALISED": "Trimmed uppercase BNF classification with non-code characters removed.",
    "BNF_CHAPTER": "Derived two-character BNF chapter.",
    "BNF_SECTION": "Derived five-character BNF section when structurally valid.",
    "BNF_PARAGRAPH": "Derived eight-character BNF paragraph when structurally valid.",
    "BNF_SUBPARAGRAPH": "Derived eleven-character BNF sub-paragraph when structurally valid.",
    "PERSON_ID": "Cerner PERSON_ID via MRN/NHS alias consensus; NULL for ward stock or unresolved patients.",
    "PERSON_MATCH_METHOD": "MRN_NHS_AGREE, MRN_ONLY, NHS_ONLY, CONFLICT, AMBIGUOUS, or NONE.",
    "PERSON_MATCH_STATUS": "MATCHED or the reason no PERSON_ID was assigned.",
    "MRN_CANDIDATES": "Distinct active Cerner PERSON_ID candidates for the normalised MRN.",
    "NHS_CANDIDATES": "Distinct active Cerner PERSON_ID candidates for the checksum-valid NHS number.",
    "DMD_VTM_CONCEPT_ID": "Effective valid dm+d VTM OMOP concept_id from exact matching or approved curation.",
    "DMD_VTM_CODE": "Effective dm+d VTM SNOMED CT identifier.",
    "DMD_VTM_NAME": "Effective dm+d VTM preferred name.",
    "DRUG_MAPPING_METHOD": "CURATED_APPROVED, VTM_NAME, VTM_SYNONYM, UNMAPPED, or DISABLED.",
    "CARE_SITE_CD": "map_care_site key from a unique exact location-name match.",
    "CARE_SITE_MATCH_METHOD": "NAME_EXACT, UNMAPPED, or UNAVAILABLE.",
    "ROW_HASH": "SHA-256 of pipeline-managed business columns.",
    "SOURCE_PRESENT_IND": "False when the row disappeared from the current source snapshot.",
    "ADC_UPDT": "Timestamp this row was last inserted or changed by jac_pipeline.",
}

_issue_metrics = {"operation": "SKIP"}
if _issue_needed:
    for _source in (SRC_ISSUES_PHM, SRC_ISSUES_OPD):
        _date_mismatches = (
            spark.table(_source)
            .where(F.col("issue_date").isNotNull())
            .where(
                (F.col("issue_year") != F.year("issue_date"))
                | (F.col("issue_month") != F.month("issue_date"))
                | (F.col("issue_day") != F.dayofmonth("issue_date"))
            )
            .count()
        )
        print(
            f"[JAC] {_source} date-component mismatches: "
            f"{_date_mismatches}"
        )
        assert _date_mismatches == 0

    issues = (
        build_issue_branch(SRC_ISSUES_PHM, "PHM")
        .unionByName(build_issue_branch(SRC_ISSUES_OPD, "OPD"))
        .withColumn(
            "PHARMACY_ISSUE_ID",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("SOURCE_BRANCH"),
                    F.col("DAILYISSUES_KEY"),
                ),
                256,
            ),
        )
        .withColumn(
            "ISSUE_CATEGORY",
            decode_map(F.upper(F.col("ISSUE_TYPE")), ISSUE_CATEGORY_MAP),
        )
        .join(person_bridge, "LNKPID", "left")
        .withColumn(
            "PERSON_MATCH_METHOD",
            F.coalesce(F.col("PERSON_MATCH_METHOD"), F.lit("NONE")),
        )
        .withColumn(
            "PERSON_MATCH_STATUS",
            F.coalesce(F.col("PERSON_MATCH_STATUS"), F.lit("NONE")),
        )
        .join(effective_drug_mapping(), "JAC_DRUG_ID", "left")
    )
    if _care_site is not None:
        issues = (
            issues.withColumn(
                "_CARE_SITE_NAME",
                F.upper(F.trim(F.col("LOCATION_NAME"))),
            )
            .join(_care_site, "_CARE_SITE_NAME", "left")
            .drop("_CARE_SITE_NAME")
            .withColumn(
                "CARE_SITE_MATCH_METHOD",
                F.when(
                    F.col("CARE_SITE_CD").isNotNull(), "NAME_EXACT"
                ).otherwise("UNMAPPED"),
            )
        )
    else:
        issues = (
            issues.withColumn("CARE_SITE_CD", F.lit(None).cast("long"))
            .withColumn("CARE_SITE_MATCH_METHOD", F.lit("UNAVAILABLE"))
        )

    assert_unique_non_null(
        issues, ["PHARMACY_ISSUE_ID"], "pharmacy issues"
    )
    _issue_metrics = jac_update_table(
        issues,
        ISSUE_TABLE,
        ["PHARMACY_ISSUE_ID"],
        _issue_sources,
    )
    print(f"[JAC] map_pharmacy_issue: {_issue_metrics}")
    apply_comments(
        ISSUE_TABLE,
        ISSUE_COMMENTS,
        "JAC pharmacy supply transactions from OPD and PHM: issues, returns, credits, requests, adjustments, and transfers.",
    )
else:
    print("[JAC] map_pharmacy_issue: sources unchanged, skipping")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Homecare requests

# COMMAND ----------

HOMECARE_COMMENTS = {
    "HOMECARE_REQUEST_ITEM_ID": "Deterministic PK: SHA-256 of REQUEST_KEY and ITEM_SEQ, using HDR for itemless headers.",
    "REQUEST_KEY": "JAC homecare request identifier from lnkrb.",
    "LNKPID": "JAC internal patient link key; not a direct patient identifier.",
    "REQUEST_STATUS": "Raw JAC header status code.",
    "REQUEST_STATUS_DESC": "COMPLETE, RELEASED, IN_PROGRESS, or provisional CANCELLED_OR_ERROR.",
    "LOCATION_NAME": "Raw JAC homecare location name.",
    "COSTCENTRE_NAME": "Raw JAC homecare cost-centre name.",
    "INDICATION": "Clinical indication recorded on the request.",
    "CLINIC": "Clinic recorded on the request.",
    "SUPPLY_START_DATE": "Requested supply start date.",
    "SUPPLY_INTERVAL": "Source supply interval.",
    "SUPPLY_PERIOD": "Source supply period.",
    "TOTAL_ITEM_COUNT": "Header-reported total item count.",
    "COMPLETE_ITEM_COUNT": "Header-reported completed item count.",
    "REQUEST_CREATED": "Source request-created date.",
    "REQUEST_RELEASED": "Source request-released date.",
    "REQUEST_COMPLETED": "Source request-completed date.",
    "SOURCE_RECORD_UPDATED_DT": "Upstream header mirror timestamp.",
    "ITEM_SEQ": "Item sequence within the request; NULL for an itemless header.",
    "ITEM_TYPE": "Raw JAC homecare item type.",
    "ITEM_DESCRIPTION": "Source medicine or supply description.",
    "PACK_DESCRIPTION": "Source pack description.",
    "ITEM_STATUS": "Raw JAC item status code.",
    "ITEM_STATUS_DESC": "COMPLETE, INCOMPLETE, or provisional CANCELLED_OR_ERROR.",
    "REQUEST_QUANTITY": "Requested quantity from the item.",
    "ORIGINAL_QUANTITY": "Original requested quantity from the item.",
    "QUANTITY_DELIVERED": "Delivered quantity from the item.",
    "ORDER_UNIT": "Source order unit.",
    "LABEL_DIRECTIONS": "Source dispensing label directions.",
    "NFD_REASON": "Source reason the item was not fulfilled.",
    "ITEM_SOURCE_RECORD_UPDATED_DT": "Upstream item mirror timestamp.",
    "PERSON_ID": "Cerner PERSON_ID via MRN/NHS alias consensus.",
    "PERSON_MATCH_METHOD": "MRN_NHS_AGREE, MRN_ONLY, NHS_ONLY, CONFLICT, AMBIGUOUS, or NONE.",
    "PERSON_MATCH_STATUS": "MATCHED or the reason no PERSON_ID was assigned.",
    "MRN_CANDIDATES": "Distinct active Cerner PERSON_ID candidates for the normalised MRN.",
    "NHS_CANDIDATES": "Distinct active Cerner PERSON_ID candidates for the checksum-valid NHS number.",
    "NAME_KEY": "Lowercase deterministic item-description key used for exact dm+d matching.",
    "DMD_VTM_CONCEPT_ID": "Valid dm+d VTM OMOP concept_id from exact item-description matching.",
    "DMD_VTM_CODE": "Matched dm+d VTM SNOMED CT identifier.",
    "DMD_VTM_NAME": "Matched dm+d VTM preferred name.",
    "DRUG_MAPPING_METHOD": "VTM_NAME, VTM_SYNONYM, UNMAPPED, DISABLED, or NULL for itemless headers.",
    "CARE_SITE_CD": "map_care_site key from a unique exact location-name match.",
    "CARE_SITE_MATCH_METHOD": "NAME_EXACT, UNMAPPED, or UNAVAILABLE.",
    "ROW_HASH": "SHA-256 of pipeline-managed business columns.",
    "SOURCE_PRESENT_IND": "False when the row disappeared from the current source snapshot.",
    "ADC_UPDT": "Timestamp this row was last inserted or changed by jac_pipeline.",
}

_homecare_metrics = {"operation": "SKIP"}
if _homecare_needed:
    headers = (
        spark.table(SRC_HC_HDR)
        .select(
            F.col("lnkrb").alias("REQUEST_KEY"),
            F.col("lnkpid").alias("LNKPID"),
            blank_to_null(F.col("request_status")).alias("REQUEST_STATUS"),
            blank_to_null(F.col("location")).alias("LOCATION_NAME"),
            blank_to_null(F.col("costcentre")).alias("COSTCENTRE_NAME"),
            blank_to_null(F.col("indication")).alias("INDICATION"),
            blank_to_null(F.col("clinic")).alias("CLINIC"),
            F.col("start_date").alias("SUPPLY_START_DATE"),
            F.col("supply_interval").cast("int").alias("SUPPLY_INTERVAL"),
            F.col("supply_period").cast("int").alias("SUPPLY_PERIOD"),
            F.col("total_item_count").cast("int").alias("TOTAL_ITEM_COUNT"),
            F.col("complete_item_count")
            .cast("int")
            .alias("COMPLETE_ITEM_COUNT"),
            F.col("request_created").alias("REQUEST_CREATED"),
            F.col("request_released").alias("REQUEST_RELEASED"),
            F.col("request_completed").alias("REQUEST_COMPLETED"),
            F.col("Record_Updated_dt").alias(
                "SOURCE_RECORD_UPDATED_DT"
            ),
        )
        .withColumn(
            "REQUEST_STATUS_DESC",
            decode_map(
                F.upper(F.col("REQUEST_STATUS")),
                HC_REQUEST_STATUS,
                default=None,
            ),
        )
    )
    assert_unique_non_null(headers, ["REQUEST_KEY"], "homecare headers")

    items = (
        spark.table(SRC_HC_ITEM)
        .select(
            F.col("lnkrb").alias("REQUEST_KEY"),
            F.col("lnkitb").cast("long").alias("ITEM_SEQ"),
            blank_to_null(F.col("type")).alias("ITEM_TYPE"),
            blank_to_null(F.col("item_description")).alias(
                "ITEM_DESCRIPTION"
            ),
            blank_to_null(F.col("pack_description")).alias(
                "PACK_DESCRIPTION"
            ),
            blank_to_null(F.col("item_status")).alias("ITEM_STATUS"),
            F.col("request_quantity")
            .cast("long")
            .alias("REQUEST_QUANTITY"),
            F.col("original_quantity")
            .cast("long")
            .alias("ORIGINAL_QUANTITY"),
            F.col("quantity_delivered")
            .cast("long")
            .alias("QUANTITY_DELIVERED"),
            blank_to_null(F.col("order_unit")).alias("ORDER_UNIT"),
            blank_to_null(F.col("label_directions")).alias(
                "LABEL_DIRECTIONS"
            ),
            blank_to_null(F.col("nfd_reason")).alias("NFD_REASON"),
            F.col("Record_Updated_dt").alias(
                "ITEM_SOURCE_RECORD_UPDATED_DT"
            ),
        )
        .withColumn(
            "ITEM_STATUS_DESC",
            decode_map(
                F.upper(F.col("ITEM_STATUS")),
                HC_ITEM_STATUS,
                default=None,
            ),
        )
    )
    assert_unique_non_null(
        items, ["REQUEST_KEY", "ITEM_SEQ"], "homecare items"
    )

    homecare = (
        headers.join(items, "REQUEST_KEY", "left")
        .withColumn(
            "HOMECARE_REQUEST_ITEM_ID",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("REQUEST_KEY"),
                    F.coalesce(
                        F.col("ITEM_SEQ").cast("string"), F.lit("HDR")
                    ),
                ),
                256,
            ),
        )
        .join(person_bridge, "LNKPID", "left")
        .withColumn(
            "PERSON_MATCH_METHOD",
            F.coalesce(F.col("PERSON_MATCH_METHOD"), F.lit("NONE")),
        )
        .withColumn(
            "PERSON_MATCH_STATUS",
            F.coalesce(F.col("PERSON_MATCH_STATUS"), F.lit("NONE")),
        )
    )
    if ENABLE_DMD:
        homecare = (
            homecare.withColumn(
                "NAME_KEY",
                normalise_drug_name(F.col("ITEM_DESCRIPTION")),
            )
            .join(
                dmd_vtm_pool().select(
                    "NAME_KEY",
                    F.col("concept_id")
                    .cast("long")
                    .alias("DMD_VTM_CONCEPT_ID"),
                    F.col("concept_code").alias("DMD_VTM_CODE"),
                    F.col("concept_name").alias("DMD_VTM_NAME"),
                    F.col("MATCH_SOURCE"),
                ),
                "NAME_KEY",
                "left",
            )
            .withColumn(
                "DRUG_MAPPING_METHOD",
                F.when(F.col("ITEM_DESCRIPTION").isNull(), F.lit(None))
                .when(
                    F.col("DMD_VTM_CONCEPT_ID").isNotNull(),
                    F.col("MATCH_SOURCE"),
                )
                .otherwise(F.lit("UNMAPPED")),
            )
            .drop("MATCH_SOURCE")
        )
    else:
        homecare = (
            homecare.withColumn("NAME_KEY", F.lit(None).cast("string"))
            .withColumn(
                "DMD_VTM_CONCEPT_ID", F.lit(None).cast("long")
            )
            .withColumn("DMD_VTM_CODE", F.lit(None).cast("string"))
            .withColumn("DMD_VTM_NAME", F.lit(None).cast("string"))
            .withColumn("DRUG_MAPPING_METHOD", F.lit("DISABLED"))
        )

    if _care_site is not None:
        homecare = (
            homecare.withColumn(
                "_CARE_SITE_NAME",
                F.upper(F.trim(F.col("LOCATION_NAME"))),
            )
            .join(_care_site, "_CARE_SITE_NAME", "left")
            .drop("_CARE_SITE_NAME")
            .withColumn(
                "CARE_SITE_MATCH_METHOD",
                F.when(
                    F.col("CARE_SITE_CD").isNotNull(), "NAME_EXACT"
                ).otherwise("UNMAPPED"),
            )
        )
    else:
        homecare = (
            homecare.withColumn("CARE_SITE_CD", F.lit(None).cast("long"))
            .withColumn("CARE_SITE_MATCH_METHOD", F.lit("UNAVAILABLE"))
        )

    assert_unique_non_null(
        homecare,
        ["HOMECARE_REQUEST_ITEM_ID"],
        "homecare request items",
    )
    _homecare_metrics = jac_update_table(
        homecare,
        HOMECARE_TABLE,
        ["HOMECARE_REQUEST_ITEM_ID"],
        _homecare_sources,
    )
    print(f"[JAC] map_homecare_request: {_homecare_metrics}")
    apply_comments(
        HOMECARE_TABLE,
        HOMECARE_COMMENTS,
        "JAC specialist and high-cost homecare requests at item grain; delivery-contact PII excluded.",
    )
else:
    print("[JAC] map_homecare_request: sources unchanged, skipping")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation and run summary

# COMMAND ----------

_summary = {
    "pipeline": "jac_pipeline",
    "status": "SUCCESS",
    "run_id": JAC_RUN_ID,
    "target_schema": TARGET_SCHEMA,
    "force_full_refresh": FORCE_FULL_REFRESH,
    "dmd_mapping_enabled": ENABLE_DMD,
    "started_at": _STARTED_AT,
    "drug_operation": _drug_metrics,
    "issue_operation": _issue_metrics,
    "homecare_operation": _homecare_metrics,
}

for _branch, _source in (
    ("PHM", SRC_ISSUES_PHM),
    ("OPD", SRC_ISSUES_OPD),
):
    _raw = (
        spark.table(_source)
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum("issue_value").alias("value"),
        )
        .collect()[0]
    )
    _bronze = (
        spark.table(ISSUE_TABLE)
        .where(
            (F.col("SOURCE_BRANCH") == _branch)
            & F.col("SOURCE_PRESENT_IND")
        )
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum("ISSUE_VALUE_GBP").alias("value"),
        )
        .collect()[0]
    )
    assert _bronze["rows"] == _raw["rows"], (
        f"{_branch}: bronze present rows {_bronze['rows']} "
        f"!= raw rows {_raw['rows']}"
    )
    assert (
        abs(float((_bronze["value"] or 0) - (_raw["value"] or 0)))
        < 0.01
    ), f"{_branch}: issue value mismatch"
    _bnf_mismatches = (
        spark.table(ISSUE_TABLE)
        .where(
            (F.col("SOURCE_BRANCH") == _branch)
            & F.col("SOURCE_PRESENT_IND")
        )
        .select("DAILYISSUES_KEY", "BNF_CODE_RAW")
        .alias("bronze")
        .join(
            spark.table(_source)
            .select(
                F.col("dailyissues").alias("DAILYISSUES_KEY"),
                F.col("bnf_code").alias("_RAW_BNF_CODE"),
            )
            .alias("raw"),
            "DAILYISSUES_KEY",
        )
        .where(
            ~F.col("BNF_CODE_RAW").eqNullSafe(F.col("_RAW_BNF_CODE"))
        )
        .count()
    )
    assert _bnf_mismatches == 0, f"{_branch}: BNF raw preservation failed"
    _summary[f"{_branch}_rows"] = int(_bronze["rows"])

_summary["issue_rows_absent_from_source"] = (
    spark.table(ISSUE_TABLE)
    .where(~F.col("SOURCE_PRESENT_IND"))
    .count()
)
_summary["homecare_rows_absent_from_source"] = (
    spark.table(HOMECARE_TABLE)
    .where(~F.col("SOURCE_PRESENT_IND"))
    .count()
)

_issue_coverage = (
    spark.table(ISSUE_TABLE)
    .where(F.col("SOURCE_PRESENT_IND"))
    .agg(
        F.avg(
            F.when(F.col("PERSON_ID").isNotNull(), 1.0).otherwise(0.0)
        ).alias("person_rate"),
        F.avg(
            F.when(
                F.col("LNKPID").isNotNull(),
                F.when(
                    F.col("PERSON_ID").isNotNull(), 1.0
                ).otherwise(0.0),
            )
        ).alias("person_rate_candidates"),
        F.avg(
            F.when(
                F.col("DMD_VTM_CONCEPT_ID").isNotNull(), 1.0
            ).otherwise(0.0)
        ).alias("dmd_rate"),
        F.avg(
            F.when(
                F.col("QUANTITY_PARSE_METHOD").isNotNull(), 1.0
            ).otherwise(0.0)
        ).alias("quantity_parse_rate"),
        F.avg(
            F.when(
                F.col("BNF_CODE_RAW").isNotNull()
                & (F.col("BNF_CODE_RAW") != ""),
                1.0,
            ).otherwise(0.0)
        ).alias("bnf_rate"),
        F.avg(
            F.when(F.col("CARE_SITE_CD").isNotNull(), 1.0).otherwise(
                0.0
            )
        ).alias("care_site_rate"),
        F.min("ISSUE_DATE").alias("min_issue_date"),
        F.max("ISSUE_DATE").alias("max_issue_date"),
    )
    .collect()[0]
)
for _metric in (
    "person_rate",
    "person_rate_candidates",
    "dmd_rate",
    "quantity_parse_rate",
    "bnf_rate",
    "care_site_rate",
):
    _summary[f"issue_{_metric}"] = round(
        float(_issue_coverage[_metric] or 0), 4
    )
assert _summary["issue_person_rate_candidates"] >= 0.80
assert _summary["issue_quantity_parse_rate"] >= 0.95
if ENABLE_DMD:
    assert _summary["issue_dmd_rate"] >= 0.70

assert str(_issue_coverage["min_issue_date"]) >= "2016-01-01"
assert str(_issue_coverage["max_issue_date"]) <= bronze_utc_now()[:10]
_summary["min_issue_date"] = _issue_coverage["min_issue_date"]
_summary["max_issue_date"] = _issue_coverage["max_issue_date"]

_raw_homecare_rows = (
    spark.table(SRC_HC_HDR)
    .select(F.col("lnkrb").alias("REQUEST_KEY"))
    .join(
        spark.table(SRC_HC_ITEM).select(
            F.col("lnkrb").alias("REQUEST_KEY"),
            F.col("lnkitb").alias("ITEM_SEQ"),
        ),
        "REQUEST_KEY",
        "left",
    )
    .count()
)
_raw_homecare_requests = spark.table(SRC_HC_HDR).select("lnkrb").distinct().count()
_homecare_coverage = (
    spark.table(HOMECARE_TABLE)
    .where(F.col("SOURCE_PRESENT_IND"))
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("REQUEST_KEY").alias("requests"),
        F.avg(
            F.when(F.col("PERSON_ID").isNotNull(), 1.0).otherwise(0.0)
        ).alias("person_rate"),
        F.avg(
            F.when(F.col("ITEM_SEQ").isNull(), 1.0).otherwise(0.0)
        ).alias("itemless_header_rate"),
        F.avg(
            F.when(
                F.col("DMD_VTM_CONCEPT_ID").isNotNull(), 1.0
            ).otherwise(0.0)
        ).alias("dmd_rate"),
        F.avg(
            F.when(F.col("CARE_SITE_CD").isNotNull(), 1.0).otherwise(
                0.0
            )
        ).alias("care_site_rate"),
    )
    .collect()[0]
)
assert _homecare_coverage["rows"] == _raw_homecare_rows
assert _homecare_coverage["requests"] == _raw_homecare_requests
assert float(_homecare_coverage["person_rate"] or 0) >= 0.80
for _metric in (
    "rows",
    "requests",
    "person_rate",
    "itemless_header_rate",
    "dmd_rate",
    "care_site_rate",
):
    _value = _homecare_coverage[_metric]
    _summary[f"homecare_{_metric}"] = (
        round(float(_value), 4)
        if _metric
        in (
            "person_rate",
            "itemless_header_rate",
            "dmd_rate",
            "care_site_rate",
        )
        else int(_value)
    )

_drug_summary = (
    spark.table(DRUG_TABLE)
    .where(F.col("SOURCE_PRESENT_IND"))
    .agg(
        F.count(F.lit(1)).alias("drugs"),
        (
            F.sum(
                F.when(
                    F.col("DMD_VTM_CONCEPT_ID").isNotNull(),
                    F.col("SOURCE_ROW_COUNT"),
                ).otherwise(F.lit(0))
            )
            / F.sum("SOURCE_ROW_COUNT")
        ).alias("row_weighted_mapping_rate"),
    )
    .collect()[0]
)
_summary["drug_count"] = int(_drug_summary["drugs"])
_summary["drug_row_weighted_mapping_rate"] = round(
    float(_drug_summary["row_weighted_mapping_rate"] or 0), 4
)

_banned_columns = {
    "patient_name",
    "patient_hospitalno",
    "patient_natno",
    "patient_reference",
    "surname",
    "forenames",
    "hospital_no",
    "nhs_no",
    "username",
    "terminal",
    "comment",
    "requestor",
    "reference",
    "homecarecontactname",
    "homecarecontactaddr1",
    "homecarecontactaddr2",
    "homecarecontactaddr3",
    "homecarecontactaddr4",
    "homecarecontactpostcode",
    "homecarecontacttelephone",
}
for _table in (ISSUE_TABLE, HOMECARE_TABLE, DRUG_TABLE):
    _leaked = _banned_columns & {
        column.lower() for column in spark.table(_table).columns
    }
    assert not _leaked, f"{_table} leaks excluded columns: {_leaked}"

assert verify_unique_key(
    spark.table(ISSUE_TABLE), ["PHARMACY_ISSUE_ID"]
) == 0
assert verify_unique_key(
    spark.table(HOMECARE_TABLE), ["HOMECARE_REQUEST_ITEM_ID"]
) == 0
assert verify_unique_key(
    spark.table(DRUG_TABLE), ["JAC_DRUG_ID"]
) == 0

_summary["completed_at"] = bronze_utc_now()
_result = bronze_json(_summary)
print(_result)
dbutils.notebook.exit(_result)
