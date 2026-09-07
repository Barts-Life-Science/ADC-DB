# Databricks notebook source
# MAGIC %md
# MAGIC # EndoBase (ENDOBASE3) Bronze Pipeline
# MAGIC
# MAGIC Publishes two weekly full-snapshot datasets:
# MAGIC
# MAGIC - `map_endobase_patient`: one pseudonymous crosswalk row per `PATIENT_TBL.PRIMARY_NO`, with deterministic MRN/NHS linkage to Millennium `PERSON_ID`. Direct identifiers are used only transiently and are not published.
# MAGIC - `map_endobase_exam_term`: one row per `DGVS_EXAM_TERM_TBL.PRIMARY_NO`, retaining structured, edited, unconfirmed, dependent, and free-text report terms. `TERM_TEXT` is clinical free text and may contain identifiers.
# MAGIC
# MAGIC **Critical join warning:** `DGVS_EXAM_TERM_TBL.EXAM_P` points to the un-landed `EXAM_TBL.PRIMARY_NO`. It must never be joined directly to `PATIENT_TBL.PRIMARY_NO` or `map_endobase_patient.ENDOBASE_PATIENT_ID`; the apparent dense-integer overlap is spurious. Until `EXAM_TBL` lands, term rows publish `PERSON_ID = NULL` and `PERSON_LINK_STATUS = 'EXAM_TABLE_NOT_LANDED'`.
# MAGIC
# MAGIC Sources are the two landed EndoBase weekly `wt_updt` whole-table snapshots. Deferred feature gates cover `EXAM_TBL`, DGVS term/tab decodes, HL7 merge chains, and qualifier tables. Ownership before production promotion: Endoscopy service (source semantics), Information Governance (clinical free text and pseudonymous crosswalk), and the clinical terminology owner (`endobase_term_map` approvals).

# COMMAND ----------

# MAGIC %run ./_bronze_common

# COMMAND ----------

from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F


def _widget_text(name: str, default: str) -> None:
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)


for _widget_name, _widget_default in {
    "target_schema": "8_dev.bronze",
    "force_full_refresh": "false",
    "mapping_version": "2026.07.v1",
    "min_patient_rows": "400000",
    "min_term_rows": "9000000",
    "max_source_age_days": "21",
    "max_snapshot_skew_days": "3",
    "enable_exact_candidates": "false",
    # Dev validation overrides. Production defaults remain the landed raw tables.
    "source_patient": "4_prod.raw.endobase3_dbo_patient_tbl",
    "source_term": "4_prod.raw.endobase3_dbo_dgvs_exam_term_tbl",
}.items():
    _widget_text(_widget_name, _widget_default)


TARGET_SCHEMA = bronze_value("target_schema", "8_dev.bronze")
FORCE_FULL_REFRESH = bronze_bool("force_full_refresh", False)
MAPPING_VERSION = bronze_value("mapping_version", "2026.07.v1")
ENABLE_EXACT_CANDIDATES = bronze_bool("enable_exact_candidates", False)
ENDOBASE_RUN_ID = bronze_run_id()
_STARTED_AT = bronze_utc_now()

SRC_TERM = bronze_value(
    "source_term", "4_prod.raw.endobase3_dbo_dgvs_exam_term_tbl"
)
SRC_PATIENT = bronze_value(
    "source_patient", "4_prod.raw.endobase3_dbo_patient_tbl"
)
DEFAULT_SRC_TERM = "4_prod.raw.endobase3_dbo_dgvs_exam_term_tbl"
DEFAULT_SRC_PATIENT = "4_prod.raw.endobase3_dbo_patient_tbl"
PERSON_ALIAS = "4_prod.raw.mill_person_alias"
MILL_PERSON = "4_prod.raw.mill_person"
OMOP_CONCEPT = "3_lookup.omop.concept"
OMOP_CONCEPT_SYNONYM = "3_lookup.omop.concept_synonym"

TGT_PATIENT = f"{TARGET_SCHEMA}.map_endobase_patient"
TGT_TERM = f"{TARGET_SCHEMA}.map_endobase_exam_term"
TGT_TERM_MAP = f"{bronze_control_schema(TARGET_SCHEMA)}.endobase_term_map"
STATE_TABLE = f"{bronze_control_schema(TARGET_SCHEMA)}.endobase_pipeline_state"

_using_source_overrides = (
    SRC_TERM != DEFAULT_SRC_TERM or SRC_PATIENT != DEFAULT_SRC_PATIENT
)
assert not (
    _using_source_overrides
    and TARGET_SCHEMA in {"8_dev.bronze", "4_prod.bronze"}
), (
    "Source override widgets are validation-only and require an isolated target_schema; "
    "they cannot write to the shared dev or production bronze schemas."
)

MRN_ALIAS_TYPE = 10
NHS_ALIAS_TYPE = 18

GATE_TABLES = {
    "exam_tbl": "4_prod.raw.endobase3_dbo_exam_tbl",
    "dgvs_term_tbl": "4_prod.raw.endobase3_dbo_dgvs_term_tbl",
    "dgvs_tab_tbl": "4_prod.raw.endobase3_dbo_dgvs_tab_tbl",
    "hl7_merge_tbl": "4_prod.raw.endobase3_dbo_hl7_merge_tbl",
    "dgvs_exam_location_tbl": "4_prod.raw.endobase3_dbo_dgvs_exam_location_tbl",
    "dgvs_exam_attrib_tbl": "4_prod.raw.endobase3_dbo_dgvs_exam_attrib_tbl",
    "dgvs_exam_attrib_val_tbl": "4_prod.raw.endobase3_dbo_dgvs_exam_attrib_val_tbl",
}


def qident(name: str) -> str:
    q = chr(96)
    return q + name.replace(q, q + q) + q


def qname(name: str) -> str:
    return ".".join(qident(part) for part in name.split("."))


print(
    f"[ENDOBASE] target={TARGET_SCHEMA}, run_id={ENDOBASE_RUN_ID}, "
    f"force_full_refresh={FORCE_FULL_REFRESH}, "
    f"exact_candidates={ENABLE_EXACT_CANDIDATES}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source-contract preflight
# MAGIC
# MAGIC Missing columns are breaking drift and abort the run. Additive columns are reported. Both whole-table snapshots must have one `ADC_UPDT`, meet volume/freshness floors, and fall within the configured extraction-date skew.

# COMMAND ----------

EXPECTED_TERM_COLUMNS = {
    "PRIMARY_NO",
    "EXAM_P",
    "TERM_P",
    "TAB_P",
    "CREATED",
    "TEXT_ASCII",
    "TEXT_RTF",
    "CONFIRMED_FLG",
    "TEXT_CHANGED_FLG",
    "DISPLAY_ORDER",
    "SUB_TAB_P",
    "SRC_EXAM_TERM_P",
    "TYPE_FLG",
    "GUID",
    "CAPTION_FLG",
    "CAPTION_P",
    "ADC_UPDT",
}

EXPECTED_PATIENT_COLUMNS = {
    "PRIMARY_NO",
    "EB2_RSN",
    "PATIENT_ID",
    "FIRST_NAME",
    "LAST_NAME",
    "BIRTHDAY",
    "AGE",
    "SSN",
    "HL7_KEY",
    "ADMISSION_P",
    "SALUTATION_P",
    "GENDER_P",
    "OCCUPATION",
    "BIRTH_NAME",
    "BIRTH_PLACE",
    "ZIP_CODE",
    "CITY",
    "AREA_CODE",
    "STREET_NUMBER",
    "HOME_TEL",
    "INFO_HOME_TEL",
    "OFFICE_TEL",
    "INFO_OFFICE_TEL",
    "FAX",
    "INFO_FAX",
    "INSURANCE_CODE",
    "INSURANCE_NAME",
    "INSURANCE_CUST_NO",
    "INSURANCE_STAT1_P",
    "INSURANCE_STAT2_P",
    "HOME_DOCTOR_P",
    "MIDDLE_NAME",
    "RACE_P",
    "DATA1",
    "DATA2",
    "DATA3",
    "NESSESARY",
    "DATE_OF_DEATH",
    "CAUSE_DEATH_P",
    "CAUSE_OF_DEATH",
    "COMMENT_OF_DEATH",
    "CHK_PATIENT_ID",
    "CHK_FIRST_NAME",
    "CHK_LAST_NAME",
    "USER_ID",
    "UNIT_ID",
    "CREATE_DATE",
    "DEATH_PROCEDURE_RELATED_FLG",
    "HEIGHT",
    "WEIGHT",
    "WAIST",
    "GUID",
    "UPDATE_TIME",
    "UNCONFIRMED_SSN",
    "ADC_UPDT",
}

_required_sources = [SRC_TERM, SRC_PATIENT, PERSON_ALIAS, MILL_PERSON, OMOP_CONCEPT]
if ENABLE_EXACT_CANDIDATES:
    _required_sources.append(OMOP_CONCEPT_SYNONYM)
_missing_sources = [table for table in _required_sources if not bronze_table_exists(table)]
assert not _missing_sources, f"Missing required sources: {_missing_sources}"


def check_columns(table: str, expected: set) -> dict:
    actual = set(spark.table(table).columns)
    missing = sorted(expected - actual)
    extras = sorted(actual - expected)
    assert not missing, f"{table}: missing breaking source columns {missing}"
    if extras:
        print(f"[ENDOBASE][WARN] {table}: additive source columns {extras}")
    return {"missing": missing, "additive": extras}


def preflight(table: str, min_rows: int, extra_aggs=None) -> dict:
    extra_aggs = list(extra_aggs or [])
    stats = (
        spark.table(table)
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("ADC_UPDT").alias("adc_versions"),
            F.sum(F.when(F.col("ADC_UPDT").isNull(), 1).otherwise(0)).alias(
                "adc_nulls"
            ),
            F.max("ADC_UPDT").alias("adc_max"),
            F.datediff(F.current_date(), F.to_date(F.max("ADC_UPDT"))).alias(
                "age_days"
            ),
            F.countDistinct("PRIMARY_NO").alias("distinct_keys"),
            F.sum(F.when(F.col("PRIMARY_NO").isNull(), 1).otherwise(0)).alias(
                "null_keys"
            ),
            *extra_aggs,
        )
        .collect()[0]
        .asDict()
    )
    assert stats["rows"] >= min_rows, (
        f"{table}: {stats['rows']} rows < floor {min_rows}"
    )
    assert stats["adc_versions"] == 1, (
        f"{table}: {stats['adc_versions']} ADC_UPDT values; partial snapshot, aborting"
    )
    assert stats["adc_nulls"] == 0, f"{table}: NULL ADC_UPDT rows in snapshot"
    assert stats["adc_max"] is not None, f"{table}: snapshot timestamp is NULL"
    assert stats["age_days"] <= int(bronze_value("max_source_age_days", "21")), (
        f"{table}: snapshot {stats['age_days']}d old"
    )
    assert stats["null_keys"] == 0, f"{table}: NULL PRIMARY_NO values"
    assert stats["distinct_keys"] == stats["rows"], (
        f"{table}: PRIMARY_NO is not unique"
    )
    stats["loaded"] = str(stats.pop("adc_max"))
    return stats


_patient_drift = check_columns(SRC_PATIENT, EXPECTED_PATIENT_COLUMNS)
_term_drift = check_columns(SRC_TERM, EXPECTED_TERM_COLUMNS)

preflight_report = {
    "patient": preflight(
        SRC_PATIENT,
        int(bronze_value("min_patient_rows", "400000")),
    ),
    "term": preflight(
        SRC_TERM,
        int(bronze_value("min_term_rows", "9000000")),
        extra_aggs=[
            F.countDistinct("EXAM_P").alias("distinct_exams"),
            F.sum(F.when(F.col("TERM_P") == 0, 1).otherwise(0)).alias(
                "free_text_rows"
            ),
            F.sum(F.when(F.col("TEXT_CHANGED_FLG") == 1, 1).otherwise(0)).alias(
                "text_changed_rows"
            ),
            F.sum(F.when(F.col("CONFIRMED_FLG") == 1, 1).otherwise(0)).alias(
                "confirmed_rows"
            ),
        ],
    ),
    "column_drift": {"patient": _patient_drift, "term": _term_drift},
}

_patient_loaded = preflight_report["patient"]["loaded"][:10]
_term_loaded = preflight_report["term"]["loaded"][:10]
_snapshot_skew_days = abs(
    spark.sql(
        f"SELECT datediff(DATE'{_term_loaded}', DATE'{_patient_loaded}') AS skew"
    ).collect()[0]["skew"]
    or 0
)
assert _snapshot_skew_days <= int(
    bronze_value("max_snapshot_skew_days", "3")
), (
    f"snapshot skew {_snapshot_skew_days}d between term and patient extracts "
    "exceeds tolerance"
)
preflight_report["snapshot_skew_days"] = int(_snapshot_skew_days)

gate_status = {
    name: bronze_table_exists(table) for name, table in GATE_TABLES.items()
}
preflight_report["gates"] = gate_status


def _meaningful_string(column_name: str):
    return F.col(column_name).isNotNull() & (F.trim(F.col(column_name)) != "")


def _meaningful_numeric(column_name: str):
    return F.col(column_name).isNotNull() & (F.col(column_name) != 0)


_patient_tripwire = (
    spark.table(SRC_PATIENT)
    .agg(
        *[
            F.sum(F.when(_meaningful_numeric(column), 1).otherwise(0)).alias(column)
            for column in (
                "DEATH_PROCEDURE_RELATED_FLG",
                "HEIGHT",
                "WEIGHT",
                "WAIST",
            )
        ],
        *[
            F.sum(F.when(_meaningful_string(column), 1).otherwise(0)).alias(column)
            for column in ("CAUSE_OF_DEATH", "HL7_KEY")
        ],
    )
    .collect()[0]
    .asDict()
)
_term_tripwire = (
    spark.table(SRC_TERM)
    .agg(
        F.sum(F.when(_meaningful_numeric("CAPTION_FLG"), 1).otherwise(0)).alias(
            "CAPTION_FLG"
        ),
        F.sum(F.when(_meaningful_numeric("CAPTION_P"), 1).otherwise(0)).alias(
            "CAPTION_P"
        ),
    )
    .collect()[0]
    .asDict()
)
_tripwire_warnings = {
    f"patient.{column}": int(count)
    for column, count in _patient_tripwire.items()
    if count
}
_tripwire_warnings.update(
    {
        f"term.{column}": int(count)
        for column, count in _term_tripwire.items()
        if count
    }
)
if _tripwire_warnings:
    print(
        "[ENDOBASE][WARN] previously empty/constant excluded columns now carry "
        f"values: {bronze_json(_tripwire_warnings)}"
    )
preflight_report["excluded_column_tripwire"] = {
    "counts": {"patient": _patient_tripwire, "term": _term_tripwire},
    "warnings": _tripwire_warnings,
}
print(bronze_json(preflight_report))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snapshot-aware loading engine

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
    COMMENT 'EndoBase bronze pipeline checkpoints: last processed Delta version per target and source.'
    """
)

HASH_EXCLUDE = {"ROW_HASH", "ADC_UPDT", "SOURCE_PRESENT_IND"}


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
    recorded_sources = {
        row["source_table"]
        for row in spark.table(STATE_TABLE)
        .where(F.col("target_table") == target)
        .select("source_table")
        .distinct()
        .collect()
    }
    if recorded_sources != set(sources):
        return True
    for source in sources:
        previous = last_committed_version(target, source)
        if previous is None or source_version(source) != previous:
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
    return df.groupBy(*keys).count().where(F.col("count") > 1).count()


def assert_unique_non_null(df: DataFrame, keys, label: str) -> None:
    null_condition = reduce(
        lambda left, right: left | right,
        [F.col(key).isNull() for key in keys],
    )
    null_count = df.where(null_condition).count()
    duplicate_count = verify_unique_key(df, keys)
    assert null_count == 0, f"{label}: {null_count} rows have NULL key values"
    assert duplicate_count == 0, f"{label}: {duplicate_count} duplicate keys"


def ensure_cdf(table: str) -> bool:
    detail = spark.sql(f"DESCRIBE DETAIL {qname(table)}").collect()[0]
    properties = detail["properties"] or {}
    if str(properties.get("delta.enableChangeDataFeed", "false")).lower() == "true":
        return False
    spark.sql(
        f"ALTER TABLE {qname(table)} SET TBLPROPERTIES "
        "('delta.enableChangeDataFeed' = 'true')"
    )
    return True


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
            ENDOBASE_RUN_ID,
        )
        for index, (source, version) in enumerate(versions.items())
    ]
    updates = spark.createDataFrame(
        rows,
        """
        target_table string,
        source_table string,
        source_version long,
        source_rows long,
        run_id string
        """,
    ).withColumn("committed_at", F.current_timestamp())
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
    stale_condition = (F.col("target_table") == target) & (
        ~F.col("source_table").isin(*list(versions))
    )
    if spark.table(STATE_TABLE).where(stale_condition).limit(1).count():
        DeltaTable.forName(spark, STATE_TABLE).delete(stale_condition)


def endobase_update_table(
    df: DataFrame,
    target: str,
    keys: list,
    sources: list,
) -> dict:
    """Full-snapshot upsert with row hashes, soft deletes, and source checkpoints."""
    versions = {source: source_version(source) for source in sources}
    output_rows = df.count()
    staged = (
        with_row_hash(df)
        .withColumn("SOURCE_PRESENT_IND", F.lit(True))
        .withColumn("ADC_UPDT", F.current_timestamp())
    )
    if not bronze_table_exists(target):
        (
            staged.write.format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .saveAsTable(target)
        )
        metrics = {"operation": "CREATE", "rows": int(output_rows)}
    else:
        ensure_cdf(target)
        condition = " AND ".join(
            f"t.{qident(key)} <=> s.{qident(key)}" for key in keys
        )
        update_values = {
            column: f"s.{qident(column)}" for column in staged.columns
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
    ensure_cdf(target)
    record_checkpoints(target, versions, output_rows)
    return metrics


def apply_comments(table: str, comments: dict, table_comment: str) -> int:
    """Apply only changed comments so metadata-only versions do not break gating."""

    def escape(value):
        return str(value).replace("\\", "\\\\").replace("'", "''")

    catalog, schema, table_name = table.split(".")
    current_table = (
        spark.table(qname(f"{catalog}.information_schema.tables"))
        .where(
            (F.col("table_schema") == schema)
            & (F.col("table_name") == table_name)
        )
        .select("comment")
        .collect()[0]["comment"]
    )
    current_columns = {
        row["column_name"]: row["comment"]
        for row in spark.table(qname(f"{catalog}.information_schema.columns"))
        .where(
            (F.col("table_schema") == schema)
            & (F.col("table_name") == table_name)
        )
        .select("column_name", "comment")
        .collect()
    }
    changed = 0
    if current_table != table_comment:
        spark.sql(
            f"COMMENT ON TABLE {qname(table)} IS '{escape(table_comment)}'"
        )
        changed += 1
    for column, comment in comments.items():
        if column in current_columns and current_columns[column] != comment:
            spark.sql(
                f"ALTER TABLE {qname(table)} ALTER COLUMN {qident(column)} "
                f"COMMENT '{escape(comment)}'"
            )
            changed += 1
    return changed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deterministic person linkage and patient crosswalk
# MAGIC
# MAGIC MRN aliases use type 10. Checksum-valid NHS numbers use type 18. Each arm yields a person only when its normalised alias has exactly one distinct active `PERSON_ID`; conflicts are never guessed, and DOB is QA-only.

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
    return aliases.withColumn(
        out_person,
        F.when(F.col(out_count) == 1, F.col("_person_id").cast("long")),
    ).drop("_person_id")


def build_patient_crosswalk() -> DataFrame:
    src = spark.table(SRC_PATIENT).select(
        F.col("PRIMARY_NO").alias("ENDOBASE_PATIENT_ID"),
        F.col("GUID").alias("ENDOBASE_PATIENT_GUID"),
        mrn_norm(F.col("PATIENT_ID")).alias("_mrn"),
        nhs_norm(F.col("SSN")).alias("_nhs_raw"),
        F.to_date("BIRTHDAY").alias("_dob"),
        F.to_date("CREATE_DATE").alias("SOURCE_CREATE_DT"),
        F.col("UPDATE_TIME").alias("SOURCE_UPDATE_TS"),
    )
    assert_unique_non_null(src, ["ENDOBASE_PATIENT_ID"], SRC_PATIENT)
    src = src.withColumn(
        "NHS_CHECKSUM_VALID_IND",
        F.coalesce(nhs_checksum_ok(F.col("_nhs_raw")), F.lit(False)),
    ).withColumn(
        "_nhs",
        F.when(F.col("NHS_CHECKSUM_VALID_IND"), F.col("_nhs_raw")),
    )

    linked = (
        src.join(
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
            "MRN_CANDIDATES", F.coalesce("MRN_CANDIDATES", F.lit(0))
        )
        .withColumn(
            "NHS_CANDIDATES", F.coalesce("NHS_CANDIDATES", F.lit(0))
        )
        .withColumn(
            "PERSON_LINK_STATUS",
            F.when(
                F.col("_mrn_person_id").isNotNull()
                & F.col("_nhs_person_id").isNotNull()
                & (F.col("_mrn_person_id") == F.col("_nhs_person_id")),
                "CONSENSUS",
            )
            .when(
                F.col("_mrn_person_id").isNotNull()
                & F.col("_nhs_person_id").isNotNull(),
                "CONFLICT",
            )
            .when(F.col("_mrn_person_id").isNotNull(), "MRN_ONLY")
            .when(F.col("_nhs_person_id").isNotNull(), "NHS_ONLY")
            .when(
                (F.col("MRN_CANDIDATES") > 1)
                | (F.col("NHS_CANDIDATES") > 1),
                "AMBIGUOUS",
            )
            .otherwise("UNMATCHED"),
        )
        .withColumn(
            "PERSON_ID",
            F.when(
                F.col("PERSON_LINK_STATUS").isin("CONSENSUS", "MRN_ONLY"),
                F.col("_mrn_person_id"),
            ).when(
                F.col("PERSON_LINK_STATUS") == "NHS_ONLY",
                F.col("_nhs_person_id"),
            ),
        )
    )

    dob_base = spark.table(MILL_PERSON).select(
        F.col("PERSON_ID").cast("long").alias("PERSON_ID"),
        F.to_date("BIRTH_DT_TM").alias("_mill_dob"),
    )
    dob_stats = dob_base.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("PERSON_ID").alias("person_ids"),
        F.sum(F.when(F.col("PERSON_ID").isNull(), 1).otherwise(0)).alias(
            "null_person_ids"
        ),
    ).collect()[0]
    assert dob_stats["null_person_ids"] == 0, "mill_person has NULL PERSON_ID rows"
    assert dob_stats["rows"] == dob_stats["person_ids"], (
        "mill_person carries duplicate PERSON_ID rows; DOB QA join would fan out"
    )
    dob = dob_base.dropDuplicates(["PERSON_ID"])

    return (
        linked.join(dob, "PERSON_ID", "left")
        .withColumn(
            "DOB_CONCORDANCE_IND",
            F.when(
                F.col("PERSON_ID").isNull()
                | F.col("_dob").isNull()
                | F.col("_mill_dob").isNull()
                | (F.col("_dob") == F.to_date(F.lit("1900-01-01"))),
                F.lit(None).cast("boolean"),
            ).otherwise(F.col("_dob") == F.col("_mill_dob")),
        )
        .select(
            "ENDOBASE_PATIENT_ID",
            "ENDOBASE_PATIENT_GUID",
            "PERSON_ID",
            "PERSON_LINK_STATUS",
            "MRN_CANDIDATES",
            "NHS_CANDIDATES",
            "NHS_CHECKSUM_VALID_IND",
            "DOB_CONCORDANCE_IND",
            "SOURCE_CREATE_DT",
            "SOURCE_UPDATE_TS",
        )
    )


PATIENT_COMMENTS = {
    "ENDOBASE_PATIENT_ID": "EndoBase PATIENT_TBL.PRIMARY_NO; target grain key.",
    "ENDOBASE_PATIENT_GUID": "Stable source GUID for the EndoBase patient record; pseudonymous, not a direct demographic identifier.",
    "PERSON_ID": "Cerner Millennium PERSON_ID resolved deterministically from unique active MRN/NHS aliases; NULL for conflicts, ambiguity, or no match.",
    "PERSON_LINK_STATUS": "CONSENSUS, MRN_ONLY, NHS_ONLY, CONFLICT, AMBIGUOUS, or UNMATCHED. No conflict or ambiguous alias is guessed.",
    "MRN_CANDIDATES": "Distinct active Millennium PERSON_ID candidates for the normalised EndoBase MRN; the MRN itself is not published.",
    "NHS_CANDIDATES": "Distinct active Millennium PERSON_ID candidates for the checksum-valid NHS number; the NHS number itself is not published.",
    "NHS_CHECKSUM_VALID_IND": "True when the transiently normalised source NHS number passes the NHS Mod-11 checksum; the number is not retained.",
    "DOB_CONCORDANCE_IND": "QA-only date equality between source DOB and Millennium DOB for a resolved person; NULL for missing/unlinked values and the EndoBase 1900-01-01 placeholder. Never used to select a person.",
    "SOURCE_CREATE_DT": "EndoBase patient-record creation date from CREATE_DATE.",
    "SOURCE_UPDATE_TS": "EndoBase per-record update timestamp from UPDATE_TIME.",
    "ROW_HASH": "SHA-256 of pipeline-managed business columns; ROW_HASH, SOURCE_PRESENT_IND and ADC_UPDT are excluded from the hash.",
    "SOURCE_PRESENT_IND": "False when the EndoBase patient record disappeared from the latest full source snapshot; reactivated on reappearance.",
    "ADC_UPDT": "Timestamp this row was last inserted or changed by endobase_pipeline.",
}

_patient_metrics = {"operation": "SKIP"}
_patient_sources = [SRC_PATIENT, PERSON_ALIAS, MILL_PERSON]
if needs_run(TGT_PATIENT, _patient_sources):
    _patient_stage = build_patient_crosswalk()
    _patient_metrics = endobase_update_table(
        _patient_stage,
        TGT_PATIENT,
        keys=["ENDOBASE_PATIENT_ID"],
        sources=_patient_sources,
    )
    print(f"[ENDOBASE] map_endobase_patient: {_patient_metrics}")
else:
    print("[ENDOBASE] map_endobase_patient: sources unchanged, skipping")

_patient_comment_changes = apply_comments(
    TGT_PATIENT,
    PATIENT_COMMENTS,
    "Pseudonymous EndoBase patient crosswalk at one row per PATIENT_TBL.PRIMARY_NO. MRN, NHS number, DOB, names, addresses, contacts, insurance and death details are used only as explicitly documented for linkage/QA and are not published.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Curated term-map scaffold and candidate queue
# MAGIC
# MAGIC The key is `(DGVS_TERM_ID, NORMALIZED_TEXT)`, because `TERM_P` alone is not a stable value key. Pipeline refreshes preserve all human curation fields. Only human-approved mappings that remain valid standard OMOP concepts are applied. Exact-name suggestions remain `PROPOSED`; fuzzy or embedding matches must never auto-approve. Curators must treat negated/situational phrases safely and leave them unmapped unless the mapped concept preserves that meaning.

# COMMAND ----------

_map_existed = bronze_table_exists(TGT_TERM_MAP)
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {qname(TGT_TERM_MAP)} (
      DGVS_TERM_ID BIGINT NOT NULL,
      NORMALIZED_TEXT STRING NOT NULL,
      EXAMPLE_ROW_COUNT BIGINT NOT NULL,
      OMOP_CONCEPT_ID BIGINT,
      SNOMED_CODE STRING,
      CONCEPT_NAME STRING,
      MAPPING_METHOD STRING NOT NULL,
      CURATED_STATUS STRING NOT NULL,
      CURATED_NOTES STRING,
      MAPPING_VERSION STRING NOT NULL,
      ADC_UPDT TIMESTAMP NOT NULL
    ) USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """
)
ensure_cdf(TGT_TERM_MAP)


def normalised_term_text(column):
    return F.upper(F.regexp_replace(F.trim(column.cast("string")), r"\s+", " "))


def _latest_delta_metrics(table: str) -> dict:
    history = spark.sql(f"DESCRIBE HISTORY {qname(table)} LIMIT 1").collect()[0]
    operation_metrics = history["operationMetrics"] or {}
    return {
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


def refresh_term_map(mapping_sources: list) -> dict:
    versions = {source: source_version(source) for source in mapping_sources}
    candidates = (
        spark.table(SRC_TERM)
        .where((F.col("TERM_P") != 0) & (F.col("TEXT_CHANGED_FLG") == 0))
        .groupBy(
            F.col("TERM_P").cast("long").alias("DGVS_TERM_ID"),
            normalised_term_text(F.col("TEXT_ASCII")).alias("NORMALIZED_TEXT"),
        )
        .agg(F.count(F.lit(1)).alias("EXAMPLE_ROW_COUNT"))
    )
    assert_unique_non_null(
        candidates,
        ["DGVS_TERM_ID", "NORMALIZED_TEXT"],
        "EndoBase term-map candidates",
    )
    candidate_count = candidates.count()
    (
        DeltaTable.forName(spark, TGT_TERM_MAP)
        .alias("t")
        .merge(
            candidates.alias("s"),
            "t.DGVS_TERM_ID = s.DGVS_TERM_ID "
            "AND t.NORMALIZED_TEXT = s.NORMALIZED_TEXT",
        )
        .whenMatchedUpdate(
            condition="NOT (t.EXAMPLE_ROW_COUNT <=> s.EXAMPLE_ROW_COUNT)",
            set={
                "EXAMPLE_ROW_COUNT": "s.EXAMPLE_ROW_COUNT",
                "ADC_UPDT": "current_timestamp()",
            },
        )
        .whenNotMatchedInsert(
            values={
                "DGVS_TERM_ID": "s.DGVS_TERM_ID",
                "NORMALIZED_TEXT": "s.NORMALIZED_TEXT",
                "EXAMPLE_ROW_COUNT": "s.EXAMPLE_ROW_COUNT",
                "OMOP_CONCEPT_ID": "CAST(NULL AS BIGINT)",
                "SNOMED_CODE": "CAST(NULL AS STRING)",
                "CONCEPT_NAME": "CAST(NULL AS STRING)",
                "MAPPING_METHOD": "'MANUAL'",
                "CURATED_STATUS": "'PROPOSED'",
                "CURATED_NOTES": "CAST(NULL AS STRING)",
                "MAPPING_VERSION": f"'{MAPPING_VERSION.replace(chr(39), chr(39) * 2)}'",
                "ADC_UPDT": "current_timestamp()",
            }
        )
        .whenNotMatchedBySourceUpdate(
            condition="t.EXAMPLE_ROW_COUNT <> 0",
            set={
                "EXAMPLE_ROW_COUNT": "CAST(0 AS BIGINT)",
                "ADC_UPDT": "current_timestamp()",
            },
        )
        .execute()
    )
    metrics = {"candidate_merge": _latest_delta_metrics(TGT_TERM_MAP)}

    if ENABLE_EXACT_CANDIDATES:
        DeltaTable.forName(spark, TGT_TERM_MAP).update(
            condition=(
                "CURATED_STATUS = 'PROPOSED' "
                "AND MAPPING_METHOD = 'EXACT_NAME'"
            ),
            set={
                "OMOP_CONCEPT_ID": "CAST(NULL AS BIGINT)",
                "SNOMED_CODE": "CAST(NULL AS STRING)",
                "CONCEPT_NAME": "CAST(NULL AS STRING)",
                "MAPPING_METHOD": "'MANUAL'",
                "ADC_UPDT": "current_timestamp()",
            },
        )
        pool = (
            spark.table(OMOP_CONCEPT)
            .where(
                (F.col("standard_concept") == "S")
                & F.col("invalid_reason").isNull()
                & F.col("domain_id").isin(
                    "Procedure",
                    "Condition",
                    "Observation",
                    "Measurement",
                    "Meas Value",
                )
            )
            .select(
                F.col("concept_id").cast("long").alias("OMOP_CONCEPT_ID"),
                F.col("concept_code").alias("CONCEPT_CODE"),
                F.col("concept_name").alias("CONCEPT_NAME"),
                F.col("vocabulary_id").alias("VOCABULARY_ID"),
            )
        )
        preferred_names = pool.select(
            "OMOP_CONCEPT_ID",
            "CONCEPT_CODE",
            "CONCEPT_NAME",
            "VOCABULARY_ID",
            normalised_term_text(F.col("CONCEPT_NAME")).alias("NORMALIZED_TEXT"),
        )
        synonym_names = (
            spark.table(OMOP_CONCEPT_SYNONYM)
            .select(
                F.col("concept_id").cast("long").alias("OMOP_CONCEPT_ID"),
                "concept_synonym_name",
            )
            .join(pool, "OMOP_CONCEPT_ID")
            .select(
                "OMOP_CONCEPT_ID",
                "CONCEPT_CODE",
                "CONCEPT_NAME",
                "VOCABULARY_ID",
                normalised_term_text(F.col("concept_synonym_name")).alias(
                    "NORMALIZED_TEXT"
                ),
            )
        )
        unique_names = (
            preferred_names.unionByName(synonym_names)
            .where(F.col("NORMALIZED_TEXT").isNotNull())
            .groupBy("NORMALIZED_TEXT")
            .agg(
                F.countDistinct("OMOP_CONCEPT_ID").alias("_concept_count"),
                F.min("OMOP_CONCEPT_ID").alias("OMOP_CONCEPT_ID"),
                F.min("CONCEPT_CODE").alias("CONCEPT_CODE"),
                F.min("CONCEPT_NAME").alias("CONCEPT_NAME"),
                F.min("VOCABULARY_ID").alias("VOCABULARY_ID"),
            )
            .where(F.col("_concept_count") == 1)
            .drop("_concept_count")
        )
        (
            DeltaTable.forName(spark, TGT_TERM_MAP)
            .alias("t")
            .merge(
                unique_names.alias("s"),
                "t.NORMALIZED_TEXT = s.NORMALIZED_TEXT",
            )
            .whenMatchedUpdate(
                condition=(
                    "t.CURATED_STATUS = 'PROPOSED' "
                    "AND t.OMOP_CONCEPT_ID IS NULL"
                ),
                set={
                    "OMOP_CONCEPT_ID": "s.OMOP_CONCEPT_ID",
                    "SNOMED_CODE": (
                        "CASE WHEN s.VOCABULARY_ID = 'SNOMED' "
                        "THEN s.CONCEPT_CODE ELSE NULL END"
                    ),
                    "CONCEPT_NAME": "s.CONCEPT_NAME",
                    "MAPPING_METHOD": "'EXACT_NAME'",
                    "ADC_UPDT": "current_timestamp()",
                },
            )
            .execute()
        )
        metrics["exact_name_merge"] = _latest_delta_metrics(TGT_TERM_MAP)

    record_checkpoints(TGT_TERM_MAP, versions, candidate_count)
    metrics["candidate_rows"] = int(candidate_count)
    return metrics


_mapping_sources = [SRC_TERM]
if ENABLE_EXACT_CANDIDATES:
    _mapping_sources.extend([OMOP_CONCEPT, OMOP_CONCEPT_SYNONYM])

_mapping_metrics = {"operation": "SKIP"}
if (not _map_existed) or needs_run(TGT_TERM_MAP, _mapping_sources):
    _mapping_metrics = refresh_term_map(_mapping_sources)
    print(f"[ENDOBASE] endobase_term_map: {_mapping_metrics}")
else:
    print("[ENDOBASE] endobase_term_map: sources unchanged, skipping")

TERM_MAP_COMMENTS = {
    "DGVS_TERM_ID": "Local EndoBase DGVS TERM_P identifier. It is not a stable value key without NORMALIZED_TEXT.",
    "NORMALIZED_TEXT": "Uppercase, trimmed, whitespace-collapsed source clinical term text; second half of the curation key. It may contain identifiers and must be handled under bronze clinical-text controls.",
    "EXAMPLE_ROW_COUNT": "Current source-row count for this unedited non-free-text (DGVS term, normalised text) pair.",
    "OMOP_CONCEPT_ID": "Human-reviewed target OMOP concept. Only APPROVED rows with a currently valid standard concept are applied.",
    "SNOMED_CODE": "SNOMED CT code when the selected concept is represented in SNOMED; may be NULL for other standard vocabularies.",
    "CONCEPT_NAME": "Preferred concept name recorded for reviewer context.",
    "MAPPING_METHOD": "MANUAL or EXACT_NAME. Exact-name candidates remain review proposals and never auto-approve.",
    "CURATED_STATUS": "PROPOSED, APPROVED, or REJECTED. Only APPROVED rows are applied.",
    "CURATED_NOTES": "Human terminology-review notes, preserved by pipeline refreshes.",
    "MAPPING_VERSION": "Version label assigned when the candidate row entered the curation queue.",
    "ADC_UPDT": "Timestamp this map row was last inserted or pipeline-managed candidate fields changed; curators should update it with manual edits.",
}

_map_comment_changes = apply_comments(
    TGT_TERM_MAP,
    TERM_MAP_COMMENTS,
    "Curated EndoBase DGVS term-text map at one row per (DGVS_TERM_ID, NORMALIZED_TEXT). NORMALIZED_TEXT is source clinical text and may contain identifiers. Pipeline refreshes preserve curation fields. Exact matches remain PROPOSED and require human approval. Negated or situational phrases must map to concepts preserving that meaning or remain unmapped; never map them to a bare finding merely because names overlap.",
)

_term_map = spark.table(TGT_TERM_MAP)
assert_unique_non_null(
    _term_map,
    ["DGVS_TERM_ID", "NORMALIZED_TEXT"],
    TGT_TERM_MAP,
)
_invalid_map_statuses = (
    _term_map.where(
        ~F.col("CURATED_STATUS").isin("PROPOSED", "APPROVED", "REJECTED")
    )
    .select(
        "DGVS_TERM_ID",
        F.sha2("NORMALIZED_TEXT", 256).alias("NORMALIZED_TEXT_SHA256"),
        "CURATED_STATUS",
    )
    .limit(25)
    .collect()
)
assert not _invalid_map_statuses, (
    f"Invalid endobase_term_map CURATED_STATUS rows: {_invalid_map_statuses}"
)

_approved_map = _term_map.where(F.col("CURATED_STATUS") == "APPROVED")
_valid_standard_concepts = (
    spark.table(OMOP_CONCEPT)
    .where(
        (F.col("standard_concept") == "S")
        & F.col("invalid_reason").isNull()
    )
    .select(F.col("concept_id").cast("long").alias("OMOP_CONCEPT_ID"))
)
_invalid_approved = (
    _approved_map.select(
        "DGVS_TERM_ID",
        F.sha2("NORMALIZED_TEXT", 256).alias("NORMALIZED_TEXT_SHA256"),
        "OMOP_CONCEPT_ID",
    )
    .join(_valid_standard_concepts, "OMOP_CONCEPT_ID", "left_anti")
    .limit(25)
    .collect()
)
assert not _invalid_approved, (
    "Approved EndoBase mappings must reference current valid standard OMOP "
    f"concepts; offenders: {_invalid_approved}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam-term publication
# MAGIC
# MAGIC **Do not join `EXAM_P` to the patient table.** `ENDOBASE_EXAM_ID` is an opaque foreign key to the un-landed `EXAM_TBL`. The only inputs to this builder are the term snapshot and the approved term map; person linkage remains explicitly gated.

# COMMAND ----------


def nullif_zero(column):
    return F.when(column != 0, column)


def build_exam_terms() -> DataFrame:
    normalised = normalised_term_text(F.col("TEXT_ASCII"))
    src = spark.table(SRC_TERM).select(
        F.col("PRIMARY_NO").alias("ENDOBASE_EXAM_TERM_ID"),
        F.col("EXAM_P").alias("ENDOBASE_EXAM_ID"),
        F.lit(None).cast("long").alias("PERSON_ID"),
        F.lit("EXAM_TABLE_NOT_LANDED").alias("PERSON_LINK_STATUS"),
        nullif_zero(F.col("TERM_P")).cast("long").alias("DGVS_TERM_ID"),
        (F.col("TERM_P") == 0).alias("FREE_TEXT_IND"),
        F.rtrim(F.col("TEXT_ASCII")).alias("TERM_TEXT"),
        normalised.alias("_norm_text"),
        nullif_zero(F.col("TAB_P")).alias("SECTION_TAB_ID"),
        nullif_zero(F.col("SUB_TAB_P")).alias("SUBSECTION_TAB_ID"),
        nullif_zero(F.col("SRC_EXAM_TERM_P")).alias("PARENT_EXAM_TERM_ID"),
        (F.col("CONFIRMED_FLG") == 1).alias("CONFIRMED_IND"),
        (F.col("TEXT_CHANGED_FLG") == 1).alias("TEXT_CHANGED_IND"),
        F.col("DISPLAY_ORDER"),
        F.col("TYPE_FLG"),
        F.col("GUID").alias("SOURCE_ROW_GUID"),
        F.col("CREATED").alias("CREATED_TS"),
        F.col("TEXT_CHANGED_FLG").alias("_changed_raw"),
    )
    assert_unique_non_null(src, ["ENDOBASE_EXAM_TERM_ID"], SRC_TERM)
    approved = (
        spark.table(TGT_TERM_MAP)
        .where(F.col("CURATED_STATUS") == "APPROVED")
        .select(
            "DGVS_TERM_ID",
            F.col("NORMALIZED_TEXT").alias("_norm_text"),
            F.col("OMOP_CONCEPT_ID").cast("long").alias("OMOP_CONCEPT_ID"),
            "SNOMED_CODE",
            "MAPPING_VERSION",
        )
    )
    return (
        src.join(approved, ["DGVS_TERM_ID", "_norm_text"], "left")
        .withColumn(
            "TERM_MAPPING_STATUS",
            F.when(F.col("FREE_TEXT_IND"), "FREE_TEXT")
            .when(F.col("_changed_raw") == 1, "TEXT_CHANGED")
            .when(F.col("OMOP_CONCEPT_ID").isNotNull(), "APPROVED")
            .otherwise("UNMAPPED"),
        )
        .withColumn(
            "OMOP_CONCEPT_ID",
            F.when(
                F.col("TERM_MAPPING_STATUS") == "APPROVED",
                F.col("OMOP_CONCEPT_ID"),
            ),
        )
        .withColumn(
            "SNOMED_CODE",
            F.when(
                F.col("TERM_MAPPING_STATUS") == "APPROVED",
                F.col("SNOMED_CODE"),
            ),
        )
        .withColumn(
            "TERM_MAPPING_VERSION",
            F.when(
                F.col("TERM_MAPPING_STATUS") == "APPROVED",
                F.col("MAPPING_VERSION"),
            ),
        )
        .select(
            "ENDOBASE_EXAM_TERM_ID",
            "ENDOBASE_EXAM_ID",
            "PERSON_ID",
            "PERSON_LINK_STATUS",
            "DGVS_TERM_ID",
            "FREE_TEXT_IND",
            "TERM_TEXT",
            "SECTION_TAB_ID",
            "SUBSECTION_TAB_ID",
            "PARENT_EXAM_TERM_ID",
            "CONFIRMED_IND",
            "TEXT_CHANGED_IND",
            "DISPLAY_ORDER",
            "TYPE_FLG",
            "SOURCE_ROW_GUID",
            "CREATED_TS",
            "OMOP_CONCEPT_ID",
            "SNOMED_CODE",
            "TERM_MAPPING_STATUS",
            "TERM_MAPPING_VERSION",
        )
    )


TERM_COMMENTS = {
    "ENDOBASE_EXAM_TERM_ID": "DGVS_EXAM_TERM_TBL.PRIMARY_NO; target grain key, one row per source report term.",
    "ENDOBASE_EXAM_ID": "Foreign key EXAM_P to the un-landed EXAM_TBL.PRIMARY_NO. Do NOT join to map_endobase_patient.ENDOBASE_PATIENT_ID; dense-integer overlap is spurious.",
    "PERSON_ID": "NULL while EXAM_TBL is not landed. Future linkage must follow EXAM_P to EXAM_TBL.PATIENT_P and then the EndoBase patient crosswalk.",
    "PERSON_LINK_STATUS": "EXAM_TABLE_NOT_LANDED in this release; explicitly records the person-linkage feature gate.",
    "DGVS_TERM_ID": "Local DGVS TERM_P identifier; NULL for TERM_P=0 free-text rows. It is not a stable value key without normalised text.",
    "FREE_TEXT_IND": "True when TERM_P=0 and the row is clinician-authored narrative rather than a DGVS template term.",
    "TERM_TEXT": "Right-trimmed TEXT_ASCII clinical report text. This is clinical free text and may contain patient identifiers; handle under the same controls as other bronze clinical text.",
    "SECTION_TAB_ID": "TAB_P when non-zero. Section-name decoding is gated on DGVS_TAB_TBL landing.",
    "SUBSECTION_TAB_ID": "SUB_TAB_P when non-zero. Subsection-name decoding is gated on DGVS_TAB_TBL landing.",
    "PARENT_EXAM_TERM_ID": "SRC_EXAM_TERM_P when non-zero; identifies the parent term for dependent or auto-inserted statements.",
    "CONFIRMED_IND": "True when CONFIRMED_FLG=1. Unconfirmed rows are deliberately retained.",
    "TEXT_CHANGED_IND": "True when TEXT_CHANGED_FLG=1. Edited rows are retained but excluded from candidate generation and mapping application.",
    "DISPLAY_ORDER": "Source report-term display order.",
    "TYPE_FLG": "Raw EndoBase TYPE_FLG. Semantics remain undocumented; observed values are predominantly 2 with rare 4.",
    "SOURCE_ROW_GUID": "Source DGVS exam-term GUID.",
    "CREATED_TS": "Report-authoring timestamp from CREATED; this is not the procedure date, which belongs to the un-landed EXAM_TBL.",
    "OMOP_CONCEPT_ID": "Applied only from a human-APPROVED term-map row whose concept is currently valid and standard; NULL for free text, edited text, and unmapped rows.",
    "SNOMED_CODE": "SNOMED CT code recorded by the approved map when applicable; NULL unless TERM_MAPPING_STATUS=APPROVED.",
    "TERM_MAPPING_STATUS": "APPROVED, UNMAPPED, TEXT_CHANGED, or FREE_TEXT. Edited and free-text rows can never carry an applied concept.",
    "TERM_MAPPING_VERSION": "Curation version for an applied APPROVED mapping; NULL otherwise.",
    "ROW_HASH": "SHA-256 of pipeline-managed business columns; ROW_HASH, SOURCE_PRESENT_IND and ADC_UPDT are excluded from the hash.",
    "SOURCE_PRESENT_IND": "False when the source term disappeared from the latest full snapshot; reactivated on reappearance.",
    "ADC_UPDT": "Timestamp this row was last inserted or changed by endobase_pipeline.",
}

_term_sources = [SRC_TERM, TGT_TERM_MAP]
assert SRC_PATIENT not in _term_sources, (
    "Term builder must never depend on or join directly to the EndoBase patient table"
)

_term_metrics = {"operation": "SKIP"}
if needs_run(TGT_TERM, _term_sources):
    _term_stage = build_exam_terms()
    _term_metrics = endobase_update_table(
        _term_stage,
        TGT_TERM,
        keys=["ENDOBASE_EXAM_TERM_ID"],
        sources=_term_sources,
    )
    print(f"[ENDOBASE] map_endobase_exam_term: {_term_metrics}")
else:
    print("[ENDOBASE] map_endobase_exam_term: sources unchanged, skipping")

_term_comment_changes = apply_comments(
    TGT_TERM,
    TERM_COMMENTS,
    "EndoBase DGVS report terms at one row per DGVS_EXAM_TERM_TBL.PRIMARY_NO. All free-text, edited, unconfirmed and dependent rows are retained. TERM_TEXT is clinical free text. ENDOBASE_EXAM_ID points to un-landed EXAM_TBL and must never be joined directly to map_endobase_patient.ENDOBASE_PATIENT_ID.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation, curation report, and JSON run result

# COMMAND ----------

_banned_identifier_columns = {
    "patient_id",
    "ssn",
    "first_name",
    "last_name",
    "middle_name",
    "birth_name",
    "birthday",
    "age",
    "birth_place",
    "zip_code",
    "city",
    "area_code",
    "street_number",
    "home_tel",
    "info_home_tel",
    "office_tel",
    "info_office_tel",
    "fax",
    "info_fax",
    "insurance_code",
    "insurance_name",
    "insurance_cust_no",
    "occupation",
    "race_p",
    "salutation_p",
    "date_of_death",
    "cause_of_death",
    "comment_of_death",
    "chk_patient_id",
    "chk_first_name",
    "chk_last_name",
    "hl7_key",
    "text_rtf",
}
for _table in (TGT_PATIENT, TGT_TERM, TGT_TERM_MAP):
    _leaked = _banned_identifier_columns & {
        column.lower() for column in spark.table(_table).columns
    }
    assert not _leaked, f"{_table} leaks excluded identifier columns: {_leaked}"

assert "TEXT_RTF" not in spark.table(TGT_TERM).columns
assert verify_unique_key(
    spark.table(TGT_PATIENT), ["ENDOBASE_PATIENT_ID"]
) == 0
assert verify_unique_key(
    spark.table(TGT_TERM), ["ENDOBASE_EXAM_TERM_ID"]
) == 0


def _cdf_enabled(table: str) -> bool:
    properties = spark.sql(f"DESCRIBE DETAIL {qname(table)}").collect()[0][
        "properties"
    ] or {}
    return str(properties.get("delta.enableChangeDataFeed", "false")).lower() == "true"


for _table in (TGT_PATIENT, TGT_TERM, TGT_TERM_MAP):
    assert _cdf_enabled(_table), f"CDF is not enabled on {_table}"

_patient_present = spark.table(TGT_PATIENT).where(F.col("SOURCE_PRESENT_IND"))
_patient_status_rows = (
    _patient_present.groupBy("PERSON_LINK_STATUS")
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(F.col("PERSON_ID").isNotNull(), 1).otherwise(0)).alias(
            "linked_rows"
        ),
    )
    .collect()
)
_patient_status_counts = {
    row["PERSON_LINK_STATUS"]: int(row["rows"]) for row in _patient_status_rows
}
_patient_current_rows = sum(_patient_status_counts.values())
_patient_linked_rows = sum(int(row["linked_rows"]) for row in _patient_status_rows)
_patient_link_rate = (
    _patient_linked_rows / _patient_current_rows if _patient_current_rows else 0.0
)
assert _patient_current_rows == int(preflight_report["patient"]["rows"]), (
    f"patient present rows {_patient_current_rows} != raw rows "
    f"{preflight_report['patient']['rows']}"
)
assert _patient_linked_rows >= 460000, (
    f"patient linked rows {_patient_linked_rows} below 460000 baseline floor"
)
assert _patient_link_rate >= 0.97, (
    f"patient link rate {_patient_link_rate:.4f} below 97%"
)
_conflict_rate = _patient_status_counts.get("CONFLICT", 0) / _patient_current_rows
assert _conflict_rate <= 0.001, (
    f"patient conflict rate {_conflict_rate:.4%} exceeds 0.1%"
)
_invalid_patient_resolution_rows = _patient_present.where(
    (
        F.col("PERSON_LINK_STATUS").isin("CONSENSUS", "MRN_ONLY", "NHS_ONLY")
        & F.col("PERSON_ID").isNull()
    )
    | (
        F.col("PERSON_LINK_STATUS").isin("CONFLICT", "AMBIGUOUS", "UNMATCHED")
        & F.col("PERSON_ID").isNotNull()
    )
).count()
assert _invalid_patient_resolution_rows == 0, (
    f"{_invalid_patient_resolution_rows} patient rows violate linkage status semantics"
)
_patient_dob_summary = (
    _patient_present.agg(
        F.sum(F.when(F.col("DOB_CONCORDANCE_IND") == True, 1).otherwise(0)).alias(
            "concordant"
        ),
        F.sum(F.when(F.col("DOB_CONCORDANCE_IND") == False, 1).otherwise(0)).alias(
            "discordant"
        ),
        F.sum(F.when(F.col("DOB_CONCORDANCE_IND").isNull(), 1).otherwise(0)).alias(
            "not_assessed"
        ),
    )
    .collect()[0]
    .asDict()
)

_term_present = spark.table(TGT_TERM).where(F.col("SOURCE_PRESENT_IND"))
_term_validation = (
    _term_present.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("ENDOBASE_EXAM_ID").alias("distinct_exams"),
        F.sum(F.when(F.col("FREE_TEXT_IND"), 1).otherwise(0)).alias(
            "free_text_rows"
        ),
        F.sum(
            F.when(F.col("TERM_MAPPING_STATUS") == "APPROVED", 1).otherwise(0)
        ).alias("approved_rows"),
        F.sum(
            F.when(
                (F.col("TERM_MAPPING_STATUS") != "APPROVED")
                & (
                    F.col("OMOP_CONCEPT_ID").isNotNull()
                    | F.col("SNOMED_CODE").isNotNull()
                    | F.col("TERM_MAPPING_VERSION").isNotNull()
                ),
                1,
            ).otherwise(0)
        ).alias("mapping_leak_rows"),
        F.sum(
            F.when(
                (F.col("TERM_MAPPING_STATUS") == "APPROVED")
                & (
                    F.col("FREE_TEXT_IND")
                    | F.col("TEXT_CHANGED_IND")
                    | F.col("OMOP_CONCEPT_ID").isNull()
                ),
                1,
            ).otherwise(0)
        ).alias("unsafe_approved_rows"),
        F.sum(F.when(F.col("PERSON_ID").isNotNull(), 1).otherwise(0)).alias(
            "person_populated_rows"
        ),
        F.sum(
            F.when(
                F.col("PERSON_LINK_STATUS") != "EXAM_TABLE_NOT_LANDED", 1
            ).otherwise(0)
        ).alias("unexpected_person_status_rows"),
    )
    .collect()[0]
    .asDict()
)
assert _term_validation["rows"] == int(preflight_report["term"]["rows"]), (
    f"term present rows {_term_validation['rows']} != raw rows "
    f"{preflight_report['term']['rows']}"
)
assert _term_validation["distinct_exams"] == int(
    preflight_report["term"]["distinct_exams"]
)
assert _term_validation["free_text_rows"] == int(
    preflight_report["term"]["free_text_rows"]
)
assert _term_validation["mapping_leak_rows"] == 0
assert _term_validation["unsafe_approved_rows"] == 0
assert _term_validation["person_populated_rows"] == 0
assert _term_validation["unexpected_person_status_rows"] == 0

_term_mapping_status_counts = {
    row["TERM_MAPPING_STATUS"]: int(row["count"])
    for row in _term_present.groupBy("TERM_MAPPING_STATUS").count().collect()
}
_mapping_status_counts = {
    row["CURATED_STATUS"]: int(row["count"])
    for row in spark.table(TGT_TERM_MAP).groupBy("CURATED_STATUS").count().collect()
}
_mapping_method_counts = {
    row["MAPPING_METHOD"]: int(row["count"])
    for row in spark.table(TGT_TERM_MAP).groupBy("MAPPING_METHOD").count().collect()
}

_top_candidates = (
    spark.table(TGT_TERM_MAP)
    .where(F.col("CURATED_STATUS") == "PROPOSED")
    .select(
        "DGVS_TERM_ID",
        "NORMALIZED_TEXT",
        "EXAMPLE_ROW_COUNT",
        "MAPPING_METHOD",
        "OMOP_CONCEPT_ID",
        "SNOMED_CODE",
        "CONCEPT_NAME",
        "MAPPING_VERSION",
    )
    .orderBy(
        F.desc("EXAMPLE_ROW_COUNT"),
        F.asc("DGVS_TERM_ID"),
        F.asc("NORMALIZED_TEXT"),
    )
    .limit(500)
)
_top_candidate_count = _top_candidates.count()
if _top_candidate_count:
    display(_top_candidates)

_exact_candidate_count = _mapping_method_counts.get("EXACT_NAME", 0)
if _exact_candidate_count:
    display(
        spark.table(TGT_TERM_MAP)
        .where(
            (F.col("CURATED_STATUS") == "PROPOSED")
            & (F.col("MAPPING_METHOD") == "EXACT_NAME")
        )
        .orderBy(
            F.desc("EXAMPLE_ROW_COUNT"),
            F.asc("DGVS_TERM_ID"),
            F.asc("NORMALIZED_TEXT"),
        )
        .limit(100)
    )

_patient_absent_rows = spark.table(TGT_PATIENT).where(
    ~F.col("SOURCE_PRESENT_IND")
).count()
_term_absent_rows = spark.table(TGT_TERM).where(
    ~F.col("SOURCE_PRESENT_IND")
).count()

payload = {
    "pipeline": "endobase_pipeline",
    "status": "SUCCESS",
    "run_id": ENDOBASE_RUN_ID,
    "target_schema": TARGET_SCHEMA,
    "started_at": _STARTED_AT,
    "completed_at": bronze_utc_now(),
    "force_full_refresh": FORCE_FULL_REFRESH,
    "preflight": preflight_report,
    "patient_metrics": _patient_metrics,
    "patient": {
        "present_rows": int(_patient_current_rows),
        "absent_rows": int(_patient_absent_rows),
        "linked_rows": int(_patient_linked_rows),
        "link_rate": round(float(_patient_link_rate), 6),
        "link_status_counts": _patient_status_counts,
        "dob_concordance": _patient_dob_summary,
    },
    "term_metrics": _term_metrics,
    "term": {
        "present_rows": int(_term_validation["rows"]),
        "absent_rows": int(_term_absent_rows),
        "distinct_exams": int(_term_validation["distinct_exams"]),
        "free_text_rows": int(_term_validation["free_text_rows"]),
        "mapping_status_counts": _term_mapping_status_counts,
    },
    "mapping": {
        "refresh_metrics": _mapping_metrics,
        "approved": int(_mapping_status_counts.get("APPROVED", 0)),
        "proposed": int(_mapping_status_counts.get("PROPOSED", 0)),
        "rejected": int(_mapping_status_counts.get("REJECTED", 0)),
        "method_counts": _mapping_method_counts,
        "applied_rows": int(_term_validation["approved_rows"]),
        "top_candidate_report_rows": int(_top_candidate_count),
    },
    "gates": gate_status,
    "comment_changes": {
        "patient": int(_patient_comment_changes),
        "term_map": int(_map_comment_changes),
        "term": int(_term_comment_changes),
    },
}

_result = bronze_json(payload)
print(_result)
dbutils.notebook.exit(_result)
