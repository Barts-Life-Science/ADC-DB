# Databricks notebook source
# MAGIC %md
# MAGIC # Device Pipeline
# MAGIC
# MAGIC Production device-mapping entry point. It runs a validated full rebuild when
# MAGIC outputs or checkpoints are missing, otherwise it consumes CDF/version changes.
# MAGIC Run through `bronze_pipeline` so `map_pipeline` creates implant details first.
# MAGIC
# MAGIC The implementation preserves one compatibility row per EVENT_ID, retains the
# MAGIC complete candidate bridge, and uses no Spark cache or DataFrame persistence.
# MAGIC Procedure context is sourced only from the canonical Bronze procedure table;
# MAGIC this consumer validates upstream CDF properties but never mutates its sources.

# COMMAND ----------

# MAGIC %pip install openai pyarrow

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_bronze_common

# COMMAND ----------

from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
import re
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Capture the legacy function before defining the compatibility override below.
_LEGACY_CREATE_IMPLANT_DETAILS_INCR = globals().get("create_implant_details_incr")

# COMMAND ----------


@dataclass(frozen=True)
class DeviceMappingConfig:
    implant_source: str = "4_prod.bronze.map_implant_details"
    procedures_source: str = "4_prod.bronze.map_procedure"
    gudid_device_source: str = "3_lookup.devices.device"
    gmdn_source: str = "3_lookup.devices.gmdnterms"
    opcs_lookup_source: str = "3_lookup.device_mapping.opcs_to_device"
    gmdn_standard_lookup_source: str = "3_lookup.device_mapping.gmdn_to_snomed"
    concept_source: str = "3_lookup.omop.concept"

    target_table: str = "4_prod.bronze.map_device_mapping"
    candidate_table: str = "6_mgmt.bronze.map_device_mapping_candidates"
    anchor_evidence_table: str = "3_lookup.device_mapping.anchor_evidence_v2"
    anchor_lookup_table: str = "3_lookup.device_mapping.anchor_lookup_v2"
    embedding_cache_table: str = "3_lookup.device_mapping.embedding_cache_v2"
    embedding_match_cache_table: str = "3_lookup.device_mapping.layer5_match_cache_v2"
    legacy_embedding_cache_table: str = "3_lookup.device_mapping.embedding_cache"
    legacy_embedding_match_cache_table: str = "3_lookup.device_mapping.layer5_match_cache"
    checkpoint_table: str = "3_lookup.device_mapping.map_device_mapping_checkpoints"

    pipeline_name: str = "map_device_mapping_v2"
    mapping_schema_version: str = "3.1.5"
    normalization_version: str = "3.0.0"
    brand_rules_version: str = "3.0.1"

    embedding_model: str = "azure_openai_embedding_endpoint"
    embedding_base_url: str = (
        "https://adb-3660342888273328.8.azuredatabricks.net/serving-endpoints"
    )
    embedding_api_batch_size: int = 96
    embedding_flush_size: int = 384
    embedding_capacity_retries: int = 8
    embedding_retry_max_seconds: int = 60
    similarity_batch_size: int = 256
    similarity_top_k: int = 25
    default_embedding_threshold: float = 0.65
    procedure_boosted_threshold: float = 0.58

    auto_full_sync_on_reference_change: bool = True
    cdf_retention: str = "interval 30 days"

    @property
    def tracked_tables(self) -> Tuple[str, ...]:
        return (
            self.implant_source,
            self.procedures_source,
            self.gudid_device_source,
            self.gmdn_source,
            self.opcs_lookup_source,
            self.gmdn_standard_lookup_source,
            self.concept_source,
        )

    @property
    def reference_tables(self) -> Tuple[str, ...]:
        return (
            self.gudid_device_source,
            self.gmdn_source,
            self.opcs_lookup_source,
            self.gmdn_standard_lookup_source,
            self.concept_source,
        )


DEFAULT_DEVICE_MAPPING_CONFIG = DeviceMappingConfig()


def implementation_version(config: DeviceMappingConfig) -> str:
    return "|".join([
        config.mapping_schema_version,
        config.normalization_version,
        config.brand_rules_version,
        config.embedding_model,
    ])


def _sf(name, data_type, comment="", nullable=True):
    metadata = {"comment": comment} if comment else {}
    return T.StructField(name, data_type, nullable, metadata=metadata)


schema_map_device_mapping_v2 = T.StructType([
    _sf("EVENT_ID", T.LongType(), "Source implant description event identifier.", False),
    _sf("ENCNTR_ID", T.LongType(), "Source encounter identifier."),
    _sf("PERSON_ID", T.LongType(), "Source person identifier."),
    _sf("IMPLANT_DESCRIPTION", T.StringType(), "Raw source implant description."),
    _sf("NORMALIZED_DESCRIPTION", T.StringType(), "Versioned punctuation/whitespace normalized description."),
    _sf("CLEANED_DESCRIPTION", T.StringType(), "Versioned fallback-matching description."),
    _sf("SOURCE_PRIMARY_PROCEDURE", T.StringType(), "Source primary procedure description."),
    _sf("SOURCE_PROCEDURE_CAT", T.LongType(), "Source procedure catalogue code."),
    _sf("SOURCE_CATALOGUE_NUMBER", T.StringType(), "Source manufacturer catalogue or part number."),
    _sf("SOURCE_MANUFACTURER", T.StringType(), "Source manufacturer text."),
    _sf("SOURCE_GS1_IDENTIFIER", T.StringType(), "Source GS1 UDI device identifier."),
    _sf("SOURCE_HIBCC_DEVICE_ID", T.StringType(), "Source HIBCC UDI device identifier."),
    _sf("UDI_DI", T.StringType(), "Coalesced source UDI-DI; issuer is retained separately."),
    _sf("UDI_ISSUER", T.StringType(), "GS1, HIBCC or null."),
    _sf("EFFECTIVE_SERIAL_NUMBER", T.StringType(), "GS1 serial number coalesced with raw serial number."),
    _sf("EFFECTIVE_EXPIRY_DATE", T.TimestampType(), "GS1 expiry date coalesced with raw expiry date."),
    _sf("SOURCE_ADC_UPDT", T.TimestampType(), "Source map_implant_details update timestamp."),
    _sf("gmdncode", T.LongType(), "Selected GMDN code."),
    _sf("gmdn_name", T.StringType(), "Selected GMDN preferred term name."),
    _sf("GMDN_DEFINITION", T.StringType(), "GMDN preferred term definition."),
    _sf("GMDN_CODE_STATUS", T.StringType(), "Status supplied by the GMDN reference source."),
    _sf("GMDN_IMPLANTABLE", T.StringType(), "Implantable indicator supplied by the GMDN reference source."),
    _sf("GMDN_REFERENCE_PRESENT_IND", T.BooleanType(), "Whether the selected code exists in the current GMDN source."),
    _sf("GMDN_MAPPING_PROVENANCE", T.StringType(), "REFERENCE, MANUAL_RULE or DIRECT_STANDARD_ONLY."),
    _sf("STANDARD_CONCEPT_ID", T.LongType(), "Selected standard OMOP device concept identifier."),
    _sf("STANDARD_CONCEPT_NAME", T.StringType(), "Selected standard OMOP device concept name."),
    _sf("STANDARD_VOCABULARY_ID", T.StringType(), "Vocabulary of STANDARD_CONCEPT_ID."),
    _sf("snomed_concept_id", T.LongType(), "Compatibility field populated only for SNOMED concepts."),
    _sf("snomed_name", T.StringType(), "Compatibility field populated only for SNOMED concepts."),
    _sf("device_type", T.StringType(), "Selected device category/type."),
    _sf("MAPPED_DEVICE_NAME", T.StringType(), "Best display name with explicit source fields retained."),
    _sf("mapping_layer", T.StringType(), "Selected mapping evidence layer."),
    _sf("mapping_confidence", T.DoubleType(), "Deterministic evidence score between zero and one."),
    _sf("confidence_tier", T.StringType(), "HIGH, MEDIUM, LOW or UNMAPPED."),
    _sf("MAPPING_STATUS", T.StringType(), "MAPPED, AMBIGUOUS_SELECTED or UNMAPPED."),
    _sf("MAPPING_RULE_ID", T.StringType(), "Rule or algorithm identifier for the selected candidate."),
    _sf("MATCHED_FIELD", T.StringType(), "Source field that produced the selected candidate."),
    _sf("MATCHED_VALUE", T.StringType(), "Normalized source value used for the selected candidate."),
    _sf("MAPPING_CANDIDATE_COUNT", T.IntegerType(), "Number of distinct mapping candidates retained."),
    _sf("MAPPING_DISTINCT_CONCEPT_COUNT", T.IntegerType(), "Number of distinct selected standard/GMDN tuples."),
    _sf("MAPPING_AMBIGUOUS_IND", T.BooleanType(), "Whether multiple credible candidates existed."),
    _sf("PROCEDURE_SUPPORT_IND", T.BooleanType(), "Whether procedure context supports the selected candidate."),
    _sf("MATCHED_OPCS_CODE", T.StringType(), "OPCS code supporting the selected candidate."),
    _sf("GUDID_PRIMARY_DI", T.StringType(), "Matched GUDID primary device identifier."),
    _sf("GUDID_BRAND_NAME", T.StringType(), "Matched GUDID brand name."),
    _sf("GUDID_COMPANY_NAME", T.StringType(), "Matched GUDID company name."),
    _sf("GUDID_MODEL_NUMBER", T.StringType(), "Matched GUDID version/model number."),
    _sf("GUDID_CATALOGUE_NUMBER", T.StringType(), "Matched GUDID catalogue number."),
    _sf("GUDID_DEVICE_STATUS", T.StringType(), "Matched GUDID device record status."),
    _sf("GUDID_DISTRIBUTION_STATUS", T.StringType(), "Matched commercial distribution status."),
    _sf("SOURCE_ROW_HASH", T.StringType(), "Hash of source fields relevant to mapping."),
    _sf("MAPPING_SCHEMA_VERSION", T.StringType(), "Output schema and mapping implementation version."),
    _sf("NORMALIZATION_VERSION", T.StringType(), "Description normalization version."),
    _sf("BRAND_RULES_VERSION", T.StringType(), "Boundary-aware phrase-rule version."),
    _sf("MAPPED_AT", T.TimestampType(), "Timestamp when this event was last evaluated."),
    _sf("PIPELINE_LOADED_AT", T.TimestampType(), "Timestamp this row was materially written."),
    _sf("ROW_HASH", T.StringType(), "Hash used to avoid rewriting unchanged rows."),
])


schema_device_mapping_candidate_v2 = T.StructType([
    _sf("CANDIDATE_ID", T.StringType(), "Deterministic event/candidate key.", False),
    _sf("EVENT_ID", T.LongType(), "Source implant event identifier.", False),
    _sf("ENCNTR_ID", T.LongType(), "Source encounter identifier."),
    _sf("PERSON_ID", T.LongType(), "Source person identifier."),
    _sf("IMPLANT_DESCRIPTION", T.StringType(), "Raw source implant description."),
    _sf("NORMALIZED_DESCRIPTION", T.StringType(), "Normalized source description."),
    _sf("CLEANED_DESCRIPTION", T.StringType(), "Fallback normalized source description."),
    _sf("gmdncode", T.LongType(), "Candidate GMDN code."),
    _sf("gmdn_name", T.StringType(), "Candidate GMDN preferred term."),
    _sf("GMDN_DEFINITION", T.StringType(), "Candidate GMDN definition."),
    _sf("GMDN_CODE_STATUS", T.StringType(), "Candidate GMDN status."),
    _sf("GMDN_IMPLANTABLE", T.StringType(), "Candidate GMDN implantable indicator."),
    _sf("GMDN_REFERENCE_PRESENT_IND", T.BooleanType(), "Candidate code exists in current GMDN source."),
    _sf("GMDN_MAPPING_PROVENANCE", T.StringType(), "Candidate GMDN provenance."),
    _sf("STANDARD_CONCEPT_ID", T.LongType(), "Candidate standard OMOP concept."),
    _sf("STANDARD_CONCEPT_NAME", T.StringType(), "Candidate standard concept name."),
    _sf("STANDARD_VOCABULARY_ID", T.StringType(), "Candidate standard concept vocabulary."),
    _sf("snomed_concept_id", T.LongType(), "SNOMED compatibility concept."),
    _sf("snomed_name", T.StringType(), "SNOMED compatibility concept name."),
    _sf("device_type", T.StringType(), "Candidate device type."),
    _sf("MAPPING_LAYER", T.StringType(), "Candidate evidence layer."),
    _sf("MAPPING_CONFIDENCE", T.DoubleType(), "Candidate evidence score."),
    _sf("EVIDENCE_PRIORITY", T.IntegerType(), "Lower values have stronger evidence precedence."),
    _sf("MAPPING_RULE_ID", T.StringType(), "Candidate rule identifier."),
    _sf("MATCHED_FIELD", T.StringType(), "Source field producing the candidate."),
    _sf("MATCHED_VALUE", T.StringType(), "Normalized value producing the candidate."),
    _sf("REFERENCE_CANDIDATE_COUNT", T.IntegerType(), "Candidates produced by this evidence source."),
    _sf("MAPPING_AMBIGUOUS_IND", T.BooleanType(), "Evidence source produced competing candidates."),
    _sf("PROCEDURE_SUPPORT_IND", T.BooleanType(), "Procedure context supports this candidate."),
    _sf("MATCHED_OPCS_CODE", T.StringType(), "Supporting OPCS code."),
    _sf("GUDID_PRIMARY_DI", T.StringType(), "Matched GUDID primary DI."),
    _sf("GUDID_BRAND_NAME", T.StringType(), "Matched GUDID brand."),
    _sf("GUDID_COMPANY_NAME", T.StringType(), "Matched GUDID company."),
    _sf("GUDID_MODEL_NUMBER", T.StringType(), "Matched GUDID model."),
    _sf("GUDID_CATALOGUE_NUMBER", T.StringType(), "Matched GUDID catalogue number."),
    _sf("GUDID_DEVICE_STATUS", T.StringType(), "Matched GUDID record status."),
    _sf("GUDID_DISTRIBUTION_STATUS", T.StringType(), "Matched GUDID distribution status."),
    _sf("CANDIDATE_RANK", T.IntegerType(), "Deterministic compatibility rank within EVENT_ID."),
    _sf("MAPPING_CANDIDATE_COUNT", T.IntegerType(), "Total retained candidates for EVENT_ID."),
    _sf("MAPPING_DISTINCT_CONCEPT_COUNT", T.IntegerType(), "Distinct standard/GMDN candidate tuples."),
    _sf("MAPPING_SCHEMA_VERSION", T.StringType(), "Mapping implementation version."),
    _sf("CREATED_AT", T.TimestampType(), "Candidate evaluation timestamp."),
])


TARGET_TABLE_COMMENT = (
    "Bronze implant-to-device mapping foundation at one row per source EVENT_ID. "
    "Every source event is retained, including unmapped events. Mapping provenance, "
    "ambiguity, source/reference fields and version metadata are explicit."
)

CANDIDATE_TABLE_COMMENT = (
    "Lossless mapping candidates for map_device_mapping. Candidate rank provides a "
    "deterministic compatibility selection without discarding alternative evidence."
)

# COMMAND ----------


def _sql_name(table_name: str) -> str:
    quote = chr(96)
    return ".".join(quote + part.replace(quote, quote + quote) + quote for part in table_name.split("."))


def _sql_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")


def _table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def _is_empty(df: Optional[DataFrame]) -> bool:
    return df is None or df.limit(1).count() == 0


def _union_distinct(
    dataframes: Iterable[Optional[DataFrame]],
    column: str = "EVENT_ID",
) -> Optional[DataFrame]:
    frames = [
        df.select(F.col(column).cast("long").alias(column)).where(F.col(column).isNotNull())
        for df in dataframes
        if df is not None
    ]
    if not frames:
        return None
    return reduce(lambda left, right: left.unionByName(right), frames).distinct()


def _device_hardening_sql_identifier(table_name: str) -> str:
    return '.'.join('`' + part.replace('`', '``') + '`' for part in table_name.split('.'))

def _device_hardening_snapshot_fallback_allowed(exc: BaseException) -> bool:
    messages = []
    current = exc
    while current is not None and len(messages) < 8:
        messages.append(str(current).lower())
        current = current.__cause__ or current.__context__
    combined = ' '.join(messages)
    return any(marker in combined for marker in (
        'delta_unsupported_time_travel_beyond_deleted_file_retention_duration',
        'time travel beyond delta.deletedfileretentionduration',
        'delta_log_file_not_found',
        'unable to retrieve the delta log files to construct table version',
        'delta log file not found',
    ))

def _device_hardening_read_pinned_snapshot(table_name: str, version: int):
    requested_version = int(version)
    try:
        pinned = (
            spark.read.format('delta')
            .option('versionAsOf', requested_version)
            .table(table_name)
        )
        # Spark Connect defers Delta-log validation until analysis/action.
        _ = pinned.schema
        pinned.limit(1).collect()
        return pinned
    except Exception as exc:
        if not _device_hardening_snapshot_fallback_allowed(exc):
            raise
        row = (
            spark.sql('DESCRIBE HISTORY ' + _device_hardening_sql_identifier(table_name) + ' LIMIT 1')
            .select('version')
            .first()
        )
        latest_version = None if row is None else int(row['version'])
        if latest_version != requested_version:
            raise
        print(
            '[WARN] Explicit time travel is outside retained Delta history for '
            + table_name + ' at current version ' + str(requested_version)
            + '; reading the same current snapshot without versionAsOf.'
        )
        current_snapshot = spark.table(table_name)
        _ = current_snapshot.schema
        return current_snapshot

def _device_hardening_validate_cdf(changes):
    # Keep lazy Spark Connect failures inside the caller's recovery block.
    _ = changes.schema
    changes.limit(1).collect()
    return changes

def _read_snapshot(
    table_name: str,
    source_versions: Optional[Dict[str, int]] = None,
) -> DataFrame:
    if source_versions is not None and table_name in source_versions:
        return _device_hardening_read_pinned_snapshot(
            table_name,
            int(source_versions[table_name]),
        )
    return spark.table(table_name)


def get_latest_delta_version(table_name: str) -> int:
    row = spark.sql(f"DESCRIBE HISTORY {_sql_name(table_name)} LIMIT 1").select("version").first()
    if row is None:
        raise RuntimeError(f"No Delta history is available for {table_name}")
    return int(row.version)


def _latest_by_key(
    df: DataFrame,
    keys: Sequence[str],
    order_columns: Sequence[str],
) -> DataFrame:
    ordering = [F.col(column).desc_nulls_last() for column in order_columns]
    window = Window.partitionBy(*keys).orderBy(*ordering)
    return (
        df.withColumn("_LATEST_ROW_NUMBER", F.row_number().over(window))
        .where(F.col("_LATEST_ROW_NUMBER") == 1)
        .drop("_LATEST_ROW_NUMBER")
    )


def _blank_to_null(column):
    value = F.trim(column.cast("string"))
    return F.when(value == "", F.lit(None).cast("string")).otherwise(value)


def normalize_description_col(column):
    value = F.lower(F.coalesce(column.cast("string"), F.lit("")))
    value = F.regexp_replace(value, r"[\u0000-\u001f]+", " ")
    value = F.regexp_replace(value, r"[^\p{L}\p{N}]+", " ")
    return F.trim(F.regexp_replace(value, r"\s+", " "))


def clean_description_col(column):
    value = normalize_description_col(column)
    value = F.regexp_replace(
        value,
        r"\b(ref|reference|lot|batch|serial|sn|pn|cat|catalogue|catalog|number|no)\s*[a-z0-9-]+\b",
        " ",
    )
    value = F.regexp_replace(value, r"\b\d{5,}\b", " ")
    return F.trim(F.regexp_replace(value, r"\s+", " "))


def canonical_identifier_col(column):
    return F.upper(F.regexp_replace(F.coalesce(column.cast("string"), F.lit("")), r"[^A-Za-z0-9]", ""))


def _hash_columns(columns: Sequence[str]):
    return F.sha2(
        F.to_json(
            F.struct(*[
                F.coalesce(F.col(column).cast("string"), F.lit("<NULL>")).alias(column)
                for column in columns
            ])
        ),
        256,
    )


def _align_to_schema(df: DataFrame, schema: T.StructType) -> DataFrame:
    source_columns = set(df.columns)
    projections = []
    for field in schema.fields:
        if field.name in source_columns:
            projections.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            projections.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(*projections)


def _apply_comments(table_name: str, schema: T.StructType, table_comment: str) -> None:
    spark.sql(f"COMMENT ON TABLE {_sql_name(table_name)} IS '{_sql_string(table_comment)}'")
    quote = chr(96)
    for field in schema.fields:
        comment = field.metadata.get("comment")
        if comment:
            column_name = quote + field.name.replace(quote, quote + quote) + quote
            spark.sql(
                f"ALTER TABLE {_sql_name(table_name)} ALTER COLUMN {column_name} "
                f"COMMENT '{_sql_string(comment)}'"
            )


def _replace_table(
    df: DataFrame,
    table_name: str,
    schema: T.StructType,
    table_comment: str,
    create_backup: bool = False,
    backup_suffix: Optional[str] = None,
) -> Optional[str]:
    aligned = _align_to_schema(df, schema)
    staging_table = f"{table_name}__staging_{uuid4().hex}"
    (
        aligned.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true")
        .saveAsTable(staging_table)
    )
    backup_table = None
    if create_backup and _table_exists(table_name):
        suffix = backup_suffix or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup_table = f"{table_name}__pre_v2_{suffix}"
        spark.sql(f"CREATE TABLE {_sql_name(backup_table)} SHALLOW CLONE {_sql_name(table_name)}")
    spark.sql(f"CREATE OR REPLACE TABLE {_sql_name(table_name)} DEEP CLONE {_sql_name(staging_table)}")
    spark.sql(
        f"ALTER TABLE {_sql_name(table_name)} SET TBLPROPERTIES "
        "('delta.enableChangeDataFeed'='true', 'delta.appendOnly'='false')"
    )
    _apply_comments(table_name, schema, table_comment)
    spark.sql(f"DROP TABLE IF EXISTS {_sql_name(staging_table)}")
    return backup_table


def _merge_changed_rows(
    source: DataFrame,
    target_table: str,
    key_columns: Sequence[str],
    row_hash_column: str = "ROW_HASH",
) -> None:
    if _is_empty(source):
        return
    target_schema = spark.table(target_table).schema
    aligned = _align_to_schema(source, target_schema)
    assignments = {field.name: f"source.{field.name}" for field in target_schema.fields}
    condition = " AND ".join(
        f"target.{column} <=> source.{column}" for column in key_columns
    )
    (
        DeltaTable.forName(spark, target_table)
        .alias("target")
        .merge(aligned.alias("source"), condition)
        .whenMatchedUpdate(
            condition=f"NOT (target.{row_hash_column} <=> source.{row_hash_column})",
            set=assignments,
        )
        .whenNotMatchedInsert(values=assignments)
        .execute()
    )


def _delete_key_subset(
    target_table: str,
    keys: Optional[DataFrame],
    key_column: str = "EVENT_ID",
) -> None:
    if keys is None or _is_empty(keys) or not _table_exists(target_table):
        return
    source = keys.select(F.col(key_column).cast("long").alias(key_column)).distinct()
    (
        DeltaTable.forName(spark, target_table)
        .alias("target")
        .merge(source.alias("source"), f"target.{key_column} <=> source.{key_column}")
        .whenMatchedDelete()
        .execute()
    )


def _replace_event_subset(
    new_rows: Optional[DataFrame],
    affected_event_ids: DataFrame,
    target_table: str,
    schema: Optional[T.StructType] = None,
) -> None:
    _delete_key_subset(target_table, affected_event_ids, "EVENT_ID")
    if new_rows is None or _is_empty(new_rows):
        return
    output = _align_to_schema(new_rows, schema) if schema is not None else new_rows
    output.write.format("delta").mode("append").saveAsTable(target_table)

# COMMAND ----------


def extract_udi_components_udf():
    """
    Drop-in replacement for the original UDI parser.

    It accepts scanner prefixes, parenthesized GS1 AIs and group separators,
    preserves the leading plus on HIBCC identifiers, and prevents variable-length
    fields from consuming a following application identifier.
    """
    import calendar
    import re
    from datetime import datetime

    def parse_date(value, expiry=False):
        if not value or len(value) != 6 or not value.isdigit():
            return None
        year = 2000 + int(value[0:2])
        month = int(value[2:4])
        day = int(value[4:6])
        if month < 1 or month > 12:
            return None
        if day == 0 and expiry:
            day = calendar.monthrange(year, month)[1]
        if day < 1:
            return None
        try:
            return datetime(year, month, day)
        except ValueError:
            return None

    def normalize_gs1(value):
        text = value.strip()
        text = re.sub(r"^\][A-Za-z0-9]{2}", "", text)
        text = re.sub(r"\((01|10|11|17|21)\)", r"\x1d\1", text)
        return text

    def parse_gs1(value, result):
        text = normalize_gs1(value)
        if not re.search(r"(?:^|\x1d)01\d{14}", text) and not text.startswith("01"):
            return
        fixed_patterns = {
            "gs1_di": r"(?:^|\x1d)01(\d{14})",
            "gs1_exp_date": r"(?:^|\x1d)17(\d{6})",
            "gs1_prod_date": r"(?:^|\x1d)11(\d{6})",
        }
        for key, pattern in fixed_patterns.items():
            match = re.search(pattern, text)
            if match:
                if key == "gs1_exp_date":
                    result[key] = parse_date(match.group(1), expiry=True)
                elif key == "gs1_prod_date":
                    result[key] = parse_date(match.group(1), expiry=False)
                else:
                    result[key] = match.group(1)
        for ai, key in (("10", "gs1_batch"), ("21", "gs1_serial")):
            match = re.search(
                rf"(?:^|\x1d){ai}(.+?)(?=\x1d(?:01|10|11|17|21)|$)",
                text,
            )
            if match:
                result[key] = match.group(1).strip("\x1d ").strip() or result[key]
        compact = text.replace("\x1d", "")
        if compact.startswith("01") and len(compact) >= 16 and result["gs1_di"] is None:
            result["gs1_di"] = compact[2:16]
        for ai, key, is_expiry in (
            ("17", "gs1_exp_date", True),
            ("11", "gs1_prod_date", False),
        ):
            match = re.search(ai + r"(\d{6})", compact[16:])
            if match and result[key] is None:
                result[key] = parse_date(match.group(1), expiry=is_expiry)
        for ai, key in (("10", "gs1_batch"), ("21", "gs1_serial")):
            if result[key] is None:
                match = re.search(ai + r"(.+?)(?=(?:01|10|11|17|21)\d|$)", compact[16:])
                if match:
                    result[key] = match.group(1).strip() or None

    def parse_hibcc(value, result):
        text = value.strip()
        if not text.startswith("+"):
            return
        parts = [part for part in re.split(r"[\x1d/]", text) if part]
        for part in parts:
            if part.startswith("+$$"):
                result["hibcc_pi"] = part
            elif part.startswith("+") and result["hibcc_di"] is None:
                result["hibcc_di"] = part

    def extract(serial_number, catalogue_number):
        result = {
            "gs1_di": None,
            "gs1_prod_date": None,
            "gs1_exp_date": None,
            "gs1_batch": None,
            "gs1_serial": None,
            "hibcc_di": None,
            "hibcc_pi": None,
        }
        for raw_value in (serial_number, catalogue_number):
            if raw_value is None:
                continue
            value = str(raw_value).strip()
            if not value:
                continue
            parse_gs1(value, result)
            parse_hibcc(value, result)
        return (
            result["gs1_di"],
            result["gs1_prod_date"],
            result["gs1_exp_date"],
            result["gs1_batch"],
            result["gs1_serial"],
            result["hibcc_di"],
            result["hibcc_pi"],
        )

    schema = T.StructType([
        T.StructField("gs1_di", T.StringType()),
        T.StructField("gs1_prod_date", T.TimestampType()),
        T.StructField("gs1_exp_date", T.TimestampType()),
        T.StructField("gs1_batch", T.StringType()),
        T.StructField("gs1_serial", T.StringType()),
        T.StructField("hibcc_di", T.StringType()),
        T.StructField("hibcc_pi", T.StringType()),
    ])
    return F.udf(extract, schema)

# COMMAND ----------


def ensure_checkpoint_table(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> None:
    if _table_exists(config.checkpoint_table):
        return
    schema = T.StructType([
        T.StructField("PIPELINE_NAME", T.StringType(), False),
        T.StructField("SOURCE_TABLE", T.StringType(), False),
        T.StructField("SOURCE_VERSION", T.LongType(), False),
        T.StructField("MAPPING_SCHEMA_VERSION", T.StringType(), False),
        T.StructField("UPDATED_AT", T.TimestampType(), False),
    ])
    (
        spark.createDataFrame([], schema)
        .write.format("delta")
        .mode("overwrite")
        .option("delta.enableChangeDataFeed", "true")
        .saveAsTable(config.checkpoint_table)
    )


def get_device_mapping_checkpoints(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> Dict[str, int]:
    if not _table_exists(config.checkpoint_table):
        return {}
    rows = (
        spark.table(config.checkpoint_table)
        .where(F.col("PIPELINE_NAME") == config.pipeline_name)
        .select("SOURCE_TABLE", "SOURCE_VERSION")
        .collect()
    )
    return {row.SOURCE_TABLE: int(row.SOURCE_VERSION) for row in rows}


def get_checkpoint_mapping_version(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> Optional[str]:
    if not _table_exists(config.checkpoint_table):
        return None
    row = (
        spark.table(config.checkpoint_table)
        .where(F.col("PIPELINE_NAME") == config.pipeline_name)
        .select("MAPPING_SCHEMA_VERSION")
        .limit(1)
        .first()
    )
    return row.MAPPING_SCHEMA_VERSION if row else None


def save_device_mapping_checkpoints(
    versions: Dict[str, int],
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> None:
    ensure_checkpoint_table(config)
    now = datetime.now(timezone.utc)
    rows = [
        (
            config.pipeline_name,
            table_name,
            int(version),
            implementation_version(config),
            now,
        )
        for table_name, version in versions.items()
    ]
    schema = spark.table(config.checkpoint_table).schema
    updates = spark.createDataFrame(rows, schema)
    (
        DeltaTable.forName(spark, config.checkpoint_table)
        .alias("target")
        .merge(
            updates.alias("source"),
            "target.PIPELINE_NAME = source.PIPELINE_NAME "
            "AND target.SOURCE_TABLE = source.SOURCE_TABLE",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def read_cdf_versions(
    table_name: str,
    starting_version: int,
    ending_version: int,
) -> Optional[DataFrame]:
    if starting_version > ending_version:
        return None
    try:
        changes = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", int(starting_version))
            .option("endingVersion", int(ending_version))
            .table(table_name)
        )
        return _device_hardening_validate_cdf(changes)
    except Exception as exc:
        raise RuntimeError(
            f"CDF is unavailable for {table_name} from version {starting_version} "
            f"to {ending_version}. Run deploy_map_device_mapping_improvements() "
            "to take a new validated snapshot."
        ) from exc


def configure_device_mapping_source_cdf(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> None:
    expected_sources = {
        "4_prod.bronze.map_implant_details": config.implant_source,
        "4_prod.bronze.map_procedure": config.procedures_source,
    }
    mismatches = {
        expected: actual
        for expected, actual in expected_sources.items()
        if actual != expected
    }
    if mismatches:
        raise RuntimeError(
            "Device mapping accepts only canonical Bronze event sources; "
            f"source contract mismatch: {mismatches}"
        )

    for table_name in (config.implant_source, config.procedures_source):
        if not _table_exists(table_name):
            raise RuntimeError(
                f"Required canonical Bronze source is missing: {table_name}. "
                "Run map_pipeline before device_pipeline."
            )
        detail = (
            spark.sql(f"DESCRIBE DETAIL {_sql_name(table_name)}")
            .select("format", "properties")
            .first()
        )
        properties = dict(detail.properties or {})
        if str(detail.format).lower() != "delta":
            raise RuntimeError(f"Device source is not a Delta table: {table_name}")
        if str(properties.get("delta.enableChangeDataFeed", "false")).lower() != "true":
            raise RuntimeError(
                f"Canonical source CDF is not enabled for {table_name}. "
                "The owning map component must enable CDF before device_pipeline runs."
            )
        print(f"[CHECK] canonical source CDF ready: {table_name}")


def current_device_mapping_versions(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> Dict[str, int]:
    return {table_name: get_latest_delta_version(table_name) for table_name in config.tracked_tables}

# COMMAND ----------


SOURCE_HASH_COLUMNS = [
    "ENCNTR_ID",
    "PERSON_ID",
    "IMPLANT_DESCRIPTION",
    "SOURCE_PRIMARY_PROCEDURE",
    "SOURCE_PROCEDURE_CAT",
    "SOURCE_CATALOGUE_NUMBER",
    "SOURCE_MANUFACTURER",
    "SOURCE_GS1_IDENTIFIER",
    "SOURCE_HIBCC_DEVICE_ID",
    "EFFECTIVE_SERIAL_NUMBER",
    "EFFECTIVE_EXPIRY_DATE",
    "SOURCE_ADC_UPDT",
]


def build_implant_source_base(
    event_ids: Optional[DataFrame] = None,
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
    source_versions: Optional[Dict[str, int]] = None,
) -> DataFrame:
    source = _read_snapshot(config.implant_source, source_versions)
    if event_ids is not None:
        source = source.join(
            event_ids.select(F.col("EVENT_ID").cast("long").alias("EVENT_ID")).distinct(),
            "EVENT_ID",
            "inner",
        )

    result = source.select(
        F.col("EVENT_ID").cast("long").alias("EVENT_ID"),
        F.col("ENCNTR_ID").cast("long").alias("ENCNTR_ID"),
        F.col("PERSON_ID").cast("long").alias("PERSON_ID"),
        F.col("IMPLANT_DESCRIPTION").cast("string").alias("IMPLANT_DESCRIPTION"),
        F.col("PRIMARY_PROCEDURE").cast("string").alias("SOURCE_PRIMARY_PROCEDURE"),
        F.col("PROCEDURE_CAT").cast("long").alias("SOURCE_PROCEDURE_CAT"),
        F.col("CATALOGUE_NUMBER").cast("string").alias("SOURCE_CATALOGUE_NUMBER"),
        F.col("MANUFACTURER").cast("string").alias("SOURCE_MANUFACTURER"),
        F.col("GS1_IDENTIFIER").cast("string").alias("SOURCE_GS1_IDENTIFIER"),
        F.col("HIBCC_DEVICE_ID").cast("string").alias("SOURCE_HIBCC_DEVICE_ID"),
        F.col("SERIAL_NUMBER").cast("string").alias("SOURCE_SERIAL_NUMBER"),
        F.col("GS1_SERIAL_NUMBER").cast("string").alias("SOURCE_GS1_SERIAL_NUMBER"),
        F.col("EXPIRY_DATE").cast("timestamp").alias("SOURCE_EXPIRY_DATE"),
        F.col("GS1_EXPIRY_DATE").cast("timestamp").alias("SOURCE_GS1_EXPIRY_DATE"),
        F.col("ADC_UPDT").cast("timestamp").alias("SOURCE_ADC_UPDT"),
    )

    result = (
        result
        .withColumn("NORMALIZED_DESCRIPTION", normalize_description_col(F.col("IMPLANT_DESCRIPTION")))
        .withColumn("CLEANED_DESCRIPTION", clean_description_col(F.col("IMPLANT_DESCRIPTION")))
        .withColumn("CATALOGUE_KEY", canonical_identifier_col(F.col("SOURCE_CATALOGUE_NUMBER")))
        .withColumn("MANUFACTURER_KEY", canonical_identifier_col(F.col("SOURCE_MANUFACTURER")))
        .withColumn(
            "UDI_DI",
            F.coalesce(
                _blank_to_null(F.col("SOURCE_GS1_IDENTIFIER")),
                _blank_to_null(F.col("SOURCE_HIBCC_DEVICE_ID")),
            ),
        )
        .withColumn(
            "UDI_ISSUER",
            F.when(_blank_to_null(F.col("SOURCE_GS1_IDENTIFIER")).isNotNull(), F.lit("GS1"))
            .when(_blank_to_null(F.col("SOURCE_HIBCC_DEVICE_ID")).isNotNull(), F.lit("HIBCC"))
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn("UDI_DI_KEY", canonical_identifier_col(F.col("UDI_DI")))
        .withColumn(
            "EFFECTIVE_SERIAL_NUMBER",
            F.coalesce(
                _blank_to_null(F.col("SOURCE_GS1_SERIAL_NUMBER")),
                _blank_to_null(F.col("SOURCE_SERIAL_NUMBER")),
            ),
        )
        .withColumn(
            "EFFECTIVE_EXPIRY_DATE",
            F.coalesce(F.col("SOURCE_GS1_EXPIRY_DATE"), F.col("SOURCE_EXPIRY_DATE")),
        )
    )
    result = result.withColumn("SOURCE_ROW_HASH", _hash_columns(SOURCE_HASH_COLUMNS))
    return result


PROCEDURE_DEVICE_CATEGORIES = {
    "W37": ["hip", "femoral", "acetabular", "prosthesis"],
    "W38": ["hip", "femoral", "acetabular", "prosthesis"],
    "W39": ["hip", "femoral", "acetabular", "prosthesis"],
    "W40": ["knee", "tibial", "femoral", "patella", "prosthesis"],
    "W41": ["knee", "tibial", "femoral", "patella", "prosthesis"],
    "W42": ["knee", "tibial", "femoral", "patella", "prosthesis"],
    "W19": ["nail", "screw", "plate", "wire", "pin", "fixation"],
    "W20": ["nail", "screw", "plate", "wire", "pin", "fixation"],
    "K59": ["defibrillator", "icd", "lead", "cardiac"],
    "K60": ["pacemaker", "lead", "generator", "cardiac"],
    "K61": ["pacemaker", "lead", "generator", "cardiac"],
    "C71": ["lens", "intraocular", "iol", "ophthalmic"],
    "C72": ["lens", "intraocular", "iol", "ophthalmic"],
    "C75": ["lens", "intraocular", "iol", "ophthalmic"],
    "K40": ["stent", "graft", "coronary"],
    "K41": ["stent", "graft", "coronary"],
    "L29": ["stent", "graft", "vascular", "aortic"],
    "T20": ["mesh", "hernia", "patch"],
    "T21": ["mesh", "hernia", "patch"],
    "T22": ["mesh", "hernia", "patch"],
    "T24": ["mesh", "hernia", "patch"],
    "T27": ["mesh", "hernia", "patch"],
    "T06": ["pectus", "chest", "funnel", "bar"],
}


def _procedure_prefix_dataframe() -> DataFrame:
    rows = [
        (prefix, keywords)
        for prefix, keywords in PROCEDURE_DEVICE_CATEGORIES.items()
    ]
    schema = T.StructType([
        T.StructField("OPCS_PREFIX", T.StringType(), False),
        T.StructField("PROCEDURE_KEYWORDS", T.ArrayType(T.StringType()), False),
    ])
    return spark.createDataFrame(rows, schema)


def build_procedure_context(
    source: DataFrame,
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
    source_versions: Optional[Dict[str, int]] = None,
) -> Tuple[DataFrame, DataFrame]:
    encounters = source.select("ENCNTR_ID").where(F.col("ENCNTR_ID").isNotNull()).distinct()
    raw_procedures = _read_snapshot(config.procedures_source, source_versions)
    available = {name.lower() for name in raw_procedures.columns}
    required = {"encntr_id", "opcs4_code", "opcs4_term", "source_string", "adc_updt"}
    missing = sorted(required - available)
    if missing:
        raise RuntimeError(
            f"Canonical procedure source {config.procedures_source} is missing "
            f"required columns: {missing}"
        )

    procedures = (
        raw_procedures
        .join(encounters, "ENCNTR_ID", "inner")
        .where(F.col("OPCS4_CODE").isNotNull())
        .select(
            F.col("ENCNTR_ID").cast("long").alias("ENCNTR_ID"),
            F.col("OPCS4_CODE").cast("string").alias("OPCS_CODE"),
            canonical_identifier_col(F.col("OPCS4_CODE")).alias("OPCS_CODE_KEY"),
            F.coalesce(F.col("OPCS4_TERM"), F.col("SOURCE_STRING")).cast("string").alias("OPCS_NAME"),
            F.col("ADC_UPDT").cast("timestamp").alias("PROCEDURE_ADC_UPDT"),
        )
    )

    procedures = (
        procedures
        .where(F.col("OPCS_CODE_KEY") != "")
        .dropDuplicates(["ENCNTR_ID", "OPCS_CODE_KEY"])
    )

    prefixes = F.broadcast(_procedure_prefix_dataframe())
    procedure_features = procedures.join(
        prefixes,
        F.substring(F.col("OPCS_CODE_KEY"), 1, 3) == F.col("OPCS_PREFIX"),
        "left",
    )
    empty_array = F.expr("array()").cast("array<string>")
    context = (
        procedure_features
        .groupBy("ENCNTR_ID")
        .agg(
            F.sort_array(F.collect_set("OPCS_CODE_KEY")).alias("OPCS_CODES"),
            F.array_distinct(
                F.flatten(
                    F.collect_list(F.coalesce(F.col("PROCEDURE_KEYWORDS"), empty_array))
                )
            ).alias("PROCEDURE_KEYWORDS"),
            F.max("PROCEDURE_ADC_UPDT").alias("PROCEDURE_ADC_UPDT"),
        )
        .withColumn("PROCEDURE_CONTEXT_KEY", F.concat_ws("|", F.col("OPCS_CODES")))
    )
    result = (
        source
        .join(context, "ENCNTR_ID", "left")
        .withColumn("OPCS_CODES", F.coalesce(F.col("OPCS_CODES"), empty_array))
        .withColumn("PROCEDURE_KEYWORDS", F.coalesce(F.col("PROCEDURE_KEYWORDS"), empty_array))
        .withColumn(
            "PROCEDURE_CONTEXT_KEY",
            F.when(F.col("PROCEDURE_CONTEXT_KEY").isNull(), F.lit("NO_OPCS"))
            .otherwise(F.col("PROCEDURE_CONTEXT_KEY")),
        )
        .withColumn(
            "MAPPING_CONTEXT",
            F.concat_ws(
                " ",
                F.col("NORMALIZED_DESCRIPTION"),
                normalize_description_col(F.col("SOURCE_PRIMARY_PROCEDURE")),
                F.concat_ws(" ", F.col("OPCS_CODES")),
            ),
        )
    )
    return result, procedures


# COMMAND ----------


def build_gmdn_reference(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
    source_versions: Optional[Dict[str, int]] = None,
) -> DataFrame:
    source = (
        _read_snapshot(config.gmdn_source, source_versions)
        .select(
            F.col("primarydi").cast("string").alias("PRIMARY_DI"),
            F.col("gmdncode").cast("long").alias("gmdncode"),
            F.col("gmdnptname").cast("string").alias("gmdn_name"),
            F.col("gmdnptdefinition").cast("string").alias("GMDN_DEFINITION"),
            F.col("gmdncodestatus").cast("string").alias("GMDN_CODE_STATUS"),
            F.col("implantable").cast("string").alias("GMDN_IMPLANTABLE"),
            F.col("ADC_UPDT").cast("timestamp").alias("GMDN_ADC_UPDT"),
        )
        .where(F.col("gmdncode").isNotNull())
    )
    return _latest_by_key(
        source,
        ["gmdncode"],
        ["GMDN_ADC_UPDT", "gmdn_name", "PRIMARY_DI"],
    )


def build_gudid_reference(
    gmdn_reference: DataFrame,
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
    source_versions: Optional[Dict[str, int]] = None,
    relevant_source: Optional[DataFrame] = None,
) -> DataFrame:
    device = (
        _read_snapshot(config.gudid_device_source, source_versions)
        .select(
            F.col("primarydi").cast("string").alias("GUDID_PRIMARY_DI"),
            canonical_identifier_col(F.col("primarydi")).alias("PRIMARY_DI_KEY"),
            F.col("publicdevicerecordkey").cast("string").alias("GUDID_RECORD_KEY"),
            F.col("publicversionstatus").cast("string").alias("GUDID_PUBLIC_VERSION_STATUS"),
            F.col("devicerecordstatus").cast("string").alias("GUDID_DEVICE_STATUS"),
            F.col("publicversionnumber").cast("string").alias("GUDID_PUBLIC_VERSION_NUMBER"),
            F.col("publicversiondate").cast("string").alias("GUDID_PUBLIC_VERSION_DATE"),
            F.col("devicecommdistributionstatus").cast("string").alias("GUDID_DISTRIBUTION_STATUS"),
            F.col("brandname").cast("string").alias("GUDID_BRAND_NAME"),
            F.col("versionmodelnumber").cast("string").alias("GUDID_MODEL_NUMBER"),
            F.col("catalognumber").cast("string").alias("GUDID_CATALOGUE_NUMBER"),
            canonical_identifier_col(F.col("catalognumber")).alias("CATALOGUE_KEY"),
            F.col("companyname").cast("string").alias("GUDID_COMPANY_NAME"),
            canonical_identifier_col(F.col("companyname")).alias("COMPANY_KEY"),
            F.col("devicedescription").cast("string").alias("GUDID_DEVICE_DESCRIPTION"),
            F.col("ADC_UPDT").cast("timestamp").alias("GUDID_ADC_UPDT"),
        )
        .where(F.col("PRIMARY_DI_KEY") != "")
    )
    if relevant_source is not None:
        udi_keys = (
            relevant_source.select(F.col("UDI_DI_KEY").alias("PRIMARY_DI_KEY"))
            .where(F.col("PRIMARY_DI_KEY") != "")
            .distinct()
        )
        catalogue_keys = (
            relevant_source.select("CATALOGUE_KEY")
            .where(F.col("CATALOGUE_KEY") != "")
            .distinct()
        )
        device = (
            device.join(udi_keys, "PRIMARY_DI_KEY", "left_semi")
            .unionByName(
                device.join(catalogue_keys, "CATALOGUE_KEY", "left_semi")
            )
            .dropDuplicates(["PRIMARY_DI_KEY", "GUDID_RECORD_KEY"])
        )
    device_window = Window.partitionBy("PRIMARY_DI_KEY").orderBy(
        F.col("GUDID_ADC_UPDT").desc_nulls_last(),
        F.col("GUDID_PUBLIC_VERSION_NUMBER").desc_nulls_last(),
        F.col("GUDID_RECORD_KEY").desc_nulls_last(),
    )
    latest_device = (
        device
        .withColumn("GUDID_RECORD_COUNT", F.count("*").over(Window.partitionBy("PRIMARY_DI_KEY")))
        .withColumn("_rn", F.row_number().over(device_window))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    terms = (
        _read_snapshot(config.gmdn_source, source_versions)
        .select(
            canonical_identifier_col(F.col("primarydi")).alias("PRIMARY_DI_KEY"),
            F.col("gmdncode").cast("long").alias("gmdncode"),
            F.col("ADC_UPDT").cast("timestamp").alias("GMDN_LINK_ADC_UPDT"),
        )
        .where((F.col("PRIMARY_DI_KEY") != "") & F.col("gmdncode").isNotNull())
    )
    terms = _latest_by_key(terms, ["PRIMARY_DI_KEY", "gmdncode"], ["GMDN_LINK_ADC_UPDT"])
    reference = latest_device.join(terms, "PRIMARY_DI_KEY", "left")
    reference = reference.join(gmdn_reference, "gmdncode", "left")
    counts = reference.groupBy("PRIMARY_DI_KEY").agg(
        F.countDistinct("gmdncode").alias("GUDID_GMDN_CANDIDATE_COUNT")
    )
    return reference.join(counts, "PRIMARY_DI_KEY", "left")


MANUAL_STANDARD_MAPPINGS = [
    (36093, 4239189, "Bone screw"),
    (47279, 4098108, "Bone plate"),
    (41888, 40355609, "Intrauterine contraceptive device"),
    (36566, 45768044, "Surgical mesh"),
    (35506, 4245414, "Bone cement"),
    (47337, 45768074, "Knee tibia prosthesis"),
    (36182, 4156227, "Pericardial patch"),
    (36611, 45759677, "Tibial insert"),
    (38630, 4231009, "Cardiac pacemaker electrode"),
    (61067, 4158970, "Glaucoma drainage device"),
    (38631, 45772840, "Implantable cardiac pacemaker"),
    (61669, 4300890, "Bone staple"),
    (38482, 45761793, "Acetabular shell"),
    (47331, 45772834, "Acetabular liner"),
]


def build_gmdn_standard_lookup(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
    source_versions: Optional[Dict[str, int]] = None,
) -> DataFrame:
    concept = (
        _read_snapshot(config.concept_source, source_versions)
        .select(
            F.col("concept_id").cast("long").alias("STANDARD_CONCEPT_ID"),
            F.col("concept_name").cast("string").alias("CONCEPT_NAME"),
            F.col("vocabulary_id").cast("string").alias("STANDARD_VOCABULARY_ID"),
            F.col("domain_id").cast("string").alias("DOMAIN_ID"),
            F.col("standard_concept").cast("string").alias("STANDARD_CONCEPT_FLAG"),
            F.col("invalid_reason").cast("string").alias("INVALID_REASON"),
        )
    )
    mapped = (
        _read_snapshot(config.gmdn_standard_lookup_source, source_versions)
        .select(
            F.col("gmdncode").cast("long").alias("gmdncode"),
            F.col("snomed_concept_id").cast("long").alias("STANDARD_CONCEPT_ID"),
            F.col("snomed_concept_name").cast("string").alias("LOOKUP_CONCEPT_NAME"),
            F.col("similarity_score").cast("double").alias("STANDARD_MAPPING_SCORE"),
        )
        .join(concept, "STANDARD_CONCEPT_ID", "left")
        .where(
            (F.col("DOMAIN_ID") == "Device")
            & (F.col("STANDARD_CONCEPT_FLAG") == "S")
            & F.col("INVALID_REASON").isNull()
        )
        .withColumn("MAPPING_SOURCE_PRIORITY", F.lit(1))
    )

    manual_schema = T.StructType([
        T.StructField("gmdncode", T.LongType(), False),
        T.StructField("STANDARD_CONCEPT_ID", T.LongType(), False),
        T.StructField("LOOKUP_CONCEPT_NAME", T.StringType(), True),
    ])
    manual = (
        spark.createDataFrame(MANUAL_STANDARD_MAPPINGS, manual_schema)
        .join(concept, "STANDARD_CONCEPT_ID", "left")
        .where(
            (F.col("DOMAIN_ID") == "Device")
            & (F.col("STANDARD_CONCEPT_FLAG") == "S")
            & F.col("INVALID_REASON").isNull()
        )
        .withColumn("STANDARD_MAPPING_SCORE", F.lit(1.0))
        .withColumn("MAPPING_SOURCE_PRIORITY", F.lit(0))
    )
    candidates = mapped.unionByName(manual, allowMissingColumns=True)
    window = Window.partitionBy("gmdncode").orderBy(
        F.col("MAPPING_SOURCE_PRIORITY").asc(),
        F.when(F.col("STANDARD_VOCABULARY_ID") == "SNOMED", F.lit(0)).otherwise(F.lit(1)).asc(),
        F.col("STANDARD_MAPPING_SCORE").desc_nulls_last(),
        F.col("STANDARD_CONCEPT_ID").asc(),
    )
    return (
        candidates
        .withColumn("STANDARD_MAPPING_CANDIDATE_COUNT", F.count("*").over(Window.partitionBy("gmdncode")))
        .withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .select(
            "gmdncode",
            "STANDARD_CONCEPT_ID",
            F.coalesce(F.col("CONCEPT_NAME"), F.col("LOOKUP_CONCEPT_NAME")).alias("STANDARD_CONCEPT_NAME"),
            "STANDARD_VOCABULARY_ID",
            "STANDARD_MAPPING_SCORE",
            "STANDARD_MAPPING_CANDIDATE_COUNT",
            F.when(F.col("MAPPING_SOURCE_PRIORITY") == 0, F.lit("MANUAL"))
            .otherwise(F.lit("LOOKUP")).alias("STANDARD_MAPPING_SOURCE"),
        )
    )

# COMMAND ----------


# Each rule is:
# rule_id, priority, regex, context_regex, gmdncode, gmdn_name,
# direct_standard_concept_id, direct_standard_name, device_type, confidence.
BRAND_RULES = [
    ("BIOLOX_INSERT", 1, r"\bbiolox\b.*\b(insert|liner|cup)\b", r"\b(hip|acetabular|femoral|arthroplasty)\b", 47496, "Ceramic acetabular cup prosthesis", None, None, "acetabular component", 0.97),
    ("BIOLOX_HEAD", 2, r"\bbiolox\b", r"\b(hip|femoral|head|arthroplasty)\b", 38156, "Ceramic femoral head prosthesis", None, None, "femoral head", 0.97),
    ("IOL_EYECEE", 10, r"\beyecee(\s+one|\s+iol)?\b", None, 35658, "Posterior-chamber intraocular lens, pseudophakic", None, None, "intraocular lens", 0.96),
    ("IOL_RAYONE", 11, r"\b(rayone|ray\s+one|rayner\s+c\s*flex|c\s*flex)\b", None, 35658, "Posterior-chamber intraocular lens, pseudophakic", None, None, "intraocular lens", 0.96),
    ("IOL_MODEL", 12, r"\b(envista|tecnis|acrysof|clareon|softport|sofport|versario|carlevale)\b", None, 35658, "Posterior-chamber intraocular lens, pseudophakic", None, None, "intraocular lens", 0.96),
    ("IOL_PHRASE", 13, r"\b(pre\s*loaded\s+lens|aspheric\s+lens|intra\s*ocular\s+lens|intraocular\s+lens|pc\s*iol|pciol|\biol\b)\b", None, 35658, "Posterior-chamber intraocular lens, pseudophakic", None, None, "intraocular lens", 0.95),
    ("IUD_MEDICATED", 20, r"\b(mirena|kyleena|jaydess|levosert)\b", None, 41888, "Intrauterine contraceptive device, medicated", None, None, "intrauterine contraceptive device", 0.96),
    ("IUD_GENERIC", 21, r"\b(iud|ius|intrauterine\s+(device|contraceptive|delivery\s+system))\b", None, 41888, "Intrauterine contraceptive device, medicated", None, None, "intrauterine contraceptive device", 0.92),
    ("IUD_COPPER", 19, r"\b(t\s*safe|tsafe|flexi\s*t|tcu\s*380a?|tcu380a?|cu\s*380a?|copper\s+(coil|t|iud|device)|intra\s*uterine.*copper|intrauterine.*copper)\b", None, None, None, 40355609, "Intrauterine contraceptive device", "intrauterine contraceptive device", 0.97),
    ("CONTRACEPTIVE_IMPLANT", 23, r"\b(nexplanon|implanon)\b", None, 47654, "Implantable contraceptive hormone delivery system", None, None, "contraceptive implant", 0.96),
    ("BONE_ANCHOR_NONABS", 30, r"\b(quick\s*anchor|quickanchor|fastin)\b", None, 45062, "Tendon/ligament bone anchor, non-bioabsorbable", None, None, "anchor", 0.95),
    ("BONE_ANCHOR_ABS", 31, r"\b(microfix|lupine|rigidfix|versalok)\b", None, 45061, "Tendon/ligament bone anchor, bioabsorbable", None, None, "anchor", 0.95),
    ("BIOABSORBABLE_SCREW", 32, r"\bmilagro\b", None, 45039, "Bioabsorbable orthopaedic bone screw", None, None, "screw", 0.95),
    ("BONE_WIRE", 40, r"\b(kirschner\s*wire|k\s*wire|kwire|steinmann\s+pin)\b", None, 35685, "Orthopaedic bone wire", None, None, "wire", 0.94),
    ("BONE_SCREW", 41, r"\b(cortex|cortical|cancellous|locking|lock|torx|stardrive|mandible)\s+screw\b", None, 36093, "Orthopaedic bone screw, non-bioabsorbable", None, None, "screw", 0.94),
    ("HIP_FIXATION", 42, r"\b(dynamic\s+(hip|condylar)\s+screw|dhs(\s*dcs)?|dcs\s+screw)\b", None, 66943, "Femoral fixation plate/blade", None, None, "plate", 0.94),
    ("BONE_PLATE", 43, r"\b(lcp|recon|reconstruction|mandible|mini|matrix\s+(midface|neuro))\s+plate\b", None, 47279, "Orthopaedic bone fixation plate, non-bioabsorbable", None, None, "plate", 0.93),
    ("FEMUR_NAIL", 44, r"\b(tfna|gamma\s*(3)?|pfna|femoral\s+nail|femur\s+nail|helical\s+blade)\b", None, 33187, "Femur nail", None, None, "nail", 0.94),
    ("TIBIA_NAIL", 45, r"\b(tibia|tibial)\s+nail\b", None, 38152, "Tibia nail", None, None, "nail", 0.94),
    ("CERAMIC_HEAD", 50, r"\b(ceramic\s+femoral\s+head|ceramic\s+head|delta\s+ceramic)\b", r"\b(hip|femoral|arthroplasty)\b", 38156, "Ceramic femoral head prosthesis", None, None, "femoral head", 0.94),
    ("ACETABULAR_COMPONENT", 51, r"\b(ceramic|delta)\s+(cup|liner)\b", r"\b(hip|acetabular|arthroplasty)\b", 47496, "Ceramic acetabular cup prosthesis", None, None, "acetabular component", 0.93),
    ("POLY_LINER", 52, r"\b(poly|polyethylene|pe)\s+liner\b", r"\b(hip|acetabular|arthroplasty)\b", 36777, "Acetabular cup prosthesis liner", None, None, "liner", 0.93),
    ("TIBIAL_COMPONENT", 53, r"\b(tibial\s+(tray|baseplate|component|plate)|attune\s+tibial|triathlon\s+tibial|persona\s+tibial)\b", None, 47501, "Knee joint tibial prosthesis", None, None, "tibial component", 0.94),
    ("BONE_CEMENT", 60, r"\b(palacos|simplex|smartset|cmw|bone\s+cement)\b", None, 35506, "Orthopaedic bone cement", None, None, "bone cement", 0.94),
    ("CEMENT_RESTRICTOR", 61, r"\b(cement\s+restrictor|hardinge)\b", None, 38073, "Metallic orthopaedic cement restrictor", None, None, "cement restrictor", 0.93),
    ("BIOLOGIC_PATCH", 70, r"\b(xenosure|xeno\s+sure|biologic(al)?\s+patch)\b", None, 35273, "Cardiovascular patch, animal-derived", None, None, "patch", 0.94),
    ("PERICARDIAL_PATCH", 71, r"\b(peri\s*guard|supple\s+peri|tutoplast)\b", None, 36182, "Pericardium prosthesis", None, None, "patch", 0.94),
    ("VASCULAR_GRAFT", 72, r"\b(gelweave|gel\s+weave|dacron\s+graft|vascular\s+(prosthesis|graft)|vascutek)\b", None, 35281, "Synthetic vascular graft", None, None, "graft", 0.94),
    ("SURGICAL_MESH", 73, r"\b(parietex|prolene\s+mesh|ultrapro\s+mesh|hernia\s+mesh|surgical\s+mesh|progrip)\b", None, 36566, "Surgical mesh, synthetic, non-bioabsorbable", None, None, "mesh", 0.94),
    ("PACEMAKER_LEAD", 80, r"\b(ingevity|tendril|pacemaker\s+lead)\b", None, 38630, "Implantable cardiac pacemaker electrode", None, None, "lead", 0.95),
    ("PACEMAKER_GENERATOR", 81, r"\b(advisa|ensura|azure|accolade|proponent|pacemaker\s+generator)\b", None, 38472, "Implantable cardiac pacemaker, dual-chamber", None, None, "pacemaker", 0.95),
    ("ICD_CRTD", 82, r"\b(emblem|s\s*icd|resonate|crt\s*d)\b", None, 47270, "Cardiac resynchronization therapy implantable defibrillator", None, None, "defibrillator", 0.95),
    ("ANTIBACTERIAL_ENVELOPE", 83, r"\btyrx\b", None, 62026, "Cardiac implantable electronic device antibacterial envelope", None, None, "envelope", 0.95),
    ("MITRAL_VALVE", 84, r"\bperimount\b.*\bmitral\b", None, 60244, "Mitral heart valve bioprosthesis", None, None, "heart valve", 0.96),
    ("AORTIC_VALVE", 85, r"\bperimount\b", r"\b(aortic|avr|valve)\b", 60242, "Aortic heart valve bioprosthesis", None, None, "heart valve", 0.94),
    ("GLAUCOMA_DEVICE", 90, r"\b(i\s*stent|istent|xen\s+implant)\b", r"\b(glaucoma|ophthalm|eye|trabecular)\b", 61067, "Glaucoma drainage device", None, None, "glaucoma device", 0.95),
    ("CAPSULAR_RING", 91, r"\b(capsular\s+tension\s+ring|tension\s+ring)\b", r"\b(cataract|lens|ophthalm|eye)\b", 42839, "Capsular tension ring", None, None, "ring", 0.94),
    ("STAPES_PROSTHESIS", 92, r"\b(stapes\s+(piston|prosthesis)|smart\s+stapes)\b", None, 35690, "Ossicular prosthesis, partial", None, None, "prosthesis", 0.95),
    ("BONE_STAPLE", 93, r"\b(static\s+staple|bone\s+staple)\b", None, 61669, "Orthopaedic bone staple, non-adjustable", None, None, "staple", 0.94),
    ("PECTUS_BAR", 94, r"\b(pectus\s+bar|nuss\s+bar|\bnuss\b)\b", None, 61522, "Funnel chest remodelling bar", None, None, "bar", 0.94),
    ("BREAST_IMPLANT", 95, r"\b(breast\s+implant|natrelle|siltex)\b", None, 36197, "Silicone gel-filled breast implant", None, None, "breast implant", 0.94),
    ("MENTOR_BREAST", 96, r"\bmentor\b", r"\b(breast|mammary|reconstruction|augmentation)\b", 36197, "Silicone gel-filled breast implant", None, None, "breast implant", 0.92),
]


def brand_rule_dataframe() -> DataFrame:
    schema = T.StructType([
        T.StructField("MAPPING_RULE_ID", T.StringType(), False),
        T.StructField("RULE_PRIORITY", T.IntegerType(), False),
        T.StructField("RULE_PATTERN", T.StringType(), False),
        T.StructField("RULE_CONTEXT_PATTERN", T.StringType(), True),
        T.StructField("gmdncode", T.LongType(), True),
        T.StructField("GMDN_NAME_HINT", T.StringType(), True),
        T.StructField("STANDARD_CONCEPT_ID_HINT", T.LongType(), True),
        T.StructField("STANDARD_CONCEPT_NAME_HINT", T.StringType(), True),
        T.StructField("DEVICE_TYPE_HINT", T.StringType(), True),
        T.StructField("RULE_CONFIDENCE", T.DoubleType(), False),
    ])
    return spark.createDataFrame(BRAND_RULES, schema)

# COMMAND ----------


RAW_CANDIDATE_SCHEMA = T.StructType([
    T.StructField("EVENT_ID", T.LongType(), False),
    T.StructField("ENCNTR_ID", T.LongType(), True),
    T.StructField("PERSON_ID", T.LongType(), True),
    T.StructField("IMPLANT_DESCRIPTION", T.StringType(), True),
    T.StructField("NORMALIZED_DESCRIPTION", T.StringType(), True),
    T.StructField("CLEANED_DESCRIPTION", T.StringType(), True),
    T.StructField("gmdncode", T.LongType(), True),
    T.StructField("GMDN_NAME_HINT", T.StringType(), True),
    T.StructField("STANDARD_CONCEPT_ID_HINT", T.LongType(), True),
    T.StructField("STANDARD_CONCEPT_NAME_HINT", T.StringType(), True),
    T.StructField("STANDARD_VOCABULARY_HINT", T.StringType(), True),
    T.StructField("DEVICE_TYPE_HINT", T.StringType(), True),
    T.StructField("MAPPING_LAYER", T.StringType(), False),
    T.StructField("MAPPING_CONFIDENCE", T.DoubleType(), False),
    T.StructField("EVIDENCE_PRIORITY", T.IntegerType(), False),
    T.StructField("MAPPING_RULE_ID", T.StringType(), False),
    T.StructField("MATCHED_FIELD", T.StringType(), True),
    T.StructField("MATCHED_VALUE", T.StringType(), True),
    T.StructField("REFERENCE_CANDIDATE_COUNT", T.IntegerType(), True),
    T.StructField("MAPPING_AMBIGUOUS_IND", T.BooleanType(), True),
    T.StructField("PROCEDURE_SUPPORT_IND", T.BooleanType(), True),
    T.StructField("MATCHED_OPCS_CODE", T.StringType(), True),
    T.StructField("GUDID_PRIMARY_DI", T.StringType(), True),
    T.StructField("GUDID_BRAND_NAME", T.StringType(), True),
    T.StructField("GUDID_COMPANY_NAME", T.StringType(), True),
    T.StructField("GUDID_MODEL_NUMBER", T.StringType(), True),
    T.StructField("GUDID_CATALOGUE_NUMBER", T.StringType(), True),
    T.StructField("GUDID_DEVICE_STATUS", T.StringType(), True),
    T.StructField("GUDID_DISTRIBUTION_STATUS", T.StringType(), True),
])


def _select_raw_candidate(df: DataFrame) -> DataFrame:
    return _align_to_schema(df, RAW_CANDIDATE_SCHEMA)


def _empty_raw_candidates() -> DataFrame:
    return spark.createDataFrame([], RAW_CANDIDATE_SCHEMA)


def build_udi_candidates(
    source: DataFrame,
    gudid_reference: DataFrame,
) -> DataFrame:
    joined = (
        source.alias("source")
        .where(F.col("UDI_DI_KEY") != "")
        .join(
            gudid_reference.alias("reference"),
            F.col("source.UDI_DI_KEY") == F.col("reference.PRIMARY_DI_KEY"),
            "inner",
        )
    )
    result = joined.select(
        F.col("source.EVENT_ID"),
        F.col("source.ENCNTR_ID"),
        F.col("source.PERSON_ID"),
        F.col("source.IMPLANT_DESCRIPTION"),
        F.col("source.NORMALIZED_DESCRIPTION"),
        F.col("source.CLEANED_DESCRIPTION"),
        F.col("reference.gmdncode"),
        F.col("reference.gmdn_name").alias("GMDN_NAME_HINT"),
        F.lit(None).cast("long").alias("STANDARD_CONCEPT_ID_HINT"),
        F.lit(None).cast("string").alias("STANDARD_CONCEPT_NAME_HINT"),
        F.lit(None).cast("string").alias("STANDARD_VOCABULARY_HINT"),
        F.lit(None).cast("string").alias("DEVICE_TYPE_HINT"),
        F.lit("LAYER1_UDI_GUDID").alias("MAPPING_LAYER"),
        F.when(F.col("reference.GUDID_GMDN_CANDIDATE_COUNT") <= 1, F.lit(0.99))
        .otherwise(F.lit(0.96)).alias("MAPPING_CONFIDENCE"),
        F.lit(10).alias("EVIDENCE_PRIORITY"),
        F.lit("UDI_EXACT_GUDID").alias("MAPPING_RULE_ID"),
        F.col("source.UDI_ISSUER").alias("MATCHED_FIELD"),
        F.col("source.UDI_DI_KEY").alias("MATCHED_VALUE"),
        F.greatest(
            F.lit(1),
            F.coalesce(F.col("reference.GUDID_GMDN_CANDIDATE_COUNT"), F.lit(1)),
        ).cast("int").alias("REFERENCE_CANDIDATE_COUNT"),
        (F.coalesce(F.col("reference.GUDID_GMDN_CANDIDATE_COUNT"), F.lit(1)) > 1)
        .alias("MAPPING_AMBIGUOUS_IND"),
        F.lit(False).alias("PROCEDURE_SUPPORT_IND"),
        F.lit(None).cast("string").alias("MATCHED_OPCS_CODE"),
        F.col("reference.GUDID_PRIMARY_DI"),
        F.col("reference.GUDID_BRAND_NAME"),
        F.col("reference.GUDID_COMPANY_NAME"),
        F.col("reference.GUDID_MODEL_NUMBER"),
        F.col("reference.GUDID_CATALOGUE_NUMBER"),
        F.col("reference.GUDID_DEVICE_STATUS"),
        F.col("reference.GUDID_DISTRIBUTION_STATUS"),
    )
    return _select_raw_candidate(result)


def build_catalogue_candidates(
    source: DataFrame,
    gudid_reference: DataFrame,
) -> DataFrame:
    joined = (
        source.alias("source")
        .where(F.col("CATALOGUE_KEY") != "")
        .join(
            gudid_reference.alias("reference"),
            F.col("source.CATALOGUE_KEY") == F.col("reference.CATALOGUE_KEY"),
            "inner",
        )
        .withColumn(
            "MANUFACTURER_MATCH_IND",
            (F.col("source.MANUFACTURER_KEY") != "")
            & (F.col("source.MANUFACTURER_KEY") == F.col("reference.COMPANY_KEY")),
        )
    )
    counts = joined.groupBy("EVENT_ID").agg(
        F.countDistinct("reference.GUDID_PRIMARY_DI").alias("PRIMARY_DI_CANDIDATES"),
        F.countDistinct(
            F.when(F.col("MANUFACTURER_MATCH_IND"), F.col("reference.GUDID_PRIMARY_DI"))
        ).alias("MANUFACTURER_MATCHING_PRIMARY_DI"),
    )
    joined = joined.join(counts, "EVENT_ID", "inner")
    eligible = joined.where(
        (F.col("PRIMARY_DI_CANDIDATES") == 1)
        | F.col("MANUFACTURER_MATCH_IND")
    )
    result = eligible.select(
        F.col("source.EVENT_ID"),
        F.col("source.ENCNTR_ID"),
        F.col("source.PERSON_ID"),
        F.col("source.IMPLANT_DESCRIPTION"),
        F.col("source.NORMALIZED_DESCRIPTION"),
        F.col("source.CLEANED_DESCRIPTION"),
        F.col("reference.gmdncode"),
        F.col("reference.gmdn_name").alias("GMDN_NAME_HINT"),
        F.lit(None).cast("long").alias("STANDARD_CONCEPT_ID_HINT"),
        F.lit(None).cast("string").alias("STANDARD_CONCEPT_NAME_HINT"),
        F.lit(None).cast("string").alias("STANDARD_VOCABULARY_HINT"),
        F.lit(None).cast("string").alias("DEVICE_TYPE_HINT"),
        F.lit("LAYER1B_CATALOGUE_GUDID").alias("MAPPING_LAYER"),
        F.when(
            F.col("MANUFACTURER_MATCH_IND")
            & (F.col("MANUFACTURER_MATCHING_PRIMARY_DI") == 1),
            F.lit(0.97),
        )
        .when(F.col("PRIMARY_DI_CANDIDATES") == 1, F.lit(0.92))
        .otherwise(F.lit(0.88))
        .alias("MAPPING_CONFIDENCE"),
        F.lit(20).alias("EVIDENCE_PRIORITY"),
        F.when(F.col("MANUFACTURER_MATCH_IND"), F.lit("CATALOGUE_MANUFACTURER_EXACT"))
        .otherwise(F.lit("CATALOGUE_UNIQUE_EXACT"))
        .alias("MAPPING_RULE_ID"),
        F.when(F.col("MANUFACTURER_MATCH_IND"), F.lit("CATALOGUE_NUMBER+MANUFACTURER"))
        .otherwise(F.lit("CATALOGUE_NUMBER"))
        .alias("MATCHED_FIELD"),
        F.col("source.CATALOGUE_KEY").alias("MATCHED_VALUE"),
        F.col("PRIMARY_DI_CANDIDATES").cast("int").alias("REFERENCE_CANDIDATE_COUNT"),
        (
            (F.col("PRIMARY_DI_CANDIDATES") > 1)
            | (F.coalesce(F.col("reference.GUDID_GMDN_CANDIDATE_COUNT"), F.lit(1)) > 1)
        ).alias("MAPPING_AMBIGUOUS_IND"),
        F.lit(False).alias("PROCEDURE_SUPPORT_IND"),
        F.lit(None).cast("string").alias("MATCHED_OPCS_CODE"),
        F.col("reference.GUDID_PRIMARY_DI"),
        F.col("reference.GUDID_BRAND_NAME"),
        F.col("reference.GUDID_COMPANY_NAME"),
        F.col("reference.GUDID_MODEL_NUMBER"),
        F.col("reference.GUDID_CATALOGUE_NUMBER"),
        F.col("reference.GUDID_DEVICE_STATUS"),
        F.col("reference.GUDID_DISTRIBUTION_STATUS"),
    )
    return _select_raw_candidate(result)


def build_brand_candidates(source: DataFrame) -> DataFrame:
    rules = F.broadcast(brand_rule_dataframe())
    candidates = (
        source.crossJoin(rules)
        .where(F.expr("NORMALIZED_DESCRIPTION RLIKE RULE_PATTERN"))
        .where(
            F.col("RULE_CONTEXT_PATTERN").isNull()
            | F.expr("MAPPING_CONTEXT RLIKE RULE_CONTEXT_PATTERN")
        )
    )
    counts = candidates.groupBy("EVENT_ID").agg(
        F.countDistinct("MAPPING_RULE_ID").alias("RULE_CANDIDATE_COUNT")
    )
    result = (
        candidates.join(counts, "EVENT_ID", "inner")
        .select(
            "EVENT_ID",
            "ENCNTR_ID",
            "PERSON_ID",
            "IMPLANT_DESCRIPTION",
            "NORMALIZED_DESCRIPTION",
            "CLEANED_DESCRIPTION",
            "gmdncode",
            F.col("GMDN_NAME_HINT"),
            F.col("STANDARD_CONCEPT_ID_HINT"),
            F.col("STANDARD_CONCEPT_NAME_HINT"),
            F.lit(None).cast("string").alias("STANDARD_VOCABULARY_HINT"),
            F.col("DEVICE_TYPE_HINT"),
            F.lit("LAYER3_BRAND_RULE").alias("MAPPING_LAYER"),
            F.col("RULE_CONFIDENCE").alias("MAPPING_CONFIDENCE"),
            F.lit(40).alias("EVIDENCE_PRIORITY"),
            "MAPPING_RULE_ID",
            F.lit("IMPLANT_DESCRIPTION").alias("MATCHED_FIELD"),
            F.col("MAPPING_RULE_ID").alias("MATCHED_VALUE"),
            F.col("RULE_CANDIDATE_COUNT").cast("int").alias("REFERENCE_CANDIDATE_COUNT"),
            (F.col("RULE_CANDIDATE_COUNT") > 1).alias("MAPPING_AMBIGUOUS_IND"),
            F.lit(False).alias("PROCEDURE_SUPPORT_IND"),
            F.lit(None).cast("string").alias("MATCHED_OPCS_CODE"),
            F.lit(None).cast("string").alias("GUDID_PRIMARY_DI"),
            F.lit(None).cast("string").alias("GUDID_BRAND_NAME"),
            F.lit(None).cast("string").alias("GUDID_COMPANY_NAME"),
            F.lit(None).cast("string").alias("GUDID_MODEL_NUMBER"),
            F.lit(None).cast("string").alias("GUDID_CATALOGUE_NUMBER"),
            F.lit(None).cast("string").alias("GUDID_DEVICE_STATUS"),
            F.lit(None).cast("string").alias("GUDID_DISTRIBUTION_STATUS"),
        )
    )
    return _select_raw_candidate(result)


def build_opcs_candidates(
    source: DataFrame,
    procedures: DataFrame,
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
    source_versions: Optional[Dict[str, int]] = None,
) -> DataFrame:
    lookup = (
        _read_snapshot(config.opcs_lookup_source, source_versions)
        .select(
            canonical_identifier_col(F.col("opcs_code")).alias("OPCS_CODE_KEY"),
            F.col("opcs_code").cast("string").alias("LOOKUP_OPCS_CODE"),
            F.col("device_concept_id").cast("long").alias("STANDARD_CONCEPT_ID_HINT"),
            F.col("device_name").cast("string").alias("STANDARD_CONCEPT_NAME_HINT"),
            F.col("device_type").cast("string").alias("DEVICE_TYPE_HINT"),
        )
        .where(F.col("OPCS_CODE_KEY") != "")
    )
    joined = (
        source.alias("source")
        .join(procedures.alias("procedure"), "ENCNTR_ID", "inner")
        .join(
            F.broadcast(lookup).alias("lookup"),
            F.col("procedure.OPCS_CODE_KEY") == F.col("lookup.OPCS_CODE_KEY"),
            "inner",
        )
        .withColumn(
            "DESCRIPTION_SUPPORT_IND",
            F.instr(
                F.col("source.NORMALIZED_DESCRIPTION"),
                F.lower(F.coalesce(F.col("lookup.DEVICE_TYPE_HINT"), F.lit(""))),
            ) > 0,
        )
    )
    counts = joined.groupBy("EVENT_ID").agg(
        F.countDistinct("lookup.STANDARD_CONCEPT_ID_HINT").alias("OPCS_CONCEPT_CANDIDATES"),
        F.countDistinct("procedure.OPCS_CODE_KEY").alias("OPCS_CODE_CANDIDATES"),
    )
    joined = joined.join(counts, "EVENT_ID", "inner")
    result = joined.select(
        F.col("source.EVENT_ID"),
        F.col("source.ENCNTR_ID"),
        F.col("source.PERSON_ID"),
        F.col("source.IMPLANT_DESCRIPTION"),
        F.col("source.NORMALIZED_DESCRIPTION"),
        F.col("source.CLEANED_DESCRIPTION"),
        F.lit(None).cast("long").alias("gmdncode"),
        F.lit(None).cast("string").alias("GMDN_NAME_HINT"),
        F.col("lookup.STANDARD_CONCEPT_ID_HINT"),
        F.col("lookup.STANDARD_CONCEPT_NAME_HINT"),
        F.lit(None).cast("string").alias("STANDARD_VOCABULARY_HINT"),
        F.col("lookup.DEVICE_TYPE_HINT"),
        F.lit("LAYER6_OPCS_STANDARD").alias("MAPPING_LAYER"),
        F.least(
            F.lit(0.82),
            F.lit(0.66)
            + F.when(F.col("DESCRIPTION_SUPPORT_IND"), F.lit(0.10)).otherwise(F.lit(0.0))
            + F.when(F.col("OPCS_CONCEPT_CANDIDATES") == 1, F.lit(0.04)).otherwise(F.lit(-0.04)),
        ).alias("MAPPING_CONFIDENCE"),
        F.lit(70).alias("EVIDENCE_PRIORITY"),
        F.lit("OPCS_DEVICE_LOOKUP").alias("MAPPING_RULE_ID"),
        F.lit("OPCS_CODE").alias("MATCHED_FIELD"),
        F.col("procedure.OPCS_CODE_KEY").alias("MATCHED_VALUE"),
        F.col("OPCS_CONCEPT_CANDIDATES").cast("int").alias("REFERENCE_CANDIDATE_COUNT"),
        (F.col("OPCS_CONCEPT_CANDIDATES") > 1).alias("MAPPING_AMBIGUOUS_IND"),
        F.col("DESCRIPTION_SUPPORT_IND").alias("PROCEDURE_SUPPORT_IND"),
        F.col("procedure.OPCS_CODE").alias("MATCHED_OPCS_CODE"),
        F.lit(None).cast("string").alias("GUDID_PRIMARY_DI"),
        F.lit(None).cast("string").alias("GUDID_BRAND_NAME"),
        F.lit(None).cast("string").alias("GUDID_COMPANY_NAME"),
        F.lit(None).cast("string").alias("GUDID_MODEL_NUMBER"),
        F.lit(None).cast("string").alias("GUDID_CATALOGUE_NUMBER"),
        F.lit(None).cast("string").alias("GUDID_DEVICE_STATUS"),
        F.lit(None).cast("string").alias("GUDID_DISTRIBUTION_STATUS"),
    )
    return _select_raw_candidate(result)

# COMMAND ----------


ANCHOR_EVIDENCE_SCHEMA = T.StructType([
    T.StructField("EVENT_ID", T.LongType(), False),
    T.StructField("NORMALIZED_DESCRIPTION", T.StringType(), True),
    T.StructField("CLEANED_DESCRIPTION", T.StringType(), True),
    T.StructField("gmdncode", T.LongType(), False),
    T.StructField("gmdn_name", T.StringType(), True),
    T.StructField("device_type", T.StringType(), True),
    T.StructField("EVIDENCE_LAYER", T.StringType(), False),
    T.StructField("SOURCE_ADC_UPDT", T.TimestampType(), True),
    T.StructField("MAPPING_SCHEMA_VERSION", T.StringType(), False),
    T.StructField("UPDATED_AT", T.TimestampType(), False),
])

ANCHOR_LOOKUP_SCHEMA = T.StructType([
    T.StructField("KEY_TYPE", T.StringType(), False),
    T.StructField("ANCHOR_KEY", T.StringType(), False),
    T.StructField("gmdncode", T.LongType(), True),
    T.StructField("gmdn_name", T.StringType(), True),
    T.StructField("device_type", T.StringType(), True),
    T.StructField("ANCHOR_COUNT", T.LongType(), False),
    T.StructField("DISTINCT_GMDN_COUNT", T.IntegerType(), False),
    T.StructField("IS_AMBIGUOUS", T.BooleanType(), False),
    T.StructField("UPDATED_AT", T.TimestampType(), False),
])


def build_anchor_evidence(
    source: DataFrame,
    direct_candidates: DataFrame,
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> DataFrame:
    direct = direct_candidates.where(
        F.col("MAPPING_LAYER").isin("LAYER1_UDI_GUDID", "LAYER1B_CATALOGUE_GUDID")
        & F.col("gmdncode").isNotNull()
    )
    per_event = direct.groupBy("EVENT_ID").agg(
        F.countDistinct("gmdncode").alias("DIRECT_GMDN_COUNT")
    )
    window = Window.partitionBy("EVENT_ID").orderBy(
        F.col("EVIDENCE_PRIORITY").asc(),
        F.col("MAPPING_CONFIDENCE").desc(),
        F.col("gmdncode").asc(),
    )
    selected = (
        direct.join(per_event, "EVENT_ID", "inner")
        .where(F.col("DIRECT_GMDN_COUNT") == 1)
        .withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop("_rn", "DIRECT_GMDN_COUNT")
    )
    evidence = (
        source.select(
            "EVENT_ID",
            "NORMALIZED_DESCRIPTION",
            "CLEANED_DESCRIPTION",
            "SOURCE_ADC_UPDT",
        )
        .join(
            selected.select(
                "EVENT_ID",
                "gmdncode",
                F.col("GMDN_NAME_HINT").alias("gmdn_name"),
                F.col("DEVICE_TYPE_HINT").alias("device_type"),
                F.col("MAPPING_LAYER").alias("EVIDENCE_LAYER"),
            ),
            "EVENT_ID",
            "inner",
        )
        .withColumn("MAPPING_SCHEMA_VERSION", F.lit(config.mapping_schema_version))
        .withColumn("UPDATED_AT", F.current_timestamp())
    )
    return _align_to_schema(evidence, ANCHOR_EVIDENCE_SCHEMA)


def build_anchor_lookup_from_evidence(evidence: DataFrame) -> DataFrame:
    exact = evidence.select(
        F.lit("EXACT").alias("KEY_TYPE"),
        F.col("NORMALIZED_DESCRIPTION").alias("ANCHOR_KEY"),
        "gmdncode",
        "gmdn_name",
        "device_type",
        "SOURCE_ADC_UPDT",
    )
    cleaned = evidence.select(
        F.lit("CLEANED").alias("KEY_TYPE"),
        F.col("CLEANED_DESCRIPTION").alias("ANCHOR_KEY"),
        "gmdncode",
        "gmdn_name",
        "device_type",
        "SOURCE_ADC_UPDT",
    )
    keys = exact.unionByName(cleaned).where(
        F.col("ANCHOR_KEY").isNotNull() & (F.col("ANCHOR_KEY") != "")
    )
    by_code = keys.groupBy("KEY_TYPE", "ANCHOR_KEY", "gmdncode").agg(
        F.count("*").alias("CODE_COUNT"),
        F.max("SOURCE_ADC_UPDT").alias("LATEST_SOURCE_ADC_UPDT"),
        F.max("gmdn_name").alias("gmdn_name"),
        F.max("device_type").alias("device_type"),
    )
    stats = by_code.groupBy("KEY_TYPE", "ANCHOR_KEY").agg(
        F.sum("CODE_COUNT").alias("ANCHOR_COUNT"),
        F.countDistinct("gmdncode").cast("int").alias("DISTINCT_GMDN_COUNT"),
    )
    window = Window.partitionBy("KEY_TYPE", "ANCHOR_KEY").orderBy(
        F.col("CODE_COUNT").desc(),
        F.col("LATEST_SOURCE_ADC_UPDT").desc_nulls_last(),
        F.col("gmdncode").asc(),
    )
    selected = (
        by_code
        .withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop("_rn")
        .join(stats, ["KEY_TYPE", "ANCHOR_KEY"], "inner")
        .withColumn("IS_AMBIGUOUS", F.col("DISTINCT_GMDN_COUNT") > 1)
        .withColumn("UPDATED_AT", F.current_timestamp())
    )
    return _align_to_schema(selected, ANCHOR_LOOKUP_SCHEMA)


def build_anchor_candidates(
    source: DataFrame,
    anchor_lookup: DataFrame,
) -> DataFrame:
    safe_anchors = F.broadcast(anchor_lookup.where(~F.col("IS_AMBIGUOUS")))

    def create_for_key(key_type, source_column, layer, priority, base_confidence):
        joined = source.alias("source").join(
            safe_anchors.where(F.col("KEY_TYPE") == key_type).alias("anchor"),
            F.col(f"source.{source_column}") == F.col("anchor.ANCHOR_KEY"),
            "inner",
        )
        result = joined.select(
            F.col("source.EVENT_ID"),
            F.col("source.ENCNTR_ID"),
            F.col("source.PERSON_ID"),
            F.col("source.IMPLANT_DESCRIPTION"),
            F.col("source.NORMALIZED_DESCRIPTION"),
            F.col("source.CLEANED_DESCRIPTION"),
            F.col("anchor.gmdncode"),
            F.col("anchor.gmdn_name").alias("GMDN_NAME_HINT"),
            F.lit(None).cast("long").alias("STANDARD_CONCEPT_ID_HINT"),
            F.lit(None).cast("string").alias("STANDARD_CONCEPT_NAME_HINT"),
            F.lit(None).cast("string").alias("STANDARD_VOCABULARY_HINT"),
            F.col("anchor.device_type").alias("DEVICE_TYPE_HINT"),
            F.lit(layer).alias("MAPPING_LAYER"),
            F.least(
                F.lit(0.97),
                F.lit(base_confidence)
                + F.log1p(F.col("anchor.ANCHOR_COUNT").cast("double")) * F.lit(0.01),
            ).alias("MAPPING_CONFIDENCE"),
            F.lit(priority).alias("EVIDENCE_PRIORITY"),
            F.lit(f"SAFE_{key_type}_ANCHOR").alias("MAPPING_RULE_ID"),
            F.lit(source_column).alias("MATCHED_FIELD"),
            F.col(f"source.{source_column}").alias("MATCHED_VALUE"),
            F.lit(1).cast("int").alias("REFERENCE_CANDIDATE_COUNT"),
            F.lit(False).alias("MAPPING_AMBIGUOUS_IND"),
            F.lit(False).alias("PROCEDURE_SUPPORT_IND"),
            F.lit(None).cast("string").alias("MATCHED_OPCS_CODE"),
            F.lit(None).cast("string").alias("GUDID_PRIMARY_DI"),
            F.lit(None).cast("string").alias("GUDID_BRAND_NAME"),
            F.lit(None).cast("string").alias("GUDID_COMPANY_NAME"),
            F.lit(None).cast("string").alias("GUDID_MODEL_NUMBER"),
            F.lit(None).cast("string").alias("GUDID_CATALOGUE_NUMBER"),
            F.lit(None).cast("string").alias("GUDID_DEVICE_STATUS"),
            F.lit(None).cast("string").alias("GUDID_DISTRIBUTION_STATUS"),
        )
        return _select_raw_candidate(result)

    exact = create_for_key("EXACT", "NORMALIZED_DESCRIPTION", "LAYER2_EXACT_ANCHOR", 30, 0.92)
    cleaned = create_for_key("CLEANED", "CLEANED_DESCRIPTION", "LAYER4_CLEANED_ANCHOR", 50, 0.84)
    return exact.unionByName(cleaned)

# COMMAND ----------


EMBEDDING_CACHE_SCHEMA = T.StructType([
    T.StructField("CACHE_KEY", T.StringType(), False),
    T.StructField("TEXT", T.StringType(), False),
    T.StructField("TEXT_TYPE", T.StringType(), False),
    T.StructField("EMBEDDING_MODEL", T.StringType(), False),
    T.StructField("NORMALIZATION_VERSION", T.StringType(), False),
    T.StructField("EMBEDDING", T.ArrayType(T.DoubleType()), False),
    T.StructField("CREATED_AT", T.TimestampType(), False),
    T.StructField("UPDATED_AT", T.TimestampType(), False),
])

EMBEDDING_MATCH_CACHE_SCHEMA = T.StructType([
    T.StructField("CACHE_KEY", T.StringType(), False),
    T.StructField("CLEANED_DESCRIPTION", T.StringType(), False),
    T.StructField("PROCEDURE_CONTEXT_KEY", T.StringType(), False),
    T.StructField("gmdncode", T.LongType(), True),
    T.StructField("gmdn_name", T.StringType(), True),
    T.StructField("SIMILARITY_SCORE", T.DoubleType(), True),
    T.StructField("MATCH_STATUS", T.StringType(), False),
    T.StructField("PROCEDURE_SUPPORT_IND", T.BooleanType(), False),
    T.StructField("EMBEDDING_MODEL", T.StringType(), False),
    T.StructField("NORMALIZATION_VERSION", T.StringType(), False),
    T.StructField("BRAND_RULES_VERSION", T.StringType(), False),
    T.StructField("GMDN_REFERENCE_VERSION", T.LongType(), False),
    T.StructField("CREATED_AT", T.TimestampType(), False),
    T.StructField("UPDATED_AT", T.TimestampType(), False),
])


def _ensure_delta_table(table_name: str, schema: T.StructType) -> None:
    if _table_exists(table_name):
        return
    (
        spark.createDataFrame([], schema)
        .write.format("delta")
        .mode("overwrite")
        .option("delta.enableChangeDataFeed", "true")
        .saveAsTable(table_name)
    )


def ensure_device_mapping_auxiliary_tables(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> None:
    _ensure_delta_table(config.embedding_cache_table, EMBEDDING_CACHE_SCHEMA)
    _ensure_delta_table(config.embedding_match_cache_table, EMBEDDING_MATCH_CACHE_SCHEMA)
    _ensure_delta_table(config.anchor_evidence_table, ANCHOR_EVIDENCE_SCHEMA)
    _ensure_delta_table(config.anchor_lookup_table, ANCHOR_LOOKUP_SCHEMA)
    ensure_checkpoint_table(config)


def _embedding_cache_key(text_column, text_type: str, config: DeviceMappingConfig):
    return F.sha2(
        F.concat_ws(
            "||",
            text_column,
            F.lit(text_type),
            F.lit(config.embedding_model),
            F.lit(config.normalization_version),
        ),
        256,
    )


_LEGACY_EMBEDDING_CACHE_MIGRATED = False


def migrate_legacy_embedding_cache(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> int:
    """Reuse exact-text vectors created by the previous production device pipeline."""
    global _LEGACY_EMBEDDING_CACHE_MIGRATED
    if _LEGACY_EMBEDDING_CACHE_MIGRATED:
        return 0
    if not _table_exists(config.legacy_embedding_cache_table):
        _LEGACY_EMBEDDING_CACHE_MIGRATED = True
        return 0

    legacy = (
        spark.table(config.legacy_embedding_cache_table)
        .where(F.col("text_type").isin("gmdn", "layer5"))
        .where(F.col("text").isNotNull() & (F.trim(F.col("text")) != ""))
        .where(F.col("embedding").isNotNull() & (F.size(F.col("embedding")) == 3072))
        .select(
            F.trim(F.col("text")).alias("TEXT"),
            F.when(F.col("text_type") == "gmdn", F.lit("gmdn"))
            .otherwise(F.lit("implant_description"))
            .alias("TEXT_TYPE"),
            F.col("embedding").cast("array<double>").alias("EMBEDDING"),
            F.col("created_at").cast("timestamp").alias("LEGACY_CREATED_AT"),
        )
        .withColumn("EMBEDDING_MODEL", F.lit(config.embedding_model))
        .withColumn("NORMALIZATION_VERSION", F.lit(config.normalization_version))
        .withColumn(
            "CACHE_KEY",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("TEXT"),
                    F.col("TEXT_TYPE"),
                    F.col("EMBEDDING_MODEL"),
                    F.col("NORMALIZATION_VERSION"),
                ),
                256,
            ),
        )
        .withColumn(
            "_LEGACY_RANK",
            F.row_number().over(
                Window.partitionBy("CACHE_KEY").orderBy(F.col("LEGACY_CREATED_AT").desc_nulls_last())
            ),
        )
        .where(F.col("_LEGACY_RANK") == 1)
        .select(
            "CACHE_KEY",
            "TEXT",
            "TEXT_TYPE",
            "EMBEDDING_MODEL",
            "NORMALIZATION_VERSION",
            "EMBEDDING",
            F.coalesce(F.col("LEGACY_CREATED_AT"), F.current_timestamp()).alias("CREATED_AT"),
            F.current_timestamp().alias("UPDATED_AT"),
        )
    )
    legacy = legacy.join(
        spark.table(config.embedding_cache_table).select("CACHE_KEY"),
        "CACHE_KEY",
        "left_anti",
    )
    reusable_count = legacy.count()
    if reusable_count:
        assignments = {
            field.name: f"source.{field.name}"
            for field in EMBEDDING_CACHE_SCHEMA.fields
        }
        (
            DeltaTable.forName(spark, config.embedding_cache_table)
            .alias("target")
            .merge(legacy.alias("source"), "target.CACHE_KEY = source.CACHE_KEY")
            .whenNotMatchedInsert(values=assignments)
            .execute()
        )
        print(
            f"[EMBEDDING] migrated {reusable_count:,} exact-text vectors from "
            f"{config.legacy_embedding_cache_table}; legacy match decisions were not copied."
        )
    _LEGACY_EMBEDDING_CACHE_MIGRATED = True
    return reusable_count


def _get_embedding_client(config: DeviceMappingConfig):
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RuntimeError(
            "The openai package is required by the embedding fallback. Install it as a "
            "job environment dependency rather than with a notebook-scoped pip restart."
        ) from exc
    token = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .apiToken()
        .get()
    )
    return OpenAI(api_key=token, base_url=config.embedding_base_url)


def _merge_embedding_rows(
    rows: List[tuple],
    config: DeviceMappingConfig,
) -> None:
    if not rows:
        return
    updates = spark.createDataFrame(rows, EMBEDDING_CACHE_SCHEMA)
    assignments = {field.name: f"source.{field.name}" for field in EMBEDDING_CACHE_SCHEMA.fields}
    (
        DeltaTable.forName(spark, config.embedding_cache_table)
        .alias("target")
        .merge(updates.alias("source"), "target.CACHE_KEY = source.CACHE_KEY")
        .whenMatchedUpdate(set=assignments)
        .whenNotMatchedInsert(values=assignments)
        .execute()
    )


def get_or_create_embeddings(
    texts: DataFrame,
    text_column: str,
    text_type: str,
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> DataFrame:
    ensure_device_mapping_auxiliary_tables(config)
    migrate_legacy_embedding_cache(config)
    keys = (
        texts.select(F.trim(F.col(text_column)).alias("TEXT"))
        .where(F.col("TEXT").isNotNull() & (F.col("TEXT") != ""))
        .distinct()
        .withColumn("TEXT_TYPE", F.lit(text_type))
        .withColumn("EMBEDDING_MODEL", F.lit(config.embedding_model))
        .withColumn("NORMALIZATION_VERSION", F.lit(config.normalization_version))
        .withColumn("CACHE_KEY", _embedding_cache_key(F.col("TEXT"), text_type, config))
    )
    existing = spark.table(config.embedding_cache_table).select("CACHE_KEY")
    missing = keys.join(existing, "CACHE_KEY", "left_anti")

    if not _is_empty(missing):
        import random
        import time

        client = _get_embedding_client(config)
        pending: List[tuple] = []
        batch: List[Tuple[str, str]] = []

        def flush_pending() -> None:
            nonlocal pending
            if not pending:
                return
            rows = pending
            pending = []
            _merge_embedding_rows(rows, config)

        def is_capacity_error(error: Exception) -> bool:
            message = str(error).lower()
            return any(marker in message for marker in (
                "429",
                "request_limit_exceeded",
                "nocapacity",
                "no capacity",
                "high demand",
                "rate limit",
                "maximum usage size",
            ))

        def retry_delay(error: Exception, attempt: int, capacity_error: bool) -> float:
            response = getattr(error, "response", None)
            headers = getattr(response, "headers", None)
            if headers:
                retry_after = headers.get("retry-after") or headers.get("Retry-After")
                try:
                    return min(float(retry_after), float(config.embedding_retry_max_seconds))
                except (TypeError, ValueError):
                    pass
            base = 5.0 if capacity_error else 2.0
            maximum = float(config.embedding_retry_max_seconds if capacity_error else 10)
            return min(maximum, base * (2 ** max(attempt - 1, 0))) + random.random()

        def process_batch(items: List[Tuple[str, str]]) -> None:
            nonlocal pending
            if not items:
                return
            cache_keys = [item[0] for item in items]
            values = [item[1] for item in items]
            last_error = None
            attempt = 0
            while True:
                try:
                    response = client.embeddings.create(
                        model=config.embedding_model,
                        input=values,
                        encoding_format="float",
                    )
                    ordered = sorted(response.data, key=lambda item: item.index)
                    if len(ordered) != len(values):
                        raise RuntimeError("Embedding endpoint returned an unexpected result count")
                    now = datetime.now(timezone.utc)
                    for cache_key, value, item in zip(cache_keys, values, ordered):
                        pending.append(
                            (
                                cache_key,
                                value,
                                text_type,
                                config.embedding_model,
                                config.normalization_version,
                                [float(number) for number in item.embedding],
                                now,
                                now,
                            )
                        )
                    if len(pending) >= config.embedding_flush_size:
                        flush_pending()
                    return
                except Exception as exc:
                    last_error = exc
                    capacity_error = is_capacity_error(exc)
                    if capacity_error and len(items) > 1:
                        midpoint = len(items) // 2
                        print(
                            f"[EMBEDDING] endpoint capacity limited; splitting batch "
                            f"{len(items)} into {midpoint} and {len(items) - midpoint}."
                        )
                        process_batch(items[:midpoint])
                        process_batch(items[midpoint:])
                        return
                    attempt += 1
                    max_attempts = config.embedding_capacity_retries if capacity_error else 3
                    if attempt >= max_attempts:
                        flush_pending()
                        raise RuntimeError(
                            "Embedding endpoint failed after retries; completed batches were "
                            f"saved and the next run will resume from the cache. Last error: {last_error}"
                        ) from exc
                    delay = retry_delay(exc, attempt, capacity_error)
                    print(
                        f"[EMBEDDING] retry {attempt + 1}/{max_attempts} for batch_size={len(items)} "
                        f"after {delay:.1f}s: {str(exc).splitlines()[0][:300]}"
                    )
                    time.sleep(delay)

        for row in missing.select("CACHE_KEY", "TEXT").toLocalIterator():
            batch.append((row.CACHE_KEY, row.TEXT))
            if len(batch) >= config.embedding_api_batch_size:
                process_batch(batch)
                batch = []
        process_batch(batch)
        flush_pending()

    cache = spark.table(config.embedding_cache_table).select(
        "CACHE_KEY",
        "TEXT",
        "TEXT_TYPE",
        "EMBEDDING_MODEL",
        "NORMALIZATION_VERSION",
        "EMBEDDING",
    )
    return keys.join(cache, ["CACHE_KEY", "TEXT", "TEXT_TYPE", "EMBEDDING_MODEL", "NORMALIZATION_VERSION"], "inner")


GMDN_EXCLUSIONS = {
    "lens spoon",
    "lens forceps",
    "vascular graft tunneller",
    "pre-prescription spectacle lens",
    "manual intraocular lens injector",
    "cardiac pulse generator programmer",
    "intraocular lens folder",
    "intraocular lens calculation software",
    "intraocular lens implantation cord",
}

MATERIAL_CONFLICTS = {
    "polyethylene": ["ceramic"],
    "titanium": ["ceramic"],
    "metal": ["ceramic"],
    "cocrmo": ["ceramic"],
    "cobalt": ["ceramic"],
    "stainless": ["ceramic"],
}

BODY_PART_CONFLICTS = {
    "tibial": ["ankle", "humeral", "shoulder"],
    "knee": ["ankle", "shoulder", "hip"],
    "hip": ["ankle", "shoulder", "knee", "humeral"],
    "acetabular": ["humeral", "shoulder", "glenoid"],
    "femoral": ["humeral", "shoulder", "glenoid"],
    "radius": ["intrauterine", "contraceptive"],
    "radial": ["intrauterine", "contraceptive"],
    "biolox": ["intraocular"],
}

COPPER_IUD_PATTERN = r"\b(t\s*safe|tsafe|flexi\s*t|tcu\s*380a?|tcu380a?|cu\s*380a?|copper\s+(coil|t|iud|device)|intra\s*uterine.*copper|intrauterine.*copper)\b"


def _valid_embedding_match(description: str, gmdn_name: str) -> bool:
    if not description or not gmdn_name:
        return False
    description_lower = description.lower()
    gmdn_lower = gmdn_name.lower()
    if re.search(COPPER_IUD_PATTERN, description_lower) and "medicated" in gmdn_lower:
        return False
    if any(excluded in gmdn_lower for excluded in GMDN_EXCLUSIONS):
        return False
    for source_term, forbidden_terms in MATERIAL_CONFLICTS.items():
        if source_term in description_lower and any(term in gmdn_lower for term in forbidden_terms):
            return False
    for source_term, forbidden_terms in BODY_PART_CONFLICTS.items():
        if source_term in description_lower and any(term in gmdn_lower for term in forbidden_terms):
            return False
    return True


def _valid_embedding_match_condition(description, gmdn_name):
    description_lower = F.lower(description)
    gmdn_lower = F.lower(gmdn_name)
    valid = description.isNotNull() & gmdn_name.isNotNull()
    valid = valid & ~(
        description_lower.rlike(COPPER_IUD_PATTERN)
        & gmdn_lower.contains("medicated")
    )
    for excluded in GMDN_EXCLUSIONS:
        valid = valid & ~gmdn_lower.contains(excluded)
    for source_term, forbidden_terms in MATERIAL_CONFLICTS.items():
        conflict = F.lit(False)
        for forbidden_term in forbidden_terms:
            conflict = conflict | gmdn_lower.contains(forbidden_term)
        valid = valid & ~(description_lower.contains(source_term) & conflict)
    for source_term, forbidden_terms in BODY_PART_CONFLICTS.items():
        conflict = F.lit(False)
        for forbidden_term in forbidden_terms:
            conflict = conflict | gmdn_lower.contains(forbidden_term)
        valid = valid & ~(description_lower.contains(source_term) & conflict)
    return valid


def _embedding_match_key(
    cleaned_description,
    procedure_context,
    gmdn_reference_version: int,
    config: DeviceMappingConfig,
):
    return F.sha2(
        F.concat_ws(
            "||",
            cleaned_description,
            procedure_context,
            F.lit(config.embedding_model),
            F.lit(config.normalization_version),
            F.lit(config.brand_rules_version),
            F.lit(str(gmdn_reference_version)),
        ),
        256,
    )


def _merge_embedding_match_rows(
    rows: List[tuple],
    config: DeviceMappingConfig,
) -> None:
    if not rows:
        return
    updates = spark.createDataFrame(rows, EMBEDDING_MATCH_CACHE_SCHEMA)
    _merge_embedding_match_dataframe(updates, config)


def _merge_embedding_match_dataframe(
    updates: DataFrame,
    config: DeviceMappingConfig,
    update_existing: bool = True,
) -> None:
    assignments = {
        field.name: f"source.{field.name}"
        for field in EMBEDDING_MATCH_CACHE_SCHEMA.fields
    }
    merge = (
        DeltaTable.forName(spark, config.embedding_match_cache_table)
        .alias("target")
        .merge(updates.alias("source"), "target.CACHE_KEY = source.CACHE_KEY")
    )
    if update_existing:
        merge = merge.whenMatchedUpdate(set=assignments)
    merge.whenNotMatchedInsert(values=assignments).execute()


def seed_legacy_embedding_match_cache(
    contexts: DataFrame,
    gmdn_reference_version: int,
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> int:
    """Revalidate legacy nearest-neighbour results under current rules and context."""
    if not _table_exists(config.legacy_embedding_match_cache_table):
        return 0

    legacy_window = Window.partitionBy("cleaned_desc").orderBy(
        F.col("created_at").desc_nulls_last(),
        F.col("similarity_score").desc_nulls_last(),
        F.col("gmdncode").asc_nulls_last(),
    )
    legacy = (
        spark.table(config.legacy_embedding_match_cache_table)
        .where(F.col("match_status").isin("matched", "matched_boosted", "below_threshold"))
        .where(F.col("cleaned_desc").isNotNull())
        .where(F.col("gmdncode").isNotNull() & F.col("gmdn_name").isNotNull())
        .withColumn("_LEGACY_RANK", F.row_number().over(legacy_window))
        .where(F.col("_LEGACY_RANK") == 1)
        .select(
            F.col("cleaned_desc").alias("CLEANED_DESCRIPTION"),
            F.col("gmdncode").cast("long").alias("LEGACY_GMDNCODE"),
            F.col("gmdn_name").cast("string").alias("LEGACY_GMDN_NAME"),
            F.col("similarity_score").cast("double").alias("LEGACY_SIMILARITY_SCORE"),
            F.col("created_at").cast("timestamp").alias("LEGACY_CREATED_AT"),
        )
    )
    joined = (
        contexts.join(F.broadcast(legacy), "CLEANED_DESCRIPTION", "inner")
        .where(
            _valid_embedding_match_condition(
                F.col("CLEANED_DESCRIPTION"),
                F.col("LEGACY_GMDN_NAME"),
            )
        )
    )
    procedure_support = F.coalesce(
        F.exists(
            F.col("PROCEDURE_KEYWORDS"),
            lambda keyword: F.instr(F.lower(F.col("LEGACY_GMDN_NAME")), F.lower(keyword)) > 0,
        ),
        F.lit(False),
    )
    threshold = F.when(
        procedure_support,
        F.lit(config.procedure_boosted_threshold),
    ).otherwise(F.lit(config.default_embedding_threshold))
    status = (
        F.when(
            F.col("LEGACY_SIMILARITY_SCORE") >= threshold,
            F.when(procedure_support, F.lit("MATCHED_BOOSTED")).otherwise(F.lit("MATCHED")),
        )
        .otherwise(F.lit("BELOW_THRESHOLD"))
    )
    updates = joined.select(
        "CACHE_KEY",
        "CLEANED_DESCRIPTION",
        "PROCEDURE_CONTEXT_KEY",
        F.col("LEGACY_GMDNCODE").alias("gmdncode"),
        F.col("LEGACY_GMDN_NAME").alias("gmdn_name"),
        F.col("LEGACY_SIMILARITY_SCORE").alias("SIMILARITY_SCORE"),
        status.alias("MATCH_STATUS"),
        procedure_support.alias("PROCEDURE_SUPPORT_IND"),
        F.lit(config.embedding_model).alias("EMBEDDING_MODEL"),
        F.lit(config.normalization_version).alias("NORMALIZATION_VERSION"),
        F.lit(config.brand_rules_version).alias("BRAND_RULES_VERSION"),
        F.lit(int(gmdn_reference_version)).cast("long").alias("GMDN_REFERENCE_VERSION"),
        F.coalesce(F.col("LEGACY_CREATED_AT"), F.current_timestamp()).alias("CREATED_AT"),
        F.current_timestamp().alias("UPDATED_AT"),
    )
    updates = updates.join(
        spark.table(config.embedding_match_cache_table).select("CACHE_KEY"),
        "CACHE_KEY",
        "left_anti",
    )
    seeded_count = updates.count()
    if seeded_count:
        _merge_embedding_match_dataframe(updates, config, update_existing=False)
        print(
            f"[EMBEDDING] revalidated {seeded_count:,} legacy match contexts; "
            "current false-positive and procedure-threshold rules were applied."
        )
    return seeded_count


def _compute_embedding_match_rows(
    description_rows: DataFrame,
    gmdn_rows: DataFrame,
    gmdn_reference_version: int,
    config: DeviceMappingConfig,
) -> List[tuple]:
    import numpy as np

    gmdn_local = list(
        gmdn_rows.select("gmdncode", "gmdn_name", "EMBEDDING")
        .where(F.col("EMBEDDING").isNotNull())
        .orderBy("gmdncode")
        .toLocalIterator()
    )
    if not gmdn_local:
        raise RuntimeError("No GMDN embeddings are available")

    codes = np.asarray([int(row.gmdncode) for row in gmdn_local], dtype=np.int64)
    names = [row.gmdn_name for row in gmdn_local]
    matrix = np.asarray([row.EMBEDDING for row in gmdn_local], dtype=np.float32)
    norms = np.linalg.norm(matrix, axis=1)
    valid = norms > 0
    codes = codes[valid]
    names = [name for name, keep in zip(names, valid.tolist()) if keep]
    matrix = matrix[valid] / norms[valid, None]
    if matrix.shape[0] == 0:
        raise RuntimeError("All cached GMDN embeddings have zero norm")

    results: List[tuple] = []
    batch: List[object] = []
    now = datetime.now(timezone.utc)

    def process_batch(rows: List[object]) -> None:
        if not rows:
            return
        usable = []
        missing = []
        for row in rows:
            if row.EMBEDDING is not None and len(row.EMBEDDING) == matrix.shape[1]:
                usable.append(row)
            else:
                missing.append(row)
        for row in missing:
            results.append(
                (
                    row.CACHE_KEY,
                    row.CLEANED_DESCRIPTION,
                    row.PROCEDURE_CONTEXT_KEY,
                    None,
                    None,
                    0.0,
                    "NO_EMBEDDING",
                    False,
                    config.embedding_model,
                    config.normalization_version,
                    config.brand_rules_version,
                    int(gmdn_reference_version),
                    now,
                    now,
                )
            )
        if not usable:
            return
        vectors = np.asarray([row.EMBEDDING for row in usable], dtype=np.float32)
        vector_norms = np.linalg.norm(vectors, axis=1)
        nonzero = vector_norms > 0
        vectors[nonzero] = vectors[nonzero] / vector_norms[nonzero, None]
        similarities = vectors @ matrix.T
        top_k = min(config.similarity_top_k, matrix.shape[0])
        if top_k == matrix.shape[0]:
            candidate_indices = np.argsort(similarities, axis=1)[:, ::-1]
        else:
            unsorted = np.argpartition(similarities, -top_k, axis=1)[:, -top_k:]
            partial_scores = np.take_along_axis(similarities, unsorted, axis=1)
            order = np.argsort(partial_scores, axis=1)[:, ::-1]
            candidate_indices = np.take_along_axis(unsorted, order, axis=1)

        for position, row in enumerate(usable):
            description = row.CLEANED_DESCRIPTION or ""
            keywords = set(row.PROCEDURE_KEYWORDS or [])
            selected = None
            for candidate_index in candidate_indices[position]:
                name = names[int(candidate_index)]
                if _valid_embedding_match(description, name):
                    score = float(similarities[position, int(candidate_index)])
                    selected = (int(codes[int(candidate_index)]), name, score)
                    break
            if selected is None:
                code = None
                name = None
                score = 0.0
                status = "REJECTED_FALSE_POSITIVE"
                procedure_support = False
            else:
                code, name, score = selected
                name_lower = name.lower()
                procedure_support = any(keyword in name_lower for keyword in keywords)
                threshold = (
                    config.procedure_boosted_threshold
                    if procedure_support
                    else config.default_embedding_threshold
                )
                status = "MATCHED_BOOSTED" if procedure_support and score >= threshold else (
                    "MATCHED" if score >= threshold else "BELOW_THRESHOLD"
                )
            results.append(
                (
                    row.CACHE_KEY,
                    row.CLEANED_DESCRIPTION,
                    row.PROCEDURE_CONTEXT_KEY,
                    code,
                    name,
                    float(score),
                    status,
                    bool(procedure_support),
                    config.embedding_model,
                    config.normalization_version,
                    config.brand_rules_version,
                    int(gmdn_reference_version),
                    now,
                    now,
                )
            )

    for row in description_rows.select(
        "CACHE_KEY",
        "CLEANED_DESCRIPTION",
        "PROCEDURE_CONTEXT_KEY",
        "PROCEDURE_KEYWORDS",
        "EMBEDDING",
    ).toLocalIterator():
        batch.append(row)
        if len(batch) >= config.similarity_batch_size:
            process_batch(batch)
            batch = []
    process_batch(batch)
    return results


def build_embedding_candidates(
    source: DataFrame,
    high_evidence_event_ids: Optional[DataFrame],
    gmdn_reference: DataFrame,
    gmdn_reference_version: int,
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> DataFrame:
    ensure_device_mapping_auxiliary_tables(config)
    eligible = source
    if high_evidence_event_ids is not None:
        eligible = eligible.join(
            high_evidence_event_ids.select("EVENT_ID").distinct(),
            "EVENT_ID",
            "left_anti",
        )
    eligible = eligible.where(
        F.col("CLEANED_DESCRIPTION").isNotNull()
        & (F.length(F.col("CLEANED_DESCRIPTION")) >= 3)
    )
    contexts = (
        eligible.select(
            "CLEANED_DESCRIPTION",
            "PROCEDURE_CONTEXT_KEY",
            "PROCEDURE_KEYWORDS",
        )
        .dropDuplicates(["CLEANED_DESCRIPTION", "PROCEDURE_CONTEXT_KEY"])
        .withColumn(
            "CACHE_KEY",
            _embedding_match_key(
                F.col("CLEANED_DESCRIPTION"),
                F.col("PROCEDURE_CONTEXT_KEY"),
                gmdn_reference_version,
                config,
            ),
        )
    )
    seed_legacy_embedding_match_cache(
        contexts,
        gmdn_reference_version,
        config,
    )
    cache = spark.table(config.embedding_match_cache_table).select(
        "CACHE_KEY",
        "gmdncode",
        "gmdn_name",
        "SIMILARITY_SCORE",
        "MATCH_STATUS",
        "PROCEDURE_SUPPORT_IND",
    )
    missing_contexts = contexts.join(cache.select("CACHE_KEY"), "CACHE_KEY", "left_anti")

    if not _is_empty(missing_contexts):
        description_embeddings = get_or_create_embeddings(
            missing_contexts,
            "CLEANED_DESCRIPTION",
            "implant_description",
            config,
        ).select(
            F.col("TEXT").alias("CLEANED_DESCRIPTION"),
            F.col("EMBEDDING").alias("DESCRIPTION_EMBEDDING"),
        )
        gmdn_terms = gmdn_reference.select("gmdncode", "gmdn_name").where(
            F.col("gmdn_name").isNotNull() & (F.trim(F.col("gmdn_name")) != "")
        )
        gmdn_embeddings = get_or_create_embeddings(
            gmdn_terms,
            "gmdn_name",
            "gmdn",
            config,
        ).select(
            F.col("TEXT").alias("gmdn_name"),
            F.col("EMBEDDING"),
        )
        descriptions_local = (
            missing_contexts
            .join(description_embeddings, "CLEANED_DESCRIPTION", "left")
            .withColumnRenamed("DESCRIPTION_EMBEDDING", "EMBEDDING")
        )
        gmdn_local = gmdn_terms.join(gmdn_embeddings, "gmdn_name", "inner")
        match_rows = _compute_embedding_match_rows(
            descriptions_local,
            gmdn_local,
            gmdn_reference_version,
            config,
        )
        _merge_embedding_match_rows(match_rows, config)

    matches = (
        contexts
        .join(
            spark.table(config.embedding_match_cache_table).select(
                "CACHE_KEY",
                "gmdncode",
                "gmdn_name",
                "SIMILARITY_SCORE",
                "MATCH_STATUS",
                "PROCEDURE_SUPPORT_IND",
            ),
            "CACHE_KEY",
            "inner",
        )
        .where(F.col("MATCH_STATUS").isin("MATCHED", "MATCHED_BOOSTED"))
    )
    joined = eligible.join(
        matches,
        ["CLEANED_DESCRIPTION", "PROCEDURE_CONTEXT_KEY"],
        "inner",
    )
    result = joined.select(
        "EVENT_ID",
        "ENCNTR_ID",
        "PERSON_ID",
        "IMPLANT_DESCRIPTION",
        "NORMALIZED_DESCRIPTION",
        "CLEANED_DESCRIPTION",
        "gmdncode",
        F.col("gmdn_name").alias("GMDN_NAME_HINT"),
        F.lit(None).cast("long").alias("STANDARD_CONCEPT_ID_HINT"),
        F.lit(None).cast("string").alias("STANDARD_CONCEPT_NAME_HINT"),
        F.lit(None).cast("string").alias("STANDARD_VOCABULARY_HINT"),
        F.lit(None).cast("string").alias("DEVICE_TYPE_HINT"),
        F.lit("LAYER5_GMDN_EMBEDDING").alias("MAPPING_LAYER"),
        F.col("SIMILARITY_SCORE").cast("double").alias("MAPPING_CONFIDENCE"),
        F.lit(60).alias("EVIDENCE_PRIORITY"),
        F.when(F.col("MATCH_STATUS") == "MATCHED_BOOSTED", F.lit("GMDN_EMBEDDING_PROCEDURE_SUPPORTED"))
        .otherwise(F.lit("GMDN_EMBEDDING")).alias("MAPPING_RULE_ID"),
        F.lit("CLEANED_DESCRIPTION+PROCEDURE_CONTEXT").alias("MATCHED_FIELD"),
        F.col("CLEANED_DESCRIPTION").alias("MATCHED_VALUE"),
        F.lit(1).cast("int").alias("REFERENCE_CANDIDATE_COUNT"),
        F.lit(False).alias("MAPPING_AMBIGUOUS_IND"),
        F.col("PROCEDURE_SUPPORT_IND"),
        F.lit(None).cast("string").alias("MATCHED_OPCS_CODE"),
        F.lit(None).cast("string").alias("GUDID_PRIMARY_DI"),
        F.lit(None).cast("string").alias("GUDID_BRAND_NAME"),
        F.lit(None).cast("string").alias("GUDID_COMPANY_NAME"),
        F.lit(None).cast("string").alias("GUDID_MODEL_NUMBER"),
        F.lit(None).cast("string").alias("GUDID_CATALOGUE_NUMBER"),
        F.lit(None).cast("string").alias("GUDID_DEVICE_STATUS"),
        F.lit(None).cast("string").alias("GUDID_DISTRIBUTION_STATUS"),
    )
    return _select_raw_candidate(result)

# COMMAND ----------


DEVICE_NOUN_PATTERN = (
    r"\b(stent|valve|pacemaker|defibrillator|catheter|graft|shunt|occluder|"
    r"filter|coil|lead|generator|pump|balloon|prosthesis|implant|plate|screw|"
    r"nail|rod|pin|wire|cage|spacer|cup|stem|head|liner|insert|anchor|fixator|"
    r"staple|clip|mesh|patch|sling|suture|plug|lens|ring|electrode|stimulator|"
    r"port|drain|tube|cannula|bar|cement|envelope)\b"
)


def enrich_and_rank_candidates(
    raw_candidates: DataFrame,
    gmdn_reference: DataFrame,
    standard_lookup: DataFrame,
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
    source_versions: Optional[Dict[str, int]] = None,
) -> DataFrame:
    if _is_empty(raw_candidates):
        return spark.createDataFrame([], schema_device_mapping_candidate_v2)

    candidate = raw_candidates.alias("candidate")
    gmdn = gmdn_reference.select(
        F.col("gmdncode").alias("REF_GMDNCODE"),
        F.col("gmdn_name").alias("REF_GMDN_NAME"),
        F.col("GMDN_DEFINITION").alias("REF_GMDN_DEFINITION"),
        F.col("GMDN_CODE_STATUS").alias("REF_GMDN_CODE_STATUS"),
        F.col("GMDN_IMPLANTABLE").alias("REF_GMDN_IMPLANTABLE"),
    ).alias("gmdn")
    standard = standard_lookup.select(
        F.col("gmdncode").alias("STD_GMDNCODE"),
        F.col("STANDARD_CONCEPT_ID").alias("LOOKUP_STANDARD_CONCEPT_ID"),
        F.col("STANDARD_CONCEPT_NAME").alias("LOOKUP_STANDARD_CONCEPT_NAME"),
        F.col("STANDARD_VOCABULARY_ID").alias("LOOKUP_STANDARD_VOCABULARY_ID"),
    ).alias("standard")

    enriched = (
        candidate
        .join(gmdn, F.col("candidate.gmdncode") == F.col("gmdn.REF_GMDNCODE"), "left")
        .join(standard, F.col("candidate.gmdncode") == F.col("standard.STD_GMDNCODE"), "left")
        .withColumn(
            "_PROVISIONAL_STANDARD_CONCEPT_ID",
            F.coalesce(
                F.col("STANDARD_CONCEPT_ID_HINT"),
                F.col("standard.LOOKUP_STANDARD_CONCEPT_ID"),
            ),
        )
    )

    concept = (
        _read_snapshot(config.concept_source, source_versions)
        .select(
            F.col("concept_id").cast("long").alias("CONCEPT_ID"),
            F.col("concept_name").cast("string").alias("CONCEPT_NAME"),
            F.col("vocabulary_id").cast("string").alias("CONCEPT_VOCABULARY_ID"),
            F.col("domain_id").cast("string").alias("CONCEPT_DOMAIN_ID"),
            F.col("standard_concept").cast("string").alias("CONCEPT_STANDARD_FLAG"),
            F.col("invalid_reason").cast("string").alias("CONCEPT_INVALID_REASON"),
        )
        .alias("concept")
    )
    enriched = enriched.join(
        concept,
        F.col("_PROVISIONAL_STANDARD_CONCEPT_ID") == F.col("concept.CONCEPT_ID"),
        "left",
    )
    valid_standard = (
        (F.col("CONCEPT_DOMAIN_ID") == "Device")
        & (F.col("CONCEPT_STANDARD_FLAG") == "S")
        & F.col("CONCEPT_INVALID_REASON").isNull()
    )

    enriched = (
        enriched
        .withColumn(
            "gmdn_name",
            F.coalesce(F.col("REF_GMDN_NAME"), F.col("GMDN_NAME_HINT")),
        )
        .withColumn("GMDN_DEFINITION", F.col("REF_GMDN_DEFINITION"))
        .withColumn("GMDN_CODE_STATUS", F.col("REF_GMDN_CODE_STATUS"))
        .withColumn("GMDN_IMPLANTABLE", F.col("REF_GMDN_IMPLANTABLE"))
        .withColumn(
            "GMDN_REFERENCE_PRESENT_IND",
            F.col("REF_GMDNCODE").isNotNull(),
        )
        .withColumn(
            "GMDN_MAPPING_PROVENANCE",
            F.when(F.col("gmdncode").isNull(), F.lit("DIRECT_STANDARD_ONLY"))
            .when(F.col("REF_GMDNCODE").isNotNull(), F.lit("REFERENCE"))
            .otherwise(F.lit("MANUAL_RULE")),
        )
        .withColumn(
            "STANDARD_CONCEPT_ID",
            F.when(valid_standard, F.col("_PROVISIONAL_STANDARD_CONCEPT_ID"))
            .otherwise(F.lit(None).cast("long")),
        )
        .withColumn(
            "STANDARD_CONCEPT_NAME",
            F.when(
                valid_standard,
                F.coalesce(
                    F.col("CONCEPT_NAME"),
                    F.col("STANDARD_CONCEPT_NAME_HINT"),
                    F.col("standard.LOOKUP_STANDARD_CONCEPT_NAME"),
                ),
            ).otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "STANDARD_VOCABULARY_ID",
            F.when(valid_standard, F.col("CONCEPT_VOCABULARY_ID"))
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "snomed_concept_id",
            F.when(
                F.col("STANDARD_VOCABULARY_ID") == "SNOMED",
                F.col("STANDARD_CONCEPT_ID"),
            ).otherwise(F.lit(None).cast("long")),
        )
        .withColumn(
            "snomed_name",
            F.when(
                F.col("STANDARD_VOCABULARY_ID") == "SNOMED",
                F.col("STANDARD_CONCEPT_NAME"),
            ).otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "_EXTRACTED_DEVICE_TYPE",
            F.regexp_extract(
                F.lower(
                    F.coalesce(
                        F.col("DEVICE_TYPE_HINT"),
                        F.col("gmdn_name"),
                        F.col("STANDARD_CONCEPT_NAME"),
                        F.lit(""),
                    )
                ),
                DEVICE_NOUN_PATTERN,
                1,
            ),
        )
        .withColumn(
            "device_type",
            F.coalesce(
                _blank_to_null(F.col("DEVICE_TYPE_HINT")),
                _blank_to_null(F.col("_EXTRACTED_DEVICE_TYPE")),
            ),
        )
    )

    dedupe_window = Window.partitionBy(
        "EVENT_ID",
        "MAPPING_LAYER",
        "MAPPING_RULE_ID",
        "gmdncode",
        "STANDARD_CONCEPT_ID",
        "MATCHED_VALUE",
    ).orderBy(
        F.col("MAPPING_CONFIDENCE").desc(),
        F.col("GUDID_PRIMARY_DI").asc_nulls_last(),
    )
    invalid_copper_medicated = (
        F.lower(F.col("NORMALIZED_DESCRIPTION")).rlike(COPPER_IUD_PATTERN)
        & (F.col("gmdncode") == F.lit(41888))
    )
    deduped = (
        enriched.where(~F.coalesce(invalid_copper_medicated, F.lit(False)))
        .withColumn("_DUPLICATE_RANK", F.row_number().over(dedupe_window))
        .where(F.col("_DUPLICATE_RANK") == 1)
        .drop("_DUPLICATE_RANK")
    )

    concept_tuple = F.concat_ws(
        "|",
        F.coalesce(F.col("gmdncode").cast("string"), F.lit("NULL")),
        F.coalesce(F.col("STANDARD_CONCEPT_ID").cast("string"), F.lit("NULL")),
    )
    counts = deduped.groupBy("EVENT_ID").agg(
        F.count("*").cast("int").alias("MAPPING_CANDIDATE_COUNT"),
        F.countDistinct(concept_tuple).cast("int").alias("MAPPING_DISTINCT_CONCEPT_COUNT"),
    )
    rank_window = Window.partitionBy("EVENT_ID").orderBy(
        F.col("EVIDENCE_PRIORITY").asc(),
        F.col("MAPPING_CONFIDENCE").desc(),
        F.col("PROCEDURE_SUPPORT_IND").desc(),
        F.col("STANDARD_CONCEPT_ID").isNotNull().desc(),
        F.col("GMDN_REFERENCE_PRESENT_IND").desc(),
        F.col("MAPPING_RULE_ID").asc(),
        F.col("gmdncode").asc_nulls_last(),
        F.col("STANDARD_CONCEPT_ID").asc_nulls_last(),
    )

    ranked = (
        deduped
        .join(counts, "EVENT_ID", "inner")
        .withColumn("CANDIDATE_RANK", F.row_number().over(rank_window).cast("int"))
        .withColumn(
            "MAPPING_AMBIGUOUS_IND",
            F.coalesce(F.col("MAPPING_AMBIGUOUS_IND"), F.lit(False))
            | (F.col("MAPPING_DISTINCT_CONCEPT_COUNT") > 1),
        )
        .withColumn(
            "CANDIDATE_ID",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("EVENT_ID").cast("string"),
                    F.col("MAPPING_LAYER"),
                    F.col("MAPPING_RULE_ID"),
                    F.coalesce(F.col("gmdncode").cast("string"), F.lit("NULL")),
                    F.coalesce(F.col("STANDARD_CONCEPT_ID").cast("string"), F.lit("NULL")),
                    F.coalesce(F.col("MATCHED_VALUE"), F.lit("NULL")),
                ),
                256,
            ),
        )
        .withColumn("MAPPING_SCHEMA_VERSION", F.lit(config.mapping_schema_version))
        .withColumn("CREATED_AT", F.current_timestamp())
    )
    return _align_to_schema(ranked, schema_device_mapping_candidate_v2)


TARGET_HASH_EXCLUSIONS = {"MAPPED_AT", "PIPELINE_LOADED_AT", "ROW_HASH"}


def build_final_mapping(
    source: DataFrame,
    ranked_candidates: DataFrame,
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> DataFrame:
    winner = ranked_candidates.where(F.col("CANDIDATE_RANK") == 1)
    result = (
        source.alias("source")
        .join(winner.alias("winner"), "EVENT_ID", "left")
        .select(
            F.col("EVENT_ID"),
            F.col("source.ENCNTR_ID"),
            F.col("source.PERSON_ID"),
            F.col("source.IMPLANT_DESCRIPTION"),
            F.col("source.NORMALIZED_DESCRIPTION"),
            F.col("source.CLEANED_DESCRIPTION"),
            F.col("source.SOURCE_PRIMARY_PROCEDURE"),
            F.col("source.SOURCE_PROCEDURE_CAT"),
            F.col("source.SOURCE_CATALOGUE_NUMBER"),
            F.col("source.SOURCE_MANUFACTURER"),
            F.col("source.SOURCE_GS1_IDENTIFIER"),
            F.col("source.SOURCE_HIBCC_DEVICE_ID"),
            F.col("source.UDI_DI"),
            F.col("source.UDI_ISSUER"),
            F.col("source.EFFECTIVE_SERIAL_NUMBER"),
            F.col("source.EFFECTIVE_EXPIRY_DATE"),
            F.col("source.SOURCE_ADC_UPDT"),
            F.col("winner.gmdncode"),
            F.col("winner.gmdn_name"),
            F.col("winner.GMDN_DEFINITION"),
            F.col("winner.GMDN_CODE_STATUS"),
            F.col("winner.GMDN_IMPLANTABLE"),
            F.coalesce(F.col("winner.GMDN_REFERENCE_PRESENT_IND"), F.lit(False))
            .alias("GMDN_REFERENCE_PRESENT_IND"),
            F.col("winner.GMDN_MAPPING_PROVENANCE"),
            F.col("winner.STANDARD_CONCEPT_ID"),
            F.col("winner.STANDARD_CONCEPT_NAME"),
            F.col("winner.STANDARD_VOCABULARY_ID"),
            F.col("winner.snomed_concept_id"),
            F.col("winner.snomed_name"),
            F.col("winner.device_type"),
            F.coalesce(
                F.col("winner.STANDARD_CONCEPT_NAME"),
                F.col("winner.gmdn_name"),
                F.col("winner.device_type"),
                F.col("source.IMPLANT_DESCRIPTION"),
            ).alias("MAPPED_DEVICE_NAME"),
            F.coalesce(F.col("winner.MAPPING_LAYER"), F.lit("UNMAPPED")).alias("mapping_layer"),
            F.coalesce(F.col("winner.MAPPING_CONFIDENCE"), F.lit(0.0)).alias("mapping_confidence"),
            F.when(F.col("winner.CANDIDATE_ID").isNull(), F.lit("UNMAPPED"))
            .when(F.col("winner.MAPPING_CONFIDENCE") >= 0.90, F.lit("HIGH"))
            .when(F.col("winner.MAPPING_CONFIDENCE") >= 0.70, F.lit("MEDIUM"))
            .otherwise(F.lit("LOW"))
            .alias("confidence_tier"),
            F.when(F.col("winner.CANDIDATE_ID").isNull(), F.lit("UNMAPPED"))
            .when(F.col("winner.MAPPING_AMBIGUOUS_IND"), F.lit("AMBIGUOUS_SELECTED"))
            .otherwise(F.lit("MAPPED"))
            .alias("MAPPING_STATUS"),
            F.col("winner.MAPPING_RULE_ID"),
            F.col("winner.MATCHED_FIELD"),
            F.col("winner.MATCHED_VALUE"),
            F.coalesce(F.col("winner.MAPPING_CANDIDATE_COUNT"), F.lit(0)).cast("int")
            .alias("MAPPING_CANDIDATE_COUNT"),
            F.coalesce(F.col("winner.MAPPING_DISTINCT_CONCEPT_COUNT"), F.lit(0)).cast("int")
            .alias("MAPPING_DISTINCT_CONCEPT_COUNT"),
            F.coalesce(F.col("winner.MAPPING_AMBIGUOUS_IND"), F.lit(False))
            .alias("MAPPING_AMBIGUOUS_IND"),
            F.coalesce(F.col("winner.PROCEDURE_SUPPORT_IND"), F.lit(False))
            .alias("PROCEDURE_SUPPORT_IND"),
            F.col("winner.MATCHED_OPCS_CODE"),
            F.col("winner.GUDID_PRIMARY_DI"),
            F.col("winner.GUDID_BRAND_NAME"),
            F.col("winner.GUDID_COMPANY_NAME"),
            F.col("winner.GUDID_MODEL_NUMBER"),
            F.col("winner.GUDID_CATALOGUE_NUMBER"),
            F.col("winner.GUDID_DEVICE_STATUS"),
            F.col("winner.GUDID_DISTRIBUTION_STATUS"),
            F.col("source.SOURCE_ROW_HASH"),
            F.lit(config.mapping_schema_version).alias("MAPPING_SCHEMA_VERSION"),
            F.lit(config.normalization_version).alias("NORMALIZATION_VERSION"),
            F.lit(config.brand_rules_version).alias("BRAND_RULES_VERSION"),
            F.current_timestamp().alias("MAPPED_AT"),
            F.current_timestamp().alias("PIPELINE_LOADED_AT"),
        )
    )
    aligned = _align_to_schema(result, schema_map_device_mapping_v2)
    hash_columns = [
        field.name
        for field in schema_map_device_mapping_v2.fields
        if field.name not in TARGET_HASH_EXCLUSIONS
    ]
    return aligned.drop("ROW_HASH").withColumn("ROW_HASH", _hash_columns(hash_columns))


def prepare_mapping_references(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
    source_versions: Optional[Dict[str, int]] = None,
    relevant_source: Optional[DataFrame] = None,
) -> Dict[str, DataFrame]:
    gmdn = build_gmdn_reference(config, source_versions)
    gudid = build_gudid_reference(
        gmdn,
        config,
        source_versions,
        relevant_source,
    )
    standard = build_gmdn_standard_lookup(config, source_versions)
    return {"gmdn": gmdn, "gudid": gudid, "standard": standard}


def release_mapping_references(references: Optional[Dict[str, DataFrame]]) -> None:
    if not references:
        return
    for dataframe in references.values():
        None


def build_device_mapping_outputs(
    source: DataFrame,
    anchor_lookup: DataFrame,
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
    source_versions: Optional[Dict[str, int]] = None,
    references: Optional[Dict[str, DataFrame]] = None,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    source_with_context, procedures = build_procedure_context(
        source,
        config,
        source_versions,
    )
    owns_references = references is None
    if references is None:
        references = prepare_mapping_references(config, source_versions, source)
    gmdn_reference = references["gmdn"]
    gudid_reference = references["gudid"]
    standard_lookup = references["standard"]

    try:
        udi = build_udi_candidates(source_with_context, gudid_reference)
        catalogue = build_catalogue_candidates(source_with_context, gudid_reference)
        anchors = build_anchor_candidates(source_with_context, anchor_lookup)
        brands = build_brand_candidates(source_with_context)
        opcs = build_opcs_candidates(
            source_with_context,
            procedures,
            config,
            source_versions,
        )

        high_evidence = (
            udi.unionByName(catalogue)
            .unionByName(anchors)
            .unionByName(brands)
            .where(F.col("MAPPING_CONFIDENCE") >= 0.90)
            .select("EVENT_ID")
            .distinct()
        )
        gmdn_version = (
            source_versions[config.gmdn_source]
            if source_versions and config.gmdn_source in source_versions
            else get_latest_delta_version(config.gmdn_source)
        )
        embedding = build_embedding_candidates(
            source_with_context,
            high_evidence,
            gmdn_reference,
            int(gmdn_version),
            config,
        )
        raw_candidates = (
            udi.unionByName(catalogue)
            .unionByName(anchors)
            .unionByName(brands)
            .unionByName(embedding)
            .unionByName(opcs)
        )
        ranked = enrich_and_rank_candidates(
            raw_candidates,
            gmdn_reference,
            standard_lookup,
            config,
            source_versions,
        )
        final = build_final_mapping(source_with_context, ranked, config)
        direct = udi.unionByName(catalogue)
        return final, ranked, direct
    finally:
        if owns_references:
            release_mapping_references(references)


def validate_device_mapping_outputs(
    source: DataFrame,
    final: DataFrame,
    candidates: DataFrame,
    label: str = "device mapping",
) -> Dict[str, int]:
    source_count = source.count()
    final_count = final.count()
    final_distinct = final.select("EVENT_ID").distinct().count()
    null_event_ids = final.where(F.col("EVENT_ID").isNull()).count()
    duplicate_candidate_ids = (
        candidates.groupBy("CANDIDATE_ID").count().where(F.col("count") > 1).count()
    )
    missing_events = (
        source.select("EVENT_ID")
        .join(final.select("EVENT_ID"), "EVENT_ID", "left_anti")
        .count()
    )
    extra_events = (
        final.select("EVENT_ID")
        .join(source.select("EVENT_ID"), "EVENT_ID", "left_anti")
        .count()
    )
    invalid_confidence = final.where(
        (F.col("mapping_confidence") < 0) | (F.col("mapping_confidence") > 1)
    ).count()
    mislabeled_snomed = final.where(
        F.col("snomed_concept_id").isNotNull()
        & (F.col("STANDARD_VOCABULARY_ID") != "SNOMED")
    ).count()
    biolox_iol = final.where(
        F.lower(F.col("IMPLANT_DESCRIPTION")).contains("biolox")
        & (F.col("gmdncode") == 35658)
    ).count()
    radius_iud = final.where(
        F.lower(F.col("IMPLANT_DESCRIPTION")).rlike(r"\b(radius|radial)\b")
        & (F.col("gmdncode") == 41888)
    ).count()
    copper_medicatd_gmdn = final.where(
        normalize_description_col(F.col("IMPLANT_DESCRIPTION")).rlike(COPPER_IUD_PATTERN)
        & (F.col("gmdncode") == 41888)
    ).count()

    results = {
        "source_rows": source_count,
        "target_rows": final_count,
        "target_distinct_event_ids": final_distinct,
        "null_event_ids": null_event_ids,
        "missing_events": missing_events,
        "extra_events": extra_events,
        "duplicate_candidate_ids": duplicate_candidate_ids,
        "invalid_confidence": invalid_confidence,
        "mislabeled_snomed": mislabeled_snomed,
        "biolox_mapped_to_iol": biolox_iol,
        "radius_mapped_to_iud": radius_iud,
        "copper_mapped_to_medicated_gmdn": copper_medicatd_gmdn,
    }
    failures = {
        key: value
        for key, value in results.items()
        if key not in {
            "source_rows",
            "target_rows",
            "target_distinct_event_ids",
        }
        and value != 0
    }
    if final_count != source_count or final_distinct != source_count:
        failures["row_count_or_grain"] = (
            f"source={source_count}, target={final_count}, distinct={final_distinct}"
        )
    if failures:
        raise AssertionError(f"{label} validation failed: {failures}")
    print(f"[OK] {label} validation passed: {results}")
    return results

# COMMAND ----------


ANCHOR_EVIDENCE_COMMENT = (
    "High-evidence event-level observations used to build safe description anchors. "
    "Only unambiguous UDI or catalogue/GUDID evidence is retained."
)

ANCHOR_LOOKUP_COMMENT = (
    "Version-2 device description anchors derived from event-level high-evidence observations. "
    "Conflicting descriptions are retained with IS_AMBIGUOUS=true and are not used for mapping."
)


def build_direct_candidates_for_anchors(
    source: DataFrame,
    references: Dict[str, DataFrame],
) -> DataFrame:
    return build_udi_candidates(source, references["gudid"]).unionByName(
        build_catalogue_candidates(source, references["gudid"])
    )


def _write_anchor_lookup(
    anchor_lookup: DataFrame,
    config: DeviceMappingConfig,
) -> None:
    (
        _align_to_schema(anchor_lookup, ANCHOR_LOOKUP_SCHEMA)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true")
        .saveAsTable(config.anchor_lookup_table)
    )


def _anchor_changed_keys(
    old_lookup: DataFrame,
    new_lookup: DataFrame,
) -> DataFrame:
    fingerprint_columns = ["gmdncode", "IS_AMBIGUOUS", "DISTINCT_GMDN_COUNT"]
    old = old_lookup.select(
        "KEY_TYPE",
        "ANCHOR_KEY",
        _hash_columns(fingerprint_columns).alias("OLD_FINGERPRINT"),
    )
    new = new_lookup.select(
        "KEY_TYPE",
        "ANCHOR_KEY",
        _hash_columns(fingerprint_columns).alias("NEW_FINGERPRINT"),
    )
    return (
        old.join(new, ["KEY_TYPE", "ANCHOR_KEY"], "full")
        .where(~F.col("OLD_FINGERPRINT").eqNullSafe(F.col("NEW_FINGERPRINT")))
        .select("KEY_TYPE", "ANCHOR_KEY")
        .where(F.col("ANCHOR_KEY").isNotNull())
        .distinct()
    )


def _events_for_anchor_keys(
    source: DataFrame,
    changed_keys: Optional[DataFrame],
) -> Optional[DataFrame]:
    if changed_keys is None or _is_empty(changed_keys):
        return None
    exact_keys = changed_keys.where(F.col("KEY_TYPE") == "EXACT").select(
        F.col("ANCHOR_KEY").alias("NORMALIZED_DESCRIPTION")
    )
    cleaned_keys = changed_keys.where(F.col("KEY_TYPE") == "CLEANED").select(
        F.col("ANCHOR_KEY").alias("CLEANED_DESCRIPTION")
    )
    exact_events = source.join(exact_keys, "NORMALIZED_DESCRIPTION", "inner").select("EVENT_ID")
    cleaned_events = source.join(cleaned_keys, "CLEANED_DESCRIPTION", "inner").select("EVENT_ID")
    return exact_events.unionByName(cleaned_events).distinct()


def _sync_anchor_evidence(
    evidence: DataFrame,
    affected_event_ids: DataFrame,
    config: DeviceMappingConfig,
) -> None:
    _replace_event_subset(
        evidence,
        affected_event_ids,
        config.anchor_evidence_table,
        ANCHOR_EVIDENCE_SCHEMA,
    )


def rebuild_map_device_mapping_v2(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
    create_backup: bool = True,
    configure_source_retention: bool = True,
    refresh_implant_details: bool = True,
) -> Dict[str, object]:
    run_started = datetime.now(timezone.utc)
    backup_suffix = run_started.strftime("%Y%m%d_%H%M%S")
    source = None
    final = None
    candidates = None
    evidence = None
    anchors = None
    references = None

    if configure_source_retention:
        configure_device_mapping_source_cdf(config)
    ensure_device_mapping_auxiliary_tables(config)
    versions = current_device_mapping_versions(config)

    try:
        source = build_implant_source_base(
            None,
            config,
            source_versions=versions,
        )
        references = prepare_mapping_references(config, versions, source)
        direct = build_direct_candidates_for_anchors(source, references)
        evidence = build_anchor_evidence(source, direct, config)
        anchors = build_anchor_lookup_from_evidence(evidence)

        final, candidates, _ = build_device_mapping_outputs(
            source,
            anchors,
            config,
            versions,
            references=references,
        )
        final = final
        candidates = candidates
        validation = validate_device_mapping_outputs(
            source,
            final,
            candidates,
            "map_device_mapping full deployment",
        )

        _replace_table(
            candidates,
            config.candidate_table,
            schema_device_mapping_candidate_v2,
            CANDIDATE_TABLE_COMMENT,
        )
        _replace_table(
            evidence,
            config.anchor_evidence_table,
            ANCHOR_EVIDENCE_SCHEMA,
            ANCHOR_EVIDENCE_COMMENT,
        )
        _replace_table(
            anchors,
            config.anchor_lookup_table,
            ANCHOR_LOOKUP_SCHEMA,
            ANCHOR_LOOKUP_COMMENT,
        )
        backup_table = _replace_table(
            final,
            config.target_table,
            schema_map_device_mapping_v2,
            TARGET_TABLE_COMMENT,
            create_backup=create_backup,
            backup_suffix=backup_suffix,
        )
        save_device_mapping_checkpoints(versions, config)
        refresh_stats = (
            refresh_implant_detail_mapping_columns(config=config)
            if refresh_implant_details
            else None
        )
        return {
            "status": "FULL_DEPLOYMENT_COMPLETE",
            "started_at": run_started.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "target_table": config.target_table,
            "candidate_table": config.candidate_table,
            "backup_table": backup_table,
            "source_versions": versions,
            "validation": validation,
            "implant_detail_refresh": refresh_stats,
        }
    finally:
        for dataframe in (source, final, candidates, evidence, anchors):
            if dataframe is not None:
                None
        release_mapping_references(references)


def deploy_map_device_mapping_improvements(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
    create_backup: bool = True,
) -> Dict[str, object]:
    """Explicit one-time deployment wrapper. Nothing invokes this automatically."""
    return rebuild_map_device_mapping_v2(
        config=config,
        create_backup=create_backup,
        configure_source_retention=True,
        refresh_implant_details=True,
    )

# COMMAND ----------


def _changed_event_ids_from_implant_cdf(
    cdf: DataFrame,
) -> DataFrame:
    return (
        cdf.where(F.col("_change_type").isin("insert", "update_postimage", "delete"))
        .select(F.col("EVENT_ID").cast("long").alias("EVENT_ID"))
        .where(F.col("EVENT_ID").isNotNull())
        .distinct()
    )


def _changed_event_ids_from_procedure_cdf(
    cdf: DataFrame,
    config: DeviceMappingConfig,
) -> DataFrame:
    encounters = (
        cdf.where(F.col("_change_type").isin("insert", "update_postimage", "delete"))
        .select(F.col("ENCNTR_ID").cast("long").alias("ENCNTR_ID"))
        .where(F.col("ENCNTR_ID").isNotNull())
        .distinct()
    )
    return (
        spark.table(config.implant_source)
        .select("EVENT_ID", "ENCNTR_ID")
        .join(encounters, "ENCNTR_ID", "inner")
        .select(F.col("EVENT_ID").cast("long").alias("EVENT_ID"))
        .distinct()
    )


def _source_rows_requiring_remap(
    source_change_ids: Optional[DataFrame],
    config: DeviceMappingConfig,
) -> Tuple[Optional[DataFrame], Optional[DataFrame]]:
    if source_change_ids is None or _is_empty(source_change_ids):
        return None, None
    current_source = build_implant_source_base(source_change_ids, config)
    current_ids = current_source.select("EVENT_ID")
    deleted_ids = source_change_ids.join(current_ids, "EVENT_ID", "left_anti")
    target_hash = spark.table(config.target_table).select(
        "EVENT_ID",
        F.col("SOURCE_ROW_HASH").alias("TARGET_SOURCE_ROW_HASH"),
    )
    changed_ids = (
        current_source
        .join(target_hash, "EVENT_ID", "left")
        .where(
            F.col("TARGET_SOURCE_ROW_HASH").isNull()
            | ~F.col("SOURCE_ROW_HASH").eqNullSafe(F.col("TARGET_SOURCE_ROW_HASH"))
        )
        .select("EVENT_ID")
        .distinct()
    )
    return changed_ids, deleted_ids


def process_device_mapping_incremental_v2(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
    refresh_implant_details: bool = True,
) -> Dict[str, object]:
    required_tables = (
        config.target_table,
        config.candidate_table,
        config.anchor_evidence_table,
        config.anchor_lookup_table,
    )
    missing_tables = [table for table in required_tables if not _table_exists(table)]
    if missing_tables:
        raise RuntimeError(
            "The v2 deployment is not initialized. Missing: "
            + ", ".join(missing_tables)
            + ". Run deploy_map_device_mapping_improvements() once."
        )

    checkpoints = get_device_mapping_checkpoints(config)
    missing_checkpoints = [
        table for table in config.tracked_tables if table not in checkpoints
    ]
    if missing_checkpoints:
        raise RuntimeError(
            "Missing version checkpoints for: "
            + ", ".join(missing_checkpoints)
            + ". Run deploy_map_device_mapping_improvements() once."
        )
    if get_checkpoint_mapping_version(config) != implementation_version(config):
        return rebuild_map_device_mapping_v2(
            config,
            create_backup=False,
            configure_source_retention=False,
            refresh_implant_details=refresh_implant_details,
        )

    current_versions = current_device_mapping_versions(config)
    changed_ranges = {
        table: (checkpoints[table] + 1, current_versions[table])
        for table in config.tracked_tables
        if current_versions[table] > checkpoints[table]
    }
    if not changed_ranges:
        return {
            "status": "NO_CHANGES",
            "source_versions": current_versions,
        }

    reference_changes = [
        table for table in config.reference_tables if table in changed_ranges
    ]
    if reference_changes:
        if not config.auto_full_sync_on_reference_change:
            raise RuntimeError(
                "Reference tables changed and require a full remap: "
                + ", ".join(reference_changes)
            )
        return rebuild_map_device_mapping_v2(
            config,
            create_backup=False,
            configure_source_retention=False,
            refresh_implant_details=refresh_implant_details,
        )

    source_change_ids = None
    procedure_event_ids = None
    if config.implant_source in changed_ranges:
        start, end = changed_ranges[config.implant_source]
        source_change_ids = _changed_event_ids_from_implant_cdf(
            read_cdf_versions(config.implant_source, start, end)
        )
    if config.procedures_source in changed_ranges:
        start, end = changed_ranges[config.procedures_source]
        procedure_event_ids = _changed_event_ids_from_procedure_cdf(
            read_cdf_versions(config.procedures_source, start, end),
            config,
        )

    changed_source_ids, deleted_source_ids = _source_rows_requiring_remap(
        source_change_ids,
        config,
    )
    initial_event_ids = _union_distinct(
        [changed_source_ids, deleted_source_ids, procedure_event_ids]
    )
    if initial_event_ids is None or _is_empty(initial_event_ids):
        save_device_mapping_checkpoints(current_versions, config)
        return {
            "status": "NO_MAPPING_INPUT_CHANGES",
            "source_versions": current_versions,
        }

    references = prepare_mapping_references(config, current_versions)
    initial_source = None
    all_source = None
    source = None
    final = None
    candidates = None
    try:
        old_anchors = spark.table(config.anchor_lookup_table)
        initial_source = build_implant_source_base(initial_event_ids, config)
        initial_direct = build_direct_candidates_for_anchors(initial_source, references)
        initial_evidence = build_anchor_evidence(initial_source, initial_direct, config)
        _sync_anchor_evidence(initial_evidence, initial_event_ids, config)

        interim_anchors = build_anchor_lookup_from_evidence(
            spark.table(config.anchor_evidence_table)
        )
        changed_anchor_keys = _anchor_changed_keys(old_anchors, interim_anchors)
        _write_anchor_lookup(interim_anchors, config)

        expanded_event_ids = None
        if not _is_empty(changed_anchor_keys):
            all_source = build_implant_source_base(None, config)
            expanded_event_ids = _events_for_anchor_keys(all_source, changed_anchor_keys)
        affected_event_ids = _union_distinct([initial_event_ids, expanded_event_ids])
        if affected_event_ids is None:
            affected_event_ids = initial_event_ids

        source = build_implant_source_base(affected_event_ids, config)
        final_direct = build_direct_candidates_for_anchors(source, references)
        final_evidence = build_anchor_evidence(source, final_direct, config)
        _sync_anchor_evidence(final_evidence, affected_event_ids, config)
        final_anchors = build_anchor_lookup_from_evidence(
            spark.table(config.anchor_evidence_table)
        )
        _write_anchor_lookup(final_anchors, config)

        final, candidates, _ = build_device_mapping_outputs(
            source,
            final_anchors,
            config,
            current_versions,
            references=references,
        )
        final = final
        candidates = candidates
        validation = validate_device_mapping_outputs(
            source,
            final,
            candidates,
            "map_device_mapping incremental subset",
        )

        current_event_ids = source.select("EVENT_ID")
        deleted_event_ids = affected_event_ids.join(
            current_event_ids,
            "EVENT_ID",
            "left_anti",
        )
        _delete_key_subset(config.target_table, deleted_event_ids)
        _merge_changed_rows(final, config.target_table, ["EVENT_ID"])
        _replace_event_subset(
            candidates,
            affected_event_ids,
            config.candidate_table,
            schema_device_mapping_candidate_v2,
        )
        save_device_mapping_checkpoints(current_versions, config)
        refresh_stats = (
            refresh_implant_detail_mapping_columns(
                event_ids=affected_event_ids,
                config=config,
            )
            if refresh_implant_details
            else None
        )
        return {
            "status": "INCREMENTAL_COMPLETE",
            "affected_event_ids": affected_event_ids.count(),
            "deleted_event_ids": deleted_event_ids.count(),
            "changed_anchor_keys": changed_anchor_keys.count(),
            "validation": validation,
            "source_versions": current_versions,
            "implant_detail_refresh": refresh_stats,
        }
    finally:
        for dataframe in (initial_source, all_source, source, final, candidates):
            if dataframe is not None:
                None
        release_mapping_references(references)


def process_device_mapping_incremental(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> Dict[str, object]:
    """Drop-in production entry point replacing Device_Mapping_Pipeline.run_incremental."""
    return process_device_mapping_incremental_v2(config)

# COMMAND ----------


def _deduplicated_target_mapping(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> DataFrame:
    mapping = spark.table(config.target_table)
    order_columns = [
        F.col("mapping_confidence").desc_nulls_last(),
    ]
    if "MAPPED_AT" in mapping.columns:
        order_columns.append(F.col("MAPPED_AT").desc_nulls_last())
    order_columns.extend([
        F.col("snomed_concept_id").asc_nulls_last(),
        F.col("gmdncode").asc_nulls_last(),
    ])
    window = Window.partitionBy("EVENT_ID").orderBy(*order_columns)
    return (
        mapping
        .withColumn("_EVENT_RANK", F.row_number().over(window))
        .where(F.col("_EVENT_RANK") == 1)
        .drop("_EVENT_RANK")
    )


def get_device_mapping_by_event(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> DataFrame:
    mapping = _deduplicated_target_mapping(config)
    return mapping.select(
        "EVENT_ID",
        F.col("snomed_concept_id").cast("int").alias("SNOMED_DEVICE_CONCEPT_ID"),
        F.col("snomed_name").cast("string").alias("SNOMED_DEVICE_CONCEPT_NAME"),
        F.col("device_type").cast("string").alias("DEVICE_TYPE"),
        F.col("mapping_confidence").cast("double").alias("MAPPING_CONFIDENCE"),
    )


def get_device_mapping_enrichment_by_event(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> DataFrame:
    """Return the complete event-level enrichment used by canonical implant details."""
    mapping = _deduplicated_target_mapping(config)
    result = mapping.select(
        "EVENT_ID",
        F.col("snomed_concept_id").cast("long").alias("SNOMED_DEVICE_CONCEPT_ID"),
        F.col("snomed_name").cast("string").alias("SNOMED_DEVICE_CONCEPT_NAME"),
        F.col("device_type").cast("string").alias("DEVICE_TYPE"),
        F.col("mapping_confidence").cast("double").alias("MAPPING_CONFIDENCE"),
        F.col("gmdncode").cast("long").alias("GMDN_CODE"),
        F.col("gmdn_name").cast("string").alias("GMDN_NAME"),
        F.col("mapping_layer").cast("string").alias("MAPPING_LAYER"),
        F.col("confidence_tier").cast("string").alias("CONFIDENCE_TIER"),
        F.concat(
            F.lit("EVENT_ID:"),
            F.coalesce(F.col("mapping_layer"), F.lit("UNKNOWN")),
        ).alias("MAPPING_SOURCE"),
        F.col("MAPPING_CANDIDATE_COUNT").cast("int").alias("MAPPING_CANDIDATE_COUNT"),
        F.col("MAPPING_AMBIGUOUS_IND").cast("boolean").alias("MAPPING_CONFLICT_IND"),
    )
    hash_columns = [
        "SNOMED_DEVICE_CONCEPT_ID",
        "SNOMED_DEVICE_CONCEPT_NAME",
        "DEVICE_TYPE",
        "MAPPING_CONFIDENCE",
        "GMDN_CODE",
        "GMDN_NAME",
        "MAPPING_LAYER",
        "CONFIDENCE_TIER",
        "MAPPING_SOURCE",
        "MAPPING_CANDIDATE_COUNT",
        "MAPPING_CONFLICT_IND",
    ]
    return result.withColumn(
        "MAPPING_ROW_HASH",
        F.sha2(F.to_json(F.struct(*[F.col(name) for name in hash_columns])), 256),
    )


def get_device_mapping_lookup(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> DataFrame:
    """
    Compatibility lookup for the unmodified legacy description join.

    Only descriptions with one distinct mapping tuple are returned. This prevents
    the old unordered first() aggregation from contaminating records while the
    wrapper below replaces the final result with an EVENT_ID join.
    """
    mapping = _deduplicated_target_mapping(config).withColumn(
        "normalized_desc",
        normalize_description_col(F.col("IMPLANT_DESCRIPTION")),
    )
    tuple_value = F.concat_ws(
        "|",
        F.coalesce(F.col("snomed_concept_id").cast("string"), F.lit("NULL")),
        F.coalesce(F.col("device_type"), F.lit("NULL")),
    )
    safe_keys = mapping.groupBy("normalized_desc").agg(
        F.countDistinct(tuple_value).alias("DISTINCT_MAPPING_COUNT")
    ).where(F.col("DISTINCT_MAPPING_COUNT") == 1)
    window = Window.partitionBy("normalized_desc").orderBy(
        F.col("mapping_confidence").desc_nulls_last(),
        F.col("EVENT_ID").asc(),
    )
    return (
        mapping.join(safe_keys.select("normalized_desc"), "normalized_desc", "inner")
        .where(F.col("snomed_concept_id").isNotNull())
        .withColumn("_DESCRIPTION_RANK", F.row_number().over(window))
        .where(F.col("_DESCRIPTION_RANK") == 1)
        .select(
            "normalized_desc",
            F.col("snomed_concept_id").cast("int").alias("SNOMED_DEVICE_CONCEPT_ID"),
            F.col("snomed_name").alias("SNOMED_DEVICE_CONCEPT_NAME"),
            F.col("device_type").alias("DEVICE_TYPE"),
            F.col("mapping_confidence").alias("MAPPING_CONFIDENCE"),
        )
    )


def attach_device_mapping_by_event(
    implant_df: DataFrame,
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> DataFrame:
    mapping_columns = [
        "SNOMED_DEVICE_CONCEPT_ID",
        "SNOMED_DEVICE_CONCEPT_NAME",
        "DEVICE_TYPE",
        "MAPPING_CONFIDENCE",
    ]
    result = implant_df
    for column in mapping_columns:
        if column in result.columns:
            result = result.drop(column)
    return result.join(F.broadcast(get_device_mapping_by_event(config)), "EVENT_ID", "left")


def create_implant_details_incr():
    """
    Drop-in wrapper for Map Pipeline.

    Import this notebook after the legacy create_implant_details_incr definition and
    before the function is called. The legacy extraction/pivot remains in place, the
    corrected UDI parser is used through global function resolution, and the final
    mapping is replaced with an EVENT_ID join.
    """
    if _LEGACY_CREATE_IMPLANT_DETAILS_INCR is None:
        raise RuntimeError(
            "No legacy create_implant_details_incr function was present when this "
            "notebook was imported. Import it after the function definition."
        )
    result = _LEGACY_CREATE_IMPLANT_DETAILS_INCR()
    if result is None:
        return None
    return attach_device_mapping_by_event(result)


def refresh_implant_detail_mapping_columns(
    event_ids: Optional[DataFrame] = None,
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
    implant_table: Optional[str] = None,
) -> Dict[str, object]:
    """Reconcile event-level mappings into any compatible implant target."""
    target_table = implant_table or config.implant_source
    if not _table_exists(target_table):
        return {
            "target_table": target_table,
            "evaluated_event_ids": 0,
            "updated_columns": [],
            "status": "TARGET_MISSING",
        }

    implant_ids = spark.table(target_table).select("EVENT_ID")
    if event_ids is not None:
        implant_ids = implant_ids.join(
            event_ids.select("EVENT_ID").distinct(),
            "EVENT_ID",
            "inner",
        )

    mapping = get_device_mapping_enrichment_by_event(config)
    updates = implant_ids.distinct().join(mapping, "EVENT_ID", "left")
    source_columns = set(updates.columns)
    target_columns = set(spark.table(target_table).columns)
    preferred_columns = [
        "SNOMED_DEVICE_CONCEPT_ID",
        "SNOMED_DEVICE_CONCEPT_NAME",
        "DEVICE_TYPE",
        "MAPPING_CONFIDENCE",
        "GMDN_CODE",
        "GMDN_NAME",
        "MAPPING_LAYER",
        "CONFIDENCE_TIER",
        "MAPPING_SOURCE",
        "MAPPING_CANDIDATE_COUNT",
        "MAPPING_CONFLICT_IND",
        "MAPPING_ROW_HASH",
    ]
    update_columns = [
        name
        for name in preferred_columns
        if name in source_columns and name in target_columns
    ]
    target_types = {
        field.name: field.dataType
        for field in spark.table(target_table).schema.fields
    }
    updates = updates.select(
        F.col("EVENT_ID"),
        *[
            F.col(name).cast(target_types[name]).alias(name)
            for name in update_columns
        ],
    )
    evaluated_count = int(updates.count())
    if evaluated_count == 0 or not update_columns:
        return {
            "target_table": target_table,
            "evaluated_event_ids": evaluated_count,
            "updated_columns": update_columns,
            "status": "NO_ROWS" if evaluated_count == 0 else "NO_COMPATIBLE_COLUMNS",
        }

    change_condition = " OR ".join(
        f"NOT (target.`{name}` <=> source.`{name}`)"
        for name in update_columns
    )
    assignments = {
        name: f"source.`{name}`"
        for name in update_columns
    }
    (
        DeltaTable.forName(spark, target_table)
        .alias("target")
        .merge(updates.alias("source"), "target.EVENT_ID = source.EVENT_ID")
        .whenMatchedUpdate(condition=change_condition, set=assignments)
        .execute()
    )
    return {
        "target_table": target_table,
        "evaluated_event_ids": evaluated_count,
        "updated_columns": update_columns,
        "status": "RECONCILED",
    }


def run_map_device_updates(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> Dict[str, object]:
    return process_device_mapping_incremental_v2(
        config=config,
        refresh_implant_details=True,
    )


def run_device_mapping_post_deployment_checks(
    config: DeviceMappingConfig = DEFAULT_DEVICE_MAPPING_CONFIG,
) -> Dict[str, int]:
    target = spark.table(config.target_table)
    source = spark.table(config.implant_source)
    candidate = spark.table(config.candidate_table)
    results = {
        "target_duplicate_event_ids": target.groupBy("EVENT_ID").count().where(F.col("count") > 1).count(),
        "source_events_missing_target": source.select("EVENT_ID").join(target.select("EVENT_ID"), "EVENT_ID", "left_anti").count(),
        "target_events_missing_source": target.select("EVENT_ID").join(source.select("EVENT_ID"), "EVENT_ID", "left_anti").count(),
        "duplicate_candidate_ids": candidate.groupBy("CANDIDATE_ID").count().where(F.col("count") > 1).count(),
        "biolox_mapped_to_iol": target.where(
            F.lower(F.col("IMPLANT_DESCRIPTION")).contains("biolox")
            & (F.col("gmdncode") == 35658)
        ).count(),
        "radius_mapped_to_iud": target.where(
            F.lower(F.col("IMPLANT_DESCRIPTION")).rlike(r"\b(radius|radial)\b")
            & (F.col("gmdncode") == 41888)
        ).count(),
        "mislabeled_snomed": target.where(
            F.col("snomed_concept_id").isNotNull()
            & (F.col("STANDARD_VOCABULARY_ID") != "SNOMED")
        ).count(),
    }
    failures = {key: value for key, value in results.items() if value != 0}
    if failures:
        raise AssertionError(f"Post-deployment checks failed: {failures}")
    print(f"[OK] Post-deployment checks passed: {results}")
    return results

# COMMAND ----------

_device_config = DEFAULT_DEVICE_MAPPING_CONFIG
_device_missing_sources = [
    table_name
    for table_name in (_device_config.implant_source, _device_config.procedures_source)
    if not _table_exists(table_name)
]
if _device_missing_sources:
    raise RuntimeError(
        "Required canonical Bronze device sources are missing: "
        + ", ".join(_device_missing_sources)
        + ". Run map_pipeline before device_pipeline."
    )

_device_outputs = (
    _device_config.target_table,
    _device_config.candidate_table,
    _device_config.anchor_evidence_table,
    _device_config.anchor_lookup_table,
    _device_config.checkpoint_table,
)
_device_force_full = bronze_bool("force_full_refresh", False) or any(
    not _table_exists(table_name) for table_name in _device_outputs
)
_device_create_backup = bronze_bool("create_cutover_backups", True)
_device_run_checks = bronze_bool("run_post_deployment_checks", False)
_device_parent_run_id = bronze_value("pipeline_run_id", "")

try:
    if _device_force_full:
        print("[MODE] device mapping FULL_REBUILD")
        _device_result = deploy_map_device_mapping_improvements(
            config=_device_config,
            create_backup=_device_create_backup,
        )
    else:
        print("[MODE] device mapping INCREMENTAL")
        try:
            _device_result = process_device_mapping_incremental_v2(
                config=_device_config,
                refresh_implant_details=True,
            )
        except Exception as _device_incremental_error:
            _device_message = str(_device_incremental_error).lower()
            _device_recoverable = any(marker in _device_message for marker in (
                "cdf is unavailable",
                "not initialized",
                "missing version checkpoints",
                "run deploy_map_device_mapping_improvements",
            ))
            if not _device_recoverable:
                raise
            print(
                "[RECOVERY] Incremental device state cannot be replayed; "
                "running a validated full rebuild."
            )
            _device_result = rebuild_map_device_mapping_v2(
                config=_device_config,
                create_backup=_device_create_backup,
                configure_source_retention=True,
                refresh_implant_details=True,
            )

    _device_published_reconciliation = refresh_implant_detail_mapping_columns(
        config=_device_config,
        implant_table="4_prod.bronze.map_implant_details",
    )
    _device_checks = (
        run_device_mapping_post_deployment_checks(_device_config)
        if _device_run_checks
        else None
    )
    _device_summary = {
        "status": "SUCCESS",
        "pipeline": "device_pipeline",
        "parent_run_id": _device_parent_run_id,
        "forced_full": _device_force_full,
        "procedure_source": _device_config.procedures_source,
        "result": _device_result,
        "canonical_implant_source": _device_config.implant_source,
        "published_reconciliation": _device_published_reconciliation,
        "post_deployment_checks": _device_checks,
    }
    print(bronze_json(_device_summary))
    dbutils.notebook.exit(bronze_json(_device_summary))
except Exception as _device_error:
    print(
        bronze_json({
            "status": "FAILED",
            "pipeline": "device_pipeline",
            "parent_run_id": _device_parent_run_id,
            "error_type": type(_device_error).__name__,
            "error": str(_device_error),
        })
    )
    raise

