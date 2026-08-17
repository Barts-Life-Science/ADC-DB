# Databricks notebook source
# MAGIC %md
# MAGIC # Map 20 — Clinical
# MAGIC
# MAGIC Components: diagnosis, problem, medication administration, procedure, death. This notebook is executed by `map_pipeline` in the shared map runtime.

# COMMAND ----------

if "_PIPELINE_RUN_ID" not in globals():
    raise RuntimeError("Run this component through map_pipeline so shared contracts, checkpoints and audit state are initialized.")

# COMMAND ----------

_pipeline_component_start("map_diagnosis")
_pipeline_shared_update_table = globals().get('update_table')
_pipeline_shared_table_exists = globals().get('table_exists')
_pipeline_shared_get_max_timestamp = globals().get('get_max_timestamp')
_pipeline_shared_has_cdf_enabled = globals().get('has_cdf_enabled')
_pipeline_shared_get_incremental = globals().get('get_incremental_data_with_cdf')
globals().pop('_map_updates_original_update_table', None)
globals().pop('_map_medical_personnel_original_update_table', None)
globals().pop('_MAP_UPDATES_ORIGINAL_UPDATE_TABLE', None)

from datetime import datetime
from functools import reduce
from typing import Dict, List, Optional
from uuid import uuid4
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DecimalType, DoubleType, LongType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window
MAP_DIAGNOSIS_PIPELINE = 'map_diagnosis_v2'
MAP_DIAGNOSIS_TARGET = '4_prod.bronze.map_diagnosis'
MAP_DIAGNOSIS_SOURCE = '4_prod.raw.mill_diagnosis'
MAP_DIAGNOSIS_NOMENCLATURE = '3_lookup.mill.map_nomenclature'
MAP_DIAGNOSIS_ENCOUNTER = '4_prod.bronze.map_encounter'
MAP_DIAGNOSIS_CODE_VALUE = '3_lookup.mill.mill_code_value'
MAP_DIAGNOSIS_CHECKPOINT = '6_mgmt.bronze.map_diagnosis_pipeline_state'
MAP_DIAGNOSIS_TRUST = 'Barts'
MAP_DIAGNOSIS_SCHEMA_VERSION = '2.0.0'
MAP_DIAGNOSIS_CDF_RETENTION = 'interval 30 days'
MAP_DIAGNOSIS_SOURCES = {'diagnosis': MAP_DIAGNOSIS_SOURCE, 'nomenclature': MAP_DIAGNOSIS_NOMENCLATURE, 'encounter': MAP_DIAGNOSIS_ENCOUNTER, 'code_value': MAP_DIAGNOSIS_CODE_VALUE}
_MAP_DIAGNOSIS_ACTIVE_SOURCE_VERSIONS: Dict[str, int] = {}
map_diagnosis_comment = 'One row per source diagnosis for the configured trust. Preserves raw diagnosis state and dates, adds explicitly sourced date/text fields, and enriches diagnoses with current nomenclature, OMOP, SNOMED, ICD-10 and code-value descriptions. Inactive source rows are retained; no clinical-value filters are applied.'

def _sf(name, data_type, comment):
    return StructField(name, data_type, True, metadata={'comment': comment})
schema_map_diagnosis_v2 = StructType([_sf('DIAGNOSIS_ID', LongType(), 'Primary key from the source diagnosis table.'), _sf('PERSON_ID', LongType(), 'Source person identifier.'), _sf('ENCNTR_ID', LongType(), 'Source encounter identifier.'), _sf('DIAG_DT_TM', TimestampType(), 'Compatibility diagnosis datetime derived from raw diagnosis datetime, encounter arrival, then row effective datetime.'), _sf('DIAG_DT_TM_RAW', TimestampType(), 'Unmodified DIAG_DT_TM from the diagnosis source.'), _sf('ARRIVE_DT_TM', TimestampType(), 'Current encounter arrival datetime used as the second date fallback.'), _sf('BEG_EFFECTIVE_DT_TM', TimestampType(), 'Source diagnosis row beginning effective datetime.'), _sf('END_EFFECTIVE_DT_TM', TimestampType(), 'Source diagnosis row ending effective datetime.'), _sf('DIAG_DT_TM_SOURCE', StringType(), 'Source selected for DIAG_DT_TM: DIAG_DT_TM, ENCOUNTER_ARRIVE_DT_TM, BEG_EFFECTIVE_DT_TM, or NULL.'), _sf('DIAG_DT_TM_BEFORE_1950_IND', BooleanType(), 'True when the derived diagnosis datetime precedes 1950-01-01; no row is filtered.'), _sf('DIAG_DT_TM_FUTURE_AT_MAP_IND', BooleanType(), 'True when the derived diagnosis datetime was in the future when mapped; no row is filtered.'), _sf('earliest_diagnosis_date', TimestampType(), 'Earliest derived diagnosis datetime across the complete current PERSON_ID and NOMENCLATURE_ID group.'), _sf('DIAG_TYPE_CD', LongType(), 'Source diagnosis type code.'), _sf('diag_type_desc', StringType(), 'Code-value display, description, or CDF meaning for DIAG_TYPE_CD.'), _sf('DIAG_PRIORITY', LongType(), 'Source diagnosis priority.'), _sf('RANKING_CD', LongType(), 'Source diagnosis ranking code.'), _sf('DIAG_PRSNL_ID', LongType(), 'Personnel identifier that added the diagnosis.'), _sf('CLINICAL_SERVICE_CD', LongType(), 'Source clinical service code.'), _sf('clinical_service_desc', StringType(), 'Code-value display, description, or CDF meaning for CLINICAL_SERVICE_CD.'), _sf('CONFIRMATION_STATUS_CD', LongType(), 'Source confirmation status code.'), _sf('confirmation_status_desc', StringType(), 'Code-value display, description, or CDF meaning for CONFIRMATION_STATUS_CD.'), _sf('CLASSIFICATION_CD', LongType(), 'Source diagnosis classification code.'), _sf('classification_desc', StringType(), 'Code-value display, description, or CDF meaning for CLASSIFICATION_CD.'), _sf('ACTIVE_IND', LongType(), 'Source active indicator; inactive rows are retained.'), _sf('ACTIVE_STATUS_CD', LongType(), 'Source row active-status code.'), _sf('ACTIVE_STATUS_DT_TM', TimestampType(), 'Datetime the source active status was set.'), _sf('CONTRIBUTOR_SYSTEM_CD', LongType(), 'Source contributor-system code.'), _sf('ORGANIZATION_ID', LongType(), 'Source organization identifier.'), _sf('TRUST', StringType(), 'Source Trust value retained for provenance.'), _sf('UPDT_CNT', LongType(), 'Source update counter.'), _sf('UPDT_DT_TM', TimestampType(), 'Source application update datetime.'), _sf('LAST_UTC_TS', TimestampType(), 'Source last UTC timestamp.'), _sf('DIAGNOSIS_DISPLAY', StringType(), 'Clinician-facing diagnosis display from the diagnosis source.'), _sf('DIAG_FTDESC', StringType(), 'Source free-text diagnosis description.'), _sf('DIAGNOSIS_TEXT', StringType(), 'First nonblank value from nomenclature source string, diagnosis display, and diagnosis free text.'), _sf('DIAGNOSIS_TEXT_SOURCE', StringType(), 'Column used to populate DIAGNOSIS_TEXT.'), _sf('DIAGNOSIS_GROUP', LongType(), 'Source diagnosis-group identifier.'), _sf('ORIGINATING_NOMENCLATURE_ID', LongType(), 'Source originating nomenclature identifier.'), _sf('CLINICAL_DIAG_PRIORITY', LongType(), 'Source clinical diagnosis priority.'), _sf('LATERALITY_CD', LongType(), 'Source laterality code.'), _sf('CONDITIONAL_QUAL_CD', LongType(), 'Source conditional qualifier code.'), _sf('PROBABILITY', DoubleType(), 'Source numeric diagnosis probability.'), _sf('SEVERITY_CLASS_CD', LongType(), 'Source severity classification code.'), _sf('SEVERITY_CD', LongType(), 'Source severity code.'), _sf('SEVERITY_FTDESC', StringType(), 'Source free-text severity.'), _sf('ASSERTED_DT_TM', TimestampType(), 'Source diagnosis asserted datetime.'), _sf('LIFE_CYCLE_STATUS_CD', LongType(), 'Source diagnosis lifecycle status code.'), _sf('LIFE_CYCLE_DT_TM', TimestampType(), 'Source diagnosis lifecycle datetime.'), _sf('NOMENCLATURE_ID', LongType(), 'Source diagnosis nomenclature identifier.'), _sf('SOURCE_IDENTIFIER', StringType(), 'Current nomenclature source-vocabulary identifier.'), _sf('SOURCE_STRING', StringType(), 'Current nomenclature source string.'), _sf('SOURCE_VOCABULARY_CD', LongType(), 'Current nomenclature source-vocabulary code.'), _sf('source_vocabulary_desc', StringType(), 'Code-value display, description, or CDF meaning for SOURCE_VOCABULARY_CD.'), _sf('VOCAB_AXIS_CD', LongType(), 'Current nomenclature vocabulary-axis code.'), _sf('vocab_axis_desc', StringType(), 'Code-value display, description, or CDF meaning for VOCAB_AXIS_CD.'), _sf('CONCEPT_CKI', StringType(), 'Complete, unmodified nomenclature concept CKI.'), _sf('CONCEPT_CKI_SOURCE', StringType(), 'Source component before the first exclamation mark in CONCEPT_CKI.'), _sf('CONCEPT_CKI_IDENTIFIER', StringType(), 'Identifier component after the final exclamation mark in CONCEPT_CKI.'), _sf('NOMENCLATURE_IS_ACTIVE', BooleanType(), 'Current nomenclature active indicator; inactive mappings are retained.'), _sf('NOMENCLATURE_SOURCE_CHANGE_TS', TimestampType(), 'True source-change timestamp carried by the nomenclature table.'), _sf('NOMENCLATURE_MAPPING_HASH', StringType(), 'Stable hash of current nomenclature mapping content, excluding load timestamps.'), _sf('FOUND_CUI', StringType(), 'CUI found during nomenclature mapping.'), _sf('OMOP_CONCEPT_ID', LongType(), 'Current mapped OMOP concept identifier.'), _sf('OMOP_CONCEPT_NAME', StringType(), 'Current mapped OMOP concept name.'), _sf('OMOP_STANDARD_CONCEPT', StringType(), 'Current OMOP standard-concept flag.'), _sf('OMOP_MATCH_NUMBER', LongType(), 'Number of OMOP mapping matches.'), _sf('OMOP_SIMILARITY', DoubleType(), 'Source-to-OMOP similarity score.'), _sf('OMOP_CONCEPT_DOMAIN', StringType(), 'Current OMOP concept domain.'), _sf('OMOP_CONCEPT_CLASS', StringType(), 'Current OMOP concept class.'), _sf('SNOMED_CODE', LongType(), 'Current mapped SNOMED code.'), _sf('SNOMED_TYPE', StringType(), 'Current SNOMED mapping method.'), _sf('SNOMED_MATCH_NUMBER', LongType(), 'Number of SNOMED mapping matches.'), _sf('SNOMED_SIMILARITY', DoubleType(), 'Source-to-SNOMED similarity score.'), _sf('SNOMED_TERM', StringType(), 'Current mapped SNOMED term.'), _sf('ICD10_CODE', StringType(), 'Current mapped ICD-10 code.'), _sf('ICD10_TYPE', StringType(), 'Current ICD-10 mapping method.'), _sf('ICD10_MATCH_NUMBER', LongType(), 'Number of ICD-10 mapping matches.'), _sf('ICD10_SIMILARITY', DoubleType(), 'Source-to-ICD-10 similarity score.'), _sf('ICD10_TERM', StringType(), 'Current mapped ICD-10 term.'), _sf('CODE_VALUE_LOOKUP_HASH', StringType(), 'Stable hash of the six code-value descriptions used by this row.'), _sf('DIAGNOSIS_ADC_UPDT', TimestampType(), 'ADC_UPDT from the diagnosis source only.'), _sf('NOMENCLATURE_ADC_UPDT', TimestampType(), 'ADC_UPDT from the nomenclature source only.'), _sf('ENCOUNTER_ADC_UPDT', TimestampType(), 'ADC_UPDT from the encounter source only.'), _sf('CODE_VALUE_ADC_UPDT', TimestampType(), 'Greatest ADC_UPDT among code-value rows used for descriptions.'), _sf('MAP_REFRESH_DT_TM', TimestampType(), 'Datetime this mapped row last materially changed.'), _sf('MAP_ROW_HASH', StringType(), 'Stable hash used to avoid rewriting unchanged mapped rows.'), _sf('ADC_UPDT', TimestampType(), 'Compatibility composite update timestamp. Never use this column as a source-system incremental checkpoint.')])
schema_map_diagnosis = schema_map_diagnosis_v2

def _sql_name(table_name: str) -> str:
    quote = chr(96)
    return '.'.join((quote + part.replace(quote, quote + quote) + quote for part in table_name.split('.')))

def _escape_comment(value: str) -> str:
    return (value or '').replace('\\', '\\\\').replace("'", "''")

def _normalized_table_name(value: str) -> str:
    return (value or '').replace(chr(96), '').strip().lower()

def _nonblank(column):
    return F.when(column.isNotNull() & (F.length(F.trim(column.cast('string'))) > 0), F.trim(column.cast('string')))

def _stable_hash(column_names: List[str]):
    fields = [F.col(name).alias(name) for name in column_names]
    return F.sha2(F.to_json(F.struct(*fields), {'ignoreNullFields': 'false'}), 256)

def _checked_long(column, label: str, required: bool=False):
    # Value path is decimal-only. An IEEE-754 double cannot hold integers above 2**53
    # exactly, so the previous double round-trip destroyed every identifier longer than
    # 16 digits before the range test ever ran. UK SNOMED extension codes are 17-18
    # digits, which is why 3_lookup.mill.map_nomenclature.SNOMED_CODE (a BIGINT column)
    # arrived here intact and left as NULL.
    # as_double below is a reject-only guard: it screens NaN, non-integers and absurd
    # magnitudes, and never produces the returned value. Keeping the magnitude guard
    # ahead of the decimal range test also stops the decimal cast from overflowing
    # under ANSI mode, because CASE branch conditions are evaluated in order.
    as_double = column.cast('double')
    as_decimal = column.cast('decimal(38,0)')
    null_result = F.lit(None) if required else F.lit(None).cast('long')
    in_long_range = as_decimal.between(F.lit(-9223372036854775808).cast('decimal(38,0)'), F.lit(9223372036854775807).cast('decimal(38,0)'))
    return F.when(column.isNull(), null_result).when(F.isnan(as_double), F.lit(None)).when(as_double != F.floor(as_double), F.lit(None)).when(F.abs(as_double) > F.lit(9223372036854775807), F.lit(None)).when(~in_long_range, F.lit(None)).otherwise(as_decimal.cast('long'))

def _empty_key_df(table_name: str, key_column: str) -> DataFrame:
    data_type = spark.table(table_name).schema[key_column].dataType
    return spark.createDataFrame([], StructType([StructField(key_column, data_type, True)]))

def _empty_map_rows() -> DataFrame:
    return spark.createDataFrame([], schema_map_diagnosis_v2)

def _is_empty(df: DataFrame) -> bool:
    return df.isEmpty()

def _align_to_v2_schema(df: DataFrame) -> DataFrame:
    expressions = []
    available = set(df.columns)
    for field in schema_map_diagnosis_v2.fields:
        if field.name in available:
            if isinstance(field.dataType, LongType):
                expression = _checked_long(F.col(field.name), field.name, required=field.name == 'DIAGNOSIS_ID')
            else:
                expression = F.col(field.name).cast(field.dataType)
            expressions.append(expression.alias(field.name, metadata=field.metadata))
        else:
            expressions.append(F.lit(None).cast(field.dataType).alias(field.name, metadata=field.metadata))
    return df.select(*expressions)

def ensure_map_diagnosis_v2_schema() -> None:
    if not spark.catalog.tableExists(MAP_DIAGNOSIS_TARGET):
        DeltaTable.createIfNotExists(spark).tableName(
            MAP_DIAGNOSIS_TARGET
        ).addColumns(
            bronze_contract_schema(MAP_DIAGNOSIS_TARGET)
        ).comment(map_diagnosis_comment).property(
            'delta.enableChangeDataFeed', 'true'
        ).property(
            'delta.enableRowTracking', 'true'
        ).property(
            'delta.autoOptimize.optimizeWrite', 'true'
        ).property(
            'delta.autoOptimize.autoCompact', 'true'
        ).property(
            'delta.deletedFileRetentionDuration', MAP_DIAGNOSIS_CDF_RETENTION
        ).property(
            'delta.logRetentionDuration', MAP_DIAGNOSIS_CDF_RETENTION
        ).property(
            'pipeline.map_diagnosis.schema_version',
            MAP_DIAGNOSIS_SCHEMA_VERSION,
        ).execute()
        print('[INFO] Created ' + MAP_DIAGNOSIS_TARGET + ' with the single-layer schema.')

def _target_table_properties() -> Dict[str, str]:
    if not spark.catalog.tableExists(MAP_DIAGNOSIS_TARGET):
        return {}
    return {row['key']: row['value'] for row in spark.sql('SHOW TBLPROPERTIES ' + _sql_name(MAP_DIAGNOSIS_TARGET)).collect()}

def _set_map_diagnosis_metadata(apply_column_comments: bool=False) -> None:
    target_sql = _sql_name(MAP_DIAGNOSIS_TARGET)
    properties = {'comment': map_diagnosis_comment, 'delta.enableChangeDataFeed': 'true', 'delta.enableDeletionVectors': 'true', 'delta.enableRowTracking': 'true', 'delta.autoOptimize.optimizeWrite': 'true', 'delta.autoOptimize.autoCompact': 'true', 'delta.deletedFileRetentionDuration': MAP_DIAGNOSIS_CDF_RETENTION, 'delta.logRetentionDuration': MAP_DIAGNOSIS_CDF_RETENTION, 'pipeline.map_diagnosis.schema_version': MAP_DIAGNOSIS_SCHEMA_VERSION, 'pipeline.map_diagnosis.cdf_checkpoint': 'delta_commit_version', 'pipeline.map_diagnosis.date_semantics': 'raw_plus_explicit_derived_fields'}
    property_sql = ', '.join(("'" + _escape_comment(key) + "' = '" + _escape_comment(value) + "'" for key, value in properties.items()))
    spark.sql('ALTER TABLE ' + target_sql + ' SET TBLPROPERTIES (' + property_sql + ')')
    if apply_column_comments:
        for field in bronze_contract_schema(MAP_DIAGNOSIS_TARGET).fields:
            comment = field.metadata.get('comment', '')
            if comment:
                spark.sql('ALTER TABLE ' + target_sql + ' ALTER COLUMN ' + chr(96) + field.name + chr(96) + " COMMENT '" + _escape_comment(comment) + "'")

def ensure_map_diagnosis_checkpoint_table() -> None:
    checkpoint_sql = _sql_name(MAP_DIAGNOSIS_CHECKPOINT)
    spark.sql("\n        CREATE TABLE IF NOT EXISTS {checkpoint} (\n            pipeline_name STRING NOT NULL,\n            source_table STRING NOT NULL,\n            last_processed_version BIGINT NOT NULL,\n            run_id STRING,\n            updated_at TIMESTAMP\n        )\n        USING DELTA\n        TBLPROPERTIES (\n            'delta.enableChangeDataFeed' = 'true',\n            'delta.deletedFileRetentionDuration' = '{retention}',\n            'delta.logRetentionDuration' = '{retention}',\n            'comment' = 'Successful source-specific Delta CDF checkpoints for map pipelines.'\n        )\n        ".format(checkpoint=checkpoint_sql, retention=MAP_DIAGNOSIS_CDF_RETENTION))

def configure_map_diagnosis_cdf_retention() -> None:
    """
    Extend the source recovery window. This performs DDL only when explicitly called.
    """
    for table_name in MAP_DIAGNOSIS_SOURCES.values():
        spark.sql('ALTER TABLE ' + _sql_name(table_name) + ' SET TBLPROPERTIES (' + "'delta.deletedFileRetentionDuration' = '" + MAP_DIAGNOSIS_CDF_RETENTION + "', " + "'delta.logRetentionDuration' = '" + MAP_DIAGNOSIS_CDF_RETENTION + "')")

def _latest_delta_version(table_name: str) -> int:
    row = spark.sql('DESCRIBE HISTORY ' + _sql_name(table_name) + ' LIMIT 1').select('version').first()
    if row is None:
        raise RuntimeError('No Delta history found for ' + table_name)
    return int(row['version'])

def _latest_source_versions() -> Dict[str, int]:
    return {name: _latest_delta_version(table) for name, table in MAP_DIAGNOSIS_SOURCES.items()}

def _checkpoint_versions() -> Dict[str, int]:
    ensure_map_diagnosis_checkpoint_table()
    rows = spark.table(MAP_DIAGNOSIS_CHECKPOINT).filter(F.col('pipeline_name') == MAP_DIAGNOSIS_PIPELINE).select('source_table', 'last_processed_version').collect()
    return {row['source_table']: int(row['last_processed_version']) for row in rows}

def _write_checkpoint_versions(end_versions: Dict[str, int], run_id: str) -> None:
    ensure_map_diagnosis_checkpoint_table()
    now = datetime.utcnow()
    rows = [(MAP_DIAGNOSIS_PIPELINE, MAP_DIAGNOSIS_SOURCES[source_name], int(version), run_id, now) for source_name, version in end_versions.items()]
    schema = StructType([StructField('pipeline_name', StringType(), False), StructField('source_table', StringType(), False), StructField('last_processed_version', LongType(), False), StructField('run_id', StringType(), True), StructField('updated_at', TimestampType(), True)])
    updates = spark.createDataFrame(rows, schema)
    DeltaTable.forName(spark, MAP_DIAGNOSIS_CHECKPOINT).alias('t').merge(updates.alias('s'), 't.pipeline_name = s.pipeline_name AND t.source_table = s.source_table').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

class MapDiagnosisCDFGap(RuntimeError):
    pass

def _m20_hardening_sql_identifier(table_name: str) -> str:
    return '.'.join('`' + part.replace('`', '``') + '`' for part in table_name.split('.'))

def _m20_hardening_snapshot_fallback_allowed(exc: BaseException) -> bool:
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

def _m20_hardening_read_pinned_snapshot(table_name: str, version: int):
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
        if not _m20_hardening_snapshot_fallback_allowed(exc):
            raise
        row = (
            spark.sql('DESCRIBE HISTORY ' + _m20_hardening_sql_identifier(table_name) + ' LIMIT 1')
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

def _m20_hardening_validate_cdf(changes):
    # Keep lazy Spark Connect failures inside the caller's recovery block.
    _ = changes.schema
    changes.limit(1).collect()
    return changes

def _cdf_changed_keys(table_name: str, key_column: str, start_version: int, end_version: int) -> DataFrame:
    if start_version > end_version:
        return _empty_key_df(table_name, key_column)
    try:
        changes = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', int(start_version)).option('endingVersion', int(end_version)).table(table_name).select(F.col(key_column)).filter(F.col(key_column).isNotNull()).dropDuplicates([key_column])
        return _m20_hardening_validate_cdf(changes)
    except Exception as exc:
        message = str(exc)
        cdf_markers = ('change data', 'change feed', 'startingVersion', 'not available', 'CDF', 'DELTA_CHANGE')
        if any((marker.lower() in message.lower() for marker in cdf_markers)):
            raise MapDiagnosisCDFGap('CDF history is unavailable for ' + table_name + ' from version ' + str(start_version) + ' to ' + str(end_version) + '. A full refresh is required.') from exc
        raise

def _read_source_snapshot(table_name: str) -> DataFrame:
    version = _MAP_DIAGNOSIS_ACTIVE_SOURCE_VERSIONS.get(table_name)
    if version is None:
        return spark.table(table_name)
    return _m20_hardening_read_pinned_snapshot(table_name, int(version))

def _current_diagnosis_snapshot() -> DataFrame:
    return _read_source_snapshot(MAP_DIAGNOSIS_SOURCE).filter(F.col('Trust') == F.lit(MAP_DIAGNOSIS_TRUST)).select('DIAGNOSIS_ID', 'PERSON_ID', 'ENCNTR_ID', 'NOMENCLATURE_ID', 'DIAG_DT_TM', 'BEG_EFFECTIVE_DT_TM', 'END_EFFECTIVE_DT_TM', 'DIAG_TYPE_CD', 'DIAG_PRIORITY', 'RANKING_CD', 'DIAG_PRSNL_ID', 'CLINICAL_SERVICE_CD', 'CONFIRMATION_STATUS_CD', 'CLASSIFICATION_CD', 'ACTIVE_IND', 'ACTIVE_STATUS_CD', 'ACTIVE_STATUS_DT_TM', 'CONTRIBUTOR_SYSTEM_CD', 'ORGANIZATION_ID', F.col('Trust').alias('TRUST'), 'UPDT_CNT', 'UPDT_DT_TM', 'LAST_UTC_TS', 'DIAGNOSIS_DISPLAY', 'DIAG_FTDESC', 'DIAGNOSIS_GROUP', 'ORIGINATING_NOMENCLATURE_ID', 'CLINICAL_DIAG_PRIORITY', 'LATERALITY_CD', 'CONDITIONAL_QUAL_CD', 'PROBABILITY', 'SEVERITY_CLASS_CD', 'SEVERITY_CD', 'SEVERITY_FTDESC', 'ASSERTED_DT_TM', 'LIFE_CYCLE_STATUS_CD', 'LIFE_CYCLE_DT_TM', 'ADC_UPDT')
NOMENCLATURE_HASH_COLUMNS = ['SOURCE_IDENTIFIER', 'SOURCE_STRING', 'SOURCE_VOCABULARY_CD', 'VOCAB_AXIS_CD', 'CONCEPT_CKI', 'IS_STANDARD_OMOP_CONCEPT', 'CONCEPT_DOMAIN', 'CONCEPT_CLASS', 'FOUND_CUI', 'OMOP_CONCEPT_ID', 'OMOP_CONCEPT_NAME', 'NUMBER_OF_OMOP_MATCHES', 'OMOP_SIMILARITY', 'SNOMED_CODE', 'SNOMED_TYPE', 'SNOMED_MATCH_COUNT', 'SNOMED_SIMILARITY', 'SNOMED_TERM', 'ICD10_CODE', 'ICD10_CODE_TYPE', 'ICD10_CODE_MATCH_COUNT', 'ICD10_SIMILARITY', 'ICD10_TERM', 'SOURCE_CHANGE_TS', 'IS_ACTIVE']

def _current_nomenclature_projection() -> DataFrame:
    nomenclature = _read_source_snapshot(MAP_DIAGNOSIS_NOMENCLATURE).select('NOMENCLATURE_ID', 'SOURCE_IDENTIFIER', 'SOURCE_STRING', 'SOURCE_VOCABULARY_CD', 'VOCAB_AXIS_CD', 'CONCEPT_CKI', 'IS_STANDARD_OMOP_CONCEPT', 'CONCEPT_DOMAIN', 'CONCEPT_CLASS', 'FOUND_CUI', 'OMOP_CONCEPT_ID', 'OMOP_CONCEPT_NAME', 'NUMBER_OF_OMOP_MATCHES', 'OMOP_SIMILARITY', 'SNOMED_CODE', 'SNOMED_TYPE', 'SNOMED_MATCH_COUNT', 'SNOMED_SIMILARITY', 'SNOMED_TERM', 'ICD10_CODE', 'ICD10_CODE_TYPE', 'ICD10_CODE_MATCH_COUNT', 'ICD10_SIMILARITY', 'ICD10_TERM', 'ADC_UPDT', 'SOURCE_CHANGE_TS', 'IS_ACTIVE')
    projected = nomenclature.withColumn('NOMENCLATURE_MAPPING_HASH', _stable_hash(NOMENCLATURE_HASH_COLUMNS))
    selection = Window.partitionBy('NOMENCLATURE_ID').orderBy(F.col('SOURCE_CHANGE_TS').desc_nulls_last(), F.col('ADC_UPDT').desc_nulls_last(), F.col('IS_ACTIVE').desc_nulls_last(), F.col('NOMENCLATURE_MAPPING_HASH').desc_nulls_last())
    return projected.withColumn('_NOMENCLATURE_RN', F.row_number().over(selection)).where(F.col('_NOMENCLATURE_RN') == 1).drop('_NOMENCLATURE_RN')

def _current_encounter_projection() -> DataFrame:
    return _read_source_snapshot(MAP_DIAGNOSIS_ENCOUNTER).select('ENCNTR_ID', 'ARRIVE_DT_TM', F.col('ADC_UPDT').alias('ENCOUNTER_ADC_UPDT'))

def _current_code_value_projection() -> DataFrame:
    values = _read_source_snapshot(MAP_DIAGNOSIS_CODE_VALUE)
    lookup = values.select('CODE_VALUE', F.coalesce(_nonblank(F.col('DISPLAY')), _nonblank(F.col('DESCRIPTION')), _nonblank(F.col('CDF_MEANING'))).alias('LOOKUP_DESC'), F.col('ADC_UPDT').alias('LOOKUP_ADC_UPDT'))
    selection = Window.partitionBy('CODE_VALUE').orderBy(F.col('LOOKUP_ADC_UPDT').desc_nulls_last(), F.col('LOOKUP_DESC').desc_nulls_last())
    return lookup.withColumn('_CODE_VALUE_RN', F.row_number().over(selection)).where(F.col('_CODE_VALUE_RN') == 1).drop('_CODE_VALUE_RN')
CODE_LOOKUPS = [('DIAG_TYPE_CD', 'diag_type_desc', 'diag_type'), ('CLINICAL_SERVICE_CD', 'clinical_service_desc', 'clinical_service'), ('CONFIRMATION_STATUS_CD', 'confirmation_status_desc', 'confirmation_status'), ('CLASSIFICATION_CD', 'classification_desc', 'classification'), ('SOURCE_VOCABULARY_CD', 'source_vocabulary_desc', 'source_vocabulary'), ('VOCAB_AXIS_CD', 'vocab_axis_desc', 'vocab_axis')]

def _derive_diagnosis_date(df: DataFrame) -> DataFrame:
    derived = F.coalesce(F.col('DIAG_DT_TM_RAW'), F.col('ARRIVE_DT_TM'), F.col('BEG_EFFECTIVE_DT_TM'))
    source = F.when(F.col('DIAG_DT_TM_RAW').isNotNull(), F.lit('DIAG_DT_TM')).when(F.col('ARRIVE_DT_TM').isNotNull(), F.lit('ENCOUNTER_ARRIVE_DT_TM')).when(F.col('BEG_EFFECTIVE_DT_TM').isNotNull(), F.lit('BEG_EFFECTIVE_DT_TM')).otherwise(F.lit(None).cast('string'))
    return df.withColumn('DIAG_DT_TM', derived).withColumn('DIAG_DT_TM_SOURCE', source).withColumn('DIAG_DT_TM_BEFORE_1950_IND', F.when(derived.isNull(), F.lit(None).cast('boolean')).otherwise(derived < F.lit('1950-01-01 00:00:00').cast('timestamp'))).withColumn('DIAG_DT_TM_FUTURE_AT_MAP_IND', F.when(derived.isNull(), F.lit(None).cast('boolean')).otherwise(derived > F.current_timestamp()))

def _earliest_dates_for_groups(diagnosis_snapshot: DataFrame, groups: DataFrame) -> DataFrame:
    group_rows = diagnosis_snapshot.alias('d').join(groups.alias('g'), (F.col('d.PERSON_ID') == F.col('g.PERSON_ID')) & (F.col('d.NOMENCLATURE_ID') == F.col('g.NOMENCLATURE_ID')), 'inner').join(_current_encounter_projection().alias('e'), F.col('d.ENCNTR_ID') == F.col('e.ENCNTR_ID'), 'left').select(F.col('d.PERSON_ID').alias('PERSON_ID'), F.col('d.NOMENCLATURE_ID').alias('NOMENCLATURE_ID'), F.coalesce(F.col('d.DIAG_DT_TM'), F.col('e.ARRIVE_DT_TM'), F.col('d.BEG_EFFECTIVE_DT_TM')).alias('__group_diag_dt_tm'))
    return group_rows.groupBy('PERSON_ID', 'NOMENCLATURE_ID').agg(F.min('__group_diag_dt_tm').alias('earliest_diagnosis_date'))

def _build_map_diagnosis_rows(diagnosis_rows: DataFrame, full_refresh: bool, earliest_dates: Optional[DataFrame]=None) -> DataFrame:
    d = diagnosis_rows.alias('d')
    e = _current_encounter_projection().alias('e')
    n = _current_nomenclature_projection().alias('n')
    base = d.join(e, F.col('d.ENCNTR_ID') == F.col('e.ENCNTR_ID'), 'left').join(n, F.col('d.NOMENCLATURE_ID').cast(DecimalType(38, 18)) == F.col('n.NOMENCLATURE_ID'), 'left').select(F.col('d.DIAGNOSIS_ID'), F.col('d.PERSON_ID'), F.col('d.ENCNTR_ID'), F.col('d.DIAG_DT_TM').alias('DIAG_DT_TM_RAW'), F.col('e.ARRIVE_DT_TM'), F.col('d.BEG_EFFECTIVE_DT_TM'), F.col('d.END_EFFECTIVE_DT_TM'), F.col('d.DIAG_TYPE_CD'), F.col('d.DIAG_PRIORITY'), F.col('d.RANKING_CD'), F.col('d.DIAG_PRSNL_ID'), F.col('d.CLINICAL_SERVICE_CD'), F.col('d.CONFIRMATION_STATUS_CD'), F.col('d.CLASSIFICATION_CD'), F.col('d.ACTIVE_IND'), F.col('d.ACTIVE_STATUS_CD'), F.col('d.ACTIVE_STATUS_DT_TM'), F.col('d.CONTRIBUTOR_SYSTEM_CD'), F.col('d.ORGANIZATION_ID'), F.col('d.TRUST'), F.col('d.UPDT_CNT'), F.col('d.UPDT_DT_TM'), F.col('d.LAST_UTC_TS'), F.col('d.DIAGNOSIS_DISPLAY'), F.col('d.DIAG_FTDESC'), F.col('d.DIAGNOSIS_GROUP'), F.col('d.ORIGINATING_NOMENCLATURE_ID'), F.col('d.CLINICAL_DIAG_PRIORITY'), F.col('d.LATERALITY_CD'), F.col('d.CONDITIONAL_QUAL_CD'), F.col('d.PROBABILITY'), F.col('d.SEVERITY_CLASS_CD'), F.col('d.SEVERITY_CD'), F.col('d.SEVERITY_FTDESC'), F.col('d.ASSERTED_DT_TM'), F.col('d.LIFE_CYCLE_STATUS_CD'), F.col('d.LIFE_CYCLE_DT_TM'), F.col('d.NOMENCLATURE_ID').alias('NOMENCLATURE_ID'), F.col('n.SOURCE_IDENTIFIER'), F.col('n.SOURCE_STRING'), F.col('n.SOURCE_VOCABULARY_CD'), F.col('n.VOCAB_AXIS_CD'), F.col('n.CONCEPT_CKI'), F.col('n.IS_ACTIVE').alias('NOMENCLATURE_IS_ACTIVE'), F.col('n.SOURCE_CHANGE_TS').alias('NOMENCLATURE_SOURCE_CHANGE_TS'), F.col('n.NOMENCLATURE_MAPPING_HASH'), F.col('n.FOUND_CUI'), F.col('n.OMOP_CONCEPT_ID'), F.col('n.OMOP_CONCEPT_NAME'), F.col('n.IS_STANDARD_OMOP_CONCEPT').alias('OMOP_STANDARD_CONCEPT'), F.col('n.NUMBER_OF_OMOP_MATCHES').alias('OMOP_MATCH_NUMBER'), F.col('n.OMOP_SIMILARITY'), F.col('n.CONCEPT_DOMAIN').alias('OMOP_CONCEPT_DOMAIN'), F.col('n.CONCEPT_CLASS').alias('OMOP_CONCEPT_CLASS'), F.col('n.SNOMED_CODE'), F.col('n.SNOMED_TYPE'), F.col('n.SNOMED_MATCH_COUNT').alias('SNOMED_MATCH_NUMBER'), F.col('n.SNOMED_SIMILARITY'), F.col('n.SNOMED_TERM'), F.col('n.ICD10_CODE'), F.col('n.ICD10_CODE_TYPE').alias('ICD10_TYPE'), F.col('n.ICD10_CODE_MATCH_COUNT').alias('ICD10_MATCH_NUMBER'), F.col('n.ICD10_SIMILARITY'), F.col('n.ICD10_TERM'), F.col('d.ADC_UPDT').alias('DIAGNOSIS_ADC_UPDT'), F.col('n.ADC_UPDT').alias('NOMENCLATURE_ADC_UPDT'), F.col('e.ENCOUNTER_ADC_UPDT'))
    base = _derive_diagnosis_date(base)
    if full_refresh:
        complete_group = Window.partitionBy('PERSON_ID', 'NOMENCLATURE_ID')
        base = base.withColumn('earliest_diagnosis_date', F.min('DIAG_DT_TM').over(complete_group))
    else:
        if earliest_dates is None:
            raise ValueError('earliest_dates is required for incremental map_diagnosis rows')
        base = base.join(earliest_dates, ['PERSON_ID', 'NOMENCLATURE_ID'], 'left')
    full_cki = F.col('CONCEPT_CKI')
    base = base.withColumn('CONCEPT_CKI_SOURCE', F.when(F.instr(full_cki, '!') > 0, F.substring_index(full_cki, '!', 1))).withColumn('CONCEPT_CKI_IDENTIFIER', F.when(full_cki.isNotNull(), F.substring_index(full_cki, '!', -1))).withColumn('DIAGNOSIS_TEXT', F.coalesce(_nonblank(F.col('SOURCE_STRING')), _nonblank(F.col('DIAGNOSIS_DISPLAY')), _nonblank(F.col('DIAG_FTDESC')))).withColumn('DIAGNOSIS_TEXT_SOURCE', F.when(_nonblank(F.col('SOURCE_STRING')).isNotNull(), F.lit('SOURCE_STRING')).when(_nonblank(F.col('DIAGNOSIS_DISPLAY')).isNotNull(), F.lit('DIAGNOSIS_DISPLAY')).when(_nonblank(F.col('DIAG_FTDESC')).isNotNull(), F.lit('DIAG_FTDESC')))
    code_values = _current_code_value_projection()
    code_adc_columns = []
    description_columns = []
    result = base
    for source_column, description_column, alias_name in CODE_LOOKUPS:
        code_column = '__' + alias_name + '_code'
        adc_column = '__' + alias_name + '_adc'
        lookup = F.broadcast(code_values.select(F.col('CODE_VALUE').alias(code_column), F.col('LOOKUP_DESC').alias(description_column), F.col('LOOKUP_ADC_UPDT').alias(adc_column)))
        result = result.join(lookup, F.col(source_column).cast('double') == F.col(code_column), 'left').drop(code_column)
        code_adc_columns.append(adc_column)
        description_columns.append(description_column)
    result = result.withColumn('CODE_VALUE_ADC_UPDT', F.greatest(*[F.col(name) for name in code_adc_columns])).withColumn('CODE_VALUE_LOOKUP_HASH', _stable_hash(description_columns)).withColumn('ADC_UPDT', F.greatest(F.col('DIAGNOSIS_ADC_UPDT'), F.col('NOMENCLATURE_ADC_UPDT'), F.col('ENCOUNTER_ADC_UPDT'), F.col('CODE_VALUE_ADC_UPDT')))
    result = result.drop(*code_adc_columns)
    row_hash_exclusions = {'MAP_ROW_HASH', 'MAP_REFRESH_DT_TM', 'ADC_UPDT', 'DIAGNOSIS_ADC_UPDT', 'NOMENCLATURE_ADC_UPDT', 'ENCOUNTER_ADC_UPDT', 'CODE_VALUE_ADC_UPDT'}
    row_hash_columns = [field.name for field in schema_map_diagnosis_v2.fields if field.name not in row_hash_exclusions]
    result = result.withColumn('MAP_ROW_HASH', _stable_hash(row_hash_columns)).withColumn('MAP_REFRESH_DT_TM', F.current_timestamp())
    return _align_to_v2_schema(result)

def _changed_nomenclature_ids(changed_keys: DataFrame) -> DataFrame:
    key_name = 'NOMENCLATURE_ID'
    if _is_empty(changed_keys):
        return changed_keys
    target_hashes = spark.table(MAP_DIAGNOSIS_TARGET).select(F.col('NOMENCLATURE_ID').cast(DecimalType(38, 18)).alias(key_name), 'NOMENCLATURE_MAPPING_HASH').join(changed_keys, key_name, 'left_semi').groupBy(key_name).agg(F.count('NOMENCLATURE_MAPPING_HASH').alias('__hash_count'), F.min('NOMENCLATURE_MAPPING_HASH').alias('__min_hash'), F.max('NOMENCLATURE_MAPPING_HASH').alias('__max_hash'))
    current = _current_nomenclature_projection().select(key_name, 'NOMENCLATURE_MAPPING_HASH').join(changed_keys, key_name, 'inner').withColumnRenamed('NOMENCLATURE_MAPPING_HASH', '__current_hash').withColumn('__current_exists', F.lit(True))
    comparison = target_hashes.alias('t').join(current.alias('c'), key_name, 'left')
    return comparison.filter(F.col('__current_exists').isNull() & (F.col('__hash_count') > 0) | F.col('__current_exists').isNotNull() & ((F.col('__hash_count') == 0) | ~F.col('__min_hash').eqNullSafe(F.col('__current_hash')) | ~F.col('__max_hash').eqNullSafe(F.col('__current_hash')))).select(F.col(key_name)).dropDuplicates([key_name])

def _target_ids_for_nomenclature_ids(nomenclature_ids: DataFrame) -> DataFrame:
    return spark.table(MAP_DIAGNOSIS_TARGET).select('DIAGNOSIS_ID', 'NOMENCLATURE_ID').join(nomenclature_ids.select(F.col('NOMENCLATURE_ID').cast('double').alias('__changed_nom_id')), F.col('NOMENCLATURE_ID') == F.col('__changed_nom_id'), 'left_semi').select('DIAGNOSIS_ID').dropDuplicates(['DIAGNOSIS_ID'])

def _target_ids_for_encounter_keys(encounter_keys: DataFrame) -> DataFrame:
    if _is_empty(encounter_keys):
        return _empty_key_df(MAP_DIAGNOSIS_SOURCE, 'DIAGNOSIS_ID')
    affected = spark.table(MAP_DIAGNOSIS_TARGET).alias('t').filter(F.col('t.DIAG_DT_TM_RAW').isNull()).select(F.col('t.DIAGNOSIS_ID').alias('DIAGNOSIS_ID'), F.col('t.ENCNTR_ID').alias('ENCNTR_ID'), F.col('t.ARRIVE_DT_TM').alias('__target_arrive_dt_tm')).join(encounter_keys, 'ENCNTR_ID', 'left_semi').alias('x')
    return affected.join(_current_encounter_projection().alias('e'), F.col('x.ENCNTR_ID') == F.col('e.ENCNTR_ID'), 'left').filter(~F.col('x.__target_arrive_dt_tm').eqNullSafe(F.col('e.ARRIVE_DT_TM'))).select(F.col('x.DIAGNOSIS_ID').alias('DIAGNOSIS_ID')).dropDuplicates(['DIAGNOSIS_ID'])

def _target_ids_for_code_values(code_values: DataFrame) -> DataFrame:
    if _is_empty(code_values):
        return _empty_key_df(MAP_DIAGNOSIS_SOURCE, 'DIAGNOSIS_ID')
    code_columns = ['DIAG_TYPE_CD', 'CLINICAL_SERVICE_CD', 'CONFIRMATION_STATUS_CD', 'CLASSIFICATION_CD', 'SOURCE_VOCABULARY_CD', 'VOCAB_AXIS_CD']
    long_codes = spark.table(MAP_DIAGNOSIS_TARGET).select('DIAGNOSIS_ID', F.explode(F.array(*[F.col(name).cast('double') for name in code_columns])).alias('CODE_VALUE')).filter(F.col('CODE_VALUE').isNotNull())
    changed = code_values.select(F.col('CODE_VALUE').cast('double').alias('CODE_VALUE'))
    return long_codes.join(F.broadcast(changed), 'CODE_VALUE', 'left_semi').select('DIAGNOSIS_ID').dropDuplicates(['DIAGNOSIS_ID'])

def _requires_full_refresh(checkpoints: Dict[str, int], end_versions: Dict[str, int]) -> bool:
    if not spark.catalog.tableExists(MAP_DIAGNOSIS_TARGET):
        return True
    properties = _target_table_properties()
    if properties.get('pipeline.map_diagnosis.schema_version') != MAP_DIAGNOSIS_SCHEMA_VERSION:
        print('[INFO] map_diagnosis schema-version property is absent or outdated.')
        return True
    expected_schema = {
        field.name.lower(): field.dataType.simpleString()
        for field in bronze_contract_schema(MAP_DIAGNOSIS_TARGET).fields
    }
    current_schema = {field.name.lower(): field.dataType.simpleString() for field in spark.table(MAP_DIAGNOSIS_TARGET).schema.fields}
    if current_schema != expected_schema:
        print('[INFO] Target schema does not exactly match the v2 replacement schema.')
        return True
    expected_tables = set(MAP_DIAGNOSIS_SOURCES.values())
    if not expected_tables.issubset(set(checkpoints)):
        print('[INFO] Source-specific checkpoints are incomplete; a full corrective refresh is required.')
        return True
    for source_name, table_name in MAP_DIAGNOSIS_SOURCES.items():
        if checkpoints[table_name] > end_versions[source_name]:
            print('[INFO] Source ' + table_name + ' has a lower current Delta version than its checkpoint; a full refresh is required.')
            return True
    legacy_row_exists = spark.table(MAP_DIAGNOSIS_TARGET).filter(F.col('MAP_ROW_HASH').isNull()).limit(1).count() > 0
    if legacy_row_exists:
        print('[INFO] Legacy target rows do not have v2 hashes; a full corrective refresh is required.')
        return True
    return False

def _prepare_full_refresh(end_versions: Dict[str, int], run_id: str, minimum_source_ratio: float) -> Dict:
    diagnosis_snapshot = _current_diagnosis_snapshot()
    source_count = diagnosis_snapshot.count()
    target_count = spark.table(MAP_DIAGNOSIS_TARGET).count()
    if target_count > 0 and source_count < target_count * minimum_source_ratio:
        print('[WARN] Current diagnosis source count is below the historical target ratio. Continuing because the version-pinned source snapshot, not the old target population, is authoritative for a reviewed full rebuild.')
    if source_count == 0:
        raise RuntimeError('Full refresh refused because the current Barts diagnosis snapshot is empty.')
    rows = _build_map_diagnosis_rows(diagnosis_snapshot, full_refresh=True)
    print('[INFO] Prepared full map_diagnosis refresh from ' + str(source_count) + ' current source rows.')
    return {'run_id': run_id, 'mode': 'full', 'rows': rows, 'delete_ids': None, 'has_rows': True, 'has_deletes': False, 'end_versions': end_versions, 'persisted': [diagnosis_snapshot]}

def _prepare_incremental_refresh(checkpoints: Dict[str, int], end_versions: Dict[str, int], run_id: str) -> Dict:
    changed = {}
    key_columns = {'diagnosis': 'DIAGNOSIS_ID', 'nomenclature': 'NOMENCLATURE_ID', 'encounter': 'ENCNTR_ID', 'code_value': 'CODE_VALUE'}
    for source_name, table_name in MAP_DIAGNOSIS_SOURCES.items():
        changed[source_name] = _cdf_changed_keys(table_name, key_columns[source_name], checkpoints[table_name] + 1, end_versions[source_name])
    diagnosis_ids = changed['diagnosis'].select('DIAGNOSIS_ID')
    changed_nom_ids = _changed_nomenclature_ids(changed['nomenclature'])
    nomenclature_diagnosis_ids = _target_ids_for_nomenclature_ids(changed_nom_ids) if not _is_empty(changed_nom_ids) else _empty_key_df(MAP_DIAGNOSIS_SOURCE, 'DIAGNOSIS_ID')
    encounter_diagnosis_ids = _target_ids_for_encounter_keys(changed['encounter'])
    code_diagnosis_ids = _target_ids_for_code_values(changed['code_value'])
    direct_refresh_ids = reduce(lambda left, right: left.unionByName(right), [diagnosis_ids, nomenclature_diagnosis_ids, encounter_diagnosis_ids, code_diagnosis_ids]).dropDuplicates(['DIAGNOSIS_ID'])
    group_seed_ids = diagnosis_ids.unionByName(encounter_diagnosis_ids).dropDuplicates(['DIAGNOSIS_ID'])
    diagnosis_snapshot = _current_diagnosis_snapshot()
    persisted = [diagnosis_snapshot]
    current_diagnosis_ids = diagnosis_snapshot.select('DIAGNOSIS_ID')
    delete_ids = diagnosis_ids.join(current_diagnosis_ids, 'DIAGNOSIS_ID', 'left_anti').dropDuplicates(['DIAGNOSIS_ID'])
    persisted.append(delete_ids)
    has_deletes = not _is_empty(delete_ids)
    if _is_empty(group_seed_ids):
        group_member_ids = _empty_key_df(MAP_DIAGNOSIS_SOURCE, 'DIAGNOSIS_ID')
    else:
        old_groups = spark.table(MAP_DIAGNOSIS_TARGET).select('DIAGNOSIS_ID', 'PERSON_ID', 'NOMENCLATURE_ID').join(group_seed_ids, 'DIAGNOSIS_ID', 'left_semi').select('PERSON_ID', 'NOMENCLATURE_ID')
        new_groups = diagnosis_snapshot.select('DIAGNOSIS_ID', 'PERSON_ID', 'NOMENCLATURE_ID').join(group_seed_ids, 'DIAGNOSIS_ID', 'left_semi').select('PERSON_ID', 'NOMENCLATURE_ID')
        affected_groups = old_groups.unionByName(new_groups).dropDuplicates(['PERSON_ID', 'NOMENCLATURE_ID'])
        group_member_ids = diagnosis_snapshot.select('DIAGNOSIS_ID', 'PERSON_ID', 'NOMENCLATURE_ID').join(affected_groups, ['PERSON_ID', 'NOMENCLATURE_ID'], 'left_semi').select('DIAGNOSIS_ID').dropDuplicates(['DIAGNOSIS_ID'])
    refresh_ids = direct_refresh_ids.unionByName(group_member_ids).dropDuplicates(['DIAGNOSIS_ID'])
    persisted.append(refresh_ids)
    has_rows = not _is_empty(refresh_ids)
    if has_rows:
        refresh_source_rows = diagnosis_snapshot.join(refresh_ids, 'DIAGNOSIS_ID', 'inner')
        refresh_groups = refresh_source_rows.select('PERSON_ID', 'NOMENCLATURE_ID').dropDuplicates(['PERSON_ID', 'NOMENCLATURE_ID'])
        earliest_dates = _earliest_dates_for_groups(diagnosis_snapshot, refresh_groups)
        rows = _build_map_diagnosis_rows(refresh_source_rows, full_refresh=False, earliest_dates=earliest_dates)
    else:
        rows = _empty_map_rows()
    print('[INFO] Prepared incremental map_diagnosis update. ' + 'refresh_rows=' + str(has_rows) + ', deletes=' + str(has_deletes) + '.')
    return {'run_id': run_id, 'mode': 'incremental', 'rows': rows, 'delete_ids': delete_ids, 'has_rows': has_rows, 'has_deletes': has_deletes, 'end_versions': end_versions, 'persisted': persisted}

def prepare_map_diagnosis_update(force_full_refresh: Optional[bool]=None, auto_full_refresh_on_cdf_gap: bool=False, minimum_full_refresh_source_ratio: float=0.98) -> Dict:
    global _MAP_DIAGNOSIS_ACTIVE_SOURCE_VERSIONS
    ensure_map_diagnosis_v2_schema()
    ensure_map_diagnosis_checkpoint_table()
    run_id = uuid4().hex
    end_versions = _latest_source_versions()
    _MAP_DIAGNOSIS_ACTIVE_SOURCE_VERSIONS = {MAP_DIAGNOSIS_SOURCES[source_name]: version for source_name, version in end_versions.items()}
    checkpoints = _checkpoint_versions()
    auto_requires_full = _requires_full_refresh(checkpoints, end_versions)
    if force_full_refresh is True or (force_full_refresh is None and auto_requires_full):
        return _prepare_full_refresh(end_versions, run_id, minimum_full_refresh_source_ratio)
    if force_full_refresh is False and auto_requires_full:
        raise RuntimeError('Incremental mode was explicitly requested, but the target/checkpoint state requires a full refresh.')
    try:
        return _prepare_incremental_refresh(checkpoints, end_versions, run_id)
    except MapDiagnosisCDFGap:
        if not auto_full_refresh_on_cdf_gap:
            raise
        print('[WARN] A source CDF range has expired; switching to a guarded full refresh.')
        return _prepare_full_refresh(end_versions, run_id, minimum_full_refresh_source_ratio)

def _overwrite_map_diagnosis_rows(rows: DataFrame) -> None:
    bronze_project_contract(
        rows, MAP_DIAGNOSIS_TARGET
    ).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('delta.enableChangeDataFeed', 'true').option('delta.enableDeletionVectors', 'true').option('delta.enableRowTracking', 'true').saveAsTable(MAP_DIAGNOSIS_TARGET)
    _set_map_diagnosis_metadata(apply_column_comments=True)

def _merge_map_diagnosis_rows(rows: DataFrame, full_refresh: bool) -> None:
    rows = bronze_project_contract(rows, MAP_DIAGNOSIS_TARGET)
    target_columns = list(rows.columns)
    insert_values = {name: 's.' + name for name in target_columns}
    update_values = {
        name: 's.' + name
        for name in target_columns
        if name != 'DIAGNOSIS_ID'
    }
    comparisons = ' OR '.join(
        f'NOT (t.`{name}` <=> s.`{name}`)'
        for name in target_columns
        if name != 'DIAGNOSIS_ID'
    )
    merge = DeltaTable.forName(spark, MAP_DIAGNOSIS_TARGET).alias('t').merge(
        rows.alias('s'),
        't.DIAGNOSIS_ID <=> s.DIAGNOSIS_ID',
    ).whenMatchedUpdate(
        condition=comparisons or 'false',
        set=update_values,
    ).whenNotMatchedInsert(values=insert_values)
    if full_refresh:
        merge = merge.whenNotMatchedBySourceDelete()
    merge.execute()

def _delete_map_diagnosis_ids(delete_ids: DataFrame) -> None:
    keys = delete_ids.select(_checked_long(F.col('DIAGNOSIS_ID'), 'DIAGNOSIS_ID', required=True).alias('DIAGNOSIS_ID')).dropDuplicates(['DIAGNOSIS_ID'])
    DeltaTable.forName(spark, MAP_DIAGNOSIS_TARGET).alias('t').merge(keys.alias('d'), 't.DIAGNOSIS_ID <=> d.DIAGNOSIS_ID').whenMatchedDelete().execute()

def commit_map_diagnosis_update(plan: Dict) -> Dict:
    global _MAP_DIAGNOSIS_ACTIVE_SOURCE_VERSIONS
    try:
        if plan['mode'] == 'full':
            _overwrite_map_diagnosis_rows(plan['rows'])
        else:
            if plan['has_rows']:
                _merge_map_diagnosis_rows(plan['rows'], full_refresh=False)
            if plan['has_deletes']:
                _delete_map_diagnosis_ids(plan['delete_ids'])
            _set_map_diagnosis_metadata(apply_column_comments=False)
        _write_checkpoint_versions(plan['end_versions'], plan['run_id'])
        print('[SUCCESS] map_diagnosis ' + plan['mode'] + ' update committed with run_id=' + plan['run_id'] + '.')
        return {'run_id': plan['run_id'], 'mode': plan['mode'], 'checkpoint_versions': plan['end_versions']}
    finally:
        _MAP_DIAGNOSIS_ACTIVE_SOURCE_VERSIONS = {}
        for frame in plan.get('persisted', []):
            try:
                None
            except Exception:
                pass

def run_map_diagnosis_update(force_full_refresh: Optional[bool]=None, auto_full_refresh_on_cdf_gap: bool=False, minimum_full_refresh_source_ratio: float=0.98) -> Dict:
    global _MAP_DIAGNOSIS_ACTIVE_SOURCE_VERSIONS
    try:
        plan = prepare_map_diagnosis_update(force_full_refresh=force_full_refresh, auto_full_refresh_on_cdf_gap=auto_full_refresh_on_cdf_gap, minimum_full_refresh_source_ratio=minimum_full_refresh_source_ratio)
    except Exception:
        _MAP_DIAGNOSIS_ACTIVE_SOURCE_VERSIONS = {}
        raise
    return commit_map_diagnosis_update(plan)
if '_MAP_UPDATES_ORIGINAL_UPDATE_TABLE' not in globals():
    _MAP_UPDATES_ORIGINAL_UPDATE_TABLE = globals().get('update_table')
_MAP_DIAGNOSIS_PENDING_PLAN = None

def create_diagnosis_mapping_incr():
    global _MAP_DIAGNOSIS_PENDING_PLAN
    global _MAP_DIAGNOSIS_ACTIVE_SOURCE_VERSIONS
    if _MAP_DIAGNOSIS_PENDING_PLAN is not None:
        raise RuntimeError('A map_diagnosis plan is already pending. Commit it before preparing another.')
    try:
        _MAP_DIAGNOSIS_PENDING_PLAN = prepare_map_diagnosis_update()
    except Exception:
        _MAP_DIAGNOSIS_ACTIVE_SOURCE_VERSIONS = {}
        raise
    return _MAP_DIAGNOSIS_PENDING_PLAN['rows']

def update_table(source_df, target_table: str, index_column, target_schema: StructType=None, table_comment: str=None, update_condition: str=None):
    global _MAP_DIAGNOSIS_PENDING_PLAN
    if _normalized_table_name(target_table) == _normalized_table_name(MAP_DIAGNOSIS_TARGET):
        if _MAP_DIAGNOSIS_PENDING_PLAN is None:
            raise RuntimeError('No prepared map_diagnosis v2 plan exists. Call create_diagnosis_mapping_incr first.')
        result = commit_map_diagnosis_update(_MAP_DIAGNOSIS_PENDING_PLAN)
        _MAP_DIAGNOSIS_PENDING_PLAN = None
        return result
    if _MAP_UPDATES_ORIGINAL_UPDATE_TABLE is None:
        raise RuntimeError('The original Map Pipeline update_table function was not defined when Map Updates was imported.')
    return _MAP_UPDATES_ORIGINAL_UPDATE_TABLE(source_df, target_table, index_column, target_schema, table_comment, update_condition)

def validate_map_diagnosis_v2() -> Dict[str, DataFrame]:
    target_sql = _sql_name(MAP_DIAGNOSIS_TARGET)
    source_sql = _sql_name(MAP_DIAGNOSIS_SOURCE)
    nomenclature_sql = _sql_name(MAP_DIAGNOSIS_NOMENCLATURE)
    return {'coverage': spark.sql("\n            WITH source_ids AS (\n                SELECT DISTINCT DIAGNOSIS_ID\n                FROM {source}\n                WHERE Trust = '{trust}'\n            ),\n            target_ids AS (\n                SELECT DISTINCT DIAGNOSIS_ID\n                FROM {target}\n            )\n            SELECT\n                (SELECT COUNT(*) FROM source_ids) AS source_ids,\n                (SELECT COUNT(*) FROM target_ids) AS target_ids,\n                (SELECT COUNT(*) FROM source_ids LEFT ANTI JOIN target_ids USING (DIAGNOSIS_ID)) AS missing_target_ids,\n                (SELECT COUNT(*) FROM target_ids LEFT ANTI JOIN source_ids USING (DIAGNOSIS_ID)) AS stale_target_ids\n            ".format(source=source_sql, target=target_sql, trust=MAP_DIAGNOSIS_TRUST.replace("'", "''"))), 'integrity': spark.sql('\n            SELECT\n                COUNT(*) AS rows,\n                COUNT(DISTINCT DIAGNOSIS_ID) AS distinct_diagnosis_ids,\n                COUNT_IF(MAP_ROW_HASH IS NULL) AS null_row_hashes,\n                COUNT_IF(DIAGNOSIS_TEXT IS NULL) AS missing_diagnosis_text,\n                COUNT_IF(DIAG_DT_TM_SOURCE IS NULL) AS missing_date_source\n            FROM {target}\n            '.format(target=target_sql)), 'earliest_dates': spark.sql('\n            WITH expected AS (\n                SELECT PERSON_ID, NOMENCLATURE_ID, MIN(DIAG_DT_TM) AS expected_earliest\n                FROM {target}\n                GROUP BY PERSON_ID, NOMENCLATURE_ID\n            )\n            SELECT COUNT_IF(NOT (t.earliest_diagnosis_date <=> e.expected_earliest)) AS incorrect_rows\n            FROM {target} t\n            JOIN expected e\n              ON t.PERSON_ID <=> e.PERSON_ID\n             AND t.NOMENCLATURE_ID <=> e.NOMENCLATURE_ID\n            '.format(target=target_sql)), 'nomenclature_freshness': spark.sql("\n            SELECT\n                COUNT_IF(NOT (t.NOMENCLATURE_MAPPING_HASH <=> n.current_hash)) AS stale_mapping_rows\n            FROM {target} t\n            LEFT JOIN (\n                SELECT\n                    NOMENCLATURE_ID,\n                    SHA2(\n                        TO_JSON(\n                            NAMED_STRUCT(\n                                'SOURCE_IDENTIFIER', SOURCE_IDENTIFIER,\n                                'SOURCE_STRING', SOURCE_STRING,\n                                'SOURCE_VOCABULARY_CD', SOURCE_VOCABULARY_CD,\n                                'VOCAB_AXIS_CD', VOCAB_AXIS_CD,\n                                'CONCEPT_CKI', CONCEPT_CKI,\n                                'IS_STANDARD_OMOP_CONCEPT', IS_STANDARD_OMOP_CONCEPT,\n                                'CONCEPT_DOMAIN', CONCEPT_DOMAIN,\n                                'CONCEPT_CLASS', CONCEPT_CLASS,\n                                'FOUND_CUI', FOUND_CUI,\n                                'OMOP_CONCEPT_ID', OMOP_CONCEPT_ID,\n                                'OMOP_CONCEPT_NAME', OMOP_CONCEPT_NAME,\n                                'NUMBER_OF_OMOP_MATCHES', NUMBER_OF_OMOP_MATCHES,\n                                'OMOP_SIMILARITY', OMOP_SIMILARITY,\n                                'SNOMED_CODE', SNOMED_CODE,\n                                'SNOMED_TYPE', SNOMED_TYPE,\n                                'SNOMED_MATCH_COUNT', SNOMED_MATCH_COUNT,\n                                'SNOMED_SIMILARITY', SNOMED_SIMILARITY,\n                                'SNOMED_TERM', SNOMED_TERM,\n                                'ICD10_CODE', ICD10_CODE,\n                                'ICD10_CODE_TYPE', ICD10_CODE_TYPE,\n                                'ICD10_CODE_MATCH_COUNT', ICD10_CODE_MATCH_COUNT,\n                                'ICD10_SIMILARITY', ICD10_SIMILARITY,\n                                'ICD10_TERM', ICD10_TERM,\n                                'SOURCE_CHANGE_TS', SOURCE_CHANGE_TS,\n                                'IS_ACTIVE', IS_ACTIVE\n                            ),\n                            MAP('ignoreNullFields', 'false')\n                        ),\n                        256\n                    ) AS current_hash\n                FROM {nomenclature}\n            ) n\n              ON CAST(t.NOMENCLATURE_ID AS DECIMAL(38,18)) = n.NOMENCLATURE_ID\n            ".format(target=target_sql, nomenclature=nomenclature_sql))}

try:
    run_map_diagnosis_update(force_full_refresh=(True if _pipeline_component_force_full_refresh('map_diagnosis') else None), auto_full_refresh_on_cdf_gap=True)
    _PIPELINE_UPDATED_TARGETS.extend(['4_prod.bronze.map_diagnosis'])
    _pipeline_mark_component_complete('map_diagnosis', ['4_prod.bronze.map_diagnosis'])
    _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_diagnosis'})
except Exception as exc:
    _pipeline_record_error(None, exc)
    raise
finally:
    if _pipeline_shared_update_table is not None:
        update_table = _pipeline_shared_update_table
    if _pipeline_shared_table_exists is not None:
        table_exists = _pipeline_shared_table_exists
    if _pipeline_shared_get_max_timestamp is not None:
        get_max_timestamp = _pipeline_shared_get_max_timestamp
    if _pipeline_shared_has_cdf_enabled is not None:
        has_cdf_enabled = _pipeline_shared_has_cdf_enabled
    if _pipeline_shared_get_incremental is not None:
        get_incremental_data_with_cdf = _pipeline_shared_get_incremental

# COMMAND ----------

_pipeline_component_start("map_problem")
_pipeline_shared_update_table = globals().get('update_table')
_pipeline_shared_table_exists = globals().get('table_exists')
_pipeline_shared_get_max_timestamp = globals().get('get_max_timestamp')
_pipeline_shared_has_cdf_enabled = globals().get('has_cdf_enabled')
_pipeline_shared_get_incremental = globals().get('get_incremental_data_with_cdf')
globals().pop('_map_updates_original_update_table', None)
globals().pop('_map_medical_personnel_original_update_table', None)
globals().pop('_MAP_UPDATES_ORIGINAL_UPDATE_TABLE', None)

from datetime import datetime, timezone
from functools import reduce
import json
from typing import Dict, List, Optional, Sequence, Tuple
from uuid import uuid4
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
MAP_PROBLEM_PIPELINE = 'map_problem_v3'
MAP_PROBLEM_TARGET = '4_prod.bronze.map_problem'
MAP_PROBLEM_HISTORY = '4_prod.bronze.map_problem_history'
MAP_PROBLEM_SOURCE = '4_prod.raw.mill_problem'
MAP_PROBLEM_NOMENCLATURE = '3_lookup.mill.map_nomenclature'
MAP_PROBLEM_ENCOUNTER = '4_prod.bronze.map_encounter'
MAP_PROBLEM_CODE_VALUE = '3_lookup.mill.mill_code_value'
MAP_PROBLEM_CHECKPOINT = '6_mgmt.bronze.map_problem_pipeline_state'
MAP_PROBLEM_AUDIT = '6_mgmt.bronze.map_problem_pipeline_audit'
MAP_PROBLEM_SCHEMA_VERSION = '3.0.0'
MAP_PROBLEM_CDF_RETENTION = 'interval 30 days'
MAP_PROBLEM_SOURCES = {'problem': MAP_PROBLEM_SOURCE, 'nomenclature': MAP_PROBLEM_NOMENCLATURE, 'encounter': MAP_PROBLEM_ENCOUNTER, 'code_value': MAP_PROBLEM_CODE_VALUE}
_MAP_PROBLEM_ACTIVE_SOURCE_VERSIONS: Dict[str, int] = {}
_MAP_PROBLEM_PENDING_PLAN: Optional[Dict] = None
map_problem_comment = 'One current row per source PROBLEM_ID across all Trust values. The selected source revision is explicit and auditable, raw state and date fields are retained, no clinical or date filters are applied, and current nomenclature/code-value/encounter enrichments are refreshed from source-specific Delta CDF checkpoints. Complete source revision history is retained in 4_prod.bronze.map_problem_history.'
map_problem_history_comment = 'One row per source PROBLEM_INSTANCE_ID, including inactive revisions and retained tombstones for physical source deletes. No Trust, active-status, date, or clinical filters are applied. Current enrichment fields are recalculated when upstream mapping, code-value, or encounter data changes.'

def _sf(name: str, data_type: T.DataType, comment: str) -> T.StructField:
    return T.StructField(name, data_type, True, metadata={'comment': comment})
_PROBLEM_SCHEMA_FIELDS = [_sf('PROBLEM_ID', T.LongType(), 'Compatibility primary key: one current row per source problem.'), _sf('PROBLEM_INSTANCE_ID', T.LongType(), 'Selected source revision identifier.'), _sf('PROBLEM_UUID', T.StringType(), 'Source problem UUID.'), _sf('PROBLEM_INSTANCE_UUID', T.StringType(), 'Source problem revision UUID.'), _sf('PROBLEM_REVISION_COUNT', T.LongType(), 'Number of current source revisions for PROBLEM_ID.'), _sf('PROBLEM_REVISION_RANK', T.LongType(), 'Deterministic revision rank; 1 is the current selected revision.'), _sf('IS_CURRENT_PROBLEM_REVISION', T.BooleanType(), 'True for the revision selected for map_problem.'), _sf('PERSON_ID', T.LongType(), 'Source person identifier.'), _sf('NOMENCLATURE_ID', T.LongType(), 'Source current nomenclature identifier.'), _sf('ORIGINATING_NOMENCLATURE_ID', T.LongType(), 'Source originating nomenclature identifier.'), _sf('ORGANIZATION_ID', T.LongType(), 'Source organization identifier.'), _sf('TRUST', T.StringType(), 'Unfiltered source Trust value retained for provenance.'), _sf('INST_ID', T.LongType(), 'Source instance identifier.'), _sf('TXN_ID_TEXT', T.StringType(), 'Source transaction identifier text.'), _sf('PROBLEM_FTDESC', T.StringType(), 'Source free-text problem description.'), _sf('ANNOTATED_DISPLAY', T.StringType(), 'Source clinician-facing annotated display.'), _sf('PROBLEM_DISPLAY', T.StringType(), 'First nonblank clinical display from annotated text, free text, nomenclature, SNOMED, OMOP, or ICD-10.'), _sf('PROBLEM_DISPLAY_SOURCE', T.StringType(), 'Column used to populate PROBLEM_DISPLAY.'), _sf('ESTIMATED_RESOLUTION_DT_TM', T.TimestampType(), 'Source estimated resolution datetime.'), _sf('ACTUAL_RESOLUTION_DT_TM', T.TimestampType(), 'Source actual resolution datetime.'), _sf('CLASSIFICATION_CD', T.LongType(), 'Source problem classification code.'), _sf('classification_desc', T.StringType(), 'Best available code-value label for CLASSIFICATION_CD.'), _sf('PERSISTENCE_CD', T.LongType(), 'Source persistence code.'), _sf('persistence_desc', T.StringType(), 'Best available code-value label for PERSISTENCE_CD.'), _sf('CONFIRMATION_STATUS_CD', T.LongType(), 'Source confirmation status code.'), _sf('confirmation_status_desc', T.StringType(), 'Best available code-value label for CONFIRMATION_STATUS_CD.'), _sf('LIFE_CYCLE_STATUS_CD', T.LongType(), 'Source problem lifecycle status code.'), _sf('life_cycle_status_desc', T.StringType(), 'Best available code-value label for LIFE_CYCLE_STATUS_CD.'), _sf('LIFE_CYCLE_DT_TM', T.TimestampType(), 'Source lifecycle status datetime.'), _sf('LIFE_CYCLE_DT_CD', T.LongType(), 'Source lifecycle datetime precision code.'), _sf('LIFE_CYCLE_DT_FLAG', T.LongType(), 'Source lifecycle datetime precision flag.'), _sf('RANKING_CD', T.LongType(), 'Source user-defined problem ranking code.'), _sf('ranking_desc', T.StringType(), 'Best available code-value label for RANKING_CD.'), _sf('CERTAINTY_CD', T.LongType(), 'Source qualitative certainty code.'), _sf('certainty_desc', T.StringType(), 'Best available code-value label for CERTAINTY_CD.'), _sf('PROBABILITY', T.DoubleType(), 'Source numeric probability.'), _sf('PERSON_AWARE_CD', T.LongType(), 'Source patient-awareness code.'), _sf('person_aware_desc', T.StringType(), 'Best available code-value label for PERSON_AWARE_CD.'), _sf('PROGNOSIS_CD', T.LongType(), 'Source prognosis code.'), _sf('prognosis_desc', T.StringType(), 'Best available code-value label for PROGNOSIS_CD.'), _sf('PERSON_AWARE_PROGNOSIS_CD', T.LongType(), 'Source patient-awareness-of-prognosis code.'), _sf('person_aware_prognosis_desc', T.StringType(), 'Best available code-value label for PERSON_AWARE_PROGNOSIS_CD.'), _sf('FAMILY_AWARE_CD', T.LongType(), 'Source family-awareness code.'), _sf('family_aware_desc', T.StringType(), 'Best available code-value label for FAMILY_AWARE_CD.'), _sf('SENSITIVITY', T.DoubleType(), 'Source sensitivity value.'), _sf('COURSE_CD', T.LongType(), 'Source disease-course code.'), _sf('course_desc', T.StringType(), 'Best available code-value label for COURSE_CD.'), _sf('CANCEL_REASON_CD', T.LongType(), 'Source cancellation-reason code.'), _sf('cancel_reason_desc', T.StringType(), 'Best available code-value label for CANCEL_REASON_CD.'), _sf('QUALIFIER_CD', T.LongType(), 'Source problem qualifier code.'), _sf('qualifier_desc', T.StringType(), 'Best available code-value label for QUALIFIER_CD.'), _sf('SEVERITY_CLASS_CD', T.LongType(), 'Source severity classification code.'), _sf('severity_class_desc', T.StringType(), 'Best available code-value label for SEVERITY_CLASS_CD.'), _sf('SEVERITY_CD', T.LongType(), 'Source severity code.'), _sf('severity_desc', T.StringType(), 'Best available code-value label for SEVERITY_CD.'), _sf('SEVERITY_FTDESC', T.StringType(), 'Source free-text severity description.'), _sf('LATERALITY_CD', T.LongType(), 'Source laterality code.'), _sf('laterality_desc', T.StringType(), 'Best available code-value label for LATERALITY_CD.'), _sf('COND_TYPE_FLAG', T.LongType(), 'Source condition-type flag.'), _sf('PROBLEM_TYPE_FLAG', T.LongType(), 'Source problem-type flag: problem, medical history, or pregnancy.'), _sf('SHOW_IN_PM_HISTORY_IND', T.LongType(), 'Source display-in-past-medical-history indicator.'), _sf('IMPAIRMENT_TYPE_CD', T.LongType(), 'Source impairment-type code.'), _sf('impairment_type_desc', T.StringType(), 'Best available code-value label for IMPAIRMENT_TYPE_CD.'), _sf('OTHER_IMPAIRMENT_TEXT', T.StringType(), 'Source free-text impairment description.'), _sf('ONSET_DT_CD', T.LongType(), 'Source onset date code.'), _sf('ONSET_DT_TM', T.TimestampType(), 'Unmodified source onset datetime.'), _sf('ONSET_DT_FLAG', T.LongType(), 'Source onset date precision flag.'), _sf('ONSET_TZ', T.LongType(), 'Source onset timezone code.'), _sf('ASSERTED_DT_TM', T.TimestampType(), 'Source asserted datetime.'), _sf('ASSERTED_DT_FLAG', T.LongType(), 'Source asserted date precision flag.'), _sf('ASSERTED_DT_CD', T.LongType(), 'Source asserted date precision code.'), _sf('ASSERTED_TZ', T.LongType(), 'Source asserted timezone code.'), _sf('STATUS_UPDT_DT_TM', T.TimestampType(), 'Source problem status-update datetime.'), _sf('STATUS_UPDT_PRECISION_CD', T.LongType(), 'Source status-update precision code.'), _sf('status_updt_precision_desc', T.StringType(), 'Best available code-value label for STATUS_UPDT_PRECISION_CD.'), _sf('STATUS_UPDT_FLAG', T.LongType(), 'Source status-update precision flag.'), _sf('BEG_EFFECTIVE_DT_TM', T.TimestampType(), 'Source row beginning effective datetime.'), _sf('END_EFFECTIVE_DT_TM', T.TimestampType(), 'Source row ending effective datetime.'), _sf('BEG_EFFECTIVE_TZ', T.LongType(), 'Source beginning-effective timezone code.'), _sf('LIFE_CYCLE_TZ', T.LongType(), 'Source lifecycle timezone code.'), _sf('CALC_DT_TM', T.TimestampType(), 'Compatibility problem event datetime with explicit source precedence.'), _sf('CALC_DT_TM_SOURCE', T.StringType(), 'Source selected for CALC_DT_TM.'), _sf('CALC_DT_TM_BEFORE_1950_IND', T.BooleanType(), 'True when CALC_DT_TM precedes 1950-01-01; no row is filtered.'), _sf('CALC_DT_TM_FUTURE_AT_MAP_IND', T.BooleanType(), 'True when CALC_DT_TM was future-dated when mapped; no row is filtered.'), _sf('earliest_problem_date', T.TimestampType(), 'Earliest unfiltered ONSET_DT_TM for the complete PERSON_ID/NOMENCLATURE_ID group.'), _sf('ACTIVE_IND', T.LongType(), 'Unmodified source active indicator; inactive rows are retained.'), _sf('ACTIVE_STATUS_CD', T.LongType(), 'Unmodified source row active-status code.'), _sf('active_status_desc', T.StringType(), 'Best available code-value label for ACTIVE_STATUS_CD.'), _sf('ACTIVE_STATUS_DT_TM', T.TimestampType(), 'Source row active-status datetime.'), _sf('ACTIVE_STATUS_PRSNL_ID', T.LongType(), 'Unmodified source active-status personnel identifier, including sentinel values.'), _sf('DATA_STATUS_CD', T.LongType(), 'Unmodified source data-status code.'), _sf('data_status_desc', T.StringType(), 'Best available code-value label for DATA_STATUS_CD.'), _sf('DATA_STATUS_DT_TM', T.TimestampType(), 'Source data-status datetime.'), _sf('DATA_STATUS_PRSNL_ID', T.LongType(), 'Source data-status personnel identifier.'), _sf('DEL_IND', T.LongType(), 'Unmodified source delete indicator.'), _sf('CONTRIBUTOR_SYSTEM_CD', T.LongType(), 'Source contributor-system code.'), _sf('contributor_system_desc', T.StringType(), 'Best available code-value label for CONTRIBUTOR_SYSTEM_CD.'), _sf('UPDT_APPLCTX', T.LongType(), 'Source update application context.'), _sf('UPDT_CNT', T.LongType(), 'Source update counter.'), _sf('UPDT_DT_TM', T.TimestampType(), 'Source application update datetime.'), _sf('UPDT_ID', T.LongType(), 'Source update personnel identifier.'), _sf('UPDT_TASK', T.LongType(), 'Source update task identifier.'), _sf('LAST_UTC_TS', T.TimestampType(), 'Source last UTC timestamp.'), _sf('ORIGINATING_ENCNTR_ID', T.LongType(), 'Unmodified source originating encounter identifier.'), _sf('UPDATE_ENCNTR_ID', T.LongType(), 'Unmodified source update encounter identifier.'), _sf('ENCNTR_ID', T.LongType(), 'Unmodified direct source encounter identifier.'), _sf('ORIGINATING_ENCNTR_VALID_IND', T.BooleanType(), 'True when the originating encounter exists and belongs to PERSON_ID.'), _sf('UPDATE_ENCNTR_VALID_IND', T.BooleanType(), 'True when the update encounter exists and belongs to PERSON_ID.'), _sf('SOURCE_ENCNTR_VALID_IND', T.BooleanType(), 'True when the direct source encounter exists and belongs to PERSON_ID.'), _sf('CALC_ENCNTR', T.LongType(), 'Validated direct encounter, overlapping encounter, nearest prior encounter, or nearest following encounter.'), _sf('CALC_ENCNTR_SOURCE', T.StringType(), 'Selection source for CALC_ENCNTR.'), _sf('CALC_ENCNTR_VALIDATED_IND', T.BooleanType(), 'True when CALC_ENCNTR was validated against map_encounter and PERSON_ID.'), _sf('CALC_ENCNTR_DISTANCE_SECONDS', T.LongType(), 'Temporal distance from CALC_DT_TM to the selected encounter; zero when inside its interval.'), _sf('CALC_ENC_WITHIN', T.LongType(), 'Encounter containing CALC_DT_TM; overlapping ties use latest arrival.'), _sf('CALC_ENC_BEFORE', T.LongType(), 'Nearest encounter ending before CALC_DT_TM.'), _sf('CALC_ENC_AFTER', T.LongType(), 'Nearest encounter beginning after CALC_DT_TM.'), _sf('CALC_ENCNTR_ADC_UPDT', T.TimestampType(), 'ADC_UPDT from the selected encounter row.'), _sf('SOURCE_IDENTIFIER', T.StringType(), 'Nomenclature source identifier with originating-nomenclature fallback.'), _sf('SOURCE_STRING', T.StringType(), 'Nomenclature source string with originating-nomenclature fallback.'), _sf('SOURCE_VOCABULARY_CD', T.LongType(), 'Nomenclature source-vocabulary code with fallback.'), _sf('source_vocabulary_desc', T.StringType(), 'Best available code-value label for SOURCE_VOCABULARY_CD.'), _sf('VOCAB_AXIS_CD', T.LongType(), 'Nomenclature vocabulary-axis code with fallback.'), _sf('vocab_axis_desc', T.StringType(), 'Best available code-value label for VOCAB_AXIS_CD.'), _sf('CONCEPT_CKI', T.StringType(), 'Complete unmodified nomenclature concept CKI.'), _sf('CONCEPT_CKI_SOURCE', T.StringType(), 'Component before the first exclamation mark in CONCEPT_CKI.'), _sf('CONCEPT_CKI_IDENTIFIER', T.StringType(), 'Component after the final exclamation mark in CONCEPT_CKI.'), _sf('CONCEPT_CKI_PROCESSED', T.StringType(), 'Compatibility alias for CONCEPT_CKI_IDENTIFIER.'), _sf('NOMENCLATURE_IS_ACTIVE', T.BooleanType(), 'Current nomenclature active indicator; inactive mappings are retained.'), _sf('NOMENCLATURE_SOURCE_CHANGE_TS', T.TimestampType(), 'Latest source-change timestamp across primary and originating nomenclature.'), _sf('NOMENCLATURE_MAPPING_HASH', T.StringType(), 'Stable hash of mapping content used for stale-enrichment detection.'), _sf('FOUND_CUI', T.StringType(), 'CUI found during nomenclature mapping.'), _sf('OMOP_CONCEPT_CLASS', T.StringType(), 'Current OMOP concept class.'), _sf('SOURCE_MAPPING_NOMENCLATURE_ID', T.LongType(), 'Nomenclature row used for source identifier/string fields.'), _sf('OMOP_MAPPING_NOMENCLATURE_ID', T.LongType(), 'Nomenclature row used for OMOP mapping fields.'), _sf('SNOMED_MAPPING_NOMENCLATURE_ID', T.LongType(), 'Nomenclature row used for SNOMED mapping fields.'), _sf('ICD10_MAPPING_NOMENCLATURE_ID', T.LongType(), 'Nomenclature row used for ICD-10 mapping fields.'), _sf('OPCS4_MAPPING_NOMENCLATURE_ID', T.LongType(), 'Nomenclature row used for OPCS-4 mapping fields.'), _sf('OMOP_CONCEPT_ID', T.LongType(), 'Current mapped OMOP concept identifier with originating-nomenclature fallback.'), _sf('OMOP_CONCEPT_NAME', T.StringType(), 'Current mapped OMOP concept name.'), _sf('OMOP_STANDARD_CONCEPT', T.StringType(), 'Current OMOP standard-concept flag.'), _sf('OMOP_MATCH_NUMBER', T.LongType(), 'Number of OMOP mapping matches.'), _sf('OMOP_SIMILARITY', T.DoubleType(), 'Source-to-OMOP similarity score.'), _sf('OMOP_CONCEPT_DOMAIN', T.StringType(), 'Current OMOP concept domain.'), _sf('SNOMED_CODE', T.LongType(), 'Current mapped SNOMED code with originating-nomenclature fallback.'), _sf('SNOMED_TYPE', T.StringType(), 'Current SNOMED mapping method.'), _sf('SNOMED_MATCH_NUMBER', T.LongType(), 'Number of SNOMED mapping matches.'), _sf('SNOMED_SIMILARITY', T.DoubleType(), 'Source-to-SNOMED similarity score.'), _sf('SNOMED_TERM', T.StringType(), 'Current mapped SNOMED term.'), _sf('ICD10_CODE', T.StringType(), 'Current mapped ICD-10 code with originating-nomenclature fallback.'), _sf('ICD10_TYPE', T.StringType(), 'Current ICD-10 mapping method.'), _sf('ICD10_MATCH_NUMBER', T.LongType(), 'Number of ICD-10 mapping matches.'), _sf('ICD10_SIMILARITY', T.DoubleType(), 'Source-to-ICD-10 similarity score.'), _sf('ICD10_TERM', T.StringType(), 'Current mapped ICD-10 term.'), _sf('OPCS4_CODE', T.StringType(), 'Current mapped OPCS-4 code with originating-nomenclature fallback.'), _sf('OPCS4_TYPE', T.StringType(), 'Current OPCS-4 mapping method.'), _sf('OPCS4_MATCH_NUMBER', T.LongType(), 'Number of OPCS-4 mapping matches.'), _sf('OPCS4_SIMILARITY', T.DoubleType(), 'Source-to-OPCS-4 similarity score.'), _sf('OPCS4_TERM', T.StringType(), 'Current mapped OPCS-4 term.'), _sf('SIMILARITY_SOURCE_SNOMED', T.DoubleType(), 'Source string to SNOMED similarity.'), _sf('SIMILARITY_SOURCE_ICD10', T.DoubleType(), 'Source string to ICD-10 similarity.'), _sf('SIMILARITY_SOURCE_OPCS4', T.DoubleType(), 'Source string to OPCS-4 similarity.'), _sf('SIMILARITY_SOURCE_OMOP', T.DoubleType(), 'Source string to OMOP similarity.'), _sf('SIMILARITY_SNOMED_ICD10', T.DoubleType(), 'SNOMED to ICD-10 similarity.'), _sf('SIMILARITY_SNOMED_OPCS4', T.DoubleType(), 'SNOMED to OPCS-4 similarity.'), _sf('SIMILARITY_SNOMED_OMOP', T.DoubleType(), 'SNOMED to OMOP similarity.'), _sf('SIMILARITY_ICD10_OMOP', T.DoubleType(), 'ICD-10 to OMOP similarity.'), _sf('SIMILARITY_OPCS4_OMOP', T.DoubleType(), 'OPCS-4 to OMOP similarity.'), _sf('CODE_VALUE_LOOKUP_HASH', T.StringType(), 'Stable hash of code-value descriptions used by the row.'), _sf('PROBLEM_ADC_UPDT', T.TimestampType(), 'ADC_UPDT from mill_problem only.'), _sf('NOMENCLATURE_ADC_UPDT', T.TimestampType(), 'Latest ADC_UPDT across primary and originating nomenclature rows.'), _sf('CODE_VALUE_ADC_UPDT', T.TimestampType(), 'Greatest ADC_UPDT among code-value rows used by this problem.'), _sf('MAP_REFRESH_DT_TM', T.TimestampType(), 'Datetime the mapped row last materially changed.'), _sf('MAP_ROW_HASH', T.StringType(), 'Stable hash used to avoid rewriting unchanged mapped rows.'), _sf('ADC_UPDT', T.TimestampType(), 'Compatibility composite update timestamp; never use as an incremental checkpoint.')]
schema_map_problem_v3 = T.StructType(_PROBLEM_SCHEMA_FIELDS)
schema_map_problem = schema_map_problem_v3
schema_map_problem_history_v3 = T.StructType(_PROBLEM_SCHEMA_FIELDS + [_sf('SOURCE_DELETED_IND', T.BooleanType(), 'True when the source revision was physically deleted after being observed.'), _sf('SOURCE_DELETE_COMMIT_VERSION', T.LongType(), 'Source Delta commit version that deleted the revision.'), _sf('SOURCE_DELETE_COMMIT_TIMESTAMP', T.TimestampType(), 'Datetime the source delete was applied to history.')])
LONG_SOURCE_COLUMNS = ['PROBLEM_INSTANCE_ID', 'PROBLEM_ID', 'NOMENCLATURE_ID', 'PERSON_ID', 'CLASSIFICATION_CD', 'PERSISTENCE_CD', 'CONFIRMATION_STATUS_CD', 'LIFE_CYCLE_STATUS_CD', 'ONSET_DT_CD', 'RANKING_CD', 'CERTAINTY_CD', 'PERSON_AWARE_CD', 'PROGNOSIS_CD', 'PERSON_AWARE_PROGNOSIS_CD', 'FAMILY_AWARE_CD', 'ACTIVE_IND', 'ACTIVE_STATUS_CD', 'ACTIVE_STATUS_PRSNL_ID', 'CONTRIBUTOR_SYSTEM_CD', 'DATA_STATUS_CD', 'DATA_STATUS_PRSNL_ID', 'UPDT_APPLCTX', 'UPDT_CNT', 'UPDT_ID', 'UPDT_TASK', 'COURSE_CD', 'CANCEL_REASON_CD', 'ONSET_DT_FLAG', 'STATUS_UPDT_PRECISION_CD', 'STATUS_UPDT_FLAG', 'QUALIFIER_CD', 'SEVERITY_CLASS_CD', 'SEVERITY_CD', 'ONSET_TZ', 'BEG_EFFECTIVE_TZ', 'LIFE_CYCLE_TZ', 'DEL_IND', 'COND_TYPE_FLAG', 'LIFE_CYCLE_DT_CD', 'LIFE_CYCLE_DT_FLAG', 'ORGANIZATION_ID', 'PROBLEM_TYPE_FLAG', 'SHOW_IN_PM_HISTORY_IND', 'LATERALITY_CD', 'ORIGINATING_NOMENCLATURE_ID', 'INST_ID', 'UPDATE_ENCNTR_ID', 'ORIGINATING_ENCNTR_ID', 'ENCNTR_ID', 'ASSERTED_DT_FLAG', 'ASSERTED_DT_CD', 'ASSERTED_TZ', 'IMPAIRMENT_TYPE_CD']
STRING_SOURCE_COLUMNS = ['PROBLEM_FTDESC', 'ANNOTATED_DISPLAY', 'PROBLEM_INSTANCE_UUID', 'PROBLEM_UUID', 'TXN_ID_TEXT', 'OTHER_IMPAIRMENT_TEXT', 'Trust']
TIMESTAMP_SOURCE_COLUMNS = ['ESTIMATED_RESOLUTION_DT_TM', 'ACTUAL_RESOLUTION_DT_TM', 'LIFE_CYCLE_DT_TM', 'ONSET_DT_TM', 'ACTIVE_STATUS_DT_TM', 'BEG_EFFECTIVE_DT_TM', 'END_EFFECTIVE_DT_TM', 'DATA_STATUS_DT_TM', 'UPDT_DT_TM', 'STATUS_UPDT_DT_TM', 'LAST_UTC_TS', 'ADC_UPDT', 'ASSERTED_DT_TM']
DOUBLE_SOURCE_COLUMNS = ['PROBABILITY', 'SENSITIVITY']
CODE_DESCRIPTION_COLUMNS = {'CLASSIFICATION_CD': 'classification_desc', 'PERSISTENCE_CD': 'persistence_desc', 'CONFIRMATION_STATUS_CD': 'confirmation_status_desc', 'LIFE_CYCLE_STATUS_CD': 'life_cycle_status_desc', 'RANKING_CD': 'ranking_desc', 'CERTAINTY_CD': 'certainty_desc', 'PERSON_AWARE_CD': 'person_aware_desc', 'PROGNOSIS_CD': 'prognosis_desc', 'PERSON_AWARE_PROGNOSIS_CD': 'person_aware_prognosis_desc', 'FAMILY_AWARE_CD': 'family_aware_desc', 'COURSE_CD': 'course_desc', 'CANCEL_REASON_CD': 'cancel_reason_desc', 'QUALIFIER_CD': 'qualifier_desc', 'SEVERITY_CLASS_CD': 'severity_class_desc', 'SEVERITY_CD': 'severity_desc', 'LATERALITY_CD': 'laterality_desc', 'IMPAIRMENT_TYPE_CD': 'impairment_type_desc', 'STATUS_UPDT_PRECISION_CD': 'status_updt_precision_desc', 'ACTIVE_STATUS_CD': 'active_status_desc', 'DATA_STATUS_CD': 'data_status_desc', 'CONTRIBUTOR_SYSTEM_CD': 'contributor_system_desc', 'SOURCE_VOCABULARY_CD': 'source_vocabulary_desc', 'VOCAB_AXIS_CD': 'vocab_axis_desc'}
RAW_CODE_COLUMNS = [column for column in CODE_DESCRIPTION_COLUMNS if column not in {'SOURCE_VOCABULARY_CD', 'VOCAB_AXIS_CD'}]
NOMENCLATURE_COLUMNS = ['SOURCE_IDENTIFIER', 'SOURCE_STRING', 'SOURCE_VOCABULARY_CD', 'VOCAB_AXIS_CD', 'IS_STANDARD_OMOP_CONCEPT', 'CONCEPT_DOMAIN', 'CONCEPT_CLASS', 'FOUND_CUI', 'CONCEPT_CKI', 'ADC_UPDT', 'SNOMED_CODE', 'SNOMED_TYPE', 'SNOMED_MATCH_COUNT', 'SNOMED_TERM', 'ICD10_CODE', 'ICD10_CODE_TYPE', 'ICD10_CODE_MATCH_COUNT', 'ICD10_TERM', 'OPCS4_CODE', 'OPCS4_CODE_TYPE', 'OPCS4_CODE_MATCH_COUNT', 'OPCS4_TERM', 'OMOP_CONCEPT_ID', 'OMOP_CONCEPT_NAME', 'NUMBER_OF_OMOP_MATCHES', 'SNOMED_SIMILARITY', 'ICD10_SIMILARITY', 'OPCS4_SIMILARITY', 'OMOP_SIMILARITY', 'SIMILARITY_SOURCE_SNOMED', 'SIMILARITY_SOURCE_ICD10', 'SIMILARITY_SOURCE_OPCS4', 'SIMILARITY_SOURCE_OMOP', 'SIMILARITY_SNOMED_ICD10', 'SIMILARITY_SNOMED_OPCS4', 'SIMILARITY_SNOMED_OMOP', 'SIMILARITY_ICD10_OMOP', 'SIMILARITY_OPCS4_OMOP', 'SOURCE_CHANGE_TS', 'IS_ACTIVE']

class MapProblemCDFGap(RuntimeError):
    pass

def _sql_name(table_name: str) -> str:
    quote = chr(96)
    return '.'.join((quote + part.replace(quote, quote + quote) + quote for part in table_name.split('.')))

def _sql_column(column_name: str) -> str:
    quote = chr(96)
    return quote + column_name.replace(quote, quote + quote) + quote

def _escape_sql_string(value: str) -> str:
    return (value or '').replace('\\', '\\\\').replace("'", "''")

def _normalized_table_name(value: str) -> str:
    return (value or '').replace(chr(96), '').strip().lower()

def _table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _is_empty(df: Optional[DataFrame]) -> bool:
    return df is None or df.isEmpty()

def _nonblank(column: F.Column) -> F.Column:
    return F.when(column.isNotNull() & (F.length(F.trim(column.cast('string'))) > 0), F.trim(column.cast('string')))

def _checked_long(column: F.Column, label: str, required: bool=False) -> F.Column:
    # Value path is decimal-only; see the diagnosis-lane copy of this helper. The double
    # is a reject-only guard for NaN, non-integers and absurd magnitudes and never
    # produces the returned value.
    as_double = column.cast('double')
    as_decimal = column.cast('decimal(38,0)')
    null_result = F.lit(None) if required else F.lit(None).cast('long')
    in_long_range = as_decimal.between(F.lit(-9223372036854775808).cast('decimal(38,0)'), F.lit(9223372036854775807).cast('decimal(38,0)'))
    return F.when(column.isNull(), null_result).when(F.isnan(as_double), F.lit(None)).when(as_double != F.floor(as_double), F.lit(None)).when(F.abs(as_double) > F.lit(9223372036854775807), F.lit(None)).when(~in_long_range, F.lit(None)).otherwise(as_decimal.cast('long'))

def _stable_hash(column_names: Sequence[str]) -> F.Column:
    return F.sha2(F.to_json(F.struct(*[F.col(name).alias(name) for name in column_names]), {'ignoreNullFields': 'false'}), 256)

def _empty_long_key(column_name: str) -> DataFrame:
    return spark.createDataFrame([], T.StructType([T.StructField(column_name, T.LongType(), True)]))

def _empty_pair_df() -> DataFrame:
    return spark.createDataFrame([], T.StructType([T.StructField('PERSON_ID', T.LongType(), True), T.StructField('NOMENCLATURE_ID', T.LongType(), True)]))

def _empty_problem_rows() -> DataFrame:
    return spark.createDataFrame([], schema_map_problem_v3)

def _empty_history_rows() -> DataFrame:
    return spark.createDataFrame([], schema_map_problem_history_v3)

def _union_keys(frames: Sequence[DataFrame], key_column: str) -> DataFrame:
    usable = [frame.select(key_column) for frame in frames if frame is not None]
    if not usable:
        return _empty_long_key(key_column)
    return reduce(lambda left, right: left.unionByName(right), usable).dropDuplicates([key_column])

def _align_to_schema(df: DataFrame, schema: T.StructType) -> DataFrame:
    expressions: List[F.Column] = []
    available = set(df.columns)
    for field in schema.fields:
        if field.name in available:
            if isinstance(field.dataType, T.LongType):
                expression = _checked_long(F.col(field.name), field.name, required=field.name in {'PROBLEM_ID', 'PROBLEM_INSTANCE_ID'})
            else:
                expression = F.col(field.name).cast(field.dataType)
        else:
            expression = F.lit(None).cast(field.dataType)
        expressions.append(expression.alias(field.name, metadata=field.metadata))
    return df.select(*expressions)

def _latest_delta_version(table_name: str) -> int:
    row = spark.sql('DESCRIBE HISTORY ' + _sql_name(table_name) + ' LIMIT 1').select('version').first()
    if row is None:
        raise RuntimeError('No Delta history exists for ' + table_name)
    return int(row['version'])

def _read_snapshot(table_name: str, version: int) -> DataFrame:
    return _m20_hardening_read_pinned_snapshot(table_name, int(version))

def _read_cdf(table_name: str, starting_version: int, ending_version: int) -> DataFrame:
    if starting_version > ending_version:
        return _read_snapshot(table_name, ending_version).limit(0)
    try:
        changes = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', int(starting_version)).option('endingVersion', int(ending_version)).table(table_name)
        return _m20_hardening_validate_cdf(changes)
    except Exception as exc:
        raise MapProblemCDFGap('CDF could not be read for ' + table_name + ' from version ' + str(starting_version) + ' through ' + str(ending_version) + '. No timestamp fallback was used because that can silently lose deletes, ' + 'same-timestamp rows, and late-arriving data.') from exc

def _capture_source_versions() -> Dict[str, int]:
    return {source_name: _latest_delta_version(table_name) for source_name, table_name in MAP_PROBLEM_SOURCES.items()}

def _active_source_version(source_name: str) -> int:
    table_name = MAP_PROBLEM_SOURCES[source_name]
    if table_name not in _MAP_PROBLEM_ACTIVE_SOURCE_VERSIONS:
        raise RuntimeError('No captured source version exists for ' + table_name)
    return int(_MAP_PROBLEM_ACTIVE_SOURCE_VERSIONS[table_name])

def ensure_map_problem_control_tables() -> None:
    spark.sql("\n        CREATE TABLE IF NOT EXISTS {state} (\n            source_table STRING NOT NULL,\n            last_processed_version BIGINT NOT NULL,\n            last_processed_at TIMESTAMP NOT NULL,\n            last_run_id STRING NOT NULL\n        )\n        USING DELTA\n        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')\n        ".format(state=_sql_name(MAP_PROBLEM_CHECKPOINT)))
    spark.sql('\n        CREATE TABLE IF NOT EXISTS {audit} (\n            run_id STRING NOT NULL,\n            event_timestamp TIMESTAMP NOT NULL,\n            status STRING NOT NULL,\n            mode STRING NOT NULL,\n            message STRING,\n            source_versions_json STRING,\n            metrics_json STRING\n        )\n        USING DELTA\n        '.format(audit=_sql_name(MAP_PROBLEM_AUDIT)))

def _read_checkpoint_versions() -> Dict[str, int]:
    if not _table_exists(MAP_PROBLEM_CHECKPOINT):
        return {}
    rows = spark.table(MAP_PROBLEM_CHECKPOINT).filter(F.col('source_table').isin(*MAP_PROBLEM_SOURCES.values())).select('source_table', 'last_processed_version').collect()
    return {row['source_table']: int(row['last_processed_version']) for row in rows if row['last_processed_version'] is not None}

def _write_checkpoint_versions(end_versions: Dict[str, int], run_id: str) -> None:
    completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [(MAP_PROBLEM_SOURCES[source_name], int(version), completed_at, run_id) for source_name, version in end_versions.items()]
    schema = T.StructType([T.StructField('source_table', T.StringType(), False), T.StructField('last_processed_version', T.LongType(), False), T.StructField('last_processed_at', T.TimestampType(), False), T.StructField('last_run_id', T.StringType(), False)])
    updates = spark.createDataFrame(rows, schema)
    DeltaTable.forName(spark, MAP_PROBLEM_CHECKPOINT).alias('t').merge(updates.alias('s'), 't.source_table = s.source_table').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def _write_audit_event(run_id: str, status: str, mode: str, message: str, source_versions: Optional[Dict[str, int]]=None, metrics: Optional[Dict[str, object]]=None) -> None:
    try:
        row = [(run_id, datetime.now(timezone.utc).replace(tzinfo=None), status, mode, message[:8000], json.dumps(source_versions or {}, sort_keys=True, default=str), json.dumps(metrics or {}, sort_keys=True, default=str))]
        schema = T.StructType([T.StructField('run_id', T.StringType(), False), T.StructField('event_timestamp', T.TimestampType(), False), T.StructField('status', T.StringType(), False), T.StructField('mode', T.StringType(), False), T.StructField('message', T.StringType(), True), T.StructField('source_versions_json', T.StringType(), True), T.StructField('metrics_json', T.StringType(), True)])
        spark.createDataFrame(row, schema).write.mode('append').saveAsTable(MAP_PROBLEM_AUDIT)
    except Exception as exc:
        print('[WARN] Could not write map_problem audit event: ' + str(exc))

def _get_table_properties(table_name: str) -> Dict[str, str]:
    if not _table_exists(table_name):
        return {}
    return {row['key']: row['value'] for row in spark.sql('SHOW TBLPROPERTIES ' + _sql_name(table_name)).collect()}

def _set_table_properties_if_changed(table_name: str, properties: Dict[str, str]) -> None:
    current = _get_table_properties(table_name)
    changed = {key: str(value) for key, value in properties.items() if current.get(key) != str(value)}
    if not changed:
        return
    assignments = ', '.join(("'" + _escape_sql_string(key) + "' = '" + _escape_sql_string(value) + "'" for key, value in changed.items()))
    spark.sql('ALTER TABLE ' + _sql_name(table_name) + ' SET TBLPROPERTIES (' + assignments + ')')

def _set_table_comment_if_changed(table_name: str, comment: str) -> None:
    try:
        current = spark.catalog.getTable(table_name).description
    except Exception:
        current = None
    if current == comment:
        return
    spark.sql('COMMENT ON TABLE ' + _sql_name(table_name) + " IS '" + _escape_sql_string(comment) + "'")

def _apply_column_comments_if_changed(table_name: str, schema: T.StructType) -> None:
    current_comments = {field.name: field.metadata.get('comment', '') for field in spark.table(table_name).schema.fields}
    for field in schema.fields:
        desired = field.metadata.get('comment', '')
        if not desired or current_comments.get(field.name) == desired:
            continue
        spark.sql('ALTER TABLE ' + _sql_name(table_name) + ' ALTER COLUMN ' + _sql_column(field.name) + " COMMENT '" + _escape_sql_string(desired) + "'")

def _target_properties(history: bool=False) -> Dict[str, str]:
    prefix = 'pipeline.map_problem_history' if history else 'pipeline.map_problem'
    return {'delta.enableChangeDataFeed': 'true', 'delta.enableDeletionVectors': 'true', 'delta.enableRowTracking': 'true', 'delta.autoOptimize.optimizeWrite': 'true', 'delta.autoOptimize.autoCompact': 'true', 'delta.deletedFileRetentionDuration': MAP_PROBLEM_CDF_RETENTION, 'delta.logRetentionDuration': MAP_PROBLEM_CDF_RETENTION, prefix + '.schema_version': MAP_PROBLEM_SCHEMA_VERSION, prefix + '.cdf_checkpoint': 'delta_commit_version', prefix + '.filters': 'none', prefix + '.revision_selection': 'PROBLEM_INSTANCE_ID_desc_then_source_update_fields', prefix + '.date_semantics': 'raw_plus_explicit_derived_fields'}

def _set_target_metadata(apply_column_comments: bool=False) -> None:
    if _table_exists(MAP_PROBLEM_TARGET):
        _set_table_properties_if_changed(MAP_PROBLEM_TARGET, _target_properties(history=False))
        _set_table_comment_if_changed(MAP_PROBLEM_TARGET, map_problem_comment)
        if apply_column_comments:
            _apply_column_comments_if_changed(
                MAP_PROBLEM_TARGET,
                bronze_contract_schema(MAP_PROBLEM_TARGET),
            )
    if _table_exists(MAP_PROBLEM_HISTORY):
        _set_table_properties_if_changed(MAP_PROBLEM_HISTORY, _target_properties(history=True))
        _set_table_comment_if_changed(MAP_PROBLEM_HISTORY, map_problem_history_comment)
        if apply_column_comments:
            _apply_column_comments_if_changed(MAP_PROBLEM_HISTORY, schema_map_problem_history_v3)

def configure_map_problem_cdf_retention() -> None:
    properties = {'delta.enableChangeDataFeed': 'true', 'delta.deletedFileRetentionDuration': MAP_PROBLEM_CDF_RETENTION, 'delta.logRetentionDuration': MAP_PROBLEM_CDF_RETENTION}
    for table_name in MAP_PROBLEM_SOURCES.values():
        _set_table_properties_if_changed(table_name, properties)

def _target_requires_full_rebuild(checkpoints: Dict[str, int], end_versions: Dict[str, int]) -> Tuple[bool, str]:
    if not _table_exists(MAP_PROBLEM_TARGET):
        return (True, 'current target does not exist')
    if not _table_exists(MAP_PROBLEM_HISTORY):
        return (True, 'revision-history target does not exist')
    current_props = _get_table_properties(MAP_PROBLEM_TARGET)
    history_props = _get_table_properties(MAP_PROBLEM_HISTORY)
    if current_props.get('pipeline.map_problem.schema_version') != MAP_PROBLEM_SCHEMA_VERSION:
        return (True, 'current target schema version is absent or outdated')
    if history_props.get('pipeline.map_problem_history.schema_version') != MAP_PROBLEM_SCHEMA_VERSION:
        return (True, 'history target schema version is absent or outdated')
    expected_current = {
        field.name.lower(): field.dataType.simpleString()
        for field in bronze_contract_schema(MAP_PROBLEM_TARGET).fields
    }
    actual_current = {field.name.lower(): field.dataType.simpleString() for field in spark.table(MAP_PROBLEM_TARGET).schema.fields}
    expected_history = {field.name.lower(): field.dataType.simpleString() for field in schema_map_problem_history_v3.fields}
    actual_history = {field.name.lower(): field.dataType.simpleString() for field in spark.table(MAP_PROBLEM_HISTORY).schema.fields}
    if actual_current != expected_current:
        return (True, 'current target schema does not exactly match v3')
    if actual_history != expected_history:
        return (True, 'history target schema does not exactly match v3')
    required = set(MAP_PROBLEM_SOURCES.values())
    if not required.issubset(checkpoints):
        return (True, 'source-specific Delta checkpoints are incomplete')
    for source_name, table_name in MAP_PROBLEM_SOURCES.items():
        if checkpoints[table_name] > end_versions[source_name]:
            return (True, 'checkpoint is ahead of current source version for ' + table_name)
    legacy_current = spark.table(MAP_PROBLEM_TARGET).filter(F.col('MAP_ROW_HASH').isNull()).limit(1).count() > 0
    if legacy_current:
        return (True, 'legacy current rows do not have v3 hashes')
    return (False, 'incremental state is compatible')

def _prepare_problem_source(raw: DataFrame) -> DataFrame:
    expressions: List[F.Column] = []
    for column_name in LONG_SOURCE_COLUMNS:
        expressions.append(_checked_long(F.col(column_name), 'MILL_PROBLEM.' + column_name, required=column_name in {'PROBLEM_ID', 'PROBLEM_INSTANCE_ID'}).alias(column_name))
    for column_name in STRING_SOURCE_COLUMNS:
        output_name = 'TRUST' if column_name == 'Trust' else column_name
        expressions.append(F.col(column_name).cast('string').alias(output_name))
    for column_name in TIMESTAMP_SOURCE_COLUMNS:
        output_name = 'PROBLEM_ADC_UPDT' if column_name == 'ADC_UPDT' else column_name
        expressions.append(F.col(column_name).cast('timestamp').alias(output_name))
    for column_name in DOUBLE_SOURCE_COLUMNS:
        expressions.append(F.col(column_name).cast('double').alias(column_name))
    prepared = raw.select(*expressions)
    source_columns = list(prepared.columns)
    instance_window = Window.partitionBy('PROBLEM_INSTANCE_ID').orderBy(F.col('UPDT_DT_TM').desc_nulls_last(), F.col('UPDT_CNT').desc_nulls_last(), F.col('PROBLEM_ADC_UPDT').desc_nulls_last(), F.col('LAST_UTC_TS').desc_nulls_last(), F.col('PROBLEM_ID').desc_nulls_last(), F.col('_SOURCE_INSTANCE_ROW_HASH').desc())
    prepared = prepared.withColumn('_SOURCE_INSTANCE_ROW_HASH', _stable_hash(source_columns)).withColumn('_SOURCE_INSTANCE_ROW_NUMBER', F.row_number().over(instance_window)).where(F.col('_SOURCE_INSTANCE_ROW_NUMBER') == 1).drop('_SOURCE_INSTANCE_ROW_HASH', '_SOURCE_INSTANCE_ROW_NUMBER')
    revision_window = Window.partitionBy('PROBLEM_ID').orderBy(F.col('PROBLEM_INSTANCE_ID').desc_nulls_last(), F.col('UPDT_DT_TM').desc_nulls_last(), F.col('UPDT_CNT').desc_nulls_last(), F.col('PROBLEM_ADC_UPDT').desc_nulls_last(), F.col('LAST_UTC_TS').desc_nulls_last())
    count_window = Window.partitionBy('PROBLEM_ID')
    return prepared.withColumn('PROBLEM_REVISION_RANK', F.row_number().over(revision_window).cast('long')).withColumn('PROBLEM_REVISION_COUNT', F.count(F.lit(1)).over(count_window).cast('long')).withColumn('IS_CURRENT_PROBLEM_REVISION', F.col('PROBLEM_REVISION_RANK') == F.lit(1))

def _current_problem_snapshot() -> DataFrame:
    return _prepare_problem_source(_read_snapshot(MAP_PROBLEM_SOURCE, _active_source_version('problem')))

def _prepare_nomenclature_snapshot() -> DataFrame:
    raw = _read_snapshot(MAP_PROBLEM_NOMENCLATURE, _active_source_version('nomenclature'))
    columns = [_checked_long(F.col('NOMENCLATURE_ID'), 'NOMENCLATURE.NOMENCLATURE_ID', required=True).alias('NOMENCLATURE_ID')]
    columns.extend((F.col(name).alias(name) for name in NOMENCLATURE_COLUMNS))
    prepared = raw.select(*columns)
    mapping_hash_columns = [name for name in NOMENCLATURE_COLUMNS if name not in {'ADC_UPDT', 'SOURCE_CHANGE_TS'}]
    prepared = prepared.withColumn('NOMENCLATURE_MAPPING_HASH', _stable_hash(mapping_hash_columns))
    latest_window = Window.partitionBy('NOMENCLATURE_ID').orderBy(F.col('SOURCE_CHANGE_TS').desc_nulls_last(), F.col('ADC_UPDT').desc_nulls_last())
    return prepared.withColumn('_NOM_RANK', F.row_number().over(latest_window)).filter(F.col('_NOM_RANK') == 1).drop('_NOM_RANK')

def _prefixed_nomenclature(nomenclature: DataFrame, prefix: str) -> DataFrame:
    return nomenclature.select(F.col('NOMENCLATURE_ID').alias(prefix + '_KEY'), *[F.col(name).alias(prefix + '_' + name) for name in NOMENCLATURE_COLUMNS + ['NOMENCLATURE_MAPPING_HASH']])

def _mapping_source_id(primary_value: F.Column, originating_value: F.Column) -> F.Column:
    return F.when(primary_value.isNotNull(), F.col('NOMENCLATURE_ID')).when(originating_value.isNotNull(), F.col('ORIGINATING_NOMENCLATURE_ID')).cast('long')

def _enrich_nomenclature(problems: DataFrame, nomenclature: DataFrame) -> DataFrame:
    primary = _prefixed_nomenclature(nomenclature, '_PRI')
    originating = _prefixed_nomenclature(nomenclature, '_ORG')
    result = problems.join(primary, F.col('NOMENCLATURE_ID') == F.col('_PRI_KEY'), 'left').join(originating, F.col('ORIGINATING_NOMENCLATURE_ID') == F.col('_ORG_KEY'), 'left')

    def fallback(name: str) -> F.Column:
        return F.coalesce(F.col('_PRI_' + name), F.col('_ORG_' + name))
    result = result.withColumn('SOURCE_IDENTIFIER', fallback('SOURCE_IDENTIFIER')).withColumn('SOURCE_STRING', fallback('SOURCE_STRING')).withColumn('SOURCE_VOCABULARY_CD', _checked_long(fallback('SOURCE_VOCABULARY_CD'), 'SOURCE_VOCABULARY_CD')).withColumn('VOCAB_AXIS_CD', _checked_long(fallback('VOCAB_AXIS_CD'), 'VOCAB_AXIS_CD')).withColumn('CONCEPT_CKI', fallback('CONCEPT_CKI')).withColumn('CONCEPT_CKI_SOURCE', F.when(F.col('CONCEPT_CKI').contains('!'), F.substring_index(F.col('CONCEPT_CKI'), '!', 1))).withColumn('CONCEPT_CKI_IDENTIFIER', F.when(F.col('CONCEPT_CKI').isNotNull(), F.substring_index(F.col('CONCEPT_CKI'), '!', -1))).withColumn('CONCEPT_CKI_PROCESSED', F.col('CONCEPT_CKI_IDENTIFIER')).withColumn('NOMENCLATURE_IS_ACTIVE', fallback('IS_ACTIVE').cast('boolean')).withColumn('NOMENCLATURE_SOURCE_CHANGE_TS', F.greatest(F.col('_PRI_SOURCE_CHANGE_TS'), F.col('_ORG_SOURCE_CHANGE_TS'))).withColumn('NOMENCLATURE_ADC_UPDT', F.greatest(F.col('_PRI_ADC_UPDT'), F.col('_ORG_ADC_UPDT'))).withColumn('NOMENCLATURE_MAPPING_HASH', _stable_hash(['_PRI_NOMENCLATURE_MAPPING_HASH', '_ORG_NOMENCLATURE_MAPPING_HASH'])).withColumn('FOUND_CUI', fallback('FOUND_CUI')).withColumn('OMOP_CONCEPT_CLASS', fallback('CONCEPT_CLASS')).withColumn('SOURCE_MAPPING_NOMENCLATURE_ID', _mapping_source_id(F.coalesce(F.col('_PRI_SOURCE_IDENTIFIER'), F.col('_PRI_SOURCE_STRING')), F.coalesce(F.col('_ORG_SOURCE_IDENTIFIER'), F.col('_ORG_SOURCE_STRING')))).withColumn('OMOP_MAPPING_NOMENCLATURE_ID', _mapping_source_id(F.col('_PRI_OMOP_CONCEPT_ID'), F.col('_ORG_OMOP_CONCEPT_ID'))).withColumn('SNOMED_MAPPING_NOMENCLATURE_ID', _mapping_source_id(F.col('_PRI_SNOMED_CODE'), F.col('_ORG_SNOMED_CODE'))).withColumn('ICD10_MAPPING_NOMENCLATURE_ID', _mapping_source_id(F.col('_PRI_ICD10_CODE'), F.col('_ORG_ICD10_CODE'))).withColumn('OPCS4_MAPPING_NOMENCLATURE_ID', _mapping_source_id(F.col('_PRI_OPCS4_CODE'), F.col('_ORG_OPCS4_CODE'))).withColumn('OMOP_CONCEPT_ID', _checked_long(fallback('OMOP_CONCEPT_ID'), 'OMOP_CONCEPT_ID')).withColumn('OMOP_CONCEPT_NAME', fallback('OMOP_CONCEPT_NAME')).withColumn('OMOP_STANDARD_CONCEPT', fallback('IS_STANDARD_OMOP_CONCEPT')).withColumn('OMOP_MATCH_NUMBER', _checked_long(fallback('NUMBER_OF_OMOP_MATCHES'), 'OMOP_MATCH_NUMBER')).withColumn('OMOP_SIMILARITY', fallback('OMOP_SIMILARITY').cast('double')).withColumn('OMOP_CONCEPT_DOMAIN', fallback('CONCEPT_DOMAIN')).withColumn('SNOMED_CODE', _checked_long(fallback('SNOMED_CODE'), 'SNOMED_CODE')).withColumn('SNOMED_TYPE', fallback('SNOMED_TYPE')).withColumn('SNOMED_MATCH_NUMBER', _checked_long(fallback('SNOMED_MATCH_COUNT'), 'SNOMED_MATCH_NUMBER')).withColumn('SNOMED_SIMILARITY', fallback('SNOMED_SIMILARITY').cast('double')).withColumn('SNOMED_TERM', fallback('SNOMED_TERM')).withColumn('ICD10_CODE', fallback('ICD10_CODE')).withColumn('ICD10_TYPE', fallback('ICD10_CODE_TYPE')).withColumn('ICD10_MATCH_NUMBER', _checked_long(fallback('ICD10_CODE_MATCH_COUNT'), 'ICD10_MATCH_NUMBER')).withColumn('ICD10_SIMILARITY', fallback('ICD10_SIMILARITY').cast('double')).withColumn('ICD10_TERM', fallback('ICD10_TERM')).withColumn('OPCS4_CODE', fallback('OPCS4_CODE')).withColumn('OPCS4_TYPE', fallback('OPCS4_CODE_TYPE')).withColumn('OPCS4_MATCH_NUMBER', _checked_long(fallback('OPCS4_CODE_MATCH_COUNT'), 'OPCS4_MATCH_NUMBER')).withColumn('OPCS4_SIMILARITY', fallback('OPCS4_SIMILARITY').cast('double')).withColumn('OPCS4_TERM', fallback('OPCS4_TERM'))
    for name in ['SIMILARITY_SOURCE_SNOMED', 'SIMILARITY_SOURCE_ICD10', 'SIMILARITY_SOURCE_OPCS4', 'SIMILARITY_SOURCE_OMOP', 'SIMILARITY_SNOMED_ICD10', 'SIMILARITY_SNOMED_OPCS4', 'SIMILARITY_SNOMED_OMOP', 'SIMILARITY_ICD10_OMOP', 'SIMILARITY_OPCS4_OMOP']:
        result = result.withColumn(name, fallback(name).cast('double'))
    temporary_columns = [column_name for column_name in result.columns if column_name.startswith('_PRI_') or column_name.startswith('_ORG_')]
    return result.drop(*temporary_columns)

def _prepare_code_maps() -> DataFrame:
    raw = _read_snapshot(MAP_PROBLEM_CODE_VALUE, _active_source_version('code_value'))
    prepared = raw.select(_checked_long(F.col('CODE_VALUE'), 'CODE_VALUE', required=True).alias('CODE_VALUE'), F.coalesce(_nonblank(F.col('DESCRIPTION')), _nonblank(F.col('DISPLAY')), _nonblank(F.col('CDF_MEANING')), _nonblank(F.col('DEFINITION'))).alias('CODE_LABEL'), F.col('ADC_UPDT').cast('timestamp').alias('CODE_ADC_UPDT'), _checked_long(F.col('UPDT_CNT'), 'CODE_VALUE.UPDT_CNT').alias('UPDT_CNT'), F.col('UPDT_DT_TM').cast('timestamp').alias('UPDT_DT_TM'))
    latest_window = Window.partitionBy('CODE_VALUE').orderBy(F.col('CODE_ADC_UPDT').desc_nulls_last(), F.col('UPDT_CNT').desc_nulls_last(), F.col('UPDT_DT_TM').desc_nulls_last())
    current = prepared.withColumn('_RANK', F.row_number().over(latest_window)).filter(F.col('_RANK') == 1).drop('_RANK', 'UPDT_CNT', 'UPDT_DT_TM')
    return current.agg(F.map_from_entries(F.collect_list(F.struct(F.col('CODE_VALUE'), F.col('CODE_LABEL')))).alias('_CODE_LABEL_MAP'), F.map_from_entries(F.collect_list(F.struct(F.col('CODE_VALUE'), F.col('CODE_ADC_UPDT')))).alias('_CODE_ADC_MAP'))

def _enrich_code_descriptions(problems: DataFrame, code_maps: DataFrame) -> DataFrame:
    result = problems.crossJoin(F.broadcast(code_maps))
    update_expressions: List[F.Column] = []
    description_columns: List[str] = []
    for code_column, description_column in CODE_DESCRIPTION_COLUMNS.items():
        result = result.withColumn(description_column, F.element_at(F.col('_CODE_LABEL_MAP'), F.col(code_column).cast('long')))
        update_expressions.append(F.element_at(F.col('_CODE_ADC_MAP'), F.col(code_column).cast('long')))
        description_columns.append(description_column)
    result = result.withColumn('CODE_VALUE_ADC_UPDT', F.greatest(*update_expressions)).withColumn('CODE_VALUE_LOOKUP_HASH', _stable_hash(description_columns))
    return result.drop('_CODE_LABEL_MAP', '_CODE_ADC_MAP')

def _add_problem_dates_and_display(problems: DataFrame) -> DataFrame:
    calc_source = F.when(F.col('ONSET_DT_TM').isNotNull(), F.lit('ONSET_DT_TM')).when(F.col('ASSERTED_DT_TM').isNotNull(), F.lit('ASSERTED_DT_TM')).when(F.col('STATUS_UPDT_DT_TM').isNotNull(), F.lit('STATUS_UPDT_DT_TM')).when(F.col('LIFE_CYCLE_DT_TM').isNotNull(), F.lit('LIFE_CYCLE_DT_TM')).when(F.col('BEG_EFFECTIVE_DT_TM').isNotNull(), F.lit('BEG_EFFECTIVE_DT_TM')).when(F.col('ACTIVE_STATUS_DT_TM').isNotNull(), F.lit('ACTIVE_STATUS_DT_TM')).when(F.col('DATA_STATUS_DT_TM').isNotNull(), F.lit('DATA_STATUS_DT_TM')).when(F.col('PROBLEM_ADC_UPDT').isNotNull(), F.lit('PROBLEM_ADC_UPDT'))
    display_value = F.coalesce(_nonblank(F.col('ANNOTATED_DISPLAY')), _nonblank(F.col('PROBLEM_FTDESC')), _nonblank(F.col('SOURCE_STRING')), _nonblank(F.col('SNOMED_TERM')), _nonblank(F.col('OMOP_CONCEPT_NAME')), _nonblank(F.col('ICD10_TERM')))
    display_source = F.when(_nonblank(F.col('ANNOTATED_DISPLAY')).isNotNull(), F.lit('ANNOTATED_DISPLAY')).when(_nonblank(F.col('PROBLEM_FTDESC')).isNotNull(), F.lit('PROBLEM_FTDESC')).when(_nonblank(F.col('SOURCE_STRING')).isNotNull(), F.lit('SOURCE_STRING')).when(_nonblank(F.col('SNOMED_TERM')).isNotNull(), F.lit('SNOMED_TERM')).when(_nonblank(F.col('OMOP_CONCEPT_NAME')).isNotNull(), F.lit('OMOP_CONCEPT_NAME')).when(_nonblank(F.col('ICD10_TERM')).isNotNull(), F.lit('ICD10_TERM'))
    return problems.withColumn('CALC_DT_TM', F.coalesce(F.col('ONSET_DT_TM'), F.col('ASSERTED_DT_TM'), F.col('STATUS_UPDT_DT_TM'), F.col('LIFE_CYCLE_DT_TM'), F.col('BEG_EFFECTIVE_DT_TM'), F.col('ACTIVE_STATUS_DT_TM'), F.col('DATA_STATUS_DT_TM'), F.col('PROBLEM_ADC_UPDT'))).withColumn('CALC_DT_TM_SOURCE', calc_source).withColumn('CALC_DT_TM_BEFORE_1950_IND', F.col('CALC_DT_TM') < F.lit('1950-01-01').cast('timestamp')).withColumn('CALC_DT_TM_FUTURE_AT_MAP_IND', F.col('CALC_DT_TM') > F.current_timestamp()).withColumn('PROBLEM_DISPLAY', display_value).withColumn('PROBLEM_DISPLAY_SOURCE', display_source)

def _earliest_dates_for_groups(problem_snapshot: DataFrame, groups: Optional[DataFrame]=None) -> DataFrame:
    source = problem_snapshot.select('PERSON_ID', 'NOMENCLATURE_ID', 'ONSET_DT_TM')
    if groups is not None:
        source = source.join(groups.dropDuplicates(['PERSON_ID', 'NOMENCLATURE_ID']), ['PERSON_ID', 'NOMENCLATURE_ID'], 'left_semi')
    return source.groupBy('PERSON_ID', 'NOMENCLATURE_ID').agg(F.min('ONSET_DT_TM').alias('earliest_problem_date'))

def _attach_earliest_dates(problems: DataFrame, earliest_dates: DataFrame) -> DataFrame:
    return problems.join(earliest_dates, ['PERSON_ID', 'NOMENCLATURE_ID'], 'left')

def _encounter_arrays_for_people(people: DataFrame) -> DataFrame:
    encounter_snapshot = _read_snapshot(MAP_PROBLEM_ENCOUNTER, _active_source_version('encounter')).select(_checked_long(F.col('PERSON_ID'), 'MAP_ENCOUNTER.PERSON_ID').alias('PERSON_ID'), _checked_long(F.col('ENCNTR_ID'), 'MAP_ENCOUNTER.ENCNTR_ID', required=True).alias('ENCNTR_ID'), F.col('ARRIVE_DT_TM').cast('timestamp').alias('ARRIVE_DT_TM'), F.col('DEPART_DT_TM').cast('timestamp').alias('DEPART_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('ADC_UPDT'))
    relevant = encounter_snapshot.join(people.select('PERSON_ID').dropDuplicates(['PERSON_ID']), 'PERSON_ID', 'left_semi')
    return relevant.groupBy('PERSON_ID').agg(F.collect_list(F.struct(F.col('ARRIVE_DT_TM').alias('ARRIVE_DT_TM'), F.col('DEPART_DT_TM').alias('DEPART_DT_TM'), F.col('ENCNTR_ID').alias('ENCNTR_ID'), F.col('ADC_UPDT').alias('ADC_UPDT'))).alias('_ENCOUNTERS'))

def _attach_encounter_associations(problems: DataFrame) -> DataFrame:
    people = problems.select('PERSON_ID').where(F.col('PERSON_ID').isNotNull())
    arrays = _encounter_arrays_for_people(people)
    encounter_array_type = 'array<struct<ARRIVE_DT_TM:timestamp,DEPART_DT_TM:timestamp,ENCNTR_ID:bigint,ADC_UPDT:timestamp>>'
    result = problems.join(arrays, 'PERSON_ID', 'left').withColumn('_ENCOUNTERS', F.coalesce(F.col('_ENCOUNTERS'), F.from_json(F.lit('[]'), encounter_array_type)))
    result = result.withColumn('ORIGINATING_ENCNTR_VALID_IND', F.coalesce(F.expr('exists(_ENCOUNTERS, x -> x.ENCNTR_ID = ORIGINATING_ENCNTR_ID AND ORIGINATING_ENCNTR_ID NOT IN (0, 1))'), F.lit(False))).withColumn('UPDATE_ENCNTR_VALID_IND', F.coalesce(F.expr('exists(_ENCOUNTERS, x -> x.ENCNTR_ID = UPDATE_ENCNTR_ID AND UPDATE_ENCNTR_ID NOT IN (0, 1))'), F.lit(False))).withColumn('SOURCE_ENCNTR_VALID_IND', F.coalesce(F.expr('exists(_ENCOUNTERS, x -> x.ENCNTR_ID = ENCNTR_ID AND ENCNTR_ID NOT IN (0, 1))'), F.lit(False))).withColumn('_DIRECT_ENCNTR_ID', F.when(F.col('ORIGINATING_ENCNTR_VALID_IND'), F.col('ORIGINATING_ENCNTR_ID')).when(F.col('UPDATE_ENCNTR_VALID_IND'), F.col('UPDATE_ENCNTR_ID')).when(F.col('SOURCE_ENCNTR_VALID_IND'), F.col('ENCNTR_ID'))).withColumn('_DIRECT_ENCNTR_SOURCE', F.when(F.col('ORIGINATING_ENCNTR_VALID_IND'), F.lit('ORIGINATING_ENCNTR_ID')).when(F.col('UPDATE_ENCNTR_VALID_IND'), F.lit('UPDATE_ENCNTR_ID')).when(F.col('SOURCE_ENCNTR_VALID_IND'), F.lit('ENCNTR_ID'))).withColumn('_DIRECT_MATCH', F.expr('try_element_at(filter(_ENCOUNTERS, x -> x.ENCNTR_ID = _DIRECT_ENCNTR_ID), 1)')).withColumn('_WITHIN_MATCH', F.expr("\n                array_max(\n                    transform(\n                        filter(\n                            _ENCOUNTERS,\n                            x -> CALC_DT_TM IS NOT NULL\n                                 AND CALC_DT_TM >= x.ARRIVE_DT_TM\n                                 AND (x.DEPART_DT_TM IS NULL OR CALC_DT_TM <= x.DEPART_DT_TM)\n                        ),\n                        x -> named_struct(\n                            'EVENT_DT_TM', x.ARRIVE_DT_TM,\n                            'ENCNTR_ID', x.ENCNTR_ID,\n                            'ADC_UPDT', x.ADC_UPDT\n                        )\n                    )\n                )\n                ")).withColumn('_BEFORE_MATCH', F.expr("\n                array_max(\n                    transform(\n                        filter(\n                            _ENCOUNTERS,\n                            x -> CALC_DT_TM IS NOT NULL\n                                 AND x.DEPART_DT_TM IS NOT NULL\n                                 AND x.DEPART_DT_TM <= CALC_DT_TM\n                        ),\n                        x -> named_struct(\n                            'EVENT_DT_TM', x.DEPART_DT_TM,\n                            'ENCNTR_ID', x.ENCNTR_ID,\n                            'ADC_UPDT', x.ADC_UPDT\n                        )\n                    )\n                )\n                ")).withColumn('_AFTER_MATCH', F.expr("\n                array_min(\n                    transform(\n                        filter(\n                            _ENCOUNTERS,\n                            x -> CALC_DT_TM IS NOT NULL\n                                 AND x.ARRIVE_DT_TM > CALC_DT_TM\n                        ),\n                        x -> named_struct(\n                            'EVENT_DT_TM', x.ARRIVE_DT_TM,\n                            'ENCNTR_ID', x.ENCNTR_ID,\n                            'ADC_UPDT', x.ADC_UPDT\n                        )\n                    )\n                )\n                "))
    result = result.withColumn('CALC_ENC_WITHIN', F.col('_WITHIN_MATCH.ENCNTR_ID')).withColumn('CALC_ENC_BEFORE', F.col('_BEFORE_MATCH.ENCNTR_ID')).withColumn('CALC_ENC_AFTER', F.col('_AFTER_MATCH.ENCNTR_ID')).withColumn('CALC_ENCNTR', F.coalesce(F.col('_DIRECT_ENCNTR_ID'), F.col('CALC_ENC_WITHIN'), F.col('CALC_ENC_BEFORE'), F.col('CALC_ENC_AFTER'))).withColumn('CALC_ENCNTR_SOURCE', F.when(F.col('_DIRECT_ENCNTR_ID').isNotNull(), F.col('_DIRECT_ENCNTR_SOURCE')).when(F.col('CALC_ENC_WITHIN').isNotNull(), F.lit('WITHIN')).when(F.col('CALC_ENC_BEFORE').isNotNull(), F.lit('BEFORE')).when(F.col('CALC_ENC_AFTER').isNotNull(), F.lit('AFTER'))).withColumn('CALC_ENCNTR_VALIDATED_IND', F.col('CALC_ENCNTR').isNotNull()).withColumn('CALC_ENCNTR_ADC_UPDT', F.when(F.col('_DIRECT_ENCNTR_ID').isNotNull(), F.col('_DIRECT_MATCH.ADC_UPDT')).when(F.col('CALC_ENC_WITHIN').isNotNull(), F.col('_WITHIN_MATCH.ADC_UPDT')).when(F.col('CALC_ENC_BEFORE').isNotNull(), F.col('_BEFORE_MATCH.ADC_UPDT')).when(F.col('CALC_ENC_AFTER').isNotNull(), F.col('_AFTER_MATCH.ADC_UPDT')))
    direct_distance = F.when(F.col('_DIRECT_MATCH').isNull() | F.col('CALC_DT_TM').isNull(), F.lit(None).cast('long')).when(F.col('CALC_DT_TM') < F.col('_DIRECT_MATCH.ARRIVE_DT_TM'), F.unix_timestamp(F.col('_DIRECT_MATCH.ARRIVE_DT_TM')) - F.unix_timestamp(F.col('CALC_DT_TM'))).when(F.col('_DIRECT_MATCH.DEPART_DT_TM').isNotNull() & (F.col('CALC_DT_TM') > F.col('_DIRECT_MATCH.DEPART_DT_TM')), F.unix_timestamp(F.col('CALC_DT_TM')) - F.unix_timestamp(F.col('_DIRECT_MATCH.DEPART_DT_TM'))).otherwise(F.lit(0))
    result = result.withColumn('CALC_ENCNTR_DISTANCE_SECONDS', F.when(F.col('_DIRECT_ENCNTR_ID').isNotNull(), direct_distance).when(F.col('CALC_ENC_WITHIN').isNotNull(), F.lit(0)).when(F.col('CALC_ENC_BEFORE').isNotNull(), F.unix_timestamp(F.col('CALC_DT_TM')) - F.unix_timestamp(F.col('_BEFORE_MATCH.EVENT_DT_TM'))).when(F.col('CALC_ENC_AFTER').isNotNull(), F.unix_timestamp(F.col('_AFTER_MATCH.EVENT_DT_TM')) - F.unix_timestamp(F.col('CALC_DT_TM'))).cast('long'))
    return result.drop('_ENCOUNTERS', '_DIRECT_ENCNTR_ID', '_DIRECT_ENCNTR_SOURCE', '_DIRECT_MATCH', '_WITHIN_MATCH', '_BEFORE_MATCH', '_AFTER_MATCH')

def _finalize_problem_rows(problems: DataFrame) -> DataFrame:
    result = problems.withColumn('ADC_UPDT', F.greatest(F.col('PROBLEM_ADC_UPDT'), F.col('NOMENCLATURE_ADC_UPDT'), F.col('CODE_VALUE_ADC_UPDT'), F.col('CALC_ENCNTR_ADC_UPDT')))
    hash_columns = sorted((column_name for column_name in result.columns if column_name not in {'MAP_ROW_HASH', 'MAP_REFRESH_DT_TM'} and (not column_name.startswith('_'))))
    return result.withColumn('MAP_ROW_HASH', _stable_hash(hash_columns)).withColumn('MAP_REFRESH_DT_TM', F.current_timestamp())

def _build_enriched_problem_rows(source_rows: DataFrame, complete_problem_snapshot: DataFrame, nomenclature: DataFrame, code_maps: DataFrame, earliest_dates: Optional[DataFrame]=None) -> DataFrame:
    if earliest_dates is None:
        groups = source_rows.select('PERSON_ID', 'NOMENCLATURE_ID').dropDuplicates(['PERSON_ID', 'NOMENCLATURE_ID'])
        earliest_dates = _earliest_dates_for_groups(complete_problem_snapshot, groups)
    return _finalize_problem_rows(_attach_encounter_associations(_attach_earliest_dates(_add_problem_dates_and_display(_enrich_code_descriptions(_enrich_nomenclature(source_rows, nomenclature), code_maps)), earliest_dates)))

def _history_and_current_rows(source_rows: DataFrame, complete_problem_snapshot: DataFrame, nomenclature: DataFrame, code_maps: DataFrame, earliest_dates: Optional[DataFrame]=None) -> Tuple[DataFrame, DataFrame]:
    enriched = _build_enriched_problem_rows(source_rows, complete_problem_snapshot, nomenclature, code_maps, earliest_dates=earliest_dates)
    history = _align_to_schema(enriched.withColumn('SOURCE_DELETED_IND', F.lit(False)).withColumn('SOURCE_DELETE_COMMIT_VERSION', F.lit(None).cast('long')).withColumn('SOURCE_DELETE_COMMIT_TIMESTAMP', F.lit(None).cast('timestamp')), schema_map_problem_history_v3)
    current = _align_to_schema(history.filter(F.col('IS_CURRENT_PROBLEM_REVISION')), schema_map_problem_v3)
    return (history, current)

def _cdf_changed_keys(source_name: str, key_column: str, checkpoints: Dict[str, int], end_versions: Dict[str, int]) -> DataFrame:
    table_name = MAP_PROBLEM_SOURCES[source_name]
    start_version = checkpoints[table_name] + 1
    end_version = end_versions[source_name]
    changes = _read_cdf(table_name, start_version, end_version)
    return changes.select(_checked_long(F.col(key_column), table_name + '.' + key_column).alias(key_column)).where(F.col(key_column).isNotNull()).dropDuplicates([key_column])

def _problem_cdf_context(checkpoints: Dict[str, int], end_versions: Dict[str, int]) -> Tuple[DataFrame, DataFrame, DataFrame]:
    table_name = MAP_PROBLEM_SOURCE
    changes = _read_cdf(table_name, checkpoints[table_name] + 1, end_versions['problem'])
    problem_ids = changes.select(_checked_long(F.col('PROBLEM_ID'), 'CDF.PROBLEM_ID').alias('PROBLEM_ID')).where(F.col('PROBLEM_ID').isNotNull()).dropDuplicates(['PROBLEM_ID'])
    instance_ids = changes.select(_checked_long(F.col('PROBLEM_INSTANCE_ID'), 'CDF.PROBLEM_INSTANCE_ID').alias('PROBLEM_INSTANCE_ID')).where(F.col('PROBLEM_INSTANCE_ID').isNotNull()).dropDuplicates(['PROBLEM_INSTANCE_ID'])
    groups = changes.select(_checked_long(F.col('PERSON_ID'), 'CDF.PERSON_ID').alias('PERSON_ID'), _checked_long(F.col('NOMENCLATURE_ID'), 'CDF.NOMENCLATURE_ID').alias('NOMENCLATURE_ID')).where(F.col('PERSON_ID').isNotNull() & F.col('NOMENCLATURE_ID').isNotNull()).dropDuplicates(['PERSON_ID', 'NOMENCLATURE_ID'])
    return (problem_ids, instance_ids, groups)

def _problem_ids_for_nomenclature_ids(problem_snapshot: DataFrame, nomenclature_ids: DataFrame) -> DataFrame:
    if _is_empty(nomenclature_ids):
        return _empty_long_key('PROBLEM_ID')
    primary = problem_snapshot.select('PROBLEM_ID', 'NOMENCLATURE_ID').join(nomenclature_ids.withColumnRenamed('NOMENCLATURE_ID', '_CHANGED_NOMENCLATURE_ID'), F.col('NOMENCLATURE_ID') == F.col('_CHANGED_NOMENCLATURE_ID'), 'left_semi').select('PROBLEM_ID')
    originating = problem_snapshot.select('PROBLEM_ID', 'ORIGINATING_NOMENCLATURE_ID').join(nomenclature_ids.withColumnRenamed('NOMENCLATURE_ID', '_CHANGED_NOMENCLATURE_ID'), F.col('ORIGINATING_NOMENCLATURE_ID') == F.col('_CHANGED_NOMENCLATURE_ID'), 'left_semi').select('PROBLEM_ID')
    return primary.unionByName(originating).dropDuplicates(['PROBLEM_ID'])

def _problem_ids_for_people(problem_snapshot: DataFrame, people: DataFrame) -> DataFrame:
    if _is_empty(people):
        return _empty_long_key('PROBLEM_ID')
    return problem_snapshot.select('PROBLEM_ID', 'PERSON_ID').join(people, 'PERSON_ID', 'left_semi').select('PROBLEM_ID').dropDuplicates(['PROBLEM_ID'])

def _problem_ids_for_code_values(problem_snapshot: DataFrame, nomenclature: DataFrame, code_values: DataFrame) -> DataFrame:
    if _is_empty(code_values):
        return _empty_long_key('PROBLEM_ID')
    exploded_problem_codes = problem_snapshot.select('PROBLEM_ID', F.explode(F.array(*[F.col(name).cast('long') for name in RAW_CODE_COLUMNS])).alias('CODE_VALUE')).where(F.col('CODE_VALUE').isNotNull())
    direct = exploded_problem_codes.join(code_values, 'CODE_VALUE', 'left_semi').select('PROBLEM_ID').dropDuplicates(['PROBLEM_ID'])
    changed_nomenclature_ids = nomenclature.select('NOMENCLATURE_ID', F.explode(F.array(F.col('SOURCE_VOCABULARY_CD').cast('long'), F.col('VOCAB_AXIS_CD').cast('long'))).alias('CODE_VALUE')).where(F.col('CODE_VALUE').isNotNull()).join(code_values, 'CODE_VALUE', 'left_semi').select('NOMENCLATURE_ID').dropDuplicates(['NOMENCLATURE_ID'])
    nomenclature_impacts = _problem_ids_for_nomenclature_ids(problem_snapshot, changed_nomenclature_ids)
    return direct.unionByName(nomenclature_impacts).dropDuplicates(['PROBLEM_ID'])

def _expand_earliest_date_groups(problem_snapshot: DataFrame, seed_problem_ids: DataFrame, cdf_groups: DataFrame) -> DataFrame:
    if _is_empty(seed_problem_ids) and _is_empty(cdf_groups):
        return _empty_long_key('PROBLEM_ID')
    new_groups = problem_snapshot.select('PROBLEM_ID', 'PERSON_ID', 'NOMENCLATURE_ID').join(seed_problem_ids, 'PROBLEM_ID', 'left_semi').select('PERSON_ID', 'NOMENCLATURE_ID')
    old_groups = _empty_pair_df()
    if _table_exists(MAP_PROBLEM_TARGET) and (not _is_empty(seed_problem_ids)):
        old_groups = spark.table(MAP_PROBLEM_TARGET).select('PROBLEM_ID', 'PERSON_ID', 'NOMENCLATURE_ID').join(seed_problem_ids, 'PROBLEM_ID', 'left_semi').select('PERSON_ID', 'NOMENCLATURE_ID')
    affected_groups = new_groups.unionByName(old_groups).unionByName(cdf_groups).where(F.col('PERSON_ID').isNotNull() & F.col('NOMENCLATURE_ID').isNotNull()).dropDuplicates(['PERSON_ID', 'NOMENCLATURE_ID'])
    return problem_snapshot.select('PROBLEM_ID', 'PERSON_ID', 'NOMENCLATURE_ID').join(affected_groups, ['PERSON_ID', 'NOMENCLATURE_ID'], 'left_semi').select('PROBLEM_ID').dropDuplicates(['PROBLEM_ID'])

def _deleted_problem_instance_ids(changed_instance_ids: DataFrame, problem_snapshot: DataFrame) -> DataFrame:
    current_ids = problem_snapshot.select('PROBLEM_INSTANCE_ID').dropDuplicates(['PROBLEM_INSTANCE_ID'])
    return changed_instance_ids.join(current_ids, 'PROBLEM_INSTANCE_ID', 'left_anti').dropDuplicates(['PROBLEM_INSTANCE_ID'])

def _prepare_full_refresh(end_versions: Dict[str, int], run_id: str, minimum_source_ratio: float) -> Dict:
    problem_snapshot = _current_problem_snapshot()
    current_source_count = problem_snapshot.filter(F.col('IS_CURRENT_PROBLEM_REVISION')).count()
    if current_source_count == 0:
        raise RuntimeError('Full refresh refused because the unfiltered source snapshot is empty.')
    if _table_exists(MAP_PROBLEM_TARGET):
        target_count = spark.table(MAP_PROBLEM_TARGET).count()
        if target_count > 0 and current_source_count < target_count * minimum_source_ratio:
            print('[WARN] Current problem source count is below the historical target ratio. Continuing because the version-pinned current source revisions, not the old target population, are authoritative for a reviewed full rebuild.')
    nomenclature = _prepare_nomenclature_snapshot()
    code_maps = _prepare_code_maps()
    earliest_dates = _earliest_dates_for_groups(problem_snapshot)
    history_rows, current_rows = _history_and_current_rows(problem_snapshot, problem_snapshot, nomenclature, code_maps, earliest_dates=earliest_dates)
    print('[INFO] Prepared full unfiltered map_problem refresh from ' + str(current_source_count) + ' current problems and all source revisions.')
    return {'run_id': run_id, 'mode': 'full', 'current_rows': current_rows, 'history_rows': history_rows, 'delete_problem_ids': None, 'delete_instance_ids': None, 'has_current_rows': True, 'has_history_rows': True, 'has_problem_deletes': False, 'has_instance_deletes': False, 'end_versions': end_versions, 'persisted': [problem_snapshot, nomenclature, code_maps, history_rows], 'metrics': {'current_source_count': current_source_count}}

def _prepare_incremental_refresh(checkpoints: Dict[str, int], end_versions: Dict[str, int], run_id: str) -> Dict:
    problem_snapshot = _current_problem_snapshot()
    nomenclature = _prepare_nomenclature_snapshot()
    code_maps = _prepare_code_maps()
    persisted: List[DataFrame] = [problem_snapshot, nomenclature, code_maps]
    changed_problem_ids, changed_instance_ids, cdf_groups = _problem_cdf_context(checkpoints, end_versions)
    changed_nomenclature_ids = _cdf_changed_keys('nomenclature', 'NOMENCLATURE_ID', checkpoints, end_versions)
    changed_people = _read_cdf(MAP_PROBLEM_ENCOUNTER, checkpoints[MAP_PROBLEM_ENCOUNTER] + 1, end_versions['encounter']).select(_checked_long(F.col('PERSON_ID'), 'ENCOUNTER_CDF.PERSON_ID').alias('PERSON_ID')).where(F.col('PERSON_ID').isNotNull()).dropDuplicates(['PERSON_ID'])
    changed_code_values = _cdf_changed_keys('code_value', 'CODE_VALUE', checkpoints, end_versions)
    nomenclature_problem_ids = _problem_ids_for_nomenclature_ids(problem_snapshot, changed_nomenclature_ids)
    encounter_problem_ids = _problem_ids_for_people(problem_snapshot, changed_people)
    code_problem_ids = _problem_ids_for_code_values(problem_snapshot, nomenclature, changed_code_values)
    direct_refresh_ids = _union_keys([changed_problem_ids, nomenclature_problem_ids, encounter_problem_ids, code_problem_ids], 'PROBLEM_ID')
    group_member_ids = _expand_earliest_date_groups(problem_snapshot, direct_refresh_ids, cdf_groups)
    refresh_ids = _union_keys([direct_refresh_ids, group_member_ids], 'PROBLEM_ID')
    persisted.append(refresh_ids)
    deleted_instance_ids = _deleted_problem_instance_ids(changed_instance_ids, problem_snapshot)
    persisted.append(deleted_instance_ids)
    has_instance_deletes = not _is_empty(deleted_instance_ids)
    has_refresh_ids = not _is_empty(refresh_ids)
    if has_refresh_ids:
        refresh_source_rows = problem_snapshot.join(refresh_ids, 'PROBLEM_ID', 'inner')
        refresh_groups = refresh_source_rows.select('PERSON_ID', 'NOMENCLATURE_ID').dropDuplicates(['PERSON_ID', 'NOMENCLATURE_ID'])
        earliest_dates = _earliest_dates_for_groups(problem_snapshot, refresh_groups)
        history_rows, current_rows = _history_and_current_rows(refresh_source_rows, problem_snapshot, nomenclature, code_maps, earliest_dates=earliest_dates)
        persisted.append(history_rows)
    else:
        history_rows = _empty_history_rows()
        current_rows = _empty_problem_rows()
    current_ids = current_rows.select('PROBLEM_ID').dropDuplicates(['PROBLEM_ID'])
    delete_problem_ids = refresh_ids.join(current_ids, 'PROBLEM_ID', 'left_anti')
    persisted.append(delete_problem_ids)
    has_problem_deletes = not _is_empty(delete_problem_ids)
    print('[INFO] Prepared incremental map_problem update. refresh_ids=' + str(has_refresh_ids) + ', problem_deletes=' + str(has_problem_deletes) + ', instance_tombstones=' + str(has_instance_deletes) + '.')
    return {'run_id': run_id, 'mode': 'incremental', 'current_rows': current_rows, 'history_rows': history_rows, 'delete_problem_ids': delete_problem_ids, 'delete_instance_ids': deleted_instance_ids, 'has_current_rows': has_refresh_ids, 'has_history_rows': has_refresh_ids, 'has_problem_deletes': has_problem_deletes, 'has_instance_deletes': has_instance_deletes, 'end_versions': end_versions, 'persisted': persisted, 'metrics': {}}

def prepare_map_problem_update(force_full_refresh: Optional[bool]=None, auto_full_refresh_on_cdf_gap: bool=False, minimum_full_refresh_source_ratio: float=0.95) -> Dict:
    global _MAP_PROBLEM_ACTIVE_SOURCE_VERSIONS
    ensure_map_problem_control_tables()
    run_id = uuid4().hex
    end_versions = _capture_source_versions()
    _MAP_PROBLEM_ACTIVE_SOURCE_VERSIONS = {MAP_PROBLEM_SOURCES[source_name]: version for source_name, version in end_versions.items()}
    checkpoints = _read_checkpoint_versions()
    requires_full, reason = _target_requires_full_rebuild(checkpoints, end_versions)
    if force_full_refresh is True or (force_full_refresh is None and requires_full):
        print('[INFO] Preparing full map_problem refresh: ' + reason)
        return _prepare_full_refresh(end_versions, run_id, minimum_full_refresh_source_ratio)
    if force_full_refresh is False and requires_full:
        raise RuntimeError('Incremental mode was explicitly requested, but a full corrective refresh is required: ' + reason)
    try:
        return _prepare_incremental_refresh(checkpoints, end_versions, run_id)
    except MapProblemCDFGap:
        if not auto_full_refresh_on_cdf_gap:
            raise
        print('[WARN] Source CDF history has expired; switching to a guarded full refresh.')
        return _prepare_full_refresh(end_versions, run_id, minimum_full_refresh_source_ratio)

def _assert_unique_rows(rows: DataFrame, key_column: str, label: str) -> None:
    duplicates = rows.groupBy(key_column).count().filter(F.col('count') > 1).limit(1)
    if not duplicates.isEmpty():
        raise RuntimeError(label + ' contains duplicate ' + key_column + ' values.')

def _overwrite_map_problem_tables(history_rows: DataFrame, current_rows: DataFrame) -> None:
    stage_table = MAP_PROBLEM_HISTORY + '__rebuild_' + uuid4().hex
    try:
        _align_to_schema(history_rows, schema_map_problem_history_v3).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(stage_table)
        staged_history = spark.table(stage_table)
        _assert_unique_rows(staged_history, 'PROBLEM_INSTANCE_ID', 'history refresh')
        staged_current = bronze_project_contract(
            staged_history.filter(F.col('IS_CURRENT_PROBLEM_REVISION')),
            MAP_PROBLEM_TARGET,
        )
        _assert_unique_rows(staged_current, 'PROBLEM_ID', 'current refresh')
        staged_history.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('delta.enableChangeDataFeed', 'true').option('delta.enableDeletionVectors', 'true').option('delta.enableRowTracking', 'true').saveAsTable(MAP_PROBLEM_HISTORY)
        staged_current.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('delta.enableChangeDataFeed', 'true').option('delta.enableDeletionVectors', 'true').option('delta.enableRowTracking', 'true').saveAsTable(MAP_PROBLEM_TARGET)
        _set_target_metadata(apply_column_comments=True)
    finally:
        spark.sql(f'DROP TABLE IF EXISTS {_sql_name(stage_table)}')

def _merge_rows(table_name: str, rows: DataFrame, key_column: str, hash_column: str='MAP_ROW_HASH') -> None:
    if _is_empty(rows):
        return
    if bronze_is_primary_table(table_name):
        rows = bronze_project_contract(rows, table_name)
    _assert_unique_rows(rows, key_column, table_name + ' merge source')
    target_columns = [field.name for field in spark.table(table_name).schema.fields if field.name in rows.columns]
    insert_values = {name: 's.' + _sql_column(name) for name in target_columns}
    update_values = {name: 's.' + _sql_column(name) for name in target_columns if name != key_column}
    condition = 'NOT (t.{hash_col} <=> s.{hash_col})'.format(hash_col=_sql_column(hash_column))
    if table_name == MAP_PROBLEM_HISTORY:
        condition = condition + ' OR COALESCE(t.SOURCE_DELETED_IND, false) <> COALESCE(s.SOURCE_DELETED_IND, false)'
    DeltaTable.forName(spark, table_name).alias('t').merge(rows.alias('s'), 't.' + _sql_column(key_column) + ' <=> s.' + _sql_column(key_column)).whenMatchedUpdate(condition=condition, set=update_values).whenNotMatchedInsert(values=insert_values).execute()

def _delete_current_problem_ids(problem_ids: DataFrame) -> None:
    if _is_empty(problem_ids):
        return
    keys = problem_ids.select(_checked_long(F.col('PROBLEM_ID'), 'PROBLEM_ID', required=True).alias('PROBLEM_ID')).dropDuplicates(['PROBLEM_ID'])
    DeltaTable.forName(spark, MAP_PROBLEM_TARGET).alias('t').merge(keys.alias('d'), 't.PROBLEM_ID <=> d.PROBLEM_ID').whenMatchedDelete().execute()

def _mark_history_tombstones(instance_ids: DataFrame, source_version: int) -> None:
    if _is_empty(instance_ids):
        return
    applied_at = datetime.now(timezone.utc).replace(tzinfo=None)
    tombstones = instance_ids.select(_checked_long(F.col('PROBLEM_INSTANCE_ID'), 'PROBLEM_INSTANCE_ID', required=True).alias('PROBLEM_INSTANCE_ID')).dropDuplicates(['PROBLEM_INSTANCE_ID']).withColumn('SOURCE_DELETED_IND', F.lit(True)).withColumn('SOURCE_DELETE_COMMIT_VERSION', F.lit(int(source_version)).cast('long')).withColumn('SOURCE_DELETE_COMMIT_TIMESTAMP', F.lit(applied_at).cast('timestamp'))
    DeltaTable.forName(spark, MAP_PROBLEM_HISTORY).alias('t').merge(tombstones.alias('s'), 't.PROBLEM_INSTANCE_ID <=> s.PROBLEM_INSTANCE_ID').whenMatchedUpdate(set={'SOURCE_DELETED_IND': 's.SOURCE_DELETED_IND', 'SOURCE_DELETE_COMMIT_VERSION': 's.SOURCE_DELETE_COMMIT_VERSION', 'SOURCE_DELETE_COMMIT_TIMESTAMP': 's.SOURCE_DELETE_COMMIT_TIMESTAMP'}).whenNotMatchedInsert(values={'PROBLEM_INSTANCE_ID': 's.PROBLEM_INSTANCE_ID', 'SOURCE_DELETED_IND': 's.SOURCE_DELETED_IND', 'SOURCE_DELETE_COMMIT_VERSION': 's.SOURCE_DELETE_COMMIT_VERSION', 'SOURCE_DELETE_COMMIT_TIMESTAMP': 's.SOURCE_DELETE_COMMIT_TIMESTAMP'}).execute()

def commit_map_problem_update(plan: Dict) -> Dict:
    global _MAP_PROBLEM_ACTIVE_SOURCE_VERSIONS
    mode = plan['mode']
    run_id = plan['run_id']
    try:
        _write_audit_event(run_id, 'STARTED', mode, 'Committing prepared map_problem update.', plan['end_versions'], plan.get('metrics'))
        if mode == 'full':
            _overwrite_map_problem_tables(plan['history_rows'], plan['current_rows'])
        else:
            if plan['has_history_rows']:
                _merge_rows(MAP_PROBLEM_HISTORY, plan['history_rows'], 'PROBLEM_INSTANCE_ID')
            if plan['has_instance_deletes']:
                _mark_history_tombstones(plan['delete_instance_ids'], plan['end_versions']['problem'])
            if plan['has_current_rows']:
                _merge_rows(MAP_PROBLEM_TARGET, plan['current_rows'], 'PROBLEM_ID')
            if plan['has_problem_deletes']:
                _delete_current_problem_ids(plan['delete_problem_ids'])
            _set_target_metadata(apply_column_comments=False)
        _write_checkpoint_versions(plan['end_versions'], run_id)
        result = {'run_id': run_id, 'mode': mode, 'checkpoint_versions': plan['end_versions'], **plan.get('metrics', {})}
        _write_audit_event(run_id, 'SUCCEEDED', mode, 'map_problem current and history updates committed.', plan['end_versions'], result)
        print('[SUCCESS] map_problem ' + mode + ' update committed with run_id=' + run_id + '.')
        return result
    except Exception as exc:
        _write_audit_event(run_id, 'FAILED', mode, str(exc), plan.get('end_versions'), plan.get('metrics'))
        raise
    finally:
        _MAP_PROBLEM_ACTIVE_SOURCE_VERSIONS = {}
        seen = set()
        for frame in plan.get('persisted', []):
            identifier = id(frame)
            if identifier in seen:
                continue
            seen.add(identifier)
            try:
                None
            except Exception:
                pass

def run_map_problem_update(force_full_refresh: Optional[bool]=None, auto_full_refresh_on_cdf_gap: bool=False, minimum_full_refresh_source_ratio: float=0.95) -> Dict:
    global _MAP_PROBLEM_ACTIVE_SOURCE_VERSIONS
    try:
        plan = prepare_map_problem_update(force_full_refresh=force_full_refresh, auto_full_refresh_on_cdf_gap=auto_full_refresh_on_cdf_gap, minimum_full_refresh_source_ratio=minimum_full_refresh_source_ratio)
    except Exception:
        _MAP_PROBLEM_ACTIVE_SOURCE_VERSIONS = {}
        raise
    return commit_map_problem_update(plan)
if '_MAP_UPDATES_ORIGINAL_UPDATE_TABLE' not in globals():
    _MAP_UPDATES_ORIGINAL_UPDATE_TABLE = globals().get('update_table')

def create_problem_mapping_incr() -> DataFrame:
    global _MAP_PROBLEM_PENDING_PLAN
    global _MAP_PROBLEM_ACTIVE_SOURCE_VERSIONS
    if _MAP_PROBLEM_PENDING_PLAN is not None:
        raise RuntimeError('A map_problem plan is already pending. Commit it before preparing another.')
    try:
        _MAP_PROBLEM_PENDING_PLAN = prepare_map_problem_update()
    except Exception:
        _MAP_PROBLEM_ACTIVE_SOURCE_VERSIONS = {}
        raise
    return _MAP_PROBLEM_PENDING_PLAN['current_rows']

def update_table(source_df, target_table: str, index_column, target_schema: T.StructType=None, table_comment: str=None, update_condition: str=None):
    global _MAP_PROBLEM_PENDING_PLAN
    if _normalized_table_name(target_table) == _normalized_table_name(MAP_PROBLEM_TARGET):
        if _MAP_PROBLEM_PENDING_PLAN is None:
            raise RuntimeError('No prepared map_problem v3 plan exists. Call create_problem_mapping_incr first.')
        try:
            return commit_map_problem_update(_MAP_PROBLEM_PENDING_PLAN)
        finally:
            _MAP_PROBLEM_PENDING_PLAN = None
    if _MAP_UPDATES_ORIGINAL_UPDATE_TABLE is None:
        raise RuntimeError('The original Map Pipeline update_table function was not defined when Map Problem Replacement was imported.')
    return _MAP_UPDATES_ORIGINAL_UPDATE_TABLE(source_df, target_table, index_column, target_schema, table_comment, update_condition)

def validate_map_problem_v3() -> Dict[str, DataFrame]:
    target = _sql_name(MAP_PROBLEM_TARGET)
    history = _sql_name(MAP_PROBLEM_HISTORY)
    source = _sql_name(MAP_PROBLEM_SOURCE)
    nomenclature = _sql_name(MAP_PROBLEM_NOMENCLATURE)
    encounter = _sql_name(MAP_PROBLEM_ENCOUNTER)
    return {'coverage': spark.sql('\n            WITH source_ids AS (\n                SELECT DISTINCT CAST(PROBLEM_ID AS BIGINT) AS PROBLEM_ID FROM {source}\n            ), target_ids AS (\n                SELECT DISTINCT PROBLEM_ID FROM {target}\n            )\n            SELECT\n                (SELECT COUNT(*) FROM source_ids) AS source_problem_ids,\n                (SELECT COUNT(*) FROM target_ids) AS target_problem_ids,\n                (SELECT COUNT(*) FROM source_ids LEFT ANTI JOIN target_ids USING (PROBLEM_ID)) AS missing_target_ids,\n                (SELECT COUNT(*) FROM target_ids LEFT ANTI JOIN source_ids USING (PROBLEM_ID)) AS stale_target_ids\n            '.format(source=source, target=target)), 'revision_coverage': spark.sql('\n            WITH source_ids AS (\n                SELECT DISTINCT CAST(PROBLEM_INSTANCE_ID AS BIGINT) AS PROBLEM_INSTANCE_ID FROM {source}\n            ), history_ids AS (\n                SELECT DISTINCT PROBLEM_INSTANCE_ID\n                FROM {history}\n                WHERE NOT COALESCE(SOURCE_DELETED_IND, false)\n            )\n            SELECT\n                (SELECT COUNT(*) FROM source_ids) AS source_instance_ids,\n                (SELECT COUNT(*) FROM history_ids) AS active_history_instance_ids,\n                (SELECT COUNT(*) FROM source_ids LEFT ANTI JOIN history_ids USING (PROBLEM_INSTANCE_ID)) AS missing_history_instances,\n                (SELECT COUNT(*) FROM history_ids LEFT ANTI JOIN source_ids USING (PROBLEM_INSTANCE_ID)) AS stale_active_history_instances\n            '.format(source=source, history=history)), 'integrity': spark.sql('\n            SELECT\n                COUNT(*) AS rows,\n                COUNT(DISTINCT PROBLEM_ID) AS distinct_problem_ids,\n                COUNT_IF(PROBLEM_INSTANCE_ID IS NULL) AS null_problem_instance_ids,\n                COUNT_IF(MAP_ROW_HASH IS NULL) AS null_row_hashes,\n                COUNT_IF(PROBLEM_DISPLAY IS NULL) AS missing_problem_display,\n                COUNT_IF(CALC_DT_TM_SOURCE IS NULL) AS missing_calc_date_source\n            FROM {target}\n            '.format(target=target)), 'selected_revision': spark.sql('\n            WITH expected AS (\n                SELECT\n                    CAST(PROBLEM_ID AS BIGINT) AS PROBLEM_ID,\n                    CAST(PROBLEM_INSTANCE_ID AS BIGINT) AS PROBLEM_INSTANCE_ID\n                FROM {source}\n                QUALIFY ROW_NUMBER() OVER (\n                    PARTITION BY PROBLEM_ID\n                    ORDER BY PROBLEM_INSTANCE_ID DESC NULLS LAST,\n                             UPDT_DT_TM DESC NULLS LAST,\n                             UPDT_CNT DESC NULLS LAST,\n                             ADC_UPDT DESC NULLS LAST,\n                             LAST_UTC_TS DESC NULLS LAST\n                ) = 1\n            )\n            SELECT COUNT_IF(NOT (t.PROBLEM_INSTANCE_ID <=> e.PROBLEM_INSTANCE_ID)) AS incorrect_selected_revisions\n            FROM {target} t\n            FULL OUTER JOIN expected e USING (PROBLEM_ID)\n            '.format(target=target, source=source)), 'earliest_dates': spark.sql('\n            WITH expected AS (\n                SELECT\n                    CAST(PERSON_ID AS BIGINT) AS PERSON_ID,\n                    CAST(NOMENCLATURE_ID AS BIGINT) AS NOMENCLATURE_ID,\n                    MIN(ONSET_DT_TM) AS expected_earliest\n                FROM {source}\n                GROUP BY CAST(PERSON_ID AS BIGINT), CAST(NOMENCLATURE_ID AS BIGINT)\n            )\n            SELECT COUNT_IF(NOT (t.earliest_problem_date <=> e.expected_earliest)) AS incorrect_rows\n            FROM {target} t\n            LEFT JOIN expected e\n              ON t.PERSON_ID <=> e.PERSON_ID\n             AND t.NOMENCLATURE_ID <=> e.NOMENCLATURE_ID\n            '.format(target=target, source=source)), 'nomenclature_freshness': spark.sql('\n            SELECT COUNT_IF(\n                NOT (\n                    t.NOMENCLATURE_ADC_UPDT <=> GREATEST(p.ADC_UPDT, o.ADC_UPDT)\n                )\n            ) AS stale_nomenclature_rows\n            FROM {history} t\n            LEFT JOIN {nomenclature} p\n              ON CAST(t.NOMENCLATURE_ID AS DECIMAL(38,18)) = p.NOMENCLATURE_ID\n            LEFT JOIN {nomenclature} o\n              ON CAST(t.ORIGINATING_NOMENCLATURE_ID AS DECIMAL(38,18)) = o.NOMENCLATURE_ID\n            WHERE COALESCE(t.IS_CURRENT_PROBLEM_REVISION, false)\n              AND NOT COALESCE(t.SOURCE_DELETED_IND, false)\n            '.format(history=history, nomenclature=nomenclature)), 'encounter_integrity': spark.sql('\n            SELECT\n                COUNT_IF(t.CALC_ENCNTR IS NOT NULL AND e.ENCNTR_ID IS NULL) AS calc_encounter_not_found,\n                COUNT_IF(\n                    t.CALC_ENCNTR IS NOT NULL\n                    AND e.ENCNTR_ID IS NOT NULL\n                    AND t.PERSON_ID <> CAST(e.PERSON_ID AS BIGINT)\n                ) AS calc_encounter_wrong_person\n            FROM {target} t\n            LEFT JOIN {encounter} e\n              ON t.CALC_ENCNTR = CAST(e.ENCNTR_ID AS BIGINT)\n            '.format(target=target, encounter=encounter))}

try:
    run_map_problem_update(force_full_refresh=(True if _pipeline_component_force_full_refresh('map_problem') else None), auto_full_refresh_on_cdf_gap=True)
    _PIPELINE_UPDATED_TARGETS.extend(['4_prod.bronze.map_problem'])
    _pipeline_mark_component_complete('map_problem', ['4_prod.bronze.map_problem'])
    _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_problem'})
except Exception as exc:
    _pipeline_record_error(None, exc)
    raise
finally:
    if _pipeline_shared_update_table is not None:
        update_table = _pipeline_shared_update_table
    if _pipeline_shared_table_exists is not None:
        table_exists = _pipeline_shared_table_exists
    if _pipeline_shared_get_max_timestamp is not None:
        get_max_timestamp = _pipeline_shared_get_max_timestamp
    if _pipeline_shared_has_cdf_enabled is not None:
        has_cdf_enabled = _pipeline_shared_has_cdf_enabled
    if _pipeline_shared_get_incremental is not None:
        get_incremental_data_with_cdf = _pipeline_shared_get_incremental

# COMMAND ----------

_pipeline_component_start("map_med_admin")
_pipeline_shared_update_table = globals().get('update_table')
_pipeline_shared_table_exists = globals().get('table_exists')
_pipeline_shared_get_max_timestamp = globals().get('get_max_timestamp')
_pipeline_shared_has_cdf_enabled = globals().get('has_cdf_enabled')
_pipeline_shared_get_incremental = globals().get('get_incremental_data_with_cdf')
globals().pop('_map_updates_original_update_table', None)
globals().pop('_map_medical_personnel_original_update_table', None)
globals().pop('_MAP_UPDATES_ORIGINAL_UPDATE_TABLE', None)

from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
import json
import uuid
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

@dataclass(frozen=True)
class MapMedAdminConfig:
    clinical_event_table: str = '4_prod.raw.mill_clinical_event'
    med_admin_event_table: str = '4_prod.raw.mill_med_admin_event'
    med_result_table: str = '4_prod.raw.mill_ce_med_result'
    orders_table: str = '4_prod.raw.mill_orders'
    order_ingredient_table: str = '4_prod.raw.mill_order_ingredient'
    order_synonym_table: str = '3_lookup.mill.mill_order_catalog_synonym'
    medication_lookup_table: str = '3_lookup.mill.map_med_lookup'
    code_value_table: str = '3_lookup.mill.mill_code_value'
    rxnconso_table: str = '3_lookup.rxnorm.rxnconso'
    concept_table: str = '3_lookup.omop.concept'
    concept_relationship_table: str = '3_lookup.omop.concept_relationship'
    target_table: str = '4_prod.bronze.map_med_admin'
    ingredient_target_table: str = '4_prod.bronze.map_med_admin_ingredient'
    state_table: str = '6_mgmt.bronze.map_med_admin_pipeline_state'
    audit_table: str = '6_mgmt.bronze.map_med_admin_pipeline_audit'
    schema_version: str = '3.0.0'
    mapping_version: str = '3.0.0'
    unit_rules_version: str = '3.0.0'
    default_similarity_threshold: float = 0.6
    maintain_ingredient_child: bool = True
    enable_exact_unique_name_fallback: bool = True
    extreme_absolute_mass_mg: float = 1000000000.0
    extreme_absolute_volume_ml: float = 1000000000.0
    extreme_raw_dose: float = 1000000000.0

    @property
    def tracked_tables(self) -> Tuple[str, ...]:
        return (self.clinical_event_table, self.med_admin_event_table, self.med_result_table, self.orders_table, self.order_ingredient_table, self.order_synonym_table, self.medication_lookup_table, self.code_value_table, self.rxnconso_table, self.concept_table, self.concept_relationship_table)
MAP_MED_ADMIN_CONFIG = MapMedAdminConfig()
map_med_admin_comment = 'Medication-administration foundation at one row per logical EVENT_ID. No source rows are excluded because of result status, validity, event type, Trust or encounter availability. Raw source values, source identifiers, mapping provenance, mapping ambiguity, unit-parsing status and pipeline provenance are retained. All order-ingredient actions/components are preserved in map_med_admin_ingredient.'
map_med_admin_ingredient_comment = 'Lossless medication-order ingredient actions/components associated with map_med_admin events. Rows retain actual/template match path, action/component sequence and source provenance.'
_PENDING_MED_ADMIN_PLAN: Dict[str, object] = {}
_PENDING_MED_ADMIN_CACHES: List[str] = []

def _ma_field(name: str, data_type: T.DataType, comment: str, nullable: bool=True) -> T.StructField:
    return T.StructField(name, data_type, nullable, metadata={'comment': comment})
schema_map_med_admin = T.StructType([_ma_field('PERSON_ID', T.LongType(), 'Best available source person identifier.'), _ma_field('ENCNTR_ID', T.LongType(), 'Best available source encounter identifier.'), _ma_field('EVENT_ID', T.LongType(), 'Logical clinical event identifier and target key.', nullable=False), _ma_field('ORDER_ID', T.LongType(), 'Best available source medication order identifier.'), _ma_field('EVENT_TYPE_CD', T.LongType(), 'Unfiltered medication-administration event type code.'), _ma_field('EVENT_TYPE_DISPLAY', T.StringType(), 'Best available display for EVENT_TYPE_CD.'), _ma_field('RESULT_STATUS_CD', T.LongType(), 'Unfiltered clinical-event result status code.'), _ma_field('RESULT_STATUS_DISPLAY', T.StringType(), 'Best available display for RESULT_STATUS_CD.'), _ma_field('ADMIN_START_DT_TM', T.TimestampType(), 'Convenience administration start timestamp.'), _ma_field('ADMIN_END_DT_TM', T.TimestampType(), 'Convenience administration end timestamp.'), _ma_field('ADMIN_START_DT_TM_SOURCE', T.StringType(), 'Source field supplying ADMIN_START_DT_TM.'), _ma_field('ADMIN_END_DT_TM_SOURCE', T.StringType(), 'Source field supplying ADMIN_END_DT_TM.'), _ma_field('ORDER_SYNONYM_ID', T.LongType(), 'Medication order synonym identifier.'), _ma_field('ORDER_CKI', T.StringType(), 'Actual order CKI from ORDERS.CKI or ORDER_CATALOG_SYNONYM.CKI.'), _ma_field('MULTUM', T.StringType(), 'Multum code from the medication mapping lookup.'), _ma_field('RXNORM_CUI', T.StringType(), 'RxNorm concept identifier from the medication mapping lookup.'), _ma_field('RXNORM_STR', T.StringType(), 'Deterministically selected preferred RxNorm description.'), _ma_field('SNOMED_CODE', T.StringType(), 'Legacy semantics: medication-lookup SNOMED code, else the legacy ORDER_MNEMONIC term-match lanes; see SNOMED_VALIDATED_CODE for the standard-validated variant.'), _ma_field('SNOMED_STR', T.StringType(), 'Legacy semantics: description of the medication-lookup SNOMED code only; null whenever the code came from a legacy term-match lane.'), _ma_field('ORDER_MNEMONIC', T.StringType(), 'Best available order mnemonic without replacing source-specific fields.'), _ma_field('ORDER_DETAIL', T.StringType(), 'Best available order/ingredient detail display text.'), _ma_field('ORDER_STRENGTH', T.DoubleType(), 'Representative ingredient strength.'), _ma_field('ORDER_STRENGTH_UNIT_CD', T.LongType(), 'Representative ingredient strength unit code.'), _ma_field('ORDER_STRENGTH_UNIT_DISPLAY', T.StringType(), 'Display for representative strength unit.'), _ma_field('ORDER_VOLUME', T.DoubleType(), 'Representative ingredient volume.'), _ma_field('ORDER_VOLUME_UNIT_CD', T.LongType(), 'Representative ingredient volume unit code.'), _ma_field('ORDER_VOLUME_UNIT_DISPLAY', T.StringType(), 'Display for representative volume unit.'), _ma_field('ADMIN_ROUTE_CD', T.LongType(), 'Source administration route code.'), _ma_field('ADMIN_ROUTE_DISPLAY', T.StringType(), 'Display for administration route.'), _ma_field('INITIAL_DOSAGE', T.DoubleType(), 'Source initial dosage value.'), _ma_field('INITIAL_DOSAGE_UNIT_CD', T.LongType(), 'Source initial dosage unit code.'), _ma_field('INITIAL_DOSAGE_UNIT_DISPLAY', T.StringType(), 'Display for initial dosage unit.'), _ma_field('ADMIN_DOSAGE', T.DoubleType(), 'Source administered dosage value.'), _ma_field('ADMIN_DOSAGE_UNIT_CD', T.LongType(), 'Source administered dosage unit code.'), _ma_field('ADMIN_DOSAGE_UNIT_DISPLAY', T.StringType(), 'Display for administered dosage unit.'), _ma_field('INITIAL_VOLUME', T.DoubleType(), 'Source initial volume; never substituted for a mass dose.'), _ma_field('INFUSED_VOLUME', T.DoubleType(), 'Source infused volume.'), _ma_field('INFUSED_VOLUME_UNIT_CD', T.LongType(), 'Source infused-volume unit code.'), _ma_field('INFUSED_VOLUME_UNIT_DISPLAY', T.StringType(), 'Display for infused-volume unit.'), _ma_field('INFUSION_RATE', T.DoubleType(), 'Source infusion rate.'), _ma_field('INFUSION_UNIT_CD', T.LongType(), 'Source infusion-rate unit code.'), _ma_field('INFUSION_UNIT_DISPLAY', T.StringType(), 'Display for infusion-rate unit.'), _ma_field('NURSE_UNIT_CD', T.LongType(), 'Source documenting nurse-unit code.'), _ma_field('NURSE_UNIT_DISPLAY', T.StringType(), 'Display for documenting nurse unit.'), _ma_field('POSITION_CD', T.LongType(), 'Source documenting personnel position code.'), _ma_field('POSITION_DISPLAY', T.StringType(), 'Display for documenting position.'), _ma_field('PRSNL_ID', T.LongType(), 'Personnel identifier documenting the medication event.'), _ma_field('ADC_UPDT', T.TimestampType(), 'Greatest row-level source ADC timestamp; not used as a pipeline checkpoint.'), _ma_field('DOSE_IN_MG', T.DoubleType(), 'Absolute administered mass in mg only for exact absolute mass units.'), _ma_field('DOSE_IN_ML', T.DoubleType(), 'Absolute administered volume in mL only for exact absolute volume units.'), _ma_field('DOSE_UNIT_CATEGORY', T.StringType(), 'ABSOLUTE_MASS, ABSOLUTE_VOLUME, UNITS, DISCRETE, RATIO_OR_RATE, OTHER or MISSING.'), _ma_field('SNOMED_SOURCE', T.StringType(), 'Legacy semantics: ORIGINAL, SNOMED_SCT, OMOP_FORWARD, OMOP_BACKWARD, SNOMED_SCT_SIMPLIFIED, OMOP_FORWARD_SIMPLIFIED, OMOP_BACKWARD_SIMPLIFIED, OMOP or NOT_FOUND; see SNOMED_VALIDATED_SOURCE.'), _ma_field('OMOP_CONCEPT_ID', T.LongType(), 'Legacy semantics: first OMOP concept found across the Multum, RxNorm, RxNorm Extension, SNOMED, exact-name then simplified-name lanes; combination products are preserved and the concept is not Standard-validated. See OMOP_STANDARD_CONCEPT_ID.'), _ma_field('OMOP_CONCEPT_NAME', T.StringType(), 'Legacy semantics: name of the legacy OMOP_CONCEPT_ID; see OMOP_STANDARD_CONCEPT_NAME.'), _ma_field('OMOP_STANDARD_CONCEPT', T.StringType(), 'Legacy semantics: standard_concept flag carried straight from the legacy concept row, so null for non-standard concepts; see OMOP_STANDARD_CONCEPT_FLAG.'), _ma_field('OMOP_TYPE', T.StringType(), 'Legacy semantics: MULTUM, RXNORM, RXNORMEXT, SNOMED, NAME_MATCH_<vocabulary_id> or SIMPLIFIED_MATCH_<vocabulary_id>, else null; see OMOP_MAPPING_METHOD for the standard-validated label.'), _ma_field('CLINICAL_EVENT_ID', T.LongType(), 'Unique source CLINICAL_EVENT row identifier.'), _ma_field('MED_ADMIN_EVENT_ID', T.LongType(), 'Unique source MED_ADMIN_EVENT row identifier.'), _ma_field('MAE_ORDER_ID', T.LongType(), 'Order identifier recorded on MED_ADMIN_EVENT.'), _ma_field('MR_SYNONYM_ID', T.LongType(), 'Synonym identifier recorded on CE_MED_RESULT.'), _ma_field('TEMPLATE_ORDER_ID', T.LongType(), 'Template order identifier used for ingredient linkage.'), _ma_field('CE_VALID_FROM_DT_TM', T.TimestampType(), 'Source clinical-event validity start.'), _ma_field('CE_VALID_UNTIL_DT_TM', T.TimestampType(), 'Source clinical-event validity end.'), _ma_field('CE_IS_CURRENT_IND', T.BooleanType(), 'True when selected clinical-event row is currently valid.'), _ma_field('CE_VERSION_COUNT', T.LongType(), 'Number of source clinical-event rows for EVENT_ID.'), _ma_field('CE_CURRENT_VERSION_COUNT', T.LongType(), 'Number of currently valid clinical-event rows for EVENT_ID.'), _ma_field('MAE_ROW_COUNT', T.LongType(), 'Number of MED_ADMIN_EVENT rows for EVENT_ID.'), _ma_field('MR_ROW_COUNT', T.LongType(), 'Number of CE_MED_RESULT rows for EVENT_ID.'), _ma_field('MR_CURRENT_ROW_COUNT', T.LongType(), 'Number of currently valid CE_MED_RESULT rows for EVENT_ID.'), _ma_field('EVENT_CLASS_CD', T.LongType(), 'Source clinical-event class code.'), _ma_field('EVENT_CD', T.LongType(), 'Source clinical-event code.'), _ma_field('EVENT_TAG', T.StringType(), 'Source clinical-event display tag.'), _ma_field('EVENT_TITLE_TEXT', T.StringType(), 'Source clinical-event title text.'), _ma_field('PARENT_EVENT_ID', T.LongType(), 'Source parent event identifier.'), _ma_field('EVENT_RELTN_CD', T.LongType(), 'Source event relationship code.'), _ma_field('RECORD_STATUS_CD', T.LongType(), 'Source clinical-event record status code.'), _ma_field('AUTHENTIC_FLAG', T.LongType(), 'Source clinical-event authentication flag.'), _ma_field('PUBLISH_FLAG', T.LongType(), 'Source clinical-event publish flag.'), _ma_field('PERFORMED_DT_TM', T.TimestampType(), 'Source clinical-event performed timestamp.'), _ma_field('PERFORMED_PRSNL_ID', T.LongType(), 'Source performing personnel identifier.'), _ma_field('VERIFIED_DT_TM', T.TimestampType(), 'Source clinical-event verified timestamp.'), _ma_field('CE_VERIFIED_PRSNL_ID', T.LongType(), 'Source clinical-event verifying personnel identifier.'), _ma_field('ENTRY_MODE_CD', T.LongType(), 'Source clinical-event entry mode code.'), _ma_field('CE_SOURCE_CD', T.LongType(), 'Source clinical-event source code.'), _ma_field('CONTRIBUTOR_SYSTEM_CD', T.LongType(), 'Source clinical-event contributor-system code.'), _ma_field('DEVICE_FREE_TXT', T.StringType(), 'Source device free text.'), _ma_field('ORDER_ACTION_SEQUENCE', T.LongType(), 'Source clinical-event order action sequence.'), _ma_field('DOCUMENTATION_ACTION_SEQUENCE', T.LongType(), 'Source medication documentation action sequence.'), _ma_field('POSITIVE_PATIENT_IDENT_IND', T.LongType(), 'Positive patient identification indicator.'), _ma_field('POSITIVE_MED_IDENT_IND', T.LongType(), 'Positive medication identification indicator.'), _ma_field('ORDER_RESULT_VARIANCE_IND', T.LongType(), 'Indicator that documented administration differed from order details.'), _ma_field('CLINICAL_WARNING_CNT', T.LongType(), 'Number of clinical warnings presented.'), _ma_field('EVENT_CNT', T.LongType(), 'Source count of administration events in the event range.'), _ma_field('SOURCE_APPLICATION_FLAG', T.LongType(), 'Source application used to chart the event.'), _ma_field('MAE_VERIFIED_PRSNL_ID', T.LongType(), 'Personnel identifier verifying the administration.'), _ma_field('NEEDS_VERIFY_FLAG', T.LongType(), 'Source verification-state flag.'), _ma_field('VERIFICATION_DT_TM', T.TimestampType(), 'Administration verification timestamp.'), _ma_field('SCHEDULED_DT_TM', T.TimestampType(), 'Scheduled administration timestamp.'), _ma_field('CAREAWARE_USED_IND', T.LongType(), 'CareAware-used indicator.'), _ma_field('CRITICAL_DRUG_IND', T.LongType(), 'Critical-drug indicator.'), _ma_field('MR_VALID_FROM_DT_TM', T.TimestampType(), 'CE_MED_RESULT validity start.'), _ma_field('MR_VALID_UNTIL_DT_TM', T.TimestampType(), 'CE_MED_RESULT validity end.'), _ma_field('MR_IS_CURRENT_IND', T.BooleanType(), 'True when selected CE_MED_RESULT row is currently valid.'), _ma_field('ADMIN_NOTE', T.StringType(), 'Source administration note.'), _ma_field('ADMIN_PROV_ID', T.LongType(), 'Source administering provider identifier.'), _ma_field('ADMIN_SITE_CD', T.LongType(), 'Source administration body-site code.'), _ma_field('ADMIN_SITE_DISPLAY', T.StringType(), 'Display for administration site.'), _ma_field('ADMIN_METHOD_CD', T.LongType(), 'Source administration method code.'), _ma_field('ADMIN_METHOD_DISPLAY', T.StringType(), 'Display for administration method.'), _ma_field('ADMIN_PT_LOC_CD', T.LongType(), 'Source patient-location code at administration.'), _ma_field('TOTAL_INTAKE_VOLUME', T.DoubleType(), 'Unchanged source total-intake volume.'), _ma_field('DILUENT_TYPE_CD', T.LongType(), 'Source diluent type code.'), _ma_field('DILUENT_TYPE_DISPLAY', T.StringType(), 'Display for diluent type.'), _ma_field('PH_DISPENSE_ID', T.LongType(), 'Source pharmacy dispense identifier.'), _ma_field('INFUSION_TIME_CD', T.LongType(), 'Source infusion-time unit/code.'), _ma_field('MEDICATION_FORM_CD', T.LongType(), 'Source medication form code.'), _ma_field('MEDICATION_FORM_DISPLAY', T.StringType(), 'Display for medication form.'), _ma_field('ADMIN_STRENGTH', T.DoubleType(), 'Unchanged source administered strength.'), _ma_field('ADMIN_STRENGTH_UNIT_CD', T.LongType(), 'Source administered-strength unit code.'), _ma_field('ADMIN_STRENGTH_UNIT_DISPLAY', T.StringType(), 'Display for administered-strength unit.'), _ma_field('SUBSTANCE_LOT_NUMBER', T.StringType(), 'Unchanged source substance lot/sequence text.'), _ma_field('SUBSTANCE_EXP_DT_TM', T.TimestampType(), 'Source substance expiration timestamp.'), _ma_field('SUBSTANCE_EXP_DT_TXT', T.StringType(), 'Source substance expiration text.'), _ma_field('SUBSTANCE_MANUFACTURER_CD', T.LongType(), 'Source substance manufacturer code.'), _ma_field('SUBSTANCE_MANUFACTURER_DISPLAY', T.StringType(), 'Display for substance manufacturer.'), _ma_field('REFUSAL_CD', T.LongType(), 'Source refusal reason code.'), _ma_field('REFUSAL_DISPLAY', T.StringType(), 'Display for refusal reason.'), _ma_field('SYSTEM_ENTRY_DT_TM', T.TimestampType(), 'Source system-entry timestamp.'), _ma_field('IV_EVENT_CD', T.LongType(), 'Source IV event code.'), _ma_field('IV_EVENT_DISPLAY', T.StringType(), 'Display for IV event code.'), _ma_field('REMAINING_VOLUME', T.DoubleType(), 'Source remaining volume.'), _ma_field('REMAINING_VOLUME_UNIT_CD', T.LongType(), 'Source remaining-volume unit code.'), _ma_field('REMAINING_VOLUME_UNIT_DISPLAY', T.StringType(), 'Display for remaining-volume unit.'), _ma_field('WEIGHT_VALUE', T.DoubleType(), 'Source dosing weight value.'), _ma_field('WEIGHT_UNIT_CD', T.LongType(), 'Source dosing weight unit code.'), _ma_field('WEIGHT_UNIT_DISPLAY', T.StringType(), 'Display for dosing weight unit.'), _ma_field('BOLUS_TYPE_CD', T.LongType(), 'Source bolus type code.'), _ma_field('BOLUS_TYPE_DISPLAY', T.StringType(), 'Display for bolus type.'), _ma_field('ORDER_STATUS_CD', T.LongType(), 'Source order status code.'), _ma_field('ORDER_STATUS_DISPLAY', T.StringType(), 'Display for order status.'), _ma_field('ORIG_ORDER_DT_TM', T.TimestampType(), 'Source original order timestamp.'), _ma_field('CURRENT_START_DT_TM', T.TimestampType(), 'Source current order start timestamp.'), _ma_field('PROJECTED_STOP_DT_TM', T.TimestampType(), 'Source projected order stop timestamp.'), _ma_field('ORDER_STATUS_DT_TM', T.TimestampType(), 'Source order status timestamp.'), _ma_field('DISCONTINUE_IND', T.LongType(), 'Source order discontinue indicator.'), _ma_field('DISCONTINUE_EFFECTIVE_DT_TM', T.TimestampType(), 'Source order discontinue timestamp.'), _ma_field('SUSPEND_IND', T.LongType(), 'Source order suspend indicator.'), _ma_field('SUSPEND_EFFECTIVE_DT_TM', T.TimestampType(), 'Source order suspend timestamp.'), _ma_field('RESUME_IND', T.LongType(), 'Source order resume indicator.'), _ma_field('RESUME_EFFECTIVE_DT_TM', T.TimestampType(), 'Source order resume timestamp.'), _ma_field('PRN_IND', T.LongType(), 'Source PRN indicator.'), _ma_field('IV_IND', T.LongType(), 'Source IV indicator.'), _ma_field('FREQUENCY_ID', T.LongType(), 'Source order frequency identifier.'), _ma_field('ORDER_DETAIL_DISPLAY_LINE', T.StringType(), 'Source order detail display line.'), _ma_field('CLINICAL_DISPLAY_LINE', T.StringType(), 'Source clinical display line.'), _ma_field('HNA_ORDER_MNEMONIC', T.StringType(), 'Source HNA order mnemonic.'), _ma_field('ORDERED_AS_MNEMONIC', T.StringType(), 'Source ordered-as mnemonic.'), _ma_field('ORDERS_CKI', T.StringType(), 'CKI stored on the source order.'), _ma_field('SYNONYM_CKI', T.StringType(), 'CKI stored on the order catalogue synonym.'), _ma_field('CONCEPT_CKI', T.StringType(), 'Concept CKI stored on the order catalogue synonym.'), _ma_field('PRODUCT_ID', T.LongType(), 'Source product identifier.'), _ma_field('MED_ORDER_TYPE_CD', T.LongType(), 'Source medication order type code.'), _ma_field('MED_ORDER_TYPE_DISPLAY', T.StringType(), 'Display for medication order type.'), _ma_field('INGREDIENT_MATCH_PATH', T.StringType(), 'TEMPLATE_SYNONYM or ACTUAL_SYNONYM for representative ingredient.'), _ma_field('INGREDIENT_ORDER_ID', T.LongType(), 'ORDER_INGREDIENT order identifier for representative row.'), _ma_field('INGREDIENT_ACTION_SEQUENCE', T.LongType(), 'Latest representative ingredient action sequence.'), _ma_field('INGREDIENT_COMP_SEQUENCE', T.LongType(), 'Representative ingredient component sequence.'), _ma_field('INGREDIENT_SYNONYM_ID', T.LongType(), 'Representative ingredient synonym identifier.'), _ma_field('INGREDIENT_TYPE_FLAG', T.LongType(), 'Representative ingredient type flag.'), _ma_field('CLINICALLY_SIGNIFICANT_FLAG', T.LongType(), 'Representative clinically-significant flag.'), _ma_field('INCLUDE_IN_TOTAL_VOLUME_FLAG', T.LongType(), 'Representative include-in-total-volume flag.'), _ma_field('ORDERED_DOSE', T.DoubleType(), 'Representative ordered dose.'), _ma_field('ORDERED_DOSE_UNIT_CD', T.LongType(), 'Representative ordered-dose unit code.'), _ma_field('ORDERED_DOSE_UNIT_DISPLAY', T.StringType(), 'Display for representative ordered-dose unit.'), _ma_field('DOSE_QUANTITY', T.DoubleType(), 'Representative ingredient dose quantity.'), _ma_field('DOSE_QUANTITY_UNIT_CD', T.LongType(), 'Representative dose-quantity unit code.'), _ma_field('DOSE_QUANTITY_UNIT_DISPLAY', T.StringType(), 'Display for dose-quantity unit.'), _ma_field('FREETEXT_DOSE', T.StringType(), 'Representative source free-text dose.'), _ma_field('NORMALIZED_RATE', T.DoubleType(), 'Representative normalized ingredient rate.'), _ma_field('NORMALIZED_RATE_UNIT_CD', T.LongType(), 'Representative normalized-rate unit code.'), _ma_field('NORMALIZED_RATE_UNIT_DISPLAY', T.StringType(), 'Display for normalized-rate unit.'), _ma_field('CONCENTRATION', T.DoubleType(), 'Representative ingredient concentration.'), _ma_field('CONCENTRATION_UNIT_CD', T.LongType(), 'Representative concentration unit code.'), _ma_field('CONCENTRATION_UNIT_DISPLAY', T.StringType(), 'Display for concentration unit.'), _ma_field('INGREDIENT_ROW_COUNT', T.LongType(), 'Number of matching ingredient rows for EVENT_ID.'), _ma_field('INGREDIENT_ACTION_COUNT', T.LongType(), 'Distinct ingredient action sequences for EVENT_ID.'), _ma_field('INGREDIENT_COMPONENT_COUNT', T.LongType(), 'Distinct ingredient components for EVENT_ID.'), _ma_field('INGREDIENT_MULTI_ACTION_IND', T.BooleanType(), 'True when multiple ingredient actions exist.'), _ma_field('INGREDIENT_MULTI_COMPONENT_IND', T.BooleanType(), 'True when multiple ingredient components exist.'), _ma_field('DOSE_VALUE_EFFECTIVE', T.DoubleType(), 'ADMIN_DOSAGE then INITIAL_DOSAGE; volume is never substituted.'), _ma_field('DOSE_VALUE_SOURCE', T.StringType(), 'ADMIN_DOSAGE, INITIAL_DOSAGE or null.'), _ma_field('DOSE_UNIT_NORMALIZED', T.StringType(), 'Normalized exact source dosage unit.'), _ma_field('DOSE_NUMERATOR_UNIT', T.StringType(), 'Normalized numerator for compound units.'), _ma_field('DOSE_DENOMINATOR_UNIT', T.StringType(), 'Normalized denominator for compound units.'), _ma_field('DOSE_IN_UNITS', T.DoubleType(), 'Absolute administered amount in base international units.'), _ma_field('DOSE_STANDARDIZATION_STATUS', T.StringType(), 'EXACT_CONVERSION, COMPOUND_UNIT_NOT_ABSOLUTE, UNSUPPORTED_UNIT, MISSING_VALUE or MISSING_UNIT.'), _ma_field('DOSE_COMPOUND_UNIT_IND', T.BooleanType(), 'True when source unit is a ratio or rate.'), _ma_field('DOSE_NEGATIVE_IND', T.BooleanType(), 'True when source effective dosage is negative.'), _ma_field('DOSE_NUMERIC_EXTREME_IND', T.BooleanType(), 'Broad non-filtering flag for extreme source/standardized dosage values.'), _ma_field('LOOKUP_OMOP_CONCEPT_ID', T.LongType(), 'OMOP concept supplied by medication lookup after its own threshold rule.'), _ma_field('LOOKUP_OMOP_CONCEPT_TERM', T.StringType(), 'Term supplied for LOOKUP_OMOP_CONCEPT_ID.'), _ma_field('LOOKUP_OMOP_METHOD', T.StringType(), 'STANDARD_MAP or VECTOR_SIMILARITY from medication lookup.'), _ma_field('LOOKUP_SIMILARITY_SCORE', T.DoubleType(), 'Medication lookup similarity score.'), _ma_field('LOOKUP_SIMILARITY_THRESHOLD', T.DoubleType(), 'Threshold applied to the lookup similarity candidate.'), _ma_field('LOOKUP_SIMILARITY_STATUS', T.StringType(), 'Similarity status supplied by medication lookup.'), _ma_field('LOOKUP_EMBEDDING_MODEL_VERSION', T.StringType(), 'Embedding model version supplied by medication lookup.'), _ma_field('LOOKUP_RAW_SIMILARITY_OMOP_CONCEPT_ID', T.LongType(), 'Raw similarity candidate identifier.'), _ma_field('LOOKUP_RAW_SIMILARITY_OMOP_CONCEPT_TERM', T.StringType(), 'Raw similarity candidate term.'), _ma_field('LOOKUP_STANDARDIZED_SIMILARITY_OMOP_CONCEPT_ID', T.LongType(), 'Standardized similarity candidate identifier.'), _ma_field('LOOKUP_STANDARDIZED_SIMILARITY_OMOP_CONCEPT_TERM', T.StringType(), 'Standardized similarity candidate term.'), _ma_field('LOOKUP_SNOMED_CODE', T.StringType(), 'Direct SNOMED code supplied by medication lookup.'), _ma_field('LOOKUP_SNOMED_FROM_OMOP', T.StringType(), 'SNOMED code supplied by lookup OMOP reverse mapping.'), _ma_field('LOOKUP_SOURCE_CHANGE_TS', T.TimestampType(), 'Medication lookup source-change timestamp.'), _ma_field('LOOKUP_SOURCE_ROW_HASH', T.StringType(), 'Medication lookup source row hash.'), _ma_field('OMOP_SOURCE_CONCEPT_ID', T.LongType(), 'Best exact source concept before Standard mapping.'), _ma_field('OMOP_SOURCE_CONCEPT_NAME', T.StringType(), 'Name of OMOP_SOURCE_CONCEPT_ID.'), _ma_field('OMOP_SOURCE_VOCABULARY_ID', T.StringType(), 'Vocabulary of OMOP_SOURCE_CONCEPT_ID.'), _ma_field('OMOP_SOURCE_CONCEPT_CODE', T.StringType(), 'Code of OMOP_SOURCE_CONCEPT_ID.'), _ma_field('OMOP_SOURCE_STANDARD_CONCEPT', T.StringType(), 'Source concept standard flag.'), _ma_field('OMOP_SOURCE_CONCEPT_VALID_IND', T.BooleanType(), 'True when source concept exists and is not invalid.'), _ma_field('OMOP_MAPPING_METHOD', T.StringType(), 'Evidence method used to select OMOP source/standard concepts.'), _ma_field('OMOP_MAPPING_CONFIDENCE', T.DoubleType(), 'Evidence confidence, not a calibrated clinical probability.'), _ma_field('OMOP_SOURCE_MATCH_CANDIDATE_COUNT', T.LongType(), 'Number of exact concepts for selected source evidence key.'), _ma_field('OMOP_STANDARD_CANDIDATE_COUNT', T.LongType(), 'Number of valid Standard Maps-to candidates.'), _ma_field('OMOP_STANDARD_CONCEPT_ID', T.LongType(), 'Validated Standard OMOP Drug/Ingredient concept identifier; standard-validated replacement for the legacy-semantics OMOP_CONCEPT_ID.'), _ma_field('OMOP_STANDARD_CONCEPT_NAME', T.StringType(), 'Name of the validated Standard OMOP concept in OMOP_STANDARD_CONCEPT_ID.'), _ma_field('OMOP_STANDARD_CONCEPT_FLAG', T.StringType(), 'S only when OMOP_STANDARD_CONCEPT_ID is a valid Standard Drug/Ingredient concept.'), _ma_field('OMOP_MAPPING_AMBIGUOUS_IND', T.BooleanType(), 'True when source or Standard mapping is ambiguous.'), _ma_field('SNOMED_VALIDATED_CODE', T.StringType(), 'SNOMED code retained with explicit validation and provenance; standard-validated replacement for the legacy-semantics SNOMED_CODE.'), _ma_field('SNOMED_VALIDATED_STR', T.StringType(), 'Deterministically selected description of SNOMED_VALIDATED_CODE.'), _ma_field('SNOMED_VALIDATED_SOURCE', T.StringType(), 'LOOKUP_DIRECT, LOOKUP_FROM_OMOP, OMOP_MAPPED_FROM or UNMAPPED provenance for SNOMED_VALIDATED_CODE.'), _ma_field('SNOMED_CONCEPT_ID', T.LongType(), 'OMOP concept identifier for SNOMED_VALIDATED_CODE when present.'), _ma_field('SNOMED_VALID_DRUG_DOMAIN_IND', T.BooleanType(), 'True when SNOMED_VALIDATED_CODE is a valid Drug/Ingredient-domain concept.'), _ma_field('SNOMED_CANDIDATE_COUNT', T.LongType(), 'Number of valid SNOMED reverse-map candidates for SNOMED_VALIDATED_CODE.'), _ma_field('SNOMED_MAPPING_AMBIGUOUS_IND', T.BooleanType(), 'True when multiple SNOMED candidates exist for SNOMED_VALIDATED_CODE.'), _ma_field('ORGANIZATION_ID', T.LongType(), 'Best available source organization identifier.'), _ma_field('TRUST', T.StringType(), 'Best available source Trust value; no filtering is applied.'), _ma_field('SOURCE_ID_CONFLICT_IND', T.BooleanType(), 'True when populated source person/encounter/order identifiers disagree.'), _ma_field('SOURCE_ORGANIZATION_CONFLICT_IND', T.BooleanType(), 'True when populated source organization identifiers disagree.'), _ma_field('SOURCE_TRUST_CONFLICT_IND', T.BooleanType(), 'True when populated source Trust values disagree.'), _ma_field('CE_ADC_UPDT', T.TimestampType(), 'Selected CLINICAL_EVENT ADC timestamp.'), _ma_field('MAE_ADC_UPDT', T.TimestampType(), 'Selected MED_ADMIN_EVENT ADC timestamp.'), _ma_field('MR_ADC_UPDT', T.TimestampType(), 'Selected CE_MED_RESULT ADC timestamp.'), _ma_field('ORDERS_ADC_UPDT', T.TimestampType(), 'Selected ORDERS ADC timestamp.'), _ma_field('OI_ADC_UPDT', T.TimestampType(), 'Selected ORDER_INGREDIENT ADC timestamp.'), _ma_field('SYNONYM_ADC_UPDT', T.TimestampType(), 'ORDER_CATALOG_SYNONYM ADC timestamp.'), _ma_field('LOOKUP_ADC_UPDT', T.TimestampType(), 'Medication mapping lookup ADC timestamp.'), _ma_field('CODE_LOOKUP_ADC_UPDT', T.TimestampType(), 'Greatest code-value ADC timestamp used by the row.'), _ma_field('TRIGGER_SOURCES', T.StringType(), 'Comma-separated sources causing the event rebuild.'), _ma_field('SOURCE_VERSIONS_JSON', T.StringType(), 'Captured Delta versions used to build the row.'), _ma_field('MAPPING_SCHEMA_VERSION', T.StringType(), 'Medication mapping implementation version.'), _ma_field('UNIT_RULES_VERSION', T.StringType(), 'Dose-unit parsing rule version.'), _ma_field('ROW_HASH', T.LongType(), 'Stable source-derived row hash used to avoid unchanged rewrites.'), _ma_field('PIPELINE_RUN_ID', T.StringType(), 'Pipeline run that last materially changed the row.'), _ma_field('PIPELINE_UPDT_DT_TM', T.TimestampType(), 'Timestamp at which the target row last materially changed.')])
schema_map_med_admin_ingredient = T.StructType([_ma_field('EVENT_ID', T.LongType(), 'Parent medication administration EVENT_ID.', nullable=False), _ma_field('ORDER_ID', T.LongType(), 'Event order identifier.'), _ma_field('TEMPLATE_ORDER_ID', T.LongType(), 'Event template order identifier.'), _ma_field('INGREDIENT_MATCH_PATH', T.StringType(), 'TEMPLATE or ACTUAL order linkage.', nullable=False), _ma_field('INGREDIENT_ORDER_ID', T.LongType(), 'Source ORDER_INGREDIENT order identifier.', nullable=False), _ma_field('ACTION_SEQUENCE', T.LongType(), 'Source ingredient action sequence.'), _ma_field('COMP_SEQUENCE', T.LongType(), 'Source ingredient component sequence.'), _ma_field('CATALOG_TYPE_CD', T.LongType(), 'Source ingredient catalog type code.'), _ma_field('CATALOG_CD', T.LongType(), 'Source ingredient catalog code.'), _ma_field('SYNONYM_ID', T.LongType(), 'Source ingredient synonym identifier.'), _ma_field('ORDER_MNEMONIC', T.StringType(), 'Source ingredient mnemonic.'), _ma_field('ORDER_DETAIL_DISPLAY_LINE', T.StringType(), 'Source ingredient detail display line.'), _ma_field('STRENGTH', T.DoubleType(), 'Source ingredient strength.'), _ma_field('STRENGTH_UNIT_CD', T.LongType(), 'Source strength unit code.'), _ma_field('VOLUME', T.DoubleType(), 'Source ingredient volume.'), _ma_field('VOLUME_UNIT_CD', T.LongType(), 'Source volume unit code.'), _ma_field('FREETEXT_DOSE', T.StringType(), 'Source free-text dose.'), _ma_field('FREQ_CD', T.LongType(), 'Source ingredient frequency code.'), _ma_field('IV_SEQ', T.LongType(), 'Source IV sequence.'), _ma_field('DOSE_QUANTITY', T.DoubleType(), 'Source dose quantity.'), _ma_field('DOSE_QUANTITY_UNIT_CD', T.LongType(), 'Source dose-quantity unit code.'), _ma_field('ORDERED_AS_MNEMONIC', T.StringType(), 'Source ordered-as mnemonic.'), _ma_field('SUPPLIED_AS_MNEMONIC', T.StringType(), 'Source supplied-as mnemonic.'), _ma_field('HNA_ORDER_MNEMONIC', T.StringType(), 'Source HNA ingredient mnemonic.'), _ma_field('INGREDIENT_TYPE_FLAG', T.LongType(), 'Source ingredient type flag.'), _ma_field('CLINICALLY_SIGNIFICANT_FLAG', T.LongType(), 'Source clinically significant flag.'), _ma_field('INCLUDE_IN_TOTAL_VOLUME_FLAG', T.LongType(), 'Source include-in-total-volume flag.'), _ma_field('ORDERED_DOSE', T.DoubleType(), 'Source ordered dose.'), _ma_field('ORDERED_DOSE_UNIT_CD', T.LongType(), 'Source ordered-dose unit code.'), _ma_field('NORMALIZED_RATE', T.DoubleType(), 'Source normalized rate.'), _ma_field('NORMALIZED_RATE_UNIT_CD', T.LongType(), 'Source normalized-rate unit code.'), _ma_field('CONCENTRATION', T.DoubleType(), 'Source concentration.'), _ma_field('CONCENTRATION_UNIT_CD', T.LongType(), 'Source concentration unit code.'), _ma_field('DOSING_CAPACITY', T.DoubleType(), 'Source dosing capacity.'), _ma_field('DAYS_OF_ADMINISTRATION_DISPLAY', T.StringType(), 'Source administration-days display.'), _ma_field('DOSE_ADJUSTMENT_DISPLAY', T.StringType(), 'Source dose-adjustment display.'), _ma_field('ORDERED_AS_SYNONYM_ID', T.LongType(), 'Source ordered-as synonym identifier.'), _ma_field('UPDT_CNT', T.LongType(), 'Source ingredient update counter.'), _ma_field('UPDT_DT_TM', T.TimestampType(), 'Source application update timestamp.'), _ma_field('UPDT_ID', T.LongType(), 'Source application updater identifier.'), _ma_field('UPDT_TASK', T.LongType(), 'Source application update task.'), _ma_field('UPDT_APPLCTX', T.LongType(), 'Source application update context.'), _ma_field('LAST_UTC_TS', T.TimestampType(), 'Source last-UTC timestamp.'), _ma_field('INST_ID', T.LongType(), 'Source instance identifier.'), _ma_field('TXN_ID_TEXT', T.StringType(), 'Source transaction identifier.'), _ma_field('ADC_UPDT', T.TimestampType(), 'Source ingredient ADC timestamp.'), _ma_field('TRUST', T.StringType(), 'Source ingredient Trust value.'), _ma_field('ENCNTR_ID', T.LongType(), 'Source ingredient encounter identifier.'), _ma_field('ORGANIZATION_ID', T.LongType(), 'Source ingredient organization identifier.'), _ma_field('SOURCE_VERSIONS_JSON', T.StringType(), 'Captured Delta versions used to build the row.'), _ma_field('ROW_HASH', T.LongType(), 'Stable source-derived row hash.'), _ma_field('PIPELINE_RUN_ID', T.StringType(), 'Pipeline run that last materially changed the row.'), _ma_field('PIPELINE_UPDT_DT_TM', T.TimestampType(), 'Pipeline material-change timestamp.')])

def _ma_sql_identifier(name: str) -> str:
    tick = chr(96)
    return '.'.join((tick + part.replace(tick, tick + tick) + tick for part in name.split('.')))

def _ma_sql_string(value: str) -> str:
    return value.replace('\\', '\\\\').replace("'", "''")

def _ma_table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _ma_changes_stage_name(target_table: str, run_id: str) -> str:
    """Return a run-scoped Delta stage beside the canonical target."""
    parts = target_table.split('.')
    token = ''.join((character for character in run_id if character.isalnum()))[:24]
    if not token:
        token = uuid.uuid4().hex[:24]
    stage_name = f'{parts[-1]}__changes_stage_{token}'
    return '.'.join(parts[:-1] + [stage_name])

def _ma_materialize_changes_stage(changes: DataFrame, stage_table: str) -> DataFrame:
    """Materialize a reused change set without Spark cache/persist."""
    changes.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(stage_table)
    return spark.table(stage_table)

def _ma_drop_stage(stage_table: Optional[str]) -> None:
    if not stage_table:
        return
    spark.sql(f'DROP TABLE IF EXISTS {_ma_sql_identifier(stage_table)}')

def _ma_latest_delta_version(table_name: str) -> int:
    row = spark.sql(f'DESCRIBE HISTORY {_ma_sql_identifier(table_name)} LIMIT 1').select('version').first()
    if row is None:
        raise RuntimeError(f'No Delta history returned for {table_name}')
    return int(row['version'])

def _ma_capture_source_versions(config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> Dict[str, int]:
    return {table_name: _ma_latest_delta_version(table_name) for table_name in config.tracked_tables}

def _ma_read_snapshot(table_name: str, version: int) -> DataFrame:
    return _m20_hardening_read_pinned_snapshot(table_name, int(version))

def _ma_read_cdf(table_name: str, starting_version: int, ending_version: int) -> Optional[DataFrame]:
    if starting_version > ending_version:
        return None
    try:
        changes = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', int(starting_version)).option('endingVersion', int(ending_version)).table(table_name)
        return _m20_hardening_validate_cdf(changes)
    except Exception as exc:
        raise RuntimeError(f'CDF could not be read for {table_name} from version {starting_version} through {ending_version}. Timestamp fallback is intentionally disabled because it can miss deletes, late rows and independent-source updates. Run process_med_admin_incremental(force_full_rebuild=True) after review.') from exc

def _ma_checked_long(column: F.Column, label: str, required: bool=False) -> F.Column:
    # Value path is decimal-only; see the diagnosis-lane copy of this helper. The double
    # is a reject-only guard for NaN, non-integers and absurd magnitudes and never
    # produces the returned value.
    as_double = column.cast('double')
    as_decimal = column.cast('decimal(38,0)')
    null_result = F.lit(None) if required else F.lit(None).cast('long')
    in_long_range = as_decimal.between(F.lit(-9223372036854775808).cast('decimal(38,0)'), F.lit(9223372036854775807).cast('decimal(38,0)'))
    return F.when(column.isNull(), null_result).when(F.isnan(as_double), F.lit(None)).when(as_double != F.floor(as_double), F.lit(None)).when(F.abs(as_double) > F.lit(9223372036854775807), F.lit(None)).when(~in_long_range, F.lit(None)).otherwise(as_decimal.cast('long'))

def _ma_nonblank(column: F.Column) -> F.Column:
    value = F.trim(column.cast('string'))
    return F.when(value == '', F.lit(None).cast('string')).otherwise(value)

def _ma_normalize_term(column: F.Column) -> F.Column:
    value = F.lower(F.coalesce(column.cast('string'), F.lit('')))
    value = F.regexp_replace(value, '[\\u0000-\\u001f]+', ' ')
    value = F.regexp_replace(value, '[^\\p{L}\\p{N}]+', ' ')
    return F.trim(F.regexp_replace(value, '\\s+', ' '))

def _ma_distinct_nonnull_count(columns: Sequence[F.Column]) -> F.Column:
    values = F.array(*[column.cast('string') for column in columns])
    return F.size(F.array_distinct(F.filter(values, lambda value: value.isNotNull())))

def _ma_has_conflict(columns: Sequence[F.Column]) -> F.Column:
    return _ma_distinct_nonnull_count(columns) > F.lit(1)

def _ma_schema_select(df: DataFrame, schema: T.StructType) -> DataFrame:
    expressions: List[F.Column] = []
    available = set(df.columns)
    for field in schema.fields:
        if field.name in available:
            expressions.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            expressions.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(*expressions)

def _ma_empty_change_set() -> DataFrame:
    schema = T.StructType(list(schema_map_med_admin.fields) + [T.StructField('_source_change_type', T.StringType(), False)])
    return spark.createDataFrame([], schema)

def _ma_hash_columns(df: DataFrame, schema: T.StructType, excluded: Sequence[str]) -> DataFrame:
    excluded_set = set(excluded)
    columns = [field.name for field in schema.fields if field.name not in excluded_set and field.name in df.columns]
    values = [F.coalesce(F.col(column).cast('string'), F.lit('<NULL>')) for column in columns]
    return df.withColumn('ROW_HASH', F.xxhash64(*values))

def _ma_release_caches() -> None:
    while _PENDING_MED_ADMIN_CACHES:
        stage_table = _PENDING_MED_ADMIN_CACHES.pop()
        try:
            _ma_drop_stage(stage_table)
        except Exception as drop_exc:
            print(f'[WARN] map_med_admin persist stage drop failed for {stage_table}; the next pipeline startup sweep will retry: {str(drop_exc)[:500]}')

def _ma_persist(frame: DataFrame) -> DataFrame:
    """Materialize a frame that is read more than once.

    Serverless has no cache/persist, so an unmaterialized frame is recomputed in full at
    every reference. The medication-administration build reads its joined base seven times
    and the ingredient set three, each expansion rescanning clinical_event and the order
    tables, which is the difference between a minutes-long component and a multi-hour one.
    Round-tripping through a run-scoped Delta stage buys the same reuse a cache would.
    """
    columns = frame.columns
    if len(set(columns)) != len(columns):
        print('[WARN] map_med_admin _ma_persist skipped: frame has duplicate column names and cannot be staged')
        return frame
    stage_table = _ma_changes_stage_name(MAP_MED_ADMIN_CONFIG.target_table, 'persist' + uuid.uuid4().hex)
    staged = _ma_materialize_changes_stage(frame, stage_table)
    _PENDING_MED_ADMIN_CACHES.append(stage_table)
    return staged

def _ma_is_empty(frame: Optional[DataFrame]) -> bool:
    return frame is None or frame.limit(1).count() == 0

def _ma_union_frames(frames: Iterable[Optional[DataFrame]], allow_missing_columns: bool=True) -> Optional[DataFrame]:
    present = [frame for frame in frames if frame is not None]
    if not present:
        return None
    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=allow_missing_columns), present)

def _ma_ensure_control_tables(config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> None:
    spark.sql(f"\n        CREATE TABLE IF NOT EXISTS {_ma_sql_identifier(config.state_table)} (\n            source_table STRING NOT NULL,\n            last_processed_version BIGINT NOT NULL,\n            last_processed_at TIMESTAMP NOT NULL,\n            last_run_id STRING NOT NULL,\n            schema_version STRING NOT NULL\n        )\n        USING DELTA\n        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')\n    ")
    spark.sql(f'\n        CREATE TABLE IF NOT EXISTS {_ma_sql_identifier(config.audit_table)} (\n            run_id STRING NOT NULL,\n            event_timestamp TIMESTAMP NOT NULL,\n            status STRING NOT NULL,\n            mode STRING NOT NULL,\n            message STRING,\n            source_versions_json STRING,\n            metrics_json STRING,\n            schema_version STRING NOT NULL\n        )\n        USING DELTA\n    ')

def _ma_read_pipeline_state(config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> Dict[str, int]:
    if not _ma_table_exists(config.state_table):
        return {}
    rows = spark.table(config.state_table).where(F.col('source_table').isin(*config.tracked_tables)).select('source_table', 'last_processed_version', 'schema_version').collect()
    if any((row['schema_version'] != config.schema_version for row in rows)):
        return {}
    return {row['source_table']: int(row['last_processed_version']) for row in rows if row['last_processed_version'] is not None}

def _ma_commit_pipeline_state(source_versions: Dict[str, int], run_id: str, completed_at: datetime, config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> None:
    rows = [(source, int(version), completed_at, run_id, config.schema_version) for source, version in source_versions.items()]
    schema = T.StructType([T.StructField('source_table', T.StringType(), False), T.StructField('last_processed_version', T.LongType(), False), T.StructField('last_processed_at', T.TimestampType(), False), T.StructField('last_run_id', T.StringType(), False), T.StructField('schema_version', T.StringType(), False)])
    updates = spark.createDataFrame(rows, schema)
    DeltaTable.forName(spark, config.state_table).alias('t').merge(updates.alias('s'), 't.source_table = s.source_table').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def _ma_write_audit_event(run_id: str, status: str, mode: str, message: str, source_versions: Optional[Dict[str, int]]=None, metrics: Optional[Dict[str, object]]=None, config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> None:
    row = [(run_id, datetime.now(timezone.utc), status, mode, message, json.dumps(source_versions or {}, sort_keys=True), json.dumps(metrics or {}, sort_keys=True, default=str), config.schema_version)]
    schema = T.StructType([T.StructField('run_id', T.StringType(), False), T.StructField('event_timestamp', T.TimestampType(), False), T.StructField('status', T.StringType(), False), T.StructField('mode', T.StringType(), False), T.StructField('message', T.StringType(), True), T.StructField('source_versions_json', T.StringType(), True), T.StructField('metrics_json', T.StringType(), True), T.StructField('schema_version', T.StringType(), False)])
    spark.createDataFrame(row, schema).write.mode('append').saveAsTable(config.audit_table)

def _ma_target_requires_full_rebuild(state: Dict[str, int], config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> Tuple[bool, str]:
    if not _ma_table_exists(config.target_table):
        return (True, 'target table does not exist')
    if any((table not in state for table in config.tracked_tables)):
        return (True, 'complete exact-version pipeline state is unavailable')
    current_schema = {field.name: field.dataType.simpleString() for field in spark.table(config.target_table).schema.fields}
    required_types = {
        field.name: field.dataType.simpleString()
        for field in bronze_contract_schema(config.target_table).fields
    }
    mismatches = {name: (current_schema.get(name), expected) for name, expected in required_types.items() if current_schema.get(name) != expected}
    if mismatches:
        first = next(iter(mismatches.items()))
        return (True, f'target schema differs at {first[0]}: {first[1]}')
    if config.maintain_ingredient_child and (not _ma_table_exists(config.ingredient_target_table)):
        return (True, 'ingredient child table does not exist')
    return (False, 'target schema and exact-version state are current')

def _ma_set_target_metadata(full_rebuild: bool, config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> None:
    if not full_rebuild:
        return
    spark.sql(f"COMMENT ON TABLE {_ma_sql_identifier(config.target_table)} IS '{_ma_sql_string(map_med_admin_comment)}'")
    spark.sql(f"\n        ALTER TABLE {_ma_sql_identifier(config.target_table)} SET TBLPROPERTIES (\n            'map_med_admin.schemaVersion' = '{_ma_sql_string(config.schema_version)}',\n            'map_med_admin.mappingVersion' = '{_ma_sql_string(config.mapping_version)}',\n            'map_med_admin.unitRulesVersion' = '{_ma_sql_string(config.unit_rules_version)}'\n        )\n    ")
    if config.maintain_ingredient_child:
        spark.sql(f"COMMENT ON TABLE {_ma_sql_identifier(config.ingredient_target_table)} IS '{_ma_sql_string(map_med_admin_ingredient_comment)}'")
_MA_MASS_FACTORS_MG: Dict[str, float] = {'nanogram': 1e-06, 'ng': 1e-06, 'microgram': 0.001, 'mcg': 0.001, 'ug': 0.001, 'milligram': 1.0, 'mg': 1.0, 'gram': 1000.0, 'g': 1000.0}
_MA_VOLUME_FACTORS_ML: Dict[str, float] = {'microlitre': 0.001, 'microliter': 0.001, 'ul': 0.001, 'millilitre': 1.0, 'milliliter': 1.0, 'ml': 1.0, 'cc': 1.0, 'litre': 1000.0, 'liter': 1000.0, 'l': 1000.0}
_MA_UNIT_FACTORS: Dict[str, float] = {'milliunit': 0.001, 'unit': 1.0, 'international unit': 1.0, 'iu': 1.0, 'million unit': 1000000.0, 'megaunit': 1000000.0}
_MA_DISCRETE_UNITS: Tuple[str, ...] = ('application', 'spray', 'patch', 'scoop', 'inch', 'dose', 'puff', 'drop', 'tablet', 'capsule', 'sachet', 'nebule', 'device', 'ampoule', 'ampoule pair', 'enema', 'pot', 'inhalation', 'pessary', 'plaster')

def _ma_normalize_unit(column: F.Column) -> F.Column:
    value = F.lower(F.trim(F.coalesce(column.cast('string'), F.lit(''))))
    value = F.regexp_replace(value, '[\\r\\n\\t]+', ' ')
    value = F.regexp_replace(value, 'µ', 'u')
    value = F.regexp_replace(value, '_', ' ')
    value = F.regexp_replace(value, '\\s+', ' ')
    value = F.regexp_replace(value, '\\((s|es|ies)\\)$', '')
    value = F.regexp_replace(value, '(?<=\\p{L})s$', '')
    value = F.regexp_replace(value, '\\s*/\\s*', '/')
    value = F.regexp_replace(value, '\\s*-\\s*', ' ')
    return F.trim(value)

def _ma_map_factor(normalized_unit: F.Column, factors: Dict[str, float]) -> F.Column:
    expression = F.lit(None).cast('double')
    for unit, factor in factors.items():
        expression = F.when(normalized_unit == F.lit(unit), F.lit(float(factor))).otherwise(expression)
    return expression

def add_standardized_columns(df: DataFrame, standardization_cases: Optional[Dict[str, object]]=None, config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> DataFrame:
    """Compatibility replacement using exact units rather than substring matching."""
    normalized = _ma_normalize_unit(F.col('ADMIN_DOSAGE_UNIT_DISPLAY'))
    effective_value = F.coalesce(F.col('ADMIN_DOSAGE').cast('double'), F.col('INITIAL_DOSAGE').cast('double'))
    effective_source = F.when(F.col('ADMIN_DOSAGE').isNotNull(), F.lit('ADMIN_DOSAGE')).when(F.col('INITIAL_DOSAGE').isNotNull(), F.lit('INITIAL_DOSAGE')).otherwise(F.lit(None).cast('string'))
    compound = F.instr(normalized, '/') > F.lit(0)
    numerator = F.when(compound, F.split(normalized, '/').getItem(0))
    denominator = F.when(compound, F.concat_ws('/', F.slice(F.split(normalized, '/'), 2, 10)))
    mass_factor = _ma_map_factor(normalized, _MA_MASS_FACTORS_MG)
    volume_factor = _ma_map_factor(normalized, _MA_VOLUME_FACTORS_ML)
    unit_factor = _ma_map_factor(normalized, _MA_UNIT_FACTORS)
    discrete = normalized.isin(*_MA_DISCRETE_UNITS)
    category = F.when(normalized == '', F.lit('MISSING')).when(compound, F.lit('RATIO_OR_RATE')).when(mass_factor.isNotNull(), F.lit('ABSOLUTE_MASS')).when(volume_factor.isNotNull(), F.lit('ABSOLUTE_VOLUME')).when(unit_factor.isNotNull(), F.lit('UNITS')).when(discrete, F.lit('DISCRETE')).otherwise(F.lit('OTHER'))
    dose_mg = F.when(effective_value.isNotNull() & mass_factor.isNotNull() & ~compound, effective_value * mass_factor)
    dose_ml = F.when(effective_value.isNotNull() & volume_factor.isNotNull() & ~compound, effective_value * volume_factor)
    dose_units = F.when(effective_value.isNotNull() & unit_factor.isNotNull() & ~compound, effective_value * unit_factor)
    status = F.when(effective_value.isNull(), F.lit('MISSING_VALUE')).when(normalized == '', F.lit('MISSING_UNIT')).when(compound, F.lit('COMPOUND_UNIT_NOT_ABSOLUTE')).when(mass_factor.isNotNull() | volume_factor.isNotNull() | unit_factor.isNotNull(), F.lit('EXACT_CONVERSION')).otherwise(F.lit('UNSUPPORTED_UNIT'))
    extreme = (F.abs(effective_value) > F.lit(config.extreme_raw_dose)) | (F.abs(dose_mg) > F.lit(config.extreme_absolute_mass_mg)) | (F.abs(dose_ml) > F.lit(config.extreme_absolute_volume_ml))
    return df.withColumns({'DOSE_VALUE_EFFECTIVE': effective_value, 'DOSE_VALUE_SOURCE': effective_source, 'DOSE_UNIT_NORMALIZED': F.when(normalized == '', F.lit(None)).otherwise(normalized), 'DOSE_NUMERATOR_UNIT': numerator, 'DOSE_DENOMINATOR_UNIT': denominator, 'DOSE_IN_MG': dose_mg, 'DOSE_IN_ML': dose_ml, 'DOSE_IN_UNITS': dose_units, 'DOSE_UNIT_CATEGORY': category, 'DOSE_STANDARDIZATION_STATUS': status, 'DOSE_COMPOUND_UNIT_IND': compound, 'DOSE_NEGATIVE_IND': F.when(effective_value.isNull(), F.lit(False)).otherwise(effective_value < 0), 'DOSE_NUMERIC_EXTREME_IND': F.coalesce(extreme, F.lit(False)), 'UNIT_RULES_VERSION': F.lit(config.unit_rules_version)})

def get_unit_conversion_maps_med_admin() -> Tuple[Dict[str, float], Dict[str, float]]:
    """Compatibility API. Keys are exact normalized units, not substrings."""
    return (dict(_MA_MASS_FACTORS_MG), dict(_MA_VOLUME_FACTORS_ML))

def create_standardization_expressions_med_admin(weight_conversions: Optional[Dict[str, float]]=None, volume_conversions: Optional[Dict[str, float]]=None) -> Dict[str, object]:
    """Compatibility API retained for callers; add_standardized_columns owns parsing."""
    return {'weight_conversions': weight_conversions or dict(_MA_MASS_FACTORS_MG), 'volume_conversions': volume_conversions or dict(_MA_VOLUME_FACTORS_ML), 'matching': 'EXACT_NORMALIZED_ONLY'}

def get_simplified_drug_name_native(column: F.Column) -> F.Column:
    """Compatibility replacement: retain the complete normalized term; never take the first token."""
    return _ma_normalize_term(column)
extract_drug_name_med_admin = get_simplified_drug_name_native

def _ma_filter_event_keys(frame: DataFrame, event_column: str, event_keys: Optional[DataFrame]) -> DataFrame:
    if event_keys is None:
        return frame
    keys = event_keys.select(F.col('EVENT_ID').cast('long').alias('_FILTER_EVENT_ID')).dropDuplicates(['_FILTER_EVENT_ID'])
    return frame.join(keys, F.col(event_column).cast('long') == F.col('_FILTER_EVENT_ID'), 'inner').drop('_FILTER_EVENT_ID')

def _ma_prepare_clinical_event(raw: DataFrame, event_keys: Optional[DataFrame], run_timestamp: datetime) -> DataFrame:
    selected = _ma_filter_event_keys(raw, 'EVENT_ID', event_keys).select(_ma_checked_long(F.col('EVENT_ID'), 'CLINICAL_EVENT.EVENT_ID', True).alias('EVENT_ID'), _ma_checked_long(F.col('CLINICAL_EVENT_ID'), 'CLINICAL_EVENT.CLINICAL_EVENT_ID').alias('CLINICAL_EVENT_ID'), _ma_checked_long(F.col('PERSON_ID'), 'CLINICAL_EVENT.PERSON_ID').alias('CE_PERSON_ID'), _ma_checked_long(F.col('ENCNTR_ID'), 'CLINICAL_EVENT.ENCNTR_ID').alias('CE_ENCNTR_ID'), _ma_checked_long(F.col('ORDER_ID'), 'CLINICAL_EVENT.ORDER_ID').alias('CE_ORDER_ID'), _ma_checked_long(F.col('RESULT_STATUS_CD'), 'CLINICAL_EVENT.RESULT_STATUS_CD').alias('RESULT_STATUS_CD'), F.col('EVENT_START_DT_TM').cast('timestamp').alias('CE_EVENT_START_DT_TM'), F.col('EVENT_END_DT_TM').cast('timestamp').alias('CE_EVENT_END_DT_TM'), F.col('VALID_FROM_DT_TM').cast('timestamp').alias('CE_VALID_FROM_DT_TM'), F.col('VALID_UNTIL_DT_TM').cast('timestamp').alias('CE_VALID_UNTIL_DT_TM'), _ma_checked_long(F.col('EVENT_CLASS_CD'), 'CLINICAL_EVENT.EVENT_CLASS_CD').alias('EVENT_CLASS_CD'), _ma_checked_long(F.col('EVENT_CD'), 'CLINICAL_EVENT.EVENT_CD').alias('EVENT_CD'), F.col('EVENT_TAG').cast('string').alias('EVENT_TAG'), F.col('EVENT_TITLE_TEXT').cast('string').alias('EVENT_TITLE_TEXT'), _ma_checked_long(F.col('PARENT_EVENT_ID'), 'CLINICAL_EVENT.PARENT_EVENT_ID').alias('PARENT_EVENT_ID'), _ma_checked_long(F.col('EVENT_RELTN_CD'), 'CLINICAL_EVENT.EVENT_RELTN_CD').alias('EVENT_RELTN_CD'), _ma_checked_long(F.col('RECORD_STATUS_CD'), 'CLINICAL_EVENT.RECORD_STATUS_CD').alias('RECORD_STATUS_CD'), _ma_checked_long(F.col('AUTHENTIC_FLAG'), 'CLINICAL_EVENT.AUTHENTIC_FLAG').alias('AUTHENTIC_FLAG'), _ma_checked_long(F.col('PUBLISH_FLAG'), 'CLINICAL_EVENT.PUBLISH_FLAG').alias('PUBLISH_FLAG'), F.col('PERFORMED_DT_TM').cast('timestamp').alias('PERFORMED_DT_TM'), _ma_checked_long(F.col('PERFORMED_PRSNL_ID'), 'CLINICAL_EVENT.PERFORMED_PRSNL_ID').alias('PERFORMED_PRSNL_ID'), F.col('VERIFIED_DT_TM').cast('timestamp').alias('VERIFIED_DT_TM'), _ma_checked_long(F.col('VERIFIED_PRSNL_ID'), 'CLINICAL_EVENT.VERIFIED_PRSNL_ID').alias('CE_VERIFIED_PRSNL_ID'), _ma_checked_long(F.col('ENTRY_MODE_CD'), 'CLINICAL_EVENT.ENTRY_MODE_CD').alias('ENTRY_MODE_CD'), _ma_checked_long(F.col('SOURCE_CD'), 'CLINICAL_EVENT.SOURCE_CD').alias('CE_SOURCE_CD'), _ma_checked_long(F.col('CONTRIBUTOR_SYSTEM_CD'), 'CLINICAL_EVENT.CONTRIBUTOR_SYSTEM_CD').alias('CONTRIBUTOR_SYSTEM_CD'), F.col('DEVICE_FREE_TXT').cast('string').alias('DEVICE_FREE_TXT'), _ma_checked_long(F.col('ORDER_ACTION_SEQUENCE'), 'CLINICAL_EVENT.ORDER_ACTION_SEQUENCE').alias('ORDER_ACTION_SEQUENCE'), F.col('ADC_UPDT').cast('timestamp').alias('CE_ADC_UPDT'), _ma_checked_long(F.col('ORGANIZATION_ID'), 'CLINICAL_EVENT.ORGANIZATION_ID').alias('CE_ORGANIZATION_ID'), F.col('Trust').cast('string').alias('CE_TRUST'))
    selected = selected.withColumn('CE_IS_CURRENT_IND', F.coalesce(F.col('CE_VALID_UNTIL_DT_TM') > F.lit(run_timestamp), F.lit(False)))
    stats = selected.groupBy('EVENT_ID').agg(F.count(F.lit(1)).cast('long').alias('CE_VERSION_COUNT'), F.sum(F.col('CE_IS_CURRENT_IND').cast('long')).cast('long').alias('CE_CURRENT_VERSION_COUNT'))
    window = Window.partitionBy('EVENT_ID').orderBy(F.col('CE_IS_CURRENT_IND').desc(), F.col('CE_VALID_FROM_DT_TM').desc_nulls_last(), F.col('CE_ADC_UPDT').desc_nulls_last(), F.col('CLINICAL_EVENT_ID').desc_nulls_last())
    latest = selected.withColumn('_CE_RN', F.row_number().over(window)).where(F.col('_CE_RN') == 1).drop('_CE_RN')
    return latest.join(stats, 'EVENT_ID', 'left')

def _ma_prepare_med_admin_event(raw: DataFrame, event_keys: Optional[DataFrame]) -> DataFrame:
    selected = _ma_filter_event_keys(raw, 'EVENT_ID', event_keys).select(_ma_checked_long(F.col('EVENT_ID'), 'MED_ADMIN_EVENT.EVENT_ID', True).alias('EVENT_ID'), _ma_checked_long(F.col('MED_ADMIN_EVENT_ID'), 'MED_ADMIN_EVENT.MED_ADMIN_EVENT_ID').alias('MED_ADMIN_EVENT_ID'), _ma_checked_long(F.col('ORDER_ID'), 'MED_ADMIN_EVENT.ORDER_ID').alias('MAE_ORDER_ID'), _ma_checked_long(F.col('PRSNL_ID'), 'MED_ADMIN_EVENT.PRSNL_ID').alias('PRSNL_ID'), _ma_checked_long(F.col('DOCUMENTATION_ACTION_SEQUENCE'), 'MED_ADMIN_EVENT.DOCUMENTATION_ACTION_SEQUENCE').alias('DOCUMENTATION_ACTION_SEQUENCE'), _ma_checked_long(F.col('POSITIVE_PATIENT_IDENT_IND'), 'MED_ADMIN_EVENT.POSITIVE_PATIENT_IDENT_IND').alias('POSITIVE_PATIENT_IDENT_IND'), _ma_checked_long(F.col('POSITIVE_MED_IDENT_IND'), 'MED_ADMIN_EVENT.POSITIVE_MED_IDENT_IND').alias('POSITIVE_MED_IDENT_IND'), _ma_checked_long(F.col('ORDER_RESULT_VARIANCE_IND'), 'MED_ADMIN_EVENT.ORDER_RESULT_VARIANCE_IND').alias('ORDER_RESULT_VARIANCE_IND'), _ma_checked_long(F.col('CLINICAL_WARNING_CNT'), 'MED_ADMIN_EVENT.CLINICAL_WARNING_CNT').alias('CLINICAL_WARNING_CNT'), _ma_checked_long(F.col('POSITION_CD'), 'MED_ADMIN_EVENT.POSITION_CD').alias('POSITION_CD'), _ma_checked_long(F.col('NURSE_UNIT_CD'), 'MED_ADMIN_EVENT.NURSE_UNIT_CD').alias('NURSE_UNIT_CD'), _ma_checked_long(F.col('EVENT_CNT'), 'MED_ADMIN_EVENT.EVENT_CNT').alias('EVENT_CNT'), F.col('BEG_DT_TM').cast('timestamp').alias('MAE_BEG_DT_TM'), F.col('END_DT_TM').cast('timestamp').alias('MAE_END_DT_TM'), _ma_checked_long(F.col('SOURCE_APPLICATION_FLAG'), 'MED_ADMIN_EVENT.SOURCE_APPLICATION_FLAG').alias('SOURCE_APPLICATION_FLAG'), _ma_checked_long(F.col('EVENT_TYPE_CD'), 'MED_ADMIN_EVENT.EVENT_TYPE_CD').alias('EVENT_TYPE_CD'), _ma_checked_long(F.col('TEMPLATE_ORDER_ID'), 'MED_ADMIN_EVENT.TEMPLATE_ORDER_ID').alias('MAE_TEMPLATE_ORDER_ID'), _ma_checked_long(F.col('VERIFIED_PRSNL_ID'), 'MED_ADMIN_EVENT.VERIFIED_PRSNL_ID').alias('MAE_VERIFIED_PRSNL_ID'), _ma_checked_long(F.col('NEEDS_VERIFY_FLAG'), 'MED_ADMIN_EVENT.NEEDS_VERIFY_FLAG').alias('NEEDS_VERIFY_FLAG'), F.col('VERIFICATION_DT_TM').cast('timestamp').alias('VERIFICATION_DT_TM'), F.col('SCHEDULED_DT_TM').cast('timestamp').alias('SCHEDULED_DT_TM'), _ma_checked_long(F.col('CAREAWARE_USED_IND'), 'MED_ADMIN_EVENT.CAREAWARE_USED_IND').alias('CAREAWARE_USED_IND'), _ma_checked_long(F.col('CRITICAL_DRUG_IND'), 'MED_ADMIN_EVENT.CRITICAL_DRUG_IND').alias('CRITICAL_DRUG_IND'), F.col('UPDT_DT_TM').cast('timestamp').alias('MAE_UPDT_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('MAE_ADC_UPDT'), _ma_checked_long(F.col('ENCNTR_ID'), 'MED_ADMIN_EVENT.ENCNTR_ID').alias('MAE_ENCNTR_ID'), _ma_checked_long(F.col('ORGANIZATION_ID'), 'MED_ADMIN_EVENT.ORGANIZATION_ID').alias('MAE_ORGANIZATION_ID'), F.col('Trust').cast('string').alias('MAE_TRUST'))
    stats = selected.groupBy('EVENT_ID').agg(F.count(F.lit(1)).cast('long').alias('MAE_ROW_COUNT'))
    window = Window.partitionBy('EVENT_ID').orderBy(F.col('MAE_ADC_UPDT').desc_nulls_last(), F.col('MAE_UPDT_DT_TM').desc_nulls_last(), F.col('DOCUMENTATION_ACTION_SEQUENCE').desc_nulls_last(), F.col('MED_ADMIN_EVENT_ID').desc_nulls_last())
    latest = selected.withColumn('_MAE_RN', F.row_number().over(window)).where(F.col('_MAE_RN') == 1).drop('_MAE_RN')
    return latest.join(stats, 'EVENT_ID', 'left')

def _ma_prepare_med_result(raw: DataFrame, event_keys: Optional[DataFrame], run_timestamp: datetime) -> DataFrame:
    selected = _ma_filter_event_keys(raw, 'EVENT_ID', event_keys).select(_ma_checked_long(F.col('EVENT_ID'), 'CE_MED_RESULT.EVENT_ID', True).alias('EVENT_ID'), F.col('VALID_FROM_DT_TM').cast('timestamp').alias('MR_VALID_FROM_DT_TM'), F.col('VALID_UNTIL_DT_TM').cast('timestamp').alias('MR_VALID_UNTIL_DT_TM'), F.col('ADMIN_START_DT_TM').cast('timestamp').alias('MR_ADMIN_START_DT_TM'), F.col('ADMIN_END_DT_TM').cast('timestamp').alias('MR_ADMIN_END_DT_TM'), F.col('ADMIN_NOTE').cast('string').alias('ADMIN_NOTE'), _ma_checked_long(F.col('ADMIN_PROV_ID'), 'CE_MED_RESULT.ADMIN_PROV_ID').alias('ADMIN_PROV_ID'), _ma_checked_long(F.col('ADMIN_ROUTE_CD'), 'CE_MED_RESULT.ADMIN_ROUTE_CD').alias('ADMIN_ROUTE_CD'), _ma_checked_long(F.col('ADMIN_SITE_CD'), 'CE_MED_RESULT.ADMIN_SITE_CD').alias('ADMIN_SITE_CD'), _ma_checked_long(F.col('ADMIN_METHOD_CD'), 'CE_MED_RESULT.ADMIN_METHOD_CD').alias('ADMIN_METHOD_CD'), _ma_checked_long(F.col('ADMIN_PT_LOC_CD'), 'CE_MED_RESULT.ADMIN_PT_LOC_CD').alias('ADMIN_PT_LOC_CD'), F.col('INITIAL_DOSAGE').cast('double').alias('INITIAL_DOSAGE'), F.col('ADMIN_DOSAGE').cast('double').alias('ADMIN_DOSAGE'), _ma_checked_long(F.col('DOSAGE_UNIT_CD'), 'CE_MED_RESULT.DOSAGE_UNIT_CD').alias('ADMIN_DOSAGE_UNIT_CD'), F.col('INITIAL_VOLUME').cast('double').alias('INITIAL_VOLUME'), F.col('TOTAL_INTAKE_VOLUME').cast('double').alias('TOTAL_INTAKE_VOLUME'), _ma_checked_long(F.col('DILUENT_TYPE_CD'), 'CE_MED_RESULT.DILUENT_TYPE_CD').alias('DILUENT_TYPE_CD'), _ma_checked_long(F.col('PH_DISPENSE_ID'), 'CE_MED_RESULT.PH_DISPENSE_ID').alias('PH_DISPENSE_ID'), F.col('INFUSION_RATE').cast('double').alias('INFUSION_RATE'), _ma_checked_long(F.col('INFUSION_UNIT_CD'), 'CE_MED_RESULT.INFUSION_UNIT_CD').alias('INFUSION_UNIT_CD'), _ma_checked_long(F.col('INFUSION_TIME_CD'), 'CE_MED_RESULT.INFUSION_TIME_CD').alias('INFUSION_TIME_CD'), _ma_checked_long(F.col('MEDICATION_FORM_CD'), 'CE_MED_RESULT.MEDICATION_FORM_CD').alias('MEDICATION_FORM_CD'), F.col('ADMIN_STRENGTH').cast('double').alias('ADMIN_STRENGTH'), _ma_checked_long(F.col('ADMIN_STRENGTH_UNIT_CD'), 'CE_MED_RESULT.ADMIN_STRENGTH_UNIT_CD').alias('ADMIN_STRENGTH_UNIT_CD'), F.col('SUBSTANCE_LOT_NUMBER').cast('string').alias('SUBSTANCE_LOT_NUMBER'), F.col('SUBSTANCE_EXP_DT_TM').cast('timestamp').alias('SUBSTANCE_EXP_DT_TM'), F.col('SUBSTANCE_EXP_DT_TXT').cast('string').alias('SUBSTANCE_EXP_DT_TXT'), _ma_checked_long(F.col('SUBSTANCE_MANUFACTURER_CD'), 'CE_MED_RESULT.SUBSTANCE_MANUFACTURER_CD').alias('SUBSTANCE_MANUFACTURER_CD'), _ma_checked_long(F.col('REFUSAL_CD'), 'CE_MED_RESULT.REFUSAL_CD').alias('REFUSAL_CD'), F.col('SYSTEM_ENTRY_DT_TM').cast('timestamp').alias('SYSTEM_ENTRY_DT_TM'), _ma_checked_long(F.col('IV_EVENT_CD'), 'CE_MED_RESULT.IV_EVENT_CD').alias('IV_EVENT_CD'), F.col('INFUSED_VOLUME').cast('double').alias('INFUSED_VOLUME'), _ma_checked_long(F.col('INFUSED_VOLUME_UNIT_CD'), 'CE_MED_RESULT.INFUSED_VOLUME_UNIT_CD').alias('INFUSED_VOLUME_UNIT_CD'), F.col('REMAINING_VOLUME').cast('double').alias('REMAINING_VOLUME'), _ma_checked_long(F.col('REMAINING_VOLUME_UNIT_CD'), 'CE_MED_RESULT.REMAINING_VOLUME_UNIT_CD').alias('REMAINING_VOLUME_UNIT_CD'), _ma_checked_long(F.col('SYNONYM_ID'), 'CE_MED_RESULT.SYNONYM_ID').alias('MR_SYNONYM_ID'), F.col('WEIGHT_VALUE').cast('double').alias('WEIGHT_VALUE'), _ma_checked_long(F.col('WEIGHT_UNIT_CD'), 'CE_MED_RESULT.WEIGHT_UNIT_CD').alias('WEIGHT_UNIT_CD'), _ma_checked_long(F.col('BOLUS_TYPE_CD'), 'CE_MED_RESULT.BOLUS_TYPE_CD').alias('BOLUS_TYPE_CD'), F.col('UPDT_DT_TM').cast('timestamp').alias('MR_UPDT_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('MR_ADC_UPDT'), _ma_checked_long(F.col('ENCNTR_ID'), 'CE_MED_RESULT.ENCNTR_ID').alias('MR_ENCNTR_ID'), _ma_checked_long(F.col('ORGANIZATION_ID'), 'CE_MED_RESULT.ORGANIZATION_ID').alias('MR_ORGANIZATION_ID'), F.col('Trust').cast('string').alias('MR_TRUST'))
    selected = selected.withColumn('MR_IS_CURRENT_IND', F.coalesce(F.col('MR_VALID_UNTIL_DT_TM') > F.lit(run_timestamp), F.lit(False)))
    stats = selected.groupBy('EVENT_ID').agg(F.count(F.lit(1)).cast('long').alias('MR_ROW_COUNT'), F.sum(F.col('MR_IS_CURRENT_IND').cast('long')).cast('long').alias('MR_CURRENT_ROW_COUNT'))
    window = Window.partitionBy('EVENT_ID').orderBy(F.col('MR_IS_CURRENT_IND').desc(), F.col('MR_VALID_FROM_DT_TM').desc_nulls_last(), F.col('MR_ADC_UPDT').desc_nulls_last(), F.col('SYSTEM_ENTRY_DT_TM').desc_nulls_last(), F.col('MR_UPDT_DT_TM').desc_nulls_last())
    latest = selected.withColumn('_MR_RN', F.row_number().over(window)).where(F.col('_MR_RN') == 1).drop('_MR_RN')
    return latest.join(stats, 'EVENT_ID', 'left')

def _ma_prepare_orders(raw: DataFrame, order_keys: DataFrame) -> DataFrame:
    keys = order_keys.select(F.col('ORDER_ID').cast('long').alias('_FILTER_ORDER_ID')).where(F.col('_FILTER_ORDER_ID').isNotNull()).dropDuplicates(['_FILTER_ORDER_ID'])
    selected = raw.join(keys, F.col('ORDER_ID').cast('long') == F.col('_FILTER_ORDER_ID'), 'inner').drop('_FILTER_ORDER_ID').select(_ma_checked_long(F.col('ORDER_ID'), 'ORDERS.ORDER_ID', True).alias('ORDER_ID'), _ma_checked_long(F.col('PERSON_ID'), 'ORDERS.PERSON_ID').alias('ORD_PERSON_ID'), _ma_checked_long(F.col('ENCNTR_ID'), 'ORDERS.ENCNTR_ID').alias('ORD_ENCNTR_ID'), _ma_checked_long(F.col('CATALOG_CD'), 'ORDERS.CATALOG_CD').alias('ORDER_CATALOG_CD'), _ma_checked_long(F.col('ORDER_STATUS_CD'), 'ORDERS.ORDER_STATUS_CD').alias('ORDER_STATUS_CD'), F.col('ORDER_MNEMONIC').cast('string').alias('ORD_ORDER_MNEMONIC'), F.col('ORDER_DETAIL_DISPLAY_LINE').cast('string').alias('ORDER_DETAIL_DISPLAY_LINE'), _ma_checked_long(F.col('TEMPLATE_ORDER_ID'), 'ORDERS.TEMPLATE_ORDER_ID').alias('ORD_TEMPLATE_ORDER_ID'), _ma_checked_long(F.col('SYNONYM_ID'), 'ORDERS.SYNONYM_ID').alias('ORDER_SYNONYM_ID'), F.col('ORIG_ORDER_DT_TM').cast('timestamp').alias('ORIG_ORDER_DT_TM'), F.col('CURRENT_START_DT_TM').cast('timestamp').alias('CURRENT_START_DT_TM'), F.col('PROJECTED_STOP_DT_TM').cast('timestamp').alias('PROJECTED_STOP_DT_TM'), F.col('STATUS_DT_TM').cast('timestamp').alias('ORDER_STATUS_DT_TM'), _ma_checked_long(F.col('DISCONTINUE_IND'), 'ORDERS.DISCONTINUE_IND').alias('DISCONTINUE_IND'), F.col('DISCONTINUE_EFFECTIVE_DT_TM').cast('timestamp').alias('DISCONTINUE_EFFECTIVE_DT_TM'), _ma_checked_long(F.col('SUSPEND_IND'), 'ORDERS.SUSPEND_IND').alias('SUSPEND_IND'), F.col('SUSPEND_EFFECTIVE_DT_TM').cast('timestamp').alias('SUSPEND_EFFECTIVE_DT_TM'), _ma_checked_long(F.col('RESUME_IND'), 'ORDERS.RESUME_IND').alias('RESUME_IND'), F.col('RESUME_EFFECTIVE_DT_TM').cast('timestamp').alias('RESUME_EFFECTIVE_DT_TM'), _ma_checked_long(F.col('PRN_IND'), 'ORDERS.PRN_IND').alias('PRN_IND'), _ma_checked_long(F.col('IV_IND'), 'ORDERS.IV_IND').alias('IV_IND'), _ma_checked_long(F.col('FREQUENCY_ID'), 'ORDERS.FREQUENCY_ID').alias('FREQUENCY_ID'), F.col('CLINICAL_DISPLAY_LINE').cast('string').alias('CLINICAL_DISPLAY_LINE'), F.col('HNA_ORDER_MNEMONIC').cast('string').alias('HNA_ORDER_MNEMONIC'), F.col('ORDERED_AS_MNEMONIC').cast('string').alias('ORDERED_AS_MNEMONIC'), F.col('CKI').cast('string').alias('ORDERS_CKI'), _ma_checked_long(F.col('PRODUCT_ID'), 'ORDERS.PRODUCT_ID').alias('PRODUCT_ID'), _ma_checked_long(F.col('MED_ORDER_TYPE_CD'), 'ORDERS.MED_ORDER_TYPE_CD').alias('MED_ORDER_TYPE_CD'), F.col('UPDT_DT_TM').cast('timestamp').alias('ORD_UPDT_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('ORDERS_ADC_UPDT'), _ma_checked_long(F.col('ORGANIZATION_ID'), 'ORDERS.ORGANIZATION_ID').alias('ORD_ORGANIZATION_ID'), F.col('Trust').cast('string').alias('ORD_TRUST'))
    window = Window.partitionBy('ORDER_ID').orderBy(F.col('ORDERS_ADC_UPDT').desc_nulls_last(), F.col('ORD_UPDT_DT_TM').desc_nulls_last(), F.col('ORDER_STATUS_DT_TM').desc_nulls_last())
    return selected.withColumn('_ORD_RN', F.row_number().over(window)).where(F.col('_ORD_RN') == 1).drop('_ORD_RN')

def _ma_prepare_order_synonym(raw: DataFrame) -> DataFrame:
    selected = raw.select(_ma_checked_long(F.col('SYNONYM_ID'), 'ORDER_CATALOG_SYNONYM.SYNONYM_ID', True).alias('ORDER_SYNONYM_ID'), F.col('MNEMONIC').cast('string').alias('SYNONYM_MNEMONIC'), F.col('CKI').cast('string').alias('SYNONYM_CKI'), F.col('CONCEPT_CKI').cast('string').alias('CONCEPT_CKI'), F.col('ADC_UPDT').cast('timestamp').alias('SYNONYM_ADC_UPDT'), F.col('UPDT_DT_TM').cast('timestamp').alias('SYNONYM_UPDT_DT_TM'))
    window = Window.partitionBy('ORDER_SYNONYM_ID').orderBy(F.col('SYNONYM_ADC_UPDT').desc_nulls_last(), F.col('SYNONYM_UPDT_DT_TM').desc_nulls_last())
    return selected.withColumn('_OS_RN', F.row_number().over(window)).where(F.col('_OS_RN') == 1).drop('_OS_RN')

def _ma_prepare_medication_lookup(raw: DataFrame) -> DataFrame:
    selected = raw.select(_ma_checked_long(F.col('SYNONYM_ID'), 'MAP_MED_LOOKUP.SYNONYM_ID', True).alias('ORDER_SYNONYM_ID'), F.col('HNA_ORDER_MNEMONIC').cast('string').alias('LOOKUP_HNA_ORDER_MNEMONIC'), F.col('MULTUM_CODE').cast('string').alias('MULTUM'), F.col('RXNORM_CODE').cast('string').alias('RXNORM_CUI'), F.col('SNOMED_CODE').cast('string').alias('LOOKUP_SNOMED_CODE'), F.col('SNOMED_FROM_OMOP').cast('string').alias('LOOKUP_SNOMED_FROM_OMOP'), _ma_checked_long(F.col('MAPPED_OMOP_CONCEPT_ID'), 'MAP_MED_LOOKUP.MAPPED_OMOP_CONCEPT_ID').alias('LKP_MAPPED_OMOP_CONCEPT_ID'), F.col('MAPPED_OMOP_CONCEPT_TERM').cast('string').alias('LKP_MAPPED_OMOP_CONCEPT_TERM'), _ma_checked_long(F.col('SIMILARITY_OMOP_CONCEPT_ID'), 'MAP_MED_LOOKUP.SIMILARITY_OMOP_CONCEPT_ID').alias('LKP_SIMILARITY_OMOP_CONCEPT_ID'), F.col('SIMILARITY_OMOP_CONCEPT_TERM').cast('string').alias('LKP_SIMILARITY_OMOP_CONCEPT_TERM'), F.col('SIMILARITY_SCORE').cast('double').alias('LOOKUP_SIMILARITY_SCORE'), _ma_checked_long(F.col('RAW_SIMILARITY_OMOP_CONCEPT_ID'), 'MAP_MED_LOOKUP.RAW_SIMILARITY_OMOP_CONCEPT_ID').alias('LOOKUP_RAW_SIMILARITY_OMOP_CONCEPT_ID'), F.col('RAW_SIMILARITY_OMOP_CONCEPT_TERM').cast('string').alias('LOOKUP_RAW_SIMILARITY_OMOP_CONCEPT_TERM'), _ma_checked_long(F.col('STANDARDIZED_SIMILARITY_OMOP_CONCEPT_ID'), 'MAP_MED_LOOKUP.STANDARDIZED_SIMILARITY_OMOP_CONCEPT_ID').alias('LOOKUP_STANDARDIZED_SIMILARITY_OMOP_CONCEPT_ID'), F.col('STANDARDIZED_SIMILARITY_OMOP_CONCEPT_TERM').cast('string').alias('LOOKUP_STANDARDIZED_SIMILARITY_OMOP_CONCEPT_TERM'), F.col('SIMILARITY_STATUS').cast('string').alias('LOOKUP_SIMILARITY_STATUS'), F.col('SOURCE_CHANGE_TS').cast('timestamp').alias('LOOKUP_SOURCE_CHANGE_TS'), F.col('SOURCE_ROW_HASH').cast('string').alias('LOOKUP_SOURCE_ROW_HASH'), F.col('ADC_UPDT').cast('timestamp').alias('LOOKUP_ADC_UPDT'), F.col('EMBEDDING_MODEL_VERSION').cast('string').alias('LOOKUP_EMBEDDING_MODEL_VERSION'), F.col('SIMILARITY_THRESHOLD').cast('double').alias('LOOKUP_SIMILARITY_THRESHOLD_RAW'))
    window = Window.partitionBy('ORDER_SYNONYM_ID').orderBy(F.col('LOOKUP_ADC_UPDT').desc_nulls_last(), F.col('LOOKUP_SOURCE_CHANGE_TS').desc_nulls_last())
    return selected.withColumn('_LKP_RN', F.row_number().over(window)).where(F.col('_LKP_RN') == 1).drop('_LKP_RN')

def _ma_prepare_order_ingredient(raw: DataFrame) -> DataFrame:
    return raw.select(_ma_checked_long(F.col('ORDER_ID'), 'ORDER_INGREDIENT.ORDER_ID', True).alias('INGREDIENT_ORDER_ID'), _ma_checked_long(F.col('ACTION_SEQUENCE'), 'ORDER_INGREDIENT.ACTION_SEQUENCE').alias('ACTION_SEQUENCE'), _ma_checked_long(F.col('COMP_SEQUENCE'), 'ORDER_INGREDIENT.COMP_SEQUENCE').alias('COMP_SEQUENCE'), _ma_checked_long(F.col('CATALOG_TYPE_CD'), 'ORDER_INGREDIENT.CATALOG_TYPE_CD').alias('OI_CATALOG_TYPE_CD'), _ma_checked_long(F.col('CATALOG_CD'), 'ORDER_INGREDIENT.CATALOG_CD').alias('OI_CATALOG_CD'), _ma_checked_long(F.col('SYNONYM_ID'), 'ORDER_INGREDIENT.SYNONYM_ID').alias('INGREDIENT_SYNONYM_ID'), F.col('ORDER_MNEMONIC').cast('string').alias('OI_ORDER_MNEMONIC'), F.col('ORDER_DETAIL_DISPLAY_LINE').cast('string').alias('OI_ORDER_DETAIL_DISPLAY_LINE'), F.col('STRENGTH').cast('double').alias('OI_STRENGTH'), _ma_checked_long(F.col('STRENGTH_UNIT'), 'ORDER_INGREDIENT.STRENGTH_UNIT').alias('OI_STRENGTH_UNIT_CD'), F.col('VOLUME').cast('double').alias('OI_VOLUME'), _ma_checked_long(F.col('VOLUME_UNIT'), 'ORDER_INGREDIENT.VOLUME_UNIT').alias('OI_VOLUME_UNIT_CD'), F.col('FREETEXT_DOSE').cast('string').alias('FREETEXT_DOSE'), _ma_checked_long(F.col('FREQ_CD'), 'ORDER_INGREDIENT.FREQ_CD').alias('OI_FREQ_CD'), _ma_checked_long(F.col('IV_SEQ'), 'ORDER_INGREDIENT.IV_SEQ').alias('OI_IV_SEQ'), F.col('DOSE_QUANTITY').cast('double').alias('DOSE_QUANTITY'), _ma_checked_long(F.col('DOSE_QUANTITY_UNIT'), 'ORDER_INGREDIENT.DOSE_QUANTITY_UNIT').alias('DOSE_QUANTITY_UNIT_CD'), F.col('ORDERED_AS_MNEMONIC').cast('string').alias('OI_ORDERED_AS_MNEMONIC'), F.col('SUPPLIED_AS_MNEMONIC').cast('string').alias('OI_SUPPLIED_AS_MNEMONIC'), F.col('HNA_ORDER_MNEMONIC').cast('string').alias('OI_HNA_ORDER_MNEMONIC'), _ma_checked_long(F.col('INGREDIENT_TYPE_FLAG'), 'ORDER_INGREDIENT.INGREDIENT_TYPE_FLAG').alias('INGREDIENT_TYPE_FLAG'), _ma_checked_long(F.col('CLINICALLY_SIGNIFICANT_FLAG'), 'ORDER_INGREDIENT.CLINICALLY_SIGNIFICANT_FLAG').alias('CLINICALLY_SIGNIFICANT_FLAG'), _ma_checked_long(F.col('INCLUDE_IN_TOTAL_VOLUME_FLAG'), 'ORDER_INGREDIENT.INCLUDE_IN_TOTAL_VOLUME_FLAG').alias('INCLUDE_IN_TOTAL_VOLUME_FLAG'), F.col('ORDERED_DOSE').cast('double').alias('ORDERED_DOSE'), _ma_checked_long(F.col('ORDERED_DOSE_UNIT_CD'), 'ORDER_INGREDIENT.ORDERED_DOSE_UNIT_CD').alias('ORDERED_DOSE_UNIT_CD'), F.col('NORMALIZED_RATE').cast('double').alias('NORMALIZED_RATE'), _ma_checked_long(F.col('NORMALIZED_RATE_UNIT_CD'), 'ORDER_INGREDIENT.NORMALIZED_RATE_UNIT_CD').alias('NORMALIZED_RATE_UNIT_CD'), F.col('CONCENTRATION').cast('double').alias('CONCENTRATION'), _ma_checked_long(F.col('CONCENTRATION_UNIT_CD'), 'ORDER_INGREDIENT.CONCENTRATION_UNIT_CD').alias('CONCENTRATION_UNIT_CD'), F.col('DOSING_CAPACITY').cast('double').alias('DOSING_CAPACITY'), F.col('DAYS_OF_ADMINISTRATION_DISPLAY').cast('string').alias('DAYS_OF_ADMINISTRATION_DISPLAY'), F.col('DOSE_ADJUSTMENT_DISPLAY').cast('string').alias('DOSE_ADJUSTMENT_DISPLAY'), _ma_checked_long(F.col('ORDERED_AS_SYNONYM_ID'), 'ORDER_INGREDIENT.ORDERED_AS_SYNONYM_ID').alias('ORDERED_AS_SYNONYM_ID'), _ma_checked_long(F.col('UPDT_CNT'), 'ORDER_INGREDIENT.UPDT_CNT').alias('OI_UPDT_CNT'), F.col('UPDT_DT_TM').cast('timestamp').alias('OI_UPDT_DT_TM'), _ma_checked_long(F.col('UPDT_ID'), 'ORDER_INGREDIENT.UPDT_ID').alias('OI_UPDT_ID'), _ma_checked_long(F.col('UPDT_TASK'), 'ORDER_INGREDIENT.UPDT_TASK').alias('OI_UPDT_TASK'), _ma_checked_long(F.col('UPDT_APPLCTX'), 'ORDER_INGREDIENT.UPDT_APPLCTX').alias('OI_UPDT_APPLCTX'), F.col('LAST_UTC_TS').cast('timestamp').alias('OI_LAST_UTC_TS'), _ma_checked_long(F.col('INST_ID'), 'ORDER_INGREDIENT.INST_ID').alias('OI_INST_ID'), F.col('TXN_ID_TEXT').cast('string').alias('OI_TXN_ID_TEXT'), F.col('ADC_UPDT').cast('timestamp').alias('OI_ADC_UPDT'), F.col('Trust').cast('string').alias('OI_TRUST'), _ma_checked_long(F.col('ENCNTR_ID'), 'ORDER_INGREDIENT.ENCNTR_ID').alias('OI_ENCNTR_ID'), _ma_checked_long(F.col('ORGANIZATION_ID'), 'ORDER_INGREDIENT.ORGANIZATION_ID').alias('OI_ORGANIZATION_ID'))

def _ma_prepare_code_lookup(raw: DataFrame) -> DataFrame:
    selected = raw.select(_ma_checked_long(F.col('CODE_VALUE'), 'CODE_VALUE.CODE_VALUE', True).alias('CODE_VALUE'), F.coalesce(_ma_nonblank(F.col('DISPLAY')), _ma_nonblank(F.col('CDF_MEANING'))).alias('CODE_DESCRIPTION'), F.col('ADC_UPDT').cast('timestamp').alias('CODE_ADC_UPDT'))
    window = Window.partitionBy('CODE_VALUE').orderBy(F.col('CODE_ADC_UPDT').desc_nulls_last(), F.col('CODE_DESCRIPTION').asc_nulls_last())
    return selected.withColumn('_CV_RN', F.row_number().over(window)).where(F.col('_CV_RN') == 1).drop('_CV_RN')
_MA_CODE_DISPLAY_COLUMNS: Dict[str, str] = {'EVENT_TYPE_CD': 'EVENT_TYPE_DISPLAY', 'RESULT_STATUS_CD': 'RESULT_STATUS_DISPLAY', 'ADMIN_ROUTE_CD': 'ADMIN_ROUTE_DISPLAY', 'ADMIN_SITE_CD': 'ADMIN_SITE_DISPLAY', 'ADMIN_METHOD_CD': 'ADMIN_METHOD_DISPLAY', 'INITIAL_DOSAGE_UNIT_CD': 'INITIAL_DOSAGE_UNIT_DISPLAY', 'ADMIN_DOSAGE_UNIT_CD': 'ADMIN_DOSAGE_UNIT_DISPLAY', 'INFUSED_VOLUME_UNIT_CD': 'INFUSED_VOLUME_UNIT_DISPLAY', 'INFUSION_UNIT_CD': 'INFUSION_UNIT_DISPLAY', 'NURSE_UNIT_CD': 'NURSE_UNIT_DISPLAY', 'POSITION_CD': 'POSITION_DISPLAY', 'DILUENT_TYPE_CD': 'DILUENT_TYPE_DISPLAY', 'MEDICATION_FORM_CD': 'MEDICATION_FORM_DISPLAY', 'ADMIN_STRENGTH_UNIT_CD': 'ADMIN_STRENGTH_UNIT_DISPLAY', 'SUBSTANCE_MANUFACTURER_CD': 'SUBSTANCE_MANUFACTURER_DISPLAY', 'REFUSAL_CD': 'REFUSAL_DISPLAY', 'IV_EVENT_CD': 'IV_EVENT_DISPLAY', 'REMAINING_VOLUME_UNIT_CD': 'REMAINING_VOLUME_UNIT_DISPLAY', 'WEIGHT_UNIT_CD': 'WEIGHT_UNIT_DISPLAY', 'BOLUS_TYPE_CD': 'BOLUS_TYPE_DISPLAY', 'ORDER_STATUS_CD': 'ORDER_STATUS_DISPLAY', 'MED_ORDER_TYPE_CD': 'MED_ORDER_TYPE_DISPLAY', 'ORDER_STRENGTH_UNIT_CD': 'ORDER_STRENGTH_UNIT_DISPLAY', 'ORDER_VOLUME_UNIT_CD': 'ORDER_VOLUME_UNIT_DISPLAY', 'ORDERED_DOSE_UNIT_CD': 'ORDERED_DOSE_UNIT_DISPLAY', 'DOSE_QUANTITY_UNIT_CD': 'DOSE_QUANTITY_UNIT_DISPLAY', 'NORMALIZED_RATE_UNIT_CD': 'NORMALIZED_RATE_UNIT_DISPLAY', 'CONCENTRATION_UNIT_CD': 'CONCENTRATION_UNIT_DISPLAY'}

def _ma_add_code_descriptions(frame: DataFrame, lookup: DataFrame) -> DataFrame:
    result = frame.withColumn('CODE_LOOKUP_ADC_UPDT', F.lit(None).cast('timestamp'))
    for index, (code_column, description_column) in enumerate(_MA_CODE_DISPLAY_COLUMNS.items()):
        if code_column not in result.columns:
            continue
        code_alias = f'_CV_CODE_{index}'
        description_alias = f'_CV_DESC_{index}'
        timestamp_alias = f'_CV_TS_{index}'
        dimension = lookup.select(F.col('CODE_VALUE').alias(code_alias), F.col('CODE_DESCRIPTION').alias(description_alias), F.col('CODE_ADC_UPDT').alias(timestamp_alias))
        result = result.join(F.broadcast(dimension), F.col(code_column) == F.col(code_alias), 'left').withColumn(description_column, F.col(description_alias)).withColumn('CODE_LOOKUP_ADC_UPDT', F.greatest(F.col('CODE_LOOKUP_ADC_UPDT'), F.col(timestamp_alias))).drop(code_alias, description_alias, timestamp_alias)
    return result

def _ma_attach_representative_ingredient(events: DataFrame, ingredients: DataFrame) -> DataFrame:
    event_columns = events.select('EVENT_ID', 'ORDER_ID', 'TEMPLATE_ORDER_ID', 'ORDER_SYNONYM_ID')
    ingredient_columns = ingredients.columns
    template_candidates = event_columns.alias('e').join(ingredients.alias('i'), (F.col('e.TEMPLATE_ORDER_ID') == F.col('i.INGREDIENT_ORDER_ID')) & (F.col('e.ORDER_SYNONYM_ID') == F.col('i.INGREDIENT_SYNONYM_ID')), 'inner').select(F.col('e.EVENT_ID').alias('EVENT_ID'), *[F.col(f'i.{column}').alias(column) for column in ingredient_columns], F.lit('TEMPLATE_SYNONYM').alias('INGREDIENT_MATCH_PATH'), F.lit(1).alias('_INGREDIENT_PATH_PRIORITY'))
    actual_candidates = event_columns.alias('e').join(ingredients.alias('i'), (F.col('e.ORDER_ID') == F.col('i.INGREDIENT_ORDER_ID')) & (F.col('e.ORDER_SYNONYM_ID') == F.col('i.INGREDIENT_SYNONYM_ID')), 'inner').select(F.col('e.EVENT_ID').alias('EVENT_ID'), *[F.col(f'i.{column}').alias(column) for column in ingredient_columns], F.lit('ACTUAL_SYNONYM').alias('INGREDIENT_MATCH_PATH'), F.lit(2).alias('_INGREDIENT_PATH_PRIORITY'))
    candidates = template_candidates.unionByName(actual_candidates)
    stats = candidates.groupBy('EVENT_ID').agg(F.count(F.lit(1)).cast('long').alias('INGREDIENT_ROW_COUNT'), F.countDistinct('ACTION_SEQUENCE').cast('long').alias('INGREDIENT_ACTION_COUNT'), F.countDistinct(F.struct(F.col('INGREDIENT_ORDER_ID'), F.col('INGREDIENT_SYNONYM_ID'), F.col('COMP_SEQUENCE'))).cast('long').alias('INGREDIENT_COMPONENT_COUNT'))
    window = Window.partitionBy('EVENT_ID').orderBy(F.col('_INGREDIENT_PATH_PRIORITY').asc(), F.col('ACTION_SEQUENCE').desc_nulls_last(), F.col('CLINICALLY_SIGNIFICANT_FLAG').desc_nulls_last(), F.col('COMP_SEQUENCE').asc_nulls_last(), F.col('OI_ADC_UPDT').desc_nulls_last(), F.col('OI_UPDT_DT_TM').desc_nulls_last())
    representative = candidates.withColumn('_INGREDIENT_RN', F.row_number().over(window)).where(F.col('_INGREDIENT_RN') == 1).drop('_INGREDIENT_RN', '_INGREDIENT_PATH_PRIORITY').withColumnRenamed('ACTION_SEQUENCE', 'INGREDIENT_ACTION_SEQUENCE').withColumnRenamed('COMP_SEQUENCE', 'INGREDIENT_COMP_SEQUENCE')
    representative = representative.join(stats, 'EVENT_ID', 'left')
    return events.join(representative, 'EVENT_ID', 'left').withColumn('INGREDIENT_MULTI_ACTION_IND', F.coalesce(F.col('INGREDIENT_ACTION_COUNT') > 1, F.lit(False))).withColumn('INGREDIENT_MULTI_COMPONENT_IND', F.coalesce(F.col('INGREDIENT_COMPONENT_COUNT') > 1, F.lit(False)))

def _ma_build_ingredient_child_rows(events: DataFrame, ingredients: DataFrame, source_versions_json: str, run_id: str, run_timestamp: datetime) -> DataFrame:
    links = events.select('EVENT_ID', 'ORDER_ID', 'TEMPLATE_ORDER_ID').dropDuplicates(['EVENT_ID'])
    ingredient_columns = ingredients.columns
    template_rows = links.alias('e').join(ingredients.alias('i'), F.col('e.TEMPLATE_ORDER_ID') == F.col('i.INGREDIENT_ORDER_ID'), 'inner').select(F.col('e.EVENT_ID').alias('EVENT_ID'), F.col('e.ORDER_ID').alias('ORDER_ID'), F.col('e.TEMPLATE_ORDER_ID').alias('TEMPLATE_ORDER_ID'), F.lit('TEMPLATE').alias('INGREDIENT_MATCH_PATH'), *[F.col(f'i.{column}').alias(column) for column in ingredient_columns], F.lit(1).alias('_PATH_PRIORITY'))
    actual_rows = links.alias('e').join(ingredients.alias('i'), F.col('e.ORDER_ID') == F.col('i.INGREDIENT_ORDER_ID'), 'inner').select(F.col('e.EVENT_ID').alias('EVENT_ID'), F.col('e.ORDER_ID').alias('ORDER_ID'), F.col('e.TEMPLATE_ORDER_ID').alias('TEMPLATE_ORDER_ID'), F.lit('ACTUAL').alias('INGREDIENT_MATCH_PATH'), *[F.col(f'i.{column}').alias(column) for column in ingredient_columns], F.lit(2).alias('_PATH_PRIORITY'))
    combined = template_rows.unionByName(actual_rows)
    identity = ['EVENT_ID', 'INGREDIENT_ORDER_ID', 'ACTION_SEQUENCE', 'COMP_SEQUENCE', 'INGREDIENT_SYNONYM_ID', 'OI_ADC_UPDT']
    window = Window.partitionBy(*identity).orderBy(F.col('_PATH_PRIORITY').asc())
    deduplicated = combined.withColumn('_PATH_RN', F.row_number().over(window)).where(F.col('_PATH_RN') == 1).drop('_PATH_RN', '_PATH_PRIORITY')
    child = deduplicated.select('EVENT_ID', 'ORDER_ID', 'TEMPLATE_ORDER_ID', 'INGREDIENT_MATCH_PATH', 'INGREDIENT_ORDER_ID', F.col('ACTION_SEQUENCE').alias('ACTION_SEQUENCE'), F.col('COMP_SEQUENCE').alias('COMP_SEQUENCE'), F.col('OI_CATALOG_TYPE_CD').alias('CATALOG_TYPE_CD'), F.col('OI_CATALOG_CD').alias('CATALOG_CD'), F.col('INGREDIENT_SYNONYM_ID').alias('SYNONYM_ID'), F.col('OI_ORDER_MNEMONIC').alias('ORDER_MNEMONIC'), F.col('OI_ORDER_DETAIL_DISPLAY_LINE').alias('ORDER_DETAIL_DISPLAY_LINE'), F.col('OI_STRENGTH').alias('STRENGTH'), F.col('OI_STRENGTH_UNIT_CD').alias('STRENGTH_UNIT_CD'), F.col('OI_VOLUME').alias('VOLUME'), F.col('OI_VOLUME_UNIT_CD').alias('VOLUME_UNIT_CD'), 'FREETEXT_DOSE', F.col('OI_FREQ_CD').alias('FREQ_CD'), F.col('OI_IV_SEQ').alias('IV_SEQ'), 'DOSE_QUANTITY', 'DOSE_QUANTITY_UNIT_CD', F.col('OI_ORDERED_AS_MNEMONIC').alias('ORDERED_AS_MNEMONIC'), F.col('OI_SUPPLIED_AS_MNEMONIC').alias('SUPPLIED_AS_MNEMONIC'), F.col('OI_HNA_ORDER_MNEMONIC').alias('HNA_ORDER_MNEMONIC'), 'INGREDIENT_TYPE_FLAG', 'CLINICALLY_SIGNIFICANT_FLAG', 'INCLUDE_IN_TOTAL_VOLUME_FLAG', 'ORDERED_DOSE', 'ORDERED_DOSE_UNIT_CD', 'NORMALIZED_RATE', 'NORMALIZED_RATE_UNIT_CD', 'CONCENTRATION', 'CONCENTRATION_UNIT_CD', 'DOSING_CAPACITY', 'DAYS_OF_ADMINISTRATION_DISPLAY', 'DOSE_ADJUSTMENT_DISPLAY', 'ORDERED_AS_SYNONYM_ID', F.col('OI_UPDT_CNT').alias('UPDT_CNT'), F.col('OI_UPDT_DT_TM').alias('UPDT_DT_TM'), F.col('OI_UPDT_ID').alias('UPDT_ID'), F.col('OI_UPDT_TASK').alias('UPDT_TASK'), F.col('OI_UPDT_APPLCTX').alias('UPDT_APPLCTX'), F.col('OI_LAST_UTC_TS').alias('LAST_UTC_TS'), F.col('OI_INST_ID').alias('INST_ID'), F.col('OI_TXN_ID_TEXT').alias('TXN_ID_TEXT'), F.col('OI_ADC_UPDT').alias('ADC_UPDT'), F.col('OI_TRUST').alias('TRUST'), F.col('OI_ENCNTR_ID').alias('ENCNTR_ID'), F.col('OI_ORGANIZATION_ID').alias('ORGANIZATION_ID'), F.lit(source_versions_json).alias('SOURCE_VERSIONS_JSON'), F.lit(run_id).alias('PIPELINE_RUN_ID'), F.lit(run_timestamp).cast('timestamp').alias('PIPELINE_UPDT_DT_TM'))
    child = _ma_hash_columns(child, schema_map_med_admin_ingredient, excluded=('ROW_HASH', 'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM'))
    return _ma_schema_select(child, schema_map_med_admin_ingredient)

def _ma_build_joined_source_rows(snapshots: Dict[str, DataFrame], event_keys: Optional[DataFrame], run_id: str, run_timestamp: datetime, source_versions: Dict[str, int], config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> Tuple[DataFrame, DataFrame]:
    mae_raw = snapshots[config.med_admin_event_table]
    if event_keys is None:
        event_keys = mae_raw.select(F.col('EVENT_ID').cast('long').alias('EVENT_ID')).where(F.col('EVENT_ID').isNotNull()).dropDuplicates(['EVENT_ID'])
    clinical = _ma_prepare_clinical_event(snapshots[config.clinical_event_table], event_keys, run_timestamp)
    med_admin = _ma_prepare_med_admin_event(mae_raw, event_keys)
    med_result = _ma_prepare_med_result(snapshots[config.med_result_table], event_keys, run_timestamp)
    base = clinical.join(med_admin, 'EVENT_ID', 'inner').join(med_result, 'EVENT_ID', 'left').withColumn('PERSON_ID', F.coalesce(F.col('CE_PERSON_ID'))).withColumn('ENCNTR_ID', F.coalesce(F.col('CE_ENCNTR_ID'), F.col('MAE_ENCNTR_ID'), F.col('MR_ENCNTR_ID'))).withColumn('ORDER_ID', F.coalesce(F.col('CE_ORDER_ID'), F.col('MAE_ORDER_ID')))
    order_keys = base.select('ORDER_ID').where(F.col('ORDER_ID').isNotNull()).dropDuplicates(['ORDER_ID'])
    orders = _ma_prepare_orders(snapshots[config.orders_table], order_keys)
    base = base.join(orders, 'ORDER_ID', 'left').withColumn('PERSON_ID', F.coalesce(F.col('PERSON_ID'), F.col('ORD_PERSON_ID'))).withColumn('ENCNTR_ID', F.coalesce(F.col('ENCNTR_ID'), F.col('ORD_ENCNTR_ID'))).withColumn('TEMPLATE_ORDER_ID', F.coalesce(F.col('ORD_TEMPLATE_ORDER_ID'), F.col('MAE_TEMPLATE_ORDER_ID'))).withColumn('ORDER_SYNONYM_ID', F.coalesce(F.col('ORDER_SYNONYM_ID'), F.col('MR_SYNONYM_ID')))
    synonym = _ma_prepare_order_synonym(snapshots[config.order_synonym_table])
    medication_lookup = _ma_prepare_medication_lookup(snapshots[config.medication_lookup_table])
    base = base.join(F.broadcast(synonym), 'ORDER_SYNONYM_ID', 'left').join(F.broadcast(medication_lookup), 'ORDER_SYNONYM_ID', 'left')
    base = _ma_persist(base)
    ingredients = _ma_prepare_order_ingredient(snapshots[config.order_ingredient_table])
    relevant_ingredient_orders = base.select(F.col('ORDER_ID').alias('INGREDIENT_ORDER_ID')).unionByName(base.select(F.col('TEMPLATE_ORDER_ID').alias('INGREDIENT_ORDER_ID'))).where(F.col('INGREDIENT_ORDER_ID').isNotNull()).dropDuplicates(['INGREDIENT_ORDER_ID'])
    ingredients = _ma_persist(ingredients.join(relevant_ingredient_orders, 'INGREDIENT_ORDER_ID', 'inner'))
    child_base = base.select('EVENT_ID', 'ORDER_ID', 'TEMPLATE_ORDER_ID').dropDuplicates(['EVENT_ID'])
    source_versions_json = json.dumps(source_versions, sort_keys=True)
    child_rows = _ma_build_ingredient_child_rows(child_base, ingredients, source_versions_json, run_id, run_timestamp)
    base = _ma_attach_representative_ingredient(base, ingredients)
    return (base, child_rows)

def _ma_preferred_rxnorm_description(raw: DataFrame) -> DataFrame:
    selected = raw.where(F.col('RXCUI').isNotNull() & F.col('STR').isNotNull()).select(F.col('RXCUI').cast('string').alias('RXNORM_CUI'), F.col('STR').cast('string').alias('RXNORM_STR'), F.col('LAT').cast('string').alias('_RX_LAT'), F.col('SUPPRESS').cast('string').alias('_RX_SUPPRESS'), F.col('SAB').cast('string').alias('_RX_SAB'), F.col('ISPREF').cast('string').alias('_RX_ISPREF'), F.col('TTY').cast('string').alias('_RX_TTY'), F.col('RXAUI').cast('string').alias('_RX_AUI'))
    tty_priority = F.when(F.col('_RX_TTY').isin('IN', 'PIN', 'MIN'), 1).when(F.col('_RX_TTY').isin('SCD', 'SBD', 'SCDG', 'SBDG'), 2).when(F.col('_RX_TTY').isin('BN', 'PSN'), 3).otherwise(9)
    window = Window.partitionBy('RXNORM_CUI').orderBy(F.when(F.col('_RX_LAT') == 'ENG', 0).otherwise(1), F.when(F.col('_RX_SUPPRESS') == 'N', 0).otherwise(1), F.when(F.col('_RX_SAB') == 'RXNORM', 0).otherwise(1), F.when(F.col('_RX_ISPREF') == 'Y', 0).otherwise(1), tty_priority, F.length('RXNORM_STR').asc(), F.col('RXNORM_STR').asc(), F.col('_RX_AUI').asc_nulls_last())
    return selected.withColumn('_RX_RN', F.row_number().over(window)).where(F.col('_RX_RN') == 1).select('RXNORM_CUI', 'RXNORM_STR')

def _ma_preferred_snomed_description(raw: DataFrame) -> DataFrame:
    selected = raw.where((F.col('SAB') == 'SNOMEDCT_US') & F.col('CODE').isNotNull() & F.col('STR').isNotNull()).select(F.col('CODE').cast('string').alias('SNOMED_CODE'), F.col('STR').cast('string').alias('_RXN_SNOMED_STR'), F.col('LAT').cast('string').alias('_SNO_LAT'), F.col('SUPPRESS').cast('string').alias('_SNO_SUPPRESS'), F.col('ISPREF').cast('string').alias('_SNO_ISPREF'), F.col('TTY').cast('string').alias('_SNO_TTY'), F.col('RXAUI').cast('string').alias('_SNO_AUI'))
    window = Window.partitionBy('SNOMED_CODE').orderBy(F.when(F.col('_SNO_LAT') == 'ENG', 0).otherwise(1), F.when(F.col('_SNO_SUPPRESS') == 'N', 0).otherwise(1), F.when(F.col('_SNO_ISPREF') == 'Y', 0).otherwise(1), F.when(F.col('_SNO_TTY').isin('PT', 'FN'), 0).otherwise(1), F.length('_RXN_SNOMED_STR').asc(), F.col('_RXN_SNOMED_STR').asc(), F.col('_SNO_AUI').asc_nulls_last())
    return selected.withColumn('_SNO_RN', F.row_number().over(window)).where(F.col('_SNO_RN') == 1).select('SNOMED_CODE', '_RXN_SNOMED_STR')

def _ma_prepare_concepts(raw: DataFrame) -> DataFrame:
    return raw.select(_ma_checked_long(F.col('concept_id'), 'OMOP.CONCEPT_ID', True).alias('CONCEPT_ID'), F.col('concept_name').cast('string').alias('CONCEPT_NAME'), F.col('domain_id').cast('string').alias('DOMAIN_ID'), F.col('vocabulary_id').cast('string').alias('VOCABULARY_ID'), F.col('concept_class_id').cast('string').alias('CONCEPT_CLASS_ID'), F.col('standard_concept').cast('string').alias('STANDARD_CONCEPT'), F.col('concept_code').cast('string').alias('CONCEPT_CODE'), F.col('invalid_reason').cast('string').alias('INVALID_REASON'))

def _ma_concept_code_dimension(concepts: DataFrame, vocabulary: str, key_name: str, prefix: str) -> DataFrame:
    filtered = concepts.where((F.col('VOCABULARY_ID') == vocabulary) & F.col('CONCEPT_CODE').isNotNull() & F.col('INVALID_REASON').isNull())
    counts = filtered.groupBy('CONCEPT_CODE').agg(F.countDistinct('CONCEPT_ID').cast('long').alias(f'{prefix}_CANDIDATE_COUNT'))
    window = Window.partitionBy('CONCEPT_CODE').orderBy(F.when(F.col('STANDARD_CONCEPT') == 'S', 0).otherwise(1), F.when(F.col('DOMAIN_ID').isin('Drug', 'Ingredient'), 0).otherwise(1), F.col('CONCEPT_ID').asc())
    selected = filtered.withColumn('_CODE_RN', F.row_number().over(window)).where(F.col('_CODE_RN') == 1).drop('_CODE_RN').join(counts, 'CONCEPT_CODE', 'left')
    return selected.select(F.col('CONCEPT_CODE').alias(key_name), F.col('CONCEPT_ID').alias(f'{prefix}_CONCEPT_ID'), F.col(f'{prefix}_CANDIDATE_COUNT'))

def _ma_exact_standard_name_dimension(concepts: DataFrame) -> DataFrame:
    candidates = concepts.where(F.col('INVALID_REASON').isNull() & (F.col('STANDARD_CONCEPT') == 'S') & F.col('DOMAIN_ID').isin('Drug', 'Ingredient')).withColumn('_EXACT_NAME_KEY', _ma_normalize_term(F.col('CONCEPT_NAME'))).where(F.col('_EXACT_NAME_KEY') != '')
    counts = candidates.groupBy('_EXACT_NAME_KEY').agg(F.countDistinct('CONCEPT_ID').cast('long').alias('EXACT_NAME_CANDIDATE_COUNT'))
    unique = candidates.join(counts, '_EXACT_NAME_KEY', 'inner').where(F.col('EXACT_NAME_CANDIDATE_COUNT') == 1)
    window = Window.partitionBy('_EXACT_NAME_KEY').orderBy(F.col('CONCEPT_ID').asc())
    return unique.withColumn('_NAME_RN', F.row_number().over(window)).where(F.col('_NAME_RN') == 1).select(F.col('_EXACT_NAME_KEY').alias('EXACT_NAME_KEY'), F.col('CONCEPT_ID').alias('EXACT_NAME_CONCEPT_ID'), 'EXACT_NAME_CANDIDATE_COUNT')

def _ma_apply_omop_mapping(frame: DataFrame, concepts: DataFrame, relationships_raw: DataFrame, config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> DataFrame:
    threshold = F.coalesce(F.col('LOOKUP_SIMILARITY_THRESHOLD_RAW'), F.lit(config.default_similarity_threshold))
    frame = frame.withColumns({'LOOKUP_SIMILARITY_THRESHOLD': threshold, 'LOOKUP_OMOP_CONCEPT_ID': F.coalesce(F.col('LKP_MAPPED_OMOP_CONCEPT_ID'), F.when(F.col('LOOKUP_SIMILARITY_SCORE') >= threshold, F.col('LKP_SIMILARITY_OMOP_CONCEPT_ID'))), 'LOOKUP_OMOP_CONCEPT_TERM': F.coalesce(F.col('LKP_MAPPED_OMOP_CONCEPT_TERM'), F.when(F.col('LOOKUP_SIMILARITY_SCORE') >= threshold, F.col('LKP_SIMILARITY_OMOP_CONCEPT_TERM'))), 'LOOKUP_OMOP_METHOD': F.when(F.col('LKP_MAPPED_OMOP_CONCEPT_ID').isNotNull(), F.lit('STANDARD_MAP')).when(F.col('LKP_SIMILARITY_OMOP_CONCEPT_ID').isNotNull() & (F.col('LOOKUP_SIMILARITY_SCORE') >= threshold), F.lit('VECTOR_SIMILARITY'))})
    rxnorm = _ma_concept_code_dimension(concepts, 'RxNorm', '_RXNORM_CODE_KEY', 'RXNORM_CODE')
    rxnorm_ext = _ma_concept_code_dimension(concepts, 'RxNorm Extension', '_RXNORM_EXT_CODE_KEY', 'RXNORM_EXT_CODE')
    snomed = _ma_concept_code_dimension(concepts, 'SNOMED', '_SNOMED_CODE_KEY', 'SNOMED_CODE_MATCH')
    multum = _ma_concept_code_dimension(concepts, 'Multum', '_MULTUM_CODE_KEY', 'MULTUM_CODE')
    frame = frame.join(rxnorm, F.col('RXNORM_CUI') == F.col('_RXNORM_CODE_KEY'), 'left').join(rxnorm_ext, F.col('RXNORM_CUI') == F.col('_RXNORM_EXT_CODE_KEY'), 'left').join(snomed, F.coalesce(_ma_nonblank(F.col('LOOKUP_SNOMED_CODE')), _ma_nonblank(F.col('LOOKUP_SNOMED_FROM_OMOP'))) == F.col('_SNOMED_CODE_KEY'), 'left').join(multum, F.col('MULTUM') == F.col('_MULTUM_CODE_KEY'), 'left').withColumn('_ORDER_TERM_EXACT', _ma_normalize_term(F.col('ORDER_MNEMONIC')))
    if config.enable_exact_unique_name_fallback:
        exact_names = _ma_exact_standard_name_dimension(concepts)
        frame = frame.join(exact_names, F.col('_ORDER_TERM_EXACT') == F.col('EXACT_NAME_KEY'), 'left')
    else:
        frame = frame.withColumn('EXACT_NAME_CONCEPT_ID', F.lit(None).cast('long')).withColumn('EXACT_NAME_CANDIDATE_COUNT', F.lit(None).cast('long'))
    source_concept_id = F.coalesce(F.col('LOOKUP_OMOP_CONCEPT_ID'), F.col('RXNORM_CODE_CONCEPT_ID'), F.col('RXNORM_EXT_CODE_CONCEPT_ID'), F.col('SNOMED_CODE_MATCH_CONCEPT_ID'), F.col('MULTUM_CODE_CONCEPT_ID'), F.col('EXACT_NAME_CONCEPT_ID'))
    method = F.when(F.col('LOOKUP_OMOP_CONCEPT_ID').isNotNull(), F.concat(F.lit('LOOKUP_'), F.col('LOOKUP_OMOP_METHOD'))).when(F.col('RXNORM_CODE_CONCEPT_ID').isNotNull(), F.lit('RXNORM_CODE')).when(F.col('RXNORM_EXT_CODE_CONCEPT_ID').isNotNull(), F.lit('RXNORM_EXTENSION_CODE')).when(F.col('SNOMED_CODE_MATCH_CONCEPT_ID').isNotNull(), F.lit('SNOMED_CODE')).when(F.col('MULTUM_CODE_CONCEPT_ID').isNotNull(), F.lit('MULTUM_CODE')).when(F.col('EXACT_NAME_CONCEPT_ID').isNotNull(), F.lit('EXACT_UNIQUE_STANDARD_NAME')).otherwise(F.lit('UNMAPPED'))
    confidence = F.when(F.col('LKP_MAPPED_OMOP_CONCEPT_ID').isNotNull(), F.lit(1.0)).when(F.col('LOOKUP_OMOP_METHOD') == 'VECTOR_SIMILARITY', F.col('LOOKUP_SIMILARITY_SCORE')).when(F.col('RXNORM_CODE_CONCEPT_ID').isNotNull(), F.lit(0.99)).when(F.col('RXNORM_EXT_CODE_CONCEPT_ID').isNotNull(), F.lit(0.98)).when(F.col('SNOMED_CODE_MATCH_CONCEPT_ID').isNotNull(), F.lit(0.98)).when(F.col('MULTUM_CODE_CONCEPT_ID').isNotNull(), F.lit(0.97)).when(F.col('EXACT_NAME_CONCEPT_ID').isNotNull(), F.lit(0.9))
    source_count = F.when(F.col('LOOKUP_OMOP_CONCEPT_ID').isNotNull(), F.lit(1).cast('long')).when(F.col('RXNORM_CODE_CONCEPT_ID').isNotNull(), F.col('RXNORM_CODE_CANDIDATE_COUNT')).when(F.col('RXNORM_EXT_CODE_CONCEPT_ID').isNotNull(), F.col('RXNORM_EXT_CODE_CANDIDATE_COUNT')).when(F.col('SNOMED_CODE_MATCH_CONCEPT_ID').isNotNull(), F.col('SNOMED_CODE_MATCH_CANDIDATE_COUNT')).when(F.col('MULTUM_CODE_CONCEPT_ID').isNotNull(), F.col('MULTUM_CODE_CANDIDATE_COUNT')).when(F.col('EXACT_NAME_CONCEPT_ID').isNotNull(), F.col('EXACT_NAME_CANDIDATE_COUNT'))
    frame = frame.withColumns({'OMOP_SOURCE_CONCEPT_ID': source_concept_id.cast('long'), 'OMOP_MAPPING_METHOD': method, 'OMOP_MAPPING_CONFIDENCE': confidence.cast('double'), 'OMOP_SOURCE_MATCH_CANDIDATE_COUNT': source_count.cast('long')})
    source_meta = concepts.select(F.col('CONCEPT_ID').alias('_SOURCE_META_ID'), F.col('CONCEPT_NAME').alias('OMOP_SOURCE_CONCEPT_NAME'), F.col('VOCABULARY_ID').alias('OMOP_SOURCE_VOCABULARY_ID'), F.col('CONCEPT_CODE').alias('OMOP_SOURCE_CONCEPT_CODE'), F.col('STANDARD_CONCEPT').alias('OMOP_SOURCE_STANDARD_CONCEPT'), F.col('DOMAIN_ID').alias('_OMOP_SOURCE_DOMAIN_ID'), F.col('INVALID_REASON').alias('_OMOP_SOURCE_INVALID_REASON'))
    frame = frame.join(source_meta, F.col('OMOP_SOURCE_CONCEPT_ID') == F.col('_SOURCE_META_ID'), 'left').withColumn('OMOP_SOURCE_CONCEPT_VALID_IND', F.col('_SOURCE_META_ID').isNotNull() & F.col('_OMOP_SOURCE_INVALID_REASON').isNull())
    relationships = relationships_raw.select(_ma_checked_long(F.col('concept_id_1'), 'CONCEPT_RELATIONSHIP.CONCEPT_ID_1', True).alias('CONCEPT_ID_1'), _ma_checked_long(F.col('concept_id_2'), 'CONCEPT_RELATIONSHIP.CONCEPT_ID_2', True).alias('CONCEPT_ID_2'), F.col('relationship_id').cast('string').alias('RELATIONSHIP_ID'), F.col('invalid_reason').cast('string').alias('REL_INVALID_REASON'))
    source_ids = frame.select(F.col('OMOP_SOURCE_CONCEPT_ID').alias('CONCEPT_ID_1')).where(F.col('CONCEPT_ID_1').isNotNull()).dropDuplicates(['CONCEPT_ID_1'])
    valid_standard = concepts.where(F.col('INVALID_REASON').isNull() & (F.col('STANDARD_CONCEPT') == 'S') & F.col('DOMAIN_ID').isin('Drug', 'Ingredient')).select(F.col('CONCEPT_ID').alias('_STANDARD_CANDIDATE_ID'), F.col('CONCEPT_NAME').alias('_STANDARD_CANDIDATE_NAME'))
    maps_to = relationships.where((F.col('RELATIONSHIP_ID') == 'Maps to') & F.col('REL_INVALID_REASON').isNull()).join(source_ids, 'CONCEPT_ID_1', 'inner').join(valid_standard, F.col('CONCEPT_ID_2') == F.col('_STANDARD_CANDIDATE_ID'), 'inner').groupBy('CONCEPT_ID_1').agg(F.countDistinct('_STANDARD_CANDIDATE_ID').cast('long').alias('_MAPS_TO_COUNT'), F.min('_STANDARD_CANDIDATE_ID').cast('long').alias('_MAPS_TO_ID')).select(F.col('CONCEPT_ID_1').alias('_MAPS_TO_SOURCE_ID'), '_MAPS_TO_COUNT', '_MAPS_TO_ID')
    frame = frame.join(maps_to, F.col('OMOP_SOURCE_CONCEPT_ID') == F.col('_MAPS_TO_SOURCE_ID'), 'left')
    source_is_valid_standard = F.col('OMOP_SOURCE_CONCEPT_VALID_IND') & (F.col('OMOP_SOURCE_STANDARD_CONCEPT') == 'S') & F.col('_OMOP_SOURCE_DOMAIN_ID').isin('Drug', 'Ingredient')
    final_standard_id = F.when(source_is_valid_standard, F.col('OMOP_SOURCE_CONCEPT_ID')).when(F.col('_MAPS_TO_COUNT') == 1, F.col('_MAPS_TO_ID'))
    frame = frame.withColumns({'OMOP_CONCEPT_ID': final_standard_id.cast('long'), 'OMOP_STANDARD_CANDIDATE_COUNT': F.when(source_is_valid_standard, F.lit(1).cast('long')).otherwise(F.coalesce(F.col('_MAPS_TO_COUNT'), F.lit(0).cast('long'))), 'OMOP_MAPPING_AMBIGUOUS_IND': F.coalesce((F.col('OMOP_SOURCE_MATCH_CANDIDATE_COUNT') > 1) | (F.col('_MAPS_TO_COUNT') > 1), F.lit(False))})
    standard_meta = concepts.select(F.col('CONCEPT_ID').alias('_FINAL_STANDARD_ID'), F.col('CONCEPT_NAME').alias('OMOP_CONCEPT_NAME'), F.col('STANDARD_CONCEPT').alias('_FINAL_STANDARD_FLAG'), F.col('DOMAIN_ID').alias('_FINAL_STANDARD_DOMAIN'), F.col('INVALID_REASON').alias('_FINAL_STANDARD_INVALID'))
    frame = frame.join(standard_meta, F.col('OMOP_CONCEPT_ID') == F.col('_FINAL_STANDARD_ID'), 'left').withColumns({'OMOP_STANDARD_CONCEPT': F.when((F.col('_FINAL_STANDARD_FLAG') == 'S') & F.col('_FINAL_STANDARD_DOMAIN').isin('Drug', 'Ingredient') & F.col('_FINAL_STANDARD_INVALID').isNull(), F.lit('S')), 'OMOP_TYPE': F.col('OMOP_MAPPING_METHOD')})
    return frame

def _ma_apply_snomed_mapping(frame: DataFrame, concepts: DataFrame, relationships_raw: DataFrame, rxnconso_raw: DataFrame) -> DataFrame:
    lookup_code = F.coalesce(_ma_nonblank(F.col('LOOKUP_SNOMED_CODE')), _ma_nonblank(F.col('LOOKUP_SNOMED_FROM_OMOP')))
    frame = frame.withColumn('_LOOKUP_SNOMED_SELECTED', lookup_code)
    snomed_concepts = concepts.where((F.col('VOCABULARY_ID') == 'SNOMED') & F.col('INVALID_REASON').isNull() & F.col('CONCEPT_CODE').isNotNull())
    snomed_counts = snomed_concepts.groupBy('CONCEPT_CODE').agg(F.countDistinct('CONCEPT_ID').cast('long').alias('_SNOMED_CODE_CONCEPT_COUNT'), F.max(F.when(F.col('DOMAIN_ID').isin('Drug', 'Ingredient'), 1).otherwise(0)).alias('_SNOMED_HAS_DRUG_DOMAIN'))
    window = Window.partitionBy('CONCEPT_CODE').orderBy(F.when(F.col('DOMAIN_ID').isin('Drug', 'Ingredient'), 0).otherwise(1), F.col('CONCEPT_ID').asc())
    snomed_by_code = snomed_concepts.withColumn('_SNOMED_CODE_RN', F.row_number().over(window)).where(F.col('_SNOMED_CODE_RN') == 1).drop('_SNOMED_CODE_RN').join(snomed_counts, 'CONCEPT_CODE', 'left').select(F.col('CONCEPT_CODE').alias('_LOOKUP_SNOMED_KEY'), F.col('CONCEPT_ID').alias('_LOOKUP_SNOMED_CONCEPT_ID'), F.col('CONCEPT_NAME').alias('_LOOKUP_SNOMED_CONCEPT_NAME'), F.col('DOMAIN_ID').alias('_LOOKUP_SNOMED_DOMAIN'), '_SNOMED_CODE_CONCEPT_COUNT', '_SNOMED_HAS_DRUG_DOMAIN')
    frame = frame.join(snomed_by_code, F.col('_LOOKUP_SNOMED_SELECTED') == F.col('_LOOKUP_SNOMED_KEY'), 'left')
    relationships = relationships_raw.select(_ma_checked_long(F.col('concept_id_1'), 'CONCEPT_RELATIONSHIP.CONCEPT_ID_1', True).alias('CONCEPT_ID_1'), _ma_checked_long(F.col('concept_id_2'), 'CONCEPT_RELATIONSHIP.CONCEPT_ID_2', True).alias('CONCEPT_ID_2'), F.col('relationship_id').cast('string').alias('RELATIONSHIP_ID'), F.col('invalid_reason').cast('string').alias('REL_INVALID_REASON'))
    standard_ids = frame.select(F.col('OMOP_CONCEPT_ID').alias('CONCEPT_ID_1')).where(F.col('CONCEPT_ID_1').isNotNull()).dropDuplicates(['CONCEPT_ID_1'])
    valid_snomed_drug = snomed_concepts.where(F.col('DOMAIN_ID').isin('Drug', 'Ingredient')).select(F.col('CONCEPT_ID').alias('_REVERSE_SNOMED_ID'), F.col('CONCEPT_CODE').alias('_REVERSE_SNOMED_CODE'), F.col('CONCEPT_NAME').alias('_REVERSE_SNOMED_NAME'))
    reverse = relationships.where((F.col('RELATIONSHIP_ID') == 'Mapped from') & F.col('REL_INVALID_REASON').isNull()).join(standard_ids, 'CONCEPT_ID_1', 'inner').join(valid_snomed_drug, F.col('CONCEPT_ID_2') == F.col('_REVERSE_SNOMED_ID'), 'inner').groupBy('CONCEPT_ID_1').agg(F.countDistinct('_REVERSE_SNOMED_ID').cast('long').alias('_REVERSE_SNOMED_COUNT'), F.min('_REVERSE_SNOMED_ID').cast('long').alias('_REVERSE_SNOMED_SELECTED_ID'), F.min_by(F.col('_REVERSE_SNOMED_CODE'), F.col('_REVERSE_SNOMED_ID')).alias('_REVERSE_SNOMED_SELECTED_CODE'), F.min_by(F.col('_REVERSE_SNOMED_NAME'), F.col('_REVERSE_SNOMED_ID')).alias('_REVERSE_SNOMED_SELECTED_NAME')).select(F.col('CONCEPT_ID_1').alias('_REVERSE_STANDARD_ID'), '_REVERSE_SNOMED_COUNT', '_REVERSE_SNOMED_SELECTED_ID', '_REVERSE_SNOMED_SELECTED_CODE', '_REVERSE_SNOMED_SELECTED_NAME')
    frame = frame.join(reverse, F.col('OMOP_CONCEPT_ID') == F.col('_REVERSE_STANDARD_ID'), 'left')
    selected_code = F.when(F.col('_LOOKUP_SNOMED_SELECTED').isNotNull(), F.col('_LOOKUP_SNOMED_SELECTED')).when(F.col('_REVERSE_SNOMED_COUNT') == 1, F.col('_REVERSE_SNOMED_SELECTED_CODE'))
    frame = frame.withColumns({'SNOMED_CODE': selected_code.cast('string'), 'SNOMED_CONCEPT_ID': F.when(F.col('_LOOKUP_SNOMED_SELECTED').isNotNull(), F.col('_LOOKUP_SNOMED_CONCEPT_ID')).when(F.col('_REVERSE_SNOMED_COUNT') == 1, F.col('_REVERSE_SNOMED_SELECTED_ID')).cast('long'), 'SNOMED_SOURCE': F.when(_ma_nonblank(F.col('LOOKUP_SNOMED_CODE')).isNotNull(), F.lit('LOOKUP_DIRECT')).when(_ma_nonblank(F.col('LOOKUP_SNOMED_FROM_OMOP')).isNotNull(), F.lit('LOOKUP_FROM_OMOP')).when(F.col('_REVERSE_SNOMED_COUNT') == 1, F.lit('OMOP_MAPPED_FROM')).otherwise(F.lit('UNMAPPED')), 'SNOMED_VALID_DRUG_DOMAIN_IND': F.when(F.col('_LOOKUP_SNOMED_SELECTED').isNotNull(), F.col('_SNOMED_HAS_DRUG_DOMAIN') == 1).when(F.col('_REVERSE_SNOMED_COUNT') == 1, F.lit(True)).otherwise(F.lit(False)), 'SNOMED_CANDIDATE_COUNT': F.when(F.col('_LOOKUP_SNOMED_SELECTED').isNotNull(), F.coalesce(F.col('_SNOMED_CODE_CONCEPT_COUNT'), F.lit(0).cast('long'))).otherwise(F.coalesce(F.col('_REVERSE_SNOMED_COUNT'), F.lit(0).cast('long'))), 'SNOMED_MAPPING_AMBIGUOUS_IND': F.coalesce((F.col('_SNOMED_CODE_CONCEPT_COUNT') > 1) | (F.col('_REVERSE_SNOMED_COUNT') > 1), F.lit(False))})
    preferred = _ma_preferred_snomed_description(rxnconso_raw)
    frame = frame.join(preferred, 'SNOMED_CODE', 'left').withColumn('SNOMED_STR', F.coalesce(F.when(F.col('_LOOKUP_SNOMED_SELECTED').isNotNull(), F.col('_LOOKUP_SNOMED_CONCEPT_NAME')), F.when(F.col('_REVERSE_SNOMED_COUNT') == 1, F.col('_REVERSE_SNOMED_SELECTED_NAME')), F.col('_RXN_SNOMED_STR')))
    return frame

# --- Legacy concept mapping -------------------------------------------------
# Ported from /Workspace/Shared/ADC-DB/Prod/Pipelines/Map Pipeline (read-only):
#   augment_snomed_codes        L3726-3998
#   add_omop_mappings           L4022-4191
#   backfill_snomed_from_omop   L4195-4306
# The legacy-named concept columns must keep these old semantics; the
# standard-validated values produced by _ma_apply_omop_mapping and
# _ma_apply_snomed_mapping move to their own columns first.

def _ma_legacy_simplified_udf_equivalent(column: F.Column) -> F.Column:
    # Native equivalent of the legacy extract_drug_name_med_admin UDF (L3713-3720):
    # falsy input -> null; digit anywhere -> lowercase first whitespace-delimited word;
    # otherwise the trimmed lowercase string.
    value = F.lower(column.cast('string'))
    return F.when(_ma_nonblank(column).isNull(), F.lit(None).cast('string')).when(value.rlike('[0-9]'), F.element_at(F.split(F.trim(value), r'\s+'), 1)).otherwise(F.trim(value))

def _ma_legacy_simplified_native(column: F.Column) -> F.Column:
    # Native equivalent of the legacy get_simplified_drug_name_native helper (L4005-4020).
    # add_omop_mappings deliberately uses this variant, which splits on a single space.
    value = F.lower(column.cast('string'))
    return F.when(column.cast('string').rlike('[0-9]'), F.split(value, ' ').getItem(0)).otherwise(F.trim(value))

def _ma_legacy_dedupe_term_pool(pool: DataFrame) -> DataFrame:
    # Legacy row_number over the lowercased term ordered by the SNOMED code, keeping the first row.
    window = Window.partitionBy('_LG_TERM').orderBy(F.col('_LG_SCT').asc_nulls_last())
    return pool.where(F.col('_LG_TERM').isNotNull() & F.col('_LG_SCT').isNotNull()).withColumn('_LG_PRN', F.row_number().over(window)).where(F.col('_LG_PRN') == 1).select('_LG_TERM', '_LG_SCT')

def _ma_legacy_sct_term_pool(rxnconso_raw: DataFrame) -> DataFrame:
    # Legacy MedAdminReferenceTables.rxnorm_snomed (L3684-3696): one SNOMEDCT_US row per RXCUI
    # keeping the shortest CODE. The legacy ordering had no tiebreak; a deterministic one is added.
    base = rxnconso_raw.where(F.col('SAB') == 'SNOMEDCT_US').select(F.col('RXCUI').cast('string').alias('_LG_RXCUI'), F.col('CODE').cast('string').alias('_LG_SCT'), F.col('STR').cast('string').alias('_LG_STR'))
    window = Window.partitionBy('_LG_RXCUI').orderBy(F.length(F.col('_LG_SCT')).asc_nulls_last(), F.col('_LG_SCT').asc_nulls_last(), F.col('_LG_STR').asc_nulls_last())
    picked = base.withColumn('_LG_RXRN', F.row_number().over(window)).where(F.col('_LG_RXRN') == 1)
    return _ma_legacy_dedupe_term_pool(picked.select(F.lower(F.col('_LG_STR')).alias('_LG_TERM'), F.col('_LG_SCT')))

def _ma_legacy_omop_term_pool(concepts: DataFrame, relationships: DataFrame, backward: bool) -> DataFrame:
    # Legacy forward_mappings (L3784-3807) and backward_mappings (L3836-3859). The legacy code
    # collected every valid SNOMED concept_id into a Python list for an isin() filter; an inner
    # join on the same identifiers is used here, which is equivalent and far cheaper.
    valid_snomed = concepts.where((F.col('VOCABULARY_ID') == 'SNOMED') & F.col('STANDARD_CONCEPT').isNotNull() & F.col('DOMAIN_ID').isin('Drug', 'Ingredient') & F.col('INVALID_REASON').isNull()).select(F.col('CONCEPT_ID').alias('_LG_VALID_ID'))
    drugs = concepts.where(F.col('DOMAIN_ID').isin('Drug', 'Ingredient') & F.col('INVALID_REASON').isNull()).select(F.col('CONCEPT_ID').alias('_LG_DRUG_ID'), F.lower(F.col('CONCEPT_NAME')).alias('_LG_TERM'))
    snomed_side = concepts.where(F.col('INVALID_REASON').isNull()).select(F.col('CONCEPT_ID').alias('_LG_SNO_ID'), F.col('CONCEPT_CODE').alias('_LG_SCT'))
    edges = relationships.where(F.col('RELATIONSHIP_ID').isin('Maps to', 'Mapped from'))
    if backward:
        edges = edges.select(F.col('CONCEPT_ID_1').alias('_LG_SNO_SIDE'), F.col('CONCEPT_ID_2').alias('_LG_DRUG_SIDE'))
    else:
        edges = edges.select(F.col('CONCEPT_ID_2').alias('_LG_SNO_SIDE'), F.col('CONCEPT_ID_1').alias('_LG_DRUG_SIDE'))
    joined = edges.join(valid_snomed, F.col('_LG_SNO_SIDE') == F.col('_LG_VALID_ID'), 'inner').join(drugs, F.col('_LG_DRUG_SIDE') == F.col('_LG_DRUG_ID'), 'inner').join(snomed_side, F.col('_LG_SNO_SIDE') == F.col('_LG_SNO_ID'), 'inner')
    return _ma_legacy_dedupe_term_pool(joined.select('_LG_TERM', '_LG_SCT'))

def _ma_legacy_term_dimension(pool: DataFrame, key_alias: str, code_alias: str) -> DataFrame:
    return pool.select(F.col('_LG_TERM').alias(key_alias), F.col('_LG_SCT').alias(code_alias))

def _ma_legacy_rank_window():
    # Legacy add_omop_mappings rank_window (L4038-4043): Standard, then null, then Classification,
    # breaking ties on the highest concept_id.
    return Window.partitionBy('_LG_JOIN_KEY').orderBy(F.when(F.col('STANDARD_CONCEPT') == 'S', 1).when(F.col('STANDARD_CONCEPT').isNull(), 2).otherwise(3), F.col('CONCEPT_ID').desc())

def _ma_legacy_code_dimension(concepts: DataFrame, vocabulary: str, key_alias: str, prefix: str) -> DataFrame:
    # Legacy add_omop_mappings.prepare_lookup (L4045-4058). concepts_base is deliberately NOT
    # filtered on invalid_reason and is not restricted to the Drug/Ingredient domains.
    ranked = concepts.where((F.col('VOCABULARY_ID') == vocabulary) & F.col('CONCEPT_CODE').isNotNull()).withColumn('_LG_JOIN_KEY', F.col('CONCEPT_CODE'))
    ranked = ranked.withColumn('_LG_RANK', F.row_number().over(_ma_legacy_rank_window())).where(F.col('_LG_RANK') == 1)
    return ranked.select(F.col('_LG_JOIN_KEY').alias(key_alias), F.col('CONCEPT_ID').alias(prefix + '_ID'), F.col('CONCEPT_NAME').alias(prefix + '_NAME'), F.col('STANDARD_CONCEPT').alias(prefix + '_STD'))

def _ma_legacy_name_dimension(concepts: DataFrame, key_expression: F.Column, key_alias: str, prefix: str) -> DataFrame:
    # Legacy add_omop_mappings exact_name_concepts (L4072-4085) and simplified_name_concepts
    # (L4087-4100): valid Drug/Ingredient concepts only, ranked with the same window.
    drugs = concepts.where(F.col('DOMAIN_ID').isin('Drug', 'Ingredient') & F.col('INVALID_REASON').isNull()).withColumn('_LG_JOIN_KEY', key_expression)
    ranked = drugs.where(F.col('_LG_JOIN_KEY').isNotNull()).withColumn('_LG_RANK', F.row_number().over(_ma_legacy_rank_window())).where(F.col('_LG_RANK') == 1)
    return ranked.select(F.col('_LG_JOIN_KEY').alias(key_alias), F.col('CONCEPT_ID').alias(prefix + '_ID'), F.col('CONCEPT_NAME').alias(prefix + '_NAME'), F.col('VOCABULARY_ID').alias(prefix + '_VOCAB'), F.col('STANDARD_CONCEPT').alias(prefix + '_STD'))

def _ma_apply_legacy_concept_columns(frame: DataFrame, concepts: DataFrame, relationships_raw: DataFrame, rxnconso_raw: DataFrame, config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> DataFrame:
    # Step 0: preserve the standard-validated values before the legacy names are recomputed.
    frame = frame.withColumns({'OMOP_STANDARD_CONCEPT_ID': F.col('OMOP_CONCEPT_ID').cast('long'), 'OMOP_STANDARD_CONCEPT_NAME': F.col('OMOP_CONCEPT_NAME').cast('string'), 'OMOP_STANDARD_CONCEPT_FLAG': F.col('OMOP_STANDARD_CONCEPT').cast('string'), 'SNOMED_VALIDATED_CODE': F.col('SNOMED_CODE').cast('string'), 'SNOMED_VALIDATED_STR': F.col('SNOMED_STR').cast('string'), 'SNOMED_VALIDATED_SOURCE': F.col('SNOMED_SOURCE').cast('string')})
    relationships = relationships_raw.select(_ma_checked_long(F.col('concept_id_1'), 'CONCEPT_RELATIONSHIP.CONCEPT_ID_1', True).alias('CONCEPT_ID_1'), _ma_checked_long(F.col('concept_id_2'), 'CONCEPT_RELATIONSHIP.CONCEPT_ID_2', True).alias('CONCEPT_ID_2'), F.col('relationship_id').cast('string').alias('RELATIONSHIP_ID'), F.col('invalid_reason').cast('string').alias('REL_INVALID_REASON'))
    # Step 1: legacy base SNOMED code, i.e. the legacy select at Map Pipeline L3517-3519 and L3577.
    frame = frame.withColumn('_LG_BASE_SNOMED', F.coalesce(F.col('LOOKUP_SNOMED_CODE'), F.col('LOOKUP_SNOMED_FROM_OMOP')).cast('string')).withColumn('_LG_ORDER_TERM', F.lower(F.col('ORDER_MNEMONIC').cast('string')))
    # Step 2: the six legacy augment_snomed_codes lanes. Every dimension is unique on its key, so
    # the legacy null-gated join conditions are expressed by the final coalesce and the grain is kept.
    sct_pool = _ma_legacy_sct_term_pool(rxnconso_raw)
    forward_pool = _ma_legacy_omop_term_pool(concepts, relationships, False)
    backward_pool = _ma_legacy_omop_term_pool(concepts, relationships, True)
    frame = frame.join(_ma_legacy_term_dimension(sct_pool, '_LG_SCT1_KEY', '_LG_SCT1'), F.col('_LG_ORDER_TERM') == F.col('_LG_SCT1_KEY'), 'left')
    frame = frame.join(_ma_legacy_term_dimension(forward_pool, '_LG_FWD1_KEY', '_LG_FWD1'), F.col('_LG_ORDER_TERM') == F.col('_LG_FWD1_KEY'), 'left')
    frame = frame.join(_ma_legacy_term_dimension(backward_pool, '_LG_BWD1_KEY', '_LG_BWD1'), F.col('_LG_ORDER_TERM') == F.col('_LG_BWD1_KEY'), 'left')
    frame = frame.withColumn('_LG_SIMPLE_TERM', F.when(F.coalesce(F.col('_LG_SCT1'), F.col('_LG_FWD1'), F.col('_LG_BWD1')).isNull(), _ma_legacy_simplified_udf_equivalent(F.col('_LG_ORDER_TERM'))))
    frame = frame.join(_ma_legacy_term_dimension(sct_pool, '_LG_SCT2_KEY', '_LG_SCT2'), F.col('_LG_SIMPLE_TERM') == F.col('_LG_SCT2_KEY'), 'left')
    frame = frame.join(_ma_legacy_term_dimension(forward_pool, '_LG_FWD2_KEY', '_LG_FWD2'), F.col('_LG_SIMPLE_TERM') == F.col('_LG_FWD2_KEY'), 'left')
    frame = frame.join(_ma_legacy_term_dimension(backward_pool, '_LG_BWD2_KEY', '_LG_BWD2'), F.col('_LG_SIMPLE_TERM') == F.col('_LG_BWD2_KEY'), 'left')
    augmented_code = F.coalesce(F.col('_LG_SCT1'), F.col('_LG_FWD1'), F.col('_LG_BWD1'), F.col('_LG_SCT2'), F.col('_LG_FWD2'), F.col('_LG_BWD2'))
    legacy_snomed_source = F.when(F.col('_LG_BASE_SNOMED').isNotNull(), F.lit('ORIGINAL')).when(F.col('_LG_SCT1').isNotNull(), F.lit('SNOMED_SCT')).when(F.col('_LG_FWD1').isNotNull(), F.lit('OMOP_FORWARD')).when(F.col('_LG_BWD1').isNotNull(), F.lit('OMOP_BACKWARD')).when(F.col('_LG_SCT2').isNotNull(), F.lit('SNOMED_SCT_SIMPLIFIED')).when(F.col('_LG_FWD2').isNotNull(), F.lit('OMOP_FORWARD_SIMPLIFIED')).when(F.col('_LG_BWD2').isNotNull(), F.lit('OMOP_BACKWARD_SIMPLIFIED')).otherwise(F.lit('NOT_FOUND'))
    frame = frame.withColumns({'SNOMED_CODE': F.coalesce(F.col('_LG_BASE_SNOMED'), augmented_code).cast('string'), 'SNOMED_SOURCE': legacy_snomed_source.cast('string')})
    # Step 3: legacy SNOMED_STR describes the base code only; augment_snomed_codes never set it.
    # The legacy join fanned out over every rxnconso description and an arbitrary row survived the
    # later distinct/dedup, so a deterministic preferred description is selected here instead.
    legacy_description = _ma_preferred_snomed_description(rxnconso_raw).select(F.col('SNOMED_CODE').alias('_LG_DESC_KEY'), F.col('_RXN_SNOMED_STR').alias('_LG_DESC_STR'))
    frame = frame.join(legacy_description, F.col('_LG_BASE_SNOMED') == F.col('_LG_DESC_KEY'), 'left').withColumn('SNOMED_STR', F.when(F.col('_LG_BASE_SNOMED').isNotNull(), F.col('_LG_DESC_STR')).cast('string'))
    # Step 4: legacy add_omop_mappings linear chain over the freshly augmented legacy SNOMED_CODE.
    multum_dimension = _ma_legacy_code_dimension(concepts, 'Multum', '_LG_MULTUM_KEY', '_LG_MULTUM')
    rxnorm_dimension = _ma_legacy_code_dimension(concepts, 'RxNorm', '_LG_RXNORM_KEY', '_LG_RXNORM')
    rxnorm_ext_dimension = _ma_legacy_code_dimension(concepts, 'RxNorm Extension', '_LG_RXNORMEXT_KEY', '_LG_RXNORMEXT')
    snomed_dimension = _ma_legacy_code_dimension(concepts, 'SNOMED', '_LG_SNOMEDCODE_KEY', '_LG_SNOMEDCODE')
    exact_dimension = _ma_legacy_name_dimension(concepts, F.lower(F.col('CONCEPT_NAME')), '_LG_EXACTNAME_KEY', '_LG_EXACTNAME')
    simplified_dimension = _ma_legacy_name_dimension(concepts, _ma_legacy_simplified_native(F.col('CONCEPT_NAME')), '_LG_SIMPLENAME_KEY', '_LG_SIMPLENAME')
    frame = frame.withColumn('_LG_OMOP_SIMPLE_TERM', _ma_legacy_simplified_native(F.col('ORDER_MNEMONIC')))
    frame = frame.join(multum_dimension, F.col('MULTUM') == F.col('_LG_MULTUM_KEY'), 'left').join(rxnorm_dimension, F.col('RXNORM_CUI') == F.col('_LG_RXNORM_KEY'), 'left').join(rxnorm_ext_dimension, F.col('RXNORM_CUI') == F.col('_LG_RXNORMEXT_KEY'), 'left').join(snomed_dimension, F.col('SNOMED_CODE') == F.col('_LG_SNOMEDCODE_KEY'), 'left')
    frame = frame.withColumn('_LG_CODE_MATCH_ID', F.coalesce(F.col('_LG_MULTUM_ID'), F.col('_LG_RXNORM_ID'), F.col('_LG_RXNORMEXT_ID'), F.col('_LG_SNOMEDCODE_ID')))
    frame = frame.join(exact_dimension, (F.col('_LG_ORDER_TERM') == F.col('_LG_EXACTNAME_KEY')) & F.col('_LG_CODE_MATCH_ID').isNull(), 'left')
    frame = frame.join(simplified_dimension, (F.col('_LG_OMOP_SIMPLE_TERM') == F.col('_LG_SIMPLENAME_KEY')) & F.col('_LG_CODE_MATCH_ID').isNull() & F.col('_LG_EXACTNAME_ID').isNull(), 'left')
    frame = frame.withColumns({'OMOP_CONCEPT_ID': F.coalesce(F.col('_LG_CODE_MATCH_ID'), F.col('_LG_EXACTNAME_ID'), F.col('_LG_SIMPLENAME_ID')).cast('long'), 'OMOP_CONCEPT_NAME': F.coalesce(F.col('_LG_MULTUM_NAME'), F.col('_LG_RXNORM_NAME'), F.col('_LG_RXNORMEXT_NAME'), F.col('_LG_SNOMEDCODE_NAME'), F.col('_LG_EXACTNAME_NAME'), F.col('_LG_SIMPLENAME_NAME')).cast('string'), 'OMOP_STANDARD_CONCEPT': F.coalesce(F.col('_LG_MULTUM_STD'), F.col('_LG_RXNORM_STD'), F.col('_LG_RXNORMEXT_STD'), F.col('_LG_SNOMEDCODE_STD'), F.col('_LG_EXACTNAME_STD'), F.col('_LG_SIMPLENAME_STD')).cast('string'), 'OMOP_TYPE': F.when(F.col('_LG_MULTUM_ID').isNotNull(), F.lit('MULTUM')).when(F.col('_LG_RXNORM_ID').isNotNull(), F.lit('RXNORM')).when(F.col('_LG_RXNORMEXT_ID').isNotNull(), F.lit('RXNORMEXT')).when(F.col('_LG_SNOMEDCODE_ID').isNotNull(), F.lit('SNOMED')).when(F.col('_LG_EXACTNAME_ID').isNotNull(), F.concat(F.lit('NAME_MATCH_'), F.col('_LG_EXACTNAME_VOCAB'))).when(F.col('_LG_SIMPLENAME_ID').isNotNull(), F.concat(F.lit('SIMPLIFIED_MATCH_'), F.col('_LG_SIMPLENAME_VOCAB'))).cast('string')})
    # Step 5: legacy backfill_snomed_from_omop. Every candidate for an event comes from the same
    # OMOP concept, so the legacy per-EVENT_ID lowest snomed concept_id is a concept-level minimum.
    backfill_relationships = relationships.where(F.col('RELATIONSHIP_ID').isin('Maps to', 'Maps from') & F.col('REL_INVALID_REASON').isNull())
    backfill_snomed = concepts.where((F.col('VOCABULARY_ID') == 'SNOMED') & F.col('INVALID_REASON').isNull()).select(F.col('CONCEPT_ID').alias('_LG_BF_ID'), F.col('CONCEPT_CODE').alias('_LG_BF_CODE'), F.col('CONCEPT_NAME').alias('_LG_BF_NAME'))
    backfill_dimension = backfill_relationships.join(backfill_snomed, F.col('CONCEPT_ID_2') == F.col('_LG_BF_ID'), 'inner').groupBy('CONCEPT_ID_1').agg(F.min_by(F.col('_LG_BF_CODE'), F.col('_LG_BF_ID')).alias('_LG_BF_SELECTED_CODE'), F.min_by(F.col('_LG_BF_NAME'), F.col('_LG_BF_ID')).alias('_LG_BF_SELECTED_NAME')).select(F.col('CONCEPT_ID_1').alias('_LG_BF_KEY'), '_LG_BF_SELECTED_CODE', '_LG_BF_SELECTED_NAME')
    frame = frame.withColumn('_LG_NEEDS_BACKFILL', F.col('SNOMED_CODE').isNull() & F.col('OMOP_CONCEPT_ID').isNotNull()).join(backfill_dimension, F.col('OMOP_CONCEPT_ID') == F.col('_LG_BF_KEY'), 'left')
    backfilled = F.col('_LG_NEEDS_BACKFILL') & F.col('_LG_BF_SELECTED_CODE').isNotNull()
    frame = frame.withColumns({'SNOMED_CODE': F.when(backfilled, F.col('_LG_BF_SELECTED_CODE')).otherwise(F.col('SNOMED_CODE')).cast('string'), 'SNOMED_STR': F.when(backfilled, F.col('_LG_BF_SELECTED_NAME')).otherwise(F.col('SNOMED_STR')).cast('string'), 'SNOMED_SOURCE': F.when(backfilled, F.lit('OMOP')).otherwise(F.col('SNOMED_SOURCE')).cast('string')})
    temporary = [column for column in frame.columns if column.startswith('_LG_')]
    return frame.drop(*temporary) if temporary else frame

def _ma_finalize_target_rows(base: DataFrame, snapshots: Dict[str, DataFrame], trigger_summary: Optional[DataFrame], full_rebuild: bool, run_id: str, run_timestamp: datetime, source_versions: Dict[str, int], config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> DataFrame:
    start_timestamp = F.coalesce(F.col('MAE_BEG_DT_TM'), F.col('MR_ADMIN_START_DT_TM'), F.col('CE_EVENT_START_DT_TM'))
    start_source = F.when(F.col('MAE_BEG_DT_TM').isNotNull(), F.lit('MED_ADMIN_EVENT.BEG_DT_TM')).when(F.col('MR_ADMIN_START_DT_TM').isNotNull(), F.lit('CE_MED_RESULT.ADMIN_START_DT_TM')).when(F.col('CE_EVENT_START_DT_TM').isNotNull(), F.lit('CLINICAL_EVENT.EVENT_START_DT_TM'))
    end_timestamp = F.coalesce(F.col('MAE_END_DT_TM'), F.col('MR_ADMIN_END_DT_TM'), F.col('CE_EVENT_END_DT_TM'))
    end_source = F.when(F.col('MAE_END_DT_TM').isNotNull(), F.lit('MED_ADMIN_EVENT.END_DT_TM')).when(F.col('MR_ADMIN_END_DT_TM').isNotNull(), F.lit('CE_MED_RESULT.ADMIN_END_DT_TM')).when(F.col('CE_EVENT_END_DT_TM').isNotNull(), F.lit('CLINICAL_EVENT.EVENT_END_DT_TM'))
    source_id_conflict = _ma_has_conflict([F.col('CE_PERSON_ID'), F.col('ORD_PERSON_ID')]) | _ma_has_conflict([F.col('CE_ENCNTR_ID'), F.col('MAE_ENCNTR_ID'), F.col('MR_ENCNTR_ID'), F.col('ORD_ENCNTR_ID')]) | _ma_has_conflict([F.col('CE_ORDER_ID'), F.col('MAE_ORDER_ID')])
    organization_conflict = _ma_has_conflict([F.col('CE_ORGANIZATION_ID'), F.col('MAE_ORGANIZATION_ID'), F.col('MR_ORGANIZATION_ID'), F.col('ORD_ORGANIZATION_ID'), F.col('OI_ORGANIZATION_ID')])
    trust_conflict = _ma_has_conflict([F.col('CE_TRUST'), F.col('MAE_TRUST'), F.col('MR_TRUST'), F.col('ORD_TRUST'), F.col('OI_TRUST')])
    frame = base.withColumns({'ADMIN_START_DT_TM': start_timestamp, 'ADMIN_END_DT_TM': end_timestamp, 'ADMIN_START_DT_TM_SOURCE': start_source, 'ADMIN_END_DT_TM_SOURCE': end_source, 'ORDER_CKI': F.coalesce(_ma_nonblank(F.col('ORDERS_CKI')), _ma_nonblank(F.col('SYNONYM_CKI'))), 'ORDER_MNEMONIC': F.coalesce(_ma_nonblank(F.col('ORD_ORDER_MNEMONIC')), _ma_nonblank(F.col('SYNONYM_MNEMONIC')), _ma_nonblank(F.col('LOOKUP_HNA_ORDER_MNEMONIC')), _ma_nonblank(F.col('OI_ORDER_MNEMONIC')), _ma_nonblank(F.col('HNA_ORDER_MNEMONIC')), _ma_nonblank(F.col('ORDERED_AS_MNEMONIC'))), 'ORDER_DETAIL': F.coalesce(_ma_nonblank(F.col('OI_ORDER_DETAIL_DISPLAY_LINE')), _ma_nonblank(F.col('ORDER_DETAIL_DISPLAY_LINE')), _ma_nonblank(F.col('CLINICAL_DISPLAY_LINE'))), 'ORDER_STRENGTH': F.col('OI_STRENGTH'), 'ORDER_STRENGTH_UNIT_CD': F.col('OI_STRENGTH_UNIT_CD'), 'ORDER_VOLUME': F.col('OI_VOLUME'), 'ORDER_VOLUME_UNIT_CD': F.col('OI_VOLUME_UNIT_CD'), 'INITIAL_DOSAGE_UNIT_CD': F.col('ADMIN_DOSAGE_UNIT_CD'), 'ORGANIZATION_ID': F.coalesce(F.col('CE_ORGANIZATION_ID'), F.col('MAE_ORGANIZATION_ID'), F.col('MR_ORGANIZATION_ID'), F.col('ORD_ORGANIZATION_ID'), F.col('OI_ORGANIZATION_ID')), 'TRUST': F.coalesce(_ma_nonblank(F.col('CE_TRUST')), _ma_nonblank(F.col('MAE_TRUST')), _ma_nonblank(F.col('MR_TRUST')), _ma_nonblank(F.col('ORD_TRUST')), _ma_nonblank(F.col('OI_TRUST'))), 'SOURCE_ID_CONFLICT_IND': source_id_conflict, 'SOURCE_ORGANIZATION_CONFLICT_IND': organization_conflict, 'SOURCE_TRUST_CONFLICT_IND': trust_conflict, 'ADC_UPDT': F.greatest(F.col('CE_ADC_UPDT'), F.col('MAE_ADC_UPDT'), F.col('MR_ADC_UPDT'), F.col('ORDERS_ADC_UPDT'), F.col('OI_ADC_UPDT'), F.col('SYNONYM_ADC_UPDT'), F.col('LOOKUP_ADC_UPDT')), 'MAPPING_SCHEMA_VERSION': F.lit(config.mapping_version), 'SOURCE_VERSIONS_JSON': F.lit(json.dumps(source_versions, sort_keys=True)), 'PIPELINE_RUN_ID': F.lit(run_id), 'PIPELINE_UPDT_DT_TM': F.lit(run_timestamp).cast('timestamp')})
    code_lookup = _ma_persist(_ma_prepare_code_lookup(snapshots[config.code_value_table]))
    frame = _ma_add_code_descriptions(frame, code_lookup)
    frame = add_standardized_columns(frame, config=config)
    rxnconso = snapshots[config.rxnconso_table]
    rxnorm_description = _ma_preferred_rxnorm_description(rxnconso)
    frame = frame.join(rxnorm_description, 'RXNORM_CUI', 'left')
    concepts = _ma_persist(_ma_prepare_concepts(snapshots[config.concept_table]))
    relationships = snapshots[config.concept_relationship_table]
    frame = _ma_apply_omop_mapping(frame, concepts, relationships, config)
    frame = _ma_apply_snomed_mapping(frame, concepts, relationships, rxnconso)
    frame = _ma_apply_legacy_concept_columns(frame, concepts, relationships, rxnconso, config)
    if full_rebuild:
        frame = frame.withColumn('TRIGGER_SOURCES', F.lit('FULL_REBUILD'))
    elif trigger_summary is not None:
        frame = frame.join(trigger_summary, 'EVENT_ID', 'left')
    else:
        frame = frame.withColumn('TRIGGER_SOURCES', F.lit(None).cast('string'))
    missing_columns = [field.name for field in schema_map_med_admin.fields if field.name != 'ROW_HASH' and field.name not in frame.columns]
    if missing_columns:
        raise RuntimeError('map_med_admin transformation did not produce required columns: ' + ', '.join(missing_columns))
    frame = _ma_hash_columns(frame, schema_map_med_admin, excluded=('ROW_HASH', 'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM', 'TRIGGER_SOURCES', 'SOURCE_VERSIONS_JSON'))
    return _ma_schema_select(frame, schema_map_med_admin)

def augment_snomed_codes(med_df: DataFrame, refs: Optional[object]=None) -> DataFrame:
    """
    Compatibility function. Unsafe exact/first-token term mapping has been removed.

    The production replacement entry point performs SNOMED resolution after validated
    OMOP mapping. If the frame is already resolved this function is an idempotent no-op.
    """
    return med_df

def add_omop_mappings(med_df: DataFrame, config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> DataFrame:
    """Standalone compatibility mapper using current OMOP snapshots and corrected rules."""
    concepts = _ma_prepare_concepts(spark.table(config.concept_table))
    return _ma_apply_omop_mapping(med_df, concepts, spark.table(config.concept_relationship_table), config)

def backfill_snomed_from_omop(med_df: DataFrame, config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> DataFrame:
    """Standalone compatibility mapper using valid `Mapped from` relationships."""
    concepts = _ma_prepare_concepts(spark.table(config.concept_table))
    return _ma_apply_snomed_mapping(med_df, concepts, spark.table(config.concept_relationship_table), spark.table(config.rxnconso_table))

class MedAdminReferenceTables:
    """Compatibility holder exposing corrected current reference snapshots."""

    def __init__(self, config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> None:
        self.config = config
        self.concepts = spark.table(config.concept_table)
        self.concept_relationships = spark.table(config.concept_relationship_table)
        self.rxnconso = spark.table(config.rxnconso_table)
        self.medication_lookup = spark.table(config.medication_lookup_table)

def _ma_trigger_rows(frame: DataFrame, source: str) -> DataFrame:
    return frame.select(F.col('EVENT_ID').cast('long').alias('EVENT_ID')).where(F.col('EVENT_ID').isNotNull()).withColumn('_TRIGGER_SOURCE', F.lit(source))

def _ma_aggregate_triggers(trigger_rows: DataFrame) -> DataFrame:
    return trigger_rows.groupBy('EVENT_ID').agg(F.concat_ws(',', F.sort_array(F.collect_set('_TRIGGER_SOURCE'))).alias('TRIGGER_SOURCES'))

def _ma_changed_cdf(table_name: str, state: Dict[str, int], source_versions: Dict[str, int]) -> Optional[DataFrame]:
    return _ma_read_cdf(table_name, int(state[table_name]) + 1, int(source_versions[table_name]))

def _ma_events_matching_any(target_frame: DataFrame, keys: DataFrame, pairs: Sequence[Tuple[str, str]]) -> DataFrame:
    """Return target EVENT_IDs matching any one of several key equalities.

    A join condition that is a disjunction of equalities has no equi-join key, so Spark
    plans it as a nested loop over the whole target. Running one hash join per disjunct
    and unioning the results is equivalent and linear in the size of the change set.
    """
    matched = [target_frame.alias('t').join(keys.alias('k'), F.col('t.' + left) == F.col('k.' + right), 'left_semi').select('EVENT_ID') for left, right in pairs]
    return reduce(lambda left_frame, right_frame: left_frame.unionByName(right_frame), matched).dropDuplicates(['EVENT_ID'])

def _ma_incremental_trigger_rows(state: Dict[str, int], source_versions: Dict[str, int], config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> Optional[DataFrame]:
    target = spark.table(config.target_table)
    frames: List[Optional[DataFrame]] = []
    for table_name, source_name in ((config.clinical_event_table, 'CLINICAL_EVENT'), (config.med_admin_event_table, 'MED_ADMIN_EVENT'), (config.med_result_table, 'CE_MED_RESULT')):
        cdf = _ma_changed_cdf(table_name, state, source_versions)
        if cdf is not None:
            frames.append(_ma_trigger_rows(cdf, source_name))
    orders_cdf = _ma_changed_cdf(config.orders_table, state, source_versions)
    if orders_cdf is not None:
        changed_orders = orders_cdf.select(F.col('ORDER_ID').cast('long').alias('_CHANGED_ORDER_ID')).where(F.col('_CHANGED_ORDER_ID').isNotNull()).dropDuplicates(['_CHANGED_ORDER_ID'])
        order_events = target.select('EVENT_ID', 'ORDER_ID').join(changed_orders, F.col('ORDER_ID') == F.col('_CHANGED_ORDER_ID'), 'inner')
        frames.append(_ma_trigger_rows(order_events, 'ORDERS'))
    ingredient_cdf = _ma_changed_cdf(config.order_ingredient_table, state, source_versions)
    if ingredient_cdf is not None:
        changed_ingredient_orders = ingredient_cdf.select(F.col('ORDER_ID').cast('long').alias('_CHANGED_INGREDIENT_ORDER_ID')).where(F.col('_CHANGED_INGREDIENT_ORDER_ID').isNotNull()).dropDuplicates(['_CHANGED_INGREDIENT_ORDER_ID'])
        ingredient_events = _ma_events_matching_any(target.select('EVENT_ID', 'ORDER_ID', 'TEMPLATE_ORDER_ID', 'INGREDIENT_ORDER_ID'), changed_ingredient_orders, (('ORDER_ID', '_CHANGED_INGREDIENT_ORDER_ID'), ('TEMPLATE_ORDER_ID', '_CHANGED_INGREDIENT_ORDER_ID'), ('INGREDIENT_ORDER_ID', '_CHANGED_INGREDIENT_ORDER_ID')))
        frames.append(_ma_trigger_rows(ingredient_events, 'ORDER_INGREDIENT'))
    for table_name, source_name in ((config.order_synonym_table, 'ORDER_CATALOG_SYNONYM'), (config.medication_lookup_table, 'MAP_MED_LOOKUP')):
        cdf = _ma_changed_cdf(table_name, state, source_versions)
        if cdf is not None:
            changed_synonyms = cdf.select(F.col('SYNONYM_ID').cast('long').alias('_CHANGED_SYNONYM_ID')).where(F.col('_CHANGED_SYNONYM_ID').isNotNull()).dropDuplicates(['_CHANGED_SYNONYM_ID'])
            affected = target.select('EVENT_ID', 'ORDER_SYNONYM_ID').join(changed_synonyms, F.col('ORDER_SYNONYM_ID') == F.col('_CHANGED_SYNONYM_ID'), 'inner')
            frames.append(_ma_trigger_rows(affected, source_name))
    code_cdf = _ma_changed_cdf(config.code_value_table, state, source_versions)
    if code_cdf is not None and (not _ma_is_empty(code_cdf)):
        changed_codes = code_cdf.select(F.col('CODE_VALUE').cast('long').alias('_CHANGED_CODE_VALUE')).where(F.col('_CHANGED_CODE_VALUE').isNotNull()).dropDuplicates(['_CHANGED_CODE_VALUE'])
        available_code_columns = [column for column in _MA_CODE_DISPLAY_COLUMNS if column in target.columns]
        if available_code_columns:
            affected = _ma_events_matching_any(target.select('EVENT_ID', *available_code_columns), changed_codes, [(column, '_CHANGED_CODE_VALUE') for column in available_code_columns])
            frames.append(_ma_trigger_rows(affected, 'CODE_VALUE'))
    rxn_cdf = _ma_changed_cdf(config.rxnconso_table, state, source_versions)
    if rxn_cdf is not None and (not _ma_is_empty(rxn_cdf)):
        changed_rx = rxn_cdf.select(F.col('RXCUI').cast('string').alias('_CHANGED_RXCUI'), F.col('CODE').cast('string').alias('_CHANGED_RX_CODE')).dropDuplicates(['_CHANGED_RXCUI', '_CHANGED_RX_CODE'])
        affected = _ma_events_matching_any(target.select('EVENT_ID', 'RXNORM_CUI', 'SNOMED_CODE'), changed_rx, (('RXNORM_CUI', '_CHANGED_RXCUI'), ('SNOMED_CODE', '_CHANGED_RX_CODE')))
        frames.append(_ma_trigger_rows(affected, 'RXNCONSO'))
    concept_cdf = _ma_changed_cdf(config.concept_table, state, source_versions)
    if concept_cdf is not None and (not _ma_is_empty(concept_cdf)):
        changed_concepts = concept_cdf.select(F.col('concept_id').cast('long').alias('_CHANGED_CONCEPT_ID')).where(F.col('_CHANGED_CONCEPT_ID').isNotNull()).dropDuplicates(['_CHANGED_CONCEPT_ID'])
        affected = _ma_events_matching_any(target.select('EVENT_ID', 'LOOKUP_OMOP_CONCEPT_ID', 'OMOP_SOURCE_CONCEPT_ID', 'OMOP_CONCEPT_ID', 'SNOMED_CONCEPT_ID'), changed_concepts, (('LOOKUP_OMOP_CONCEPT_ID', '_CHANGED_CONCEPT_ID'), ('OMOP_SOURCE_CONCEPT_ID', '_CHANGED_CONCEPT_ID'), ('OMOP_CONCEPT_ID', '_CHANGED_CONCEPT_ID'), ('SNOMED_CONCEPT_ID', '_CHANGED_CONCEPT_ID')))
        frames.append(_ma_trigger_rows(affected, 'OMOP_CONCEPT'))
    relationship_cdf = _ma_changed_cdf(config.concept_relationship_table, state, source_versions)
    if relationship_cdf is not None and (not _ma_is_empty(relationship_cdf)):
        changed_relationship_concepts = relationship_cdf.select(F.col('concept_id_1').cast('long').alias('_CHANGED_CONCEPT_ID')).unionByName(relationship_cdf.select(F.col('concept_id_2').cast('long').alias('_CHANGED_CONCEPT_ID'))).where(F.col('_CHANGED_CONCEPT_ID').isNotNull()).dropDuplicates(['_CHANGED_CONCEPT_ID'])
        affected = _ma_events_matching_any(target.select('EVENT_ID', 'OMOP_SOURCE_CONCEPT_ID', 'OMOP_CONCEPT_ID', 'SNOMED_CONCEPT_ID'), changed_relationship_concepts, (('OMOP_SOURCE_CONCEPT_ID', '_CHANGED_CONCEPT_ID'), ('OMOP_CONCEPT_ID', '_CHANGED_CONCEPT_ID'), ('SNOMED_CONCEPT_ID', '_CHANGED_CONCEPT_ID')))
        frames.append(_ma_trigger_rows(affected, 'OMOP_CONCEPT_RELATIONSHIP'))
    combined = _ma_union_frames(frames)
    if combined is None:
        return None
    return combined.dropDuplicates(['EVENT_ID', '_TRIGGER_SOURCE'])

def _ma_snapshot_dict(source_versions: Dict[str, int], config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> Dict[str, DataFrame]:
    return {table_name: _ma_read_snapshot(table_name, source_versions[table_name]) for table_name in config.tracked_tables}

def create_base_medication_administrations_incr(force_full_rebuild: bool=False, run_id: Optional[str]=None, run_timestamp: Optional[datetime]=None, config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> DataFrame:
    """
    Build a delete-aware map_med_admin change set without writing production.

    The returned DataFrame contains the target schema plus `_source_change_type`.
    Use process_med_admin_incremental for normal deployment.
    """
    global _PENDING_MED_ADMIN_PLAN
    _ma_release_caches()
    _PENDING_MED_ADMIN_PLAN = {}
    _ma_ensure_control_tables(config)
    run_id = run_id or str(uuid.uuid4())
    run_timestamp = run_timestamp or datetime.now(timezone.utc)
    source_versions = _ma_capture_source_versions(config)
    state = _ma_read_pipeline_state(config)
    automatic_full, reason = _ma_target_requires_full_rebuild(state, config)
    full_rebuild = bool(force_full_rebuild or automatic_full)
    mode = 'FULL_REBUILD' if full_rebuild else 'INCREMENTAL'
    print(f"[INFO] map_med_admin mode={mode}; reason={('explicit request' if force_full_rebuild else reason)}")
    print(f'[INFO] Captured source versions: {source_versions}')
    snapshots = _ma_snapshot_dict(source_versions, config)
    trigger_rows: Optional[DataFrame] = None
    trigger_summary: Optional[DataFrame] = None
    event_keys: Optional[DataFrame] = None
    if not full_rebuild:
        trigger_rows = _ma_incremental_trigger_rows(state, source_versions, config)
        if trigger_rows is None or _ma_is_empty(trigger_rows):
            empty_child = spark.createDataFrame([], schema_map_med_admin_ingredient)
            empty_keys = spark.createDataFrame([], T.StructType([T.StructField('EVENT_ID', T.LongType(), False)]))
            _PENDING_MED_ADMIN_PLAN = {'source_versions': source_versions, 'full_rebuild': False, 'mode': mode, 'run_id': run_id, 'run_timestamp': run_timestamp, 'child_rows': empty_child, 'affected_event_keys': empty_keys, 'config': config}
            return _ma_empty_change_set()
        trigger_rows = _ma_persist(trigger_rows)
        trigger_summary = _ma_aggregate_triggers(trigger_rows)
        event_keys = _ma_persist(trigger_rows.select('EVENT_ID').dropDuplicates(['EVENT_ID']))
    base, child_rows = _ma_build_joined_source_rows(snapshots=snapshots, event_keys=event_keys, run_id=run_id, run_timestamp=run_timestamp, source_versions=source_versions, config=config)
    target_rows = _ma_finalize_target_rows(base=base, snapshots=snapshots, trigger_summary=trigger_summary, full_rebuild=full_rebuild, run_id=run_id, run_timestamp=run_timestamp, source_versions=source_versions, config=config)
    if full_rebuild:
        changes = target_rows.withColumn('_source_change_type', F.lit('upsert'))
        affected_event_keys = target_rows.select('EVENT_ID')
    else:
        assert event_keys is not None
        upserts = target_rows.withColumn('_source_change_type', F.lit('upsert'))
        delete_keys = event_keys.join(target_rows.select('EVENT_ID'), 'EVENT_ID', 'left_anti')
        deletes = _ma_schema_select(delete_keys, schema_map_med_admin).withColumn('_source_change_type', F.lit('delete'))
        changes = upserts.unionByName(deletes)
        affected_event_keys = event_keys
    _PENDING_MED_ADMIN_PLAN = {'source_versions': source_versions, 'full_rebuild': full_rebuild, 'mode': mode, 'run_id': run_id, 'run_timestamp': run_timestamp, 'child_rows': child_rows, 'affected_event_keys': affected_event_keys, 'config': config}
    return changes

def _ma_overwrite_target(upserts: DataFrame, config: MapMedAdminConfig) -> None:
    bronze_project_contract(
        upserts, config.target_table
    ).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('delta.enableChangeDataFeed', 'true').option('delta.enableDeletionVectors', 'true').option('delta.enableRowTracking', 'true').option('delta.autoOptimize.optimizeWrite', 'true').option('delta.autoOptimize.autoCompact', 'true').saveAsTable(config.target_table)

def _ma_merge_target(changes: DataFrame, config: MapMedAdminConfig) -> None:
    target = DeltaTable.forName(spark, config.target_table)
    delete_keys = (
        changes.filter(F.col('_source_change_type') == 'delete')
        .select('EVENT_ID')
        .dropDuplicates(['EVENT_ID'])
    )
    upserts = bronze_project_contract(
        changes.filter(F.col('_source_change_type') == 'upsert')
        .drop('_source_change_type'),
        config.target_table,
    )
    if not _ma_is_empty(delete_keys):
        target.alias('t').merge(
            delete_keys.alias('s'), 't.EVENT_ID = s.EVENT_ID'
        ).whenMatchedDelete().execute()
    if not _ma_is_empty(upserts):
        assignments = {
            column_name: F.col(f's.{column_name}')
            for column_name in upserts.columns
        }
        comparisons = ' OR '.join(
            f'NOT (t.`{column_name}` <=> s.`{column_name}`)'
            for column_name in upserts.columns
            if column_name != 'EVENT_ID'
        )
        target.alias('t').merge(
            upserts.alias('s'), 't.EVENT_ID = s.EVENT_ID'
        ).whenMatchedUpdate(
            condition=comparisons or 'false',
            set=assignments,
        ).whenNotMatchedInsert(values=assignments).execute()

def _ma_overwrite_ingredient_target(child_rows: DataFrame, config: MapMedAdminConfig) -> None:
    _ma_schema_select(child_rows, schema_map_med_admin_ingredient).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('delta.enableChangeDataFeed', 'true').option('delta.enableDeletionVectors', 'true').option('delta.enableRowTracking', 'true').option('delta.autoOptimize.optimizeWrite', 'true').option('delta.autoOptimize.autoCompact', 'true').saveAsTable(config.ingredient_target_table)

def _ma_refresh_ingredient_events(child_rows: DataFrame, affected_event_keys: DataFrame, config: MapMedAdminConfig) -> None:
    if not _ma_table_exists(config.ingredient_target_table):
        _ma_overwrite_ingredient_target(child_rows, config)
        return
    keys = affected_event_keys.select('EVENT_ID').dropDuplicates(['EVENT_ID'])
    if not _ma_is_empty(keys):
        DeltaTable.forName(spark, config.ingredient_target_table).alias('t').merge(keys.alias('s'), 't.EVENT_ID = s.EVENT_ID').whenMatchedDelete().execute()
    if not _ma_is_empty(child_rows):
        _ma_schema_select(child_rows, schema_map_med_admin_ingredient).write.format('delta').mode('append').saveAsTable(config.ingredient_target_table)

def _ma_measure_and_validate_change_set(changes: DataFrame, full_rebuild: bool) -> Dict[str, int]:
    """Validate action/key grain and collect counts in one shuffled pass."""
    action = F.col('_source_change_type')
    per_key = changes.groupBy('EVENT_ID').agg(F.sum(F.when(action == 'upsert', 1).otherwise(0)).cast('long').alias('upsert_rows'), F.sum(F.when(action == 'delete', 1).otherwise(0)).cast('long').alias('delete_rows'), F.sum(F.when(~F.coalesce(action.isin('upsert', 'delete'), F.lit(False)), 1).otherwise(0)).cast('long').alias('invalid_rows'))
    row = per_key.agg(F.coalesce(F.sum('upsert_rows'), F.lit(0)).cast('long').alias('upsert_rows'), F.coalesce(F.sum('delete_rows'), F.lit(0)).cast('long').alias('delete_rows'), F.coalesce(F.sum('invalid_rows'), F.lit(0)).cast('long').alias('invalid_rows'), F.sum(F.when(F.col('EVENT_ID').isNull(), F.col('upsert_rows')).otherwise(0)).cast('long').alias('null_upsert_rows'), F.sum(F.when(F.col('EVENT_ID').isNull(), F.col('delete_rows')).otherwise(0)).cast('long').alias('null_delete_rows'), F.sum(F.when(F.col('upsert_rows') > 1, 1).otherwise(0)).cast('long').alias('duplicate_upsert_keys'), F.sum(F.when(F.col('delete_rows') > 1, 1).otherwise(0)).cast('long').alias('duplicate_delete_keys'), F.sum(F.when((F.col('upsert_rows') > 0) & (F.col('delete_rows') > 0), 1).otherwise(0)).cast('long').alias('conflicting_action_keys')).first()
    metrics = {name: int(row[name] or 0) for name in row.asDict()}
    if metrics['invalid_rows']:
        raise ValueError(f"Unexpected _source_change_type in map_med_admin changes: {metrics['invalid_rows']:,} rows")
    key_failures = {name: metrics[name] for name in ('null_upsert_rows', 'null_delete_rows', 'duplicate_upsert_keys', 'duplicate_delete_keys', 'conflicting_action_keys') if metrics[name]}
    if key_failures:
        raise ValueError(f'map_med_admin change set violates EVENT_ID/action grain: {key_failures}')
    if full_rebuild and metrics['delete_rows']:
        raise ValueError('Full rebuild unexpectedly contains deletes')
    return metrics

def update_map_med_admin_table(source_df: DataFrame) -> Dict[str, object]:
    global _PENDING_MED_ADMIN_PLAN
    if not _PENDING_MED_ADMIN_PLAN:
        raise RuntimeError('No pending map_med_admin plan. Build changes with create_base_medication_administrations_incr first.')
    plan = dict(_PENDING_MED_ADMIN_PLAN)
    config = plan['config']
    source_versions = dict(plan['source_versions'])
    full_rebuild = bool(plan['full_rebuild'])
    mode = str(plan['mode'])
    run_id = str(plan['run_id'])
    child_rows = plan['child_rows']
    affected_event_keys = plan['affected_event_keys']
    changes = source_df
    stage_table: Optional[str] = None
    metrics: Dict[str, object] = {}
    try:
        stage_table = _ma_changes_stage_name(config.target_table, run_id)
        changes = _ma_materialize_changes_stage(changes, stage_table)
        change_metrics = _ma_measure_and_validate_change_set(changes, full_rebuild)
        metrics.update(change_metrics)
        metrics['full_rebuild'] = full_rebuild
        if full_rebuild:
            if metrics['upsert_rows'] == 0:
                raise RuntimeError('Refusing to replace map_med_admin with an empty full rebuild')
            upserts = changes.where(F.col('_source_change_type') == 'upsert').drop('_source_change_type')
            _ma_overwrite_target(upserts, config)
            if config.maintain_ingredient_child:
                _ma_overwrite_ingredient_target(child_rows, config)
        elif metrics['upsert_rows'] or metrics['delete_rows']:
            _ma_merge_target(changes, config)
            if config.maintain_ingredient_child:
                _ma_refresh_ingredient_events(child_rows, affected_event_keys, config)
        else:
            print('[INFO] No material map_med_admin changes were found')
        _ma_set_target_metadata(full_rebuild, config)
        completed_at = datetime.now(timezone.utc)
        _ma_commit_pipeline_state(source_versions, run_id, completed_at, config)
        _ma_write_audit_event(run_id, 'SUCCESS', mode, 'map_med_admin update completed', source_versions, metrics, config)
        print(f"[SUCCESS] map_med_admin completed: {metrics['upsert_rows']:,} upserts, {metrics['delete_rows']:,} deletes, full_rebuild={full_rebuild}")
        return metrics
    except Exception as exc:
        _ma_write_audit_event(run_id, 'FAILED', mode, str(exc), source_versions, metrics, config)
        raise
    finally:
        try:
            _ma_drop_stage(stage_table)
        except Exception as cleanup_exc:
            print(f'[WARN] map_med_admin stage cleanup failed; the next pipeline startup sweep will retry: {str(cleanup_exc)[:500]}')
        _ma_release_caches()
        _PENDING_MED_ADMIN_PLAN = {}

def run_map_med_admin_post_deployment_checks(config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> Dict[str, int]:
    """Full-table checks over retained single-layer columns."""
    target = spark.table(config.target_table)
    metrics_row = target.agg(
        F.count(F.lit(1)).cast('long').alias('target_rows'),
        F.countDistinct('EVENT_ID').cast('long').alias('distinct_event_ids'),
        F.sum(F.col('EVENT_ID').isNull().cast('long')).cast('long').alias('null_event_ids'),
        F.sum(
            (
                (F.col('DOSE_UNIT_NORMALIZED') == 'mg')
                & F.col('DOSE_VALUE_EFFECTIVE').isNotNull()
                & (
                    F.abs(F.col('DOSE_IN_MG') - F.col('DOSE_VALUE_EFFECTIVE'))
                    > F.greatest(
                        F.lit(1e-09),
                        F.abs(F.col('DOSE_VALUE_EFFECTIVE')) * F.lit(1e-09),
                    )
                )
            ).cast('long')
        ).cast('long').alias('incorrect_exact_mg_rows'),
        F.sum(
            (
                F.col('OMOP_CONCEPT_ID').isNotNull()
                & (F.col('OMOP_STANDARD_CONCEPT') != 'S')
            ).cast('long')
        ).cast('long').alias('omop_not_marked_standard'),
        F.sum(
            F.coalesce(
                F.col('OMOP_MAPPING_METHOD').contains('SIMPLIFIED'),
                F.lit(False),
            ).cast('long')
        ).cast('long').alias('simplified_mapping_rows'),
        F.sum(
            (
                F.col('SNOMED_CODE').isNotNull()
                & F.col('SNOMED_STR').isNull()
            ).cast('long')
        ).cast('long').alias('snomed_without_description'),
        F.sum(
            (
                F.col('ORDER_CKI').isNotNull()
                & (F.col('ORDER_CKI') == F.col('MULTUM'))
                & F.coalesce(
                    (F.col('ORDERS_CKI') != F.col('MULTUM'))
                    & (F.col('SYNONYM_CKI') != F.col('MULTUM')),
                    F.lit(True),
                )
            ).cast('long')
        ).cast('long').alias('order_cki_is_multum_without_source_support'),
    ).first()
    metrics = {name: int(metrics_row[name] or 0) for name in metrics_row.asDict()}
    invalid_omop = (
        target.select('OMOP_CONCEPT_ID')
        .where(F.col('OMOP_CONCEPT_ID').isNotNull())
        .dropDuplicates(['OMOP_CONCEPT_ID'])
        .join(
            spark.table(config.concept_table).select(
                F.col('concept_id').cast('long').alias('OMOP_CONCEPT_ID'),
                'standard_concept', 'domain_id', 'invalid_reason',
            ),
            'OMOP_CONCEPT_ID',
            'left',
        )
        .where(
            F.col('standard_concept').isNull()
            | (F.col('standard_concept') != 'S')
            | ~F.col('domain_id').isin('Drug', 'Ingredient')
            | F.col('invalid_reason').isNotNull()
        )
        .count()
    )
    metrics['invalid_or_nonstandard_distinct_omop_ids'] = int(invalid_omop)
    if config.maintain_ingredient_child:
        child = spark.table(config.ingredient_target_table)
        metrics['duplicate_ingredient_identity_groups'] = int(
            child.groupBy(
                'EVENT_ID', 'INGREDIENT_ORDER_ID', 'ACTION_SEQUENCE',
                'COMP_SEQUENCE', 'SYNONYM_ID', 'ADC_UPDT',
            ).count().where(F.col('count') > 1).count()
        )
    failure_keys = {
        'null_event_ids', 'incorrect_exact_mg_rows',
        'omop_not_marked_standard', 'simplified_mapping_rows',
        'order_cki_is_multum_without_source_support',
        'invalid_or_nonstandard_distinct_omop_ids',
        'duplicate_ingredient_identity_groups',
    }
    failures = {
        key: value for key, value in metrics.items()
        if key in failure_keys and value != 0
    }
    if metrics['target_rows'] != metrics['distinct_event_ids']:
        failures['duplicate_event_rows'] = (
            metrics['target_rows'] - metrics['distinct_event_ids']
        )
    if failures:
        raise AssertionError(f'map_med_admin post-deployment checks failed: {failures}')
    print(f'[OK] map_med_admin post-deployment checks passed: {metrics}')
    return metrics
def run_map_med_admin_update(force_full_rebuild: bool=False, run_post_checks: bool=False, config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> Dict[str, object]:
    """Production entry point for the replacement medication-administration pipeline."""
    run_id = str(uuid.uuid4())
    run_timestamp = datetime.now(timezone.utc)
    try:
        changes = create_base_medication_administrations_incr(force_full_rebuild=force_full_rebuild, run_id=run_id, run_timestamp=run_timestamp, config=config)
        metrics = update_map_med_admin_table(changes)
        if run_post_checks:
            metrics['post_deployment_checks'] = run_map_med_admin_post_deployment_checks(config)
        return metrics
    except Exception as exc:
        if not _PENDING_MED_ADMIN_PLAN:
            try:
                _ma_ensure_control_tables(config)
                _ma_write_audit_event(run_id, 'FAILED', 'FULL_REBUILD' if force_full_rebuild else 'AUTO', str(exc), None, None, config)
            except Exception:
                pass
        raise

def process_med_admin_incremental(force_full_rebuild: bool=False, run_post_checks: bool=False, config: MapMedAdminConfig=MAP_MED_ADMIN_CONFIG) -> Dict[str, object]:
    """Drop-in replacement for the existing no-argument Map Pipeline call."""
    return run_map_med_admin_update(force_full_rebuild=force_full_rebuild, run_post_checks=run_post_checks, config=config)

try:
    _pipeline_run_recoverable('map_med_admin', _PIPELINE_FULL_REFRESH, lambda: run_map_med_admin_update(force_full_rebuild=False, run_post_checks=False), lambda: run_map_med_admin_update(force_full_rebuild=True, run_post_checks=True))
    _PIPELINE_UPDATED_TARGETS.extend(['4_prod.bronze.map_med_admin'])
    _pipeline_mark_component_complete('map_med_admin', ['4_prod.bronze.map_med_admin'])
    _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_med_admin'})
except Exception as exc:
    _pipeline_record_error(None, exc)
    raise
finally:
    if _pipeline_shared_update_table is not None:
        update_table = _pipeline_shared_update_table
    if _pipeline_shared_table_exists is not None:
        table_exists = _pipeline_shared_table_exists
    if _pipeline_shared_get_max_timestamp is not None:
        get_max_timestamp = _pipeline_shared_get_max_timestamp
    if _pipeline_shared_has_cdf_enabled is not None:
        has_cdf_enabled = _pipeline_shared_has_cdf_enabled
    if _pipeline_shared_get_incremental is not None:
        get_incremental_data_with_cdf = _pipeline_shared_get_incremental

# COMMAND ----------

_pipeline_component_start("map_procedure")
_pipeline_shared_update_table = globals().get('update_table')
_pipeline_shared_table_exists = globals().get('table_exists')
_pipeline_shared_get_max_timestamp = globals().get('get_max_timestamp')
_pipeline_shared_has_cdf_enabled = globals().get('has_cdf_enabled')
_pipeline_shared_get_incremental = globals().get('get_incremental_data_with_cdf')
globals().pop('_map_updates_original_update_table', None)
globals().pop('_map_medical_personnel_original_update_table', None)
globals().pop('_MAP_UPDATES_ORIGINAL_UPDATE_TABLE', None)

from datetime import datetime
from functools import reduce
from typing import Dict, List, Optional
from uuid import uuid4
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType, TimestampType
MAP_PROCEDURE_PIPELINE = 'map_procedure_v2'
MAP_PROCEDURE_TARGET = '4_prod.bronze.map_procedure'
MAP_PROCEDURE_SOURCE = '4_prod.raw.mill_procedure'
MAP_PROCEDURE_NOMENCLATURE = '3_lookup.mill.map_nomenclature'
MAP_PROCEDURE_ENCOUNTER = '4_prod.bronze.map_encounter'
MAP_PROCEDURE_CODE_VALUE = '3_lookup.mill.mill_code_value'
MAP_PROCEDURE_CHECKPOINT = '6_mgmt.bronze.map_procedure_pipeline_state'
MAP_PROCEDURE_TRUST: Optional[str] = 'Barts'
MAP_PROCEDURE_SCHEMA_VERSION = '2.0.0'
MAP_PROCEDURE_CDF_RETENTION = 'interval 30 days'
MAP_PROCEDURE_SOURCES = {'procedure': MAP_PROCEDURE_SOURCE, 'nomenclature': MAP_PROCEDURE_NOMENCLATURE, 'encounter': MAP_PROCEDURE_ENCOUNTER, 'code_value': MAP_PROCEDURE_CODE_VALUE}
_MAP_PROCEDURE_ACTIVE_SOURCE_VERSIONS: Dict[str, int] = {}
map_procedure_comment = 'One row per current active, dated source procedure for the configured trust. Curated scope: only rows with ACTIVE_IND = 1 and a non-null PROC_DT_TM are published here, matching the scope of the pre-replacement legacy pipeline; inactive and undated procedure rows are excluded from this table and are retained in 4_prod.raw.mill_procedure, which remains the retention layer. Within that scope the table preserves raw procedure lifecycle, provenance, partial-date and free-text information without further clinical-value filtering; enriches with current encounter person, nomenclature, OMOP, SNOMED, ICD-10, OPCS-4 and code-value descriptions. Source-specific Delta commit checkpoints drive incremental refreshes, including joined-source changes.'

def _mp_sf(name, data_type, comment):
    return StructField(name, data_type, True, metadata={'comment': comment})
schema_map_procedure_v2 = StructType([_mp_sf('PROCEDURE_ID', LongType(), 'Primary key from the source procedure table.'), _mp_sf('ENCNTR_ID', LongType(), 'Source encounter identifier.'), _mp_sf('PERSON_ID', LongType(), 'Current person identifier from the encounter source.'), _mp_sf('NOMENCLATURE_ID', LongType(), 'Source procedure nomenclature identifier.'), _mp_sf('ENCNTR_SLICE_ID', LongType(), 'Source encounter-slice identifier.'), _mp_sf('ORGANIZATION_ID', LongType(), 'Source organization associated with the procedure.'), _mp_sf('PARENT_ENTITY_ID', LongType(), 'Source parent-entity identifier.'), _mp_sf('PARENT_ENTITY_NAME', StringType(), 'Source parent-entity table name.'), _mp_sf('SVC_CAT_HIST_ID', LongType(), 'Source service-category history identifier.'), _mp_sf('UPDT_CNT', LongType(), 'Source update counter.'), _mp_sf('UPDT_DT_TM', TimestampType(), 'Source application update datetime.'), _mp_sf('UPDT_ID', LongType(), 'Personnel identifier responsible for the source update.'), _mp_sf('UPDT_TASK', LongType(), 'Source update task identifier.'), _mp_sf('UPDT_APPLCTX', LongType(), 'Source application-context identifier.'), _mp_sf('LAST_UTC_TS', TimestampType(), 'Source last UTC timestamp.'), _mp_sf('TXN_ID_TEXT', StringType(), 'Source transaction identifier text.'), _mp_sf('TRUST', StringType(), 'Source Trust retained for provenance and scope auditing.'), _mp_sf('ACTIVE_IND', LongType(), 'Source active indicator; inactive rows are retained.'), _mp_sf('ACTIVE_STATUS_CD', LongType(), 'Source active-status code.'), _mp_sf('active_status_desc', StringType(), 'Current code-value display for ACTIVE_STATUS_CD.'), _mp_sf('ACTIVE_STATUS_DT_TM', TimestampType(), 'Datetime the source active status was set.'), _mp_sf('ACTIVE_STATUS_PRSNL_ID', LongType(), 'Personnel identifier that set the active status.'), _mp_sf('BEG_EFFECTIVE_DT_TM', TimestampType(), 'Source row beginning-effective datetime.'), _mp_sf('END_EFFECTIVE_DT_TM', TimestampType(), 'Source row ending-effective datetime.'), _mp_sf('CONTRIBUTOR_SYSTEM_CD', LongType(), 'Source contributor-system code.'), _mp_sf('contributor_system_desc', StringType(), 'Current code-value display for CONTRIBUTOR_SYSTEM_CD.'), _mp_sf('CLINICAL_SERVICE_CD', LongType(), 'Source clinical-service code.'), _mp_sf('clinical_service_desc', StringType(), 'Current code-value display for CLINICAL_SERVICE_CD.'), _mp_sf('PROC_PRIORITY', LongType(), 'Source procedure priority.'), _mp_sf('PROC_FUNC_TYPE_CD', LongType(), 'Source procedure-function type code.'), _mp_sf('proc_func_type_desc', StringType(), 'Current code-value display for PROC_FUNC_TYPE_CD.'), _mp_sf('PROC_TYPE_FLAG', LongType(), 'Source procedure-type flag.'), _mp_sf('CATEGORY_CD', LongType(), 'Source procedure category code.'), _mp_sf('category_desc', StringType(), 'Current code-value display for CATEGORY_CD.'), _mp_sf('RANKING_CD', LongType(), 'Source procedure ranking code.'), _mp_sf('ranking_desc', StringType(), 'Current code-value display for RANKING_CD.'), _mp_sf('DGVP_IND', LongType(), 'Source dominant-group-variable procedure indicator.'), _mp_sf('SUPPRESS_NARRATIVE_IND', LongType(), 'Source narrative-suppression indicator.'), _mp_sf('PROC_DT_TM', TimestampType(), 'Unmodified source procedure datetime.'), _mp_sf('PROC_START_DT_TM', TimestampType(), 'Unmodified source procedure start datetime.'), _mp_sf('PROC_END_DT_TM', TimestampType(), 'Unmodified source procedure end datetime.'), _mp_sf('PROC_DT_TM_PREC_FLAG', LongType(), 'Source procedure datetime precision flag.'), _mp_sf('PROC_DT_TM_PREC_CD', LongType(), 'Source procedure datetime precision code.'), _mp_sf('proc_dt_tm_precision_desc', StringType(), 'Current code-value display for PROC_DT_TM_PREC_CD.'), _mp_sf('PROC_FT_DT_TM_IND', LongType(), 'Source free-text datetime indicator.'), _mp_sf('PROC_FT_TIME_FRAME', StringType(), 'Source free-text procedure timeframe.'), _mp_sf('PROCEDURE_DT_TM_EFFECTIVE', TimestampType(), 'Derived datetime from PROC_DT_TM, then start, then end; raw fields remain unchanged.'), _mp_sf('PROCEDURE_DT_TM_SOURCE', StringType(), 'Column used for PROCEDURE_DT_TM_EFFECTIVE.'), _mp_sf('PROC_DT_TM_MISSING_IND', BooleanType(), 'True when raw PROC_DT_TM is null; no row is filtered.'), _mp_sf('PROC_MINUTES', DoubleType(), 'Unmodified source procedure minutes, including zero.'), _mp_sf('PROC_MINUTES_SOURCE_ZERO_IND', BooleanType(), 'True when source PROC_MINUTES equals zero.'), _mp_sf('PROC_MINUTES_CALCULATED', DoubleType(), 'Duration calculated from valid start/end timestamps; does not replace source PROC_MINUTES.'), _mp_sf('ANESTHESIA_CD', LongType(), 'Source anesthesia code.'), _mp_sf('anesthesia_desc', StringType(), 'Current code-value display for ANESTHESIA_CD.'), _mp_sf('ANESTHESIA_MINUTES', DoubleType(), 'Unmodified source anesthesia minutes.'), _mp_sf('UNITS_OF_SERVICE', DoubleType(), 'Unmodified source units of service.'), _mp_sf('PROC_LOC_CD', LongType(), 'Source coded procedure location.'), _mp_sf('proc_location_desc', StringType(), 'Current code-value display for PROC_LOC_CD.'), _mp_sf('PROC_LOC_FT_IND', LongType(), 'Source free-text procedure-location indicator.'), _mp_sf('PROC_FT_LOC', StringType(), 'Source free-text procedure location.'), _mp_sf('LATERALITY_CD', LongType(), 'Source procedure laterality code.'), _mp_sf('laterality_desc', StringType(), 'Current code-value display for LATERALITY_CD.'), _mp_sf('TISSUE_TYPE_CD', LongType(), 'Source tissue-type code.'), _mp_sf('tissue_type_desc', StringType(), 'Current code-value display for TISSUE_TYPE_CD.'), _mp_sf('PROCEDURE_NOTE', StringType(), 'Unmodified source procedure note.'), _mp_sf('PROC_FTDESC', StringType(), 'Unmodified source free-text procedure description.'), _mp_sf('PROCEDURE_DISPLAY', StringType(), 'First nonblank value from nomenclature source string, PROC_FTDESC and PROCEDURE_NOTE.'), _mp_sf('PROCEDURE_DISPLAY_SOURCE', StringType(), 'Column used for PROCEDURE_DISPLAY.'), _mp_sf('COMMENT_IND', LongType(), 'Source indicator that procedure comment content exists.'), _mp_sf('LONG_TEXT_ID', LongType(), 'Source long-text identifier.'), _mp_sf('REFERENCE_NBR', StringType(), 'Source reference number; unique with contributor system where populated.'), _mp_sf('SEG_UNIQUE_KEY', StringType(), 'Source HL7 segment unique key.'), _mp_sf('CONSENT_CD', LongType(), 'Source consent code.'), _mp_sf('consent_desc', StringType(), 'Current code-value display for CONSENT_CD.'), _mp_sf('GENERIC_VAL_CD', LongType(), 'Source generic-value code.'), _mp_sf('generic_value_desc', StringType(), 'Current code-value display for GENERIC_VAL_CD.'), _mp_sf('DIAG_NOMENCLATURE_ID', LongType(), 'Source diagnosis nomenclature identifier.'), _mp_sf('MOD_NOMENCLATURE_ID', LongType(), 'Source modifier nomenclature identifier.'), _mp_sf('SOURCE_IDENTIFIER', StringType(), 'Current nomenclature source-vocabulary identifier.'), _mp_sf('SOURCE_STRING', StringType(), 'Current nomenclature source string.'), _mp_sf('SOURCE_VOCABULARY_CD', LongType(), 'Current nomenclature source-vocabulary code.'), _mp_sf('source_vocabulary_desc', StringType(), 'Current code-value display for SOURCE_VOCABULARY_CD.'), _mp_sf('VOCAB_AXIS_CD', LongType(), 'Current nomenclature vocabulary-axis code.'), _mp_sf('vocab_axis_desc', StringType(), 'Current code-value display for VOCAB_AXIS_CD.'), _mp_sf('CONCEPT_CKI', StringType(), 'Complete current nomenclature concept CKI.'), _mp_sf('CONCEPT_CKI_SOURCE', StringType(), 'Source component before the first exclamation mark in CONCEPT_CKI.'), _mp_sf('CONCEPT_CKI_IDENTIFIER', StringType(), 'Identifier component after the final exclamation mark in CONCEPT_CKI.'), _mp_sf('FOUND_CUI', StringType(), 'CUI found during nomenclature mapping.'), _mp_sf('NOMENCLATURE_IS_ACTIVE', BooleanType(), 'Current nomenclature active indicator; inactive mappings are retained.'), _mp_sf('NOMENCLATURE_SOURCE_CHANGE_TS', TimestampType(), 'True source-change timestamp from nomenclature.'), _mp_sf('NOMENCLATURE_MAPPING_HASH', StringType(), 'Stable hash of current nomenclature mapping content.'), _mp_sf('ENCOUNTER_MATCHED_IND', BooleanType(), 'True when ENCNTR_ID resolved in the encounter projection.'), _mp_sf('NOMENCLATURE_MATCHED_IND', BooleanType(), 'True when NOMENCLATURE_ID resolved in nomenclature.'), _mp_sf('OMOP_CONCEPT_ID', LongType(), 'Current mapped OMOP concept identifier.'), _mp_sf('OMOP_CONCEPT_NAME', StringType(), 'Current mapped OMOP concept name.'), _mp_sf('OMOP_STANDARD_CONCEPT', StringType(), 'Current OMOP standard-concept flag.'), _mp_sf('OMOP_MATCH_NUMBER', LongType(), 'Number of OMOP mapping matches.'), _mp_sf('OMOP_SIMILARITY', DoubleType(), 'Source-to-OMOP similarity score.'), _mp_sf('OMOP_CONCEPT_DOMAIN', StringType(), 'Current OMOP concept domain.'), _mp_sf('OMOP_CONCEPT_CLASS', StringType(), 'Current OMOP concept class.'), _mp_sf('SNOMED_CODE', LongType(), 'Current mapped SNOMED code.'), _mp_sf('SNOMED_TYPE', StringType(), 'Current SNOMED mapping method.'), _mp_sf('SNOMED_MATCH_NUMBER', LongType(), 'Number of SNOMED mapping matches.'), _mp_sf('SNOMED_SIMILARITY', DoubleType(), 'Source-to-SNOMED similarity score.'), _mp_sf('SNOMED_TERM', StringType(), 'Current mapped SNOMED term.'), _mp_sf('ICD10_CODE', StringType(), 'Current mapped ICD-10 code.'), _mp_sf('ICD10_TYPE', StringType(), 'Current ICD-10 mapping method.'), _mp_sf('ICD10_MATCH_NUMBER', LongType(), 'Number of ICD-10 mapping matches.'), _mp_sf('ICD10_SIMILARITY', DoubleType(), 'Source-to-ICD-10 similarity score.'), _mp_sf('ICD10_TERM', StringType(), 'Current mapped ICD-10 term.'), _mp_sf('OPCS4_CODE', StringType(), 'Current mapped OPCS-4 code.'), _mp_sf('OPCS4_TYPE', StringType(), 'Current OPCS-4 mapping method.'), _mp_sf('OPCS4_MATCH_NUMBER', LongType(), 'Number of OPCS-4 mapping matches.'), _mp_sf('OPCS4_SIMILARITY', DoubleType(), 'Source-to-OPCS-4 similarity score.'), _mp_sf('OPCS4_TERM', StringType(), 'Current mapped OPCS-4 term.'), _mp_sf('SIMILARITY_SOURCE_SNOMED', DoubleType(), 'Similarity between source string and SNOMED term.'), _mp_sf('SIMILARITY_SOURCE_ICD10', DoubleType(), 'Similarity between source string and ICD-10 term.'), _mp_sf('SIMILARITY_SOURCE_OPCS4', DoubleType(), 'Similarity between source string and OPCS-4 term.'), _mp_sf('SIMILARITY_SOURCE_OMOP', DoubleType(), 'Similarity between source string and OMOP term.'), _mp_sf('SIMILARITY_SNOMED_ICD10', DoubleType(), 'Similarity between SNOMED and ICD-10 terms.'), _mp_sf('SIMILARITY_SNOMED_OPCS4', DoubleType(), 'Similarity between SNOMED and OPCS-4 terms.'), _mp_sf('SIMILARITY_SNOMED_OMOP', DoubleType(), 'Similarity between SNOMED and OMOP terms.'), _mp_sf('SIMILARITY_ICD10_OMOP', DoubleType(), 'Similarity between ICD-10 and OMOP terms.'), _mp_sf('SIMILARITY_OPCS4_OMOP', DoubleType(), 'Similarity between OPCS-4 and OMOP terms.'), _mp_sf('CODE_VALUE_LOOKUP_HASH', StringType(), 'Stable hash of code-value descriptions used by the row.'), _mp_sf('PROCEDURE_ADC_UPDT', TimestampType(), 'ADC_UPDT from the procedure source only.'), _mp_sf('NOMENCLATURE_ADC_UPDT', TimestampType(), 'ADC_UPDT from the nomenclature source only.'), _mp_sf('ENCOUNTER_ADC_UPDT', TimestampType(), 'ADC_UPDT from the encounter projection only.'), _mp_sf('CODE_VALUE_ADC_UPDT', TimestampType(), 'Greatest ADC_UPDT among code-value rows used by the row.'), _mp_sf('MAP_REFRESH_DT_TM', TimestampType(), 'Datetime this mapped row last materially changed.'), _mp_sf('MAP_ROW_HASH', StringType(), 'Stable content hash used to avoid rewriting unchanged rows.'), _mp_sf('ADC_UPDT', TimestampType(), 'Compatibility composite source update timestamp; never use as a CDF checkpoint.')])
schema_map_procedure = schema_map_procedure_v2

def _mp_sql_name(table_name: str) -> str:
    quote = chr(96)
    return '.'.join((quote + part.replace(quote, quote + quote) + quote for part in table_name.split('.')))

def _mp_source_stage_name(run_id: str) -> str:
    return MAP_PROCEDURE_TARGET + '__source_stage_' + run_id[:24]

def _mp_materialize_source_stage(source: DataFrame, stage_table: str) -> DataFrame:
    source.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(stage_table)
    return spark.table(stage_table)

def _mp_drop_stage(stage_table: str) -> None:
    spark.sql(f'DROP TABLE IF EXISTS {_mp_sql_name(stage_table)}')

def _mp_escape(value: str) -> str:
    return (value or '').replace('\\', '\\\\').replace("'", "''")

def _mp_normalized_table_name(value: str) -> str:
    return (value or '').replace(chr(96), '').strip().lower()

def _mp_nonblank(column):
    return F.when(column.isNotNull() & (F.length(F.trim(column.cast('string'))) > 0), F.trim(column.cast('string')))

def _mp_stable_hash(column_names: List[str]):
    return F.sha2(F.to_json(F.struct(*[F.col(name).alias(name) for name in column_names]), {'ignoreNullFields': 'false'}), 256)

def _mp_checked_long(column, label: str, required: bool=False):
    # Value path is decimal-only; see the diagnosis-lane copy of this helper. The double
    # is a reject-only guard for NaN, non-integers and absurd magnitudes and never
    # produces the returned value. The procedure lane casts SNOMED_CODE plainly and so
    # was never a victim of the 2**53 defect; this copy is aligned for consistency.
    as_double = column.cast('double')
    as_decimal = column.cast('decimal(38,0)')
    null_result = F.lit(None) if required else F.lit(None).cast('long')
    in_long_range = as_decimal.between(F.lit(-9223372036854775808).cast('decimal(38,0)'), F.lit(9223372036854775807).cast('decimal(38,0)'))
    return F.when(column.isNull(), null_result).when(F.isnan(as_double), F.lit(None)).when(as_double != F.floor(as_double), F.lit(None)).when(F.abs(as_double) > F.lit(9223372036854775807), F.lit(None)).when(~in_long_range, F.lit(None)).otherwise(as_decimal.cast('long'))

def _mp_is_empty(df: DataFrame) -> bool:
    return df.isEmpty()

def _mp_empty_procedure_ids() -> DataFrame:
    return spark.createDataFrame([], StructType([StructField('PROCEDURE_ID', LongType(), False)]))

def _mp_empty_rows() -> DataFrame:
    return spark.createDataFrame([], schema_map_procedure_v2)

def _mp_align_to_schema(df: DataFrame) -> DataFrame:
    available = set(df.columns)
    expected = [field.name for field in schema_map_procedure_v2.fields]
    missing = [name for name in expected if name not in available]
    extras = sorted(available.difference(expected))
    if missing:
        raise RuntimeError('Map procedure output is missing required columns: ' + ', '.join(missing))
    if extras:
        print('[INFO] Dropping non-contract map_procedure columns: ' + ', '.join(extras))
    expressions = []
    for field in schema_map_procedure_v2.fields:
        expression = F.col(field.name).cast(field.dataType)
        expressions.append(expression.alias(field.name, metadata=field.metadata))
    return df.select(*expressions)

def ensure_map_procedure_v2_schema() -> None:
    if spark.catalog.tableExists(MAP_PROCEDURE_TARGET):
        return
    DeltaTable.createIfNotExists(spark).tableName(MAP_PROCEDURE_TARGET).addColumns(bronze_contract_schema(MAP_PROCEDURE_TARGET)).comment(map_procedure_comment).property('delta.enableChangeDataFeed', 'true').property('delta.enableRowTracking', 'true').property('delta.autoOptimize.optimizeWrite', 'true').property('delta.autoOptimize.autoCompact', 'true').property('delta.deletedFileRetentionDuration', MAP_PROCEDURE_CDF_RETENTION).property('delta.logRetentionDuration', MAP_PROCEDURE_CDF_RETENTION).property('pipeline.map_procedure.schema_version', MAP_PROCEDURE_SCHEMA_VERSION).execute()
    print('[INFO] Created ' + MAP_PROCEDURE_TARGET + ' with the v2 schema.')

def _mp_target_properties() -> Dict[str, str]:
    if not spark.catalog.tableExists(MAP_PROCEDURE_TARGET):
        return {}
    return {row['key']: row['value'] for row in spark.sql('SHOW TBLPROPERTIES ' + _mp_sql_name(MAP_PROCEDURE_TARGET)).collect()}

def _mp_target_comment() -> Optional[str]:
    if not spark.catalog.tableExists(MAP_PROCEDURE_TARGET):
        return None
    row = spark.sql('DESCRIBE DETAIL ' + _mp_sql_name(MAP_PROCEDURE_TARGET)).select('description').first()
    return None if row is None else row['description']

def _mp_set_target_metadata(apply_column_comments: bool=False) -> None:
    target_sql = _mp_sql_name(MAP_PROCEDURE_TARGET)
    desired = {'delta.enableChangeDataFeed': 'true', 'delta.enableDeletionVectors': 'true', 'delta.enableRowTracking': 'true', 'delta.autoOptimize.optimizeWrite': 'true', 'delta.autoOptimize.autoCompact': 'true', 'delta.deletedFileRetentionDuration': MAP_PROCEDURE_CDF_RETENTION, 'delta.logRetentionDuration': MAP_PROCEDURE_CDF_RETENTION, 'pipeline.map_procedure.schema_version': MAP_PROCEDURE_SCHEMA_VERSION, 'pipeline.map_procedure.cdf_checkpoint': 'delta_commit_version', 'pipeline.map_procedure.row_semantics': 'current_active_dated_source_rows'}
    current = _mp_target_properties()
    changed = {key: value for key, value in desired.items() if current.get(key) != value}
    if changed:
        property_sql = ', '.join(("'" + _mp_escape(key) + "' = '" + _mp_escape(value) + "'" for key, value in changed.items()))
        spark.sql('ALTER TABLE ' + target_sql + ' SET TBLPROPERTIES (' + property_sql + ')')
        print('[INFO] Updated ' + str(len(changed)) + ' map_procedure table properties.')
    if _mp_target_comment() != map_procedure_comment:
        spark.sql('COMMENT ON TABLE ' + target_sql + " IS '" + _mp_escape(map_procedure_comment) + "'")
    if apply_column_comments:
        for field in bronze_contract_schema(MAP_PROCEDURE_TARGET).fields:
            comment = field.metadata.get('comment', '')
            if comment:
                spark.sql('ALTER TABLE ' + target_sql + ' ALTER COLUMN ' + chr(96) + field.name + chr(96) + " COMMENT '" + _mp_escape(comment) + "'")

def configure_map_procedure_cdf_retention() -> None:
    """Explicitly extend source CDF/log recovery windows to thirty days."""
    for table_name in MAP_PROCEDURE_SOURCES.values():
        spark.sql('ALTER TABLE ' + _mp_sql_name(table_name) + ' SET TBLPROPERTIES (' + "'delta.deletedFileRetentionDuration' = '" + MAP_PROCEDURE_CDF_RETENTION + "', 'delta.logRetentionDuration' = '" + MAP_PROCEDURE_CDF_RETENTION + "')")

def configure_map_procedure_clustering(run_optimize: bool=False) -> None:
    """Configure liquid clustering on the merge key; OPTIMIZE is optional and explicit."""
    spark.sql('ALTER TABLE ' + _mp_sql_name(MAP_PROCEDURE_TARGET) + ' CLUSTER BY (PROCEDURE_ID)')
    if run_optimize:
        spark.sql('OPTIMIZE ' + _mp_sql_name(MAP_PROCEDURE_TARGET))

def ensure_map_procedure_checkpoint_table() -> None:
    checkpoint_sql = _mp_sql_name(MAP_PROCEDURE_CHECKPOINT)
    spark.sql("\n        CREATE TABLE IF NOT EXISTS {checkpoint} (\n            pipeline_name STRING NOT NULL,\n            source_table STRING NOT NULL,\n            last_processed_version BIGINT NOT NULL,\n            run_id STRING,\n            updated_at TIMESTAMP\n        )\n        USING DELTA\n        TBLPROPERTIES (\n            'delta.enableChangeDataFeed' = 'true',\n            'delta.deletedFileRetentionDuration' = '{retention}',\n            'delta.logRetentionDuration' = '{retention}',\n            'comment' = 'Successful source-specific Delta CDF checkpoints for map_procedure.'\n        )\n        ".format(checkpoint=checkpoint_sql, retention=MAP_PROCEDURE_CDF_RETENTION))

def _mp_latest_delta_version(table_name: str) -> int:
    row = spark.sql('DESCRIBE HISTORY ' + _mp_sql_name(table_name) + ' LIMIT 1').select('version').first()
    if row is None:
        raise RuntimeError('No Delta history found for ' + table_name)
    return int(row['version'])

def _mp_latest_source_versions() -> Dict[str, int]:
    return {source_name: _mp_latest_delta_version(table_name) for source_name, table_name in MAP_PROCEDURE_SOURCES.items()}

def _mp_checkpoint_versions() -> Dict[str, int]:
    ensure_map_procedure_checkpoint_table()
    rows = spark.table(MAP_PROCEDURE_CHECKPOINT).filter(F.col('pipeline_name') == MAP_PROCEDURE_PIPELINE).select('source_table', 'last_processed_version').collect()
    return {row['source_table']: int(row['last_processed_version']) for row in rows}

def _mp_write_checkpoint_versions(end_versions: Dict[str, int], run_id: str) -> None:
    ensure_map_procedure_checkpoint_table()
    now = datetime.utcnow()
    rows = [(MAP_PROCEDURE_PIPELINE, MAP_PROCEDURE_SOURCES[source_name], int(version), run_id, now) for source_name, version in end_versions.items()]
    schema = StructType([StructField('pipeline_name', StringType(), False), StructField('source_table', StringType(), False), StructField('last_processed_version', LongType(), False), StructField('run_id', StringType(), True), StructField('updated_at', TimestampType(), True)])
    updates = spark.createDataFrame(rows, schema)
    DeltaTable.forName(spark, MAP_PROCEDURE_CHECKPOINT).alias('t').merge(updates.alias('s'), 't.pipeline_name = s.pipeline_name AND t.source_table = s.source_table').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

class MapProcedureCDFGap(RuntimeError):
    pass

def _mp_cdf_changed_keys(table_name: str, key_column: str, output_key: str, start_version: int, end_version: int) -> DataFrame:
    if start_version > end_version:
        schema = StructType([StructField(output_key, LongType(), False)])
        return spark.createDataFrame([], schema)
    try:
        changes = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', int(start_version)).option('endingVersion', int(end_version)).table(table_name).select(_mp_checked_long(F.col(key_column), table_name + '.' + key_column).alias(output_key)).filter(F.col(output_key).isNotNull()).dropDuplicates([output_key])
        return _m20_hardening_validate_cdf(changes)
    except Exception as exc:
        message = str(exc)
        markers = ('change data', 'change feed', 'startingversion', 'not available', 'delta_change', 'retention', 'time travel')
        if any((marker in message.lower() for marker in markers)):
            raise MapProcedureCDFGap('CDF history is unavailable for ' + table_name + ' from version ' + str(start_version) + ' to ' + str(end_version) + '. A full refresh is required.') from exc
        raise

def _mp_read_source_snapshot(table_name: str) -> DataFrame:
    version = _MAP_PROCEDURE_ACTIVE_SOURCE_VERSIONS.get(table_name)
    if version is None:
        return spark.table(table_name)
    return _m20_hardening_read_pinned_snapshot(table_name, int(version))

def _mp_filter_procedure_ids(frame: DataFrame, procedure_ids: Optional[DataFrame]) -> DataFrame:
    if procedure_ids is None:
        return frame
    keys = procedure_ids.select(_mp_checked_long(F.col('PROCEDURE_ID'), 'FILTER.PROCEDURE_ID', True).alias('_FILTER_PROCEDURE_ID')).dropDuplicates(['_FILTER_PROCEDURE_ID'])
    return frame.alias('src').join(F.broadcast(keys).alias('keys'), F.col('src.PROCEDURE_ID').cast('long') == F.col('keys._FILTER_PROCEDURE_ID'), 'inner').drop('_FILTER_PROCEDURE_ID')

def _mp_current_procedure_snapshot(procedure_ids: Optional[DataFrame]=None) -> DataFrame:
    raw = _mp_filter_procedure_ids(_mp_read_source_snapshot(MAP_PROCEDURE_SOURCE), procedure_ids)
    if MAP_PROCEDURE_TRUST is not None:
        raw = raw.filter(F.col('Trust') == F.lit(MAP_PROCEDURE_TRUST))
    # Source-quality scope restored from the pre-replacement legacy pipeline, which
    # required both conditions. Dropping them admitted inactive and undated procedure
    # rows into the curated table. Both the full rebuild and the incremental refresh
    # reach the source through this function, so this is the single governing filter.
    # ACTIVE_IND is a DOUBLE in 4_prod.raw.mill_procedure, hence the defensive cast; a
    # NULL indicator is not 1 and is excluded with the rest.
    # 4_prod.raw.mill_procedure remains the retention layer for everything excluded here.
    raw = raw.filter((F.col('ACTIVE_IND').cast('long') == F.lit(1)) & F.col('PROC_DT_TM').isNotNull())
    return raw.select(_mp_checked_long(F.col('PROCEDURE_ID'), 'PROCEDURE_ID', True).alias('PROCEDURE_ID'), _mp_checked_long(F.col('ENCNTR_ID'), 'ENCNTR_ID').alias('ENCNTR_ID'), _mp_checked_long(F.col('NOMENCLATURE_ID'), 'NOMENCLATURE_ID').alias('NOMENCLATURE_ID'), _mp_checked_long(F.col('ENCNTR_SLICE_ID'), 'ENCNTR_SLICE_ID').alias('ENCNTR_SLICE_ID'), _mp_checked_long(F.col('ORGANIZATION_ID'), 'ORGANIZATION_ID').alias('ORGANIZATION_ID'), _mp_checked_long(F.col('PARENT_ENTITY_ID'), 'PARENT_ENTITY_ID').alias('PARENT_ENTITY_ID'), F.col('PARENT_ENTITY_NAME').cast('string').alias('PARENT_ENTITY_NAME'), _mp_checked_long(F.col('SVC_CAT_HIST_ID'), 'SVC_CAT_HIST_ID').alias('SVC_CAT_HIST_ID'), _mp_checked_long(F.col('UPDT_CNT'), 'UPDT_CNT').alias('UPDT_CNT'), F.col('UPDT_DT_TM').cast('timestamp').alias('UPDT_DT_TM'), _mp_checked_long(F.col('UPDT_ID'), 'UPDT_ID').alias('UPDT_ID'), _mp_checked_long(F.col('UPDT_TASK'), 'UPDT_TASK').alias('UPDT_TASK'), _mp_checked_long(F.col('UPDT_APPLCTX'), 'UPDT_APPLCTX').alias('UPDT_APPLCTX'), F.col('LAST_UTC_TS').cast('timestamp').alias('LAST_UTC_TS'), F.col('TXN_ID_TEXT').cast('string').alias('TXN_ID_TEXT'), F.col('Trust').cast('string').alias('TRUST'), _mp_checked_long(F.col('ACTIVE_IND'), 'ACTIVE_IND').alias('ACTIVE_IND'), _mp_checked_long(F.col('ACTIVE_STATUS_CD'), 'ACTIVE_STATUS_CD').alias('ACTIVE_STATUS_CD'), F.col('ACTIVE_STATUS_DT_TM').cast('timestamp').alias('ACTIVE_STATUS_DT_TM'), _mp_checked_long(F.col('ACTIVE_STATUS_PRSNL_ID'), 'ACTIVE_STATUS_PRSNL_ID').alias('ACTIVE_STATUS_PRSNL_ID'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('BEG_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('END_EFFECTIVE_DT_TM'), _mp_checked_long(F.col('CONTRIBUTOR_SYSTEM_CD'), 'CONTRIBUTOR_SYSTEM_CD').alias('CONTRIBUTOR_SYSTEM_CD'), _mp_checked_long(F.col('CLINICAL_SERVICE_CD'), 'CLINICAL_SERVICE_CD').alias('CLINICAL_SERVICE_CD'), _mp_checked_long(F.col('PROC_PRIORITY'), 'PROC_PRIORITY').alias('PROC_PRIORITY'), _mp_checked_long(F.col('PROC_FUNC_TYPE_CD'), 'PROC_FUNC_TYPE_CD').alias('PROC_FUNC_TYPE_CD'), _mp_checked_long(F.col('PROC_TYPE_FLAG'), 'PROC_TYPE_FLAG').alias('PROC_TYPE_FLAG'), _mp_checked_long(F.col('CATEGORY_CD'), 'CATEGORY_CD').alias('CATEGORY_CD'), _mp_checked_long(F.col('RANKING_CD'), 'RANKING_CD').alias('RANKING_CD'), _mp_checked_long(F.col('DGVP_IND'), 'DGVP_IND').alias('DGVP_IND'), _mp_checked_long(F.col('SUPPRESS_NARRATIVE_IND'), 'SUPPRESS_NARRATIVE_IND').alias('SUPPRESS_NARRATIVE_IND'), F.col('PROC_DT_TM').cast('timestamp').alias('PROC_DT_TM'), F.col('PROC_START_DT_TM').cast('timestamp').alias('PROC_START_DT_TM'), F.col('PROC_END_DT_TM').cast('timestamp').alias('PROC_END_DT_TM'), _mp_checked_long(F.col('PROC_DT_TM_PREC_FLAG'), 'PROC_DT_TM_PREC_FLAG').alias('PROC_DT_TM_PREC_FLAG'), _mp_checked_long(F.col('PROC_DT_TM_PREC_CD'), 'PROC_DT_TM_PREC_CD').alias('PROC_DT_TM_PREC_CD'), _mp_checked_long(F.col('PROC_FT_DT_TM_IND'), 'PROC_FT_DT_TM_IND').alias('PROC_FT_DT_TM_IND'), F.col('PROC_FT_TIME_FRAME').cast('string').alias('PROC_FT_TIME_FRAME'), F.col('PROC_MINUTES').cast('double').alias('PROC_MINUTES'), _mp_checked_long(F.col('ANESTHESIA_CD'), 'ANESTHESIA_CD').alias('ANESTHESIA_CD'), F.col('ANESTHESIA_MINUTES').cast('double').alias('ANESTHESIA_MINUTES'), F.col('UNITS_OF_SERVICE').cast('double').alias('UNITS_OF_SERVICE'), _mp_checked_long(F.col('PROC_LOC_CD'), 'PROC_LOC_CD').alias('PROC_LOC_CD'), _mp_checked_long(F.col('PROC_LOC_FT_IND'), 'PROC_LOC_FT_IND').alias('PROC_LOC_FT_IND'), F.col('PROC_FT_LOC').cast('string').alias('PROC_FT_LOC'), _mp_checked_long(F.col('LATERALITY_CD'), 'LATERALITY_CD').alias('LATERALITY_CD'), _mp_checked_long(F.col('TISSUE_TYPE_CD'), 'TISSUE_TYPE_CD').alias('TISSUE_TYPE_CD'), F.col('PROCEDURE_NOTE').cast('string').alias('PROCEDURE_NOTE'), F.col('PROC_FTDESC').cast('string').alias('PROC_FTDESC'), _mp_checked_long(F.col('COMMENT_IND'), 'COMMENT_IND').alias('COMMENT_IND'), _mp_checked_long(F.col('LONG_TEXT_ID'), 'LONG_TEXT_ID').alias('LONG_TEXT_ID'), F.col('REFERENCE_NBR').cast('string').alias('REFERENCE_NBR'), F.col('SEG_UNIQUE_KEY').cast('string').alias('SEG_UNIQUE_KEY'), _mp_checked_long(F.col('CONSENT_CD'), 'CONSENT_CD').alias('CONSENT_CD'), _mp_checked_long(F.col('GENERIC_VAL_CD'), 'GENERIC_VAL_CD').alias('GENERIC_VAL_CD'), _mp_checked_long(F.col('DIAG_NOMENCLATURE_ID'), 'DIAG_NOMENCLATURE_ID').alias('DIAG_NOMENCLATURE_ID'), _mp_checked_long(F.col('MOD_NOMENCLATURE_ID'), 'MOD_NOMENCLATURE_ID').alias('MOD_NOMENCLATURE_ID'), F.col('ADC_UPDT').cast('timestamp').alias('PROCEDURE_ADC_UPDT'))

def _mp_current_encounter_projection() -> DataFrame:
    return _mp_read_source_snapshot(MAP_PROCEDURE_ENCOUNTER).select(_mp_checked_long(F.col('ENCNTR_ID'), 'ENCOUNTER.ENCNTR_ID', True).alias('ENCNTR_ID'), _mp_checked_long(F.col('PERSON_ID'), 'ENCOUNTER.PERSON_ID').alias('PERSON_ID'), F.col('ADC_UPDT').cast('timestamp').alias('ENCOUNTER_ADC_UPDT'))

def _mp_current_code_value_projection() -> DataFrame:
    raw = _mp_read_source_snapshot(MAP_PROCEDURE_CODE_VALUE)
    description = F.coalesce(_mp_nonblank(F.col('DISPLAY')), _mp_nonblank(F.col('DESCRIPTION')), _mp_nonblank(F.col('CDF_MEANING')), _mp_nonblank(F.col('DEFINITION')))
    return raw.select(_mp_checked_long(F.col('CODE_VALUE'), 'CODE_VALUE.CODE_VALUE', True).alias('CODE_VALUE'), description.alias('CODE_DESC'), F.col('ADC_UPDT').cast('timestamp').alias('CODE_ADC_UPDT'))

def _mp_current_nomenclature_projection() -> DataFrame:
    raw = _mp_read_source_snapshot(MAP_PROCEDURE_NOMENCLATURE)
    selected = raw.select(_mp_checked_long(F.col('NOMENCLATURE_ID'), 'NOMENCLATURE.NOMENCLATURE_ID', True).alias('NOMENCLATURE_ID'), F.col('SOURCE_IDENTIFIER').cast('string').alias('SOURCE_IDENTIFIER'), F.col('SOURCE_STRING').cast('string').alias('SOURCE_STRING'), _mp_checked_long(F.col('SOURCE_VOCABULARY_CD'), 'SOURCE_VOCABULARY_CD').alias('SOURCE_VOCABULARY_CD'), _mp_checked_long(F.col('VOCAB_AXIS_CD'), 'VOCAB_AXIS_CD').alias('VOCAB_AXIS_CD'), F.col('CONCEPT_CKI').cast('string').alias('CONCEPT_CKI'), F.col('FOUND_CUI').cast('string').alias('FOUND_CUI'), F.col('IS_ACTIVE').cast('boolean').alias('NOMENCLATURE_IS_ACTIVE'), F.col('SOURCE_CHANGE_TS').cast('timestamp').alias('NOMENCLATURE_SOURCE_CHANGE_TS'), F.col('OMOP_CONCEPT_ID').cast('long').alias('OMOP_CONCEPT_ID'), F.col('OMOP_CONCEPT_NAME').cast('string').alias('OMOP_CONCEPT_NAME'), F.col('IS_STANDARD_OMOP_CONCEPT').cast('string').alias('OMOP_STANDARD_CONCEPT'), F.col('NUMBER_OF_OMOP_MATCHES').cast('long').alias('OMOP_MATCH_NUMBER'), F.col('OMOP_SIMILARITY').cast('double').alias('OMOP_SIMILARITY'), F.col('CONCEPT_DOMAIN').cast('string').alias('OMOP_CONCEPT_DOMAIN'), F.col('CONCEPT_CLASS').cast('string').alias('OMOP_CONCEPT_CLASS'), F.col('SNOMED_CODE').cast('long').alias('SNOMED_CODE'), F.col('SNOMED_TYPE').cast('string').alias('SNOMED_TYPE'), F.col('SNOMED_MATCH_COUNT').cast('long').alias('SNOMED_MATCH_NUMBER'), F.col('SNOMED_SIMILARITY').cast('double').alias('SNOMED_SIMILARITY'), F.col('SNOMED_TERM').cast('string').alias('SNOMED_TERM'), F.col('ICD10_CODE').cast('string').alias('ICD10_CODE'), F.col('ICD10_CODE_TYPE').cast('string').alias('ICD10_TYPE'), F.col('ICD10_CODE_MATCH_COUNT').cast('long').alias('ICD10_MATCH_NUMBER'), F.col('ICD10_SIMILARITY').cast('double').alias('ICD10_SIMILARITY'), F.col('ICD10_TERM').cast('string').alias('ICD10_TERM'), F.col('OPCS4_CODE').cast('string').alias('OPCS4_CODE'), F.col('OPCS4_CODE_TYPE').cast('string').alias('OPCS4_TYPE'), F.col('OPCS4_CODE_MATCH_COUNT').cast('long').alias('OPCS4_MATCH_NUMBER'), F.col('OPCS4_SIMILARITY').cast('double').alias('OPCS4_SIMILARITY'), F.col('OPCS4_TERM').cast('string').alias('OPCS4_TERM'), F.col('SIMILARITY_SOURCE_SNOMED').cast('double').alias('SIMILARITY_SOURCE_SNOMED'), F.col('SIMILARITY_SOURCE_ICD10').cast('double').alias('SIMILARITY_SOURCE_ICD10'), F.col('SIMILARITY_SOURCE_OPCS4').cast('double').alias('SIMILARITY_SOURCE_OPCS4'), F.col('SIMILARITY_SOURCE_OMOP').cast('double').alias('SIMILARITY_SOURCE_OMOP'), F.col('SIMILARITY_SNOMED_ICD10').cast('double').alias('SIMILARITY_SNOMED_ICD10'), F.col('SIMILARITY_SNOMED_OPCS4').cast('double').alias('SIMILARITY_SNOMED_OPCS4'), F.col('SIMILARITY_SNOMED_OMOP').cast('double').alias('SIMILARITY_SNOMED_OMOP'), F.col('SIMILARITY_ICD10_OMOP').cast('double').alias('SIMILARITY_ICD10_OMOP'), F.col('SIMILARITY_OPCS4_OMOP').cast('double').alias('SIMILARITY_OPCS4_OMOP'), F.col('ADC_UPDT').cast('timestamp').alias('NOMENCLATURE_ADC_UPDT'))
    hash_columns = [name for name in selected.columns if name not in {'NOMENCLATURE_ID', 'NOMENCLATURE_ADC_UPDT'}]
    return selected.withColumn('NOMENCLATURE_MAPPING_HASH', _mp_stable_hash(hash_columns))
_MP_CODE_SPECS = [('p', 'ACTIVE_STATUS_CD', 'active_status', 'active_status_desc'), ('p', 'CONTRIBUTOR_SYSTEM_CD', 'contributor_system', 'contributor_system_desc'), ('p', 'CLINICAL_SERVICE_CD', 'clinical_service', 'clinical_service_desc'), ('p', 'PROC_FUNC_TYPE_CD', 'proc_func_type', 'proc_func_type_desc'), ('p', 'CATEGORY_CD', 'category', 'category_desc'), ('p', 'RANKING_CD', 'ranking', 'ranking_desc'), ('p', 'PROC_DT_TM_PREC_CD', 'proc_dt_tm_precision', 'proc_dt_tm_precision_desc'), ('p', 'ANESTHESIA_CD', 'anesthesia', 'anesthesia_desc'), ('p', 'PROC_LOC_CD', 'proc_location', 'proc_location_desc'), ('p', 'LATERALITY_CD', 'laterality', 'laterality_desc'), ('p', 'TISSUE_TYPE_CD', 'tissue_type', 'tissue_type_desc'), ('p', 'CONSENT_CD', 'consent', 'consent_desc'), ('p', 'GENERIC_VAL_CD', 'generic_value', 'generic_value_desc'), ('n', 'SOURCE_VOCABULARY_CD', 'source_vocabulary', 'source_vocabulary_desc'), ('n', 'VOCAB_AXIS_CD', 'vocab_axis', 'vocab_axis_desc')]
_MP_CODE_COLUMNS = [spec[1] for spec in _MP_CODE_SPECS if spec[0] == 'p'] + ['SOURCE_VOCABULARY_CD', 'VOCAB_AXIS_CD']

def _mp_build_map_rows(procedure_rows: DataFrame) -> DataFrame:
    encounter = _mp_current_encounter_projection().alias('e')
    nomenclature = _mp_current_nomenclature_projection().alias('n')
    code_values = F.broadcast(_mp_current_code_value_projection())
    joined = procedure_rows.alias('p').join(encounter, F.col('p.ENCNTR_ID') == F.col('e.ENCNTR_ID'), 'left').join(nomenclature, F.col('p.NOMENCLATURE_ID') == F.col('n.NOMENCLATURE_ID'), 'left')
    for source_alias, code_column, lookup_alias, _ in _MP_CODE_SPECS:
        joined = joined.join(code_values.alias('cv_' + lookup_alias), F.col(source_alias + '.' + code_column) == F.col('cv_' + lookup_alias + '.CODE_VALUE'), 'left')
    source_string = _mp_nonblank(F.col('n.SOURCE_STRING'))
    free_text_description = _mp_nonblank(F.col('p.PROC_FTDESC'))
    procedure_note = _mp_nonblank(F.col('p.PROCEDURE_NOTE'))
    procedure_display = F.coalesce(source_string, free_text_description, procedure_note)
    procedure_display_source = F.when(source_string.isNotNull(), F.lit('SOURCE_STRING')).when(free_text_description.isNotNull(), F.lit('PROC_FTDESC')).when(procedure_note.isNotNull(), F.lit('PROCEDURE_NOTE')).otherwise(F.lit(None).cast('string'))
    effective_datetime = F.coalesce(F.col('p.PROC_DT_TM'), F.col('p.PROC_START_DT_TM'), F.col('p.PROC_END_DT_TM'))
    effective_datetime_source = F.when(F.col('p.PROC_DT_TM').isNotNull(), F.lit('PROC_DT_TM')).when(F.col('p.PROC_START_DT_TM').isNotNull(), F.lit('PROC_START_DT_TM')).when(F.col('p.PROC_END_DT_TM').isNotNull(), F.lit('PROC_END_DT_TM')).otherwise(F.lit(None).cast('string'))
    calculated_minutes = F.when(F.col('p.PROC_START_DT_TM').isNotNull() & F.col('p.PROC_END_DT_TM').isNotNull() & (F.col('p.PROC_END_DT_TM') >= F.col('p.PROC_START_DT_TM')), (F.col('p.PROC_END_DT_TM').cast('double') - F.col('p.PROC_START_DT_TM').cast('double')) / F.lit(60.0))
    code_description_expressions = [F.col('cv_' + lookup_alias + '.CODE_DESC').alias(output_column) for _, _, lookup_alias, output_column in _MP_CODE_SPECS]
    code_adc_columns = [F.col('cv_' + lookup_alias + '.CODE_ADC_UPDT') for _, _, lookup_alias, _ in _MP_CODE_SPECS]
    code_adc = F.greatest(*code_adc_columns)
    concept_cki = F.col('n.CONCEPT_CKI')
    cki_parts = F.split(concept_cki, '!')
    selected = joined.select('p.*', F.col('e.PERSON_ID').alias('PERSON_ID'), *code_description_expressions, effective_datetime.alias('PROCEDURE_DT_TM_EFFECTIVE'), effective_datetime_source.alias('PROCEDURE_DT_TM_SOURCE'), F.coalesce(F.col('p.PROC_DT_TM').isNull(), F.lit(True)).alias('PROC_DT_TM_MISSING_IND'), F.coalesce(F.col('p.PROC_MINUTES') == F.lit(0.0), F.lit(False)).alias('PROC_MINUTES_SOURCE_ZERO_IND'), calculated_minutes.alias('PROC_MINUTES_CALCULATED'), procedure_display.alias('PROCEDURE_DISPLAY'), procedure_display_source.alias('PROCEDURE_DISPLAY_SOURCE'), F.col('n.SOURCE_IDENTIFIER').alias('SOURCE_IDENTIFIER'), F.col('n.SOURCE_STRING').alias('SOURCE_STRING'), F.col('n.SOURCE_VOCABULARY_CD').alias('SOURCE_VOCABULARY_CD'), F.col('n.VOCAB_AXIS_CD').alias('VOCAB_AXIS_CD'), concept_cki.alias('CONCEPT_CKI'), F.when(concept_cki.isNotNull(), F.element_at(cki_parts, 1)).alias('CONCEPT_CKI_SOURCE'), F.when(concept_cki.isNotNull(), F.element_at(cki_parts, -1)).alias('CONCEPT_CKI_IDENTIFIER'), F.col('n.FOUND_CUI').alias('FOUND_CUI'), F.col('n.NOMENCLATURE_IS_ACTIVE').alias('NOMENCLATURE_IS_ACTIVE'), F.col('n.NOMENCLATURE_SOURCE_CHANGE_TS').alias('NOMENCLATURE_SOURCE_CHANGE_TS'), F.col('n.NOMENCLATURE_MAPPING_HASH').alias('NOMENCLATURE_MAPPING_HASH'), F.col('e.ENCNTR_ID').isNotNull().alias('ENCOUNTER_MATCHED_IND'), F.col('n.NOMENCLATURE_ID').isNotNull().alias('NOMENCLATURE_MATCHED_IND'), F.col('n.OMOP_CONCEPT_ID').alias('OMOP_CONCEPT_ID'), F.col('n.OMOP_CONCEPT_NAME').alias('OMOP_CONCEPT_NAME'), F.col('n.OMOP_STANDARD_CONCEPT').alias('OMOP_STANDARD_CONCEPT'), F.col('n.OMOP_MATCH_NUMBER').alias('OMOP_MATCH_NUMBER'), F.col('n.OMOP_SIMILARITY').alias('OMOP_SIMILARITY'), F.col('n.OMOP_CONCEPT_DOMAIN').alias('OMOP_CONCEPT_DOMAIN'), F.col('n.OMOP_CONCEPT_CLASS').alias('OMOP_CONCEPT_CLASS'), F.col('n.SNOMED_CODE').alias('SNOMED_CODE'), F.col('n.SNOMED_TYPE').alias('SNOMED_TYPE'), F.col('n.SNOMED_MATCH_NUMBER').alias('SNOMED_MATCH_NUMBER'), F.col('n.SNOMED_SIMILARITY').alias('SNOMED_SIMILARITY'), F.col('n.SNOMED_TERM').alias('SNOMED_TERM'), F.col('n.ICD10_CODE').alias('ICD10_CODE'), F.col('n.ICD10_TYPE').alias('ICD10_TYPE'), F.col('n.ICD10_MATCH_NUMBER').alias('ICD10_MATCH_NUMBER'), F.col('n.ICD10_SIMILARITY').alias('ICD10_SIMILARITY'), F.col('n.ICD10_TERM').alias('ICD10_TERM'), F.col('n.OPCS4_CODE').alias('OPCS4_CODE'), F.col('n.OPCS4_TYPE').alias('OPCS4_TYPE'), F.col('n.OPCS4_MATCH_NUMBER').alias('OPCS4_MATCH_NUMBER'), F.col('n.OPCS4_SIMILARITY').alias('OPCS4_SIMILARITY'), F.col('n.OPCS4_TERM').alias('OPCS4_TERM'), F.col('n.SIMILARITY_SOURCE_SNOMED').alias('SIMILARITY_SOURCE_SNOMED'), F.col('n.SIMILARITY_SOURCE_ICD10').alias('SIMILARITY_SOURCE_ICD10'), F.col('n.SIMILARITY_SOURCE_OPCS4').alias('SIMILARITY_SOURCE_OPCS4'), F.col('n.SIMILARITY_SOURCE_OMOP').alias('SIMILARITY_SOURCE_OMOP'), F.col('n.SIMILARITY_SNOMED_ICD10').alias('SIMILARITY_SNOMED_ICD10'), F.col('n.SIMILARITY_SNOMED_OPCS4').alias('SIMILARITY_SNOMED_OPCS4'), F.col('n.SIMILARITY_SNOMED_OMOP').alias('SIMILARITY_SNOMED_OMOP'), F.col('n.SIMILARITY_ICD10_OMOP').alias('SIMILARITY_ICD10_OMOP'), F.col('n.SIMILARITY_OPCS4_OMOP').alias('SIMILARITY_OPCS4_OMOP'), F.col('n.NOMENCLATURE_ADC_UPDT').alias('NOMENCLATURE_ADC_UPDT'), F.col('e.ENCOUNTER_ADC_UPDT').alias('ENCOUNTER_ADC_UPDT'), code_adc.alias('CODE_VALUE_ADC_UPDT'))
    description_columns = [spec[3] for spec in _MP_CODE_SPECS]
    selected = selected.withColumn('CODE_VALUE_LOOKUP_HASH', _mp_stable_hash(description_columns))
    selected = selected.withColumn('ADC_UPDT', F.greatest(F.col('PROCEDURE_ADC_UPDT'), F.col('NOMENCLATURE_SOURCE_CHANGE_TS'), F.col('NOMENCLATURE_ADC_UPDT'), F.col('ENCOUNTER_ADC_UPDT'), F.col('CODE_VALUE_ADC_UPDT')))
    row_hash_exclusions = {'MAP_ROW_HASH', 'MAP_REFRESH_DT_TM', 'ADC_UPDT', 'NOMENCLATURE_ADC_UPDT', 'ENCOUNTER_ADC_UPDT', 'CODE_VALUE_ADC_UPDT'}
    row_hash_columns = [field.name for field in schema_map_procedure_v2.fields if field.name not in row_hash_exclusions]
    selected = selected.withColumn('MAP_ROW_HASH', _mp_stable_hash(row_hash_columns)).withColumn('MAP_REFRESH_DT_TM', F.current_timestamp())
    return _mp_align_to_schema(selected)

def _mp_materially_changed_nomenclature_ids(changed_ids: DataFrame) -> DataFrame:
    if _mp_is_empty(changed_ids):
        return changed_ids.select('NOMENCLATURE_ID')
    target_hashes = spark.table(MAP_PROCEDURE_TARGET).select('NOMENCLATURE_ID', 'NOMENCLATURE_MAPPING_HASH').join(F.broadcast(changed_ids), 'NOMENCLATURE_ID', 'left_semi').groupBy('NOMENCLATURE_ID').agg(F.count('NOMENCLATURE_MAPPING_HASH').alias('_hash_count'), F.min('NOMENCLATURE_MAPPING_HASH').alias('_min_hash'), F.max('NOMENCLATURE_MAPPING_HASH').alias('_max_hash'))
    current = _mp_current_nomenclature_projection().select('NOMENCLATURE_ID', 'NOMENCLATURE_MAPPING_HASH').join(F.broadcast(changed_ids), 'NOMENCLATURE_ID', 'inner').withColumnRenamed('NOMENCLATURE_MAPPING_HASH', '_current_hash').withColumn('_current_exists', F.lit(True))
    comparison = target_hashes.join(current, 'NOMENCLATURE_ID', 'left')
    return comparison.filter(F.col('_current_exists').isNull() & (F.col('_hash_count') > 0) | F.col('_current_exists').isNotNull() & ((F.col('_hash_count') == 0) | ~F.col('_min_hash').eqNullSafe(F.col('_current_hash')) | ~F.col('_max_hash').eqNullSafe(F.col('_current_hash')))).select('NOMENCLATURE_ID').dropDuplicates(['NOMENCLATURE_ID'])

def _mp_target_ids_for_nomenclature_ids(nomenclature_ids: DataFrame) -> DataFrame:
    if _mp_is_empty(nomenclature_ids):
        return _mp_empty_procedure_ids()
    return spark.table(MAP_PROCEDURE_TARGET).select('PROCEDURE_ID', 'NOMENCLATURE_ID').join(F.broadcast(nomenclature_ids), 'NOMENCLATURE_ID', 'left_semi').select('PROCEDURE_ID').dropDuplicates(['PROCEDURE_ID'])

def _mp_target_ids_for_encounter_ids(encounter_ids: DataFrame) -> DataFrame:
    if _mp_is_empty(encounter_ids):
        return _mp_empty_procedure_ids()
    targets = spark.table(MAP_PROCEDURE_TARGET).select('PROCEDURE_ID', 'ENCNTR_ID', F.col('PERSON_ID').alias('_target_person_id')).join(F.broadcast(encounter_ids), 'ENCNTR_ID', 'left_semi')
    current = _mp_current_encounter_projection().select('ENCNTR_ID', F.col('PERSON_ID').alias('_current_person_id')).join(F.broadcast(encounter_ids), 'ENCNTR_ID', 'inner')
    return targets.join(current, 'ENCNTR_ID', 'left').filter(~F.col('_target_person_id').eqNullSafe(F.col('_current_person_id'))).select('PROCEDURE_ID').dropDuplicates(['PROCEDURE_ID'])

def _mp_target_ids_for_code_values(code_values: DataFrame) -> DataFrame:
    if _mp_is_empty(code_values):
        return _mp_empty_procedure_ids()
    sample = code_values.select('CODE_VALUE').limit(1001).collect()
    target = spark.table(MAP_PROCEDURE_TARGET)
    if len(sample) <= 1000:
        values = [int(row['CODE_VALUE']) for row in sample if row['CODE_VALUE'] is not None]
        if not values:
            return _mp_empty_procedure_ids()
        conditions = [F.col(column_name).isin(values) for column_name in _MP_CODE_COLUMNS]
        condition = reduce(lambda left, right: left | right, conditions)
        return target.filter(condition).select('PROCEDURE_ID').dropDuplicates(['PROCEDURE_ID'])
    long_codes = target.select('PROCEDURE_ID', F.explode(F.array(*[F.col(name) for name in _MP_CODE_COLUMNS])).alias('CODE_VALUE')).filter(F.col('CODE_VALUE').isNotNull())
    return long_codes.join(F.broadcast(code_values), 'CODE_VALUE', 'left_semi').select('PROCEDURE_ID').dropDuplicates(['PROCEDURE_ID'])

def _mp_requires_full_refresh(checkpoints: Dict[str, int], end_versions: Dict[str, int]) -> bool:
    if not spark.catalog.tableExists(MAP_PROCEDURE_TARGET):
        return True
    properties = _mp_target_properties()
    if properties.get('pipeline.map_procedure.schema_version') != MAP_PROCEDURE_SCHEMA_VERSION:
        print('[INFO] map_procedure schema-version property is absent or outdated.')
        return True
    expected_schema = {
        field.name.lower(): field.dataType.simpleString()
        for field in bronze_contract_schema(MAP_PROCEDURE_TARGET).fields
    }
    current_schema = {field.name.lower(): field.dataType.simpleString() for field in spark.table(MAP_PROCEDURE_TARGET).schema.fields}
    if current_schema != expected_schema:
        print('[INFO] Target schema does not exactly match the v2 replacement schema.')
        return True
    expected_tables = set(MAP_PROCEDURE_SOURCES.values())
    if not expected_tables.issubset(set(checkpoints)):
        print('[INFO] Source-specific checkpoints are incomplete; a full refresh is required.')
        return True
    for source_name, table_name in MAP_PROCEDURE_SOURCES.items():
        if checkpoints[table_name] > end_versions[source_name]:
            print('[INFO] Source Delta version is lower than its checkpoint: ' + table_name)
            return True
    if spark.table(MAP_PROCEDURE_TARGET).filter(F.col('MAP_ROW_HASH').isNull()).limit(1).count() > 0:
        print('[INFO] Legacy target rows do not have v2 hashes.')
        return True
    return False

def _mp_prepare_full_refresh(end_versions: Dict[str, int], run_id: str, minimum_source_ratio: float) -> Dict:
    stage_table = _mp_source_stage_name(run_id)
    try:
        source = _mp_materialize_source_stage(_mp_current_procedure_snapshot(), stage_table)
        source_count = source.count()
        target_count = spark.table(MAP_PROCEDURE_TARGET).count() if spark.catalog.tableExists(MAP_PROCEDURE_TARGET) else 0
        if source_count == 0:
            raise RuntimeError('Full refresh refused because the current procedure source snapshot is empty.')
        if target_count > 0 and source_count < target_count * minimum_source_ratio:
            raise RuntimeError('Full refresh safety check failed: source count ' + str(source_count) + ' is below ' + str(minimum_source_ratio) + ' of target count ' + str(target_count) + '.')
        duplicate_exists = source.groupBy('PROCEDURE_ID').count().filter(F.col('count') > 1).limit(1).count() > 0
        if duplicate_exists:
            raise RuntimeError('Full refresh refused because source PROCEDURE_ID is not unique.')
        rows = _mp_build_map_rows(source)
        print('[INFO] Prepared full map_procedure refresh from ' + str(source_count) + ' source rows.')
        return {'run_id': run_id, 'mode': 'full', 'rows': rows, 'source_count': source_count, 'delete_ids': None, 'has_rows': True, 'has_deletes': False, 'end_versions': end_versions, 'persisted': [], 'scratch_tables': [stage_table]}
    except Exception:
        _mp_drop_stage(stage_table)
        raise

def _mp_prepare_incremental_refresh(checkpoints: Dict[str, int], end_versions: Dict[str, int], run_id: str) -> Dict:
    changed_procedures = _mp_cdf_changed_keys(MAP_PROCEDURE_SOURCE, 'PROCEDURE_ID', 'PROCEDURE_ID', checkpoints[MAP_PROCEDURE_SOURCE] + 1, end_versions['procedure'])
    changed_nomenclature = _mp_cdf_changed_keys(MAP_PROCEDURE_NOMENCLATURE, 'NOMENCLATURE_ID', 'NOMENCLATURE_ID', checkpoints[MAP_PROCEDURE_NOMENCLATURE] + 1, end_versions['nomenclature'])
    changed_encounters = _mp_cdf_changed_keys(MAP_PROCEDURE_ENCOUNTER, 'ENCNTR_ID', 'ENCNTR_ID', checkpoints[MAP_PROCEDURE_ENCOUNTER] + 1, end_versions['encounter'])
    changed_code_values = _mp_cdf_changed_keys(MAP_PROCEDURE_CODE_VALUE, 'CODE_VALUE', 'CODE_VALUE', checkpoints[MAP_PROCEDURE_CODE_VALUE] + 1, end_versions['code_value'])
    materially_changed_nomenclature = _mp_materially_changed_nomenclature_ids(changed_nomenclature)
    nomenclature_procedures = _mp_target_ids_for_nomenclature_ids(materially_changed_nomenclature)
    encounter_procedures = _mp_target_ids_for_encounter_ids(changed_encounters)
    code_value_procedures = _mp_target_ids_for_code_values(changed_code_values)
    refresh_ids = reduce(lambda left, right: left.unionByName(right), [changed_procedures.select('PROCEDURE_ID'), nomenclature_procedures, encounter_procedures, code_value_procedures]).dropDuplicates(['PROCEDURE_ID'])
    current_source = _mp_current_procedure_snapshot(refresh_ids)
    current_ids = current_source.select('PROCEDURE_ID').dropDuplicates(['PROCEDURE_ID'])
    delete_ids = changed_procedures.select('PROCEDURE_ID').join(current_ids, 'PROCEDURE_ID', 'left_anti')
    has_source_rows = not _mp_is_empty(current_source)
    has_deletes = not _mp_is_empty(delete_ids)
    persisted = [current_source, delete_ids]
    if has_source_rows:
        rows = _mp_build_map_rows(current_source)
        persisted.append(rows)
        has_rows = not _mp_is_empty(rows)
    else:
        rows = _mp_empty_rows()
        has_rows = False
    print('[INFO] Prepared incremental map_procedure update: rows=' + str(has_rows) + ', deletes=' + str(has_deletes) + '.')
    return {'run_id': run_id, 'mode': 'incremental', 'rows': rows, 'delete_ids': delete_ids, 'has_rows': has_rows, 'has_deletes': has_deletes, 'end_versions': end_versions, 'persisted': persisted}

def prepare_map_procedure_update(force_full_refresh: Optional[bool]=None, auto_full_refresh_on_cdf_gap: bool=False, minimum_full_refresh_source_ratio: float=0.98) -> Dict:
    global _MAP_PROCEDURE_ACTIVE_SOURCE_VERSIONS
    ensure_map_procedure_v2_schema()
    ensure_map_procedure_checkpoint_table()
    run_id = uuid4().hex
    end_versions = _mp_latest_source_versions()
    _MAP_PROCEDURE_ACTIVE_SOURCE_VERSIONS = {MAP_PROCEDURE_SOURCES[source_name]: version for source_name, version in end_versions.items()}
    checkpoints = _mp_checkpoint_versions()
    requires_full = _mp_requires_full_refresh(checkpoints, end_versions)
    if force_full_refresh is True or (force_full_refresh is None and requires_full):
        return _mp_prepare_full_refresh(end_versions, run_id, minimum_full_refresh_source_ratio)
    if force_full_refresh is False and requires_full:
        raise RuntimeError('Incremental mode was explicitly requested, but target/checkpoint state requires a full refresh.')
    try:
        return _mp_prepare_incremental_refresh(checkpoints, end_versions, run_id)
    except MapProcedureCDFGap:
        if not auto_full_refresh_on_cdf_gap:
            raise
        print('[WARN] A source CDF range expired; switching to a guarded full refresh.')
        return _mp_prepare_full_refresh(end_versions, run_id, minimum_full_refresh_source_ratio)

def _mp_overwrite_target(rows: DataFrame) -> None:
    bronze_project_contract(
        rows, MAP_PROCEDURE_TARGET
    ).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('delta.enableChangeDataFeed', 'true').option('delta.enableDeletionVectors', 'true').option('delta.enableRowTracking', 'true').saveAsTable(MAP_PROCEDURE_TARGET)
    _mp_set_target_metadata(apply_column_comments=True)

def _mp_merge_target(rows: DataFrame) -> None:
    rows = bronze_project_contract(rows, MAP_PROCEDURE_TARGET)
    target_columns = list(rows.columns)
    insert_values = {name: 's.' + name for name in target_columns}
    update_values = {
        name: 's.' + name
        for name in target_columns
        if name != 'PROCEDURE_ID'
    }
    comparisons = ' OR '.join(
        f'NOT (t.`{name}` <=> s.`{name}`)'
        for name in target_columns
        if name != 'PROCEDURE_ID'
    )
    DeltaTable.forName(spark, MAP_PROCEDURE_TARGET).alias('t').merge(
        rows.alias('s'),
        't.PROCEDURE_ID <=> s.PROCEDURE_ID',
    ).whenMatchedUpdate(
        condition=comparisons or 'false',
        set=update_values,
    ).whenNotMatchedInsert(values=insert_values).execute()

def _mp_delete_target_ids(delete_ids: DataFrame) -> None:
    keys = delete_ids.select(_mp_checked_long(F.col('PROCEDURE_ID'), 'DELETE.PROCEDURE_ID', True).alias('PROCEDURE_ID')).dropDuplicates(['PROCEDURE_ID'])
    DeltaTable.forName(spark, MAP_PROCEDURE_TARGET).alias('t').merge(keys.alias('d'), 't.PROCEDURE_ID <=> d.PROCEDURE_ID').whenMatchedDelete().execute()

def _mp_validate_full_commit(expected_source_count: int) -> None:
    target = spark.table(MAP_PROCEDURE_TARGET)
    target_count = target.count()
    if target_count != expected_source_count:
        raise RuntimeError('Full map_procedure validation failed: target count ' + str(target_count) + ' does not equal source count ' + str(expected_source_count) + '.')
    duplicate_exists = target.groupBy('PROCEDURE_ID').count().filter(F.col('count') > 1).limit(1).count() > 0
    if duplicate_exists:
        raise RuntimeError('Full map_procedure validation failed: duplicate PROCEDURE_ID values exist.')

def commit_map_procedure_update(plan: Dict) -> Dict:
    global _MAP_PROCEDURE_ACTIVE_SOURCE_VERSIONS
    try:
        if plan['mode'] == 'full':
            _mp_overwrite_target(plan['rows'])
            _mp_validate_full_commit(plan['source_count'])
        else:
            if plan['has_rows']:
                _mp_merge_target(plan['rows'])
            if plan['has_deletes']:
                _mp_delete_target_ids(plan['delete_ids'])
            _mp_set_target_metadata(apply_column_comments=False)
        _mp_write_checkpoint_versions(plan['end_versions'], plan['run_id'])
        print('[SUCCESS] map_procedure ' + plan['mode'] + ' update committed with run_id=' + plan['run_id'] + '.')
        return {'run_id': plan['run_id'], 'mode': plan['mode'], 'checkpoint_versions': plan['end_versions']}
    finally:
        _MAP_PROCEDURE_ACTIVE_SOURCE_VERSIONS = {}
        for frame in plan.get('persisted', []):
            try:
                None
            except Exception:
                pass
        for stage_table in plan.get('scratch_tables', []):
            try:
                _mp_drop_stage(stage_table)
            except Exception as cleanup_exc:
                print('[WARN] map_procedure stage cleanup failed; the next pipeline startup sweep will retry: ' + str(cleanup_exc)[:500])

def run_map_procedure_update(force_full_refresh: Optional[bool]=None, auto_full_refresh_on_cdf_gap: bool=False, minimum_full_refresh_source_ratio: float=0.98) -> Dict:
    global _MAP_PROCEDURE_ACTIVE_SOURCE_VERSIONS
    try:
        plan = prepare_map_procedure_update(force_full_refresh=force_full_refresh, auto_full_refresh_on_cdf_gap=auto_full_refresh_on_cdf_gap, minimum_full_refresh_source_ratio=minimum_full_refresh_source_ratio)
    except Exception:
        _MAP_PROCEDURE_ACTIVE_SOURCE_VERSIONS = {}
        raise
    return commit_map_procedure_update(plan)

def process_procedure_incremental(force_full_refresh: Optional[bool]=None, auto_full_refresh_on_cdf_gap: bool=False) -> Dict:
    return run_map_procedure_update(force_full_refresh=force_full_refresh, auto_full_refresh_on_cdf_gap=auto_full_refresh_on_cdf_gap)

def validate_map_procedure_v2() -> Dict[str, DataFrame]:
    source = _mp_current_procedure_snapshot().select(
        'PROCEDURE_ID', 'PROC_DT_TM', 'PROC_MINUTES'
    )
    target = spark.table(MAP_PROCEDURE_TARGET)
    current_nomenclature = _mp_current_nomenclature_projection().select(
        'NOMENCLATURE_ID',
        F.col('NOMENCLATURE_MAPPING_HASH').alias('_current_mapping_hash'),
    )
    current_encounter = _mp_current_encounter_projection().select(
        'ENCNTR_ID', F.col('PERSON_ID').alias('_current_person_id')
    )
    source_ids = source.select(F.col('PROCEDURE_ID').alias('_source_procedure_id'))
    target_ids = target.select(F.col('PROCEDURE_ID').alias('_target_procedure_id'))
    coverage = source_ids.join(
        target_ids,
        F.col('_source_procedure_id') == F.col('_target_procedure_id'),
        'full',
    ).agg(
        F.count(F.lit(1)).alias('joined_keys'),
        F.sum(F.col('_source_procedure_id').isNotNull().cast('long')).alias('source_keys'),
        F.sum(F.col('_target_procedure_id').isNotNull().cast('long')).alias('target_keys'),
        F.sum(
            (
                F.col('_source_procedure_id').isNotNull()
                & F.col('_target_procedure_id').isNull()
            ).cast('long')
        ).alias('missing_from_target'),
        F.sum(
            (
                F.col('_source_procedure_id').isNull()
                & F.col('_target_procedure_id').isNotNull()
            ).cast('long')
        ).alias('extra_in_target'),
    )
    uniqueness = target.groupBy('PROCEDURE_ID').count().agg(
        F.count(F.lit(1)).alias('distinct_procedure_ids'),
        F.sum((F.col('count') > 1).cast('long')).alias('duplicate_procedure_ids'),
        F.max('count').alias('max_rows_per_procedure_id'),
    )
    fidelity = target.alias('t').join(source.alias('s'), 'PROCEDURE_ID', 'left').agg(
        F.count(F.lit(1)).alias('target_rows'),
        F.sum(F.col('t.PROC_DT_TM').isNull().cast('long')).alias(
            'retained_null_proc_datetime_rows'
        ),
        F.sum((F.col('s.PROC_MINUTES') == F.lit(0.0)).cast('long')).alias(
            'source_zero_minutes_rows'
        ),
        F.sum((F.col('t.PROC_MINUTES') == F.lit(0.0)).cast('long')).alias(
            'target_zero_minutes_rows'
        ),
        F.sum(F.col('t.PROCEDURE_DISPLAY').isNull().cast('long')).alias(
            'missing_procedure_display_rows'
        ),
    )
    drift = (
        target.alias('t')
        .join(current_nomenclature.alias('n'), 'NOMENCLATURE_ID', 'left')
        .join(current_encounter.alias('e'), 'ENCNTR_ID', 'left')
        .agg(
            F.sum(
                (~F.col('t.NOMENCLATURE_MAPPING_HASH').eqNullSafe(
                    F.col('n._current_mapping_hash')
                )).cast('long')
            ).alias('nomenclature_hash_drift_rows'),
            F.sum(
                (~F.col('t.PERSON_ID').eqNullSafe(
                    F.col('e._current_person_id')
                )).cast('long')
            ).alias('person_id_drift_rows'),
            F.sum(F.col('t.MAP_ROW_HASH').isNull().cast('long')).alias(
                'missing_row_hash_rows'
            ),
        )
    )
    return {
        'coverage': coverage,
        'uniqueness': uniqueness,
        'source_fidelity': fidelity,
        'joined_source_drift': drift,
    }
try:
    run_map_procedure_update(force_full_refresh=(True if _pipeline_component_force_full_refresh('map_procedure') else None), auto_full_refresh_on_cdf_gap=True)
    _PIPELINE_UPDATED_TARGETS.extend(['4_prod.bronze.map_procedure'])
    _pipeline_mark_component_complete('map_procedure', ['4_prod.bronze.map_procedure'])
    _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_procedure'})
except Exception as exc:
    _pipeline_record_error(None, exc)
    raise
finally:
    if _pipeline_shared_update_table is not None:
        update_table = _pipeline_shared_update_table
    if _pipeline_shared_table_exists is not None:
        table_exists = _pipeline_shared_table_exists
    if _pipeline_shared_get_max_timestamp is not None:
        get_max_timestamp = _pipeline_shared_get_max_timestamp
    if _pipeline_shared_has_cdf_enabled is not None:
        has_cdf_enabled = _pipeline_shared_has_cdf_enabled
    if _pipeline_shared_get_incremental is not None:
        get_incremental_data_with_cdf = _pipeline_shared_get_incremental

# COMMAND ----------

_pipeline_component_start("map_death")
_pipeline_shared_update_table = globals().get('update_table')
_pipeline_shared_table_exists = globals().get('table_exists')
_pipeline_shared_get_max_timestamp = globals().get('get_max_timestamp')
_pipeline_shared_has_cdf_enabled = globals().get('has_cdf_enabled')
_pipeline_shared_get_incremental = globals().get('get_incremental_data_with_cdf')
globals().pop('_map_updates_original_update_table', None)
globals().pop('_map_medical_personnel_original_update_table', None)
globals().pop('_MAP_UPDATES_ORIGINAL_UPDATE_TABLE', None)

from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
from typing import Dict, Iterable, Optional, Sequence, Tuple
import json
import uuid
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

@dataclass(frozen=True)
class MapDeathConfig:
    target_table: str = '4_prod.bronze.map_death'
    person_table: str = '4_prod.raw.mill_person'
    clinical_event_table: str = '4_prod.raw.mill_clinical_event'
    code_value_table: str = '3_lookup.mill.mill_code_value'
    state_table: str = '6_mgmt.bronze.map_death_pipeline_state'
    audit_table: str = '6_mgmt.bronze.map_death_pipeline_audit'
    deceased_yes_code: int = 3768549
    deceased_yes_code_set: int = 268
    deceased_source_code_set: int = 25513
    deceased_method_code_set: int = 4002029
MAP_DEATH_CONFIG = MapDeathConfig()
MAP_DEATH_SCHEMA_VERSION = '2.0.0'

def _md_source_tables(config: MapDeathConfig) -> Tuple[str, ...]:
    return (config.person_table, config.clinical_event_table, config.code_value_table)
map_death_comment = 'Current-state bronze death foundation at one row per currently deceased PERSON_ID. Raw Millennium death status, precision, lifecycle, cause, identification and provenance fields are retained. Clinical activity and clinically-significant update timestamps are kept as separate concepts. Source changes are tracked by Delta versions rather than a single timestamp watermark.'

def _md_field(name: str, data_type: T.DataType, comment: str, nullable: bool=True) -> T.StructField:
    return T.StructField(name, data_type, nullable, metadata={'comment': comment})
schema_map_death = T.StructType([_md_field('PERSON_ID', T.LongType(), 'Millennium person identifier.', nullable=False), _md_field('DECEASED_DT_TM', T.TimestampType(), 'Recorded date/time of death.'), _md_field('LAST_ENCNTR_DT_TM', T.TimestampType(), 'Last encounter timestamp recorded on PERSON.'), _md_field('LAST_CE_DT_TM', T.TimestampType(), 'Compatibility field: latest non-error clinically-significant update timestamp. This is technical update metadata, not a clinical event date.'), _md_field('CALC_DEATH_DATE', T.TimestampType(), 'Compatibility death timestamp. Uses DECEASED_DT_TM only; encounter and event activity timestamps are not represented as confirmed death dates.'), _md_field('DECEASED_SOURCE_CD', T.IntegerType(), 'Source code for deceased information.'), _md_field('DECEASED_SOURCE_DESC', T.StringType(), 'Description/display for DECEASED_SOURCE_CD when it belongs to expected code set 25513.'), _md_field('DECEASED_ID_METHOD_CD', T.IntegerType(), 'Code for the method by which death was identified.'), _md_field('DECEASED_METHOD_DESC', T.StringType(), 'Description/display for DECEASED_ID_METHOD_CD when it belongs to expected code set 4002029.'), _md_field('ADC_UPDT', T.TimestampType(), 'Greatest meaningful upstream ADC/update timestamp used by this row; not a pipeline watermark.'), _md_field('DECEASED_CD', T.IntegerType(), 'Raw PERSON deceased indicator code.'), _md_field('DECEASED_STATUS_DESC', T.StringType(), 'Resolved deceased indicator description.'), _md_field('DECEASED_STATUS_CODE_SET', T.IntegerType(), 'Code set of DECEASED_CD.'), _md_field('IS_CURRENTLY_DECEASED', T.BooleanType(), 'True when the current PERSON snapshot has the configured deceased indicator.'), _md_field('ACTIVE_IND', T.IntegerType(), 'Raw PERSON active indicator; no active-row filter is applied.'), _md_field('ACTIVE_STATUS_CD', T.IntegerType(), 'Raw PERSON row lifecycle status code.'), _md_field('ACTIVE_STATUS_DESC', T.StringType(), 'Resolved PERSON active status description.'), _md_field('END_EFFECTIVE_DT_TM', T.TimestampType(), 'Date/time after which the PERSON row is no longer effective.'), _md_field('PERSON_STATUS_CD', T.IntegerType(), 'Raw current PERSON status code.'), _md_field('PERSON_STATUS_DESC', T.StringType(), 'Resolved PERSON status description.'), _md_field('DECEASED_TZ', T.LongType(), 'Time-zone code associated with DECEASED_DT_TM.'), _md_field('DECEASED_DT_TM_PREC_FLAG', T.IntegerType(), 'Source precision flag for DECEASED_DT_TM.'), _md_field('DECEASED_DT_TM_PREC_DESC', T.StringType(), 'Decoded death timestamp precision: unknown, full date/time, month, year or day.'), _md_field('LAST_CLINICAL_EVENT_DT_TM', T.TimestampType(), 'Latest EVENT_END_DT_TM across selected latest source versions, without result-status filtering.'), _md_field('LAST_NON_ERROR_CLINICAL_EVENT_DT_TM', T.TimestampType(), 'Latest EVENT_END_DT_TM excluding explicit In Error statuses 29, 30 and 31; null status is retained.'), _md_field('LAST_CE_CLINSIG_UPDT_DT_TM', T.TimestampType(), 'Latest CLINSIG_UPDT_DT_TM across selected latest source versions, without status filtering.'), _md_field('LAST_NON_ERROR_CE_CLINSIG_UPDT_DT_TM', T.TimestampType(), 'Latest CLINSIG_UPDT_DT_TM excluding explicit In Error statuses; null status is retained.'), _md_field('LAST_KNOWN_ACTIVITY_DT_TM', T.TimestampType(), 'Greatest of LAST_ENCNTR_DT_TM and LAST_NON_ERROR_CLINICAL_EVENT_DT_TM. This is activity, not a confirmed death timestamp.'), _md_field('DEATH_DATE_ESTIMATE_DT_TM', T.TimestampType(), 'Best available recorded death or last-known activity timestamp for analytical estimation only.'), _md_field('DEATH_DATE_ESTIMATE_SOURCE', T.StringType(), 'RECORDED_DEATH, LAST_ENCOUNTER, LAST_CLINICAL_EVENT or NONE.'), _md_field('DEATH_DATE_ESTIMATE_IND', T.BooleanType(), 'True when DEATH_DATE_ESTIMATE_DT_TM is based on activity rather than a recorded death.'), _md_field('DECEASED_SOURCE_CODE_SET', T.IntegerType(), 'Actual code set resolved for DECEASED_SOURCE_CD.'), _md_field('DECEASED_SOURCE_CODE_VALID_IND', T.BooleanType(), 'Whether the non-zero source code belongs to expected code set 25513; null when code is missing.'), _md_field('DECEASED_ID_METHOD_CODE_SET', T.IntegerType(), 'Actual code set resolved for DECEASED_ID_METHOD_CD.'), _md_field('DECEASED_ID_METHOD_CODE_VALID_IND', T.BooleanType(), 'Whether the non-zero method code belongs to expected code set 4002029; null when code is missing.'), _md_field('DECEASED_NOTIFY_SOURCE_CD', T.IntegerType(), 'Raw code describing who or what notified the organisation of the death.'), _md_field('DECEASED_NOTIFY_SOURCE_DESC', T.StringType(), 'Resolved notification-source description/display.'), _md_field('DECEASED_NOTIFY_SOURCE_CODE_SET', T.IntegerType(), 'Actual code set resolved for DECEASED_NOTIFY_SOURCE_CD.'), _md_field('CAUSE_OF_DEATH', T.StringType(), 'Raw free-text cause of death.'), _md_field('CAUSE_OF_DEATH_CD', T.IntegerType(), 'Raw codified cause of death.'), _md_field('CAUSE_OF_DEATH_DESC', T.StringType(), 'Resolved cause-of-death description/display.'), _md_field('CAUSE_OF_DEATH_CODE_SET', T.IntegerType(), 'Actual code set resolved for CAUSE_OF_DEATH_CD.'), _md_field('AUTOPSY_CD', T.IntegerType(), 'Raw autopsy indicator code.'), _md_field('AUTOPSY_DESC', T.StringType(), 'Resolved autopsy code description/display.'), _md_field('AUTOPSY_CODE_SET', T.IntegerType(), 'Actual code set resolved for AUTOPSY_CD.'), _md_field('AGE_AT_DEATH', T.DoubleType(), 'Raw reported age at death; source zero values are retained.'), _md_field('AGE_AT_DEATH_UNIT_CD', T.IntegerType(), 'Raw unit code for AGE_AT_DEATH.'), _md_field('AGE_AT_DEATH_UNIT_DESC', T.StringType(), 'Resolved age-at-death unit description/display.'), _md_field('AGE_AT_DEATH_UNIT_CODE_SET', T.IntegerType(), 'Actual code set of AGE_AT_DEATH_UNIT_CD.'), _md_field('AGE_AT_DEATH_PREC_MOD_FLAG', T.IntegerType(), 'Raw precision modifier for AGE_AT_DEATH.'), _md_field('AGE_AT_DEATH_PREC_MOD_DESC', T.StringType(), 'Decoded age precision modifier: unknown, less than, greater than, approximate or exact.'), _md_field('CONTRIBUTOR_SYSTEM_CD', T.IntegerType(), 'Raw source contributor-system code.'), _md_field('CONTRIBUTOR_SYSTEM_DESC', T.StringType(), 'Resolved contributor-system description/display.'), _md_field('CONTRIBUTOR_SYSTEM_CODE_SET', T.IntegerType(), 'Actual code set of CONTRIBUTOR_SYSTEM_CD.'), _md_field('INST_ID', T.LongType(), 'Raw source instance identifier.'), _md_field('LOGICAL_DOMAIN_ID', T.LongType(), 'Raw source logical-domain identifier.'), _md_field('PERSON_UPDT_DT_TM', T.TimestampType(), 'Millennium application update timestamp on PERSON.'), _md_field('PERSON_UPDT_CNT', T.LongType(), 'Millennium update counter on PERSON.'), _md_field('PERSON_LAST_UTC_TS', T.TimestampType(), 'Raw PERSON LAST_UTC_TS.'), _md_field('PERSON_ADC_UPDT', T.TimestampType(), 'ADC ingestion/update timestamp from PERSON.'), _md_field('CLINICAL_EVENT_ADC_UPDT', T.TimestampType(), 'Greatest ADC_UPDT among selected clinical events for the person.'), _md_field('LOOKUP_ADC_UPDT', T.TimestampType(), 'Greatest ADC_UPDT among code-value rows used by this record.'), _md_field('CLINICAL_EVENT_COUNT', T.LongType(), 'Count of selected latest logical clinical-event source states for the person.'), _md_field('CLINICAL_EVENT_IN_ERROR_COUNT', T.LongType(), 'Count of selected latest event states with result status 29, 30 or 31.'), _md_field('CLINICAL_EVENT_NULL_RESULT_STATUS_COUNT', T.LongType(), 'Count of selected latest event states with null RESULT_STATUS_CD; these are not silently discarded.'), _md_field('PERSON_CDF_COMMIT_VERSION', T.LongType(), 'Latest PERSON CDF version triggering this rebuild.'), _md_field('PERSON_CDF_COMMIT_TIMESTAMP', T.TimestampType(), 'Latest PERSON CDF commit timestamp triggering this rebuild.'), _md_field('CLINICAL_EVENT_CDF_COMMIT_VERSION', T.LongType(), 'Latest CLINICAL_EVENT CDF version triggering this rebuild.'), _md_field('CLINICAL_EVENT_CDF_COMMIT_TIMESTAMP', T.TimestampType(), 'Latest CLINICAL_EVENT CDF commit timestamp triggering this rebuild.'), _md_field('TRIGGER_SOURCES', T.StringType(), 'Comma-separated source names that caused this person to be rebuilt.'), _md_field('PERSON_SOURCE_VERSION', T.LongType(), 'PERSON Delta version used for this row build.'), _md_field('CLINICAL_EVENT_SOURCE_VERSION', T.LongType(), 'CLINICAL_EVENT Delta version used for this row build.'), _md_field('CODE_VALUE_SOURCE_VERSION', T.LongType(), 'CODE_VALUE Delta version used for this row build.'), _md_field('ROW_HASH', T.LongType(), 'Hash of source-derived row content used to avoid unchanged rewrites.'), _md_field('PIPELINE_RUN_ID', T.StringType(), 'Run identifier that last materially changed the target row.'), _md_field('PIPELINE_UPDT_DT_TM', T.TimestampType(), 'Pipeline timestamp when this target row was inserted or materially updated.')])

def _md_sql_identifier(table_name: str) -> str:
    tick = chr(96)
    return '.'.join((f'{tick}{part.replace(tick, tick + tick)}{tick}' for part in table_name.split('.')))

def _md_escape_sql_string(value: str) -> str:
    return value.replace('\\', '\\\\').replace("'", "''")

def _md_table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _md_stage_name(config: MapDeathConfig, run_id: str, kind: str) -> str:
    token = ''.join((character for character in run_id if character.isalnum()))[:24]
    return f'{config.target_table}__{kind}_stage_{token}'

def _md_materialize_stage(rows: DataFrame, stage_table: str) -> DataFrame:
    rows.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(stage_table)
    return spark.table(stage_table)

def _md_drop_stage(stage_table: str) -> None:
    spark.sql(f'DROP TABLE IF EXISTS {_md_sql_identifier(stage_table)}')

def _md_latest_delta_version(table_name: str) -> int:
    row = spark.sql(f'DESCRIBE HISTORY {_md_sql_identifier(table_name)} LIMIT 1').select('version').first()
    if row is None:
        raise RuntimeError(f'No Delta history returned for {table_name}')
    return int(row['version'])

def _md_capture_source_versions(config: MapDeathConfig) -> Dict[str, int]:
    return {source: _md_latest_delta_version(source) for source in _md_source_tables(config)}

def _md_read_snapshot(table_name: str, version: int) -> DataFrame:
    return _m20_hardening_read_pinned_snapshot(table_name, int(version))

def _md_read_cdf(table_name: str, starting_version: int, ending_version: int) -> Optional[DataFrame]:
    if starting_version > ending_version:
        return None
    try:
        changes = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', int(starting_version)).option('endingVersion', int(ending_version)).table(table_name)
        return _m20_hardening_validate_cdf(changes)
    except Exception as exc:
        raise RuntimeError(
            f'CDF could not be read for {table_name} from version '
            f'{starting_version} through {ending_version}; a full rebuild is required.'
        ) from exc

def _md_checked_double_long(column: F.Column, label: str, required: bool=False) -> F.Column:
    # Value path is decimal-only; see the diagnosis-lane copy of this helper. This copy
    # previously returned as_double.cast(long) rather than column.cast(long), so it was
    # the one variant that also rounded values between 2**53 and the range limit. It
    # still yields a long, but the long is now produced from the exact decimal.
    as_double = column.cast('double')
    as_decimal = column.cast('decimal(38,0)')
    null_result = F.lit(None) if required else F.lit(None).cast('long')
    in_long_range = as_decimal.between(F.lit(-9223372036854775808).cast('decimal(38,0)'), F.lit(9223372036854775807).cast('decimal(38,0)'))
    return F.when(column.isNull(), null_result).when(F.isnan(as_double), F.lit(None)).when(as_double != F.floor(as_double), F.lit(None)).when(F.abs(as_double) > F.lit(9223372036854775807), F.lit(None)).when(~in_long_range, F.lit(None)).otherwise(as_decimal.cast('long'))

def _md_checked_int(column: F.Column, label: str) -> F.Column:
    as_double = column.cast('double')
    return F.when(column.isNull(), F.lit(None).cast('int')).when(F.isnan(as_double), F.lit(None)).when(as_double != F.floor(as_double), F.lit(None)).when((as_double < F.lit(-2147483648)) | (as_double > F.lit(2147483647)), F.lit(None)).otherwise(as_double.cast('int'))

def _md_nonblank(column: F.Column) -> F.Column:
    return F.when(F.length(F.trim(column.cast('string'))) > 0, column.cast('string'))

def _md_stable_hash(columns: Sequence[F.Column]) -> F.Column:
    return F.xxhash64(*[F.coalesce(column.cast('string'), F.lit('<NULL>')) for column in columns])

def _md_align_to_schema(df: DataFrame, schema: T.StructType) -> DataFrame:
    missing = [field.name for field in schema.fields if field.name not in df.columns]
    if missing:
        raise RuntimeError(f'map_death output is missing required columns: {missing}')
    return df.select(*[F.col(field.name).cast(field.dataType).alias(field.name, metadata=field.metadata) for field in schema.fields])

def _md_empty_person_ids() -> DataFrame:
    return spark.createDataFrame([], 'PERSON_ID long')

def _md_union_by_name(frames: Iterable[DataFrame]) -> DataFrame:
    frames = list(frames)
    if not frames:
        return _md_empty_person_ids()
    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), frames)

def _md_ensure_metadata_tables(config: MapDeathConfig) -> None:
    spark.sql(f'\n        CREATE TABLE IF NOT EXISTS {_md_sql_identifier(config.state_table)} (\n            source_table STRING,\n            last_processed_version BIGINT,\n            last_processed_at TIMESTAMP,\n            last_run_id STRING\n        )\n        USING DELTA\n    ')
    spark.sql(f'\n        CREATE TABLE IF NOT EXISTS {_md_sql_identifier(config.audit_table)} (\n            run_id STRING,\n            event_timestamp TIMESTAMP,\n            status STRING,\n            mode STRING,\n            message STRING,\n            source_versions_json STRING,\n            metrics_json STRING\n        )\n        USING DELTA\n    ')

def _md_read_pipeline_state(config: MapDeathConfig) -> Dict[str, int]:
    if not _md_table_exists(config.state_table):
        return {}
    return {row['source_table']: int(row['last_processed_version']) for row in spark.table(config.state_table).collect() if row['source_table'] is not None and row['last_processed_version'] is not None}

def _md_write_pipeline_state(config: MapDeathConfig, source_versions: Dict[str, int], run_id: str, run_timestamp: datetime) -> None:
    state_schema = T.StructType([T.StructField('source_table', T.StringType(), False), T.StructField('last_processed_version', T.LongType(), False), T.StructField('last_processed_at', T.TimestampType(), False), T.StructField('last_run_id', T.StringType(), False)])
    rows = [(source, int(version), run_timestamp, run_id) for source, version in source_versions.items()]
    source_df = spark.createDataFrame(rows, state_schema)
    DeltaTable.forName(spark, config.state_table).alias('t').merge(source_df.alias('s'), 't.source_table = s.source_table').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def _md_write_audit_event(config: MapDeathConfig, run_id: str, status: str, mode: str, message: str, source_versions: Optional[Dict[str, int]]=None, metrics: Optional[Dict[str, object]]=None) -> None:
    schema = T.StructType([T.StructField('run_id', T.StringType(), False), T.StructField('event_timestamp', T.TimestampType(), False), T.StructField('status', T.StringType(), False), T.StructField('mode', T.StringType(), False), T.StructField('message', T.StringType(), True), T.StructField('source_versions_json', T.StringType(), True), T.StructField('metrics_json', T.StringType(), True)])
    row = [(run_id, datetime.now(timezone.utc), status, mode, message, json.dumps(source_versions or {}, sort_keys=True), json.dumps(metrics or {}, sort_keys=True, default=str))]
    spark.createDataFrame(row, schema).write.format('delta').mode('append').saveAsTable(config.audit_table)

def _md_table_description(table_name: str) -> Optional[str]:
    if not _md_table_exists(table_name):
        return None
    row = spark.sql(f'DESCRIBE DETAIL {_md_sql_identifier(table_name)}').select('description').first()
    return row['description'] if row else None

def _md_get_table_property(table_name: str, property_name: str) -> Optional[str]:
    if not _md_table_exists(table_name):
        return None
    escaped = _md_escape_sql_string(property_name)
    rows = spark.sql(f"SHOW TBLPROPERTIES {_md_sql_identifier(table_name)} ('{escaped}')").collect()
    if not rows:
        return None
    value = rows[0]['value']
    if value is None or str(value).startswith('Table '):
        return None
    return str(value)

def _md_apply_target_metadata(config: MapDeathConfig) -> None:
    current_description = _md_table_description(config.target_table)
    if current_description != map_death_comment:
        escaped = _md_escape_sql_string(map_death_comment)
        spark.sql(f"COMMENT ON TABLE {_md_sql_identifier(config.target_table)} IS '{escaped}'")
    desired_properties = {'delta.enableChangeDataFeed': 'true', 'delta.enableDeletionVectors': 'true', 'map.schema.version': MAP_DEATH_SCHEMA_VERSION}
    changed = {key: value for key, value in desired_properties.items() if _md_get_table_property(config.target_table, key) != value}
    if changed:
        assignments = ', '.join((f"'{_md_escape_sql_string(key)}' = '{_md_escape_sql_string(value)}'" for key, value in changed.items()))
        spark.sql(f'ALTER TABLE {_md_sql_identifier(config.target_table)} SET TBLPROPERTIES ({assignments})')

def _md_assert_source_preflight(config: MapDeathConfig, require_cdf: bool=True) -> None:
    missing = [table_name for table_name in _md_source_tables(config) if not _md_table_exists(table_name)]
    if missing:
        raise RuntimeError(f'Required map_death source tables are missing: {missing}')
    if require_cdf:
        for table_name in _md_source_tables(config):
            if _md_get_table_property(table_name, 'delta.enableChangeDataFeed') != 'true':
                raise RuntimeError(f'Delta Change Data Feed must be enabled on {table_name}. A full canonical rebuild remains available without CDF.')

def _md_assert_incremental_target_schema(config: MapDeathConfig) -> None:
    if not _md_table_exists(config.target_table):
        raise RuntimeError('Incremental mode requires an existing map_death target')
    current = {field.name: field.dataType for field in spark.table(config.target_table).schema.fields}
    expected_fields = bronze_contract_schema(config.target_table).fields
    missing = [
        field.name for field in expected_fields
        if field.name not in current
    ]
    changed = [
        (field.name, current.get(field.name), field.dataType)
        for field in expected_fields
        if field.name in current and current[field.name] != field.dataType
    ]
    if missing or changed:
        raise RuntimeError(f'map_death target schema does not match the replacement. Run process_death_incremental(force_full_rebuild=True). Missing columns: {missing}; changed types: {changed}')

def _md_person_snapshot(config: MapDeathConfig, source_version: int) -> DataFrame:
    raw = _md_read_snapshot(config.person_table, source_version)
    selected = raw.select(_md_checked_double_long(F.col('PERSON_ID'), 'PERSON.PERSON_ID', required=True).alias('PERSON_ID'), _md_checked_int(F.col('DECEASED_CD'), 'PERSON.DECEASED_CD').alias('DECEASED_CD'), F.col('DECEASED_DT_TM').cast('timestamp').alias('DECEASED_DT_TM'), F.col('LAST_ENCNTR_DT_TM').cast('timestamp').alias('LAST_ENCNTR_DT_TM'), _md_checked_int(F.col('DECEASED_SOURCE_CD'), 'PERSON.DECEASED_SOURCE_CD').alias('DECEASED_SOURCE_CD'), _md_checked_int(F.col('DECEASED_ID_METHOD_CD'), 'PERSON.DECEASED_ID_METHOD_CD').alias('DECEASED_ID_METHOD_CD'), _md_checked_int(F.col('DECEASED_NOTIFY_SOURCE_CD'), 'PERSON.DECEASED_NOTIFY_SOURCE_CD').alias('DECEASED_NOTIFY_SOURCE_CD'), F.col('CAUSE_OF_DEATH').cast('string').alias('CAUSE_OF_DEATH'), _md_checked_int(F.col('CAUSE_OF_DEATH_CD'), 'PERSON.CAUSE_OF_DEATH_CD').alias('CAUSE_OF_DEATH_CD'), _md_checked_int(F.col('AUTOPSY_CD'), 'PERSON.AUTOPSY_CD').alias('AUTOPSY_CD'), F.col('AGE_AT_DEATH').cast('double').alias('AGE_AT_DEATH'), _md_checked_int(F.col('AGE_AT_DEATH_UNIT_CD'), 'PERSON.AGE_AT_DEATH_UNIT_CD').alias('AGE_AT_DEATH_UNIT_CD'), _md_checked_int(F.col('AGE_AT_DEATH_PREC_MOD_FLAG'), 'PERSON.AGE_AT_DEATH_PREC_MOD_FLAG').alias('AGE_AT_DEATH_PREC_MOD_FLAG'), _md_checked_double_long(F.col('DECEASED_TZ'), 'PERSON.DECEASED_TZ').alias('DECEASED_TZ'), _md_checked_int(F.col('DECEASED_DT_TM_PREC_FLAG'), 'PERSON.DECEASED_DT_TM_PREC_FLAG').alias('DECEASED_DT_TM_PREC_FLAG'), _md_checked_int(F.col('ACTIVE_IND'), 'PERSON.ACTIVE_IND').alias('ACTIVE_IND'), _md_checked_int(F.col('ACTIVE_STATUS_CD'), 'PERSON.ACTIVE_STATUS_CD').alias('ACTIVE_STATUS_CD'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('END_EFFECTIVE_DT_TM'), _md_checked_int(F.col('PERSON_STATUS_CD'), 'PERSON.PERSON_STATUS_CD').alias('PERSON_STATUS_CD'), _md_checked_int(F.col('CONTRIBUTOR_SYSTEM_CD'), 'PERSON.CONTRIBUTOR_SYSTEM_CD').alias('CONTRIBUTOR_SYSTEM_CD'), _md_checked_double_long(F.col('INST_ID'), 'PERSON.INST_ID').alias('INST_ID'), _md_checked_double_long(F.col('LOGICAL_DOMAIN_ID'), 'PERSON.LOGICAL_DOMAIN_ID').alias('LOGICAL_DOMAIN_ID'), F.col('UPDT_DT_TM').cast('timestamp').alias('PERSON_UPDT_DT_TM'), _md_checked_double_long(F.col('UPDT_CNT'), 'PERSON.UPDT_CNT').alias('PERSON_UPDT_CNT'), F.col('LAST_UTC_TS').cast('timestamp').alias('PERSON_LAST_UTC_TS'), F.col('ADC_UPDT').cast('timestamp').alias('PERSON_ADC_UPDT'))
    latest_window = Window.partitionBy('PERSON_ID').orderBy(F.col('PERSON_ADC_UPDT').desc_nulls_last(), F.col('PERSON_UPDT_DT_TM').desc_nulls_last(), F.col('PERSON_UPDT_CNT').desc_nulls_last())
    return selected.withColumn('_PERSON_ROW_NUMBER', F.row_number().over(latest_window)).filter(F.col('_PERSON_ROW_NUMBER') == 1).drop('_PERSON_ROW_NUMBER')

def _md_current_deceased_persons(person_snapshot: DataFrame, config: MapDeathConfig) -> DataFrame:
    # Cohort = persons whose current DECEASED_CD is exactly the configured deceased-yes
    # code. DECEASED_CD IS NULL means the source has never asserted any death status for
    # that person, so they are deliberately NOT in the death cohort.
    # The bare == already dropped those rows, but silently, through three-valued logic.
    # The explicit isNotNull() below selects exactly the same rows and makes the decision
    # visible: a NULL indicator is treated as "not asserted deceased", never as deceased,
    # and never as a row that needs review. Selection is unchanged by this edit.
    is_deceased = F.col('DECEASED_CD').isNotNull() & (F.col('DECEASED_CD') == F.lit(config.deceased_yes_code))
    return person_snapshot.filter(is_deceased)

def _md_existing_target_ids(config: MapDeathConfig) -> DataFrame:
    if not _md_table_exists(config.target_table):
        return _md_empty_person_ids()
    return spark.table(config.target_table).select(F.col('PERSON_ID').cast('long').alias('PERSON_ID')).filter(F.col('PERSON_ID').isNotNull()).dropDuplicates()
_MD_PERSON_CODE_COLUMNS = ('DECEASED_CD', 'DECEASED_SOURCE_CD', 'DECEASED_ID_METHOD_CD', 'DECEASED_NOTIFY_SOURCE_CD', 'CAUSE_OF_DEATH_CD', 'AUTOPSY_CD', 'AGE_AT_DEATH_UNIT_CD', 'ACTIVE_STATUS_CD', 'PERSON_STATUS_CD', 'CONTRIBUTOR_SYSTEM_CD')

def _md_needed_code_values(person_rows: DataFrame) -> DataFrame:
    code_array = F.array(*[F.col(column_name).cast('long') for column_name in _MD_PERSON_CODE_COLUMNS])
    return person_rows.select(F.explode_outer(code_array).alias('CODE_VALUE_LONG')).filter(F.col('CODE_VALUE_LONG').isNotNull() & (F.col('CODE_VALUE_LONG') != 0)).dropDuplicates()

def _md_code_lookup(config: MapDeathConfig, source_version: int, person_rows: DataFrame) -> DataFrame:
    needed = _md_needed_code_values(person_rows)
    raw = _md_read_snapshot(config.code_value_table, source_version)
    selected = raw.select(_md_checked_double_long(F.col('CODE_VALUE'), 'CODE_VALUE.CODE_VALUE', required=True).alias('CODE_VALUE_LONG'), _md_checked_int(F.col('CODE_SET'), 'CODE_VALUE.CODE_SET').alias('CODE_SET'), _md_nonblank(F.col('DESCRIPTION')).alias('_DESCRIPTION'), _md_nonblank(F.col('DISPLAY')).alias('_DISPLAY'), _md_nonblank(F.col('CDF_MEANING')).alias('_CDF_MEANING'), _md_nonblank(F.col('DEFINITION')).alias('_DEFINITION'), _md_checked_int(F.col('ACTIVE_IND'), 'CODE_VALUE.ACTIVE_IND').alias('_ACTIVE_IND'), F.col('UPDT_DT_TM').cast('timestamp').alias('_LOOKUP_UPDT_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('LOOKUP_ADC_UPDT'))
    restricted = selected.join(F.broadcast(needed), 'CODE_VALUE_LONG', 'inner')
    latest_window = Window.partitionBy('CODE_VALUE_LONG').orderBy(F.col('_ACTIVE_IND').desc_nulls_last(), F.col('LOOKUP_ADC_UPDT').desc_nulls_last(), F.col('_LOOKUP_UPDT_DT_TM').desc_nulls_last())
    return restricted.withColumn('_CODE_ROW_NUMBER', F.row_number().over(latest_window)).filter(F.col('_CODE_ROW_NUMBER') == 1).select('CODE_VALUE_LONG', 'CODE_SET', F.coalesce(F.col('_DESCRIPTION'), F.col('_DISPLAY'), F.col('_CDF_MEANING'), F.col('_DEFINITION')).alias('CODE_DESC'), 'LOOKUP_ADC_UPDT')

def _md_lookup_alias(lookup: DataFrame, prefix: str) -> DataFrame:
    return F.broadcast(lookup.select(F.col('CODE_VALUE_LONG').alias(f'{prefix}_LOOKUP_CODE'), F.col('CODE_SET').alias(f'{prefix}_CODE_SET'), F.col('CODE_DESC').alias(f'{prefix}_DESC'), F.col('LOOKUP_ADC_UPDT').alias(f'{prefix}_LOOKUP_ADC_UPDT')))

def _md_join_lookup(df: DataFrame, lookup: DataFrame, source_column: str, prefix: str) -> DataFrame:
    aliased = _md_lookup_alias(lookup, prefix)
    return df.join(aliased, F.col(source_column).cast('long') == F.col(f'{prefix}_LOOKUP_CODE'), 'left')

def create_death_code_lookups(code_values: Optional[DataFrame]=None, config: MapDeathConfig=MAP_DEATH_CONFIG) -> Tuple[DataFrame, DataFrame]:
    """
    Compatibility helper returning source and method lookup DataFrames.

    Unlike the previous helper, descriptions use DESCRIPTION, DISPLAY, CDF_MEANING and
    DEFINITION fallbacks and include code-set validity. Invalid cross-code-set values are
    retained but do not receive a misleading death-source/method description.
    """
    raw = code_values or spark.table(config.code_value_table)
    normalized = raw.select(_md_checked_double_long(F.col('CODE_VALUE'), 'CODE_VALUE.CODE_VALUE', required=True).alias('CODE_VALUE_LONG'), _md_checked_int(F.col('CODE_SET'), 'CODE_VALUE.CODE_SET').alias('CODE_SET'), F.coalesce(_md_nonblank(F.col('DESCRIPTION')), _md_nonblank(F.col('DISPLAY')), _md_nonblank(F.col('CDF_MEANING')), _md_nonblank(F.col('DEFINITION'))).alias('CODE_DESC'), F.col('ADC_UPDT').cast('timestamp').alias('LOOKUP_ADC_UPDT')).dropDuplicates(['CODE_VALUE_LONG'])
    source = normalized.select(F.col('CODE_VALUE_LONG').alias('CODE_VALUE'), 'CODE_SET', F.when(F.col('CODE_SET') == F.lit(config.deceased_source_code_set), F.col('CODE_DESC')).alias('deceased_source_desc'), (F.col('CODE_SET') == F.lit(config.deceased_source_code_set)).alias('deceased_source_code_valid_ind'), 'LOOKUP_ADC_UPDT')
    method = normalized.select(F.col('CODE_VALUE_LONG').alias('CODE_VALUE'), 'CODE_SET', F.when(F.col('CODE_SET') == F.lit(config.deceased_method_code_set), F.col('CODE_DESC')).alias('deceased_method_desc'), (F.col('CODE_SET') == F.lit(config.deceased_method_code_set)).alias('deceased_method_code_valid_ind'), 'LOOKUP_ADC_UPDT')
    return (source, method)

def _md_clinical_event_aggregate(config: MapDeathConfig, source_version: int, person_ids: DataFrame, run_timestamp: datetime) -> DataFrame:
    keys = person_ids.select(F.col('PERSON_ID').cast('long').alias('PERSON_ID')).filter(F.col('PERSON_ID').isNotNull()).dropDuplicates()
    raw = _md_read_snapshot(config.clinical_event_table, source_version)
    selected = raw.select(F.col('PERSON_ID').cast('long').alias('PERSON_ID'), F.col('EVENT_ID').cast('long').alias('EVENT_ID'), F.col('CLINICAL_EVENT_ID').cast('long').alias('CLINICAL_EVENT_ID'), F.col('VALID_FROM_DT_TM').cast('timestamp').alias('VALID_FROM_DT_TM'), F.col('VALID_UNTIL_DT_TM').cast('timestamp').alias('VALID_UNTIL_DT_TM'), F.col('UPDT_DT_TM').cast('timestamp').alias('CE_UPDT_DT_TM'), F.col('UPDT_CNT').cast('long').alias('CE_UPDT_CNT'), F.col('ADC_UPDT').cast('timestamp').alias('CE_ADC_UPDT'), F.col('EVENT_END_DT_TM').cast('timestamp').alias('EVENT_END_DT_TM'), F.col('CLINSIG_UPDT_DT_TM').cast('timestamp').alias('CLINSIG_UPDT_DT_TM'), F.col('RESULT_STATUS_CD').cast('long').alias('RESULT_STATUS_CD')).join(F.broadcast(keys), 'PERSON_ID', 'inner')
    event_key = F.when(F.col('EVENT_ID').isNotNull(), F.concat(F.lit('EVENT:'), F.col('EVENT_ID').cast('string'))).otherwise(F.concat(F.lit('ROW:'), F.col('CLINICAL_EVENT_ID').cast('string')))
    selected = selected.withColumn('_EVENT_KEY', event_key).withColumn('_OPEN_OR_CURRENT_IND', F.when(F.col('VALID_UNTIL_DT_TM').isNull() | (F.col('VALID_UNTIL_DT_TM') > F.lit(run_timestamp)), F.lit(1)).otherwise(F.lit(0)))
    latest_window = Window.partitionBy('PERSON_ID', '_EVENT_KEY').orderBy(F.col('_OPEN_OR_CURRENT_IND').desc(), F.col('VALID_UNTIL_DT_TM').desc_nulls_last(), F.col('VALID_FROM_DT_TM').desc_nulls_last(), F.col('CE_ADC_UPDT').desc_nulls_last(), F.col('CE_UPDT_DT_TM').desc_nulls_last(), F.col('CE_UPDT_CNT').desc_nulls_last(), F.col('CLINICAL_EVENT_ID').desc_nulls_last())
    latest = selected.withColumn('_CE_ROW_NUMBER', F.row_number().over(latest_window)).filter(F.col('_CE_ROW_NUMBER') == 1)
    in_error = F.col('RESULT_STATUS_CD').isin(29, 30, 31)
    non_error_or_unknown = F.col('RESULT_STATUS_CD').isNull() | ~in_error
    return latest.groupBy('PERSON_ID').agg(F.max('EVENT_END_DT_TM').alias('LAST_CLINICAL_EVENT_DT_TM'), F.max(F.when(non_error_or_unknown, F.col('EVENT_END_DT_TM'))).alias('LAST_NON_ERROR_CLINICAL_EVENT_DT_TM'), F.max('CLINSIG_UPDT_DT_TM').alias('LAST_CE_CLINSIG_UPDT_DT_TM'), F.max(F.when(non_error_or_unknown, F.col('CLINSIG_UPDT_DT_TM'))).alias('LAST_NON_ERROR_CE_CLINSIG_UPDT_DT_TM'), F.max('CE_ADC_UPDT').alias('CLINICAL_EVENT_ADC_UPDT'), F.count(F.lit(1)).cast('long').alias('CLINICAL_EVENT_COUNT'), F.sum(F.when(in_error, F.lit(1)).otherwise(F.lit(0))).cast('long').alias('CLINICAL_EVENT_IN_ERROR_COUNT'), F.sum(F.when(F.col('RESULT_STATUS_CD').isNull(), F.lit(1)).otherwise(F.lit(0))).cast('long').alias('CLINICAL_EVENT_NULL_RESULT_STATUS_COUNT'))

def get_last_clinical_events_for_death(person_ids: Optional[DataFrame]=None, source_version: Optional[int]=None, config: MapDeathConfig=MAP_DEATH_CONFIG) -> DataFrame:
    """
    Compatibility helper for the original function name.

    The returned LAST_CE_DT_TM remains a clinically-significant update timestamp for
    compatibility. LAST_CLINICAL_EVENT_DT_TM is the actual clinically relevant event date.
    """
    version = int(source_version) if source_version is not None else _md_latest_delta_version(config.clinical_event_table)
    if person_ids is None:
        person_version = _md_latest_delta_version(config.person_table)
        person_ids = _md_current_deceased_persons(_md_person_snapshot(config, person_version), config).select('PERSON_ID')
    aggregate = _md_clinical_event_aggregate(config, version, person_ids, datetime.now(timezone.utc))
    return aggregate.withColumn('LAST_CE_DT_TM', F.col('LAST_NON_ERROR_CE_CLINSIG_UPDT_DT_TM'))
_MD_TRIGGER_SCHEMA = T.StructType([T.StructField('PERSON_ID', T.LongType(), False), T.StructField('TRIGGER_SOURCE', T.StringType(), False), T.StructField('PERSON_CDF_COMMIT_VERSION', T.LongType(), True), T.StructField('PERSON_CDF_COMMIT_TIMESTAMP', T.TimestampType(), True), T.StructField('CLINICAL_EVENT_CDF_COMMIT_VERSION', T.LongType(), True), T.StructField('CLINICAL_EVENT_CDF_COMMIT_TIMESTAMP', T.TimestampType(), True)])

def _md_empty_trigger_frame() -> DataFrame:
    return spark.createDataFrame([], _MD_TRIGGER_SCHEMA)

def _md_person_cdf_triggers(config: MapDeathConfig, previous_version: int, current_version: int) -> DataFrame:
    cdf = _md_read_cdf(config.person_table, previous_version + 1, current_version)
    if cdf is None:
        return _md_empty_trigger_frame()
    return cdf.select(_md_checked_double_long(F.col('PERSON_ID'), 'PERSON CDF.PERSON_ID', required=True).alias('PERSON_ID'), F.col('_commit_version').cast('long').alias('_COMMIT_VERSION'), F.col('_commit_timestamp').cast('timestamp').alias('_COMMIT_TIMESTAMP')).groupBy('PERSON_ID').agg(F.max('_COMMIT_VERSION').alias('PERSON_CDF_COMMIT_VERSION'), F.max('_COMMIT_TIMESTAMP').alias('PERSON_CDF_COMMIT_TIMESTAMP')).select('PERSON_ID', F.lit('PERSON').alias('TRIGGER_SOURCE'), 'PERSON_CDF_COMMIT_VERSION', 'PERSON_CDF_COMMIT_TIMESTAMP', F.lit(None).cast('long').alias('CLINICAL_EVENT_CDF_COMMIT_VERSION'), F.lit(None).cast('timestamp').alias('CLINICAL_EVENT_CDF_COMMIT_TIMESTAMP'))

def _md_clinical_event_cdf_triggers(config: MapDeathConfig, previous_version: int, current_version: int) -> DataFrame:
    cdf = _md_read_cdf(config.clinical_event_table, previous_version + 1, current_version)
    if cdf is None:
        return _md_empty_trigger_frame()
    return cdf.select(F.col('PERSON_ID').cast('long').alias('PERSON_ID'), F.col('_commit_version').cast('long').alias('_COMMIT_VERSION'), F.col('_commit_timestamp').cast('timestamp').alias('_COMMIT_TIMESTAMP')).filter(F.col('PERSON_ID').isNotNull()).groupBy('PERSON_ID').agg(F.max('_COMMIT_VERSION').alias('CLINICAL_EVENT_CDF_COMMIT_VERSION'), F.max('_COMMIT_TIMESTAMP').alias('CLINICAL_EVENT_CDF_COMMIT_TIMESTAMP')).select('PERSON_ID', F.lit('CLINICAL_EVENT').alias('TRIGGER_SOURCE'), F.lit(None).cast('long').alias('PERSON_CDF_COMMIT_VERSION'), F.lit(None).cast('timestamp').alias('PERSON_CDF_COMMIT_TIMESTAMP'), 'CLINICAL_EVENT_CDF_COMMIT_VERSION', 'CLINICAL_EVENT_CDF_COMMIT_TIMESTAMP')

def _md_changed_code_values(config: MapDeathConfig, previous_version: int, current_version: int) -> DataFrame:
    cdf = _md_read_cdf(config.code_value_table, previous_version + 1, current_version)
    if cdf is None:
        return spark.createDataFrame([], 'CODE_VALUE_LONG long, CODE_CDF_COMMIT_VERSION long, CODE_CDF_COMMIT_TIMESTAMP timestamp')
    return cdf.select(_md_checked_double_long(F.col('CODE_VALUE'), 'CODE_VALUE CDF.CODE_VALUE', required=True).alias('CODE_VALUE_LONG'), F.col('_commit_version').cast('long').alias('_COMMIT_VERSION'), F.col('_commit_timestamp').cast('timestamp').alias('_COMMIT_TIMESTAMP')).groupBy('CODE_VALUE_LONG').agg(F.max('_COMMIT_VERSION').alias('CODE_CDF_COMMIT_VERSION'), F.max('_COMMIT_TIMESTAMP').alias('CODE_CDF_COMMIT_TIMESTAMP'))

def _md_scope_direct_triggers(direct_triggers: DataFrame, person_snapshot: DataFrame, existing_target_ids: DataFrame, config: MapDeathConfig) -> DataFrame:
    direct_ids = direct_triggers.select('PERSON_ID').dropDuplicates()
    currently_deceased = person_snapshot.join(F.broadcast(direct_ids), 'PERSON_ID', 'inner').filter(F.col('DECEASED_CD') == F.lit(config.deceased_yes_code)).select('PERSON_ID')
    existing = direct_ids.join(F.broadcast(existing_target_ids), 'PERSON_ID', 'inner')
    scoped_ids = currently_deceased.unionByName(existing).dropDuplicates()
    return direct_triggers.join(F.broadcast(scoped_ids), 'PERSON_ID', 'inner')

def _md_code_change_triggers(changed_codes: DataFrame, person_snapshot: DataFrame, existing_target_ids: DataFrame, config: MapDeathConfig) -> DataFrame:
    target_marker = existing_target_ids.withColumn('_TARGET_EXISTS', F.lit(True))
    scoped_persons = person_snapshot.join(F.broadcast(target_marker), 'PERSON_ID', 'left').filter((F.col('DECEASED_CD') == F.lit(config.deceased_yes_code)) | F.coalesce(F.col('_TARGET_EXISTS'), F.lit(False)))
    person_codes = scoped_persons.select('PERSON_ID', F.explode_outer(F.array(*[F.col(column_name).cast('long') for column_name in _MD_PERSON_CODE_COLUMNS])).alias('CODE_VALUE_LONG')).filter(F.col('CODE_VALUE_LONG').isNotNull() & (F.col('CODE_VALUE_LONG') != 0)).dropDuplicates()
    affected = person_codes.join(F.broadcast(changed_codes), 'CODE_VALUE_LONG', 'inner').groupBy('PERSON_ID').agg(F.max('CODE_CDF_COMMIT_VERSION').alias('_CODE_COMMIT_VERSION'), F.max('CODE_CDF_COMMIT_TIMESTAMP').alias('_CODE_COMMIT_TIMESTAMP'))
    return affected.select('PERSON_ID', F.lit('CODE_VALUE').alias('TRIGGER_SOURCE'), F.lit(None).cast('long').alias('PERSON_CDF_COMMIT_VERSION'), F.lit(None).cast('timestamp').alias('PERSON_CDF_COMMIT_TIMESTAMP'), F.lit(None).cast('long').alias('CLINICAL_EVENT_CDF_COMMIT_VERSION'), F.lit(None).cast('timestamp').alias('CLINICAL_EVENT_CDF_COMMIT_TIMESTAMP'))

def _md_aggregate_triggers(trigger_rows: DataFrame) -> DataFrame:
    return trigger_rows.groupBy('PERSON_ID').agg(F.concat_ws(',', F.sort_array(F.collect_set('TRIGGER_SOURCE'))).alias('TRIGGER_SOURCES'), F.max('PERSON_CDF_COMMIT_VERSION').alias('PERSON_CDF_COMMIT_VERSION'), F.max('PERSON_CDF_COMMIT_TIMESTAMP').alias('PERSON_CDF_COMMIT_TIMESTAMP'), F.max('CLINICAL_EVENT_CDF_COMMIT_VERSION').alias('CLINICAL_EVENT_CDF_COMMIT_VERSION'), F.max('CLINICAL_EVENT_CDF_COMMIT_TIMESTAMP').alias('CLINICAL_EVENT_CDF_COMMIT_TIMESTAMP'))

def _md_build_incremental_triggers(config: MapDeathConfig, previous_versions: Dict[str, int], current_versions: Dict[str, int], person_snapshot: DataFrame) -> DataFrame:
    existing_target_ids = _md_existing_target_ids(config)
    person_triggers = _md_person_cdf_triggers(config, previous_versions[config.person_table], current_versions[config.person_table])
    ce_triggers = _md_clinical_event_cdf_triggers(config, previous_versions[config.clinical_event_table], current_versions[config.clinical_event_table])
    direct = person_triggers.unionByName(ce_triggers)
    scoped_direct = _md_scope_direct_triggers(direct, person_snapshot, existing_target_ids, config)
    changed_codes = _md_changed_code_values(config, previous_versions[config.code_value_table], current_versions[config.code_value_table])
    code_triggers = _md_code_change_triggers(changed_codes, person_snapshot, existing_target_ids, config)
    return _md_aggregate_triggers(scoped_direct.unionByName(code_triggers))

def _md_death_precision_description(column: F.Column) -> F.Column:
    return F.when(column == 0, F.lit('Unknown')).when(column == 1, F.lit('Full date and time')).when(column == 2, F.lit('Month')).when(column == 3, F.lit('Year')).when(column == 4, F.lit('Day')).otherwise(F.lit(None).cast('string'))

def _md_age_precision_description(column: F.Column) -> F.Column:
    return F.when(column == 0, F.lit('Unknown')).when(column == 1, F.lit('Less than stated age')).when(column == 2, F.lit('Greater than stated age')).when(column == 3, F.lit('Approximate')).when(column == 4, F.lit('Exact')).otherwise(F.lit(None).cast('string'))

def _md_expected_code_validity(code_column: F.Column, code_set_column: F.Column, expected_code_set: int) -> F.Column:
    missing = code_column.isNull() | (code_column == 0)
    return F.when(missing, F.lit(None).cast('boolean')).otherwise(F.coalesce(code_set_column == F.lit(expected_code_set), F.lit(False)))

def _md_expected_description(code_column: F.Column, code_set_column: F.Column, description_column: F.Column, expected_code_set: int) -> F.Column:
    return F.when(code_column.isNotNull() & (code_column != 0) & (code_set_column == F.lit(expected_code_set)), description_column)

def _md_build_death_rows(config: MapDeathConfig, person_rows: DataFrame, clinical_event_aggregate: DataFrame, code_lookup: DataFrame, trigger_metadata: DataFrame, source_versions: Dict[str, int], run_id: str, run_timestamp: datetime) -> DataFrame:
    enriched = person_rows.join(clinical_event_aggregate, 'PERSON_ID', 'left').join(trigger_metadata, 'PERSON_ID', 'left')
    lookup_joins = (('DECEASED_CD', 'DSTATUS'), ('DECEASED_SOURCE_CD', 'DSOURCE'), ('DECEASED_ID_METHOD_CD', 'DMETHOD'), ('DECEASED_NOTIFY_SOURCE_CD', 'DNOTIFY'), ('CAUSE_OF_DEATH_CD', 'CAUSE'), ('AUTOPSY_CD', 'AUTOPSY'), ('AGE_AT_DEATH_UNIT_CD', 'AGEUNIT'), ('ACTIVE_STATUS_CD', 'ACTIVE'), ('PERSON_STATUS_CD', 'PSTATUS'), ('CONTRIBUTOR_SYSTEM_CD', 'CONTRIB'))
    for source_column, prefix in lookup_joins:
        enriched = _md_join_lookup(enriched, code_lookup, source_column, prefix)
    last_known_activity = F.greatest(F.col('LAST_ENCNTR_DT_TM'), F.col('LAST_NON_ERROR_CLINICAL_EVENT_DT_TM'))
    death_estimate = F.coalesce(F.col('DECEASED_DT_TM'), F.col('LAST_ENCNTR_DT_TM'), F.col('LAST_NON_ERROR_CLINICAL_EVENT_DT_TM'))
    death_estimate_source = F.when(F.col('DECEASED_DT_TM').isNotNull(), F.lit('RECORDED_DEATH')).when(F.col('LAST_ENCNTR_DT_TM').isNotNull(), F.lit('LAST_ENCOUNTER')).when(F.col('LAST_NON_ERROR_CLINICAL_EVENT_DT_TM').isNotNull(), F.lit('LAST_CLINICAL_EVENT')).otherwise(F.lit('NONE'))
    lookup_update_columns = [F.col(f'{prefix}_LOOKUP_ADC_UPDT') for _, prefix in lookup_joins]
    lookup_adc_updt = F.greatest(*lookup_update_columns)
    effective_adc_updt = F.greatest(F.col('PERSON_ADC_UPDT'), F.col('PERSON_UPDT_DT_TM'), F.col('CLINICAL_EVENT_ADC_UPDT'), lookup_adc_updt)
    prepared = enriched.select(F.col('PERSON_ID'), F.col('DECEASED_DT_TM'), F.col('LAST_ENCNTR_DT_TM'), F.col('LAST_NON_ERROR_CE_CLINSIG_UPDT_DT_TM').alias('LAST_CE_DT_TM'), F.col('DECEASED_DT_TM').alias('CALC_DEATH_DATE'), F.col('DECEASED_SOURCE_CD'), _md_expected_description(F.col('DECEASED_SOURCE_CD'), F.col('DSOURCE_CODE_SET'), F.col('DSOURCE_DESC'), config.deceased_source_code_set).alias('DECEASED_SOURCE_DESC'), F.col('DECEASED_ID_METHOD_CD'), _md_expected_description(F.col('DECEASED_ID_METHOD_CD'), F.col('DMETHOD_CODE_SET'), F.col('DMETHOD_DESC'), config.deceased_method_code_set).alias('DECEASED_METHOD_DESC'), effective_adc_updt.alias('ADC_UPDT'), F.col('DECEASED_CD'), F.col('DSTATUS_DESC').alias('DECEASED_STATUS_DESC'), F.col('DSTATUS_CODE_SET').alias('DECEASED_STATUS_CODE_SET'), (F.col('DECEASED_CD') == F.lit(config.deceased_yes_code)).alias('IS_CURRENTLY_DECEASED'), F.col('ACTIVE_IND'), F.col('ACTIVE_STATUS_CD'), F.col('ACTIVE_DESC').alias('ACTIVE_STATUS_DESC'), F.col('END_EFFECTIVE_DT_TM'), F.col('PERSON_STATUS_CD'), F.col('PSTATUS_DESC').alias('PERSON_STATUS_DESC'), F.col('DECEASED_TZ'), F.col('DECEASED_DT_TM_PREC_FLAG'), _md_death_precision_description(F.col('DECEASED_DT_TM_PREC_FLAG')).alias('DECEASED_DT_TM_PREC_DESC'), F.col('LAST_CLINICAL_EVENT_DT_TM'), F.col('LAST_NON_ERROR_CLINICAL_EVENT_DT_TM'), F.col('LAST_CE_CLINSIG_UPDT_DT_TM'), F.col('LAST_NON_ERROR_CE_CLINSIG_UPDT_DT_TM'), last_known_activity.alias('LAST_KNOWN_ACTIVITY_DT_TM'), death_estimate.alias('DEATH_DATE_ESTIMATE_DT_TM'), death_estimate_source.alias('DEATH_DATE_ESTIMATE_SOURCE'), (F.col('DECEASED_DT_TM').isNull() & death_estimate.isNotNull()).alias('DEATH_DATE_ESTIMATE_IND'), F.col('DSOURCE_CODE_SET').alias('DECEASED_SOURCE_CODE_SET'), _md_expected_code_validity(F.col('DECEASED_SOURCE_CD'), F.col('DSOURCE_CODE_SET'), config.deceased_source_code_set).alias('DECEASED_SOURCE_CODE_VALID_IND'), F.col('DMETHOD_CODE_SET').alias('DECEASED_ID_METHOD_CODE_SET'), _md_expected_code_validity(F.col('DECEASED_ID_METHOD_CD'), F.col('DMETHOD_CODE_SET'), config.deceased_method_code_set).alias('DECEASED_ID_METHOD_CODE_VALID_IND'), F.col('DECEASED_NOTIFY_SOURCE_CD'), F.col('DNOTIFY_DESC').alias('DECEASED_NOTIFY_SOURCE_DESC'), F.col('DNOTIFY_CODE_SET').alias('DECEASED_NOTIFY_SOURCE_CODE_SET'), F.col('CAUSE_OF_DEATH'), F.col('CAUSE_OF_DEATH_CD'), F.col('CAUSE_DESC').alias('CAUSE_OF_DEATH_DESC'), F.col('CAUSE_CODE_SET').alias('CAUSE_OF_DEATH_CODE_SET'), F.col('AUTOPSY_CD'), F.col('AUTOPSY_DESC'), F.col('AUTOPSY_CODE_SET'), F.col('AGE_AT_DEATH'), F.col('AGE_AT_DEATH_UNIT_CD'), F.col('AGEUNIT_DESC').alias('AGE_AT_DEATH_UNIT_DESC'), F.col('AGEUNIT_CODE_SET').alias('AGE_AT_DEATH_UNIT_CODE_SET'), F.col('AGE_AT_DEATH_PREC_MOD_FLAG'), _md_age_precision_description(F.col('AGE_AT_DEATH_PREC_MOD_FLAG')).alias('AGE_AT_DEATH_PREC_MOD_DESC'), F.col('CONTRIBUTOR_SYSTEM_CD'), F.col('CONTRIB_DESC').alias('CONTRIBUTOR_SYSTEM_DESC'), F.col('CONTRIB_CODE_SET').alias('CONTRIBUTOR_SYSTEM_CODE_SET'), F.col('INST_ID'), F.col('LOGICAL_DOMAIN_ID'), F.col('PERSON_UPDT_DT_TM'), F.col('PERSON_UPDT_CNT'), F.col('PERSON_LAST_UTC_TS'), F.col('PERSON_ADC_UPDT'), F.col('CLINICAL_EVENT_ADC_UPDT'), lookup_adc_updt.alias('LOOKUP_ADC_UPDT'), F.coalesce(F.col('CLINICAL_EVENT_COUNT'), F.lit(0).cast('long')).alias('CLINICAL_EVENT_COUNT'), F.coalesce(F.col('CLINICAL_EVENT_IN_ERROR_COUNT'), F.lit(0).cast('long')).alias('CLINICAL_EVENT_IN_ERROR_COUNT'), F.coalesce(F.col('CLINICAL_EVENT_NULL_RESULT_STATUS_COUNT'), F.lit(0).cast('long')).alias('CLINICAL_EVENT_NULL_RESULT_STATUS_COUNT'), F.col('PERSON_CDF_COMMIT_VERSION'), F.col('PERSON_CDF_COMMIT_TIMESTAMP'), F.col('CLINICAL_EVENT_CDF_COMMIT_VERSION'), F.col('CLINICAL_EVENT_CDF_COMMIT_TIMESTAMP'), F.coalesce(F.col('TRIGGER_SOURCES'), F.lit('UNKNOWN')).alias('TRIGGER_SOURCES'), F.lit(source_versions[config.person_table]).cast('long').alias('PERSON_SOURCE_VERSION'), F.lit(source_versions[config.clinical_event_table]).cast('long').alias('CLINICAL_EVENT_SOURCE_VERSION'), F.lit(source_versions[config.code_value_table]).cast('long').alias('CODE_VALUE_SOURCE_VERSION'), F.lit(run_id).alias('PIPELINE_RUN_ID'), F.lit(run_timestamp).cast('timestamp').alias('PIPELINE_UPDT_DT_TM'))
    hash_exclusions = {'ROW_HASH', 'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM', 'TRIGGER_SOURCES', 'PERSON_CDF_COMMIT_VERSION', 'PERSON_CDF_COMMIT_TIMESTAMP', 'CLINICAL_EVENT_CDF_COMMIT_VERSION', 'CLINICAL_EVENT_CDF_COMMIT_TIMESTAMP', 'PERSON_SOURCE_VERSION', 'CLINICAL_EVENT_SOURCE_VERSION', 'CODE_VALUE_SOURCE_VERSION'}
    hash_columns = [F.col(field.name) for field in schema_map_death.fields if field.name not in hash_exclusions and field.name != 'ROW_HASH']
    with_hash = prepared.withColumn('ROW_HASH', _md_stable_hash(hash_columns))
    return _md_align_to_schema(with_hash, schema_map_death)

def _md_full_rebuild_trigger_metadata(person_rows: DataFrame) -> DataFrame:
    return person_rows.select('PERSON_ID', F.lit('FULL_REBUILD').alias('TRIGGER_SOURCES'), F.lit(None).cast('long').alias('PERSON_CDF_COMMIT_VERSION'), F.lit(None).cast('timestamp').alias('PERSON_CDF_COMMIT_TIMESTAMP'), F.lit(None).cast('long').alias('CLINICAL_EVENT_CDF_COMMIT_VERSION'), F.lit(None).cast('timestamp').alias('CLINICAL_EVENT_CDF_COMMIT_TIMESTAMP'))

def _md_validate_output_dataframe(df: DataFrame, expected_rows: Optional[int]=None) -> Dict[str, int]:
    summary = df.agg(F.count(F.lit(1)).cast('long').alias('row_count'), F.countDistinct('PERSON_ID').cast('long').alias('distinct_person_ids'), F.sum(F.when(F.col('PERSON_ID').isNull(), F.lit(1)).otherwise(F.lit(0))).cast('long').alias('null_person_ids'), F.sum(F.when(~F.coalesce(F.col('IS_CURRENTLY_DECEASED'), F.lit(False)), F.lit(1)).otherwise(F.lit(0))).cast('long').alias('not_currently_deceased_rows'), F.sum(F.when(F.col('ROW_HASH').isNull(), F.lit(1)).otherwise(F.lit(0))).cast('long').alias('null_row_hashes')).first().asDict()
    metrics = {key: int(value or 0) for key, value in summary.items()}
    if metrics['row_count'] != metrics['distinct_person_ids']:
        raise RuntimeError(f"map_death output contains duplicate PERSON_ID rows: {metrics['row_count']} rows versus {metrics['distinct_person_ids']} distinct IDs")
    if metrics['null_person_ids'] != 0:
        raise RuntimeError('map_death output contains null PERSON_ID values')
    if metrics['not_currently_deceased_rows'] != 0:
        raise RuntimeError('map_death output contains people not currently marked deceased')
    if metrics['null_row_hashes'] != 0:
        raise RuntimeError('map_death output contains null ROW_HASH values')
    if expected_rows is not None and metrics['row_count'] != int(expected_rows):
        raise RuntimeError(f"map_death output row count {metrics['row_count']} does not match expected current-deceased count {expected_rows}")
    return metrics

def _md_safe_overwrite_target(config: MapDeathConfig, rows: DataFrame, run_id: str, expected_rows: int, safe_rebuild: bool) -> Dict[str, int]:
    rows = bronze_project_contract(rows, config.target_table)
    staging_table = f"{config.target_table.rsplit('.', 1)[0]}.__map_death_rebuild_{run_id.replace('-', '_')}"
    if safe_rebuild:
        try:
            rows.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(staging_table)
            staging_metrics = _md_validate_output_dataframe(spark.table(staging_table), expected_rows=expected_rows)
            spark.table(staging_table).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(config.target_table)
        finally:
            spark.sql(f'DROP TABLE IF EXISTS {_md_sql_identifier(staging_table)}')
    else:
        rows.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(config.target_table)
        staging_metrics = _md_validate_output_dataframe(spark.table(config.target_table), expected_rows=expected_rows)
    _md_apply_target_metadata(config)
    final_metrics = _md_validate_output_dataframe(spark.table(config.target_table), expected_rows=expected_rows)
    return {**{f'staging_{key}': value for key, value in staging_metrics.items()}, **{f'target_{key}': value for key, value in final_metrics.items()}}

def _md_merge_current_deaths(config: MapDeathConfig, updates: DataFrame) -> Dict[str, int]:
    updates = bronze_project_contract(updates, config.target_table)
    source_rows = updates.count()
    if source_rows == 0:
        return {'upsert_source_rows': 0}
    assignments = {
        column_name: F.col(f's.{column_name}')
        for column_name in updates.columns
    }
    comparisons = ' OR '.join(
        f'NOT (t.`{column_name}` <=> s.`{column_name}`)'
        for column_name in updates.columns
        if column_name != 'PERSON_ID'
    )
    DeltaTable.forName(spark, config.target_table).alias('t').merge(
        updates.alias('s'),
        't.PERSON_ID <=> s.PERSON_ID',
    ).whenMatchedUpdate(
        condition=comparisons or 'false',
        set=assignments,
    ).whenNotMatchedInsert(values=assignments).execute()
    return {'upsert_source_rows': int(source_rows)}

MAP_DEATH_REMOVAL_AUDIT_SUFFIX = '__removed_audit'
MAP_DEATH_REMOVAL_REASON_REVERSED_OR_REMOVED = 'REVERSED_OR_REMOVED'

def _md_removal_audit_table(config: MapDeathConfig) -> str:
    return config.target_table + MAP_DEATH_REMOVAL_AUDIT_SUFFIX

def _md_ensure_removal_audit_table(config: MapDeathConfig) -> str:
    audit_table = _md_removal_audit_table(config)
    spark.sql('CREATE TABLE IF NOT EXISTS ' + _md_sql_identifier(audit_table) + ' (PERSON_ID BIGINT, REMOVAL_REASON STRING, PIPELINE_RUN_ID STRING, REMOVED_TS TIMESTAMP) USING DELTA')
    return audit_table

def _md_audit_removals(config: MapDeathConfig, removals: DataFrame, run_id: Optional[str]=None, reason: str=MAP_DEATH_REMOVAL_REASON_REVERSED_OR_REMOVED) -> int:
    # Removals stay; they just stop being invisible. Capture the PERSON_IDs the delete
    # will actually affect - removal keys that are currently present in the target -
    # before the merge runs, so the audit is an exact record of what left the curated
    # table, under which run, and why.
    if not _md_table_exists(config.target_table):
        return 0
    audit_table = _md_ensure_removal_audit_table(config)
    effective_run_id = run_id if run_id is not None else str(globals().get('_PIPELINE_RUN_ID', 'UNKNOWN'))
    target_ids = spark.table(config.target_table).select(F.col('PERSON_ID').cast('long').alias('PERSON_ID')).filter(F.col('PERSON_ID').isNotNull()).dropDuplicates()
    affected = removals.select(F.col('PERSON_ID').cast('long').alias('PERSON_ID')).filter(F.col('PERSON_ID').isNotNull()).dropDuplicates().join(target_ids, 'PERSON_ID', 'inner')
    rows = affected.select(F.col('PERSON_ID'), F.lit(reason).cast('string').alias('REMOVAL_REASON'), F.lit(effective_run_id).cast('string').alias('PIPELINE_RUN_ID'), F.current_timestamp().alias('REMOVED_TS'))
    audited = rows.count()
    if audited:
        rows.write.format('delta').mode('append').saveAsTable(audit_table)
    return int(audited)

def _md_delete_reversed_or_removed_people(config: MapDeathConfig, removals: DataFrame, run_id: Optional[str]=None) -> Dict[str, int]:
    removal_rows = removals.count()
    if removal_rows == 0:
        return {'removal_source_rows': 0, 'removal_audited_rows': 0}
    audited_rows = _md_audit_removals(config, removals, run_id)
    DeltaTable.forName(spark, config.target_table).alias('t').merge(removals.alias('s'), 't.PERSON_ID <=> s.PERSON_ID').whenMatchedDelete().execute()
    return {'removal_source_rows': int(removal_rows), 'removal_audited_rows': int(audited_rows)}

def _md_validate_incremental_result(config: MapDeathConfig, current_deaths: DataFrame, removals: DataFrame) -> Dict[str, int]:
    target_ids = spark.table(config.target_table).select('PERSON_ID')
    missing_current = current_deaths.select('PERSON_ID').dropDuplicates().join(target_ids, 'PERSON_ID', 'left_anti').limit(1).count()
    lingering_removals = removals.select('PERSON_ID').dropDuplicates().join(target_ids, 'PERSON_ID', 'inner').limit(1).count()
    if missing_current:
        raise RuntimeError('At least one currently deceased changed PERSON_ID is missing after merge')
    if lingering_removals:
        raise RuntimeError('At least one reversed/deleted PERSON_ID remains in map_death')
    duplicate = spark.table(config.target_table).groupBy('PERSON_ID').count().filter(F.col('count') > 1).limit(1).count()
    if duplicate:
        raise RuntimeError('map_death contains duplicate PERSON_ID rows after incremental merge')
    return {'missing_current_after_merge': int(missing_current), 'lingering_removals_after_merge': int(lingering_removals), 'duplicate_person_ids_after_merge': int(duplicate)}

def _md_full_rebuild(config: MapDeathConfig, source_versions: Dict[str, int], run_id: str, run_timestamp: datetime, safe_rebuild: bool) -> Dict[str, object]:
    person_snapshot = _md_person_snapshot(config, source_versions[config.person_table])
    scratch_tables: List[str] = []
    try:
        current_deaths_stage = _md_stage_name(config, run_id, 'current_deaths')
        scratch_tables.append(current_deaths_stage)
        current_deaths = _md_materialize_stage(_md_current_deceased_persons(person_snapshot, config), current_deaths_stage)
        expected_rows = current_deaths.count()
        person_ids = current_deaths.select('PERSON_ID')
        clinical_events = _md_clinical_event_aggregate(config, source_versions[config.clinical_event_table], person_ids, run_timestamp)
        code_lookup_stage = _md_stage_name(config, run_id, 'code_lookup')
        scratch_tables.append(code_lookup_stage)
        code_lookup = _md_materialize_stage(_md_code_lookup(config, source_versions[config.code_value_table], current_deaths), code_lookup_stage)
        lookup_rows = code_lookup.count()
        output = _md_build_death_rows(config, current_deaths, clinical_events, code_lookup, _md_full_rebuild_trigger_metadata(current_deaths), source_versions, run_id, run_timestamp)
        write_metrics = _md_safe_overwrite_target(config, output, run_id, expected_rows, safe_rebuild)
        output_metrics = {key[len('target_'):]: value for key, value in write_metrics.items() if key.startswith('target_')}
        return {'mode': 'FULL_REBUILD', 'current_deceased_rows': int(expected_rows), 'lookup_rows': int(lookup_rows), **output_metrics, **write_metrics}
    finally:
        for stage_table in reversed(scratch_tables):
            try:
                _md_drop_stage(stage_table)
            except Exception as cleanup_exc:
                print(f'[WARN] map_death stage cleanup failed; the next pipeline startup sweep will retry: {str(cleanup_exc)[:500]}')

def _md_incremental_update(config: MapDeathConfig, previous_versions: Dict[str, int], source_versions: Dict[str, int], run_id: str, run_timestamp: datetime) -> Dict[str, object]:
    person_snapshot = _md_person_snapshot(config, source_versions[config.person_table])
    triggers = None
    current_deaths = None
    removals = None
    code_lookup = None
    output = None
    try:
        triggers = _md_build_incremental_triggers(config, previous_versions, source_versions, person_snapshot)
        changed_people = triggers.count()
        if changed_people == 0:
            return {'mode': 'INCREMENTAL', 'changed_people': 0, 'upsert_source_rows': 0, 'removal_source_rows': 0}
        changed_ids = triggers.select('PERSON_ID').dropDuplicates()
        current_deaths = person_snapshot.join(F.broadcast(changed_ids), 'PERSON_ID', 'inner').filter(F.col('DECEASED_CD') == F.lit(config.deceased_yes_code))
        current_death_count = current_deaths.count()
        removals = changed_ids.join(current_deaths.select('PERSON_ID'), 'PERSON_ID', 'left_anti')
        delete_metrics = _md_delete_reversed_or_removed_people(config, removals, run_id)
        if current_death_count == 0:
            validation_metrics = _md_validate_incremental_result(config, current_deaths, removals)
            return {'mode': 'INCREMENTAL', 'changed_people': int(changed_people), 'current_death_updates': 0, **delete_metrics, **validation_metrics}
        clinical_events = _md_clinical_event_aggregate(config, source_versions[config.clinical_event_table], current_deaths.select('PERSON_ID'), run_timestamp)
        code_lookup = _md_code_lookup(config, source_versions[config.code_value_table], current_deaths)
        lookup_rows = code_lookup.count()
        output = _md_build_death_rows(config, current_deaths, clinical_events, code_lookup, triggers, source_versions, run_id, run_timestamp)
        output_metrics = _md_validate_output_dataframe(output, expected_rows=current_death_count)
        merge_metrics = _md_merge_current_deaths(config, output)
        validation_metrics = _md_validate_incremental_result(config, current_deaths, removals)
        return {'mode': 'INCREMENTAL', 'changed_people': int(changed_people), 'current_death_updates': int(current_death_count), 'lookup_rows': int(lookup_rows), **delete_metrics, **output_metrics, **merge_metrics, **validation_metrics}
    finally:
        if output is not None:
            None
        if code_lookup is not None:
            None
        if removals is not None:
            None
        if current_deaths is not None:
            None
        if triggers is not None:
            None
        None

def process_death_incremental(force_full_rebuild: bool=False, bootstrap_if_state_missing: bool=True, safe_rebuild: bool=True, config: MapDeathConfig=MAP_DEATH_CONFIG) -> Dict[str, object]:
    """
    Drop-in replacement for the original process_death_incremental().

    Behaviour:
      * first run or missing state: version-pinned full current-state rebuild;
      * later runs: CDF by stored Delta version for PERSON, CLINICAL_EVENT and CODE_VALUE;
      * no timestamp fallback and therefore no equal-timestamp or late-arrival gap;
      * person reversals/deletions are removed from the current-state target;
      * clinical-event changes independently trigger refreshes;
      * code-value changes refresh people using affected codes;
      * explicit In Error statuses are excluded only from separately named non-error metrics;
      * null result statuses remain represented;
      * matched target rows are rewritten only when ROW_HASH changes;
      * source-version state advances only after all writes and validations succeed.
    """
    run_id = str(uuid.uuid4())
    run_timestamp = datetime.now(timezone.utc)
    mode = 'UNKNOWN'
    source_versions: Dict[str, int] = {}
    _md_ensure_metadata_tables(config)
    try:
        _md_assert_source_preflight(config, require_cdf=False)
        source_versions = _md_capture_source_versions(config)
        previous_versions = _md_read_pipeline_state(config)
        missing_state = [source for source in _md_source_tables(config) if source not in previous_versions]
        target_missing = not _md_table_exists(config.target_table)
        requires_bootstrap = target_missing or bool(missing_state)
        if requires_bootstrap and (not (bootstrap_if_state_missing or force_full_rebuild)):
            raise RuntimeError(f'map_death source-version state is missing. Run process_death_incremental(force_full_rebuild=True), or enable bootstrap_if_state_missing. Missing state: {missing_state}')
        mode = 'FULL_REBUILD' if force_full_rebuild or requires_bootstrap else 'INCREMENTAL'
        if mode == 'INCREMENTAL':
            _md_assert_source_preflight(config, require_cdf=True)
        _md_write_audit_event(config, run_id, status='STARTED', mode=mode, message='map_death replacement load started', source_versions=source_versions)
        if mode == 'FULL_REBUILD':
            metrics = _md_full_rebuild(config, source_versions, run_id, run_timestamp, safe_rebuild=safe_rebuild)
        else:
            _md_assert_incremental_target_schema(config)
            try:
                metrics = _md_incremental_update(config, previous_versions, source_versions, run_id, run_timestamp)
            except Exception as exc:
                message = str(exc)
                if 'change data feed' in message.lower() or 'starting version' in message.lower() or 'cdf' in message.lower():
                    raise RuntimeError(f'{message}. CDF history may no longer cover the stored version. Run process_death_incremental(force_full_rebuild=True).') from exc
                raise
        _md_write_pipeline_state(config, source_versions, run_id, run_timestamp)
        _md_write_audit_event(config, run_id, status='SUCCEEDED', mode=mode, message='map_death replacement load completed', source_versions=source_versions, metrics=metrics)
        print(f'[map_death] {mode} completed successfully. run_id={run_id}; metrics={json.dumps(metrics, sort_keys=True, default=str)}')
        return {'run_id': run_id, 'mode': mode, 'source_versions': source_versions, **metrics}
    except Exception as exc:
        _md_write_audit_event(config, run_id, status='FAILED', mode=mode, message=str(exc), source_versions=source_versions)
        print(f'[map_death] FAILED run_id={run_id}: {exc}')
        raise

def map_death_preflight(config: MapDeathConfig=MAP_DEATH_CONFIG) -> Dict[str, object]:
    """
    Read-only deployment check. Does not create state tables or modify map_death.
    """
    _md_assert_source_preflight(config)
    source_versions = _md_capture_source_versions(config)
    state = _md_read_pipeline_state(config)
    target_exists = _md_table_exists(config.target_table)
    target_schema_matches = False
    if target_exists:
        try:
            _md_assert_incremental_target_schema(config)
            target_schema_matches = True
        except Exception:
            target_schema_matches = False
    result = {'target_exists': target_exists, 'target_schema_matches': target_schema_matches, 'source_versions': source_versions, 'stored_state': state, 'requires_full_rebuild': not target_exists or not target_schema_matches or any((source not in state for source in _md_source_tables(config)))}
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return result

try:
    _pipeline_run_recoverable('map_death', _PIPELINE_FULL_REFRESH, lambda: process_death_incremental(force_full_rebuild=False), lambda: process_death_incremental(force_full_rebuild=True))
    _PIPELINE_UPDATED_TARGETS.extend(['4_prod.bronze.map_death'])
    _pipeline_mark_component_complete('map_death', ['4_prod.bronze.map_death'])
    _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_death'})
except Exception as exc:
    _pipeline_record_error(None, exc)
    raise
finally:
    if _pipeline_shared_update_table is not None:
        update_table = _pipeline_shared_update_table
    if _pipeline_shared_table_exists is not None:
        table_exists = _pipeline_shared_table_exists
    if _pipeline_shared_get_max_timestamp is not None:
        get_max_timestamp = _pipeline_shared_get_max_timestamp
    if _pipeline_shared_has_cdf_enabled is not None:
        has_cdf_enabled = _pipeline_shared_has_cdf_enabled
    if _pipeline_shared_get_incremental is not None:
        get_incremental_data_with_cdf = _pipeline_shared_get_incremental

