# Databricks notebook source
# MAGIC %md
# MAGIC # Map 50 — Pathology
# MAGIC
# MAGIC Components: pathology foundation and mapping reconciliation. This notebook is executed by `map_pipeline` in the shared map runtime.

# COMMAND ----------

if "_PIPELINE_RUN_ID" not in globals():
    raise RuntimeError("Run this component through map_pipeline so shared contracts, checkpoints and audit state are initialized.")

if "_pipeline_resume_skip_component" not in globals():
    def _pipeline_resume_skip_component(component_name: str, target_tables) -> bool:
        complete = _pipeline_resume_component_complete(component_name, target_tables)
        if not complete:
            return False
        print(
            f"[RESUME] {component_name}: durable completion marker found; "
            "skipping completed canonical work"
        )
        _pipeline_audit(
            None,
            "COMPONENT_RESUME_SKIP",
            {"component": component_name, "targets": list(target_tables)},
        )
        return True

# COMMAND ----------

_pipeline_component_start("map_pathology")
_pipeline_shared_update_table = globals().get('update_table')
_pipeline_shared_table_exists = globals().get('table_exists')
_pipeline_shared_get_max_timestamp = globals().get('get_max_timestamp')
_pipeline_shared_has_cdf_enabled = globals().get('has_cdf_enabled')
_pipeline_shared_get_incremental = globals().get('get_incremental_data_with_cdf')
globals().pop('_map_updates_original_update_table', None)
globals().pop('_map_medical_personnel_original_update_table', None)
globals().pop('_MAP_UPDATES_ORIGINAL_UPDATE_TABLE', None)

from datetime import datetime, timedelta, timezone
from functools import reduce
import builtins as _mp_builtins
import json
import re
import uuid
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType, TimestampType
MP_VERSION = '3.0.0'
TARGET_SCHEMA = '4_prod.bronze'
MAP_SCHEMA = '3_lookup.omop'
MP_TARGET = f'{TARGET_SCHEMA}.map_pathology'
MP_CONTROL_SCHEMA = bronze_control_schema(TARGET_SCHEMA)
MP_FLAG = f'{MAP_SCHEMA}.pathology_map_rebuild_flag'
MP_STATE = f'{MAP_SCHEMA}.pathology_pipeline_state_v2'
MP_RUN_LOG = f'{MAP_SCHEMA}.pathology_pipeline_run_log_v2'
MP_BASELINE = f'{MAP_SCHEMA}.pathology_map_baseline_v2'
MP_NATIVE_TEST = f'{MAP_SCHEMA}.pathology_native_test_crosswalk'
MP_NATIVE_RESULT = f'{MAP_SCHEMA}.pathology_native_result_crosswalk'
MP_FULL_BUILD_MANIFEST = f'{MP_CONTROL_SCHEMA}.map_pathology_full_build_manifest'
MP_FULL_BUILD_PROGRESS = f'{MP_CONTROL_SCHEMA}.map_pathology_full_build_progress'
TEST_MAP = f'{MAP_SCHEMA}.pathology_test_concept_map'
RESULT_MAP = f'{MAP_SCHEMA}.pathology_result_concept_map'
UNIT_MAP = f'{MAP_SCHEMA}.pathology_unit_map'
EXCL_TBL = f'{MAP_SCHEMA}.pathology_result_value_exclusions'
CONCEPT = '3_lookup.omop.concept'
CE = '4_prod.raw.mill_clinical_event'
ORDERS = '4_prod.raw.mill_orders'
PERSON_ALIAS = '4_prod.raw.mill_person_alias'
RESULT_LEVEL = '4_prod.raw.path_patient_resultlevel'
SAMPLE_LEVEL = '4_prod.raw.path_patient_samplelevel'
MASTER_RESULT = '4_prod.raw.path_master_resultable'
ORDER_CATALOG = '3_lookup.mill.mill_order_catalog'
CODE_VALUE = '3_lookup.mill.mill_code_value'
SOURCE_TABLES = {'mill_clinical_event': (CE, 'ADC_UPDT'), 'mill_orders': (ORDERS, 'ADC_UPDT'), 'mill_person_alias': (PERSON_ALIAS, 'ADC_UPDT'), 'path_patient_resultlevel': (RESULT_LEVEL, 'ADC_UPDT'), 'path_patient_samplelevel': (SAMPLE_LEVEL, 'ADC_UPDT'), 'path_master_resultable': (MASTER_RESULT, 'ADC_UPDT'), 'mill_order_catalog': (ORDER_CATALOG, 'ADC_UPDT'), 'mill_code_value': (CODE_VALUE, 'ADC_UPDT'), 'pathology_test_concept_map': (TEST_MAP, 'ADC_UPDT'), 'pathology_result_concept_map': (RESULT_MAP, 'ADC_UPDT'), 'pathology_unit_map': (UNIT_MAP, None)}
SOURCE_LOOKBACK = timedelta(days=2)
NATIVE_MIN_SUPPORT = 5
ENABLE_EMBED_LOOP = True
ENABLE_LIQUID_CLUSTERING = True
FULL_BUILD_SCHEMA_VERSION = '3.0.0'
FULL_BUILD_LINKED_BUCKETS = 8
FULL_BUILD_RAW_BUCKETS = 8
FULL_BUILD_STAGE_RETRIES = 2
FULL_BUILD_INPUTS = {**{source_name: table_name for source_name, (table_name, _) in SOURCE_TABLES.items()}, 'pathology_result_value_exclusions': EXCL_TBL, 'omop_concept': CONCEPT}
FULL_BUILD_GLOBAL_SOURCES = {'CE': 'mill_clinical_event', 'ORDERS': 'mill_orders', 'PERSON_ALIAS': 'mill_person_alias', 'RESULT_LEVEL': 'path_patient_resultlevel', 'SAMPLE_LEVEL': 'path_patient_samplelevel', 'MASTER_RESULT': 'path_master_resultable', 'ORDER_CATALOG': 'mill_order_catalog', 'CODE_VALUE': 'mill_code_value', 'TEST_MAP': 'pathology_test_concept_map', 'RESULT_MAP': 'pathology_result_concept_map', 'UNIT_MAP': 'pathology_unit_map', 'EXCL_TBL': 'pathology_result_value_exclusions', 'CONCEPT': 'omop_concept'}
NUMERIC_REGEX = '^\\s*(?:<=|>=|<|>|≤|≥|=)?\\s*[+-]?(?:[0-9]+(?:[.][0-9]*)?|[.][0-9]+)(?:[eE][+-]?[0-9]+)?\\s*$'
RESULT_NORMALIZE_SQL = "LOWER(TRIM(REGEXP_REPLACE(result_txt,'\\\\s+',' ')))"
TEST_TIERS = ('curated', 'auto_high', 'auto_low')
RESULT_TIERS = ('curated', 'auto_high', 'auto_low', 'auto_anchor', 'auto_value', 'auto_genpos')

def _qn(name: str) -> str:
    """Quote a multipart table name, including catalogs that begin with a digit."""
    tick = chr(96)
    return '.'.join((f'{tick}{part}{tick}' for part in name.split('.')))

def _table_exists(name: str) -> bool:
    return spark.catalog.tableExists(name)

def _column_names(name: str) -> set[str]:
    if not _table_exists(name):
        return set()
    return {field.name for field in spark.table(name).schema.fields}

def _sql_string(value: str | None) -> str:
    if value is None:
        return 'NULL'
    return "'" + value.replace('\\', '\\\\').replace("'", "''") + "'"

def _ts_literal(value: datetime | None) -> str:
    if value is None:
        value = datetime(1980, 1, 1)
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return "TIMESTAMP'" + value.strftime('%Y-%m-%d %H:%M:%S.%f') + "'"

def _empty_like(table_name: str) -> DataFrame:
    return spark.createDataFrame([], spark.table(table_name).schema)

def _union_distinct(frames: list[DataFrame], columns: list[str]) -> DataFrame:
    usable = [df.select(*columns) for df in frames if df is not None]
    if not usable:
        schema = StructType([StructField(c, StringType(), True) for c in columns])
        return spark.createDataFrame([], schema)
    return reduce(lambda left, right: left.unionByName(right), usable).dropDuplicates(columns)

def _latest_delta_version(table_name: str) -> int:
    return int(spark.sql(f'DESCRIBE HISTORY {_qn(table_name)} LIMIT 1').first()['version'])

def _max_timestamp(table_name: str, column_name: str | None) -> datetime | None:
    if column_name is None:
        return None
    row = spark.table(table_name).select(F.max(F.col(column_name)).alias('max_ts')).first()
    return row['max_ts'] if row else None

def _time_travel_retention_expired(exc: Exception) -> bool:
    message = str(exc).lower().replace('_', ' ')
    return (
        'cannot time travel beyond' in message
        or 'deletedfileretentionduration' in message.replace(' ', '')
        or 'unsupported time travel beyond deleted file retention duration' in message
    )


def _incremental_snapshot_read_unavailable(exc: Exception) -> bool:
    messages = []
    current: BaseException | None = exc
    while current is not None and len(messages) < 8:
        messages.append(str(current).lower().replace('_', ' '))
        current = current.__cause__ or current.__context__
    combined = ' '.join(messages)
    return any(
        marker in combined
        for marker in (
            'cannot time travel beyond',
            'deleted file retention',
            'change data feed',
            'starting version',
            'failed read file',
            'file not exist',
            'shallow clone file not found',
            'delta log file not found',
        )
    )


def _max_timestamp_at_version(table_name: str, column_name: str | None, version: int) -> datetime | None:
    if column_name is None:
        return None
    requested_version = int(version)
    try:
        row = (
            spark.read.format('delta')
            .option('versionAsOf', requested_version)
            .table(table_name)
            .select(F.max(F.col(column_name)).alias('max_ts'))
            .first()
        )
    except Exception as exc:
        latest_version = _latest_delta_version(table_name)
        if (
            requested_version != latest_version
            or not _time_travel_retention_expired(exc)
        ):
            raise
        print(
            f'[map_pathology_v3] current snapshot fallback for {table_name} '
            f'at version {requested_version}; explicit time travel is outside '
            'deleted-file retention but the pinned version is still current'
        )
        row = (
            spark.table(table_name)
            .select(F.max(F.col(column_name)).alias('max_ts'))
            .first()
        )
    return row['max_ts'] if row else None

def _ensure_control_tables() -> None:
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {_qn(MP_CONTROL_SCHEMA)}')
    spark.sql(f'\n        CREATE TABLE IF NOT EXISTS {_qn(MP_STATE)} (\n          source_name STRING NOT NULL,\n          table_name STRING NOT NULL,\n          last_delta_version BIGINT,\n          last_adc_updt TIMESTAMP,\n          last_success_at TIMESTAMP,\n          pipeline_version STRING\n        ) USING DELTA\n        ')
    spark.sql(f'\n        CREATE TABLE IF NOT EXISTS {_qn(MP_RUN_LOG)} (\n          run_id STRING,\n          started_at TIMESTAMP,\n          completed_at TIMESTAMP,\n          mode STRING,\n          status STRING,\n          pipeline_version STRING,\n          source_parent_count BIGINT,\n          staged_row_count BIGINT,\n          inserted_or_updated_rows BIGINT,\n          stale_rows_deleted BIGINT,\n          additive_map_keys BIGINT,\n          correction_map_keys BIGINT,\n          message STRING\n        ) USING DELTA\n        ')
    spark.sql(f'\n        CREATE TABLE IF NOT EXISTS {_qn(MP_FLAG)}\n        (id INT, rebuild_flagged BOOLEAN)\n        USING DELTA\n        ')
    spark.sql(f'\n        MERGE INTO {_qn(MP_FLAG)} t\n        USING (SELECT 1 AS id, FALSE AS rebuild_flagged) s\n        ON t.id=s.id\n        WHEN NOT MATCHED THEN INSERT *\n        ')


def _ensure_full_build_control_tables() -> None:
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {_qn(MP_CONTROL_SCHEMA)}')
    spark.sql(f'\n        CREATE TABLE IF NOT EXISTS {_qn(MP_FULL_BUILD_MANIFEST)} (\n          build_id STRING NOT NULL,\n          phase STRING NOT NULL,\n          schema_version STRING NOT NULL,\n          config_json STRING NOT NULL,\n          source_versions_json STRING NOT NULL,\n          source_cutoffs_json STRING NOT NULL,\n          run_timestamp TIMESTAMP NOT NULL,\n          started_at TIMESTAMP NOT NULL,\n          updated_at TIMESTAMP NOT NULL,\n          completed_at TIMESTAMP,\n          last_error STRING\n        ) USING DELTA\n        ')
    spark.sql(f'\n        CREATE TABLE IF NOT EXISTS {_qn(MP_FULL_BUILD_PROGRESS)} (\n          build_id STRING NOT NULL,\n          stage_name STRING NOT NULL,\n          bucket_id INT NOT NULL,\n          status STRING NOT NULL,\n          table_name STRING,\n          row_count BIGINT,\n          parent_count BIGINT,\n          started_at TIMESTAMP,\n          completed_at TIMESTAMP,\n          last_error STRING\n        ) USING DELTA\n        ')


def _read_state() -> dict[str, dict]:
    if not _table_exists(MP_STATE):
        return {}
    return {row['source_name']: row.asDict() for row in spark.table(MP_STATE).collect()}

def _capture_cutoffs() -> dict[str, dict]:
    cutoffs = {}
    for source_name, (table_name, ts_col) in SOURCE_TABLES.items():
        end_version = _latest_delta_version(table_name)
        cutoffs[source_name] = {'source_name': source_name, 'table_name': table_name, 'end_version': end_version, 'end_adc_updt': _max_timestamp_at_version(table_name, ts_col, end_version), 'timestamp_column': ts_col}
    return cutoffs

def _json_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.isoformat(timespec='microseconds')

def _full_build_config_json() -> str:
    return json.dumps({'schema_version': FULL_BUILD_SCHEMA_VERSION, 'linked_bucket_count': FULL_BUILD_LINKED_BUCKETS, 'raw_bucket_count': FULL_BUILD_RAW_BUCKETS, 'bucket_hash': 'xxhash64_parent_v1'}, sort_keys=True, separators=(',', ':'))

def _serialize_cutoffs(cutoffs: dict[str, dict]) -> str:
    payload = {}
    for source_name, cutoff in cutoffs.items():
        payload[source_name] = {'source_name': source_name, 'table_name': cutoff['table_name'], 'end_version': int(cutoff['end_version']), 'end_adc_updt': _json_timestamp(cutoff.get('end_adc_updt')), 'timestamp_column': cutoff.get('timestamp_column')}
    return json.dumps(payload, sort_keys=True, separators=(',', ':'))

def _deserialize_cutoffs(payload: str) -> dict[str, dict]:
    cutoffs = json.loads(payload)
    for cutoff in cutoffs.values():
        timestamp = cutoff.get('end_adc_updt')
        cutoff['end_adc_updt'] = datetime.fromisoformat(timestamp) if timestamp else None
        cutoff['end_version'] = int(cutoff['end_version'])
    return cutoffs

def _manifest_row_to_dict(row) -> dict:
    result = row.asDict()
    result['source_versions'] = {key: int(value) for key, value in json.loads(result['source_versions_json']).items()}
    result['cutoffs'] = _deserialize_cutoffs(result['source_cutoffs_json'])
    return result

def _write_full_build_manifest(manifest: dict) -> None:
    row = (manifest['build_id'], manifest['phase'], manifest['schema_version'], manifest['config_json'], manifest['source_versions_json'], manifest['source_cutoffs_json'], manifest['run_timestamp'], manifest['started_at'], manifest['updated_at'], manifest.get('completed_at'), manifest.get('last_error'))
    schema = 'build_id STRING, phase STRING, schema_version STRING, config_json STRING, source_versions_json STRING, source_cutoffs_json STRING, run_timestamp TIMESTAMP, started_at TIMESTAMP, updated_at TIMESTAMP, completed_at TIMESTAMP, last_error STRING'
    update = spark.createDataFrame([row], schema)
    DeltaTable.forName(spark, MP_FULL_BUILD_MANIFEST).alias('t').merge(update.alias('s'), 't.build_id=s.build_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def _update_full_build_phase(manifest: dict, phase: str, error: str | None=None) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    manifest['phase'] = phase
    manifest['updated_at'] = now
    manifest['last_error'] = error[:4000] if error else None
    if phase in {'SUCCESS', 'ABANDONED'}:
        manifest['completed_at'] = now
    _write_full_build_manifest(manifest)

def _load_or_start_full_build() -> dict:
    expected_config = _full_build_config_json()
    active = spark.table(MP_FULL_BUILD_MANIFEST).filter(~F.col('phase').isin('SUCCESS', 'ABANDONED')).orderBy(F.desc('updated_at')).limit(1).collect()
    if active:
        manifest = _manifest_row_to_dict(active[0])
        if manifest['schema_version'] != FULL_BUILD_SCHEMA_VERSION:
            raise RuntimeError(f"The active pathology full-build manifest uses schema version {manifest['schema_version']}; call abandon_map_pathology_full_build() before changing the full-build schema.")
        if manifest['config_json'] != expected_config:
            raise RuntimeError('The active pathology full-build manifest is incompatible with the current bucket configuration; call abandon_map_pathology_full_build() rather than mixing stages.')
        print(f"[map_pathology_v3] RESUME build {manifest['build_id']} from phase {manifest['phase']}")
        return manifest
    cutoffs = _capture_cutoffs()
    source_versions = {source_name: int(cutoffs[source_name]['end_version']) if source_name in cutoffs else _latest_delta_version(table_name) for source_name, table_name in FULL_BUILD_INPUTS.items()}
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    manifest = {'build_id': str(uuid.uuid4()), 'phase': 'BUILDING', 'schema_version': FULL_BUILD_SCHEMA_VERSION, 'config_json': expected_config, 'source_versions_json': json.dumps(source_versions, sort_keys=True, separators=(',', ':')), 'source_cutoffs_json': _serialize_cutoffs(cutoffs), 'source_versions': source_versions, 'cutoffs': cutoffs, 'run_timestamp': now, 'started_at': now, 'updated_at': now, 'completed_at': None, 'last_error': None}
    _write_full_build_manifest(manifest)
    print(f"[map_pathology_v3] START build {manifest['build_id']}; pinned {_mp_builtins.len(source_versions)} source versions")
    return manifest

def _write_full_build_progress(build_id: str, stage_name: str, bucket_id: int, status: str, table_name: str | None=None, row_count: int | None=None, parent_count: int | None=None, error: str | None=None) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    existing = spark.table(MP_FULL_BUILD_PROGRESS).filter((F.col('build_id') == build_id) & (F.col('stage_name') == stage_name) & (F.col('bucket_id') == int(bucket_id))).limit(1).collect()
    started_at = existing[0]['started_at'] if existing else now
    completed_at = now if status == 'COMPLETE' else None
    row = (build_id, stage_name, int(bucket_id), status, table_name, int(row_count) if row_count is not None else None, int(parent_count) if parent_count is not None else None, started_at, completed_at, error[:4000] if error else None)
    schema = 'build_id STRING, stage_name STRING, bucket_id INT, status STRING, table_name STRING, row_count BIGINT, parent_count BIGINT, started_at TIMESTAMP, completed_at TIMESTAMP, last_error STRING'
    update = spark.createDataFrame([row], schema)
    DeltaTable.forName(spark, MP_FULL_BUILD_PROGRESS).alias('t').merge(update.alias('s'), 't.build_id=s.build_id AND t.stage_name=s.stage_name AND t.bucket_id=s.bucket_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def _completed_full_build_progress(build_id: str, stage_name: str, bucket_id: int, table_name: str) -> dict | None:
    rows = spark.table(MP_FULL_BUILD_PROGRESS).filter((F.col('build_id') == build_id) & (F.col('stage_name') == stage_name) & (F.col('bucket_id') == int(bucket_id)) & (F.col('status') == 'COMPLETE')).orderBy(F.desc('completed_at')).limit(1).collect()
    if not rows or not _table_exists(table_name):
        return None
    return rows[0].asDict()

def _install_pinned_full_build_views(manifest: dict) -> tuple[dict, list[str]]:
    originals = {}
    views = []
    build_token = re.sub('[^a-zA-Z0-9]', '', manifest['build_id'])[:12]
    for global_name, source_name in FULL_BUILD_GLOBAL_SOURCES.items():
        table_name = FULL_BUILD_INPUTS[source_name]
        version = int(manifest['source_versions'][source_name])
        view_name = f'mp_path_v3_{build_token}_{source_name}'
        try:
            (
                spark.read.format('delta')
                .option('versionAsOf', version)
                .table(table_name)
                .createOrReplaceTempView(view_name)
            )
        except Exception as exc:
            latest_version = _latest_delta_version(table_name)
            if version != latest_version or not _time_travel_retention_expired(exc):
                raise
            print(
                f'[map_pathology_v3] current snapshot fallback for pinned full-build '
                f'input {table_name} at version {version}'
            )
            spark.table(table_name).createOrReplaceTempView(view_name)
        originals[global_name] = globals()[global_name]
        globals()[global_name] = view_name
        views.append(view_name)
    return (originals, views)

def _restore_pinned_full_build_views(originals: dict, views: list[str]) -> None:
    for global_name, original_value in originals.items():
        globals()[global_name] = original_value
    for view_name in views:
        try:
            spark.catalog.dropTempView(view_name)
        except Exception:
            pass

def abandon_map_pathology_full_build(drop_stages: bool=False) -> dict:
    """Explicitly abandon the latest in-flight v3 build."""
    if not _table_exists(MP_FULL_BUILD_MANIFEST):
        return {'status': 'NO_ACTIVE_BUILD'}
    active = spark.table(MP_FULL_BUILD_MANIFEST).filter(~F.col('phase').isin('SUCCESS', 'ABANDONED')).orderBy(F.desc('updated_at')).limit(1).collect()
    if not active:
        return {'status': 'NO_ACTIVE_BUILD'}
    manifest = _manifest_row_to_dict(active[0])
    tables = [row['table_name'] for row in spark.table(MP_FULL_BUILD_PROGRESS).filter(F.col('build_id') == manifest['build_id']).select('table_name').where('table_name IS NOT NULL').distinct().collect()]
    _update_full_build_phase(manifest, 'ABANDONED')
    if drop_stages:
        for table_name in tables:
            _drop_table_if_exists(table_name)
    return {'status': 'ABANDONED', 'build_id': manifest['build_id'], 'stage_tables': tables, 'dropped': bool(drop_stages)}

def _read_changes(
    source_name: str,
    state: dict[str, dict],
    cutoffs: dict[str, dict],
    force_snapshot_fallback: bool=False,
) -> tuple[DataFrame, str]:
    """
    Read source changes up to the run-start cutoff.

    Preferred path is CDF by Delta version, which retains delete keys. Spark
    Connect evaluates Delta relations lazily, so both schema analysis and a
    one-row read stay inside the guarded block. If CDF history/files are
    unavailable, fall back to the existing ADC-watermark or full-key path.
    """
    cfg = cutoffs[source_name]
    table_name = cfg['table_name']
    end_version = cfg['end_version']
    ts_col = cfg['timestamp_column']
    previous = state.get(source_name)
    if previous is None or previous.get('last_delta_version') is None:
        return (_empty_like(table_name), 'seed')
    start_version = int(previous['last_delta_version']) + 1
    if start_version > end_version:
        return (_empty_like(table_name), 'no_change')
    if not force_snapshot_fallback:
        try:
            changed = (
                spark.read.format('delta')
                .option('readChangeFeed', 'true')
                .option('startingVersion', start_version)
                .option('endingVersion', end_version)
                .table(table_name)
                .filter(F.col('_change_type') != F.lit('update_preimage'))
            )
            # Force Spark Connect analysis while the fallback handler is active.
            _ = changed.schema
            changed.limit(1).collect()
            return (changed, 'cdf')
        except Exception as exc:
            print(
                f'[map_pathology_v3] CDF fallback for {table_name}: '
                f'{str(exc).splitlines()[0]}'
            )
    else:
        print(
            f'[map_pathology_v3] forced snapshot fallback for {table_name} '
            f'after deferred CDF read failure'
        )
    if ts_col is not None:
        previous_ts = previous.get('last_adc_updt') or datetime(1980, 1, 1)
        start_ts = previous_ts - SOURCE_LOOKBACK
        end_ts = cfg['end_adc_updt']
        changed = spark.table(table_name).filter(F.col(ts_col) > F.lit(start_ts))
        if end_ts is not None:
            changed = changed.filter(F.col(ts_col) <= F.lit(end_ts))
        return (changed, 'timestamp')
    return (spark.table(table_name), 'full_key_refresh')

def _advance_state(cutoffs: dict[str, dict], source_names: list[str] | None=None) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    selected = source_names or list(cutoffs)
    rows = [(name, cutoffs[name]['table_name'], int(cutoffs[name]['end_version']), cutoffs[name]['end_adc_updt'], now, MP_VERSION) for name in selected]
    schema = 'source_name STRING, table_name STRING, last_delta_version BIGINT, last_adc_updt TIMESTAMP, last_success_at TIMESTAMP, pipeline_version STRING'
    updates = spark.createDataFrame(rows, schema)
    DeltaTable.forName(spark, MP_STATE).alias('t').merge(updates.alias('s'), 't.source_name=s.source_name').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def _set_rebuild_flag(value: bool) -> None:
    spark.sql(f'UPDATE {_qn(MP_FLAG)} SET rebuild_flagged={str(value).upper()} WHERE id=1')

def _read_rebuild_flag() -> bool:
    row = spark.table(MP_FLAG).where('id=1').select('rebuild_flagged').first()
    return bool(row['rebuild_flagged']) if row else False

def _write_run_log(**values) -> None:
    ordered = ['run_id', 'started_at', 'completed_at', 'mode', 'status', 'pipeline_version', 'source_parent_count', 'staged_row_count', 'inserted_or_updated_rows', 'stale_rows_deleted', 'additive_map_keys', 'correction_map_keys', 'message']
    row = tuple((values.get(col) for col in ordered))
    schema = 'run_id STRING, started_at TIMESTAMP, completed_at TIMESTAMP, mode STRING, status STRING, pipeline_version STRING, source_parent_count BIGINT, staged_row_count BIGINT, inserted_or_updated_rows BIGINT, stale_rows_deleted BIGINT, additive_map_keys BIGINT, correction_map_keys BIGINT, message STRING'
    spark.createDataFrame([row], schema).write.mode('append').saveAsTable(MP_RUN_LOG)
NEW_COLUMN_COMMENTS = {'source_system': 'Explicit source system: CERNER or TFC_LIMS.', 'source_parent_key': 'Stable reconciliation scope: linked EVENT_ID or raw LIMSNo+LabNo.', 'source_record_key': 'Stable row identity used for MERGE and stale-row reconciliation.', 'source_adc_updt': 'Greatest contributing source update timestamp; never changed by mapping-only backfills.', 'loaded_at': 'Timestamp at which this bronze row was written.', 'mapping_updated_at': 'Timestamp at which mapping columns were evaluated.', 'source_payload_hash': 'Hash of source/provenance fields used for complete change detection.', 'mapping_payload_hash': 'Hash of mapped/derived fields used for complete mapping change detection.', 'LIMSNo': 'TFC/LIMS source-system identifier. Combined with LabNo for raw specimen identity.', 'source_sequence_start': 'Minimum TFC result sequence in the assembled raw result island.', 'source_sequence_end': 'Maximum TFC result sequence in the assembled raw result island.', 'source_line_count': 'Number of source result-level lines assembled into this row.', 'person_id_mrn': 'PERSON_ID candidate resolved from MRN when the alias is unambiguous.', 'person_id_nhs': 'PERSON_ID candidate resolved from NHS number when the alias is unambiguous.', 'person_match_status': 'native, agreed, mrn_only, nhs_only, conflict, ambiguous, or unresolved.', 'person_match_conflict': 'TRUE when MRN and NHS resolve to different PERSON_ID values.', 'measurement_datetime_source': 'Source field selected for measurement_datetime.', 'test_mapping_match_type': 'exact_context, native_event_cd, native_nlmc, safe_code, or unmapped.', 'result_mapping_match_type': 'exact_context, native_context, safe_code_result, or unmapped.', 'unit_mapping_match_type': 'exact, normalized, or unmapped.', 'result_parse_status': 'blank, numeric, datetime, or text.', 'value_as_datetime': 'Conservatively parsed date/time result; raw text remains in value_source_value.', 'data_quality_flags': 'Pipe-delimited non-filtering quality indicators.', 'reference_nbr': 'Full Cerner reference number; lab_no remains the legacy 11-character projection.', 'range_low_raw': 'Unmodified source normal-low text.', 'range_high_raw': 'Unmodified source normal-high text.'}

def _apply_table_metadata() -> None:
    spark.sql(f"\n        ALTER TABLE {_qn(MP_TARGET)} SET TBLPROPERTIES (\n          'delta.enableChangeDataFeed'='true',\n          'delta.enableRowTracking'='true',\n          'delta.parquet.compression.codec'='zstd',\n          'comment'='Pathology foundation table v2. Correct LIMS specimen grain, complete source provenance, deterministic source reconciliation, and OMOP-aligned mappings without row-quality filters.'\n        )\n        ")
    existing = _column_names(MP_TARGET)
    for column_name, comment in NEW_COLUMN_COMMENTS.items():
        if column_name not in existing:
            continue
        escaped = comment.replace('\\', '\\\\').replace("'", "''")
        spark.sql(f"ALTER TABLE {_qn(MP_TARGET)} ALTER COLUMN {_qn(column_name)} COMMENT '{escaped}'")
    if ENABLE_LIQUID_CLUSTERING and {'source_table', 'source_parent_key'}.issubset(existing):
        try:
            spark.sql(f'ALTER TABLE {_qn(MP_TARGET)} CLUSTER BY (source_table, source_parent_key)')
        except Exception as exc:
            print(f'[map_pathology_v3] clustering note: {str(exc).splitlines()[0]}')

def _target_is_v2() -> bool:
    required = {
        'source_parent_key',
        'source_record_key',
        'source_adc_updt',
        'source_payload_hash',
        'mapping_payload_hash',
        'LIMSNo',
    }
    return (
        _table_exists(MP_TARGET)
        and required.issubset(_column_names(MP_TARGET))
        and set(_column_names(MP_TARGET))
        == set(bronze_contract_column_names(MP_TARGET))
    )

def _prepare_incremental_scope(state: dict[str, dict], cutoffs: dict[str, dict], force_snapshot_fallback: bool=False) -> tuple[DataFrame, dict[str, str]]:
    """
    Build touched parent scopes from every contributing source.

    The target is deliberately used as a compact source-to-parent index for
    order/catalog/code/master/map changes. Direct clinical-event, result-level,
    and sample-level CDF rows retain keys for deletes.
    """
    if not _target_is_v2():
        raise RuntimeError('Incremental scope requires a v2 target; run a full rebuild first.')
    changes: dict[str, DataFrame] = {}
    modes: dict[str, str] = {}
    for source_name in SOURCE_TABLES:
        changes[source_name], modes[source_name] = _read_changes(
            source_name,
            state,
            cutoffs,
            force_snapshot_fallback=force_snapshot_fallback,
        )
        print(f'[map_pathology_v3] {source_name}: {modes[source_name]}')
    target = spark.table(MP_TARGET)
    linked_target = target.filter(F.col('source_table') == 'linked')
    raw_target = target.filter(F.col('source_table') == 'raw')
    linked_ids: list[DataFrame] = []
    raw_keys: list[DataFrame] = []
    linked_ids.append(changes['mill_clinical_event'].select(F.col('EVENT_ID').cast('long').alias('EVENT_ID')).filter(F.col('EVENT_ID').isNotNull()))
    raw_keys.append(changes['path_patient_resultlevel'].select(F.col('LIMSNo').cast('int').alias('LIMSNo'), F.col('LabNo').cast('string').alias('LabNo')))
    raw_keys.append(changes['path_patient_samplelevel'].select(F.col('LIMSNo').cast('int').alias('LIMSNo'), F.col('LabNo').cast('string').alias('LabNo')))
    changed_orders = changes['mill_orders'].select(F.col('ORDER_ID').cast('long').alias('ORDER_ID')).filter(F.col('ORDER_ID').isNotNull()).dropDuplicates()
    linked_ids.append(linked_target.join(changed_orders, linked_target.order_id == changed_orders.ORDER_ID, 'inner').select(F.col('source_event_id').cast('long').alias('EVENT_ID')))
    alias_changes = changes['mill_person_alias'].select(F.col('PERSON_ID').cast('long').alias('PERSON_ID'), F.col('PERSON_ALIAS_TYPE_CD').cast('int').alias('PERSON_ALIAS_TYPE_CD'), F.col('ALIAS').cast('string').alias('ALIAS'))
    changed_people = alias_changes.select('PERSON_ID').filter(F.col('PERSON_ID').isNotNull()).dropDuplicates()
    linked_ids.append(linked_target.join(changed_people, 'PERSON_ID', 'inner').select(F.col('source_event_id').cast('long').alias('EVENT_ID')))
    changed_mrn = alias_changes.filter(F.col('PERSON_ALIAS_TYPE_CD') == 10).select(F.col('ALIAS').alias('_changed_mrn')).filter(F.col('_changed_mrn').isNotNull()).dropDuplicates()
    changed_nhs = alias_changes.filter(F.col('PERSON_ALIAS_TYPE_CD') == 18).select(F.col('ALIAS').alias('_changed_nhs')).filter(F.col('_changed_nhs').isNotNull()).dropDuplicates()
    sample_index = spark.table(SAMPLE_LEVEL).select(F.col('LIMSNo').cast('int').alias('LIMSNo'), F.col('LabNo').cast('string').alias('LabNo'), F.col('MRN').cast('string').alias('MRN'), F.col('NHSNo').cast('string').alias('NHSNo'))
    raw_keys.append(sample_index.join(changed_mrn, sample_index.MRN == changed_mrn._changed_mrn, 'inner').select('LIMSNo', 'LabNo'))
    raw_keys.append(sample_index.join(changed_nhs, sample_index.NHSNo == changed_nhs._changed_nhs, 'inner').select('LIMSNo', 'LabNo'))
    changed_catalogs = changes['mill_order_catalog'].select(F.col('CATALOG_CD').cast('long').alias('CATALOG_CD')).filter(F.col('CATALOG_CD').isNotNull()).dropDuplicates()
    linked_ids.append(linked_target.join(changed_catalogs, linked_target.catalog_cd == changed_catalogs.CATALOG_CD, 'inner').select(F.col('source_event_id').cast('long').alias('EVENT_ID')))
    changed_codes = changes['mill_code_value'].select(F.col('CODE_VALUE').cast('long').alias('CODE_VALUE')).filter(F.col('CODE_VALUE').isNotNull()).dropDuplicates()
    linked_ids.append(reduce(lambda left, right: left.unionByName(right), [linked_target.join(changed_codes, linked_target[column] == changed_codes.CODE_VALUE, 'left_semi').select(F.col('source_event_id').cast('long').alias('EVENT_ID')) for column in ('EVENT_CD', 'result_units_cd', 'normalcy_cd')]))
    changed_master = changes['path_master_resultable'].select(F.col('WkgCode').cast('string').alias('_master_wkg'), F.col('TFCCode').cast('string').alias('_master_tfc')).dropDuplicates()
    raw_keys.append(raw_target.join(changed_master, raw_target.WkgCode.eqNullSafe(changed_master._master_wkg) & raw_target.code.eqNullSafe(changed_master._master_tfc), 'inner').select('LIMSNo', F.col('lab_no').alias('LabNo')))
    changed_test = changes['pathology_test_concept_map'].select(F.col('code_system').alias('_tm_system'), F.col('code').alias('_tm_code'), F.col('description').alias('_tm_description')).dropDuplicates()
    test_affected = target.join(changed_test, (target.code_system == changed_test._tm_system) & target.code.eqNullSafe(changed_test._tm_code), 'inner')
    linked_ids.append(test_affected.filter(F.col('source_table') == 'linked').select(F.col('source_event_id').cast('long').alias('EVENT_ID')))
    raw_keys.append(test_affected.filter(F.col('source_table') == 'raw').select('LIMSNo', F.col('lab_no').alias('LabNo')))
    changed_result = changes['pathology_result_concept_map'].select(F.col('code_system').alias('_rm_system'), F.col('code').alias('_rm_code'), F.col('description').alias('_rm_description'), F.col('result_normalized').alias('_rm_result')).dropDuplicates()
    target_with_result = target.withColumn('_target_result_normalized', F.lower(F.trim(F.regexp_replace(F.col('value_source_value'), '\\s+', ' '))))
    result_affected = target_with_result.join(changed_result, (target_with_result.code_system == changed_result._rm_system) & target_with_result.code.eqNullSafe(changed_result._rm_code) & target_with_result._target_result_normalized.eqNullSafe(changed_result._rm_result), 'inner')
    linked_ids.append(result_affected.filter(F.col('source_table') == 'linked').select(F.col('source_event_id').cast('long').alias('EVENT_ID')))
    raw_keys.append(result_affected.filter(F.col('source_table') == 'raw').select('LIMSNo', F.col('lab_no').alias('LabNo')))
    changed_units = changes['pathology_unit_map'].select(F.lower(F.trim(F.col('unit_source_value'))).alias('_unit_norm')).dropDuplicates()
    unit_affected = target.withColumn('_target_unit_norm', F.lower(F.trim(F.col('unit_source_value')))).join(changed_units, F.col('_target_unit_norm').eqNullSafe(changed_units._unit_norm), 'inner')
    linked_ids.append(unit_affected.filter(F.col('source_table') == 'linked').select(F.col('source_event_id').cast('long').alias('EVENT_ID')))
    raw_keys.append(unit_affected.filter(F.col('source_table') == 'raw').select('LIMSNo', F.col('lab_no').alias('LabNo')))
    native_test = spark.table(MP_NATIVE_TEST).select(F.col('key_type').alias('_nt_type'), F.col('key_value').alias('_nt_value')).dropDuplicates()
    native_test_scope = target.filter(F.col('measurement_concept_id').isNull())
    linked_ids.append(native_test_scope.filter(F.col('source_table') == 'linked').join(native_test.filter(F.col('_nt_type') == 'EVENT_CD'), F.col('EVENT_CD').cast('string') == F.col('_nt_value'), 'left_semi').select(F.col('source_event_id').cast('long').alias('EVENT_ID')))
    raw_keys.append(native_test_scope.filter(F.col('source_table') == 'raw').join(native_test.filter(F.col('_nt_type') == 'NLMC_ID'), F.col('nlmc_id').eqNullSafe(F.col('_nt_value')), 'left_semi').select('LIMSNo', F.col('lab_no').alias('LabNo')))
    native_result = spark.table(MP_NATIVE_RESULT).select(F.col('key_type').alias('_nr_type'), F.col('key_value').alias('_nr_value'), F.col('result_normalized').alias('_nr_result')).dropDuplicates()
    native_result_scope = target_with_result.filter(F.col('value_as_concept_id').isNull() & ~F.col('result_status').isin('numeric', 'datetime', 'missing'))
    linked_ids.append(native_result_scope.filter(F.col('source_table') == 'linked').join(native_result.filter(F.col('_nr_type') == 'EVENT_CD'), (F.col('EVENT_CD').cast('string') == F.col('_nr_value')) & F.col('_target_result_normalized').eqNullSafe(F.col('_nr_result')), 'left_semi').select(F.col('source_event_id').cast('long').alias('EVENT_ID')))
    raw_keys.append(native_result_scope.filter(F.col('source_table') == 'raw').join(native_result.filter(F.col('_nr_type') == 'NLMC_ID'), F.col('nlmc_id').eqNullSafe(F.col('_nr_value')) & F.col('_target_result_normalized').eqNullSafe(F.col('_nr_result')), 'left_semi').select('LIMSNo', F.col('lab_no').alias('LabNo')))
    linked_scope = reduce(lambda left, right: left.unionByName(right), linked_ids).filter(F.col('EVENT_ID').isNotNull()).dropDuplicates(['EVENT_ID'])
    raw_scope = reduce(lambda left, right: left.unionByName(right), raw_keys).dropDuplicates(['LIMSNo', 'LabNo'])
    linked_scope.createOrReplaceTempView('mp_v2_linked_scope')
    raw_scope.createOrReplaceTempView('mp_v2_raw_scope')
    linked_parents = linked_scope.select(F.lit('linked').alias('source_table'), F.concat(F.lit('linked|'), F.col('EVENT_ID').cast('string')).alias('source_parent_key'), F.col('EVENT_ID').cast('long').alias('source_event_id'), F.lit(None).cast('int').alias('LIMSNo'), F.lit(None).cast('string').alias('lab_no'))
    raw_parents = raw_scope.select(F.lit('raw').alias('source_table'), F.concat_ws('|', F.lit('raw'), F.coalesce(F.col('LIMSNo').cast('string'), F.lit('∅')), F.coalesce(F.col('LabNo'), F.lit('∅'))).alias('source_parent_key'), F.lit(None).cast('long').alias('source_event_id'), F.col('LIMSNo').cast('int').alias('LIMSNo'), F.col('LabNo').cast('string').alias('lab_no'))
    parents = linked_parents.unionByName(raw_parents).dropDuplicates(['source_parent_key'])
    return (parents, modes)

def _scope_from_parent_keys(parents: DataFrame) -> DataFrame:
    """Register source scope views from a target-derived set of parent keys."""
    target_keys = spark.table(MP_TARGET).select('source_parent_key', 'source_table', 'source_event_id', 'LIMSNo', 'lab_no').dropDuplicates(['source_parent_key'])
    expanded = target_keys.join(parents.select('source_parent_key').dropDuplicates(), 'source_parent_key', 'inner')
    expanded.filter(F.col('source_table') == 'linked').select(F.col('source_event_id').cast('long').alias('EVENT_ID')).dropDuplicates().createOrReplaceTempView('mp_v2_linked_scope')
    expanded.filter(F.col('source_table') == 'raw').select(F.col('LIMSNo').cast('int').alias('LIMSNo'), F.col('lab_no').cast('string').alias('LabNo')).dropDuplicates().createOrReplaceTempView('mp_v2_raw_scope')
    return expanded.select('source_table', 'source_parent_key', 'source_event_id', 'LIMSNo', 'lab_no').dropDuplicates(['source_parent_key'])

def _refresh_native_crosswalks() -> None:
    """
    Materialize conservative native-key crosswalks from already accepted mappings.

    Only curated/auto_high evidence is used, a native key must point to exactly
    one concept, and it must have at least NATIVE_MIN_SUPPORT supporting rows.
    This prevents the fallback from turning a context collision into a mapping.
    """
    if not _table_exists(MP_TARGET):
        spark.sql(f'\n            CREATE OR REPLACE TABLE {_qn(MP_NATIVE_TEST)} (\n              key_type STRING, key_value STRING, measurement_concept_id BIGINT,\n              concept_name STRING, confidence_tier STRING, support_count BIGINT\n            ) USING DELTA\n            ')
        spark.sql(f'\n            CREATE OR REPLACE TABLE {_qn(MP_NATIVE_RESULT)} (\n              key_type STRING, key_value STRING, result_normalized STRING,\n              value_as_concept_id BIGINT, concept_name STRING,\n              confidence_tier STRING, support_count BIGINT\n            ) USING DELTA\n            ')
        return
    spark.sql(f"\n        CREATE OR REPLACE TABLE {_qn(MP_NATIVE_TEST)} USING DELTA AS\n        WITH evidence AS (\n          SELECT 'EVENT_CD' AS key_type, CAST(EVENT_CD AS STRING) AS key_value,\n                 measurement_concept_id AS cid, COUNT(*) AS support_count\n          FROM {_qn(MP_TARGET)}\n          WHERE source_table='linked' AND EVENT_CD IS NOT NULL\n            AND measurement_concept_id IS NOT NULL\n            AND test_confidence_tier IN ('curated','auto_high')\n          GROUP BY EVENT_CD, measurement_concept_id\n          UNION ALL\n          SELECT 'NLMC_ID', nlmc_id, measurement_concept_id, COUNT(*)\n          FROM {_qn(MP_TARGET)}\n          WHERE source_table='raw' AND nlmc_id IS NOT NULL AND TRIM(nlmc_id)<>''\n            AND measurement_concept_id IS NOT NULL\n            AND test_confidence_tier IN ('curated','auto_high')\n          GROUP BY nlmc_id, measurement_concept_id\n        ),\n        safe AS (\n          SELECT key_type, key_value, MIN(cid) AS cid, SUM(support_count) AS support_count\n          FROM evidence\n          GROUP BY key_type, key_value\n          HAVING COUNT(DISTINCT cid)=1 AND SUM(support_count)>={NATIVE_MIN_SUPPORT}\n        )\n        SELECT s.key_type, s.key_value, s.cid AS measurement_concept_id,\n               c.concept_name, 'native_safe' AS confidence_tier, s.support_count\n        FROM safe s\n        LEFT JOIN {_qn(CONCEPT)} c ON c.concept_id=s.cid\n        ")
    spark.sql(f"\n        CREATE OR REPLACE TABLE {_qn(MP_NATIVE_RESULT)} USING DELTA AS\n        WITH evidence AS (\n          SELECT 'EVENT_CD' AS key_type, CAST(EVENT_CD AS STRING) AS key_value,\n                 LOWER(TRIM(REGEXP_REPLACE(value_source_value,'\\\\s+',' '))) AS result_normalized,\n                 value_as_concept_id AS cid, COUNT(*) AS support_count\n          FROM {_qn(MP_TARGET)}\n          WHERE source_table='linked' AND EVENT_CD IS NOT NULL\n            AND value_as_concept_id IS NOT NULL\n            AND result_confidence_tier IN ('curated','auto_high')\n          GROUP BY EVENT_CD,\n                   LOWER(TRIM(REGEXP_REPLACE(value_source_value,'\\\\s+',' '))),\n                   value_as_concept_id\n          UNION ALL\n          SELECT 'NLMC_ID', nlmc_id,\n                 LOWER(TRIM(REGEXP_REPLACE(value_source_value,'\\\\s+',' '))),\n                 value_as_concept_id, COUNT(*)\n          FROM {_qn(MP_TARGET)}\n          WHERE source_table='raw' AND nlmc_id IS NOT NULL AND TRIM(nlmc_id)<>''\n            AND value_as_concept_id IS NOT NULL\n            AND result_confidence_tier IN ('curated','auto_high')\n          GROUP BY nlmc_id,\n                   LOWER(TRIM(REGEXP_REPLACE(value_source_value,'\\\\s+',' '))),\n                   value_as_concept_id\n        ),\n        safe AS (\n          SELECT key_type, key_value, result_normalized,\n                 MIN(cid) AS cid, SUM(support_count) AS support_count\n          FROM evidence\n          GROUP BY key_type, key_value, result_normalized\n          HAVING COUNT(DISTINCT cid)=1 AND SUM(support_count)>={NATIVE_MIN_SUPPORT}\n        )\n        SELECT s.key_type, s.key_value, s.result_normalized,\n               s.cid AS value_as_concept_id, c.concept_name,\n               'native_safe' AS confidence_tier, s.support_count\n        FROM safe s\n        LEFT JOIN {_qn(CONCEPT)} c ON c.concept_id=s.cid\n        ")

def _exclusion_regex_sql() -> str:
    patterns = [row['pattern'] for row in spark.table(EXCL_TBL).select('pattern').where('pattern IS NOT NULL').collect()]
    if not patterns:
        return '(?!)'
    combined = '(' + '|'.join(patterns) + ')'
    return combined.replace('\\', '\\\\').replace("'", "''")

def _tier_sql(values: tuple[str, ...]) -> str:
    return '(' + ','.join((_sql_string(v) for v in values)) + ')'

def _mp_alias_ctes(alias_person_table: str | None=None, alias_resolver_table: str | None=None) -> str:
    if alias_person_table and alias_resolver_table:
        return f'\n    pa_person_mrn AS (\n      SELECT PERSON_ID,canonical_mrn,canonical_mrn_adc\n      FROM {_qn(alias_person_table)}\n      WHERE canonical_mrn IS NOT NULL\n    ),\n    pa_person_nhs AS (\n      SELECT PERSON_ID,canonical_nhs,canonical_nhs_adc\n      FROM {_qn(alias_person_table)}\n      WHERE canonical_nhs IS NOT NULL\n    ),\n    mrn_resolver AS (\n      SELECT ALIAS,PERSON_ID,person_count,ADC_UPDT\n      FROM {_qn(alias_resolver_table)}\n      WHERE alias_type=10\n    ),\n    nhs_resolver AS (\n      SELECT ALIAS,PERSON_ID,person_count,ADC_UPDT\n      FROM {_qn(alias_resolver_table)}\n      WHERE alias_type=18\n    )'
    return f'\n    pa_person_mrn AS (\n      SELECT PERSON_ID, ALIAS AS canonical_mrn, ADC_UPDT AS canonical_mrn_adc\n      FROM (\n        SELECT CAST(PERSON_ID AS BIGINT) AS PERSON_ID, ALIAS, ADC_UPDT,\n               ROW_NUMBER() OVER (\n                 PARTITION BY PERSON_ID\n                 ORDER BY BEG_EFFECTIVE_DT_TM DESC NULLS LAST,\n                          PERSON_ALIAS_ID DESC NULLS LAST\n               ) rn\n        FROM {_qn(PERSON_ALIAS)}\n        WHERE ACTIVE_IND=1 AND PERSON_ALIAS_TYPE_CD=10\n      ) WHERE rn=1\n    ),\n    pa_person_nhs AS (\n      SELECT PERSON_ID, ALIAS AS canonical_nhs, ADC_UPDT AS canonical_nhs_adc\n      FROM (\n        SELECT CAST(PERSON_ID AS BIGINT) AS PERSON_ID, ALIAS, ADC_UPDT,\n               ROW_NUMBER() OVER (\n                 PARTITION BY PERSON_ID\n                 ORDER BY BEG_EFFECTIVE_DT_TM DESC NULLS LAST,\n                          PERSON_ALIAS_ID DESC NULLS LAST\n               ) rn\n        FROM {_qn(PERSON_ALIAS)}\n        WHERE ACTIVE_IND=1 AND PERSON_ALIAS_TYPE_CD=18\n      ) WHERE rn=1\n    ),\n    mrn_alias_stats AS (\n      SELECT ALIAS, COUNT(DISTINCT PERSON_ID) AS person_count\n      FROM {_qn(PERSON_ALIAS)}\n      WHERE ACTIVE_IND=1 AND PERSON_ALIAS_TYPE_CD=10\n      GROUP BY ALIAS\n    ),\n    mrn_alias_latest AS (\n      SELECT ALIAS, CAST(PERSON_ID AS BIGINT) AS PERSON_ID, ADC_UPDT\n      FROM (\n        SELECT ALIAS, PERSON_ID, ADC_UPDT,\n               ROW_NUMBER() OVER (\n                 PARTITION BY ALIAS\n                 ORDER BY BEG_EFFECTIVE_DT_TM DESC NULLS LAST,\n                          PERSON_ALIAS_ID DESC NULLS LAST\n               ) rn\n        FROM {_qn(PERSON_ALIAS)}\n        WHERE ACTIVE_IND=1 AND PERSON_ALIAS_TYPE_CD=10\n      ) WHERE rn=1\n    ),\n    mrn_resolver AS (\n      SELECT l.ALIAS,\n             CASE WHEN s.person_count=1 THEN l.PERSON_ID END AS PERSON_ID,\n             s.person_count, l.ADC_UPDT\n      FROM mrn_alias_latest l\n      JOIN mrn_alias_stats s USING (ALIAS)\n    ),\n    nhs_alias_stats AS (\n      SELECT ALIAS, COUNT(DISTINCT PERSON_ID) AS person_count\n      FROM {_qn(PERSON_ALIAS)}\n      WHERE ACTIVE_IND=1 AND PERSON_ALIAS_TYPE_CD=18\n      GROUP BY ALIAS\n    ),\n    nhs_alias_latest AS (\n      SELECT ALIAS, CAST(PERSON_ID AS BIGINT) AS PERSON_ID, ADC_UPDT\n      FROM (\n        SELECT ALIAS, PERSON_ID, ADC_UPDT,\n               ROW_NUMBER() OVER (\n                 PARTITION BY ALIAS\n                 ORDER BY BEG_EFFECTIVE_DT_TM DESC NULLS LAST,\n                          PERSON_ALIAS_ID DESC NULLS LAST\n               ) rn\n        FROM {_qn(PERSON_ALIAS)}\n        WHERE ACTIVE_IND=1 AND PERSON_ALIAS_TYPE_CD=18\n      ) WHERE rn=1\n    ),\n    nhs_resolver AS (\n      SELECT l.ALIAS,\n             CASE WHEN s.person_count=1 THEN l.PERSON_ID END AS PERSON_ID,\n             s.person_count, l.ADC_UPDT\n      FROM nhs_alias_latest l\n      JOIN nhs_alias_stats s USING (ALIAS)\n    )'

def _mp_build_select(full: bool, branch: str='both', bucket_id: int | None=None, bucket_count: int | None=None, alias_person_table: str | None=None, alias_resolver_table: str | None=None, run_timestamp: datetime | None=None) -> str:
    """
    Return the complete source-to-target projection.

    Incremental scope views contain whole linked EVENT_ID parents and whole raw
    (LIMSNo, LabNo) parents, so reassembly and stale-row reconciliation are safe.
    """
    if branch not in {'both', 'linked', 'raw'}:
        raise ValueError(f'Unsupported pathology branch: {branch}')
    if (bucket_id is None) != (bucket_count is None):
        raise ValueError('bucket_id and bucket_count must be supplied together')
    if bucket_id is not None and (not 0 <= int(bucket_id) < int(bucket_count)):
        raise ValueError(f'Invalid bucket {bucket_id} of {bucket_count}')
    linked_scope_join = '' if full else 'INNER JOIN mp_v2_linked_scope scope ON scope.EVENT_ID=ce.EVENT_ID'
    raw_scope_result_join = '' if full else 'INNER JOIN mp_v2_raw_scope scope ON scope.LIMSNo <=> r.LIMSNo AND scope.LabNo <=> r.LabNo'
    raw_scope_sample_join = '' if full else 'INNER JOIN mp_v2_raw_scope scope ON scope.LIMSNo <=> sl.LIMSNo AND scope.LabNo <=> sl.LabNo'
    numeric_re = NUMERIC_REGEX.replace('\\', '\\\\').replace("'", "''")
    exclusion_re = _exclusion_regex_sql()
    test_tiers = _tier_sql(TEST_TIERS)
    result_tiers = _tier_sql(RESULT_TIERS)
    alias_ctes = _mp_alias_ctes(alias_person_table, alias_resolver_table)
    linked_bucket_filter = ''
    raw_result_bucket_filter = ''
    raw_sample_bucket_filter = ''
    if bucket_id is not None:
        linked_bucket_filter = f"AND PMOD(XXHASH64(COALESCE(CAST(CAST(ce.EVENT_ID AS BIGINT) AS STRING),'∅')),{int(bucket_count)})={int(bucket_id)}"
        raw_hash = f"PMOD(XXHASH64(CONCAT_WS('|',COALESCE(CAST(CAST({{alias}}.LIMSNo AS INT) AS STRING),'∅'),COALESCE({{alias}}.LabNo,'∅'))),{int(bucket_count)})={int(bucket_id)}"
        raw_result_bucket_filter = 'WHERE ' + raw_hash.format(alias='r')
        raw_sample_bucket_filter = 'WHERE ' + raw_hash.format(alias='sl')
    if branch == 'linked':
        source_union_sql = 'SELECT * FROM linked_src'
    elif branch == 'raw':
        source_union_sql = 'SELECT * FROM raw_src'
    else:
        source_union_sql = 'SELECT * FROM linked_src UNION ALL SELECT * FROM raw_src'
    sql_text = f"\n    WITH\n    {alias_ctes},\n    linked_ranked AS (\n      SELECT\n        ce.CLINICAL_EVENT_ID, ce.EVENT_ID, ce.PERSON_ID, ce.ENCNTR_ID,\n        ce.ORDER_ID, ce.CATALOG_CD, ce.EVENT_CD, ce.PARENT_EVENT_ID,\n        ce.EVENT_RELTN_CD, ce.VALID_FROM_DT_TM, ce.VALID_UNTIL_DT_TM,\n        ce.EVENT_START_DT_TM, ce.EVENT_END_DT_TM, ce.PERFORMED_DT_TM,\n        ce.VERIFIED_DT_TM, ce.RESULT_VAL, ce.RESULT_UNITS_CD,\n        ce.NORMAL_LOW, ce.NORMAL_HIGH, ce.NORMALCY_CD,\n        ce.RECORD_STATUS_CD, ce.RESULT_STATUS_CD, ce.AUTHENTIC_FLAG,\n        ce.CLINSIG_UPDT_DT_TM, ce.UPDT_CNT, ce.CONTRIBUTOR_SYSTEM_CD,\n        ce.REFERENCE_NBR, ce.EVENT_TITLE_TEXT, ce.EVENT_TAG,\n        ce.ADC_UPDT AS ce_adc,\n        o.ORDER_MNEMONIC, o.HNA_ORDER_MNEMONIC, o.ORDERED_AS_MNEMONIC,\n        o.ADC_UPDT AS order_adc,\n        oc.PRIMARY_MNEMONIC, oc.DESCRIPTION AS catalog_description,\n        oc.ADC_UPDT AS catalog_adc,\n        ROW_NUMBER() OVER (\n          PARTITION BY ce.EVENT_ID\n          ORDER BY ce.VALID_UNTIL_DT_TM DESC NULLS LAST,\n                   ce.UPDT_CNT DESC NULLS LAST,\n                   ce.CLINSIG_UPDT_DT_TM DESC NULLS LAST,\n                   ce.ADC_UPDT DESC NULLS LAST,\n                   ce.CLINICAL_EVENT_ID DESC NULLS LAST\n        ) AS rn\n      FROM {_qn(CE)} ce\n      {linked_scope_join}\n      LEFT JOIN {_qn(ORDERS)} o ON CAST(o.ORDER_ID AS BIGINT)=ce.ORDER_ID\n      LEFT JOIN {_qn(ORDER_CATALOG)} oc ON CAST(oc.CATALOG_CD AS BIGINT)=ce.CATALOG_CD\n      WHERE ce.EVENT_CLASS_CD IN (233,236)\n        AND COALESCE(CAST(oc.CATALOG_TYPE_CD AS BIGINT),\n                     CAST(o.CATALOG_TYPE_CD AS BIGINT))=2513\n        {linked_bucket_filter}\n    ),\n    linked_src AS (\n      SELECT\n        'linked' AS source_table,\n        'CERNER' AS source_system,\n        CONCAT('linked|', CAST(ce.EVENT_ID AS STRING)) AS source_parent_key,\n        CONCAT('linked|', CAST(ce.EVENT_ID AS STRING)) AS source_record_key,\n        CAST(ce.EVENT_ID AS BIGINT) AS source_event_id,\n        FALSE AS is_synthetic_key,\n        SUBSTRING(ce.REFERENCE_NBR,1,11) AS lab_no,\n        CAST(NULL AS INT) AS LIMSNo,\n        CAST(NULL AS BIGINT) AS source_sequence_start,\n        CAST(NULL AS BIGINT) AS source_sequence_end,\n        CAST(1 AS BIGINT) AS source_line_count,\n        CAST(NULL AS STRING) AS source_month_year,\n        CAST(NULL AS STRING) AS source_month_year_auth,\n        CAST(ce.PERSON_ID AS BIGINT) AS PERSON_ID,\n        CAST(ce.ENCNTR_ID AS BIGINT) AS ENCNTR_ID,\n        CAST(NULL AS BIGINT) AS person_id_mrn,\n        CAST(NULL AS BIGINT) AS person_id_nhs,\n        'native' AS person_match_status,\n        FALSE AS person_match_conflict,\n        pm.canonical_mrn AS MRN,\n        pn.canonical_nhs AS NHS_Number,\n        CAST(ce.EVENT_CD AS INT) AS EVENT_CD,\n        COALESCE(cv.DESCRIPTION,cv.DISPLAY,ce.EVENT_TITLE_TEXT,ce.EVENT_TAG) AS EVENT_CD_DISPLAY,\n        COALESCE(ce.EVENT_END_DT_TM,ce.PERFORMED_DT_TM,\n                 ce.EVENT_START_DT_TM,ce.VERIFIED_DT_TM) AS measurement_datetime,\n        CASE WHEN ce.EVENT_END_DT_TM IS NOT NULL THEN 'EVENT_END_DT_TM'\n             WHEN ce.PERFORMED_DT_TM IS NOT NULL THEN 'PERFORMED_DT_TM'\n             WHEN ce.EVENT_START_DT_TM IS NOT NULL THEN 'EVENT_START_DT_TM'\n             WHEN ce.VERIFIED_DT_TM IS NOT NULL THEN 'VERIFIED_DT_TM'\n        END AS measurement_datetime_source,\n        'CERNER_TESTCODE' AS code_system,\n        COALESCE(NULLIF(TRIM(ce.ORDER_MNEMONIC),''),\n                 NULLIF(TRIM(ce.HNA_ORDER_MNEMONIC),''),\n                 NULLIF(TRIM(ce.ORDERED_AS_MNEMONIC),''),\n                 NULLIF(TRIM(ce.PRIMARY_MNEMONIC),''),\n                 CAST(ce.EVENT_CD AS STRING)) AS code,\n        CASE WHEN NULLIF(TRIM(ce.ORDER_MNEMONIC),'') IS NOT NULL THEN 'ORDER_MNEMONIC'\n             WHEN NULLIF(TRIM(ce.HNA_ORDER_MNEMONIC),'') IS NOT NULL THEN 'HNA_ORDER_MNEMONIC'\n             WHEN NULLIF(TRIM(ce.ORDERED_AS_MNEMONIC),'') IS NOT NULL THEN 'ORDERED_AS_MNEMONIC'\n             WHEN NULLIF(TRIM(ce.PRIMARY_MNEMONIC),'') IS NOT NULL THEN 'PRIMARY_MNEMONIC'\n             ELSE 'EVENT_CD'\n        END AS code_source,\n        COALESCE(cv.DESCRIPTION,cv.DISPLAY,ce.EVENT_TITLE_TEXT,ce.EVENT_TAG,\n                 ce.catalog_description,CAST(ce.EVENT_CD AS STRING)) AS description,\n        CASE WHEN cv.DESCRIPTION IS NOT NULL THEN 'CODE_VALUE_DESCRIPTION'\n             WHEN cv.DISPLAY IS NOT NULL THEN 'CODE_VALUE_DISPLAY'\n             WHEN ce.EVENT_TITLE_TEXT IS NOT NULL THEN 'EVENT_TITLE_TEXT'\n             WHEN ce.EVENT_TAG IS NOT NULL THEN 'EVENT_TAG'\n             WHEN ce.catalog_description IS NOT NULL THEN 'ORDER_CATALOG_DESCRIPTION'\n             ELSE 'EVENT_CD'\n        END AS description_source,\n        ce.RESULT_VAL AS result_txt,\n        COALESCE(ucv.DESCRIPTION,ucv.DISPLAY) AS unit_source_value,\n        ce.NORMAL_LOW AS range_low_raw,\n        ce.NORMAL_HIGH AS range_high_raw,\n        TRY_CAST(ce.NORMAL_LOW AS DOUBLE) AS range_low,\n        TRY_CAST(ce.NORMAL_HIGH AS DOUBLE) AS range_high,\n        COALESCE(ncv.DESCRIPTION,ncv.DISPLAY) AS normalcy,\n        CAST(NULL AS STRING) AS WkgCode,\n        CAST(NULL AS STRING) AS nlmc_id,\n        COALESCE(ce.VERIFIED_DT_TM,ce.EVENT_END_DT_TM,ce.PERFORMED_DT_TM) AS ReportDate,\n        GREATEST(ce.ce_adc,ce.order_adc,ce.catalog_adc,cv.ADC_UPDT,\n                 ucv.ADC_UPDT,ncv.ADC_UPDT,pm.canonical_mrn_adc,pn.canonical_nhs_adc)\n          AS source_adc_updt,\n        ce.REFERENCE_NBR AS reference_nbr,\n        CAST(ce.CLINICAL_EVENT_ID AS BIGINT) AS clinical_event_id,\n        CAST(ce.ORDER_ID AS BIGINT) AS order_id,\n        CAST(ce.CATALOG_CD AS BIGINT) AS catalog_cd,\n        CAST(ce.PARENT_EVENT_ID AS BIGINT) AS parent_event_id,\n        CAST(ce.EVENT_RELTN_CD AS BIGINT) AS event_reltn_cd,\n        ce.VALID_FROM_DT_TM AS valid_from_dt_tm,\n        ce.VALID_UNTIL_DT_TM AS valid_until_dt_tm,\n        ce.EVENT_START_DT_TM AS event_start_dt_tm,\n        ce.EVENT_END_DT_TM AS event_end_dt_tm,\n        ce.PERFORMED_DT_TM AS performed_dt_tm,\n        ce.VERIFIED_DT_TM AS verified_dt_tm,\n        CAST(ce.RECORD_STATUS_CD AS BIGINT) AS record_status_cd,\n        CAST(ce.RESULT_STATUS_CD AS BIGINT) AS result_status_cd,\n        CAST(ce.AUTHENTIC_FLAG AS BIGINT) AS authentic_flag,\n        ce.CLINSIG_UPDT_DT_TM AS clinsig_updt_dt_tm,\n        CAST(ce.UPDT_CNT AS BIGINT) AS source_updt_cnt,\n        CAST(ce.CONTRIBUTOR_SYSTEM_CD AS BIGINT) AS contributor_system_cd,\n        CAST(ce.RESULT_UNITS_CD AS BIGINT) AS result_units_cd,\n        CAST(ce.NORMALCY_CD AS BIGINT) AS normalcy_cd,\n        CAST(NULL AS STRING) AS legacy_wkg_code,\n        CAST(NULL AS STRING) AS legacy_tfc_code,\n        CAST(NULL AS TIMESTAMP) AS request_dt,\n        CAST(NULL AS TIMESTAMP) AS sample_dt,\n        CAST(NULL AS TIMESTAMP) AS receipt_dt,\n        CAST(NULL AS TIMESTAMP) AS booked_in_dt,\n        CAST(NULL AS STRING) AS order_no,\n        CAST(NULL AS STRING) AS visit_id,\n        CAST(NULL AS STRING) AS ass_auth_code,\n        CAST(NULL AS STRING) AS body_site_code,\n        CAST(NULL AS STRING) AS specimen_type_code,\n        CAST(NULL AS STRING) AS specimen_category,\n        CAST(NULL AS STRING) AS urgent_flag,\n        CAST(NULL AS STRING) AS source_code,\n        CAST(NULL AS STRING) AS clinician_code,\n        CAST(NULL AS STRING) AS master_section_code,\n        CAST(NULL AS STRING) AS work_section_code,\n        CAST(NULL AS STRING) AS report_section,\n        CAST(NULL AS STRING) AS master_result_type,\n        CAST(NULL AS STRING) AS master_result_format,\n        CAST(NULL AS INT) AS master_num_val_upper,\n        CAST(NULL AS INT) AS master_num_val_dps\n      FROM linked_ranked ce\n      LEFT JOIN {_qn(CODE_VALUE)} cv ON CAST(cv.CODE_VALUE AS BIGINT)=ce.EVENT_CD\n      LEFT JOIN {_qn(CODE_VALUE)} ucv ON CAST(ucv.CODE_VALUE AS BIGINT)=ce.RESULT_UNITS_CD\n      LEFT JOIN {_qn(CODE_VALUE)} ncv ON CAST(ncv.CODE_VALUE AS BIGINT)=ce.NORMALCY_CD\n      LEFT JOIN pa_person_mrn pm ON pm.PERSON_ID=CAST(ce.PERSON_ID AS BIGINT)\n      LEFT JOIN pa_person_nhs pn ON pn.PERSON_ID=CAST(ce.PERSON_ID AS BIGINT)\n      WHERE ce.rn=1\n    ),\n    rl_pre AS (\n      SELECT\n        CAST(r.LIMSNo AS INT) AS LIMSNo,\n        r.LabNo,\n        COALESCE(NULLIF(TRIM(r.TFCCode),''),NULLIF(TRIM(r.LegTFCCode),'')) AS TFCCode,\n        COALESCE(NULLIF(TRIM(r.WkgCode),''),NULLIF(TRIM(r.LegWkgCode),'')) AS WkgCode,\n        r.LegTFCCode, r.LegWkgCode, CAST(r.TFCResultSeq AS BIGINT) AS TFCResultSeq,\n        r.TFCValue, r.MonthYear, r.MonthYearAuth, r.ADC_UPDT\n      FROM {_qn(RESULT_LEVEL)} r\n      {raw_scope_result_join}\n      {raw_result_bucket_filter}\n    ),\n    rl_island AS (\n      SELECT p.*,\n        CASE\n          WHEN TFCResultSeq IS NULL THEN\n            XXHASH64(CONCAT_WS('|',COALESCE(CAST(LIMSNo AS STRING),'∅'),\n              COALESCE(LabNo,'∅'),COALESCE(TFCCode,'∅'),COALESCE(WkgCode,'∅'),\n              COALESCE(TFCValue,'∅'),COALESCE(MonthYear,'∅')))\n          WHEN TFCCode RLIKE '^(INTER|UNU)' THEN TFCResultSeq\n          ELSE TFCResultSeq - DENSE_RANK() OVER (\n            PARTITION BY LIMSNo,LabNo,TFCCode,WkgCode\n            ORDER BY TFCResultSeq\n          )\n        END AS island_id\n      FROM rl_pre p\n    ),\n    rl AS (\n      SELECT\n        LIMSNo,LabNo,TFCCode,WkgCode,\n        MAX(LegTFCCode) AS LegTFCCode,\n        MAX(LegWkgCode) AS LegWkgCode,\n        MIN(TFCResultSeq) AS TFCResultSeq,\n        MAX(TFCResultSeq) AS TFCResultSeqEnd,\n        COUNT(*) AS source_line_count,\n        MIN(MonthYear) AS MonthYear,\n        MIN(MonthYearAuth) AS MonthYearAuth,\n        CONCAT_WS(\n          '\\n',\n          TRANSFORM(\n            SORT_ARRAY(COLLECT_LIST(NAMED_STRUCT(\n              'seq',TFCResultSeq,'adc',ADC_UPDT,'value',TFCValue\n            ))),\n            x -> x.value\n          )\n        ) AS TFCValue,\n        MAX(ADC_UPDT) AS ADC_UPDT,\n        island_id\n      FROM rl_island\n      GROUP BY LIMSNo,LabNo,TFCCode,WkgCode,island_id\n    ),\n    sl1 AS (\n      SELECT *\n      FROM (\n        SELECT\n          CAST(sl.LIMSNo AS INT) AS LIMSNo, sl.LabNo, sl.MRN, sl.NHSNo,\n          sl.AssAuthCode, sl.RequestDT, sl.SampleDT, sl.ReportDate,\n          sl.ReceiptDT, sl.BookedInDT, sl.OrderNo, sl.VisitID,\n          sl.BodySiteCode, sl.CSpecTypeCode, sl.SpecimenCategory,\n          sl.UrgentFlag, sl.SourceCode, sl.ClinicianCode,\n          sl.ADC_UPDT AS sample_adc,\n          ROW_NUMBER() OVER (\n            PARTITION BY sl.LIMSNo,sl.LabNo\n            ORDER BY sl.ADC_UPDT DESC NULLS LAST,\n                     sl.SampleDT DESC NULLS LAST,\n                     sl.ReportDate DESC NULLS LAST,\n                     sl.ReceiptDT DESC NULLS LAST,\n                     sl.BookedInDT DESC NULLS LAST,\n                     sl.OrderNo ASC NULLS LAST,\n                     sl.VisitID ASC NULLS LAST,\n                     sl.MRN ASC NULLS LAST,\n                     sl.NHSNo ASC NULLS LAST\n          ) rn\n        FROM {_qn(SAMPLE_LEVEL)} sl\n        {raw_scope_sample_join}\n        {raw_sample_bucket_filter}\n      ) WHERE rn=1\n    ),\n    m1 AS (\n      SELECT *\n      FROM (\n        SELECT\n          m.WkgCode,m.TFCCode,m.TFCDesc_Full,m.TFCDesc_Rep,m.TFCDesc_WP,\n          m.ReportingSynonym,m.PMIPDesc,m.Units,m.NLMC_ID,m.SectionCode,\n          m.WorkSectionCode,m.ReportSection,m.ResultType,m.ResultFormat,\n          m.NumValUpper,m.NumValDPs,m.LastUpdateDT,m.ADC_UPDT,\n          ROW_NUMBER() OVER (\n            PARTITION BY m.WkgCode,m.TFCCode\n            ORDER BY m.LastUpdateDT DESC NULLS LAST,\n                     m.ADC_UPDT DESC NULLS LAST\n          ) rn\n        FROM {_qn(MASTER_RESULT)} m\n      ) WHERE rn=1\n    ),\n    raw_person AS (\n      SELECT\n        rl.*,sl.* EXCEPT (LIMSNo,LabNo,rn),\n        mr.PERSON_ID AS person_id_mrn,\n        nr.PERSON_ID AS person_id_nhs,\n        mr.person_count AS mrn_person_count,\n        nr.person_count AS nhs_person_count,\n        mr.ADC_UPDT AS mrn_alias_adc,\n        nr.ADC_UPDT AS nhs_alias_adc,\n        CASE\n          WHEN mr.PERSON_ID IS NOT NULL AND nr.PERSON_ID IS NOT NULL\n               AND mr.PERSON_ID<>nr.PERSON_ID THEN CAST(NULL AS BIGINT)\n          ELSE COALESCE(mr.PERSON_ID,nr.PERSON_ID)\n        END AS resolved_person_id,\n        CASE\n          WHEN mr.PERSON_ID IS NOT NULL AND nr.PERSON_ID IS NOT NULL\n               AND mr.PERSON_ID<>nr.PERSON_ID THEN 'conflict'\n          WHEN mr.PERSON_ID IS NOT NULL AND nr.PERSON_ID=mr.PERSON_ID THEN 'agreed'\n          WHEN mr.PERSON_ID IS NOT NULL THEN 'mrn_only'\n          WHEN nr.PERSON_ID IS NOT NULL THEN 'nhs_only'\n          WHEN COALESCE(mr.person_count,0)>1 OR COALESCE(nr.person_count,0)>1 THEN 'ambiguous'\n          ELSE 'unresolved'\n        END AS person_match_status\n      FROM rl\n      LEFT JOIN sl1 sl ON sl.LIMSNo <=> rl.LIMSNo AND sl.LabNo <=> rl.LabNo\n      LEFT JOIN mrn_resolver mr ON mr.ALIAS=sl.MRN\n      LEFT JOIN nhs_resolver nr ON nr.ALIAS=sl.NHSNo\n    ),\n    raw_src AS (\n      SELECT\n        'raw' AS source_table,\n        'TFC_LIMS' AS source_system,\n        CONCAT_WS('|','raw',COALESCE(CAST(rp.LIMSNo AS STRING),'∅'),\n                  COALESCE(rp.LabNo,'∅')) AS source_parent_key,\n        CONCAT_WS('|','raw',COALESCE(CAST(rp.LIMSNo AS STRING),'∅'),\n                  COALESCE(rp.LabNo,'∅'),COALESCE(rp.TFCCode,'∅'),\n                  COALESCE(rp.WkgCode,'∅'),CAST(rp.island_id AS STRING))\n          AS source_record_key,\n        COALESCE(\n          rp.TFCResultSeq,\n          XXHASH64(CONCAT_WS('|',COALESCE(CAST(rp.LIMSNo AS STRING),'∅'),\n            COALESCE(rp.LabNo,'∅'),COALESCE(rp.TFCCode,'∅'),\n            COALESCE(rp.WkgCode,'∅'),CAST(rp.island_id AS STRING)))\n        ) AS source_event_id,\n        (rp.TFCResultSeq IS NULL) AS is_synthetic_key,\n        rp.LabNo AS lab_no,\n        rp.LIMSNo,\n        rp.TFCResultSeq AS source_sequence_start,\n        rp.TFCResultSeqEnd AS source_sequence_end,\n        CAST(rp.source_line_count AS BIGINT) AS source_line_count,\n        rp.MonthYear AS source_month_year,\n        rp.MonthYearAuth AS source_month_year_auth,\n        CAST(rp.resolved_person_id AS BIGINT) AS PERSON_ID,\n        CAST(NULL AS BIGINT) AS ENCNTR_ID,\n        CAST(rp.person_id_mrn AS BIGINT) AS person_id_mrn,\n        CAST(rp.person_id_nhs AS BIGINT) AS person_id_nhs,\n        rp.person_match_status,\n        (rp.person_match_status='conflict') AS person_match_conflict,\n        COALESCE(pm.canonical_mrn,rp.MRN) AS MRN,\n        COALESCE(pn.canonical_nhs,rp.NHSNo) AS NHS_Number,\n        CAST(NULL AS INT) AS EVENT_CD,\n        CAST(NULL AS STRING) AS EVENT_CD_DISPLAY,\n        COALESCE(rp.SampleDT,rp.RequestDT,rp.ReceiptDT,rp.BookedInDT,rp.ReportDate)\n          AS measurement_datetime,\n        CASE WHEN rp.SampleDT IS NOT NULL THEN 'SampleDT'\n             WHEN rp.RequestDT IS NOT NULL THEN 'RequestDT'\n             WHEN rp.ReceiptDT IS NOT NULL THEN 'ReceiptDT'\n             WHEN rp.BookedInDT IS NOT NULL THEN 'BookedInDT'\n             WHEN rp.ReportDate IS NOT NULL THEN 'ReportDate'\n        END AS measurement_datetime_source,\n        'TFC' AS code_system,\n        rp.TFCCode AS code,\n        CASE WHEN NULLIF(TRIM(rp.TFCCode),'') IS NOT NULL\n                  AND rp.TFCCode <=> rp.LegTFCCode THEN 'LegTFCCode'\n             WHEN NULLIF(TRIM(rp.TFCCode),'') IS NOT NULL THEN 'TFCCode'\n             ELSE 'missing'\n        END AS code_source,\n        COALESCE(m1.TFCDesc_Full,m1.TFCDesc_Rep,m1.TFCDesc_WP,\n                 m1.ReportingSynonym,m1.PMIPDesc,rp.TFCCode) AS description,\n        CASE WHEN m1.TFCDesc_Full IS NOT NULL THEN 'TFCDesc_Full'\n             WHEN m1.TFCDesc_Rep IS NOT NULL THEN 'TFCDesc_Rep'\n             WHEN m1.TFCDesc_WP IS NOT NULL THEN 'TFCDesc_WP'\n             WHEN m1.ReportingSynonym IS NOT NULL THEN 'ReportingSynonym'\n             WHEN m1.PMIPDesc IS NOT NULL THEN 'PMIPDesc'\n             WHEN rp.TFCCode IS NOT NULL THEN 'TFCCode'\n             ELSE 'missing'\n        END AS description_source,\n        rp.TFCValue AS result_txt,\n        m1.Units AS unit_source_value,\n        CAST(NULL AS STRING) AS range_low_raw,\n        CAST(NULL AS STRING) AS range_high_raw,\n        CAST(NULL AS DOUBLE) AS range_low,\n        CAST(NULL AS DOUBLE) AS range_high,\n        CAST(NULL AS STRING) AS normalcy,\n        rp.WkgCode,\n        m1.NLMC_ID AS nlmc_id,\n        rp.ReportDate,\n        GREATEST(rp.ADC_UPDT,rp.sample_adc,rp.mrn_alias_adc,rp.nhs_alias_adc,\n                 m1.ADC_UPDT,pm.canonical_mrn_adc,pn.canonical_nhs_adc)\n          AS source_adc_updt,\n        CAST(NULL AS STRING) AS reference_nbr,\n        CAST(NULL AS BIGINT) AS clinical_event_id,\n        CAST(NULL AS BIGINT) AS order_id,\n        CAST(NULL AS BIGINT) AS catalog_cd,\n        CAST(NULL AS BIGINT) AS parent_event_id,\n        CAST(NULL AS BIGINT) AS event_reltn_cd,\n        CAST(NULL AS TIMESTAMP) AS valid_from_dt_tm,\n        CAST(NULL AS TIMESTAMP) AS valid_until_dt_tm,\n        CAST(NULL AS TIMESTAMP) AS event_start_dt_tm,\n        CAST(NULL AS TIMESTAMP) AS event_end_dt_tm,\n        CAST(NULL AS TIMESTAMP) AS performed_dt_tm,\n        CAST(NULL AS TIMESTAMP) AS verified_dt_tm,\n        CAST(NULL AS BIGINT) AS record_status_cd,\n        CAST(NULL AS BIGINT) AS result_status_cd,\n        CAST(NULL AS BIGINT) AS authentic_flag,\n        CAST(NULL AS TIMESTAMP) AS clinsig_updt_dt_tm,\n        CAST(NULL AS BIGINT) AS source_updt_cnt,\n        CAST(NULL AS BIGINT) AS contributor_system_cd,\n        CAST(NULL AS BIGINT) AS result_units_cd,\n        CAST(NULL AS BIGINT) AS normalcy_cd,\n        rp.LegWkgCode AS legacy_wkg_code,\n        rp.LegTFCCode AS legacy_tfc_code,\n        rp.RequestDT AS request_dt,\n        rp.SampleDT AS sample_dt,\n        rp.ReceiptDT AS receipt_dt,\n        rp.BookedInDT AS booked_in_dt,\n        rp.OrderNo AS order_no,\n        rp.VisitID AS visit_id,\n        rp.AssAuthCode AS ass_auth_code,\n        rp.BodySiteCode AS body_site_code,\n        rp.CSpecTypeCode AS specimen_type_code,\n        rp.SpecimenCategory AS specimen_category,\n        rp.UrgentFlag AS urgent_flag,\n        rp.SourceCode AS source_code,\n        rp.ClinicianCode AS clinician_code,\n        m1.SectionCode AS master_section_code,\n        m1.WorkSectionCode AS work_section_code,\n        m1.ReportSection AS report_section,\n        m1.ResultType AS master_result_type,\n        m1.ResultFormat AS master_result_format,\n        CAST(m1.NumValUpper AS INT) AS master_num_val_upper,\n        CAST(m1.NumValDPs AS INT) AS master_num_val_dps\n      FROM raw_person rp\n      LEFT JOIN m1 ON m1.WkgCode <=> rp.WkgCode AND m1.TFCCode <=> rp.TFCCode\n      LEFT JOIN pa_person_mrn pm ON pm.PERSON_ID=rp.resolved_person_id\n      LEFT JOIN pa_person_nhs pn ON pn.PERSON_ID=rp.resolved_person_id\n    ),\n    source_union AS (\n      {source_union_sql}\n    ),\n    combined AS (\n      SELECT s.*,\n        XXHASH64(\n          source_parent_key,source_record_key,PERSON_ID,ENCNTR_ID,MRN,NHS_Number,\n          measurement_datetime,code,description,result_txt,unit_source_value,\n          range_low_raw,range_high_raw,normalcy,WkgCode,nlmc_id,ReportDate,\n          reference_nbr,clinical_event_id,order_id,catalog_cd,valid_until_dt_tm,\n          source_adc_updt,request_dt,sample_dt,receipt_dt,booked_in_dt\n        ) AS source_payload_hash\n      FROM source_union s\n    ),\n    test_map_ranked AS (\n      SELECT *\n      FROM (\n        SELECT tm.*,\n          ROW_NUMBER() OVER (\n            PARTITION BY code_system,code,description\n            ORDER BY CASE confidence_tier\n                       WHEN 'curated' THEN 1 WHEN 'auto_high' THEN 2\n                       WHEN 'auto_low' THEN 3 ELSE 9 END,\n                     mapping_version DESC NULLS LAST,mapped_at DESC NULLS LAST\n          ) rn\n        FROM {_qn(TEST_MAP)} tm\n        WHERE confidence_tier IN {test_tiers}\n          AND measurement_concept_id IS NOT NULL\n      ) WHERE rn=1\n    ),\n    test_code_observed_desc AS (\n      SELECT DISTINCT code_system,code,description FROM combined\n    ),\n    test_code_desc_coverage AS (\n      SELECT o.code_system,o.code,\n             COUNT(*) AS n_observed_descriptions,\n             SUM(CASE WHEN tm.measurement_concept_id IS NULL THEN 0 ELSE 1 END) AS n_mapped_descriptions\n      FROM test_code_observed_desc o\n      LEFT JOIN test_map_ranked tm\n        ON tm.code_system=o.code_system\n       AND tm.code <=> o.code\n       AND tm.description <=> o.description\n      GROUP BY o.code_system,o.code\n    ),\n    test_code_safe AS (\n      SELECT r.code_system,r.code,MIN(r.measurement_concept_id) AS measurement_concept_id,\n             MIN(r.concept_name) AS concept_name,'safe_code' AS confidence_tier\n      FROM test_map_ranked r\n      JOIN test_code_desc_coverage cov\n        ON cov.code_system=r.code_system\n       AND cov.code <=> r.code\n       AND cov.n_mapped_descriptions=cov.n_observed_descriptions\n      GROUP BY r.code_system,r.code\n      HAVING COUNT(DISTINCT r.measurement_concept_id)=1\n    ),\n    test_joined AS (\n      SELECT c.*,\n        COALESCE(tm.measurement_concept_id,nt.measurement_concept_id,\n                 tc.measurement_concept_id) AS measurement_concept_id,\n        COALESCE(tm.concept_name,nt.concept_name,tc.concept_name)\n          AS measurement_concept_name,\n        COALESCE(tm.confidence_tier,nt.confidence_tier,tc.confidence_tier)\n          AS test_confidence_tier,\n        CASE WHEN tm.measurement_concept_id IS NOT NULL THEN 'exact_context'\n             WHEN nt.measurement_concept_id IS NOT NULL AND c.source_table='linked'\n               THEN 'native_event_cd'\n             WHEN nt.measurement_concept_id IS NOT NULL THEN 'native_nlmc'\n             WHEN tc.measurement_concept_id IS NOT NULL THEN 'safe_code'\n             ELSE 'unmapped'\n        END AS test_mapping_match_type\n      FROM combined c\n      LEFT JOIN test_map_ranked tm\n        ON tm.code_system=c.code_system\n       AND tm.code <=> c.code\n       AND tm.description <=> c.description\n      LEFT JOIN {_qn(MP_NATIVE_TEST)} nt\n        ON nt.key_type=CASE WHEN c.source_table='linked' THEN 'EVENT_CD' ELSE 'NLMC_ID' END\n       AND nt.key_value <=> CASE WHEN c.source_table='linked'\n                                THEN CAST(c.EVENT_CD AS STRING) ELSE c.nlmc_id END\n      LEFT JOIN test_code_safe tc\n        ON tc.code_system=c.code_system AND tc.code <=> c.code\n    ),\n    result_derived AS (\n      SELECT t.*,\n        CASE WHEN result_txt RLIKE '{numeric_re}' THEN 1 ELSE 0 END AS rd_result_numeric,\n        CASE WHEN result_txt RLIKE '^\\\\s*<=' THEN 4171754\n             WHEN result_txt RLIKE '^\\\\s*>=' THEN 4171755\n             WHEN result_txt RLIKE '^\\\\s*[<]' THEN 4171756\n             WHEN result_txt RLIKE '^\\\\s*[>]' THEN 4172704\n             WHEN result_txt RLIKE '^\\\\s*≤' THEN 4171754\n             WHEN result_txt RLIKE '^\\\\s*≥' THEN 4171755\n             ELSE NULL\n        END AS rd_operator_concept_id,\n        CASE WHEN result_txt RLIKE '{numeric_re}'\n             THEN TRY_CAST(\n               REGEXP_REPLACE(TRIM(result_txt),'^(?:<=|>=|<|>|≤|≥|=)\\\\s*','')\n               AS DOUBLE\n             )\n        END AS rd_value_as_number,\n        CASE WHEN result_txt IS NOT NULL AND TRIM(result_txt)<>''\n                   AND NOT (result_txt RLIKE '{numeric_re}')\n             THEN LOWER(TRIM(REGEXP_REPLACE(result_txt,'\\\\s+',' ')))\n        END AS rd_result_normalized,\n        CASE WHEN result_txt IS NOT NULL AND TRIM(result_txt)<>''\n                   AND NOT (result_txt RLIKE '{numeric_re}')\n             THEN COALESCE(\n               TRY_TO_TIMESTAMP(TRIM(result_txt),'dd.MM.yyyy'),\n               TRY_TO_TIMESTAMP(TRIM(result_txt),'dd/MM/yyyy'),\n               TRY_TO_TIMESTAMP(TRIM(result_txt),'yyyy-MM-dd'),\n               TRY_TO_TIMESTAMP(TRIM(result_txt),'dd.MM.yy'),\n               TRY_TO_TIMESTAMP(TRIM(result_txt),'dd/MM/yy')\n             )\n        END AS rd_value_as_datetime\n      FROM test_joined t\n    ),\n    result_map_ranked AS (\n      SELECT *\n      FROM (\n        SELECT rm.*,\n          ROW_NUMBER() OVER (\n            PARTITION BY code_system,code,description,result_normalized\n            ORDER BY CASE confidence_tier\n                       WHEN 'curated' THEN 1 WHEN 'auto_high' THEN 2\n                       WHEN 'auto_anchor' THEN 3 WHEN 'auto_value' THEN 4\n                       WHEN 'auto_genpos' THEN 5 WHEN 'auto_low' THEN 6 ELSE 9 END,\n                     mapping_version DESC NULLS LAST,mapped_at DESC NULLS LAST\n          ) rn\n        FROM {_qn(RESULT_MAP)} rm\n        WHERE confidence_tier IN {result_tiers}\n          AND value_as_concept_id IS NOT NULL\n      ) WHERE rn=1\n    ),\n    result_code_observed_desc AS (\n      SELECT DISTINCT code_system,code,description,rd_result_normalized\n      FROM result_derived\n      WHERE rd_result_numeric=0\n    ),\n    result_code_desc_coverage AS (\n      SELECT o.code_system,o.code,o.rd_result_normalized,\n             COUNT(*) AS n_observed_descriptions,\n             SUM(CASE WHEN rm.value_as_concept_id IS NULL THEN 0 ELSE 1 END) AS n_mapped_descriptions\n      FROM result_code_observed_desc o\n      LEFT JOIN result_map_ranked rm\n        ON rm.code_system=o.code_system\n       AND rm.code <=> o.code\n       AND rm.description <=> o.description\n       AND rm.result_normalized <=> o.rd_result_normalized\n      GROUP BY o.code_system,o.code,o.rd_result_normalized\n    ),\n    result_code_safe AS (\n      SELECT r.code_system,r.code,r.result_normalized,\n             MIN(r.value_as_concept_id) AS value_as_concept_id,\n             MIN(r.concept_name) AS concept_name,\n             MIN(r.confidence_tier) AS confidence_tier,\n             (MAX(CAST(r.is_suspected AS INT))=1) AS is_suspected,\n             MIN(r.growth_grade) AS growth_grade\n      FROM result_map_ranked r\n      JOIN result_code_desc_coverage cov\n        ON cov.code_system=r.code_system\n       AND cov.code <=> r.code\n       AND cov.rd_result_normalized <=> r.result_normalized\n       AND cov.n_mapped_descriptions=cov.n_observed_descriptions\n      GROUP BY r.code_system,r.code,r.result_normalized\n      HAVING COUNT(DISTINCT r.value_as_concept_id)=1\n    ),\n    result_joined AS (\n      SELECT d.*,\n        CASE WHEN d.rd_result_numeric=1 THEN CAST(NULL AS BIGINT)\n             ELSE COALESCE(rm.value_as_concept_id,nr.value_as_concept_id,\n                           rc.value_as_concept_id)\n        END AS value_as_concept_id,\n        COALESCE(rm.concept_name,nr.concept_name,rc.concept_name) AS result_concept_name,\n        COALESCE(rm.confidence_tier,nr.confidence_tier,rc.confidence_tier)\n          AS result_confidence_tier,\n        COALESCE(rm.is_suspected,rc.is_suspected) AS result_is_suspected,\n        COALESCE(rm.growth_grade,rc.growth_grade) AS result_growth_grade,\n        CASE WHEN d.rd_result_numeric=1 THEN 'numeric'\n             WHEN rm.value_as_concept_id IS NOT NULL THEN 'exact_context'\n             WHEN nr.value_as_concept_id IS NOT NULL THEN 'native_context'\n             WHEN rc.value_as_concept_id IS NOT NULL THEN 'safe_code_result'\n             ELSE 'unmapped'\n        END AS result_mapping_match_type\n      FROM result_derived d\n      LEFT JOIN result_map_ranked rm\n        ON rm.code_system=d.code_system\n       AND rm.code <=> d.code\n       AND rm.description <=> d.description\n       AND rm.result_normalized <=> d.rd_result_normalized\n       AND d.rd_result_numeric=0\n      LEFT JOIN {_qn(MP_NATIVE_RESULT)} nr\n        ON nr.key_type=CASE WHEN d.source_table='linked' THEN 'EVENT_CD' ELSE 'NLMC_ID' END\n       AND nr.key_value <=> CASE WHEN d.source_table='linked'\n                                THEN CAST(d.EVENT_CD AS STRING) ELSE d.nlmc_id END\n       AND nr.result_normalized <=> d.rd_result_normalized\n       AND d.rd_result_numeric=0\n      LEFT JOIN result_code_safe rc\n        ON rc.code_system=d.code_system\n       AND rc.code <=> d.code\n       AND rc.result_normalized <=> d.rd_result_normalized\n       AND d.rd_result_numeric=0\n    ),\n    unit_exact AS (\n      SELECT unit_source_value,unit_concept_id,ucum_code\n      FROM (\n        SELECT um.*,\n          ROW_NUMBER() OVER (\n            PARTITION BY unit_source_value\n            ORDER BY unit_concept_id DESC NULLS LAST,ucum_code ASC NULLS LAST\n          ) rn\n        FROM {_qn(UNIT_MAP)} um\n        WHERE unit_concept_id IS NOT NULL\n      ) WHERE rn=1\n    ),\n    unit_normalized AS (\n      SELECT LOWER(TRIM(unit_source_value)) AS unit_norm,\n             MIN(unit_concept_id) AS unit_concept_id,\n             MIN(ucum_code) AS ucum_code\n      FROM unit_exact\n      WHERE unit_source_value IS NOT NULL AND TRIM(unit_source_value)<>''\n      GROUP BY LOWER(TRIM(unit_source_value))\n      HAVING COUNT(DISTINCT unit_concept_id)=1\n    ),\n    unit_joined AS (\n      SELECT r.*,\n        COALESCE(ue.unit_concept_id,un.unit_concept_id) AS unit_concept_id,\n        COALESCE(ue.ucum_code,un.ucum_code) AS ucum_code,\n        CASE WHEN ue.unit_concept_id IS NOT NULL THEN 'exact'\n             WHEN un.unit_concept_id IS NOT NULL THEN 'normalized'\n             ELSE 'unmapped'\n        END AS unit_mapping_match_type\n      FROM result_joined r\n      LEFT JOIN unit_exact ue ON ue.unit_source_value <=> r.unit_source_value\n      LEFT JOIN unit_normalized un\n        ON un.unit_norm=LOWER(TRIM(r.unit_source_value))\n       AND ue.unit_concept_id IS NULL\n    ),\n    projected AS (\n      SELECT\n        u.source_table,\n        CAST(u.source_event_id AS BIGINT) AS source_event_id,\n        u.is_synthetic_key,\n        u.lab_no,\n        CAST(u.PERSON_ID AS BIGINT) AS PERSON_ID,\n        CAST(u.ENCNTR_ID AS BIGINT) AS ENCNTR_ID,\n        u.MRN,u.NHS_Number,u.EVENT_CD,u.EVENT_CD_DISPLAY,\n        u.measurement_datetime,u.code_system,u.code,u.description,\n        CASE WHEN mc.vocabulary_id='SNOMED' THEN mc.concept_code END AS test_snomed_code,\n        CASE WHEN mc.vocabulary_id='LOINC' THEN mc.concept_code END AS test_loinc_code,\n        CAST(u.measurement_concept_id AS BIGINT) AS test_omop_concept_id,\n        mc.standard_concept AS test_omop_standard_concept,\n        mc.vocabulary_id AS test_vocabulary_id,\n        CAST(u.measurement_concept_id AS BIGINT) AS measurement_concept_id,\n        u.measurement_concept_name,u.test_confidence_tier,\n        u.rd_value_as_number AS value_as_number,\n        CAST(u.rd_operator_concept_id AS BIGINT) AS operator_concept_id,\n        CAST(u.value_as_concept_id AS BIGINT) AS value_as_concept_id,\n        u.result_concept_name,u.result_confidence_tier,\n        u.result_is_suspected,u.result_growth_grade,\n        CASE WHEN rcpt.vocabulary_id='SNOMED' THEN rcpt.concept_code END AS result_snomed_code,\n        CASE WHEN rcpt.vocabulary_id='LOINC' THEN rcpt.concept_code END AS result_loinc_code,\n        CAST(u.value_as_concept_id AS BIGINT) AS result_omop_concept_id,\n        rcpt.standard_concept AS result_omop_standard_concept,\n        rcpt.vocabulary_id AS result_vocabulary_id,\n        CASE\n          WHEN u.result_txt IS NULL OR TRIM(u.result_txt)='' THEN 'missing'\n          WHEN u.rd_result_numeric=1 THEN 'numeric'\n          WHEN u.rd_value_as_datetime IS NOT NULL THEN 'datetime'\n          WHEN u.value_as_concept_id IS NOT NULL THEN 'mapped'\n          WHEN u.rd_result_normalized RLIKE '{exclusion_re}' THEN 'excluded'\n          WHEN NOT (u.rd_result_normalized RLIKE '[a-z0-9]') THEN 'sentinel'\n          ELSE 'free_text'\n        END AS result_status,\n        u.result_txt AS value_source_value,\n        u.unit_source_value,u.unit_concept_id,u.ucum_code,\n        u.range_low,u.range_high,u.normalcy,u.WkgCode,u.nlmc_id,u.ReportDate,\n        CURRENT_TIMESTAMP() AS ADC_UPDT,\n        u.source_system,u.source_parent_key,u.source_record_key,\n        u.source_adc_updt,CURRENT_TIMESTAMP() AS loaded_at,\n        CURRENT_TIMESTAMP() AS mapping_updated_at,\n        u.source_payload_hash,\n        XXHASH64(\n          u.measurement_concept_id,u.measurement_concept_name,u.test_confidence_tier,\n          u.value_as_concept_id,u.result_concept_name,u.result_confidence_tier,\n          u.result_is_suspected,u.result_growth_grade,u.unit_concept_id,u.ucum_code,\n          u.rd_value_as_number,u.rd_operator_concept_id\n        ) AS mapping_payload_hash,\n        u.LIMSNo,u.source_sequence_start,u.source_sequence_end,u.source_line_count,\n        u.source_month_year,u.source_month_year_auth,\n        u.person_id_mrn,u.person_id_nhs,u.person_match_status,u.person_match_conflict,\n        u.measurement_datetime_source,u.code_source,u.description_source,\n        u.test_mapping_match_type,u.result_mapping_match_type,u.unit_mapping_match_type,\n        CASE WHEN u.result_txt IS NULL OR TRIM(u.result_txt)='' THEN 'blank'\n             WHEN u.rd_result_numeric=1 THEN 'numeric'\n             WHEN u.rd_value_as_datetime IS NOT NULL THEN 'datetime'\n             ELSE 'text'\n        END AS result_parse_status,\n        u.rd_value_as_datetime AS value_as_datetime,\n        u.range_low_raw,u.range_high_raw,u.reference_nbr,u.clinical_event_id,\n        u.order_id,u.catalog_cd,u.parent_event_id,u.event_reltn_cd,\n        u.valid_from_dt_tm,u.valid_until_dt_tm,u.event_start_dt_tm,u.event_end_dt_tm,\n        u.performed_dt_tm,u.verified_dt_tm,u.record_status_cd,u.result_status_cd,\n        u.authentic_flag,u.clinsig_updt_dt_tm,u.source_updt_cnt,\n        u.contributor_system_cd,u.result_units_cd,u.normalcy_cd,\n        u.legacy_wkg_code,u.legacy_tfc_code,u.request_dt,u.sample_dt,\n        u.receipt_dt,u.booked_in_dt,u.order_no,u.visit_id,u.ass_auth_code,\n        u.body_site_code,u.specimen_type_code,u.specimen_category,u.urgent_flag,\n        u.source_code,u.clinician_code,u.master_section_code,u.work_section_code,\n        u.report_section,u.master_result_type,u.master_result_format,\n        u.master_num_val_upper,u.master_num_val_dps,\n        CONCAT_WS('|',\n          CASE WHEN u.person_match_conflict THEN 'PERSON_ID_CONFLICT' END,\n          CASE WHEN u.PERSON_ID IS NULL THEN 'PERSON_ID_UNRESOLVED' END,\n          CASE WHEN u.measurement_datetime IS NULL THEN 'MEASUREMENT_DATETIME_MISSING' END,\n          CASE WHEN u.measurement_datetime<TIMESTAMP'1900-01-01' THEN 'MEASUREMENT_DATETIME_SENTINEL' END,\n          CASE WHEN u.measurement_datetime>CURRENT_TIMESTAMP()+INTERVAL 1 DAY THEN 'MEASUREMENT_DATETIME_FUTURE' END,\n          CASE WHEN u.code IS NULL OR TRIM(u.code)='' THEN 'CODE_MISSING' END,\n          CASE WHEN u.description IS NULL OR TRIM(u.description)='' THEN 'DESCRIPTION_MISSING' END,\n          CASE WHEN u.result_txt IS NULL OR TRIM(u.result_txt)='' THEN 'RESULT_BLANK' END,\n          CASE WHEN u.is_synthetic_key THEN 'SYNTHETIC_SOURCE_KEY' END,\n          CASE WHEN u.source_table='raw' AND u.LIMSNo IS NULL THEN 'LIMSNO_MISSING' END,\n          CASE WHEN u.source_table='raw' AND u.ReportDate<u.measurement_datetime\n                     AND TO_DATE(u.ReportDate)<TO_DATE(u.measurement_datetime)\n               THEN 'REPORT_BEFORE_SAMPLE_DATE' END\n        ) AS data_quality_flags\n      FROM unit_joined u\n      LEFT JOIN {_qn(CONCEPT)} mc ON mc.concept_id=u.measurement_concept_id\n      LEFT JOIN {_qn(CONCEPT)} rcpt ON rcpt.concept_id=u.value_as_concept_id\n    )\n    SELECT * FROM projected\n    "
    if run_timestamp is not None:
        sql_text = sql_text.replace('CURRENT_TIMESTAMP()', _ts_literal(run_timestamp))
    return sql_text

def _stage_table(run_id: str, suffix: str) -> str:
    safe_run = re.sub('[^a-zA-Z0-9_]', '_', run_id)
    return f'{MP_CONTROL_SCHEMA}._tmp_map_pathology_v2_{safe_run}_{suffix}'

def _full_stage_table(build_id: str, suffix: str) -> str:
    """Durable namespace intentionally excluded from generic scratch cleanup."""
    safe_build = re.sub('[^a-zA-Z0-9_]', '_', build_id)
    return f'{MP_CONTROL_SCHEMA}.map_pathology_build_{safe_build}_{suffix}'

def _materialize_stage(sql_text: str, table_name: str, count_rows: bool=True) -> int | None:
    df = spark.sql(sql_text)
    df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(table_name)
    return int(spark.table(table_name).count()) if count_rows else None

def _validate_stage(table_name: str) -> dict[str, int]:
    stage = spark.table(table_name)
    required = {'source_table', 'source_parent_key', 'source_record_key', 'source_event_id', 'source_payload_hash', 'mapping_payload_hash'}
    missing = required - set(stage.columns)
    if missing:
        raise RuntimeError(f'Stage is missing required columns: {sorted(missing)}')
    null_keys = stage.filter(F.col('source_parent_key').isNull() | F.col('source_record_key').isNull()).limit(1).count()
    if null_keys:
        raise RuntimeError('Stage contains NULL parent or record keys.')
    duplicate = stage.groupBy('source_record_key').count().filter(F.col('count') > 1).orderBy(F.desc('count')).limit(10).collect()
    if duplicate:
        raise RuntimeError('Stage contains duplicate source_record_key values: ' + ', '.join((f"{r['source_record_key']} ({r['count']})" for r in duplicate)))
    invalid_raw_parent = stage.filter(F.col('source_table') == 'raw').filter(~F.col('source_parent_key').startswith('raw|')).limit(1).count()
    if invalid_raw_parent:
        raise RuntimeError('Raw stage contains an invalid source_parent_key.')
    return {'row_count': int(stage.count()), 'parent_count': int(stage.select('source_parent_key').distinct().count())}

def _full_replace(stage_table: str) -> None:
    bronze_project_contract(
        spark.table(stage_table), MP_TARGET
    ).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('delta.enableChangeDataFeed', 'true').option('delta.enableRowTracking', 'true').saveAsTable(MP_TARGET)
    _apply_table_metadata()

def _merge_and_reconcile(stage_table: str, touched_parents: DataFrame) -> dict[str, int]:
    """
    Merge current rows, then delete obsolete children under touched parents.

    Because the stage contains every child for each touched parent, this safely
    handles mutable result text/descriptions, re-keying, and CDF delete events.
    """
    source = bronze_project_contract(
        spark.table(stage_table), MP_TARGET
    )
    target = spark.table(MP_TARGET)
    source_keys = source.select('source_record_key').dropDuplicates()
    parent_keys = touched_parents.select('source_parent_key').dropDuplicates()
    changed_count = int(source.alias('s').join(target.select('source_record_key', 'source_payload_hash', 'mapping_payload_hash').alias('t'), 'source_record_key', 'left').filter(F.col('t.source_record_key').isNull() | ~F.col('s.source_payload_hash').eqNullSafe(F.col('t.source_payload_hash')) | ~F.col('s.mapping_payload_hash').eqNullSafe(F.col('t.mapping_payload_hash'))).count())
    assignments = {
        column_name: F.col(f's.{column_name}')
        for column_name in source.columns
    }
    comparisons = ' OR '.join(
        f'NOT (t.`{column_name}` <=> s.`{column_name}`)'
        for column_name in source.columns
        if column_name != 'source_record_key'
    )
    DeltaTable.forName(spark, MP_TARGET).alias('t').merge(
        source.alias('s'),
        't.source_record_key=s.source_record_key',
    ).whenMatchedUpdate(
        condition=comparisons or 'false',
        set=assignments,
    ).whenNotMatchedInsert(values=assignments).execute()
    stale = spark.table(MP_TARGET).join(parent_keys, 'source_parent_key', 'inner').join(source_keys, 'source_record_key', 'left_anti').select('source_record_key').dropDuplicates()
    stale_count = int(stale.count())
    if stale_count:
        DeltaTable.forName(spark, MP_TARGET).alias('t').merge(stale.alias('s'), 't.source_record_key=s.source_record_key').whenMatchedDelete().execute()
    None
    return {'changed_rows': changed_count, 'stale_rows_deleted': stale_count}

def _drop_table_if_exists(table_name: str) -> None:
    spark.sql(f'DROP TABLE IF EXISTS {_qn(table_name)}')

def _full_build_alias_person_sql() -> str:
    return f'\n    WITH ranked AS (\n      SELECT\n        CAST(PERSON_ID AS BIGINT) AS PERSON_ID,\n        CAST(PERSON_ALIAS_TYPE_CD AS INT) AS alias_type,\n        ALIAS,\n        ADC_UPDT,\n        ROW_NUMBER() OVER (\n          PARTITION BY PERSON_ID,PERSON_ALIAS_TYPE_CD\n          ORDER BY BEG_EFFECTIVE_DT_TM DESC NULLS LAST,\n                   PERSON_ALIAS_ID DESC NULLS LAST\n        ) AS rn\n      FROM {_qn(PERSON_ALIAS)}\n      WHERE ACTIVE_IND=1 AND PERSON_ALIAS_TYPE_CD IN (10,18)\n    )\n    SELECT\n      PERSON_ID,\n      MAX(CASE WHEN alias_type=10 THEN ALIAS END) AS canonical_mrn,\n      MAX(CASE WHEN alias_type=10 THEN ADC_UPDT END) AS canonical_mrn_adc,\n      MAX(CASE WHEN alias_type=18 THEN ALIAS END) AS canonical_nhs,\n      MAX(CASE WHEN alias_type=18 THEN ADC_UPDT END) AS canonical_nhs_adc\n    FROM ranked\n    WHERE rn=1\n    GROUP BY PERSON_ID\n    '

def _full_build_alias_resolver_sql() -> str:
    return f'\n    WITH base AS (\n      SELECT\n        CAST(PERSON_ALIAS_TYPE_CD AS INT) AS alias_type,\n        ALIAS,\n        CAST(PERSON_ID AS BIGINT) AS PERSON_ID,\n        ADC_UPDT,\n        BEG_EFFECTIVE_DT_TM,\n        PERSON_ALIAS_ID\n      FROM {_qn(PERSON_ALIAS)}\n      WHERE ACTIVE_IND=1\n        AND PERSON_ALIAS_TYPE_CD IN (10,18)\n        AND ALIAS IS NOT NULL\n    ),\n    stats AS (\n      SELECT alias_type,ALIAS,COUNT(DISTINCT PERSON_ID) AS person_count\n      FROM base\n      GROUP BY alias_type,ALIAS\n    ),\n    latest AS (\n      SELECT alias_type,ALIAS,PERSON_ID,ADC_UPDT\n      FROM (\n        SELECT *,\n          ROW_NUMBER() OVER (\n            PARTITION BY alias_type,ALIAS\n            ORDER BY BEG_EFFECTIVE_DT_TM DESC NULLS LAST,\n                     PERSON_ALIAS_ID DESC NULLS LAST\n          ) AS rn\n        FROM base\n      )\n      WHERE rn=1\n    )\n    SELECT\n      l.alias_type,\n      l.ALIAS,\n      CASE WHEN s.person_count=1 THEN l.PERSON_ID END AS PERSON_ID,\n      s.person_count,\n      l.ADC_UPDT\n    FROM latest l\n    JOIN stats s\n      ON s.alias_type=l.alias_type AND s.ALIAS=l.ALIAS\n    '

def _retryable_full_build_failure(exc: Exception) -> bool:
    message = str(exc).lower()
    return _mp_builtins.any((token in message for token in ('fetchfailedexception', 'metadatafetchfailedexception', 'failed shuffle fetch', 'map output validation failed', 'executor lost', 'executor heartbeat timed out', 'connection reset')))

def _run_durable_full_stage(build_id: str, stage_name: str, bucket_id: int, table_name: str, sql_text: str, validator=None) -> dict:
    completed = _completed_full_build_progress(build_id, stage_name, bucket_id, table_name)
    if completed is not None:
        print(f"[map_pathology_v3] REUSE {stage_name} bucket={bucket_id}; rows={int(completed.get('row_count') or 0):,}")
        return {'row_count': int(completed.get('row_count') or 0), 'parent_count': int(completed.get('parent_count') or 0), 'table_name': table_name}
    attempts = int(FULL_BUILD_STAGE_RETRIES) + 1
    for attempt in _mp_builtins.range(1, attempts + 1):
        _write_full_build_progress(build_id, stage_name, bucket_id, 'RUNNING', table_name=table_name)
        try:
            _drop_table_if_exists(table_name)
            materialized_count = _materialize_stage(sql_text, table_name, count_rows=validator is None)
            metrics = validator(table_name) if validator is not None else {'row_count': int(materialized_count or 0), 'parent_count': 0}
            metrics['table_name'] = table_name
            _write_full_build_progress(build_id, stage_name, bucket_id, 'COMPLETE', table_name=table_name, row_count=int(metrics.get('row_count') or 0), parent_count=int(metrics.get('parent_count') or 0))
            print(f"[map_pathology_v3] COMPLETE {stage_name} bucket={bucket_id}; rows={int(metrics.get('row_count') or 0):,}")
            return metrics
        except Exception as exc:
            _write_full_build_progress(build_id, stage_name, bucket_id, 'FAILED', table_name=table_name, error=str(exc))
            retryable = _retryable_full_build_failure(exc)
            if not retryable or attempt >= attempts:
                raise
            print(f'[map_pathology_v3] RETRY {stage_name} bucket={bucket_id}; attempt={attempt + 1}/{attempts}; reason={str(exc).splitlines()[0][:500]}')
    raise RuntimeError(f'Stage did not complete: {stage_name} bucket={bucket_id}')

def _validate_full_bucket(table_name: str, branch: str) -> dict[str, int]:
    metrics = _validate_stage(table_name)
    wrong_branch = spark.table(table_name).filter(F.col('source_table') != F.lit(branch)).limit(1).count()
    if wrong_branch:
        raise RuntimeError(f'{branch} bucket contains rows from a different source branch')
    return metrics

def _phase_reached(manifest: dict, required_phase: str) -> bool:
    order = {'BUILDING': 0, 'STAGES_COMPLETE': 1, 'CANDIDATE_COMPLETE': 2, 'PUBLISHED': 3, 'BASELINE_READY': 4, 'EMBEDDING_READY': 5, 'SUCCESS': 6, 'ABANDONED': -1}
    return order.get(manifest['phase'], -1) >= order[required_phase]

def _build_full_alias_stages(manifest: dict) -> tuple[str, str]:
    build_id = manifest['build_id']
    person_table = _full_stage_table(build_id, 'alias_person')
    resolver_table = _full_stage_table(build_id, 'alias_resolver')
    _run_durable_full_stage(build_id, 'ALIAS_PERSON', -1, person_table, _full_build_alias_person_sql())
    _run_durable_full_stage(build_id, 'ALIAS_RESOLVER', -1, resolver_table, _full_build_alias_resolver_sql())
    return (person_table, resolver_table)

def _validate_linked_full_build_keys() -> None:
    invalid = spark.sql(f'\n        SELECT 1\n        FROM {_qn(CE)} ce\n        LEFT JOIN {_qn(ORDERS)} o\n          ON CAST(o.ORDER_ID AS BIGINT)=ce.ORDER_ID\n        LEFT JOIN {_qn(ORDER_CATALOG)} oc\n          ON CAST(oc.CATALOG_CD AS BIGINT)=ce.CATALOG_CD\n        WHERE ce.EVENT_CLASS_CD IN (233,236)\n          AND COALESCE(CAST(oc.CATALOG_TYPE_CD AS BIGINT),\n                       CAST(o.CATALOG_TYPE_CD AS BIGINT))=2513\n          AND ce.EVENT_ID IS NULL\n        LIMIT 1\n        ').count()
    if invalid:
        raise RuntimeError('Pinned pathology clinical events contain a NULL EVENT_ID; the full build refuses to silently omit an unbucketable parent.')

def _build_full_output_buckets(manifest: dict, alias_person_table: str, alias_resolver_table: str) -> list[dict]:
    build_id = manifest['build_id']
    results = []
    for branch, bucket_count in (('linked', FULL_BUILD_LINKED_BUCKETS), ('raw', FULL_BUILD_RAW_BUCKETS)):
        for bucket_id in _mp_builtins.range(int(bucket_count)):
            table_name = _full_stage_table(build_id, f'{branch}_b{bucket_id:03d}')
            sql_text = _mp_build_select(full=True, branch=branch, bucket_id=bucket_id, bucket_count=bucket_count, alias_person_table=alias_person_table, alias_resolver_table=alias_resolver_table, run_timestamp=manifest['run_timestamp'])
            metrics = _run_durable_full_stage(build_id, f'{branch.upper()}_BUCKET', bucket_id, table_name, sql_text, validator=lambda name, expected=branch: _validate_full_bucket(name, expected))
            metrics['branch'] = branch
            metrics['bucket_id'] = bucket_id
            results.append(metrics)
    return results

def _build_full_candidate(manifest: dict, bucket_metrics: list[dict]) -> tuple[str, dict]:
    build_id = manifest['build_id']
    candidate_table = _full_stage_table(build_id, 'candidate')
    expected_rows = _mp_builtins.sum((int(item['row_count']) for item in bucket_metrics))
    expected_parents = _mp_builtins.sum((int(item['parent_count']) for item in bucket_metrics))
    union_sql = ' UNION ALL '.join((f"SELECT * FROM {_qn(item['table_name'])}" for item in bucket_metrics))

    def validate_candidate(table_name: str) -> dict[str, int]:
        actual_rows = int(spark.table(table_name).count())
        if actual_rows != expected_rows:
            raise RuntimeError(f'Pathology candidate row count does not equal its completed buckets: candidate={actual_rows}, buckets={expected_rows}')
        missing = {'source_table', 'source_parent_key', 'source_record_key', 'source_payload_hash', 'mapping_payload_hash'} - set(spark.table(table_name).columns)
        if missing:
            raise RuntimeError(f'Pathology candidate is missing columns: {sorted(missing)}')
        return {'row_count': actual_rows, 'parent_count': expected_parents}
    metrics = _run_durable_full_stage(build_id, 'CANDIDATE', -1, candidate_table, union_sql, validator=validate_candidate)
    return (candidate_table, metrics)

def _full_build_bucket_metrics(manifest: dict) -> list[dict]:
    rows = spark.table(MP_FULL_BUILD_PROGRESS).filter((F.col('build_id') == manifest['build_id']) & F.col('stage_name').isin('LINKED_BUCKET', 'RAW_BUCKET') & (F.col('status') == 'COMPLETE')).collect()
    expected = int(FULL_BUILD_LINKED_BUCKETS) + int(FULL_BUILD_RAW_BUCKETS)
    if _mp_builtins.len(rows) != expected:
        raise RuntimeError(f'Expected {expected} completed pathology buckets, found {_mp_builtins.len(rows)}')
    return [{'branch': 'linked' if row['stage_name'] == 'LINKED_BUCKET' else 'raw', 'bucket_id': int(row['bucket_id']), 'table_name': row['table_name'], 'row_count': int(row['row_count'] or 0), 'parent_count': int(row['parent_count'] or 0)} for row in rows]

def _run_restartable_full_build(manifest: dict) -> dict:
    originals: dict = {}
    views: list[str] = []
    try:
        if not _phase_reached(manifest, 'CANDIDATE_COMPLETE'):
            originals, views = _install_pinned_full_build_views(manifest)
            _validate_linked_full_build_keys()
            alias_person, alias_resolver = _build_full_alias_stages(manifest)
            bucket_metrics = _build_full_output_buckets(manifest, alias_person, alias_resolver)
            _update_full_build_phase(manifest, 'STAGES_COMPLETE')
            candidate_table, candidate_metrics = _build_full_candidate(manifest, bucket_metrics)
            _update_full_build_phase(manifest, 'CANDIDATE_COMPLETE')
        else:
            bucket_metrics = _full_build_bucket_metrics(manifest)
            candidate_table = _full_stage_table(manifest['build_id'], 'candidate')
            completed = _completed_full_build_progress(manifest['build_id'], 'CANDIDATE', -1, candidate_table)
            if completed is None:
                raise RuntimeError('The manifest says CANDIDATE_COMPLETE but the durable candidate table or completion marker is missing. Abandon the build explicitly.')
            candidate_metrics = {'row_count': int(completed.get('row_count') or 0), 'parent_count': int(completed.get('parent_count') or 0)}
        if not _phase_reached(manifest, 'PUBLISHED'):
            _full_replace(candidate_table)
            target_rows = int(spark.table(MP_TARGET).count())
            if target_rows != int(candidate_metrics['row_count']):
                raise RuntimeError(f"Published pathology row count differs from the validated candidate: target={target_rows}, candidate={candidate_metrics['row_count']}")
            _update_full_build_phase(manifest, 'PUBLISHED')
        elif not _target_is_v2():
            raise RuntimeError('The pathology manifest is PUBLISHED but the canonical target is missing or no longer has the v2 contract. Abandon the build explicitly.')
        return {'manifest': manifest, 'source_stage': candidate_table, 'row_count': int(candidate_metrics['row_count']), 'parent_count': int(candidate_metrics['parent_count']), 'bucket_metrics': bucket_metrics}
    finally:
        _restore_pinned_full_build_views(originals, views)

def _cleanup_full_build_stages(build_id: str) -> None:
    tables = [row['table_name'] for row in spark.table(MP_FULL_BUILD_PROGRESS).filter(F.col('build_id') == build_id).select('table_name').where('table_name IS NOT NULL').distinct().collect()]
    for table_name in tables:
        try:
            _drop_table_if_exists(table_name)
        except Exception as exc:
            print(f'[map_pathology_v3] stage cleanup warning for {table_name}: {str(exc).splitlines()[0][:500]}')

def discover_new_keys_from_stage(stage_table: str) -> dict[str, int]:
    """
    Queue unmapped keys from the exact rows already materialized for this run.

    This replaces the old source rescan and uses result_parse_status from the
    shared parser, eliminating the permissive [0-9.]+ numeric drift.
    """
    queue_name = globals().get('QUEUE', f'{MAP_SCHEMA}.pathology_embed_queue')
    embeddings_name = globals().get('EMBEDDINGS_TABLE', '3_lookup.embeddings.terms')
    if not _table_exists(queue_name):
        raise RuntimeError(f'Embedding queue does not exist: {queue_name}')
    stage = spark.table(stage_table)
    tests = stage.filter(F.col('measurement_concept_id').isNull() & F.col('code').isNotNull() & F.col('description').isNotNull()).select('code_system', 'code', 'description', F.lit(None).cast('string').alias('result_normalized'), F.concat_ws(' | ', F.lower(F.col('code')), F.lower(F.col('description'))).alias('term'), F.lit('test').alias('kind')).dropDuplicates(['code_system', 'code', 'description', 'kind'])
    results = stage.filter(F.col('value_as_concept_id').isNull() & (F.col('result_parse_status') == 'text') & (F.col('result_status') == 'free_text') & F.col('value_source_value').isNotNull()).withColumn('result_normalized', F.lower(F.trim(F.regexp_replace(F.col('value_source_value'), '\\s+', ' ')))).filter((F.col('result_normalized') != '') & F.col('result_normalized').rlike('[a-z0-9]')).select('code_system', 'code', 'description', 'result_normalized', F.concat_ws(' | ', F.lower(F.coalesce(F.col('description'), F.col('code'))), F.col('result_normalized')).alias('term'), F.lit('result').alias('kind')).dropDuplicates(['code_system', 'code', 'description', 'result_normalized', 'kind'])
    candidates = tests.unionByName(results).withColumn('term_norm', F.lower(F.col('term'))).withColumn('embed_text', F.when(F.col('kind') == 'result', F.col('result_normalized')).otherwise(F.col('term')))
    queue = spark.table(queue_name).select('code_system', 'code', 'description', 'kind', 'term', 'result_normalized')
    anti_condition = (candidates.code_system == queue.code_system) & candidates.code.eqNullSafe(queue.code) & candidates.description.eqNullSafe(queue.description) & (candidates.kind == queue.kind) & candidates.result_normalized.eqNullSafe(queue.result_normalized) & (F.lower(candidates.term) == F.lower(queue.term))
    missing = candidates.join(queue, anti_condition, 'left_anti')
    embedded = spark.table(embeddings_name).select(F.lower(F.col('term')).alias('_embedded_term')).filter(F.col('_embedded_term').isNotNull()).dropDuplicates()
    queued = missing.join(embedded, F.lower(missing.embed_text) == embedded._embedded_term, 'left').withColumn('status', F.when(F.col('_embedded_term').isNotNull(), F.lit('vector_ready')).otherwise(F.lit('pending'))).drop('_embedded_term')
    counts = {(r['kind'], r['status']): int(r['count']) for r in queued.groupBy('kind', 'status').count().collect()}
    queue_columns = set(_column_names(queue_name))
    output_columns = ['code_system', 'code', 'description', 'term', 'kind', 'status', 'term_norm', 'result_normalized', 'embed_text']
    output_columns = [c for c in output_columns if c in queue_columns]
    if queued.limit(1).count():
        queued.select(*output_columns).write.mode('append').saveAsTable(queue_name)
    return {'test_keys': _mp_builtins.sum((v for (kind, _), v in counts.items() if kind == 'test')), 'result_keys': _mp_builtins.sum((v for (kind, _), v in counts.items() if kind == 'result')), 'pending': _mp_builtins.sum((v for (_, status), v in counts.items() if status == 'pending')), 'vector_ready': _mp_builtins.sum((v for (_, status), v in counts.items() if status == 'vector_ready'))}

def _current_mapping_snapshot() -> DataFrame:
    """Return one deterministic row per consumed test/result/unit map key."""
    return spark.sql(f"\n        WITH tm AS (\n          SELECT *\n          FROM (\n            SELECT t.*,\n              ROW_NUMBER() OVER (\n                PARTITION BY code_system,code,description\n                ORDER BY CASE confidence_tier\n                           WHEN 'curated' THEN 1 WHEN 'auto_high' THEN 2\n                           WHEN 'auto_low' THEN 3 ELSE 9 END,\n                         mapping_version DESC NULLS LAST,mapped_at DESC NULLS LAST\n              ) rn\n            FROM {_qn(TEST_MAP)} t\n            WHERE confidence_tier IN {_tier_sql(TEST_TIERS)}\n          ) WHERE rn=1\n        ),\n        rm AS (\n          SELECT *\n          FROM (\n            SELECT r.*,\n              ROW_NUMBER() OVER (\n                PARTITION BY code_system,code,description,result_normalized\n                ORDER BY CASE confidence_tier\n                           WHEN 'curated' THEN 1 WHEN 'auto_high' THEN 2\n                           WHEN 'auto_anchor' THEN 3 WHEN 'auto_value' THEN 4\n                           WHEN 'auto_genpos' THEN 5 WHEN 'auto_low' THEN 6 ELSE 9 END,\n                         mapping_version DESC NULLS LAST,mapped_at DESC NULLS LAST\n              ) rn\n            FROM {_qn(RESULT_MAP)} r\n            WHERE confidence_tier IN {_tier_sql(RESULT_TIERS)}\n          ) WHERE rn=1\n        ),\n        um AS (\n          SELECT LOWER(TRIM(unit_source_value)) AS unit_norm,\n                 MIN(unit_concept_id) AS unit_concept_id,\n                 MIN(ucum_code) AS ucum_code\n          FROM {_qn(UNIT_MAP)}\n          GROUP BY LOWER(TRIM(unit_source_value))\n          HAVING COUNT(DISTINCT unit_concept_id)<=1\n        )\n        SELECT 'test' AS map_kind,code_system,code,description,\n               CAST(NULL AS STRING) AS result_normalized,\n               CAST(NULL AS STRING) AS unit_norm,\n               CAST(measurement_concept_id AS BIGINT) AS concept_id,\n               XXHASH64(measurement_concept_id,concept_name,confidence_tier,\n                        test_mapping_source,similarity_score) AS payload_hash\n        FROM tm\n        UNION ALL\n        SELECT 'result',code_system,code,description,result_normalized,NULL,\n               CAST(value_as_concept_id AS BIGINT),\n               XXHASH64(value_as_concept_id,concept_name,confidence_tier,\n                        result_mapping_source,similarity_score,is_suspected,growth_grade)\n        FROM rm\n        UNION ALL\n        SELECT 'unit',NULL,NULL,NULL,NULL,unit_norm,\n               CAST(unit_concept_id AS BIGINT),XXHASH64(unit_concept_id,ucum_code)\n        FROM um\n        ").withColumn('key_id', F.sha2(F.to_json(F.struct('map_kind', 'code_system', 'code', 'description', 'result_normalized', 'unit_norm')), 256))

def snapshot_mapping_baseline_v2() -> None:
    snapshot = _current_mapping_snapshot()
    snapshot.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(MP_BASELINE)

def classify_mapping_delta_v2() -> dict:
    current = _current_mapping_snapshot().alias('c')
    if not _table_exists(MP_BASELINE):
        delta = current.withColumn('base_concept_id', F.lit(None).cast('long')).withColumn('delta_type', F.when(F.col('concept_id').isNotNull(), F.lit('additive')).otherwise(F.lit('unchanged')))
    else:
        baseline = spark.table(MP_BASELINE).alias('b')
        delta = current.join(baseline, 'key_id', 'full').select(F.coalesce(F.col('c.map_kind'), F.col('b.map_kind')).alias('map_kind'), F.coalesce(F.col('c.code_system'), F.col('b.code_system')).alias('code_system'), F.coalesce(F.col('c.code'), F.col('b.code')).alias('code'), F.coalesce(F.col('c.description'), F.col('b.description')).alias('description'), F.coalesce(F.col('c.result_normalized'), F.col('b.result_normalized')).alias('result_normalized'), F.coalesce(F.col('c.unit_norm'), F.col('b.unit_norm')).alias('unit_norm'), F.col('c.concept_id').alias('concept_id'), F.col('b.concept_id').alias('base_concept_id'), F.col('c.payload_hash').alias('payload_hash'), F.col('b.payload_hash').alias('base_payload_hash')).withColumn('delta_type', F.when(F.col('base_concept_id').isNull() & F.col('concept_id').isNotNull(), F.lit('additive')).when(~F.col('base_concept_id').eqNullSafe(F.col('concept_id')), F.lit('correction')).when(~F.col('base_payload_hash').eqNullSafe(F.col('payload_hash')), F.lit('metadata')).otherwise(F.lit('unchanged')))
    changed = delta.filter(F.col('delta_type') != 'unchanged')
    counts = {row['delta_type']: int(row['count']) for row in changed.groupBy('delta_type').count().collect()}
    return {'delta_df': changed, 'n_additive_keys': counts.get('additive', 0), 'n_correction_keys': counts.get('correction', 0), 'n_metadata_keys': counts.get('metadata', 0)}

def _parents_affected_by_mapping_delta(delta_df: DataFrame) -> DataFrame:
    target = spark.table(MP_TARGET).withColumn('_result_normalized', F.lower(F.trim(F.regexp_replace(F.col('value_source_value'), '\\s+', ' ')))).withColumn('_unit_norm', F.lower(F.trim(F.col('unit_source_value'))))
    applicable = delta_df.filter(F.col('delta_type').isin('additive', 'metadata'))
    test_delta = applicable.filter(F.col('map_kind') == 'test').alias('d')
    result_delta = applicable.filter(F.col('map_kind') == 'result').alias('d')
    unit_delta = applicable.filter(F.col('map_kind') == 'unit').alias('d')
    test_parents = target.alias('t').join(test_delta, (F.col('t.code_system') == F.col('d.code_system')) & F.col('t.code').eqNullSafe(F.col('d.code')) & F.col('t.description').eqNullSafe(F.col('d.description')), 'inner').select(F.col('t.source_parent_key'))
    result_parents = target.alias('t').join(result_delta, (F.col('t.code_system') == F.col('d.code_system')) & F.col('t.code').eqNullSafe(F.col('d.code')) & F.col('t.description').eqNullSafe(F.col('d.description')) & F.col('t._result_normalized').eqNullSafe(F.col('d.result_normalized')), 'inner').select(F.col('t.source_parent_key'))
    unit_parents = target.alias('t').join(unit_delta, F.col('t._unit_norm').eqNullSafe(F.col('d.unit_norm')), 'inner').select(F.col('t.source_parent_key'))
    return test_parents.unionByName(result_parents).unionByName(unit_parents).dropDuplicates(['source_parent_key'])

def _direct_test_mapping_sql(delta_table: str) -> str:
    return f"\n    MERGE INTO {_qn(MP_TARGET)} t\n    USING (\n      WITH d AS (\n        SELECT code_system,code,description,delta_type\n        FROM {_qn(delta_table)}\n        WHERE map_kind='test' AND delta_type IN ('additive','metadata')\n      ),\n      tm AS (\n        SELECT *\n        FROM (\n          SELECT m.*,\n            ROW_NUMBER() OVER (\n              PARTITION BY code_system,code,description\n              ORDER BY CASE confidence_tier\n                         WHEN 'curated' THEN 1 WHEN 'auto_high' THEN 2\n                         WHEN 'auto_low' THEN 3 ELSE 9 END,\n                       mapping_version DESC NULLS LAST,\n                       mapped_at DESC NULLS LAST\n            ) AS rn\n          FROM {_qn(TEST_MAP)} m\n          WHERE confidence_tier IN {_tier_sql(TEST_TIERS)}\n            AND measurement_concept_id IS NOT NULL\n        ) WHERE rn=1\n      ),\n      tc AS (\n        SELECT c.concept_id,c.concept_code,c.vocabulary_id,c.standard_concept\n        FROM {_qn(CONCEPT)} c\n        LEFT SEMI JOIN (\n          SELECT DISTINCT measurement_concept_id AS concept_id FROM tm\n        ) i ON i.concept_id=c.concept_id\n      )\n      SELECT /*+ BROADCAST(d,tm,tc) */\n        mp.source_record_key,\n        CAST(tm.measurement_concept_id AS BIGINT) AS measurement_concept_id,\n        tm.concept_name AS measurement_concept_name,\n        tm.confidence_tier AS test_confidence_tier,\n        c.concept_code AS test_concept_code,\n        c.vocabulary_id AS test_vocabulary_id,\n        c.standard_concept AS test_standard_concept,\n        XXHASH64(\n          tm.measurement_concept_id,tm.concept_name,tm.confidence_tier,\n          mp.value_as_concept_id,mp.result_concept_name,mp.result_confidence_tier,\n          mp.result_is_suspected,mp.result_growth_grade,\n          mp.unit_concept_id,mp.ucum_code,\n          mp.value_as_number,mp.operator_concept_id\n        ) AS mapping_payload_hash\n      FROM {_qn(MP_TARGET)} mp\n      JOIN d\n        ON d.code_system=mp.code_system\n       AND d.code <=> mp.code\n       AND d.description <=> mp.description\n      JOIN tm\n        ON tm.code_system=mp.code_system\n       AND tm.code <=> mp.code\n       AND tm.description <=> mp.description\n      LEFT JOIN tc c\n        ON c.concept_id=tm.measurement_concept_id\n      WHERE d.delta_type='metadata' OR mp.measurement_concept_id IS NULL\n    ) s\n    ON t.source_record_key=s.source_record_key\n    WHEN MATCHED AND NOT (t.mapping_payload_hash <=> s.mapping_payload_hash)\n      THEN UPDATE SET\n        t.measurement_concept_id=s.measurement_concept_id,\n        t.test_omop_concept_id=s.measurement_concept_id,\n        t.measurement_concept_name=s.measurement_concept_name,\n        t.test_confidence_tier=s.test_confidence_tier,\n        t.test_snomed_code=CASE WHEN s.test_vocabulary_id='SNOMED'\n                                THEN s.test_concept_code END,\n        t.test_loinc_code=CASE WHEN s.test_vocabulary_id='LOINC'\n                               THEN s.test_concept_code END,\n        t.test_omop_standard_concept=s.test_standard_concept,\n        t.test_vocabulary_id=s.test_vocabulary_id,\n        t.mapping_payload_hash=s.mapping_payload_hash,\n        t.mapping_updated_at=CURRENT_TIMESTAMP(),\n        t.ADC_UPDT=CURRENT_TIMESTAMP()\n    "

def _direct_result_mapping_sql(delta_table: str) -> str:
    return f"\n    MERGE INTO {_qn(MP_TARGET)} t\n    USING (\n      WITH d AS (\n        SELECT code_system,code,description,result_normalized,delta_type\n        FROM {_qn(delta_table)}\n        WHERE map_kind='result' AND delta_type IN ('additive','metadata')\n      ),\n      rm AS (\n        SELECT *\n        FROM (\n          SELECT m.*,\n            ROW_NUMBER() OVER (\n              PARTITION BY code_system,code,description,result_normalized\n              ORDER BY CASE confidence_tier\n                         WHEN 'curated' THEN 1 WHEN 'auto_high' THEN 2\n                         WHEN 'auto_anchor' THEN 3 WHEN 'auto_value' THEN 4\n                         WHEN 'auto_genpos' THEN 5 WHEN 'auto_low' THEN 6\n                         ELSE 9 END,\n                       mapping_version DESC NULLS LAST,\n                       mapped_at DESC NULLS LAST\n            ) AS rn\n          FROM {_qn(RESULT_MAP)} m\n          WHERE confidence_tier IN {_tier_sql(RESULT_TIERS)}\n            AND value_as_concept_id IS NOT NULL\n        ) WHERE rn=1\n      ),\n      rc AS (\n        SELECT c.concept_id,c.concept_code,c.vocabulary_id,c.standard_concept\n        FROM {_qn(CONCEPT)} c\n        LEFT SEMI JOIN (\n          SELECT DISTINCT value_as_concept_id AS concept_id FROM rm\n        ) i ON i.concept_id=c.concept_id\n      )\n      SELECT /*+ BROADCAST(d,rm,rc) */\n        mp.source_record_key,\n        CAST(rm.value_as_concept_id AS BIGINT) AS value_as_concept_id,\n        rm.concept_name AS result_concept_name,\n        rm.confidence_tier AS result_confidence_tier,\n        rm.is_suspected AS result_is_suspected,\n        rm.growth_grade AS result_growth_grade,\n        c.concept_code AS result_concept_code,\n        c.vocabulary_id AS result_vocabulary_id,\n        c.standard_concept AS result_standard_concept,\n        CASE\n          WHEN mp.result_parse_status='blank' THEN 'missing'\n          WHEN mp.result_parse_status='numeric' THEN 'numeric'\n          WHEN mp.result_parse_status='datetime' THEN 'datetime'\n          ELSE 'mapped'\n        END AS result_status,\n        XXHASH64(\n          mp.measurement_concept_id,mp.measurement_concept_name,\n          mp.test_confidence_tier,\n          rm.value_as_concept_id,rm.concept_name,rm.confidence_tier,\n          rm.is_suspected,rm.growth_grade,\n          mp.unit_concept_id,mp.ucum_code,\n          mp.value_as_number,mp.operator_concept_id\n        ) AS mapping_payload_hash\n      FROM {_qn(MP_TARGET)} mp\n      JOIN d\n        ON d.code_system=mp.code_system\n       AND d.code <=> mp.code\n       AND d.description <=> mp.description\n       AND d.result_normalized <=>\n           LOWER(TRIM(REGEXP_REPLACE(mp.value_source_value,'\\\\s+',' ')))\n      JOIN rm\n        ON rm.code_system=mp.code_system\n       AND rm.code <=> mp.code\n       AND rm.description <=> mp.description\n       AND rm.result_normalized <=> d.result_normalized\n      LEFT JOIN rc c\n        ON c.concept_id=rm.value_as_concept_id\n      WHERE mp.result_parse_status<>'numeric'\n        AND (d.delta_type='metadata' OR mp.value_as_concept_id IS NULL)\n    ) s\n    ON t.source_record_key=s.source_record_key\n    WHEN MATCHED AND NOT (t.mapping_payload_hash <=> s.mapping_payload_hash)\n      THEN UPDATE SET\n        t.value_as_concept_id=s.value_as_concept_id,\n        t.result_omop_concept_id=s.value_as_concept_id,\n        t.result_concept_name=s.result_concept_name,\n        t.result_confidence_tier=s.result_confidence_tier,\n        t.result_is_suspected=s.result_is_suspected,\n        t.result_growth_grade=s.result_growth_grade,\n        t.result_snomed_code=CASE WHEN s.result_vocabulary_id='SNOMED'\n                                  THEN s.result_concept_code END,\n        t.result_loinc_code=CASE WHEN s.result_vocabulary_id='LOINC'\n                                 THEN s.result_concept_code END,\n        t.result_omop_standard_concept=s.result_standard_concept,\n        t.result_vocabulary_id=s.result_vocabulary_id,\n        t.result_status=s.result_status,\n        t.result_mapping_match_type='exact_context',\n        t.mapping_payload_hash=s.mapping_payload_hash,\n        t.mapping_updated_at=CURRENT_TIMESTAMP(),\n        t.ADC_UPDT=CURRENT_TIMESTAMP()\n    "

def _direct_unit_mapping_sql(delta_table: str) -> str:
    return f"\n    MERGE INTO {_qn(MP_TARGET)} t\n    USING (\n      WITH d AS (\n        SELECT unit_norm,delta_type\n        FROM {_qn(delta_table)}\n        WHERE map_kind='unit' AND delta_type IN ('additive','metadata')\n      ),\n      unit_exact AS (\n        SELECT unit_source_value,unit_concept_id,ucum_code\n        FROM (\n          SELECT m.*,\n            ROW_NUMBER() OVER (\n              PARTITION BY unit_source_value\n              ORDER BY unit_concept_id DESC NULLS LAST,\n                       ucum_code ASC NULLS LAST\n            ) AS rn\n          FROM {_qn(UNIT_MAP)} m\n          WHERE unit_concept_id IS NOT NULL\n        ) WHERE rn=1\n      ),\n      unit_normalized AS (\n        SELECT LOWER(TRIM(unit_source_value)) AS unit_norm,\n               MIN(unit_concept_id) AS unit_concept_id,\n               MIN(ucum_code) AS ucum_code\n        FROM unit_exact\n        WHERE unit_source_value IS NOT NULL AND TRIM(unit_source_value)<>''\n        GROUP BY LOWER(TRIM(unit_source_value))\n        HAVING COUNT(DISTINCT unit_concept_id)=1\n      )\n      SELECT /*+ BROADCAST(d,ue,un) */\n        mp.source_record_key,\n        COALESCE(ue.unit_concept_id,un.unit_concept_id) AS unit_concept_id,\n        COALESCE(ue.ucum_code,un.ucum_code) AS ucum_code,\n        CASE WHEN ue.unit_concept_id IS NOT NULL THEN 'exact'\n             ELSE 'normalized' END AS unit_mapping_match_type,\n        XXHASH64(\n          mp.measurement_concept_id,mp.measurement_concept_name,\n          mp.test_confidence_tier,\n          mp.value_as_concept_id,mp.result_concept_name,\n          mp.result_confidence_tier,\n          mp.result_is_suspected,mp.result_growth_grade,\n          COALESCE(ue.unit_concept_id,un.unit_concept_id),\n          COALESCE(ue.ucum_code,un.ucum_code),\n          mp.value_as_number,mp.operator_concept_id\n        ) AS mapping_payload_hash\n      FROM {_qn(MP_TARGET)} mp\n      JOIN d ON d.unit_norm=LOWER(TRIM(mp.unit_source_value))\n      LEFT JOIN unit_exact ue\n        ON ue.unit_source_value <=> mp.unit_source_value\n      LEFT JOIN unit_normalized un\n        ON un.unit_norm=LOWER(TRIM(mp.unit_source_value))\n       AND ue.unit_concept_id IS NULL\n      WHERE COALESCE(ue.unit_concept_id,un.unit_concept_id) IS NOT NULL\n        AND (d.delta_type='metadata' OR mp.unit_concept_id IS NULL)\n    ) s\n    ON t.source_record_key=s.source_record_key\n    WHEN MATCHED AND NOT (t.mapping_payload_hash <=> s.mapping_payload_hash)\n      THEN UPDATE SET\n        t.unit_concept_id=s.unit_concept_id,\n        t.ucum_code=s.ucum_code,\n        t.unit_mapping_match_type=s.unit_mapping_match_type,\n        t.mapping_payload_hash=s.mapping_payload_hash,\n        t.mapping_updated_at=CURRENT_TIMESTAMP(),\n        t.ADC_UPDT=CURRENT_TIMESTAMP()\n    "

def _merge_updated_rows(result) -> int:
    if result is None:
        return 0
    values = result.asDict()
    return int(values.get('num_updated_rows') or 0)

def _run_direct_mapping_pass(pass_name: str, sql_text: str, delta_table: str, full_manifest: dict | None) -> int:
    stage_name = f'DIRECT_MAP_{pass_name.upper()}_V1'
    if full_manifest is not None:
        completed = _completed_full_build_progress(full_manifest['build_id'], stage_name, -1, delta_table)
        if completed is not None:
            updated = int(completed.get('row_count') or 0)
            print(f'[map_pathology_v3] REUSE {stage_name}; updated_rows={updated:,}')
            return updated
    attempts = int(FULL_BUILD_STAGE_RETRIES) + 1
    for attempt in _mp_builtins.range(1, attempts + 1):
        if full_manifest is not None:
            _write_full_build_progress(full_manifest['build_id'], stage_name, -1, 'RUNNING', table_name=delta_table)
        try:
            updated = _merge_updated_rows(spark.sql(sql_text).first())
            if full_manifest is not None:
                _write_full_build_progress(full_manifest['build_id'], stage_name, -1, 'COMPLETE', table_name=delta_table, row_count=updated, parent_count=0)
            print(f'[map_pathology_v3] COMPLETE {stage_name}; updated_rows={updated:,}')
            return updated
        except Exception as exc:
            if full_manifest is not None:
                _write_full_build_progress(full_manifest['build_id'], stage_name, -1, 'FAILED', table_name=delta_table, error=str(exc))
            if not _retryable_full_build_failure(exc) or attempt >= attempts:
                raise
            print(f'[map_pathology_v3] RETRY {stage_name}; attempt={attempt + 1}/{attempts}; reason={str(exc).splitlines()[0][:500]}')
    raise RuntimeError(f'Direct mapping pass did not complete: {pass_name}')

def _apply_mapping_delta_direct(delta_df: DataFrame, run_id: str, scratch_tables: list[str], full_manifest: dict | None) -> dict[str, int]:
    delta_table = _full_stage_table(run_id, 'mapping_delta_v1') if full_manifest is not None else _stage_table(run_id, 'mapping_delta_v1')
    changed = delta_df.filter(F.col('delta_type').isin('additive', 'metadata'))
    changed.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(delta_table)
    if full_manifest is None:
        scratch_tables.append(delta_table)
    counts = {row['map_kind']: int(row['count']) for row in spark.table(delta_table).groupBy('map_kind').count().collect()}
    if full_manifest is not None:
        _write_full_build_progress(full_manifest['build_id'], 'DIRECT_MAP_DELTA_V1', -1, 'COMPLETE', table_name=delta_table, row_count=_mp_builtins.sum(counts.values()), parent_count=0)
    updated = 0
    if counts.get('test', 0):
        updated += _run_direct_mapping_pass('test', _direct_test_mapping_sql(delta_table), delta_table, full_manifest)
    if counts.get('result', 0):
        updated += _run_direct_mapping_pass('result', _direct_result_mapping_sql(delta_table), delta_table, full_manifest)
    if counts.get('unit', 0):
        updated += _run_direct_mapping_pass('unit', _direct_unit_mapping_sql(delta_table), delta_table, full_manifest)
    return {'changed_rows': int(updated), 'stale_rows_deleted': 0}

def _bulk_remap_required(error: Exception | str | None) -> bool:
    if error is None:
        return False
    message = str(error)
    return 'REMAP_MAX_SERVERLESS' in message and ('_remap_test' in message or '_build_openset_substrate' in message)

def _run_embedding_and_mapping_reconciliation(discovery_stage: str, run_id: str, scratch_tables: list[str], full_manifest: dict | None=None, skip_discovery_and_embed: bool=False) -> dict[str, int]:
    metrics = {'additive_map_keys': 0, 'correction_map_keys': 0, 'metadata_map_keys': 0, 'mapping_changed_rows': 0, 'mapping_stale_rows_deleted': 0, 'bulk_remap_deferred': 0}
    if not ENABLE_EMBED_LOOP:
        return metrics
    required_functions = ('embed_pending_capped', 'remap_keys')
    missing_functions = [name for name in required_functions if name not in globals()]
    if missing_functions:
        print(f'[map_pathology_v3] embed loop skipped; import pathology_embed_increment first. Missing: {missing_functions}')
        return metrics
    if skip_discovery_and_embed:
        print('[map_pathology_v3] RESUME embedding reconciliation after the completed discovery/embed phase')
    else:
        discovered = discover_new_keys_from_stage(discovery_stage)
        print(f'[map_pathology_v3] embedding discovery: {discovered}')
        embed_pending_capped()
        if full_manifest is not None:
            _update_full_build_phase(full_manifest, 'EMBEDDING_READY')
    try:
        remap_keys()
    except RuntimeError as exc:
        if not _bulk_remap_required(exc):
            raise
        metrics['bulk_remap_deferred'] = 1
        print(f'[map_pathology_v3] BULK_REMAP_DEFERRED: the vector-ready backlog exceeds the safe inline/serverless limit. Existing and completed map changes will be reconciled now; queued result keys remain vector_ready for the separate bulk mapping workflow. Reason: {str(exc).splitlines()[0][:1000]}')
    delta = classify_mapping_delta_v2()
    delta_df = delta['delta_df']
    metrics['additive_map_keys'] = delta['n_additive_keys']
    metrics['correction_map_keys'] = delta['n_correction_keys']
    metrics['metadata_map_keys'] = delta['n_metadata_keys']
    if delta['n_correction_keys'] > 0:
        _set_rebuild_flag(True)
        print(f"[map_pathology_v3] {delta['n_correction_keys']} mapping corrections detected; a true full rebuild is flagged for the next run.")
        None
        return metrics
    if delta['n_additive_keys'] or delta['n_metadata_keys']:
        print(f"[map_pathology_v3] applying mapping-only changes directly; no source re-scan or parent reconstruction. {delta['n_additive_keys']:,} additive and {delta['n_metadata_keys']:,} metadata keys")
        merge_metrics = _apply_mapping_delta_direct(delta_df, run_id, scratch_tables, full_manifest)
        metrics['mapping_changed_rows'] = merge_metrics['changed_rows']
        metrics['mapping_stale_rows_deleted'] = merge_metrics['stale_rows_deleted']
    snapshot_mapping_baseline_v2()
    None
    if metrics['bulk_remap_deferred']:
        print('[map_pathology_v3] native crosswalk refresh deferred with the bulk result-remap backlog')
    else:
        _refresh_native_crosswalks()
    return metrics

def _refresh_map_cutoffs(cutoffs: dict[str, dict]) -> None:
    """Advance map-state cutoffs past mappings created by the inline embed loop."""
    for source_name in ('pathology_test_concept_map', 'pathology_result_concept_map', 'pathology_unit_map'):
        table_name, ts_col = SOURCE_TABLES[source_name]
        end_version = _latest_delta_version(table_name)
        cutoffs[source_name] = {'source_name': source_name, 'table_name': table_name, 'end_version': end_version, 'end_adc_updt': _max_timestamp_at_version(table_name, ts_col, end_version), 'timestamp_column': ts_col}

def create_map_pathology(force_full: bool=False, run_embed_loop: bool=True) -> dict:
    """
    Production entry point and drop-in replacement for Map Pipeline.

    A full rebuild is forced for first deployment, missing v2 state/schema, or
    an outstanding mapping correction. Incremental runs consume per-source CDF
    or timestamp changes and reconcile complete source-parent scopes.
    """
    global ENABLE_EMBED_LOOP
    previous_embed_setting = ENABLE_EMBED_LOOP
    ENABLE_EMBED_LOOP = bool(run_embed_loop)
    _ensure_control_tables()
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    scratch_tables: list[str] = []
    full_manifest: dict | None = None
    mode = 'UNKNOWN'
    source_parent_count = 0
    staged_row_count = 0
    inserted_or_updated_rows = 0
    stale_rows_deleted = 0
    additive_map_keys = 0
    correction_map_keys = 0
    bulk_remap_deferred = 0
    try:
        state = _read_state()
        state_complete = _mp_builtins.all((name in state for name in SOURCE_TABLES))
        full = bool(force_full) or not _target_is_v2() or (not _table_exists(MP_BASELINE)) or (not state_complete) or _read_rebuild_flag()
        mode = 'FULL_REBUILD' if full else 'INCREMENTAL'
        if full:
            _ensure_full_build_control_tables()
            full_manifest = _load_or_start_full_build()
            run_id = full_manifest['build_id']
            started_at = full_manifest['started_at']
            cutoffs = full_manifest['cutoffs']
        else:
            cutoffs = _capture_cutoffs()
        print(f'[map_pathology_v3] {mode}; run_id={run_id}')
        if full_manifest is not None and _phase_reached(full_manifest, 'PUBLISHED'):
            print('[map_pathology_v3] post-publication resume: deferring native crosswalk refresh')
        else:
            _refresh_native_crosswalks()
        if full:
            full_metrics = _run_restartable_full_build(full_manifest)
            source_stage = full_metrics['source_stage']
            source_parent_count = int(full_metrics['parent_count'])
            staged_row_count = int(full_metrics['row_count'])
            inserted_or_updated_rows = staged_row_count
            if not _phase_reached(full_manifest, 'BASELINE_READY'):
                snapshot_mapping_baseline_v2()
                _set_rebuild_flag(False)
                _update_full_build_phase(full_manifest, 'BASELINE_READY')
        else:
            source_stage = _stage_table(run_id, 'source')
            scratch_tables.append(source_stage)
            try:
                touched_parents, scope_modes = _prepare_incremental_scope(state, cutoffs)
                source_parent_count = int(touched_parents.count())
            except Exception as scope_exc:
                if not _incremental_snapshot_read_unavailable(scope_exc):
                    raise
                print(
                    '[map_pathology_v3] RECOVERY: deferred CDF read failed; '
                    'rebuilding incremental scope from current snapshots and ADC watermarks. '
                    f'Reason: {str(scope_exc).splitlines()[0][:1000]}'
                )
                touched_parents, scope_modes = _prepare_incremental_scope(
                    state,
                    cutoffs,
                    force_snapshot_fallback=True,
                )
                source_parent_count = int(touched_parents.count())
            print(f'[map_pathology_v3] touched source parents: {source_parent_count:,}; modes={scope_modes}')
            if source_parent_count:
                staged_row_count = _materialize_stage(_mp_build_select(full=False), source_stage)
                validation = _validate_stage(source_stage)
                staged_row_count = validation['row_count']
                merge_metrics = _merge_and_reconcile(source_stage, touched_parents)
                inserted_or_updated_rows += merge_metrics['changed_rows']
                stale_rows_deleted += merge_metrics['stale_rows_deleted']
            else:
                spark.table(MP_TARGET).limit(0).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(source_stage)
            None
        skip_discovery_and_embed = False
        if full_manifest is not None:
            resumed_from_bulk_gate = _bulk_remap_required(full_manifest.get('last_error'))
            if resumed_from_bulk_gate and (not _phase_reached(full_manifest, 'EMBEDDING_READY')):
                _update_full_build_phase(full_manifest, 'EMBEDDING_READY')
            skip_discovery_and_embed = _phase_reached(full_manifest, 'EMBEDDING_READY')
        mapping_metrics = _run_embedding_and_mapping_reconciliation(source_stage, run_id, scratch_tables, full_manifest=full_manifest, skip_discovery_and_embed=skip_discovery_and_embed)
        additive_map_keys = mapping_metrics['additive_map_keys']
        correction_map_keys = mapping_metrics['correction_map_keys']
        bulk_remap_deferred = mapping_metrics['bulk_remap_deferred']
        inserted_or_updated_rows += mapping_metrics['mapping_changed_rows']
        stale_rows_deleted += mapping_metrics['mapping_stale_rows_deleted']
        _refresh_map_cutoffs(cutoffs)
        _advance_state(cutoffs)
        if full_manifest is not None:
            _update_full_build_phase(full_manifest, 'SUCCESS')
            _cleanup_full_build_stages(full_manifest['build_id'])
            _drop_table_if_exists(MP_FULL_BUILD_PROGRESS)
            _drop_table_if_exists(MP_FULL_BUILD_MANIFEST)
        completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        _write_run_log(run_id=run_id, started_at=started_at, completed_at=completed_at, mode=mode, status='SUCCESS', pipeline_version=MP_VERSION, source_parent_count=source_parent_count, staged_row_count=staged_row_count, inserted_or_updated_rows=inserted_or_updated_rows, stale_rows_deleted=stale_rows_deleted, additive_map_keys=additive_map_keys, correction_map_keys=correction_map_keys, message='Completed; bulk embedding remap deferred' if bulk_remap_deferred else 'Completed')
        result = {'run_id': run_id, 'mode': mode, 'source_parent_count': source_parent_count, 'staged_row_count': staged_row_count, 'inserted_or_updated_rows': inserted_or_updated_rows, 'stale_rows_deleted': stale_rows_deleted, 'additive_map_keys': additive_map_keys, 'correction_map_keys': correction_map_keys, 'bulk_remap_deferred': bulk_remap_deferred, 'rebuild_flagged': _read_rebuild_flag()}
        print(f'[map_pathology_v3] complete: {result}')
        return result
    except Exception as exc:
        completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        message = str(exc)
        if full_manifest is not None:
            try:
                _update_full_build_phase(full_manifest, full_manifest['phase'], error=message)
            except Exception as manifest_exc:
                print(f'[map_pathology_v3] manifest error recording warning: {str(manifest_exc).splitlines()[0][:500]}')
        _write_run_log(run_id=run_id, started_at=started_at, completed_at=completed_at, mode=mode, status='FAILED', pipeline_version=MP_VERSION, source_parent_count=source_parent_count, staged_row_count=staged_row_count, inserted_or_updated_rows=inserted_or_updated_rows, stale_rows_deleted=stale_rows_deleted, additive_map_keys=additive_map_keys, correction_map_keys=correction_map_keys, message=message[:4000])
        raise
    finally:
        for table_name in scratch_tables:
            try:
                _drop_table_if_exists(table_name)
            except Exception as cleanup_exc:
                print(f'[map_pathology_v3] scratch cleanup warning for {table_name}: {str(cleanup_exc)[:500]}')
        ENABLE_EMBED_LOOP = previous_embed_setting

def validate_map_pathology_v2() -> dict:
    """Read-only deployment checks over retained single-layer columns."""
    if not _target_is_v2():
        raise RuntimeError('Target does not have the retained v2 schema.')
    target = spark.table(MP_TARGET)
    summary = {
        row['source_table']: {
            'rows': int(row['rows']),
            'parents': int(row['parents']),
            'missing_measurement_datetime': int(
                row['missing_measurement_datetime']
            ),
            'missing_results': int(row['missing_results']),
        }
        for row in target.groupBy('source_table').agg(
            F.count('*').alias('rows'),
            F.countDistinct('source_parent_key').alias('parents'),
            F.sum(
                F.when(F.col('measurement_datetime').isNull(), 1).otherwise(0)
            ).alias('missing_measurement_datetime'),
            F.sum(
                F.when(F.col('result_status') == 'missing', 1).otherwise(0)
            ).alias('missing_results'),
        ).collect()
    }
    duplicate_keys = (
        target.groupBy('source_record_key').count()
        .filter(F.col('count') > 1).limit(1).count()
    )
    raw_bad_parent = (
        target.filter(F.col('source_table') == 'raw')
        .withColumn(
            '_expected_parent_key',
            F.concat_ws(
                '|', F.lit('raw'),
                F.coalesce(F.col('LIMSNo').cast('string'), F.lit('∅')),
                F.coalesce(F.col('lab_no'), F.lit('∅')),
            ),
        )
        .filter(
            ~F.col('source_parent_key').eqNullSafe(
                F.col('_expected_parent_key')
            )
        )
        .limit(1)
        .count()
    )
    result = {
        'pipeline_version': MP_VERSION,
        'summary': summary,
        'duplicate_source_record_keys': int(duplicate_keys),
        'raw_parent_key_validation_failures': int(raw_bad_parent),
        'rebuild_flagged': _read_rebuild_flag(),
    }
    print(result)
    return result
# COMMAND ----------

# MAGIC %md
# MAGIC ## Folded: `pathology_embed_increment` (definition only)
# MAGIC
# MAGIC map_50 detects the incremental embed loop with a bare `globals()` presence check for
# MAGIC `embed_pending_capped` and `remap_keys`, so folding the module in here needs no change at
# MAGIC either call site. Three deliberate departures from the standalone module:
# MAGIC
# MAGIC 1. Every module-level constant that collided with this notebook carries an `EMB_` prefix.
# MAGIC    In particular `EMB_MP_TARGET` is the legacy `4_prod.bronze.map_pathology`, which is NOT
# MAGIC    this notebook's own target (`4_prod.bronze.map_pathology`); it is read only by
# MAGIC    folded functions that are never called from here.
# MAGIC 2. Every import-time side effect is deferred into `_emb_ensure_infrastructure()`, called
# MAGIC    lazily from `embed_pending_capped` and `remap_keys`. Loading this notebook writes nothing.
# MAGIC 3. `run_increment` is folded in as a definition only and is never called.

# COMMAND ----------

# ==== BEGIN FOLD: pathology_embed_increment (definition only; no import-time I/O) ====

# COMMAND ----------

# MAGIC %md
# MAGIC # Pathology Incremental Embed Loop (pathology_embed_increment)
# MAGIC
# MAGIC Serverless weekly loop: discover new free-text CONTEXT KEYS from mill/raw → embed (capped) →
# MAGIC remap into the concept maps → additively back-fill map_pathology. The WHOLE loop runs INLINE in the
# MAGIC Map Pipeline family job via the `CC/mappipeline_map_pathology_section` section, which `%run`s this
# MAGIC module and calls the stage fns directly (see the ## INTEGRATION cell at the end). Authored + tested
# MAGIC against 8_dev; the user promotes to prod. (SUPERSEDED: the earlier paste-into-nomenclature plan.)
# MAGIC
# MAGIC **Changelog**
# MAGIC - v0.1 — config + run-log DDL
# MAGIC - v0.5 — stage 3 remap_keys (serverless Spark cross-join dot-product; NO driver numpy; test + context-scoped result)
# MAGIC - v0.7 — stage 5 apply_to_map_pathology (additive backfill via map re-join, no source re-scan; correction abort; 6-key MERGE)
# MAGIC - v1.0 — Task 11: human-gate AOAI smoke (if False) + paste-ready packaging + handoff.
# MAGIC - v2.3 — lazy openai import (_get_embedding_client) so %run works on any compute; import-time probe removed.

# COMMAND ----------

# ── pathology_embed_increment — config (dev) ────────────────────────────────────
# INTEGRATION: this module is `%run` from the Map Pipeline family job; the
# CC/mappipeline_map_pathology_section section calls the stage fns INLINE (see the
# ## INTEGRATION cell at the end). Authored + tested against 8_dev; user promotes to prod.
ENVIRONMENT       = "prod"
EMB_MAP_SCHEMA        = "3_lookup.omop"                         # prod: 3_lookup.omop (loop tables promoted there; see pathology_embed_promote_to_prod)
EMBEDDINGS_TABLE  = "3_lookup.embeddings.terms"
QUEUE             = f"{EMB_MAP_SCHEMA}.pathology_embed_queue"
EMB_TEST_MAP          = f"{EMB_MAP_SCHEMA}.pathology_test_concept_map"
EMB_RESULT_MAP        = f"{EMB_MAP_SCHEMA}.pathology_result_concept_map"
TEST_INDEX        = f"{EMB_MAP_SCHEMA}.concept_index_test"
RESULT_INDEX      = f"{EMB_MAP_SCHEMA}.concept_index_result"
ANSWER_LISTS      = f"{EMB_MAP_SCHEMA}.concept_answer_lists"
TEST_OVERRIDES    = f"{EMB_MAP_SCHEMA}.pathology_test_map_overrides"
RESULT_OVERRIDES  = f"{EMB_MAP_SCHEMA}.pathology_result_map_overrides"
EMB_EXCL_TBL          = f"{EMB_MAP_SCHEMA}.pathology_result_value_exclusions"   # 32 rows, has `pattern` (Task 8 result_status EXCL_REGEX)
EMB_UNIT_MAP          = f"{EMB_MAP_SCHEMA}.pathology_unit_map"                  # 49 rows, has `unit_source_value` (Task 8 unit backfill)
FREQ_TABLE        = f"{EMB_MAP_SCHEMA}.pathology_term_frequency"  # DEPRECATED (rde-seeded); the loop uses FREQ_TABLE_MILLRAW (Task 2)
EMB_CONCEPT           = "3_lookup.omop.concept"
RUN_LOG           = f"{EMB_MAP_SCHEMA}.pathology_embed_run_log"
# map_pathology coupling — the rebuild gate now lives in the Map-Pipeline-owned flag table
# pathology_map_rebuild_flag (single-row), which REPLACES the retired map_pathology_state table.
EMB_MP_TARGET         = "4_prod.bronze.map_pathology"         # prod: 4_prod.bronze.map_pathology
MP_REBUILD_FLAG   = f"{EMB_MAP_SCHEMA}.pathology_map_rebuild_flag"   # dev 8_dev.omop.* ; prod 3_lookup.omop.* — single-row (id INT, rebuild_flagged BOOLEAN); the Map Pipeline section owns/creates it

# thresholds (verbatim from the map builds)
SIM_HIGH          = 0.85   # test side
SIM_LOW           = 0.70
SIM_HIGH_LOINC    = 0.80   # result side
SIM_HIGH_SNOMED   = 0.82

# Tiers the incremental RESULT backfill admits into bronze. The open-set arms (auto_anchor/auto_value/
# auto_genpos) were added 2026-06-30 so the loop consumes the SAME result tiers the FULL_REBUILD does
# (promotion runbook §C). Used by apply_to_map_pathology Pass A (rj result join) + Pass B. The TEST-map
# joins stay ('curated','auto_high','auto_low') — open-set arms produce RESULT concepts only.
CONSUMED_TIERS = "('curated','auto_high','auto_low','auto_anchor','auto_value','auto_genpos')"

# cost cap (Task 4)
MAX_EMBED_TERMS   = 20_000     # per-run hard cap on distinct texts sent to AOAI
EST_USD_PER_TERM  = 0.0000013  # text-embedding-3-large ~ $0.13/1M tokens; ~10 tok/term -> rough upper bound

# P0 grain + lossless key (2026-06-30): the RESULT arm now embeds/keys on the BARE result text (the open-set
# scoring grain that Stage 3b scores against), and result_normalized is persisted LOSSLESSLY (no downstream
# substring_index recovery, which truncated values containing ' | '). Two queue columns:
#   result_normalized -- lossless result identity (NULL for kind='test')
#   embed_text        -- the text actually embedded + scored: result_normalized for results, term for tests
# Idempotent ALTER (the prod migration notebook / the fixture create the queue; this just back-adds the cols
# on an existing queue). A missing table here is fine (caught + ignored) -- the creator makes it with the cols.
# [folded] queue column top-up (ALTER TABLE) now runs inside _emb_ensure_infrastructure()

def utcnow():
    # NOTE: import locally (NOT `import datetime` at module scope). When this module is %run from a
    # host notebook that did `from datetime import datetime` (the class), a module-level `import datetime`
    # would REBIND the host's `datetime` global to the module, breaking every downstream datetime.now().
    from datetime import datetime as _dt, timezone as _tz
    return _dt.now(_tz.utc)

# COMMAND ----------

# [folded] run-log DDL (CREATE TABLE) now runs inside _emb_ensure_infrastructure()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 — pathology_term_frequency repointed to mill/raw (FREQ_TABLE_MILLRAW)
# MAGIC Builds a term/event_count table for embed-priority ordering, sourced from mill/raw (NOT rde).
# MAGIC term shapes are byte-identical to pathology_embed_queue.term so the Task-4 LOWER(term) join matches.

# COMMAND ----------

FREQ_TABLE_MILLRAW = f"{EMB_MAP_SCHEMA}.pathology_term_frequency_millraw"

def build_term_frequency_millraw():
    # HEAVY BUILD (~5 min; scans ~4.4B-row mill_clinical_event). Refresh OUT-OF-BAND on a cluster, not
    # every loop run — the loop tolerates a stale/partial freq table (priority-only, Task-4 LEFT JOIN, ec=0 sorts last).
    # term shapes IDENTICAL to the queue/rde builder: result-kind = '<desc> | <lower/trim/collapse result>',
    # test-kind = '<code> | <desc>'. Separator ' | '. Non-numeric gate via RLIKE (== map_pathology's
    # ResultNumeric definition). Used ONLY for embed PRIORITY ordering in Task 4 (LEFT JOIN; ec=0 sorts last).
    spark.sql(f"""
    CREATE OR REPLACE TABLE {FREQ_TABLE_MILLRAW} USING DELTA AS
    WITH
    m1 AS (   -- master dedup: one row per (WkgCode,TFCCode), current by LastUpdateDT
      SELECT WkgCode, TFCCode,
             COALESCE(TFCDesc_Full, TFCDesc_Rep, TFCDesc_WP) AS desc_full
      FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.WkgCode, m.TFCCode
                 ORDER BY m.LastUpdateDT DESC NULLS LAST) AS rn
        FROM 4_prod.raw.path_master_resultable m
      ) WHERE rn = 1
    ),
    raw_results AS (
      SELECT concat_ws(' | ', lower(m1.desc_full), trim(regexp_replace(lower(rl.TFCValue), '\\\\s+', ' '))) AS term,
             COUNT(*) AS n
      FROM 4_prod.raw.path_patient_resultlevel rl
      JOIN m1 ON m1.WkgCode = rl.WkgCode AND m1.TFCCode = rl.TFCCode
      WHERE rl.TFCCode IS NOT NULL AND m1.desc_full IS NOT NULL
        AND rl.TFCValue IS NOT NULL
        AND NOT (rl.TFCValue RLIKE '^\\\\s*[<>]?=?\\\\s*-?[0-9.]+\\\\s*$')
        AND trim(regexp_replace(lower(rl.TFCValue), '\\\\s+', ' ')) <> ''
      GROUP BY 1
    ),
    raw_tests AS (
      SELECT concat_ws(' | ', lower(rl.TFCCode), lower(m1.desc_full)) AS term, COUNT(*) AS n
      FROM 4_prod.raw.path_patient_resultlevel rl
      JOIN m1 ON m1.WkgCode = rl.WkgCode AND m1.TFCCode = rl.TFCCode
      WHERE rl.TFCCode IS NOT NULL AND m1.desc_full IS NOT NULL
      GROUP BY 1
    ),
    linked_base AS (   -- pathology isolation: EVENT_CLASS_CD IN (233,236) AND catalog type 2513
      SELECT o.ORDER_MNEMONIC AS code, cv.DESCRIPTION AS descr, ce.RESULT_VAL
      FROM 4_prod.raw.mill_clinical_event ce
      LEFT JOIN 3_lookup.mill.mill_order_catalog oc ON oc.CATALOG_CD = ce.CATALOG_CD
      LEFT JOIN 4_prod.raw.mill_orders o ON o.ORDER_ID = ce.ORDER_ID
      LEFT JOIN 3_lookup.mill.mill_code_value cv ON cv.CODE_VALUE = ce.EVENT_CD
      WHERE ce.EVENT_CLASS_CD IN (233, 236) AND oc.CATALOG_TYPE_CD = 2513
    ),
    linked_results AS (
      SELECT concat_ws(' | ', lower(descr), trim(regexp_replace(lower(RESULT_VAL), '\\\\s+', ' '))) AS term,
             COUNT(*) AS n
      FROM linked_base
      WHERE descr IS NOT NULL AND RESULT_VAL IS NOT NULL
        AND NOT (RESULT_VAL RLIKE '^\\\\s*[<>]?=?\\\\s*-?[0-9.]+\\\\s*$')
        AND trim(regexp_replace(lower(RESULT_VAL), '\\\\s+', ' ')) <> ''
      GROUP BY 1
    ),
    linked_tests AS (
      SELECT concat_ws(' | ', lower(code), lower(descr)) AS term, COUNT(*) AS n
      FROM linked_base WHERE code IS NOT NULL AND descr IS NOT NULL
      GROUP BY 1
    )
    SELECT term, SUM(n) AS event_count
    FROM (SELECT * FROM raw_results UNION ALL SELECT * FROM raw_tests
          UNION ALL SELECT * FROM linked_results UNION ALL SELECT * FROM linked_tests)
    GROUP BY term
    """)
    n = spark.sql(f"SELECT COUNT(*) c FROM {FREQ_TABLE_MILLRAW}").first()["c"]
    print(f"Built {FREQ_TABLE_MILLRAW}: {n:,} terms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 — Stage 1: discover_new_keys
# MAGIC Finds missing concept-map CONTEXT KEYS from mill/raw (key-first), classifies each as
# MAGIC vector_ready (text already embedded) or pending (needs AOAI), and enqueues them.
# MAGIC Scoped kind IN ('test','result'). Re-attempts previously-unmapped keys only when vector_ready.

# COMMAND ----------

def discover_new_keys(since_watermark, dry_run=False):
    """Stage 1. Find concept-map context keys present in source (ADC_UPDT > since_watermark) that are
    NOT already mapped to a non-NULL concept AND not already embedded+queued; classify each by whether
    its embedding text already exists. Re-attempts previously-unmapped keys ONLY when their text is
    already embedded (zero-cost remap retry). INSERTs new keys into QUEUE (status vector_ready|pending).
    Idempotent: re-running inserts nothing new. Scoped to kind IN ('test','result')."""
    wm = f"TIMESTAMP'{since_watermark}'"
    discover_sql = f"""
    WITH
    emb AS (SELECT DISTINCT LOWER(term) AS term FROM {EMBEDDINGS_TABLE} WHERE embedding_vector IS NOT NULL),
    -- linked (CERNER) source, pathology-isolated; description = cv.DESCRIPTION (NOT mnemonic)
    linked_base AS (
      SELECT 'CERNER_TESTCODE' AS code_system, o.ORDER_MNEMONIC AS code, cv.DESCRIPTION AS description,
             ce.RESULT_VAL AS result_txt, ce.ADC_UPDT AS adc
      FROM 4_prod.raw.mill_clinical_event ce
      LEFT JOIN 3_lookup.mill.mill_order_catalog oc ON oc.CATALOG_CD = ce.CATALOG_CD
      LEFT JOIN 4_prod.raw.mill_orders o ON o.ORDER_ID = ce.ORDER_ID
      LEFT JOIN 3_lookup.mill.mill_code_value cv ON cv.CODE_VALUE = ce.EVENT_CD
      WHERE ce.EVENT_CLASS_CD IN (233, 236) AND oc.CATALOG_TYPE_CD = 2513 AND ce.ADC_UPDT > {wm}
    ),
    -- raw (TFC) source; description = COALESCE(...). CONTROLLER DECISION: watermark = rl.ADC_UPDT alone
    -- (no samplelevel join) — discovery needs only DISTINCT keys (from rl/m1, never sl), so the deduped
    -- sl1 join map_pathology uses is unnecessary here and an un-deduped sl join would fan out.
    raw_m1 AS (
      SELECT WkgCode, TFCCode, COALESCE(TFCDesc_Full, TFCDesc_Rep, TFCDesc_WP) AS descr
      FROM (SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.WkgCode, m.TFCCode
              ORDER BY m.LastUpdateDT DESC NULLS LAST) rn FROM 4_prod.raw.path_master_resultable m) WHERE rn=1
    ),
    raw_base AS (
      SELECT 'TFC' AS code_system, rl.TFCCode AS code, raw_m1.descr AS description,
             rl.TFCValue AS result_txt, rl.ADC_UPDT AS adc
      FROM 4_prod.raw.path_patient_resultlevel rl
      JOIN raw_m1 ON raw_m1.WkgCode = rl.WkgCode AND raw_m1.TFCCode = rl.TFCCode
      WHERE raw_m1.descr IS NOT NULL AND rl.ADC_UPDT > {wm}
    ),
    src_test AS (
      SELECT DISTINCT code_system, code, description,
             concat_ws(' | ', lower(code), lower(description)) AS term
      FROM (SELECT code_system, code, description FROM linked_base
            UNION ALL SELECT code_system, code, description FROM raw_base)
      WHERE code IS NOT NULL AND description IS NOT NULL
    ),
    src_result AS (
      SELECT DISTINCT code_system, code, description, result_normalized,
             concat_ws(' | ', lower(description), result_normalized) AS term
      FROM (
        SELECT code_system, code, description,
               LOWER(TRIM(REGEXP_REPLACE(result_txt,'\\\\s+',' '))) AS result_normalized
        FROM (SELECT code_system, code, description, result_txt FROM linked_base
              UNION ALL SELECT code_system, code, description, result_txt FROM raw_base)
        WHERE description IS NOT NULL AND result_txt IS NOT NULL
          AND NOT (result_txt RLIKE '^\\\\s*[<>]?=?\\\\s*-?[0-9.]+\\\\s*$')
      )
      -- drop sentinel/punctuation-only values (?, -, :, n/a) that never carry a mappable concept
      WHERE result_normalized <> '' AND result_normalized RLIKE '[a-z0-9]'
    ),
    miss_test AS (
      SELECT s.code_system, s.code, s.description, CAST(NULL AS STRING) AS result_normalized, s.term, 'test' AS kind
      FROM src_test s
      LEFT ANTI JOIN (SELECT code_system, code, description FROM {EMB_TEST_MAP} WHERE measurement_concept_id IS NOT NULL) m
        ON m.code_system=s.code_system AND m.code=s.code AND m.description=s.description
      -- Idempotency: exclude keys ALREADY in the queue under ANY status (pending/vector_ready/done/deferred).
      -- A pending/deferred key is already queued for embed; re-discovering it would blind-append a duplicate
      -- (mode=append, no uniqueness constraint). NOTE: re-attempting an already-embedded-but-'unmapped' key
      -- when a better concept candidate later appears is OUT OF SCOPE here (it would be a separate
      -- 'flip done->vector_ready' re-remap pass) — see tracked follow-up.
      LEFT ANTI JOIN (SELECT code_system, code, description FROM {QUEUE} WHERE kind='test') q
        ON q.code_system=s.code_system AND q.code=s.code AND q.description=s.description
    ),
    miss_result AS (
      SELECT s.code_system, s.code, s.description, s.result_normalized, s.term, 'result' AS kind
      FROM src_result s
      LEFT ANTI JOIN (SELECT code_system, code, description, result_normalized FROM {EMB_RESULT_MAP} WHERE value_as_concept_id IS NOT NULL) m
        ON m.code_system=s.code_system AND m.code=s.code AND m.description=s.description AND m.result_normalized=s.result_normalized
      -- Idempotency: exclude keys ALREADY in the queue under ANY status (see miss_test note above).
      -- I2 FIX: match on the FULL term (lossless), NOT substring_index(-1) (which is lossy when the
      -- result value contains ' | ' → those keys never matched their own queue row and re-appended
      -- every run). src_result.term = concat_ws(' | ', lower(description), result_normalized), and the
      -- queue stores that exact term, so LOWER(q.term)=LOWER(s.term) is an exact identity match.
      LEFT ANTI JOIN (SELECT code_system, code, description, LOWER(term) AS qterm FROM {QUEUE} WHERE kind='result') q
        ON q.code_system=s.code_system AND q.code=s.code AND q.description=s.description AND q.qterm=LOWER(s.term)
    ),
    all_missing AS (SELECT * FROM miss_test UNION ALL SELECT * FROM miss_result),
    classified AS (
      -- term_norm: legacy column. The loop keys on LOWER(term); the bulk pipeline (Task 14h) populated
      -- term_norm with a richer renormalized form. We write LOWER(term) here (harmless for the loop) —
      -- if any external consumer relies on the Task-14h semantics, reconcile separately (tracked follow-up).
      -- P0: embed_text = the BARE result text for results (open-set scoring grain), the context term for
      -- tests. Status keys on embed_text (the text that will actually be embedded), NOT term.
      SELECT a.*, LOWER(a.term) AS term_norm,
        CASE WHEN a.kind='result' THEN a.result_normalized ELSE a.term END AS embed_text,
        CASE WHEN e.term IS NOT NULL THEN 'vector_ready' ELSE 'pending' END AS status
      FROM all_missing a
      LEFT JOIN emb e ON LOWER(CASE WHEN a.kind='result' THEN a.result_normalized ELSE a.term END) = e.term
    )
    SELECT * FROM classified
    """
    df = spark.sql(discover_sql)
    agg = {(r["kind"], r["status"]): r["n"] for r in
           df.groupBy("kind","status").count().withColumnRenamed("count","n").collect()}
    n_test  = builtins.sum(v for (k,_),v in agg.items() if k=="test")
    n_result= builtins.sum(v for (k,_),v in agg.items() if k=="result")
    n_vr    = builtins.sum(v for (_,s),v in agg.items() if s=="vector_ready")
    n_pend  = builtins.sum(v for (_,s),v in agg.items() if s=="pending")
    if not dry_run:
        # QUEUE now carries result_normalized (lossless result identity) + embed_text (the canonical
        # embed/score key: bare result text for results, context term for tests). Downstream embed_pending_capped
        # + Stage 3b key on embed_text -- never substring_index (which truncated ' | '-bearing result values).
        (df.select("code_system","code","description","term","kind","status","term_norm",
                   "result_normalized","embed_text")
           .write.format("delta").mode("append").saveAsTable(QUEUE))
    return {"n_missing_test_keys": n_test, "n_missing_result_keys": n_result,
            "n_vector_ready": n_vr, "n_needs_embed": n_pend}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 — Stage 2: embed_pending_capped (+ reused AOAI worker)
# MAGIC Worker helpers copied verbatim from pathology_omop_02_bronze_pipeline (serverless-safe secret
# MAGIC fetch; non-fatal probe). embed_pending_capped embeds DISTINCT pending texts (freq-ordered, capped),
# MAGIC flips pending->vector_ready (text now embedded) and capped overflow pending->deferred.

# COMMAND ----------

# ── Reused AOAI worker (from pathology_omop_02_bronze_pipeline) ──────────────────────────────────────
# v2.3 CHANGE: openai is imported LAZILY (inside _get_embedding_client, called only by _embed_one_batch),
# so `%run` of this module succeeds on ANY compute with zero env-prep. `import openai` + the AOAI client +
# the secret fetch happen ONLY when an actual embed runs (where openai + the adc_store secret are required
# anyway). The old import-time client + non-fatal probe are removed (nothing AOAI runs at import).
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys, os, gc, time, glob, uuid, json, socket
import builtins
from pyspark.sql.types import (StructType, StructField, StringType, ArrayType,
                               FloatType, TimestampType)
from pyspark.sql.functions import current_timestamp

AOAI_ENDPOINT    = "https://benja-m6p4lv77-eastus2.cognitiveservices.azure.com"
AOAI_API_VERSION = "2023-05-15"
AOAI_DEPLOYMENT  = "text-embedding-3-large"
EMBEDDING_MODEL  = AOAI_DEPLOYMENT

_EMBED_CLIENT = None   # memoised; built on first real embed via _get_embedding_client()
def _get_embedding_client():
    """Lazily import openai, fetch the AOAI key, and build the AzureOpenAI client. Called ONLY from
    _embed_one_batch (the sole AOAI consumer) — so merely %run-importing this module needs neither
    openai installed nor the secret scope reachable. Memoised: built once per session."""
    global _EMBED_CLIENT
    if _EMBED_CLIENT is not None:
        return _EMBED_CLIENT
    from openai import AzureOpenAI   # lazy: only when an embed actually runs
    try:
        _key = dbutils.secrets.get(scope="adc_store", key="barts_global_key")
    except NameError:
        from databricks.sdk import WorkspaceClient
        _key = WorkspaceClient().dbutils.secrets.get(scope="adc_store", key="barts_global_key")
    _EMBED_CLIENT = AzureOpenAI(api_key=_key, api_version=AOAI_API_VERSION, azure_endpoint=AOAI_ENDPOINT)
    return _EMBED_CLIENT

BATCH_SIZE          = 256
MAX_WORKERS         = 16
MAX_RETRIES         = 6
FETCH_BUFFER_TERMS  = 32_768
PROGRESS_EVERY      = 16
# COMMIT_EVERY_BATCHES removed — commits now fire per-buffer.

MAX_CONSECUTIVE_FAILURES  = 3
# Embed-loop scratch. It follows ENVIRONMENT so a production run never stages parquet in, reads
# parquet from, or takes its mutual-exclusion lock inside the development catalog. Prod stages
# alongside the embeddings table it commits into (3_lookup.embeddings.terms); dev keeps 8_dev.
STAGING_SCHEMA            = "3_lookup.embeddings" if ENVIRONMENT == "prod" else "8_dev.embeddings"
STAGING_VOLUME_NAME       = "staging_pathology"
STAGING_PATH              = f"/Volumes/{STAGING_SCHEMA.replace('.', '/')}/{STAGING_VOLUME_NAME}"
LOCK_FILE                 = f"{STAGING_PATH}/.embed_lock"
LOCK_STALE_SECONDS        = 300

# Belt-and-braces: a prod run must never reach the dev catalog through any of these four constants.
if ENVIRONMENT == "prod":
    assert not STAGING_PATH.startswith("/Volumes/8_dev/"), f"prod run staging into the dev catalog: {STAGING_PATH}"
    assert not EMBEDDINGS_TABLE.startswith("8_dev."), f"prod run embedding into the dev catalog: {EMBEDDINGS_TABLE}"
    assert not EMB_MAP_SCHEMA.startswith("8_dev."), f"prod run mapping into the dev catalog: {EMB_MAP_SCHEMA}"
    assert not EMB_MP_TARGET.startswith("8_dev."), f"prod run writing map_pathology into the dev catalog: {EMB_MP_TARGET}"

print(f"AOAI endpoint:     {AOAI_ENDPOINT}")
print(f"Deployment:        {AOAI_DEPLOYMENT}")
print(f"Embed config:      batch_size={BATCH_SIZE}, max_workers={MAX_WORKERS}, "
      f"buffer={FETCH_BUFFER_TERMS:,}, commit_per_buffer=True")
print(f"Staging path:      {STAGING_PATH}")
print(f"Lock file:         {LOCK_FILE}")

# ── Staging volume (verbatim) ───────────────────────────────────────────────────────────────────────
# [folded] staging schema/volume/directory now runs inside _emb_ensure_infrastructure()

# ── Lock helpers (verbatim) ─────────────────────────────────────────────────────────────────────────
SESSION_ID = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"

def _read_lock():
    if not os.path.exists(LOCK_FILE):
        return None
    try:
        with open(LOCK_FILE) as f:
            return json.load(f)
    except Exception:
        return None

def acquire_lock():
    existing = _read_lock()
    if existing:
        hb_age = time.time() - existing.get("heartbeat_ts", 0)
        if hb_age < LOCK_STALE_SECONDS:
            raise RuntimeError(
                f"\n*** ANOTHER SESSION IS RUNNING — REFUSING TO START ***\n"
                f"  session_id  : {existing.get('session_id')}\n"
                f"  started_utc : {existing.get('started_utc')}\n"
                f"  last_hb_ago : {hb_age:.0f}s ago (stale = {LOCK_STALE_SECONDS}s)\n"
                f"\nIf you're SURE no other session is active, delete the lock:\n"
                f"  dbutils.fs.rm('{LOCK_FILE}')"
            )
        print(f"[warn] stale lock from {existing.get('session_id')} "
              f"({hb_age:.0f}s ago) — taking over.")
    payload = {
        "session_id":   SESSION_ID,
        "started_utc":  utcnow().isoformat(),
        "heartbeat_ts": time.time(),
        "host":         socket.gethostname(),
    }
    with open(LOCK_FILE, "w") as f:
        json.dump(payload, f)
    print(f"✓ Lock acquired: {SESSION_ID}")

def heartbeat_lock():
    existing = _read_lock()
    if not existing or existing.get("session_id") != SESSION_ID:
        raise RuntimeError(f"Lock no longer owned by this session (now {existing}). Aborting.")
    existing["heartbeat_ts"] = time.time()
    with open(LOCK_FILE, "w") as f:
        json.dump(existing, f)

def release_lock():
    existing = _read_lock()
    if existing and existing.get("session_id") == SESSION_ID:
        try:
            os.remove(LOCK_FILE)
            print(f"✓ Lock released: {SESSION_ID}")
        except Exception as e:
            print(f"[warn] couldn't release lock: {e}")

# ── Transient classification + batch embed + parquet staging (verbatim) ───────────────────────────────
_TRANSIENT_SPARK_ERRORS = (
    "TEMPORARILY_UNAVAILABLE", "UNAVAILABLE", "RETRIES_EXCEEDED",
    "DEADLINE_EXCEEDED", "_InactiveRpcError",
    "channel closed", "Connection reset", "Connection refused", "EOF occurred",
)
def _is_transient(e: Exception) -> bool:
    return any(s in str(e) for s in _TRANSIENT_SPARK_ERRORS)

def _embed_one_batch(chunk: List[str]) -> List[List[float]]:
    delay = 1.0
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = _get_embedding_client().embeddings.create(
                model=AOAI_DEPLOYMENT,
                input=chunk,
                encoding_format="float",
            )
            return [d.embedding for d in resp.data]
        except Exception as e:
            last_exc = e
            time.sleep(delay)
            delay = builtins.min(delay * 2, 30.0)
    raise RuntimeError(f"embedding batch failed after {MAX_RETRIES} retries: {last_exc}")

_PA_SCHEMA = pa.schema([
    pa.field("term",             pa.string()),
    pa.field("embedding_vector", pa.list_(pa.float32())),
    pa.field("model_version",    pa.string()),
    pa.field("created_at",       pa.timestamp("us")),
    pa.field("embedded_at",      pa.timestamp("us")),
    pa.field("ADC_UPDT",         pa.timestamp("us")),
])

def _stage_one_batch_to_parquet(terms: List[str], vectors: List[List[float]]) -> str:
    now = utcnow().replace(tzinfo=None)
    n = len(terms)
    table = pa.table({
        "term":             pa.array(terms, type=pa.string()),
        "embedding_vector": pa.array(vectors, type=pa.list_(pa.float32())),
        "model_version":    pa.array([EMBEDDING_MODEL] * n, type=pa.string()),
        "created_at":       pa.array([now] * n, type=pa.timestamp("us")),
        "embedded_at":      pa.array([now] * n, type=pa.timestamp("us")),
        "ADC_UPDT":         pa.array([now] * n, type=pa.timestamp("us")),
    }, schema=_PA_SCHEMA)
    fname = f"chunk_{int(time.time()*1000)}_{uuid.uuid4().hex[:10]}.parquet"
    fpath = os.path.join(STAGING_PATH, fname)
    pq.write_table(table, fpath, compression="snappy")
    return fname

def _worker_embed_and_stage(batch_terms: List[str]) -> Tuple[int, str, float]:
    # Belt-and-braces canonicalization: lowercase the batch before embed + stage.
    # Fetch queries already lowercase via LOWER(...) but this guarantees the
    # written `term` column is canonical even if a caller bypasses the fetcher.
    batch_terms = [t.lower() for t in batch_terms if t is not None]
    t0 = time.time()
    vectors = _embed_one_batch(batch_terms)
    fname = _stage_one_batch_to_parquet(batch_terms, vectors)
    return len(batch_terms), fname, time.time() - t0

# -- embeddings write-probe helpers (verbatim); now invoked lazily from _emb_ensure_infrastructure()
_probe_schema = StructType([
    StructField("term", StringType(), True),
    StructField("embedding_vector", ArrayType(FloatType()), True),
    StructField("model_version", StringType(), True),
])
def _probe_3_lookup():
    spark.createDataFrame([], _probe_schema) \
        .withColumn("created_at", current_timestamp()) \
        .withColumn("embedded_at", current_timestamp()) \
        .withColumn("ADC_UPDT", current_timestamp()) \
        .write.format("delta").mode("append").saveAsTable(EMBEDDINGS_TABLE)
# _probe_8_dev removed. A production run must never redirect its embeddings into the dev catalog,
# so _emb_ensure_infrastructure() now fails loudly rather than falling back to it.

# [folded] embeddings write-probe (empty-DataFrame append) now runs inside _emb_ensure_infrastructure()

# ── Staged-file listing + commit-to-delta (verbatim) ──────────────────────────────────────────────────
def _list_staged_files() -> List[str]:
    return sorted(glob.glob(STAGING_PATH + "/*.parquet"))

def commit_staged_to_delta() -> dict:
    staged = _list_staged_files()
    if not staged:
        return {"status": "noop", "files": 0, "rows": 0}
    try:
        df = spark.read.schema(StructType([
            StructField("term",             StringType(),               True),
            StructField("embedding_vector", ArrayType(FloatType()),     True),
            StructField("model_version",    StringType(),               True),
            StructField("created_at",       TimestampType(),            True),
            StructField("embedded_at",       TimestampType(),            True),
            StructField("ADC_UPDT",         TimestampType(),            True),
        ])).parquet(*staged)
        n_rows = df.count()
        df.write.format("delta").mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(_emb_active_embeddings_table())
        for f in staged:
            try: os.remove(f)
            except Exception as rm_e: print(f"[warn] couldn't delete {f}: {rm_e}")
        return {"status": "ok", "files": len(staged), "rows": n_rows}
    except Exception as e:
        return {"status": "failed", "files": len(staged), "rows": 0,
                "error": str(e)[:300], "transient": _is_transient(e)}

# COMMAND ----------

def embed_pending_capped(max_terms=MAX_EMBED_TERMS, max_cost_usd=None, dry_run=False):
    """Stage 2. Embed DISTINCT pending texts (top-N by frequency, within the cap); overflow -> deferred.
    After commit, flip pending->vector_ready for keys whose text now has a vector. Returns counts."""
    _emb_ensure_infrastructure()   # folded: lazy, idempotent, replaces the import-time setup
    # P0: embed + flip on embed_text (bare result text for results / context term for tests) -- the open-set
    # scoring grain Stage 3b looks up. (Was LOWER(term) = the 'desc | result' context string for results,
    # which never matched the bare-text substrate lookup.) freq priority is a best-effort LEFT JOIN: the freq
    # table is keyed on context terms, so result embed_texts may not match -> COALESCE(ec,0) sorts them last;
    # acceptable for the capped weekly volume (a bare-result freq rebuild is a separate, non-blocking follow-up).
    fetch_sql = f"""
    WITH pend AS (SELECT DISTINCT LOWER(embed_text) AS term FROM {QUEUE} WHERE status='pending' AND embed_text IS NOT NULL),
         freq AS (SELECT LOWER(term) AS term, SUM(event_count) ec FROM {FREQ_TABLE_MILLRAW} GROUP BY LOWER(term))
    SELECT p.term
    FROM pend p
    LEFT ANTI JOIN (SELECT term FROM {EMBEDDINGS_TABLE} WHERE embedding_vector IS NOT NULL) e ON p.term=e.term
    LEFT JOIN freq f ON p.term=f.term
    ORDER BY COALESCE(f.ec,0) DESC, p.term
    """
    pending_texts = [r["term"] for r in spark.sql(fetch_sql).collect()]
    cap = max_terms
    if max_cost_usd is not None:
        cap = builtins.min(cap, int(max_cost_usd / EST_USD_PER_TERM))
    to_embed = pending_texts[:cap]
    overflow = pending_texts[cap:]
    # est_cost_usd is the ATTEMPTED upper-bound (cap * unit), not realized spend (partial failures cost less).
    est_cost = len(to_embed) * EST_USD_PER_TERM
    if dry_run:
        return {"n_needs_embed": len(pending_texts), "n_embedded": 0,
                "n_deferred": len(overflow), "est_cost_usd": est_cost, "would_embed": len(to_embed)}
    n_embedded = 0
    if to_embed:
        acquire_lock()
        try:
            batches = [to_embed[i:i+BATCH_SIZE] for i in range(0, len(to_embed), BATCH_SIZE)]
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futs = [ex.submit(_worker_embed_and_stage, b) for b in batches]
                for fut in as_completed(futs):
                    try:
                        n,_,_ = fut.result(); n_embedded += n
                    except Exception as e:
                        print(f"[warn] batch failed: {str(e)[:200]}")
            print(commit_staged_to_delta())
        finally:
            release_lock()
    # The two MERGEs run AFTER release_lock intentionally: they are pending-guarded + idempotent; the lock
    # guards AOAI spend + parquet staging, not queue state.
    spark.sql(f"""
      MERGE INTO {QUEUE} t
      USING (SELECT DISTINCT LOWER(term) AS term FROM {EMBEDDINGS_TABLE} WHERE embedding_vector IS NOT NULL) s
      ON LOWER(t.embed_text)=s.term
      WHEN MATCHED AND t.status='pending' THEN UPDATE SET t.status='vector_ready'
    """)
    # Flip capped-overflow pending->deferred IN-WAREHOUSE (no driver list; bounded regardless of backlog).
    # Deferred = still-pending texts that are NOT now embedded AND rank beyond `cap` by frequency.
    # MERGE #1 above already moved embedded texts to vector_ready; this only touches what's left pending.
    # n_deferred is the REAL flipped count (num_updated_rows), <= the overflow size.
    n_deferred = 0
    if len(overflow) > 0:
        m = spark.sql(f"""
          MERGE INTO {QUEUE} t
          USING (
            WITH pend AS (SELECT DISTINCT LOWER(embed_text) AS term FROM {QUEUE} WHERE status='pending' AND embed_text IS NOT NULL),
                 freq AS (SELECT LOWER(term) AS term, SUM(event_count) ec FROM {FREQ_TABLE_MILLRAW} GROUP BY LOWER(term)),
                 ranked AS (
                   SELECT p.term,
                          ROW_NUMBER() OVER (ORDER BY COALESCE(f.ec,0) DESC, p.term) AS rn
                   FROM pend p
                   LEFT ANTI JOIN (SELECT term FROM {EMBEDDINGS_TABLE} WHERE embedding_vector IS NOT NULL) e ON p.term=e.term
                   LEFT JOIN freq f ON p.term=f.term
                 )
            SELECT term FROM ranked WHERE rn > {cap}
          ) s
          ON LOWER(t.embed_text)=s.term
          WHEN MATCHED AND t.status='pending' THEN UPDATE SET t.status='deferred'
        """).first()
        n_deferred = int(m["num_updated_rows"]) if m is not None and "num_updated_rows" in m.asDict() else 0
    return {"n_needs_embed": len(pending_texts), "n_embedded": n_embedded,
            "n_deferred": n_deferred, "est_cost_usd": est_cost}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 — Stage 3: remap_keys (serverless Spark cross-join; NO driver numpy)
# MAGIC Chunked (REMAP_CHUNK=500) cross-join dot-product; scored materialised to a Delta scratch table
# MAGIC per chunk; test side (5a) + context-scoped result side (5b, answer-list preferred). Aborts above
# MAGIC REMAP_MAX_SERVERLESS=5000 vector_ready keys (run bulk Task-15 on a cluster instead).

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# ── Task 5 chunking config ───────────────────────────────────────────────────────────────────────────
REMAP_CHUNK         = 500    # vector_ready keys processed per cross-join chunk
REMAP_MAX_SERVERLESS = 5000  # abort guard: above this, run the bulk Task-15 build on a cluster

def _remap_test(dry_run=False):
    """Stage 3a — map vector_ready TEST keys into EMB_TEST_MAP via a serverless Spark cross-join dot-product
    (aggregate(zip_with(qv,cv,*))), chunked at REMAP_CHUNK with the cross-join materialised once per
    chunk to a Delta SCRATCH table (cache()/temp-views banned on serverless). The bbv_override layer is
    inlined as a CTE. Aborts above REMAP_MAX_SERVERLESS vector_ready keys (run the bulk cluster build)."""
    n_vr = spark.sql(
        f"SELECT COUNT(*) c FROM {QUEUE} WHERE kind='test' AND status='vector_ready'").first()["c"]
    if n_vr > REMAP_MAX_SERVERLESS:
        raise RuntimeError(
            f"_remap_test: {n_vr:,} vector_ready test keys exceed REMAP_MAX_SERVERLESS={REMAP_MAX_SERVERLESS:,}. "
            f"This is a backfill/cold-replay-scale remap — run the bulk Task-15 build on a CLASSIC CLUSTER "
            f"(pathology_omop_02c_task15_bulk_cluster), not the serverless cross-join.")
    if dry_run:
        return n_vr  # dry-run: count only, no scratch/merge

    CHUNK  = f"{EMB_MAP_SCHEMA}._tmp_remap_chunk_test"
    SCORED = f"{EMB_MAP_SCHEMA}._tmp_remap_scored_test"
    n_remapped = 0
    while True:
        # 1) materialise up to REMAP_CHUNK vector_ready test keys to a Delta scratch table (deterministic order)
        spark.sql(f"""
          CREATE OR REPLACE TABLE {CHUNK} USING DELTA AS
          SELECT eq.code_system, eq.code, eq.description, LOWER(eq.term) AS term
          FROM {QUEUE} eq
          WHERE eq.kind='test' AND eq.status='vector_ready'
          ORDER BY eq.code_system, eq.code, eq.description
          LIMIT {REMAP_CHUNK}
        """)
        if spark.sql(f"SELECT COUNT(*) c FROM {CHUNK}").first()["c"] == 0:
            break

        # 2) build `scored` (the expensive cross-join dot-product) ONCE and write it to a Delta scratch
        #    table. BROADCAST the small chunk side. cache()/temp-views are banned on serverless.
        #    The override layer reads the tiny CHUNK table directly (step 3), so only the cross-join
        #    (non-override `remaining`) needs materialising here.
        spark.sql(f"""
          CREATE OR REPLACE TABLE {SCORED} USING DELTA AS
          WITH q AS (SELECT code_system, code, description, term FROM {CHUNK}),
          ov AS (SELECT pattern, concept_id, concept_name, match_mode FROM {TEST_OVERRIDES}),
          override_hits AS (
            SELECT q.code_system, q.code, q.description
            FROM q JOIN ov
              ON (ov.match_mode='exact' AND lower(q.description)=ov.pattern)
              OR (ov.match_mode='contains' AND lower(q.description) LIKE concat('%',ov.pattern,'%'))
            GROUP BY q.code_system, q.code, q.description
          ),
          remaining AS (
            SELECT q.code_system,q.code,q.description, e.embedding_vector AS qv
            FROM q JOIN {EMBEDDINGS_TABLE} e ON q.term=LOWER(e.term)
            LEFT JOIN override_hits o ON q.code_system=o.code_system AND q.code=o.code AND q.description=o.description
            WHERE e.embedding_vector IS NOT NULL AND o.code_system IS NULL
          ),
          cand AS (
            SELECT ci.concept_id, ci.concept_name, ci.vocabulary_id, e.embedding_vector AS cv
            FROM {TEST_INDEX} ci JOIN {EMBEDDINGS_TABLE} e ON ci.term=LOWER(e.term)
            WHERE e.embedding_vector IS NOT NULL
          )
          SELECT /*+ BROADCAST(r) */
                 r.code_system,r.code,r.description,c.concept_id,c.concept_name,c.vocabulary_id,
                 -- dot product == cosine: embedding_vector is L2-unit-normalized (text-embedding-3-large default); the 0.85/0.80/0.82/0.70 thresholds depend on this.
                 aggregate(zip_with(r.qv,c.cv,(x,y)->x*y),CAST(0.0 AS DOUBLE),(a,x)->a+x) AS sim
          FROM remaining r CROSS JOIN cand c
        """)

        # 3) tiering + projection read the scratch `scored` table (cross-join NOT re-evaluated) +
        #    re-derive override_hits from the tiny CHUNK table (no cross-join) -> new_df
        new_df = spark.sql(f"""
          WITH q AS (SELECT code_system, code, description, term FROM {CHUNK}),
          ov AS (SELECT pattern, concept_id, concept_name, match_mode FROM {TEST_OVERRIDES}),
          override_hits AS (
            SELECT q.code_system, q.code, q.description, ov.concept_id, ov.concept_name,
                   'bbv_override' AS source, CAST(1.0 AS DOUBLE) AS sim, 'curated' AS tier
            FROM q JOIN ov
              ON (ov.match_mode='exact' AND lower(q.description)=ov.pattern)
              OR (ov.match_mode='contains' AND lower(q.description) LIKE concat('%',ov.pattern,'%'))
            QUALIFY ROW_NUMBER() OVER (PARTITION BY q.code_system,q.code,q.description
              ORDER BY CASE ov.match_mode WHEN 'exact' THEN 0 ELSE 1 END, length(ov.pattern) DESC)=1
          ),
          scored AS (SELECT code_system,code,description,concept_id,concept_name,vocabulary_id,sim
                     FROM {SCORED}),
          per_vocab_top AS (SELECT * FROM scored QUALIFY ROW_NUMBER() OVER
            (PARTITION BY code_system,code,description,vocabulary_id ORDER BY sim DESC)=1),
          loinc_hit AS (SELECT * FROM per_vocab_top WHERE vocabulary_id='LOINC' AND sim>={SIM_HIGH}),
          snomed_hit AS (SELECT * FROM per_vocab_top WHERE vocabulary_id='SNOMED' AND sim>={SIM_HIGH}),
          best_any AS (SELECT * FROM per_vocab_top QUALIFY ROW_NUMBER() OVER
            (PARTITION BY code_system,code,description ORDER BY sim DESC)=1),
          chosen AS (
            SELECT code_system,code,description,concept_id,concept_name,vocabulary_id,sim,
                   'embedding_loinc' AS source,'auto_high' AS tier FROM loinc_hit
            UNION ALL
            SELECT s.code_system,s.code,s.description,s.concept_id,s.concept_name,s.vocabulary_id,s.sim,
                   'embedding_snomed','auto_high' FROM snomed_hit s LEFT JOIN loinc_hit l
              ON s.code_system=l.code_system AND s.code=l.code AND s.description=l.description
              WHERE l.code_system IS NULL
            UNION ALL
            SELECT b.code_system,b.code,b.description,b.concept_id,b.concept_name,b.vocabulary_id,b.sim,
                   CASE WHEN b.vocabulary_id='LOINC' THEN 'embedding_loinc' ELSE 'embedding_snomed' END,
                   CASE WHEN b.sim>={SIM_LOW} THEN 'auto_low' ELSE 'unmapped' END
            FROM best_any b
            LEFT JOIN loinc_hit l ON b.code_system=l.code_system AND b.code=l.code AND b.description=l.description
            LEFT JOIN snomed_hit s ON b.code_system=s.code_system AND b.code=s.code AND b.description=s.description
            WHERE l.code_system IS NULL AND s.code_system IS NULL
          ),
          unioned AS (
            SELECT code_system,code,description,concept_id,concept_name,
                   CAST(NULL AS STRING) AS vocabulary_id, sim, source, tier FROM override_hits
            UNION ALL SELECT code_system,code,description,concept_id,concept_name,vocabulary_id,sim,source,tier FROM chosen
          )
          SELECT u.code_system,u.code,u.description,
                 CASE WHEN u.tier='unmapped' THEN CAST(NULL AS BIGINT) ELSE u.concept_id END AS measurement_concept_id,
                 COALESCE(CASE WHEN u.source='bbv_override' THEN c.vocabulary_id ELSE u.vocabulary_id END, u.vocabulary_id) AS concept_vocabulary_id,
                 u.concept_name, u.source AS test_mapping_source, u.sim AS similarity_score, u.tier AS confidence_tier
          FROM unioned u LEFT JOIN {EMB_CONCEPT} c ON c.concept_id=u.concept_id
        """)
        new_df = (new_df.withColumn("mapping_version", F.lit(1))
                        .withColumn("mapped_at", F.current_timestamp())
                        .withColumn("ADC_UPDT", F.current_timestamp()))
        # 4) MERGE this chunk's keys into EMB_TEST_MAP (verbatim merge condition + mapping_version bump)
        tgt = DeltaTable.forName(spark, EMB_TEST_MAP)
        (tgt.alias("t").merge(new_df.alias("s"),
            "t.code_system=s.code_system AND t.code=s.code AND t.description=s.description")
         .whenMatchedUpdate(condition=(
            "t.measurement_concept_id IS DISTINCT FROM s.measurement_concept_id "
            "OR t.confidence_tier IS DISTINCT FROM s.confidence_tier "
            "OR t.test_mapping_source IS DISTINCT FROM s.test_mapping_source "
            "OR ABS(COALESCE(t.similarity_score,0)-COALESCE(s.similarity_score,0))>0.001"),
            set={"measurement_concept_id":"s.measurement_concept_id","concept_vocabulary_id":"s.concept_vocabulary_id",
                 "concept_name":"s.concept_name","test_mapping_source":"s.test_mapping_source",
                 "similarity_score":"s.similarity_score","confidence_tier":"s.confidence_tier",
                 "mapping_version":"t.mapping_version + 1","mapped_at":"current_timestamp()","ADC_UPDT":"current_timestamp()"})
         .whenNotMatchedInsertAll().execute())
        n_chunk = spark.sql(f"SELECT COUNT(*) c FROM {CHUNK}").first()["c"]
        # n_remapped counts PROCESSED chunk keys (incl. those that mapped to 'unmapped'), not rows the map changed; the real map-change counts come from Task 9's run-log via the into-map MERGE.
        n_remapped += n_chunk
        # 5) flip THIS chunk's keys vector_ready->done (kind-scoped), then loop
        spark.sql(f"""
          MERGE INTO {QUEUE} t USING {CHUNK} s
          ON t.kind='test' AND t.code_system=s.code_system AND t.code=s.code AND t.description=s.description
          WHEN MATCHED AND t.status='vector_ready' THEN UPDATE SET t.status='done'
        """)
    spark.sql(f"DROP TABLE IF EXISTS {CHUNK}")
    spark.sql(f"DROP TABLE IF EXISTS {SCORED}")
    return n_remapped

# COMMAND ----------

# ════════════════════════════════════════════════════════════════════════════════════════════════
# Stage 3b — OPEN-SET + EMBEDDING-PRIMARY-FUSION result mapping (REBUILT 2026-06-30)
# Replaces the legacy nearest-context arm (broad LOINC/SNOMED + answer-list, polarity-BLIND) with the
# SAME arms the bulk build uses: anchor-fusion (embedding-primary polarity) + organism + generic-positive
# + value/morphology FLAG_DICT + LOINC-answer. Serverless throughout (small pools + cross-join dot-product,
# NEVER the 96k driver matrix). Routing/fusion are SINGLE-SOURCED via the canonical rerouter notebooks
# (dbutils.notebook.run) — no fourth copy of the regex/threshold logic in this module.
# ════════════════════════════════════════════════════════════════════════════════════════════════

# Open-set pool tables (small; serverless-safe). organism_vectors carries its embedding inline.
ANCHOR_TABLE   = f"{EMB_MAP_SCHEMA}.concept_result_anchors"        # 76 active surface forms
ORG_VECS       = f"{EMB_MAP_SCHEMA}.organism_vectors"              # concept_id, concept_name, embedding_vector
ORG_ALLOW      = f"{EMB_MAP_SCHEMA}.concept_result_organism_allow" # ~260 allow concepts (allow-subset, NOT 96k)
MORPH_DICT     = f"{EMB_MAP_SCHEMA}._morphology_flag_dict"         # value/morphology token -> value_concept_id
FINDING_DICT   = f"{EMB_MAP_SCHEMA}._finding_flag_dict"            # finding token_regex -> finding concept (+suborder)
SUBSTRATE      = f"{EMB_MAP_SCHEMA}._tmp_openset_substrate"        # the 29-col openset_lookup schema (this run's keys)
SUBSTRATE_FUSED= f"{EMB_MAP_SCHEMA}._tmp_openset_substrate_fused"  # after the fusion rerouter
LOINC_PROP     = f"{EMB_MAP_SCHEMA}._tmp_loinc_proposal"           # LOINC-answer arm (4-tuple grain)
VALUE_PROP     = f"{EMB_MAP_SCHEMA}._tmp_value_proposal"           # value/morphology decode (text grain)
FINDING_PROP   = f"{EMB_MAP_SCHEMA}._tmp_finding_proposal"         # finding-axis decode (text + suborder grain)
# F3 grade-strip regex, verbatim from pathology_omop_02d_openset_result_cluster _stage_queries (score_text).
GRADE_STRIP_RE = r"\\s*(\\+{1,4}|\\b[1-4]\\+|\\b(profuse|heavy|moderate|scanty|light|few|occasional)\\b)"
# bulk integrate organism acceptance gate (rerouter A2): FLOOR/MARGIN/DELTA.
OS_FLOOR_VALUE, OS_MARGIN_VALUE, OS_DELTA_VALUE = 0.63, 0.02, 0.04
# LOINC-answer arm thresholds (bulk Phase I).
FLOOR_LOINC, MARGIN_LOINC = 0.62, 0.05

# ── Polarity gate (§B) ── cache-backed veto (populated by polgate_gpu_run) ──
GATE_ENABLED       = True
GATE_TABLE         = f"{EMB_MAP_SCHEMA}.pathology_polarity_gate"
GATE_MODEL_VERSION = "deberta-v3-large-mnli-fever-anli-ling-wanli"
GATE_VERSION       = 1

# BAKED CONFIG — generated by _stage3b_config_generator from _openset_routing_config + _fusion_cfg
#   + exclusions. SOURCE-OF-TRUTH copy; regenerate + re-promote if those tables change.
_ROUTE_CFG = {
    'CLEAN_NEG_BLOCK_RE': '\\b(rh ?d|rhd|blood group|kell|duffy|kidd|anti[- ]?[abd])\\b',
    'CLEAN_NEG_MAXLEN': '45',
    'COMPOUND_RE': '[,;]| and | but | however | except |:\\s|[a-z]{2}\\.\\s+[a-z]',
    'FORCED_ANCHOR_MAP': '[["not\\\\s+isolat|no\\\\b.{0,40}isolat", "not isolated"], ["no\\\\s+growth|not\\\\b.{0,40}grow|no\\\\b.{0,40}grow", "no growth"], ["not\\\\s+detect|no\\\\b.{0,40}detect|non[- ]?detect|not\\\\s+detected|pcr negative", "not detected"], ["not\\\\s+seen|no\\\\b.{0,40}seen|not\\\\s+observed|no\\\\s+organisms?\\\\s+seen", "not seen"], ["non[- ]?react|not\\\\s+react", "non-reactive"], ["\\\\babsent\\\\b|not\\\\s+present|no\\\\b.{0,40}presen", "absent"], ["\\\\bnegative\\\\b|\\\\bneg$|[ -]ve$|cannot be excluded|cannot be ruled out|\\\\bsusceptible to\\\\b", "negative"]]',
    'FRAGMENT_RE': '^(this (hiv|hbv|hcv)? ?(antibody )?(assay|test) (is|does|can|only)|this is consistent|this confirms|samples today|weak d reaction|for [a-z]+ antigen, suggesting|insufficient identification|low level of|non-reacting|these results (would|within)|slope neg|immunisation recommended|patients (under|with)|suppression may|and should be followed|please (note|see|contact|repeat)|results within normal limits$|review results$|consider )',
    'GENERIC_POS_RE': 'isolated from .{0,30}bottle|presumptive isolate|^isolated\\.?$|^mixed growth\\b|\\bmixed growth\\.?$|^growth\\.?$|\\bheavy growth\\b|\\bscanty growth\\b|\\blight growth\\b|\\bmoderate growth\\b|\\bprofuse growth\\b',
    'GRADE_RE': '\\b(profuse|heavy|moderate|scanty|light|few|occasional)\\b|\\+{1,4}|\\b[1-4]\\+',
    'HAS_ORGANISM_TOKEN_RE': 'coccus|cocci|bacill|\\brods?\\b|coliform|strepto|staphylo|pseudomon|klebsiella|e\\.?\\s?coli|enterococc|candida|yeast|aspergill|crypto|\\bmyco|salmonella|shigella|proteus|serratia|haemophil|neisseria|clostrid|listeria|enterobact|acinetobacter|morganella|citrobacter|providencia|diphther|\\b[a-z]+ [a-z]+(?:us|um|is|ae|osa|ica)\\b',
    'ISOLATE_POS_RE': '^(isolated|presumptive isolate)',
    'JUNK_MAXLEN': '2',
    'JUNK_RE': '^[^a-z]*$|^[a-z]{1,2}[0-9][a-z0-9]*$',
    'MORPH_RE': 'gram[ -]?(?:negative|positive)|coagulase[ -]?negative|oxidase[ -]?negative|catalase[ -]?negative',
    'NEG_RE': '(?:\\bnot\\b|\\bno\\b|\\bnon[- ]?|\\bnone\\b).{0,40}?(?:detect|seen|isolat|grow|react|presen|ident)|\\bnegative\\b|\\bno growth\\b|\\bnon[- ]?reactive\\b|\\bnot detected\\b|\\bnon detected\\b|\\bnot seen\\b|\\bnot observed\\b|\\bneg$|[ -]ve$|not isolated|pcr negative|cannot be excluded|cannot be ruled out|\\bsusceptible to\\b',
    'ORG_BLOCK_RE': '(\\btest$)|(\\bserology$)|(\\bscreen$)|(\\bscreen )|(\\bset up$)|(\\bsens up$)|(\\btyping\\b)|(\\bplease\\b)|(^\\?\\s)|(antibody test)|(\\borf[0-9])|,.{1,40},.{1,40},|(are part of)|(is widespread)|(are usually)|(are normal$)|(^to )|(^hpv only$)|(^hiv,$)|\\besbl\\b|(^group [a-z] strep$)|(antigen,$)|(^hepatitis b\\.?$)|(^hbs$)|(evidence of past)|(\\bpast (exposure|infection))|(not recent)|(no recent)|(previou.{0,15}(exposure|infection))|(historic)|(coliform/pseudomon)|(shigella/enteroinvasive)|(carbapenemase producing organism)',
    'POSITIVE_OVERRIDE_MAP': '[["shigella ?/ ?(enteroinvasive e ?coli|eiec) dna detected", 21498489, "Shigella/EIEC"]]',
    'POS_RE': '\\bdetected\\b|\\bpositive\\b|\\breactive\\b|\\bisolated\\b|\\bseen\\b|\\bpresent\\b|\\bidentified\\b|\\bgrowth of\\b|\\bgrowth\\b',
    'SUSPECTED_RE': '\\b(possible|possibly|suggest(s|ive)?|presumptive(ly)?|probable|probably|likely|query)\\b',
    'THRESHOLD_DELTA': '0.04',
    'THRESHOLD_FLOOR': '0.63',
    'THRESHOLD_MARGIN': '0.02',
    'WRONG_SPECIES_DENY': '[4216359, 4017991, 37116714, 1244038]',
}

_EXCL_PATTERNS = [
    'not tested',
    'insufficient (sample|serum|specimen|quantity|volume|plasma)',
    'no sample received',
    'sample (haemolysed|not received|leaked|clotted)',
    '\\bhaemolysed\\b',
    '\\bunsuitable\\b',
    '(edta|sample) contamination',
    'duplicate (request|specimen|sample|order)',
    '\\bdeleted\\b',
    '\\bcancelled\\b',
    '(requested|ordered|booked on|check) in error',
    '\\bin error\\b',
    'wrong sample (received|type)',
    'laboratory error|technical (problem|error)|apologies',
    '^\\(?pending\\)?\\.?$',
    '\\bawaiting\\b|to follow|in progress|\\btbc\\b',
    'see (comment|report|below|above|note|attached)',
    'no result available',
    'please see|refer to',
    '(test )?not (indicated|applicable|required|performed)',
    '^n/?a\\.?$',
    '^nil$|^none$',
    'suggest repeat|please repeat|repeat (if necessary|with fresh|sample|requested)',
    'sample too old|too old for analysis|old specimen',
    'no (clotted|labelled|edta|yellow top|suitable|serum)( | [a-z]+ )?(sample|specimen)( received)?',
    'un(labelled|labeled) (sample|specimen)',
    '(booking|registration|ordering) error|error in ordering',
    'wrong patient (bled|sample)|patient mis',
    'amended report|disregard|please ignore|please note',
    'specimen leaked|leaked in transit|unable to perform',
    'inconvenience caused|we apologise|regret',
    '^(test|sample|analysis|required|result|for analysis)\\.?$',
    'analy[sz]ed at|sent (to|away)|performed (at|by) (the )?(pru|tdl|doctors|reference|sheffield|king)',
    'report from (same|the)|see .* report (from|dated)|refer to (comment|result) dated',
]

# fusion thresholds bare numeric; regex SQL literals = repr(sql_str(regex)): backslashes doubled for Spark
# SQL RLIKE + repr for Python-parse safety.
_FC_COS_STRONG   = 0.65
_FC_COS_WEAK     = 0.45
_FC_ANCHOR_FLOOR = 0.45
_FC_ANCHOR_MARGIN= 0.01
_FC_TIE_MARGIN   = 0.02
_FC_CONFIRM_BONUS= 0.05
_FC_BOOST_BONUS  = 0.03
_FC_ABSTAIN_RE_SQL   = "'^(culture|antigen|specimen|sample|microscopy|serology|pcr|histology|cytology|biochemistry|haematology|virology|mc&s|m,c&s|m c s)\\\\s*[.:]?$|^(culture|specimen|sample|microscopy|serology|pcr|test|assay)\\\\s+(complete|completed|received|processed|performed|requested|added|sent|done|awaited|pending|to follow)\\\\b'"
_FC_ADVISORY_RE_SQL  = "'^should not be based on|may represent contamination of the sample|please interpret in the context|negative predictive value'"


# ── Rerouter seams (dbutils.notebook.run) — overridable so a harness/fixture can inject ─────────────
_RR_FLOOR_VALUE, _RR_MARGIN_VALUE, _RR_DELTA_VALUE = 0.63, 0.02, 0.04  # rerouter A2 thresholds (baked)
def _reroute_openset(tbl):
    """Inlined pathology_omop_openset_rerouter — deterministic router over the stored cosines in `tbl`,
    overwriting it in place with route/final_*/tier. Config from the baked _ROUTE_CFG/_EXCL_PATTERNS
    (was spark.table(_openset_routing_config)/exclusions_dev); anchors from concept_result_anchors (data)."""
    import re, json
    from pyspark.sql import functions as F
    cfg = _ROUTE_CFG
    NEG=re.compile(cfg["NEG_RE"]); POS=re.compile(cfg["POS_RE"]); MORPH=re.compile(cfg["MORPH_RE"])
    JUNK=re.compile(cfg["JUNK_RE"]); JUNK_MAX=int(cfg["JUNK_MAXLEN"]); COMPOUND=re.compile(cfg["COMPOUND_RE"]); CLEAN_MAX=int(cfg["CLEAN_NEG_MAXLEN"])
    FORCED=[(re.compile(p),l) for p,l in json.loads(cfg["FORCED_ANCHOR_MAP"])]
    FRAGMENT=re.compile(cfg["FRAGMENT_RE"]) if "FRAGMENT_RE" in cfg else None
    ORG_BLOCK=re.compile(cfg["ORG_BLOCK_RE"]) if "ORG_BLOCK_RE" in cfg else None
    CLEAN_NEG_BLOCK=re.compile(cfg["CLEAN_NEG_BLOCK_RE"]) if "CLEAN_NEG_BLOCK_RE" in cfg else None
    DENY=set(json.loads(cfg["WRONG_SPECIES_DENY"])) if "WRONG_SPECIES_DENY" in cfg else set()
    GENERIC_POS=re.compile(cfg["GENERIC_POS_RE"]) if "GENERIC_POS_RE" in cfg else None
    HAS_ORG_TOKEN=re.compile(cfg["HAS_ORGANISM_TOKEN_RE"]) if "HAS_ORGANISM_TOKEN_RE" in cfg else None
    GRADE=re.compile(cfg["GRADE_RE"]) if "GRADE_RE" in cfg else None
    SUSPECTED=re.compile(cfg["SUSPECTED_RE"]) if "SUSPECTED_RE" in cfg else None
    ISOLATE_POS=re.compile(cfg["ISOLATE_POS_RE"]) if "ISOLATE_POS_RE" in cfg else None
    GROWTH_CID=36032835; ORGANISM_CID=4259632
    POS_OVERRIDE=[(re.compile(p),int(cid),nm) for p,cid,nm in json.loads(cfg["POSITIVE_OVERRIDE_MAP"])] if "POSITIVE_OVERRIDE_MAP" in cfg else []
    def pos_override(s):
        for pat,cid,nm in POS_OVERRIDE:
            if pat.search(s): return cid,nm
        return None
    def grade_and_suspect(s):
        g=None
        if GRADE is not None:
            m=GRADE.search(s); g=m.group(0) if m else None
        susp=bool(s.rstrip().endswith("?")) or bool(SUSPECTED.search(s)) if SUSPECTED else bool(s.rstrip().endswith("?"))
        return susp,g
    EXCL=re.compile("|".join(f"(?:{p})" for p in _EXCL_PATTERNS)) if _EXCL_PATTERNS else None
    ameta={r["anchor_label"]:(int(r["cid"]),r["polarity_class"]) for r in spark.sql(
      f"SELECT DISTINCT anchor_label, polarity_class, COALESCE(snomed_concept_id,loinc_concept_id) cid FROM {ANCHOR_TABLE} WHERE is_active=true AND COALESCE(snomed_concept_id,loinc_concept_id) IS NOT NULL").collect()}
    def intent(s):
        st=MORPH.sub(" ",s)
        if NEG.search(st):
            f="negative"
            for pat,lab in FORCED:
                if pat.search(s): f=lab; break
            return ("negative",f,(len(s)<=CLEAN_MAX) and (COMPOUND.search(s) is None))
        if POS.search(st): return ("positive",None,False)
        return ("neutral",None,False)
    rows=spark.table(tbl).collect()
    out=[]
    for r in rows:
        rn=r["result_normalized"]
        a_lab=r["anchor_label"]; a_pol=r["anchor_polarity"]; a_cid=r["anchor_concept_id"]
        a1=float(r["anchor_top1"] or 0); a2=float(r["anchor_top2"] or 0); am=float(r["anchor_margin"] or 0)
        o_cid=r["org_concept_id"]; o_name=r["org_concept_name"]
        o1=float(r["org_top1"] or 0); o2=float(r["org_top2"] or 0); om=float(r["org_margin"] or 0)
        isj=bool(len(rn)<=JUNK_MAX or JUNK.search(rn)); ise=bool(EXCL.search(rn)) if EXCL else False
        frag_blocked=bool(FRAGMENT.search(rn)) if FRAGMENT else False
        org_blocked=bool(ORG_BLOCK.search(rn)) if ORG_BLOCK else False
        cn_blocked=bool(CLEAN_NEG_BLOCK.search(rn)) if CLEAN_NEG_BLOCK else False
        deny_hit=(o_cid in DENY) if o_cid is not None else False
        intn,forced,clean=intent(rn)
        is_suspected,growth_grade=grade_and_suspect(rn)
        has_org_tok=bool(HAS_ORG_TOKEN.search(rn)) if HAS_ORG_TOKEN else False
        ovr=pos_override(rn) if (intn!="negative") else None
        act="none"; fcid=None; fname=None; ftype=None; win=None
        if isj or ise: route="excluded" if ise else "none"
        elif frag_blocked: route="none"
        elif cn_blocked: route="none"
        elif intn=="negative" and clean and (forced in ameta):
            route="anchor"; win="anchor"; act="clean_negation_forced"; fcid,fname,ftype=ameta[forced][0],forced,"generic_qualifier"
        elif ovr is not None:
            route="organism"; win="positive_override"; act="positive_override"; fcid,fname,ftype=ovr[0],ovr[1],"organism"
        elif (not org_blocked) and (not deny_hit) and (o1-a1)>=_RR_DELTA_VALUE and o1>=_RR_FLOOR_VALUE and om>=_RR_MARGIN_VALUE and o_cid is not None:
            route="organism"
        elif (intn!="negative") and (GENERIC_POS is not None) and GENERIC_POS.search(rn) and not has_org_tok:
            if ISOLATE_POS is not None and ISOLATE_POS.search(rn):
                route="generic_positive"; win="generic_positive"; act="generic_pos_isolate"; fcid,fname,ftype=ORGANISM_CID,"organism","generic_positive"
            else:
                route="generic_positive"; win="generic_positive"; act="generic_pos_growth"; fcid,fname,ftype=GROWTH_CID,"growth","generic_positive"
        elif a1>=0.60 and am>=0.05: route="anchor"
        else: route="none"
        if win is None:
            win=route
            if route=="anchor":
                if (a_pol=="negative" and intn=="positive") or (a_pol=="positive" and intn=="negative"):
                    act="forced_unmap"; route="none"; win="none"
                else: fcid,fname,ftype=a_cid,a_lab,"generic_qualifier"
            elif route=="organism":
                if intn=="negative":
                    act="forced_to_neg_anchor"; fa=forced or "negative"
                    if fa in ameta: fcid,fname,ftype=ameta[fa][0],fa,"generic_qualifier"; win="anchor"
                    else: route="none"; win="none"
                else: fcid,fname,ftype=o_cid,o_name,"organism"
        tier=("excluded" if route=="excluded" else "auto_anchor" if ftype=="generic_qualifier"
              else "auto_value" if ftype=="organism" else "auto_genpos" if ftype=="generic_positive" else "unmapped")
        out.append((rn, int(r["freq"]), a_lab, a_pol, (int(a_cid) if a_cid is not None else None),
                    builtins.round(a1,6),builtins.round(a2,6),builtins.round(am,6),
                    (int(o_cid) if o_cid is not None else None), o_name, builtins.round(o1,6),builtins.round(o2,6),builtins.round(om,6), builtins.round(o1-a1,6),
                    isj, ise, clean, intn, forced, act, win, route,
                    (int(fcid) if fcid is not None else None), fname, ftype, tier, is_suspected, growth_grade))
    SCHEMA=("result_normalized STRING, freq BIGINT, anchor_label STRING, anchor_polarity STRING, anchor_concept_id BIGINT, "
            "anchor_top1 DOUBLE, anchor_top2 DOUBLE, anchor_margin DOUBLE, org_concept_id BIGINT, org_concept_name STRING, "
            "org_top1 DOUBLE, org_top2 DOUBLE, org_margin DOUBLE, cross_pool_margin DOUBLE, is_junk BOOLEAN, is_excluded BOOLEAN, "
            "is_clean_negation BOOLEAN, polarity_intent STRING, forced_anchor_label STRING, polarity_action STRING, "
            "winning_pool STRING, route STRING, final_value_as_concept_id BIGINT, final_concept_name STRING, final_type STRING, "
            "final_tier STRING, is_suspected BOOLEAN, growth_grade STRING")
    spark.createDataFrame(out, SCHEMA).withColumn("scored_at", F.current_timestamp()) \
         .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(tbl)

def _fuse_substrate(in_lookup, out_lookup):
    """Inlined pathology_omop_openset_fusion_rerouter — embedding-primary anchor polarity re-fuse.
    _fusion_cfg values baked as SQL literals (no CROSS JOIN); ABSTAIN/ADVISORY regexes via _FC_*_RE_SQL."""
    spark.sql(f"""
    CREATE OR REPLACE TABLE {out_lookup} AS
    SELECT
      result_normalized, freq, anchor_label, anchor_polarity, anchor_concept_id,
      anchor_top1, anchor_top2, anchor_margin, org_concept_id, org_concept_name,
      org_top1, org_top2, org_margin, cross_pool_margin, is_junk, is_excluded,
      is_clean_negation, polarity_intent, forced_anchor_label,
      CASE WHEN preserve THEN polarity_action
           WHEN abstain_hit THEN 'fusion_abstain_fragment'
           WHEN anchor_accept AND cos_strong AND agree THEN 'fusion_confirm'
           WHEN anchor_accept AND cos_strong AND disagree THEN 'fusion_cosine_override'
           WHEN anchor_accept AND cos_strong AND polarity_intent='neutral' THEN 'fusion_cosine_solo'
           WHEN anchor_accept AND cos_mid AND agree THEN 'fusion_boost'
           ELSE 'fusion_silent' END AS polarity_action,
      winning_pool,
      CASE WHEN preserve THEN route WHEN abstain_hit THEN 'none' WHEN anchor_accept THEN 'anchor' ELSE 'none' END AS route,
      CASE WHEN preserve THEN final_value_as_concept_id WHEN abstain_hit THEN NULL WHEN anchor_accept THEN anchor_concept_id ELSE NULL END AS final_value_as_concept_id,
      CASE WHEN preserve THEN final_concept_name WHEN abstain_hit THEN NULL WHEN anchor_accept THEN anchor_label ELSE NULL END AS final_concept_name,
      CASE WHEN preserve THEN final_type WHEN abstain_hit THEN NULL WHEN anchor_accept THEN 'generic_qualifier' ELSE NULL END AS final_type,
      CASE WHEN preserve THEN final_tier WHEN abstain_hit THEN 'unmapped' WHEN anchor_accept THEN 'auto_anchor' ELSE 'unmapped' END AS final_tier,
      is_suspected, growth_grade,
      result_normalized AS stripped_result_normalized,
      CASE WHEN cos_strong THEN 'strong' WHEN cos_mid THEN 'mid' ELSE 'weak' END AS cos_band,
      CASE WHEN preserve THEN 'preserved'
           WHEN abstain_hit THEN 'abstain_fragment'
           WHEN anchor_accept AND cos_strong AND agree THEN 'confirm'
           WHEN anchor_accept AND cos_strong AND disagree THEN 'cosine_override'
           WHEN anchor_accept AND cos_strong AND polarity_intent='neutral' THEN 'cosine_solo'
           WHEN anchor_accept AND cos_mid AND agree THEN 'boost'
           ELSE 'silent' END AS regex_role,
      CASE WHEN abstain_hit THEN 'abstain_fragment' WHEN NOT preserve AND NOT anchor_accept THEN 'below_fusion_bar' END AS fusion_reason,
      CASE WHEN anchor_accept AND cos_strong AND agree THEN anchor_top1 + {_FC_CONFIRM_BONUS}
           WHEN anchor_accept AND cos_mid AND agree THEN anchor_top1 + {_FC_BOOST_BONUS}
           ELSE anchor_top1 END AS confidence_score,
      scored_at
    FROM (
      SELECT l.*,
        (l.route IN ('organism','generic_positive','excluded')
           OR l.polarity_action IN ('positive_override','generic_pos_isolate','generic_pos_growth','forced_to_neg_anchor')) AS preserve,
        (l.anchor_top1 >= {_FC_COS_STRONG} AND l.anchor_margin >= {_FC_TIE_MARGIN}) AS cos_strong,
        (l.anchor_top1 >= {_FC_COS_WEAK} AND l.anchor_top1 < {_FC_COS_STRONG} AND l.anchor_top1 >= {_FC_ANCHOR_FLOOR} AND l.anchor_margin >= {_FC_ANCHOR_MARGIN}) AS cos_mid,
        (l.polarity_intent IN ('negative','positive') AND l.anchor_polarity = l.polarity_intent) AS agree,
        (l.polarity_intent IN ('negative','positive') AND l.anchor_polarity IN ('negative','positive') AND l.anchor_polarity <> l.polarity_intent) AS disagree,
        ( ({_FC_ABSTAIN_RE_SQL} IS NOT NULL AND l.result_normalized RLIKE {_FC_ABSTAIN_RE_SQL} AND l.polarity_intent='neutral')
          OR ({_FC_ADVISORY_RE_SQL} IS NOT NULL AND l.result_normalized RLIKE {_FC_ADVISORY_RE_SQL}) ) AS abstain_hit,
        ((NOT (l.route IN ('organism','generic_positive','excluded')
            OR l.polarity_action IN ('positive_override','generic_pos_isolate','generic_pos_growth','forced_to_neg_anchor')))
          AND ( (l.anchor_top1 >= {_FC_COS_STRONG} AND l.anchor_margin >= {_FC_TIE_MARGIN} AND
                   ((l.polarity_intent IN ('negative','positive') AND l.anchor_polarity = l.polarity_intent)
                    OR (l.polarity_intent IN ('negative','positive') AND l.anchor_polarity IN ('negative','positive') AND l.anchor_polarity <> l.polarity_intent)
                    OR l.polarity_intent='neutral'))
                OR (l.anchor_top1 >= {_FC_COS_WEAK} AND l.anchor_top1 < {_FC_COS_STRONG} AND l.anchor_top1 >= {_FC_ANCHOR_FLOOR} AND l.anchor_margin >= {_FC_ANCHOR_MARGIN}
                    AND l.polarity_intent IN ('negative','positive') AND l.anchor_polarity = l.polarity_intent) )) AS anchor_accept
      FROM {in_lookup} l
    )
    """)


def _apply_gate_veto(fused_tbl):
    """§B. Cache-backed polarity veto on the net-new fused substrate, IN PLACE. Downgrade-only; no-op if
    GATE_ENABLED is False or the cache is absent (fail-open). Joins the cache POLE-ONLY via a pre-aggregated
    veto set (result_normalized, embedding_pole)."""
    if not GATE_ENABLED or not spark.catalog.tableExists(GATE_TABLE):
        return
    spark.sql(f"""
      CREATE OR REPLACE TABLE {fused_tbl} AS
      WITH veto AS (
        SELECT DISTINCT result_normalized, embedding_pole FROM {GATE_TABLE}
        WHERE verdict='veto' AND model_version={GATE_MODEL_VERSION!r} AND gate_version={GATE_VERSION}
      ),
      gated AS (
        SELECT l.*,
          (v.result_normalized IS NOT NULL AND l.route='anchor' AND l.final_tier<>'unmapped'
           AND l.anchor_polarity IN ('positive','negative')) AS is_vetoed
        FROM {fused_tbl} l
        LEFT JOIN veto v ON l.result_normalized=v.result_normalized AND l.anchor_polarity=v.embedding_pole
      )
      SELECT result_normalized, freq, anchor_label, anchor_polarity, anchor_concept_id,
        anchor_top1, anchor_top2, anchor_margin, org_concept_id, org_concept_name,
        org_top1, org_top2, org_margin, cross_pool_margin, is_junk, is_excluded,
        is_clean_negation, polarity_intent, forced_anchor_label,
        CASE WHEN is_vetoed THEN 'polarity_veto' ELSE polarity_action END AS polarity_action,
        winning_pool,
        CASE WHEN is_vetoed THEN 'none' ELSE route END AS route,
        CASE WHEN is_vetoed THEN CAST(NULL AS bigint) ELSE final_value_as_concept_id END AS final_value_as_concept_id,
        CASE WHEN is_vetoed THEN CAST(NULL AS string) ELSE final_concept_name END AS final_concept_name,
        CASE WHEN is_vetoed THEN CAST(NULL AS string) ELSE final_type END AS final_type,
        CASE WHEN is_vetoed THEN 'unmapped' ELSE final_tier END AS final_tier,
        is_suspected, growth_grade, stripped_result_normalized, cos_band,
        CASE WHEN is_vetoed THEN 'polarity_veto' ELSE regex_role END AS regex_role,
        CASE WHEN is_vetoed THEN 'nli_contradicts_pole' ELSE fusion_reason END AS fusion_reason,
        confidence_score, scored_at
      FROM gated
    """)

def _build_openset_substrate(dry_run=False):
    """Stage 3b-i. Score this run's distinct vector_ready RESULT keys against the SMALL open-set pools
    (anchor 76 surface forms, organism allow-subset ~260) via the serverless cross-join dot-product,
    producing the 29-col pathology_result_openset_lookup substrate. Regex/route columns are placeholder
    defaults — the rerouters (3b-ii/iii) re-derive + overwrite them from result_normalized. Text grain:
    scored on the grade-stripped score_text (COALESCE-fallback to the bare result_normalized vector, which
    Task-3's embed_text grain guarantees is in the store). Aborts above REMAP_MAX_SERVERLESS."""
    n_vr = spark.sql(
        f"SELECT COUNT(DISTINCT result_normalized) c FROM {QUEUE} "
        f"WHERE kind='result' AND status='vector_ready' AND result_normalized IS NOT NULL").first()["c"]
    if n_vr > REMAP_MAX_SERVERLESS:
        raise RuntimeError(
            f"_build_openset_substrate: {n_vr:,} distinct vector_ready result keys exceed "
            f"REMAP_MAX_SERVERLESS={REMAP_MAX_SERVERLESS:,}. Run the bulk cluster build "
            f"(pathology_omop_02d_openset_result_cluster), not the serverless cross-join.")
    if dry_run:
        return SUBSTRATE
    KEYS = f"{EMB_MAP_SCHEMA}._tmp_substrate_keys"
    QV   = f"{EMB_MAP_SCHEMA}._tmp_substrate_qv"
    AC   = f"{EMB_MAP_SCHEMA}._tmp_substrate_anchor"
    OCt  = f"{EMB_MAP_SCHEMA}._tmp_substrate_org"
    # 1) distinct keys + grade-stripped score_text
    spark.sql(f"""
      CREATE OR REPLACE TABLE {KEYS} USING DELTA AS
      SELECT DISTINCT result_normalized,
             TRIM(REGEXP_REPLACE(result_normalized, r'{GRADE_STRIP_RE}', ' ')) AS score_text,
             CAST(1 AS BIGINT) AS freq
      FROM {QUEUE}
      WHERE kind='result' AND status='vector_ready' AND result_normalized IS NOT NULL""")
    # 2) query vector: score_text first, fallback to result_normalized
    spark.sql(f"""
      CREATE OR REPLACE TABLE {QV} USING DELTA AS
      SELECT k.result_normalized, k.freq,
             COALESCE(es.embedding_vector, er.embedding_vector) AS qvec
      FROM {KEYS} k
      LEFT JOIN {EMBEDDINGS_TABLE} es ON LOWER(es.term)=k.score_text        AND es.embedding_vector IS NOT NULL
      LEFT JOIN {EMBEDDINGS_TABLE} er ON LOWER(er.term)=k.result_normalized AND er.embedding_vector IS NOT NULL
      WHERE COALESCE(es.embedding_vector, er.embedding_vector) IS NOT NULL""")
    # 3) anchor cosines (per-concept max-pool -> top1/top2 across concepts)
    spark.sql(f"""
      CREATE OR REPLACE TABLE {AC} USING DELTA AS
      WITH a AS (
        SELECT anchor_label, polarity_class,
               COALESCE(snomed_concept_id,loinc_concept_id) AS cid, LOWER(surface_form) AS sf
        FROM {ANCHOR_TABLE} WHERE is_active=true AND COALESCE(snomed_concept_id,loinc_concept_id) IS NOT NULL),
      av AS (SELECT a.anchor_label, a.polarity_class, a.cid, e.embedding_vector AS cvec
             FROM a JOIN {EMBEDDINGS_TABLE} e ON LOWER(e.term)=a.sf AND e.embedding_vector IS NOT NULL),
      sims AS (
        SELECT q.result_normalized, av.anchor_label, av.polarity_class, av.cid,
               MAX(aggregate(zip_with(q.qvec, av.cvec, (x,y)->x*y), CAST(0.0 AS DOUBLE), (s,x)->s+x)) AS sim
        FROM {QV} q CROSS JOIN av GROUP BY q.result_normalized, av.anchor_label, av.polarity_class, av.cid),
      ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY result_normalized ORDER BY sim DESC) rn FROM sims)
      SELECT r1.result_normalized, r1.anchor_label, r1.polarity_class AS anchor_polarity,
             r1.cid AS anchor_concept_id, r1.sim AS anchor_top1, COALESCE(r2.sim,0.0) AS anchor_top2,
             r1.sim - COALESCE(r2.sim,0.0) AS anchor_margin
      FROM (SELECT * FROM ranked WHERE rn=1) r1
      LEFT JOIN (SELECT result_normalized, sim FROM ranked WHERE rn=2) r2 USING (result_normalized)""")
    # 4) organism cosines (allow-subset; per-concept max-pool)
    spark.sql(f"""
      CREATE OR REPLACE TABLE {OCt} USING DELTA AS
      WITH ov AS (
        SELECT v.concept_id, v.concept_name, v.embedding_vector AS cvec
        FROM {ORG_VECS} v JOIN {ORG_ALLOW} al ON al.concept_id=v.concept_id
        WHERE v.embedding_vector IS NOT NULL),
      sims AS (
        SELECT q.result_normalized, ov.concept_id, ov.concept_name,
               MAX(aggregate(zip_with(q.qvec, ov.cvec, (x,y)->x*y), CAST(0.0 AS DOUBLE), (s,x)->s+x)) AS sim
        FROM {QV} q CROSS JOIN ov GROUP BY q.result_normalized, ov.concept_id, ov.concept_name),
      ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY result_normalized ORDER BY sim DESC) rn FROM sims)
      SELECT r1.result_normalized, r1.concept_id AS org_concept_id, r1.concept_name AS org_concept_name,
             r1.sim AS org_top1, COALESCE(r2.sim,0.0) AS org_top2, r1.sim-COALESCE(r2.sim,0.0) AS org_margin
      FROM (SELECT * FROM ranked WHERE rn=1) r1
      LEFT JOIN (SELECT result_normalized, sim FROM ranked WHERE rn=2) r2 USING (result_normalized)""")
    # 5) assemble the 29-col substrate (regex/route cols = placeholder defaults; rerouter overwrites)
    spark.sql(f"""
      CREATE OR REPLACE TABLE {SUBSTRATE} USING DELTA AS
      SELECT k.result_normalized, k.freq,
             a.anchor_label, a.anchor_polarity, a.anchor_concept_id,
             COALESCE(a.anchor_top1,0.0) anchor_top1, COALESCE(a.anchor_top2,0.0) anchor_top2,
             COALESCE(a.anchor_margin,0.0) anchor_margin,
             o.org_concept_id, o.org_concept_name,
             COALESCE(o.org_top1,0.0) org_top1, COALESCE(o.org_top2,0.0) org_top2,
             COALESCE(o.org_margin,0.0) org_margin,
             COALESCE(o.org_top1,0.0)-COALESCE(a.anchor_top1,0.0) AS cross_pool_margin,
             false AS is_junk, false AS is_excluded, false AS is_clean_negation,
             'neutral' AS polarity_intent, CAST(NULL AS STRING) AS forced_anchor_label,
             CAST(NULL AS STRING) AS polarity_action, CAST(NULL AS STRING) AS winning_pool,
             'none' AS route, CAST(NULL AS BIGINT) AS final_value_as_concept_id,
             CAST(NULL AS STRING) AS final_concept_name, CAST(NULL AS STRING) AS final_type,
             'unmapped' AS final_tier, false AS is_suspected, CAST(NULL AS STRING) AS growth_grade,
             current_timestamp() AS scored_at
      FROM {KEYS} k
      LEFT JOIN {AC}  a USING (result_normalized)
      LEFT JOIN {OCt} o USING (result_normalized)""")
    for t in (KEYS, QV, AC, OCt):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    return SUBSTRATE

def _score_loinc_answers(dry_run=False):
    """Stage 3b LOINC arm (4-tuple grain). For each vector_ready result key whose 4-tuple's test maps to a
    test_concept_id with an answer list, score the result ONLY against that test's allowed answer concepts
    (answer-list-constrained, like the bulk LOINC arm). source='embedding_loinc_answer', tier='auto_value'."""
    if dry_run:
        return LOINC_PROP
    spark.sql(f"""
      CREATE OR REPLACE TABLE {LOINC_PROP} USING DELTA AS
      WITH q AS (
        SELECT DISTINCT eq.code_system, eq.code, eq.description, eq.result_normalized
        FROM {QUEUE} eq WHERE eq.kind='result' AND eq.status='vector_ready' AND eq.result_normalized IS NOT NULL),
      qv AS (
        SELECT q.*, e.embedding_vector AS qvec, tm.measurement_concept_id AS test_cid
        FROM q JOIN {EMBEDDINGS_TABLE} e ON LOWER(e.term)=q.result_normalized AND e.embedding_vector IS NOT NULL
        JOIN {EMB_TEST_MAP} tm ON tm.code_system=q.code_system AND tm.code=q.code AND tm.description=q.description
          AND tm.confidence_tier IN ('curated','auto_high','auto_low') AND tm.measurement_concept_id IS NOT NULL),
      ans AS (
        SELECT al.test_concept_id, al.answer_concept_id, ci.concept_name, ci.vocabulary_id, e.embedding_vector AS cvec
        FROM {ANSWER_LISTS} al
        JOIN {RESULT_INDEX} ci ON ci.concept_id=al.answer_concept_id
        JOIN {EMBEDDINGS_TABLE} e ON LOWER(e.term)=LOWER(ci.concept_name) AND e.embedding_vector IS NOT NULL),
      sims AS (
        SELECT qv.code_system, qv.code, qv.description, qv.result_normalized,
               ans.answer_concept_id, ans.concept_name, ans.vocabulary_id,
               aggregate(zip_with(qv.qvec, ans.cvec, (x,y)->x*y), CAST(0.0 AS DOUBLE), (s,x)->s+x) AS sim
        FROM qv JOIN ans ON ans.test_concept_id = qv.test_cid),
      ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY code_system,code,description,result_normalized ORDER BY sim DESC) rn,
               (sim - LEAD(sim) OVER (PARTITION BY code_system,code,description,result_normalized ORDER BY sim DESC)) AS marg
        FROM sims)
      SELECT code_system, code, description, result_normalized,
             answer_concept_id AS value_as_concept_id, vocabulary_id AS concept_vocabulary_id, concept_name,
             'embedding_loinc_answer' AS result_mapping_source, sim AS similarity_score, 'auto_value' AS confidence_tier
      FROM ranked
      WHERE rn=1 AND sim >= {FLOOR_LOINC} AND COALESCE(marg,1.0) >= {MARGIN_LOINC}""")
    return LOINC_PROP

# byte-identical to pathology_omop_finding_decode.norm_expr (dual-maintenance hazard — keep in sync)
def _finding_norm_expr(col):
    return (f"regexp_replace(regexp_replace(regexp_replace(lower(trim({col})), "
            f"'\\\\.br\\\\|[\\n/]', ' '), "
            f"'see (further )?guidance.*|http\\\\S+', ' '), "
            f"'\\\\s+', ' ')")
_FIND_ADVISORY_RE = (r"film (hould|should) be con ?idered|(hould|should) be con ?idered if|(hould|should) be interpreted|"
                     r"interpretation for|hba1c interpretation|\\blimitation|"
                     r"plea ?e (end|send|refer|mea ?ure|di ?cu)|please (send|refer|measure|discuss)|"
                     r"particularly rel|\\bif .{0,25}(unexplained|new|per ?i ?t|persist)|"
                     r"^\\?| \\? ?(aki|ckd|viral|racial|ida|therapy)")
_FIND_ADVISORY_KEEP_RE = r"warning,.{0,40}if clinically indicated"
_FIND_NEG_CUE = r'(\\bno\\b|\\bnot\\b|\\bnil\\b|absent|negative for|free of)'

def _decode_value_findings(dry_run=False):
    """Stage 3b-iv. Deterministic FLAG_DICT decode over this run's net-new result keys (NO cosine).
    VALUE: morphology tokens -> value_as_concept_id (auto_value, source flag_decode). FINDING: finding
    tokens -> result_finding_* + suborder (finding axis only; NEVER value_as_concept_id). Logic ported
    from pathology_omop_morphology_normalizer (Cell 2 token explode) + pathology_omop_finding_decode."""
    if dry_run:
        return VALUE_PROP, FINDING_PROP
    from pyspark.sql import functions as _F
    from pyspark.sql.window import Window as _W
    # ---- VALUE (morphology): explode tokens on separators, exact-match the dict ----
    spark.sql(f"""
      CREATE OR REPLACE TABLE {VALUE_PROP} USING DELTA AS
      WITH ks AS (SELECT DISTINCT result_normalized FROM {QUEUE}
                  WHERE kind='result' AND status='vector_ready' AND result_normalized IS NOT NULL),
      clean AS (
        SELECT result_normalized,
               explode(split(regexp_replace(result_normalized, r'\\\\.br\\\\|[\\n/]', ' '), r'[\\s,;]+')) AS tok
        FROM ks),
      m AS (SELECT c.result_normalized, d.value_concept_id, d.concept_name
            FROM clean c JOIN {MORPH_DICT} d ON c.tok = d.flag_token)
      SELECT result_normalized, value_concept_id AS value_as_concept_id, concept_name,
             'flag_decode' AS result_mapping_source, 'auto_value' AS confidence_tier,
             CAST(1.0 AS DOUBLE) AS similarity_score
      FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY result_normalized ORDER BY value_concept_id) rn FROM m)
      WHERE rn=1""")
    # ---- FINDING: norm + advisory drop + dict rlike + finding-adjacent neg guard + suppression + suborder
    keys = spark.sql(f"SELECT DISTINCT result_normalized FROM {QUEUE} "
                     f"WHERE kind='result' AND status='vector_ready' AND result_normalized IS NOT NULL")
    src = (keys.withColumnRenamed("result_normalized","rn_raw")
              .withColumn("result_normalized", _F.expr(_finding_norm_expr("rn_raw")))
              .filter(~(_F.col("result_normalized").rlike(_FIND_ADVISORY_RE)
                        & ~_F.col("result_normalized").rlike(_FIND_ADVISORY_KEEP_RE))))
    dict_df = spark.table(FINDING_DICT)
    # cross-join string x dict; match via Spark-3.5 col-arg rlike (VERBATIM from pathology_omop_finding_decode)
    matched = (src.crossJoin(_F.broadcast(dict_df))
                  .filter(_F.col("result_normalized").rlike(_F.col("token_regex"))))
    # Adjacency neg guard: per-concept driver-collected boolean -- F.when(concept==cid, rlike(NEG_CUE.{0,25}?(?:tok)))
    # (VERBATIM from the bulk decoder; a single generic concat(...) regex is NOT equivalent on multi-finding strings).
    dict_rows = dict_df.select("finding_concept_id", "token_regex").collect()
    neg_col = _F.lit(False)
    for _r in dict_rows:
        _cid = _r["finding_concept_id"]; _tok = _r["token_regex"]
        _pat = f"{_FIND_NEG_CUE}.{{0,25}}?(?:{_tok})"
        neg_col = _F.when(_F.col("finding_concept_id") == _F.lit(_cid),
                          _F.col("result_normalized").rlike(_pat)).otherwise(neg_col)
    matched = matched.withColumn("neg_here", neg_col)
    kept = matched.filter(~_F.col("neg_here")).select(
        "result_normalized","finding_concept_id","concept_name","domain",
        "suppression_group","specificity_rank","priority")
    w_grp = _W.partitionBy("result_normalized","suppression_group")
    sup = (kept.withColumn("mx", _F.max("specificity_rank").over(w_grp))
               .filter(_F.col("specificity_rank")==_F.col("mx")).drop("mx","specificity_rank"))
    w_str = _W.partitionBy("result_normalized").orderBy("priority","finding_concept_id")
    (sup.withColumn("result_finding_suborder", _F.row_number().over(w_str)-1)
        .select(_F.col("result_normalized"),
                _F.col("finding_concept_id").alias("result_finding_concept_id"),
                _F.col("concept_name").alias("result_finding_concept_name"),
                _F.col("domain").alias("result_finding_domain"),
                "result_finding_suborder")
        .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(FINDING_PROP))
    return VALUE_PROP, FINDING_PROP

def _firewall_merge_result(src_tbl):
    """Firewall MERGE of an arm proposal (10-col schema) into EMB_RESULT_MAP. Fills only currently-unmapped
    4-tuples (confidence_tier='unmapped' — curated/auto_high/auto_low and already-filled open-set rows are
    NEVER overwritten -> first-writer wins = bulk precedence). Inserts genuinely new 4-tuples. Explicit
    insert column set matching the dev map's 13 columns (NO is_suspected/growth_grade — clone-only)."""
    src = (spark.table(src_tbl)
           .withColumn("mapping_version", F.lit(1))
           .withColumn("mapped_at", F.current_timestamp()).withColumn("ADC_UPDT", F.current_timestamp()))
    if len(src.take(1)) == 0:
        return
    (DeltaTable.forName(spark, EMB_RESULT_MAP).alias("t").merge(src.alias("s"),
        "t.code_system=s.code_system AND t.code=s.code AND t.description=s.description "
        "AND t.result_normalized=s.result_normalized")
      .whenMatchedUpdate(condition="t.confidence_tier='unmapped'", set={
        "value_as_concept_id":"s.value_as_concept_id","concept_vocabulary_id":"s.concept_vocabulary_id",
        "concept_name":"s.concept_name","result_mapping_source":"s.result_mapping_source",
        "similarity_score":"s.similarity_score","confidence_tier":"s.confidence_tier",
        "mapping_version":"t.mapping_version + 1","mapped_at":"current_timestamp()","ADC_UPDT":"current_timestamp()"})
      .whenNotMatchedInsert(values={
        "code_system":"s.code_system","code":"s.code","description":"s.description",
        "result_normalized":"s.result_normalized","value_as_concept_id":"s.value_as_concept_id",
        "concept_vocabulary_id":"s.concept_vocabulary_id","concept_name":"s.concept_name",
        "result_mapping_source":"s.result_mapping_source","similarity_score":"s.similarity_score",
        "confidence_tier":"s.confidence_tier",
        "mapping_version":"s.mapping_version","mapped_at":"s.mapped_at","ADC_UPDT":"s.ADC_UPDT"}).execute())

def _compose_and_merge_result(fused, loinc_prop, value_prop):
    """Stage 3b-v. Compose the result-value arms into EMB_RESULT_MAP in the bulk integrate order (each MERGE
    fills only still-unmapped 4-tuples -> anchor wins over LOINC over value, first-writer = bulk precedence):
      1. anchor-fusion / organism / generic-positive (from the fused substrate, bulk ACCEPTED_T gate)
      2. LOINC-answer
      3. value/morphology
    Text-grain arms (1,3) fanned over this run's distinct 4-tuples; LOINC (2) is already 4-tuple grain."""
    KEYS4 = f"{EMB_MAP_SCHEMA}._tmp_compose_keys"
    arm1  = f"{EMB_MAP_SCHEMA}._tmp_arm1"
    arm3  = f"{EMB_MAP_SCHEMA}._tmp_arm3"
    spark.sql(f"""CREATE OR REPLACE TABLE {KEYS4} USING DELTA AS
      SELECT DISTINCT code_system, code, description, result_normalized FROM {QUEUE}
      WHERE kind='result' AND status='vector_ready' AND result_normalized IS NOT NULL""")
    # arm 1: fused substrate. ACCEPTANCE GATE COPIED from pathology_omop_openset_map_integrate ACCEPTED_T:
    # final_value_as_concept_id NOT NULL, NOT is_junk, NOT is_excluded, AND route-specific (anchor|genpos|
    # organism with polarity_intent<>'negative' + floor/margin/delta). Without this the loop would promote
    # organism rows the bulk integrate rejects -> breaks parity.
    spark.sql(f"""CREATE OR REPLACE TABLE {arm1} USING DELTA AS
      SELECT k.code_system, k.code, k.description, k.result_normalized,
             s.final_value_as_concept_id AS value_as_concept_id,
             c.vocabulary_id AS concept_vocabulary_id, s.final_concept_name AS concept_name,
             CASE WHEN s.route='organism' THEN 'embedding_organism'
                  WHEN s.route='generic_positive' THEN 'deterministic_generic_positive'
                  ELSE 'embedding_anchor' END AS result_mapping_source,
             CAST(CASE WHEN s.route='organism' THEN s.org_top1
                       WHEN s.route='generic_positive' THEN 1.0
                       ELSE s.anchor_top1 END AS DOUBLE) AS similarity_score,
             s.final_tier AS confidence_tier
      FROM {KEYS4} k JOIN {fused} s USING (result_normalized)
      LEFT JOIN {EMB_CONCEPT} c ON c.concept_id = s.final_value_as_concept_id
      WHERE s.final_value_as_concept_id IS NOT NULL
        AND NOT s.is_junk AND NOT s.is_excluded
        AND ( s.route='anchor' OR s.route='generic_positive'
              OR ( s.route='organism' AND s.polarity_intent <> 'negative'
                   AND s.org_top1 >= {OS_FLOOR_VALUE} AND s.org_margin >= {OS_MARGIN_VALUE}
                   AND (s.org_top1 - s.anchor_top1) >= {OS_DELTA_VALUE} ) )""")
    _firewall_merge_result(arm1)
    # arm 2: LOINC (already 4-tuple grain)
    _firewall_merge_result(loinc_prop)
    # arm 3: value/morphology fanned over 4-tuples
    spark.sql(f"""CREATE OR REPLACE TABLE {arm3} USING DELTA AS
      SELECT k.code_system, k.code, k.description, k.result_normalized,
             v.value_as_concept_id, c.vocabulary_id AS concept_vocabulary_id, v.concept_name,
             v.result_mapping_source, v.similarity_score, v.confidence_tier
      FROM {KEYS4} k JOIN {VALUE_PROP} v USING (result_normalized)
      LEFT JOIN {EMB_CONCEPT} c ON c.concept_id = v.value_as_concept_id""")
    _firewall_merge_result(arm3)
    for t in (KEYS4, arm1, arm3):
        spark.sql(f"DROP TABLE IF EXISTS {t}")

def _remap_result(dry_run=False):
    """Stage 3b (REBUILT 2026-06-30 — open-set + fusion parity). Score net-new result keys against the
    open-set pools (serverless substrate), route+fuse via the canonical rerouters, decode value/finding
    FLAG_DICTs, compose into EMB_RESULT_MAP with the bulk three-MERGE firewall precedence, then flip
    vector_ready->done (keyed on the lossless result_normalized). Returns the finding proposal table name
    for the finding backfill (Stage 5), or None on an empty run. The legacy nearest-context arm is RETIRED."""
    n_vr = spark.sql(f"SELECT COUNT(*) c FROM {QUEUE} WHERE kind='result' AND status='vector_ready'").first()["c"]
    if dry_run:
        return n_vr
    if n_vr == 0:
        # Empty run: return None so apply_finding_to_map_pathology cannot re-apply a stale prior proposal.
        return None
    substrate = _build_openset_substrate(dry_run=False)
    _reroute_openset(substrate)                       # inlined router (was _run_openset_rerouter)
    _fuse_substrate(substrate, SUBSTRATE_FUSED)        # inlined fusion (was _run_fusion_rerouter)
    _apply_gate_veto(SUBSTRATE_FUSED)                  # §B polarity veto (cache-backed; no-op if cache absent)
    loinc_prop = _score_loinc_answers(dry_run=False)
    value_prop, finding_prop = _decode_value_findings(dry_run=False)
    _compose_and_merge_result(SUBSTRATE_FUSED, loinc_prop, value_prop)
    spark.sql(f"UPDATE {QUEUE} SET status='done' WHERE kind='result' AND status='vector_ready'")
    for t in (SUBSTRATE, SUBSTRATE_FUSED, LOINC_PROP, VALUE_PROP):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    return finding_prop

# COMMAND ----------

def remap_keys(dry_run=False):
    """Stage 3 wrapper — remap vector_ready test + result keys into the concept maps. _remap_test is the
    serverless cross-join test arm (unchanged); _remap_result is the REBUILT Stage 3b open-set+fusion arm
    (returns the finding proposal table for the Stage-5 finding backfill, or None). The backstop done-flip
    covers any residual vector_ready test rows (_remap_result already flips its own result rows to done;
    a backstop result-flip would strand the finding proposal's keys, so the sweep is TEST-scoped now)."""
    _emb_ensure_infrastructure()   # folded: lazy, idempotent, replaces the import-time setup
    n_test = _remap_test(dry_run)
    finding_prop = _remap_result(dry_run)
    if not dry_run:
        spark.sql(f"UPDATE {QUEUE} SET status='done' WHERE status='vector_ready' AND kind='test'")
    return {"n_test_remapped": n_test, "n_result_remapped": (finding_prop if dry_run else "done"),
            "finding_prop": (None if dry_run else finding_prop)}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 — Stage 4: classify_map_delta (+ pathology_map_baseline)
# MAGIC Diffs current maps vs the last-reconciled baseline → ADDITIVE (NULL→concept) vs CORRECTION
# MAGIC (concept changed). snapshot_baseline() has a TOCTOU guard: it refuses to snapshot while
# MAGIC rebuild_flagged=TRUE (a late external correction must not be baked into the baseline).

# COMMAND ----------

BASELINE = f"{EMB_MAP_SCHEMA}.pathology_map_baseline"
FINDING_BASELINE = f"{EMB_MAP_SCHEMA}.pathology_finding_baseline"  # finding-axis reconciliation baseline
def snapshot_baseline(force=False):
    """Persist the current (map, key, concept_id) set as the reconciliation baseline. Called by
    apply_to_map_pathology on the additive path AND by the map_pathology FULL_REBUILD (force=True).
    TOCTOU guard: refuse to snapshot while a correction is outstanding — otherwise a concurrent/late
    external correction could be folded into the baseline and become permanently invisible to
    classify_map_delta. Only FULL_REBUILD (which reconciles the whole table) may force.

    The rebuild flag is read from the Map-Pipeline-owned MP_REBUILD_FLAG table (single-row
    pathology_map_rebuild_flag, which replaced map_pathology_state). A missing table/row is treated
    as FALSE (no correction outstanding) so the snapshot proceeds — the Map Pipeline section creates
    the flag table before the weekly job runs, so a missing table only happens in isolated/first-run
    contexts where there is by definition no outstanding correction."""
    if not force:
        if not spark.catalog.tableExists(MP_REBUILD_FLAG):
            flag = False
        else:
            st = spark.sql(f"SELECT rebuild_flagged FROM {MP_REBUILD_FLAG} WHERE id=1").first()
            flag = bool(st["rebuild_flagged"]) if (st is not None and st["rebuild_flagged"] is not None) else False
        if flag:
            print("snapshot_baseline SKIPPED: rebuild_flagged=TRUE (correction outstanding; await FULL_REBUILD).")
            return
    spark.sql(f"""
      CREATE OR REPLACE TABLE {BASELINE} USING DELTA AS
      SELECT 'test' AS map, code_system, code, description, CAST(NULL AS STRING) AS result_normalized,
             measurement_concept_id AS concept_id FROM {EMB_TEST_MAP}
      UNION ALL
      SELECT 'result', code_system, code, description, result_normalized, value_as_concept_id FROM {EMB_RESULT_MAP}
    """)
    # FINDING axis baseline (text-grain (result_normalized, suborder) -> finding_concept_id), snapshotted
    # from the bronze finding columns. Only when EMB_MP_TARGET carries them (post finding-axis fold); otherwise
    # the finding backfill is a no-op so there is nothing to baseline.
    if _mp_has_finding_cols():
        spark.sql(f"""
          CREATE OR REPLACE TABLE {FINDING_BASELINE} USING DELTA AS
          SELECT DISTINCT
                 LOWER(TRIM(REGEXP_REPLACE(value_source_value,'\\\\s+',' '))) AS result_normalized,
                 result_finding_suborder AS suborder, result_finding_concept_id AS concept_id
          FROM {EMB_MP_TARGET} WHERE result_finding_concept_id IS NOT NULL
        """)

# COMMAND ----------

def classify_map_delta(dry_run=False):
    """Diff current maps vs the persisted baseline. additive = key absent/NULL in baseline now non-NULL.
    correction = key had a non-NULL concept in baseline, now a DIFFERENT non-NULL concept (incl. tier
    promotion that changed the concept_id). Covers BOTH the value/test maps AND the finding axis: the
    finding diff compares THIS run's finding decode (FINDING_PROP, text-grain (result_normalized,suborder))
    vs FINDING_BASELINE — a changed finding concept at any suborder is a correction (folds into
    n_correction_keys so apply_* aborts). Returns {n_additive_keys, n_correction_keys, additive_df,
    correction_df, n_finding_correction_keys}."""
    if spark.catalog.tableExists(BASELINE):
        delta_sql = f"""
        WITH cur AS (
          SELECT 'test' AS map, code_system, code, description, CAST(NULL AS STRING) AS result_normalized, measurement_concept_id AS cid FROM {EMB_TEST_MAP}
          UNION ALL SELECT 'result', code_system, code, description, result_normalized, value_as_concept_id FROM {EMB_RESULT_MAP}
        )
        SELECT cur.*, b.concept_id AS base_cid,
          CASE
            WHEN (b.concept_id IS NULL) AND cur.cid IS NOT NULL THEN 'additive'
            WHEN b.concept_id IS NOT NULL AND cur.cid IS NOT NULL AND b.concept_id <> cur.cid THEN 'correction'
            ELSE 'unchanged' END AS delta_type
        FROM cur LEFT JOIN {BASELINE} b
          ON b.map=cur.map AND b.code_system=cur.code_system AND b.code=cur.code AND b.description=cur.description
         AND b.result_normalized <=> cur.result_normalized
        WHERE NOT ((b.concept_id IS NULL AND cur.cid IS NULL) OR (b.concept_id <=> cur.cid))
        """
    else:
        # No baseline yet (first run): everything currently mapped is treated as additive baseline seed.
        delta_sql = f"""
        SELECT 'test' AS map, code_system, code, description, CAST(NULL AS STRING) AS result_normalized,
               measurement_concept_id AS cid, CAST(NULL AS BIGINT) AS base_cid, 'additive' AS delta_type
        FROM {EMB_TEST_MAP} WHERE measurement_concept_id IS NOT NULL
        UNION ALL
        SELECT 'result', code_system, code, description, result_normalized, value_as_concept_id, NULL, 'additive'
        FROM {EMB_RESULT_MAP} WHERE value_as_concept_id IS NOT NULL
        """
    d = spark.sql(delta_sql)
    add_df = d.where("delta_type='additive'")
    cor_df = d.where("delta_type='correction'")
    n_corr = cor_df.count()
    # ---- FINDING-axis delta: current run's decode (FINDING_PROP) vs FINDING_BASELINE on (result_normalized, suborder)
    n_find_corr = 0
    if spark.catalog.tableExists(FINDING_PROP) and spark.catalog.tableExists(FINDING_BASELINE):
        fc = spark.sql(f"""
          SELECT COUNT(*) c FROM {FINDING_PROP} l JOIN {FINDING_BASELINE} b
            ON l.result_normalized=b.result_normalized AND l.result_finding_suborder=b.suborder
          WHERE b.concept_id IS NOT NULL AND l.result_finding_concept_id IS NOT NULL
            AND b.concept_id <> l.result_finding_concept_id
        """).first()["c"]
        n_find_corr = int(fc)
    return {"n_additive_keys": add_df.count(), "n_correction_keys": n_corr + n_find_corr,
            "n_finding_correction_keys": n_find_corr,
            "additive_df": add_df, "correction_df": cor_df}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 — Stage 5: apply_to_map_pathology (additive backfill, 6-key MERGE, NO source re-scan)
# MAGIC Re-derives mapped columns by joining EXISTING map_pathology rows back to the maps+concept.
# MAGIC MERGE matches the verified 6-key (source_table,source_event_id,lab_no,code,description,value_source_value)
# MAGIC — the 3-key is NOT unique (1021 raw collisions). Pass A: test-unmapped. Pass B: result-only additive.
# MAGIC Correction → abort + set rebuild_flagged.

# COMMAND ----------

def apply_to_map_pathology(additive_df, correction_present, dry_run=False):
    """Stage 5. correction_present -> abort (set rebuild_flagged, no write). Else two additive passes,
    both re-deriving columns by joining EXISTING map_pathology rows back to the maps (NO source re-scan):
      Pass A: rows with measurement_concept_id IS NULL that the TEST map now covers (full re-projection).
      Pass B: rows already test-mapped but value_as_concept_id IS NULL & non-numeric, whose RESULT key
              now maps (result-only additive). Both are strictly NULL->non-null; never overwrite.

    MERGE matches the VERIFIED 6-key row identity (source_table, source_event_id, lab_no, code,
    description, value_source_value) — the 3-key (source_table,source_event_id,lab_no) is NOT unique
    (1021 raw collisions sharing a TFCResultSeq with distinct result values). code/description/
    value_source_value use IS NOT DISTINCT FROM because all can be NULL (13,849 rows have NULL code).

    I1: the backfill is BOUNDED to additive_df's keys — both passes SEMI-JOIN their NULL-mapped base
    slice to THIS run's additive keys (staged to a Delta scratch table) so only keys that became
    mappable this run are re-scanned, not the whole ~18M-row NULL slice every run. If additive_df is
    None (explicit opt-in for a full backfill), no key filter is applied and both passes full-scan."""
    if correction_present:
        if not dry_run:
            spark.sql(f"UPDATE {MP_REBUILD_FLAG} SET rebuild_flagged = TRUE WHERE id = 1")
        return {"aborted_for_correction": True, "n_map_backfilled": 0, "rebuild_flagged": True}

    # v2 FIX #1: rebuild EXCL_REGEX_SQL in scope EXACTLY as map_pathology does (doubled-backslash),
    # then interpolate with a SINGLE brace below.
    _excl = [r.pattern for r in spark.table(EMB_EXCL_TBL).select("pattern").collect()]
    EXCL_REGEX_SQL = ("(" + "|".join(_excl) + ")").replace("\\", "\\\\")

    _NUM = r"^\\s*[<>]?=?\\s*-?[0-9.]+\\s*$"   # map_pathology numeric RLIKE (doubled for the f-string)

    # I1: bound the backfill to THIS run's additive keys (else both passes re-scan ~18M rows every run).
    # Stage the additive keys to a Delta scratch table; each pass SEMI-JOINs its base to the relevant kind.
    ADD_KEYS = f"{EMB_MAP_SCHEMA}._tmp_apply_additive_keys"
    if not dry_run and additive_df is not None:
        (additive_df.select("map","code_system","code","description","result_normalized")
            .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(ADD_KEYS))
    _have_keys = (not dry_run) and (additive_df is not None)
    # If additive_df is None (explicit opt-in for a full backfill), _have_keys=False → no key filter →
    # full-scan fallback (preserves the old whole-NULL-slice behavior). The correction path already
    # returned above, so None only reaches here when a caller deliberately requests a full backfill.
    _passA_keyfilter = (f"AND EXISTS (SELECT 1 FROM {ADD_KEYS} ak WHERE ak.map='test' "
                        f"AND ak.code_system=mp.code_system AND ak.code=mp.code AND ak.description=mp.description)") if _have_keys else ""
    _passB_keyfilter = (f"AND EXISTS (SELECT 1 FROM {ADD_KEYS} ak WHERE ak.map='result' "
                        f"AND ak.code_system=mp.code_system AND ak.code=mp.code AND ak.description=mp.description "
                        f"AND ak.result_normalized = LOWER(TRIM(REGEXP_REPLACE(mp.value_source_value,'\\\\s+',' '))))") if _have_keys else ""

    # ── PASS A: test-unmapped rows the test map now covers (full re-projection) ───────────────────
    passA_sql = f"""
      MERGE INTO {EMB_MP_TARGET} t
      USING (
        WITH base AS (
          SELECT mp.source_table, mp.source_event_id, mp.lab_no, mp.code_system, mp.code, mp.description,
                 mp.value_source_value AS result_txt, mp.unit_source_value,
                 CASE WHEN mp.value_source_value RLIKE '{_NUM}' THEN 1 ELSE 0 END AS rd_result_numeric,
                 CASE WHEN NOT (mp.value_source_value RLIKE '{_NUM}')
                      THEN LOWER(TRIM(REGEXP_REPLACE(mp.value_source_value,'\\\\s+',' '))) END AS rd_result_normalized
          FROM {EMB_MP_TARGET} mp WHERE mp.measurement_concept_id IS NULL {_passA_keyfilter}
        ),
        tj AS (
          SELECT b.*, tm.measurement_concept_id, tm.concept_name AS measurement_concept_name, tm.confidence_tier AS test_confidence_tier
          FROM base b LEFT JOIN {EMB_TEST_MAP} tm
            ON tm.code_system=b.code_system AND tm.code=b.code AND tm.description=b.description
           AND tm.confidence_tier IN ('curated','auto_high','auto_low') AND tm.measurement_concept_id IS NOT NULL
        ),
        rj AS (
          SELECT tj.*, rm.value_as_concept_id, rm.concept_name AS result_concept_name, rm.confidence_tier AS result_confidence_tier
          FROM tj LEFT JOIN {EMB_RESULT_MAP} rm
            ON rm.code_system=tj.code_system AND rm.code=tj.code AND rm.description=tj.description
           AND rm.result_normalized=tj.rd_result_normalized
           AND rm.confidence_tier IN {CONSUMED_TIERS} AND rm.value_as_concept_id IS NOT NULL
           AND tj.rd_result_numeric = 0
        ),
        uj AS (
          SELECT rj.*, um.unit_concept_id, um.ucum_code
          FROM rj LEFT JOIN {EMB_UNIT_MAP} um ON um.unit_source_value=rj.unit_source_value AND rj.unit_source_value IS NOT NULL
        )
        SELECT u.source_table, u.source_event_id, u.lab_no, u.code, u.description, u.result_txt AS value_source_value,
               u.measurement_concept_id, u.measurement_concept_name, u.test_confidence_tier,
               CASE WHEN u.rd_result_numeric=1 THEN NULL ELSE u.value_as_concept_id END AS value_as_concept_id,
               u.value_as_concept_id AS value_as_concept_id_raw,
               u.result_concept_name, u.result_confidence_tier, u.unit_concept_id, u.ucum_code,
               mc.concept_code AS test_code, mc.vocabulary_id AS test_vocab, mc.standard_concept AS test_std,
               rc.concept_code AS result_code, rc.vocabulary_id AS result_vocab, rc.standard_concept AS result_std,
               CASE WHEN u.rd_result_numeric=1 THEN 'numeric'
                    WHEN (CASE WHEN u.rd_result_numeric=1 THEN NULL ELSE u.value_as_concept_id END) IS NOT NULL THEN 'mapped'
                    WHEN u.rd_result_normalized IS NOT NULL AND u.rd_result_normalized RLIKE '{EXCL_REGEX_SQL}' THEN 'excluded'
                    ELSE 'free_text' END AS result_status
        FROM uj u LEFT JOIN {EMB_CONCEPT} mc ON mc.concept_id=u.measurement_concept_id
                  LEFT JOIN {EMB_CONCEPT} rc ON rc.concept_id=u.value_as_concept_id
        WHERE u.measurement_concept_id IS NOT NULL
      ) s
      ON t.source_table = s.source_table
        AND t.source_event_id = s.source_event_id
        AND t.lab_no IS NOT DISTINCT FROM s.lab_no
        AND t.code IS NOT DISTINCT FROM s.code
        AND t.description IS NOT DISTINCT FROM s.description
        AND t.value_source_value IS NOT DISTINCT FROM s.value_source_value
      WHEN MATCHED AND t.measurement_concept_id IS NULL THEN UPDATE SET
        t.measurement_concept_id=s.measurement_concept_id, t.measurement_concept_name=s.measurement_concept_name,
        t.test_confidence_tier=s.test_confidence_tier, t.value_as_concept_id=s.value_as_concept_id,
        t.result_concept_name=s.result_concept_name, t.result_confidence_tier=s.result_confidence_tier,
        t.unit_concept_id=s.unit_concept_id, t.ucum_code=s.ucum_code,
        t.test_omop_concept_id=s.measurement_concept_id,
        t.test_snomed_code=CASE WHEN s.test_vocab='SNOMED' THEN s.test_code END,
        t.test_loinc_code=CASE WHEN s.test_vocab='LOINC' THEN s.test_code END,
        t.test_omop_standard_concept=s.test_std, t.test_vocabulary_id=s.test_vocab,
        t.result_omop_concept_id=s.value_as_concept_id_raw,
        t.result_snomed_code=CASE WHEN s.result_vocab='SNOMED' THEN s.result_code END,
        t.result_loinc_code=CASE WHEN s.result_vocab='LOINC' THEN s.result_code END,
        t.result_omop_standard_concept=s.result_std, t.result_vocabulary_id=s.result_vocab,
        t.result_status=s.result_status, t.ADC_UPDT=current_timestamp()
      """


    # ── PASS B: test-mapped, result-unmapped, non-numeric rows whose RESULT key now maps ──────────
    # Only value-side columns change; measurement_concept_id and OMOP_MANUAL_* test columns stay.
    passB_sql = f"""
      MERGE INTO {EMB_MP_TARGET} t
      USING (
        WITH base AS (
          SELECT mp.source_table, mp.source_event_id, mp.lab_no, mp.code_system, mp.code, mp.description,
                 mp.value_source_value,
                 LOWER(TRIM(REGEXP_REPLACE(mp.value_source_value,'\\\\s+',' '))) AS rd_result_normalized
          FROM {EMB_MP_TARGET} mp
          WHERE mp.measurement_concept_id IS NOT NULL AND mp.value_as_concept_id IS NULL
            AND mp.result_status <> 'numeric'
            AND NOT (mp.value_source_value RLIKE '{_NUM}')
            {_passB_keyfilter}
        )
        SELECT b.source_table, b.source_event_id, b.lab_no, b.code, b.description, b.value_source_value,
               rm.value_as_concept_id, rm.concept_name AS result_concept_name, rm.confidence_tier AS result_confidence_tier,
               rc.concept_code AS result_code, rc.vocabulary_id AS result_vocab, rc.standard_concept AS result_std,
               CASE WHEN rm.value_as_concept_id IS NOT NULL THEN 'mapped' END AS result_status
        FROM base b JOIN {EMB_RESULT_MAP} rm
          ON rm.code_system=b.code_system AND rm.code=b.code AND rm.description=b.description
         AND rm.result_normalized=b.rd_result_normalized
         AND rm.confidence_tier IN {CONSUMED_TIERS} AND rm.value_as_concept_id IS NOT NULL
        LEFT JOIN {EMB_CONCEPT} rc ON rc.concept_id=rm.value_as_concept_id
      ) s
      ON t.source_table = s.source_table
        AND t.source_event_id = s.source_event_id
        AND t.lab_no IS NOT DISTINCT FROM s.lab_no
        AND t.code IS NOT DISTINCT FROM s.code
        AND t.description IS NOT DISTINCT FROM s.description
        AND t.value_source_value IS NOT DISTINCT FROM s.value_source_value
      WHEN MATCHED AND t.value_as_concept_id IS NULL AND s.value_as_concept_id IS NOT NULL THEN UPDATE SET
        t.value_as_concept_id=s.value_as_concept_id, t.result_concept_name=s.result_concept_name,
        t.result_confidence_tier=s.result_confidence_tier,
        t.result_omop_concept_id=s.value_as_concept_id,
        t.result_snomed_code=CASE WHEN s.result_vocab='SNOMED' THEN s.result_code END,
        t.result_loinc_code=CASE WHEN s.result_vocab='LOINC' THEN s.result_code END,
        t.result_omop_standard_concept=s.result_std, t.result_vocabulary_id=s.result_vocab,
        t.result_status=s.result_status, t.ADC_UPDT=current_timestamp()
      """

    if dry_run:
        return {"aborted_for_correction": False, "n_map_backfilled": "(dry-run: see preview query)", "rebuild_flagged": False}
    # n_map_backfilled = REAL rows changed by each pass (Databricks MERGE result exposes num_updated_rows).
    # NOTE: the prior `_mapped_count` OR-predicate undercounted Pass-B (those rows were already measurement-
    # mapped, so a value-only update didn't change the OR). Sum the two passes' actual updated-row counts.
    _ma = spark.sql(passA_sql).first()
    _mb = spark.sql(passB_sql).first()
    spark.sql(f"DROP TABLE IF EXISTS {ADD_KEYS}")   # I1: drop the per-run additive-keys scratch table
    _na = int(_ma["num_updated_rows"]) if _ma is not None and "num_updated_rows" in _ma.asDict() else 0
    _nb = int(_mb["num_updated_rows"]) if _mb is not None and "num_updated_rows" in _mb.asDict() else 0
    return {"aborted_for_correction": False, "n_map_backfilled": _na + _nb,
            "n_map_backfilled_passA": _na, "n_map_backfilled_passB": _nb, "rebuild_flagged": False}

# COMMAND ----------

def _mp_has_finding_cols():
    """True iff EMB_MP_TARGET carries the 4 finding-axis columns. They land only when the finding-axis FULL_REBUILD
    CTAS runs (finding-axis runbook §2); until then the incremental finding backfill no-ops."""
    cols = {f.name for f in spark.table(EMB_MP_TARGET).schema.fields}
    return {"result_finding_concept_id","result_finding_concept_name",
            "result_finding_domain","result_finding_suborder"}.issubset(cols)

def apply_finding_to_map_pathology(finding_prop, dry_run=False):
    """Stage 5 (finding axis). Write the finding columns on bronze rows whose result decodes to findings.
    suborder 0 -> UPDATE the matching bronze row in place (keeps full payload). suborder>0 -> INSERT a sub-row
    carrying the parent identity but ALL value/test/unit/source OMOP fields NULLed (so the OMOP stage's
    measurement_concept_id IS NOT NULL filter drops it -> zero-OMOP-change). Identity = 7-key incl.
    result_finding_suborder so siblings are distinct + re-runs idempotent. NEVER touches value_as_concept_id.
    Guards: empty/absent proposal -> no-op (None on an empty run cannot re-apply a stale prior proposal);
    EMB_MP_TARGET without the finding columns (pre finding-axis fold) -> no-op with a clear message."""
    if dry_run or finding_prop is None or not spark.catalog.tableExists(finding_prop):
        return {"n_finding_updated": 0, "n_finding_inserted": 0}
    if not _mp_has_finding_cols():
        print(f"apply_finding_to_map_pathology SKIPPED: {EMB_MP_TARGET} has no result_finding_* columns "
              f"(finding-axis fold not yet applied). No-op.")
        return {"n_finding_updated": 0, "n_finding_inserted": 0, "skipped_no_finding_cols": True}
    BASE = f"{EMB_MAP_SCHEMA}._tmp_finding_base"
    spark.sql(f"""CREATE OR REPLACE TABLE {BASE} USING DELTA AS
      SELECT mp.source_table, mp.source_event_id, mp.lab_no, mp.code, mp.description, mp.value_source_value,
             LOWER(TRIM(REGEXP_REPLACE(mp.value_source_value,'\\\\s+',' '))) AS rd
      FROM {EMB_MP_TARGET} mp
      WHERE (mp.result_finding_suborder IS NULL OR mp.result_finding_suborder=0)
        AND mp.value_source_value IS NOT NULL""")
    fp = spark.table(finding_prop)
    joined = (spark.table(BASE).alias("b")
              .join(fp.alias("f"), F.col("b.rd")==F.col("f.result_normalized"))
              .select(
                F.col("b.source_table").alias("source_table"),
                F.col("b.source_event_id").alias("source_event_id"),
                F.col("b.lab_no").alias("lab_no"),
                F.col("b.code").alias("code"),
                F.col("b.description").alias("description"),
                F.col("b.value_source_value").alias("value_source_value"),
                F.col("f.result_finding_concept_id").alias("result_finding_concept_id"),
                F.col("f.result_finding_concept_name").alias("result_finding_concept_name"),
                F.col("f.result_finding_domain").alias("result_finding_domain"),
                F.col("f.result_finding_suborder").alias("result_finding_suborder")))
    tgt = DeltaTable.forName(spark, EMB_MP_TARGET)
    s0 = joined.filter("result_finding_suborder=0")
    n_upd = s0.count()
    if n_upd:
        (tgt.alias("t").merge(s0.alias("s"),
            "t.source_table=s.source_table AND t.source_event_id=s.source_event_id "
            "AND t.lab_no <=> s.lab_no AND t.code <=> s.code AND t.description <=> s.description "
            "AND t.value_source_value <=> s.value_source_value")
          .whenMatchedUpdate(condition="t.result_finding_suborder IS NULL OR t.result_finding_suborder=0", set={
            "result_finding_concept_id":"s.result_finding_concept_id",
            "result_finding_concept_name":"s.result_finding_concept_name",
            "result_finding_domain":"s.result_finding_domain",
            "result_finding_suborder":"0",
            "ADC_UPDT":"current_timestamp()"}).execute())
    sN = joined.filter("result_finding_suborder>0")
    n_ins = sN.count()
    if n_ins:
        (tgt.alias("t").merge(sN.alias("s"),
            "t.source_table=s.source_table AND t.source_event_id=s.source_event_id "
            "AND t.lab_no <=> s.lab_no AND t.code <=> s.code AND t.description <=> s.description "
            "AND t.value_source_value <=> s.value_source_value "
            "AND t.result_finding_suborder = s.result_finding_suborder")
          .whenNotMatchedInsert(values={
            "source_table":"s.source_table","source_event_id":"s.source_event_id","lab_no":"s.lab_no",
            "code":"s.code","description":"s.description","value_source_value":"s.value_source_value",
            "result_finding_concept_id":"s.result_finding_concept_id",
            "result_finding_concept_name":"s.result_finding_concept_name",
            "result_finding_domain":"s.result_finding_domain","result_finding_suborder":"s.result_finding_suborder",
            "ADC_UPDT":"current_timestamp()"}).execute())
    spark.sql(f"DROP TABLE IF EXISTS {BASE}")
    return {"n_finding_updated": n_upd, "n_finding_inserted": n_ins}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 9 — run_increment orchestrator (DEV/fixture driver; NOT the prod entry path)
# MAGIC Chains discover → embed → remap → classify → apply; writes ONE RUN_LOG row; supports dry_run
# MAGIC (exact for vector_ready keys, "pending embedding" for needs_embed). PROD entry is the
# MAGIC CC/mappipeline_map_pathology_section section (it `%run`s this module and calls the stage fns inline,
# MAGIC owning the gate/flag/baseline) — do NOT use run_increment in prod (it is the DEV/fixture driver).

# COMMAND ----------

def run_increment(since_watermark=None, max_embed_terms=MAX_EMBED_TERMS, max_cost_usd=None, dry_run=False):
    """DEV/fixture end-to-end driver + dev dry-run harness. NOT the prod entry path: in prod the
    CC/mappipeline_map_pathology_section section `%run`s this module and calls the stage fns inline,
    owning the gate/flag/baseline. Do NOT use this function in prod."""
    import uuid as _uuid
    t0 = utcnow()
    run_id = _uuid.uuid4().hex
    if since_watermark is None:
        # DEV/fixture default ONLY: the retired map_pathology_state table held last_source_watermark;
        # the replacement flag table (MP_REBUILD_FLAG) has no watermark column. Derive a family-style
        # watermark from the target instead (matches get_max_timestamp's default-date semantics:
        # full-rebuild-from-scratch when the target is empty). No state-table dependency.
        since_watermark = spark.sql(
            f"SELECT COALESCE(MAX(ADC_UPDT), TIMESTAMP'1980-01-01') FROM {EMB_MP_TARGET}").first()[0]
    d1 = discover_new_keys(since_watermark, dry_run=dry_run)
    d2 = embed_pending_capped(max_embed_terms, max_cost_usd, dry_run=dry_run)
    finding_prop = None
    if not dry_run:
        rk = remap_keys(dry_run=False)
        finding_prop = rk.get("finding_prop")   # Stage 3b finding proposal (None on empty run)
    d4 = classify_map_delta(dry_run=dry_run)
    corr = d4["n_correction_keys"] > 0
    d5 = apply_to_map_pathology(d4.get("additive_df"), corr, dry_run=dry_run)
    # finding backfill (same correction gate as the value backfill): skip on a correction; no-op pre-fold
    fr = (apply_finding_to_map_pathology(finding_prop, dry_run=dry_run)
          if (not corr) else {"n_finding_updated": 0, "n_finding_inserted": 0})
    if not dry_run and not corr:
        snapshot_baseline()
    # n_map_inserted = 0 here BY DESIGN: the new-source-row INSERT is the Task-12 MERGE owned by
    # map_pathology_pipeline (not invoked from run_increment); in prod that cell logs its own insert count.
    n_backfilled = int(d5["n_map_backfilled"]) if str(d5.get("n_map_backfilled","")).lstrip("-").isdigit() else 0
    dur = (utcnow() - t0).total_seconds()
    cols = ["run_id","run_ts","since_watermark","n_missing_test_keys","n_missing_result_keys","n_vector_ready",
            "n_needs_embed","n_embedded","n_deferred","est_cost_usd","n_additive_keys","n_correction_keys",
            "n_map_backfilled","n_map_inserted","n_bbv_result_override_skipped",
            "rebuild_flagged","aborted_for_correction","dry_run","duration_s"]
    # n_bbv_result_override_skipped=0 is a PLACEHOLDER pending the override-skip observability hook
    # (the Task-5b decision reserved this column; precise population is a future enhancement, out of
    # scope for this dev driver — but the value must be present so createDataFrame matches RUN_LOG's schema).
    row = [(run_id, t0.replace(tzinfo=None), since_watermark,
            d1["n_missing_test_keys"], d1["n_missing_result_keys"], d1["n_vector_ready"], d1["n_needs_embed"],
            d2["n_embedded"], d2["n_deferred"], float(d2["est_cost_usd"]),
            d4["n_additive_keys"], d4["n_correction_keys"],
            n_backfilled, 0, 0,
            bool(d5.get("rebuild_flagged", False)), bool(d5.get("aborted_for_correction", False)), bool(dry_run), float(dur))]
    spark.createDataFrame(row, cols).write.format("delta").mode("append").saveAsTable(RUN_LOG)
    summary = dict(zip(cols, row[0]))
    # finding-axis counts: kept on the returned summary (RUN_LOG schema unchanged to avoid a migration;
    # the prod section logs finding counts in its own cell if desired).
    summary["n_finding_updated"] = int(fr.get("n_finding_updated", 0))
    summary["n_finding_inserted"] = int(fr.get("n_finding_inserted", 0))
    summary["n_finding_correction_keys"] = int(d4.get("n_finding_correction_keys", 0))
    print(f"run_increment done (dry_run={dry_run}): {summary}")
    return summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## HUMAN-GATE — real-AOAI smoke test (guarded `if False:`; never auto-runs)
# MAGIC The ONLY path that calls real Azure OpenAI. A human flips `if False:` → `if True:` in a
# MAGIC prod-authorised session to do a single 1-term end-to-end embed→remap as a live sanity check.

# COMMAND ----------

if False:  # HUMAN GATE — flip to True only for a deliberate 1-term live AOAI sanity check
    _t = "zzz_smoke_test_term_" + utcnow().strftime("%Y%m%d%H%M%S")
    # 9-col positional INSERT (queue gained result_normalized + embed_text). kind='test' -> result_normalized
    # NULL, embed_text = the context term (tests embed/score on the context string, unchanged).
    spark.sql(f"""INSERT INTO {QUEUE} VALUES
      ('CERNER_TESTCODE','ZZSMOKE','{_t}', LOWER('ZZSMOKE | {_t}'), 'test', 'pending', LOWER('ZZSMOKE | {_t}'),
       NULL, LOWER('ZZSMOKE | {_t}'))""")
    print(embed_pending_capped(max_terms=1))   # real AOAI: ~1 embed call
    n_vec = spark.sql(f"SELECT COUNT(*) c FROM {EMBEDDINGS_TABLE} WHERE LOWER(term)=LOWER('ZZSMOKE | {_t}') AND embedding_vector IS NOT NULL").first()["c"]
    assert n_vec >= 1, "smoke: no vector landed"
    print(remap_keys())
    n_map = spark.sql(f"SELECT COUNT(*) c FROM {EMB_TEST_MAP} WHERE code='ZZSMOKE'").first()["c"]
    assert n_map >= 1, "smoke: no test-map row produced"
    print("SMOKE OK — clean up: DELETE the ZZSMOKE queue/map rows + the test embedding.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## INTEGRATION — how this module runs in prod (the Map Pipeline family job)
# MAGIC
# MAGIC **Prod entrypoint = the map-family builder** `/Workspace/Shared/ADC-DB/Prod/Pipelines/Map Pipeline`
# MAGIC (Job 1008776761501829). map_pathology builds there alongside the other `map_*` tables, in ONE compute
# MAGIC run, via the section pasted from `CC/mappipeline_map_pathology_section`. The old standalone
# MAGIC `map_pathology_pipeline` is RETIRED, and the earlier plan to paste stages 1–4 into the nomenclature
# MAGIC notebook (`2313193198223400`) is SUPERSEDED — the whole embed loop now runs INLINE in the section.
# MAGIC
# MAGIC **This module is `%run` from the Map Pipeline notebook** (co-locate it next to that notebook). On the
# MAGIC incremental path the section calls the stage functions inline, in order:
# MAGIC `discover_new_keys → embed_pending_capped → remap_keys → classify_map_delta → apply_to_map_pathology`,
# MAGIC then `snapshot_baseline()`. On FULL_REBUILD it calls `snapshot_baseline(force=True)`. The section OWNS
# MAGIC the gate/flag/baseline decision (see its flag-wiring header): it always calls
# MAGIC `apply_to_map_pathology(..., correction_present=False)` and sets the rebuild flag itself on a correction.
# MAGIC
# MAGIC **Do NOT use `run_increment` in prod** — it is the DEV/fixture end-to-end driver only (the section wires
# MAGIC the stages itself). It derives its watermark from `MAX(ADC_UPDT)` on `EMB_MP_TARGET` (no state table).
# MAGIC
# MAGIC **RUN THE MIGRATION NOTEBOOK FIRST.** `pathology_embed_promote_to_prod` must be run once (it clones the
# MAGIC maps/queue + builds the freq table + creates the run-log in `3_lookup.omop`). Only then are the prod loop
# MAGIC tables in place for these constants to point at.
# MAGIC
# MAGIC **PROD constant flips (edit this module's config cell on promotion — AND the section's Cell-3 constants):**
# MAGIC - `EMB_MAP_SCHEMA        = "3_lookup.omop"`   (was `8_dev.omop`; loop tables promoted there — NOT `4_prod.omop`)
# MAGIC - `EMB_MP_TARGET         = "4_prod.bronze.map_pathology"`
# MAGIC - `MP_REBUILD_FLAG` needs NO flip — it follows `EMB_MAP_SCHEMA` (`{EMB_MAP_SCHEMA}.pathology_map_rebuild_flag`). The
# MAGIC   Map Pipeline section creates/owns/seeds it; do NOT seed `map_pathology_state` (RETIRED).
# MAGIC - `EMBEDDINGS_TABLE` stays `3_lookup.embeddings.terms`; `EMB_CONCEPT` stays `3_lookup.omop.concept` (both global).
# MAGIC - `FREQ_TABLE_MILLRAW` reads the SAME mill/raw `4_prod.raw.*` sources (already prod paths — no change).
# MAGIC - `RUN_LOG` → `3_lookup.omop.pathology_embed_run_log` (follows `EMB_MAP_SCHEMA`; migration creates it fresh+empty).
# MAGIC
# MAGIC **ENV PREP (load-bearing — surfaced by the fixture):**
# MAGIC - v2.3: openai + the `adc_store` secret are LAZY (imported/fetched only when an actual embed runs, inside
# MAGIC   `_get_embedding_client`). So `%run`-importing this module needs NO env-prep — it loads on ANY compute.
# MAGIC - The embed path itself (`embed_pending_capped → _embed_one_batch → _get_embedding_client`) STILL requires:
# MAGIC   (a) `openai` installed (`%pip install openai pyarrow` at the top of the Map Pipeline notebook OR a cluster
# MAGIC   library); and (b) the `adc_store` secret scope (key `barts_global_key`) reachable by the job's run identity.
# MAGIC   Without both, the FIRST real embed (not the import) fails.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handoff — done vs human-gated
# MAGIC
# MAGIC **Done & verified in dev (`8_dev`):**
# MAGIC - All 5 stages + the `run_increment` dev driver authored; openai imported LAZILY (`%run`-safe on any compute).
# MAGIC - The fixture harness (`pathology_embed_increment_fixture`) runs end-to-end GREEN — **24/24 assertions**:
# MAGIC   fan-out dedup, Pass A / Pass B additive backfill, excluded branch, answer-list scoping, result-collision
# MAGIC   dedup, correction abort + TOCTOU, idempotency, NULL-safe 6-key uniqueness, source-free column parity.
# MAGIC - Integration deliverables: `CC/mappipeline_map_pathology_section` (the map_pathology section for the Map
# MAGIC   Pipeline family — reuses `update_table`/`get_max_timestamp`, owns the rebuild flag, runs the embed loop
# MAGIC   inline); `CC/mappipeline_update_table_patch` (the backward-compatible `update_condition=` upgrade to the
# MAGIC   family `update_table`); the standalone `map_pathology_pipeline` marked RETIRED.
# MAGIC - The rebuild flag lives in the single-row `{EMB_MAP_SCHEMA}.pathology_map_rebuild_flag` (owned by the section;
# MAGIC   read by this module's `snapshot_baseline` guard). `map_pathology_state` is RETIRED.
# MAGIC
# MAGIC **Remains human-gated (prod, in order):**
# MAGIC 1. Run `pathology_embed_promote_to_prod` once (clones loop tables → `3_lookup.omop`, builds freq, fresh run-log).
# MAGIC 2. Co-locate this module next to the Map Pipeline notebook; ensure `%run ./pathology_embed_increment` near its top.
# MAGIC 3. Paste the `update_table` patch (`CC/mappipeline_update_table_patch`) over the family `update_table`, then paste
# MAGIC    the `CC/mappipeline_map_pathology_section` cells after the nomenclature + concept-map builders.
# MAGIC 4. Flip the PROD constants (above) in BOTH this module's config and the section's Cell-3 constants; ensure env prep.
# MAGIC 5. Run the FIRST build as a FULL_REBUILD on a CLUSTER (seeds bronze + the baseline + the flag table).
# MAGIC 6. Schedule the weekly family job; the incremental path auto-selects via the gate. The FIRST weekly run validates
# MAGIC    the RAW incremental path + real AOAI cost (dev used a STUB embed; the RAW branch was UNEXERCISED in dev).
# MAGIC 7. The real-AOAI smoke cell (above, guarded `if False:`) for a deliberate live 1-term sanity check.
# MAGIC
# MAGIC **Cost cap:** `embed_pending_capped` embeds ≤ `MAX_EMBED_TERMS` DISTINCT texts per run; capped overflow is
# MAGIC flipped `pending → deferred` (in-warehouse, logged), picked up by a later run. `est_cost_usd` is the ATTEMPTED
# MAGIC upper bound (cap × unit cost), not realized spend.
# MAGIC
# MAGIC **Correction → rebuild policy:** a concept CORRECTION (an existing key's concept_id changed) sets the rebuild
# MAGIC flag, the section SKIPS the additive backfill, and the NEXT weekly run auto-selects FULL_REBUILD (on a cluster:
# MAGIC reconciles existing rows, clears the flag, re-baselines via `snapshot_baseline(force=True)`). ADDITIVE bumps
# MAGIC (NULL → concept) are absorbed serverless with NO rebuild.
# MAGIC
# MAGIC **Cross-reference:** the `map_pathology_pipeline` promotion runbook (MP Task 14, now retired-but-kept) + its two
# MAGIC tracked parity follow-ups — `result_status` (~513k rows) and the raw NRBC swap (~214k rows).

# COMMAND ----------

# -- Lazy infrastructure ---------------------------------------------------------------------------
# Everything pathology_embed_increment used to execute at import time (the queue ALTERs, the run-log
# DDL, the staging schema/volume/directory and the embeddings write-probe, which appended an empty
# DataFrame to a production Delta table just by being imported) now runs here, on demand and at most
# once per session. Loading this notebook therefore writes nothing, anywhere.
EMBEDDINGS_TABLE_ACTIVE = None
_EMB_INFRA_READY = False

def _emb_ensure_infrastructure():
    """Idempotent lazy provisioning for the folded embed loop; returns the active embeddings table."""
    global _EMB_INFRA_READY, EMBEDDINGS_TABLE_ACTIVE
    if _EMB_INFRA_READY:
        return EMBEDDINGS_TABLE_ACTIVE
    for _qcol in ("result_normalized STRING", "embed_text STRING"):
        try:
            spark.sql(f"ALTER TABLE {QUEUE} ADD COLUMNS ({_qcol})")
            print(f"{QUEUE}: added {_qcol}")
        except Exception as _e:
            print(f"{QUEUE} add-column note ({_qcol}): {str(_e)[:100]}")  # already present / table absent -> fine
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {RUN_LOG} (
      run_id                 STRING,
      run_ts                 TIMESTAMP,
      since_watermark        TIMESTAMP,
      n_missing_test_keys    BIGINT,
      n_missing_result_keys  BIGINT,
      n_vector_ready         BIGINT,
      n_needs_embed          BIGINT,
      n_embedded             BIGINT,
      n_deferred             BIGINT,
      est_cost_usd           DOUBLE,
      n_additive_keys        BIGINT,
      n_correction_keys      BIGINT,
      n_map_backfilled       BIGINT,
      n_map_inserted         BIGINT,
      n_bbv_result_override_skipped BIGINT,
      rebuild_flagged        BOOLEAN,
      aborted_for_correction BOOLEAN,
      dry_run                BOOLEAN,
      duration_s             DOUBLE
    ) USING DELTA
    """)
    print(f"Run-log ready: {RUN_LOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA}")
    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {STAGING_SCHEMA}.{STAGING_VOLUME_NAME}")
    except Exception as _vol_e:
        raise RuntimeError(
            f"Cannot provision the embed staging volume {STAGING_SCHEMA}.{STAGING_VOLUME_NAME}: "
            f"{_vol_e}. The run identity needs CREATE VOLUME on {STAGING_SCHEMA} once, then READ "
            "VOLUME and WRITE VOLUME on it."
        ) from _vol_e
    os.makedirs(STAGING_PATH, exist_ok=True)
    print(f"Staging volume ready: {STAGING_PATH}")
    print(f"Files already staged: {len(glob.glob(STAGING_PATH + '/*.parquet'))}")
    try:
        _probe_3_lookup()
    except Exception as e:
        raise RuntimeError(
            f"Embeddings write-probe against {EMBEDDINGS_TABLE} failed: {e}. The pathology embed "
            "loop has no fallback target -- silently redirecting production embeddings into a "
            "development table would score the concept maps against dev vectors. Fix the table or "
            "the grant and re-run."
        ) from e
    EMBEDDINGS_TABLE_ACTIVE = EMBEDDINGS_TABLE
    print(f"Embeddings target: {EMBEDDINGS_TABLE_ACTIVE}")
    _EMB_INFRA_READY = True
    return EMBEDDINGS_TABLE_ACTIVE

def _emb_active_embeddings_table():
    """Lazy accessor for the embeddings write target; provisions on first use."""
    return _emb_ensure_infrastructure()

# COMMAND ----------

# ==== END FOLD: pathology_embed_increment ====

# COMMAND ----------

print('Map Pathology v3 restartable replacement loaded. Call create_map_pathology() explicitly; no production write has run.')

try:
    _targets = ['4_prod.bronze.map_pathology']
    if not _pipeline_resume_skip_component('map_pathology', _targets):
        create_map_pathology(force_full=_PIPELINE_FULL_REFRESH, run_embed_loop=_PIPELINE_RUN_EMBEDDINGS) if _PIPELINE_RUN_PATHOLOGY else print('[VALIDATION] Pathology candidate skipped')
        _PIPELINE_UPDATED_TARGETS.extend(_targets)
        _pipeline_mark_component_complete('map_pathology', _targets)
        _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_pathology'})
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

# Finalize while the last component still owns the shared map runtime. Once a
# component %run returns, Databricks does not reliably expose that runtime to a
# subsequent command in map_pipeline.
try:
    _MAP_PIPELINE_FINAL_RESULT_JSON = finalize_map_pipeline_run()
except Exception as exc:
    _pipeline_record_error(
        None,
        exc,
        "FINALIZATION_ERROR",
    )
    raise


