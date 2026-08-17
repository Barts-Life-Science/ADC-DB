# Databricks notebook source
# MAGIC %md
# MAGIC # Map 10 — Foundation
# MAGIC
# MAGIC Components: address, address EPC, person, care site, medical personnel, encounter. This notebook is executed by `map_pipeline` in the shared map runtime.

# COMMAND ----------

if "_PIPELINE_RUN_ID" not in globals():
    raise RuntimeError("Run this component through map_pipeline so shared contracts, checkpoints and audit state are initialized.")

# COMMAND ----------

_pipeline_component_start("map_address")
_pipeline_component_start("map_address_epc", set_current=False)
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
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from uuid import uuid4
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window
RAW_ADDRESS_TABLE = '4_prod.raw.mill_address'
MAP_ADDRESS_TABLE = '4_prod.bronze.map_address'
MAP_ADDRESS_EPC_TABLE = '4_prod.bronze.map_address_epc'
CODE_VALUE_TABLE = '3_lookup.mill.mill_code_value'
ADDRESSBASE_TABLE = '3_lookup.ons.addressbase_consolidated'
POSTCODE_MAP_TABLE = '3_lookup.imd.postcode_maps'
IMD_TABLE = '3_lookup.imd.imd_2019'
EPC_TABLE = '3_lookup.geo.epc_certificates'
MAP_SCHEMA_VERSION = '2.0.0'
MATCH_ALGORITHM_VERSION = '2.0.0'
EPC_SCHEMA_VERSION = '2.0.0'
CDF_OVERLAP_HOURS = 48
_VOLATILE_COMPARE_COLUMNS = {'PIPELINE_LOADED_AT', 'MATCH_RUN_TS'}
_PENDING_TABLE_OPTIONS: Dict[str, dict] = {}
_PENDING_CACHES: Dict[str, List[DataFrame]] = {}
map_address_comment = 'Curated address foundation. One row per source ADDRESS_ID for PERSON parent entities across the full address history, plus currently effective ORGANIZATION addresses. All other parent entity types are intentionally excluded and remain available in 4_prod.raw.mill_address. Rows that fall out of that scope are deleted from this table rather than left stale. Preserves source status and provenance, adds privacy-aware postcode/address outputs, auditable UPRN matching, exact-postcode geography and IMD enrichment.'
map_address_epc_comment = 'Bronze EPC enrichment at ADDRESS_ID grain. Contains only addresses with a matched EPC certificate, so every surviving row reads EPC_MATCH_STATUS = MATCHED. Addresses with no UPRN or no certificate are absent rather than carrying placeholder rows, and an address whose match no longer holds is deleted from this table. Source and load timestamps are retained separately.'

def _sf(name, data_type, comment='', nullable=True):
    metadata = {'comment': comment} if comment else {}
    return StructField(name, data_type, nullable, metadata=metadata)
schema_map_address = StructType([_sf('ADDRESS_ID', LongType(), 'Source address primary key.'), _sf('PARENT_ENTITY_NAME', StringType(), 'Source parent entity type.'), _sf('PARENT_ENTITY_ID', LongType(), 'Source parent entity identifier.'), _sf('masked_zipcode', StringType(), 'Outward postcode for person-related rows; source postcode for other entity types.'), _sf('POSTCODE_OUTWARD', StringType(), 'Normalized UK outward postcode derived by removing the final three characters.'), _sf('POSTCODE_SECTOR', StringType(), 'Normalized postcode sector derived by removing the final two characters.'), _sf('SOURCE_ZIPCODE', StringType(), 'Privacy-aware source postcode. Person-related rows contain the outward code unless sensitive output is enabled.'), _sf('SOURCE_ZIPCODE_MASK3', StringType(), 'First three characters of the raw source postcode, retained solely to reproduce the legacy masked_zipcode value. Less precise than POSTCODE_OUTWARD.'), _sf('ZIPCODE_KEY', StringType(), 'Source normalized postcode key.'), _sf('POSTAL_IDENTIFIER', StringType(), 'Source postal identifier.'), _sf('CITY', StringType(), 'Source city text.'), _sf('CITY_CODE_VALUE', LongType(), 'Source city code value.'), _sf('full_street_address', StringType(), 'Concatenated source address. Blank for person-related rows unless sensitive output is enabled.'), _sf('SOURCE_STREET_ADDR1', StringType(), 'Privacy-aware source address line 1.'), _sf('SOURCE_STREET_ADDR2', StringType(), 'Privacy-aware source address line 2.'), _sf('SOURCE_STREET_ADDR3', StringType(), 'Privacy-aware source address line 3.'), _sf('SOURCE_STREET_ADDR4', StringType(), 'Privacy-aware source address line 4.'), _sf('STATE', StringType(), 'Source state text.'), _sf('STATE_CODE_VALUE', LongType(), 'Source state code value.'), _sf('COUNTY', StringType(), 'Source county text, coalesced with its code description.'), _sf('COUNTY_CODE_VALUE', LongType(), 'Source county code value.'), _sf('COUNTRY_RAW', StringType(), 'Source country text.'), _sf('COUNTRY_CODE_VALUE', LongType(), 'Source country code value.'), _sf('COUNTRY_DESCRIPTION', StringType(), 'Country description coalesced from code description, display and raw source text.'), _sf('country_cd', StringType(), 'Compatibility field containing COUNTRY_DESCRIPTION. Use COUNTRY_CODE_VALUE for the source code.'), _sf('ADDRESS_TYPE_CODE', LongType(), 'Source address type code.'), _sf('ACTIVE_STATUS_CODE', LongType(), 'Source active status code.'), _sf('DATA_STATUS_CODE', LongType(), 'Source data status code.'), _sf('ADDRESS_INFO_STATUS_CODE', LongType(), 'Source address information status code.'), _sf('CONTRIBUTOR_SYSTEM_CODE', LongType(), 'Source contributor system code.'), _sf('SOURCE_IDENTIFIER', StringType(), 'Source-system address identifier.'), _sf('VALIDATION_EXPIRE_DT_TM', TimestampType(), 'Source address validation expiry timestamp.'), _sf('SOURCE_UPDT_DT_TM', TimestampType(), 'Source application update timestamp.'), _sf('SOURCE_UPDT_CNT', LongType(), 'Source update counter.'), _sf('LAST_UTC_TS', TimestampType(), 'Source last UTC timestamp.'), _sf('INST_ID', LongType(), 'Source instance identifier.'), _sf('LSOA', StringType(), 'LSOA21 code derived from an exact full postcode.'), _sf('OA21CD', StringType(), '2021 Output Area code.'), _sf('MSOA21CD', StringType(), '2021 Middle Layer Super Output Area code.'), _sf('LADCD', StringType(), 'Local authority district code.'), _sf('LSOA21NM', StringType(), '2021 LSOA name.'), _sf('MSOA21NM', StringType(), '2021 MSOA name.'), _sf('LADNM', StringType(), 'Local authority district name.'), _sf('LSOA_MATCH_METHOD', StringType(), 'EXACT_SOURCE_POSTCODE, ADDRESSBASE_POSTCODE or NONE.'), _sf('IMD_Decile', StringType(), 'Compatibility string form of IMD_DECILE_NUM; null when unavailable.'), _sf('IMD_Quintile', StringType(), 'Compatibility string form of IMD_QUINTILE_NUM; null when unavailable.'), _sf('IMD_DECILE_NUM', IntegerType(), 'Numeric 2019 IMD decile.'), _sf('IMD_QUINTILE_NUM', IntegerType(), 'Numeric IMD quintile derived from decile.'), _sf('IMD_RANK', IntegerType(), '2019 IMD rank.'), _sf('IMD_SCORE', DoubleType(), '2019 IMD score.'), _sf('IMD_YEAR', IntegerType(), 'IMD publication year.'), _sf('GEOGRAPHY_VERSION', StringType(), 'Geography vintage used for OA, LSOA, MSOA and LAD.'), _sf('UPRN', LongType(), 'Accepted Unique Property Reference Number; null when no sufficiently reliable unique match exists.'), _sf('UDPRN', StringType(), 'Royal Mail Unique Delivery Point Reference Number from AddressBase.'), _sf('LATITUDE', DoubleType(), 'AddressBase latitude for the accepted UPRN.'), _sf('LONGITUDE', DoubleType(), 'AddressBase longitude for the accepted UPRN.'), _sf('ADDRESSBASE_POSTCODE', StringType(), 'AddressBase postcode for the accepted UPRN.'), _sf('ADDRESSBASE_LOGICAL_STATUS', StringType(), 'AddressBase logical status.'), _sf('ADDRESSBASE_BLPU_STATE', StringType(), 'AddressBase BLPU state.'), _sf('ADDRESSBASE_COUNTRY', StringType(), 'AddressBase country.'), _sf('ADDRESSBASE_ADMINISTRATIVE_AREA', StringType(), 'AddressBase administrative area.'), _sf('ADDRESSBASE_ADC_UPDT', TimestampType(), 'Update timestamp of the accepted AddressBase record.'), _sf('match_algorithm', IntegerType(), '0 no match; 1 unique exact full address; 2 unique postcode and building number; 3 resolved ambiguous number; 4 unique exact component.'), _sf('match_confidence', DoubleType(), 'Deterministic match score, not a calibrated probability.'), _sf('match_quality', StringType(), 'Human-readable accepted match method.'), _sf('MATCH_CANDIDATE_COUNT', IntegerType(), 'Number of candidate UPRNs considered at the accepted or rejected matching stage.'), _sf('MATCH_SCORE_MARGIN', DoubleType(), 'Difference between the best and second-best candidate scores.'), _sf('MATCH_POSTCODE_EXACT', BooleanType(), 'Whether source and AddressBase full postcodes agree exactly.'), _sf('MATCH_BUILDING_NUMBER_EXACT', BooleanType(), 'Whether normalized building numbers agree exactly.'), _sf('MATCH_SUB_BUILDING_EXACT', BooleanType(), 'Whether normalized sub-building identifiers agree exactly.'), _sf('MATCH_BUILDING_NAME_EXACT', BooleanType(), 'Whether an AddressBase building name occurs as an exact normalized source line/component.'), _sf('MATCH_ADDRESS_SIMILARITY', DoubleType(), 'Normalized Levenshtein similarity used in candidate resolution.'), _sf('MATCH_FAILURE_REASON', StringType(), 'Reason an address remains unmatched.'), _sf('MATCH_ALGORITHM_VERSION', StringType(), 'Version of the matching implementation.'), _sf('MATCH_RUN_TS', TimestampType(), 'Timestamp at which the address was last evaluated by matching logic.'), _sf('BEG_EFFECTIVE_DT_TM', TimestampType(), 'Source effective start timestamp.'), _sf('ACTIVE_IND', LongType(), 'Source active indicator.'), _sf('END_EFFECTIVE_DT_TM', TimestampType(), 'Source effective end timestamp.'), _sf('ADC_UPDT', TimestampType(), 'Source ADC update timestamp.'), _sf('POSTCODE_MAP_ADC_UPDT', TimestampType(), 'Update timestamp of the selected exact-postcode geography record.'), _sf('IMD_LOOKUP_ADC_UPDT', TimestampType(), 'Update timestamp of the selected IMD record.'), _sf('COUNTRY_LOOKUP_ADC_UPDT', TimestampType(), 'Update timestamp of the selected country code record.'), _sf('PIPELINE_LOADED_AT', TimestampType(), 'Timestamp this row was inserted or materially updated by the map pipeline.')])
schema_map_address_epc = StructType([_sf('ADDRESS_ID', LongType(), 'Primary key and foreign key to map_address.', nullable=False), _sf('UPRN', LongType(), 'UPRN from map_address.'), _sf('EPC_MATCH_STATUS', StringType(), 'Always MATCHED. Only addresses with a matched EPC certificate are published; unmatched addresses are absent from this table.'), _sf('EPC_CERTIFICATE_COUNT', IntegerType(), 'Number of EPC source rows observed for this UPRN.'), _sf('EPC_LMK_KEY', StringType(), 'EPC certificate unique identifier.'), _sf('UPRN_SOURCE', StringType(), 'Source used by the EPC register to assign the UPRN.'), _sf('EPC_POSTCODE', StringType(), 'Postcode recorded on the EPC certificate.'), _sf('LOCAL_AUTHORITY', StringType(), 'EPC local authority.'), _sf('CONSTITUENCY', StringType(), 'EPC parliamentary constituency.'), _sf('BUILDING_REFERENCE_NUMBER', StringType(), 'EPC building reference number.'), _sf('CURRENT_ENERGY_RATING', StringType(), 'Current EPC band.'), _sf('POTENTIAL_ENERGY_RATING', StringType(), 'Potential EPC band.'), _sf('CURRENT_ENERGY_EFFICIENCY', IntegerType(), 'Current SAP score.'), _sf('POTENTIAL_ENERGY_EFFICIENCY', IntegerType(), 'Potential SAP score.'), _sf('ENVIRONMENT_IMPACT_CURRENT', IntegerType(), 'Current environmental impact score.'), _sf('ENVIRONMENT_IMPACT_POTENTIAL', IntegerType(), 'Potential environmental impact score.'), _sf('WALLS_DESCRIPTION', StringType(), 'Wall construction and insulation description.'), _sf('WALLS_ENERGY_EFF', StringType(), 'Wall energy efficiency rating.'), _sf('WALLS_ENV_EFF', StringType(), 'Wall environmental efficiency rating.'), _sf('ROOF_DESCRIPTION', StringType(), 'Roof description.'), _sf('ROOF_ENERGY_EFF', StringType(), 'Roof energy efficiency rating.'), _sf('ROOF_ENV_EFF', StringType(), 'Roof environmental efficiency rating.'), _sf('FLOOR_DESCRIPTION', StringType(), 'Floor description.'), _sf('FLOOR_ENERGY_EFF', StringType(), 'Floor energy efficiency rating.'), _sf('FLOOR_ENV_EFF', StringType(), 'Floor environmental efficiency rating.'), _sf('WINDOWS_DESCRIPTION', StringType(), 'Window and glazing description.'), _sf('WINDOWS_ENERGY_EFF', StringType(), 'Window energy efficiency rating.'), _sf('WINDOWS_ENV_EFF', StringType(), 'Window environmental efficiency rating.'), _sf('MULTI_GLAZE_PROPORTION', DoubleType(), 'Proportion of multiple glazing.'), _sf('GLAZED_TYPE', StringType(), 'Glazing type.'), _sf('GLAZED_AREA', StringType(), 'Glazed area category.'), _sf('MAINHEAT_DESCRIPTION', StringType(), 'Main heating description.'), _sf('MAINHEAT_ENERGY_EFF', StringType(), 'Main heating energy efficiency rating.'), _sf('MAINHEAT_ENV_EFF', StringType(), 'Main heating environmental efficiency rating.'), _sf('MAIN_FUEL', StringType(), 'Main fuel.'), _sf('MAIN_HEATING_CONTROLS', IntegerType(), 'Main heating controls code.'), _sf('MAINHEATCONT_DESCRIPTION', StringType(), 'Main heating controls description.'), _sf('MAINHEATC_ENERGY_EFF', StringType(), 'Heating controls energy efficiency rating.'), _sf('MAINHEATC_ENV_EFF', StringType(), 'Heating controls environmental efficiency rating.'), _sf('SECONDHEAT_DESCRIPTION', StringType(), 'Secondary heating description.'), _sf('MAINS_GAS_FLAG', BooleanType(), 'Whether mains gas is available; null remains unknown.'), _sf('HOTWATER_DESCRIPTION', StringType(), 'Hot water description.'), _sf('HOT_WATER_ENERGY_EFF', StringType(), 'Hot water energy efficiency rating.'), _sf('HOT_WATER_ENV_EFF', StringType(), 'Hot water environmental efficiency rating.'), _sf('PROPERTY_TYPE', StringType(), 'Property type.'), _sf('BUILT_FORM', StringType(), 'Built form.'), _sf('TOTAL_FLOOR_AREA', DoubleType(), 'Total floor area in square metres.'), _sf('NUMBER_HABITABLE_ROOMS', IntegerType(), 'Number of habitable rooms.'), _sf('NUMBER_HEATED_ROOMS', IntegerType(), 'Number of heated rooms.'), _sf('CONSTRUCTION_AGE_BAND', StringType(), 'Construction age band.'), _sf('FLOOR_LEVEL', StringType(), 'Floor level.'), _sf('FLAT_TOP_STOREY', BooleanType(), 'Whether a flat is on the top storey.'), _sf('NUMBER_OPEN_FIREPLACES', IntegerType(), 'Number of open fireplaces.'), _sf('EXTENSION_COUNT', IntegerType(), 'Number of extensions.'), _sf('MECHANICAL_VENTILATION', StringType(), 'Mechanical ventilation type.'), _sf('FLOOR_HEIGHT', DoubleType(), 'Floor height.'), _sf('HEATING_COST_CURRENT', DoubleType(), 'Current modelled heating cost.'), _sf('HOT_WATER_COST_CURRENT', DoubleType(), 'Current modelled hot-water cost.'), _sf('LIGHTING_COST_CURRENT', DoubleType(), 'Current modelled lighting cost.'), _sf('ENERGY_CONSUMPTION_CURRENT', DoubleType(), 'Current energy consumption.'), _sf('CO2_EMISSIONS_CURRENT', DoubleType(), 'Current CO2 emissions.'), _sf('CO2_EMISS_CURR_PER_FLOOR_AREA', DoubleType(), 'Current CO2 emissions per floor area.'), _sf('EPC_INSPECTION_DATE', DateType(), 'EPC inspection date.'), _sf('EPC_LODGEMENT_DATE', DateType(), 'EPC lodgement date.'), _sf('TRANSACTION_TYPE', StringType(), 'EPC transaction type.'), _sf('TENURE', StringType(), 'Tenure recorded on the EPC.'), _sf('REPORT_TYPE', IntegerType(), 'EPC report type.'), _sf('fuel_poverty_risk', BooleanType(), 'EPC-band D-G proxy; not a complete fuel-poverty classification.'), _sf('hhsrs_cold_hazard_proxy', BooleanType(), 'EPC-band F-G cold-hazard proxy.'), _sf('spatial_heating_poverty', BooleanType(), 'True when heated rooms are fewer than habitable rooms.'), _sf('off_gas_grid', BooleanType(), 'Inverse of a known MAINS_GAS_FLAG; null remains unknown.'), _sf('ADDRESS_ADC_UPDT', TimestampType(), 'Source ADC timestamp from map_address.'), _sf('EPC_SOURCE_ADC_UPDT', TimestampType(), 'Source ADC timestamp from epc_certificates.'), _sf('ADC_UPDT', TimestampType(), 'Greatest available source update timestamp.'), _sf('PIPELINE_LOADED_AT', TimestampType(), 'Timestamp this row was inserted or materially updated.')])

def _sql_table(table_name: str) -> str:
    tick = chr(96)
    return '.'.join((f'{tick}{part}{tick}' for part in table_name.split('.')))

def _escape_sql(value: str) -> str:
    return value.replace('\\', '\\\\').replace("'", "''")

def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _table_columns(table_name: str) -> set:
    if not table_exists(table_name):
        return set()
    return set(spark.table(table_name).columns)

def _table_description(table_name: str) -> Optional[str]:
    if not table_exists(table_name):
        return None
    row = spark.sql(f'DESCRIBE DETAIL {_sql_table(table_name)}').select('description').first()
    return row.description if row else None

def _get_table_property(table_name: str, key: str) -> Optional[str]:
    if not table_exists(table_name):
        return None
    rows = spark.sql(f"SHOW TBLPROPERTIES {_sql_table(table_name)} ('{_escape_sql(key)}')").collect()
    if not rows:
        return None
    value = rows[0].value
    if value is None or str(value).startswith('Table '):
        return None
    return str(value)

def _set_table_properties(table_name: str, properties: Dict[str, str]) -> None:
    if not properties:
        return
    assignments = ', '.join((f"'{_escape_sql(k)}' = '{_escape_sql(str(v))}'" for k, v in properties.items() if v is not None))
    if assignments:
        spark.sql(f'ALTER TABLE {_sql_table(table_name)} SET TBLPROPERTIES ({assignments})')

def _format_ts(value) -> str:
    if value is None:
        return 'NONE'
    if getattr(value, 'tzinfo', None) is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()

def _max_timestamp(table_name: str, column_name: str='ADC_UPDT'):
    if not table_exists(table_name) or column_name not in _table_columns(table_name):
        return None
    return spark.table(table_name).agg(F.max(F.col(column_name)).alias('max_ts')).first().max_ts

def _last_successful_merge_timestamp(table_name: str):
    if not table_exists(table_name):
        return None
    history = spark.sql(f'DESCRIBE HISTORY {_sql_table(table_name)}').filter(F.col('operation').isin('MERGE', 'WRITE', 'CREATE TABLE AS SELECT', 'REPLACE TABLE AS SELECT', 'CLONE', 'RESTORE')).orderBy(F.col('version').desc()).select('timestamp').limit(1).collect()
    return history[0].timestamp if history else None

def _latest_data_version(table_name: str) -> str:
    if not table_exists(table_name):
        return 'NONE'
    history = spark.sql(f'DESCRIBE HISTORY {_sql_table(table_name)}').filter(F.col('operation').isin('MERGE', 'WRITE', 'UPDATE', 'DELETE', 'CREATE TABLE AS SELECT', 'REPLACE TABLE AS SELECT', 'COPY INTO')).orderBy(F.col('version').desc()).select('version').limit(1).collect()
    return str(history[0].version) if history else 'NONE'

def _schema_select(df: DataFrame, schema: StructType) -> DataFrame:
    expressions = []
    source_columns = set(df.columns)
    for field in schema.fields:
        if field.name in source_columns:
            expressions.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            expressions.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(*expressions)

def _delete_rows(keys: DataFrame, schema: StructType) -> DataFrame:
    selected = _schema_select(keys, schema)
    return selected.withColumn('_source_change_type', F.lit('delete'))

_PERSIST_STAGE_PREFIX = MAP_ADDRESS_TABLE.rsplit('.', 1)[0]
_PERSIST_STAGES: Dict[int, Tuple[DataFrame, str]] = {}

def _persist_stage_materialize(frame: Optional[DataFrame]) -> Optional[DataFrame]:
    """Materialize a frame that is read more than once.

    Serverless has no cache/persist, so an unmaterialized frame is recomputed in full at every
    reference: the address build reads its prepared source and the deduplicated AddressBase
    snapshot once per matching lane, and map_person reads its person snapshot three times.
    Round-tripping through a run-scoped Delta stage buys the same reuse a cache would.

    The stage name starts with map_ and carries __changes_stage_ so the _map_common
    startup sweep collects anything a crashed run leaves behind. The staged frame is the dict key,
    which also keeps it alive so its id() cannot be recycled onto a different object.
    """
    if frame is None:
        return frame
    if id(frame) in _PERSIST_STAGES:
        return frame
    columns = frame.columns
    if len(set(columns)) != len(columns):
        print('[WARN] persist stage skipped: frame has duplicate column names and cannot be staged')
        return frame
    stage_table = f'{_PERSIST_STAGE_PREFIX}.map_persist__changes_stage_{uuid4().hex}'
    try:
        frame.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(stage_table)
    except Exception as stage_error:
        print(f'[WARN] persist stage write failed for {stage_table}; continuing unmaterialized: {str(stage_error)[:500]}')
        try:
            spark.sql(f'DROP TABLE IF EXISTS {_sql_table(stage_table)}')
        except Exception:
            pass
        return frame
    staged = spark.table(stage_table)
    _PERSIST_STAGES[id(staged)] = (staged, stage_table)
    return staged

def _persist_stage_drop(frame: Optional[DataFrame]) -> None:
    if frame is None:
        return
    entry = _PERSIST_STAGES.pop(id(frame), None)
    if entry is None:
        return
    try:
        spark.sql(f'DROP TABLE IF EXISTS {_sql_table(entry[1])}')
    except Exception as drop_error:
        print(f'[WARN] persist stage drop failed for {entry[1]}; the next pipeline startup sweep will retry: {str(drop_error)[:500]}')

def _maybe_persist(df: DataFrame) -> DataFrame:
    return _persist_stage_materialize(df)

def _safe_unpersist(df: DataFrame) -> None:
    _persist_stage_drop(df)

def _register_cache(target_table: str, *dataframes: DataFrame) -> None:
    _PENDING_CACHES.setdefault(target_table, [])
    _PENDING_CACHES[target_table].extend([df for df in dataframes if df is not None])

def _release_caches(target_table: str) -> None:
    for df in _PENDING_CACHES.pop(target_table, []):
        try:
            _safe_unpersist(df)
        except Exception:
            pass

def _ensure_table_schema(target_table: str, target_schema: StructType, table_comment: Optional[str]=None) -> None:
    if not table_exists(target_table):
        builder = DeltaTable.createIfNotExists(spark).tableName(target_table).addColumns(target_schema).property('delta.enableChangeDataFeed', 'true').property('delta.enableRowTracking', 'true')
        if table_comment:
            builder = builder.comment(table_comment)
        builder.execute()
        return
    current_schema = spark.table(target_table).schema
    current_fields = {field.name: field for field in current_schema.fields}
    for field in target_schema.fields:
        if field.name not in current_fields:
            ddl = f'ALTER TABLE {_sql_table(target_table)} ADD COLUMN {chr(96)}{field.name}{chr(96)} {field.dataType.simpleString()}'
            comment = field.metadata.get('comment', '')
            if comment:
                ddl += f" COMMENT '{_escape_sql(comment)}'"
            spark.sql(ddl)
        elif current_fields[field.name].dataType != field.dataType:
            raise TypeError(f'{target_table}.{field.name} is {current_fields[field.name].dataType.simpleString()} but the replacement expects {field.dataType.simpleString()}. Use an explicit reviewed migration rather than an automatic full-table rewrite.')
    if table_comment and _table_description(target_table) != table_comment:
        spark.sql(f"COMMENT ON TABLE {_sql_table(target_table)} IS '{_escape_sql(table_comment)}'")

def create_table_with_schema(source_df: DataFrame, target_table: str, target_schema: StructType=None, table_comment: str=None):
    target_schema = (
        bronze_contract_schema(target_table)
        if bronze_is_primary_table(target_table)
        else (target_schema or source_df.schema)
    )
    _ensure_table_schema(target_table, target_schema, table_comment)
    aligned = _schema_select(source_df, target_schema)
    if aligned.take(1):
        aligned.write.mode('append').saveAsTable(target_table)

def verify_no_duplicates(df: Optional[DataFrame], key_column) -> bool:
    if df is None:
        return True
    keys = [key_column] if isinstance(key_column, str) else list(key_column)
    upserts = df.filter(F.coalesce(F.col('_source_change_type'), F.lit('upsert')) != 'delete') if '_source_change_type' in df.columns else df
    null_condition = reduce(lambda a, b: a | b, [F.col(key).isNull() for key in keys])
    if upserts.filter(null_condition).limit(1).collect():
        print(f'[ERROR] Null merge key detected in {keys}')
        return False
    duplicate = upserts.groupBy(*keys).count().filter(F.col('count') > 1).limit(1).collect()
    if duplicate:
        print(f'[ERROR] Duplicate merge key detected in {keys}: {duplicate[0].asDict()}')
        return False
    print(f'[OK] No duplicate keys detected for {keys}')
    return True

def update_table(source_df: Optional[DataFrame], target_table: str, index_column, target_schema: StructType=None, table_comment: str=None, update_condition=None, full_sync: Optional[bool]=None):
    if source_df is None:
        print(f'[INFO] No updates for {target_table}')
        return
    target_schema = (
        bronze_contract_schema(target_table)
        if bronze_is_primary_table(target_table)
        else (target_schema or StructType([field for field in source_df.schema.fields if not field.name.startswith('_')]))
    )
    _ensure_table_schema(target_table, target_schema, table_comment)
    pending = _PENDING_TABLE_OPTIONS.get(target_table, {})
    if full_sync is None:
        full_sync = bool(pending.get('full_sync', False))
    keys = [index_column] if isinstance(index_column, str) else list(index_column)
    if '_source_change_type' in source_df.columns:
        source_df = _maybe_persist(source_df)
    stage_table = None
    try:
        delete_keys = None
        if '_source_change_type' in source_df.columns:
            delete_keys = source_df.filter(F.col('_source_change_type') == 'delete').select(*keys).dropDuplicates(keys)
            upserts = source_df.filter(F.col('_source_change_type') != 'delete').drop('_source_change_type')
        else:
            upserts = source_df
        stage_table = f'{target_table}__merge_stage_{uuid4().hex}'
        _schema_select(upserts, target_schema).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(stage_table)
        aligned = spark.table(stage_table)
        upsert_count = aligned.count()
        duplicate = aligned.groupBy(*keys).count().where(F.col('count') > 1).limit(1).collect()
        if duplicate:
            raise RuntimeError(f'{target_table} source violates merge grain {keys}: {duplicate[0].asDict(recursive=True)}')
        if full_sync and upsert_count == 0:
            raise RuntimeError(f'Refusing an empty full synchronization of {target_table}. Force deletion explicitly if the source table is genuinely empty.')
        if delete_keys is not None and delete_keys.take(1):
            delete_condition = ' AND '.join([f't.{chr(96)}{key}{chr(96)} = d.{chr(96)}{key}{chr(96)}' for key in keys])
            DeltaTable.forName(spark, target_table).alias('t').merge(delete_keys.alias('d'), delete_condition).whenMatchedDelete().execute()
        if upsert_count > 0 or full_sync:
            merge_condition = ' AND '.join([f't.{chr(96)}{key}{chr(96)} = s.{chr(96)}{key}{chr(96)}' for key in keys])
            target = DeltaTable.forName(spark, target_table)
            merge = target.alias('t').merge(aligned.alias('s'), merge_condition)
            update_columns = [column for column in aligned.columns if column not in keys]
            assignments = {column: F.col(f's.{column}') for column in update_columns}
            insertions = {column: F.col(f's.{column}') for column in aligned.columns}
            if update_condition is None:
                compare_columns = [column for column in update_columns if column not in _VOLATILE_COMPARE_COLUMNS]
                if compare_columns:
                    changed_condition = reduce(lambda a, b: a | b, [~F.col(f't.{column}').eqNullSafe(F.col(f's.{column}')) for column in compare_columns])
                    merge = merge.whenMatchedUpdate(condition=changed_condition, set=assignments)
            else:
                merge = merge.whenMatchedUpdate(condition=update_condition, set=assignments)
            merge = merge.whenNotMatchedInsert(values=insertions)
            if full_sync:
                merge = merge.whenNotMatchedBySourceDelete()
            merge.execute()
            print(f'[SUCCESS] {target_table}: merged {upsert_count:,} source rows (full_sync={full_sync})')
        else:
            print(f'[INFO] {target_table}: no upsert rows')
        _set_table_properties(target_table, pending.get('properties', {}))
        _PENDING_TABLE_OPTIONS.pop(target_table, None)
    finally:
        if stage_table is not None:
            spark.sql(f'DROP TABLE IF EXISTS {_sql_table(stage_table)}')
        _safe_unpersist(source_df)
        _release_caches(target_table)

_CDF_DEFAULT_RETENTION_HOURS = 168.0
_CDF_RETENTION_SAFETY_HOURS = 6.0
_CDF_RETENTION_UNIT_HOURS = {'second': 1.0 / 3600.0, 'seconds': 1.0 / 3600.0, 'minute': 1.0 / 60.0, 'minutes': 1.0 / 60.0, 'hour': 1.0, 'hours': 1.0, 'day': 24.0, 'days': 24.0, 'week': 168.0, 'weeks': 168.0}

def _cdf_retention_hours(table_name: str) -> float:
    """delta.deletedFileRetentionDuration in hours; Delta's 7-day default when unset."""
    try:
        rows = spark.sql(f'SHOW TBLPROPERTIES {_sql_table(table_name)}').collect()
    except Exception:
        return _CDF_DEFAULT_RETENTION_HOURS
    setting = None
    for row in rows:
        if str(row['key']).strip().lower() == 'delta.deletedfileretentionduration':
            setting = str(row['value'])
            break
    if not setting:
        return _CDF_DEFAULT_RETENTION_HOURS
    tokens = setting.replace(',', ' ').split()
    total = 0.0
    matched = False
    index = 0
    while index < len(tokens):
        token = tokens[index].strip('\'"')
        if token.isdigit() and index + 1 < len(tokens):
            per_unit = _CDF_RETENTION_UNIT_HOURS.get(tokens[index + 1].strip('\'"').lower())
            if per_unit is not None:
                total = total + float(token) * per_unit
                matched = True
                index = index + 1
        index = index + 1
    return total if matched else _CDF_DEFAULT_RETENTION_HOURS

def _cdf_retention_floor(table_name: str):
    """Earliest commit Delta will still serve for this table, less a safety margin."""
    try:
        now = spark.sql('SELECT current_timestamp() AS ts').collect()[0]['ts']
    except Exception:
        return None
    return now - timedelta(hours=_cdf_retention_hours(table_name) - _CDF_RETENTION_SAFETY_HOURS)

def _read_latest_cdf(source_table: str, key_columns: Sequence[str], starting_timestamp) -> Optional[DataFrame]:
    if starting_timestamp is None:
        return None
    start = starting_timestamp - timedelta(hours=CDF_OVERLAP_HOURS)
    start_text = start.strftime('%Y-%m-%d %H:%M:%S')
    retention_floor = _cdf_retention_floor(source_table)
    if retention_floor is not None and start < retention_floor:
        print(f'[WARN] CDF start {start_text} for {source_table} is outside its deleted-file retention window (floor {_format_ts(retention_floor)}); falling back to a full synchronization.')
        return None
    try:
        latest_commit = spark.sql(f'DESCRIBE HISTORY {_sql_table(source_table)}').orderBy(F.col('version').desc()).select('timestamp').limit(1).collect()
        if latest_commit and start > latest_commit[0].timestamp:
            print(f'[INFO] No CDF commits for {source_table} after {start_text}; latest commit is {_format_ts(latest_commit[0].timestamp)}')
            return spark.table(source_table).limit(0).withColumn('_source_change_type', F.lit('upsert'))
        changes = spark.read.format('delta').option('readChangeFeed', 'true').option('startingTimestamp', start_text).table(source_table).filter(F.col('_change_type').isin('insert', 'update_postimage', 'delete'))
        changes.limit(1).collect()
        order_window = Window.partitionBy(*key_columns).orderBy(F.col('_commit_version').desc(), F.when(F.col('_change_type') == 'delete', F.lit(1)).otherwise(F.lit(0)).desc())
        return changes.withColumn('_cdf_row_number', F.row_number().over(order_window)).filter(F.col('_cdf_row_number') == 1).withColumn('_source_change_type', F.when(F.col('_change_type') == 'delete', F.lit('delete')).otherwise(F.lit('upsert'))).drop('_change_type', '_commit_version', '_commit_timestamp', '_cdf_row_number')
    except Exception as error:
        print(f'[WARN] CDF read failed for {source_table} from {start_text}; falling back to a full synchronization. {str(error).splitlines()[0]}')
        return None

def _nonblank(column):
    return F.when(column.isNotNull() & (F.trim(column) != ''), F.trim(column))

def _normalize_postcode(column):
    cleaned = F.upper(F.regexp_replace(F.coalesce(column.cast('string'), F.lit('')), '[^A-Za-z0-9]', ''))
    return F.when(cleaned != '', cleaned)

def _normalize_address(column):
    value = F.upper(F.coalesce(column.cast('string'), F.lit('')))
    value = F.regexp_replace(value, '[^A-Z0-9 ]', ' ')
    replacements = [('\\bST\\b', 'STREET'), ('\\bRD\\b', 'ROAD'), ('\\bAVE\\b', 'AVENUE'), ('\\bGDNS\\b', 'GARDENS'), ('\\bCRES\\b', 'CRESCENT'), ('\\bTER\\b', 'TERRACE'), ('\\bSQ\\b', 'SQUARE'), ('\\bCT\\b', 'COURT'), ('\\bPL\\b', 'PLACE'), ('\\bBLDG\\b', 'BUILDING'), ('\\bHSE\\b', 'HOUSE'), ('\\bFLT\\b', 'FLAT'), ('\\bAPT\\b', 'APARTMENT')]
    for pattern, replacement in replacements:
        value = F.regexp_replace(value, pattern, replacement)
    return F.trim(F.regexp_replace(value, '\\s+', ' '))

def _strip_subbuilding_label(column):
    return F.regexp_replace(_normalize_address(column), '^(FLAT|APARTMENT|UNIT)\\s+', '')

def _line_building_number(line_column):
    normalized = _normalize_address(line_column)
    direct = F.regexp_extract(normalized, '^(\\d+[A-Z]?)\\b', 1)
    after_subbuilding = F.regexp_extract(normalized, '^(?:FLAT|APARTMENT|UNIT)\\s+[A-Z0-9/-]+(?:\\s+|,\\s*)(\\d+[A-Z]?)\\b', 1)
    candidate = F.when(normalized.rlike('^(FLAT|APARTMENT|UNIT)\\s+'), after_subbuilding).otherwise(direct)
    return F.when(candidate != '', candidate)

def _extract_subbuilding(full_address_column):
    extracted = F.regexp_extract(_normalize_address(full_address_column), '(?:^|\\s)(?:FLAT|APARTMENT|UNIT)\\s+([A-Z0-9/-]+)\\b', 1)
    return F.when(extracted != '', extracted)

def _address_reference_properties() -> Dict[str, str]:
    return {'pipeline.map.schema_version': MAP_SCHEMA_VERSION, 'pipeline.map.match_algorithm_version': MATCH_ALGORITHM_VERSION, 'pipeline.map.addressbase_data_version': _latest_data_version(ADDRESSBASE_TABLE), 'pipeline.map.postcode_map_data_version': _latest_data_version(POSTCODE_MAP_TABLE), 'pipeline.map.imd_data_version': _latest_data_version(IMD_TABLE), 'pipeline.map.code_value_data_version': _latest_data_version(CODE_VALUE_TABLE)}

def _address_requires_full_sync(force_full: Optional[bool]) -> Tuple[bool, Dict[str, str]]:
    properties = _address_reference_properties()
    if force_full is True or not table_exists(MAP_ADDRESS_TABLE):
        return (True, properties)
    # Only retained single-layer columns may participate in the compatibility gate.
    required_columns = {'ADDRESS_ID', 'UPRN', 'MATCH_CANDIDATE_COUNT', 'COUNTRY_CODE_VALUE', 'ADC_UPDT'}
    if not required_columns.issubset(_table_columns(MAP_ADDRESS_TABLE)):
        return (True, properties)
    for key, current_value in properties.items():
        if _get_table_property(MAP_ADDRESS_TABLE, key) != current_value:
            print(f'[INFO] Full address synchronization required because {key} changed')
            return (True, properties)
    return (False, properties)

def _prepare_raw_addresses(raw: DataFrame, include_sensitive_address_columns: bool) -> DataFrame:
    raw_full_address = F.trim(F.regexp_replace(F.concat_ws(' ', _nonblank(F.col('STREET_ADDR')), _nonblank(F.col('STREET_ADDR2')), _nonblank(F.col('STREET_ADDR3')), _nonblank(F.col('STREET_ADDR4'))), '\\s+', ' '))
    postcode_source = F.coalesce(_nonblank(F.col('ZIPCODE')), _nonblank(F.col('POSTAL_IDENTIFIER')))
    clean_postcode = _normalize_postcode(postcode_source)
    outward = F.when(F.length(clean_postcode).between(5, 7), F.substring(clean_postcode, 1, F.length(clean_postcode) - 3))
    sector = F.when(F.length(clean_postcode).between(5, 7), F.substring(clean_postcode, 1, F.length(clean_postcode) - 2))
    is_person = F.upper(F.coalesce(F.col('PARENT_ENTITY_NAME'), F.lit(''))).contains('PERSON')
    can_expose = F.lit(bool(include_sensitive_address_columns)) | ~is_person
    line_norms = F.array(_normalize_address(F.col('STREET_ADDR')), _normalize_address(F.col('STREET_ADDR2')), _normalize_address(F.col('STREET_ADDR3')), _normalize_address(F.col('STREET_ADDR4')))
    source_full_norm = _normalize_address(F.concat_ws(' ', raw_full_address, _nonblank(F.col('CITY'))))
    building_number = F.coalesce(_line_building_number(F.col('STREET_ADDR')), _line_building_number(F.col('STREET_ADDR2')), _line_building_number(F.col('STREET_ADDR3')), _line_building_number(F.col('STREET_ADDR4')))
    return raw.withColumn('ADDRESS_ID', F.col('ADDRESS_ID').cast('long')).withColumn('PARENT_ENTITY_ID', F.col('PARENT_ENTITY_ID').cast('long')).withColumn('clean_zipcode', clean_postcode).withColumn('POSTCODE_OUTWARD', outward).withColumn('POSTCODE_SECTOR', sector).withColumn('masked_zipcode', F.when(is_person, outward).otherwise(_nonblank(postcode_source))).withColumn('SOURCE_ZIPCODE_MASK3', F.substring(F.col('ZIPCODE'), 1, 3)).withColumn('SOURCE_ZIPCODE', F.when(can_expose, _nonblank(postcode_source)).otherwise(outward)).withColumn('full_street_address', F.when(can_expose, raw_full_address).otherwise(F.lit(''))).withColumn('SOURCE_STREET_ADDR1', F.when(can_expose, F.col('STREET_ADDR'))).withColumn('SOURCE_STREET_ADDR2', F.when(can_expose, F.col('STREET_ADDR2'))).withColumn('SOURCE_STREET_ADDR3', F.when(can_expose, F.col('STREET_ADDR3'))).withColumn('SOURCE_STREET_ADDR4', F.when(can_expose, F.col('STREET_ADDR4'))).withColumn('_source_full_norm', source_full_norm).withColumn('_source_line_norms', line_norms).withColumn('_source_building_number', building_number).withColumn('_source_subbuilding_norm', _extract_subbuilding(raw_full_address)).withColumn('CITY_CODE_VALUE', F.col('CITY_CD').cast('long')).withColumn('STATE_CODE_VALUE', F.col('STATE_CD').cast('long')).withColumn('COUNTY_CODE_VALUE', F.col('COUNTY_CD').cast('long')).withColumn('COUNTRY_CODE_VALUE', F.col('COUNTRY_CD').cast('long')).withColumn('ADDRESS_TYPE_CODE', F.col('ADDRESS_TYPE_CD').cast('long')).withColumn('ACTIVE_STATUS_CODE', F.col('ACTIVE_STATUS_CD').cast('long')).withColumn('DATA_STATUS_CODE', F.col('DATA_STATUS_CD').cast('long')).withColumn('ADDRESS_INFO_STATUS_CODE', F.col('ADDRESS_INFO_STATUS_CD').cast('long')).withColumn('CONTRIBUTOR_SYSTEM_CODE', F.col('CONTRIBUTOR_SYSTEM_CD').cast('long')).withColumn('SOURCE_UPDT_DT_TM', F.col('UPDT_DT_TM')).withColumn('SOURCE_UPDT_CNT', F.col('UPDT_CNT').cast('long')).withColumn('INST_ID', F.col('INST_ID').cast('long')).withColumn('ACTIVE_IND', F.col('ACTIVE_IND').cast('long'))

def _prepare_addressbase() -> DataFrame:
    base = spark.table(ADDRESSBASE_TABLE).filter((F.col('address_quality') == 'VALID') & F.col('UPRN').isNotNull()).withColumn('_ab_row_number', F.row_number().over(Window.partitionBy('UPRN').orderBy(F.col('ADC_UPDT').desc_nulls_last(), F.col('POSTCODE').desc_nulls_last()))).filter(F.col('_ab_row_number') == 1).drop('_ab_row_number').withColumn('_ab_postcode_clean', _normalize_postcode(F.col('POSTCODE'))).withColumn('_ab_building_number', F.upper(F.regexp_replace(F.trim(F.coalesce(F.col('BUILDING_NUMBER'), F.lit(''))), '\\s+', ''))).withColumn('_ab_subbuilding_norm', _strip_subbuilding_label(F.col('SUB_BUILDING_NAME'))).withColumn('_ab_building_name_norm', _normalize_address(F.col('BUILDING_NAME'))).withColumn('_ab_thoroughfare_norm', _normalize_address(F.col('THOROUGHFARE'))).withColumn('_ab_full_norm', _normalize_address(F.coalesce(_nonblank(F.col('standardized_address')), F.concat_ws(' ', F.col('SUB_BUILDING_NAME'), F.col('BUILDING_NAME'), F.col('BUILDING_NUMBER'), F.col('DEPENDENT_THOROUGHFARE'), F.col('THOROUGHFARE'), F.col('DOUBLE_DEPENDENT_LOCALITY'), F.col('DEPENDENT_LOCALITY'), F.col('POST_TOWN')))))
    return base

def _candidate_projection(joined: DataFrame, algorithm: int, confidence, quality: str, candidate_count, score_margin, similarity, number_exact, subbuilding_exact, building_name_exact) -> DataFrame:
    return joined.select(F.col('p.ADDRESS_ID').alias('ADDRESS_ID'), F.col('a.UPRN').cast('long').alias('UPRN'), F.col('a.UDPRN').alias('UDPRN'), F.col('a.LATITUDE').cast('double').alias('LATITUDE'), F.col('a.LONGITUDE').cast('double').alias('LONGITUDE'), F.col('a.POSTCODE').alias('ADDRESSBASE_POSTCODE'), F.col('a.LOGICAL_STATUS').alias('ADDRESSBASE_LOGICAL_STATUS'), F.col('a.BLPU_STATE').alias('ADDRESSBASE_BLPU_STATE'), F.col('a.COUNTRY').alias('ADDRESSBASE_COUNTRY'), F.col('a.ADMINISTRATIVE_AREA').alias('ADDRESSBASE_ADMINISTRATIVE_AREA'), F.col('a.ADC_UPDT').alias('ADDRESSBASE_ADC_UPDT'), F.lit(algorithm).cast('int').alias('match_algorithm'), confidence.cast('double').alias('match_confidence'), F.lit(quality).alias('match_quality'), candidate_count.cast('int').alias('MATCH_CANDIDATE_COUNT'), score_margin.cast('double').alias('MATCH_SCORE_MARGIN'), F.lit(True).alias('MATCH_POSTCODE_EXACT'), number_exact.cast('boolean').alias('MATCH_BUILDING_NUMBER_EXACT'), subbuilding_exact.cast('boolean').alias('MATCH_SUB_BUILDING_EXACT'), building_name_exact.cast('boolean').alias('MATCH_BUILDING_NAME_EXACT'), similarity.cast('double').alias('MATCH_ADDRESS_SIMILARITY'))

def _resolve_uprn_matches(addresses: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
    prepared = _maybe_persist(addresses)
    addressbase = _maybe_persist(_prepare_addressbase())
    full_counts = addressbase.filter(F.col('_ab_postcode_clean').isNotNull() & (F.col('_ab_full_norm') != '')).groupBy('_ab_postcode_clean', '_ab_full_norm').agg(F.countDistinct('UPRN').alias('_candidate_count')).filter(F.col('_candidate_count') == 1)
    full_unique = addressbase.join(full_counts, ['_ab_postcode_clean', '_ab_full_norm'], 'inner')
    exact_join = prepared.alias('p').join(full_unique.alias('a'), (F.col('p.clean_zipcode') == F.col('a._ab_postcode_clean')) & (F.col('p._source_full_norm') == F.col('a._ab_full_norm')) & (F.col('p._source_full_norm') != ''), 'inner')
    exact_matches = _candidate_projection(exact_join, 1, F.lit(1.0), 'Unique exact full address', F.lit(1), F.lit(1.0), F.lit(1.0), F.col('p._source_building_number').eqNullSafe(F.col('a._ab_building_number')) & F.col('p._source_building_number').isNotNull(), F.col('p._source_subbuilding_norm').eqNullSafe(F.col('a._ab_subbuilding_norm')) & F.col('p._source_subbuilding_norm').isNotNull(), (F.col('a._ab_building_name_norm') != '') & F.expr('instr(p._source_full_norm, a._ab_building_name_norm) > 0')).dropDuplicates(['ADDRESS_ID'])
    unresolved_1 = prepared.join(exact_matches.select('ADDRESS_ID'), 'ADDRESS_ID', 'left_anti')
    number_counts = addressbase.filter(F.col('_ab_postcode_clean').isNotNull() & (F.col('_ab_building_number') != '')).groupBy('_ab_postcode_clean', '_ab_building_number').agg(F.countDistinct('UPRN').alias('_candidate_count'))
    number_unique = addressbase.join(number_counts.filter(F.col('_candidate_count') == 1), ['_ab_postcode_clean', '_ab_building_number'], 'inner')
    number_unique_join = unresolved_1.alias('p').join(number_unique.alias('a'), (F.col('p.clean_zipcode') == F.col('a._ab_postcode_clean')) & (F.col('p._source_building_number') == F.col('a._ab_building_number')) & F.col('p._source_building_number').isNotNull(), 'inner')
    number_unique_matches = _candidate_projection(number_unique_join, 2, F.lit(0.95), 'Unique postcode and building number', F.lit(1), F.lit(1.0), F.lit(None).cast('double'), F.lit(True), F.col('p._source_subbuilding_norm').eqNullSafe(F.col('a._ab_subbuilding_norm')) & F.col('p._source_subbuilding_norm').isNotNull(), (F.col('a._ab_building_name_norm') != '') & F.expr('instr(p._source_full_norm, a._ab_building_name_norm) > 0')).dropDuplicates(['ADDRESS_ID'])
    unresolved_2 = unresolved_1.join(number_unique_matches.select('ADDRESS_ID'), 'ADDRESS_ID', 'left_anti')
    ambiguous_reference = addressbase.join(number_counts.filter(F.col('_candidate_count') > 1), ['_ab_postcode_clean', '_ab_building_number'], 'inner')
    ambiguous_candidates = unresolved_2.alias('p').join(ambiguous_reference.alias('a'), (F.col('p.clean_zipcode') == F.col('a._ab_postcode_clean')) & (F.col('p._source_building_number') == F.col('a._ab_building_number')) & F.col('p._source_building_number').isNotNull(), 'inner').withColumn('_similarity_denominator', F.greatest(F.length(F.col('p._source_full_norm')), F.length(F.col('a._ab_full_norm')))).withColumn('_address_similarity', F.when(F.col('_similarity_denominator') > 0, F.lit(1.0) - F.levenshtein(F.col('p._source_full_norm'), F.col('a._ab_full_norm')) / F.col('_similarity_denominator'))).withColumn('_subbuilding_exact', F.col('p._source_subbuilding_norm').isNotNull() & (F.col('a._ab_subbuilding_norm') != '') & (F.col('p._source_subbuilding_norm') == F.col('a._ab_subbuilding_norm'))).withColumn('_building_name_exact', (F.col('a._ab_building_name_norm') != '') & F.expr('instr(p._source_full_norm, a._ab_building_name_norm) > 0')).withColumn('_thoroughfare_exact', (F.col('a._ab_thoroughfare_norm') != '') & F.expr('instr(p._source_full_norm, a._ab_thoroughfare_norm) > 0')).withColumn('_candidate_score', F.least(F.lit(1.0), F.coalesce(F.col('_address_similarity'), F.lit(0.0)) * F.lit(0.75) + F.when(F.col('_subbuilding_exact'), F.lit(0.15)).otherwise(F.lit(0.0)) + F.when(F.col('_building_name_exact'), F.lit(0.07)).otherwise(F.lit(0.0)) + F.when(F.col('_thoroughfare_exact'), F.lit(0.03)).otherwise(F.lit(0.0))))
    ambiguous_order = Window.partitionBy(F.col('p.ADDRESS_ID')).orderBy(F.col('_candidate_score').desc(), F.col('a.UPRN').asc())
    ambiguous_partition = Window.partitionBy(F.col('p.ADDRESS_ID'))
    ambiguous_ranked = ambiguous_candidates.withColumn('_candidate_rank', F.row_number().over(ambiguous_order)).withColumn('_second_score', F.lead('_candidate_score', 1).over(ambiguous_order)).withColumn('_actual_candidate_count', F.count('*').over(ambiguous_partition)).withColumn('_score_margin', F.col('_candidate_score') - F.coalesce(F.col('_second_score'), F.lit(0.0)))
    ambiguous_top = ambiguous_ranked.filter(F.col('_candidate_rank') == 1)
    ambiguous_diagnostics = ambiguous_top.select(F.col('p.ADDRESS_ID').alias('ADDRESS_ID'), F.col('_actual_candidate_count').cast('int').alias('_DIAG_CANDIDATE_COUNT'), F.col('_score_margin').cast('double').alias('_DIAG_SCORE_MARGIN'), F.col('_address_similarity').cast('double').alias('_DIAG_SIMILARITY'), F.col('_subbuilding_exact').alias('_DIAG_SUBBUILDING'), F.col('_building_name_exact').alias('_DIAG_BUILDING_NAME'))
    accepted_ambiguous = ambiguous_top.filter(F.col('_subbuilding_exact') & (F.col('_address_similarity') >= F.lit(0.65)) & (F.col('_score_margin') >= F.lit(0.02)) | F.col('_building_name_exact') & F.col('_thoroughfare_exact') & (F.col('_address_similarity') >= F.lit(0.7)) & (F.col('_score_margin') >= F.lit(0.03)) | (F.col('_address_similarity') >= F.lit(0.94)) & (F.col('_score_margin') >= F.lit(0.05)))
    ambiguous_matches = _candidate_projection(accepted_ambiguous, 3, F.col('_candidate_score'), 'Ambiguous number resolved by components', F.col('_actual_candidate_count'), F.col('_score_margin'), F.col('_address_similarity'), F.lit(True), F.col('_subbuilding_exact'), F.col('_building_name_exact')).dropDuplicates(['ADDRESS_ID'])
    unresolved_3 = unresolved_2.join(ambiguous_matches.select('ADDRESS_ID'), 'ADDRESS_ID', 'left_anti')
    addressbase_components = addressbase.select('_ab_postcode_clean', 'UPRN', F.explode(F.array('_ab_subbuilding_norm', '_ab_building_name_norm', '_ab_thoroughfare_norm', _normalize_address(F.col('ORGANISATION_NAME')))).alias('_component_key')).filter(F.col('_component_key').isNotNull() & (F.col('_component_key') != ''))
    component_unique = addressbase_components.groupBy('_ab_postcode_clean', '_component_key').agg(F.countDistinct('UPRN').alias('_component_candidate_count'), F.min('UPRN').alias('_component_uprn')).filter(F.col('_component_candidate_count') == 1)
    source_components = unresolved_3.select('ADDRESS_ID', 'clean_zipcode', F.explode('_source_line_norms').alias('_component_key')).filter(F.col('_component_key').isNotNull() & (F.col('_component_key') != ''))
    component_hits = source_components.alias('s').join(component_unique.alias('u'), (F.col('s.clean_zipcode') == F.col('u._ab_postcode_clean')) & (F.col('s._component_key') == F.col('u._component_key')), 'inner').groupBy(F.col('s.ADDRESS_ID').alias('ADDRESS_ID')).agg(F.countDistinct('u._component_uprn').alias('_resolved_uprn_count'), F.min('u._component_uprn').alias('_component_uprn')).filter(F.col('_resolved_uprn_count') == 1)
    component_join = unresolved_3.join(component_hits, 'ADDRESS_ID', 'inner').alias('p').join(addressbase.alias('a'), F.col('p._component_uprn') == F.col('a.UPRN'), 'inner').withColumn('_component_similarity_denominator', F.greatest(F.length(F.col('p._source_full_norm')), F.length(F.col('a._ab_full_norm')))).withColumn('_component_similarity', F.when(F.col('_component_similarity_denominator') > 0, F.lit(1.0) - F.levenshtein(F.col('p._source_full_norm'), F.col('a._ab_full_norm')) / F.col('_component_similarity_denominator')))
    component_matches = _candidate_projection(component_join, 4, F.lit(0.9), 'Unique exact postcode and address component', F.lit(1), F.lit(1.0), F.col('_component_similarity'), F.col('p._source_building_number').eqNullSafe(F.col('a._ab_building_number')) & F.col('p._source_building_number').isNotNull(), F.col('p._source_subbuilding_norm').eqNullSafe(F.col('a._ab_subbuilding_norm')) & F.col('p._source_subbuilding_norm').isNotNull(), (F.col('a._ab_building_name_norm') != '') & F.expr('instr(p._source_full_norm, a._ab_building_name_norm) > 0')).dropDuplicates(['ADDRESS_ID'])
    matches = exact_matches.unionByName(number_unique_matches).unionByName(ambiguous_matches).unionByName(component_matches)
    return (matches, ambiguous_diagnostics, addressbase)

def _prepare_code_lookup() -> DataFrame:
    selected = spark.table(CODE_VALUE_TABLE).select(F.col('CODE_VALUE').cast('double').alias('_code_value'), F.col('DESCRIPTION').alias('_code_description'), F.col('DISPLAY').alias('_code_display'), F.col('ADC_UPDT').alias('_code_adc_updt'))
    ordering = Window.partitionBy('_code_value').orderBy(F.col('_code_adc_updt').desc_nulls_last(), F.col('_code_description').desc_nulls_last(), F.col('_code_display').desc_nulls_last())
    return selected.withColumn('_code_row_number', F.row_number().over(ordering)).where(F.col('_code_row_number') == 1).drop('_code_row_number')

def _prepare_postcode_map() -> DataFrame:
    postcode = spark.table(POSTCODE_MAP_TABLE).withColumn('_postcode_clean', _normalize_postcode(F.col('pcd7'))).withColumn('_postcode_record_count', F.count('*').over(Window.partitionBy('_postcode_clean'))).withColumn('_postcode_row_number', F.row_number().over(Window.partitionBy('_postcode_clean').orderBy(F.when(F.col('doterm').isNull() | (F.col('doterm') == 0), F.lit(0)).otherwise(F.lit(1)), F.col('dointr').desc_nulls_last(), F.col('ADC_UPDT').desc_nulls_last(), F.col('lsoa21cd').asc_nulls_last()))).filter(F.col('_postcode_row_number') == 1)
    return postcode.select('_postcode_clean', F.col('oa21cd').alias('OA21CD'), F.col('lsoa21cd').alias('LSOA'), F.col('msoa21cd').alias('MSOA21CD'), F.col('ladcd').alias('LADCD'), F.col('lsoa21nm').alias('LSOA21NM'), F.col('msoa21nm').alias('MSOA21NM'), F.col('ladnm').alias('LADNM'), F.col('ADC_UPDT').alias('POSTCODE_MAP_ADC_UPDT'), '_postcode_record_count')

def _prepare_imd() -> DataFrame:
    imd = spark.table(IMD_TABLE).filter((F.col('DateCode') == 2019) & (F.col('Indices_of_Deprivation') == 'a. Index of Multiple Deprivation (IMD)')).withColumn('_measurement', F.regexp_replace(F.trim(F.col('Measurement')), '\\s+', '')).groupBy('FeatureCode').pivot('_measurement', ['Decile', 'Rank', 'Score']).agg(F.max('Value'))
    imd_updates = spark.table(IMD_TABLE).filter((F.col('DateCode') == 2019) & (F.col('Indices_of_Deprivation') == 'a. Index of Multiple Deprivation (IMD)')).groupBy('FeatureCode').agg(F.max('ADC_UPDT').alias('IMD_LOOKUP_ADC_UPDT'))
    return imd.join(imd_updates, 'FeatureCode', 'left').select('FeatureCode', F.col('Decile').cast('int').alias('IMD_DECILE_NUM'), F.col('Rank').cast('int').alias('IMD_RANK'), F.col('Score').cast('double').alias('IMD_SCORE'), 'IMD_LOOKUP_ADC_UPDT')

def _mad_address_in_scope() -> F.Column:
    """
    Canonical map_address scope.

    PERSON rows are kept across their full address history so that historical
    person addresses stay analysable. ORGANIZATION rows are kept only while they
    are currently effective. Every other PARENT_ENTITY_NAME is out of scope and
    remains available in 4_prod.raw.mill_address.
    """
    entity = F.upper(F.trim(F.coalesce(F.col('PARENT_ENTITY_NAME'), F.lit(''))))
    org_current = (F.col('ACTIVE_IND').cast('long') == 1) & (F.col('END_EFFECTIVE_DT_TM').isNull() | (F.col('END_EFFECTIVE_DT_TM') > F.current_timestamp()))
    return (entity == F.lit('PERSON')) | ((entity == F.lit('ORGANIZATION')) & org_current)

def create_address_mapping_incr(full_refresh: Optional[bool]=None, include_sensitive_address_columns: bool=False) -> Optional[DataFrame]:
    full_sync, properties = _address_requires_full_sync(full_refresh)
    if full_sync:
        source = spark.table(RAW_ADDRESS_TABLE).withColumn('_source_change_type', F.lit('upsert'))
    else:
        source = _read_latest_cdf(RAW_ADDRESS_TABLE, ['ADDRESS_ID'], _last_successful_merge_timestamp(MAP_ADDRESS_TABLE))
        if source is None:
            full_sync = True
            source = spark.table(RAW_ADDRESS_TABLE).withColumn('_source_change_type', F.lit('upsert'))
    if not source.take(1):
        print('[INFO] No address changes to process')
        return None
    delete_keys = source.filter(F.col('_source_change_type') == 'delete').select(F.col('ADDRESS_ID').cast('long').alias('ADDRESS_ID')).dropDuplicates(['ADDRESS_ID'])
    _address_upserts_all = source.filter(F.col('_source_change_type') != 'delete').drop('_source_change_type')
    _address_scope = _mad_address_in_scope()
    delete_keys = delete_keys.unionByName(_address_upserts_all.filter(~_address_scope).select(F.col('ADDRESS_ID').cast('long').alias('ADDRESS_ID'))).dropDuplicates(['ADDRESS_ID'])
    upsert_source = _address_upserts_all.filter(_address_scope)
    if not upsert_source.take(1):
        result = _delete_rows(delete_keys, schema_map_address)
        _PENDING_TABLE_OPTIONS[MAP_ADDRESS_TABLE] = {'full_sync': False, 'properties': properties}
        return result
    addresses = _maybe_persist(_prepare_raw_addresses(upsert_source, include_sensitive_address_columns=include_sensitive_address_columns))
    matches, ambiguity_diagnostics, addressbase = _resolve_uprn_matches(addresses)
    matched = addresses.alias('p').join(matches.alias('m'), 'ADDRESS_ID', 'left').join(ambiguity_diagnostics.alias('d'), 'ADDRESS_ID', 'left').withColumn('UPRN', F.col('m.UPRN')).withColumn('UDPRN', F.col('m.UDPRN')).withColumn('LATITUDE', F.col('m.LATITUDE')).withColumn('LONGITUDE', F.col('m.LONGITUDE')).withColumn('ADDRESSBASE_POSTCODE', F.col('m.ADDRESSBASE_POSTCODE')).withColumn('ADDRESSBASE_LOGICAL_STATUS', F.col('m.ADDRESSBASE_LOGICAL_STATUS')).withColumn('ADDRESSBASE_BLPU_STATE', F.col('m.ADDRESSBASE_BLPU_STATE')).withColumn('ADDRESSBASE_COUNTRY', F.col('m.ADDRESSBASE_COUNTRY')).withColumn('ADDRESSBASE_ADMINISTRATIVE_AREA', F.col('m.ADDRESSBASE_ADMINISTRATIVE_AREA')).withColumn('ADDRESSBASE_ADC_UPDT', F.col('m.ADDRESSBASE_ADC_UPDT')).withColumn('match_algorithm', F.coalesce(F.col('m.match_algorithm'), F.lit(0))).withColumn('match_confidence', F.coalesce(F.col('m.match_confidence'), F.lit(0.0))).withColumn('match_quality', F.coalesce(F.col('m.match_quality'), F.lit('No accepted match'))).withColumn('MATCH_CANDIDATE_COUNT', F.coalesce(F.col('m.MATCH_CANDIDATE_COUNT'), F.col('d._DIAG_CANDIDATE_COUNT'), F.lit(0))).withColumn('MATCH_SCORE_MARGIN', F.coalesce(F.col('m.MATCH_SCORE_MARGIN'), F.col('d._DIAG_SCORE_MARGIN'))).withColumn('MATCH_POSTCODE_EXACT', F.coalesce(F.col('m.MATCH_POSTCODE_EXACT'), F.lit(False))).withColumn('MATCH_BUILDING_NUMBER_EXACT', F.coalesce(F.col('m.MATCH_BUILDING_NUMBER_EXACT'), F.lit(False))).withColumn('MATCH_SUB_BUILDING_EXACT', F.coalesce(F.col('m.MATCH_SUB_BUILDING_EXACT'), F.col('d._DIAG_SUBBUILDING'), F.lit(False))).withColumn('MATCH_BUILDING_NAME_EXACT', F.coalesce(F.col('m.MATCH_BUILDING_NAME_EXACT'), F.col('d._DIAG_BUILDING_NAME'), F.lit(False))).withColumn('MATCH_ADDRESS_SIMILARITY', F.coalesce(F.col('m.MATCH_ADDRESS_SIMILARITY'), F.col('d._DIAG_SIMILARITY'))).withColumn('MATCH_FAILURE_REASON', F.when(F.col('UPRN').isNotNull(), F.lit(None).cast('string')).when(F.col('p.clean_zipcode').isNull(), F.lit('NO_USABLE_FULL_POSTCODE')).when(F.col('d._DIAG_CANDIDATE_COUNT') > 1, F.lit('AMBIGUOUS_CANDIDATES_BELOW_ACCEPTANCE_THRESHOLD')).when(F.col('p._source_building_number').isNull() & (F.col('p._source_full_norm') == ''), F.lit('INSUFFICIENT_ADDRESS_COMPONENTS')).otherwise(F.lit('NO_UNIQUE_ACCEPTABLE_CANDIDATE'))).withColumn('MATCH_ALGORITHM_VERSION', F.lit(MATCH_ALGORITHM_VERSION)).withColumn('MATCH_RUN_TS', F.current_timestamp())
    code_lookup = _prepare_code_lookup()
    country_lookup = code_lookup.select(F.col('_code_value').alias('_country_code'), F.col('_code_description').alias('_country_description'), F.col('_code_display').alias('_country_display'), F.col('_code_adc_updt').alias('COUNTRY_LOOKUP_ADC_UPDT'))
    county_lookup = code_lookup.select(F.col('_code_value').alias('_county_code'), F.col('_code_description').alias('_county_description'), F.col('_code_display').alias('_county_display'))
    decoded = matched.alias('x').join(F.broadcast(country_lookup).alias('country'), F.col('x.COUNTRY_CD').cast('double') == F.col('country._country_code'), 'left').join(F.broadcast(county_lookup).alias('county'), F.col('x.COUNTY_CD').cast('double') == F.col('county._county_code'), 'left').withColumn('COUNTRY_RAW', F.col('x.COUNTRY')).withColumn('COUNTRY_DESCRIPTION', F.coalesce(_nonblank(F.col('country._country_description')), _nonblank(F.col('country._country_display')), _nonblank(F.col('x.COUNTRY')))).withColumn('country_cd', F.col('COUNTRY_DESCRIPTION')).withColumn('COUNTY', F.coalesce(_nonblank(F.col('x.COUNTY')), _nonblank(F.col('county._county_description')), _nonblank(F.col('county._county_display'))))
    postcode_map = _prepare_postcode_map()
    source_geo = postcode_map.select(F.col('_postcode_clean').alias('_source_geo_postcode'), *[F.col(column).alias(f'_source_{column}') for column in ['OA21CD', 'LSOA', 'MSOA21CD', 'LADCD', 'LSOA21NM', 'MSOA21NM', 'LADNM', 'POSTCODE_MAP_ADC_UPDT']])
    addressbase_geo = postcode_map.select(F.col('_postcode_clean').alias('_addressbase_geo_postcode'), *[F.col(column).alias(f'_addressbase_{column}') for column in ['OA21CD', 'LSOA', 'MSOA21CD', 'LADCD', 'LSOA21NM', 'MSOA21NM', 'LADNM', 'POSTCODE_MAP_ADC_UPDT']])
    geography = decoded.alias('x').join(source_geo.alias('sg'), F.col('x.clean_zipcode') == F.col('sg._source_geo_postcode'), 'left').join(addressbase_geo.alias('ag'), _normalize_postcode(F.col('x.ADDRESSBASE_POSTCODE')) == F.col('ag._addressbase_geo_postcode'), 'left').withColumn('OA21CD', F.coalesce(F.col('sg._source_OA21CD'), F.col('ag._addressbase_OA21CD'))).withColumn('LSOA', F.coalesce(F.col('sg._source_LSOA'), F.col('ag._addressbase_LSOA'))).withColumn('MSOA21CD', F.coalesce(F.col('sg._source_MSOA21CD'), F.col('ag._addressbase_MSOA21CD'))).withColumn('LADCD', F.coalesce(F.col('sg._source_LADCD'), F.col('ag._addressbase_LADCD'))).withColumn('LSOA21NM', F.coalesce(F.col('sg._source_LSOA21NM'), F.col('ag._addressbase_LSOA21NM'))).withColumn('MSOA21NM', F.coalesce(F.col('sg._source_MSOA21NM'), F.col('ag._addressbase_MSOA21NM'))).withColumn('LADNM', F.coalesce(F.col('sg._source_LADNM'), F.col('ag._addressbase_LADNM'))).withColumn('POSTCODE_MAP_ADC_UPDT', F.coalesce(F.col('sg._source_POSTCODE_MAP_ADC_UPDT'), F.col('ag._addressbase_POSTCODE_MAP_ADC_UPDT'))).withColumn('LSOA_MATCH_METHOD', F.when(F.col('sg._source_LSOA').isNotNull(), F.lit('EXACT_SOURCE_POSTCODE')).when(F.col('ag._addressbase_LSOA').isNotNull(), F.lit('ADDRESSBASE_POSTCODE')).otherwise(F.lit('NONE')))
    imd = _prepare_imd()
    final_upserts = geography.join(imd, geography.LSOA == imd.FeatureCode, 'left').withColumn('IMD_QUINTILE_NUM', F.when(F.col('IMD_DECILE_NUM').between(1, 2), F.lit(1)).when(F.col('IMD_DECILE_NUM').between(3, 4), F.lit(2)).when(F.col('IMD_DECILE_NUM').between(5, 6), F.lit(3)).when(F.col('IMD_DECILE_NUM').between(7, 8), F.lit(4)).when(F.col('IMD_DECILE_NUM').between(9, 10), F.lit(5))).withColumn('IMD_Decile', F.col('IMD_DECILE_NUM').cast('string')).withColumn('IMD_Quintile', F.col('IMD_QUINTILE_NUM').cast('string')).withColumn('IMD_YEAR', F.lit(2019)).withColumn('GEOGRAPHY_VERSION', F.lit('2021')).withColumn('PIPELINE_LOADED_AT', F.current_timestamp())
    final_upserts = _schema_select(final_upserts, schema_map_address).withColumn('_source_change_type', F.lit('upsert'))
    result = final_upserts
    if delete_keys.take(1):
        result = result.unionByName(_delete_rows(delete_keys, schema_map_address), allowMissingColumns=True)
    _PENDING_TABLE_OPTIONS[MAP_ADDRESS_TABLE] = {'full_sync': full_sync, 'properties': properties}
    _register_cache(MAP_ADDRESS_TABLE, addresses, addressbase)
    return result
_EPC_DIRECT_COLUMNS = ['CURRENT_ENERGY_RATING', 'POTENTIAL_ENERGY_RATING', 'CURRENT_ENERGY_EFFICIENCY', 'POTENTIAL_ENERGY_EFFICIENCY', 'ENVIRONMENT_IMPACT_CURRENT', 'ENVIRONMENT_IMPACT_POTENTIAL', 'WALLS_DESCRIPTION', 'WALLS_ENERGY_EFF', 'WALLS_ENV_EFF', 'ROOF_DESCRIPTION', 'ROOF_ENERGY_EFF', 'ROOF_ENV_EFF', 'FLOOR_DESCRIPTION', 'FLOOR_ENERGY_EFF', 'FLOOR_ENV_EFF', 'WINDOWS_DESCRIPTION', 'WINDOWS_ENERGY_EFF', 'WINDOWS_ENV_EFF', 'MULTI_GLAZE_PROPORTION', 'GLAZED_TYPE', 'GLAZED_AREA', 'MAINHEAT_DESCRIPTION', 'MAINHEAT_ENERGY_EFF', 'MAINHEAT_ENV_EFF', 'MAIN_FUEL', 'MAIN_HEATING_CONTROLS', 'MAINHEATCONT_DESCRIPTION', 'MAINHEATC_ENERGY_EFF', 'MAINHEATC_ENV_EFF', 'SECONDHEAT_DESCRIPTION', 'MAINS_GAS_FLAG', 'HOTWATER_DESCRIPTION', 'HOT_WATER_ENERGY_EFF', 'HOT_WATER_ENV_EFF', 'PROPERTY_TYPE', 'BUILT_FORM', 'TOTAL_FLOOR_AREA', 'NUMBER_HABITABLE_ROOMS', 'NUMBER_HEATED_ROOMS', 'CONSTRUCTION_AGE_BAND', 'FLOOR_LEVEL', 'FLAT_TOP_STOREY', 'NUMBER_OPEN_FIREPLACES', 'EXTENSION_COUNT', 'MECHANICAL_VENTILATION', 'FLOOR_HEIGHT', 'HEATING_COST_CURRENT', 'HOT_WATER_COST_CURRENT', 'LIGHTING_COST_CURRENT', 'ENERGY_CONSUMPTION_CURRENT', 'CO2_EMISSIONS_CURRENT', 'CO2_EMISS_CURR_PER_FLOOR_AREA', 'TRANSACTION_TYPE', 'TENURE', 'REPORT_TYPE', 'fuel_poverty_risk', 'hhsrs_cold_hazard_proxy', 'spatial_heating_poverty', 'off_gas_grid']

def _epc_properties() -> Dict[str, str]:
    return {'pipeline.epc.schema_version': EPC_SCHEMA_VERSION, 'pipeline.epc.map_schema_version': _get_table_property(MAP_ADDRESS_TABLE, 'pipeline.map.schema_version') or 'UNKNOWN'}

def _epc_requires_full_sync(force_full: Optional[bool]) -> Tuple[bool, Dict[str, str]]:
    properties = _epc_properties()
    if force_full is True or not table_exists(MAP_ADDRESS_EPC_TABLE):
        return (True, properties)
    # EPC diagnostics/provenance columns are built internally but intentionally cut.
    required_columns = {'ADDRESS_ID', 'UPRN', 'ADDRESS_ADC_UPDT', 'ENVIRONMENT_IMPACT_CURRENT', 'ADC_UPDT'}
    if not required_columns.issubset(_table_columns(MAP_ADDRESS_EPC_TABLE)):
        return (True, properties)
    for key, current_value in properties.items():
        if _get_table_property(MAP_ADDRESS_EPC_TABLE, key) != current_value:
            return (True, properties)
    return (False, properties)

def _prepare_epc_source(required_uprns: DataFrame) -> DataFrame:
    source = spark.table(EPC_TABLE).withColumn('_UPRN_LONG', F.col('UPRN').cast('long')).filter(F.col('_UPRN_LONG').isNotNull()).join(required_uprns.select(F.col('UPRN').alias('_REQUIRED_UPRN')).dropDuplicates(), F.col('_UPRN_LONG') == F.col('_REQUIRED_UPRN'), 'left_semi')
    partition = Window.partitionBy('_UPRN_LONG')
    ordering = Window.partitionBy('_UPRN_LONG').orderBy(F.col('LODGEMENT_DATE').desc_nulls_last(), F.col('INSPECTION_DATE').desc_nulls_last(), F.col('ADC_UPDT').desc_nulls_last(), F.col('LMK_KEY').desc_nulls_last())
    return source.withColumn('EPC_CERTIFICATE_COUNT', F.count('*').over(partition)).withColumn('_epc_row_number', F.row_number().over(ordering)).filter(F.col('_epc_row_number') == 1).drop('_epc_row_number')

def create_address_epc_mapping(full_refresh: Optional[bool]=None) -> DataFrame:
    full_sync, properties = _epc_requires_full_sync(full_refresh)
    deleted_address_ids = None
    if full_sync:
        addresses = spark.table(MAP_ADDRESS_TABLE).select('ADDRESS_ID', 'UPRN', 'ADC_UPDT')
    else:
        last_merge = _last_successful_merge_timestamp(MAP_ADDRESS_EPC_TABLE)
        address_changes = _read_latest_cdf(MAP_ADDRESS_TABLE, ['ADDRESS_ID'], last_merge)
        epc_changes = _read_latest_cdf(EPC_TABLE, ['UPRN'], last_merge)
        if address_changes is None or epc_changes is None:
            full_sync = True
            addresses = spark.table(MAP_ADDRESS_TABLE).select('ADDRESS_ID', 'UPRN', 'ADC_UPDT')
        else:
            deleted_address_ids = address_changes.filter(F.col('_source_change_type') == 'delete').select(F.col('ADDRESS_ID').cast('long').alias('ADDRESS_ID')).dropDuplicates(['ADDRESS_ID'])
            changed_address_ids = address_changes.select(F.col('ADDRESS_ID').cast('long').alias('ADDRESS_ID')).dropDuplicates(['ADDRESS_ID'])
            changed_uprns = epc_changes.select(F.col('UPRN').cast('long').alias('UPRN')).filter(F.col('UPRN').isNotNull()).dropDuplicates(['UPRN'])
            current_addresses = spark.table(MAP_ADDRESS_TABLE).select('ADDRESS_ID', 'UPRN', 'ADC_UPDT')
            by_address = current_addresses.join(changed_address_ids, 'ADDRESS_ID', 'inner')
            by_epc = current_addresses.join(changed_uprns, 'UPRN', 'inner')
            addresses = by_address.unionByName(by_epc).dropDuplicates(['ADDRESS_ID'])
    addresses = _maybe_persist(addresses)
    required_uprns = addresses.select('UPRN').filter(F.col('UPRN').isNotNull()).dropDuplicates(['UPRN'])
    epc = _maybe_persist(_prepare_epc_source(required_uprns))
    select_expressions = [F.col('a.ADDRESS_ID').cast('long').alias('ADDRESS_ID'), F.col('a.UPRN').cast('long').alias('UPRN'), F.lit('MATCHED').alias('EPC_MATCH_STATUS'), F.col('e.EPC_CERTIFICATE_COUNT').cast('int').alias('EPC_CERTIFICATE_COUNT'), F.col('e.LMK_KEY').alias('EPC_LMK_KEY'), F.col('e.UPRN_SOURCE').alias('UPRN_SOURCE'), F.col('e.POSTCODE').alias('EPC_POSTCODE'), F.col('e.LOCAL_AUTHORITY').alias('LOCAL_AUTHORITY'), F.col('e.CONSTITUENCY').alias('CONSTITUENCY'), F.col('e.BUILDING_REFERENCE_NUMBER').alias('BUILDING_REFERENCE_NUMBER')]
    select_expressions.extend([F.col(f'e.{column}').alias(column) for column in _EPC_DIRECT_COLUMNS])
    select_expressions.extend([F.col('e.INSPECTION_DATE').alias('EPC_INSPECTION_DATE'), F.col('e.LODGEMENT_DATE').alias('EPC_LODGEMENT_DATE'), F.col('a.ADC_UPDT').alias('ADDRESS_ADC_UPDT'), F.col('e.ADC_UPDT').alias('EPC_SOURCE_ADC_UPDT'), F.greatest(F.col('a.ADC_UPDT'), F.col('e.ADC_UPDT')).alias('ADC_UPDT'), F.current_timestamp().alias('PIPELINE_LOADED_AT')])
    joined = addresses.alias('a').join(epc.alias('e'), F.col('a.UPRN') == F.col('e._UPRN_LONG'), 'left')
    upserts = joined.filter(F.col('e.LMK_KEY').isNotNull()).select(*select_expressions)
    upserts = _schema_select(upserts, schema_map_address_epc).withColumn('_source_change_type', F.lit('upsert'))
    result = upserts
    unmatched_address_ids = joined.filter(F.col('e.LMK_KEY').isNull()).select(F.col('a.ADDRESS_ID').cast('long').alias('ADDRESS_ID')).dropDuplicates(['ADDRESS_ID'])
    if deleted_address_ids is not None:
        unmatched_address_ids = unmatched_address_ids.unionByName(deleted_address_ids).dropDuplicates(['ADDRESS_ID'])
    if not full_sync and unmatched_address_ids.take(1):
        result = result.unionByName(_delete_rows(unmatched_address_ids, schema_map_address_epc), allowMissingColumns=True)
    _PENDING_TABLE_OPTIONS[MAP_ADDRESS_EPC_TABLE] = {'full_sync': full_sync, 'properties': properties}
    _register_cache(MAP_ADDRESS_EPC_TABLE, addresses, epc)
    return result

def run_map_updates(force_full_address: bool=False, force_full_epc: bool=False, include_sensitive_address_columns: bool=False, validate_keys: bool=False):
    address_updates = create_address_mapping_incr(full_refresh=True if force_full_address else None, include_sensitive_address_columns=include_sensitive_address_columns)
    if address_updates is not None:
        if not validate_keys or verify_no_duplicates(address_updates, 'ADDRESS_ID'):
            update_table(address_updates, MAP_ADDRESS_TABLE, 'ADDRESS_ID', schema_map_address, map_address_comment)
        else:
            raise ValueError('map_address update aborted because duplicate ADDRESS_ID values were found')
    epc_updates = create_address_epc_mapping(full_refresh=True if force_full_epc else None)
    if not validate_keys or verify_no_duplicates(epc_updates, 'ADDRESS_ID'):
        update_table(epc_updates, MAP_ADDRESS_EPC_TABLE, 'ADDRESS_ID', schema_map_address_epc, map_address_epc_comment)
    else:
        raise ValueError('map_address_epc update aborted because duplicate ADDRESS_ID values were found')

def run_post_deployment_checks() -> Dict[str, int]:
    source_rows = (
        spark.table(RAW_ADDRESS_TABLE)
        .filter(_mad_address_in_scope())
        .select(F.col('ADDRESS_ID').cast('long').alias('ADDRESS_ID'))
    )
    address_rows = spark.table(MAP_ADDRESS_TABLE).select('ADDRESS_ID')
    source_missing = source_rows.join(address_rows, 'ADDRESS_ID', 'left_anti').count()
    target_orphans = address_rows.join(source_rows, 'ADDRESS_ID', 'left_anti').count()
    address_duplicates = (
        address_rows.groupBy('ADDRESS_ID').count()
        .filter(F.col('count') > 1).count()
    )
    epc = spark.table(MAP_ADDRESS_EPC_TABLE)
    epc_rows_without_certificate = epc.filter(
        F.col('UPRN').isNull() | F.col('EPC_LMK_KEY').isNull()
    ).count()
    epc_orphans = (
        epc.select('ADDRESS_ID')
        .join(address_rows, 'ADDRESS_ID', 'left_anti')
        .count()
    )
    results = {
        'in_scope_source_addresses_missing_from_map_address': source_missing,
        'map_address_rows_not_in_source': target_orphans,
        'duplicate_map_address_ids': address_duplicates,
        'map_address_epc_rows_without_business_key': epc_rows_without_certificate,
        'map_address_epc_orphans': epc_orphans,
    }
    failures = {key: value for key, value in results.items() if value != 0}
    if failures:
        raise AssertionError(f'Post-deployment checks failed: {failures}')
    print(f'[OK] Post-deployment checks passed: {results}')
    return results
def _create_widgets():
    try:
        dbutils.widgets.dropdown('execute_updates', 'false', ['false', 'true'])
        dbutils.widgets.dropdown('force_full_address', 'false', ['false', 'true'])
        dbutils.widgets.dropdown('force_full_epc', 'false', ['false', 'true'])
        dbutils.widgets.dropdown('include_sensitive_address_columns', 'false', ['false', 'true'])
        dbutils.widgets.dropdown('validate_keys', 'false', ['false', 'true'])
        dbutils.widgets.dropdown('run_post_deployment_checks', 'false', ['false', 'true'])
    except Exception:
        pass

def _widget_bool(name: str, default: bool=False) -> bool:
    try:
        return dbutils.widgets.get(name).strip().lower() == 'true'
    except Exception:
        return default

try:
    run_map_updates(force_full_address=_PIPELINE_FULL_REFRESH, force_full_epc=_PIPELINE_FULL_REFRESH, include_sensitive_address_columns=False, validate_keys=False)
    _PIPELINE_UPDATED_TARGETS.extend(['4_prod.bronze.map_address', '4_prod.bronze.map_address_epc'])
    _pipeline_mark_component_complete('map_address', ['4_prod.bronze.map_address', '4_prod.bronze.map_address_epc'])
    _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_address'})
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

_pipeline_component_start("map_person")
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
from typing import Dict, List, Optional, Sequence, Tuple
from uuid import uuid4
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window
MP_RAW_PERSON_TABLE = '4_prod.raw.mill_person'
MP_CANONICAL_PERSON_TYPE_CD = 903
MP_MAP_ADDRESS_TABLE = '4_prod.bronze.map_address'
MP_CODE_VALUE_TABLE = '3_lookup.mill.mill_code_value'
MP_TARGET_TABLE = '4_prod.bronze.map_person'
MP_SCHEMA_VERSION = '2.0.0'
MP_LOGIC_VERSION = '2.0.0'
MP_MAX_INCREMENTAL_CODE_VALUES = 5000
MP_ENABLE_PERSISTENCE = True
_MP_PENDING_RUN: Dict[str, object] = {}
_MP_CACHES: List[DataFrame] = []
map_person_comment = 'Curated person foundation at one row per source PERSON_ID, restricted to real persons (PERSON_TYPE_CD = 903). Free-text, contributor-system and untyped person rows are intentionally excluded and remain available in 4_prod.raw.mill_person. Within that scope every person is preserved without active, demographic or birth-date filters; lifecycle, provenance, demographic, death and birth-precision fields are retained; calendar birth date is derived from the locally entered timestamp; and a deterministic current address is linked while latest-known address provenance is retained.'

def _mp_sf(name, data_type, comment='', nullable=True):
    metadata = {'comment': comment} if comment else {}
    return StructField(name, data_type, nullable, metadata=metadata)
schema_map_person = StructType([_mp_sf('person_id', LongType(), 'Source PERSON_ID represented as BIGINT.', nullable=False), _mp_sf('person_type_cd', LongType(), 'Source PERSON_TYPE_CD.'), _mp_sf('person_type_display', StringType(), 'Decoded person type.'), _mp_sf('is_person_type_person', BooleanType(), 'True when the source person type is Person.'), _mp_sf('gender_cd', LongType(), 'Source SEX_CD.'), _mp_sf('gender_display', StringType(), 'Decoded display for SEX_CD.'), _mp_sf('gender_cdf_meaning', StringType(), 'Cerner CDF meaning for SEX_CD.'), _mp_sf('gender_is_unmapped', BooleanType(), 'True when gender code is null, zero or has no lookup row.'), _mp_sf('ethnicity_cd', LongType(), 'Source ETHNIC_GRP_CD.'), _mp_sf('ethnicity_display', StringType(), 'Decoded display for ETHNIC_GRP_CD.'), _mp_sf('ethnicity_cdf_meaning', StringType(), 'Cerner CDF meaning for ETHNIC_GRP_CD.'), _mp_sf('ethnicity_is_unmapped', BooleanType(), 'True when ethnicity code is null, zero or has no lookup row.'), _mp_sf('race_cd', LongType(), 'Source RACE_CD. Retained even when currently unpopulated.'), _mp_sf('race_display', StringType(), 'Decoded display for RACE_CD.'), _mp_sf('birth_year', IntegerType(), 'Calendar birth year derived from birth_date.'), _mp_sf('birth_month', IntegerType(), 'Calendar birth month derived from birth_date.'), _mp_sf('birth_day', IntegerType(), 'Calendar birth day derived from birth_date.'), _mp_sf('birth_datetime', TimestampType(), 'Compatibility birth timestamp using ABS_BIRTH_DT_TM first and BIRTH_DT_TM fallback.'), _mp_sf('birth_date', DateType(), 'Calendar birth date using ABS_BIRTH_DT_TM first and BIRTH_DT_TM fallback.'), _mp_sf('birth_datetime_utc', TimestampType(), 'Raw source BIRTH_DT_TM.'), _mp_sf('birth_datetime_local', TimestampType(), 'Raw locally entered ABS_BIRTH_DT_TM.'), _mp_sf('birth_date_source', StringType(), 'ABS_BIRTH_DT_TM, BIRTH_DT_TM or NONE.'), _mp_sf('birth_dt_cd', LongType(), 'Source BIRTH_DT_CD describing the kind of birth value.'), _mp_sf('birth_dt_display', StringType(), 'Decoded display for BIRTH_DT_CD.'), _mp_sf('birth_precision_flag', LongType(), 'Source BIRTH_PREC_FLAG.'), _mp_sf('birth_tz', LongType(), 'Source BIRTH_TZ.'), _mp_sf('birth_date_quality_status', StringType(), 'MISSING, BEFORE_1901, FUTURE or PLAUSIBLE.'), _mp_sf('birth_date_is_plausible', BooleanType(), 'True when birth_date is from 1901-01-01 through the current date.'), _mp_sf('language_cd', LongType(), 'Source LANGUAGE_CD.'), _mp_sf('language_display', StringType(), 'Decoded display for LANGUAGE_CD.'), _mp_sf('language_dialect_cd', LongType(), 'Source LANGUAGE_DIALECT_CD.'), _mp_sf('language_dialect_display', StringType(), 'Decoded display for LANGUAGE_DIALECT_CD.'), _mp_sf('marital_type_cd', LongType(), 'Source MARITAL_TYPE_CD.'), _mp_sf('marital_type_display', StringType(), 'Decoded display for MARITAL_TYPE_CD.'), _mp_sf('religion_cd', LongType(), 'Source RELIGION_CD.'), _mp_sf('religion_display', StringType(), 'Decoded display for RELIGION_CD.'), _mp_sf('citizenship_cd', LongType(), 'Source CITIZENSHIP_CD.'), _mp_sf('citizenship_display', StringType(), 'Decoded display for CITIZENSHIP_CD.'), _mp_sf('nationality_cd', LongType(), 'Source NATIONALITY_CD.'), _mp_sf('nationality_display', StringType(), 'Decoded display for NATIONALITY_CD.'), _mp_sf('resident_cd', LongType(), 'Source RESIDENT_CD.'), _mp_sf('resident_display', StringType(), 'Decoded display for RESIDENT_CD.'), _mp_sf('personal_pronoun_cd', LongType(), 'Source PERSONAL_PRONOUN_CD represented as BIGINT.'), _mp_sf('personal_pronoun_display', StringType(), 'Decoded display for PERSONAL_PRONOUN_CD.'), _mp_sf('personal_pronoun_other_txt', StringType(), 'Source free-text personal pronoun value.'), _mp_sf('active_ind', LongType(), 'Source ACTIVE_IND; rows are retained for every value.'), _mp_sf('active_status_cd', LongType(), 'Source ACTIVE_STATUS_CD.'), _mp_sf('active_status_display', StringType(), 'Decoded display for ACTIVE_STATUS_CD.'), _mp_sf('active_status_dt_tm', TimestampType(), 'Source ACTIVE_STATUS_DT_TM.'), _mp_sf('beg_effective_dt_tm', TimestampType(), 'Source BEG_EFFECTIVE_DT_TM.'), _mp_sf('end_effective_dt_tm', TimestampType(), 'Source END_EFFECTIVE_DT_TM.'), _mp_sf('is_current_active', BooleanType(), 'True when ACTIVE_IND is one and the source row is currently effective.'), _mp_sf('person_status_cd', LongType(), 'Source PERSON_STATUS_CD.'), _mp_sf('person_status_display', StringType(), 'Decoded display for PERSON_STATUS_CD.'), _mp_sf('data_status_cd', LongType(), 'Source DATA_STATUS_CD.'), _mp_sf('data_status_display', StringType(), 'Decoded display for DATA_STATUS_CD.'), _mp_sf('data_status_dt_tm', TimestampType(), 'Source DATA_STATUS_DT_TM.'), _mp_sf('contributor_system_cd', LongType(), 'Source CONTRIBUTOR_SYSTEM_CD.'), _mp_sf('contributor_system_display', StringType(), 'Decoded display for CONTRIBUTOR_SYSTEM_CD.'), _mp_sf('create_dt_tm', TimestampType(), 'Source CREATE_DT_TM.'), _mp_sf('source_updt_dt_tm', TimestampType(), 'Source UPDT_DT_TM.'), _mp_sf('source_updt_cnt', LongType(), 'Source UPDT_CNT.'), _mp_sf('last_utc_ts', TimestampType(), 'Source LAST_UTC_TS.'), _mp_sf('inst_id', LongType(), 'Source INST_ID.'), _mp_sf('logical_domain_id', LongType(), 'Source LOGICAL_DOMAIN_ID.'), _mp_sf('last_encntr_dt_tm', TimestampType(), 'Source LAST_ENCNTR_DT_TM.'), _mp_sf('sex_age_change_ind', LongType(), 'Source SEX_AGE_CHANGE_IND.'), _mp_sf('deceased_cd', LongType(), 'Source DECEASED_CD.'), _mp_sf('deceased_display', StringType(), 'Decoded display for DECEASED_CD.'), _mp_sf('deceased_dt_tm', TimestampType(), 'Source DECEASED_DT_TM.'), _mp_sf('deceased_dt_tm_precision_flag', LongType(), 'Source DECEASED_DT_TM_PREC_FLAG.'), _mp_sf('deceased_source_cd', LongType(), 'Source DECEASED_SOURCE_CD.'), _mp_sf('deceased_source_display', StringType(), 'Decoded display for DECEASED_SOURCE_CD.'), _mp_sf('deceased_notify_source_cd', LongType(), 'Source DECEASED_NOTIFY_SOURCE_CD.'), _mp_sf('deceased_notify_source_display', StringType(), 'Decoded display for DECEASED_NOTIFY_SOURCE_CD.'), _mp_sf('deceased_id_method_cd', LongType(), 'Source DECEASED_ID_METHOD_CD.'), _mp_sf('deceased_id_method_display', StringType(), 'Decoded display for DECEASED_ID_METHOD_CD.'), _mp_sf('cause_of_death_cd', LongType(), 'Source CAUSE_OF_DEATH_CD. Free-text cause is deliberately not copied.'), _mp_sf('cause_of_death_display', StringType(), 'Decoded display for CAUSE_OF_DEATH_CD.'), _mp_sf('age_at_death', DoubleType(), 'Source AGE_AT_DEATH.'), _mp_sf('age_at_death_unit_cd', LongType(), 'Source AGE_AT_DEATH_UNIT_CD.'), _mp_sf('age_at_death_unit_display', StringType(), 'Decoded display for AGE_AT_DEATH_UNIT_CD.'), _mp_sf('age_at_death_precision_modifier_flag', LongType(), 'Source AGE_AT_DEATH_PREC_MOD_FLAG.'), _mp_sf('autopsy_cd', LongType(), 'Source AUTOPSY_CD.'), _mp_sf('autopsy_display', StringType(), 'Decoded display for AUTOPSY_CD.'), _mp_sf('species_cd', LongType(), 'Source SPECIES_CD.'), _mp_sf('species_display', StringType(), 'Decoded display for SPECIES_CD.'), _mp_sf('confid_level_cd', LongType(), 'Source CONFID_LEVEL_CD.'), _mp_sf('confid_level_display', StringType(), 'Decoded display for CONFID_LEVEL_CD.'), _mp_sf('confid_level_source_cd', LongType(), 'Source CONFID_LEVEL_SOURCE_CD.'), _mp_sf('vip_cd', LongType(), 'Source VIP_CD.'), _mp_sf('vip_display', StringType(), 'Decoded display for VIP_CD.'), _mp_sf('archive_env_id', LongType(), 'Source ARCHIVE_ENV_ID.'), _mp_sf('archive_status_cd', LongType(), 'Source ARCHIVE_STATUS_CD.'), _mp_sf('archive_status_display', StringType(), 'Decoded display for ARCHIVE_STATUS_CD.'), _mp_sf('last_accessed_dt_tm', TimestampType(), 'Source LAST_ACCESSED_DT_TM.'), _mp_sf('archive_status_dt_tm', TimestampType(), 'Source ARCHIVE_STATUS_DT_TM.'), _mp_sf('next_restore_dt_tm', TimestampType(), 'Source NEXT_RESTORE_DT_TM.'), _mp_sf('address_id', LongType(), 'Compatibility current address identifier; null when no address is currently active and effective.'), _mp_sf('current_address_id', LongType(), 'Address identifier selected from an active and currently effective person address.'), _mp_sf('latest_known_address_id', LongType(), 'Latest deterministically ranked address, even when inactive or not currently effective.'), _mp_sf('address_selection_status', StringType(), 'CURRENT, LATEST_INACTIVE, LATEST_NOT_EFFECTIVE, LATEST_KNOWN or NO_ADDRESS.'), _mp_sf('address_active_ind', LongType(), 'ACTIVE_IND of the selected latest-known address.'), _mp_sf('address_beg_effective_dt_tm', TimestampType(), 'Effective start of the selected latest-known address.'), _mp_sf('address_end_effective_dt_tm', TimestampType(), 'Effective end of the selected latest-known address.'), _mp_sf('address_adc_updt', TimestampType(), 'ADC_UPDT of the selected latest-known address.'), _mp_sf('person_adc_updt', TimestampType(), 'Raw person source ADC_UPDT.'), _mp_sf('ADC_UPDT', TimestampType(), 'Greatest available person or selected-address source update timestamp.'), _mp_sf('PIPELINE_LOADED_AT', TimestampType(), 'Timestamp this row was inserted or materially updated.'), _mp_sf('map_schema_version', StringType(), 'Version of this map_person schema and transformation contract.')])
_MP_CODE_ENRICHMENTS = [('person_type_cd', 'person_type'), ('gender_cd', 'gender'), ('ethnicity_cd', 'ethnicity'), ('race_cd', 'race'), ('birth_dt_cd', 'birth_dt'), ('language_cd', 'language'), ('language_dialect_cd', 'language_dialect'), ('marital_type_cd', 'marital_type'), ('religion_cd', 'religion'), ('citizenship_cd', 'citizenship'), ('nationality_cd', 'nationality'), ('resident_cd', 'resident'), ('personal_pronoun_cd', 'personal_pronoun'), ('active_status_cd', 'active_status'), ('person_status_cd', 'person_status'), ('data_status_cd', 'data_status'), ('contributor_system_cd', 'contributor_system'), ('deceased_cd', 'deceased'), ('deceased_source_cd', 'deceased_source'), ('deceased_notify_source_cd', 'deceased_notify_source'), ('deceased_id_method_cd', 'deceased_id_method'), ('cause_of_death_cd', 'cause_of_death'), ('age_at_death_unit_cd', 'age_at_death_unit'), ('autopsy_cd', 'autopsy'), ('species_cd', 'species'), ('confid_level_cd', 'confid_level'), ('vip_cd', 'vip'), ('archive_status_cd', 'archive_status')]
_MP_RAW_CODE_COLUMNS = ['PERSON_TYPE_CD', 'SEX_CD', 'ETHNIC_GRP_CD', 'RACE_CD', 'BIRTH_DT_CD', 'LANGUAGE_CD', 'LANGUAGE_DIALECT_CD', 'MARITAL_TYPE_CD', 'RELIGION_CD', 'CITIZENSHIP_CD', 'NATIONALITY_CD', 'RESIDENT_CD', 'PERSONAL_PRONOUN_CD', 'ACTIVE_STATUS_CD', 'PERSON_STATUS_CD', 'DATA_STATUS_CD', 'CONTRIBUTOR_SYSTEM_CD', 'DECEASED_CD', 'DECEASED_SOURCE_CD', 'DECEASED_NOTIFY_SOURCE_CD', 'DECEASED_ID_METHOD_CD', 'CAUSE_OF_DEATH_CD', 'AGE_AT_DEATH_UNIT_CD', 'AUTOPSY_CD', 'SPECIES_CD', 'CONFID_LEVEL_CD', 'VIP_CD', 'ARCHIVE_STATUS_CD']
_MP_COMPARE_EXCLUSIONS = {'PIPELINE_LOADED_AT'}

class _MPCdfUnavailable(RuntimeError):
    pass

class _MPForceFullSync(RuntimeError):
    pass

def _mp_sql_table(table_name: str) -> str:
    tick = chr(96)
    return '.'.join((tick + part.replace(tick, tick + tick) + tick for part in table_name.split('.')))

def _mp_sql_column(column_name: str) -> str:
    tick = chr(96)
    return tick + column_name.replace(tick, tick + tick) + tick

def _mp_escape_sql_string(value: object) -> str:
    return str(value).replace('\\', '\\\\').replace("'", "''")

def _mp_table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _mp_table_columns(table_name: str) -> Dict[str, object]:
    if not _mp_table_exists(table_name):
        return {}
    return {field.name: field.dataType for field in spark.table(table_name).schema.fields}

def _mp_get_table_properties(table_name: str) -> Dict[str, str]:
    if not _mp_table_exists(table_name):
        return {}
    try:
        rows = spark.sql('SHOW TBLPROPERTIES ' + _mp_sql_table(table_name)).collect()
        return {row['key']: row['value'] for row in rows}
    except Exception:
        return {}

def _mp_get_table_property(table_name: str, key: str) -> Optional[str]:
    return _mp_get_table_properties(table_name).get(key)

def _mp_set_table_properties(table_name: str, properties: Dict[str, object]):
    if not properties:
        return
    assignments = ', '.join(("'" + _mp_escape_sql_string(key) + "' = '" + _mp_escape_sql_string(value) + "'" for key, value in properties.items()))
    spark.sql('ALTER TABLE ' + _mp_sql_table(table_name) + ' SET TBLPROPERTIES (' + assignments + ')')

def _mp_current_table_version(table_name: str) -> int:
    rows = spark.sql('DESCRIBE HISTORY ' + _mp_sql_table(table_name) + ' LIMIT 1').collect()
    if not rows:
        raise ValueError('No Delta history is available for ' + table_name)
    return int(rows[0]['version'])

def _mp_capture_source_versions() -> Dict[str, int]:
    for source_table in (MP_RAW_PERSON_TABLE, MP_MAP_ADDRESS_TABLE, MP_CODE_VALUE_TABLE):
        if not _mp_table_exists(source_table):
            raise ValueError('Required source table does not exist: ' + source_table)
    return {'person_source_version': _mp_current_table_version(MP_RAW_PERSON_TABLE), 'address_source_version': _mp_current_table_version(MP_MAP_ADDRESS_TABLE), 'code_source_version': _mp_current_table_version(MP_CODE_VALUE_TABLE)}

def _mp_checkpoint_property_name(version_name: str) -> str:
    return 'pipeline.map_person.' + version_name

def _mp_stored_source_versions() -> Optional[Dict[str, int]]:
    values = {}
    for name in ('person_source_version', 'address_source_version', 'code_source_version'):
        value = _mp_get_table_property(MP_TARGET_TABLE, _mp_checkpoint_property_name(name))
        if value is None:
            return None
        try:
            values[name] = int(value)
        except ValueError:
            return None
    return values

def _mp_cache(df: DataFrame) -> DataFrame:
    if not MP_ENABLE_PERSISTENCE:
        return df
    cached = _persist_stage_materialize(df)
    _MP_CACHES.append(cached)
    return cached

def _mp_release_caches():
    while _MP_CACHES:
        df = _MP_CACHES.pop()
        try:
            _persist_stage_drop(df)
        except Exception:
            pass

def _mp_schema_select(df: DataFrame, schema: StructType=schema_map_person) -> DataFrame:
    available = set(df.columns)
    expressions = []
    for field in schema.fields:
        if field.name in available:
            expressions.append(F.col(field.name).cast(field.dataType).alias(field.name, metadata=field.metadata))
        else:
            expressions.append(F.lit(None).cast(field.dataType).alias(field.name, metadata=field.metadata))
    return df.select(*expressions)

def _mp_empty_person_ids() -> DataFrame:
    return spark.createDataFrame([], StructType([StructField('person_id', LongType(), False)]))

def _mp_effective_now(begin_column, end_column):
    now = F.current_timestamp()
    return (begin_column.isNull() | (begin_column <= now)) & (end_column.isNull() | (end_column > now))

def _mp_has_rows(df: DataFrame) -> bool:
    return bool(df.take(1))

def _mp_requires_full_sync(force_full: Optional[bool], current_versions: Dict[str, int]) -> Tuple[bool, str, Optional[Dict[str, int]]]:
    if force_full is True:
        return (True, 'forced by caller', None)
    if not _mp_table_exists(MP_TARGET_TABLE):
        return (True, 'target table does not exist', None)
    if _mp_get_table_property(MP_TARGET_TABLE, 'pipeline.map_person.schema_version') != MP_SCHEMA_VERSION:
        return (True, 'schema version changed', None)
    expected = {
        field.name.lower(): field.dataType.simpleString()
        for field in bronze_contract_schema(MP_TARGET_TABLE).fields
    }
    actual = {
        field.name.lower(): field.dataType.simpleString()
        for field in spark.table(MP_TARGET_TABLE).schema.fields
    }
    if actual != expected:
        return (True, 'target schema differs from the retained-column contract', None)
    stored_versions = _mp_stored_source_versions()
    if stored_versions is None:
        return (True, 'source-version checkpoints are absent', None)
    for name, current_version in current_versions.items():
        if stored_versions[name] > current_version:
            return (True, 'source table was replaced or restored: ' + name, stored_versions)
    return (False, 'incremental checkpoints are usable', stored_versions)
def _mp_read_cdf(table_name: str, starting_version_exclusive: int, ending_version_inclusive: int) -> DataFrame:
    if ending_version_inclusive <= starting_version_exclusive:
        raise ValueError('CDF read requested without a version interval')
    try:
        changes = (
            spark.read.format('delta')
            .option('readChangeFeed', 'true')
            .option('startingVersion', str(starting_version_exclusive + 1))
            .option('endingVersion', str(ending_version_inclusive))
            .table(table_name)
        )
        # Spark Connect defers Delta CDF validation until plan analysis. Resolve the
        # schema here so missing log/history errors are caught by this fallback
        # instead of surfacing later during the target MERGE.
        _ = changes.schema
        changes.limit(1).collect()
        return changes.filter(
            F.col('_change_type').isin(
                'insert', 'update_preimage', 'update_postimage', 'delete'
            )
        )
    except Exception as error:
        first_line = str(error).splitlines()[0]
        raise _MPCdfUnavailable(
            'CDF is unavailable for ' + table_name + ' from version '
            + str(starting_version_exclusive + 1) + ' through '
            + str(ending_version_inclusive) + ': ' + first_line
        ) from error

def _mp_person_ids_affected_by_code_changes(code_changes: DataFrame) -> DataFrame:
    changed_rows = code_changes.select(F.col('CODE_VALUE').cast('long').alias('code_value')).filter(F.col('code_value').isNotNull()).distinct().limit(MP_MAX_INCREMENTAL_CODE_VALUES + 1).collect()
    changed_values = [int(row['code_value']) for row in changed_rows]
    if len(changed_values) > MP_MAX_INCREMENTAL_CODE_VALUES:
        raise _MPForceFullSync('More than ' + str(MP_MAX_INCREMENTAL_CODE_VALUES) + ' code values changed in one interval')
    if not changed_values:
        return _mp_empty_person_ids()
    source = spark.table(MP_RAW_PERSON_TABLE)
    predicates = [F.col(column_name).cast('long').isin(changed_values) for column_name in _MP_RAW_CODE_COLUMNS]
    condition = reduce(lambda left, right: left | right, predicates)
    return source.filter(condition).select(F.col('PERSON_ID').cast('long').alias('person_id')).filter(F.col('person_id').isNotNull()).distinct()

def _mp_incremental_affected_person_ids(stored_versions: Dict[str, int], current_versions: Dict[str, int]) -> DataFrame:
    affected_parts: List[DataFrame] = []
    if current_versions['person_source_version'] > stored_versions['person_source_version']:
        person_changes = _mp_read_cdf(MP_RAW_PERSON_TABLE, stored_versions['person_source_version'], current_versions['person_source_version'])
        affected_parts.append(person_changes.select(F.col('PERSON_ID').cast('long').alias('person_id')).filter(F.col('person_id').isNotNull()).distinct())
    if current_versions['address_source_version'] > stored_versions['address_source_version']:
        address_changes = _mp_read_cdf(MP_MAP_ADDRESS_TABLE, stored_versions['address_source_version'], current_versions['address_source_version'])
        affected_parts.append(address_changes.filter(F.upper(F.trim(F.col('PARENT_ENTITY_NAME'))) == F.lit('PERSON')).select(F.col('PARENT_ENTITY_ID').cast('long').alias('person_id')).filter(F.col('person_id').isNotNull()).distinct())
    if current_versions['code_source_version'] > stored_versions['code_source_version']:
        code_changes = _mp_read_cdf(MP_CODE_VALUE_TABLE, stored_versions['code_source_version'], current_versions['code_source_version'])
        affected_parts.append(_mp_person_ids_affected_by_code_changes(code_changes))
    if not affected_parts:
        return _mp_empty_person_ids()
    return reduce(lambda left, right: left.unionByName(right), affected_parts).distinct()

def _mp_person_snapshot(person_ids: Optional[DataFrame]=None) -> DataFrame:
    source = spark.table(MP_RAW_PERSON_TABLE).filter(F.col('PERSON_TYPE_CD').cast('long') == F.lit(MP_CANONICAL_PERSON_TYPE_CD)).select(F.col('PERSON_ID').cast('long').alias('person_id'), F.col('PERSON_TYPE_CD').cast('long').alias('person_type_cd'), F.col('SEX_CD').cast('long').alias('gender_cd'), F.col('ETHNIC_GRP_CD').cast('long').alias('ethnicity_cd'), F.col('RACE_CD').cast('long').alias('race_cd'), F.col('BIRTH_DT_TM').cast('timestamp').alias('birth_datetime_utc'), F.col('ABS_BIRTH_DT_TM').cast('timestamp').alias('birth_datetime_local'), F.col('BIRTH_DT_CD').cast('long').alias('birth_dt_cd'), F.col('BIRTH_PREC_FLAG').cast('long').alias('birth_precision_flag'), F.col('BIRTH_TZ').cast('long').alias('birth_tz'), F.col('LANGUAGE_CD').cast('long').alias('language_cd'), F.col('LANGUAGE_DIALECT_CD').cast('long').alias('language_dialect_cd'), F.col('MARITAL_TYPE_CD').cast('long').alias('marital_type_cd'), F.col('RELIGION_CD').cast('long').alias('religion_cd'), F.col('CITIZENSHIP_CD').cast('long').alias('citizenship_cd'), F.col('NATIONALITY_CD').cast('long').alias('nationality_cd'), F.col('RESIDENT_CD').cast('long').alias('resident_cd'), F.col('PERSONAL_PRONOUN_CD').cast('long').alias('personal_pronoun_cd'), F.col('PERSONAL_PRONOUN_OTHER_TXT').cast('string').alias('personal_pronoun_other_txt'), F.col('ACTIVE_IND').cast('long').alias('active_ind'), F.col('ACTIVE_STATUS_CD').cast('long').alias('active_status_cd'), F.col('ACTIVE_STATUS_DT_TM').cast('timestamp').alias('active_status_dt_tm'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('beg_effective_dt_tm'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('end_effective_dt_tm'), F.col('PERSON_STATUS_CD').cast('long').alias('person_status_cd'), F.col('DATA_STATUS_CD').cast('long').alias('data_status_cd'), F.col('DATA_STATUS_DT_TM').cast('timestamp').alias('data_status_dt_tm'), F.col('CONTRIBUTOR_SYSTEM_CD').cast('long').alias('contributor_system_cd'), F.col('CREATE_DT_TM').cast('timestamp').alias('create_dt_tm'), F.col('UPDT_DT_TM').cast('timestamp').alias('source_updt_dt_tm'), F.col('UPDT_CNT').cast('long').alias('source_updt_cnt'), F.col('LAST_UTC_TS').cast('timestamp').alias('last_utc_ts'), F.col('INST_ID').cast('long').alias('inst_id'), F.col('LOGICAL_DOMAIN_ID').cast('long').alias('logical_domain_id'), F.col('LAST_ENCNTR_DT_TM').cast('timestamp').alias('last_encntr_dt_tm'), F.col('SEX_AGE_CHANGE_IND').cast('long').alias('sex_age_change_ind'), F.col('DECEASED_CD').cast('long').alias('deceased_cd'), F.col('DECEASED_DT_TM').cast('timestamp').alias('deceased_dt_tm'), F.col('DECEASED_DT_TM_PREC_FLAG').cast('long').alias('deceased_dt_tm_precision_flag'), F.col('DECEASED_SOURCE_CD').cast('long').alias('deceased_source_cd'), F.col('DECEASED_NOTIFY_SOURCE_CD').cast('long').alias('deceased_notify_source_cd'), F.col('DECEASED_ID_METHOD_CD').cast('long').alias('deceased_id_method_cd'), F.col('CAUSE_OF_DEATH_CD').cast('long').alias('cause_of_death_cd'), F.col('AGE_AT_DEATH').cast('double').alias('age_at_death'), F.col('AGE_AT_DEATH_UNIT_CD').cast('long').alias('age_at_death_unit_cd'), F.col('AGE_AT_DEATH_PREC_MOD_FLAG').cast('long').alias('age_at_death_precision_modifier_flag'), F.col('AUTOPSY_CD').cast('long').alias('autopsy_cd'), F.col('SPECIES_CD').cast('long').alias('species_cd'), F.col('CONFID_LEVEL_CD').cast('long').alias('confid_level_cd'), F.col('CONFID_LEVEL_SOURCE_CD').cast('long').alias('confid_level_source_cd'), F.col('VIP_CD').cast('long').alias('vip_cd'), F.col('ARCHIVE_ENV_ID').cast('long').alias('archive_env_id'), F.col('ARCHIVE_STATUS_CD').cast('long').alias('archive_status_cd'), F.col('LAST_ACCESSED_DT_TM').cast('timestamp').alias('last_accessed_dt_tm'), F.col('ARCHIVE_STATUS_DT_TM').cast('timestamp').alias('archive_status_dt_tm'), F.col('NEXT_RESTORE_DT_TM').cast('timestamp').alias('next_restore_dt_tm'), F.col('ADC_UPDT').cast('timestamp').alias('person_adc_updt'))
    if person_ids is not None:
        source = source.join(person_ids.select('person_id').distinct(), 'person_id', 'left_semi')
    return source

def _mp_selected_person_addresses(person_ids: Optional[DataFrame]=None) -> DataFrame:
    source = spark.table(MP_MAP_ADDRESS_TABLE).filter(F.upper(F.trim(F.col('PARENT_ENTITY_NAME'))) == F.lit('PERSON')).select(F.col('PARENT_ENTITY_ID').cast('long').alias('person_id'), F.col('ADDRESS_ID').cast('long').alias('_address_id'), F.col('ACTIVE_IND').cast('long').alias('address_active_ind'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('address_beg_effective_dt_tm'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('address_end_effective_dt_tm'), F.col('ADC_UPDT').cast('timestamp').alias('address_adc_updt'))
    if person_ids is not None:
        source = source.join(person_ids.select('person_id').distinct(), 'person_id', 'left_semi')
    source = source.withColumn('_address_effective_now', _mp_effective_now(F.col('address_beg_effective_dt_tm'), F.col('address_end_effective_dt_tm'))).withColumn('_address_is_current', (F.col('address_active_ind') == 1) & F.col('_address_effective_now'))
    ranking = Window.partitionBy('person_id').orderBy(F.col('_address_is_current').desc(), (F.col('address_active_ind') == 1).desc(), F.col('_address_effective_now').desc(), F.col('address_beg_effective_dt_tm').desc_nulls_last(), F.col('address_adc_updt').desc_nulls_last(), F.col('_address_id').desc_nulls_last())
    return source.withColumn('_address_rank', F.row_number().over(ranking)).filter(F.col('_address_rank') == 1).drop('_address_rank').withColumn('current_address_id', F.when(F.col('_address_is_current'), F.col('_address_id')).cast('long')).withColumn('latest_known_address_id', F.col('_address_id').cast('long')).withColumn('address_selection_status', F.when(F.col('_address_is_current'), F.lit('CURRENT')).when(F.col('address_active_ind') != 1, F.lit('LATEST_INACTIVE')).when(~F.col('_address_effective_now'), F.lit('LATEST_NOT_EFFECTIVE')).otherwise(F.lit('LATEST_KNOWN'))).select('person_id', 'current_address_id', 'latest_known_address_id', 'address_selection_status', 'address_active_ind', 'address_beg_effective_dt_tm', 'address_end_effective_dt_tm', 'address_adc_updt')

def _mp_code_maps(persons: DataFrame) -> DataFrame:
    code_columns = [column_name for column_name, _ in _MP_CODE_ENRICHMENTS]
    needed_parts = [persons.select(F.col(column_name).cast('long').alias('code_value')) for column_name in code_columns]
    needed_codes = reduce(lambda left, right: left.unionByName(right), needed_parts).filter(F.col('code_value').isNotNull()).distinct()
    lookup = spark.table(MP_CODE_VALUE_TABLE).select(F.col('CODE_VALUE').cast('long').alias('code_value'), F.col('DISPLAY').cast('string').alias('_display'), F.col('CDF_MEANING').cast('string').alias('_cdf_meaning'), F.col('DESCRIPTION').cast('string').alias('_description'), F.col('ACTIVE_IND').cast('long').alias('_lookup_active_ind'), F.col('UPDT_DT_TM').cast('timestamp').alias('_lookup_updt_dt_tm'), F.col('ADC_UPDT').cast('timestamp').alias('_lookup_adc_updt'))
    ranking = Window.partitionBy('code_value').orderBy(F.col('_lookup_adc_updt').desc_nulls_last(), F.col('_lookup_updt_dt_tm').desc_nulls_last(), F.col('_lookup_active_ind').desc_nulls_last())
    lookup = lookup.withColumn('_lookup_rank', F.row_number().over(ranking)).filter(F.col('_lookup_rank') == 1).drop('_lookup_rank').join(needed_codes, 'code_value', 'inner').withColumn('_code_display', F.coalesce(F.col('_display'), F.col('_cdf_meaning'), F.col('_description')))
    return lookup.agg(F.map_from_entries(F.collect_list(F.struct(F.col('code_value'), F.col('_code_display')))).alias('_code_display_map'), F.map_from_entries(F.collect_list(F.struct(F.col('code_value'), F.col('_cdf_meaning')))).alias('_code_cdf_meaning_map'))

def _mp_enrich_code_values(persons: DataFrame) -> DataFrame:
    result = persons.crossJoin(F.broadcast(_mp_code_maps(persons)))
    for code_column, prefix in _MP_CODE_ENRICHMENTS:
        result = result.withColumn(prefix + '_display', F.element_at(F.col('_code_display_map'), F.col(code_column)))
    result = result.withColumn('gender_cdf_meaning', F.element_at(F.col('_code_cdf_meaning_map'), F.col('gender_cd'))).withColumn('ethnicity_cdf_meaning', F.element_at(F.col('_code_cdf_meaning_map'), F.col('ethnicity_cd'))).drop('_code_display_map', '_code_cdf_meaning_map')
    return result

def _mp_build_person_rows(persons: DataFrame) -> DataFrame:
    persons = _mp_cache(persons)
    addresses = _mp_selected_person_addresses(persons.select('person_id'))
    enriched = _mp_enrich_code_values(persons).join(addresses, 'person_id', 'left')
    selected_birth_timestamp = F.coalesce(F.col('birth_datetime_local'), F.col('birth_datetime_utc'))
    selected_birth_date = F.to_date(selected_birth_timestamp)
    person_effective_now = _mp_effective_now(F.col('beg_effective_dt_tm'), F.col('end_effective_dt_tm'))
    result = enriched.withColumn('birth_datetime', selected_birth_timestamp).withColumn('birth_date', selected_birth_date).withColumn('birth_year', F.year(F.col('birth_date')).cast('int')).withColumn('birth_month', F.month(F.col('birth_date')).cast('int')).withColumn('birth_day', F.dayofmonth(F.col('birth_date')).cast('int')).withColumn('birth_date_source', F.when(F.col('birth_datetime_local').isNotNull(), F.lit('ABS_BIRTH_DT_TM')).when(F.col('birth_datetime_utc').isNotNull(), F.lit('BIRTH_DT_TM')).otherwise(F.lit('NONE'))).withColumn('birth_date_quality_status', F.when(F.col('birth_date').isNull(), F.lit('MISSING')).when(F.col('birth_date') < F.lit('1901-01-01').cast('date'), F.lit('BEFORE_1901')).when(F.col('birth_date') > F.current_date(), F.lit('FUTURE')).otherwise(F.lit('PLAUSIBLE'))).withColumn('birth_date_is_plausible', F.col('birth_date').isNotNull() & (F.col('birth_date') >= F.lit('1901-01-01').cast('date')) & (F.col('birth_date') <= F.current_date())).withColumn('is_person_type_person', (F.col('person_type_cd') == 903) | (F.upper(F.trim(F.col('person_type_display'))) == F.lit('PERSON'))).withColumn('gender_is_unmapped', F.col('gender_cd').isNull() | (F.col('gender_cd') == 0) | F.col('gender_display').isNull()).withColumn('ethnicity_is_unmapped', F.col('ethnicity_cd').isNull() | (F.col('ethnicity_cd') == 0) | F.col('ethnicity_display').isNull()).withColumn('is_current_active', (F.col('active_ind') == 1) & person_effective_now).withColumn('address_selection_status', F.coalesce(F.col('address_selection_status'), F.lit('NO_ADDRESS'))).withColumn('address_id', F.col('current_address_id').cast('long')).withColumn('ADC_UPDT', F.greatest(F.col('person_adc_updt'), F.col('address_adc_updt'))).withColumn('PIPELINE_LOADED_AT', F.current_timestamp()).withColumn('map_schema_version', F.lit(MP_SCHEMA_VERSION))
    return _mp_schema_select(result).withColumn('_source_change_type', F.lit('upsert'))

def _mp_delete_rows(person_ids: DataFrame) -> DataFrame:
    return _mp_schema_select(person_ids.select(F.col('person_id').cast('long').alias('person_id'))).withColumn('_source_change_type', F.lit('delete'))

def create_person_mapping_incr(full_refresh: Optional[bool]=None) -> Optional[DataFrame]:
    """
    Drop-in replacement retaining the historical function name.

    Full mode returns one row for every source PERSON_ID.
    Incremental mode returns upsert rows for people affected by person, address or code-value
    changes plus delete markers for source PERSON_ID values that no longer exist.
    """
    global _MP_PENDING_RUN
    _mp_release_caches()
    _MP_PENDING_RUN = {}
    current_versions = _mp_capture_source_versions()
    full_sync, reason, stored_versions = _mp_requires_full_sync(full_refresh, current_versions)
    affected_ids = None
    if not full_sync:
        try:
            affected_ids = _mp_cache(_mp_incremental_affected_person_ids(stored_versions, current_versions))
            if not _mp_has_rows(affected_ids):
                _MP_PENDING_RUN = {'full_sync': False, 'reason': 'source versions advanced without relevant row changes', 'versions': current_versions}
                print('[INFO] map_person: no affected PERSON_ID values')
                return None
        except (_MPCdfUnavailable, _MPForceFullSync) as error:
            full_sync = True
            reason = str(error)
            affected_ids = None
            print('[WARN] map_person: ' + reason)
            print('[INFO] map_person: falling back to a complete synchronization')
    if full_sync:
        persons = _mp_person_snapshot()
        result = _mp_build_person_rows(persons)
    else:
        persons = _mp_person_snapshot(affected_ids)
        current_person_ids = persons.select('person_id').distinct()
        deleted_person_ids = affected_ids.join(current_person_ids, 'person_id', 'left_anti')
        result = _mp_build_person_rows(persons).unionByName(_mp_delete_rows(deleted_person_ids), allowMissingColumns=True)
    _MP_PENDING_RUN = {'full_sync': full_sync, 'reason': reason, 'versions': current_versions}
    print('[INFO] map_person: prepared ' + ('full' if full_sync else 'incremental') + ' synchronization; ' + reason)
    return result

def _mp_apply_comments():
    table_sql = _mp_sql_table(MP_TARGET_TABLE)
    detail = spark.sql('DESCRIBE DETAIL ' + table_sql).select('description').first()
    if not detail or detail['description'] != map_person_comment:
        spark.sql('COMMENT ON TABLE ' + table_sql + " IS '" + _mp_escape_sql_string(map_person_comment) + "'")
    current_fields = {field.name: field for field in spark.table(MP_TARGET_TABLE).schema.fields}
    for field in bronze_contract_schema(MP_TARGET_TABLE).fields:
        comment = field.metadata.get('comment')
        if not comment:
            continue
        current = current_fields.get(field.name)
        if current and current.metadata.get('comment') == comment:
            continue
        spark.sql('ALTER TABLE ' + table_sql + ' ALTER COLUMN ' + _mp_sql_column(field.name) + " COMMENT '" + _mp_escape_sql_string(comment) + "'")
def _mp_run_properties(versions: Dict[str, int]) -> Dict[str, object]:
    address_schema_version = _mp_get_table_property(MP_MAP_ADDRESS_TABLE, 'pipeline.map.schema_version') or 'UNKNOWN'
    properties: Dict[str, object] = {'pipeline.map_person.schema_version': MP_SCHEMA_VERSION, 'pipeline.map_person.logic_version': MP_LOGIC_VERSION, 'pipeline.map_person.address_schema_version': address_schema_version, 'pipeline.map_person.last_success_at': datetime.now(timezone.utc).isoformat(), 'delta.enableChangeDataFeed': 'true', 'delta.enableRowTracking': 'true'}
    for name, version in versions.items():
        properties[_mp_checkpoint_property_name(name)] = str(version)
    return properties

def _mp_commit_run_metadata(versions: Dict[str, int], apply_comments: bool=False):
    _mp_set_table_properties(MP_TARGET_TABLE, _mp_run_properties(versions))
    if apply_comments:
        _mp_apply_comments()

def _mp_write_full_snapshot(upserts: DataFrame):
    aligned = bronze_project_contract(
        _mp_schema_select(upserts),
        MP_TARGET_TABLE,
    )
    stage_table = f'{MP_TARGET_TABLE}__rebuild_{uuid4().hex}'
    try:
        aligned.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(stage_table)
        staged = spark.table(stage_table)
        invalid = staged.where(F.col('person_id').isNull()).limit(1).count() or staged.groupBy('person_id').count().where(F.col('count') > 1).limit(1).count()
        if invalid:
            raise RuntimeError('map_person full snapshot contains null or duplicate person_id')
        staged.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('delta.enableChangeDataFeed', 'true').option('delta.enableRowTracking', 'true').saveAsTable(MP_TARGET_TABLE)
    finally:
        spark.sql(f'DROP TABLE IF EXISTS {_mp_sql_table(stage_table)}')
    try:
        spark.sql('ALTER TABLE ' + _mp_sql_table(MP_TARGET_TABLE) + ' ALTER COLUMN ' + _mp_sql_column('person_id') + ' SET NOT NULL')
    except Exception as error:
        print('[WARN] map_person: could not apply NOT NULL metadata to person_id: ' + str(error).splitlines()[0])

def _mp_merge_incremental(updates: DataFrame):
    updates = updates
    try:
        deletes = updates.filter(F.col('_source_change_type') == 'delete').select('person_id')
        upserts = bronze_project_contract(
            _mp_schema_select(
                updates.filter(F.col('_source_change_type') == 'upsert')
                .drop('_source_change_type')
            ),
            MP_TARGET_TABLE,
        )
        target = DeltaTable.forName(spark, MP_TARGET_TABLE)
        if _mp_has_rows(deletes):
            target.alias('t').merge(deletes.alias('s'), 't.person_id = s.person_id').whenMatchedDelete().execute()
        if _mp_has_rows(upserts):
            compare_columns = [
                column_name for column_name in upserts.columns
                if column_name != 'person_id'
                and column_name not in _MP_COMPARE_EXCLUSIONS
            ]
            changed_condition = ' OR '.join(
                (
                    'NOT (t.' + _mp_sql_column(column_name)
                    + ' <=> s.' + _mp_sql_column(column_name) + ')'
                    for column_name in compare_columns
                )
            )
            assignments = {
                column_name: F.col('s.' + _mp_sql_column(column_name))
                for column_name in upserts.columns
            }
            target.alias('t').merge(upserts.alias('s'), 't.person_id = s.person_id').whenMatchedUpdate(condition=changed_condition, set=assignments).whenNotMatchedInsert(values=assignments).execute()
    finally:
        None

def verify_person_no_duplicates(df: DataFrame) -> bool:
    duplicate_exists = _mp_has_rows(df.filter(F.col('_source_change_type') == 'upsert').groupBy('person_id').count().filter(F.col('count') > 1))
    if duplicate_exists:
        print('[ERROR] map_person update contains duplicate person_id values')
        return False
    return True

def run_map_person_update(force_full_person: bool=False, validate_keys: bool=False):
    """
    Build and apply map_person updates.

    force_full_person=True performs a complete source-to-target synchronization.
    The first deployment also selects full mode automatically because the old schema differs.
    """
    updates = None
    try:
        updates = create_person_mapping_incr(full_refresh=True if force_full_person else None)
        pending = dict(_MP_PENDING_RUN)
        versions = pending.get('versions')
        if versions is None:
            raise RuntimeError('map_person run metadata was not prepared')
        if updates is None:
            _mp_commit_run_metadata(versions, apply_comments=False)
            print('[SUCCESS] map_person: checkpoints advanced; no target rows changed')
            return
        if validate_keys and (not verify_person_no_duplicates(updates)):
            raise ValueError('map_person update aborted because duplicate person_id values were found')
        if pending.get('full_sync'):
            upserts = updates.filter(F.col('_source_change_type') == 'upsert').drop('_source_change_type')
            _mp_write_full_snapshot(upserts)
            _mp_commit_run_metadata(versions, apply_comments=True)
            print('[SUCCESS] map_person: complete synchronization committed')
        else:
            _mp_merge_incremental(updates)
            _mp_commit_run_metadata(versions, apply_comments=False)
            print('[SUCCESS] map_person: incremental synchronization committed')
    finally:
        _mp_release_caches()

def run_map_person_post_deployment_checks() -> Dict[str, int]:
    source_ids = spark.table(MP_RAW_PERSON_TABLE).filter(F.col('PERSON_TYPE_CD').cast('long') == F.lit(MP_CANONICAL_PERSON_TYPE_CD)).select(F.col('PERSON_ID').cast('long').alias('person_id'))
    target = spark.table(MP_TARGET_TABLE)
    target_ids = target.select('person_id')
    source_count = source_ids.count()
    target_count = target_ids.count()
    source_duplicate_ids = source_ids.groupBy('person_id').count().filter(F.col('count') > 1).count()
    target_duplicate_ids = target_ids.groupBy('person_id').count().filter(F.col('count') > 1).count()
    null_target_ids = target_ids.filter(F.col('person_id').isNull()).count()
    source_missing = source_ids.join(target_ids, 'person_id', 'left_anti').count()
    target_orphans = target_ids.join(source_ids, 'person_id', 'left_anti').count()
    source_person = spark.table(MP_RAW_PERSON_TABLE).select(F.col('PERSON_ID').cast('long').alias('person_id'), F.col('ACTIVE_IND').cast('long').alias('_expected_active_ind'), F.to_date(F.coalesce(F.col('ABS_BIRTH_DT_TM').cast('timestamp'), F.col('BIRTH_DT_TM').cast('timestamp'))).alias('_expected_birth_date'))
    comparison = target.select('person_id', 'active_ind', 'birth_date', 'current_address_id').join(source_person, 'person_id', 'inner')
    lifecycle_mismatches = comparison.filter(~F.col('active_ind').eqNullSafe(F.col('_expected_active_ind'))).count()
    birth_mismatches = comparison.filter(~F.col('birth_date').eqNullSafe(F.col('_expected_birth_date'))).count()
    expected_addresses = _mp_selected_person_addresses(target_ids).select('person_id', F.col('current_address_id').alias('_expected_current_address_id'))
    address_mismatches = target.select('person_id', 'current_address_id').join(expected_addresses, 'person_id', 'left').filter(~F.col('current_address_id').eqNullSafe(F.col('_expected_current_address_id'))).count()
    person_id_is_bigint = int(isinstance(next((field.dataType for field in spark.table(MP_TARGET_TABLE).schema.fields if field.name == 'person_id')), LongType))
    results = {'source_rows': source_count, 'target_rows': target_count, 'source_duplicate_person_ids': source_duplicate_ids, 'target_duplicate_person_ids': target_duplicate_ids, 'null_target_person_ids': null_target_ids, 'source_person_ids_missing_from_target': source_missing, 'target_person_ids_not_in_source': target_orphans, 'active_indicator_mismatches': lifecycle_mismatches, 'birth_date_derivation_mismatches': birth_mismatches, 'current_address_selection_mismatches': address_mismatches, 'person_id_is_bigint': person_id_is_bigint}
    failure_keys = {'source_duplicate_person_ids', 'target_duplicate_person_ids', 'null_target_person_ids', 'source_person_ids_missing_from_target', 'target_person_ids_not_in_source', 'active_indicator_mismatches', 'birth_date_derivation_mismatches', 'current_address_selection_mismatches'}
    failures = {key: results[key] for key in failure_keys if results[key] != 0}
    if person_id_is_bigint != 1:
        failures['person_id_is_bigint'] = person_id_is_bigint
    if failures:
        raise AssertionError('map_person post-deployment checks failed: ' + str(failures))
    print('[OK] map_person post-deployment checks passed: ' + str(results))
    return results

def _mp_create_widgets():
    try:
        dbutils.widgets.dropdown('execute_person_updates', 'false', ['false', 'true'])
        dbutils.widgets.dropdown('force_full_person', 'false', ['false', 'true'])
        dbutils.widgets.dropdown('validate_person_keys', 'false', ['false', 'true'])
        dbutils.widgets.dropdown('run_person_checks', 'false', ['false', 'true'])
    except Exception:
        pass

def _mp_widget_bool(name: str, default: bool=False) -> bool:
    try:
        return dbutils.widgets.get(name).strip().lower() == 'true'
    except Exception:
        return default

try:
    _pipeline_run_recoverable(
        'map_person',
        _PIPELINE_FULL_REFRESH,
        lambda: run_map_person_update(
            force_full_person=False,
            validate_keys=False,
        ),
        lambda: run_map_person_update(
            force_full_person=True,
            validate_keys=False,
        ),
        targets=[MP_TARGET_TABLE],
    )
    _PIPELINE_UPDATED_TARGETS.extend(['4_prod.bronze.map_person'])
    _pipeline_mark_component_complete('map_person', ['4_prod.bronze.map_person'])
    _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_person'})
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

_pipeline_component_start("map_care_site")
_pipeline_shared_update_table = globals().get('update_table')
_pipeline_shared_table_exists = globals().get('table_exists')
_pipeline_shared_get_max_timestamp = globals().get('get_max_timestamp')
_pipeline_shared_has_cdf_enabled = globals().get('has_cdf_enabled')
_pipeline_shared_get_incremental = globals().get('get_incremental_data_with_cdf')
globals().pop('_map_updates_original_update_table', None)
globals().pop('_map_medical_personnel_original_update_table', None)
globals().pop('_MAP_UPDATES_ORIGINAL_UPDATE_TABLE', None)

import uuid
from functools import reduce
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window
MAP_CARE_SITE_TARGET = '4_prod.bronze.map_care_site'
MAP_LOCATION_SOURCE = '4_prod.raw.mill_location'
MAP_LOCATION_GROUP_SOURCE = '4_prod.raw.mill_location_group'
MAP_ORGANIZATION_SOURCE = '4_prod.raw.mill_organization'
MAP_CODE_VALUE_SOURCE = '3_lookup.mill.mill_code_value'
MAP_ADDRESS_SOURCE = '4_prod.bronze.map_address'
BUILDING_GROUP_TYPE_CD = 778
FACILITY_GROUP_TYPE_CD = 783
BUILDING_LOCATION_TYPE_CD = 778
FACILITY_LOCATION_TYPE_CD = 783
NURSE_UNIT_LOCATION_TYPE_CD = 794
MAP_CARE_SITE_COMMENT = 'Curated care-site foundation scoped to nurse units (location_type_cd = 794), derived from Millennium location, hierarchy, organization, code-value and organization-address sources. Beds, rooms, inventory locators, HIM locations and every other location type are intentionally excluded and remain available in 4_prod.raw.mill_location. Rows no longer present in the current nurse-unit source are retained with source_present_ind = false.'

def _schema_field(name, data_type, comment):
    return StructField(name, data_type, True, {'comment': comment})
schema_map_care_site_v2 = StructType([_schema_field('care_site_cd', LongType(), 'Millennium LOCATION_CD, represented as an integer identifier.'), _schema_field('location_type_cd', LongType(), 'Millennium location type code.'), _schema_field('location_type_name', StringType(), 'Resolved display or description of the location type.'), _schema_field('care_site_name', StringType(), 'Best available source name: DISPLAY, DESCRIPTION, then source code.'), _schema_field('care_site_display', StringType(), 'Raw code-value DISPLAY for LOCATION_CD.'), _schema_field('care_site_description', StringType(), 'Raw code-value DESCRIPTION for LOCATION_CD.'), _schema_field('care_site_cdf_meaning', StringType(), 'Raw code-value CDF_MEANING for LOCATION_CD.'), _schema_field('care_site_code_active_ind', LongType(), 'ACTIVE_IND from the LOCATION_CD code-value row.'), _schema_field('care_site_code_begin_effective_dt_tm', TimestampType(), 'Code-value begin-effective timestamp.'), _schema_field('care_site_code_end_effective_dt_tm', TimestampType(), 'Code-value end-effective timestamp.'), _schema_field('care_site_code_adc_updt', TimestampType(), 'ADC_UPDT from the LOCATION_CD code-value row.'), _schema_field('location_type_code_active_ind', LongType(), 'ACTIVE_IND from the location-type code-value row.'), _schema_field('location_type_code_adc_updt', TimestampType(), 'ADC_UPDT from the location-type code-value row.'), _schema_field('building_cd', LongType(), 'Deterministically selected building code, or the location itself for building rows.'), _schema_field('building_name', StringType(), 'Best available building name.'), _schema_field('building_display', StringType(), 'Raw code-value DISPLAY for the selected building.'), _schema_field('building_description', StringType(), 'Raw code-value DESCRIPTION for the selected building.'), _schema_field('building_code_active_ind', LongType(), 'ACTIVE_IND from the selected building code-value row.'), _schema_field('building_code_adc_updt', TimestampType(), 'ADC_UPDT from the selected building code-value row.'), _schema_field('building_parent_count', IntegerType(), 'Number of distinct building parents represented in location_group.'), _schema_field('building_active_parent_count', IntegerType(), 'Number of active distinct building parents.'), _schema_field('building_relation_active_ind', LongType(), 'ACTIVE_IND for the selected building relationship.'), _schema_field('building_relation_effective_now_ind', BooleanType(), 'Whether the selected building relationship is effective now.'), _schema_field('building_relation_beg_effective_dt_tm', TimestampType(), 'Begin-effective timestamp for the selected building relationship.'), _schema_field('building_relation_end_effective_dt_tm', TimestampType(), 'End-effective timestamp for the selected building relationship.'), _schema_field('building_relation_sequence', DoubleType(), 'Source display sequence for the selected building relationship.'), _schema_field('building_relation_adc_updt', TimestampType(), 'ADC_UPDT for the selected building relationship.'), _schema_field('building_hierarchy_ambiguous_ind', BooleanType(), 'True when more than one distinct building parent exists.'), _schema_field('building_resolution_source', StringType(), 'SELF, RELATIONSHIP or NONE.'), _schema_field('facility_cd', LongType(), 'Deterministically selected facility code.'), _schema_field('facility_name', StringType(), 'Best available facility name.'), _schema_field('facility_display', StringType(), 'Raw code-value DISPLAY for the selected facility.'), _schema_field('facility_description', StringType(), 'Raw code-value DESCRIPTION for the selected facility.'), _schema_field('facility_code_active_ind', LongType(), 'ACTIVE_IND from the selected facility code-value row.'), _schema_field('facility_code_adc_updt', TimestampType(), 'ADC_UPDT from the selected facility code-value row.'), _schema_field('facility_parent_count', IntegerType(), 'Number of distinct facility parents on the selected relationship path.'), _schema_field('facility_active_parent_count', IntegerType(), 'Number of active distinct facility parents on the selected path.'), _schema_field('facility_relation_active_ind', LongType(), 'ACTIVE_IND for the selected facility relationship.'), _schema_field('facility_relation_effective_now_ind', BooleanType(), 'Whether the selected facility relationship is effective now.'), _schema_field('facility_relation_beg_effective_dt_tm', TimestampType(), 'Begin-effective timestamp for the selected facility relationship.'), _schema_field('facility_relation_end_effective_dt_tm', TimestampType(), 'End-effective timestamp for the selected facility relationship.'), _schema_field('facility_relation_sequence', DoubleType(), 'Source display sequence for the selected facility relationship.'), _schema_field('facility_relation_adc_updt', TimestampType(), 'ADC_UPDT for the selected facility relationship.'), _schema_field('facility_hierarchy_ambiguous_ind', BooleanType(), 'True when more than one distinct facility parent exists.'), _schema_field('facility_resolution_source', StringType(), 'SELF, VIA_BUILDING, DIRECT or NONE.'), _schema_field('ORGANIZATION_ID', LongType(), 'Millennium organization identifier.'), _schema_field('organization_name', StringType(), 'Best available organization name.'), _schema_field('organization_name_key', StringType(), 'Normalized organization name key.'), _schema_field('organization_active_ind', LongType(), 'Organization ACTIVE_IND.'), _schema_field('organization_active_status_cd', LongType(), 'Organization ACTIVE_STATUS_CD.'), _schema_field('organization_beg_effective_dt_tm', TimestampType(), 'Organization begin-effective timestamp.'), _schema_field('organization_end_effective_dt_tm', TimestampType(), 'Organization end-effective timestamp.'), _schema_field('organization_status_cd', LongType(), 'Organization status code.'), _schema_field('organization_class_cd', LongType(), 'Organization class code.'), _schema_field('organization_external_ind', LongType(), 'Whether the organization is external.'), _schema_field('organization_contributor_system_cd', LongType(), 'Organization contributor-system code.'), _schema_field('organization_contributor_source_cd', LongType(), 'Organization contributor-source code.'), _schema_field('organization_trust', StringType(), 'Trust classification from the organization source.'), _schema_field('organization_adc_updt', TimestampType(), 'ADC_UPDT from the organization row.'), _schema_field('address_id', LongType(), 'Best available organization address: current address, otherwise latest known address.'), _schema_field('current_address_id', LongType(), 'Address identifier only when the selected organization address is current.'), _schema_field('latest_known_address_id', LongType(), 'Latest known organization address regardless of current status.'), _schema_field('address_selection_status', StringType(), 'CURRENT, LATEST_INACTIVE, LATEST_NOT_EFFECTIVE or LATEST_KNOWN.'), _schema_field('address_is_current_ind', BooleanType(), 'Whether the selected organization address is active and effective now.'), _schema_field('address_active_ind', LongType(), 'ACTIVE_IND from the selected address.'), _schema_field('address_beg_effective_dt_tm', TimestampType(), 'Begin-effective timestamp from the selected address.'), _schema_field('address_end_effective_dt_tm', TimestampType(), 'End-effective timestamp from the selected address.'), _schema_field('masked_zipcode', StringType(), 'Masked postcode from map_address.'), _schema_field('city', StringType(), 'City from map_address.'), _schema_field('full_street_address', StringType(), 'Organization street address from map_address.'), _schema_field('lsoa', StringType(), 'Lower Layer Super Output Area from map_address.'), _schema_field('uprn', LongType(), 'Unique Property Reference Number from map_address.'), _schema_field('latitude', DoubleType(), 'Latitude from map_address.'), _schema_field('longitude', DoubleType(), 'Longitude from map_address.'), _schema_field('address_match_algorithm', IntegerType(), 'Address matching algorithm identifier.'), _schema_field('address_match_confidence', DoubleType(), 'Address matching confidence.'), _schema_field('address_match_quality', StringType(), 'Address matching quality label.'), _schema_field('address_country_cd', StringType(), 'Country description/code from map_address.'), _schema_field('address_adc_updt', TimestampType(), 'ADC_UPDT from the selected address.'), _schema_field('location_active_ind', LongType(), 'Location ACTIVE_IND; retained rather than filtered.'), _schema_field('location_active_status_cd', LongType(), 'Location ACTIVE_STATUS_CD.'), _schema_field('location_beg_effective_dt_tm', TimestampType(), 'Location begin-effective timestamp.'), _schema_field('location_end_effective_dt_tm', TimestampType(), 'Location end-effective timestamp.'), _schema_field('census_ind', LongType(), 'Whether the location participates in census reporting.'), _schema_field('registration_ind', LongType(), 'Whether registration is supported at the location.'), _schema_field('resource_ind', LongType(), 'Whether service-resource rows exist for the location.'), _schema_field('patcare_node_ind', LongType(), 'Patient-care node indicator.'), _schema_field('contributor_system_cd', LongType(), 'Location contributor-system code.'), _schema_field('contributor_source_cd', LongType(), 'Location contributor-source code.'), _schema_field('trust', StringType(), 'Trust classification from the location source.'), _schema_field('location_updt_dt_tm', TimestampType(), 'Native source update timestamp.'), _schema_field('location_adc_updt', TimestampType(), 'ADC_UPDT from the location source.'), _schema_field('source_present_ind', BooleanType(), 'True when the key is present in the current location source snapshot.'), _schema_field('source_absent_detected_ts', TimestampType(), 'First pipeline timestamp at which a previously present key was absent.'), _schema_field('ADC_UPDT', TimestampType(), 'Greatest update timestamp across all contributing source records.')])
schema_map_care_site = schema_map_care_site_v2
map_care_site_comment = MAP_CARE_SITE_COMMENT
_LOW_TS = F.lit('1900-01-01 00:00:00').cast('timestamp')
_HIGH_TS = F.lit('9999-12-31 23:59:59').cast('timestamp')

def _effective_now(begin_col, end_col):
    now = F.current_timestamp()
    return (F.coalesce(begin_col, _LOW_TS) <= now) & (F.coalesce(end_col, _HIGH_TS) > now)

def _non_blank(column):
    return F.when(column.isNotNull() & (F.trim(column.cast('string')) != ''), column.cast('string'))

def _best_name(display_col, description_col, code_col):
    return F.coalesce(_non_blank(display_col), _non_blank(description_col), code_col.cast('string'))

def _table_exists(table_name):
    return spark.catalog.tableExists(table_name)

def _quoted_identifier(value):
    tick = chr(96)
    return tick + value.replace(tick, tick + tick) + tick

def _quoted_table(table_name):
    return '.'.join((_quoted_identifier(part) for part in table_name.split('.')))

def _sql_string(value):
    return value.replace('\\', '\\\\').replace("'", "''")

def _align_to_schema(df, schema, defaults=None, allow_missing=False):
    defaults = defaults or {}
    actual_names = {name.lower(): name for name in df.columns}
    missing = [field.name for field in schema.fields if field.name.lower() not in actual_names and field.name not in defaults]
    if missing and (not allow_missing):
        raise ValueError('DataFrame is missing required map_care_site columns: ' + ', '.join(missing))
    expressions = []
    for field in schema.fields:
        actual_name = actual_names.get(field.name.lower())
        if actual_name is not None:
            expression = F.col(_quoted_identifier(actual_name))
        elif field.name in defaults:
            default_value = defaults[field.name]
            expression = default_value if hasattr(default_value, '_jc') else F.lit(default_value)
        else:
            expression = F.lit(None)
        expressions.append(expression.cast(field.dataType).alias(field.name, metadata=field.metadata))
    return df.select(*expressions)

def _ranked_location_relationships(group_type_cd, relation_name):
    """
    Return one deterministic relationship per child plus relationship-quality columns.

    No source relationships are filtered out for being inactive or ineffective. Those
    attributes are used only to rank the canonical relationship and are retained in
    the output so downstream consumers can make their own decisions.
    """
    base = spark.table(MAP_LOCATION_GROUP_SOURCE).filter(F.col('LOCATION_GROUP_TYPE_CD') == F.lit(group_type_cd)).select(F.col('CHILD_LOC_CD').cast('long').alias('CHILD_LOC_CD'), F.col('PARENT_LOC_CD').cast('long').alias('PARENT_LOC_CD'), F.col('ACTIVE_IND').cast('long').alias('ACTIVE_IND'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('BEG_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('END_EFFECTIVE_DT_TM'), F.col('SEQUENCE').cast('double').alias('SEQUENCE'), F.col('UPDT_DT_TM').cast('timestamp').alias('UPDT_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('ADC_UPDT')).withColumn('_effective_now_ind', _effective_now(F.col('BEG_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM'))).withColumn('_current_relation_ind', (F.col('ACTIVE_IND') == 1) & F.col('_effective_now_ind'))
    stats = base.groupBy('CHILD_LOC_CD').agg(F.countDistinct('PARENT_LOC_CD').cast('int').alias(f'{relation_name}_parent_count'), F.countDistinct(F.when(F.col('ACTIVE_IND') == 1, F.col('PARENT_LOC_CD'))).cast('int').alias(f'{relation_name}_active_parent_count')).withColumn(f'{relation_name}_hierarchy_ambiguous_ind', F.col(f'{relation_name}_parent_count') > 1)
    ranking = Window.partitionBy('CHILD_LOC_CD').orderBy(F.col('_current_relation_ind').desc(), (F.col('ACTIVE_IND') == 1).desc(), F.col('_effective_now_ind').desc(), F.col('ADC_UPDT').desc_nulls_last(), F.col('UPDT_DT_TM').desc_nulls_last(), F.col('SEQUENCE').asc_nulls_last(), F.col('PARENT_LOC_CD').asc_nulls_last())
    selected = base.withColumn('_relationship_rank', F.row_number().over(ranking)).filter(F.col('_relationship_rank') == 1).select('CHILD_LOC_CD', F.col('PARENT_LOC_CD').alias(f'{relation_name}_cd'), F.col('ACTIVE_IND').alias(f'{relation_name}_relation_active_ind'), F.col('_effective_now_ind').alias(f'{relation_name}_relation_effective_now_ind'), F.col('BEG_EFFECTIVE_DT_TM').alias(f'{relation_name}_relation_beg_effective_dt_tm'), F.col('END_EFFECTIVE_DT_TM').alias(f'{relation_name}_relation_end_effective_dt_tm'), F.col('SEQUENCE').alias(f'{relation_name}_relation_sequence'), F.col('ADC_UPDT').alias(f'{relation_name}_relation_adc_updt'))
    return selected.join(stats, 'CHILD_LOC_CD', 'left')

def create_building_locations():
    """Drop-in deterministic replacement for the original helper."""
    return _ranked_location_relationships(BUILDING_GROUP_TYPE_CD, 'building')

def create_facility_locations():
    """Drop-in deterministic replacement for the original helper."""
    return _ranked_location_relationships(FACILITY_GROUP_TYPE_CD, 'facility')

def _renamed_building_relationships(df):
    return df.select(F.col('CHILD_LOC_CD').alias('_building_child_cd'), F.col('building_cd').alias('_building_parent_cd'), F.col('building_parent_count').alias('_building_parent_count'), F.col('building_active_parent_count').alias('_building_active_parent_count'), F.col('building_relation_active_ind').alias('_building_relation_active_ind'), F.col('building_relation_effective_now_ind').alias('_building_relation_effective_now_ind'), F.col('building_relation_beg_effective_dt_tm').alias('_building_relation_beg_effective_dt_tm'), F.col('building_relation_end_effective_dt_tm').alias('_building_relation_end_effective_dt_tm'), F.col('building_relation_sequence').alias('_building_relation_sequence'), F.col('building_relation_adc_updt').alias('_building_relation_adc_updt'), F.col('building_hierarchy_ambiguous_ind').alias('_building_hierarchy_ambiguous_ind'))

def _renamed_facility_relationships(df, stem):
    return df.select(F.col('CHILD_LOC_CD').alias(f'{stem}_child_cd'), F.col('facility_cd').alias(f'{stem}_cd'), F.col('facility_parent_count').alias(f'{stem}_parent_count'), F.col('facility_active_parent_count').alias(f'{stem}_active_parent_count'), F.col('facility_relation_active_ind').alias(f'{stem}_relation_active_ind'), F.col('facility_relation_effective_now_ind').alias(f'{stem}_relation_effective_now_ind'), F.col('facility_relation_beg_effective_dt_tm').alias(f'{stem}_relation_beg_effective_dt_tm'), F.col('facility_relation_end_effective_dt_tm').alias(f'{stem}_relation_end_effective_dt_tm'), F.col('facility_relation_sequence').alias(f'{stem}_relation_sequence'), F.col('facility_relation_adc_updt').alias(f'{stem}_relation_adc_updt'), F.col('facility_hierarchy_ambiguous_ind').alias(f'{stem}_hierarchy_ambiguous_ind'))

def _current_locations():
    """Read every source location without active, effective, trust or type filters."""
    source = spark.table(MAP_LOCATION_SOURCE).select(F.col('LOCATION_CD').cast('long').alias('location_cd'), F.col('LOCATION_TYPE_CD').cast('long').alias('location_type_cd'), F.col('ORGANIZATION_ID').cast('long').alias('organization_id'), F.col('ACTIVE_IND').cast('long').alias('location_active_ind'), F.col('ACTIVE_STATUS_CD').cast('long').alias('location_active_status_cd'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('location_beg_effective_dt_tm'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('location_end_effective_dt_tm'), F.col('CENSUS_IND').cast('long').alias('census_ind'), F.col('REGISTRATION_IND').cast('long').alias('registration_ind'), F.col('RESOURCE_IND').cast('long').alias('resource_ind'), F.col('PATCARE_NODE_IND').cast('long').alias('patcare_node_ind'), F.col('CONTRIBUTOR_SYSTEM_CD').cast('long').alias('contributor_system_cd'), F.col('CONTRIBUTOR_SOURCE_CD').cast('long').alias('contributor_source_cd'), F.col('Trust').cast('string').alias('trust'), F.col('UPDT_DT_TM').cast('timestamp').alias('location_updt_dt_tm'), F.col('ADC_UPDT').cast('timestamp').alias('location_adc_updt'))
    ranking = Window.partitionBy('location_cd').orderBy(F.col('location_adc_updt').desc_nulls_last(), F.col('location_updt_dt_tm').desc_nulls_last(), F.col('location_cd').asc_nulls_last())
    return source.withColumn('_location_rank', F.row_number().over(ranking)).filter(F.col('_location_rank') == 1).drop('_location_rank')

def _current_organizations():
    """Return one deterministic organization row per organization identifier."""
    source = spark.table(MAP_ORGANIZATION_SOURCE).select(F.col('ORGANIZATION_ID').cast('long').alias('_organization_id'), F.col('ORG_NAME').cast('string').alias('_organization_name'), F.col('ORG_NAME_KEY').cast('string').alias('_organization_name_key'), F.col('ACTIVE_IND').cast('long').alias('_organization_active_ind'), F.col('ACTIVE_STATUS_CD').cast('long').alias('_organization_active_status_cd'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('_organization_beg_effective_dt_tm'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('_organization_end_effective_dt_tm'), F.col('ORG_STATUS_CD').cast('long').alias('_organization_status_cd'), F.col('ORG_CLASS_CD').cast('long').alias('_organization_class_cd'), F.col('EXTERNAL_IND').cast('long').alias('_organization_external_ind'), F.col('CONTRIBUTOR_SYSTEM_CD').cast('long').alias('_organization_contributor_system_cd'), F.col('CONTRIBUTOR_SOURCE_CD').cast('long').alias('_organization_contributor_source_cd'), F.col('Trust').cast('string').alias('_organization_trust'), F.col('UPDT_DT_TM').cast('timestamp').alias('_organization_updt_dt_tm'), F.col('ADC_UPDT').cast('timestamp').alias('_organization_adc_updt'))
    ranking = Window.partitionBy('_organization_id').orderBy(F.col('_organization_adc_updt').desc_nulls_last(), F.col('_organization_updt_dt_tm').desc_nulls_last(), F.col('_organization_active_ind').desc_nulls_last(), F.col('_organization_id').asc_nulls_last())
    return source.withColumn('_organization_rank', F.row_number().over(ranking)).filter(F.col('_organization_rank') == 1).drop('_organization_rank', '_organization_updt_dt_tm')

def _organization_addresses():
    """
    Rank only organization addresses.

    Current addresses are preferred, but the latest known address is retained as a
    fallback. Address status and effective dates make that fallback explicit.
    """
    source = spark.table(MAP_ADDRESS_SOURCE).filter(F.upper(F.trim(F.col('PARENT_ENTITY_NAME'))) == 'ORGANIZATION').select(F.col('PARENT_ENTITY_ID').cast('long').alias('_address_organization_id'), F.col('ADDRESS_ID').cast('long').alias('_selected_address_id'), F.col('ACTIVE_IND').cast('long').alias('_address_active_ind'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('_address_beg_effective_dt_tm'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('_address_end_effective_dt_tm'), F.col('masked_zipcode').cast('string').alias('_masked_zipcode'), F.col('CITY').cast('string').alias('_city'), F.col('full_street_address').cast('string').alias('_full_street_address'), F.col('LSOA').cast('string').alias('_lsoa'), F.col('UPRN').cast('long').alias('_uprn'), F.col('LATITUDE').cast('double').alias('_latitude'), F.col('LONGITUDE').cast('double').alias('_longitude'), F.col('match_algorithm').cast('int').alias('_address_match_algorithm'), F.col('match_confidence').cast('double').alias('_address_match_confidence'), F.col('match_quality').cast('string').alias('_address_match_quality'), F.col('country_cd').cast('string').alias('_address_country_cd'), F.col('ADC_UPDT').cast('timestamp').alias('_address_adc_updt')).withColumn('_address_effective_now_ind', _effective_now(F.col('_address_beg_effective_dt_tm'), F.col('_address_end_effective_dt_tm'))).withColumn('_address_is_current_ind', (F.col('_address_active_ind') == 1) & F.col('_address_effective_now_ind'))
    ranking = Window.partitionBy('_address_organization_id').orderBy(F.col('_address_is_current_ind').desc(), (F.col('_address_active_ind') == 1).desc(), F.col('_address_effective_now_ind').desc(), F.col('_address_adc_updt').desc_nulls_last(), F.col('_selected_address_id').desc_nulls_last())
    return source.withColumn('_address_rank', F.row_number().over(ranking)).filter(F.col('_address_rank') == 1).drop('_address_rank').withColumn('_current_address_id', F.when(F.col('_address_is_current_ind'), F.col('_selected_address_id')).cast('long')).withColumn('_latest_known_address_id', F.col('_selected_address_id').cast('long')).withColumn('_address_selection_status', F.when(F.col('_address_is_current_ind'), F.lit('CURRENT')).when(F.col('_address_active_ind') != 1, F.lit('LATEST_INACTIVE')).when(~F.col('_address_effective_now_ind'), F.lit('LATEST_NOT_EFFECTIVE')).otherwise(F.lit('LATEST_KNOWN')))

def _relevant_code_values(locations, buildings, facilities):
    """Project and restrict code_value before joining it repeatedly."""
    needed_codes = locations.select(F.col('location_cd').alias('code_value')).unionByName(locations.select(F.col('location_type_cd').alias('code_value'))).unionByName(buildings.select(F.col('building_cd').alias('code_value'))).unionByName(facilities.select(F.col('facility_cd').alias('code_value'))).filter(F.col('code_value').isNotNull()).distinct()
    source = spark.table(MAP_CODE_VALUE_SOURCE).select(F.col('CODE_VALUE').cast('long').alias('code_value'), F.col('CODE_SET').cast('long').alias('code_set'), F.col('CDF_MEANING').cast('string').alias('cdf_meaning'), F.col('DISPLAY').cast('string').alias('display'), F.col('DESCRIPTION').cast('string').alias('description'), F.col('ACTIVE_IND').cast('long').alias('code_active_ind'), F.col('BEGIN_EFFECTIVE_DT_TM').cast('timestamp').alias('code_begin_effective_dt_tm'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('code_end_effective_dt_tm'), F.col('UPDT_DT_TM').cast('timestamp').alias('code_updt_dt_tm'), F.col('ADC_UPDT').cast('timestamp').alias('code_adc_updt'))
    ranking = Window.partitionBy('code_value').orderBy(F.col('code_adc_updt').desc_nulls_last(), F.col('code_updt_dt_tm').desc_nulls_last(), F.col('code_value').asc_nulls_last())
    deduplicated = source.withColumn('_code_rank', F.row_number().over(ranking)).filter(F.col('_code_rank') == 1).drop('_code_rank', 'code_updt_dt_tm')
    return deduplicated.join(F.broadcast(needed_codes), 'code_value', 'left_semi')

def create_care_site_mapping_incr():
    """
    Drop-in replacement retaining the historical function name.

    The lane is scoped to nurse units (location_type_cd = 794). Beds, rooms,
    inventory locators, HIM locations and every other location type are
    intentionally excluded and remain available in 4_prod.raw.mill_location.

    Within that scope this returns a full current snapshot. Full reconciliation
    is required because care-site attributes can change through any of five
    upstream tables and source removals cannot be detected with ADC_UPDT alone.
    """
    locations = _current_locations().filter(F.col('location_type_cd') == F.lit(NURSE_UNIT_LOCATION_TYPE_CD))
    building_relationships = create_building_locations()
    facility_relationships = create_facility_locations()
    building_link = _renamed_building_relationships(building_relationships)
    via_facility = _renamed_facility_relationships(facility_relationships, '_via_facility')
    direct_facility = _renamed_facility_relationships(facility_relationships, '_direct_facility')
    hierarchy = locations.alias('loc').join(building_link.alias('b'), F.col('loc.location_cd') == F.col('b._building_child_cd'), 'left').withColumn('building_cd', F.when(F.col('loc.location_type_cd') == BUILDING_LOCATION_TYPE_CD, F.col('loc.location_cd')).otherwise(F.col('b._building_parent_cd'))).withColumn('building_resolution_source', F.when(F.col('loc.location_type_cd') == BUILDING_LOCATION_TYPE_CD, F.lit('SELF')).when(F.col('b._building_parent_cd').isNotNull(), F.lit('RELATIONSHIP')).otherwise(F.lit('NONE'))).withColumn('building_parent_count', F.when(F.col('loc.location_type_cd') == BUILDING_LOCATION_TYPE_CD, F.lit(0)).otherwise(F.coalesce(F.col('b._building_parent_count'), F.lit(0)))).withColumn('building_active_parent_count', F.when(F.col('loc.location_type_cd') == BUILDING_LOCATION_TYPE_CD, F.lit(0)).otherwise(F.coalesce(F.col('b._building_active_parent_count'), F.lit(0)))).withColumn('building_relation_active_ind', F.when(F.col('loc.location_type_cd') != BUILDING_LOCATION_TYPE_CD, F.col('b._building_relation_active_ind')).cast('long')).withColumn('building_relation_effective_now_ind', F.when(F.col('loc.location_type_cd') != BUILDING_LOCATION_TYPE_CD, F.col('b._building_relation_effective_now_ind')).cast('boolean')).withColumn('building_relation_beg_effective_dt_tm', F.when(F.col('loc.location_type_cd') != BUILDING_LOCATION_TYPE_CD, F.col('b._building_relation_beg_effective_dt_tm')).cast('timestamp')).withColumn('building_relation_end_effective_dt_tm', F.when(F.col('loc.location_type_cd') != BUILDING_LOCATION_TYPE_CD, F.col('b._building_relation_end_effective_dt_tm')).cast('timestamp')).withColumn('building_relation_sequence', F.when(F.col('loc.location_type_cd') != BUILDING_LOCATION_TYPE_CD, F.col('b._building_relation_sequence')).cast('double')).withColumn('building_relation_adc_updt', F.when(F.col('loc.location_type_cd') != BUILDING_LOCATION_TYPE_CD, F.col('b._building_relation_adc_updt')).cast('timestamp')).withColumn('building_hierarchy_ambiguous_ind', F.when(F.col('loc.location_type_cd') == BUILDING_LOCATION_TYPE_CD, F.lit(False)).otherwise(F.coalesce(F.col('b._building_hierarchy_ambiguous_ind'), F.lit(False)))).join(via_facility.alias('vf'), F.col('building_cd') == F.col('vf._via_facility_child_cd'), 'left').join(direct_facility.alias('df'), F.col('loc.location_cd') == F.col('df._direct_facility_child_cd'), 'left').withColumn('_use_via_facility', (F.col('loc.location_type_cd') != FACILITY_LOCATION_TYPE_CD) & F.col('vf._via_facility_cd').isNotNull()).withColumn('_use_direct_facility', (F.col('loc.location_type_cd') != FACILITY_LOCATION_TYPE_CD) & F.col('vf._via_facility_cd').isNull() & F.col('df._direct_facility_cd').isNotNull()).withColumn('facility_cd', F.when(F.col('loc.location_type_cd') == FACILITY_LOCATION_TYPE_CD, F.col('loc.location_cd')).when(F.col('_use_via_facility'), F.col('vf._via_facility_cd')).when(F.col('_use_direct_facility'), F.col('df._direct_facility_cd')).cast('long')).withColumn('facility_resolution_source', F.when(F.col('loc.location_type_cd') == FACILITY_LOCATION_TYPE_CD, F.lit('SELF')).when(F.col('_use_via_facility'), F.lit('VIA_BUILDING')).when(F.col('_use_direct_facility'), F.lit('DIRECT')).otherwise(F.lit('NONE'))).withColumn('facility_parent_count', F.when(F.col('loc.location_type_cd') == FACILITY_LOCATION_TYPE_CD, F.lit(0)).when(F.col('_use_via_facility'), F.col('vf._via_facility_parent_count')).when(F.col('_use_direct_facility'), F.col('df._direct_facility_parent_count')).otherwise(F.lit(0)).cast('int')).withColumn('facility_active_parent_count', F.when(F.col('loc.location_type_cd') == FACILITY_LOCATION_TYPE_CD, F.lit(0)).when(F.col('_use_via_facility'), F.col('vf._via_facility_active_parent_count')).when(F.col('_use_direct_facility'), F.col('df._direct_facility_active_parent_count')).otherwise(F.lit(0)).cast('int')).withColumn('facility_relation_active_ind', F.when(F.col('_use_via_facility'), F.col('vf._via_facility_relation_active_ind')).when(F.col('_use_direct_facility'), F.col('df._direct_facility_relation_active_ind')).cast('long')).withColumn('facility_relation_effective_now_ind', F.when(F.col('_use_via_facility'), F.col('vf._via_facility_relation_effective_now_ind')).when(F.col('_use_direct_facility'), F.col('df._direct_facility_relation_effective_now_ind')).cast('boolean')).withColumn('facility_relation_beg_effective_dt_tm', F.when(F.col('_use_via_facility'), F.col('vf._via_facility_relation_beg_effective_dt_tm')).when(F.col('_use_direct_facility'), F.col('df._direct_facility_relation_beg_effective_dt_tm')).cast('timestamp')).withColumn('facility_relation_end_effective_dt_tm', F.when(F.col('_use_via_facility'), F.col('vf._via_facility_relation_end_effective_dt_tm')).when(F.col('_use_direct_facility'), F.col('df._direct_facility_relation_end_effective_dt_tm')).cast('timestamp')).withColumn('facility_relation_sequence', F.when(F.col('_use_via_facility'), F.col('vf._via_facility_relation_sequence')).when(F.col('_use_direct_facility'), F.col('df._direct_facility_relation_sequence')).cast('double')).withColumn('facility_relation_adc_updt', F.when(F.col('_use_via_facility'), F.col('vf._via_facility_relation_adc_updt')).when(F.col('_use_direct_facility'), F.col('df._direct_facility_relation_adc_updt')).cast('timestamp')).withColumn('facility_hierarchy_ambiguous_ind', F.when(F.col('loc.location_type_cd') == FACILITY_LOCATION_TYPE_CD, F.lit(False)).when(F.col('_use_via_facility'), F.col('vf._via_facility_hierarchy_ambiguous_ind')).when(F.col('_use_direct_facility'), F.col('df._direct_facility_hierarchy_ambiguous_ind')).otherwise(F.lit(False)).cast('boolean'))
    code_values = _relevant_code_values(locations, building_relationships, facility_relationships)
    organizations = _current_organizations()
    addresses = _organization_addresses()
    enriched = hierarchy.alias('h').join(F.broadcast(code_values).alias('cs'), F.col('h.location_cd') == F.col('cs.code_value'), 'left').join(F.broadcast(code_values).alias('lt'), F.col('h.location_type_cd') == F.col('lt.code_value'), 'left').join(F.broadcast(code_values).alias('bc'), F.col('h.building_cd') == F.col('bc.code_value'), 'left').join(F.broadcast(code_values).alias('fc'), F.col('h.facility_cd') == F.col('fc.code_value'), 'left').join(F.broadcast(organizations).alias('org'), F.col('h.organization_id') == F.col('org._organization_id'), 'left').join(F.broadcast(addresses).alias('addr'), F.col('h.organization_id') == F.col('addr._address_organization_id'), 'left')
    source_adc_updt = F.greatest(F.col('h.location_adc_updt'), F.col('cs.code_adc_updt'), F.col('lt.code_adc_updt'), F.col('h.building_relation_adc_updt'), F.col('bc.code_adc_updt'), F.col('h.facility_relation_adc_updt'), F.col('fc.code_adc_updt'), F.col('org._organization_adc_updt'), F.col('addr._address_adc_updt'))
    result = enriched.select(F.col('h.location_cd').alias('care_site_cd'), F.col('h.location_type_cd').alias('location_type_cd'), _best_name(F.col('lt.display'), F.col('lt.description'), F.col('h.location_type_cd')).alias('location_type_name'), _best_name(F.col('cs.display'), F.col('cs.description'), F.col('h.location_cd')).alias('care_site_name'), F.col('cs.display').alias('care_site_display'), F.col('cs.description').alias('care_site_description'), F.col('cs.cdf_meaning').alias('care_site_cdf_meaning'), F.col('cs.code_active_ind').alias('care_site_code_active_ind'), F.col('cs.code_begin_effective_dt_tm').alias('care_site_code_begin_effective_dt_tm'), F.col('cs.code_end_effective_dt_tm').alias('care_site_code_end_effective_dt_tm'), F.col('cs.code_adc_updt').alias('care_site_code_adc_updt'), F.col('lt.code_active_ind').alias('location_type_code_active_ind'), F.col('lt.code_adc_updt').alias('location_type_code_adc_updt'), F.col('h.building_cd').alias('building_cd'), F.when(F.col('h.building_cd').isNotNull(), _best_name(F.col('bc.display'), F.col('bc.description'), F.col('h.building_cd'))).alias('building_name'), F.col('bc.display').alias('building_display'), F.col('bc.description').alias('building_description'), F.col('bc.code_active_ind').alias('building_code_active_ind'), F.col('bc.code_adc_updt').alias('building_code_adc_updt'), F.col('h.building_parent_count').alias('building_parent_count'), F.col('h.building_active_parent_count').alias('building_active_parent_count'), F.col('h.building_relation_active_ind').alias('building_relation_active_ind'), F.col('h.building_relation_effective_now_ind').alias('building_relation_effective_now_ind'), F.col('h.building_relation_beg_effective_dt_tm').alias('building_relation_beg_effective_dt_tm'), F.col('h.building_relation_end_effective_dt_tm').alias('building_relation_end_effective_dt_tm'), F.col('h.building_relation_sequence').alias('building_relation_sequence'), F.col('h.building_relation_adc_updt').alias('building_relation_adc_updt'), F.col('h.building_hierarchy_ambiguous_ind').alias('building_hierarchy_ambiguous_ind'), F.col('h.building_resolution_source').alias('building_resolution_source'), F.col('h.facility_cd').alias('facility_cd'), F.when(F.col('h.facility_cd').isNotNull(), _best_name(F.col('fc.display'), F.col('fc.description'), F.col('h.facility_cd'))).alias('facility_name'), F.col('fc.display').alias('facility_display'), F.col('fc.description').alias('facility_description'), F.col('fc.code_active_ind').alias('facility_code_active_ind'), F.col('fc.code_adc_updt').alias('facility_code_adc_updt'), F.col('h.facility_parent_count').alias('facility_parent_count'), F.col('h.facility_active_parent_count').alias('facility_active_parent_count'), F.col('h.facility_relation_active_ind').alias('facility_relation_active_ind'), F.col('h.facility_relation_effective_now_ind').alias('facility_relation_effective_now_ind'), F.col('h.facility_relation_beg_effective_dt_tm').alias('facility_relation_beg_effective_dt_tm'), F.col('h.facility_relation_end_effective_dt_tm').alias('facility_relation_end_effective_dt_tm'), F.col('h.facility_relation_sequence').alias('facility_relation_sequence'), F.col('h.facility_relation_adc_updt').alias('facility_relation_adc_updt'), F.col('h.facility_hierarchy_ambiguous_ind').alias('facility_hierarchy_ambiguous_ind'), F.col('h.facility_resolution_source').alias('facility_resolution_source'), F.col('h.organization_id').alias('ORGANIZATION_ID'), F.coalesce(_non_blank(F.col('org._organization_name')), _non_blank(F.col('org._organization_name_key'))).alias('organization_name'), F.col('org._organization_name_key').alias('organization_name_key'), F.col('org._organization_active_ind').alias('organization_active_ind'), F.col('org._organization_active_status_cd').alias('organization_active_status_cd'), F.col('org._organization_beg_effective_dt_tm').alias('organization_beg_effective_dt_tm'), F.col('org._organization_end_effective_dt_tm').alias('organization_end_effective_dt_tm'), F.col('org._organization_status_cd').alias('organization_status_cd'), F.col('org._organization_class_cd').alias('organization_class_cd'), F.col('org._organization_external_ind').alias('organization_external_ind'), F.col('org._organization_contributor_system_cd').alias('organization_contributor_system_cd'), F.col('org._organization_contributor_source_cd').alias('organization_contributor_source_cd'), F.col('org._organization_trust').alias('organization_trust'), F.col('org._organization_adc_updt').alias('organization_adc_updt'), F.col('addr._selected_address_id').alias('address_id'), F.col('addr._current_address_id').alias('current_address_id'), F.col('addr._latest_known_address_id').alias('latest_known_address_id'), F.col('addr._address_selection_status').alias('address_selection_status'), F.col('addr._address_is_current_ind').alias('address_is_current_ind'), F.col('addr._address_active_ind').alias('address_active_ind'), F.col('addr._address_beg_effective_dt_tm').alias('address_beg_effective_dt_tm'), F.col('addr._address_end_effective_dt_tm').alias('address_end_effective_dt_tm'), F.col('addr._masked_zipcode').alias('masked_zipcode'), F.col('addr._city').alias('city'), F.col('addr._full_street_address').alias('full_street_address'), F.col('addr._lsoa').alias('lsoa'), F.col('addr._uprn').alias('uprn'), F.col('addr._latitude').alias('latitude'), F.col('addr._longitude').alias('longitude'), F.col('addr._address_match_algorithm').alias('address_match_algorithm'), F.col('addr._address_match_confidence').alias('address_match_confidence'), F.col('addr._address_match_quality').alias('address_match_quality'), F.col('addr._address_country_cd').alias('address_country_cd'), F.col('addr._address_adc_updt').alias('address_adc_updt'), F.col('h.location_active_ind').alias('location_active_ind'), F.col('h.location_active_status_cd').alias('location_active_status_cd'), F.col('h.location_beg_effective_dt_tm').alias('location_beg_effective_dt_tm'), F.col('h.location_end_effective_dt_tm').alias('location_end_effective_dt_tm'), F.col('h.census_ind').alias('census_ind'), F.col('h.registration_ind').alias('registration_ind'), F.col('h.resource_ind').alias('resource_ind'), F.col('h.patcare_node_ind').alias('patcare_node_ind'), F.col('h.contributor_system_cd').alias('contributor_system_cd'), F.col('h.contributor_source_cd').alias('contributor_source_cd'), F.col('h.trust').alias('trust'), F.col('h.location_updt_dt_tm').alias('location_updt_dt_tm'), F.col('h.location_adc_updt').alias('location_adc_updt'), F.lit(True).alias('source_present_ind'), F.lit(None).cast('timestamp').alias('source_absent_detected_ts'), source_adc_updt.alias('ADC_UPDT'))
    return _align_to_schema(result, schema_map_care_site_v2, allow_missing=False)

def _schema_requires_rewrite(target_table, desired_schema):
    if not _table_exists(target_table):
        return False
    current = {field.name.lower(): field for field in spark.table(target_table).schema.fields}
    for desired in desired_schema.fields:
        existing = current.get(desired.name.lower())
        if existing is None or existing.dataType != desired.dataType:
            return True
    return False

def _table_stage_name(target_table):
    parts = target_table.split('.')
    stage_table = f'__map_care_site_stage_{uuid.uuid4().hex[:12]}'
    return '.'.join(parts[:-1] + [stage_table])

def _apply_table_metadata(target_table, schema, table_comment):
    quoted_target = _quoted_table(target_table)
    properties = {'delta.enableChangeDataFeed': 'true', 'delta.enableRowTracking': 'true', 'delta.autoOptimize.optimizeWrite': 'true', 'delta.autoOptimize.autoCompact': 'true'}
    existing_properties = {}
    try:
        existing_properties = {row['key']: row['value'] for row in spark.sql(f'SHOW TBLPROPERTIES {quoted_target}').collect()}
    except Exception:
        existing_properties = {}
    changed_properties = {key: value for key, value in properties.items() if existing_properties.get(key) != value}
    if changed_properties:
        assignments = ', '.join((f"'{_sql_string(key)}' = '{_sql_string(value)}'" for key, value in changed_properties.items()))
        spark.sql(f'ALTER TABLE {quoted_target} SET TBLPROPERTIES ({assignments})')
    current_description = None
    try:
        current_description = spark.sql(f'DESCRIBE DETAIL {quoted_target}').select('description').first()['description']
    except Exception:
        current_description = None
    if (current_description or '') != (table_comment or ''):
        spark.sql(f"COMMENT ON TABLE {quoted_target} IS '{_sql_string(table_comment)}'")
    current_comments = {field.name.lower(): field.metadata.get('comment', '') for field in spark.table(target_table).schema.fields}
    for field in schema.fields:
        desired_comment = field.metadata.get('comment', '')
        if current_comments.get(field.name.lower(), '') != desired_comment:
            spark.sql(f"ALTER TABLE {quoted_target} ALTER COLUMN {_quoted_identifier(field.name)} COMMENT '{_sql_string(desired_comment)}'")

def _write_replacement_with_history(source_df, target_table, target_schema):
    """Rewrite a primary contract while retaining previously published keys."""
    source_df = bronze_project_contract(source_df, target_table)
    historical = None
    if _table_exists(target_table):
        current_keys = source_df.select('care_site_cd').distinct()
        historical = (
            bronze_project_contract(spark.table(target_table), target_table)
            .alias('t')
            .join(
                current_keys.alias('s'),
                F.col('t.care_site_cd').cast('long')
                == F.col('s.care_site_cd'),
                'left_anti',
            )
        )
    combined = (
        source_df if historical is None
        else source_df.unionByName(historical)
    )
    stage_table = _table_stage_name(target_table)
    try:
        combined.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(stage_table)
        spark.table(stage_table).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(target_table)
    finally:
        spark.sql(f'DROP TABLE IF EXISTS {_quoted_table(stage_table)}')

def _merge_change_condition(schema, key_columns):
    key_names = {name.lower() for name in key_columns}
    comparisons = []
    for field in schema.fields:
        if field.name.lower() in key_names:
            continue
        quoted = _quoted_identifier(field.name)
        comparisons.append(f'NOT (t.{quoted} <=> s.{quoted})')
    return ' OR '.join(comparisons)

def _validate_source_snapshot(source_df):
    row = source_df.agg(F.count('*').alias('rows'), F.countDistinct('care_site_cd').alias('distinct_keys'), F.count(F.when(F.col('care_site_cd').isNull(), 1)).alias('null_keys')).first()
    if row['null_keys'] != 0:
        raise ValueError('map_care_site source snapshot contains a NULL care_site_cd')
    if row['rows'] != row['distinct_keys']:
        raise ValueError('map_care_site source snapshot contains duplicate care_site_cd')
    return row['rows']

def _validate_target(target_table, expected_source_rows):
    row = spark.table(target_table).agg(
        F.count('*').alias('target_rows'),
        F.countDistinct('care_site_cd').alias('distinct_keys'),
        F.count(F.when(F.col('care_site_cd').isNull(), 1)).alias('null_keys'),
    ).first()
    if row['target_rows'] != row['distinct_keys'] or row['null_keys'] != 0:
        raise ValueError('Post-merge validation failed: target key is not unique/non-null')
    if int(row['target_rows']) < int(expected_source_rows):
        raise ValueError(
            f"Post-merge validation failed: target rows {row['target_rows']} "
            f"< current source rows {expected_source_rows}"
        )
    print(
        f"[map_care_site] validation passed: {row['target_rows']} retained rows; "
        f"{expected_source_rows} keys were present in the current source"
    )

def update_map_care_site_table(source_df, target_table=MAP_CARE_SITE_TARGET, target_schema=schema_map_care_site_v2, table_comment=MAP_CARE_SITE_COMMENT):
    """
    Full-source, change-aware synchronization for map_care_site.

    A schema rewrite is performed only when the desired columns or BIGINT types are
    missing. Subsequent runs use a full-source MERGE and update only changed rows.
    Source-absent keys are retained and tombstoned instead of being silently deleted.

    The source is deliberately not cached because PERSIST/CACHE is unsupported on
    Databricks serverless compute. Validation is consolidated into one action before
    the merge, so the full source plan is evaluated at most twice.
    """
    target_schema = bronze_contract_schema(target_table)
    aligned_source = bronze_project_contract(
        _align_to_schema(source_df, target_schema, allow_missing=False),
        target_table,
    )
    source_count = _validate_source_snapshot(aligned_source)
    if not _table_exists(target_table):
        aligned_source.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(target_table)
        print(f'[map_care_site] created {target_table} with {source_count} current rows')
    elif _schema_requires_rewrite(target_table, target_schema):
        _write_replacement_with_history(aligned_source, target_table, target_schema)
        print(f'[map_care_site] migrated and reconciled {target_table}')
    else:
        target = DeltaTable.forName(spark, target_table)
        change_condition = _merge_change_condition(target_schema, ['care_site_cd'])
        assignments = {
            column_name: F.col(f's.{column_name}')
            for column_name in aligned_source.columns
        }
        target.alias('t').merge(
            aligned_source.alias('s'),
            't.care_site_cd <=> s.care_site_cd',
        ).whenMatchedUpdate(
            condition=change_condition,
            set=assignments,
        ).whenNotMatchedInsert(values=assignments).execute()
        print(f'[map_care_site] reconciled {source_count} current source rows')
    _apply_table_metadata(target_table, target_schema, table_comment)
    _validate_target(target_table, source_count)

def run_map_care_site_update():
    """Standalone entry point for controlled execution outside Map Pipeline."""
    source_df = create_care_site_mapping_incr()
    update_map_care_site_table(source_df)
    return spark.table(MAP_CARE_SITE_TARGET)
if '_map_updates_original_update_table' not in globals():
    candidate = globals().get('update_table')
    if candidate is not None and (not getattr(candidate, '_is_map_care_site_override', False)):
        _map_updates_original_update_table = candidate
    else:
        _map_updates_original_update_table = None

def update_table(source_df, target_table, index_column, target_schema=None, table_comment=None, update_condition=None):
    """
    Drop-in wrapper matching Map Pipeline's update_table signature.

    Only map_care_site is intercepted. Every other target delegates to the original
    Map Pipeline update_table function.
    """
    if target_table.lower() == MAP_CARE_SITE_TARGET.lower():
        return update_map_care_site_table(source_df=source_df, target_table=target_table, target_schema=schema_map_care_site_v2, table_comment=MAP_CARE_SITE_COMMENT)
    if _map_updates_original_update_table is None:
        raise RuntimeError('The original Map Pipeline update_table function is unavailable. Load this override after Map Pipeline defines update_table, or call run_map_care_site_update() directly.')
    return _map_updates_original_update_table(source_df=source_df, target_table=target_table, index_column=index_column, target_schema=target_schema, table_comment=table_comment, update_condition=update_condition)
update_table._is_map_care_site_override = True
print('[map_care_site] replacement functions loaded. No production data has been changed.')

try:
    run_map_care_site_update()
    _PIPELINE_UPDATED_TARGETS.extend(['4_prod.bronze.map_care_site'])
    _pipeline_mark_component_complete('map_care_site', ['4_prod.bronze.map_care_site'])
    _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_care_site'})
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

_pipeline_component_start("map_medical_personnel")
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
from uuid import uuid4
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

@dataclass(frozen=True)
class MapMedicalPersonnelConfig:
    target_table: str = '4_prod.bronze.map_medical_personnel'
    group_bridge_table: str = '4_prod.bronze.map_medical_personnel_group'
    alias_bridge_table: str = '4_prod.bronze.map_medical_personnel_alias'
    event_state_table: str = '6_mgmt.bronze.map_medical_personnel_event_state'
    location_evidence_table: str = '4_prod.bronze.map_medical_personnel_location_evidence'
    checkpoint_table: str = '6_mgmt.bronze.map_medical_personnel_checkpoints'
    pipeline_name: str = 'map_medical_personnel_v2'
    schema_version: str = '2'
    prsnl_source: str = '4_prod.raw.mill_prsnl'
    prsnl_group_source: str = '4_prod.raw.mill_prsnl_group'
    prsnl_group_reltn_source: str = '4_prod.raw.mill_prsnl_group_reltn'
    prsnl_alias_source: str = '4_prod.raw.mill_prsnl_alias'
    clinical_event_source: str = '4_prod.raw.mill_clinical_event'
    encounter_source: str = '4_prod.raw.mill_encounter'
    encounter_location_history_source: str = '4_prod.raw.mill_encntr_loc_hist'
    code_value_source: str = '3_lookup.mill.mill_code_value'
    cdf_retention: str = 'interval 30 days'
    broadcast_key_limit: int = 1000000

    @property
    def all_sources(self) -> Tuple[str, ...]:
        return (self.prsnl_source, self.prsnl_group_source, self.prsnl_group_reltn_source, self.prsnl_alias_source, self.clinical_event_source, self.encounter_source, self.encounter_location_history_source, self.code_value_source)

    @property
    def location_cdf_sources(self) -> Tuple[str, ...]:
        return (self.clinical_event_source, self.encounter_source, self.encounter_location_history_source)
DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG = MapMedicalPersonnelConfig()
MAP_MEDICAL_PERSONNEL_COMMENT = 'Unfiltered one-row-per-personnel foundation sourced from Millennium personnel. Includes exact identifiers, status and effective dates, deterministic compatibility group and alias values, assigned and event-inferred care locations, evidence quality fields, and per-source update provenance. Lossless group and alias details are stored in companion bridge tables.'
MAP_MEDICAL_PERSONNEL_GROUP_COMMENT = 'Lossless personnel-group relationship bridge. Source rows are retained regardless of active or effective status; status is used only for deterministic compatibility selection in map_medical_personnel.'
MAP_MEDICAL_PERSONNEL_ALIAS_COMMENT = 'Lossless personnel-alias bridge containing doctor, external, NPI, GDP and all other alias types with status, effective dates and source provenance.'
MAP_MEDICAL_PERSONNEL_EVENT_STATE_COMMENT = 'Compact canonical logical-clinical-event state used to derive personnel location evidence. One row per logical event key; physical event-version count and validity are retained. This is a pipeline support table, not a clinical reporting table.'
MAP_MEDICAL_PERSONNEL_LOCATION_COMMENT = 'Personnel and nurse-unit evidence aggregate derived from canonical clinical events. Contains all observed locations, deterministic rank, event counts and latest event time. No personnel rows are filtered from the main foundation table.'

def _mmp_field(name: str, data_type: T.DataType, comment: str, nullable: bool=True) -> T.StructField:
    return T.StructField(name, data_type, nullable, {'comment': comment})
schema_map_medical_personnel_v2 = T.StructType([_mmp_field('PERSON_ID', T.LongType(), 'Exact Millennium personnel identifier.', False), _mmp_field('PHYSICIAN_IND', T.BooleanType(), 'Source physician indicator represented as BOOLEAN.'), _mmp_field('POSITION_CD', T.LongType(), 'Personnel position code.'), _mmp_field('position_name', T.StringType(), 'Best position label: description, display, then CDF meaning. Null when the source code is null or zero; the numeric code is never used as a label.'), _mmp_field('position_display', T.StringType(), 'Raw code-value display for POSITION_CD.'), _mmp_field('position_description', T.StringType(), 'Raw code-value description for POSITION_CD.'), _mmp_field('position_cdf_meaning', T.StringType(), 'Raw CDF meaning for POSITION_CD.'), _mmp_field('position_code_active_ind', T.LongType(), 'Code-value ACTIVE_IND for POSITION_CD.'), _mmp_field('PRSNL_TYPE_CD', T.LongType(), 'Personnel type code.'), _mmp_field('prsnl_type_name', T.StringType(), 'Resolved personnel type label.'), _mmp_field('PHYSICIAN_STATUS_CD', T.LongType(), 'Source physician status code.'), _mmp_field('physician_status_name', T.StringType(), 'Resolved physician status label.'), _mmp_field('ACTIVE_IND', T.LongType(), 'Personnel source ACTIVE_IND; retained rather than filtered.'), _mmp_field('ACTIVE_STATUS_CD', T.LongType(), 'Personnel active-status code.'), _mmp_field('active_status_name', T.StringType(), 'Resolved personnel active-status label.'), _mmp_field('DATA_STATUS_CD', T.LongType(), 'Personnel data-status code.'), _mmp_field('data_status_name', T.StringType(), 'Resolved personnel data-status label.'), _mmp_field('CONTRIBUTOR_SYSTEM_CD', T.LongType(), 'Personnel contributor-system code.'), _mmp_field('contributor_system_name', T.StringType(), 'Resolved contributor-system label.'), _mmp_field('BEG_EFFECTIVE_DT_TM', T.TimestampType(), 'Personnel begin-effective timestamp.'), _mmp_field('END_EFFECTIVE_DT_TM', T.TimestampType(), 'Personnel end-effective timestamp.'), _mmp_field('CREATE_DT_TM', T.TimestampType(), 'Personnel source creation timestamp.'), _mmp_field('UPDT_DT_TM', T.TimestampType(), 'Native personnel source update timestamp.'), _mmp_field('EXTERNAL_IND', T.BooleanType(), 'Whether the personnel record is external.'), _mmp_field('NAME_FULL_FORMATTED', T.StringType(), 'Source formatted personnel name; access remains subject to governance.'), _mmp_field('HAS_CLINICAL_ROLE_IND', T.BooleanType(), 'True when physician indicator is true or a non-zero position code exists.'), _mmp_field('PRIM_ASSIGN_LOC_CD', T.LongType(), 'Raw primary assigned location from mill_prsnl.'), _mmp_field('prim_assign_loc_name', T.StringType(), 'Resolved raw primary assigned location label.'), _mmp_field('inferred_primary_care_site_cd', T.LongType(), 'Most strongly evidenced nurse-unit code from canonical clinical events.'), _mmp_field('inferred_primary_care_site_name', T.StringType(), 'Resolved inferred nurse-unit label.'), _mmp_field('primary_care_site_cd', T.LongType(), 'Assigned location when available, otherwise inferred event-based nurse unit.'), _mmp_field('primary_care_site_name', T.StringType(), 'Resolved selected primary care-site label.'), _mmp_field('primary_care_site_source', T.StringType(), 'ASSIGNED_LOCATION, EVENT_INFERENCE or NONE.'), _mmp_field('primary_site_event_count', T.LongType(), 'Canonical logical-event count supporting the inferred location.'), _mmp_field('primary_site_last_event_dt_tm', T.TimestampType(), 'Latest event timestamp supporting the inferred location.'), _mmp_field('primary_site_location_tie_count', T.IntegerType(), 'Number of locations tied on the highest event count.'), _mmp_field('SRVCATEGORY', T.StringType(), 'Deterministically selected service-category label; NULL means no assignment.'), _mmp_field('SRVCATEGORY_GROUP_ID', T.LongType(), 'Selected service-category personnel-group identifier.'), _mmp_field('SRVCATEGORY_GROUP_NAME', T.StringType(), 'Raw selected service-category group name.'), _mmp_field('SRVCATEGORY_ASSIGNMENT_COUNT', T.IntegerType(), 'All source-present service-category relationships.'), _mmp_field('SRVCATEGORY_ACTIVE_ASSIGNMENT_COUNT', T.IntegerType(), 'Active service-category relationships.'), _mmp_field('SRVCATEGORY_SELECTED_PRIMARY_IND', T.BooleanType(), 'Whether the selected service-category relationship is primary.'), _mmp_field('SRVCATEGORY_SELECTED_INACTIVE_IND', T.BooleanType(), 'Whether the selected service-category relationship or group is inactive.'), _mmp_field('SURGSPEC', T.StringType(), 'Deterministically selected surgical-specialty label; NULL means no assignment.'), _mmp_field('SURGSPEC_GROUP_ID', T.LongType(), 'Selected surgical-specialty personnel-group identifier.'), _mmp_field('SURGSPEC_GROUP_NAME', T.StringType(), 'Raw selected surgical-specialty group name.'), _mmp_field('SURGSPEC_ASSIGNMENT_COUNT', T.IntegerType(), 'All source-present surgical-specialty relationships.'), _mmp_field('SURGSPEC_ACTIVE_ASSIGNMENT_COUNT', T.IntegerType(), 'Active surgical-specialty relationships.'), _mmp_field('SURGSPEC_SELECTED_PRIMARY_IND', T.BooleanType(), 'Whether the selected surgical-specialty relationship is primary.'), _mmp_field('SURGSPEC_SELECTED_INACTIVE_IND', T.BooleanType(), 'Whether the selected surgical-specialty relationship or group is inactive.'), _mmp_field('MEDSERVICE', T.StringType(), 'Deterministically selected medical-service label; NULL means no assignment.'), _mmp_field('MEDSERVICE_GROUP_ID', T.LongType(), 'Selected medical-service personnel-group identifier.'), _mmp_field('MEDSERVICE_GROUP_NAME', T.StringType(), 'Raw selected medical-service group name.'), _mmp_field('MEDSERVICE_ASSIGNMENT_COUNT', T.IntegerType(), 'All source-present medical-service relationships.'), _mmp_field('MEDSERVICE_ACTIVE_ASSIGNMENT_COUNT', T.IntegerType(), 'Active medical-service relationships.'), _mmp_field('MEDSERVICE_SELECTED_PRIMARY_IND', T.BooleanType(), 'Whether the selected medical-service relationship is primary.'), _mmp_field('MEDSERVICE_SELECTED_INACTIVE_IND', T.BooleanType(), 'Whether the selected medical-service relationship or group is inactive.'), _mmp_field('NPI', T.StringType(), 'Selected active National Provider Identifier alias where available.'), _mmp_field('NPI_ALIAS_COUNT', T.IntegerType(), 'Number of source-present NPI aliases.'), _mmp_field('DOCNBR', T.StringType(), 'Selected organization doctor-number alias.'), _mmp_field('DOCNBR_ALIAS_COUNT', T.IntegerType(), 'Number of source-present doctor-number aliases.'), _mmp_field('GDP_NUMBER', T.StringType(), 'Selected General Dental Practitioner number.'), _mmp_field('GDP_ALIAS_COUNT', T.IntegerType(), 'Number of source-present GDP aliases.'), _mmp_field('EXTERNAL_PROVIDER_ID', T.StringType(), 'Selected external provider identifier.'), _mmp_field('EXTERNAL_PROVIDER_ID_COUNT', T.IntegerType(), 'Number of source-present external identifiers.'), _mmp_field('PRSNL_ADC_UPDT', T.TimestampType(), 'ADC_UPDT from mill_prsnl.'), _mmp_field('GROUP_ADC_UPDT', T.TimestampType(), 'Greatest update timestamp across contributing group records.'), _mmp_field('LOCATION_EVIDENCE_ADC_UPDT', T.TimestampType(), 'Greatest update timestamp across supporting event/location evidence.'), _mmp_field('ALIAS_ADC_UPDT', T.TimestampType(), 'Greatest update timestamp across contributing alias records.'), _mmp_field('CODE_LOOKUP_ADC_UPDT', T.TimestampType(), 'Greatest update timestamp across joined code-value rows.'), _mmp_field('SOURCE_PRESENT_IND', T.BooleanType(), 'True when PERSON_ID is present in the current personnel source snapshot.'), _mmp_field('SOURCE_ABSENT_DETECTED_TS', T.TimestampType(), 'First pipeline timestamp when a previously present personnel key was absent.'), _mmp_field('ADC_UPDT', T.TimestampType(), 'Greatest source update timestamp for this derived row; not used as a pipeline checkpoint.'), _mmp_field('ROW_HASH', T.LongType(), 'Stable row hash used to avoid no-op rewrites.'), _mmp_field('PIPELINE_RUN_ID', T.StringType(), 'Identifier for the pipeline run that last changed the row.'), _mmp_field('PIPELINE_UPDT_DT_TM', T.TimestampType(), 'Pipeline timestamp that last changed the row.')])
schema_map_medical_personnel = schema_map_medical_personnel_v2
map_medical_personnel_comment = MAP_MEDICAL_PERSONNEL_COMMENT
schema_map_medical_personnel_group = T.StructType([_mmp_field('PRSNL_GROUP_RELTN_ID', T.LongType(), 'Personnel-group relationship identifier.', False), _mmp_field('PERSON_ID', T.LongType(), 'Personnel identifier.'), _mmp_field('PRSNL_GROUP_ID', T.LongType(), 'Personnel-group identifier.'), _mmp_field('PRSNL_GROUP_TYPE_CD', T.LongType(), 'Personnel-group type code.'), _mmp_field('CDF_MEANING', T.StringType(), 'CDF meaning for the group type code.'), _mmp_field('GROUP_TYPE_DISPLAY', T.StringType(), 'Code-value display for the group type.'), _mmp_field('GROUP_TYPE_DESCRIPTION', T.StringType(), 'Code-value description for the group type.'), _mmp_field('PRSNL_GROUP_NAME', T.StringType(), 'Raw personnel-group name.'), _mmp_field('PRSNL_GROUP_DESC', T.StringType(), 'Raw personnel-group description.'), _mmp_field('GROUP_LABEL', T.StringType(), 'Best label from group name, description and code metadata.'), _mmp_field('PRIMARY_IND', T.BooleanType(), 'Source relationship primary indicator.'), _mmp_field('RELATION_ACTIVE_IND', T.LongType(), 'Relationship ACTIVE_IND.'), _mmp_field('RELATION_ACTIVE_STATUS_CD', T.LongType(), 'Relationship active-status code.'), _mmp_field('RELATION_BEG_EFFECTIVE_DT_TM', T.TimestampType(), 'Relationship begin-effective timestamp.'), _mmp_field('RELATION_END_EFFECTIVE_DT_TM', T.TimestampType(), 'Relationship end-effective timestamp.'), _mmp_field('RELATION_EFFECTIVE_NOW_IND', T.BooleanType(), 'Whether the relationship is effective at pipeline time.'), _mmp_field('GROUP_ACTIVE_IND', T.LongType(), 'Personnel-group ACTIVE_IND.'), _mmp_field('GROUP_ACTIVE_STATUS_CD', T.LongType(), 'Personnel-group active-status code.'), _mmp_field('GROUP_BEG_EFFECTIVE_DT_TM', T.TimestampType(), 'Group begin-effective timestamp.'), _mmp_field('GROUP_END_EFFECTIVE_DT_TM', T.TimestampType(), 'Group end-effective timestamp.'), _mmp_field('GROUP_EFFECTIVE_NOW_IND', T.BooleanType(), 'Whether the group is effective at pipeline time.'), _mmp_field('RELATION_ADC_UPDT', T.TimestampType(), 'Relationship ADC_UPDT.'), _mmp_field('GROUP_ADC_UPDT', T.TimestampType(), 'Personnel-group ADC_UPDT.'), _mmp_field('GROUP_TYPE_CODE_ADC_UPDT', T.TimestampType(), 'Group type code-value ADC_UPDT.'), _mmp_field('ADC_UPDT', T.TimestampType(), 'Greatest source update timestamp for the bridge row.'), _mmp_field('SOURCE_PRESENT_IND', T.BooleanType(), 'True when the relationship exists in the current source snapshot.'), _mmp_field('SOURCE_ABSENT_DETECTED_TS', T.TimestampType(), 'First detected source-absence timestamp.'), _mmp_field('ROW_HASH', T.LongType(), 'Stable bridge row hash.'), _mmp_field('PIPELINE_RUN_ID', T.StringType(), 'Pipeline run identifier.'), _mmp_field('PIPELINE_UPDT_DT_TM', T.TimestampType(), 'Pipeline update timestamp.')])
schema_map_medical_personnel_alias = T.StructType([_mmp_field('PRSNL_ALIAS_ID', T.LongType(), 'Personnel-alias identifier.', False), _mmp_field('PERSON_ID', T.LongType(), 'Personnel identifier.'), _mmp_field('ALIAS_POOL_CD', T.LongType(), 'Alias pool code.'), _mmp_field('PRSNL_ALIAS_TYPE_CD', T.LongType(), 'Personnel alias type code.'), _mmp_field('ALIAS_TYPE_CDF_MEANING', T.StringType(), 'CDF meaning for the alias type.'), _mmp_field('ALIAS_TYPE_DISPLAY', T.StringType(), 'Display for the alias type.'), _mmp_field('ALIAS_TYPE_DESCRIPTION', T.StringType(), 'Description for the alias type.'), _mmp_field('ALIAS', T.StringType(), 'Raw personnel alias value.'), _mmp_field('ACTIVE_IND', T.LongType(), 'Alias ACTIVE_IND.'), _mmp_field('ACTIVE_STATUS_CD', T.LongType(), 'Alias active-status code.'), _mmp_field('BEG_EFFECTIVE_DT_TM', T.TimestampType(), 'Alias begin-effective timestamp.'), _mmp_field('END_EFFECTIVE_DT_TM', T.TimestampType(), 'Alias end-effective timestamp.'), _mmp_field('EFFECTIVE_NOW_IND', T.BooleanType(), 'Whether the alias is effective at pipeline time.'), _mmp_field('CONTRIBUTOR_SYSTEM_CD', T.LongType(), 'Alias contributor-system code.'), _mmp_field('ADC_UPDT', T.TimestampType(), 'Alias ADC_UPDT.'), _mmp_field('SOURCE_PRESENT_IND', T.BooleanType(), 'True when the alias exists in the current source snapshot.'), _mmp_field('SOURCE_ABSENT_DETECTED_TS', T.TimestampType(), 'First detected source-absence timestamp.'), _mmp_field('ROW_HASH', T.LongType(), 'Stable bridge row hash.'), _mmp_field('PIPELINE_RUN_ID', T.StringType(), 'Pipeline run identifier.'), _mmp_field('PIPELINE_UPDT_DT_TM', T.TimestampType(), 'Pipeline update timestamp.')])
schema_map_medical_personnel_event_state = T.StructType([_mmp_field('EVENT_KEY', T.StringType(), 'Stable E:event_id or C:clinical_event_id logical key.', False), _mmp_field('EVENT_ID', T.LongType(), 'Logical Millennium event identifier.'), _mmp_field('CLINICAL_EVENT_ID', T.LongType(), 'Selected physical clinical-event row identifier.'), _mmp_field('ENCNTR_ID', T.LongType(), 'Encounter identifier.'), _mmp_field('PERFORMED_PRSNL_ID', T.LongType(), 'Personnel who performed the event.'), _mmp_field('EVENT_DT_TM', T.TimestampType(), 'Best event time: performed, start, end, then source update.'), _mmp_field('EVENT_DT_TM_SOURCE', T.StringType(), 'Column used to derive EVENT_DT_TM.'), _mmp_field('VALID_UNTIL_DT_TM', T.TimestampType(), 'Selected event-version valid-until timestamp.'), _mmp_field('EVENT_CURRENT_IND', T.BooleanType(), 'Whether the selected event version is current at pipeline time.'), _mmp_field('EVENT_VERSION_COUNT', T.IntegerType(), 'Physical source-version rows observed for the logical event during a full rebuild.'), _mmp_field('LOC_NURSE_UNIT_CD', T.LongType(), 'Event-time nurse unit, with current encounter fallback.'), _mmp_field('LOCATION_SOURCE', T.StringType(), 'ENCOUNTER_LOCATION_HISTORY, ENCOUNTER_CURRENT or NONE.'), _mmp_field('ENCNTR_LOC_HIST_ID', T.LongType(), 'Selected encounter-location-history row identifier.'), _mmp_field('EVENT_ADC_UPDT', T.TimestampType(), 'Clinical-event ADC_UPDT.'), _mmp_field('LOCATION_HISTORY_ADC_UPDT', T.TimestampType(), 'Selected location-history ADC_UPDT.'), _mmp_field('ENCOUNTER_ADC_UPDT', T.TimestampType(), 'Encounter ADC_UPDT used for fallback.'), _mmp_field('ADC_UPDT', T.TimestampType(), 'Greatest source update timestamp for the event state.'), _mmp_field('SOURCE_PRESENT_IND', T.BooleanType(), 'True when the logical event remains represented in the source snapshot.'), _mmp_field('SOURCE_ABSENT_DETECTED_TS', T.TimestampType(), 'First detected source-absence timestamp.'), _mmp_field('ROW_HASH', T.LongType(), 'Stable event-state row hash.'), _mmp_field('PIPELINE_RUN_ID', T.StringType(), 'Pipeline run identifier.'), _mmp_field('PIPELINE_UPDT_DT_TM', T.TimestampType(), 'Pipeline update timestamp.')])
schema_map_medical_personnel_location_evidence = T.StructType([_mmp_field('PERSON_ID', T.LongType(), 'Personnel identifier.', False), _mmp_field('LOC_NURSE_UNIT_CD', T.LongType(), 'Observed nurse-unit code.', False), _mmp_field('EVENT_COUNT', T.LongType(), 'Current canonical logical-event count at this location.'), _mmp_field('FIRST_EVENT_DT_TM', T.TimestampType(), 'Earliest supporting event timestamp.'), _mmp_field('LAST_EVENT_DT_TM', T.TimestampType(), 'Latest supporting event timestamp.'), _mmp_field('LOCATION_RANK', T.IntegerType(), 'Deterministic personnel location rank.'), _mmp_field('TOP_COUNT_TIE_COUNT', T.IntegerType(), 'Number of locations tied on the maximum event count.'), _mmp_field('EVIDENCE_ADC_UPDT', T.TimestampType(), 'Greatest source update timestamp among supporting events.'), _mmp_field('SOURCE_PRESENT_IND', T.BooleanType(), 'True when current canonical evidence supports the row.'), _mmp_field('SOURCE_ABSENT_DETECTED_TS', T.TimestampType(), 'First detected evidence-absence timestamp.'), _mmp_field('ROW_HASH', T.LongType(), 'Stable evidence row hash.'), _mmp_field('PIPELINE_RUN_ID', T.StringType(), 'Pipeline run identifier.'), _mmp_field('PIPELINE_UPDT_DT_TM', T.TimestampType(), 'Pipeline update timestamp.')])
_MMP_LOW_TS = F.lit('1900-01-01 00:00:00').cast('timestamp')
_MMP_HIGH_TS = F.lit('9999-12-31 23:59:59').cast('timestamp')

def _mmp_table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _mmp_sql_name(table_name: str) -> str:
    tick = chr(96)
    return '.'.join((tick + part.replace(tick, tick + tick) + tick for part in table_name.split('.')))

def _mmp_sql_string(value: str) -> str:
    return value.replace('\\', '\\\\').replace("'", "''")

def _mmp_nonblank(column: F.Column) -> F.Column:
    return F.when(column.isNotNull() & (F.trim(column.cast('string')) != ''), F.trim(column.cast('string')))

def _mmp_effective_now(begin_column: F.Column, end_column: F.Column, run_timestamp: datetime) -> F.Column:
    now = F.lit(run_timestamp).cast('timestamp')
    return (F.coalesce(begin_column, _MMP_LOW_TS) <= now) & (F.coalesce(end_column, _MMP_HIGH_TS) > now)

def _mmp_checked_long(column: F.Column, label: str) -> F.Column:
    as_double = column.cast('double')
    invalid = column.isNotNull() & ((as_double != F.floor(as_double)) | (F.abs(as_double) > F.lit(9007199254740991.0)))
    return F.when(invalid, F.lit(None)).otherwise(column.cast('long'))

def _mmp_align_to_schema(df: DataFrame, schema: T.StructType, allow_missing: bool=False) -> DataFrame:
    actual = {name.lower(): name for name in df.columns}
    missing = [field.name for field in schema.fields if field.name.lower() not in actual]
    if missing and (not allow_missing):
        raise ValueError('DataFrame is missing required columns: ' + ', '.join(missing))
    expressions = []
    for field in schema.fields:
        actual_name = actual.get(field.name.lower())
        expression = F.col(actual_name) if actual_name is not None else F.lit(None)
        expressions.append(expression.cast(field.dataType).alias(field.name, metadata=field.metadata))
    return df.select(*expressions)

def _mmp_add_row_hash(df: DataFrame, schema: T.StructType, exclusions: Sequence[str]) -> DataFrame:
    excluded = {name.lower() for name in exclusions}
    columns = [field.name for field in schema.fields if field.name.lower() not in excluded and field.name.lower() != 'row_hash' and (field.name in df.columns)]
    normalized = [F.coalesce(F.col(name).cast('string'), F.lit('<NULL>')) for name in columns]
    return df.withColumn('ROW_HASH', F.xxhash64(*normalized))

def _mmp_get_latest_delta_version(table_name: str) -> int:
    rows = spark.sql(f'DESCRIBE HISTORY {_mmp_sql_name(table_name)} LIMIT 1').select('version').collect()
    if not rows:
        raise RuntimeError(f'No Delta history returned for {table_name}')
    return int(rows[0]['version'])

def _m10_hardening_sql_identifier(table_name: str) -> str:
    return '.'.join('`' + part.replace('`', '``') + '`' for part in table_name.split('.'))

def _m10_hardening_snapshot_fallback_allowed(exc: BaseException) -> bool:
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

def _m10_hardening_read_pinned_snapshot(table_name: str, version: int):
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
        if not _m10_hardening_snapshot_fallback_allowed(exc):
            raise
        row = (
            spark.sql('DESCRIBE HISTORY ' + _m10_hardening_sql_identifier(table_name) + ' LIMIT 1')
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

def _m10_hardening_validate_cdf(changes):
    # Keep lazy Spark Connect failures inside the caller's recovery block.
    _ = changes.schema
    changes.limit(1).collect()
    return changes

def _mmp_read_snapshot(table_name: str, versions: Optional[Dict[str, int]]=None) -> DataFrame:
    if versions is None or table_name not in versions:
        return spark.table(table_name)
    return _m10_hardening_read_pinned_snapshot(table_name, int(versions[table_name]))

def _mmp_read_cdf(table_name: str, starting_version: int, ending_version: int) -> Optional[DataFrame]:
    if starting_version > ending_version:
        return None
    try:
        changes = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', int(starting_version)).option('endingVersion', int(ending_version)).table(table_name)
        changes.limit(1).collect()
        return changes
    except Exception as exc:
        raise RuntimeError(f'CDF is unavailable for {table_name} from version {starting_version} to {ending_version}. Timestamp fallback is intentionally disabled because it can lose late or backfilled rows. Run deploy_map_medical_personnel_improvements() to rebuild.') from exc

def _mmp_is_empty(df: Optional[DataFrame]) -> bool:
    return df is None or len(df.take(1)) == 0

def _mmp_union_distinct(frames: Iterable[Optional[DataFrame]], columns: Sequence[str]) -> Optional[DataFrame]:
    present = [frame.select(*columns) for frame in frames if frame is not None]
    if not present:
        return None
    return reduce(lambda left, right: left.unionByName(right), present).dropDuplicates(list(columns))

def _mmp_semijoin_keys(source: DataFrame, keys: DataFrame, key_columns: Sequence[str], broadcast_limit: int, key_count: Optional[int]=None) -> DataFrame:
    if key_count is None:
        key_count = keys.count()
    key_df = F.broadcast(keys) if key_count <= broadcast_limit else keys
    return source.join(key_df, list(key_columns), 'left_semi')

def _mmp_get_table_comment(table_name: str) -> Optional[str]:
    if not _mmp_table_exists(table_name):
        return None
    try:
        return spark.catalog.getTable(table_name).description
    except Exception:
        return None

def _mmp_set_table_comment_if_changed(table_name: str, comment: str) -> None:
    if _mmp_get_table_comment(table_name) != comment:
        spark.sql(f"COMMENT ON TABLE {_mmp_sql_name(table_name)} IS '{_mmp_sql_string(comment)}'")

def _mmp_get_table_properties(table_name: str) -> Dict[str, str]:
    if not _mmp_table_exists(table_name):
        return {}
    return {row['key']: row['value'] for row in spark.sql(f'SHOW TBLPROPERTIES {_mmp_sql_name(table_name)}').collect()}

def _mmp_set_properties_if_changed(table_name: str, desired: Dict[str, str]) -> None:
    current = _mmp_get_table_properties(table_name)
    changes = {key: value for key, value in desired.items() if current.get(key) != value}
    if not changes:
        return
    assignments = ', '.join((f"'{_mmp_sql_string(key)}' = '{_mmp_sql_string(value)}'" for key, value in changes.items()))
    spark.sql(f'ALTER TABLE {_mmp_sql_name(table_name)} SET TBLPROPERTIES ({assignments})')

def _mmp_apply_column_comments_if_changed(table_name: str, schema: T.StructType) -> None:
    current = {field.name.lower(): field.metadata.get('comment', '') for field in spark.table(table_name).schema.fields}
    for field in schema.fields:
        desired = field.metadata.get('comment', '')
        if not desired:
            continue
        if current.get(field.name.lower(), '') == desired:
            continue
        spark.sql(f"ALTER TABLE {_mmp_sql_name(table_name)} ALTER COLUMN {chr(96)}{field.name}{chr(96)} COMMENT '{_mmp_sql_string(desired)}'")

def _mmp_configure_table(table_name: str, schema: T.StructType, comment: str, config: MapMedicalPersonnelConfig) -> None:
    if bronze_is_primary_table(table_name):
        schema = bronze_contract_schema(table_name)
    _mmp_set_properties_if_changed(table_name, {'delta.enableChangeDataFeed': 'true', 'delta.deletedFileRetentionDuration': config.cdf_retention, 'delta.logRetentionDuration': config.cdf_retention, 'pipeline.map_medical_personnel.schema_version': config.schema_version})
    _mmp_set_table_comment_if_changed(table_name, comment)
    _mmp_apply_column_comments_if_changed(table_name, schema)
def _mmp_ensure_cluster_by(table_name: str, columns: Sequence[str]) -> None:
    if not columns or not _mmp_table_exists(table_name):
        return
    try:
        detail = spark.sql(f'DESCRIBE DETAIL {_mmp_sql_name(table_name)}').first()
        current = [str(value).lower() for value in detail['clusteringColumns'] or []]
        desired = [value.lower() for value in columns]
        if current == desired:
            return
        column_sql = ', '.join((f'{chr(96)}{column}{chr(96)}' for column in columns))
        spark.sql(f'ALTER TABLE {_mmp_sql_name(table_name)} CLUSTER BY ({column_sql})')
    except Exception as exc:
        print(f'[map_medical_personnel] warning: could not configure liquid clustering on {table_name}: {exc}')

def _mmp_schema_matches(table_name: str, schema: T.StructType) -> bool:
    if not _mmp_table_exists(table_name):
        return False
    current = [(field.name.lower(), field.dataType.simpleString()) for field in spark.table(table_name).schema.fields]
    expected = [(field.name.lower(), field.dataType.simpleString()) for field in schema.fields]
    return current == expected

def _mmp_write_replacement(df: DataFrame, table_name: str, schema: T.StructType) -> None:
    effective_schema = (
        bronze_contract_schema(table_name)
        if bronze_is_primary_table(table_name)
        else schema
    )
    aligned = _mmp_align_to_schema(df, effective_schema)
    if bronze_is_primary_table(table_name):
        aligned = bronze_project_contract(aligned, table_name)
    aligned.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(table_name)

def _mmp_merge_change_condition(schema: T.StructType, key_columns: Sequence[str]) -> str:
    keys = {column.lower() for column in key_columns}
    comparisons = [f'NOT (t.{field.name} <=> s.{field.name})' for field in schema.fields if field.name.lower() not in keys and field.name not in {'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM', 'SOURCE_ABSENT_DETECTED_TS'}]
    return ' OR '.join(comparisons) if comparisons else 'false'

def _mmp_reconcile_full_snapshot(current_df: DataFrame, table_name: str, schema: T.StructType, key_columns: Sequence[str], comment: str, config: MapMedicalPersonnelConfig) -> None:
    effective_schema = (
        bronze_contract_schema(table_name)
        if bronze_is_primary_table(table_name)
        else schema
    )
    aligned = _mmp_align_to_schema(current_df, effective_schema)
    if bronze_is_primary_table(table_name):
        aligned = bronze_project_contract(aligned, table_name)
    if not _mmp_schema_matches(table_name, effective_schema):
        _mmp_write_replacement(aligned, table_name, effective_schema)
    else:
        condition = ' AND '.join(
            (f't.{column} <=> s.{column}' for column in key_columns)
        )
        change_condition = _mmp_merge_change_condition(
            effective_schema, key_columns
        )
        assignments = {
            column_name: F.col(f's.{column_name}')
            for column_name in aligned.columns
        }
        DeltaTable.forName(spark, table_name).alias('t').merge(
            aligned.alias('s'), condition
        ).whenMatchedUpdate(
            condition=change_condition, set=assignments
        ).whenNotMatchedInsert(values=assignments).execute()
    _mmp_configure_table(table_name, effective_schema, comment, config)

def _mmp_validate_unique(df: DataFrame, keys: Sequence[str], label: str) -> int:
    metrics = df.agg(F.count(F.lit(1)).alias('rows'), F.countDistinct(F.struct(*[F.col(key) for key in keys])).alias('distinct_keys'), F.sum(F.when(reduce(lambda left, right: left | right, [F.col(key).isNull() for key in keys]), F.lit(1)).otherwise(F.lit(0))).alias('null_keys')).first()
    row_count = int(metrics['rows'])
    distinct_count = int(metrics['distinct_keys'])
    null_count = int(metrics['null_keys'] or 0)
    if row_count != distinct_count or null_count:
        raise ValueError(f'{label} failed key validation: rows={row_count}, distinct={distinct_count}, null_keys={null_count}')
    return row_count

def ensure_map_medical_personnel_checkpoint_table(config: MapMedicalPersonnelConfig=DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG) -> None:
    spark.sql(f"\n        CREATE TABLE IF NOT EXISTS {_mmp_sql_name(config.checkpoint_table)} (\n            pipeline_name STRING,\n            source_table STRING,\n            last_processed_version BIGINT,\n            processed_at TIMESTAMP,\n            run_id STRING\n        )\n        USING DELTA\n        TBLPROPERTIES (\n            'delta.enableChangeDataFeed' = 'true',\n            'delta.deletedFileRetentionDuration' =\n                '{_mmp_sql_string(config.cdf_retention)}',\n            'delta.logRetentionDuration' =\n                '{_mmp_sql_string(config.cdf_retention)}'\n        )\n        ")

def get_map_medical_personnel_checkpoints(config: MapMedicalPersonnelConfig=DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG) -> Dict[str, int]:
    ensure_map_medical_personnel_checkpoint_table(config)
    rows = spark.table(config.checkpoint_table).where(F.col('pipeline_name') == config.pipeline_name).select('source_table', 'last_processed_version').collect()
    return {row['source_table']: int(row['last_processed_version']) for row in rows}

def write_map_medical_personnel_checkpoints(versions: Dict[str, int], run_id: str, config: MapMedicalPersonnelConfig=DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG) -> None:
    if not versions:
        return
    ensure_map_medical_personnel_checkpoint_table(config)
    rows = [(config.pipeline_name, source, int(version), run_id) for source, version in versions.items()]
    updates = spark.createDataFrame(rows, 'pipeline_name string, source_table string, last_processed_version long, run_id string').withColumn('processed_at', F.current_timestamp())
    DeltaTable.forName(spark, config.checkpoint_table).alias('t').merge(updates.alias('s'), 't.pipeline_name = s.pipeline_name AND t.source_table = s.source_table').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def configure_map_medical_personnel_source_retention(config: MapMedicalPersonnelConfig=DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG) -> None:
    """Explicit DDL helper. Nothing invokes this on notebook import."""
    for table_name in config.location_cdf_sources:
        _mmp_set_properties_if_changed(table_name, {'delta.enableChangeDataFeed': 'true', 'delta.deletedFileRetentionDuration': config.cdf_retention, 'delta.logRetentionDuration': config.cdf_retention})

def _mmp_code_lookup(versions: Optional[Dict[str, int]], config: MapMedicalPersonnelConfig) -> DataFrame:
    source = _mmp_read_snapshot(config.code_value_source, versions).select(_mmp_checked_long(F.col('CODE_VALUE'), 'CODE_VALUE').alias('CODE_VALUE'), _mmp_checked_long(F.col('CODE_SET'), 'CODE_SET').alias('CODE_SET'), F.col('CDF_MEANING').cast('string').alias('CDF_MEANING'), F.col('DISPLAY').cast('string').alias('DISPLAY'), F.col('DESCRIPTION').cast('string').alias('DESCRIPTION'), F.col('ACTIVE_IND').cast('long').alias('CODE_ACTIVE_IND'), F.col('BEGIN_EFFECTIVE_DT_TM').cast('timestamp').alias('CODE_BEGIN_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('CODE_END_EFFECTIVE_DT_TM'), F.col('UPDT_DT_TM').cast('timestamp').alias('CODE_UPDT_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('CODE_ADC_UPDT'))
    rank_window = Window.partitionBy('CODE_VALUE').orderBy(F.col('CODE_ADC_UPDT').desc_nulls_last(), F.col('CODE_UPDT_DT_TM').desc_nulls_last(), F.col('CODE_VALUE').asc_nulls_last())
    return source.withColumn('_CODE_RANK', F.row_number().over(rank_window)).where(F.col('_CODE_RANK') == 1).drop('_CODE_RANK', 'CODE_UPDT_DT_TM')

def _mmp_label_from_code(prefix: str) -> F.Column:
    code_value = F.col(f'{prefix}_CODE_VALUE')
    has_code = code_value.isNotNull() & (code_value != F.lit(0))
    return F.when(has_code, F.coalesce(_mmp_nonblank(F.col(f'{prefix}_DESCRIPTION')), _mmp_nonblank(F.col(f'{prefix}_DISPLAY')), _mmp_nonblank(F.col(f'{prefix}_CDF_MEANING'))))

def _mmp_join_code(df: DataFrame, lookup: DataFrame, source_column: str, prefix: str) -> DataFrame:
    projected = lookup.select(F.col('CODE_VALUE').alias(f'{prefix}_CODE_VALUE'), F.col('CODE_SET').alias(f'{prefix}_CODE_SET'), F.col('CDF_MEANING').alias(f'{prefix}_CDF_MEANING'), F.col('DISPLAY').alias(f'{prefix}_DISPLAY'), F.col('DESCRIPTION').alias(f'{prefix}_DESCRIPTION'), F.col('CODE_ACTIVE_IND').alias(f'{prefix}_CODE_ACTIVE_IND'), F.col('CODE_ADC_UPDT').alias(f'{prefix}_CODE_ADC_UPDT'))
    return df.join(F.broadcast(projected), F.col(source_column) == F.col(f'{prefix}_CODE_VALUE'), 'left')

def create_group_types(versions: Optional[Dict[str, int]]=None, config: MapMedicalPersonnelConfig=DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG) -> DataFrame:
    """Compatibility helper retaining the original function name."""
    return _mmp_code_lookup(versions, config).where(F.col('CDF_MEANING').isin('SRVCATEGORY', 'SURGSPEC', 'MEDSERVICE')).select('CODE_VALUE', 'CDF_MEANING', F.coalesce(_mmp_nonblank(F.col('DESCRIPTION')), _mmp_nonblank(F.col('DISPLAY')), _mmp_nonblank(F.col('CDF_MEANING'))).alias('group_description'))

def _mmp_build_group_bridge(versions: Dict[str, int], run_id: str, run_timestamp: datetime, config: MapMedicalPersonnelConfig) -> DataFrame:
    code_lookup = _mmp_code_lookup(versions, config).select(F.col('CODE_VALUE').alias('_GROUP_TYPE_CODE_VALUE'), F.col('CDF_MEANING'), F.col('DISPLAY').alias('GROUP_TYPE_DISPLAY'), F.col('DESCRIPTION').alias('GROUP_TYPE_DESCRIPTION'), F.col('CODE_ADC_UPDT').alias('GROUP_TYPE_CODE_ADC_UPDT'))
    group_source = _mmp_read_snapshot(config.prsnl_group_source, versions).select(_mmp_checked_long(F.col('PRSNL_GROUP_ID'), 'PRSNL_GROUP.PRSNL_GROUP_ID').alias('PRSNL_GROUP_ID'), _mmp_checked_long(F.col('PRSNL_GROUP_TYPE_CD'), 'PRSNL_GROUP.PRSNL_GROUP_TYPE_CD').alias('PRSNL_GROUP_TYPE_CD'), F.col('PRSNL_GROUP_NAME').cast('string').alias('PRSNL_GROUP_NAME'), F.col('PRSNL_GROUP_DESC').cast('string').alias('PRSNL_GROUP_DESC'), F.col('ACTIVE_IND').cast('long').alias('GROUP_ACTIVE_IND'), _mmp_checked_long(F.col('ACTIVE_STATUS_CD'), 'PRSNL_GROUP.ACTIVE_STATUS_CD').alias('GROUP_ACTIVE_STATUS_CD'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('GROUP_BEG_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('GROUP_END_EFFECTIVE_DT_TM'), F.col('UPDT_CNT').cast('long').alias('_GROUP_UPDT_CNT'), F.col('UPDT_DT_TM').cast('timestamp').alias('_GROUP_UPDT_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('GROUP_ADC_UPDT'))
    group_rank = Window.partitionBy('PRSNL_GROUP_ID').orderBy(F.col('GROUP_ADC_UPDT').desc_nulls_last(), F.col('_GROUP_UPDT_CNT').desc_nulls_last(), F.col('_GROUP_UPDT_DT_TM').desc_nulls_last())
    group_source = group_source.withColumn('_GROUP_ROW_NUMBER', F.row_number().over(group_rank)).where(F.col('_GROUP_ROW_NUMBER') == 1).drop('_GROUP_ROW_NUMBER', '_GROUP_UPDT_CNT', '_GROUP_UPDT_DT_TM').join(F.broadcast(code_lookup), F.col('PRSNL_GROUP_TYPE_CD') == F.col('_GROUP_TYPE_CODE_VALUE'), 'left').drop('_GROUP_TYPE_CODE_VALUE')
    relation_source = _mmp_read_snapshot(config.prsnl_group_reltn_source, versions).select(_mmp_checked_long(F.col('PRSNL_GROUP_RELTN_ID'), 'PRSNL_GROUP_RELTN.PRSNL_GROUP_RELTN_ID').alias('PRSNL_GROUP_RELTN_ID'), _mmp_checked_long(F.col('PERSON_ID'), 'PRSNL_GROUP_RELTN.PERSON_ID').alias('PERSON_ID'), _mmp_checked_long(F.col('PRSNL_GROUP_ID'), 'PRSNL_GROUP_RELTN.PRSNL_GROUP_ID').alias('PRSNL_GROUP_ID'), F.col('PRIMARY_IND').cast('boolean').alias('PRIMARY_IND'), F.col('ACTIVE_IND').cast('long').alias('RELATION_ACTIVE_IND'), _mmp_checked_long(F.col('ACTIVE_STATUS_CD'), 'PRSNL_GROUP_RELTN.ACTIVE_STATUS_CD').alias('RELATION_ACTIVE_STATUS_CD'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('RELATION_BEG_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('RELATION_END_EFFECTIVE_DT_TM'), F.col('UPDT_CNT').cast('long').alias('_RELATION_UPDT_CNT'), F.col('UPDT_DT_TM').cast('timestamp').alias('_RELATION_UPDT_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('RELATION_ADC_UPDT'))
    relation_rank = Window.partitionBy('PRSNL_GROUP_RELTN_ID').orderBy(F.col('RELATION_ADC_UPDT').desc_nulls_last(), F.col('_RELATION_UPDT_CNT').desc_nulls_last(), F.col('_RELATION_UPDT_DT_TM').desc_nulls_last())
    relation_source = relation_source.withColumn('_RELATION_ROW_NUMBER', F.row_number().over(relation_rank)).where(F.col('_RELATION_ROW_NUMBER') == 1).drop('_RELATION_ROW_NUMBER', '_RELATION_UPDT_CNT', '_RELATION_UPDT_DT_TM')
    result = relation_source.join(group_source, 'PRSNL_GROUP_ID', 'left').withColumn('GROUP_LABEL', F.coalesce(_mmp_nonblank(F.col('PRSNL_GROUP_NAME')), _mmp_nonblank(F.col('PRSNL_GROUP_DESC')), _mmp_nonblank(F.col('GROUP_TYPE_DESCRIPTION')), _mmp_nonblank(F.col('GROUP_TYPE_DISPLAY')), _mmp_nonblank(F.col('CDF_MEANING')), F.col('PRSNL_GROUP_ID').cast('string'))).withColumn('RELATION_EFFECTIVE_NOW_IND', _mmp_effective_now(F.col('RELATION_BEG_EFFECTIVE_DT_TM'), F.col('RELATION_END_EFFECTIVE_DT_TM'), run_timestamp)).withColumn('GROUP_EFFECTIVE_NOW_IND', _mmp_effective_now(F.col('GROUP_BEG_EFFECTIVE_DT_TM'), F.col('GROUP_END_EFFECTIVE_DT_TM'), run_timestamp)).withColumn('ADC_UPDT', F.greatest(F.col('RELATION_ADC_UPDT'), F.col('GROUP_ADC_UPDT'), F.col('GROUP_TYPE_CODE_ADC_UPDT'))).withColumn('SOURCE_PRESENT_IND', F.lit(True)).withColumn('SOURCE_ABSENT_DETECTED_TS', F.lit(None).cast('timestamp')).withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_UPDT_DT_TM', F.lit(run_timestamp).cast('timestamp'))
    result = _mmp_add_row_hash(result, schema_map_medical_personnel_group, ['SOURCE_ABSENT_DETECTED_TS', 'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM'])
    return _mmp_align_to_schema(result, schema_map_medical_personnel_group)

def _mmp_refresh_group_bridge(versions: Dict[str, int], run_id: str, run_timestamp: datetime, config: MapMedicalPersonnelConfig) -> int:
    current = _mmp_build_group_bridge(versions, run_id, run_timestamp, config)
    count = _mmp_validate_unique(current, ['PRSNL_GROUP_RELTN_ID'], 'medical personnel group bridge')
    _mmp_reconcile_full_snapshot(current, config.group_bridge_table, schema_map_medical_personnel_group, ['PRSNL_GROUP_RELTN_ID'], MAP_MEDICAL_PERSONNEL_GROUP_COMMENT, config)
    _mmp_ensure_cluster_by(config.group_bridge_table, ['PERSON_ID'])
    return count

def _mmp_group_summary(config: MapMedicalPersonnelConfig) -> DataFrame:
    bridge = spark.table(config.group_bridge_table).where(F.col('SOURCE_PRESENT_IND') == F.lit(True)).where(F.col('CDF_MEANING').isin('SRVCATEGORY', 'SURGSPEC', 'MEDSERVICE'))
    ranking = Window.partitionBy('PERSON_ID', 'CDF_MEANING').orderBy(F.col('RELATION_ACTIVE_IND').desc_nulls_last(), F.col('GROUP_ACTIVE_IND').desc_nulls_last(), F.col('RELATION_EFFECTIVE_NOW_IND').desc_nulls_last(), F.col('GROUP_EFFECTIVE_NOW_IND').desc_nulls_last(), F.col('PRIMARY_IND').desc_nulls_last(), F.col('ADC_UPDT').desc_nulls_last(), F.col('PRSNL_GROUP_RELTN_ID').desc_nulls_last())
    ranked = bridge.withColumn('_SELECTION_RANK', F.row_number().over(ranking))
    counts = bridge.groupBy('PERSON_ID', 'CDF_MEANING').agg(F.count(F.lit(1)).cast('int').alias('_ASSIGNMENT_COUNT'), F.sum(F.when((F.col('RELATION_ACTIVE_IND') == 1) & (F.col('GROUP_ACTIVE_IND') == 1), F.lit(1)).otherwise(F.lit(0))).cast('int').alias('_ACTIVE_ASSIGNMENT_COUNT'), F.max('ADC_UPDT').alias('_GROUP_TYPE_ADC_UPDT'))
    selected = ranked.where(F.col('_SELECTION_RANK') == 1).join(counts, ['PERSON_ID', 'CDF_MEANING'], 'left').withColumn('_SELECTED_INACTIVE_IND', (F.coalesce(F.col('RELATION_ACTIVE_IND'), F.lit(0)) != 1) | (F.coalesce(F.col('GROUP_ACTIVE_IND'), F.lit(0)) != 1) | ~F.coalesce(F.col('RELATION_EFFECTIVE_NOW_IND'), F.lit(False)) | ~F.coalesce(F.col('GROUP_EFFECTIVE_NOW_IND'), F.lit(False)))
    result = selected.select('PERSON_ID').distinct()
    for meaning in ('SRVCATEGORY', 'SURGSPEC', 'MEDSERVICE'):
        prefix = meaning
        values = selected.where(F.col('CDF_MEANING') == meaning).select('PERSON_ID', F.col('GROUP_LABEL').alias(prefix), F.col('PRSNL_GROUP_ID').alias(f'{prefix}_GROUP_ID'), F.col('PRSNL_GROUP_NAME').alias(f'{prefix}_GROUP_NAME'), F.col('_ASSIGNMENT_COUNT').alias(f'{prefix}_ASSIGNMENT_COUNT'), F.col('_ACTIVE_ASSIGNMENT_COUNT').alias(f'{prefix}_ACTIVE_ASSIGNMENT_COUNT'), F.col('PRIMARY_IND').cast('boolean').alias(f'{prefix}_SELECTED_PRIMARY_IND'), F.col('_SELECTED_INACTIVE_IND').cast('boolean').alias(f'{prefix}_SELECTED_INACTIVE_IND'), F.col('_GROUP_TYPE_ADC_UPDT').alias(f'_{prefix}_ADC_UPDT'))
        result = result.join(values, 'PERSON_ID', 'left')
    return result.withColumn('GROUP_ADC_UPDT', F.greatest(F.col('_SRVCATEGORY_ADC_UPDT'), F.col('_SURGSPEC_ADC_UPDT'), F.col('_MEDSERVICE_ADC_UPDT'))).drop('_SRVCATEGORY_ADC_UPDT', '_SURGSPEC_ADC_UPDT', '_MEDSERVICE_ADC_UPDT')

def get_group_assignments(prsnl_group_reltn: Optional[DataFrame]=None, valid_groups: Optional[DataFrame]=None, config: MapMedicalPersonnelConfig=DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG) -> DataFrame:
    """
    Compatibility replacement.

    The supplied DataFrames are intentionally ignored. The lossless bridge is
    the authoritative source, and compatibility scalar values are selected
    deterministically without converting missing values to empty strings.
    """
    if not _mmp_table_exists(config.group_bridge_table):
        raise RuntimeError(f'{config.group_bridge_table} is not initialized. Run deploy_map_medical_personnel_improvements() once.')
    return _mmp_group_summary(config)

def _mmp_build_alias_bridge(versions: Dict[str, int], run_id: str, run_timestamp: datetime, config: MapMedicalPersonnelConfig) -> DataFrame:
    lookup = _mmp_code_lookup(versions, config).select(F.col('CODE_VALUE').alias('_ALIAS_TYPE_CODE_VALUE'), F.col('CDF_MEANING').alias('ALIAS_TYPE_CDF_MEANING'), F.col('DISPLAY').alias('ALIAS_TYPE_DISPLAY'), F.col('DESCRIPTION').alias('ALIAS_TYPE_DESCRIPTION'))
    source = _mmp_read_snapshot(config.prsnl_alias_source, versions).select(_mmp_checked_long(F.col('PRSNL_ALIAS_ID'), 'PRSNL_ALIAS.PRSNL_ALIAS_ID').alias('PRSNL_ALIAS_ID'), _mmp_checked_long(F.col('PERSON_ID'), 'PRSNL_ALIAS.PERSON_ID').alias('PERSON_ID'), _mmp_checked_long(F.col('ALIAS_POOL_CD'), 'PRSNL_ALIAS.ALIAS_POOL_CD').alias('ALIAS_POOL_CD'), _mmp_checked_long(F.col('PRSNL_ALIAS_TYPE_CD'), 'PRSNL_ALIAS.PRSNL_ALIAS_TYPE_CD').alias('PRSNL_ALIAS_TYPE_CD'), F.col('ALIAS').cast('string').alias('ALIAS'), F.col('ACTIVE_IND').cast('long').alias('ACTIVE_IND'), _mmp_checked_long(F.col('ACTIVE_STATUS_CD'), 'PRSNL_ALIAS.ACTIVE_STATUS_CD').alias('ACTIVE_STATUS_CD'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('BEG_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('END_EFFECTIVE_DT_TM'), _mmp_checked_long(F.col('CONTRIBUTOR_SYSTEM_CD'), 'PRSNL_ALIAS.CONTRIBUTOR_SYSTEM_CD').alias('CONTRIBUTOR_SYSTEM_CD'), F.col('UPDT_CNT').cast('long').alias('_UPDT_CNT'), F.col('UPDT_DT_TM').cast('timestamp').alias('_UPDT_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('ADC_UPDT'))
    rank_window = Window.partitionBy('PRSNL_ALIAS_ID').orderBy(F.col('ADC_UPDT').desc_nulls_last(), F.col('_UPDT_CNT').desc_nulls_last(), F.col('_UPDT_DT_TM').desc_nulls_last())
    source = source.withColumn('_ROW_NUMBER', F.row_number().over(rank_window)).where(F.col('_ROW_NUMBER') == 1).drop('_ROW_NUMBER', '_UPDT_CNT', '_UPDT_DT_TM').join(F.broadcast(lookup), F.col('PRSNL_ALIAS_TYPE_CD') == F.col('_ALIAS_TYPE_CODE_VALUE'), 'left').drop('_ALIAS_TYPE_CODE_VALUE').withColumn('EFFECTIVE_NOW_IND', _mmp_effective_now(F.col('BEG_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM'), run_timestamp)).withColumn('SOURCE_PRESENT_IND', F.lit(True)).withColumn('SOURCE_ABSENT_DETECTED_TS', F.lit(None).cast('timestamp')).withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_UPDT_DT_TM', F.lit(run_timestamp).cast('timestamp'))
    source = _mmp_add_row_hash(source, schema_map_medical_personnel_alias, ['SOURCE_ABSENT_DETECTED_TS', 'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM'])
    return _mmp_align_to_schema(source, schema_map_medical_personnel_alias)

def _mmp_refresh_alias_bridge(versions: Dict[str, int], run_id: str, run_timestamp: datetime, config: MapMedicalPersonnelConfig) -> int:
    current = _mmp_build_alias_bridge(versions, run_id, run_timestamp, config)
    count = _mmp_validate_unique(current, ['PRSNL_ALIAS_ID'], 'medical personnel alias bridge')
    _mmp_reconcile_full_snapshot(current, config.alias_bridge_table, schema_map_medical_personnel_alias, ['PRSNL_ALIAS_ID'], MAP_MEDICAL_PERSONNEL_ALIAS_COMMENT, config)
    _mmp_ensure_cluster_by(config.alias_bridge_table, ['PERSON_ID'])
    return count

def _mmp_alias_summary(config: MapMedicalPersonnelConfig) -> DataFrame:
    relevant = spark.table(config.alias_bridge_table).where(F.col('SOURCE_PRESENT_IND') == F.lit(True)).where(F.col('ALIAS_TYPE_CDF_MEANING').isin('NPI', 'DOCNBR', 'GDP', 'EXTERNALID'))
    ranking = Window.partitionBy('PERSON_ID', 'ALIAS_TYPE_CDF_MEANING').orderBy(F.col('ACTIVE_IND').desc_nulls_last(), F.col('EFFECTIVE_NOW_IND').desc_nulls_last(), F.col('ADC_UPDT').desc_nulls_last(), F.col('PRSNL_ALIAS_ID').desc_nulls_last())
    ranked = relevant.withColumn('_SELECTION_RANK', F.row_number().over(ranking))
    counts = relevant.groupBy('PERSON_ID', 'ALIAS_TYPE_CDF_MEANING').agg(F.count(F.lit(1)).cast('int').alias('_ALIAS_COUNT'), F.max('ADC_UPDT').alias('_ALIAS_TYPE_ADC_UPDT'))
    selected = ranked.where(F.col('_SELECTION_RANK') == 1).join(counts, ['PERSON_ID', 'ALIAS_TYPE_CDF_MEANING'], 'left')
    result = selected.select('PERSON_ID').distinct()
    definitions = {'NPI': ('NPI', 'NPI_ALIAS_COUNT'), 'DOCNBR': ('DOCNBR', 'DOCNBR_ALIAS_COUNT'), 'GDP': ('GDP_NUMBER', 'GDP_ALIAS_COUNT'), 'EXTERNALID': ('EXTERNAL_PROVIDER_ID', 'EXTERNAL_PROVIDER_ID_COUNT')}
    update_columns = []
    for meaning, (value_name, count_name) in definitions.items():
        prefix = meaning.replace(' ', '_')
        values = selected.where(F.col('ALIAS_TYPE_CDF_MEANING') == meaning).select('PERSON_ID', F.col('ALIAS').alias(value_name), F.col('_ALIAS_COUNT').alias(count_name), F.col('_ALIAS_TYPE_ADC_UPDT').alias(f'_{prefix}_ADC_UPDT'))
        result = result.join(values, 'PERSON_ID', 'left')
        update_columns.append(f'_{prefix}_ADC_UPDT')
    return result.withColumn('ALIAS_ADC_UPDT', F.greatest(*[F.col(name) for name in update_columns])).drop(*update_columns)

def _mmp_event_key_expression() -> F.Column:
    return F.when(F.col('EVENT_ID').isNotNull() & (F.col('EVENT_ID') != 0), F.concat(F.lit('E:'), F.col('EVENT_ID').cast('string'))).when(F.col('CLINICAL_EVENT_ID').isNotNull(), F.concat(F.lit('C:'), F.col('CLINICAL_EVENT_ID').cast('string')))

def _mmp_prepare_event_rows(source: DataFrame, run_timestamp: datetime) -> DataFrame:
    prepared = source.select(_mmp_checked_long(F.col('EVENT_ID'), 'CLINICAL_EVENT.EVENT_ID').alias('EVENT_ID'), _mmp_checked_long(F.col('CLINICAL_EVENT_ID'), 'CLINICAL_EVENT.CLINICAL_EVENT_ID').alias('CLINICAL_EVENT_ID'), _mmp_checked_long(F.col('ENCNTR_ID'), 'CLINICAL_EVENT.ENCNTR_ID').alias('ENCNTR_ID'), _mmp_checked_long(F.col('PERFORMED_PRSNL_ID'), 'CLINICAL_EVENT.PERFORMED_PRSNL_ID').alias('PERFORMED_PRSNL_ID'), F.col('PERFORMED_DT_TM').cast('timestamp').alias('PERFORMED_DT_TM'), F.col('EVENT_START_DT_TM').cast('timestamp').alias('EVENT_START_DT_TM'), F.col('EVENT_END_DT_TM').cast('timestamp').alias('EVENT_END_DT_TM'), F.col('UPDT_DT_TM').cast('timestamp').alias('EVENT_UPDT_DT_TM'), F.col('VALID_UNTIL_DT_TM').cast('timestamp').alias('VALID_UNTIL_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('EVENT_ADC_UPDT')).withColumn('EVENT_KEY', _mmp_event_key_expression())
    event_time = F.coalesce(F.col('PERFORMED_DT_TM'), F.col('EVENT_START_DT_TM'), F.col('EVENT_END_DT_TM'), F.col('EVENT_UPDT_DT_TM'))
    event_time_source = F.when(F.col('PERFORMED_DT_TM').isNotNull(), F.lit('PERFORMED_DT_TM')).when(F.col('EVENT_START_DT_TM').isNotNull(), F.lit('EVENT_START_DT_TM')).when(F.col('EVENT_END_DT_TM').isNotNull(), F.lit('EVENT_END_DT_TM')).when(F.col('EVENT_UPDT_DT_TM').isNotNull(), F.lit('UPDT_DT_TM'))
    current = F.col('VALID_UNTIL_DT_TM').isNull() | (F.col('VALID_UNTIL_DT_TM') > F.lit(run_timestamp).cast('timestamp'))
    return prepared.where(F.col('EVENT_KEY').isNotNull()).withColumn('EVENT_DT_TM', event_time).withColumn('EVENT_DT_TM_SOURCE', event_time_source).withColumn('EVENT_CURRENT_IND', current)

def _mmp_canonicalize_events(prepared: DataFrame, include_version_count: bool) -> DataFrame:
    ranking = Window.partitionBy('EVENT_KEY').orderBy(F.col('EVENT_CURRENT_IND').desc_nulls_last(), F.col('VALID_UNTIL_DT_TM').desc_nulls_last(), F.col('EVENT_ADC_UPDT').desc_nulls_last(), F.col('EVENT_UPDT_DT_TM').desc_nulls_last(), F.col('CLINICAL_EVENT_ID').desc_nulls_last())
    if include_version_count:
        version_window = Window.partitionBy('EVENT_KEY')
        prepared = prepared.withColumn('EVENT_VERSION_COUNT', F.count(F.lit(1)).over(version_window).cast('int'))
    elif 'EVENT_VERSION_COUNT' not in prepared.columns:
        prepared = prepared.withColumn('EVENT_VERSION_COUNT', F.lit(None).cast('int'))
    return prepared.withColumn('_EVENT_RANK', F.row_number().over(ranking)).where(F.col('_EVENT_RANK') == 1).drop('_EVENT_RANK', 'PERFORMED_DT_TM', 'EVENT_START_DT_TM', 'EVENT_END_DT_TM', 'EVENT_UPDT_DT_TM')

def _mmp_canonical_events_from_snapshot(versions: Dict[str, int], run_timestamp: datetime, config: MapMedicalPersonnelConfig, event_keys: Optional[DataFrame]=None, key_count: Optional[int]=None) -> DataFrame:
    raw = _mmp_read_snapshot(config.clinical_event_source, versions).select('EVENT_ID', 'CLINICAL_EVENT_ID', 'ENCNTR_ID', 'PERFORMED_PRSNL_ID', 'PERFORMED_DT_TM', 'EVENT_START_DT_TM', 'EVENT_END_DT_TM', 'UPDT_DT_TM', 'VALID_UNTIL_DT_TM', 'ADC_UPDT')
    prepared = _mmp_prepare_event_rows(raw, run_timestamp)
    if event_keys is not None:
        prepared = _mmp_semijoin_keys(prepared, event_keys, ['EVENT_KEY'], config.broadcast_key_limit, key_count)
    return _mmp_canonicalize_events(prepared, include_version_count=True)

def _mmp_prepare_location_history(versions: Dict[str, int], config: MapMedicalPersonnelConfig, encounter_ids: Optional[DataFrame]=None, encounter_count: Optional[int]=None) -> DataFrame:
    history = _mmp_read_snapshot(config.encounter_location_history_source, versions).select(_mmp_checked_long(F.col('ENCNTR_LOC_HIST_ID'), 'ENCNTR_LOC_HIST.ENCNTR_LOC_HIST_ID').alias('ENCNTR_LOC_HIST_ID'), _mmp_checked_long(F.col('ENCNTR_ID'), 'ENCNTR_LOC_HIST.ENCNTR_ID').alias('ENCNTR_ID'), _mmp_checked_long(F.col('LOC_NURSE_UNIT_CD'), 'ENCNTR_LOC_HIST.LOC_NURSE_UNIT_CD').alias('HIST_LOC_NURSE_UNIT_CD'), F.col('ACTIVE_IND').cast('long').alias('HIST_ACTIVE_IND'), F.col('ARRIVE_DT_TM').cast('timestamp').alias('HIST_ARRIVE_DT_TM'), F.col('DEPART_DT_TM').cast('timestamp').alias('HIST_DEPART_DT_TM'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('HIST_BEG_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('HIST_END_EFFECTIVE_DT_TM'), F.col('TRANSACTION_DT_TM').cast('timestamp').alias('HIST_TRANSACTION_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('LOCATION_HISTORY_ADC_UPDT'))
    if encounter_ids is not None:
        history = _mmp_semijoin_keys(history, encounter_ids, ['ENCNTR_ID'], config.broadcast_key_limit, encounter_count)
    return history

def _mmp_prepare_encounter_locations(versions: Dict[str, int], config: MapMedicalPersonnelConfig, encounter_ids: Optional[DataFrame]=None, encounter_count: Optional[int]=None) -> DataFrame:
    encounters = _mmp_read_snapshot(config.encounter_source, versions).select(_mmp_checked_long(F.col('ENCNTR_ID'), 'ENCOUNTER.ENCNTR_ID').alias('ENCNTR_ID'), _mmp_checked_long(F.col('LOC_NURSE_UNIT_CD'), 'ENCOUNTER.LOC_NURSE_UNIT_CD').alias('CURRENT_LOC_NURSE_UNIT_CD'), F.col('UPDT_CNT').cast('long').alias('_ENCOUNTER_UPDT_CNT'), F.col('UPDT_DT_TM').cast('timestamp').alias('_ENCOUNTER_UPDT_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('ENCOUNTER_ADC_UPDT'))
    if encounter_ids is not None:
        encounters = _mmp_semijoin_keys(encounters, encounter_ids, ['ENCNTR_ID'], config.broadcast_key_limit, encounter_count)
    ranking = Window.partitionBy('ENCNTR_ID').orderBy(F.col('ENCOUNTER_ADC_UPDT').desc_nulls_last(), F.col('_ENCOUNTER_UPDT_CNT').desc_nulls_last(), F.col('_ENCOUNTER_UPDT_DT_TM').desc_nulls_last())
    return encounters.withColumn('_ENCOUNTER_RANK', F.row_number().over(ranking)).where(F.col('_ENCOUNTER_RANK') == 1).drop('_ENCOUNTER_RANK', '_ENCOUNTER_UPDT_CNT', '_ENCOUNTER_UPDT_DT_TM')

def _mmp_attach_event_locations(events: DataFrame, versions: Dict[str, int], run_id: str, run_timestamp: datetime, config: MapMedicalPersonnelConfig) -> DataFrame:
    encounter_ids = events.select('ENCNTR_ID').where(F.col('ENCNTR_ID').isNotNull()).distinct()
    encounter_count = config.broadcast_key_limit + 1
    history = _mmp_prepare_location_history(versions, config, encounter_ids, encounter_count)
    encounters = _mmp_prepare_encounter_locations(versions, config, encounter_ids, encounter_count)
    history_start = F.coalesce(F.col('h.HIST_ARRIVE_DT_TM'), F.col('h.HIST_BEG_EFFECTIVE_DT_TM'), F.col('h.HIST_TRANSACTION_DT_TM'), _MMP_LOW_TS)
    history_end = F.coalesce(F.col('h.HIST_DEPART_DT_TM'), F.col('h.HIST_END_EFFECTIVE_DT_TM'), _MMP_HIGH_TS)
    temporal_match = (F.col('e.ENCNTR_ID') == F.col('h.ENCNTR_ID')) & F.col('e.EVENT_DT_TM').isNotNull() & (F.col('e.EVENT_DT_TM') >= history_start) & (F.col('e.EVENT_DT_TM') < history_end)
    joined = events.alias('e').join(history.alias('h'), temporal_match, 'left')
    history_rank = Window.partitionBy(F.col('e.EVENT_KEY')).orderBy(F.col('h.ENCNTR_LOC_HIST_ID').isNotNull().desc(), F.col('h.HIST_ACTIVE_IND').desc_nulls_last(), F.col('h.HIST_TRANSACTION_DT_TM').desc_nulls_last(), F.col('h.HIST_ARRIVE_DT_TM').desc_nulls_last(), F.col('h.ENCNTR_LOC_HIST_ID').desc_nulls_last())
    selected = joined.withColumn('_HISTORY_RANK', F.row_number().over(history_rank)).where(F.col('_HISTORY_RANK') == 1).select('e.*', F.col('h.ENCNTR_LOC_HIST_ID').alias('ENCNTR_LOC_HIST_ID'), F.col('h.HIST_LOC_NURSE_UNIT_CD').alias('HIST_LOC_NURSE_UNIT_CD'), F.col('h.LOCATION_HISTORY_ADC_UPDT').alias('LOCATION_HISTORY_ADC_UPDT'))
    selected = selected.join(encounters, 'ENCNTR_ID', 'left')
    result = selected.withColumn('LOC_NURSE_UNIT_CD', F.coalesce(F.col('HIST_LOC_NURSE_UNIT_CD'), F.col('CURRENT_LOC_NURSE_UNIT_CD'))).withColumn('LOCATION_SOURCE', F.when(F.col('HIST_LOC_NURSE_UNIT_CD').isNotNull(), F.lit('ENCOUNTER_LOCATION_HISTORY')).when(F.col('CURRENT_LOC_NURSE_UNIT_CD').isNotNull(), F.lit('ENCOUNTER_CURRENT')).otherwise(F.lit('NONE'))).withColumn('ADC_UPDT', F.greatest(F.col('EVENT_ADC_UPDT'), F.col('LOCATION_HISTORY_ADC_UPDT'), F.col('ENCOUNTER_ADC_UPDT'))).withColumn('SOURCE_PRESENT_IND', F.lit(True)).withColumn('SOURCE_ABSENT_DETECTED_TS', F.lit(None).cast('timestamp')).withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_UPDT_DT_TM', F.lit(run_timestamp).cast('timestamp')).drop('HIST_LOC_NURSE_UNIT_CD', 'CURRENT_LOC_NURSE_UNIT_CD')
    result = _mmp_add_row_hash(result, schema_map_medical_personnel_event_state, ['SOURCE_ABSENT_DETECTED_TS', 'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM'])
    return _mmp_align_to_schema(result, schema_map_medical_personnel_event_state)

def _mmp_build_full_event_state(versions: Dict[str, int], run_id: str, run_timestamp: datetime, config: MapMedicalPersonnelConfig) -> DataFrame:
    canonical = _mmp_canonical_events_from_snapshot(versions, run_timestamp, config)
    return _mmp_attach_event_locations(canonical, versions, run_id, run_timestamp, config)

def _mmp_build_location_evidence(event_state: DataFrame, run_id: str, run_timestamp: datetime) -> DataFrame:
    usable = event_state.where((F.col('SOURCE_PRESENT_IND') == F.lit(True)) & (F.col('EVENT_CURRENT_IND') == F.lit(True)) & F.col('PERFORMED_PRSNL_ID').isNotNull() & (F.col('PERFORMED_PRSNL_ID') != 0) & F.col('LOC_NURSE_UNIT_CD').isNotNull() & (F.col('LOC_NURSE_UNIT_CD') != 0))
    aggregated = usable.groupBy(F.col('PERFORMED_PRSNL_ID').alias('PERSON_ID'), 'LOC_NURSE_UNIT_CD').agg(F.count(F.lit(1)).cast('long').alias('EVENT_COUNT'), F.min('EVENT_DT_TM').alias('FIRST_EVENT_DT_TM'), F.max('EVENT_DT_TM').alias('LAST_EVENT_DT_TM'), F.max('ADC_UPDT').alias('EVIDENCE_ADC_UPDT'))
    person_window = Window.partitionBy('PERSON_ID')
    rank_window = Window.partitionBy('PERSON_ID').orderBy(F.col('EVENT_COUNT').desc(), F.col('LAST_EVENT_DT_TM').desc_nulls_last(), F.col('LOC_NURSE_UNIT_CD').asc())
    result = aggregated.withColumn('_MAX_EVENT_COUNT', F.max('EVENT_COUNT').over(person_window)).withColumn('TOP_COUNT_TIE_COUNT', F.sum(F.when(F.col('EVENT_COUNT') == F.col('_MAX_EVENT_COUNT'), F.lit(1)).otherwise(F.lit(0))).over(person_window).cast('int')).withColumn('LOCATION_RANK', F.row_number().over(rank_window).cast('int')).drop('_MAX_EVENT_COUNT').withColumn('SOURCE_PRESENT_IND', F.lit(True)).withColumn('SOURCE_ABSENT_DETECTED_TS', F.lit(None).cast('timestamp')).withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_UPDT_DT_TM', F.lit(run_timestamp).cast('timestamp'))
    result = _mmp_add_row_hash(result, schema_map_medical_personnel_location_evidence, ['SOURCE_ABSENT_DETECTED_TS', 'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM'])
    return _mmp_align_to_schema(result, schema_map_medical_personnel_location_evidence)

def _mmp_merge_subset_with_tombstones(current_df: DataFrame, affected_keys: DataFrame, table_name: str, schema: T.StructType, key_columns: Sequence[str], run_id: str, run_timestamp: datetime) -> None:
    aligned_current = _mmp_align_to_schema(current_df, schema)
    existing = _mmp_semijoin_keys(spark.table(table_name), affected_keys, key_columns, DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG.broadcast_key_limit)
    missing = existing.join(aligned_current.select(*key_columns), list(key_columns), 'left_anti')
    missing = missing.withColumn('SOURCE_PRESENT_IND', F.lit(False)).withColumn('SOURCE_ABSENT_DETECTED_TS', F.coalesce(F.col('SOURCE_ABSENT_DETECTED_TS'), F.lit(run_timestamp).cast('timestamp'))).withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_UPDT_DT_TM', F.lit(run_timestamp).cast('timestamp'))
    missing = _mmp_add_row_hash(missing, schema, ['SOURCE_ABSENT_DETECTED_TS', 'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM'])
    source = aligned_current.unionByName(_mmp_align_to_schema(missing, schema))
    condition = ' AND '.join((f't.{column} <=> s.{column}' for column in key_columns))
    change_condition = _mmp_merge_change_condition(schema, key_columns)
    DeltaTable.forName(spark, table_name).alias('t').merge(source.alias('s'), condition).whenMatchedUpdateAll(condition=change_condition).whenNotMatchedInsertAll().execute()

def _mmp_changed_event_keys_from_cdf(changes: Optional[DataFrame], run_timestamp: datetime) -> Optional[DataFrame]:
    if changes is None:
        return None
    return _mmp_prepare_event_rows(changes, run_timestamp).select('EVENT_KEY').distinct()

def _mmp_current_event_rows_from_cdf(changes: Optional[DataFrame], run_timestamp: datetime, config: MapMedicalPersonnelConfig) -> Optional[DataFrame]:
    """
    Return current canonical postimages directly from CDF.

    This is the normal fast path. Snapshot lookups are needed only for changed
    logical events whose CDF range contains no current postimage, such as a
    deletion or an update solely to a superseded physical version.
    """
    if changes is None:
        return None
    postimages = changes.where(F.col('_change_type').isin('insert', 'update_postimage'))
    prepared = _mmp_prepare_event_rows(postimages, run_timestamp).where(F.col('EVENT_CURRENT_IND') == F.lit(True))
    canonical = _mmp_canonicalize_events(prepared, include_version_count=False)
    if _mmp_table_exists(config.event_state_table):
        existing_counts = spark.table(config.event_state_table).select('EVENT_KEY', F.col('EVENT_VERSION_COUNT').alias('_EXISTING_EVENT_VERSION_COUNT'))
        canonical = canonical.drop('EVENT_VERSION_COUNT').join(existing_counts, 'EVENT_KEY', 'left').withColumn('EVENT_VERSION_COUNT', F.coalesce(F.col('_EXISTING_EVENT_VERSION_COUNT'), F.lit(1)).cast('int')).drop('_EXISTING_EVENT_VERSION_COUNT')
    return canonical

def _mmp_changed_encounter_ids_from_cdf(changes: Optional[DataFrame]) -> Optional[DataFrame]:
    if changes is None:
        return None
    return changes.select(_mmp_checked_long(F.col('ENCNTR_ID'), 'CDF.ENCNTR_ID').alias('ENCNTR_ID')).where(F.col('ENCNTR_ID').isNotNull()).distinct()

def _mmp_refresh_location_support_incremental(checkpoints: Dict[str, int], current_versions: Dict[str, int], run_id: str, run_timestamp: datetime, config: MapMedicalPersonnelConfig) -> Dict[str, int]:
    changed_ranges = {table: (checkpoints[table] + 1, current_versions[table]) for table in config.location_cdf_sources if current_versions[table] > checkpoints[table]}
    if not changed_ranges:
        return {'changed_event_keys': 0, 'changed_encounters': 0, 'affected_personnel': 0}
    cdf = {table: _mmp_read_cdf(table, start, end) for table, (start, end) in changed_ranges.items()}
    clinical_changes = cdf.get(config.clinical_event_source)
    clinical_event_keys = _mmp_changed_event_keys_from_cdf(clinical_changes, run_timestamp)
    encounter_ids = _mmp_union_distinct([_mmp_changed_encounter_ids_from_cdf(cdf.get(config.encounter_source)), _mmp_changed_encounter_ids_from_cdf(cdf.get(config.encounter_location_history_source))], ['ENCNTR_ID'])
    encounter_event_keys = None
    if encounter_ids is not None:
        encounter_event_keys = spark.table(config.event_state_table).where(F.col('SOURCE_PRESENT_IND') == F.lit(True)).join(encounter_ids, 'ENCNTR_ID', 'left_semi').select('EVENT_KEY').distinct()
    event_keys = _mmp_union_distinct([clinical_event_keys, encounter_event_keys], ['EVENT_KEY'])
    if event_keys is None:
        return {'changed_event_keys': 0, 'changed_encounters': 0, 'affected_personnel': 0}
    event_key_count = event_keys.count()
    if event_key_count == 0:
        return {'changed_event_keys': 0, 'changed_encounters': 0, 'affected_personnel': 0}
    old_personnel_rows = spark.table(config.event_state_table).join(event_keys, 'EVENT_KEY', 'left_semi').select(F.col('PERFORMED_PRSNL_ID').alias('PERSON_ID')).where(F.col('PERSON_ID').isNotNull() & (F.col('PERSON_ID') != 0)).distinct().limit(config.broadcast_key_limit + 1).collect()
    if len(old_personnel_rows) > config.broadcast_key_limit:
        raise RuntimeError(f'Incremental location refresh affects more than {config.broadcast_key_limit:,} existing personnel. Run rebuild_map_medical_personnel_v2() rather than collecting an unbounded driver-side key set.')
    old_personnel = spark.createDataFrame([(int(row['PERSON_ID']),) for row in old_personnel_rows], 'PERSON_ID long').dropDuplicates(['PERSON_ID'])
    cdf_current = _mmp_current_event_rows_from_cdf(clinical_changes, run_timestamp, config)
    if cdf_current is None:
        cdf_current_keys = None
    else:
        cdf_current_keys = cdf_current.select('EVENT_KEY').distinct()
    fallback_keys = None
    if clinical_event_keys is not None:
        fallback_keys = clinical_event_keys if cdf_current_keys is None else clinical_event_keys.join(cdf_current_keys, 'EVENT_KEY', 'left_anti')
    fallback_count = 0 if fallback_keys is None else fallback_keys.count()
    fallback_current = None
    if fallback_count:
        fallback_current = _mmp_canonical_events_from_snapshot(current_versions, run_timestamp, config, fallback_keys, fallback_count)
    location_only = None
    if encounter_event_keys is not None:
        location_only_keys = encounter_event_keys if clinical_event_keys is None else encounter_event_keys.join(clinical_event_keys, 'EVENT_KEY', 'left_anti')
        location_only = spark.table(config.event_state_table).where(F.col('SOURCE_PRESENT_IND') == F.lit(True)).join(location_only_keys, 'EVENT_KEY', 'left_semi').select('EVENT_KEY', 'EVENT_ID', 'CLINICAL_EVENT_ID', 'ENCNTR_ID', 'PERFORMED_PRSNL_ID', 'EVENT_DT_TM', 'EVENT_DT_TM_SOURCE', 'VALID_UNTIL_DT_TM', 'EVENT_CURRENT_IND', 'EVENT_VERSION_COUNT', 'EVENT_ADC_UPDT')
    event_bases = [frame for frame in (cdf_current, fallback_current, location_only) if frame is not None]
    if event_bases:
        canonical = reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), event_bases).dropDuplicates(['EVENT_KEY'])
    else:
        canonical = spark.createDataFrame([], T.StructType([T.StructField('EVENT_KEY', T.StringType()), T.StructField('EVENT_ID', T.LongType()), T.StructField('CLINICAL_EVENT_ID', T.LongType()), T.StructField('ENCNTR_ID', T.LongType()), T.StructField('PERFORMED_PRSNL_ID', T.LongType()), T.StructField('EVENT_DT_TM', T.TimestampType()), T.StructField('EVENT_DT_TM_SOURCE', T.StringType()), T.StructField('VALID_UNTIL_DT_TM', T.TimestampType()), T.StructField('EVENT_CURRENT_IND', T.BooleanType()), T.StructField('EVENT_VERSION_COUNT', T.IntegerType()), T.StructField('EVENT_ADC_UPDT', T.TimestampType())]))
    new_state = _mmp_attach_event_locations(canonical, current_versions, run_id, run_timestamp, config)
    _mmp_merge_subset_with_tombstones(new_state, event_keys, config.event_state_table, schema_map_medical_personnel_event_state, ['EVENT_KEY'], run_id, run_timestamp)
    affected_personnel = old_personnel.unionByName(new_state.select(F.col('PERFORMED_PRSNL_ID').alias('PERSON_ID')).where(F.col('PERSON_ID').isNotNull() & (F.col('PERSON_ID') != 0))).distinct()
    affected_count = affected_personnel.count()
    if affected_count:
        state_subset = spark.table(config.event_state_table).join(affected_personnel, F.col('PERFORMED_PRSNL_ID') == F.col('PERSON_ID'), 'left_semi')
        evidence = _mmp_build_location_evidence(state_subset, run_id, run_timestamp)
        evidence_keys = spark.table(config.location_evidence_table).join(affected_personnel, 'PERSON_ID', 'left_semi').select('PERSON_ID', 'LOC_NURSE_UNIT_CD').unionByName(evidence.select('PERSON_ID', 'LOC_NURSE_UNIT_CD')).distinct()
        _mmp_merge_subset_with_tombstones(evidence, evidence_keys, config.location_evidence_table, schema_map_medical_personnel_location_evidence, ['PERSON_ID', 'LOC_NURSE_UNIT_CD'], run_id, run_timestamp)
    return {'changed_event_keys': event_key_count, 'changed_encounters': 0 if encounter_ids is None else encounter_ids.count(), 'affected_personnel': affected_count}

def _mmp_primary_location_summary(versions: Dict[str, int], config: MapMedicalPersonnelConfig) -> DataFrame:
    evidence = spark.table(config.location_evidence_table).where(F.col('SOURCE_PRESENT_IND') == F.lit(True)).where(F.col('LOCATION_RANK') == 1).select('PERSON_ID', F.col('LOC_NURSE_UNIT_CD').alias('inferred_primary_care_site_cd'), F.col('EVENT_COUNT').alias('primary_site_event_count'), F.col('LAST_EVENT_DT_TM').alias('primary_site_last_event_dt_tm'), F.col('TOP_COUNT_TIE_COUNT').alias('primary_site_location_tie_count'), F.col('EVIDENCE_ADC_UPDT').alias('LOCATION_EVIDENCE_ADC_UPDT'))
    lookup = _mmp_code_lookup(versions, config).select(F.col('CODE_VALUE').alias('_INFERRED_LOCATION_CODE'), F.coalesce(_mmp_nonblank(F.col('DISPLAY')), _mmp_nonblank(F.col('DESCRIPTION')), _mmp_nonblank(F.col('CDF_MEANING')), F.col('CODE_VALUE').cast('string')).alias('inferred_primary_care_site_name'), F.col('CODE_ADC_UPDT').alias('_INFERRED_LOCATION_CODE_ADC_UPDT'))
    return evidence.join(F.broadcast(lookup), F.col('inferred_primary_care_site_cd') == F.col('_INFERRED_LOCATION_CODE'), 'left').drop('_INFERRED_LOCATION_CODE')

def get_primary_care_locations(clinical_events: Optional[DataFrame]=None, encounters: Optional[DataFrame]=None, code_values: Optional[DataFrame]=None, config: MapMedicalPersonnelConfig=DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG) -> DataFrame:
    """
    Compatibility replacement.

    The original arguments are ignored. The function reads the incrementally
    maintained location-evidence table and therefore avoids rescanning and
    globally windowing raw clinical events.
    """
    if not _mmp_table_exists(config.location_evidence_table):
        raise RuntimeError(f'{config.location_evidence_table} is not initialized. Run deploy_map_medical_personnel_improvements() once.')
    versions = {config.code_value_source: _mmp_get_latest_delta_version(config.code_value_source)}
    return _mmp_primary_location_summary(versions, config).select('PERSON_ID', F.col('inferred_primary_care_site_cd').alias('primary_care_site_cd'), F.col('inferred_primary_care_site_name').alias('primary_care_site_name'))

def _mmp_prepare_prsnl_source(versions: Dict[str, int], config: MapMedicalPersonnelConfig) -> DataFrame:
    source = _mmp_read_snapshot(config.prsnl_source, versions).select(_mmp_checked_long(F.col('PERSON_ID'), 'PRSNL.PERSON_ID').alias('PERSON_ID'), F.col('PHYSICIAN_IND').cast('boolean').alias('PHYSICIAN_IND'), _mmp_checked_long(F.col('POSITION_CD'), 'PRSNL.POSITION_CD').alias('POSITION_CD'), _mmp_checked_long(F.col('PRSNL_TYPE_CD'), 'PRSNL.PRSNL_TYPE_CD').alias('PRSNL_TYPE_CD'), _mmp_checked_long(F.col('PHYSICIAN_STATUS_CD'), 'PRSNL.PHYSICIAN_STATUS_CD').alias('PHYSICIAN_STATUS_CD'), F.col('ACTIVE_IND').cast('long').alias('ACTIVE_IND'), _mmp_checked_long(F.col('ACTIVE_STATUS_CD'), 'PRSNL.ACTIVE_STATUS_CD').alias('ACTIVE_STATUS_CD'), _mmp_checked_long(F.col('DATA_STATUS_CD'), 'PRSNL.DATA_STATUS_CD').alias('DATA_STATUS_CD'), _mmp_checked_long(F.col('CONTRIBUTOR_SYSTEM_CD'), 'PRSNL.CONTRIBUTOR_SYSTEM_CD').alias('CONTRIBUTOR_SYSTEM_CD'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('BEG_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('END_EFFECTIVE_DT_TM'), F.col('CREATE_DT_TM').cast('timestamp').alias('CREATE_DT_TM'), F.col('UPDT_DT_TM').cast('timestamp').alias('UPDT_DT_TM'), F.col('EXTERNAL_IND').cast('boolean').alias('EXTERNAL_IND'), F.col('NAME_FULL_FORMATTED').cast('string').alias('NAME_FULL_FORMATTED'), _mmp_checked_long(F.col('PRIM_ASSIGN_LOC_CD'), 'PRSNL.PRIM_ASSIGN_LOC_CD').alias('PRIM_ASSIGN_LOC_CD'), F.col('UPDT_CNT').cast('long').alias('_UPDT_CNT'), F.col('LAST_UTC_TS').cast('timestamp').alias('_LAST_UTC_TS'), F.col('ADC_UPDT').cast('timestamp').alias('PRSNL_ADC_UPDT'))
    ranking = Window.partitionBy('PERSON_ID').orderBy(F.col('PRSNL_ADC_UPDT').desc_nulls_last(), F.col('_UPDT_CNT').desc_nulls_last(), F.col('UPDT_DT_TM').desc_nulls_last(), F.col('_LAST_UTC_TS').desc_nulls_last())
    return source.withColumn('_PRSNL_RANK', F.row_number().over(ranking)).where(F.col('_PRSNL_RANK') == 1).drop('_PRSNL_RANK', '_UPDT_CNT', '_LAST_UTC_TS')

def _mmp_build_target_snapshot(versions: Dict[str, int], run_id: str, run_timestamp: datetime, config: MapMedicalPersonnelConfig) -> DataFrame:
    prsnl = _mmp_prepare_prsnl_source(versions, config)
    lookup = _mmp_code_lookup(versions, config)
    result = prsnl
    code_joins = [('POSITION_CD', 'POSITION'), ('PRSNL_TYPE_CD', 'PRSNL_TYPE'), ('PHYSICIAN_STATUS_CD', 'PHYSICIAN_STATUS'), ('ACTIVE_STATUS_CD', 'ACTIVE_STATUS'), ('DATA_STATUS_CD', 'DATA_STATUS'), ('CONTRIBUTOR_SYSTEM_CD', 'CONTRIBUTOR_SYSTEM'), ('PRIM_ASSIGN_LOC_CD', 'PRIM_ASSIGN_LOC')]
    for source_column, prefix in code_joins:
        result = _mmp_join_code(result, lookup, source_column, prefix)
    groups = _mmp_group_summary(config)
    aliases = _mmp_alias_summary(config)
    locations = _mmp_primary_location_summary(versions, config)
    result = result.join(groups, 'PERSON_ID', 'left').join(aliases, 'PERSON_ID', 'left').join(locations, 'PERSON_ID', 'left')
    assigned_location = F.when(F.col('PRIM_ASSIGN_LOC_CD').isNotNull() & (F.col('PRIM_ASSIGN_LOC_CD') != 0), F.col('PRIM_ASSIGN_LOC_CD'))
    assigned_location_name = F.when(assigned_location.isNotNull(), _mmp_label_from_code('PRIM_ASSIGN_LOC'))
    inferred_location = F.when(F.col('inferred_primary_care_site_cd').isNotNull() & (F.col('inferred_primary_care_site_cd') != 0), F.col('inferred_primary_care_site_cd'))
    result = result.withColumn('position_name', _mmp_label_from_code('POSITION')).withColumn('position_display', F.col('POSITION_DISPLAY')).withColumn('position_description', F.col('POSITION_DESCRIPTION')).withColumn('position_cdf_meaning', F.col('POSITION_CDF_MEANING')).withColumn('position_code_active_ind', F.col('POSITION_CODE_ACTIVE_IND')).withColumn('prsnl_type_name', _mmp_label_from_code('PRSNL_TYPE')).withColumn('physician_status_name', _mmp_label_from_code('PHYSICIAN_STATUS')).withColumn('active_status_name', _mmp_label_from_code('ACTIVE_STATUS')).withColumn('data_status_name', _mmp_label_from_code('DATA_STATUS')).withColumn('contributor_system_name', _mmp_label_from_code('CONTRIBUTOR_SYSTEM')).withColumn('prim_assign_loc_name', assigned_location_name).withColumn('primary_care_site_cd', F.coalesce(assigned_location, inferred_location)).withColumn('primary_care_site_name', F.coalesce(assigned_location_name, F.col('inferred_primary_care_site_name'))).withColumn('primary_care_site_source', F.when(assigned_location.isNotNull(), F.lit('ASSIGNED_LOCATION')).when(inferred_location.isNotNull(), F.lit('EVENT_INFERENCE')).otherwise(F.lit('NONE'))).withColumn('HAS_CLINICAL_ROLE_IND', F.coalesce(F.col('PHYSICIAN_IND'), F.lit(False)) | F.col('POSITION_CD').isNotNull() & (F.col('POSITION_CD') != 0))
    for count_column in ('SRVCATEGORY_ASSIGNMENT_COUNT', 'SRVCATEGORY_ACTIVE_ASSIGNMENT_COUNT', 'SURGSPEC_ASSIGNMENT_COUNT', 'SURGSPEC_ACTIVE_ASSIGNMENT_COUNT', 'MEDSERVICE_ASSIGNMENT_COUNT', 'MEDSERVICE_ACTIVE_ASSIGNMENT_COUNT', 'NPI_ALIAS_COUNT', 'DOCNBR_ALIAS_COUNT', 'GDP_ALIAS_COUNT', 'EXTERNAL_PROVIDER_ID_COUNT'):
        result = result.withColumn(count_column, F.coalesce(F.col(count_column), F.lit(0)).cast('int'))
    code_update_columns = [f'{prefix}_CODE_ADC_UPDT' for _, prefix in code_joins] + ['_INFERRED_LOCATION_CODE_ADC_UPDT']
    result = result.withColumn('CODE_LOOKUP_ADC_UPDT', F.greatest(*[F.col(name) for name in code_update_columns])).withColumn('SOURCE_PRESENT_IND', F.lit(True)).withColumn('SOURCE_ABSENT_DETECTED_TS', F.lit(None).cast('timestamp')).withColumn('ADC_UPDT', F.greatest(F.col('PRSNL_ADC_UPDT'), F.col('GROUP_ADC_UPDT'), F.col('LOCATION_EVIDENCE_ADC_UPDT'), F.col('ALIAS_ADC_UPDT'), F.col('CODE_LOOKUP_ADC_UPDT'))).withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_UPDT_DT_TM', F.lit(run_timestamp).cast('timestamp'))
    result = _mmp_add_row_hash(result, schema_map_medical_personnel_v2, ['SOURCE_ABSENT_DETECTED_TS', 'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM'])
    return _mmp_align_to_schema(result, schema_map_medical_personnel_v2)
_map_medical_pending_versions: Optional[Dict[str, int]] = None
_map_medical_pending_run_id: Optional[str] = None
_map_medical_last_location_stats: Optional[Dict[str, int]] = None

def create_medical_personnel_mapping_incr(config: MapMedicalPersonnelConfig=DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG) -> DataFrame:
    """
    Drop-in replacement retaining the original function name.

    Group and alias sources are fully reconciled because they are compact.
    Clinical-event location state is refreshed from source-version CDF ranges.
    The returned DataFrame is a complete current personnel snapshot so source
    removals and changes in any small upstream source are reconciled correctly.
    """
    required_tables = (config.group_bridge_table, config.alias_bridge_table, config.event_state_table, config.location_evidence_table, config.target_table)
    missing_tables = [table for table in required_tables if not _mmp_table_exists(table)]
    if missing_tables:
        raise RuntimeError('Medical-personnel v2 is not deployed. Missing tables: ' + ', '.join(missing_tables) + '. Run deploy_map_medical_personnel_improvements() once.')
    expected_schemas = ((config.target_table, schema_map_medical_personnel_v2), (config.group_bridge_table, schema_map_medical_personnel_group), (config.alias_bridge_table, schema_map_medical_personnel_alias), (config.event_state_table, schema_map_medical_personnel_event_state), (config.location_evidence_table, schema_map_medical_personnel_location_evidence))
    incompatible = [table for table, schema in expected_schemas if not _mmp_schema_matches(table, schema)]
    if incompatible:
        raise RuntimeError('Medical-personnel v2 schema mismatch for: ' + ', '.join(incompatible) + '. Run deploy_map_medical_personnel_improvements() once.')
    checkpoints = get_map_medical_personnel_checkpoints(config)
    missing_checkpoints = [source for source in config.location_cdf_sources if source not in checkpoints]
    if missing_checkpoints:
        raise RuntimeError('Missing source-version checkpoints for: ' + ', '.join(missing_checkpoints) + '. Run deploy_map_medical_personnel_improvements() once.')
    versions = {table: _mmp_get_latest_delta_version(table) for table in config.all_sources}
    run_id = str(uuid4())
    run_timestamp = datetime.now(timezone.utc)
    location_stats = _mmp_refresh_location_support_incremental(checkpoints, versions, run_id, run_timestamp, config)
    _mmp_refresh_group_bridge(versions, run_id, run_timestamp, config)
    _mmp_refresh_alias_bridge(versions, run_id, run_timestamp, config)
    result = _mmp_build_target_snapshot(versions, run_id, run_timestamp, config)
    global _map_medical_pending_versions
    global _map_medical_pending_run_id
    global _map_medical_last_location_stats
    _map_medical_pending_versions = versions
    _map_medical_pending_run_id = run_id
    _map_medical_last_location_stats = location_stats
    return result

def update_map_medical_personnel_table(source_df: DataFrame, target_table: str=DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG.target_table, target_schema: T.StructType=schema_map_medical_personnel_v2, table_comment: str=MAP_MEDICAL_PERSONNEL_COMMENT, config: MapMedicalPersonnelConfig=DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG) -> Dict[str, object]:
    """Change-aware reconciliation without public presence-marker columns."""
    target_schema = bronze_contract_schema(target_table)
    if not _mmp_schema_matches(target_table, target_schema):
        raise RuntimeError(
            f'{target_table} does not match the single-layer contract. '
            'Run bronze_single_table_cutover before enabling this notebook.'
        )
    aligned = bronze_project_contract(
        _mmp_align_to_schema(source_df, target_schema),
        target_table,
    )
    source_count = _mmp_validate_unique(
        aligned, ['PERSON_ID'], 'map_medical_personnel source'
    )
    condition = 't.PERSON_ID <=> s.PERSON_ID'
    change_condition = _mmp_merge_change_condition(
        target_schema, ['PERSON_ID']
    )
    assignments = {
        column_name: F.col(f's.{column_name}')
        for column_name in aligned.columns
    }
    DeltaTable.forName(spark, target_table).alias('t').merge(
        aligned.alias('s'), condition
    ).whenMatchedUpdate(
        condition=change_condition, set=assignments
    ).whenNotMatchedInsert(values=assignments).execute()
    _mmp_configure_table(target_table, target_schema, table_comment, config)
    if _map_medical_pending_versions is not None and _map_medical_pending_run_id is not None:
        write_map_medical_personnel_checkpoints(
            _map_medical_pending_versions,
            _map_medical_pending_run_id,
            config,
        )
    target_metrics = spark.table(target_table).agg(
        F.count(F.lit(1)).alias('rows'),
        F.countDistinct('PERSON_ID').alias('distinct_person_ids'),
    ).first()
    if int(target_metrics['rows']) < source_count:
        raise ValueError(
            f"Target validation failed: target rows {target_metrics['rows']} "
            f"< source rows {source_count}"
        )
    return {
        'status': 'SUCCESS',
        'run_id': _map_medical_pending_run_id,
        'source_rows': source_count,
        'target_rows': int(target_metrics['rows']),
        'distinct_person_ids': int(target_metrics['distinct_person_ids']),
        'location_refresh': _map_medical_last_location_stats,
        'checkpoint_versions': _map_medical_pending_versions,
    }

def deploy_map_medical_personnel_improvements(config: MapMedicalPersonnelConfig=DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG, create_backup: bool=True, configure_source_retention: bool=True) -> Dict[str, object]:
    """
    Explicit one-time deployment and bootstrap.

    This is intentionally expensive once: it creates canonical event state from
    the full clinical-event snapshot. Subsequent Map Pipeline runs use CDF-driven
    subset refreshes and the compact support tables.
    """
    run_id = str(uuid4())
    run_timestamp = datetime.now(timezone.utc)
    if configure_source_retention:
        configure_map_medical_personnel_source_retention(config)
    versions = {table: _mmp_get_latest_delta_version(table) for table in config.all_sources}
    group_rows = _mmp_refresh_group_bridge(versions, run_id, run_timestamp, config)
    alias_rows = _mmp_refresh_alias_bridge(versions, run_id, run_timestamp, config)
    event_state = _mmp_build_full_event_state(versions, run_id, run_timestamp, config)
    _mmp_write_replacement(event_state, config.event_state_table, schema_map_medical_personnel_event_state)
    event_rows = _mmp_validate_unique(spark.table(config.event_state_table), ['EVENT_KEY'], 'medical personnel event state')
    _mmp_configure_table(config.event_state_table, schema_map_medical_personnel_event_state, MAP_MEDICAL_PERSONNEL_EVENT_STATE_COMMENT, config)
    _mmp_ensure_cluster_by(config.event_state_table, ['PERFORMED_PRSNL_ID', 'ENCNTR_ID', 'EVENT_KEY'])
    evidence = _mmp_build_location_evidence(spark.table(config.event_state_table), run_id, run_timestamp)
    _mmp_write_replacement(evidence, config.location_evidence_table, schema_map_medical_personnel_location_evidence)
    evidence_rows = _mmp_validate_unique(spark.table(config.location_evidence_table), ['PERSON_ID', 'LOC_NURSE_UNIT_CD'], 'medical personnel location evidence')
    _mmp_configure_table(config.location_evidence_table, schema_map_medical_personnel_location_evidence, MAP_MEDICAL_PERSONNEL_LOCATION_COMMENT, config)
    _mmp_ensure_cluster_by(config.location_evidence_table, ['PERSON_ID'])
    backup_table = None
    if create_backup and _mmp_table_exists(config.target_table):
        suffix = run_timestamp.strftime('%Y%m%d_%H%M%S')
        backup_table = f'{config.target_table}__pre_v2_{suffix}'
        spark.sql(f'CREATE TABLE {_mmp_sql_name(backup_table)} SHALLOW CLONE {_mmp_sql_name(config.target_table)}')
    target = _mmp_build_target_snapshot(versions, run_id, run_timestamp, config)
    _mmp_write_replacement(target, config.target_table, schema_map_medical_personnel_v2)
    target_rows = _mmp_validate_unique(spark.table(config.target_table), ['PERSON_ID'], 'map_medical_personnel deployment')
    _mmp_configure_table(config.target_table, schema_map_medical_personnel_v2, MAP_MEDICAL_PERSONNEL_COMMENT, config)
    write_map_medical_personnel_checkpoints(versions, run_id, config)
    return {'status': 'SUCCESS', 'run_id': run_id, 'target_rows': target_rows, 'group_bridge_rows': group_rows, 'alias_bridge_rows': alias_rows, 'event_state_rows': event_rows, 'location_evidence_rows': evidence_rows, 'backup_table': backup_table, 'checkpoint_versions': versions}

def rebuild_map_medical_personnel_v2(config: MapMedicalPersonnelConfig=DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG, create_backup: bool=True) -> Dict[str, object]:
    return deploy_map_medical_personnel_improvements(config=config, create_backup=create_backup, configure_source_retention=False)

def run_map_medical_personnel_update(config: MapMedicalPersonnelConfig=DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG) -> Dict[str, object]:
    source_df = create_medical_personnel_mapping_incr(config)
    return update_map_medical_personnel_table(source_df=source_df, target_table=config.target_table, target_schema=schema_map_medical_personnel_v2, table_comment=MAP_MEDICAL_PERSONNEL_COMMENT, config=config)
if '_map_medical_personnel_original_update_table' not in globals():
    _candidate_update_table = globals().get('update_table')
    if _candidate_update_table is not None and (not getattr(_candidate_update_table, '_is_map_medical_personnel_override', False)):
        _map_medical_personnel_original_update_table = _candidate_update_table
    else:
        _map_medical_personnel_original_update_table = None

def update_table(source_df, target_table, index_column, target_schema=None, table_comment=None, update_condition=None):
    """Drop-in wrapper matching Map Pipeline's existing signature."""
    if target_table.lower() == DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG.target_table.lower():
        return update_map_medical_personnel_table(source_df=source_df, target_table=target_table, target_schema=schema_map_medical_personnel_v2, table_comment=MAP_MEDICAL_PERSONNEL_COMMENT, config=DEFAULT_MAP_MEDICAL_PERSONNEL_CONFIG)
    if _map_medical_personnel_original_update_table is None:
        raise RuntimeError('The original Map Pipeline update_table function is unavailable. Load this replacement after Map Pipeline defines update_table, or call run_map_medical_personnel_update() directly.')
    return _map_medical_personnel_original_update_table(source_df=source_df, target_table=target_table, index_column=index_column, target_schema=target_schema, table_comment=table_comment, update_condition=update_condition)
update_table._is_map_medical_personnel_override = True
print('[map_medical_personnel] replacement functions loaded. No production data has been changed.')

try:
    _pipeline_run_recoverable('map_medical_personnel', _PIPELINE_FULL_REFRESH, lambda: run_map_medical_personnel_update(), lambda: rebuild_map_medical_personnel_v2(create_backup=False))
    _PIPELINE_UPDATED_TARGETS.extend(['4_prod.bronze.map_medical_personnel'])
    _pipeline_mark_component_complete('map_medical_personnel', ['4_prod.bronze.map_medical_personnel'])
    _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_medical_personnel'})
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

_pipeline_component_start("map_encounter")
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
from typing import Dict, List, Optional, Sequence, Tuple
import json
import uuid
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

MAP_ENCOUNTER_EVENT_SOURCE = '4_prod.raw.mill_clinical_event'
MAP_ENCOUNTER_EVENT_BOUNDS_TABLE = '4_prod.bronze.map_encounter_event_bounds'
MAP_ENCOUNTER_EVENT_BOUNDS_COMMENT = 'Per-encounter first and last clinical-event timestamps: the minimum and maximum of mill_clinical_event.CLINSIG_UPDT_DT_TM per ENCNTR_ID, restricted to plausible values from year 1950 up to the current timestamp. Maintained only to reproduce the legacy arrival and departure derivation published by map_encounter as ARRIVE_DT_TM_COMPAT and DEPART_DT_TM_COMPAT. Incremental runs maintain a monotone running minimum and maximum and cannot retract a bound when source events are deleted; a full refresh restores exactness.'
MAP_ENCOUNTER_EVENT_BOUNDS_WATERMARK_PROPERTY = 'pipeline.encounter_event_bounds.watermark'
MAP_ENCOUNTER_ORDER_SOURCE = '4_prod.raw.mill_orders'
MAP_ENCOUNTER_LOCATION_HISTORY_SOURCE = '4_prod.raw.mill_encntr_loc_hist'
MAP_ENCOUNTER_ACTIVITY_BOUNDS_TABLE = '4_prod.bronze.map_encounter_activity_bounds'
MAP_ENCOUNTER_ACTIVITY_BOUNDS_COMMENT = 'Per-encounter clinical-activity evidence at one row per ENCNTR_ID, holding only encounters that have some recorded activity. Clinical events are taken from mill_clinical_event.EVENT_END_DT_TM over current rows (VALID_UNTIL_DT_TM on or after 2099-01-01) whose RESULT_STATUS_CD is not Canceled, In Error or Not Done. Orders are taken from mill_orders.ORIG_ORDER_DT_TM excluding order templates and cancelled, voided-without-results and future orders. Ward occupancy is taken from mill_encntr_loc_hist rows with ACTIVE_IND = 1 and a nurse unit. Every column is recomputed in full on every run, so a bound retracts when its source rows are deleted or corrected; that is the difference from map_encounter_event_bounds, which merges monotonically and cannot retract. This table exists to support the evidence and best-guess arrival, departure and length-of-stay columns on map_encounter. It deliberately does not feed the legacy compatibility timestamps, which continue to read map_encounter_event_bounds.'
MAP_ENCOUNTER_ACTIVITY_ORDER_WATERMARK_PROPERTY = 'pipeline.encounter_activity_bounds.order_watermark'
MAP_ENCOUNTER_ACTIVITY_LOCATION_WATERMARK_PROPERTY = 'pipeline.encounter_activity_bounds.location_watermark'
MAP_ENCOUNTER_EVENT_VALID_UNTIL_FLOOR = '2099-01-01'
MAP_ENCOUNTER_EXCLUDED_RESULT_STATUS_CDS = (26, 28, 29, 30, 31, 36)
MAP_ENCOUNTER_TEMPLATE_ORDER_FLAGS = (1, 2)
MAP_ENCOUNTER_EXCLUDED_ORDER_STATUS_CDS = (2542, 2544, 2546)
MAP_ENCOUNTER_CONTEMPORANEITY_SECONDS = 7 * 24 * 60 * 60

@dataclass(frozen=True)
class MapEncounterConfig:
    source_table: str = '4_prod.raw.mill_encounter'
    code_value_table: str = '3_lookup.mill.mill_code_value'
    event_source_table: str = MAP_ENCOUNTER_EVENT_SOURCE
    event_bounds_table: str = MAP_ENCOUNTER_EVENT_BOUNDS_TABLE
    order_source_table: str = MAP_ENCOUNTER_ORDER_SOURCE
    location_history_table: str = MAP_ENCOUNTER_LOCATION_HISTORY_SOURCE
    activity_bounds_table: str = MAP_ENCOUNTER_ACTIVITY_BOUNDS_TABLE
    target_table: str = '4_prod.bronze.map_encounter'
    state_table: str = '6_mgmt.bronze.map_encounter_pipeline_state'
    audit_table: str = '6_mgmt.bronze.map_encounter_pipeline_audit'
    schema_version: str = '3.0.0'
    max_incremental_lookup_codes: int = 5000
MAP_ENCOUNTER_CONFIG = MapEncounterConfig()
map_encounter_comment = 'Curated encounter foundation at one row per source ENCNTR_ID, with no row filtering: every source encounter is present. Timestamps are published in five families. Raw: every source timestamp column is retained byte-for-byte, with SCHEDULED_ARRIVAL_DT_TM and SCHEDULED_DEPARTURE_DT_TM aliasing the estimated columns to name them for what they are, booking slots rather than attendance records. Evidence: one column per independent witness of attendance, aggregated from clinical events, orders and nurse-unit occupancy and summarised by ATTENDANCE_EVIDENCE and ATTENDANCE_WITNESS_COUNT. Best guess: ARRIVAL_DT_TM_BEST, DEPARTURE_DT_TM_BEST and LENGTH_OF_STAY_MINUTES, each labelled with the arm that supplied it and a confidence. Estimated, workflow and creation timestamps never supply a best guess and no departure is ever synthesised from an arrival, so a null here means the evidence is absent rather than that a value was invented; length of stay is emitted only where both endpoints are direct observations. Workflow: ENCOUNTER_COMPLETE_DT_TM_EFFECTIVE records when the encounter record was completed, which is not the end of the encounter. Compatibility: ARRIVE_DT_TM_COMPAT and DEPART_DT_TM_COMPAT reproduce the legacy clinical-event-bounded derivation unchanged, including its seven-day outlier rule and its midnight-after-arrival fallback, for consumers that must match historical outputs, and the two matching _FABRICATED_IND flags mark the legacy values that have no observational basis. Source lifecycle, classification, location and pipeline audit fields are retained.'
_PENDING_ENCOUNTER_PLAN: Dict[str, object] = {}
_PENDING_ENCOUNTER_CACHES: List[DataFrame] = []

def _field(name: str, data_type: T.DataType, comment: str, nullable: bool=True) -> T.StructField:
    return T.StructField(name, data_type, nullable, metadata={'comment': comment})
schema_map_encounter = T.StructType([_field('ENCNTR_ID', T.LongType(), 'Source encounter primary key.', nullable=False), _field('PERSON_ID', T.LongType(), 'Source person identifier.'), _field('ARRIVE_DT_TM', T.TimestampType(), 'Unchanged source actual arrival timestamp; null remains null.'), _field('DEPART_DT_TM', T.TimestampType(), 'Unchanged source actual departure timestamp; null remains null.'), _field('ENCNTR_CLASS_CD', T.LongType(), 'Source encounter class code.'), _field('encntr_class_desc', T.StringType(), 'Best available label for ENCNTR_CLASS_CD.'), _field('ENCNTR_TYPE_CD', T.LongType(), 'Source encounter type code.'), _field('encntr_type_desc', T.StringType(), 'Best available label for ENCNTR_TYPE_CD.'), _field('ENCNTR_STATUS_CD', T.LongType(), 'Source encounter status code.'), _field('encntr_status_desc', T.StringType(), 'Best available label for ENCNTR_STATUS_CD.'), _field('ADMIT_SRC_CD', T.LongType(), 'Source admission source code.'), _field('admit_src_desc', T.StringType(), 'Best available label for ADMIT_SRC_CD.'), _field('DISCH_TO_LOCTN_CD', T.LongType(), 'Source discharge destination code.'), _field('disch_loctn_desc', T.StringType(), 'Best available label for DISCH_TO_LOCTN_CD.'), _field('MED_SERVICE_CD', T.LongType(), 'Source medical service code.'), _field('med_service_desc', T.StringType(), 'Best available label for MED_SERVICE_CD.'), _field('LOC_NURSE_UNIT_CD', T.LongType(), 'Source current nurse-unit code.'), _field('nurse_unit_desc', T.StringType(), 'Best available label for LOC_NURSE_UNIT_CD.'), _field('SPECIALTY_UNIT_CD', T.LongType(), 'Source specialty-unit code, including source sentinel values.'), _field('specialty_unit_desc', T.StringType(), 'Best available label for SPECIALTY_UNIT_CD.'), _field('REG_PRSNL_ID', T.LongType(), 'Personnel identifier associated with registration.'), _field('ADC_UPDT', T.TimestampType(), 'Unchanged encounter-source ADC update timestamp; not used as a CDF checkpoint.'), _field('CREATE_DT_TM', T.TimestampType(), 'Source encounter row creation timestamp.'), _field('CREATE_PRSNL_ID', T.LongType(), 'Personnel identifier responsible for encounter creation.'), _field('PRE_REG_DT_TM', T.TimestampType(), 'Source pre-registration timestamp.'), _field('PRE_REG_PRSNL_ID', T.LongType(), 'Personnel identifier responsible for pre-registration.'), _field('REG_DT_TM', T.TimestampType(), 'Source registration or admission-process timestamp.'), _field('EST_ARRIVE_DT_TM', T.TimestampType(), 'Source estimated arrival timestamp.'), _field('EST_DEPART_DT_TM', T.TimestampType(), 'Source estimated departure timestamp.'), _field('DISCH_DT_TM', T.TimestampType(), 'Source actual discharge timestamp.'), _field('DISCH_PRSNL_ID', T.LongType(), 'Personnel identifier associated with discharge.'), _field('ENCNTR_COMPLETE_DT_TM', T.TimestampType(), 'Timestamp at which the source encounter became complete.'), _field('COMPLETE_REG_DT_TM', T.TimestampType(), 'Timestamp at which registration data collection became complete.'), _field('COMPLETE_REG_PRSNL_ID', T.LongType(), 'Personnel identifier completing registration.'), _field('INPATIENT_ADMIT_DT_TM', T.TimestampType(), 'Source inpatient admission timestamp.'), _field('TRIAGE_DT_TM', T.TimestampType(), 'Source triage timestamp.'), _field('ADMIT_DECISION_DT_TM', T.TimestampType(), 'Source admission decision timestamp.'), _field('INITIAL_CONTACT_DT_TM', T.TimestampType(), 'Source initial contact timestamp.'), _field('REFERRAL_RCVD_DT_TM', T.TimestampType(), 'Source referral-received timestamp.'), _field('SERVICE_DT_TM', T.TimestampType(), 'Source service timestamp.'), _field('SCHEDULED_ARRIVAL_DT_TM', T.TimestampType(), 'Alias of EST_ARRIVE_DT_TM, named for what it is. A booking slot, not an attendance record: 99.07% fall on a five-minute boundary, 100% carry zero seconds and 406,239 are future-dated. Never read it as an arrival.'), _field('SCHEDULED_DEPARTURE_DT_TM', T.TimestampType(), 'Alias of EST_DEPART_DT_TM, named for what it is. A booking slot, not a departure record; never read it as an attendance end.'), _field('FIRST_CLINICAL_EVENT_DT_TM', T.TimestampType(), 'Earliest EVENT_END_DT_TM among current, non-error clinical events for this encounter. Clinical time, which retrospective documentation backdates freely, so it witnesses attendance but does not time it.'), _field('LAST_CLINICAL_EVENT_DT_TM', T.TimestampType(), 'Latest EVENT_END_DT_TM among current, non-error clinical events for this encounter.'), _field('CLINICAL_EVENT_COUNT', T.LongType(), 'Count of current, non-error clinical events for this encounter; zero when there are none.'), _field('FIRST_CONTEMPORANEOUS_EVENT_DT_TM', T.TimestampType(), 'Earliest EVENT_END_DT_TM restricted to events whose clinical time is within seven days of their documentation time. The only event-derived column allowed to supply an arrival.'), _field('LAST_CONTEMPORANEOUS_EVENT_DT_TM', T.TimestampType(), 'Latest EVENT_END_DT_TM restricted to events whose clinical time is within seven days of their documentation time.'), _field('CONTEMPORANEOUS_EVENT_COUNT', T.LongType(), 'Count of contemporaneously documented clinical events; zero when there are none.'), _field('FIRST_ORDER_DT_TM', T.TimestampType(), 'Earliest ORIG_ORDER_DT_TM among active, non-template, non-cancelled orders for this encounter.'), _field('LAST_ORDER_DT_TM', T.TimestampType(), 'Latest ORIG_ORDER_DT_TM among active, non-template, non-cancelled orders for this encounter.'), _field('ORDER_COUNT', T.LongType(), 'Count of active, non-template, non-cancelled orders for this encounter; zero when there are none.'), _field('WARD_MOVE_COUNT', T.LongType(), 'Number of distinct nurse-unit occupancy intervals recorded in location history; zero when there are none.'), _field('WARD_OCCUPANCY_MINUTES', T.LongType(), 'Total minutes occupied across closed nurse-unit intervals in location history.'), _field('LAST_WARD_IN_DT_TM', T.TimestampType(), 'Start of the most recent nurse-unit occupancy interval.'), _field('LAST_WARD_OUT_DT_TM', T.TimestampType(), 'End of the most recent nurse-unit occupancy interval; null while that interval is still open.'), _field('LAST_WARD_STILL_OPEN_IND', T.BooleanType(), 'True when the most recent nurse-unit occupancy interval has no recorded end.'), _field('ATTENDANCE_EVIDENCE', T.StringType(), 'OBSERVED when at least one independent witness places the patient at this encounter; INFERRED when only non-contemporaneous clinical events exist; CANCELLED or FUTURE_SCHEDULED when there is no evidence at all and the encounter is cancelled or its booking slot is still in the future; otherwise NONE.'), _field('ATTENDANCE_WITNESS_COUNT', T.IntegerType(), 'How many of the five independent witnesses fired: source arrival, source registration, contemporaneous clinical events, orders, ward moves.'), _field('ARRIVAL_DT_TM_BEST', T.TimestampType(), 'Best available arrival: the first plausible arm of ARRIVE_DT_TM, then REG_DT_TM, then the earliest contemporaneous activity where attendance is observed. Null when nothing witnesses attendance. Booking slots and workflow stamps never contribute, so a null means the evidence is absent rather than that a value was invented.'), _field('ARRIVAL_METHOD', T.StringType(), 'Arm supplying ARRIVAL_DT_TM_BEST: ARRIVE_DT_TM, REG_DT_TM, FIRST_ACTIVITY or null.'), _field('ARRIVAL_CORROBORATED_IND', T.BooleanType(), 'True when an independent witness agrees with ARRIVAL_DT_TM_BEST to within 24 hours; null when there is no best arrival.'), _field('ARRIVAL_CONFIDENCE', T.StringType(), 'HIGH for a corroborated ARRIVE_DT_TM, MEDIUM for an uncorroborated ARRIVE_DT_TM or for any REG_DT_TM, LOW for FIRST_ACTIVITY, null when there is no best arrival.'), _field('DEPARTURE_DT_TM_BEST', T.TimestampType(), 'Best available departure: the first plausible arm of DEPART_DT_TM, then DISCH_DT_TM, then the end of the last closed ward interval, then the last recorded activity on a closed encounter. Null when nothing is recorded, when the winning candidate preceded the arrival, or when it is an administrative or fixed-window close rather than an observed departure; DEPARTURE_REJECTED_REASON says which.'), _field('DEPARTURE_METHOD', T.StringType(), 'Arm supplying DEPARTURE_DT_TM_BEST: DEPART_DT_TM, DISCH_DT_TM, WARD_OUT, LAST_ACTIVITY or null.'), _field('DEPARTURE_CORROBORATED_IND', T.BooleanType(), 'True when an independent witness agrees with DEPARTURE_DT_TM_BEST to within 24 hours; null when there is no best departure. Only clinical events and orders count as witnesses. Location history does not: it mirrors the encounter departure stamps to the minute, so counting it let a departure corroborate itself and carried 20,763,298 of the 37,421,948 original HIGH labels on nothing else.'), _field('DEPARTURE_CONFIDENCE', T.StringType(), 'HIGH for a corroborated DEPART_DT_TM, MEDIUM for an uncorroborated DEPART_DT_TM or for any DISCH_DT_TM or WARD_OUT, LOW for LAST_ACTIVITY, null when there is no best departure.'), _field('DEPART_DT_TM_ADMIN_CLOSE_IND', T.BooleanType(), 'True when the source DEPART_DT_TM or DISCH_DT_TM is an administrative close rather than an observed departure: the encounter was held open more than thirty days past its arrival and more than thirty days past its last clinical event or order. 75.1 per cent of the 7,121,656 rows this flags close between 03:00 and 05:59 against a 1.8 per cent base rate, clustering at 03:34-03:39 and 04:34-04:39, one nightly job seen in GMT and BST; the dominant mechanism is a two-year inactivity timeout, with the close landing a median 630 days after the last clinical event. Location history is never consulted to detect this, because its LAST_WARD_OUT_DT_TM equals DEPART_DT_TM for 100.00 per cent of flagged rows and mirrors the close rather than witnessing it. The raw DEPART_DT_TM and DISCH_DT_TM columns are untouched.'), _field('DEPART_DT_TM_FIXED_WINDOW_CLOSE_IND', T.BooleanType(), 'True when the source DEPART_DT_TM or DISCH_DT_TM is a fixed-window close rather than an observed departure: the encounter closed between fifty-nine and sixty-one days after its arrival, between 03:00 and 06:59. This is a second job, distinct from the inactivity timeout behind DEPART_DT_TM_ADMIN_CLOSE_IND. It closes on a fixed window measured from the arrival, so clinical activity continues right up to it and the gap from the last activity is only about eight days, which is why no inactivity threshold can reach it. Outpatient Services is the population: 94.2 per cent of the stays that type still published after the inactivity rule fall inside the band and 99.98 per cent of those close between 03:00 and 06:59, against a 4.21 per cent overnight base rate across all published stays. That window is an hour later than the inactivity job and its boundary is sharp: hour 6 carries 47,796 of these stays and hours 2 and 7 carry none, while everything from hour 8 onward is spread across the working day and is genuine. The same test fires on 46 rows in 30.9 million across every other encounter type combined, so it is written against the signature rather than against an encounter-type allowlist. The raw DEPART_DT_TM and DISCH_DT_TM columns are untouched.'), _field('DEPARTURE_REJECTED_REASON', T.StringType(), 'Why the winning departure arm was refused. BEFORE_ARRIVAL when it produced a timestamp earlier than ARRIVAL_DT_TM_BEST; ADMINISTRATIVE_CLOSE when DEPART_DT_TM_ADMIN_CLOSE_IND is true; FIXED_WINDOW_CLOSE when DEPART_DT_TM_FIXED_WINDOW_CLOSE_IND is true. In every case the departure, its method and any length of stay are published as null rather than silently asserted, and the source columns still carry the value.'), _field('LENGTH_OF_STAY_MINUTES', T.LongType(), 'Minutes between ARRIVAL_DT_TM_BEST and DEPARTURE_DT_TM_BEST, emitted only when both endpoints came from a direct observation arm. Activity-derived endpoints are accurate to about six weeks and are excluded deliberately, so this column can never be negative.'), _field('LENGTH_OF_STAY_METHOD', T.StringType(), 'The two contributing arms joined by _TO_ when a length of stay is emitted; INSUFFICIENT_PRECISION when both endpoints exist but at least one is activity-derived; INCOMPLETE_ENDPOINTS when an endpoint is missing.'), _field('LENGTH_OF_STAY_CONFIDENCE', T.StringType(), 'HIGH when both endpoint confidences are HIGH, otherwise MEDIUM; null when no length of stay is emitted.'), _field('WARD_LENGTH_OF_STAY_MINUTES', T.LongType(), 'Total closed nurse-unit occupancy in minutes, published only where the last ward interval is closed. A different measure from LENGTH_OF_STAY_MINUTES: bed occupancy rather than encounter duration.'), _field('ENCOUNTER_COMPLETE_DT_TM_EFFECTIVE', T.TimestampType(), 'Workflow completion timestamp taken from ENCNTR_COMPLETE_DT_TM, ignored when it carries the 2100-12-31 open-ended sentinel. This is when the encounter record was completed, not when the patient left.'), _field('ARRIVE_DT_TM_COMPAT', T.TimestampType(), 'Legacy-compatible arrival, reproducing the historical derivation exactly: the first clinical-event timestamp when it is more than seven days after the source arrival, otherwise ARRIVE_DT_TM, then EST_ARRIVE_DT_TM, then the first clinical-event timestamp. There is no REG_DT_TM arm. Degrades to the raw and estimated values alone when the event-bounds table is unavailable.'), _field('DEPART_DT_TM_COMPAT', T.TimestampType(), 'Legacy-compatible departure, reproducing the historical derivation exactly: the last clinical-event timestamp when the source departure is more than seven days after it, otherwise DEPART_DT_TM, then the last clinical-event timestamp, then midnight on the day after ARRIVE_DT_TM_COMPAT. Degrades to the raw value alone when the event-bounds table is unavailable.'), _field('ARRIVE_DT_TM_COMPAT_FABRICATED_IND', T.BooleanType(), 'True when ARRIVE_DT_TM_COMPAT was taken from the estimated-arrival booking slot and therefore has no observational basis. Consumers migrating off the compat columns should exclude these rows rather than trust the timestamp.'), _field('DEPART_DT_TM_COMPAT_FABRICATED_IND', T.BooleanType(), 'True when DEPART_DT_TM_COMPAT is the legacy synthetic midnight-after-arrival value and therefore has no observational basis.'), _field('RAW_DEPART_BEFORE_ARRIVE_IND', T.BooleanType(), 'True when non-null raw departure precedes non-null raw arrival.'), _field('COMPAT_END_BEFORE_START_IND', T.BooleanType(), 'True when DEPART_DT_TM_COMPAT precedes ARRIVE_DT_TM_COMPAT.'), _field('ENCNTR_TYPE_CLASS_CD', T.LongType(), 'Source broad encounter type-class code.'), _field('encntr_type_class_desc', T.StringType(), 'Best available label for ENCNTR_TYPE_CLASS_CD.'), _field('ADMIT_TYPE_CD', T.LongType(), 'Source admission type code.'), _field('admit_type_desc', T.StringType(), 'Best available label for ADMIT_TYPE_CD.'), _field('ADMIT_MODE_CD', T.LongType(), 'Source method-of-arrival code.'), _field('admit_mode_desc', T.StringType(), 'Best available label for ADMIT_MODE_CD.'), _field('DISCH_DISPOSITION_CD', T.LongType(), 'Source discharge disposition code.'), _field('disch_disposition_desc', T.StringType(), 'Best available label for DISCH_DISPOSITION_CD.'), _field('SERVICE_CATEGORY_CD', T.LongType(), 'Source current service-category code.'), _field('service_category_desc', T.StringType(), 'Best available label for SERVICE_CATEGORY_CD.'), _field('PROGRAM_SERVICE_CD', T.LongType(), 'Source program-service code, including source sentinel values.'), _field('FINANCIAL_CLASS_CD', T.LongType(), 'Source financial-class code, including source sentinel values.'), _field('REFERRAL_SOURCE_CD', T.LongType(), 'Source referral-source code.'), _field('referral_source_desc', T.StringType(), 'Best available label for REFERRAL_SOURCE_CD.'), _field('ED_REFERRAL_SOURCE_CD', T.LongType(), 'Source emergency-department referral-source code.'), _field('ed_referral_source_desc', T.StringType(), 'Best available label for ED_REFERRAL_SOURCE_CD.'), _field('ORDER_SOURCE_CD', T.LongType(), 'Source order-source code.'), _field('CONTRIBUTOR_SYSTEM_CD', T.LongType(), 'Source contributor-system code.'), _field('contributor_system_desc', T.StringType(), 'Best available label for CONTRIBUTOR_SYSTEM_CD.'), _field('LOCATION_CD', T.LongType(), 'Source current lowest-level permanent location code.'), _field('LOC_FACILITY_CD', T.LongType(), 'Source current facility code.'), _field('loc_facility_desc', T.StringType(), 'Best available label for LOC_FACILITY_CD.'), _field('LOC_BUILDING_CD', T.LongType(), 'Source current building code.'), _field('LOC_ROOM_CD', T.LongType(), 'Source current room code.'), _field('LOC_BED_CD', T.LongType(), 'Source current bed code.'), _field('ORGANIZATION_ID', T.LongType(), 'Source organization identifier for the encounter.'), _field('SERVICE_PROVIDER_ORG_ID', T.LongType(), 'Source organization primarily responsible for the encounter.'), _field('PLACE_OF_SVC_ORG_ID', T.LongType(), 'Source place-of-service organization identifier.'), _field('ENCNTR_FINANCIAL_ID', T.LongType(), 'Source encounter-financial identifier.'), _field('REASON_FOR_VISIT', T.StringType(), 'Unchanged source reason-for-visit text.'), _field('EXTERNAL_IND', T.LongType(), 'Source external-encounter indicator.'), _field('ACTIVE_IND', T.LongType(), 'Source row active indicator; no filtering is applied.'), _field('ACTIVE_STATUS_CD', T.LongType(), 'Source row active-status code.'), _field('active_status_desc', T.StringType(), 'Best available label for ACTIVE_STATUS_CD.'), _field('ACTIVE_STATUS_DT_TM', T.TimestampType(), 'Timestamp at which source active status was set.'), _field('ACTIVE_STATUS_PRSNL_ID', T.LongType(), 'Personnel identifier associated with active-status change.'), _field('BEG_EFFECTIVE_DT_TM', T.TimestampType(), 'Source row effective-start timestamp.'), _field('END_EFFECTIVE_DT_TM', T.TimestampType(), 'Source row effective-end timestamp.'), _field('DATA_STATUS_CD', T.LongType(), 'Source row data-status code.'), _field('data_status_desc', T.StringType(), 'Best available label for DATA_STATUS_CD.'), _field('DATA_STATUS_DT_TM', T.TimestampType(), 'Timestamp at which source data status was set.'), _field('DATA_STATUS_PRSNL_ID', T.LongType(), 'Personnel identifier associated with data-status change.'), _field('UPDT_CNT', T.LongType(), 'Source application update counter.'), _field('UPDT_DT_TM', T.TimestampType(), 'Source application update timestamp.'), _field('UPDT_ID', T.LongType(), 'Source application updater identifier.'), _field('UPDT_TASK', T.LongType(), 'Source application update task.'), _field('UPDT_APPLCTX', T.LongType(), 'Source application update context.'), _field('LAST_UTC_TS', T.TimestampType(), 'Source last-UTC timestamp.'), _field('INST_ID', T.LongType(), 'Source instance identifier.'), _field('TRUST', T.StringType(), 'Source Trust value; no Trust filter is applied.'), _field('TXN_ID_TEXT', T.StringType(), 'Source transaction identifier text.'), _field('CODE_LOOKUP_ADC_UPDT', T.TimestampType(), 'Greatest ADC_UPDT among code-value rows used by this encounter.'), _field('TRIGGER_SOURCES', T.StringType(), 'Comma-separated upstream sources that caused the row rebuild.'), _field('ENCOUNTER_CDF_COMMIT_VERSION', T.LongType(), 'Latest encounter CDF commit version triggering the rebuild.'), _field('ENCOUNTER_CDF_COMMIT_TIMESTAMP', T.TimestampType(), 'Latest encounter CDF commit timestamp triggering the rebuild.'), _field('ENCOUNTER_CDF_CHANGE_TYPE', T.StringType(), 'Latest encounter CDF change type triggering the rebuild.'), _field('ROW_HASH', T.LongType(), 'Stable hash of source-derived row content used to avoid unchanged rewrites.'), _field('PIPELINE_RUN_ID', T.StringType(), 'Pipeline run identifier that last materially changed the target row.'), _field('PIPELINE_UPDT_DT_TM', T.TimestampType(), 'Pipeline timestamp at which the target row last materially changed.')])
CODE_DESCRIPTION_COLUMNS: Dict[str, str] = {'ENCNTR_CLASS_CD': 'encntr_class_desc', 'ENCNTR_TYPE_CD': 'encntr_type_desc', 'ENCNTR_STATUS_CD': 'encntr_status_desc', 'ADMIT_SRC_CD': 'admit_src_desc', 'DISCH_TO_LOCTN_CD': 'disch_loctn_desc', 'MED_SERVICE_CD': 'med_service_desc', 'LOC_NURSE_UNIT_CD': 'nurse_unit_desc', 'SPECIALTY_UNIT_CD': 'specialty_unit_desc', 'ENCNTR_TYPE_CLASS_CD': 'encntr_type_class_desc', 'ADMIT_TYPE_CD': 'admit_type_desc', 'ADMIT_MODE_CD': 'admit_mode_desc', 'DISCH_DISPOSITION_CD': 'disch_disposition_desc', 'SERVICE_CATEGORY_CD': 'service_category_desc', 'REFERRAL_SOURCE_CD': 'referral_source_desc', 'ED_REFERRAL_SOURCE_CD': 'ed_referral_source_desc', 'CONTRIBUTOR_SYSTEM_CD': 'contributor_system_desc', 'LOC_FACILITY_CD': 'loc_facility_desc', 'ACTIVE_STATUS_CD': 'active_status_desc', 'DATA_STATUS_CD': 'data_status_desc'}
LONG_SOURCE_COLUMNS: Sequence[str] = ('ENCNTR_ID', 'PERSON_ID', 'ENCNTR_CLASS_CD', 'ENCNTR_TYPE_CD', 'ENCNTR_STATUS_CD', 'ADMIT_SRC_CD', 'DISCH_TO_LOCTN_CD', 'MED_SERVICE_CD', 'LOC_NURSE_UNIT_CD', 'SPECIALTY_UNIT_CD', 'REG_PRSNL_ID', 'CREATE_PRSNL_ID', 'PRE_REG_PRSNL_ID', 'DISCH_PRSNL_ID', 'COMPLETE_REG_PRSNL_ID', 'ENCNTR_TYPE_CLASS_CD', 'ADMIT_TYPE_CD', 'ADMIT_MODE_CD', 'DISCH_DISPOSITION_CD', 'SERVICE_CATEGORY_CD', 'PROGRAM_SERVICE_CD', 'FINANCIAL_CLASS_CD', 'REFERRAL_SOURCE_CD', 'ED_REFERRAL_SOURCE_CD', 'ORDER_SOURCE_CD', 'CONTRIBUTOR_SYSTEM_CD', 'LOCATION_CD', 'LOC_FACILITY_CD', 'LOC_BUILDING_CD', 'LOC_ROOM_CD', 'LOC_BED_CD', 'ORGANIZATION_ID', 'SERVICE_PROVIDER_ORG_ID', 'PLACE_OF_SVC_ORG_ID', 'ENCNTR_FINANCIAL_ID', 'EXTERNAL_IND', 'ACTIVE_IND', 'ACTIVE_STATUS_CD', 'ACTIVE_STATUS_PRSNL_ID', 'DATA_STATUS_CD', 'DATA_STATUS_PRSNL_ID', 'UPDT_CNT', 'UPDT_ID', 'UPDT_TASK', 'UPDT_APPLCTX', 'INST_ID')
TIMESTAMP_SOURCE_COLUMNS: Sequence[str] = ('ARRIVE_DT_TM', 'DEPART_DT_TM', 'ADC_UPDT', 'CREATE_DT_TM', 'PRE_REG_DT_TM', 'REG_DT_TM', 'EST_ARRIVE_DT_TM', 'EST_DEPART_DT_TM', 'DISCH_DT_TM', 'ENCNTR_COMPLETE_DT_TM', 'COMPLETE_REG_DT_TM', 'INPATIENT_ADMIT_DT_TM', 'TRIAGE_DT_TM', 'ADMIT_DECISION_DT_TM', 'INITIAL_CONTACT_DT_TM', 'REFERRAL_RCVD_DT_TM', 'SERVICE_DT_TM', 'ACTIVE_STATUS_DT_TM', 'BEG_EFFECTIVE_DT_TM', 'END_EFFECTIVE_DT_TM', 'DATA_STATUS_DT_TM', 'UPDT_DT_TM', 'LAST_UTC_TS')
STRING_SOURCE_COLUMNS: Sequence[str] = ('REASON_FOR_VISIT', 'TXN_ID_TEXT')
CODE_TRIGGER_COLUMNS: Sequence[str] = tuple(CODE_DESCRIPTION_COLUMNS.keys())
_VOLATILE_HASH_COLUMNS = {'TRIGGER_SOURCES', 'ENCOUNTER_CDF_COMMIT_VERSION', 'ENCOUNTER_CDF_COMMIT_TIMESTAMP', 'ENCOUNTER_CDF_CHANGE_TYPE', 'ROW_HASH', 'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM'}

def _sql_identifier(table_name: str) -> str:
    tick = chr(96)
    return '.'.join((f'{tick}{part.replace(tick, tick + tick)}{tick}' for part in table_name.split('.')))

def _sql_column(column_name: str) -> str:
    tick = chr(96)
    return f'{tick}{column_name.replace(tick, tick + tick)}{tick}'

def _escape_sql_string(value: str) -> str:
    return value.replace('\\', '\\\\').replace("'", "''")

def _table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _latest_delta_version(table_name: str) -> int:
    row = spark.sql(f'DESCRIBE HISTORY {_sql_identifier(table_name)} LIMIT 1').select('version').first()
    if row is None:
        raise RuntimeError(f'No Delta history returned for {table_name}')
    return int(row['version'])

def _read_snapshot(table_name: str, version: int) -> DataFrame:
    return _m10_hardening_read_pinned_snapshot(table_name, int(version))

def _read_cdf(table_name: str, starting_version: int, ending_version: int) -> Optional[DataFrame]:
    if starting_version > ending_version:
        return None
    try:
        changes = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', int(starting_version)).option('endingVersion', int(ending_version)).table(table_name)
        changes.limit(1).collect()
        return changes
    except Exception as exc:
        raise RuntimeError(f'CDF could not be read for {table_name} from version {starting_version} through {ending_version}. No timestamp fallback was used because that can silently lose deletes and late-arriving rows. Run run_map_encounter_update(force_full_rebuild=True).') from exc

def _capture_source_versions(config: MapEncounterConfig) -> Dict[str, int]:
    return {config.source_table: _latest_delta_version(config.source_table), config.code_value_table: _latest_delta_version(config.code_value_table)}

def _checked_long(column: F.Column, label: str, required: bool=False) -> F.Column:
    as_double = column.cast('double')
    null_result = F.lit(None) if required else F.lit(None).cast('long')
    return F.when(column.isNull(), null_result).when(F.isnan(as_double), F.lit(None)).when(as_double != F.floor(as_double), F.lit(None)).when(F.abs(as_double) > F.lit(9007199254740991), F.lit(None)).otherwise(column.cast('long'))

def _nonblank(column: F.Column) -> F.Column:
    return F.when(column.isNotNull() & (F.length(F.trim(column.cast('string'))) > 0), F.trim(column.cast('string')))

def _schema_select(df: DataFrame, schema: T.StructType) -> DataFrame:
    expressions = []
    for field in schema.fields:
        if field.name in df.columns:
            expression = F.col(field.name).cast(field.dataType)
        else:
            expression = F.lit(None).cast(field.dataType)
        expressions.append(expression.alias(field.name, metadata=field.metadata))
    return df.select(*expressions)

def _is_empty(df: Optional[DataFrame]) -> bool:
    return df is None or df.limit(1).count() == 0

def _register_cache(df: DataFrame) -> DataFrame:
    cached = _persist_stage_materialize(df)
    _PENDING_ENCOUNTER_CACHES.append(cached)
    return cached

def _release_caches() -> None:
    while _PENDING_ENCOUNTER_CACHES:
        df = _PENDING_ENCOUNTER_CACHES.pop()
        try:
            _persist_stage_drop(df)
        except Exception:
            pass

def _ensure_control_tables(config: MapEncounterConfig) -> None:
    spark.sql(f"\n        CREATE TABLE IF NOT EXISTS {_sql_identifier(config.state_table)} (\n            source_table STRING NOT NULL,\n            last_processed_version BIGINT NOT NULL,\n            last_processed_at TIMESTAMP NOT NULL,\n            last_run_id STRING NOT NULL\n        )\n        USING DELTA\n        TBLPROPERTIES (\n            'delta.enableChangeDataFeed' = 'true'\n        )\n    ")
    spark.sql(f'\n        CREATE TABLE IF NOT EXISTS {_sql_identifier(config.audit_table)} (\n            run_id STRING NOT NULL,\n            event_timestamp TIMESTAMP NOT NULL,\n            status STRING NOT NULL,\n            mode STRING NOT NULL,\n            message STRING,\n            source_versions_json STRING,\n            metrics_json STRING\n        )\n        USING DELTA\n    ')

def _read_pipeline_state(config: MapEncounterConfig) -> Dict[str, int]:
    if not _table_exists(config.state_table):
        return {}
    rows = spark.table(config.state_table).filter(F.col('source_table').isin(config.source_table, config.code_value_table, config.event_source_table, config.order_source_table, config.location_history_table)).select('source_table', 'last_processed_version').collect()
    return {row['source_table']: int(row['last_processed_version']) for row in rows if row['last_processed_version'] is not None}

def _commit_pipeline_state(config: MapEncounterConfig, source_versions: Dict[str, int], run_id: str, completed_at: datetime) -> None:
    rows = [(source, int(version), completed_at, run_id) for source, version in source_versions.items()]
    schema = T.StructType([T.StructField('source_table', T.StringType(), False), T.StructField('last_processed_version', T.LongType(), False), T.StructField('last_processed_at', T.TimestampType(), False), T.StructField('last_run_id', T.StringType(), False)])
    updates = spark.createDataFrame(rows, schema)
    DeltaTable.forName(spark, config.state_table).alias('t').merge(updates.alias('s'), 't.source_table = s.source_table').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def _write_audit_event(config: MapEncounterConfig, run_id: str, status: str, mode: str, message: str, source_versions: Optional[Dict[str, int]]=None, metrics: Optional[Dict[str, object]]=None) -> None:
    try:
        row = [(run_id, datetime.now(timezone.utc), status, mode, message[:8000], json.dumps(source_versions or {}, sort_keys=True, default=str), json.dumps(metrics or {}, sort_keys=True, default=str))]
        schema = T.StructType([T.StructField('run_id', T.StringType(), False), T.StructField('event_timestamp', T.TimestampType(), False), T.StructField('status', T.StringType(), False), T.StructField('mode', T.StringType(), False), T.StructField('message', T.StringType(), True), T.StructField('source_versions_json', T.StringType(), True), T.StructField('metrics_json', T.StringType(), True)])
        spark.createDataFrame(row, schema).write.mode('append').saveAsTable(config.audit_table)
    except Exception as audit_exc:
        print(f'[WARN] Could not write map_encounter audit event: {audit_exc}')

def _get_table_properties(table_name: str) -> Dict[str, str]:
    if not _table_exists(table_name):
        return {}
    return {row['key']: row['value'] for row in spark.sql(f'SHOW TBLPROPERTIES {_sql_identifier(table_name)}').collect()}

def _set_table_properties_if_changed(table_name: str, properties: Dict[str, str]) -> None:
    current = _get_table_properties(table_name)
    changed = {key: value for key, value in properties.items() if current.get(key) != value}
    if not changed:
        return
    assignments = ', '.join((f"'{_escape_sql_string(key)}' = '{_escape_sql_string(value)}'" for key, value in changed.items()))
    spark.sql(f'ALTER TABLE {_sql_identifier(table_name)} SET TBLPROPERTIES ({assignments})')

def _table_description(table_name: str) -> Optional[str]:
    if not _table_exists(table_name):
        return None
    try:
        return spark.catalog.getTable(table_name).description
    except Exception:
        return None

def _set_table_comment_if_changed(table_name: str, comment: str) -> None:
    if _table_description(table_name) == comment:
        return
    spark.sql(f"COMMENT ON TABLE {_sql_identifier(table_name)} IS '{_escape_sql_string(comment)}'")

def _apply_column_comments_if_changed(table_name: str, schema: T.StructType) -> None:
    current = spark.table(table_name).schema
    current_comments = {field.name: field.metadata.get('comment', '') for field in current.fields}
    for field in schema.fields:
        desired = field.metadata.get('comment', '')
        if not desired or current_comments.get(field.name) == desired:
            continue
        spark.sql(f"ALTER TABLE {_sql_identifier(table_name)} ALTER COLUMN {_sql_column(field.name)} COMMENT '{_escape_sql_string(desired)}'")

def _target_properties(config: MapEncounterConfig) -> Dict[str, str]:
    return {'delta.enableChangeDataFeed': 'true', 'delta.enableDeletionVectors': 'true', 'delta.enableRowTracking': 'true', 'delta.autoOptimize.optimizeWrite': 'true', 'delta.autoOptimize.autoCompact': 'true', 'delta.deletedFileRetentionDuration': 'interval 30 days', 'pipeline.map_encounter.schema_version': config.schema_version, 'pipeline.map_encounter.date_semantics': 'raw_plus_evidence_plus_best_guess_plus_legacy_compat', 'pipeline.map_encounter.clinical_event_dependency': 'event_bounds_and_activity_bounds', 'pipeline.map_encounter.event_bounds_table': config.event_bounds_table, 'pipeline.map_encounter.activity_bounds_table': config.activity_bounds_table, 'pipeline.map_encounter.cdf_checkpoint': 'delta_commit_version'}

def _set_target_metadata(config: MapEncounterConfig) -> None:
    _set_table_properties_if_changed(config.target_table, _target_properties(config))
    _set_table_comment_if_changed(config.target_table, map_encounter_comment)
    # The build schema remains intentionally wide so source-derived fields can
    # participate in encounter logic. Persist metadata only for the retained
    # single-layer Bronze contract; cut columns no longer exist on the target.
    _apply_column_comments_if_changed(
        config.target_table,
        bronze_contract_schema(config.target_table),
    )

def _target_requires_full_rebuild(config: MapEncounterConfig, state: Dict[str, int]) -> Tuple[bool, str]:
    if not _table_exists(config.target_table):
        return (True, 'target table does not exist')
    current_properties = _get_table_properties(config.target_table)
    if current_properties.get('pipeline.map_encounter.schema_version') != config.schema_version:
        return (True, 'schema-version property is absent or outdated')
    expected = {
        field.name.lower(): field.dataType.simpleString()
        for field in bronze_contract_schema(config.target_table).fields
    }
    current = {field.name.lower(): field.dataType.simpleString() for field in spark.table(config.target_table).schema.fields}
    if current != expected:
        return (True, 'target schema does not exactly match the replacement schema')
    required_state = {config.source_table, config.code_value_table}
    if not required_state.issubset(state):
        return (True, 'source-version state is incomplete')
    return (False, 'incremental state is compatible')

def create_code_lookup_encounter(code_values: DataFrame, description_alias: str) -> DataFrame:
    """
    Compatibility replacement for the original helper.

    It uses DESCRIPTION, then DISPLAY, then CDF_MEANING. Code zero remains code zero
    and receives no invented label when no code-value row exists.
    """
    return code_values.select(F.col('CODE_VALUE').cast('long').alias('CODE_VALUE'), F.coalesce(_nonblank(F.col('DESCRIPTION')), _nonblank(F.col('DISPLAY')), _nonblank(F.col('CDF_MEANING'))).alias(description_alias))

def _prepare_code_lookup(config: MapEncounterConfig, version: int) -> DataFrame:
    source = _read_snapshot(config.code_value_table, version)
    prepared = source.select(_checked_long(F.col('CODE_VALUE'), 'CODE_VALUE', required=True).alias('CODE_VALUE'), F.coalesce(_nonblank(F.col('DESCRIPTION')), _nonblank(F.col('DISPLAY')), _nonblank(F.col('CDF_MEANING'))).alias('CODE_LABEL'), F.col('ADC_UPDT').alias('LOOKUP_ADC_UPDT'), _checked_long(F.col('UPDT_CNT'), 'CODE_VALUE.UPDT_CNT').alias('_LOOKUP_UPDT_CNT'), F.col('UPDT_DT_TM').alias('_LOOKUP_UPDT_DT_TM'))
    window = Window.partitionBy('CODE_VALUE').orderBy(F.col('LOOKUP_ADC_UPDT').desc_nulls_last(), F.col('_LOOKUP_UPDT_CNT').desc_nulls_last(), F.col('_LOOKUP_UPDT_DT_TM').desc_nulls_last())
    return prepared.withColumn('_LOOKUP_RANK', F.row_number().over(window)).filter(F.col('_LOOKUP_RANK') == 1).drop('_LOOKUP_RANK', '_LOOKUP_UPDT_CNT', '_LOOKUP_UPDT_DT_TM')

def _prepare_encounter_source(raw: DataFrame) -> DataFrame:
    expressions: List[F.Column] = []
    for column_name in LONG_SOURCE_COLUMNS:
        expressions.append(_checked_long(F.col(column_name), f'MILL_ENCOUNTER.{column_name}', required=column_name == 'ENCNTR_ID').alias(column_name))
    for column_name in TIMESTAMP_SOURCE_COLUMNS:
        expressions.append(F.col(column_name).alias(column_name))
    for column_name in STRING_SOURCE_COLUMNS:
        expressions.append(F.col(column_name).alias(column_name))
    expressions.append(F.col('Trust').alias('TRUST'))
    prepared = raw.select(*expressions)
    latest_window = Window.partitionBy('ENCNTR_ID').orderBy(F.col('ADC_UPDT').desc_nulls_last(), F.col('UPDT_CNT').desc_nulls_last(), F.col('UPDT_DT_TM').desc_nulls_last(), F.col('LAST_UTC_TS').desc_nulls_last())
    return prepared.withColumn('_SOURCE_ROW_NUMBER', F.row_number().over(latest_window)).filter(F.col('_SOURCE_ROW_NUMBER') == 1).drop('_SOURCE_ROW_NUMBER')

def _enrich_code_descriptions(encounters: DataFrame, lookup: DataFrame) -> DataFrame:
    result = encounters
    update_columns: List[str] = []
    for index, (code_column, description_column) in enumerate(CODE_DESCRIPTION_COLUMNS.items()):
        value_column = f'_LOOKUP_CODE_{index}'
        update_column = f'_LOOKUP_ADC_UPDT_{index}'
        projected = lookup.select(F.col('CODE_VALUE').alias(value_column), F.col('CODE_LABEL').alias(description_column), F.col('LOOKUP_ADC_UPDT').alias(update_column))
        result = result.join(F.broadcast(projected), F.col(code_column) == F.col(value_column), 'left').drop(value_column)
        update_columns.append(update_column)
    result = result.withColumn('CODE_LOOKUP_ADC_UPDT', F.greatest(*[F.col(column) for column in update_columns]))
    return result.drop(*update_columns)

_ENCOUNTER_EVENT_BOUNDS_SCHEMA = T.StructType([T.StructField('ENCNTR_ID', T.LongType(), True, metadata={'comment': 'Millennium encounter identifier.'}), T.StructField('FIRST_EVENT_DT_TM', T.TimestampType(), True, metadata={'comment': 'Earliest plausible mill_clinical_event.CLINSIG_UPDT_DT_TM recorded against the encounter.'}), T.StructField('LAST_EVENT_DT_TM', T.TimestampType(), True, metadata={'comment': 'Latest plausible mill_clinical_event.CLINSIG_UPDT_DT_TM recorded against the encounter.'})])
_ENCOUNTER_CHANGED_ID_SCHEMA = T.StructType([T.StructField('ENCNTR_ID', T.LongType(), True)])

def _enc_reject_sentinel(column: F.Column) -> F.Column:
    """Cerner writes 2100-12-31 as an open-ended sentinel; it is not a real timestamp."""
    return F.when(column.isNotNull() & (F.year(column) < 2100), column)

def _encounter_event_bounds_frame(config: MapEncounterConfig=MAP_ENCOUNTER_CONFIG) -> DataFrame:
    """Event bounds for the compatibility timestamps, or an empty typed frame."""
    if not _table_exists(config.event_bounds_table):
        print(f'[WARN] {config.event_bounds_table} does not exist; compatibility timestamps degrade to the raw and estimated values only.')
        return spark.createDataFrame([], _ENCOUNTER_EVENT_BOUNDS_SCHEMA)
    return spark.table(config.event_bounds_table).select(F.col('ENCNTR_ID').cast('long').alias('ENCNTR_ID'), F.col('FIRST_EVENT_DT_TM').cast('timestamp').alias('FIRST_EVENT_DT_TM'), F.col('LAST_EVENT_DT_TM').cast('timestamp').alias('LAST_EVENT_DT_TM'))

def _encounter_event_bounds_watermark(config: MapEncounterConfig) -> Optional[str]:
    return _get_table_properties(config.event_bounds_table).get(MAP_ENCOUNTER_EVENT_BOUNDS_WATERMARK_PROPERTY) or None

def build_encounter_event_bounds(full_refresh: bool, config: MapEncounterConfig=MAP_ENCOUNTER_CONFIG) -> Optional[DataFrame]:
    """
    Maintain per-encounter first and last clinical-event timestamps.

    The bounds are the minimum and maximum of
    mill_clinical_event.CLINSIG_UPDT_DT_TM per ENCNTR_ID, restricted to
    plausible values from year 1950 up to the current timestamp. No
    VALID_UNTIL, ACTIVE_IND or RESULT_STATUS filter is applied, because the
    legacy derivation this reproduces applied none. The bounds exist only to
    populate ARRIVE_DT_TM_COMPAT and DEPART_DT_TM_COMPAT.

    Full mode aggregates the whole source, overwrites the bounds table and
    returns None, because every encounter is potentially affected. Incremental
    mode reads only source rows newer than the stored ADC_UPDT watermark,
    merges a running minimum and maximum, and returns a one-column DataFrame of
    the ENCNTR_ID values whose bounds may have moved.

    Incremental mode maintains a monotone running minimum and maximum, so it
    cannot retract a bound if source events are deleted or their timestamps
    move inwards. A full_refresh run restores exactness. This is an accepted and
    documented limitation that matches the legacy behaviour.
    """
    events = spark.table(config.event_source_table)
    event_time = F.col('CLINSIG_UPDT_DT_TM').cast('timestamp')
    plausible = event_time.isNotNull() & (F.year(event_time) >= F.lit(1950)) & (event_time <= F.current_timestamp())
    upper_row = events.select(F.max(F.col('ADC_UPDT').cast('timestamp')).cast('string').alias('upper_watermark')).first()
    upper_watermark = None if upper_row is None else upper_row['upper_watermark']
    watermark = None if full_refresh else _encounter_event_bounds_watermark(config)
    if full_refresh or watermark is None or not _table_exists(config.event_bounds_table):
        aggregated = events.filter(plausible).groupBy(F.col('ENCNTR_ID').cast('long').alias('ENCNTR_ID')).agg(F.min(event_time).alias('FIRST_EVENT_DT_TM'), F.max(event_time).alias('LAST_EVENT_DT_TM'))
        aggregated = _schema_select(aggregated, _ENCOUNTER_EVENT_BOUNDS_SCHEMA).filter(F.col('ENCNTR_ID').isNotNull())
        aggregated.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(config.event_bounds_table)
        _set_table_comment_if_changed(config.event_bounds_table, MAP_ENCOUNTER_EVENT_BOUNDS_COMMENT)
        _apply_column_comments_if_changed(config.event_bounds_table, _ENCOUNTER_EVENT_BOUNDS_SCHEMA)
        if upper_watermark is not None:
            _set_table_properties_if_changed(config.event_bounds_table, {MAP_ENCOUNTER_EVENT_BOUNDS_WATERMARK_PROPERTY: upper_watermark})
        print('[INFO] map_encounter event bounds rebuilt in full')
        return None
    if upper_watermark is None:
        return spark.createDataFrame([], _ENCOUNTER_CHANGED_ID_SCHEMA)
    window = plausible & (F.col('ADC_UPDT').cast('timestamp') > F.lit(watermark).cast('timestamp')) & (F.col('ADC_UPDT').cast('timestamp') <= F.lit(upper_watermark).cast('timestamp'))
    aggregated = events.filter(window).groupBy(F.col('ENCNTR_ID').cast('long').alias('ENCNTR_ID')).agg(F.min(event_time).alias('FIRST_EVENT_DT_TM'), F.max(event_time).alias('LAST_EVENT_DT_TM'))
    aggregated = _schema_select(aggregated, _ENCOUNTER_EVENT_BOUNDS_SCHEMA).filter(F.col('ENCNTR_ID').isNotNull())
    if not aggregated.take(1):
        _set_table_properties_if_changed(config.event_bounds_table, {MAP_ENCOUNTER_EVENT_BOUNDS_WATERMARK_PROPERTY: upper_watermark})
        print('[INFO] map_encounter event bounds unchanged since the last watermark')
        return spark.createDataFrame([], _ENCOUNTER_CHANGED_ID_SCHEMA)
    DeltaTable.forName(spark, config.event_bounds_table).alias('t').merge(aggregated.alias('s'), 't.ENCNTR_ID = s.ENCNTR_ID').whenMatchedUpdate(set={'FIRST_EVENT_DT_TM': F.least(F.col('t.FIRST_EVENT_DT_TM'), F.col('s.FIRST_EVENT_DT_TM')), 'LAST_EVENT_DT_TM': F.greatest(F.col('t.LAST_EVENT_DT_TM'), F.col('s.LAST_EVENT_DT_TM'))}).whenNotMatchedInsertAll().execute()
    _set_table_properties_if_changed(config.event_bounds_table, {MAP_ENCOUNTER_EVENT_BOUNDS_WATERMARK_PROPERTY: upper_watermark})
    return aggregated.select('ENCNTR_ID').dropDuplicates(['ENCNTR_ID'])

_ENCOUNTER_ACTIVITY_BOUNDS_SCHEMA = T.StructType([T.StructField('ENCNTR_ID', T.LongType(), True, metadata={'comment': 'Millennium encounter identifier. One row per encounter with some recorded clinical activity; encounters with none are absent from this table.'}), T.StructField('FIRST_CLINICAL_EVENT_DT_TM', T.TimestampType(), True, metadata={'comment': 'Earliest plausible mill_clinical_event.EVENT_END_DT_TM for the encounter, over current rows only and excluding cancelled, in-error and not-done results. EVENT_END_DT_TM is clinical time and is freely backdated by retrospective documentation, so this can precede a known arrival by years. It is evidence that the encounter happened, not a usable arrival.'}), T.StructField('LAST_CLINICAL_EVENT_DT_TM', T.TimestampType(), True, metadata={'comment': 'Latest plausible mill_clinical_event.EVENT_END_DT_TM for the encounter under the same filters.'}), T.StructField('CLINICAL_EVENT_COUNT', T.LongType(), True, metadata={'comment': 'Number of qualifying clinical events, zero when the encounter has none.'}), T.StructField('FIRST_CONTEMPORANEOUS_EVENT_DT_TM', T.TimestampType(), True, metadata={'comment': 'Earliest clinical event whose clinical time is no more than seven days before its documentation time (EVENT_END_DT_TM at or after VALID_FROM_DT_TM minus seven days), which strips most retrospectively backdated entries. This is the only event-derived timestamp permitted to set a best-guess arrival, and then only at low confidence: measured against known arrivals it still precedes them 27.0 per cent of the time, with a 1st percentile of minus 48.9 days.'}), T.StructField('LAST_CONTEMPORANEOUS_EVENT_DT_TM', T.TimestampType(), True, metadata={'comment': 'Latest clinical event under the same contemporaneity filter.'}), T.StructField('CONTEMPORANEOUS_EVENT_COUNT', T.LongType(), True, metadata={'comment': 'Number of clinical events passing the contemporaneity filter, zero when the encounter has none.'}), T.StructField('FIRST_ORDER_DT_TM', T.TimestampType(), True, metadata={'comment': 'Earliest plausible mill_orders.ORIG_ORDER_DT_TM for the encounter, excluding order templates and cancelled, voided-without-results and future orders. Orders sit a median 1.4 minutes after a known arrival but precede it 9.9 per cent of the time, because orders can be placed at referral.'}), T.StructField('LAST_ORDER_DT_TM', T.TimestampType(), True, metadata={'comment': 'Latest plausible mill_orders.ORIG_ORDER_DT_TM for the encounter under the same filters.'}), T.StructField('ORDER_COUNT', T.LongType(), True, metadata={'comment': 'Number of qualifying orders, zero when the encounter has none.'}), T.StructField('WARD_MOVE_COUNT', T.LongType(), True, metadata={'comment': 'Number of distinct nurse-unit placements recorded in mill_encntr_loc_hist, deduplicated on nurse unit and the placement arrival and departure pair. Zero when the encounter has none.'}), T.StructField('WARD_OCCUPANCY_MINUTES', T.LongType(), True, metadata={'comment': 'Total minutes spent in nurse units, summed over placements that have both an arrival and a later departure. Null when no placement is closed. Overlapping placements are summed as recorded, so read this as evidence of inpatient duration rather than an exact bed-occupancy measure.'}), T.StructField('LAST_WARD_IN_DT_TM', T.TimestampType(), True, metadata={'comment': 'Latest plausible nurse-unit arrival recorded in mill_encntr_loc_hist.'}), T.StructField('LAST_WARD_OUT_DT_TM', T.TimestampType(), True, metadata={'comment': 'Latest plausible nurse-unit departure recorded in mill_encntr_loc_hist. Location history mirrors the encounter DISCH_DT_TM to the minute 99.6 per cent of the time, so this contributes a departure only for the few thousand encounters that have neither DEPART_DT_TM nor DISCH_DT_TM.'}), T.StructField('LAST_WARD_STILL_OPEN_IND', T.BooleanType(), True, metadata={'comment': 'True when the most recent nurse-unit placement has no recorded departure after it, meaning the patient is still in situ. When true, LAST_WARD_OUT_DT_TM must not be read as a departure.'})])

def _enc_plausible_timestamp(column: F.Column) -> F.Column:
    """A source timestamp is usable as evidence only when it is present, after 1950 and not in the future."""
    stamped = column.cast('timestamp')
    return F.when(stamped.isNotNull() & (F.year(stamped) >= F.lit(1950)) & (stamped <= F.current_timestamp()), stamped)

def _encounter_clinical_event_activity(config: MapEncounterConfig) -> DataFrame:
    """
    Per-encounter clinical-event evidence.

    Current rows only (VALID_UNTIL_DT_TM on or after 2099-01-01, which drops
    878,955,314 of 4,514,629,697 rows) and result statuses that are not
    Canceled, In Error or Not Done. mill_clinical_event has no ACTIVE_IND
    column, so those two filters are the whole validity test.

    The contemporaneous aggregates keep only events whose clinical time is near
    their documentation time. Without that restriction the minimum event time
    precedes a known arrival 36.1 per cent of the time, with a 1st percentile of
    minus 51.7 years, because retrospective documentation backdates clinical
    time freely.
    """
    events = spark.table(config.event_source_table)
    valid_row = F.col('VALID_UNTIL_DT_TM').isNull() | (F.col('VALID_UNTIL_DT_TM').cast('timestamp') >= F.lit(MAP_ENCOUNTER_EVENT_VALID_UNTIL_FLOOR).cast('timestamp'))
    status_kept = F.col('RESULT_STATUS_CD').isNull() | (~F.col('RESULT_STATUS_CD').cast('long').isin(*MAP_ENCOUNTER_EXCLUDED_RESULT_STATUS_CDS))
    event_time = _enc_plausible_timestamp(F.col('EVENT_END_DT_TM'))
    documented_time = F.col('VALID_FROM_DT_TM').cast('timestamp')
    contemporaneous = event_time.isNotNull() & documented_time.isNotNull() & (F.unix_timestamp(event_time) >= (F.unix_timestamp(documented_time) - F.lit(MAP_ENCOUNTER_CONTEMPORANEITY_SECONDS)))
    aggregated = events.filter(valid_row & status_kept).groupBy(F.col('ENCNTR_ID').cast('long').alias('ENCNTR_ID')).agg(F.min(event_time).alias('FIRST_CLINICAL_EVENT_DT_TM'), F.max(event_time).alias('LAST_CLINICAL_EVENT_DT_TM'), F.count(event_time).cast('long').alias('CLINICAL_EVENT_COUNT'), F.min(F.when(contemporaneous, event_time)).alias('FIRST_CONTEMPORANEOUS_EVENT_DT_TM'), F.max(F.when(contemporaneous, event_time)).alias('LAST_CONTEMPORANEOUS_EVENT_DT_TM'), F.count(F.when(contemporaneous, event_time)).cast('long').alias('CONTEMPORANEOUS_EVENT_COUNT'))
    return aggregated.filter(F.col('ENCNTR_ID').isNotNull() & (F.col('ENCNTR_ID') != F.lit(0)))

def _encounter_order_activity(config: MapEncounterConfig) -> DataFrame:
    """
    Per-encounter order evidence.

    Order templates are excluded because 80,398,642 of 633,468,099 source rows
    are templates rather than real orders. Cancelled, voided-without-results and
    future orders are excluded because none of them witnesses an attendance.
    """
    orders = spark.table(config.order_source_table)
    template_flag = F.coalesce(F.col('TEMPLATE_ORDER_FLAG').cast('long'), F.lit(0))
    status_code = F.coalesce(F.col('ORDER_STATUS_CD').cast('long'), F.lit(0))
    kept = (F.coalesce(F.col('ACTIVE_IND').cast('long'), F.lit(1)) == F.lit(1)) & (~template_flag.isin(*MAP_ENCOUNTER_TEMPLATE_ORDER_FLAGS)) & (~status_code.isin(*MAP_ENCOUNTER_EXCLUDED_ORDER_STATUS_CDS))
    order_time = _enc_plausible_timestamp(F.col('ORIG_ORDER_DT_TM'))
    aggregated = orders.filter(kept).groupBy(F.col('ENCNTR_ID').cast('long').alias('ENCNTR_ID')).agg(F.min(order_time).alias('FIRST_ORDER_DT_TM'), F.max(order_time).alias('LAST_ORDER_DT_TM'), F.count(order_time).cast('long').alias('ORDER_COUNT'))
    return aggregated.filter(F.col('ENCNTR_ID').isNotNull() & (F.col('ENCNTR_ID') != F.lit(0)))

def _encounter_ward_activity(config: MapEncounterConfig) -> DataFrame:
    """
    Per-encounter nurse-unit occupancy.

    Only the occupancy is taken from location history. Its own arrival and
    departure timestamps are a denormalised mirror of the encounter's, agreeing
    to the minute 99.995 per cent and 99.6 per cent of the time respectively, so
    they are not an independent witness and are not read as one.
    """
    moves = spark.table(config.location_history_table)
    nurse_unit = F.col('LOC_NURSE_UNIT_CD').cast('long')
    in_scope = (F.coalesce(F.col('ACTIVE_IND').cast('long'), F.lit(0)) == F.lit(1)) & nurse_unit.isNotNull() & (nurse_unit != F.lit(0))
    distinct_moves = moves.filter(in_scope).select(F.col('ENCNTR_ID').cast('long').alias('ENCNTR_ID'), nurse_unit.alias('_NURSE_UNIT_CD'), _enc_plausible_timestamp(F.col('ARRIVE_DT_TM')).alias('_WARD_IN_DT_TM'), _enc_plausible_timestamp(F.col('DEPART_DT_TM')).alias('_WARD_OUT_DT_TM')).dropDuplicates(['ENCNTR_ID', '_NURSE_UNIT_CD', '_WARD_IN_DT_TM', '_WARD_OUT_DT_TM'])
    closed_move = F.col('_WARD_IN_DT_TM').isNotNull() & F.col('_WARD_OUT_DT_TM').isNotNull() & (F.col('_WARD_OUT_DT_TM') >= F.col('_WARD_IN_DT_TM'))
    occupancy_minutes = F.when(closed_move, (F.unix_timestamp(F.col('_WARD_OUT_DT_TM')) - F.unix_timestamp(F.col('_WARD_IN_DT_TM'))) / F.lit(60))
    aggregated = distinct_moves.groupBy('ENCNTR_ID').agg(F.count(F.lit(1)).cast('long').alias('WARD_MOVE_COUNT'), F.round(F.sum(occupancy_minutes)).cast('long').alias('WARD_OCCUPANCY_MINUTES'), F.max(F.col('_WARD_IN_DT_TM')).alias('LAST_WARD_IN_DT_TM'), F.max(F.col('_WARD_OUT_DT_TM')).alias('LAST_WARD_OUT_DT_TM'))
    aggregated = aggregated.withColumn('LAST_WARD_STILL_OPEN_IND', F.col('LAST_WARD_IN_DT_TM').isNotNull() & (F.col('LAST_WARD_OUT_DT_TM').isNull() | (F.col('LAST_WARD_OUT_DT_TM') < F.col('LAST_WARD_IN_DT_TM'))))
    return aggregated.filter(F.col('ENCNTR_ID').isNotNull() & (F.col('ENCNTR_ID') != F.lit(0)))

def _encounter_activity_bounds_frame(config: MapEncounterConfig=MAP_ENCOUNTER_CONFIG) -> DataFrame:
    """Activity evidence for the best-guess timestamps, or an empty typed frame."""
    if not _table_exists(config.activity_bounds_table):
        print(f'[WARN] {config.activity_bounds_table} does not exist; encounter activity evidence is unavailable and the best-guess timestamps degrade to the source columns alone.')
        return spark.createDataFrame([], _ENCOUNTER_ACTIVITY_BOUNDS_SCHEMA)
    return _schema_select(spark.table(config.activity_bounds_table), _ENCOUNTER_ACTIVITY_BOUNDS_SCHEMA)

def _encounter_changed_ids_since(table: str, watermark: Optional[str], upper_watermark: Optional[str]) -> DataFrame:
    """Encounter identifiers whose rows in an activity source moved inside the watermark window."""
    if watermark is None or upper_watermark is None:
        return spark.createDataFrame([], _ENCOUNTER_CHANGED_ID_SCHEMA)
    updated = F.col('ADC_UPDT').cast('timestamp')
    window = updated.isNotNull() & (updated > F.lit(watermark).cast('timestamp')) & (updated <= F.lit(upper_watermark).cast('timestamp'))
    return spark.table(table).filter(window).select(F.col('ENCNTR_ID').cast('long').alias('ENCNTR_ID')).dropDuplicates(['ENCNTR_ID'])

def build_encounter_activity_bounds(full_refresh: bool, config: MapEncounterConfig=MAP_ENCOUNTER_CONFIG, event_changed_encounters: Optional[DataFrame]=None) -> Optional[DataFrame]:
    """
    Maintain per-encounter clinical-activity evidence.

    The values are always recomputed in full from mill_clinical_event,
    mill_orders and mill_encntr_loc_hist and the table is overwritten, in both
    full and incremental runs. This is the deliberate difference from
    build_encounter_event_bounds, whose least/greatest merge cannot retract a
    bound when source rows are deleted or corrected. That is tolerable when the
    only job is reproducing a legacy derivation and is not tolerable for a
    curated best guess, so this lane pays for one aggregation pass per source
    per run and keeps the answer exact.

    The return value is only about which encounters need their canonical row
    rebuilt, not about the values. Full mode returns None, meaning every
    encounter is potentially affected. Incremental mode returns the ENCNTR_ID
    values whose activity sources moved inside the ADC_UPDT watermark window,
    unioned with event_changed_encounters, which the caller has already computed
    against mill_clinical_event and which is passed in rather than rescanned.

    A source row deleted outright does not bump ADC_UPDT, so its encounter can
    miss the incremental trigger even though the recomputed values are correct;
    the next full_refresh picks it up. This matches the limitation the rest of
    the map pipeline lives with.
    """
    table_existed = _table_exists(config.activity_bounds_table)
    order_upper_row = spark.table(config.order_source_table).select(F.max(F.col('ADC_UPDT').cast('timestamp')).cast('string').alias('upper_watermark')).first()
    order_upper = None if order_upper_row is None else order_upper_row['upper_watermark']
    location_upper_row = spark.table(config.location_history_table).select(F.max(F.col('ADC_UPDT').cast('timestamp')).cast('string').alias('upper_watermark')).first()
    location_upper = None if location_upper_row is None else location_upper_row['upper_watermark']
    properties = _get_table_properties(config.activity_bounds_table) if table_existed else {}
    order_watermark = None if full_refresh else (properties.get(MAP_ENCOUNTER_ACTIVITY_ORDER_WATERMARK_PROPERTY) or None)
    location_watermark = None if full_refresh else (properties.get(MAP_ENCOUNTER_ACTIVITY_LOCATION_WATERMARK_PROPERTY) or None)
    aggregated = _encounter_clinical_event_activity(config).join(_encounter_order_activity(config), 'ENCNTR_ID', 'full_outer').join(_encounter_ward_activity(config), 'ENCNTR_ID', 'full_outer')
    for count_column in ('CLINICAL_EVENT_COUNT', 'CONTEMPORANEOUS_EVENT_COUNT', 'ORDER_COUNT', 'WARD_MOVE_COUNT'):
        aggregated = aggregated.withColumn(count_column, F.coalesce(F.col(count_column), F.lit(0)).cast('long'))
    aggregated = aggregated.withColumn('LAST_WARD_STILL_OPEN_IND', F.coalesce(F.col('LAST_WARD_STILL_OPEN_IND'), F.lit(False)))
    aggregated = _schema_select(aggregated, _ENCOUNTER_ACTIVITY_BOUNDS_SCHEMA).filter(F.col('ENCNTR_ID').isNotNull())
    aggregated.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(config.activity_bounds_table)
    _set_table_comment_if_changed(config.activity_bounds_table, MAP_ENCOUNTER_ACTIVITY_BOUNDS_COMMENT)
    _apply_column_comments_if_changed(config.activity_bounds_table, _ENCOUNTER_ACTIVITY_BOUNDS_SCHEMA)
    advanced_properties = {}
    if order_upper is not None:
        advanced_properties[MAP_ENCOUNTER_ACTIVITY_ORDER_WATERMARK_PROPERTY] = order_upper
    if location_upper is not None:
        advanced_properties[MAP_ENCOUNTER_ACTIVITY_LOCATION_WATERMARK_PROPERTY] = location_upper
    if advanced_properties:
        _set_table_properties_if_changed(config.activity_bounds_table, advanced_properties)
    if full_refresh or not table_existed or order_watermark is None or location_watermark is None:
        print('[INFO] map_encounter activity bounds recomputed in full; every encounter is treated as affected')
        return None
    changed_frames = [_encounter_changed_ids_since(config.order_source_table, order_watermark, order_upper), _encounter_changed_ids_since(config.location_history_table, location_watermark, location_upper)]
    if event_changed_encounters is not None:
        changed_frames.append(event_changed_encounters.select(F.col('ENCNTR_ID').cast('long').alias('ENCNTR_ID')))
    changed = reduce(lambda left, right: left.unionByName(right), changed_frames)
    return changed.filter(F.col('ENCNTR_ID').isNotNull() & (F.col('ENCNTR_ID') != F.lit(0))).dropDuplicates(['ENCNTR_ID'])

MAP_ENCOUNTER_CORROBORATION_SECONDS = 24 * 60 * 60
MAP_ENCOUNTER_ADMIN_CLOSE_SECONDS = 30 * 24 * 60 * 60
MAP_ENCOUNTER_FIXED_WINDOW_MIN_SECONDS = 59 * 24 * 60 * 60
MAP_ENCOUNTER_FIXED_WINDOW_MAX_SECONDS = 61 * 24 * 60 * 60
MAP_ENCOUNTER_FIXED_WINDOW_CLOSE_HOURS = (3, 4, 5, 6)

def _enc_activity_column(encounters: DataFrame, name: str, data_type: str) -> F.Column:
    """Read a column joined from the activity-bounds sidecar, tolerating its absence."""
    return F.col(name) if name in encounters.columns else F.lit(None).cast(data_type)

def _add_encounter_time_model(encounters: DataFrame) -> DataFrame:
    """
    Add the derived timestamp families. Raw source columns are never modified.

    Evidence (what independent sources witness about this encounter): the
    clinical-event, order and ward-occupancy columns joined from the
    activity-bounds sidecar, summarised by ATTENDANCE_EVIDENCE and
    ATTENDANCE_WITNESS_COUNT.
    Best guess (the headline interface): ARRIVAL_DT_TM_BEST,
    DEPARTURE_DT_TM_BEST and LENGTH_OF_STAY_MINUTES, each with a _METHOD naming
    the arm that supplied it and a _CONFIDENCE. Booking slots (EST_ARRIVE_DT_TM,
    EST_DEPART_DT_TM), workflow stamps (ENCNTR_COMPLETE_DT_TM, CREATE_DT_TM,
    BEG_EFFECTIVE_DT_TM) and any synthetic arrival-plus-one-day departure are
    never used: measurement showed EST_ARRIVE_DT_TM to be a five-minute booking
    slot that nonetheless asserted an arrival for 3,172,028 encounters carrying
    no attendance evidence of any kind. Length of stay is stricter than either
    endpoint and is emitted only where both came from a direct observation.
    Workflow (when the record was completed): ENCOUNTER_COMPLETE_DT_TM_EFFECTIVE.
    Compatibility (what the legacy pipeline published under the raw column
    names): ARRIVE_DT_TM_COMPAT and DEPART_DT_TM_COMPAT, reproducing the legacy
    clinical-event bounds and its seven-day outlier rule unchanged, with
    _FABRICATED_IND flags marking the values that have no observational basis.
    """
    arrive_raw = _enc_reject_sentinel(F.col('ARRIVE_DT_TM'))
    est_arrive_raw = _enc_reject_sentinel(F.col('EST_ARRIVE_DT_TM'))
    depart_raw = _enc_reject_sentinel(F.col('DEPART_DT_TM'))
    complete_raw = _enc_reject_sentinel(F.col('ENCNTR_COMPLETE_DT_TM'))
    first_event = F.col('FIRST_EVENT_DT_TM') if 'FIRST_EVENT_DT_TM' in encounters.columns else F.lit(None).cast('timestamp')
    last_event = F.col('LAST_EVENT_DT_TM') if 'LAST_EVENT_DT_TM' in encounters.columns else F.lit(None).cast('timestamp')
    one_week_seconds = F.lit(7 * 24 * 60 * 60)
    compat_arrive = F.when(first_event.isNotNull() & arrive_raw.isNotNull() & ((F.unix_timestamp(first_event) - F.unix_timestamp(arrive_raw)) > one_week_seconds), first_event).otherwise(F.coalesce(arrive_raw, est_arrive_raw, first_event))
    compat_depart_bounded = F.when(depart_raw.isNotNull() & last_event.isNotNull() & ((F.unix_timestamp(depart_raw) - F.unix_timestamp(last_event)) > one_week_seconds), last_event).otherwise(F.coalesce(depart_raw, last_event))
    compat_depart = F.coalesce(compat_depart_bounded, F.date_add(compat_arrive, 1).cast('timestamp'))
    ward_occupancy = _enc_activity_column(encounters, 'WARD_OCCUPANCY_MINUTES', 'long')
    ward_still_open = F.coalesce(_enc_activity_column(encounters, 'LAST_WARD_STILL_OPEN_IND', 'boolean'), F.lit(False))
    clinical_event_count = F.coalesce(_enc_activity_column(encounters, 'CLINICAL_EVENT_COUNT', 'long'), F.lit(0))
    contemporaneous_count = F.coalesce(_enc_activity_column(encounters, 'CONTEMPORANEOUS_EVENT_COUNT', 'long'), F.lit(0))
    order_count = F.coalesce(_enc_activity_column(encounters, 'ORDER_COUNT', 'long'), F.lit(0))
    ward_move_count = F.coalesce(_enc_activity_column(encounters, 'WARD_MOVE_COUNT', 'long'), F.lit(0))
    first_contemporaneous = _enc_plausible_timestamp(_enc_activity_column(encounters, 'FIRST_CONTEMPORANEOUS_EVENT_DT_TM', 'timestamp'))
    last_contemporaneous = _enc_plausible_timestamp(_enc_activity_column(encounters, 'LAST_CONTEMPORANEOUS_EVENT_DT_TM', 'timestamp'))
    first_order = _enc_plausible_timestamp(_enc_activity_column(encounters, 'FIRST_ORDER_DT_TM', 'timestamp'))
    last_order = _enc_plausible_timestamp(_enc_activity_column(encounters, 'LAST_ORDER_DT_TM', 'timestamp'))
    ward_out = _enc_plausible_timestamp(_enc_activity_column(encounters, 'LAST_WARD_OUT_DT_TM', 'timestamp'))
    arrive_observed = _enc_plausible_timestamp(F.col('ARRIVE_DT_TM'))
    reg_observed = _enc_plausible_timestamp(F.col('REG_DT_TM'))
    depart_observed = _enc_plausible_timestamp(F.col('DEPART_DT_TM'))
    disch_observed = _enc_plausible_timestamp(F.col('DISCH_DT_TM'))
    tolerance_seconds = F.lit(MAP_ENCOUNTER_CORROBORATION_SECONDS)
    encounters = encounters.withColumn('SCHEDULED_ARRIVAL_DT_TM', F.col('EST_ARRIVE_DT_TM'))
    encounters = encounters.withColumn('SCHEDULED_DEPARTURE_DT_TM', F.col('EST_DEPART_DT_TM'))
    encounters = encounters.withColumn('CLINICAL_EVENT_COUNT', clinical_event_count.cast('long'))
    encounters = encounters.withColumn('CONTEMPORANEOUS_EVENT_COUNT', contemporaneous_count.cast('long'))
    encounters = encounters.withColumn('ORDER_COUNT', order_count.cast('long'))
    encounters = encounters.withColumn('WARD_MOVE_COUNT', ward_move_count.cast('long'))
    encounters = encounters.withColumn('LAST_WARD_STILL_OPEN_IND', ward_still_open)
    witness_arrive = F.when(arrive_observed.isNotNull(), F.lit(1)).otherwise(F.lit(0))
    witness_reg = F.when(reg_observed.isNotNull(), F.lit(1)).otherwise(F.lit(0))
    witness_event = F.when(contemporaneous_count > F.lit(0), F.lit(1)).otherwise(F.lit(0))
    witness_order = F.when(order_count > F.lit(0), F.lit(1)).otherwise(F.lit(0))
    witness_ward = F.when(ward_move_count > F.lit(0), F.lit(1)).otherwise(F.lit(0))
    encounters = encounters.withColumn('ATTENDANCE_WITNESS_COUNT', (witness_arrive + witness_reg + witness_event + witness_order + witness_ward).cast('int'))
    status_text = F.upper(F.coalesce(F.col('encntr_status_desc'), F.lit('')))
    cancelled_status = status_text.rlike('CANCEL|REJECT')
    future_slot = F.col('EST_ARRIVE_DT_TM').isNotNull() & (F.col('EST_ARRIVE_DT_TM') > F.current_timestamp())
    encounters = encounters.withColumn('ATTENDANCE_EVIDENCE', F.when(F.col('ATTENDANCE_WITNESS_COUNT') > F.lit(0), F.lit('OBSERVED')).when(clinical_event_count > F.lit(0), F.lit('INFERRED')).when(cancelled_status, F.lit('CANCELLED')).when(future_slot, F.lit('FUTURE_SCHEDULED')).otherwise(F.lit('NONE')))
    activity_arrive = F.least(first_contemporaneous, first_order)
    attendance_observed = F.col('ATTENDANCE_EVIDENCE') == F.lit('OBSERVED')
    encounters = encounters.withColumn('ARRIVAL_METHOD', F.when(arrive_observed.isNotNull(), F.lit('ARRIVE_DT_TM')).when(reg_observed.isNotNull(), F.lit('REG_DT_TM')).when(attendance_observed & activity_arrive.isNotNull(), F.lit('FIRST_ACTIVITY')))
    encounters = encounters.withColumn('ARRIVAL_DT_TM_BEST', F.when(F.col('ARRIVAL_METHOD') == F.lit('ARRIVE_DT_TM'), arrive_observed).when(F.col('ARRIVAL_METHOD') == F.lit('REG_DT_TM'), reg_observed).when(F.col('ARRIVAL_METHOD') == F.lit('FIRST_ACTIVITY'), activity_arrive))
    best_arrival = F.col('ARRIVAL_DT_TM_BEST')
    arrive_near_event = first_contemporaneous.isNotNull() & (F.abs(F.unix_timestamp(first_contemporaneous) - F.unix_timestamp(best_arrival)) <= tolerance_seconds)
    arrive_near_order = first_order.isNotNull() & (F.abs(F.unix_timestamp(first_order) - F.unix_timestamp(best_arrival)) <= tolerance_seconds)
    arrive_arms_agree = first_contemporaneous.isNotNull() & first_order.isNotNull() & (F.abs(F.unix_timestamp(first_contemporaneous) - F.unix_timestamp(first_order)) <= tolerance_seconds)
    encounters = encounters.withColumn('ARRIVAL_CORROBORATED_IND', F.when(best_arrival.isNull(), F.lit(None).cast('boolean')).when(F.col('ARRIVAL_METHOD') == F.lit('FIRST_ACTIVITY'), arrive_arms_agree).otherwise(arrive_near_event | arrive_near_order))
    encounters = encounters.withColumn('ARRIVAL_CONFIDENCE', F.when(best_arrival.isNull(), F.lit(None).cast('string')).when(F.col('ARRIVAL_METHOD') == F.lit('FIRST_ACTIVITY'), F.lit('LOW')).when(F.col('ARRIVAL_METHOD') == F.lit('REG_DT_TM'), F.lit('MEDIUM')).when(F.col('ARRIVAL_CORROBORATED_IND'), F.lit('HIGH')).otherwise(F.lit('MEDIUM')))
    ward_depart = F.when(~ward_still_open, ward_out)
    encounter_closed = complete_raw.isNotNull() | status_text.rlike('DISCHARGED|CLOSED|COMPLETE')
    activity_depart = F.greatest(last_contemporaneous, last_order)
    departure_method = F.when(depart_observed.isNotNull(), F.lit('DEPART_DT_TM')).when(disch_observed.isNotNull(), F.lit('DISCH_DT_TM')).when(ward_depart.isNotNull(), F.lit('WARD_OUT')).when(encounter_closed & activity_depart.isNotNull(), F.lit('LAST_ACTIVITY'))
    departure_candidate = F.when(departure_method == F.lit('DEPART_DT_TM'), depart_observed).when(departure_method == F.lit('DISCH_DT_TM'), disch_observed).when(departure_method == F.lit('WARD_OUT'), ward_depart).when(departure_method == F.lit('LAST_ACTIVITY'), activity_depart)
    # A raw departure stamp is not always an observation: a nightly job closes
    # dormant encounters, a median 630 days after the last clinical event. It is
    # an administrative close when the encounter sat open more than thirty days
    # past both its arrival and its last independent activity. Both conditions
    # are required -- the arrival guard is what keeps the rule off short real
    # stays whose linked activity happens to be backdated. Ward-out is excluded
    # deliberately: it is a mirror of DEPART_DT_TM, not a witness to it.
    admin_close_seconds = F.lit(MAP_ENCOUNTER_ADMIN_CLOSE_SECONDS)
    last_clinical_event = _enc_plausible_timestamp(_enc_activity_column(encounters, 'LAST_CLINICAL_EVENT_DT_TM', 'timestamp'))
    last_independent_activity = F.greatest(last_clinical_event, last_order)
    admin_close_anchor = F.coalesce(last_independent_activity, best_arrival)
    admin_close = departure_method.isin('DEPART_DT_TM', 'DISCH_DT_TM') & departure_candidate.isNotNull() & admin_close_anchor.isNotNull() & ((F.unix_timestamp(departure_candidate) - F.unix_timestamp(admin_close_anchor)) > admin_close_seconds) & (best_arrival.isNull() | ((F.unix_timestamp(departure_candidate) - F.unix_timestamp(best_arrival)) > admin_close_seconds))
    encounters = encounters.withColumn('DEPART_DT_TM_ADMIN_CLOSE_IND', F.coalesce(admin_close, F.lit(False)))
    # A second close mechanism runs on a fixed window rather than on inactivity, so
    # activity continues right up to it -- the gap from the last activity is about
    # eight days -- and no inactivity threshold can reach it. Outpatient Services
    # closes roughly sixty days after arrival: 94.2% of the stays that type still
    # published after the rule above sit in a two-day band at 59-61 days and 99.98%
    # of those close between 03:00 and 06:59 -- an hour later than the inactivity
    # job, so do not reuse that window here. The boundary is sharp: hour 6 carries
    # 47,796 of them, hours 2 and 7 carry none, and from hour 8 on the band is
    # spread across the working day and genuine. The band is the evidence, so the
    # test keys off the signature rather than the encounter type; across every
    # other type it fires on 46 rows in 30.9M.
    fixed_window_span = F.unix_timestamp(departure_candidate) - F.unix_timestamp(best_arrival)
    fixed_window_close = departure_method.isin('DEPART_DT_TM', 'DISCH_DT_TM') & departure_candidate.isNotNull() & best_arrival.isNotNull() & (fixed_window_span >= F.lit(MAP_ENCOUNTER_FIXED_WINDOW_MIN_SECONDS)) & (fixed_window_span <= F.lit(MAP_ENCOUNTER_FIXED_WINDOW_MAX_SECONDS)) & F.hour(departure_candidate).isin(*MAP_ENCOUNTER_FIXED_WINDOW_CLOSE_HOURS)
    encounters = encounters.withColumn('DEPART_DT_TM_FIXED_WINDOW_CLOSE_IND', F.coalesce(fixed_window_close, F.lit(False)))
    encounters = encounters.withColumn('DEPARTURE_REJECTED_REASON', F.when(departure_candidate.isNotNull() & best_arrival.isNotNull() & (departure_candidate < best_arrival), F.lit('BEFORE_ARRIVAL')).when(F.col('DEPART_DT_TM_ADMIN_CLOSE_IND'), F.lit('ADMINISTRATIVE_CLOSE')).when(F.col('DEPART_DT_TM_FIXED_WINDOW_CLOSE_IND'), F.lit('FIXED_WINDOW_CLOSE')))
    encounters = encounters.withColumn('DEPARTURE_DT_TM_BEST', F.when(F.col('DEPARTURE_REJECTED_REASON').isNull(), departure_candidate))
    encounters = encounters.withColumn('DEPARTURE_METHOD', F.when(F.col('DEPARTURE_DT_TM_BEST').isNotNull(), departure_method))
    best_departure = F.col('DEPARTURE_DT_TM_BEST')
    depart_near_event = last_contemporaneous.isNotNull() & (F.abs(F.unix_timestamp(last_contemporaneous) - F.unix_timestamp(best_departure)) <= tolerance_seconds)
    depart_near_order = last_order.isNotNull() & (F.abs(F.unix_timestamp(last_order) - F.unix_timestamp(best_departure)) <= tolerance_seconds)
    depart_arms_agree = last_contemporaneous.isNotNull() & last_order.isNotNull() & (F.abs(F.unix_timestamp(last_contemporaneous) - F.unix_timestamp(last_order)) <= tolerance_seconds)
    encounters = encounters.withColumn('DEPARTURE_CORROBORATED_IND', F.when(best_departure.isNull(), F.lit(None).cast('boolean')).when(F.col('DEPARTURE_METHOD') == F.lit('LAST_ACTIVITY'), depart_arms_agree).when(F.col('DEPARTURE_METHOD') == F.lit('WARD_OUT'), depart_near_event | depart_near_order).otherwise(depart_near_event | depart_near_order))
    encounters = encounters.withColumn('DEPARTURE_CONFIDENCE', F.when(best_departure.isNull(), F.lit(None).cast('string')).when(F.col('DEPARTURE_METHOD') == F.lit('LAST_ACTIVITY'), F.lit('LOW')).when(F.col('DEPARTURE_METHOD').isin('DISCH_DT_TM', 'WARD_OUT'), F.lit('MEDIUM')).when(F.col('DEPARTURE_CORROBORATED_IND'), F.lit('HIGH')).otherwise(F.lit('MEDIUM')))
    direct_arrival = F.col('ARRIVAL_METHOD').isin('ARRIVE_DT_TM', 'REG_DT_TM')
    direct_departure = F.col('DEPARTURE_METHOD').isin('DEPART_DT_TM', 'DISCH_DT_TM', 'WARD_OUT')
    stay_minutes = ((F.unix_timestamp(best_departure) - F.unix_timestamp(best_arrival)) / F.lit(60)).cast('long')
    encounters = encounters.withColumn('LENGTH_OF_STAY_MINUTES', F.when(best_arrival.isNotNull() & best_departure.isNotNull() & direct_arrival & direct_departure, stay_minutes))
    encounters = encounters.withColumn('LENGTH_OF_STAY_METHOD', F.when(F.col('LENGTH_OF_STAY_MINUTES').isNotNull(), F.concat_ws('_TO_', F.col('ARRIVAL_METHOD'), F.col('DEPARTURE_METHOD'))).when(best_arrival.isNull() | best_departure.isNull(), F.lit('INCOMPLETE_ENDPOINTS')).otherwise(F.lit('INSUFFICIENT_PRECISION')))
    encounters = encounters.withColumn('LENGTH_OF_STAY_CONFIDENCE', F.when(F.col('LENGTH_OF_STAY_MINUTES').isNull(), F.lit(None).cast('string')).when((F.col('ARRIVAL_CONFIDENCE') == F.lit('HIGH')) & (F.col('DEPARTURE_CONFIDENCE') == F.lit('HIGH')), F.lit('HIGH')).otherwise(F.lit('MEDIUM')))
    encounters = encounters.withColumn('WARD_LENGTH_OF_STAY_MINUTES', F.when((ward_move_count > F.lit(0)) & ~ward_still_open, ward_occupancy.cast('long')))
    encounters = encounters.withColumn('ENCOUNTER_COMPLETE_DT_TM_EFFECTIVE', complete_raw)
    encounters = encounters.withColumn('ARRIVE_DT_TM_COMPAT', compat_arrive)
    encounters = encounters.withColumn('DEPART_DT_TM_COMPAT', compat_depart)
    encounters = encounters.withColumn('ARRIVE_DT_TM_COMPAT_FABRICATED_IND', F.col('ARRIVE_DT_TM_COMPAT').isNotNull() & arrive_raw.isNull() & est_arrive_raw.isNotNull())
    encounters = encounters.withColumn('DEPART_DT_TM_COMPAT_FABRICATED_IND', F.col('DEPART_DT_TM_COMPAT').isNotNull() & compat_depart_bounded.isNull())
    encounters = encounters.withColumn('RAW_DEPART_BEFORE_ARRIVE_IND', F.col('ARRIVE_DT_TM').isNotNull() & F.col('DEPART_DT_TM').isNotNull() & (F.col('DEPART_DT_TM') < F.col('ARRIVE_DT_TM')))
    encounters = encounters.withColumn('COMPAT_END_BEFORE_START_IND', F.col('ARRIVE_DT_TM_COMPAT').isNotNull() & F.col('DEPART_DT_TM_COMPAT').isNotNull() & (F.col('DEPART_DT_TM_COMPAT') < F.col('ARRIVE_DT_TM_COMPAT')))
    return encounters

def _add_row_hash(encounters: DataFrame) -> DataFrame:
    hash_columns = [field.name for field in schema_map_encounter.fields if field.name not in _VOLATILE_HASH_COLUMNS]
    normalized = [F.coalesce(F.col(column).cast('string'), F.lit('<NULL>')) for column in hash_columns if column in encounters.columns]
    return encounters.withColumn('ROW_HASH', F.xxhash64(*normalized))

def _aggregate_triggers(trigger_rows: DataFrame) -> DataFrame:
    return trigger_rows.groupBy('ENCNTR_ID').agg(F.concat_ws(',', F.sort_array(F.collect_set('_TRIGGER_SOURCE'))).alias('TRIGGER_SOURCES'), F.max('_ENCOUNTER_COMMIT_VERSION').cast('long').alias('ENCOUNTER_CDF_COMMIT_VERSION'), F.max('_ENCOUNTER_COMMIT_TIMESTAMP').alias('ENCOUNTER_CDF_COMMIT_TIMESTAMP'), F.first('_ENCOUNTER_CHANGE_TYPE', ignorenulls=True).alias('ENCOUNTER_CDF_CHANGE_TYPE'))

def _build_target_rows(raw: DataFrame, lookup: DataFrame, trigger_rows: Optional[DataFrame], run_id: str, run_timestamp: datetime, full_rebuild: bool, source_version: int, config: MapEncounterConfig=MAP_ENCOUNTER_CONFIG) -> DataFrame:
    encounters = _prepare_encounter_source(raw)
    if full_rebuild:
        encounters = encounters.withColumn('TRIGGER_SOURCES', F.lit('FULL_REBUILD')).withColumn('ENCOUNTER_CDF_COMMIT_VERSION', F.lit(int(source_version)).cast('long')).withColumn('ENCOUNTER_CDF_COMMIT_TIMESTAMP', F.lit(None).cast('timestamp')).withColumn('ENCOUNTER_CDF_CHANGE_TYPE', F.lit('snapshot'))
    else:
        if trigger_rows is None:
            raise ValueError('Incremental target rows require trigger metadata')
        encounters = encounters.join(_aggregate_triggers(trigger_rows), 'ENCNTR_ID', 'inner')
    encounters = _enrich_code_descriptions(encounters, lookup)
    encounters = encounters.join(_encounter_event_bounds_frame(config), 'ENCNTR_ID', 'left')
    encounters = encounters.join(_encounter_activity_bounds_frame(config), 'ENCNTR_ID', 'left')
    encounters = _add_encounter_time_model(encounters)
    encounters = _add_row_hash(encounters)
    encounters = encounters.withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_UPDT_DT_TM', F.lit(run_timestamp).cast('timestamp'))
    return _schema_select(encounters, schema_map_encounter)

def _delete_change_rows(keys: DataFrame) -> DataFrame:
    return _schema_select(keys.select(F.col('ENCNTR_ID').cast('long').alias('ENCNTR_ID')), schema_map_encounter).withColumn('_source_change_type', F.lit('delete'))

def _latest_encounter_cdf(config: MapEncounterConfig, starting_version: int, ending_version: int) -> Tuple[Optional[DataFrame], Optional[DataFrame], Optional[DataFrame]]:
    changes = _read_cdf(config.source_table, starting_version, ending_version)
    if changes is None:
        return (None, None, None)
    changes = changes.filter(F.col('_change_type').isin('insert', 'update_preimage', 'update_postimage', 'delete')).withColumn('_ENCNTR_ID_LONG', _checked_long(F.col('ENCNTR_ID'), 'MILL_ENCOUNTER CDF ENCNTR_ID', required=True))
    priority = F.when(F.col('_change_type').isin('update_postimage', 'insert'), F.lit(4)).when(F.col('_change_type') == 'delete', F.lit(3)).otherwise(F.lit(2))
    latest_window = Window.partitionBy('_ENCNTR_ID_LONG').orderBy(F.col('_commit_version').desc(), priority.desc())
    latest = changes.withColumn('_CDF_ROW_NUMBER', F.row_number().over(latest_window)).filter(F.col('_CDF_ROW_NUMBER') == 1).drop('_CDF_ROW_NUMBER')
    trigger_rows = latest.select(F.col('_ENCNTR_ID_LONG').alias('ENCNTR_ID'), F.lit('MILL_ENCOUNTER').alias('_TRIGGER_SOURCE'), F.col('_commit_version').cast('long').alias('_ENCOUNTER_COMMIT_VERSION'), F.col('_commit_timestamp').alias('_ENCOUNTER_COMMIT_TIMESTAMP'), F.col('_change_type').alias('_ENCOUNTER_CHANGE_TYPE'))
    upserts = latest.filter(F.col('_change_type').isin('insert', 'update_postimage')).drop('_change_type', '_commit_version', '_commit_timestamp', '_ENCNTR_ID_LONG')
    deletes = latest.filter(F.col('_change_type').isin('delete', 'update_preimage')).select(F.col('_ENCNTR_ID_LONG').alias('ENCNTR_ID')).dropDuplicates(['ENCNTR_ID'])
    return (upserts, deletes, trigger_rows)

def _changed_lookup_codes(config: MapEncounterConfig, starting_version: int, ending_version: int) -> List[int]:
    changes = _read_cdf(config.code_value_table, starting_version, ending_version)
    if changes is None:
        return []
    changed_codes = changes.filter(F.col('_change_type').isin('insert', 'update_preimage', 'update_postimage', 'delete')).select(_checked_long(F.col('CODE_VALUE'), 'MILL_CODE_VALUE CDF CODE_VALUE', required=True).alias('CODE_VALUE')).dropDuplicates(['CODE_VALUE'])
    try:
        count = changed_codes.count()
        if count > config.max_incremental_lookup_codes:
            raise OverflowError(f'{count:,} code values changed, exceeding the safe incremental threshold of {config.max_incremental_lookup_codes:,}')
        return [int(row['CODE_VALUE']) for row in changed_codes.collect()]
    finally:
        None

def _raw_rows_affected_by_lookup_codes(raw_snapshot: DataFrame, changed_codes: Sequence[int]) -> Optional[DataFrame]:
    if not changed_codes:
        return None
    source_codes = F.array(*[F.col(column).cast('long') for column in CODE_TRIGGER_COLUMNS])
    changed_code_array = F.array(*[F.lit(int(code)).cast('long') for code in changed_codes])
    affected = raw_snapshot.filter(F.size(F.array_intersect(source_codes, changed_code_array)) > 0)
    return _register_cache(affected)

def _union_raw_sources(encounter_upserts: Optional[DataFrame], lookup_affected: Optional[DataFrame], bounds_affected: Optional[DataFrame]=None) -> Optional[DataFrame]:
    frames = [frame for frame in (encounter_upserts, lookup_affected, bounds_affected) if frame is not None]
    if not frames:
        return None
    if len(frames) == 1:
        return frames[0]
    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), frames)

def _union_trigger_rows(encounter_triggers: Optional[DataFrame], lookup_affected: Optional[DataFrame], bounds_affected: Optional[DataFrame]=None) -> Optional[DataFrame]:
    frames: List[DataFrame] = []
    if encounter_triggers is not None:
        frames.append(encounter_triggers)
    for affected_rows, trigger_label in ((lookup_affected, 'MILL_CODE_VALUE'), (bounds_affected, 'MILL_CLINICAL_EVENT_BOUNDS')):
        if affected_rows is None:
            continue
        frames.append(affected_rows.select(_checked_long(F.col('ENCNTR_ID'), f'MILL_ENCOUNTER.ENCNTR_ID {trigger_label} trigger', required=True).alias('ENCNTR_ID'), F.lit(trigger_label).alias('_TRIGGER_SOURCE'), F.lit(None).cast('long').alias('_ENCOUNTER_COMMIT_VERSION'), F.lit(None).cast('timestamp').alias('_ENCOUNTER_COMMIT_TIMESTAMP'), F.lit(None).cast('string').alias('_ENCOUNTER_CHANGE_TYPE')))
    if not frames:
        return None
    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), frames)

def _remove_upserted_delete_keys(delete_keys: Optional[DataFrame], target_rows: Optional[DataFrame]) -> Optional[DataFrame]:
    if delete_keys is None or target_rows is None:
        return delete_keys
    upsert_keys = target_rows.select('ENCNTR_ID').dropDuplicates(['ENCNTR_ID'])
    return delete_keys.join(upsert_keys, 'ENCNTR_ID', 'left_anti')

def create_encounter_mapping_incr(force_full_rebuild: bool=False, run_id: Optional[str]=None, run_timestamp: Optional[datetime]=None, config: MapEncounterConfig=MAP_ENCOUNTER_CONFIG) -> DataFrame:
    """
    Build map_encounter changes without writing the target.

    The returned DataFrame contains all target columns plus _source_change_type,
    whose value is upsert or delete. Call update_map_encounter_table with the
    returned DataFrame, or use run_map_encounter_update.
    """
    global _PENDING_ENCOUNTER_PLAN
    _release_caches()
    _ensure_control_tables(config)
    run_id = run_id or str(uuid.uuid4())
    run_timestamp = run_timestamp or datetime.now(timezone.utc)
    source_versions = _capture_source_versions(config)
    state = _read_pipeline_state(config)
    automatic_full, reason = _target_requires_full_rebuild(config, state)
    full_rebuild = force_full_rebuild or automatic_full
    mode = 'FULL_REBUILD' if full_rebuild else 'INCREMENTAL'
    print(f"[INFO] map_encounter mode={mode}; reason={('explicit request' if force_full_rebuild else reason)}")
    print(f'[INFO] Captured source versions: {source_versions}')
    encounter_version = source_versions[config.source_table]
    lookup_version = source_versions[config.code_value_table]
    lookup = _prepare_code_lookup(config, lookup_version)
    changed_bound_encounters = build_encounter_event_bounds(full_refresh=full_rebuild, config=config)
    changed_activity_encounters = build_encounter_activity_bounds(full_refresh=full_rebuild, config=config, event_changed_encounters=changed_bound_encounters)
    if full_rebuild:
        raw = _read_snapshot(config.source_table, encounter_version)
        target_rows = _build_target_rows(raw=raw, lookup=lookup, trigger_rows=None, run_id=run_id, run_timestamp=run_timestamp, full_rebuild=True, source_version=encounter_version, config=config)
        change_set = target_rows.withColumn('_source_change_type', F.lit('upsert'))
    else:
        encounter_start = state[config.source_table] + 1
        lookup_start = state[config.code_value_table] + 1
        encounter_upserts, delete_keys, encounter_triggers = _latest_encounter_cdf(config, encounter_start, encounter_version)
        try:
            changed_codes = _changed_lookup_codes(config, lookup_start, lookup_version)
        except OverflowError as exc:
            print(f'[INFO] {exc}. Switching this run to a full rebuild.')
            raw = _read_snapshot(config.source_table, encounter_version)
            target_rows = _build_target_rows(raw=raw, lookup=lookup, trigger_rows=None, run_id=run_id, run_timestamp=run_timestamp, full_rebuild=True, source_version=encounter_version, config=config)
            change_set = target_rows.withColumn('_source_change_type', F.lit('upsert'))
            full_rebuild = True
            mode = 'FULL_REBUILD'
        else:
            lookup_affected = None
            raw_snapshot = None
            if changed_codes:
                raw_snapshot = _read_snapshot(config.source_table, encounter_version)
                lookup_affected = _raw_rows_affected_by_lookup_codes(raw_snapshot, changed_codes)
            bounds_affected = None
            changed_activity_frames = [frame for frame in (changed_bound_encounters, changed_activity_encounters) if frame is not None]
            if changed_activity_frames:
                changed_bound_ids = reduce(lambda left, right: left.unionByName(right), [frame.select(F.col('ENCNTR_ID').cast('long').alias('ENCNTR_ID')) for frame in changed_activity_frames]).filter(F.col('ENCNTR_ID').isNotNull()).dropDuplicates(['ENCNTR_ID'])
                if changed_bound_ids.take(1):
                    if raw_snapshot is None:
                        raw_snapshot = _read_snapshot(config.source_table, encounter_version)
                    bounds_affected = raw_snapshot.alias('_bounds_raw').join(changed_bound_ids.alias('_bounds_ids'), F.col('_bounds_raw.ENCNTR_ID').cast('long') == F.col('_bounds_ids.ENCNTR_ID'), 'left_semi')
            raw_upserts = _union_raw_sources(encounter_upserts, lookup_affected, bounds_affected)
            trigger_rows = _union_trigger_rows(encounter_triggers, lookup_affected, bounds_affected)
            target_rows = None
            if raw_upserts is not None:
                target_rows = _build_target_rows(raw=raw_upserts, lookup=lookup, trigger_rows=trigger_rows, run_id=run_id, run_timestamp=run_timestamp, full_rebuild=False, source_version=encounter_version, config=config)
            delete_keys = _remove_upserted_delete_keys(delete_keys, target_rows)
            frames: List[DataFrame] = []
            if target_rows is not None:
                frames.append(target_rows.withColumn('_source_change_type', F.lit('upsert')))
            if delete_keys is not None:
                frames.append(_delete_change_rows(delete_keys))
            if frames:
                change_set = reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), frames)
            else:
                empty_schema = T.StructType(list(schema_map_encounter.fields) + [T.StructField('_source_change_type', T.StringType(), False)])
                change_set = spark.createDataFrame([], empty_schema)
    _PENDING_ENCOUNTER_PLAN = {'config': config, 'run_id': run_id, 'run_timestamp': run_timestamp, 'source_versions': source_versions, 'full_rebuild': full_rebuild, 'mode': mode}
    return change_set

def _validate_change_set(changes: DataFrame, full_rebuild: bool) -> None:
    if changes.filter(F.col('ENCNTR_ID').isNull()).limit(1).collect():
        raise ValueError('map_encounter change set contains a null ENCNTR_ID')
    duplicate = changes.groupBy('ENCNTR_ID').count().filter(F.col('count') > 1).limit(1).collect()
    if duplicate:
        raise ValueError(f'map_encounter change set contains duplicate keys: {duplicate[0].asDict()}')
    invalid_action = changes.filter(~F.col('_source_change_type').isin('upsert', 'delete')).limit(1).collect()
    if invalid_action:
        raise ValueError('Unexpected _source_change_type in map_encounter changes')
    if full_rebuild and changes.filter(F.col('_source_change_type') == 'delete').limit(1).collect():
        raise ValueError('Full rebuild change set unexpectedly contains deletes')

def _overwrite_target(upserts: DataFrame, config: MapEncounterConfig) -> None:
    bronze_project_contract(
        upserts,
        config.target_table,
    ).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('delta.enableChangeDataFeed', 'true').option('delta.enableDeletionVectors', 'true').option('delta.enableRowTracking', 'true').saveAsTable(config.target_table)

def _merge_target(changes: DataFrame, config: MapEncounterConfig) -> None:
    target = DeltaTable.forName(spark, config.target_table)
    delete_keys = (
        changes.filter(F.col('_source_change_type') == 'delete')
        .select('ENCNTR_ID')
        .dropDuplicates(['ENCNTR_ID'])
    )
    upserts = bronze_project_contract(
        changes.filter(F.col('_source_change_type') == 'upsert')
        .drop('_source_change_type'),
        config.target_table,
    )
    if delete_keys.take(1):
        target.alias('t').merge(
            delete_keys.alias('s'),
            't.ENCNTR_ID = s.ENCNTR_ID',
        ).whenMatchedDelete().execute()
    if upserts.take(1):
        assignments = {
            column_name: F.col(f's.{column_name}')
            for column_name in upserts.columns
        }
        update_columns = [
            column_name for column_name in upserts.columns
            if column_name != 'ENCNTR_ID'
        ]
        changed = ' OR '.join(
            f'NOT (t.`{column_name}` <=> s.`{column_name}`)'
            for column_name in update_columns
        )
        target.alias('t').merge(
            upserts.alias('s'),
            't.ENCNTR_ID = s.ENCNTR_ID',
        ).whenMatchedUpdate(
            condition=changed or 'false',
            set=assignments,
        ).whenNotMatchedInsert(values=assignments).execute()

def update_map_encounter_table(source_df: DataFrame) -> Dict[str, object]:
    """
    Apply a change set produced by create_encounter_mapping_incr.

    Full rebuilds overwrite the old incompatible schema atomically. Incremental
    runs use one Delta MERGE for inserts, changed updates and physical deletes.
    Pipeline state advances only after the target operation succeeds.
    """
    global _PENDING_ENCOUNTER_PLAN
    if not _PENDING_ENCOUNTER_PLAN:
        raise RuntimeError('No pending encounter plan. Call create_encounter_mapping_incr first.')
    plan = dict(_PENDING_ENCOUNTER_PLAN)
    config: MapEncounterConfig = plan['config']
    run_id = str(plan['run_id'])
    source_versions = dict(plan['source_versions'])
    full_rebuild = bool(plan['full_rebuild'])
    mode = str(plan['mode'])
    changes = source_df
    metrics: Dict[str, object] = {}
    try:
        action_counts = {row['_source_change_type']: int(row['count']) for row in changes.groupBy('_source_change_type').count().collect()}
        metrics['upsert_rows'] = action_counts.get('upsert', 0)
        metrics['delete_rows'] = action_counts.get('delete', 0)
        metrics['full_rebuild'] = full_rebuild
        _validate_change_set(changes, full_rebuild)
        if full_rebuild:
            if metrics['upsert_rows'] == 0:
                raise RuntimeError('Refusing to replace map_encounter with an empty full rebuild')
            upserts = changes.filter(F.col('_source_change_type') == 'upsert').drop('_source_change_type')
            _overwrite_target(upserts, config)
        elif metrics['upsert_rows'] or metrics['delete_rows']:
            _merge_target(changes, config)
        else:
            print('[INFO] No material map_encounter changes were found')
        _set_target_metadata(config)
        completed_at = datetime.now(timezone.utc)
        _commit_pipeline_state(config, source_versions, run_id, completed_at)
        _write_audit_event(config, run_id, 'SUCCESS', mode, 'map_encounter update completed', source_versions, metrics)
        print(f"[SUCCESS] map_encounter completed: {metrics['upsert_rows']:,} upserts, {metrics['delete_rows']:,} deletes, full_rebuild={full_rebuild}")
        return metrics
    except Exception as exc:
        _write_audit_event(config, run_id, 'FAILED', mode, str(exc), source_versions, metrics)
        raise
    finally:
        None
        _release_caches()
        _PENDING_ENCOUNTER_PLAN = {}

def run_map_encounter_update(force_full_rebuild: bool=False, run_post_checks: bool=False, config: MapEncounterConfig=MAP_ENCOUNTER_CONFIG) -> Dict[str, object]:
    """
    Production entry point replacing the original map_encounter execution cell.
    """
    run_id = str(uuid.uuid4())
    run_timestamp = datetime.now(timezone.utc)
    try:
        changes = create_encounter_mapping_incr(force_full_rebuild=force_full_rebuild, run_id=run_id, run_timestamp=run_timestamp, config=config)
        metrics = update_map_encounter_table(changes)
        if run_post_checks:
            metrics['post_deployment_checks'] = run_map_encounter_post_deployment_checks(config)
        return metrics
    except Exception as exc:
        if not _PENDING_ENCOUNTER_PLAN:
            try:
                _ensure_control_tables(config)
                _write_audit_event(config, run_id, 'FAILED', 'FULL_REBUILD' if force_full_rebuild else 'AUTO', str(exc), None, None)
            except Exception:
                pass
        raise

def run_map_encounter_post_deployment_checks(config: MapEncounterConfig=MAP_ENCOUNTER_CONFIG) -> Dict[str, int]:
    """
    Deep checks intended for first deployment or controlled validation runs.

    These checks scan the full source and target and should not run on every
    scheduled increment.
    """
    source = spark.table(config.source_table).select(_checked_long(F.col('ENCNTR_ID'), 'MILL_ENCOUNTER.ENCNTR_ID validation', required=True).alias('ENCNTR_ID'), F.col('ARRIVE_DT_TM').alias('SOURCE_ARRIVE_DT_TM'), F.col('DEPART_DT_TM').alias('SOURCE_DEPART_DT_TM'))
    target = spark.table(config.target_table).select('ENCNTR_ID', F.col('ARRIVE_DT_TM').alias('TARGET_ARRIVE_DT_TM'), F.col('DEPART_DT_TM').alias('TARGET_DEPART_DT_TM'))
    source_ids = source.select('ENCNTR_ID')
    target_ids = target.select('ENCNTR_ID')
    joined = source.join(target, 'ENCNTR_ID', 'inner')
    results = {'source_rows': source_ids.count(), 'target_rows': target_ids.count(), 'source_rows_missing_from_target': source_ids.join(target_ids, 'ENCNTR_ID', 'left_anti').count(), 'target_rows_missing_from_source': target_ids.join(source_ids, 'ENCNTR_ID', 'left_anti').count(), 'duplicate_target_encounter_ids': target_ids.groupBy('ENCNTR_ID').count().filter(F.col('count') > 1).count(), 'null_target_encounter_ids': target_ids.filter(F.col('ENCNTR_ID').isNull()).count(), 'raw_arrival_mismatches': joined.filter(~F.col('SOURCE_ARRIVE_DT_TM').eqNullSafe(F.col('TARGET_ARRIVE_DT_TM'))).count(), 'raw_departure_mismatches': joined.filter(~F.col('SOURCE_DEPART_DT_TM').eqNullSafe(F.col('TARGET_DEPART_DT_TM'))).count()}
    failures = {key: value for key, value in results.items() if key not in {'source_rows', 'target_rows'} and value != 0}
    if results['source_rows'] != results['target_rows']:
        failures['source_target_row_count_difference'] = results['source_rows'] - results['target_rows']
    target_schema = {field.name: field.dataType.simpleString() for field in spark.table(config.target_table).schema.fields}
    if target_schema.get('ENCNTR_ID') != 'bigint':
        failures['ENCNTR_ID_not_bigint'] = 1
    if target_schema.get('PERSON_ID') != 'bigint':
        failures['PERSON_ID_not_bigint'] = 1
    if failures:
        raise AssertionError(f'map_encounter post-deployment checks failed: {failures}')
    print(f'[OK] map_encounter post-deployment checks passed: {results}')
    return results

try:
    _pipeline_run_recoverable('map_encounter', _PIPELINE_FULL_REFRESH, lambda: run_map_encounter_update(force_full_rebuild=False, run_post_checks=False), lambda: run_map_encounter_update(force_full_rebuild=True, run_post_checks=True))
    _PIPELINE_UPDATED_TARGETS.extend(['4_prod.bronze.map_encounter'])
    _pipeline_mark_component_complete('map_encounter', ['4_prod.bronze.map_encounter'])
    _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_encounter'})
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

