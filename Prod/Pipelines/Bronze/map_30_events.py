# Databricks notebook source
# MAGIC %md
# MAGIC # Map 30 — Event Domains
# MAGIC
# MAGIC Components: numeric, text, date, nomenclature and coded events. This notebook is executed by `map_pipeline` in the shared map runtime.

# COMMAND ----------

if "_PIPELINE_RUN_ID" not in globals():
    raise RuntimeError("Run this component through map_pipeline so shared contracts, checkpoints and audit state are initialized.")

# COMMAND ----------

_pipeline_component_start("map_numeric_events")
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
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


_MP30_PERSIST_STAGES: Dict[int, Tuple[DataFrame, str]] = {}


def _mp30_sql_identifier(table_name: str) -> str:
    tick = chr(96)
    return '.'.join((f'{tick}{part.replace(tick, tick + tick)}{tick}' for part in table_name.split('.')))


def _mp30_stage_prefix() -> str:
    """Resolve the catalog.schema that run-scoped staging tables are written into."""
    candidate = globals().get('_PIPELINE_TARGET_PREFIX')
    if isinstance(candidate, str) and candidate.count('.') == 1:
        return candidate
    for config_name in ('MAP_TEXT_EVENTS_CONFIG', 'MAP_NUMERIC_EVENTS_CONFIG'):
        target = getattr(globals().get(config_name), 'target_table', None)
        if isinstance(target, str) and target.count('.') == 2:
            return target.rsplit('.', 1)[0]
    nomen_target = globals().get('MAP_NOMEN_TARGET')
    if isinstance(nomen_target, str) and nomen_target.count('.') == 2:
        return nomen_target.rsplit('.', 1)[0]
    raise RuntimeError('map_30_events could not resolve a schema for persist staging')


def _mp30_persist_stage(frame):
    """Materialize an intermediate frame to a run-scoped Delta table and read it back.

    Serverless supports no .cache()/.persist()/.localCheckpoint(), so a frame referenced by
    more than one Spark action re-executes its entire upstream DAG on every action. Staging
    replaces that with one write plus cheap table reads. Stage names match the _map_common
    orphan-staging sweep so an aborted run cannot leave the table behind, and a write failure
    degrades to the unstaged frame rather than failing the component.
    """
    if frame is None:
        return frame
    if id(frame) in _MP30_PERSIST_STAGES:
        return frame
    columns = frame.columns
    if len(set(columns)) != len(columns):
        print('[WARN] persist stage skipped: frame has duplicate column names and cannot be staged')
        return frame
    stage_table = f'{_mp30_stage_prefix()}.map_persist__changes_stage_{uuid.uuid4().hex}'
    try:
        frame.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(stage_table)
    except Exception as stage_error:
        print(f'[WARN] persist stage write failed for {stage_table}; continuing unmaterialized: {str(stage_error)[:500]}')
        try:
            spark.sql(f'DROP TABLE IF EXISTS {_mp30_sql_identifier(stage_table)}')
        except Exception:
            pass
        return frame
    staged = spark.table(stage_table)
    _MP30_PERSIST_STAGES[id(staged)] = (staged, stage_table)
    return staged


def _mp30_persist_stage_drop(frame) -> None:
    if frame is None:
        return
    entry = _MP30_PERSIST_STAGES.pop(id(frame), None)
    if entry is None:
        return
    try:
        spark.sql(f'DROP TABLE IF EXISTS {_mp30_sql_identifier(entry[1])}')
    except Exception as drop_error:
        print(f'[WARN] persist stage drop failed for {entry[1]}; the next pipeline startup sweep will retry: {str(drop_error)[:500]}')


@dataclass(frozen=True)
class MapNumericEventsConfig:
    """Configuration for the scoped canonical map_numeric_events build.

    numeric_semantic_only is True, so the canonical numeric table is scoped to
    numerically meaningful events: rows whose IN_NUMERIC_SCOPE_IND is true because the
    Millennium event class is Numeric (233) or because a numeric token was parsed from
    the result text. Rows outside that scope are published by the text lane instead of
    padding a measurement-shaped table with values that were never numeric.

    Scope is a curation boundary, not a retention decision. 4_prod.raw.mill_ce_string_result
    remains the retention layer and keeps every source row unfiltered. The gates that read
    this flag are _mne_full_rebuild() and _mne_incremental_update(); the incremental gate
    marks rows that leave the scope as deleted rather than dropping them silently.
    """
    target_table: str = '4_prod.bronze.map_numeric_events'
    string_result_table: str = '4_prod.raw.mill_ce_string_result'
    clinical_event_table: str = '4_prod.raw.mill_clinical_event'
    code_value_table: str = '3_lookup.mill.mill_code_value'
    order_catalog_table: str = '3_lookup.mill.mill_order_catalog'
    manual_map_table: str = '3_lookup.omop.barts_new_maps'
    concept_table: str = '3_lookup.omop.concept'
    state_table: str = '6_mgmt.bronze.map_numeric_events_pipeline_state'
    audit_table: str = '6_mgmt.bronze.map_numeric_events_pipeline_audit'
    pipeline_name: str = 'map_numeric_events_v2'
    trust_scope: Optional[str] = 'Barts'
    numeric_semantic_only: bool = True
    broadcast_event_id_limit: int = 10000000
    cdf_retention_days: int = 30
MAP_NUMERIC_EVENTS_CONFIG = MapNumericEventsConfig()

def _mne_source_tables(config: MapNumericEventsConfig) -> Tuple[str, ...]:
    return (config.string_result_table, config.clinical_event_table, config.code_value_table, config.order_catalog_table, config.manual_map_table, config.concept_table)
map_numeric_events_comment = 'Source-aligned latest-state bronze foundation for Millennium CE_STRING_RESULT rows. One row is retained per EVENT_ID, scoped to numerically meaningful events (IN_NUMERIC_SCOPE_IND: the Millennium event class is Numeric, or a numeric token was parsed from the result text); within that scope no validity or result-status filtering is applied. Rows whose result text does not parse as a number are published by map_text_events instead, and 4_prod.raw.mill_ce_string_result remains the retention layer holding every source row. Raw result text, loss-aware numeric parsing, clinical-event and parent context, reference ranges, status, provenance, lookup values, OMOP mapping candidates, source Delta versions, and deletion/current-state flags are retained so downstream layers can apply their own rules.'

def _mne_field(name: str, data_type: T.DataType, comment: str) -> T.StructField:
    return T.StructField(name, data_type, True, metadata={'comment': comment})
schema_map_numeric_events = T.StructType([_mne_field('EVENT_ID', T.LongType(), 'Unique logical clinical event identifier.'), _mne_field('ENCNTR_ID', T.LongType(), 'Canonical encounter identifier; clinical-event value preferred.'), _mne_field('PERSON_ID', T.LongType(), 'Person identifier from the clinical event.'), _mne_field('ORDER_ID', T.LongType(), 'Order identifier from the clinical event.'), _mne_field('EVENT_CLASS_CD', T.LongType(), 'Clinical-event storage class code.'), _mne_field('PERFORMED_PRSNL_ID', T.LongType(), 'Personnel identifier for the performer.'), _mne_field('NUMERIC_RESULT', T.DoubleType(), 'Parsed numeric token. Raw text and parse method are retained separately.'), _mne_field('UNIT_OF_MEASURE_CD', T.LongType(), 'Unit code from CE_STRING_RESULT; sentinel zero is preserved.'), _mne_field('UNIT_OF_MEASURE_DISPLAY', T.StringType(), 'Resolved unit display; null remains null for unmapped/sentinel codes.'), _mne_field('EVENT_TITLE_TEXT', T.StringType(), 'Clinical-event title text.'), _mne_field('EVENT_CD', T.LongType(), 'Clinical event code.'), _mne_field('EVENT_CD_DISPLAY', T.StringType(), 'Resolved description/display for EVENT_CD.'), _mne_field('CATALOG_CD', T.LongType(), 'Order catalog code.'), _mne_field('CATALOG_DISPLAY', T.StringType(), 'Resolved catalog description/mnemonic from the matching catalog row.'), _mne_field('CATALOG_TYPE_CD', T.LongType(), 'Order catalog type code.'), _mne_field('CATALOG_TYPE_DISPLAY', T.StringType(), 'Resolved description/display for CATALOG_TYPE_CD.'), _mne_field('CONTRIBUTOR_SYSTEM_CD', T.LongType(), 'Contributor system code.'), _mne_field('CONTRIBUTOR_SYSTEM_DISPLAY', T.StringType(), 'Resolved contributor-system display.'), _mne_field('REFERENCE_NBR', T.StringType(), 'Source reference number.'), _mne_field('PARENT_EVENT_ID', T.LongType(), 'Parent logical clinical event identifier.'), _mne_field('NORMALCY_CD', T.LongType(), 'Normalcy code.'), _mne_field('NORMALCY_DISPLAY', T.StringType(), 'Resolved normalcy display.'), _mne_field('ENTRY_MODE_CD', T.LongType(), 'Entry mode code.'), _mne_field('ENTRY_MODE_DISPLAY', T.StringType(), 'Resolved entry-mode display.'), _mne_field('NORMAL_LOW', T.DoubleType(), 'Parsed low reference-range numeric token.'), _mne_field('NORMAL_HIGH', T.DoubleType(), 'Parsed high reference-range numeric token.'), _mne_field('PERFORMED_DT_TM', T.TimestampType(), 'Date/time the result was performed or authored.'), _mne_field('CLINSIG_UPDT_DT_TM', T.TimestampType(), 'Clinically significant update timestamp.'), _mne_field('PARENT_EVENT_TITLE_TEXT', T.StringType(), 'Parent clinical-event title.'), _mne_field('PARENT_EVENT_CD', T.LongType(), 'Parent clinical event code.'), _mne_field('PARENT_EVENT_CD_DISPLAY', T.StringType(), 'Resolved parent event-code display.'), _mne_field('PARENT_CATALOG_CD', T.LongType(), 'Parent order catalog code.'), _mne_field('PARENT_CATALOG_DISPLAY', T.StringType(), 'Resolved parent catalog display.'), _mne_field('PARENT_CATALOG_TYPE_CD', T.LongType(), 'Parent order catalog type code.'), _mne_field('PARENT_CATALOG_TYPE_DISPLAY', T.StringType(), 'Resolved parent catalog-type display.'), _mne_field('PARENT_REFERENCE_NBR', T.StringType(), 'Parent source reference number.'), _mne_field('ADC_UPDT', T.TimestampType(), 'Greatest meaningful upstream update timestamp; never used as the CDF checkpoint.'), _mne_field('OMOP_MANUAL_TABLE', T.StringType(), 'Selected manual OMOP target table.'), _mne_field('OMOP_MANUAL_COLUMN', T.StringType(), 'Selected manual OMOP target field.'), _mne_field('OMOP_MANUAL_CONCEPT', T.StringType(), 'Selected manual concept identifier, retained as text for compatibility.'), _mne_field('OMOP_MANUAL_UNITS', T.StringType(), 'Selected manual unit concept identifier, retained as text for compatibility.'), _mne_field('OMOP_MANUAL_CONCEPT_NAME', T.StringType(), 'Selected concept name.'), _mne_field('OMOP_MANUAL_STANDARD_CONCEPT', T.StringType(), 'Selected concept standard-concept flag.'), _mne_field('OMOP_MANUAL_CONCEPT_DOMAIN', T.StringType(), 'Selected concept domain.'), _mne_field('OMOP_MANUAL_CONCEPT_CLASS', T.StringType(), 'Selected concept class.'), _mne_field('STRING_RESULT_TEXT', T.StringType(), 'Unmodified CE_STRING_RESULT result text.'), _mne_field('RESULT_TEXT_EFFECTIVE', T.StringType(), 'Best available raw result text; string-result value preferred over CE RESULT_VAL.'), _mne_field('STRING_RESULT_FORMAT_CD', T.LongType(), 'String-result format code.'), _mne_field('STRING_RESULT_FORMAT_DISPLAY', T.StringType(), 'Resolved string-result format display.'), _mne_field('EQUATION_ID', T.LongType(), 'Calculation equation identifier from CE_STRING_RESULT.'), _mne_field('CALCULATION_EQUATION', T.StringType(), 'Calculation equation text from CE_STRING_RESULT.'), _mne_field('LAST_NORM_DT_TM', T.TimestampType(), 'Last normalization date/time from CE_STRING_RESULT.'), _mne_field('FEASIBLE_IND', T.LongType(), 'Source feasible indicator; retained without interpretation.'), _mne_field('INACCURATE_IND', T.LongType(), 'Source inaccurate indicator; retained without filtering.'), _mne_field('MODIFY_FLAG', T.LongType(), 'Source modification flag.'), _mne_field('STRING_LONG_TEXT_ID', T.LongType(), 'Associated long-text identifier.'), _mne_field('RESULT_OPERATOR', T.StringType(), 'Normalized comparator parsed from result text: <, <=, >, or >=.'), _mne_field('RESULT_SUFFIX_TEXT', T.StringType(), 'Text following a parsed numeric token, such as a unit or percent sign.'), _mne_field('NUMERIC_PARSE_METHOD', T.StringType(), 'DIRECT, COMPARATOR, THOUSANDS, NUMERIC_WITH_SUFFIX, BLANK, or NON_NUMERIC.'), _mne_field('NUMERIC_PARSE_SUCCESS_IND', T.BooleanType(), 'True when a numeric token was parsed.'), _mne_field('NUMERIC_FINITE_IND', T.BooleanType(), 'True when the parsed value is neither NaN nor positive/negative infinity.'), _mne_field('POTENTIAL_IDENTIFIER_IND', T.BooleanType(), 'Flags long/leading-zero digit strings that may be identifiers rather than measurements.'), _mne_field('EVENT_CLASS_NUMERIC_IND', T.BooleanType(), 'True when EVENT_CLASS_CD is Millennium Numeric class 233.'), _mne_field('NUMERIC_SEMANTIC_IND', T.BooleanType(), 'True when event class is numeric or a numeric token was parsed.'), _mne_field('IN_NUMERIC_SCOPE_IND', T.BooleanType(), 'Current semantic-scope flag; retained even when semantic-only mode is disabled.'), _mne_field('NUMERIC_RESULT_FLOAT_COMPAT', T.FloatType(), 'Optional legacy float representation; may be lossy and must not be used as the canonical value.'), _mne_field('STRING_RESULT_NORMAL_LOW_TEXT', T.StringType(), 'Raw normal-low text from CE_STRING_RESULT.'), _mne_field('STRING_RESULT_NORMAL_HIGH_TEXT', T.StringType(), 'Raw normal-high text from CE_STRING_RESULT.'), _mne_field('STRING_RESULT_CRITICAL_LOW_TEXT', T.StringType(), 'Raw critical-low text from CE_STRING_RESULT.'), _mne_field('STRING_RESULT_CRITICAL_HIGH_TEXT', T.StringType(), 'Raw critical-high text from CE_STRING_RESULT.'), _mne_field('CLINICAL_EVENT_NORMAL_LOW_TEXT', T.StringType(), 'Raw normal-low text from CLINICAL_EVENT.'), _mne_field('CLINICAL_EVENT_NORMAL_HIGH_TEXT', T.StringType(), 'Raw normal-high text from CLINICAL_EVENT.'), _mne_field('CLINICAL_EVENT_CRITICAL_LOW_TEXT', T.StringType(), 'Raw critical-low text from CLINICAL_EVENT.'), _mne_field('CLINICAL_EVENT_CRITICAL_HIGH_TEXT', T.StringType(), 'Raw critical-high text from CLINICAL_EVENT.'), _mne_field('NORMAL_LOW_TEXT', T.StringType(), 'Effective raw normal-low text; clinical-event value preferred.'), _mne_field('NORMAL_HIGH_TEXT', T.StringType(), 'Effective raw normal-high text; clinical-event value preferred.'), _mne_field('CRITICAL_LOW_TEXT', T.StringType(), 'Effective raw critical-low text; clinical-event value preferred.'), _mne_field('CRITICAL_HIGH_TEXT', T.StringType(), 'Effective raw critical-high text; clinical-event value preferred.'), _mne_field('NORMAL_LOW_OPERATOR', T.StringType(), 'Comparator parsed from NORMAL_LOW_TEXT.'), _mne_field('NORMAL_HIGH_OPERATOR', T.StringType(), 'Comparator parsed from NORMAL_HIGH_TEXT.'), _mne_field('CRITICAL_LOW_OPERATOR', T.StringType(), 'Comparator parsed from CRITICAL_LOW_TEXT.'), _mne_field('CRITICAL_HIGH_OPERATOR', T.StringType(), 'Comparator parsed from CRITICAL_HIGH_TEXT.'), _mne_field('CRITICAL_LOW', T.DoubleType(), 'Parsed low critical-range numeric token.'), _mne_field('CRITICAL_HIGH', T.DoubleType(), 'Parsed high critical-range numeric token.'), _mne_field('NORMAL_RANGE_SOURCE', T.StringType(), 'CLINICAL_EVENT, STRING_RESULT, or null according to the selected range source.'), _mne_field('STRING_RESULT_VALID_FROM_DT_TM', T.TimestampType(), 'Beginning of validity for the selected string-result row.'), _mne_field('STRING_RESULT_VALID_UNTIL_DT_TM', T.TimestampType(), 'End of validity for the selected string-result row.'), _mne_field('STRING_RESULT_UPDT_DT_TM', T.TimestampType(), 'Millennium update timestamp for the selected string-result row.'), _mne_field('STRING_RESULT_UPDT_CNT', T.LongType(), 'Millennium update counter for the selected string-result row.'), _mne_field('STRING_RESULT_LAST_UTC_TS', T.TimestampType(), 'Last UTC timestamp from the selected string-result row.'), _mne_field('STRING_RESULT_ADC_UPDT', T.TimestampType(), 'ADC update timestamp from CE_STRING_RESULT; may be null.'), _mne_field('STRING_RESULT_EFFECTIVE_UPDT_DT_TM', T.TimestampType(), 'COALESCE of source update timestamps for audit/backfill, not CDF state.'), _mne_field('STRING_RESULT_ENCNTR_ID', T.LongType(), 'Encounter identifier supplied by CE_STRING_RESULT.'), _mne_field('STRING_RESULT_ORGANIZATION_ID', T.LongType(), 'Organization identifier supplied by CE_STRING_RESULT.'), _mne_field('STRING_RESULT_TRUST', T.StringType(), 'Trust supplied by CE_STRING_RESULT.'), _mne_field('CLINICAL_EVENT_ID', T.LongType(), 'Physical/versioned CLINICAL_EVENT row identifier.'), _mne_field('CLINICAL_EVENT_ENCNTR_ID', T.LongType(), 'Encounter identifier supplied by CLINICAL_EVENT.'), _mne_field('CLINICAL_EVENT_ORGANIZATION_ID', T.LongType(), 'Organization identifier supplied by CLINICAL_EVENT.'), _mne_field('CLINICAL_EVENT_TRUST', T.StringType(), 'Trust supplied by CLINICAL_EVENT.'), _mne_field('ORGANIZATION_ID', T.LongType(), 'Canonical organization identifier; clinical-event value preferred.'), _mne_field('CLINICAL_EVENT_VALID_FROM_DT_TM', T.TimestampType(), 'Beginning of validity for the selected clinical-event row.'), _mne_field('CLINICAL_EVENT_VALID_UNTIL_DT_TM', T.TimestampType(), 'End of validity for the selected clinical-event row.'), _mne_field('CLINICAL_EVENT_UPDT_DT_TM', T.TimestampType(), 'Millennium update timestamp for the selected clinical-event row.'), _mne_field('CLINICAL_EVENT_UPDT_CNT', T.LongType(), 'Millennium update counter for the selected clinical-event row.'), _mne_field('CLINICAL_EVENT_ADC_UPDT', T.TimestampType(), 'ADC update timestamp from the selected clinical-event row.'), _mne_field('EVENT_START_DT_TM', T.TimestampType(), 'Clinical-event start date/time.'), _mne_field('EVENT_END_DT_TM', T.TimestampType(), 'Clinical-event end date/time.'), _mne_field('SERIES_REF_NBR', T.StringType(), 'Clinical-event series reference number.'), _mne_field('ACCESSION_NBR', T.StringType(), 'Clinical-event accession number.'), _mne_field('EVENT_CLASS_DISPLAY', T.StringType(), 'Resolved display for EVENT_CLASS_CD.'), _mne_field('EVENT_TAG', T.StringType(), 'Clinical-event display tag.'), _mne_field('RESULT_VAL', T.StringType(), 'Raw CLINICAL_EVENT result value retained for comparison/fallback.'), _mne_field('RESULT_UNITS_CD', T.LongType(), 'Result unit code from CLINICAL_EVENT.'), _mne_field('RESULT_UNITS_DISPLAY', T.StringType(), 'Resolved display for RESULT_UNITS_CD.'), _mne_field('RESULT_TIME_UNITS_CD', T.LongType(), 'Result time-unit code from CLINICAL_EVENT.'), _mne_field('RESULT_TIME_UNITS_DISPLAY', T.StringType(), 'Resolved display for RESULT_TIME_UNITS_CD.'), _mne_field('EFFECTIVE_UNIT_OF_MEASURE_CD', T.LongType(), 'Non-zero string-result unit preferred, then non-zero CE result unit, with sentinels retained as fallback.'), _mne_field('TASK_ASSAY_CD', T.LongType(), 'Task/assay code from CLINICAL_EVENT.'), _mne_field('TASK_ASSAY_DISPLAY', T.StringType(), 'Resolved task/assay display.'), _mne_field('RESULT_STATUS_CD', T.LongType(), 'Result status code retained without filtering.'), _mne_field('RESULT_STATUS_DISPLAY', T.StringType(), 'Resolved result-status display.'), _mne_field('RECORD_STATUS_CD', T.LongType(), 'Record status code retained without filtering.'), _mne_field('RECORD_STATUS_DISPLAY', T.StringType(), 'Resolved record-status display.'), _mne_field('AUTHENTIC_FLAG', T.LongType(), 'Clinical-event authentication flag.'), _mne_field('PUBLISH_FLAG', T.LongType(), 'Clinical-event publish/viewability flag.'), _mne_field('QC_REVIEW_CD', T.LongType(), 'Quality-control review code.'), _mne_field('QC_REVIEW_DISPLAY', T.StringType(), 'Resolved quality-control review display.'), _mne_field('NORMALCY_METHOD_CD', T.LongType(), 'Normalcy derivation method code.'), _mne_field('NORMALCY_METHOD_DISPLAY', T.StringType(), 'Resolved normalcy-method display.'), _mne_field('EVENT_RELTN_CD', T.LongType(), 'Clinical-event relationship code.'), _mne_field('EVENT_RELTN_DISPLAY', T.StringType(), 'Resolved clinical-event relationship display.'), _mne_field('SOURCE_CD', T.LongType(), 'Clinical-event result source code.'), _mne_field('SOURCE_DISPLAY', T.StringType(), 'Resolved result-source display.'), _mne_field('VERIFIED_DT_TM', T.TimestampType(), 'Date/time the clinical event was verified.'), _mne_field('VERIFIED_PRSNL_ID', T.LongType(), 'Personnel identifier for the verifier.'), _mne_field('PERFORMED_TZ', T.LongType(), 'Time-zone code associated with PERFORMED_DT_TM.'), _mne_field('VERIFIED_TZ', T.LongType(), 'Time-zone code associated with VERIFIED_DT_TM.'), _mne_field('SRC_EVENT_ID', T.LongType(), 'Source-system event identifier.'), _mne_field('SRC_CLINSIG_UPDT_DT_TM', T.TimestampType(), 'Source clinically-significant update timestamp.'), _mne_field('DEVICE_FREE_TXT', T.StringType(), 'Device free text from CLINICAL_EVENT.'), _mne_field('PARENT_CLINICAL_EVENT_ID', T.LongType(), 'Physical/versioned parent CLINICAL_EVENT row identifier.'), _mne_field('PARENT_EVENT_CLASS_CD', T.LongType(), 'Parent event class code.'), _mne_field('PARENT_EVENT_CLASS_DISPLAY', T.StringType(), 'Resolved parent event-class display.'), _mne_field('PARENT_VALID_FROM_DT_TM', T.TimestampType(), 'Beginning of validity for the selected parent row.'), _mne_field('PARENT_VALID_UNTIL_DT_TM', T.TimestampType(), 'End of validity for the selected parent row.'), _mne_field('PARENT_UPDT_DT_TM', T.TimestampType(), 'Millennium update timestamp for the selected parent row.'), _mne_field('PARENT_UPDT_CNT', T.LongType(), 'Millennium update counter for the selected parent row.'), _mne_field('PARENT_ADC_UPDT', T.TimestampType(), 'ADC update timestamp from the selected parent row.'), _mne_field('PARENT_RESULT_STATUS_CD', T.LongType(), 'Parent result status retained without filtering.'), _mne_field('PARENT_RESULT_STATUS_DISPLAY', T.StringType(), 'Resolved parent result-status display.'), _mne_field('CATALOG_PRIMARY_MNEMONIC', T.StringType(), 'Primary mnemonic from the matching order catalog row.'), _mne_field('CATALOG_DEPT_DISPLAY_NAME', T.StringType(), 'Departmental display name from the matching order catalog row.'), _mne_field('CATALOG_ACTIVE_IND', T.LongType(), 'Active indicator from the matching order catalog row.'), _mne_field('PARENT_CATALOG_PRIMARY_MNEMONIC', T.StringType(), 'Primary mnemonic from the parent order catalog row.'), _mne_field('PARENT_CATALOG_DEPT_DISPLAY_NAME', T.StringType(), 'Departmental display name from the parent order catalog row.'), _mne_field('PARENT_CATALOG_ACTIVE_IND', T.LongType(), 'Active indicator from the parent order catalog row.'), _mne_field('EVENT_LABEL', T.StringType(), 'Best available event label without overwriting source-specific display columns.'), _mne_field('LOOKUP_ADC_UPDT', T.TimestampType(), 'Greatest ADC_UPDT among code-value and catalog rows used for enrichment.'), _mne_field('CLINICAL_EVENT_FOUND_IND', T.BooleanType(), 'True when a clinical-event row was found.'), _mne_field('PARENT_EVENT_FOUND_IND', T.BooleanType(), 'True when a parent clinical-event row was found.'), _mne_field('ORDER_CATALOG_FOUND_IND', T.BooleanType(), 'True when the main order-catalog row was found.'), _mne_field('PARENT_ORDER_CATALOG_FOUND_IND', T.BooleanType(), 'True when the parent order-catalog row was found.'), _mne_field('ENCOUNTER_ID_MATCH_IND', T.BooleanType(), 'Comparison of source and clinical-event encounter IDs when both exist.'), _mne_field('UNIT_CODE_CONFLICT_IND', T.BooleanType(), 'True when non-zero string-result and CE result-unit codes disagree.'), _mne_field('SOURCE_CURRENT_IND', T.BooleanType(), 'Validity flag evaluated at the fixed run timestamp; no row is filtered by it.'), _mne_field('CLINICAL_EVENT_CURRENT_IND', T.BooleanType(), 'Clinical-event validity flag evaluated at the fixed run timestamp.'), _mne_field('CLINICAL_EVENT_IN_ERROR_IND', T.BooleanType(), 'True for Millennium result-status codes 29, 30, or 31; rows are retained.'), _mne_field('SOURCE_DELETED_IND', T.BooleanType(), 'True when a changed EVENT_ID no longer has a source string-result row.'), _mne_field('OMOP_MANUAL_CONCEPT_ID', T.LongType(), 'Typed selected manual OMOP concept identifier.'), _mne_field('OMOP_MANUAL_UNIT_CONCEPT_ID', T.LongType(), 'Typed selected manual OMOP unit concept identifier.'), _mne_field('OMOP_MAPPING_VALID_IND', T.BooleanType(), 'True when the selected concept exists and is currently valid.'), _mne_field('OMOP_MAPPING_RULE_ID', T.StringType(), 'Stable identifier for the selected manual concept-mapping rule.'), _mne_field('OMOP_MAPPING_MATCH_TYPE', T.StringType(), 'EVENT_CODE or EVENT_CODE_AND_CLASS.'), _mne_field('OMOP_MAPPING_CANDIDATE_COUNT', T.LongType(), 'Number of matching concept-mapping rows.'), _mne_field('OMOP_MAPPING_DISTINCT_CONCEPT_COUNT', T.LongType(), 'Number of distinct candidate concept identifiers.'), _mne_field('OMOP_MAPPING_AMBIGUOUS_IND', T.BooleanType(), 'True when more than one distinct concept candidate exists.'), _mne_field('OMOP_MAPPING_CANDIDATE_CONCEPT_IDS', T.ArrayType(T.LongType()), 'Sorted distinct candidate concept identifiers.'), _mne_field('OMOP_UNIT_MAPPING_VALID_IND', T.BooleanType(), 'True when the selected unit concept exists and is currently valid.'), _mne_field('OMOP_UNIT_MAPPING_RULE_ID', T.StringType(), 'Stable identifier for the selected unit-mapping rule.'), _mne_field('OMOP_UNIT_MAPPING_CANDIDATE_COUNT', T.LongType(), 'Number of matching unit-mapping rows.'), _mne_field('OMOP_UNIT_MAPPING_DISTINCT_CONCEPT_COUNT', T.LongType(), 'Number of distinct unit concept candidates.'), _mne_field('OMOP_UNIT_MAPPING_AMBIGUOUS_IND', T.BooleanType(), 'True when more than one distinct unit concept candidate exists.'), _mne_field('OMOP_UNIT_MAPPING_CANDIDATE_CONCEPT_IDS', T.ArrayType(T.LongType()), 'Sorted distinct candidate unit concept identifiers.'), _mne_field('OMOP_MAPPING_ADC_UPDT', T.TimestampType(), 'Greatest update timestamp among selected/matching manual map rows.'), _mne_field('STRING_RESULT_CDF_COMMIT_VERSION', T.LongType(), 'Latest string-result CDF commit version that triggered this refresh.'), _mne_field('STRING_RESULT_CDF_COMMIT_TIMESTAMP', T.TimestampType(), 'Latest string-result CDF commit timestamp that triggered this refresh.'), _mne_field('STRING_RESULT_CDF_CHANGE_TYPE', T.StringType(), 'Latest string-result CDF change type that triggered this refresh.'), _mne_field('TRIGGER_SOURCES', T.StringType(), 'Comma-separated upstream sources that caused this row to be rebuilt.'), _mne_field('STRING_RESULT_SOURCE_VERSION', T.LongType(), 'CE_STRING_RESULT Delta version used for this row build.'), _mne_field('CLINICAL_EVENT_SOURCE_VERSION', T.LongType(), 'CLINICAL_EVENT Delta version used for this row build.'), _mne_field('CODE_VALUE_SOURCE_VERSION', T.LongType(), 'CODE_VALUE Delta version used for this row build.'), _mne_field('ORDER_CATALOG_SOURCE_VERSION', T.LongType(), 'ORDER_CATALOG Delta version used for this row build.'), _mne_field('MANUAL_MAP_SOURCE_VERSION', T.LongType(), 'Manual map Delta version used for this row build.'), _mne_field('CONCEPT_SOURCE_VERSION', T.LongType(), 'OMOP concept Delta version used for this row build.'), _mne_field('ROW_HASH', T.LongType(), 'Stable hash of source-derived row content used to avoid unchanged rewrites.'), _mne_field('PIPELINE_RUN_ID', T.StringType(), 'Unique identifier for the pipeline run that last changed this row.'), _mne_field('PIPELINE_UPDT_DT_TM', T.TimestampType(), 'Pipeline timestamp for the last target change.')])

def _mne_sql_identifier(table_name: str) -> str:
    tick = chr(96)
    return '.'.join((f'{tick}{part.replace(tick, tick + tick)}{tick}' for part in table_name.split('.')))

def _mne_escape_sql_string(value: str) -> str:
    return value.replace('\\', '\\\\').replace("'", "''")

def _mne_table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _mne_latest_delta_version(table_name: str) -> int:
    row = spark.sql(f'DESCRIBE HISTORY {_mne_sql_identifier(table_name)} LIMIT 1').select('version').first()
    if row is None:
        raise RuntimeError(f'No Delta history was returned for {table_name}')
    return int(row['version'])

def _mne_capture_source_versions(config: MapNumericEventsConfig) -> Dict[str, int]:
    return {source: _mne_latest_delta_version(source) for source in _mne_source_tables(config)}

def _m30_hardening_sql_identifier(table_name: str) -> str:
    return '.'.join('`' + part.replace('`', '``') + '`' for part in table_name.split('.'))

def _m30_hardening_snapshot_fallback_allowed(exc: BaseException) -> bool:
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

def _m30_hardening_read_pinned_snapshot(table_name: str, version: int):
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
        if not _m30_hardening_snapshot_fallback_allowed(exc):
            raise
        row = (
            spark.sql('DESCRIBE HISTORY ' + _m30_hardening_sql_identifier(table_name) + ' LIMIT 1')
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

def _m30_hardening_validate_cdf(changes):
    # Keep lazy Spark Connect failures inside the caller's recovery block.
    _ = changes.schema
    changes.limit(1).collect()
    return changes

def _mne_read_snapshot(table_name: str, version: int) -> DataFrame:
    return _m30_hardening_read_pinned_snapshot(table_name, int(version))

def _mne_read_cdf(table_name: str, starting_version: int, ending_version: int) -> Optional[DataFrame]:
    if starting_version > ending_version:
        return None
    try:
        changes = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', int(starting_version)).option('endingVersion', int(ending_version)).table(table_name)
        return _m30_hardening_validate_cdf(changes)
    except Exception as exc:
        raise RuntimeError(f'CDF could not be read for {table_name} from version {starting_version} through {ending_version}. No timestamp fallback was used, because it can silently lose null or late-arriving timestamps. Run process_numeric_events_incremental(force_full_rebuild=True).') from exc

def _mne_checked_double_id(column: Column, label: str, required: bool=False) -> Column:
    max_exact_double_integer = 9007199254740991
    null_result = F.lit(None) if required else F.lit(None).cast('long')
    return F.when(column.isNull(), null_result).when(F.isnan(column), F.lit(None)).when(column != F.floor(column), F.lit(None)).when(F.abs(column) > F.lit(max_exact_double_integer), F.lit(None)).otherwise(column.cast('long'))

def _mne_nonblank(column: Column) -> Column:
    return F.when(F.length(F.trim(column.cast('string'))) > 0, F.trim(column.cast('string')))

def _mne_stable_hash_columns(columns: Sequence[Column]) -> Column:
    normalized = [F.coalesce(column.cast('string'), F.lit('<NULL>')) for column in columns]
    return F.xxhash64(*normalized)

def _mne_align_to_schema(df: DataFrame, schema: T.StructType) -> DataFrame:
    missing = [field.name for field in schema.fields if field.name not in df.columns]
    if missing:
        raise RuntimeError(f'Numeric-events output is missing required columns: {missing}')
    return df.select(*[F.col(field.name).cast(field.dataType).alias(field.name, metadata=field.metadata) for field in schema.fields])

def _mne_latest_by_key(df: DataFrame, keys: Sequence[str], order_columns: Sequence[str]) -> DataFrame:
    ordering = [F.col(column).desc_nulls_last() for column in order_columns]
    window = Window.partitionBy(*keys).orderBy(*ordering)
    return df.withColumn('_MNE_LATEST_ROW', F.row_number().over(window)).where(F.col('_MNE_LATEST_ROW') == 1).drop('_MNE_LATEST_ROW')

def _mne_empty_long_keys(column_name: str='EVENT_ID') -> DataFrame:
    return spark.createDataFrame([], T.StructType([T.StructField(column_name, T.LongType(), True)]))

def _mne_union_event_triggers(frames: Iterable[Optional[DataFrame]]) -> DataFrame:
    usable = [frame.select('EVENT_ID', 'TRIGGER_SOURCE') for frame in frames if frame is not None]
    if not usable:
        return _mne_empty_long_keys().withColumn('TRIGGER_SOURCE', F.lit(None).cast('string'))
    return reduce(lambda left, right: left.unionByName(right), usable)

def _mne_ensure_metadata_tables(config: MapNumericEventsConfig) -> None:
    spark.sql(f'\n        CREATE TABLE IF NOT EXISTS {_mne_sql_identifier(config.state_table)} (\n            source_table STRING,\n            last_processed_version BIGINT,\n            last_processed_at TIMESTAMP,\n            last_run_id STRING\n        )\n        USING DELTA\n    ')
    spark.sql(f'\n        CREATE TABLE IF NOT EXISTS {_mne_sql_identifier(config.audit_table)} (\n            pipeline_name STRING,\n            run_id STRING,\n            event_timestamp TIMESTAMP,\n            status STRING,\n            mode STRING,\n            message STRING,\n            source_versions_json STRING,\n            metrics_json STRING\n        )\n        USING DELTA\n    ')

def _mne_read_pipeline_state(config: MapNumericEventsConfig) -> Dict[str, int]:
    if not _mne_table_exists(config.state_table):
        return {}
    rows = spark.table(config.state_table).where(F.col('source_table').isin(list(_mne_source_tables(config)))).select('source_table', 'last_processed_version').collect()
    return {row['source_table']: int(row['last_processed_version']) for row in rows if row['last_processed_version'] is not None}

def _mne_commit_pipeline_state(config: MapNumericEventsConfig, source_versions: Dict[str, int], run_id: str, completed_at: datetime) -> None:
    rows = [(source, int(version), completed_at, run_id) for source, version in source_versions.items()]
    schema = T.StructType([T.StructField('source_table', T.StringType(), False), T.StructField('last_processed_version', T.LongType(), False), T.StructField('last_processed_at', T.TimestampType(), False), T.StructField('last_run_id', T.StringType(), False)])
    updates = spark.createDataFrame(rows, schema)
    DeltaTable.forName(spark, config.state_table).alias('t').merge(updates.alias('s'), 't.source_table = s.source_table').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def _mne_write_audit_event(config: MapNumericEventsConfig, run_id: str, status: str, mode: str, message: str, source_versions: Optional[Dict[str, int]]=None, metrics: Optional[Dict[str, object]]=None) -> None:
    try:
        row = [(config.pipeline_name, run_id, datetime.now(timezone.utc), status, mode, message[:8000], json.dumps(source_versions or {}, sort_keys=True, default=str), json.dumps(metrics or {}, sort_keys=True, default=str))]
        schema = T.StructType([T.StructField('pipeline_name', T.StringType(), False), T.StructField('run_id', T.StringType(), False), T.StructField('event_timestamp', T.TimestampType(), False), T.StructField('status', T.StringType(), False), T.StructField('mode', T.StringType(), False), T.StructField('message', T.StringType(), True), T.StructField('source_versions_json', T.StringType(), True), T.StructField('metrics_json', T.StringType(), True)])
        spark.createDataFrame(row, schema).write.mode('append').saveAsTable(config.audit_table)
    except Exception as audit_exc:
        print(f'[WARN] Could not write numeric-events audit event: {audit_exc}')

def _mne_apply_trust_scope(df: DataFrame, config: MapNumericEventsConfig) -> DataFrame:
    if config.trust_scope is None or 'Trust' not in df.columns:
        return df
    return df.where(F.col('Trust') == F.lit(config.trust_scope))

def _mne_latest_string_results(config: MapNumericEventsConfig, source_version: int, event_keys: Optional[DataFrame]=None, broadcast_keys: bool=False) -> DataFrame:
    df = _mne_apply_trust_scope(_mne_read_snapshot(config.string_result_table, source_version), config).withColumn('EVENT_ID', _mne_checked_double_id(F.col('EVENT_ID'), 'CE_STRING_RESULT.EVENT_ID', required=True))
    if event_keys is not None:
        keys = event_keys.select(F.col('EVENT_ID').cast('long').alias('EVENT_ID')).dropDuplicates()
        if broadcast_keys:
            keys = F.broadcast(keys)
        df = df.join(keys, 'EVENT_ID', 'left_semi')
    df = df.withColumn('_MNE_SR_EFFECTIVE_UPDT', F.coalesce(F.col('ADC_UPDT'), F.col('UPDT_DT_TM'), F.col('VALID_FROM_DT_TM'), F.col('LAST_NORM_DT_TM'))).withColumn('_MNE_SR_TEXT_TIE', F.coalesce(F.col('STRING_RESULT_TEXT'), F.lit('')))
    df = _mne_latest_by_key(df, ['EVENT_ID'], ['UPDT_CNT', '_MNE_SR_EFFECTIVE_UPDT', 'VALID_FROM_DT_TM', 'VALID_UNTIL_DT_TM', '_MNE_SR_TEXT_TIE'])
    return df.select(F.col('EVENT_ID'), F.col('VALID_UNTIL_DT_TM').alias('SR_VALID_UNTIL_DT_TM'), F.col('VALID_FROM_DT_TM').alias('SR_VALID_FROM_DT_TM'), F.col('STRING_RESULT_TEXT').alias('SR_STRING_RESULT_TEXT'), F.col('STRING_RESULT_FORMAT_CD').cast('long').alias('SR_STRING_RESULT_FORMAT_CD'), F.col('EQUATION_ID').cast('long').alias('SR_EQUATION_ID'), F.col('LAST_NORM_DT_TM').alias('SR_LAST_NORM_DT_TM'), F.col('UNIT_OF_MEASURE_CD').cast('long').alias('SR_UNIT_OF_MEASURE_CD'), F.col('FEASIBLE_IND').cast('long').alias('SR_FEASIBLE_IND'), F.col('INACCURATE_IND').cast('long').alias('SR_INACCURATE_IND'), F.col('UPDT_DT_TM').alias('SR_UPDT_DT_TM'), F.col('UPDT_ID').cast('long').alias('SR_UPDT_ID'), F.col('UPDT_TASK').cast('long').alias('SR_UPDT_TASK'), F.col('UPDT_CNT').cast('long').alias('SR_UPDT_CNT'), F.col('UPDT_APPLCTX').cast('long').alias('SR_UPDT_APPLCTX'), F.col('MODIFY_FLAG').cast('long').alias('SR_MODIFY_FLAG'), F.col('NORMAL_LOW').alias('SR_NORMAL_LOW_TEXT'), F.col('NORMAL_HIGH').alias('SR_NORMAL_HIGH_TEXT'), F.col('CRITICAL_LOW').alias('SR_CRITICAL_LOW_TEXT'), F.col('CRITICAL_HIGH').alias('SR_CRITICAL_HIGH_TEXT'), F.col('CALCULATION_EQUATION').alias('SR_CALCULATION_EQUATION'), F.col('STRING_LONG_TEXT_ID').cast('long').alias('SR_STRING_LONG_TEXT_ID'), F.col('LAST_UTC_TS').alias('SR_LAST_UTC_TS'), F.col('ADC_UPDT').alias('SR_ADC_UPDT'), F.col('_MNE_SR_EFFECTIVE_UPDT').alias('SR_EFFECTIVE_UPDT_DT_TM'), F.col('ENCNTR_ID').cast('long').alias('SR_ENCNTR_ID'), F.col('ORGANIZATION_ID').cast('long').alias('SR_ORGANIZATION_ID'), F.col('Trust').alias('SR_TRUST'))
_MNE_CE_SOURCE_COLUMNS = ['CLINICAL_EVENT_ID', 'ENCNTR_ID', 'PERSON_ID', 'EVENT_START_DT_TM', 'EVENT_ID', 'VALID_UNTIL_DT_TM', 'EVENT_TITLE_TEXT', 'ORDER_ID', 'CATALOG_CD', 'SERIES_REF_NBR', 'ACCESSION_NBR', 'CONTRIBUTOR_SYSTEM_CD', 'REFERENCE_NBR', 'PARENT_EVENT_ID', 'EVENT_RELTN_CD', 'VALID_FROM_DT_TM', 'EVENT_CLASS_CD', 'EVENT_CD', 'EVENT_TAG', 'EVENT_END_DT_TM', 'RESULT_VAL', 'RESULT_UNITS_CD', 'RESULT_TIME_UNITS_CD', 'TASK_ASSAY_CD', 'RECORD_STATUS_CD', 'RESULT_STATUS_CD', 'AUTHENTIC_FLAG', 'PUBLISH_FLAG', 'QC_REVIEW_CD', 'NORMALCY_CD', 'NORMALCY_METHOD_CD', 'VERIFIED_DT_TM', 'VERIFIED_PRSNL_ID', 'PERFORMED_DT_TM', 'PERFORMED_PRSNL_ID', 'UPDT_DT_TM', 'UPDT_CNT', 'NORMAL_LOW', 'NORMAL_HIGH', 'CRITICAL_LOW', 'CRITICAL_HIGH', 'CLINSIG_UPDT_DT_TM', 'ENTRY_MODE_CD', 'SOURCE_CD', 'PERFORMED_TZ', 'VERIFIED_TZ', 'SRC_EVENT_ID', 'SRC_CLINSIG_UPDT_DT_TM', 'DEVICE_FREE_TXT', 'ADC_UPDT', 'ORGANIZATION_ID', 'Trust']

def _mne_latest_clinical_events(config: MapNumericEventsConfig, source_version: int, event_keys: DataFrame, prefix: str, broadcast_keys: bool=False) -> DataFrame:
    keys = event_keys.select(F.col('EVENT_ID').cast('long').alias('EVENT_ID')).dropDuplicates()
    if broadcast_keys:
        keys = F.broadcast(keys)
    source = _mne_apply_trust_scope(_mne_read_snapshot(config.clinical_event_table, source_version), config).select(*[column for column in _MNE_CE_SOURCE_COLUMNS])
    source = source.withColumn('EVENT_ID', F.col('EVENT_ID').cast('long')).join(keys, 'EVENT_ID', 'left_semi')
    source = source.withColumn('_MNE_CE_EFFECTIVE_UPDT', F.coalesce(F.col('ADC_UPDT'), F.col('UPDT_DT_TM'), F.col('VALID_FROM_DT_TM')))
    source = _mne_latest_by_key(source, ['EVENT_ID'], ['UPDT_CNT', '_MNE_CE_EFFECTIVE_UPDT', 'VALID_FROM_DT_TM', 'VALID_UNTIL_DT_TM', 'CLINICAL_EVENT_ID'])
    return source.select(F.col('EVENT_ID'), *[F.col(column).alias(f'{prefix}_{column}') for column in _MNE_CE_SOURCE_COLUMNS if column != 'EVENT_ID'])

def _mne_code_value_snapshot(config: MapNumericEventsConfig, source_version: int) -> DataFrame:
    return _mne_read_snapshot(config.code_value_table, source_version).select(_mne_checked_double_id(F.col('CODE_VALUE'), 'CODE_VALUE.CODE_VALUE').alias('CODE_VALUE'), F.col('DESCRIPTION'), F.col('DISPLAY'), F.col('CDF_MEANING'), F.col('ACTIVE_IND').cast('long').alias('ACTIVE_IND'), F.col('END_EFFECTIVE_DT_TM'), F.col('ADC_UPDT')).withColumn('BEST_DISPLAY', F.coalesce(_mne_nonblank(F.col('DESCRIPTION')), _mne_nonblank(F.col('DISPLAY')), _mne_nonblank(F.col('CDF_MEANING'))))

def _mne_order_catalog_snapshot(config: MapNumericEventsConfig, source_version: int) -> DataFrame:
    return _mne_read_snapshot(config.order_catalog_table, source_version).select(_mne_checked_double_id(F.col('CATALOG_CD'), 'ORDER_CATALOG.CATALOG_CD').alias('CATALOG_CD'), F.col('CATALOG_TYPE_CD').cast('long').alias('CATALOG_TYPE_CD'), F.col('DESCRIPTION'), F.col('PRIMARY_MNEMONIC'), F.col('DEPT_DISPLAY_NAME'), F.col('ACTIVE_IND').cast('long').alias('ACTIVE_IND'), F.col('EVENT_CD').cast('long').alias('EVENT_CD'), F.col('ADC_UPDT')).withColumn('BEST_DISPLAY', F.coalesce(_mne_nonblank(F.col('DESCRIPTION')), _mne_nonblank(F.col('PRIMARY_MNEMONIC')), _mne_nonblank(F.col('DEPT_DISPLAY_NAME'))))

def _mne_join_code_display(df: DataFrame, code_lookup: DataFrame, code_column: str, display_column: str, sequence_number: int) -> Tuple[DataFrame, str]:
    key_column = f'_MNE_CV_KEY_{sequence_number}'
    value_column = f'_MNE_CV_VALUE_{sequence_number}'
    update_column = f'_MNE_CV_ADC_{sequence_number}'
    lookup = F.broadcast(code_lookup.select(F.col('CODE_VALUE').alias(key_column), F.col('BEST_DISPLAY').alias(value_column), F.col('ADC_UPDT').alias(update_column)))
    joined = df.join(lookup, F.col(code_column) == F.col(key_column), 'left').withColumn(display_column, F.col(value_column)).drop(key_column, value_column)
    return (joined, update_column)
_MNE_NUMERIC_TOKEN = '[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?'
_MNE_COMPARATOR_PATTERN = f'^\\s*(<=|>=|<|>|≤|≥)\\s*({_MNE_NUMERIC_TOKEN})\\s*$'
_MNE_THOUSANDS_PATTERN = '^\\s*[+-]?\\d{1,3}(?:,\\d{3})+(?:\\.\\d+)?\\s*$'
# `[\\s\\S]`, NOT `.`. `.` is line-terminator sensitive and the two regex engines Databricks
# uses for the same expression disagree: java.util.regex (Spark JVM, and the constant-folding
# path) excludes CR/LF, Photon matches them. Measured 2026-07-25 on '75mg\r\n75mg\r\n'
# (hex 37356D670D0A37356D670D0A): against the table COLUMN group 2 is 'mg\r\n75mg'; against
# the identical value as a SQL LITERAL there is no match at all. So NUMERIC_PARSE_SUCCESS_IND
# -- hence IN_NUMERIC_SCOPE_IND, hence IN_TEXT_SCOPE_IND, which is its exact complement at
# line 2044 -- was NON-DETERMINISTIC across two evaluations of the same uncached plan. That is
# what failed the 2026-07-25 prebuild: _mne_incremental_update built the MERGE source
# restrictively and then re-evaluated the same plan permissively in its presence assertion.
# 913 of the 986 engine-divergent changed events were absent, against 53 of 85,468
# non-divergent ones. `[\\s\\S]` means "any character" in both engines with no mode flag, and
# reproduces the permissive reading already materialised in the live canonical, so no row
# changes lane. Do not "simplify" this back to `.`. The twin definition further down this
# file -- the nested copy the text lane execs -- must always be changed with it.
_MNE_SUFFIX_PATTERN = f'^\\s*({_MNE_NUMERIC_TOKEN})\\s*([\\s\\S]+?)\\s*$'

def _mne_numeric_parse_expressions(text_column_name: str) -> Dict[str, Column]:
    tick = chr(96)
    quoted_name = f'{tick}{text_column_name.replace(tick, tick + tick)}{tick}'
    text_column = F.col(text_column_name)
    text = F.trim(text_column.cast('string'))
    direct = F.expr(f'try_cast({quoted_name} as double)')
    comparator_token = F.regexp_extract(text, _MNE_COMPARATOR_PATTERN, 2)
    comparator_value = F.when(comparator_token != '', comparator_token.cast('double'))
    thousands_value = F.when(text.rlike(_MNE_THOUSANDS_PATTERN), F.regexp_replace(text, ',', '').cast('double'))
    suffix_token = F.regexp_extract(text, _MNE_SUFFIX_PATTERN, 1)
    suffix_text = F.regexp_extract(text, _MNE_SUFFIX_PATTERN, 2)
    suffix_value = F.when(direct.isNull() & comparator_value.isNull() & (suffix_token != ''), suffix_token.cast('double'))
    value = F.coalesce(direct, comparator_value, thousands_value, suffix_value)
    operator = F.when(F.regexp_extract(text, _MNE_COMPARATOR_PATTERN, 1) == '≤', F.lit('<=')).when(F.regexp_extract(text, _MNE_COMPARATOR_PATTERN, 1) == '≥', F.lit('>=')).otherwise(F.when(F.regexp_extract(text, _MNE_COMPARATOR_PATTERN, 1) != '', F.regexp_extract(text, _MNE_COMPARATOR_PATTERN, 1)))
    method = F.when(text_column.isNull() | (text == ''), F.lit('BLANK')).when(direct.isNotNull(), F.lit('DIRECT')).when(comparator_value.isNotNull(), F.lit('COMPARATOR')).when(thousands_value.isNotNull(), F.lit('THOUSANDS')).when(suffix_value.isNotNull(), F.lit('NUMERIC_WITH_SUFFIX')).otherwise(F.lit('NON_NUMERIC'))
    return {'value': value, 'operator': operator, 'suffix': F.when(suffix_value.isNotNull(), F.trim(suffix_text)), 'method': method}

def _mne_is_current(valid_from: Column, valid_until: Column, run_timestamp: datetime) -> Column:
    run_ts = F.lit(run_timestamp).cast('timestamp')
    return (valid_from.isNull() | (valid_from <= run_ts)) & (valid_until.isNull() | (valid_until > run_ts))

def _mne_resolved_manual_maps(config: MapNumericEventsConfig, source_versions: Optional[Dict[str, int]]=None, manual_maps: Optional[DataFrame]=None, concepts: Optional[DataFrame]=None) -> DataFrame:
    if manual_maps is None:
        if source_versions is None:
            raise ValueError('source_versions is required when manual_maps is not supplied')
        manual_maps = _mne_read_snapshot(config.manual_map_table, source_versions[config.manual_map_table])
    if concepts is None:
        if source_versions is None:
            raise ValueError('source_versions is required when concepts is not supplied')
        concepts = _mne_read_snapshot(config.concept_table, source_versions[config.concept_table])
    prepared_maps = manual_maps.select(F.col('OMOPTable').alias('MAP_OMOP_TABLE'), F.col('OMOPField').alias('MAP_OMOP_FIELD'), F.col('SourceTable').alias('MAP_SOURCE_TABLE'), F.col('SourceField').alias('MAP_SOURCE_FIELD'), F.col('SourceValue').alias('MAP_SOURCE_VALUE'), F.col('OMOPConceptId').alias('MAP_OMOP_CONCEPT_TEXT'), F.col('EVENT_CLASS_CD').alias('MAP_EVENT_CLASS_TEXT'), F.col('STANDARD_CONCEPT').alias('MAP_STANDARD_CONCEPT'), F.col('ADC_UPDT').alias('MAP_ADC_UPDT')).withColumn('MAP_SOURCE_VALUE_LONG', F.expr('try_cast(MAP_SOURCE_VALUE as bigint)')).withColumn('MAP_EVENT_CLASS_CD', F.expr('try_cast(MAP_EVENT_CLASS_TEXT as bigint)')).withColumn('MAP_OMOP_CONCEPT_ID', F.expr('try_cast(MAP_OMOP_CONCEPT_TEXT as bigint)')).withColumn('MAP_RULE_ID', F.sha2(F.concat_ws('|', F.coalesce(F.col('MAP_SOURCE_TABLE'), F.lit('')), F.coalesce(F.col('MAP_SOURCE_FIELD'), F.lit('')), F.coalesce(F.col('MAP_SOURCE_VALUE'), F.lit('')), F.coalesce(F.col('MAP_EVENT_CLASS_TEXT'), F.lit('*')), F.coalesce(F.col('MAP_OMOP_TABLE'), F.lit('')), F.coalesce(F.col('MAP_OMOP_FIELD'), F.lit('')), F.coalesce(F.col('MAP_OMOP_CONCEPT_TEXT'), F.lit(''))), 256))
    mapped_ids = prepared_maps.select(F.col('MAP_OMOP_CONCEPT_ID').alias('_MNE_MAPPED_CONCEPT_ID')).where(F.col('_MNE_MAPPED_CONCEPT_ID').isNotNull()).dropDuplicates()
    concept_details = concepts.select(F.col('concept_id').cast('long').alias('CONCEPT_ID'), F.col('concept_name').alias('CONCEPT_NAME'), F.col('standard_concept').alias('CONCEPT_STANDARD'), F.col('domain_id').alias('CONCEPT_DOMAIN'), F.col('concept_class_id').alias('CONCEPT_CLASS'), F.col('invalid_reason').alias('CONCEPT_INVALID_REASON')).join(F.broadcast(mapped_ids), F.col('CONCEPT_ID') == F.col('_MNE_MAPPED_CONCEPT_ID'), 'inner').drop('_MNE_MAPPED_CONCEPT_ID')
    domain_score = F.when(F.col('CONCEPT_DOMAIN') == 'Drug', F.lit(100)).when(F.col('CONCEPT_DOMAIN') == 'Measurement', F.lit(90)).when(F.col('CONCEPT_DOMAIN') == 'Observation', F.lit(80)).when(F.col('CONCEPT_DOMAIN') == 'Procedure', F.lit(70)).when(F.col('CONCEPT_DOMAIN') == 'Condition', F.lit(60)).when(F.col('CONCEPT_DOMAIN') == 'Device', F.lit(50)).otherwise(F.lit(0))
    standard_score = F.when(F.col('CONCEPT_STANDARD') == 'S', F.lit(3)).when(F.col('CONCEPT_STANDARD') == 'C', F.lit(2)).when(F.col('CONCEPT_STANDARD').isNotNull(), F.lit(1)).otherwise(F.lit(0))
    return prepared_maps.join(concept_details, F.col('MAP_OMOP_CONCEPT_ID') == F.col('CONCEPT_ID'), 'left').withColumn('MAP_CONCEPT_VALID_IND', F.col('CONCEPT_ID').isNotNull() & F.col('CONCEPT_INVALID_REASON').isNull()).withColumn('MAP_DOMAIN_SCORE', domain_score).withColumn('MAP_STANDARD_SCORE', standard_score)

def _mne_attach_omop_mappings(df: DataFrame, resolved_maps: DataFrame) -> DataFrame:
    concept_maps = F.broadcast(resolved_maps.where((F.col('MAP_SOURCE_TABLE') == 'dbo.PI_CDE_CLINICAL_EVENT') & (F.col('MAP_SOURCE_FIELD') == 'EVENT_CD') & F.col('MAP_OMOP_FIELD').isin('measurement_concept_id', 'observation_concept_id')))
    concept_candidates = df.select('EVENT_ID', 'EVENT_CD', 'EVENT_CLASS_CD').join(concept_maps, (F.col('EVENT_CD') == F.col('MAP_SOURCE_VALUE_LONG')) & (F.col('MAP_EVENT_CLASS_CD').isNull() | (F.col('EVENT_CLASS_CD') == F.col('MAP_EVENT_CLASS_CD'))), 'inner').withColumn('MAP_MATCH_TYPE', F.when(F.col('MAP_EVENT_CLASS_CD').isNotNull(), F.lit('EVENT_CODE_AND_CLASS')).otherwise(F.lit('EVENT_CODE'))).withColumn('_MNE_MAP_SELECTION_RANK', F.struct(F.col('MAP_CONCEPT_VALID_IND').cast('int'), F.col('MAP_EVENT_CLASS_CD').isNotNull().cast('int'), F.col('MAP_STANDARD_SCORE'), F.col('MAP_DOMAIN_SCORE'), (-F.coalesce(F.col('MAP_OMOP_CONCEPT_ID'), F.lit(9223372036854775807))).alias('lower_concept_preferred'), F.col('MAP_RULE_ID'))).withColumn('_MNE_MAP_PAYLOAD', F.struct('MAP_OMOP_TABLE', 'MAP_OMOP_FIELD', 'MAP_OMOP_CONCEPT_TEXT', 'MAP_OMOP_CONCEPT_ID', 'MAP_RULE_ID', 'MAP_MATCH_TYPE', 'MAP_CONCEPT_VALID_IND', 'CONCEPT_NAME', 'CONCEPT_STANDARD', 'CONCEPT_DOMAIN', 'CONCEPT_CLASS'))
    concept_summary = concept_candidates.groupBy('EVENT_ID').agg(F.count(F.lit(1)).cast('long').alias('OMOP_MAPPING_CANDIDATE_COUNT'), F.countDistinct('MAP_OMOP_CONCEPT_ID').cast('long').alias('OMOP_MAPPING_DISTINCT_CONCEPT_COUNT'), F.sort_array(F.collect_set('MAP_OMOP_CONCEPT_ID')).alias('OMOP_MAPPING_CANDIDATE_CONCEPT_IDS'), F.max('MAP_ADC_UPDT').alias('_MNE_CONCEPT_MAP_ADC_UPDT'), F.max_by('_MNE_MAP_PAYLOAD', '_MNE_MAP_SELECTION_RANK').alias('_MNE_SELECTED_MAP')).select('EVENT_ID', 'OMOP_MAPPING_CANDIDATE_COUNT', 'OMOP_MAPPING_DISTINCT_CONCEPT_COUNT', 'OMOP_MAPPING_CANDIDATE_CONCEPT_IDS', '_MNE_CONCEPT_MAP_ADC_UPDT', F.col('_MNE_SELECTED_MAP.MAP_OMOP_TABLE').alias('OMOP_MANUAL_TABLE'), F.col('_MNE_SELECTED_MAP.MAP_OMOP_FIELD').alias('OMOP_MANUAL_COLUMN'), F.col('_MNE_SELECTED_MAP.MAP_OMOP_CONCEPT_TEXT').alias('OMOP_MANUAL_CONCEPT'), F.col('_MNE_SELECTED_MAP.MAP_OMOP_CONCEPT_ID').alias('OMOP_MANUAL_CONCEPT_ID'), F.col('_MNE_SELECTED_MAP.MAP_RULE_ID').alias('OMOP_MAPPING_RULE_ID'), F.col('_MNE_SELECTED_MAP.MAP_MATCH_TYPE').alias('OMOP_MAPPING_MATCH_TYPE'), F.col('_MNE_SELECTED_MAP.MAP_CONCEPT_VALID_IND').alias('OMOP_MAPPING_VALID_IND'), F.col('_MNE_SELECTED_MAP.CONCEPT_NAME').alias('OMOP_MANUAL_CONCEPT_NAME'), F.col('_MNE_SELECTED_MAP.CONCEPT_STANDARD').alias('OMOP_MANUAL_STANDARD_CONCEPT'), F.col('_MNE_SELECTED_MAP.CONCEPT_DOMAIN').alias('OMOP_MANUAL_CONCEPT_DOMAIN'), F.col('_MNE_SELECTED_MAP.CONCEPT_CLASS').alias('OMOP_MANUAL_CONCEPT_CLASS'))
    unit_maps = F.broadcast(resolved_maps.where((F.col('MAP_SOURCE_TABLE') == 'dbo.PI_CDE_CLINICAL_EVENT') & (F.col('MAP_SOURCE_FIELD') == 'EVENT_RESULT_UNITS_CD') & (F.col('MAP_OMOP_FIELD') == 'unit_concept_id')))
    unit_candidates = df.select('EVENT_ID', 'EFFECTIVE_UNIT_OF_MEASURE_CD').join(unit_maps, F.col('EFFECTIVE_UNIT_OF_MEASURE_CD') == F.col('MAP_SOURCE_VALUE_LONG'), 'inner').withColumn('_MNE_UNIT_SELECTION_RANK', F.struct(F.col('MAP_CONCEPT_VALID_IND').cast('int'), F.col('MAP_STANDARD_SCORE'), F.col('MAP_DOMAIN_SCORE'), (-F.coalesce(F.col('MAP_OMOP_CONCEPT_ID'), F.lit(9223372036854775807))).alias('lower_concept_preferred'), F.col('MAP_RULE_ID'))).withColumn('_MNE_UNIT_PAYLOAD', F.struct('MAP_OMOP_CONCEPT_TEXT', 'MAP_OMOP_CONCEPT_ID', 'MAP_RULE_ID', 'MAP_CONCEPT_VALID_IND'))
    unit_summary = unit_candidates.groupBy('EVENT_ID').agg(F.count(F.lit(1)).cast('long').alias('OMOP_UNIT_MAPPING_CANDIDATE_COUNT'), F.countDistinct('MAP_OMOP_CONCEPT_ID').cast('long').alias('OMOP_UNIT_MAPPING_DISTINCT_CONCEPT_COUNT'), F.sort_array(F.collect_set('MAP_OMOP_CONCEPT_ID')).alias('OMOP_UNIT_MAPPING_CANDIDATE_CONCEPT_IDS'), F.max('MAP_ADC_UPDT').alias('_MNE_UNIT_MAP_ADC_UPDT'), F.max_by('_MNE_UNIT_PAYLOAD', '_MNE_UNIT_SELECTION_RANK').alias('_MNE_SELECTED_UNIT_MAP')).select('EVENT_ID', 'OMOP_UNIT_MAPPING_CANDIDATE_COUNT', 'OMOP_UNIT_MAPPING_DISTINCT_CONCEPT_COUNT', 'OMOP_UNIT_MAPPING_CANDIDATE_CONCEPT_IDS', '_MNE_UNIT_MAP_ADC_UPDT', F.col('_MNE_SELECTED_UNIT_MAP.MAP_OMOP_CONCEPT_TEXT').alias('OMOP_MANUAL_UNITS'), F.col('_MNE_SELECTED_UNIT_MAP.MAP_OMOP_CONCEPT_ID').alias('OMOP_MANUAL_UNIT_CONCEPT_ID'), F.col('_MNE_SELECTED_UNIT_MAP.MAP_RULE_ID').alias('OMOP_UNIT_MAPPING_RULE_ID'), F.col('_MNE_SELECTED_UNIT_MAP.MAP_CONCEPT_VALID_IND').alias('OMOP_UNIT_MAPPING_VALID_IND'))
    empty_long_array = F.expr('CAST(array() AS ARRAY<BIGINT>)')
    result = df.join(concept_summary, 'EVENT_ID', 'left').join(unit_summary, 'EVENT_ID', 'left').withColumn('OMOP_MAPPING_CANDIDATE_COUNT', F.coalesce(F.col('OMOP_MAPPING_CANDIDATE_COUNT'), F.lit(0).cast('long'))).withColumn('OMOP_MAPPING_DISTINCT_CONCEPT_COUNT', F.coalesce(F.col('OMOP_MAPPING_DISTINCT_CONCEPT_COUNT'), F.lit(0).cast('long'))).withColumn('OMOP_MAPPING_CANDIDATE_CONCEPT_IDS', F.coalesce(F.col('OMOP_MAPPING_CANDIDATE_CONCEPT_IDS'), empty_long_array)).withColumn('OMOP_MAPPING_AMBIGUOUS_IND', F.col('OMOP_MAPPING_DISTINCT_CONCEPT_COUNT') > F.lit(1)).withColumn('OMOP_UNIT_MAPPING_CANDIDATE_COUNT', F.coalesce(F.col('OMOP_UNIT_MAPPING_CANDIDATE_COUNT'), F.lit(0).cast('long'))).withColumn('OMOP_UNIT_MAPPING_DISTINCT_CONCEPT_COUNT', F.coalesce(F.col('OMOP_UNIT_MAPPING_DISTINCT_CONCEPT_COUNT'), F.lit(0).cast('long'))).withColumn('OMOP_UNIT_MAPPING_CANDIDATE_CONCEPT_IDS', F.coalesce(F.col('OMOP_UNIT_MAPPING_CANDIDATE_CONCEPT_IDS'), empty_long_array)).withColumn('OMOP_UNIT_MAPPING_AMBIGUOUS_IND', F.col('OMOP_UNIT_MAPPING_DISTINCT_CONCEPT_COUNT') > F.lit(1)).withColumn('OMOP_MAPPING_ADC_UPDT', F.greatest(F.col('_MNE_CONCEPT_MAP_ADC_UPDT'), F.col('_MNE_UNIT_MAP_ADC_UPDT'))).drop('_MNE_CONCEPT_MAP_ADC_UPDT', '_MNE_UNIT_MAP_ADC_UPDT')
    return result

def add_manual_omop_mappings_numeric(df: DataFrame, barts_mapfile: DataFrame, concepts: DataFrame) -> DataFrame:
    """Compatibility replacement for the original helper, without discarding alternate mappings."""
    working = df
    if 'EFFECTIVE_UNIT_OF_MEASURE_CD' not in working.columns:
        working = working.withColumn('EFFECTIVE_UNIT_OF_MEASURE_CD', F.col('UNIT_OF_MEASURE_CD').cast('long'))
    resolved = _mne_resolved_manual_maps(MAP_NUMERIC_EVENTS_CONFIG, manual_maps=barts_mapfile, concepts=concepts)
    return _mne_attach_omop_mappings(working, resolved)

def _mne_build_enriched_numeric_events(config: MapNumericEventsConfig, string_rows: DataFrame, source_versions: Dict[str, int], run_id: str, run_timestamp: datetime, broadcast_keys: bool) -> DataFrame:
    event_keys = string_rows.select('EVENT_ID').dropDuplicates()
    clinical = _mne_latest_clinical_events(config, source_versions[config.clinical_event_table], event_keys, prefix='CE', broadcast_keys=broadcast_keys)
    joined = string_rows.join(clinical, 'EVENT_ID', 'left')
    parent_keys = clinical.select(F.col('CE_PARENT_EVENT_ID').cast('long').alias('EVENT_ID')).where(F.col('EVENT_ID').isNotNull()).dropDuplicates()
    parent = _mne_latest_clinical_events(config, source_versions[config.clinical_event_table], parent_keys, prefix='PARENT', broadcast_keys=broadcast_keys).withColumnRenamed('EVENT_ID', '_MNE_PARENT_LOOKUP_EVENT_ID')
    joined = joined.join(parent, F.col('CE_PARENT_EVENT_ID') == F.col('_MNE_PARENT_LOOKUP_EVENT_ID'), 'left').drop('_MNE_PARENT_LOOKUP_EVENT_ID')
    order_catalog = _mne_order_catalog_snapshot(config, source_versions[config.order_catalog_table])
    main_catalog = F.broadcast(order_catalog.select(F.col('CATALOG_CD').alias('_MNE_OC_CATALOG_CD'), F.col('CATALOG_TYPE_CD').alias('_MNE_OC_CATALOG_TYPE_CD'), F.col('BEST_DISPLAY').alias('_MNE_OC_DISPLAY'), F.col('PRIMARY_MNEMONIC').alias('_MNE_OC_PRIMARY_MNEMONIC'), F.col('DEPT_DISPLAY_NAME').alias('_MNE_OC_DEPT_DISPLAY_NAME'), F.col('ACTIVE_IND').alias('_MNE_OC_ACTIVE_IND'), F.col('ADC_UPDT').alias('_MNE_OC_ADC_UPDT')))
    parent_catalog = F.broadcast(order_catalog.select(F.col('CATALOG_CD').alias('_MNE_POC_CATALOG_CD'), F.col('CATALOG_TYPE_CD').alias('_MNE_POC_CATALOG_TYPE_CD'), F.col('BEST_DISPLAY').alias('_MNE_POC_DISPLAY'), F.col('PRIMARY_MNEMONIC').alias('_MNE_POC_PRIMARY_MNEMONIC'), F.col('DEPT_DISPLAY_NAME').alias('_MNE_POC_DEPT_DISPLAY_NAME'), F.col('ACTIVE_IND').alias('_MNE_POC_ACTIVE_IND'), F.col('ADC_UPDT').alias('_MNE_POC_ADC_UPDT')))
    joined = joined.join(main_catalog, F.col('CE_CATALOG_CD').cast('long') == F.col('_MNE_OC_CATALOG_CD'), 'left').join(parent_catalog, F.col('PARENT_CATALOG_CD').cast('long') == F.col('_MNE_POC_CATALOG_CD'), 'left')
    base = joined.select(F.col('EVENT_ID').cast('long').alias('EVENT_ID'), F.coalesce(F.col('CE_ENCNTR_ID'), F.col('SR_ENCNTR_ID')).cast('long').alias('ENCNTR_ID'), F.col('CE_PERSON_ID').cast('long').alias('PERSON_ID'), F.col('CE_ORDER_ID').cast('long').alias('ORDER_ID'), F.col('CE_EVENT_CLASS_CD').cast('long').alias('EVENT_CLASS_CD'), F.col('CE_PERFORMED_PRSNL_ID').cast('long').alias('PERFORMED_PRSNL_ID'), F.col('SR_UNIT_OF_MEASURE_CD').cast('long').alias('UNIT_OF_MEASURE_CD'), F.col('CE_EVENT_TITLE_TEXT').alias('EVENT_TITLE_TEXT'), F.col('CE_EVENT_CD').cast('long').alias('EVENT_CD'), F.col('CE_CATALOG_CD').cast('long').alias('CATALOG_CD'), F.col('_MNE_OC_CATALOG_TYPE_CD').cast('long').alias('CATALOG_TYPE_CD'), F.col('_MNE_OC_DISPLAY').alias('CATALOG_DISPLAY'), F.col('CE_CONTRIBUTOR_SYSTEM_CD').cast('long').alias('CONTRIBUTOR_SYSTEM_CD'), F.col('CE_REFERENCE_NBR').alias('REFERENCE_NBR'), F.col('CE_PARENT_EVENT_ID').cast('long').alias('PARENT_EVENT_ID'), F.col('CE_NORMALCY_CD').cast('long').alias('NORMALCY_CD'), F.col('CE_ENTRY_MODE_CD').cast('long').alias('ENTRY_MODE_CD'), F.col('CE_PERFORMED_DT_TM').alias('PERFORMED_DT_TM'), F.col('CE_CLINSIG_UPDT_DT_TM').alias('CLINSIG_UPDT_DT_TM'), F.col('PARENT_EVENT_TITLE_TEXT').alias('PARENT_EVENT_TITLE_TEXT'), F.col('PARENT_EVENT_CD').cast('long').alias('PARENT_EVENT_CD'), F.col('PARENT_CATALOG_CD').cast('long').alias('PARENT_CATALOG_CD'), F.col('_MNE_POC_CATALOG_TYPE_CD').cast('long').alias('PARENT_CATALOG_TYPE_CD'), F.col('_MNE_POC_DISPLAY').alias('PARENT_CATALOG_DISPLAY'), F.col('PARENT_REFERENCE_NBR').alias('PARENT_REFERENCE_NBR'), F.col('SR_STRING_RESULT_TEXT').alias('STRING_RESULT_TEXT'), F.coalesce(_mne_nonblank(F.col('SR_STRING_RESULT_TEXT')), _mne_nonblank(F.col('CE_RESULT_VAL'))).alias('RESULT_TEXT_EFFECTIVE'), F.col('SR_STRING_RESULT_FORMAT_CD').cast('long').alias('STRING_RESULT_FORMAT_CD'), F.col('SR_EQUATION_ID').cast('long').alias('EQUATION_ID'), F.col('SR_CALCULATION_EQUATION').alias('CALCULATION_EQUATION'), F.col('SR_LAST_NORM_DT_TM').alias('LAST_NORM_DT_TM'), F.col('SR_FEASIBLE_IND').cast('long').alias('FEASIBLE_IND'), F.col('SR_INACCURATE_IND').cast('long').alias('INACCURATE_IND'), F.col('SR_MODIFY_FLAG').cast('long').alias('MODIFY_FLAG'), F.col('SR_STRING_LONG_TEXT_ID').cast('long').alias('STRING_LONG_TEXT_ID'), F.col('SR_NORMAL_LOW_TEXT').alias('STRING_RESULT_NORMAL_LOW_TEXT'), F.col('SR_NORMAL_HIGH_TEXT').alias('STRING_RESULT_NORMAL_HIGH_TEXT'), F.col('SR_CRITICAL_LOW_TEXT').alias('STRING_RESULT_CRITICAL_LOW_TEXT'), F.col('SR_CRITICAL_HIGH_TEXT').alias('STRING_RESULT_CRITICAL_HIGH_TEXT'), F.col('CE_NORMAL_LOW').alias('CLINICAL_EVENT_NORMAL_LOW_TEXT'), F.col('CE_NORMAL_HIGH').alias('CLINICAL_EVENT_NORMAL_HIGH_TEXT'), F.col('CE_CRITICAL_LOW').alias('CLINICAL_EVENT_CRITICAL_LOW_TEXT'), F.col('CE_CRITICAL_HIGH').alias('CLINICAL_EVENT_CRITICAL_HIGH_TEXT'), F.col('SR_VALID_FROM_DT_TM').alias('STRING_RESULT_VALID_FROM_DT_TM'), F.col('SR_VALID_UNTIL_DT_TM').alias('STRING_RESULT_VALID_UNTIL_DT_TM'), F.col('SR_UPDT_DT_TM').alias('STRING_RESULT_UPDT_DT_TM'), F.col('SR_UPDT_CNT').cast('long').alias('STRING_RESULT_UPDT_CNT'), F.col('SR_LAST_UTC_TS').alias('STRING_RESULT_LAST_UTC_TS'), F.col('SR_ADC_UPDT').alias('STRING_RESULT_ADC_UPDT'), F.col('SR_EFFECTIVE_UPDT_DT_TM').alias('STRING_RESULT_EFFECTIVE_UPDT_DT_TM'), F.col('SR_ENCNTR_ID').cast('long').alias('STRING_RESULT_ENCNTR_ID'), F.col('SR_ORGANIZATION_ID').cast('long').alias('STRING_RESULT_ORGANIZATION_ID'), F.col('SR_TRUST').alias('STRING_RESULT_TRUST'), F.col('CE_CLINICAL_EVENT_ID').cast('long').alias('CLINICAL_EVENT_ID'), F.col('CE_ENCNTR_ID').cast('long').alias('CLINICAL_EVENT_ENCNTR_ID'), F.col('CE_ORGANIZATION_ID').cast('long').alias('CLINICAL_EVENT_ORGANIZATION_ID'), F.col('CE_Trust').alias('CLINICAL_EVENT_TRUST'), F.coalesce(F.col('CE_ORGANIZATION_ID'), F.col('SR_ORGANIZATION_ID')).cast('long').alias('ORGANIZATION_ID'), F.col('CE_VALID_FROM_DT_TM').alias('CLINICAL_EVENT_VALID_FROM_DT_TM'), F.col('CE_VALID_UNTIL_DT_TM').alias('CLINICAL_EVENT_VALID_UNTIL_DT_TM'), F.col('CE_UPDT_DT_TM').alias('CLINICAL_EVENT_UPDT_DT_TM'), F.col('CE_UPDT_CNT').cast('long').alias('CLINICAL_EVENT_UPDT_CNT'), F.col('CE_ADC_UPDT').alias('CLINICAL_EVENT_ADC_UPDT'), F.col('CE_EVENT_START_DT_TM').alias('EVENT_START_DT_TM'), F.col('CE_EVENT_END_DT_TM').alias('EVENT_END_DT_TM'), F.col('CE_SERIES_REF_NBR').alias('SERIES_REF_NBR'), F.col('CE_ACCESSION_NBR').alias('ACCESSION_NBR'), F.col('CE_EVENT_TAG').alias('EVENT_TAG'), F.col('CE_RESULT_VAL').alias('RESULT_VAL'), F.col('CE_RESULT_UNITS_CD').cast('long').alias('RESULT_UNITS_CD'), F.col('CE_RESULT_TIME_UNITS_CD').cast('long').alias('RESULT_TIME_UNITS_CD'), F.col('CE_TASK_ASSAY_CD').cast('long').alias('TASK_ASSAY_CD'), F.col('CE_RESULT_STATUS_CD').cast('long').alias('RESULT_STATUS_CD'), F.col('CE_RECORD_STATUS_CD').cast('long').alias('RECORD_STATUS_CD'), F.col('CE_AUTHENTIC_FLAG').cast('long').alias('AUTHENTIC_FLAG'), F.col('CE_PUBLISH_FLAG').cast('long').alias('PUBLISH_FLAG'), F.col('CE_QC_REVIEW_CD').cast('long').alias('QC_REVIEW_CD'), F.col('CE_NORMALCY_METHOD_CD').cast('long').alias('NORMALCY_METHOD_CD'), F.col('CE_EVENT_RELTN_CD').cast('long').alias('EVENT_RELTN_CD'), F.col('CE_SOURCE_CD').cast('long').alias('SOURCE_CD'), F.col('CE_VERIFIED_DT_TM').alias('VERIFIED_DT_TM'), F.col('CE_VERIFIED_PRSNL_ID').cast('long').alias('VERIFIED_PRSNL_ID'), F.col('CE_PERFORMED_TZ').cast('long').alias('PERFORMED_TZ'), F.col('CE_VERIFIED_TZ').cast('long').alias('VERIFIED_TZ'), F.col('CE_SRC_EVENT_ID').cast('long').alias('SRC_EVENT_ID'), F.col('CE_SRC_CLINSIG_UPDT_DT_TM').alias('SRC_CLINSIG_UPDT_DT_TM'), F.col('CE_DEVICE_FREE_TXT').alias('DEVICE_FREE_TXT'), F.col('PARENT_CLINICAL_EVENT_ID').cast('long').alias('PARENT_CLINICAL_EVENT_ID'), F.col('PARENT_EVENT_CLASS_CD').cast('long').alias('PARENT_EVENT_CLASS_CD'), F.col('PARENT_VALID_FROM_DT_TM').alias('PARENT_VALID_FROM_DT_TM'), F.col('PARENT_VALID_UNTIL_DT_TM').alias('PARENT_VALID_UNTIL_DT_TM'), F.col('PARENT_UPDT_DT_TM').alias('PARENT_UPDT_DT_TM'), F.col('PARENT_UPDT_CNT').cast('long').alias('PARENT_UPDT_CNT'), F.col('PARENT_ADC_UPDT').alias('PARENT_ADC_UPDT'), F.col('PARENT_RESULT_STATUS_CD').cast('long').alias('PARENT_RESULT_STATUS_CD'), F.col('_MNE_OC_PRIMARY_MNEMONIC').alias('CATALOG_PRIMARY_MNEMONIC'), F.col('_MNE_OC_DEPT_DISPLAY_NAME').alias('CATALOG_DEPT_DISPLAY_NAME'), F.col('_MNE_OC_ACTIVE_IND').cast('long').alias('CATALOG_ACTIVE_IND'), F.col('_MNE_POC_PRIMARY_MNEMONIC').alias('PARENT_CATALOG_PRIMARY_MNEMONIC'), F.col('_MNE_POC_DEPT_DISPLAY_NAME').alias('PARENT_CATALOG_DEPT_DISPLAY_NAME'), F.col('_MNE_POC_ACTIVE_IND').cast('long').alias('PARENT_CATALOG_ACTIVE_IND'), F.col('_MNE_OC_CATALOG_CD').isNotNull().alias('ORDER_CATALOG_FOUND_IND'), F.col('_MNE_POC_CATALOG_CD').isNotNull().alias('PARENT_ORDER_CATALOG_FOUND_IND'), F.col('CE_CLINICAL_EVENT_ID').isNotNull().alias('CLINICAL_EVENT_FOUND_IND'), F.col('PARENT_CLINICAL_EVENT_ID').isNotNull().alias('PARENT_EVENT_FOUND_IND'), F.col('STRING_RESULT_CDF_COMMIT_VERSION'), F.col('STRING_RESULT_CDF_COMMIT_TIMESTAMP'), F.col('STRING_RESULT_CDF_CHANGE_TYPE'), F.col('TRIGGER_SOURCES'), F.col('_MNE_OC_ADC_UPDT'), F.col('_MNE_POC_ADC_UPDT'))
    result_parse = _mne_numeric_parse_expressions('STRING_RESULT_TEXT')
    normal_low_parse = _mne_numeric_parse_expressions('NORMAL_LOW_TEXT')
    normal_high_parse = _mne_numeric_parse_expressions('NORMAL_HIGH_TEXT')
    critical_low_parse = _mne_numeric_parse_expressions('CRITICAL_LOW_TEXT')
    critical_high_parse = _mne_numeric_parse_expressions('CRITICAL_HIGH_TEXT')
    base = base.withColumn('NORMAL_LOW_TEXT', F.coalesce(_mne_nonblank(F.col('CLINICAL_EVENT_NORMAL_LOW_TEXT')), _mne_nonblank(F.col('STRING_RESULT_NORMAL_LOW_TEXT')))).withColumn('NORMAL_HIGH_TEXT', F.coalesce(_mne_nonblank(F.col('CLINICAL_EVENT_NORMAL_HIGH_TEXT')), _mne_nonblank(F.col('STRING_RESULT_NORMAL_HIGH_TEXT')))).withColumn('CRITICAL_LOW_TEXT', F.coalesce(_mne_nonblank(F.col('CLINICAL_EVENT_CRITICAL_LOW_TEXT')), _mne_nonblank(F.col('STRING_RESULT_CRITICAL_LOW_TEXT')))).withColumn('CRITICAL_HIGH_TEXT', F.coalesce(_mne_nonblank(F.col('CLINICAL_EVENT_CRITICAL_HIGH_TEXT')), _mne_nonblank(F.col('STRING_RESULT_CRITICAL_HIGH_TEXT'))))
    normal_low_parse = _mne_numeric_parse_expressions('NORMAL_LOW_TEXT')
    normal_high_parse = _mne_numeric_parse_expressions('NORMAL_HIGH_TEXT')
    critical_low_parse = _mne_numeric_parse_expressions('CRITICAL_LOW_TEXT')
    critical_high_parse = _mne_numeric_parse_expressions('CRITICAL_HIGH_TEXT')
    base = base.withColumn('NUMERIC_RESULT', result_parse['value']).withColumn('NUMERIC_RESULT_FLOAT_COMPAT', result_parse['value'].cast('float')).withColumn('RESULT_OPERATOR', result_parse['operator']).withColumn('RESULT_SUFFIX_TEXT', result_parse['suffix']).withColumn('NUMERIC_PARSE_METHOD', result_parse['method']).withColumn('NUMERIC_PARSE_SUCCESS_IND', F.col('NUMERIC_RESULT').isNotNull()).withColumn('NUMERIC_FINITE_IND', F.when(F.col('NUMERIC_RESULT').isNull(), F.lit(False)).otherwise(~F.isnan(F.col('NUMERIC_RESULT')) & (F.abs(F.col('NUMERIC_RESULT')) != F.lit(float('inf'))))).withColumn('POTENTIAL_IDENTIFIER_IND', F.coalesce(F.trim(F.col('STRING_RESULT_TEXT')).rlike('^\\d{16,}$') | F.trim(F.col('STRING_RESULT_TEXT')).rlike('^0\\d{7,}$'), F.lit(False))).withColumn('EVENT_CLASS_NUMERIC_IND', F.col('EVENT_CLASS_CD') == F.lit(233)).withColumn('NUMERIC_SEMANTIC_IND', F.col('EVENT_CLASS_NUMERIC_IND') | F.col('NUMERIC_PARSE_SUCCESS_IND')).withColumn('IN_NUMERIC_SCOPE_IND', F.col('NUMERIC_SEMANTIC_IND')).withColumn('NORMAL_LOW', normal_low_parse['value']).withColumn('NORMAL_HIGH', normal_high_parse['value']).withColumn('CRITICAL_LOW', critical_low_parse['value']).withColumn('CRITICAL_HIGH', critical_high_parse['value']).withColumn('NORMAL_LOW_OPERATOR', normal_low_parse['operator']).withColumn('NORMAL_HIGH_OPERATOR', normal_high_parse['operator']).withColumn('CRITICAL_LOW_OPERATOR', critical_low_parse['operator']).withColumn('CRITICAL_HIGH_OPERATOR', critical_high_parse['operator']).withColumn('NORMAL_RANGE_SOURCE', F.when(_mne_nonblank(F.col('CLINICAL_EVENT_NORMAL_LOW_TEXT')).isNotNull() | _mne_nonblank(F.col('CLINICAL_EVENT_NORMAL_HIGH_TEXT')).isNotNull(), F.lit('CLINICAL_EVENT')).when(_mne_nonblank(F.col('STRING_RESULT_NORMAL_LOW_TEXT')).isNotNull() | _mne_nonblank(F.col('STRING_RESULT_NORMAL_HIGH_TEXT')).isNotNull(), F.lit('STRING_RESULT'))).withColumn('EFFECTIVE_UNIT_OF_MEASURE_CD', F.coalesce(F.when(F.col('UNIT_OF_MEASURE_CD') != 0, F.col('UNIT_OF_MEASURE_CD')), F.when(F.col('RESULT_UNITS_CD') != 0, F.col('RESULT_UNITS_CD')), F.col('UNIT_OF_MEASURE_CD'), F.col('RESULT_UNITS_CD'))).withColumn('ENCOUNTER_ID_MATCH_IND', F.when(F.col('STRING_RESULT_ENCNTR_ID').isNotNull() & F.col('CLINICAL_EVENT_ENCNTR_ID').isNotNull(), F.col('STRING_RESULT_ENCNTR_ID') == F.col('CLINICAL_EVENT_ENCNTR_ID'))).withColumn('UNIT_CODE_CONFLICT_IND', F.when(F.col('UNIT_OF_MEASURE_CD').isNotNull() & (F.col('UNIT_OF_MEASURE_CD') != 0) & F.col('RESULT_UNITS_CD').isNotNull() & (F.col('RESULT_UNITS_CD') != 0), F.col('UNIT_OF_MEASURE_CD') != F.col('RESULT_UNITS_CD')).otherwise(F.lit(False))).withColumn('SOURCE_CURRENT_IND', _mne_is_current(F.col('STRING_RESULT_VALID_FROM_DT_TM'), F.col('STRING_RESULT_VALID_UNTIL_DT_TM'), run_timestamp)).withColumn('CLINICAL_EVENT_CURRENT_IND', F.when(F.col('CLINICAL_EVENT_FOUND_IND'), _mne_is_current(F.col('CLINICAL_EVENT_VALID_FROM_DT_TM'), F.col('CLINICAL_EVENT_VALID_UNTIL_DT_TM'), run_timestamp)).otherwise(F.lit(False))).withColumn('CLINICAL_EVENT_IN_ERROR_IND', F.coalesce(F.col('RESULT_STATUS_CD').isin(29, 30, 31), F.lit(False))).withColumn('SOURCE_DELETED_IND', F.lit(False))
    code_lookup = _mne_code_value_snapshot(config, source_versions[config.code_value_table])
    code_resolutions = [('UNIT_OF_MEASURE_CD', 'UNIT_OF_MEASURE_DISPLAY'), ('STRING_RESULT_FORMAT_CD', 'STRING_RESULT_FORMAT_DISPLAY'), ('EVENT_CLASS_CD', 'EVENT_CLASS_DISPLAY'), ('EVENT_CD', 'EVENT_CD_DISPLAY'), ('CATALOG_TYPE_CD', 'CATALOG_TYPE_DISPLAY'), ('CONTRIBUTOR_SYSTEM_CD', 'CONTRIBUTOR_SYSTEM_DISPLAY'), ('NORMALCY_CD', 'NORMALCY_DISPLAY'), ('NORMALCY_METHOD_CD', 'NORMALCY_METHOD_DISPLAY'), ('ENTRY_MODE_CD', 'ENTRY_MODE_DISPLAY'), ('RESULT_UNITS_CD', 'RESULT_UNITS_DISPLAY'), ('RESULT_TIME_UNITS_CD', 'RESULT_TIME_UNITS_DISPLAY'), ('TASK_ASSAY_CD', 'TASK_ASSAY_DISPLAY'), ('RESULT_STATUS_CD', 'RESULT_STATUS_DISPLAY'), ('RECORD_STATUS_CD', 'RECORD_STATUS_DISPLAY'), ('QC_REVIEW_CD', 'QC_REVIEW_DISPLAY'), ('EVENT_RELTN_CD', 'EVENT_RELTN_DISPLAY'), ('SOURCE_CD', 'SOURCE_DISPLAY'), ('PARENT_EVENT_CLASS_CD', 'PARENT_EVENT_CLASS_DISPLAY'), ('PARENT_EVENT_CD', 'PARENT_EVENT_CD_DISPLAY'), ('PARENT_CATALOG_TYPE_CD', 'PARENT_CATALOG_TYPE_DISPLAY'), ('PARENT_RESULT_STATUS_CD', 'PARENT_RESULT_STATUS_DISPLAY')]
    lookup_timestamps: List[str] = []
    for sequence_number, (code_column, display_column) in enumerate(code_resolutions):
        base, timestamp_column = _mne_join_code_display(base, code_lookup, code_column, display_column, sequence_number)
        lookup_timestamps.append(timestamp_column)
    base = base.withColumn('LOOKUP_ADC_UPDT', F.greatest(*[F.col(column) for column in lookup_timestamps], F.col('_MNE_OC_ADC_UPDT'), F.col('_MNE_POC_ADC_UPDT'))).withColumn('EVENT_LABEL', F.coalesce(_mne_nonblank(F.col('EVENT_TITLE_TEXT')), _mne_nonblank(F.col('EVENT_CD_DISPLAY')), _mne_nonblank(F.col('CATALOG_DISPLAY')), _mne_nonblank(F.col('PARENT_EVENT_TITLE_TEXT')), _mne_nonblank(F.col('PARENT_EVENT_CD_DISPLAY')), F.col('EVENT_CD').cast('string'))).drop(*lookup_timestamps, '_MNE_OC_ADC_UPDT', '_MNE_POC_ADC_UPDT')
    resolved_maps = _mne_resolved_manual_maps(config, source_versions=source_versions)
    base = _mne_attach_omop_mappings(base, resolved_maps)
    base = base.withColumn('ADC_UPDT', F.greatest(F.col('STRING_RESULT_EFFECTIVE_UPDT_DT_TM'), F.col('CLINICAL_EVENT_ADC_UPDT'), F.col('CLINICAL_EVENT_UPDT_DT_TM'), F.col('PARENT_ADC_UPDT'), F.col('PARENT_UPDT_DT_TM'), F.col('LOOKUP_ADC_UPDT'), F.col('OMOP_MAPPING_ADC_UPDT'))).withColumn('STRING_RESULT_SOURCE_VERSION', F.lit(source_versions[config.string_result_table]).cast('long')).withColumn('CLINICAL_EVENT_SOURCE_VERSION', F.lit(source_versions[config.clinical_event_table]).cast('long')).withColumn('CODE_VALUE_SOURCE_VERSION', F.lit(source_versions[config.code_value_table]).cast('long')).withColumn('ORDER_CATALOG_SOURCE_VERSION', F.lit(source_versions[config.order_catalog_table]).cast('long')).withColumn('MANUAL_MAP_SOURCE_VERSION', F.lit(source_versions[config.manual_map_table]).cast('long')).withColumn('CONCEPT_SOURCE_VERSION', F.lit(source_versions[config.concept_table]).cast('long'))
    hash_exclusions = {'ROW_HASH', 'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM', 'TRIGGER_SOURCES', 'STRING_RESULT_CDF_COMMIT_VERSION', 'STRING_RESULT_CDF_COMMIT_TIMESTAMP', 'STRING_RESULT_CDF_CHANGE_TYPE'}
    hash_columns = [F.col(field.name) for field in schema_map_numeric_events.fields if field.name not in hash_exclusions and field.name in base.columns]
    base = base.withColumn('ROW_HASH', _mne_stable_hash_columns(hash_columns)).withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_UPDT_DT_TM', F.lit(run_timestamp))
    return _mne_align_to_schema(base, schema_map_numeric_events)

def _mne_string_result_changed_metadata(config: MapNumericEventsConfig, previous_version: int, current_version: int) -> DataFrame:
    cdf = _mne_read_cdf(config.string_result_table, previous_version + 1, current_version)
    schema = T.StructType([T.StructField('EVENT_ID', T.LongType(), True), T.StructField('STRING_RESULT_CDF_COMMIT_VERSION', T.LongType(), True), T.StructField('STRING_RESULT_CDF_COMMIT_TIMESTAMP', T.TimestampType(), True), T.StructField('STRING_RESULT_CDF_CHANGE_TYPE', T.StringType(), True)])
    if cdf is None:
        return spark.createDataFrame([], schema)
    if config.trust_scope is not None and 'Trust' in cdf.columns:
        cdf = cdf.where(F.col('Trust') == F.lit(config.trust_scope))
    prepared = cdf.select(_mne_checked_double_id(F.col('EVENT_ID'), 'CE_STRING_RESULT.CDF.EVENT_ID').alias('EVENT_ID'), F.col('_commit_version').cast('long').alias('_commit_version'), F.col('_commit_timestamp').alias('_commit_timestamp'), F.col('_change_type').alias('_change_type'), F.when(F.col('_change_type') == 'delete', F.lit(4)).when(F.col('_change_type') == 'update_postimage', F.lit(3)).when(F.col('_change_type') == 'insert', F.lit(2)).otherwise(F.lit(1)).alias('_change_rank')).where(F.col('EVENT_ID').isNotNull())
    return prepared.groupBy('EVENT_ID').agg(F.max('_commit_version').alias('STRING_RESULT_CDF_COMMIT_VERSION'), F.max('_commit_timestamp').alias('STRING_RESULT_CDF_COMMIT_TIMESTAMP'), F.max_by('_change_type', F.struct('_commit_version', '_commit_timestamp', '_change_rank')).alias('STRING_RESULT_CDF_CHANGE_TYPE'))

def _mne_simple_cdf_keys(table_name: str, key_column: str, previous_version: int, current_version: int, key_is_double: bool) -> DataFrame:
    cdf = _mne_read_cdf(table_name, previous_version + 1, current_version)
    if cdf is None:
        return spark.createDataFrame([], 'CHANGE_KEY long')
    key = _mne_checked_double_id(F.col(key_column), f'{table_name}.{key_column}') if key_is_double else F.col(key_column).cast('long')
    return cdf.select(key.alias('CHANGE_KEY')).where(F.col('CHANGE_KEY').isNotNull()).dropDuplicates()

def _mne_target_events_for_changed_codes(target: DataFrame, changed_codes: DataFrame) -> DataFrame:
    candidate_columns = [column for column in ('UNIT_OF_MEASURE_CD', 'STRING_RESULT_FORMAT_CD', 'EVENT_CLASS_CD', 'EVENT_CD', 'CATALOG_TYPE_CD', 'CONTRIBUTOR_SYSTEM_CD', 'NORMALCY_CD', 'NORMALCY_METHOD_CD', 'ENTRY_MODE_CD', 'RESULT_UNITS_CD', 'RESULT_TIME_UNITS_CD', 'TASK_ASSAY_CD', 'RESULT_STATUS_CD', 'RECORD_STATUS_CD', 'QC_REVIEW_CD', 'EVENT_RELTN_CD', 'SOURCE_CD', 'PARENT_EVENT_CLASS_CD', 'PARENT_EVENT_CD', 'PARENT_CATALOG_TYPE_CD', 'PARENT_RESULT_STATUS_CD') if column in target.columns]
    if not candidate_columns:
        return _mne_empty_long_keys()
    exploded = target.select('EVENT_ID', F.explode_outer(F.array_distinct(F.array(*[F.col(column).cast('long') for column in candidate_columns]))).alias('CHANGE_KEY')).where(F.col('CHANGE_KEY').isNotNull())
    return exploded.join(F.broadcast(changed_codes), 'CHANGE_KEY', 'inner').select('EVENT_ID').dropDuplicates()

def _mne_target_events_for_changed_catalogs(target: DataFrame, changed_catalogs: DataFrame) -> DataFrame:
    candidate_columns = [column for column in ('CATALOG_CD', 'PARENT_CATALOG_CD') if column in target.columns]
    if not candidate_columns:
        return _mne_empty_long_keys()
    exploded = target.select('EVENT_ID', F.explode_outer(F.array_distinct(F.array(*[F.col(column).cast('long') for column in candidate_columns]))).alias('CHANGE_KEY')).where(F.col('CHANGE_KEY').isNotNull())
    return exploded.join(F.broadcast(changed_catalogs), 'CHANGE_KEY', 'inner').select('EVENT_ID').dropDuplicates()

def _mne_manual_map_changed_values(config: MapNumericEventsConfig, previous_version: int, current_version: int) -> Tuple[DataFrame, DataFrame]:
    cdf = _mne_read_cdf(config.manual_map_table, previous_version + 1, current_version)
    empty = spark.createDataFrame([], 'CHANGE_KEY long')
    if cdf is None:
        return (empty, empty)
    relevant = cdf.where((F.col('SourceTable') == 'dbo.PI_CDE_CLINICAL_EVENT') & ((F.col('SourceField') == 'EVENT_CD') & F.col('OMOPField').isin('measurement_concept_id', 'observation_concept_id') | (F.col('SourceField') == 'EVENT_RESULT_UNITS_CD') & (F.col('OMOPField') == 'unit_concept_id'))).withColumn('CHANGE_KEY', F.expr('try_cast(SourceValue as bigint)'))
    event_values = relevant.where(F.col('SourceField') == 'EVENT_CD').select('CHANGE_KEY').where(F.col('CHANGE_KEY').isNotNull()).dropDuplicates()
    unit_values = relevant.where(F.col('SourceField') == 'EVENT_RESULT_UNITS_CD').select('CHANGE_KEY').where(F.col('CHANGE_KEY').isNotNull()).dropDuplicates()
    return (event_values, unit_values)

def _mne_target_events_for_changed_manual_maps(target: DataFrame, event_values: DataFrame, unit_values: DataFrame) -> DataFrame:
    event_matches = target.select('EVENT_ID', F.col('EVENT_CD').cast('long').alias('CHANGE_KEY')).join(F.broadcast(event_values), 'CHANGE_KEY', 'inner').select('EVENT_ID')
    unit_matches = target.select('EVENT_ID', F.explode_outer(F.array_distinct(F.array(F.col('UNIT_OF_MEASURE_CD').cast('long'), F.col('RESULT_UNITS_CD').cast('long'), F.col('EFFECTIVE_UNIT_OF_MEASURE_CD').cast('long')))).alias('CHANGE_KEY')).where(F.col('CHANGE_KEY').isNotNull()).join(F.broadcast(unit_values), 'CHANGE_KEY', 'inner').select('EVENT_ID')
    return event_matches.unionByName(unit_matches).dropDuplicates()

def _mne_target_events_for_changed_concepts(
    config: MapNumericEventsConfig,
    target: DataFrame,
    changed_concepts: DataFrame,
    manual_map_version: int,
) -> DataFrame:
    """Re-drive affected events from retained source keys and current mapping rules.

    Unit candidate audit arrays were intentionally cut from the single-layer target.
    Deriving triggers from the pinned manual-map snapshot preserves complete concept
    invalidation semantics without depending on removed output columns.
    """
    changed = (
        changed_concepts.select(
            F.col('CHANGE_KEY').cast('long').alias('CHANGED_CONCEPT_ID')
        )
        .where(F.col('CHANGED_CONCEPT_ID').isNotNull())
        .dropDuplicates()
    )
    affected_rules = (
        _mne_read_snapshot(config.manual_map_table, manual_map_version)
        .where(
            (F.col('SourceTable') == 'dbo.PI_CDE_CLINICAL_EVENT')
            & (
                (
                    (F.col('SourceField') == 'EVENT_CD')
                    & F.col('OMOPField').isin(
                        'measurement_concept_id', 'observation_concept_id'
                    )
                )
                | (
                    (F.col('SourceField') == 'EVENT_RESULT_UNITS_CD')
                    & (F.col('OMOPField') == 'unit_concept_id')
                )
            )
        )
        .select(
            F.col('SourceField').alias('MAP_SOURCE_FIELD'),
            F.expr('try_cast(SourceValue as bigint)').alias('MAP_SOURCE_VALUE_LONG'),
            F.expr('try_cast(OMOPConceptId as bigint)').alias('MAP_OMOP_CONCEPT_ID'),
            F.expr('try_cast(EVENT_CLASS_CD as bigint)').alias('MAP_EVENT_CLASS_CD'),
        )
        .where(
            F.col('MAP_SOURCE_VALUE_LONG').isNotNull()
            & F.col('MAP_OMOP_CONCEPT_ID').isNotNull()
        )
        .join(
            F.broadcast(changed),
            F.col('MAP_OMOP_CONCEPT_ID') == F.col('CHANGED_CONCEPT_ID'),
            'inner',
        )
    )
    event_rules = (
        affected_rules.where(F.col('MAP_SOURCE_FIELD') == 'EVENT_CD')
        .select('MAP_SOURCE_VALUE_LONG', 'MAP_EVENT_CLASS_CD')
        .dropDuplicates()
    )
    event_matches = (
        target.alias('t')
        .join(
            F.broadcast(event_rules).alias('r'),
            (F.col('t.EVENT_CD').cast('long') == F.col('r.MAP_SOURCE_VALUE_LONG'))
            & (
                F.col('r.MAP_EVENT_CLASS_CD').isNull()
                | (
                    F.col('t.EVENT_CLASS_CD').cast('long')
                    == F.col('r.MAP_EVENT_CLASS_CD')
                )
            ),
            'inner',
        )
        .select(F.col('t.EVENT_ID').alias('EVENT_ID'))
    )
    unit_rules = (
        affected_rules.where(
            F.col('MAP_SOURCE_FIELD') == 'EVENT_RESULT_UNITS_CD'
        )
        .select('MAP_SOURCE_VALUE_LONG')
        .dropDuplicates()
    )
    unit_matches = (
        target.alias('t')
        .join(
            F.broadcast(unit_rules).alias('r'),
            F.col('t.EFFECTIVE_UNIT_OF_MEASURE_CD').cast('long')
            == F.col('r.MAP_SOURCE_VALUE_LONG'),
            'inner',
        )
        .select(F.col('t.EVENT_ID').alias('EVENT_ID'))
    )
    return event_matches.unionByName(unit_matches).dropDuplicates()

def _mne_build_changed_event_keys(config: MapNumericEventsConfig, previous_versions: Dict[str, int], current_versions: Dict[str, int]) -> DataFrame:
    target = spark.table(config.target_table)
    string_meta = _mne_string_result_changed_metadata(config, previous_versions[config.string_result_table], current_versions[config.string_result_table])
    trigger_frames: List[DataFrame] = [string_meta.select('EVENT_ID').withColumn('TRIGGER_SOURCE', F.lit('STRING_RESULT'))]
    ce_ids = _mne_simple_cdf_keys(config.clinical_event_table, 'EVENT_ID', previous_versions[config.clinical_event_table], current_versions[config.clinical_event_table], key_is_double=False)
    direct_ce = ce_ids.select(F.col('CHANGE_KEY').alias('EVENT_ID')).withColumn('TRIGGER_SOURCE', F.lit('CLINICAL_EVENT'))
    parent_ce = target.select('EVENT_ID', 'PARENT_EVENT_ID').join(ce_ids, F.col('PARENT_EVENT_ID') == F.col('CHANGE_KEY'), 'inner').select('EVENT_ID').withColumn('TRIGGER_SOURCE', F.lit('PARENT_CLINICAL_EVENT'))
    trigger_frames.extend([direct_ce, parent_ce])
    code_ids = _mne_simple_cdf_keys(config.code_value_table, 'CODE_VALUE', previous_versions[config.code_value_table], current_versions[config.code_value_table], key_is_double=True)
    trigger_frames.append(_mne_target_events_for_changed_codes(target, code_ids).withColumn('TRIGGER_SOURCE', F.lit('CODE_VALUE')))
    catalog_ids = _mne_simple_cdf_keys(config.order_catalog_table, 'CATALOG_CD', previous_versions[config.order_catalog_table], current_versions[config.order_catalog_table], key_is_double=True)
    trigger_frames.append(_mne_target_events_for_changed_catalogs(target, catalog_ids).withColumn('TRIGGER_SOURCE', F.lit('ORDER_CATALOG')))
    event_map_values, unit_map_values = _mne_manual_map_changed_values(config, previous_versions[config.manual_map_table], current_versions[config.manual_map_table])
    trigger_frames.append(_mne_target_events_for_changed_manual_maps(target, event_map_values, unit_map_values).withColumn('TRIGGER_SOURCE', F.lit('MANUAL_MAP')))
    concept_ids = _mne_simple_cdf_keys(config.concept_table, 'concept_id', previous_versions[config.concept_table], current_versions[config.concept_table], key_is_double=False)
    trigger_frames.append(_mne_target_events_for_changed_concepts(
        config,
        target,
        concept_ids,
        current_versions[config.manual_map_table],
    ).withColumn('TRIGGER_SOURCE', F.lit('OMOP_CONCEPT')))
    triggers = _mne_union_event_triggers(trigger_frames)
    grouped = triggers.where(F.col('EVENT_ID').isNotNull()).groupBy('EVENT_ID').agg(F.concat_ws(',', F.sort_array(F.collect_set('TRIGGER_SOURCE'))).alias('TRIGGER_SOURCES'))
    return grouped.join(string_meta, 'EVENT_ID', 'left')

def _mne_apply_target_properties(config: MapNumericEventsConfig) -> None:
    retention_days = _pipeline_builtins.max(int(config.cdf_retention_days), 8)
    spark.sql(f"\n        ALTER TABLE {_mne_sql_identifier(config.target_table)}\n        SET TBLPROPERTIES (\n            'delta.enableChangeDataFeed' = 'true',\n            'delta.enableDeletionVectors' = 'true',\n            'delta.deletedFileRetentionDuration' = 'interval {retention_days} days',\n            'delta.logRetentionDuration' = 'interval {_pipeline_builtins.max(retention_days, 30)} days'\n        )\n    ")
    spark.sql(f"COMMENT ON TABLE {_mne_sql_identifier(config.target_table)} IS '{_mne_escape_sql_string(map_numeric_events_comment)}'")
    spark.sql(f'ALTER TABLE {_mne_sql_identifier(config.target_table)} CLUSTER BY (EVENT_ID)')

def _mne_ensure_target_schema(config: MapNumericEventsConfig) -> None:
    if not _mne_table_exists(config.target_table):
        return
    current_schema = spark.table(config.target_table).schema
    current_fields = {field.name: field for field in current_schema.fields}
    additions: List[str] = []
    tick = chr(96)
    for field in bronze_contract_schema(config.target_table).fields:
        if field.name not in current_fields:
            comment = _mne_escape_sql_string(field.metadata.get('comment', ''))
            definition = f'{tick}{field.name}{tick} {field.dataType.simpleString()}'
            if comment:
                definition += f" COMMENT '{comment}'"
            additions.append(definition)
        elif current_fields[field.name].dataType != field.dataType:
            raise RuntimeError(f'Existing column {field.name} has type {current_fields[field.name].dataType.simpleString()}, expected {field.dataType.simpleString()}. Run process_numeric_events_incremental(force_full_rebuild=True) to apply the schema safely from source data.')
    if additions:
        spark.sql(f"ALTER TABLE {_mne_sql_identifier(config.target_table)} ADD COLUMNS ({', '.join(additions)})")
    _mne_apply_target_properties(config)

def _mne_validate_unique_event_grain(table_name: str) -> Dict[str, int]:
    row = spark.sql(f'\n        SELECT\n            COUNT(*) AS row_count,\n            COUNT(DISTINCT EVENT_ID) AS distinct_event_ids,\n            COUNT_IF(EVENT_ID IS NULL) AS null_event_ids\n        FROM {_mne_sql_identifier(table_name)}\n    ').first()
    metrics = {'row_count': int(row['row_count']), 'distinct_event_ids': int(row['distinct_event_ids']), 'null_event_ids': int(row['null_event_ids'])}
    if metrics['null_event_ids'] != 0:
        raise RuntimeError(f'{table_name} contains null EVENT_ID values: {metrics}')
    if metrics['row_count'] != metrics['distinct_event_ids']:
        raise RuntimeError(f'{table_name} is not unique by EVENT_ID: {metrics}')
    return metrics

def _mne_full_rebuild(config: MapNumericEventsConfig, source_versions: Dict[str, int], run_id: str, run_timestamp: datetime, safe_rebuild: bool, backup_existing: bool) -> Dict[str, object]:
    print('[INFO] Building the unfiltered latest CE_STRING_RESULT snapshot.')
    string_rows = _mne_latest_string_results(config, source_versions[config.string_result_table]).withColumn('STRING_RESULT_CDF_COMMIT_VERSION', F.lit(None).cast('long')).withColumn('STRING_RESULT_CDF_COMMIT_TIMESTAMP', F.lit(None).cast('timestamp')).withColumn('STRING_RESULT_CDF_CHANGE_TYPE', F.lit(None).cast('string')).withColumn('TRIGGER_SOURCES', F.lit('FULL_REBUILD'))
    rebuilt = _mne_build_enriched_numeric_events(config, string_rows, source_versions, run_id, run_timestamp, broadcast_keys=False)
    if config.numeric_semantic_only:
        rebuilt = rebuilt.where(F.col('IN_NUMERIC_SCOPE_IND'))
    rebuilt = bronze_project_contract(rebuilt, config.target_table)
    backup_table: Optional[str] = None
    if safe_rebuild:
        suffix = run_id.replace('-', '')
        staging_table = f'{config.target_table}__rebuild_{suffix}'
        print(f'[INFO] Writing validated rebuild staging table {staging_table}')
        rebuilt.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(staging_table)
        metrics = _mne_validate_unique_event_grain(staging_table)
        print(f'[INFO] Staging validation passed: {metrics}')
        if backup_existing and _mne_table_exists(config.target_table):
            backup_table = f'{config.target_table}__backup_{suffix}'
            spark.sql(f'CREATE TABLE {_mne_sql_identifier(backup_table)} SHALLOW CLONE {_mne_sql_identifier(config.target_table)}')
            print(f'[INFO] Existing target preserved as shallow clone {backup_table}')
        spark.table(staging_table).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(config.target_table)
        spark.sql(f'DROP TABLE {_mne_sql_identifier(staging_table)}')
    else:
        rebuilt.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(config.target_table)
        metrics = _mne_validate_unique_event_grain(config.target_table)
    _mne_apply_target_properties(config)
    return {'mode': 'FULL_REBUILD', 'backup_table': backup_table, **metrics}

def _mne_merge_active_updates(config: MapNumericEventsConfig, updates: DataFrame) -> Dict[str, int]:
    updates = bronze_project_contract(updates, config.target_table)
    source_rows = updates.count()
    if source_rows == 0:
        return {'active_source_rows': 0}
    assignments = {
        column_name: F.col(f's.{column_name}')
        for column_name in updates.columns
    }
    comparisons = ' OR '.join(
        f'NOT (t.`{column_name}` <=> s.`{column_name}`)'
        for column_name in updates.columns
        if column_name != 'EVENT_ID'
    )
    DeltaTable.forName(spark, config.target_table).alias('t').merge(
        updates.alias('s'),
        't.EVENT_ID <=> s.EVENT_ID',
    ).whenMatchedUpdate(
        condition=comparisons or 'false',
        set=assignments,
    ).whenNotMatchedInsert(values=assignments).execute()
    return {'active_source_rows': int(source_rows)}

def _mne_mark_deleted_or_out_of_scope(config: MapNumericEventsConfig, deleted_keys: DataFrame, source_versions: Dict[str, int], run_id: str, run_timestamp: datetime) -> Dict[str, int]:
    prepared = deleted_keys.select('EVENT_ID', 'STRING_RESULT_CDF_COMMIT_VERSION', 'STRING_RESULT_CDF_COMMIT_TIMESTAMP', 'STRING_RESULT_CDF_CHANGE_TYPE', 'TRIGGER_SOURCES').withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_UPDT_DT_TM', F.lit(run_timestamp)).withColumn('STRING_RESULT_SOURCE_VERSION', F.lit(source_versions[config.string_result_table]).cast('long')).withColumn('CLINICAL_EVENT_SOURCE_VERSION', F.lit(source_versions[config.clinical_event_table]).cast('long')).withColumn('CODE_VALUE_SOURCE_VERSION', F.lit(source_versions[config.code_value_table]).cast('long')).withColumn('ORDER_CATALOG_SOURCE_VERSION', F.lit(source_versions[config.order_catalog_table]).cast('long')).withColumn('MANUAL_MAP_SOURCE_VERSION', F.lit(source_versions[config.manual_map_table]).cast('long')).withColumn('CONCEPT_SOURCE_VERSION', F.lit(source_versions[config.concept_table]).cast('long'))
    try:
        deleted_count = prepared.count()
        if deleted_count == 0:
            return {'deleted_or_out_of_scope_keys': 0}
        requested_updates = {
            'SOURCE_DELETED_IND': 'true',
            'SOURCE_CURRENT_IND': 'false',
            'IN_NUMERIC_SCOPE_IND': 'false',
            'STRING_RESULT_CDF_COMMIT_VERSION': 's.STRING_RESULT_CDF_COMMIT_VERSION',
            'STRING_RESULT_CDF_COMMIT_TIMESTAMP': 's.STRING_RESULT_CDF_COMMIT_TIMESTAMP',
            'STRING_RESULT_CDF_CHANGE_TYPE': 's.STRING_RESULT_CDF_CHANGE_TYPE',
            'TRIGGER_SOURCES': 's.TRIGGER_SOURCES',
            'STRING_RESULT_SOURCE_VERSION': 's.STRING_RESULT_SOURCE_VERSION',
            'CLINICAL_EVENT_SOURCE_VERSION': 's.CLINICAL_EVENT_SOURCE_VERSION',
            'CODE_VALUE_SOURCE_VERSION': 's.CODE_VALUE_SOURCE_VERSION',
            'ORDER_CATALOG_SOURCE_VERSION': 's.ORDER_CATALOG_SOURCE_VERSION',
            'MANUAL_MAP_SOURCE_VERSION': 's.MANUAL_MAP_SOURCE_VERSION',
            'CONCEPT_SOURCE_VERSION': 's.CONCEPT_SOURCE_VERSION',
            'ROW_HASH': "xxhash64(coalesce(cast(t.ROW_HASH as string), ''), 'SOURCE_DELETED')",
            'PIPELINE_RUN_ID': 's.PIPELINE_RUN_ID',
            'PIPELINE_UPDT_DT_TM': 's.PIPELINE_UPDT_DT_TM',
        }
        allowed = bronze_contract_column_set(config.target_table)
        retained_updates = {
            column_name: expression
            for column_name, expression in requested_updates.items()
            if column_name.lower() in allowed
        }
        DeltaTable.forName(spark, config.target_table).alias('t').merge(
            prepared.alias('s'),
            't.EVENT_ID <=> s.EVENT_ID',
        ).whenMatchedUpdate(set=retained_updates).execute()
        return {'deleted_or_out_of_scope_keys': int(deleted_count)}
    finally:
        None

def _mne_incremental_update(config: MapNumericEventsConfig, previous_versions: Dict[str, int], source_versions: Dict[str, int], run_id: str, run_timestamp: datetime) -> Dict[str, object]:
    _mne_ensure_target_schema(config)
    changed_keys = _mp30_persist_stage(_mne_build_changed_event_keys(config, previous_versions, source_versions))
    try:
        changed_count = changed_keys.count()
        if changed_count == 0:
            return {'mode': 'INCREMENTAL', 'changed_event_ids': 0, 'active_source_rows': 0, 'deleted_or_out_of_scope_keys': 0}
        broadcast_keys = changed_count <= int(config.broadcast_event_id_limit)
        string_rows = _mne_latest_string_results(config, source_versions[config.string_result_table], event_keys=changed_keys, broadcast_keys=broadcast_keys)
        string_rows = _mp30_persist_stage(string_rows.join(changed_keys, 'EVENT_ID', 'inner'))
        all_updates = _mne_build_enriched_numeric_events(config, string_rows, source_versions, run_id, run_timestamp, broadcast_keys=broadcast_keys)
        updates = _mp30_persist_stage(all_updates.where(F.col('IN_NUMERIC_SCOPE_IND')) if config.numeric_semantic_only else all_updates)
        try:
            retained_keys = updates.select('EVENT_ID').dropDuplicates()
            deleted_or_out_of_scope = changed_keys.join(retained_keys, 'EVENT_ID', 'left_anti')
            metrics: Dict[str, object] = {'mode': 'INCREMENTAL', 'changed_event_ids': int(changed_count), 'broadcast_event_ids': bool(broadcast_keys)}
            metrics.update(_mne_merge_active_updates(config, updates))
            metrics.update(_mne_mark_deleted_or_out_of_scope(config, deleted_or_out_of_scope, source_versions, run_id, run_timestamp))
            missing_active = retained_keys.join(spark.table(config.target_table).select('EVENT_ID'), 'EVENT_ID', 'left_anti').limit(1).count()
            if missing_active:
                raise RuntimeError('At least one changed source EVENT_ID was absent after the merge')
            return metrics
        finally:
            _mp30_persist_stage_drop(updates)
            _mp30_persist_stage_drop(string_rows)
    finally:
        _mp30_persist_stage_drop(changed_keys)

def process_numeric_events_incremental(force_full_rebuild: bool=False, bootstrap_if_state_missing: bool=True, safe_rebuild: bool=True, backup_existing: bool=True, config: MapNumericEventsConfig=MAP_NUMERIC_EVENTS_CONFIG) -> Dict[str, object]:
    """
    Drop-in replacement for the original process_numeric_events_incremental().

    Behaviour:
      * first run or missing source-version state: validated full rebuild from source;
      * later runs: CDF by stored Delta version for every mutable upstream source;
      * no timestamp fallback and no target MAX(ADC_UPDT) scan;
      * no validity, result-status, parse-success, or source-current filtering;
      * the configured Trust value is an explicit dataset scope and can be disabled;
      * string-result, clinical-event, parent-event, code-value, catalog, manual-map,
        and OMOP concept changes can all trigger refreshes;
      * physical source deletion or semantic-scope exit marks the target row deleted;
      * state advances only after all target writes and validation succeed.
    """
    run_id = str(uuid.uuid4())
    run_timestamp = datetime.now(timezone.utc)
    mode = 'UNKNOWN'
    source_versions: Dict[str, int] = {}
    _mne_ensure_metadata_tables(config)
    try:
        source_versions = _mne_capture_source_versions(config)
        previous_versions = _mne_read_pipeline_state(config)
        missing_state = [source for source in _mne_source_tables(config) if source not in previous_versions]
        target_missing = not _mne_table_exists(config.target_table)
        requires_bootstrap = target_missing or bool(missing_state)
        if requires_bootstrap and (not (bootstrap_if_state_missing or force_full_rebuild)):
            raise RuntimeError(f'Pipeline state is missing for one or more sources. Run with force_full_rebuild=True or bootstrap_if_state_missing=True. Missing state: {missing_state}')
        mode = 'FULL_REBUILD' if force_full_rebuild or requires_bootstrap else 'INCREMENTAL'
        _mne_write_audit_event(config, run_id, status='STARTED', mode=mode, message='map_numeric_events load started', source_versions=source_versions)
        if mode == 'FULL_REBUILD':
            metrics = _mne_full_rebuild(config, source_versions, run_id, run_timestamp, safe_rebuild=safe_rebuild, backup_existing=backup_existing)
        else:
            for source in _mne_source_tables(config):
                previous_version = previous_versions[source]
                current_version = source_versions[source]
                if previous_version > current_version:
                    raise RuntimeError(f'Stored state version {previous_version} is ahead of current version {current_version} for {source}')
            metrics = _mne_incremental_update(config, previous_versions, source_versions, run_id, run_timestamp)
        completed_at = datetime.now(timezone.utc)
        _mne_commit_pipeline_state(config, source_versions, run_id, completed_at)
        _mne_write_audit_event(config, run_id, status='SUCCESS', mode=mode, message='map_numeric_events load completed', source_versions=source_versions, metrics=metrics)
        print(f'[SUCCESS] map_numeric_events {mode} completed: {metrics}')
        return metrics
    except Exception as exc:
        _mne_write_audit_event(config, run_id, status='FAILED', mode=mode, message=str(exc), source_versions=source_versions)
        print(f'[ERROR] map_numeric_events {mode} failed: {exc}')
        raise

def rebuild_map_numeric_events_v2(safe_rebuild: bool=True, backup_existing: bool=True, config: MapNumericEventsConfig=MAP_NUMERIC_EVENTS_CONFIG) -> Dict[str, object]:
    """Explicit one-time V2 rebuild helper."""
    return process_numeric_events_incremental(force_full_rebuild=True, bootstrap_if_state_missing=True, safe_rebuild=safe_rebuild, backup_existing=backup_existing, config=config)

def plan_map_numeric_events_run(config: MapNumericEventsConfig=MAP_NUMERIC_EVENTS_CONFIG) -> Dict[str, object]:
    """Read-only run-plan summary; it does not create metadata tables or modify state."""
    source_versions = _mne_capture_source_versions(config)
    state = _mne_read_pipeline_state(config) if _mne_table_exists(config.state_table) else {}
    missing_state = [source for source in _mne_source_tables(config) if source not in state]
    target_exists = _mne_table_exists(config.target_table)
    planned_mode = 'FULL_REBUILD' if not target_exists or missing_state else 'INCREMENTAL'
    return {'target_table': config.target_table, 'target_exists': target_exists, 'planned_mode': planned_mode, 'trust_scope': config.trust_scope, 'numeric_semantic_only': config.numeric_semantic_only, 'current_source_versions': source_versions, 'stored_source_versions': state, 'missing_state_sources': missing_state}

def validate_map_numeric_events(compare_to_source: bool=False, config: MapNumericEventsConfig=MAP_NUMERIC_EVENTS_CONFIG) -> Dict[str, object]:
    """Read-only validation over the retained single-layer numeric contract."""
    if not _mne_table_exists(config.target_table):
        raise RuntimeError(f'Target table does not exist: {config.target_table}')
    metrics: Dict[str, object] = _mne_validate_unique_event_grain(config.target_table)
    quality = spark.sql(f"""
        SELECT
            COUNT_IF(STRING_RESULT_ADC_UPDT IS NULL) AS null_string_result_adc_updt,
            COUNT_IF(STRING_RESULT_EFFECTIVE_UPDT_DT_TM IS NULL) AS null_effective_source_updt,
            COUNT_IF(SOURCE_DELETED_IND = TRUE) AS source_deleted_rows,
            COUNT_IF(
                OMOP_MANUAL_CONCEPT_ID IS NOT NULL
                AND OMOP_MAPPING_VALID_IND = FALSE
            ) AS selected_invalid_omop_concepts,
            COUNT_IF(EVENT_LABEL IS NULL) AS missing_event_label,
            COUNT_IF(
                UNIT_OF_MEASURE_CD = 0
                AND UNIT_OF_MEASURE_DISPLAY IS NULL
            ) AS sentinel_zero_unit_rows,
            COUNT_IF(
                CATALOG_CD = 0
                AND CATALOG_DISPLAY IS NULL
            ) AS sentinel_zero_catalog_rows
        FROM {_mne_sql_identifier(config.target_table)}
    """).first()
    metrics.update({key: int(value or 0) for key, value in quality.asDict().items()})
    if compare_to_source:
        if config.numeric_semantic_only:
            raise RuntimeError(
                'compare_to_source=True with numeric_semantic_only=True requires '
                'the semantic classification plan and is intentionally not implicit.'
            )
        source_version = _mne_latest_delta_version(config.string_result_table)
        source_ids = _mne_latest_string_results(
            config, source_version
        ).select('EVENT_ID').dropDuplicates()
        target_ids = (
            spark.table(config.target_table)
            .where(~F.col('SOURCE_DELETED_IND'))
            .select('EVENT_ID')
        )
        metrics['missing_source_event_ids'] = source_ids.join(
            target_ids, 'EVENT_ID', 'left_anti'
        ).count()
        metrics['active_target_ids_absent_from_source'] = target_ids.join(
            source_ids, 'EVENT_ID', 'left_anti'
        ).count()
    return metrics
def configure_map_numeric_events_cdf_retention(retention_days: int=30, config: MapNumericEventsConfig=MAP_NUMERIC_EVENTS_CONFIG) -> None:
    """
    Optional operational helper.

    It changes shared source-table retention and is therefore never called automatically.
    Longer retention reduces rebuild risk after delayed or failed runs.
    """
    if retention_days < 8:
        raise ValueError('retention_days must be at least 8')
    for source in _mne_source_tables(config):
        spark.sql(f"\n            ALTER TABLE {_mne_sql_identifier(source)}\n            SET TBLPROPERTIES (\n                'delta.deletedFileRetentionDuration' = 'interval {int(retention_days)} days',\n                'delta.logRetentionDuration' = 'interval {_pipeline_builtins.max(int(retention_days), 30)} days'\n            )\n        ")
        print(f'[INFO] Updated CDF-supporting retention for {source}')

def optimize_map_numeric_events(config: MapNumericEventsConfig=MAP_NUMERIC_EVENTS_CONFIG) -> None:
    """
    Explicit maintenance helper. It is not called by the load because OPTIMIZE on this table is
    a large operation and should be scheduled independently.
    """
    if not _mne_table_exists(config.target_table):
        raise RuntimeError(f'Target table does not exist: {config.target_table}')
    _mne_apply_target_properties(config)
    spark.sql(f'OPTIMIZE {_mne_sql_identifier(config.target_table)}')

def preview_map_numeric_events(event_ids: Sequence[int], config: MapNumericEventsConfig=MAP_NUMERIC_EVENTS_CONFIG) -> DataFrame:
    """
    Build current replacement output for a small explicit EVENT_ID list without writing data.

    This is intended for deployment smoke tests and row-level comparison with the existing target.
    """
    normalized_ids = sorted({int(event_id) for event_id in event_ids})
    if not normalized_ids:
        raise ValueError('event_ids must contain at least one EVENT_ID')
    if len(normalized_ids) > int(config.broadcast_event_id_limit):
        raise ValueError(f'preview is limited to {config.broadcast_event_id_limit} distinct EVENT_ID values')
    source_versions = _mne_capture_source_versions(config)
    keys = spark.createDataFrame([(event_id,) for event_id in normalized_ids], 'EVENT_ID long')
    string_rows = _mne_latest_string_results(config, source_versions[config.string_result_table], event_keys=keys, broadcast_keys=True).withColumn('STRING_RESULT_CDF_COMMIT_VERSION', F.lit(None).cast('long')).withColumn('STRING_RESULT_CDF_COMMIT_TIMESTAMP', F.lit(None).cast('timestamp')).withColumn('STRING_RESULT_CDF_CHANGE_TYPE', F.lit(None).cast('string')).withColumn('TRIGGER_SOURCES', F.lit('PREVIEW'))
    return _mne_build_enriched_numeric_events(config, string_rows, source_versions, run_id=f'preview-{uuid.uuid4()}', run_timestamp=datetime.now(timezone.utc), broadcast_keys=True)
print('map_numeric_events replacement functions loaded. Call plan_map_numeric_events_run() for a read-only plan, or process_numeric_events_incremental() to run. A state-less first run performs a validated full rebuild.')

try:
    _pipeline_run_recoverable('map_numeric_events', _PIPELINE_FULL_REFRESH, lambda: process_numeric_events_incremental(force_full_rebuild=False, backup_existing=False), lambda: process_numeric_events_incremental(force_full_rebuild=True, backup_existing=False))
    _PIPELINE_UPDATED_TARGETS.extend(['4_prod.bronze.map_numeric_events'])
    _pipeline_mark_component_complete('map_numeric_events', ['4_prod.bronze.map_numeric_events'])
    _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_numeric_events'})
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

_pipeline_component_start("map_text_events")
_pipeline_shared_update_table = globals().get('update_table')
_pipeline_shared_table_exists = globals().get('table_exists')
_pipeline_shared_get_max_timestamp = globals().get('get_max_timestamp')
_pipeline_shared_has_cdf_enabled = globals().get('has_cdf_enabled')
_pipeline_shared_get_incremental = globals().get('get_incremental_data_with_cdf')
globals().pop('_map_updates_original_update_table', None)
globals().pop('_map_medical_personnel_original_update_table', None)
globals().pop('_MAP_UPDATES_ORIGINAL_UPDATE_TABLE', None)

# Retired duplicate numeric fallback. The primary numeric implementation above is the
# only definition; keeping a second schema/merge implementation here allowed drift.
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
import uuid
import builtins as _mte_builtins

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


@dataclass(frozen=True)
class MapTextEventsConfig(MapNumericEventsConfig):
    target_table: str = "4_prod.bronze.map_text_events"
    state_table: str = "6_mgmt.bronze.map_text_events_pipeline_state"
    audit_table: str = "6_mgmt.bronze.map_text_events_pipeline_audit"
    pipeline_name: str = "map_text_events_v2"
    long_text_table: str = "4_prod.raw.mill_long_text"
    current_view: str = "4_prod.bronze.map_text_events_current"
    numeric_base_table: str = "4_prod.bronze.map_numeric_events"
    numeric_state_table: str = "6_mgmt.bronze.map_numeric_events_pipeline_state"

    # None is the no-filter bronze default. Set to "Barts" only when Trust is an intentional
    # dataset boundary. Null Trust rows are retained when the scope is None.
    trust_scope: Optional[str] = None

    # The canonical text table is scoped to non-numeric, genuinely textual results.
    # IN_TEXT_SCOPE_IND is the exact complement of numeric parse success
    # (NOT NUMERIC_PARSE_SUCCESS_IND), so the text and numeric lanes cover the whole source
    # between them and no parsed numeric result is also counted as text. The one deliberate
    # overlap is a Millennium Numeric-class event whose text does not parse: it stays in the
    # numeric scope for its event class and in the text scope because nothing was parsed.
    # Scope is a curation boundary, not a retention decision. 4_prod.raw.mill_ce_string_result
    # remains the retention layer and keeps every source row unfiltered. Set False only to
    # restore the old unscoped table contract.
    text_semantic_only: bool = True
    numeric_semantic_only: bool = True
    cdf_retention_days: int = 30


MAP_TEXT_EVENTS_CONFIG = MapTextEventsConfig()


def _mte_source_tables(config: MapTextEventsConfig) -> Tuple[str, ...]:
    return tuple(_mne_source_tables(config)) + (config.long_text_table,)


map_text_events_comment = (
    "Curated latest-state bronze table for Millennium CE_STRING_RESULT rows whose result is "
    "genuinely textual. One row is retained per EVENT_ID, scoped to IN_TEXT_SCOPE_IND, which is "
    "the exact complement of numeric parse success, so map_text_events and map_numeric_events "
    "cover the source between them and no parsed numeric result is also counted as text; the one "
    "deliberate overlap is a Millennium Numeric-class event whose text does not parse. Within "
    "that scope no validity, result-status or Trust filtering is applied, and "
    "4_prod.raw.mill_ce_string_result remains the retention layer holding every source row. "
    "Short text and LONG_TEXT are both preserved; clinical-event, parent, catalog, code-value, "
    "provenance, validity, quality, source-version, deletion, and deterministic OMOP mapping "
    "metadata are included so downstream layers can apply their own rules."
)


def _mte_field(name: str, data_type: T.DataType, comment: str) -> T.StructField:
    return T.StructField(name, data_type, True, metadata={"comment": comment})


_MTE_EXTRA_FIELDS = [
    _mte_field("TEXT_RESULT", T.StringType(), "Best available text: LONG_TEXT, then STRING_RESULT_TEXT, then CLINICAL_EVENT.RESULT_VAL."),
    _mte_field("TEXT_RESULT_SHORT", T.StringType(), "Unmodified CE_STRING_RESULT.STRING_RESULT_TEXT, normally limited to 255 characters."),
    _mte_field("LONG_TEXT", T.StringType(), "Full text recovered from MILL_LONG_TEXT when STRING_LONG_TEXT_ID resolves."),
    _mte_field("LONG_TEXT_ID", T.LongType(), "Resolved MILL_LONG_TEXT identifier."),
    _mte_field("LONG_TEXT_FOUND_IND", T.BooleanType(), "True when STRING_LONG_TEXT_ID resolved to a long-text row."),
    _mte_field("LONG_TEXT_ACTIVE_IND", T.LongType(), "MILL_LONG_TEXT active indicator."),
    _mte_field("LONG_TEXT_UPDT_DT_TM", T.TimestampType(), "MILL_LONG_TEXT source update timestamp."),
    _mte_field("LONG_TEXT_ADC_UPDT", T.TimestampType(), "MILL_LONG_TEXT ADC update timestamp."),
    _mte_field("LONG_TEXT_TRUST", T.StringType(), "Trust supplied by MILL_LONG_TEXT."),
    _mte_field("TEXT_RESULT_SOURCE", T.StringType(), "LONG_TEXT, STRING_RESULT_TEXT, CLINICAL_EVENT_RESULT_VAL, or null."),
    _mte_field("TEXT_RESULT_LENGTH", T.LongType(), "Character length of TEXT_RESULT."),
    _mte_field("TEXT_RESULT_TRUNCATION_RECOVERED_IND", T.BooleanType(), "True when LONG_TEXT is longer than the short string result."),
    _mte_field("TEXT_SEMANTIC_IND", T.BooleanType(), "True when the raw string does not parse as a finite numeric value."),
    _mte_field("IN_TEXT_SCOPE_IND", T.BooleanType(), "Text semantic flag retained even when semantic-only mode is disabled."),
    _mte_field("RESULT_DT_TM", T.TimestampType(), "Clinically relevant result time: EVENT_END, PERFORMED, EVENT_START, then VERIFIED."),
    _mte_field("PARENT_EVENT_LABEL", T.StringType(), "Best available parent title/code/catalog label."),
    _mte_field("RESULT_DATETIME_PLACEHOLDER_IND", T.BooleanType(), "True for result timestamps before 1900-01-01."),
    _mte_field("RESULT_DATETIME_FUTURE_IND", T.BooleanType(), "True for result timestamps more than one day after the fixed run timestamp."),
    _mte_field("OMOP_MANUAL_VALUE_CONCEPT", T.StringType(), "Selected manual value_as_concept_id retained as text for compatibility."),
    _mte_field("OMOP_MANUAL_VALUE_CONCEPT_ID", T.LongType(), "Typed selected manual value concept identifier."),
    _mte_field("OMOP_MANUAL_VALUE_CONCEPT_NAME", T.StringType(), "Selected value concept name."),
    _mte_field("OMOP_MANUAL_VALUE_STANDARD_CONCEPT", T.StringType(), "Selected value concept standard-concept flag."),
    _mte_field("OMOP_MANUAL_VALUE_CONCEPT_DOMAIN", T.StringType(), "Selected value concept domain."),
    _mte_field("OMOP_MANUAL_VALUE_CONCEPT_CLASS", T.StringType(), "Selected value concept class."),
    _mte_field("OMOP_VALUE_MAPPING_VALID_IND", T.BooleanType(), "True when the selected value concept exists and is currently valid."),
    _mte_field("OMOP_VALUE_MAPPING_RULE_ID", T.StringType(), "Stable identifier for the selected value mapping rule."),
    _mte_field("OMOP_VALUE_MAPPING_MATCH_TYPE", T.StringType(), "How the selected value rule matched."),
    _mte_field("OMOP_VALUE_MAPPING_CANDIDATE_COUNT", T.LongType(), "Number of matching value-mapping rows."),
    _mte_field("OMOP_VALUE_MAPPING_DISTINCT_CONCEPT_COUNT", T.LongType(), "Number of distinct candidate value concepts."),
    _mte_field("OMOP_VALUE_MAPPING_AMBIGUOUS_IND", T.BooleanType(), "True when multiple distinct value concepts matched."),
    _mte_field("OMOP_VALUE_MAPPING_CANDIDATE_CONCEPT_IDS", T.ArrayType(T.LongType()), "Sorted distinct candidate value concept identifiers."),
    _mte_field("OMOP_MAPPING_SPECIFICITY", T.LongType(), "Specificity score of the selected event concept rule."),
    _mte_field("OMOP_VALUE_MAPPING_SPECIFICITY", T.LongType(), "Specificity score of the selected value concept rule."),
    _mte_field("LONG_TEXT_SOURCE_VERSION", T.LongType(), "MILL_LONG_TEXT Delta version used for this row build."),
]


_MTE_EXTRA_BY_NAME = {field.name: field for field in _MTE_EXTRA_FIELDS}
_MNE_FIELDS_BY_NAME = {field.name: field for field in schema_map_numeric_events.fields}

_MTE_LEGACY_ORDER = [
    "EVENT_ID", "ENCNTR_ID", "PERSON_ID", "ORDER_ID", "EVENT_CLASS_CD",
    "PERFORMED_PRSNL_ID", "TEXT_RESULT", "UNIT_OF_MEASURE_CD",
    "UNIT_OF_MEASURE_DISPLAY", "EVENT_TITLE_TEXT", "EVENT_CD", "EVENT_CD_DISPLAY",
    "CATALOG_CD", "CATALOG_DISPLAY", "CATALOG_TYPE_CD", "CATALOG_TYPE_DISPLAY",
    "CONTRIBUTOR_SYSTEM_CD", "CONTRIBUTOR_SYSTEM_DISPLAY", "REFERENCE_NBR",
    "PARENT_EVENT_ID", "NORMALCY_CD", "NORMALCY_DISPLAY", "ENTRY_MODE_CD",
    "ENTRY_MODE_DISPLAY", "PERFORMED_DT_TM", "CLINSIG_UPDT_DT_TM",
    "PARENT_EVENT_TITLE_TEXT", "PARENT_EVENT_CD", "PARENT_EVENT_CD_DISPLAY",
    "PARENT_CATALOG_CD", "PARENT_CATALOG_DISPLAY", "PARENT_CATALOG_TYPE_CD",
    "PARENT_CATALOG_TYPE_DISPLAY", "PARENT_REFERENCE_NBR", "ADC_UPDT",
    "OMOP_MANUAL_TABLE", "OMOP_MANUAL_COLUMN", "OMOP_MANUAL_CONCEPT",
    "OMOP_MANUAL_VALUE_CONCEPT", "OMOP_MANUAL_CONCEPT_NAME",
    "OMOP_MANUAL_STANDARD_CONCEPT", "OMOP_MANUAL_CONCEPT_DOMAIN",
    "OMOP_MANUAL_CONCEPT_CLASS",
]


def _mte_schema() -> T.StructType:
    fields: List[T.StructField] = []
    seen = set()
    for name in _MTE_LEGACY_ORDER:
        field = _MTE_EXTRA_BY_NAME.get(name) or _MNE_FIELDS_BY_NAME.get(name)
        if field is None:
            raise RuntimeError(f"No schema field definition for {name}")
        fields.append(field)
        seen.add(name)
    for field in schema_map_numeric_events.fields:
        if field.name not in seen:
            fields.append(field)
            seen.add(field.name)
    for field in _MTE_EXTRA_FIELDS:
        if field.name not in seen:
            fields.append(field)
            seen.add(field.name)
    return T.StructType(fields)


schema_map_text_events = _mte_schema()


def _mte_align_to_schema(df: DataFrame) -> DataFrame:
    # Avoid df.columns here: Spark Connect would analyze the entire full-source plan
    # before the write. The explicit projection remains the authoritative contract check.
    return df.select(*[
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in schema_map_text_events.fields
    ])


def _mte_numeric_source_tables(config: MapTextEventsConfig) -> Tuple[str, ...]:
    return tuple(_mne_source_tables(config))


def _mte_read_numeric_base_state(config: MapTextEventsConfig) -> Dict[str, int]:
    if (
        not _mne_table_exists(config.numeric_base_table)
        or not _mne_table_exists(config.numeric_state_table)
    ):
        return {}
    available = {field.name for field in spark.table(config.numeric_base_table).schema.fields}
    required_columns = {field.name for field in schema_map_numeric_events.fields}
    if not required_columns.issubset(available):
        return {}

    required = set(_mte_numeric_source_tables(config))
    rows = (
        spark.table(config.numeric_state_table)
        .where(F.col("source_table").isin(sorted(required)))
        .select("source_table", "last_processed_version")
        .collect()
    )
    versions = {
        row["source_table"]: int(row["last_processed_version"])
        for row in rows
        if row["last_processed_version"] is not None
    }
    return versions if required.issubset(versions) else {}


def _mte_align_source_versions_to_numeric_base(
    config: MapTextEventsConfig,
    source_versions: Dict[str, int],
    previous_versions: Optional[Dict[str, int]] = None,
) -> Dict[str, int]:
    # Alignment exists solely to manufacture the version equality that
    # _mte_can_reuse_numeric_base demands. A scoped numeric base can never be reused,
    # so aligning to it would only rewrite this lane's watermark to a table it did
    # not read. Gate on the same predicate as the reuse decision so the two can never
    # disagree.
    if _mte_numeric_base_is_scoped(config):
        return source_versions
    numeric_versions = _mte_read_numeric_base_state(config)
    if not numeric_versions:
        return source_versions
    previous_versions = previous_versions or {}
    if any(
        previous_versions.get(source, -1) > numeric_versions[source]
        for source in _mte_numeric_source_tables(config)
    ):
        print(
            "[WARN] map_text_events state is ahead of the numeric foundation; "
            "building directly from version-pinned sources."
        )
        return source_versions
    aligned = dict(source_versions)
    for source in _mte_numeric_source_tables(config):
        # Alignment may only move a watermark backward. Backward costs a reprocessed
        # window that merges idempotently; forward would record sources as consumed to
        # a version this lane never read, and those changes would never be picked up.
        captured = source_versions.get(source)
        aligned[source] = (
            numeric_versions[source]
            if captured is None
            # Qualified for the same reason as everywhere else in this file: it is exec'd
            # inside _map_common's namespace, where the star import has shadowed builtin
            # min with the pyspark Column function. Both operands here are plain ints, so
            # the bare name would raise a message-free AssertionError. This cell already
            # binds _mte_builtins at line 1206 and uses it at 2317/2326/3012; this one
            # backward-clamp was the only call left unqualified.
            else _mte_builtins.min(int(captured), int(numeric_versions[source]))
        )
    print(
        "[INFO] map_text_events aligned shared source versions to the "
        f"materialized numeric foundation {config.numeric_base_table}."
    )
    return aligned


def _mte_capture_source_versions(
    config: MapTextEventsConfig,
    previous_versions: Optional[Dict[str, int]] = None,
) -> Dict[str, int]:
    versions = _mne_capture_source_versions(config)
    versions[config.long_text_table] = _mne_latest_delta_version(config.long_text_table)
    return _mte_align_source_versions_to_numeric_base(
        config,
        versions,
        previous_versions=previous_versions,
    )


def _mte_read_pipeline_state(config: MapTextEventsConfig) -> Dict[str, int]:
    if not _mne_table_exists(config.state_table):
        return {}
    rows = (
        spark.table(config.state_table)
        .where(F.col("source_table").isin(list(_mte_source_tables(config))))
        .select("source_table", "last_processed_version")
        .collect()
    )
    return {
        row["source_table"]: int(row["last_processed_version"])
        for row in rows
        if row["last_processed_version"] is not None
    }


def _mte_numeric_base_is_scoped(config: MapTextEventsConfig) -> bool:
    """True when the materialized numeric canonical table holds a semantically scoped subset.

    The declared configuration is authoritative and costs nothing to read. The data probe is
    an independent check for a base table written by an earlier run under a different flag; it
    stops at the first out-of-scope active row, so it is cheap exactly when the answer is "not
    scoped". Any failure is reported as scoped, because refusing reuse is always safe.
    """
    numeric_config = globals().get("MAP_NUMERIC_EVENTS_CONFIG")
    if bool(getattr(numeric_config, "numeric_semantic_only", False)):
        return True
    if bool(getattr(config, "numeric_semantic_only", False)):
        return True
    try:
        base = spark.table(config.numeric_base_table)
        if "IN_NUMERIC_SCOPE_IND" not in base.columns:
            return False
        out_of_scope = base.where(
            ~F.coalesce(F.col("IN_NUMERIC_SCOPE_IND"), F.lit(False))
            & ~F.coalesce(F.col("SOURCE_DELETED_IND"), F.lit(False))
        )
        return out_of_scope.limit(1).count() == 0
    except Exception as error:
        print(f"[WARN] Could not probe the scope of {config.numeric_base_table}: {error}")
        return True


def _mte_can_reuse_numeric_base(
    config: MapTextEventsConfig,
    source_versions: Dict[str, int],
) -> bool:
    """Decide whether the text lane may build from the materialized numeric canonical table.

    Reuse is refused whenever the numeric base is scoped. map_numeric_events is built with
    numeric_semantic_only=True, so its canonical table holds only IN_NUMERIC_SCOPE_IND rows,
    while the text lane's own scope is the complement of numeric parse success. Reusing the
    scoped numeric base would therefore truncate the text lane's input to almost nothing,
    silently. State-version equality and a column superset cannot detect a row-scope
    difference, which is exactly why this guard has to be explicit. Do not re-enable reuse
    without also removing the numeric scope. The text lane then builds from version-pinned
    sources, which is correct but slower.
    """
    if _mte_numeric_base_is_scoped(config):
        print(
            "[INFO] The numeric foundation "
            f"{config.numeric_base_table} is semantically scoped; map_text_events will build "
            "from version-pinned sources instead of reusing it."
        )
        return False
    numeric_versions = _mte_read_numeric_base_state(config)
    if not numeric_versions:
        return False
    if any(
        numeric_versions[source] != source_versions.get(source)
        for source in _mte_numeric_source_tables(config)
    ):
        return False
    available = {field.name for field in spark.table(config.numeric_base_table).schema.fields}
    required = {field.name for field in schema_map_numeric_events.fields}
    return required.issubset(available)


def _mte_materialized_numeric_base(
    config: MapTextEventsConfig,
    string_rows: Optional[DataFrame],
    source_versions: Dict[str, int],
    broadcast_keys: bool,
    full_source: bool,
    constant_trigger_sources: Optional[str],
) -> Optional[DataFrame]:
    if not _mte_can_reuse_numeric_base(config, source_versions):
        return None

    trigger_columns = [
        "STRING_RESULT_CDF_COMMIT_VERSION",
        "STRING_RESULT_CDF_COMMIT_TIMESTAMP",
        "STRING_RESULT_CDF_CHANGE_TYPE",
        "TRIGGER_SOURCES",
    ]
    base = (
        spark.table(config.numeric_base_table)
        .where(~F.coalesce(F.col("SOURCE_DELETED_IND"), F.lit(False)))
        .drop(*trigger_columns)
    )

    if full_source:
        return (
            base.withColumn("STRING_RESULT_CDF_COMMIT_VERSION", F.lit(None).cast("long"))
            .withColumn("STRING_RESULT_CDF_COMMIT_TIMESTAMP", F.lit(None).cast("timestamp"))
            .withColumn("STRING_RESULT_CDF_CHANGE_TYPE", F.lit(None).cast("string"))
            .withColumn("TRIGGER_SOURCES", F.lit(constant_trigger_sources or "FULL_REBUILD"))
        )

    if string_rows is None:
        raise RuntimeError("string_rows is required for a scoped numeric-foundation read")

    metadata = string_rows.select("EVENT_ID", *trigger_columns).dropDuplicates(["EVENT_ID"])
    if constant_trigger_sources is not None:
        metadata = metadata.withColumn("TRIGGER_SOURCES", F.lit(constant_trigger_sources))
    if broadcast_keys:
        metadata = F.broadcast(metadata)
    return base.join(metadata, "EVENT_ID", "inner")


def _mte_normalized_text(column: Column) -> Column:
    return F.when(
        F.length(F.trim(column.cast("string"))) > 0,
        F.upper(F.trim(column.cast("string"))),
    )


# COMMAND ----------

def _mte_latest_long_text(
    config: MapTextEventsConfig,
    source_version: int,
    long_text_keys: DataFrame,
    broadcast_keys: bool,
) -> DataFrame:
    keys = (
        long_text_keys.select(F.col("LONG_TEXT_ID").cast("long").alias("LONG_TEXT_ID"))
        .where(F.col("LONG_TEXT_ID").isNotNull() & (F.col("LONG_TEXT_ID") != 0))
        .dropDuplicates()
    )
    if broadcast_keys:
        keys = F.broadcast(keys)
    source = (
        _mne_read_snapshot(config.long_text_table, source_version)
        .withColumn(
            "LONG_TEXT_ID",
            _mne_checked_double_id(F.col("LONG_TEXT_ID"), "MILL_LONG_TEXT.LONG_TEXT_ID"),
        )
        .join(keys, "LONG_TEXT_ID", "left_semi")
        .withColumn(
            "_MTE_LONG_EFFECTIVE_UPDT",
            F.coalesce(F.col("ADC_UPDT"), F.col("UPDT_DT_TM"), F.col("LAST_UTC_TS")),
        )
    )
    source = _mne_latest_by_key(
        source,
        ["LONG_TEXT_ID"],
        ["UPDT_CNT", "_MTE_LONG_EFFECTIVE_UPDT", "ACTIVE_STATUS_DT_TM", "LONG_TEXT"],
    )
    return source.select(
        F.col("LONG_TEXT_ID"),
        F.col("LONG_TEXT").alias("LT_LONG_TEXT"),
        F.col("ACTIVE_IND").cast("long").alias("LT_ACTIVE_IND"),
        F.col("PARENT_ENTITY_NAME").alias("LT_PARENT_ENTITY_NAME"),
        F.col("PARENT_ENTITY_ID").cast("long").alias("LT_PARENT_ENTITY_ID"),
        F.col("UPDT_DT_TM").alias("LT_UPDT_DT_TM"),
        F.col("ADC_UPDT").alias("LT_ADC_UPDT"),
        F.col("Trust").alias("LT_TRUST"),
    )


def _mte_long_text_change_triggers(
    config: MapTextEventsConfig,
    previous_version: int,
    current_version: int,
) -> DataFrame:
    if previous_version == current_version:
        return _mne_empty_long_keys().withColumn("TRIGGER_SOURCE", F.lit("LONG_TEXT"))
    changes = _mne_read_cdf(
        config.long_text_table,
        previous_version + 1,
        current_version,
    ).select(
        _mne_checked_double_id(F.col("LONG_TEXT_ID"), "MILL_LONG_TEXT CDF LONG_TEXT_ID")
        .alias("CHANGED_LONG_TEXT_ID")
    ).where(F.col("CHANGED_LONG_TEXT_ID").isNotNull()).dropDuplicates()
    return (
        spark.table(config.target_table)
        .join(
            F.broadcast(changes),
            F.col("STRING_LONG_TEXT_ID") == F.col("CHANGED_LONG_TEXT_ID"),
            "inner",
        )
        .select("EVENT_ID")
        .dropDuplicates()
        .withColumn("TRIGGER_SOURCE", F.lit("LONG_TEXT"))
    )


# COMMAND ----------

def _mte_resolved_manual_rules(
    config: MapTextEventsConfig,
    source_versions: Optional[Dict[str, int]] = None,
    manual_maps: Optional[DataFrame] = None,
    concepts: Optional[DataFrame] = None,
) -> DataFrame:
    if manual_maps is None:
        if source_versions is None:
            raise ValueError("source_versions is required when manual_maps is not supplied")
        manual_maps = _mne_read_snapshot(
            config.manual_map_table,
            source_versions[config.manual_map_table],
        )
    if concepts is None:
        if source_versions is None:
            raise ValueError("source_versions is required when concepts is not supplied")
        concepts = _mne_read_snapshot(
            config.concept_table,
            source_versions[config.concept_table],
        )

    relevant = (
        manual_maps.where(
            (F.col("SourceTable") == F.lit("dbo.PI_CDE_CLINICAL_EVENT"))
            & F.col("SourceField").isin("EVENT_CD", "EVENT_RESULT_TXT")
            & F.col("OMOPField").isin(
                "measurement_concept_id",
                "observation_concept_id",
                "value_as_concept_id",
            )
        )
        .select(
            F.col("OMOPTable").alias("RULE_OMOP_TABLE"),
            F.col("OMOPField").alias("RULE_OMOP_FIELD"),
            F.col("SourceField").alias("RULE_SOURCE_FIELD"),
            F.col("SourceValue").cast("string").alias("RULE_SOURCE_VALUE"),
            F.col("OMOPConceptId").cast("string").alias("RULE_CONCEPT_ID_TEXT"),
            F.col("EVENT_CD").cast("string").alias("RULE_CONTEXT_EVENT_CD_TEXT"),
            F.col("EVENT_CLASS_CD").cast("string").alias("RULE_CONTEXT_EVENT_CLASS_TEXT"),
            F.col("EVENT_RESULT_TXT").cast("string").alias("RULE_CONTEXT_RESULT_TEXT"),
            F.col("ADC_UPDT").alias("RULE_ADC_UPDT"),
        )
        .withColumn("RULE_SOURCE_VALUE_NORM", _mte_normalized_text(F.col("RULE_SOURCE_VALUE")))
        .withColumn("RULE_CONTEXT_RESULT_NORM", _mte_normalized_text(F.col("RULE_CONTEXT_RESULT_TEXT")))
        .withColumn("RULE_SOURCE_VALUE_LONG", F.expr("try_cast(RULE_SOURCE_VALUE as bigint)"))
        .withColumn("RULE_CONTEXT_EVENT_CD", F.expr("try_cast(RULE_CONTEXT_EVENT_CD_TEXT as bigint)"))
        .withColumn("RULE_CONTEXT_EVENT_CLASS_CD", F.expr("try_cast(RULE_CONTEXT_EVENT_CLASS_TEXT as bigint)"))
        .withColumn("RULE_CONCEPT_ID", F.expr("try_cast(RULE_CONCEPT_ID_TEXT as bigint)"))
        .withColumn(
            "RULE_KIND",
            F.when(F.col("RULE_OMOP_FIELD") == "value_as_concept_id", F.lit("VALUE"))
            .otherwise(F.lit("CONCEPT")),
        )
        .withColumn(
            "RULE_SPECIFICITY",
            (
                F.lit(10)
                + F.when(F.col("RULE_CONTEXT_EVENT_CD").isNotNull(), 4).otherwise(0)
                + F.when(F.col("RULE_CONTEXT_EVENT_CLASS_CD").isNotNull(), 2).otherwise(0)
                + F.when(F.col("RULE_CONTEXT_RESULT_NORM").isNotNull(), 8).otherwise(0)
            ).cast("long"),
        )
        .withColumn(
            "RULE_ID",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(F.col(column).cast("string"), F.lit("<NULL>"))
                        for column in [
                            "RULE_OMOP_TABLE",
                            "RULE_OMOP_FIELD",
                            "RULE_SOURCE_FIELD",
                            "RULE_SOURCE_VALUE",
                            "RULE_CONCEPT_ID_TEXT",
                            "RULE_CONTEXT_EVENT_CD_TEXT",
                            "RULE_CONTEXT_EVENT_CLASS_TEXT",
                            "RULE_CONTEXT_RESULT_TEXT",
                        ]
                    ],
                ),
                256,
            ),
        )
    )

    concept_lookup = concepts.select(
        F.col("concept_id").cast("long").alias("LOOKUP_CONCEPT_ID"),
        F.col("concept_name").alias("RULE_CONCEPT_NAME"),
        F.col("standard_concept").alias("RULE_STANDARD_CONCEPT"),
        F.col("domain_id").alias("RULE_DOMAIN_ID"),
        F.col("concept_class_id").alias("RULE_CONCEPT_CLASS_ID"),
        F.col("invalid_reason").alias("RULE_INVALID_REASON"),
    )
    return (
        relevant.join(
            F.broadcast(concept_lookup),
            F.col("RULE_CONCEPT_ID") == F.col("LOOKUP_CONCEPT_ID"),
            "left",
        )
        .withColumn(
            "RULE_CONCEPT_VALID_IND",
            F.col("LOOKUP_CONCEPT_ID").isNotNull() & F.col("RULE_INVALID_REASON").isNull(),
        )
        .withColumn(
            "RULE_STANDARD_RANK",
            F.when(F.col("RULE_STANDARD_CONCEPT") == "S", 2)
            .when(F.col("RULE_STANDARD_CONCEPT") == "C", 1)
            .otherwise(0),
        )
        .withColumn(
            "RULE_DOMAIN_RANK",
            F.when(F.col("RULE_DOMAIN_ID") == "Drug", 1)
            .when(F.col("RULE_DOMAIN_ID") == "Measurement", 2)
            .when(F.col("RULE_DOMAIN_ID") == "Procedure", 3)
            .when(F.col("RULE_DOMAIN_ID") == "Condition", 4)
            .when(F.col("RULE_DOMAIN_ID") == "Device", 5)
            .when(F.col("RULE_DOMAIN_ID") == "Observation", 6)
            .otherwise(999),
        )
        .drop("LOOKUP_CONCEPT_ID")
    )


def _mte_rule_context_condition(event_alias: str = "e", rule_alias: str = "r") -> Column:
    return (
        (F.col(f"{rule_alias}.RULE_CONTEXT_EVENT_CD").isNull()
         | (F.col(f"{event_alias}.EVENT_CD") == F.col(f"{rule_alias}.RULE_CONTEXT_EVENT_CD")))
        & (F.col(f"{rule_alias}.RULE_CONTEXT_EVENT_CLASS_CD").isNull()
           | (F.col(f"{event_alias}.EVENT_CLASS_CD") == F.col(f"{rule_alias}.RULE_CONTEXT_EVENT_CLASS_CD")))
        & (F.col(f"{rule_alias}.RULE_CONTEXT_RESULT_NORM").isNull()
           | (F.col(f"{event_alias}.TEXT_RESULT_SHORT_NORM") == F.col(f"{rule_alias}.RULE_CONTEXT_RESULT_NORM"))
           | (F.col(f"{event_alias}.TEXT_RESULT_FULL_NORM") == F.col(f"{rule_alias}.RULE_CONTEXT_RESULT_NORM")))
    )


def _mte_match_manual_rules(events: DataFrame, rules: DataFrame, rule_kind: str) -> DataFrame:
    event_columns = events.select(
        "EVENT_ID",
        F.col("EVENT_CD").cast("long").alias("EVENT_CD"),
        F.col("EVENT_CLASS_CD").cast("long").alias("EVENT_CLASS_CD"),
        F.col("STRING_RESULT_TEXT").cast("string").alias("TEXT_RESULT_SHORT_RAW"),
        F.col("TEXT_RESULT").cast("string").alias("TEXT_RESULT_FULL_RAW"),
        _mte_normalized_text(F.col("STRING_RESULT_TEXT")).alias("TEXT_RESULT_SHORT_NORM"),
        _mte_normalized_text(F.col("TEXT_RESULT")).alias("TEXT_RESULT_FULL_NORM"),
    ).alias("e")
    selected_rules = rules.where(F.col("RULE_KIND") == rule_kind)
    rule_columns = selected_rules.columns

    event_code_rules = selected_rules.where(
        (F.col("RULE_SOURCE_FIELD") == "EVENT_CD")
        & F.col("RULE_SOURCE_VALUE_LONG").isNotNull()
    ).alias("r")
    event_matches = (
        event_columns.join(
            F.broadcast(event_code_rules),
            F.col("e.EVENT_CD") == F.col("r.RULE_SOURCE_VALUE_LONG"),
            "inner",
        )
        .where(_mte_rule_context_condition())
        .select(
            F.col("e.EVENT_ID"),
            *[F.col(f"r.{column}").alias(column) for column in rule_columns],
            F.lit("EVENT_CD").alias("RULE_MATCH_TYPE"),
            F.lit(True).alias("RULE_EXACT_MATCH_IND"),
        )
    )

    text_rules = selected_rules.where(
        (F.col("RULE_SOURCE_FIELD") == "EVENT_RESULT_TXT")
        & F.col("RULE_SOURCE_VALUE_NORM").isNotNull()
    ).alias("r")

    def text_matches(key_column: str, raw_column: str, match_type: str) -> DataFrame:
        return (
            event_columns.join(
                F.broadcast(text_rules),
                F.col(f"e.{key_column}") == F.col("r.RULE_SOURCE_VALUE_NORM"),
                "inner",
            )
            .where(_mte_rule_context_condition())
            .select(
                F.col("e.EVENT_ID"),
                *[F.col(f"r.{column}").alias(column) for column in rule_columns],
                F.lit(match_type).alias("RULE_MATCH_TYPE"),
                (F.col(f"e.{raw_column}") == F.col("r.RULE_SOURCE_VALUE"))
                .alias("RULE_EXACT_MATCH_IND"),
            )
        )

    return (
        event_matches
        .unionByName(text_matches("TEXT_RESULT_SHORT_NORM", "TEXT_RESULT_SHORT_RAW", "TEXT_SHORT_NORMALIZED"))
        .unionByName(text_matches("TEXT_RESULT_FULL_NORM", "TEXT_RESULT_FULL_RAW", "TEXT_FULL_NORMALIZED"))
        .dropDuplicates(["EVENT_ID", "RULE_ID"])
        .withColumn(
            "RULE_EFFECTIVE_SPECIFICITY",
            F.col("RULE_SPECIFICITY") + F.when(F.col("RULE_EXACT_MATCH_IND"), 1).otherwise(0),
        )
    )


def _mte_select_mapping(matches: DataFrame, value_mapping: bool) -> DataFrame:
    window = Window.partitionBy("EVENT_ID").orderBy(
        F.col("RULE_CONCEPT_VALID_IND").desc(),
        F.col("RULE_EFFECTIVE_SPECIFICITY").desc(),
        F.col("RULE_STANDARD_RANK").desc(),
        F.col("RULE_DOMAIN_RANK").asc(),
        F.col("RULE_CONCEPT_ID").asc_nulls_last(),
        F.col("RULE_ID").asc(),
    )
    selected = matches.withColumn("_MTE_RULE_RANK", F.row_number().over(window)).where(
        F.col("_MTE_RULE_RANK") == 1
    )
    # A Column expression avoids an eager Spark Connect schema lookup.
    summary = matches.groupBy(F.col("EVENT_ID")).agg(
        F.count(F.lit(1)).cast("long").alias("CANDIDATE_COUNT"),
        F.countDistinct("RULE_CONCEPT_ID").cast("long").alias("DISTINCT_CONCEPT_COUNT"),
        F.sort_array(F.collect_set(F.col("RULE_CONCEPT_ID").cast("long"))).alias("CANDIDATE_IDS"),
        F.max("RULE_ADC_UPDT").alias("MAPPING_ADC_UPDT"),
    )
    selected = selected.join(summary, "EVENT_ID", "left")

    if value_mapping:
        return selected.select(
            "EVENT_ID",
            F.col("RULE_CONCEPT_ID_TEXT").alias("OMOP_MANUAL_VALUE_CONCEPT"),
            F.col("RULE_CONCEPT_ID").cast("long").alias("OMOP_MANUAL_VALUE_CONCEPT_ID"),
            F.col("RULE_CONCEPT_NAME").alias("OMOP_MANUAL_VALUE_CONCEPT_NAME"),
            F.col("RULE_STANDARD_CONCEPT").alias("OMOP_MANUAL_VALUE_STANDARD_CONCEPT"),
            F.col("RULE_DOMAIN_ID").alias("OMOP_MANUAL_VALUE_CONCEPT_DOMAIN"),
            F.col("RULE_CONCEPT_CLASS_ID").alias("OMOP_MANUAL_VALUE_CONCEPT_CLASS"),
            F.col("RULE_CONCEPT_VALID_IND").alias("OMOP_VALUE_MAPPING_VALID_IND"),
            F.col("RULE_ID").alias("OMOP_VALUE_MAPPING_RULE_ID"),
            F.col("RULE_MATCH_TYPE").alias("OMOP_VALUE_MAPPING_MATCH_TYPE"),
            F.col("RULE_EFFECTIVE_SPECIFICITY").cast("long").alias("OMOP_VALUE_MAPPING_SPECIFICITY"),
            F.col("CANDIDATE_COUNT").alias("OMOP_VALUE_MAPPING_CANDIDATE_COUNT"),
            F.col("DISTINCT_CONCEPT_COUNT").alias("OMOP_VALUE_MAPPING_DISTINCT_CONCEPT_COUNT"),
            (F.col("DISTINCT_CONCEPT_COUNT") > 1).alias("OMOP_VALUE_MAPPING_AMBIGUOUS_IND"),
            F.col("CANDIDATE_IDS").alias("OMOP_VALUE_MAPPING_CANDIDATE_CONCEPT_IDS"),
            F.col("MAPPING_ADC_UPDT").alias("_MTE_VALUE_MAPPING_ADC_UPDT"),
        )

    return selected.select(
        "EVENT_ID",
        F.col("RULE_OMOP_TABLE").alias("OMOP_MANUAL_TABLE"),
        F.col("RULE_OMOP_FIELD").alias("OMOP_MANUAL_COLUMN"),
        F.col("RULE_CONCEPT_ID_TEXT").alias("OMOP_MANUAL_CONCEPT"),
        F.col("RULE_CONCEPT_ID").cast("long").alias("OMOP_MANUAL_CONCEPT_ID"),
        F.col("RULE_CONCEPT_NAME").alias("OMOP_MANUAL_CONCEPT_NAME"),
        F.col("RULE_STANDARD_CONCEPT").alias("OMOP_MANUAL_STANDARD_CONCEPT"),
        F.col("RULE_DOMAIN_ID").alias("OMOP_MANUAL_CONCEPT_DOMAIN"),
        F.col("RULE_CONCEPT_CLASS_ID").alias("OMOP_MANUAL_CONCEPT_CLASS"),
        F.col("RULE_CONCEPT_VALID_IND").alias("OMOP_MAPPING_VALID_IND"),
        F.col("RULE_ID").alias("OMOP_MAPPING_RULE_ID"),
        F.col("RULE_MATCH_TYPE").alias("OMOP_MAPPING_MATCH_TYPE"),
        F.col("RULE_EFFECTIVE_SPECIFICITY").cast("long").alias("OMOP_MAPPING_SPECIFICITY"),
        F.col("CANDIDATE_COUNT").alias("OMOP_MAPPING_CANDIDATE_COUNT"),
        F.col("DISTINCT_CONCEPT_COUNT").alias("OMOP_MAPPING_DISTINCT_CONCEPT_COUNT"),
        (F.col("DISTINCT_CONCEPT_COUNT") > 1).alias("OMOP_MAPPING_AMBIGUOUS_IND"),
        F.col("CANDIDATE_IDS").alias("OMOP_MAPPING_CANDIDATE_CONCEPT_IDS"),
        F.col("MAPPING_ADC_UPDT").alias("_MTE_CONCEPT_MAPPING_ADC_UPDT"),
    )


def _mte_attach_text_mapping_selections(
    base: DataFrame,
    concept_selection: DataFrame,
    value_selection: DataFrame,
) -> DataFrame:
    replacement_columns = [
        "OMOP_MANUAL_TABLE", "OMOP_MANUAL_COLUMN", "OMOP_MANUAL_CONCEPT",
        "OMOP_MANUAL_CONCEPT_ID", "OMOP_MANUAL_CONCEPT_NAME",
        "OMOP_MANUAL_STANDARD_CONCEPT", "OMOP_MANUAL_CONCEPT_DOMAIN",
        "OMOP_MANUAL_CONCEPT_CLASS", "OMOP_MAPPING_VALID_IND",
        "OMOP_MAPPING_RULE_ID", "OMOP_MAPPING_MATCH_TYPE",
        "OMOP_MAPPING_SPECIFICITY", "OMOP_MAPPING_CANDIDATE_COUNT",
        "OMOP_MAPPING_DISTINCT_CONCEPT_COUNT", "OMOP_MAPPING_AMBIGUOUS_IND",
        "OMOP_MAPPING_CANDIDATE_CONCEPT_IDS", "_MTE_CONCEPT_MAPPING_ADC_UPDT",
        "OMOP_MANUAL_VALUE_CONCEPT", "OMOP_MANUAL_VALUE_CONCEPT_ID",
        "OMOP_MANUAL_VALUE_CONCEPT_NAME", "OMOP_MANUAL_VALUE_STANDARD_CONCEPT",
        "OMOP_MANUAL_VALUE_CONCEPT_DOMAIN", "OMOP_MANUAL_VALUE_CONCEPT_CLASS",
        "OMOP_VALUE_MAPPING_VALID_IND", "OMOP_VALUE_MAPPING_RULE_ID",
        "OMOP_VALUE_MAPPING_MATCH_TYPE", "OMOP_VALUE_MAPPING_SPECIFICITY",
        "OMOP_VALUE_MAPPING_CANDIDATE_COUNT",
        "OMOP_VALUE_MAPPING_DISTINCT_CONCEPT_COUNT",
        "OMOP_VALUE_MAPPING_AMBIGUOUS_IND",
        "OMOP_VALUE_MAPPING_CANDIDATE_CONCEPT_IDS",
        "_MTE_VALUE_MAPPING_ADC_UPDT",
    ]
    base = base.drop(*replacement_columns)
    return (
        base.join(concept_selection, "EVENT_ID", "left")
        .join(value_selection, "EVENT_ID", "left")
        .withColumn(
            "OMOP_MAPPING_ADC_UPDT",
            F.greatest(
                F.col("_MTE_CONCEPT_MAPPING_ADC_UPDT"),
                F.col("_MTE_VALUE_MAPPING_ADC_UPDT"),
            ),
        )
        .withColumn(
            "OMOP_MAPPING_AMBIGUOUS_IND",
            F.coalesce(F.col("OMOP_MAPPING_AMBIGUOUS_IND"), F.lit(False)),
        )
        .withColumn(
            "OMOP_VALUE_MAPPING_AMBIGUOUS_IND",
            F.coalesce(F.col("OMOP_VALUE_MAPPING_AMBIGUOUS_IND"), F.lit(False)),
        )
        .drop("_MTE_CONCEPT_MAPPING_ADC_UPDT", "_MTE_VALUE_MAPPING_ADC_UPDT")
    )


def _mte_attach_text_mappings(base: DataFrame, rules: DataFrame) -> DataFrame:
    concept_selection = _mte_select_mapping(
        _mte_match_manual_rules(base, rules, "CONCEPT"),
        value_mapping=False,
    )
    value_selection = _mte_select_mapping(
        _mte_match_manual_rules(base, rules, "VALUE"),
        value_mapping=True,
    )
    return _mte_attach_text_mapping_selections(
        base,
        concept_selection,
        value_selection,
    )


def add_manual_omop_mappings_text(
    df: DataFrame,
    barts_mapfile: DataFrame,
    concepts: DataFrame,
) -> DataFrame:
    """Compatibility wrapper with deterministic, ambiguity-preserving text mappings."""
    prepared = df
    if "STRING_RESULT_TEXT" not in prepared.columns and "TEXT_RESULT" in prepared.columns:
        prepared = prepared.withColumn("STRING_RESULT_TEXT", F.col("TEXT_RESULT"))
    rules = _mte_resolved_manual_rules(
        MAP_TEXT_EVENTS_CONFIG,
        manual_maps=barts_mapfile,
        concepts=concepts,
    )
    return _mte_attach_text_mappings(prepared, rules)


# COMMAND ----------

def _mte_add_text_columns(
    config: MapTextEventsConfig,
    base: DataFrame,
    long_text: DataFrame,
    source_versions: Dict[str, int],
    run_timestamp: datetime,
) -> DataFrame:
    base = base.join(
        long_text,
        F.col("STRING_LONG_TEXT_ID") == F.col("LONG_TEXT_ID"),
        "left",
    )
    long_value = _mne_nonblank(F.col("LT_LONG_TEXT"))
    short_value = _mne_nonblank(F.col("STRING_RESULT_TEXT"))
    ce_value = _mne_nonblank(F.col("RESULT_VAL"))
    return (
        base.withColumn("TEXT_RESULT_SHORT", F.col("STRING_RESULT_TEXT"))
        .withColumn("LONG_TEXT", F.col("LT_LONG_TEXT"))
        .withColumn("LONG_TEXT_ID", F.col("LONG_TEXT_ID").cast("long"))
        .withColumn("LONG_TEXT_FOUND_IND", F.col("LONG_TEXT_ID").isNotNull())
        .withColumn("LONG_TEXT_ACTIVE_IND", F.col("LT_ACTIVE_IND").cast("long"))
        .withColumn("LONG_TEXT_UPDT_DT_TM", F.col("LT_UPDT_DT_TM"))
        .withColumn("LONG_TEXT_ADC_UPDT", F.col("LT_ADC_UPDT"))
        .withColumn("LONG_TEXT_TRUST", F.col("LT_TRUST"))
        .withColumn("TEXT_RESULT", F.coalesce(long_value, short_value, ce_value))
        .withColumn("RESULT_TEXT_EFFECTIVE", F.col("TEXT_RESULT"))
        .withColumn(
            "TEXT_RESULT_SOURCE",
            F.when(long_value.isNotNull(), F.lit("LONG_TEXT"))
            .when(short_value.isNotNull(), F.lit("STRING_RESULT_TEXT"))
            .when(ce_value.isNotNull(), F.lit("CLINICAL_EVENT_RESULT_VAL")),
        )
        .withColumn("TEXT_RESULT_LENGTH", F.length(F.col("TEXT_RESULT")).cast("long"))
        .withColumn(
            "TEXT_RESULT_TRUNCATION_RECOVERED_IND",
            F.coalesce(
                F.length(F.col("LONG_TEXT")) > F.length(F.col("TEXT_RESULT_SHORT")),
                F.lit(False),
            ),
        )
        .withColumn(
            "TEXT_SEMANTIC_IND",
            ~F.coalesce(F.col("NUMERIC_PARSE_SUCCESS_IND"), F.lit(False)),
        )
        .withColumn("IN_TEXT_SCOPE_IND", F.col("TEXT_SEMANTIC_IND"))
        .withColumn(
            "RESULT_DT_TM",
            F.coalesce(
                F.col("EVENT_END_DT_TM"),
                F.col("PERFORMED_DT_TM"),
                F.col("EVENT_START_DT_TM"),
                F.col("VERIFIED_DT_TM"),
            ),
        )
        .withColumn(
            "PARENT_EVENT_LABEL",
            F.coalesce(
                _mne_nonblank(F.col("PARENT_EVENT_TITLE_TEXT")),
                _mne_nonblank(F.col("PARENT_EVENT_CD_DISPLAY")),
                _mne_nonblank(F.col("PARENT_CATALOG_DISPLAY")),
                F.col("PARENT_EVENT_CD").cast("string"),
            ),
        )
        .withColumn(
            "RESULT_DATETIME_PLACEHOLDER_IND",
            F.coalesce(F.col("RESULT_DT_TM") < F.lit(datetime(1900, 1, 1)), F.lit(False)),
        )
        .withColumn(
            "RESULT_DATETIME_FUTURE_IND",
            F.coalesce(
                F.col("RESULT_DT_TM") > (F.lit(run_timestamp) + F.expr("INTERVAL 1 DAY")),
                F.lit(False),
            ),
        )
        .withColumn(
            "LONG_TEXT_SOURCE_VERSION",
            F.lit(source_versions[config.long_text_table]).cast("long"),
        )
    )


def _mte_finalize_text_events(
    base: DataFrame,
    run_id: str,
    run_timestamp: datetime,
) -> DataFrame:
    base = (
        base.withColumn(
            "ADC_UPDT",
            F.greatest(
                F.col("ADC_UPDT"),
                F.col("LONG_TEXT_ADC_UPDT"),
                F.col("LONG_TEXT_UPDT_DT_TM"),
                F.col("OMOP_MAPPING_ADC_UPDT"),
            ),
        )
        .drop("ROW_HASH", "PIPELINE_RUN_ID", "PIPELINE_UPDT_DT_TM")
    )
    hash_exclusions = {
        "ROW_HASH",
        "PIPELINE_RUN_ID",
        "PIPELINE_UPDT_DT_TM",
        "TRIGGER_SOURCES",
        "STRING_RESULT_CDF_COMMIT_VERSION",
        "STRING_RESULT_CDF_COMMIT_TIMESTAMP",
        "STRING_RESULT_CDF_CHANGE_TYPE",
    }
    hash_columns = [
        F.col(field.name)
        for field in schema_map_text_events.fields
        if field.name not in hash_exclusions
    ]
    return _mte_align_to_schema(
        base.withColumn("ROW_HASH", _mne_stable_hash_columns(hash_columns))
        .withColumn("PIPELINE_RUN_ID", F.lit(run_id))
        .withColumn("PIPELINE_UPDT_DT_TM", F.lit(run_timestamp))
    )


def _mte_build_enriched_text_events(
    config: MapTextEventsConfig,
    string_rows: Optional[DataFrame],
    source_versions: Dict[str, int],
    run_id: str,
    run_timestamp: datetime,
    broadcast_keys: bool,
    full_source: bool = False,
    constant_trigger_sources: Optional[str] = None,
) -> DataFrame:
    base = _mte_materialized_numeric_base(
        config,
        string_rows,
        source_versions,
        broadcast_keys=broadcast_keys,
        full_source=full_source,
        constant_trigger_sources=constant_trigger_sources,
    )
    if base is None:
        if string_rows is None:
            raise RuntimeError(
                "A materialized numeric foundation was unavailable and string_rows was not supplied"
            )
        base = _mne_build_enriched_numeric_events(
            config,
            string_rows,
            source_versions,
            run_id,
            run_timestamp,
            broadcast_keys=broadcast_keys,
        )
        long_text_key_source = string_rows
        long_text_key_column = "SR_STRING_LONG_TEXT_ID"
    else:
        long_text_key_source = base
        long_text_key_column = "STRING_LONG_TEXT_ID"

    long_text_keys = long_text_key_source.select(
        F.col(long_text_key_column).cast("long").alias("LONG_TEXT_ID")
    ).where(F.col("LONG_TEXT_ID").isNotNull() & (F.col("LONG_TEXT_ID") != 0)).dropDuplicates()
    long_text = _mte_latest_long_text(
        config,
        source_versions[config.long_text_table],
        long_text_keys,
        broadcast_keys=broadcast_keys,
    )
    base = _mte_add_text_columns(config, base, long_text, source_versions, run_timestamp)
    rules = _mte_resolved_manual_rules(config, source_versions=source_versions)
    base = _mte_attach_text_mappings(base, rules)
    return _mte_finalize_text_events(base, run_id, run_timestamp)


def _mte_manual_map_change_triggers(
    config: MapTextEventsConfig,
    previous_version: int,
    current_version: int,
    concept_version: int,
) -> DataFrame:
    if previous_version == current_version:
        return _mne_empty_long_keys().withColumn("TRIGGER_SOURCE", F.lit("TEXT_MANUAL_MAP"))
    changed_maps = _mne_read_cdf(
        config.manual_map_table,
        previous_version + 1,
        current_version,
    )
    concepts = _mne_read_snapshot(config.concept_table, concept_version)
    rules = _mte_resolved_manual_rules(
        config,
        manual_maps=changed_maps,
        concepts=concepts,
    )
    target = spark.table(config.target_table)
    return (
        _mte_match_manual_rules(target, rules, "CONCEPT").select("EVENT_ID")
        .unionByName(_mte_match_manual_rules(target, rules, "VALUE").select("EVENT_ID"))
        .dropDuplicates()
        .withColumn("TRIGGER_SOURCE", F.lit("TEXT_MANUAL_MAP"))
    )


def _mte_value_concept_change_triggers(
    config: MapTextEventsConfig,
    previous_version: int,
    current_version: int,
) -> DataFrame:
    if previous_version == current_version:
        return _mne_empty_long_keys().withColumn("TRIGGER_SOURCE", F.lit("TEXT_OMOP_CONCEPT"))
    changed = _mne_simple_cdf_keys(
        config.concept_table,
        "concept_id",
        previous_version,
        current_version,
        key_is_double=False,
    ).select(F.col("CHANGE_KEY").cast("long").alias("CHANGED_CONCEPT_ID"))
    target = spark.table(config.target_table)
    selected = target.join(
        F.broadcast(changed),
        (F.col("OMOP_MANUAL_CONCEPT_ID") == F.col("CHANGED_CONCEPT_ID"))
        | (F.col("OMOP_MANUAL_VALUE_CONCEPT_ID") == F.col("CHANGED_CONCEPT_ID")),
        "inner",
    ).select("EVENT_ID")
    arrays = (
        target.select(
            "EVENT_ID",
            F.explode_outer(
                F.flatten(
                    F.array(
                        F.coalesce(
                            F.col("OMOP_MAPPING_CANDIDATE_CONCEPT_IDS"),
                            F.expr("CAST(array() AS ARRAY<BIGINT>)"),
                        ),
                        F.coalesce(
                            F.col("OMOP_VALUE_MAPPING_CANDIDATE_CONCEPT_IDS"),
                            F.expr("CAST(array() AS ARRAY<BIGINT>)"),
                        ),
                    )
                )
            ).alias("CANDIDATE_CONCEPT_ID"),
        )
        .where(F.col("CANDIDATE_CONCEPT_ID").isNotNull())
        .join(
            F.broadcast(changed),
            F.col("CANDIDATE_CONCEPT_ID") == F.col("CHANGED_CONCEPT_ID"),
            "left_semi",
        )
        .select("EVENT_ID")
    )
    return (
        selected.unionByName(arrays)
        .dropDuplicates()
        .withColumn("TRIGGER_SOURCE", F.lit("TEXT_OMOP_CONCEPT"))
    )


def _mte_build_changed_event_keys(
    config: MapTextEventsConfig,
    previous_versions: Dict[str, int],
    current_versions: Dict[str, int],
) -> DataFrame:
    core = _mne_build_changed_event_keys(config, previous_versions, current_versions)
    core_meta = core.select(
        "EVENT_ID",
        "STRING_RESULT_CDF_COMMIT_VERSION",
        "STRING_RESULT_CDF_COMMIT_TIMESTAMP",
        "STRING_RESULT_CDF_CHANGE_TYPE",
    )
    core_triggers = core.select(
        "EVENT_ID",
        F.explode_outer(F.split(F.col("TRIGGER_SOURCES"), ",")).alias("TRIGGER_SOURCE"),
    )
    extra_triggers = [
        _mte_long_text_change_triggers(
            config,
            previous_versions[config.long_text_table],
            current_versions[config.long_text_table],
        ),
        _mte_manual_map_change_triggers(
            config,
            previous_versions[config.manual_map_table],
            current_versions[config.manual_map_table],
            current_versions[config.concept_table],
        ),
        _mte_value_concept_change_triggers(
            config,
            previous_versions[config.concept_table],
            current_versions[config.concept_table],
        ),
    ]
    all_triggers = reduce(
        lambda left, right: left.unionByName(right),
        [core_triggers] + extra_triggers,
    )
    grouped = (
        all_triggers.where(F.col("EVENT_ID").isNotNull())
        .groupBy("EVENT_ID")
        .agg(
            F.concat_ws(
                ",",
                F.sort_array(F.collect_set("TRIGGER_SOURCE")),
            ).alias("TRIGGER_SOURCES")
        )
    )
    return grouped.join(core_meta, "EVENT_ID", "left")


# COMMAND ----------

def create_text_event_code_lookup(code_values: DataFrame, alias_suffix: str) -> DataFrame:
    """Compatibility helper that uses DISPLAY first and preserves DESCRIPTION semantics."""
    return code_values.select(
        F.col("CODE_VALUE"),
        F.coalesce(
            _mne_nonblank(F.col("DISPLAY")),
            _mne_nonblank(F.col("DESCRIPTION")),
            _mne_nonblank(F.col("DEFINITION")),
            _mne_nonblank(F.col("CDF_MEANING")),
        ).alias(f"{alias_suffix}_desc"),
    ).alias(alias_suffix)


def _mte_apply_target_properties(
    config: MapTextEventsConfig,
    apply_column_comments: bool = False,
) -> None:
    retention_days = _mte_builtins.max(int(config.cdf_retention_days), 8)
    spark.sql(f"""
        ALTER TABLE {_mne_sql_identifier(config.target_table)}
        SET TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.enableDeletionVectors' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'delta.deletedFileRetentionDuration' = 'interval {retention_days} days',
            'delta.logRetentionDuration' = 'interval {_mte_builtins.max(retention_days, 30)} days',
            'map_text_events.schemaVersion' = '2'
        )
    """)
    spark.sql(
        f"COMMENT ON TABLE {_mne_sql_identifier(config.target_table)} "
        f"IS '{_mne_escape_sql_string(map_text_events_comment)}'"
    )
    try:
        spark.sql(
            f"ALTER TABLE {_mne_sql_identifier(config.target_table)} CLUSTER BY (EVENT_ID)"
        )
    except Exception as exc:
        print(f"[WARN] Could not enable liquid clustering on {config.target_table}: {exc}")

    if apply_column_comments:
        important = set(_MTE_LEGACY_ORDER) | {
            "STRING_RESULT_TEXT", "TEXT_RESULT_SHORT", "LONG_TEXT", "STRING_LONG_TEXT_ID",
            "STRING_RESULT_VALID_FROM_DT_TM", "STRING_RESULT_VALID_UNTIL_DT_TM",
            "STRING_RESULT_ADC_UPDT", "STRING_RESULT_EFFECTIVE_UPDT_DT_TM",
            "RESULT_STATUS_CD", "RECORD_STATUS_CD", "SOURCE_CURRENT_IND",
            "SOURCE_DELETED_IND", "RESULT_DT_TM", "EVENT_LABEL", "PARENT_EVENT_LABEL",
            "ROW_HASH", "TRIGGER_SOURCES", "PIPELINE_RUN_ID", "PIPELINE_UPDT_DT_TM",
        }
        for field in bronze_contract_schema(config.target_table).fields:
            if field.name not in important:
                continue
            comment = field.metadata.get("comment", "")
            if not comment:
                continue
            spark.sql(
                f"ALTER TABLE {_mne_sql_identifier(config.target_table)} "
                f"ALTER COLUMN {_mne_sql_identifier(field.name)} "
                f"COMMENT '{_mne_escape_sql_string(comment)}'"
            )


def _mte_create_current_view(config: MapTextEventsConfig) -> None:
    spark.sql(f"""
        CREATE OR REPLACE VIEW {_mne_sql_identifier(config.current_view)} AS
        SELECT *
        FROM {_mne_sql_identifier(config.target_table)}
        WHERE SOURCE_DELETED_IND = false
    """)


def _mte_target_schema_version(config: MapTextEventsConfig) -> Optional[str]:
    if not _mne_table_exists(config.target_table):
        return None
    try:
        row = spark.sql(
            f"SHOW TBLPROPERTIES {_mne_sql_identifier(config.target_table)} "
            "('map_text_events.schemaVersion')"
        ).first()
    except Exception:
        return None
    if row is None:
        return None
    values = row.asDict()
    value = values.get("value")
    if value is None and len(row) > 0:
        value = row[0]
    return None if value is None else str(value)


def _mte_ensure_target_schema(config: MapTextEventsConfig) -> None:
    if not _mne_table_exists(config.target_table):
        return
    current_fields = {field.name: field for field in spark.table(config.target_table).schema.fields}
    additions: List[str] = []
    tick = chr(96)
    for field in bronze_contract_schema(config.target_table).fields:
        if field.name not in current_fields:
            definition = f"{tick}{field.name}{tick} {field.dataType.simpleString()}"
            comment = field.metadata.get("comment", "")
            if comment:
                definition += f" COMMENT '{_mne_escape_sql_string(comment)}'"
            additions.append(definition)
        elif current_fields[field.name].dataType != field.dataType:
            raise RuntimeError(
                f"Existing column {field.name} has type "
                f"{current_fields[field.name].dataType.simpleString()}, expected "
                f"{field.dataType.simpleString()}. Run rebuild_map_text_events_v2() to apply "
                "the source-derived schema safely."
            )
    if additions:
        spark.sql(
            f"ALTER TABLE {_mne_sql_identifier(config.target_table)} "
            f"ADD COLUMNS ({', '.join(additions)})"
        )
    _mte_apply_target_properties(config, apply_column_comments=False)


def _mte_full_rebuild(
    config: MapTextEventsConfig,
    source_versions: Dict[str, int],
    run_id: str,
    run_timestamp: datetime,
    safe_rebuild: bool,
    backup_existing: bool,
) -> Dict[str, object]:
    print("[INFO] Building all latest CE_STRING_RESULT rows without timestamp/status/validity filters.")
    suffix = run_id.replace("-", "")
    staging_table = f"{config.target_table}__rebuild_{suffix}"
    stage_prefix = f"{config.target_table}__pipeline_stage_{suffix}"
    long_text_stage = f"{stage_prefix}_long_text"
    mapping_input_stage = f"{stage_prefix}_mapping_input"
    concept_stage = f"{stage_prefix}_concept"
    value_stage = f"{stage_prefix}_value"
    scratch_tables = [long_text_stage, mapping_input_stage, concept_stage, value_stage]
    backup_table: Optional[str] = None
    string_rows: Optional[DataFrame] = None
    numeric_base_reused = _mte_can_reuse_numeric_base(config, source_versions)

    try:
        for table_name in scratch_tables + [staging_table]:
            spark.sql(f"DROP TABLE IF EXISTS {_mne_sql_identifier(table_name)}")

        if numeric_base_reused:
            expected_source_rows = (
                spark.table(config.numeric_base_table)
                .where(~F.coalesce(F.col("SOURCE_DELETED_IND"), F.lit(False)))
                .count()
            )
            print(
                "[INFO] Reusing version-aligned numeric foundation "
                f"{config.numeric_base_table} ({expected_source_rows:,} rows)."
            )
            broadcast_stage_keys = expected_source_rows <= int(config.broadcast_event_id_limit)
            numeric_base = _mte_materialized_numeric_base(
                config,
                None,
                source_versions,
                broadcast_keys=False,
                full_source=True,
                constant_trigger_sources="FULL_REBUILD",
            )
            if numeric_base is None:
                raise RuntimeError("The numeric foundation became unavailable during the full rebuild")

            print(f"[INFO] Materializing long-text lookup stage: {long_text_stage}")
            long_text_keys = (
                numeric_base.select(
                    F.col("STRING_LONG_TEXT_ID").cast("long").alias("LONG_TEXT_ID")
                )
                .where(F.col("LONG_TEXT_ID").isNotNull() & (F.col("LONG_TEXT_ID") != 0))
                .dropDuplicates()
            )
            long_text = _mte_latest_long_text(
                config,
                source_versions[config.long_text_table],
                long_text_keys,
                broadcast_keys=broadcast_stage_keys,
            )
            (
                long_text.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(long_text_stage)
            )

            print(f"[INFO] Materializing narrow text-mapping input: {mapping_input_stage}")
            joined = numeric_base.alias("b").join(
                spark.table(long_text_stage).alias("l"),
                F.col("b.STRING_LONG_TEXT_ID") == F.col("l.LONG_TEXT_ID"),
                "left",
            )
            mapping_input = joined.select(
                F.col("b.EVENT_ID").alias("EVENT_ID"),
                F.col("b.EVENT_CD").cast("long").alias("EVENT_CD"),
                F.col("b.EVENT_CLASS_CD").cast("long").alias("EVENT_CLASS_CD"),
                F.col("b.STRING_RESULT_TEXT").cast("string").alias("STRING_RESULT_TEXT"),
                F.coalesce(
                    _mne_nonblank(F.col("l.LT_LONG_TEXT")),
                    _mne_nonblank(F.col("b.STRING_RESULT_TEXT")),
                    _mne_nonblank(F.col("b.RESULT_VAL")),
                ).alias("TEXT_RESULT"),
            )
            (
                mapping_input.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(mapping_input_stage)
            )

            rules = _mte_resolved_manual_rules(config, source_versions=source_versions)
            mapping_events = spark.table(mapping_input_stage)
            print(f"[INFO] Materializing concept mapping selections: {concept_stage}")
            concept_selection = _mte_select_mapping(
                _mte_match_manual_rules(mapping_events, rules, "CONCEPT"),
                value_mapping=False,
            )
            (
                concept_selection.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(concept_stage)
            )
            print(f"[INFO] Materializing value mapping selections: {value_stage}")
            value_selection = _mte_select_mapping(
                _mte_match_manual_rules(mapping_events, rules, "VALUE"),
                value_mapping=True,
            )
            (
                value_selection.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(value_stage)
            )

            final_base = _mte_add_text_columns(
                config,
                numeric_base,
                spark.table(long_text_stage),
                source_versions,
                run_timestamp,
            )
            final_base = _mte_attach_text_mapping_selections(
                final_base,
                spark.table(concept_stage),
                spark.table(value_stage),
            )
            rebuilt = _mte_finalize_text_events(final_base, run_id, run_timestamp)
        else:
            string_rows = (
                _mne_latest_string_results(
                    config,
                    source_versions[config.string_result_table],
                )
                .withColumn("STRING_RESULT_CDF_COMMIT_VERSION", F.lit(None).cast("long"))
                .withColumn("STRING_RESULT_CDF_COMMIT_TIMESTAMP", F.lit(None).cast("timestamp"))
                .withColumn("STRING_RESULT_CDF_CHANGE_TYPE", F.lit(None).cast("string"))
                .withColumn("TRIGGER_SOURCES", F.lit("FULL_REBUILD"))
                
            )
            expected_source_rows = string_rows.count()
            rebuilt = _mte_build_enriched_text_events(
                config,
                string_rows,
                source_versions,
                run_id,
                run_timestamp,
                broadcast_keys=False,
                full_source=True,
                constant_trigger_sources="FULL_REBUILD",
            )

        if config.text_semantic_only:
            rebuilt = rebuilt.where(F.col("IN_TEXT_SCOPE_IND"))

        rebuilt = bronze_project_contract(rebuilt, config.target_table)
        output_table = staging_table if safe_rebuild else config.target_table
        print(f"[INFO] Writing validated text-events output: {output_table}")
        (
            rebuilt.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(output_table)
        )
        metrics = _mne_validate_unique_event_grain(output_table)
        if not config.text_semantic_only and metrics["row_count"] != expected_source_rows:
            raise RuntimeError(
                "Row preservation failed during map_text_events rebuild: "
                f"source={expected_source_rows}, output={metrics['row_count']}"
            )

        if safe_rebuild:
            if backup_existing and _mne_table_exists(config.target_table):
                backup_table = f"{config.target_table}__backup_{suffix}"
                spark.sql(
                    f"CREATE TABLE {_mne_sql_identifier(backup_table)} "
                    f"SHALLOW CLONE {_mne_sql_identifier(config.target_table)}"
                )
                print(f"[INFO] Existing target preserved as {backup_table}")
            (
                spark.table(staging_table).write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(config.target_table)
            )
            spark.sql(f"DROP TABLE {_mne_sql_identifier(staging_table)}")

        _mte_apply_target_properties(config, apply_column_comments=True)
        _mte_create_current_view(config)
        return {
            "mode": "FULL_REBUILD",
            "expected_source_rows": int(expected_source_rows),
            "numeric_base_reused": bool(numeric_base_reused),
            "backup_table": backup_table,
            **metrics,
        }
    finally:
        if string_rows is not None:
            None
        for table_name in scratch_tables:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {_mne_sql_identifier(table_name)}")
            except Exception as cleanup_exc:
                print(f"[WARN] Could not drop text-events scratch table {table_name}: {cleanup_exc}")
        if safe_rebuild and _mne_table_exists(staging_table):
            spark.sql(f"DROP TABLE {_mne_sql_identifier(staging_table)}")


def _mte_mark_deleted_or_out_of_scope(
    config: MapTextEventsConfig,
    deleted_keys: DataFrame,
    source_versions: Dict[str, int],
    run_id: str,
    run_timestamp: datetime,
) -> Dict[str, int]:
    prepared = (
        deleted_keys.select(
            "EVENT_ID",
            "STRING_RESULT_CDF_COMMIT_VERSION",
            "STRING_RESULT_CDF_COMMIT_TIMESTAMP",
            "STRING_RESULT_CDF_CHANGE_TYPE",
            "TRIGGER_SOURCES",
        )
        .withColumn("PIPELINE_RUN_ID", F.lit(run_id))
        .withColumn("PIPELINE_UPDT_DT_TM", F.lit(run_timestamp))
        .withColumn("STRING_RESULT_SOURCE_VERSION", F.lit(source_versions[config.string_result_table]).cast("long"))
        .withColumn("CLINICAL_EVENT_SOURCE_VERSION", F.lit(source_versions[config.clinical_event_table]).cast("long"))
        .withColumn("CODE_VALUE_SOURCE_VERSION", F.lit(source_versions[config.code_value_table]).cast("long"))
        .withColumn("ORDER_CATALOG_SOURCE_VERSION", F.lit(source_versions[config.order_catalog_table]).cast("long"))
        .withColumn("MANUAL_MAP_SOURCE_VERSION", F.lit(source_versions[config.manual_map_table]).cast("long"))
        .withColumn("CONCEPT_SOURCE_VERSION", F.lit(source_versions[config.concept_table]).cast("long"))
        .withColumn("LONG_TEXT_SOURCE_VERSION", F.lit(source_versions[config.long_text_table]).cast("long"))
        
    )
    try:
        deleted_count = prepared.count()
        if deleted_count == 0:
            return {"deleted_or_out_of_scope_keys": 0}
        requested_updates = {
            "SOURCE_DELETED_IND": "true",
            "SOURCE_CURRENT_IND": "false",
            "IN_TEXT_SCOPE_IND": "false",
            "IN_NUMERIC_SCOPE_IND": "false",
            "STRING_RESULT_CDF_COMMIT_VERSION": "s.STRING_RESULT_CDF_COMMIT_VERSION",
            "STRING_RESULT_CDF_COMMIT_TIMESTAMP": "s.STRING_RESULT_CDF_COMMIT_TIMESTAMP",
            "STRING_RESULT_CDF_CHANGE_TYPE": "s.STRING_RESULT_CDF_CHANGE_TYPE",
            "TRIGGER_SOURCES": "s.TRIGGER_SOURCES",
            "STRING_RESULT_SOURCE_VERSION": "s.STRING_RESULT_SOURCE_VERSION",
            "CLINICAL_EVENT_SOURCE_VERSION": "s.CLINICAL_EVENT_SOURCE_VERSION",
            "CODE_VALUE_SOURCE_VERSION": "s.CODE_VALUE_SOURCE_VERSION",
            "ORDER_CATALOG_SOURCE_VERSION": "s.ORDER_CATALOG_SOURCE_VERSION",
            "MANUAL_MAP_SOURCE_VERSION": "s.MANUAL_MAP_SOURCE_VERSION",
            "CONCEPT_SOURCE_VERSION": "s.CONCEPT_SOURCE_VERSION",
            "LONG_TEXT_SOURCE_VERSION": "s.LONG_TEXT_SOURCE_VERSION",
            "ROW_HASH": "xxhash64(coalesce(cast(t.ROW_HASH as string), ''), 'SOURCE_DELETED')",
            "PIPELINE_RUN_ID": "s.PIPELINE_RUN_ID",
            "PIPELINE_UPDT_DT_TM": "s.PIPELINE_UPDT_DT_TM",
        }
        allowed = bronze_contract_column_set(config.target_table)
        retained_updates = {
            column_name: expression
            for column_name, expression in requested_updates.items()
            if column_name.lower() in allowed
        }
        (
            DeltaTable.forName(spark, config.target_table)
            .alias("t")
            .merge(prepared.alias("s"), "t.EVENT_ID <=> s.EVENT_ID")
            .whenMatchedUpdate(set=retained_updates)
            .execute()
        )
        return {"deleted_or_out_of_scope_keys": int(deleted_count)}
    finally:
        None


def _mte_incremental_update(
    config: MapTextEventsConfig,
    previous_versions: Dict[str, int],
    source_versions: Dict[str, int],
    run_id: str,
    run_timestamp: datetime,
) -> Dict[str, object]:
    _mte_ensure_target_schema(config)
    changed_keys = _mp30_persist_stage(
        _mte_build_changed_event_keys(
            config,
            previous_versions,
            source_versions,
        )
    )
    try:
        changed_count = changed_keys.count()
        if changed_count == 0:
            return {
                "mode": "INCREMENTAL",
                "changed_event_ids": 0,
                "active_source_rows": 0,
                "deleted_or_out_of_scope_keys": 0,
            }
        broadcast_keys = changed_count <= int(config.broadcast_event_id_limit)
        string_rows = _mp30_persist_stage(
            _mne_latest_string_results(
                config,
                source_versions[config.string_result_table],
                event_keys=changed_keys,
                broadcast_keys=broadcast_keys,
            ).join(changed_keys, "EVENT_ID", "inner")
        )
        all_updates = _mte_build_enriched_text_events(
            config,
            string_rows,
            source_versions,
            run_id,
            run_timestamp,
            broadcast_keys=broadcast_keys,
        )
        updates = _mp30_persist_stage(
            all_updates.where(F.col("IN_TEXT_SCOPE_IND"))
            if config.text_semantic_only
            else all_updates
        )
        try:
            retained_keys = updates.select("EVENT_ID").dropDuplicates()
            removed = changed_keys.join(retained_keys, "EVENT_ID", "left_anti")
            metrics: Dict[str, object] = {
                "mode": "INCREMENTAL",
                "changed_event_ids": int(changed_count),
                "broadcast_event_ids": bool(broadcast_keys),
            }
            metrics.update(_mne_merge_active_updates(config, updates))
            metrics.update(
                _mte_mark_deleted_or_out_of_scope(
                    config,
                    removed,
                    source_versions,
                    run_id,
                    run_timestamp,
                )
            )
            missing_after_merge = (
                retained_keys.join(
                    spark.table(config.target_table)
                    .where(~F.col("SOURCE_DELETED_IND"))
                    .select("EVENT_ID"),
                    "EVENT_ID",
                    "left_anti",
                )
                .limit(1)
                .count()
            )
            if missing_after_merge:
                raise RuntimeError("At least one changed source EVENT_ID was absent after merge")
            return metrics
        finally:
            _mp30_persist_stage_drop(updates)
            _mp30_persist_stage_drop(string_rows)
    finally:
        _mp30_persist_stage_drop(changed_keys)


# COMMAND ----------

def process_text_events_incremental(
    force_full_rebuild: bool = False,
    bootstrap_if_state_missing: bool = True,
    safe_rebuild: bool = True,
    backup_existing: bool = True,
    config: MapTextEventsConfig = MAP_TEXT_EVENTS_CONFIG,
) -> Dict[str, object]:
    """
    Drop-in replacement for the original process_text_events_incremental().

    The first deployment performs a version-pinned, validated full rebuild. Later runs consume CDF
    by stored Delta version for CE_STRING_RESULT, CLINICAL_EVENT, LONG_TEXT, CODE_VALUE,
    ORDER_CATALOG, manual maps, and OMOP concepts. State advances only after all target writes and
    validation succeed. No target MAX(ADC_UPDT) or timestamp fallback is used.
    """
    run_id = str(uuid.uuid4())
    run_timestamp = datetime.now(timezone.utc)
    mode = "UNKNOWN"
    source_versions: Dict[str, int] = {}
    _mne_ensure_metadata_tables(config)

    try:
        previous_versions = _mte_read_pipeline_state(config)
        source_versions = _mte_capture_source_versions(
            config,
            previous_versions=previous_versions,
        )
        missing_state = [
            source for source in _mte_source_tables(config)
            if source not in previous_versions
        ]
        target_missing = not _mne_table_exists(config.target_table)
        schema_upgrade_required = (
            not target_missing and _mte_target_schema_version(config) != "2"
        )
        requires_bootstrap = target_missing or bool(missing_state) or schema_upgrade_required

        if requires_bootstrap and not (bootstrap_if_state_missing or force_full_rebuild):
            raise RuntimeError(
                "map_text_events V2 requires a full rebuild because the target, schema-version "
                f"property, or source-version state is missing. Missing state: {missing_state}"
            )

        mode = "FULL_REBUILD" if (force_full_rebuild or requires_bootstrap) else "INCREMENTAL"
        _mne_write_audit_event(
            config,
            run_id,
            status="STARTED",
            mode=mode,
            message="map_text_events V2 load started",
            source_versions=source_versions,
        )

        if mode == "FULL_REBUILD":
            metrics = _mte_full_rebuild(
                config,
                source_versions,
                run_id,
                run_timestamp,
                safe_rebuild=safe_rebuild,
                backup_existing=backup_existing,
            )
        else:
            for source in _mte_source_tables(config):
                previous_version = previous_versions[source]
                current_version = source_versions[source]
                if previous_version > current_version:
                    raise RuntimeError(
                        f"Stored version {previous_version} is ahead of current version "
                        f"{current_version} for {source}"
                    )
            metrics = _mte_incremental_update(
                config,
                previous_versions,
                source_versions,
                run_id,
                run_timestamp,
            )

        completed_at = datetime.now(timezone.utc)
        _mne_commit_pipeline_state(
            config,
            source_versions,
            run_id,
            completed_at,
        )
        _mne_write_audit_event(
            config,
            run_id,
            status="SUCCESS",
            mode=mode,
            message="map_text_events V2 load completed",
            source_versions=source_versions,
            metrics=metrics,
        )
        print(f"[SUCCESS] map_text_events {mode} completed: {metrics}")
        return metrics
    except Exception as exc:
        _mne_write_audit_event(
            config,
            run_id,
            status="FAILED",
            mode=mode,
            message=str(exc),
            source_versions=source_versions,
        )
        print(f"[ERROR] map_text_events {mode} failed: {exc}")
        raise


def rebuild_map_text_events_v2(
    safe_rebuild: bool = True,
    backup_existing: bool = True,
    config: MapTextEventsConfig = MAP_TEXT_EVENTS_CONFIG,
) -> Dict[str, object]:
    """Explicit one-time migration/backfill, including null-ADC_UPDT and LONG_TEXT rows."""
    return process_text_events_incremental(
        force_full_rebuild=True,
        bootstrap_if_state_missing=True,
        safe_rebuild=safe_rebuild,
        backup_existing=backup_existing,
        config=config,
    )


def deploy_map_text_events_improvements(
    safe_rebuild: bool = True,
    backup_existing: bool = True,
    config: MapTextEventsConfig = MAP_TEXT_EVENTS_CONFIG,
) -> Dict[str, object]:
    return rebuild_map_text_events_v2(
        safe_rebuild=safe_rebuild,
        backup_existing=backup_existing,
        config=config,
    )


def plan_map_text_events_run(
    config: MapTextEventsConfig = MAP_TEXT_EVENTS_CONFIG,
) -> Dict[str, object]:
    """Read-only deployment/run plan. It does not create tables or modify state."""
    state = _mte_read_pipeline_state(config) if _mne_table_exists(config.state_table) else {}
    source_versions = _mte_capture_source_versions(
        config,
        previous_versions=state,
    )
    missing_state = [
        source for source in _mte_source_tables(config)
        if source not in state
    ]
    target_exists = _mne_table_exists(config.target_table)
    schema_version = _mte_target_schema_version(config) if target_exists else None
    planned_mode = (
        "FULL_REBUILD"
        if (not target_exists or missing_state or schema_version != "2")
        else "INCREMENTAL"
    )
    return {
        "target_table": config.target_table,
        "target_exists": target_exists,
        "schema_version": schema_version,
        "planned_mode": planned_mode,
        "trust_scope": config.trust_scope,
        "text_semantic_only": config.text_semantic_only,
        "current_source_versions": source_versions,
        "stored_source_versions": state,
        "missing_state_sources": missing_state,
    }


def validate_map_text_events(
    compare_to_source: bool = False,
    config: MapTextEventsConfig = MAP_TEXT_EVENTS_CONFIG,
) -> Dict[str, object]:
    """Read-only validation over the retained single-layer text contract."""
    if not _mne_table_exists(config.target_table):
        raise RuntimeError(f"Target table does not exist: {config.target_table}")
    metrics: Dict[str, object] = _mne_validate_unique_event_grain(config.target_table)
    quality = spark.sql(f"""
        SELECT
            COUNT_IF(STRING_RESULT_ADC_UPDT IS NULL) AS null_string_result_adc_updt_retained,
            COUNT_IF(STRING_RESULT_EFFECTIVE_UPDT_DT_TM IS NULL) AS null_effective_source_updt,
            COUNT_IF(TEXT_RESULT IS NULL) AS null_effective_text,
            COUNT_IF(SOURCE_DELETED_IND = TRUE) AS source_deleted_rows,
            COUNT_IF(
                OMOP_MANUAL_CONCEPT_ID IS NOT NULL
                AND OMOP_MAPPING_VALID_IND = FALSE
            ) AS selected_invalid_concept_rows,
            COUNT_IF(EVENT_LABEL IS NULL) AS missing_event_label
        FROM {_mne_sql_identifier(config.target_table)}
    """).first()
    metrics.update({key: int(value or 0) for key, value in quality.asDict().items()})
    if compare_to_source:
        if config.text_semantic_only:
            raise RuntimeError(
                "compare_to_source=True with text_semantic_only=True requires "
                "the full classification plan and is intentionally not implicit."
            )
        source_version = _mne_latest_delta_version(config.string_result_table)
        source_ids = _mne_latest_string_results(
            config, source_version
        ).select("EVENT_ID").dropDuplicates()
        target_ids = (
            spark.table(config.target_table)
            .where(~F.col("SOURCE_DELETED_IND"))
            .select("EVENT_ID")
        )
        metrics["missing_source_event_ids"] = source_ids.join(
            target_ids, "EVENT_ID", "left_anti"
        ).count()
        metrics["active_target_ids_absent_from_source"] = target_ids.join(
            source_ids, "EVENT_ID", "left_anti"
        ).count()
    return metrics
def configure_map_text_events_cdf_retention(
    retention_days: int = 30,
    config: MapTextEventsConfig = MAP_TEXT_EVENTS_CONFIG,
) -> None:
    """Explicit shared-source maintenance helper; never called automatically."""
    if retention_days < 8:
        raise ValueError("retention_days must be at least 8")
    for source in _mte_source_tables(config):
        spark.sql(f"""
            ALTER TABLE {_mne_sql_identifier(source)}
            SET TBLPROPERTIES (
                'delta.deletedFileRetentionDuration' = 'interval {int(retention_days)} days',
                'delta.logRetentionDuration' = 'interval {_mte_builtins.max(int(retention_days), 30)} days'
            )
        """)
        print(f"[INFO] Updated CDF-supporting retention for {source}")


def optimize_map_text_events(
    config: MapTextEventsConfig = MAP_TEXT_EVENTS_CONFIG,
) -> None:
    """Explicit maintenance operation; schedule independently from the incremental load."""
    if not _mne_table_exists(config.target_table):
        raise RuntimeError(f"Target table does not exist: {config.target_table}")
    _mte_apply_target_properties(config, apply_column_comments=False)
    spark.sql(f"OPTIMIZE {_mne_sql_identifier(config.target_table)}")


def preview_map_text_events(
    event_ids: Sequence[int],
    config: MapTextEventsConfig = MAP_TEXT_EVENTS_CONFIG,
) -> DataFrame:
    """Build replacement rows for explicit EVENT_ID values without writing data."""
    normalized_ids = sorted({int(event_id) for event_id in event_ids})
    if not normalized_ids:
        raise ValueError("event_ids must contain at least one EVENT_ID")
    if len(normalized_ids) > int(config.broadcast_event_id_limit):
        raise ValueError(
            f"preview is limited to {config.broadcast_event_id_limit} distinct EVENT_ID values"
        )
    source_versions = _mte_capture_source_versions(config)
    keys = spark.createDataFrame([(event_id,) for event_id in normalized_ids], "EVENT_ID long")
    string_rows = (
        _mne_latest_string_results(
            config,
            source_versions[config.string_result_table],
            event_keys=keys,
            broadcast_keys=True,
        )
        .withColumn("STRING_RESULT_CDF_COMMIT_VERSION", F.lit(None).cast("long"))
        .withColumn("STRING_RESULT_CDF_COMMIT_TIMESTAMP", F.lit(None).cast("timestamp"))
        .withColumn("STRING_RESULT_CDF_CHANGE_TYPE", F.lit(None).cast("string"))
        .withColumn("TRIGGER_SOURCES", F.lit("PREVIEW"))
    )
    return _mte_build_enriched_text_events(
        config,
        string_rows,
        source_versions,
        run_id=f"preview-{uuid.uuid4()}",
        run_timestamp=datetime.now(timezone.utc),
        broadcast_keys=True,
    )

print('map_text_events replacement functions loaded. Call plan_map_text_events_run() for a read-only plan, or deploy_map_text_events_improvements() for the one-time validated migration.')

try:
    _pipeline_run_recoverable('map_text_events', _PIPELINE_FULL_REFRESH, lambda: process_text_events_incremental(force_full_rebuild=False, backup_existing=False), lambda: process_text_events_incremental(force_full_rebuild=True, backup_existing=False))
    _PIPELINE_UPDATED_TARGETS.extend(['4_prod.bronze.map_text_events'])
    _pipeline_mark_component_complete('map_text_events', ['4_prod.bronze.map_text_events'])
    _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_text_events'})
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

_pipeline_component_start("map_date_events")
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
from typing import Dict, Optional, Sequence
import json
import uuid
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

@dataclass(frozen=True)
class MapDateEventsConfig:
    target_table: str = '4_prod.bronze.map_date_events'
    date_result_table: str = '4_prod.raw.mill_ce_date_result'
    clinical_event_table: str = '4_prod.raw.mill_clinical_event'
    code_value_table: str = '3_lookup.mill.mill_code_value'
    order_catalog_table: str = '3_lookup.mill.mill_order_catalog'
    state_table: str = '6_mgmt.bronze.map_date_events_pipeline_state'
    audit_table: str = '6_mgmt.bronze.map_date_events_pipeline_audit'
MAP_DATE_EVENTS_CONFIG = MapDateEventsConfig()

def _source_tables(config: MapDateEventsConfig):
    return (config.date_result_table, config.clinical_event_table, config.code_value_table, config.order_catalog_table)
map_date_events_comment = 'Unfiltered latest-source-state bronze mapping for Millennium CE date results. One row is retained per EVENT_ID. Source validity, status, Trust, audit timestamps, upstream versions, change triggers, and lookup provenance are retained so downstream layers can apply their own current-state and data-quality rules.'

def _field(name: str, data_type: T.DataType, comment: str) -> T.StructField:
    return T.StructField(name, data_type, True, metadata={'comment': comment})
schema_map_date_events = T.StructType([_field('EVENT_ID', T.LongType(), 'Unique logical clinical event identifier.'), _field('ENCNTR_ID', T.LongType(), 'Canonical encounter identifier; clinical-event value preferred, date-result value used as fallback.'), _field('PERSON_ID', T.LongType(), 'Person identifier from the clinical event.'), _field('ORDER_ID', T.LongType(), 'Order identifier from the clinical event.'), _field('EVENT_CLASS_CD', T.IntegerType(), 'Clinical-event storage class code.'), _field('PERFORMED_PRSNL_ID', T.LongType(), 'Personnel identifier for the performer.'), _field('RESULT_DT_TM', T.TimestampType(), 'Date/time value stored by CE_DATE_RESULT.'), _field('EVENT_TITLE_TEXT', T.StringType(), 'Clinical-event title text.'), _field('EVENT_CD', T.IntegerType(), 'Clinical event code.'), _field('EVENT_CD_DISPLAY', T.StringType(), 'Resolved description/display for EVENT_CD.'), _field('CATALOG_CD', T.IntegerType(), 'Order catalog code.'), _field('CATALOG_DISPLAY', T.StringType(), 'Resolved display from the matching order catalog row only.'), _field('CATALOG_TYPE_CD', T.IntegerType(), 'Order catalog type code.'), _field('CATALOG_TYPE_DISPLAY', T.StringType(), 'Resolved description/display for CATALOG_TYPE_CD.'), _field('CONTRIBUTOR_SYSTEM_CD', T.IntegerType(), 'Contributor system code.'), _field('CONTRIBUTOR_SYSTEM_DISPLAY', T.StringType(), 'Resolved description/display for contributor system.'), _field('REFERENCE_NBR', T.StringType(), 'Source reference number.'), _field('PARENT_EVENT_ID', T.LongType(), 'Parent logical clinical event identifier.'), _field('NORMALCY_CD', T.IntegerType(), 'Normalcy code.'), _field('NORMALCY_DISPLAY', T.StringType(), 'Resolved description/display for normalcy.'), _field('ENTRY_MODE_CD', T.IntegerType(), 'Entry mode code.'), _field('ENTRY_MODE_DISPLAY', T.StringType(), 'Resolved description/display for entry mode.'), _field('PERFORMED_DT_TM', T.TimestampType(), 'Date/time the clinical event was performed or authored.'), _field('CLINSIG_UPDT_DT_TM', T.TimestampType(), 'Clinically significant update timestamp.'), _field('PARENT_EVENT_TITLE_TEXT', T.StringType(), 'Parent clinical-event title.'), _field('PARENT_EVENT_CD', T.IntegerType(), 'Parent clinical event code.'), _field('PARENT_EVENT_CD_DISPLAY', T.StringType(), 'Resolved description/display for the parent event code.'), _field('PARENT_CATALOG_CD', T.IntegerType(), 'Parent order catalog code.'), _field('PARENT_CATALOG_DISPLAY', T.StringType(), 'Resolved display from the parent order catalog row only.'), _field('PARENT_CATALOG_TYPE_CD', T.IntegerType(), 'Parent order catalog type code.'), _field('PARENT_CATALOG_TYPE_DISPLAY', T.StringType(), 'Resolved description/display for parent catalog type.'), _field('PARENT_REFERENCE_NBR', T.StringType(), 'Parent source reference number.'), _field('ADC_UPDT', T.TimestampType(), 'Greatest meaningful upstream row update timestamp; not used as a CDF watermark.'), _field('EVENT_LABEL', T.StringType(), 'Best available event label without overwriting source-specific display columns.'), _field('DATE_TYPE_FLAG', T.IntegerType(), 'Type discriminator from CE_DATE_RESULT.'), _field('RESULT_TZ', T.LongType(), 'Time-zone code associated with RESULT_DT_TM.'), _field('DATE_RESULT_VALID_FROM_DT_TM', T.TimestampType(), 'Beginning of validity for the selected date-result source row.'), _field('DATE_RESULT_VALID_UNTIL_DT_TM', T.TimestampType(), 'End of validity for the selected date-result source row.'), _field('DATE_RESULT_UPDT_DT_TM', T.TimestampType(), 'Millennium update timestamp for the selected date-result row.'), _field('DATE_RESULT_UPDT_CNT', T.LongType(), 'Millennium update counter for the selected date-result row.'), _field('DATE_RESULT_ADC_UPDT', T.TimestampType(), 'ADC ingestion/update timestamp from the selected date-result row.'), _field('DATE_RESULT_EFFECTIVE_UPDT_DT_TM', T.TimestampType(), 'COALESCE(DATE_RESULT_ADC_UPDT, DATE_RESULT_UPDT_DT_TM), for audit only.'), _field('DATE_RESULT_ENCNTR_ID', T.LongType(), 'Encounter identifier supplied by CE_DATE_RESULT.'), _field('DATE_RESULT_ORGANIZATION_ID', T.LongType(), 'Organization identifier supplied by CE_DATE_RESULT.'), _field('TRUST', T.StringType(), 'Trust value supplied by CE_DATE_RESULT; no Trust filter is applied.'), _field('CLINICAL_EVENT_ID', T.LongType(), 'Physical/versioned clinical_event row identifier.'), _field('CLINICAL_EVENT_ENCNTR_ID', T.LongType(), 'Encounter identifier supplied by CLINICAL_EVENT.'), _field('CLINICAL_EVENT_ORGANIZATION_ID', T.LongType(), 'Organization identifier supplied by CLINICAL_EVENT.'), _field('CLINICAL_EVENT_TRUST', T.StringType(), 'Trust value supplied by CLINICAL_EVENT.'), _field('ORGANIZATION_ID', T.LongType(), 'Canonical organization identifier; clinical-event value preferred.'), _field('CLINICAL_EVENT_VALID_FROM_DT_TM', T.TimestampType(), 'Beginning of validity for the selected clinical-event row.'), _field('CLINICAL_EVENT_VALID_UNTIL_DT_TM', T.TimestampType(), 'End of validity for the selected clinical-event row.'), _field('CLINICAL_EVENT_UPDT_DT_TM', T.TimestampType(), 'Millennium update timestamp for the selected clinical-event row.'), _field('CLINICAL_EVENT_UPDT_CNT', T.LongType(), 'Millennium update counter for the selected clinical-event row.'), _field('CLINICAL_EVENT_ADC_UPDT', T.TimestampType(), 'ADC ingestion/update timestamp from the selected clinical-event row.'), _field('EVENT_START_DT_TM', T.TimestampType(), 'Clinical start date/time from CLINICAL_EVENT.'), _field('EVENT_END_DT_TM', T.TimestampType(), 'Clinically relevant event date/time from CLINICAL_EVENT.'), _field('RESULT_STATUS_CD', T.LongType(), 'Result status code; retained without filtering.'), _field('RECORD_STATUS_CD', T.LongType(), 'Record status code; retained without filtering.'), _field('AUTHENTIC_FLAG', T.LongType(), 'Clinical-event authentication flag.'), _field('PUBLISH_FLAG', T.LongType(), 'Clinical-event publish/viewability flag.'), _field('EVENT_RELTN_CD', T.LongType(), 'Clinical-event relationship code.'), _field('EVENT_TAG', T.StringType(), 'Clinical-event display tag.'), _field('SOURCE_CD', T.LongType(), 'Clinical-event result source code.'), _field('VERIFIED_DT_TM', T.TimestampType(), 'Date/time the clinical event was verified.'), _field('VERIFIED_PRSNL_ID', T.LongType(), 'Personnel identifier for the verifier.'), _field('PARENT_CLINICAL_EVENT_ID', T.LongType(), 'Physical/versioned parent clinical_event row identifier.'), _field('PARENT_VALID_FROM_DT_TM', T.TimestampType(), 'Beginning of validity for the selected parent event row.'), _field('PARENT_VALID_UNTIL_DT_TM', T.TimestampType(), 'End of validity for the selected parent event row.'), _field('PARENT_UPDT_DT_TM', T.TimestampType(), 'Millennium update timestamp for the selected parent event row.'), _field('PARENT_UPDT_CNT', T.LongType(), 'Millennium update counter for the selected parent event row.'), _field('PARENT_ADC_UPDT', T.TimestampType(), 'ADC ingestion/update timestamp for the selected parent event row.'), _field('PARENT_RESULT_STATUS_CD', T.LongType(), 'Parent result status code; retained without filtering.'), _field('CATALOG_PRIMARY_MNEMONIC', T.StringType(), 'Primary mnemonic from the matching order catalog row.'), _field('CATALOG_DEPT_DISPLAY_NAME', T.StringType(), 'Departmental display name from the matching order catalog row.'), _field('CATALOG_ACTIVE_IND', T.LongType(), 'Active indicator from the matching order catalog row.'), _field('PARENT_CATALOG_PRIMARY_MNEMONIC', T.StringType(), 'Primary mnemonic from the parent order catalog row.'), _field('PARENT_CATALOG_DEPT_DISPLAY_NAME', T.StringType(), 'Departmental display name from the parent order catalog row.'), _field('PARENT_CATALOG_ACTIVE_IND', T.LongType(), 'Active indicator from the parent order catalog row.'), _field('LOOKUP_ADC_UPDT', T.TimestampType(), 'Greatest ADC_UPDT among lookup rows used to enrich the record.'), _field('CLINICAL_EVENT_FOUND_IND', T.BooleanType(), 'True when a clinical-event source row was found.'), _field('PARENT_EVENT_FOUND_IND', T.BooleanType(), 'True when a parent clinical-event source row was found.'), _field('ORDER_CATALOG_FOUND_IND', T.BooleanType(), 'True when the main order catalog row was found.'), _field('PARENT_ORDER_CATALOG_FOUND_IND', T.BooleanType(), 'True when the parent order catalog row was found.'), _field('ENCOUNTER_ID_MATCH_IND', T.BooleanType(), 'Comparison of date-result and clinical-event encounter identifiers when both exist.'), _field('SOURCE_CURRENT_IND', T.BooleanType(), 'Validity flag evaluated at the fixed pipeline run timestamp; no row is filtered by it.'), _field('SOURCE_DELETED_IND', T.BooleanType(), 'True when a changed EVENT_ID no longer has any date-result source row.'), _field('DATE_RESULT_CDF_COMMIT_VERSION', T.LongType(), 'Latest date-result CDF commit version that triggered this refresh.'), _field('DATE_RESULT_CDF_COMMIT_TIMESTAMP', T.TimestampType(), 'Latest date-result CDF commit timestamp that triggered this refresh.'), _field('DATE_RESULT_CDF_CHANGE_TYPE', T.StringType(), 'Latest date-result CDF change type that triggered this refresh.'), _field('TRIGGER_SOURCES', T.StringType(), 'Comma-separated upstream sources that caused this row to be rebuilt.'), _field('DATE_RESULT_SOURCE_VERSION', T.LongType(), 'Date-result Delta version used for this row build.'), _field('CLINICAL_EVENT_SOURCE_VERSION', T.LongType(), 'Clinical-event Delta version used for this row build.'), _field('CODE_VALUE_SOURCE_VERSION', T.LongType(), 'Code-value Delta version used for this row build.'), _field('ORDER_CATALOG_SOURCE_VERSION', T.LongType(), 'Order-catalog Delta version used for this row build.'), _field('ROW_HASH', T.LongType(), 'Fast hash of source-derived row content used to avoid unchanged rewrites.'), _field('PIPELINE_RUN_ID', T.StringType(), 'Unique identifier for the pipeline run that last changed this row.'), _field('PIPELINE_UPDT_DT_TM', T.TimestampType(), 'Pipeline timestamp for the last target change.')])

def _sql_identifier(table_name: str) -> str:
    tick = chr(96)
    return '.'.join((f'{tick}{part.replace(tick, tick + tick)}{tick}' for part in table_name.split('.')))

def _escape_sql_string(value: str) -> str:
    return value.replace('\\', '\\\\').replace("'", "''")

def _table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _latest_delta_version(table_name: str) -> int:
    row = spark.sql(f'DESCRIBE HISTORY {_sql_identifier(table_name)} LIMIT 1').select('version').first()
    if row is None:
        raise RuntimeError(f'No Delta history was returned for {table_name}')
    return int(row['version'])

def _capture_source_versions(config: MapDateEventsConfig) -> Dict[str, int]:
    return {source: _latest_delta_version(source) for source in _source_tables(config)}

def _read_snapshot(table_name: str, version: int) -> DataFrame:
    return _m30_hardening_read_pinned_snapshot(table_name, int(version))

def _read_cdf(table_name: str, starting_version: int, ending_version: int) -> Optional[DataFrame]:
    if starting_version > ending_version:
        return None
    try:
        changes = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', int(starting_version)).option('endingVersion', int(ending_version)).table(table_name)
        return _m30_hardening_validate_cdf(changes)
    except Exception as exc:
        raise RuntimeError(f'CDF could not be read for {table_name} from version {starting_version} through {ending_version}. No timestamp fallback was used, because it can silently lose null or late-arriving timestamps. Run process_date_events_incremental(force_full_rebuild=True).') from exc

def _checked_double_id(column: F.Column, label: str) -> F.Column:
    max_exact_double_integer = 9007199254740991
    return F.when(column.isNull(), F.lit(None)).when(F.isnan(column), F.lit(None)).when(column != F.floor(column), F.lit(None)).when(F.abs(column) > F.lit(max_exact_double_integer), F.lit(None)).otherwise(column.cast('long'))

def _checked_int(column: F.Column, label: str) -> F.Column:
    return F.when(column.isNull(), F.lit(None).cast('int')).when((column < F.lit(-2147483648)) | (column > F.lit(2147483647)), F.lit(None)).otherwise(column.cast('int'))

def _nonblank(column: F.Column) -> F.Column:
    return F.when(F.length(F.trim(column.cast('string'))) > 0, column)

def _stable_hash_columns(columns: Sequence[F.Column]) -> F.Column:
    normalized = [F.coalesce(column.cast('string'), F.lit('<NULL>')) for column in columns]
    return F.xxhash64(*normalized)

def _align_to_schema(df: DataFrame, schema: T.StructType) -> DataFrame:
    # Avoid eager Spark Connect analysis of the full unresolved enrichment plan.
    return df.select(*[F.col(field.name).cast(field.dataType).alias(field.name, metadata=field.metadata) for field in schema.fields])

def _ensure_metadata_tables(config: MapDateEventsConfig) -> None:
    spark.sql(f'\n        CREATE TABLE IF NOT EXISTS {_sql_identifier(config.state_table)} (\n            source_table STRING,\n            last_processed_version BIGINT,\n            last_processed_at TIMESTAMP,\n            last_run_id STRING\n        )\n        USING DELTA\n    ')
    spark.sql(f'\n        CREATE TABLE IF NOT EXISTS {_sql_identifier(config.audit_table)} (\n            run_id STRING,\n            event_timestamp TIMESTAMP,\n            status STRING,\n            mode STRING,\n            message STRING,\n            source_versions_json STRING,\n            metrics_json STRING\n        )\n        USING DELTA\n    ')

def _read_pipeline_state(config: MapDateEventsConfig) -> Dict[str, int]:
    if not _table_exists(config.state_table):
        return {}
    rows = spark.table(config.state_table).filter(F.col('source_table').isin(list(_source_tables(config)))).select('source_table', 'last_processed_version').collect()
    return {row['source_table']: int(row['last_processed_version']) for row in rows if row['last_processed_version'] is not None}

def _commit_pipeline_state(config: MapDateEventsConfig, source_versions: Dict[str, int], run_id: str, completed_at: datetime) -> None:
    rows = [(source, int(version), completed_at, run_id) for source, version in source_versions.items()]
    schema = T.StructType([T.StructField('source_table', T.StringType(), False), T.StructField('last_processed_version', T.LongType(), False), T.StructField('last_processed_at', T.TimestampType(), False), T.StructField('last_run_id', T.StringType(), False)])
    updates = spark.createDataFrame(rows, schema)
    DeltaTable.forName(spark, config.state_table).alias('t').merge(updates.alias('s'), 't.source_table = s.source_table').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def _write_audit_event(config: MapDateEventsConfig, run_id: str, status: str, mode: str, message: str, source_versions: Optional[Dict[str, int]]=None, metrics: Optional[Dict[str, object]]=None) -> None:
    try:
        row = [(run_id, datetime.now(timezone.utc), status, mode, message[:8000], json.dumps(source_versions or {}, sort_keys=True, default=str), json.dumps(metrics or {}, sort_keys=True, default=str))]
        schema = T.StructType([T.StructField('run_id', T.StringType(), False), T.StructField('event_timestamp', T.TimestampType(), False), T.StructField('status', T.StringType(), False), T.StructField('mode', T.StringType(), False), T.StructField('message', T.StringType(), True), T.StructField('source_versions_json', T.StringType(), True), T.StructField('metrics_json', T.StringType(), True)])
        spark.createDataFrame(row, schema).write.mode('append').saveAsTable(config.audit_table)
    except Exception as audit_exc:
        print(f'[WARN] Could not write pipeline audit event: {audit_exc}')

def _latest_date_results(config: MapDateEventsConfig, source_version: int, event_keys: Optional[DataFrame]=None) -> DataFrame:
    raw = _read_snapshot(config.date_result_table, source_version).select('EVENT_ID', 'RESULT_DT_TM', 'VALID_FROM_DT_TM', 'VALID_UNTIL_DT_TM', 'UPDT_DT_TM', 'UPDT_CNT', 'DATE_TYPE_FLAG', 'RESULT_TZ', 'ADC_UPDT', 'ENCNTR_ID', 'ORGANIZATION_ID', 'Trust').withColumn('_EVENT_ID', _checked_double_id(F.col('EVENT_ID'), 'CE_DATE_RESULT.EVENT_ID'))
    if event_keys is not None:
        keys = event_keys.select(F.col('EVENT_ID').cast('long').alias('_KEY_EVENT_ID')).dropDuplicates()
        raw = raw.join(keys, F.col('_EVENT_ID') == F.col('_KEY_EVENT_ID'), 'inner').drop('_KEY_EVENT_ID')
    tie_hash = _stable_hash_columns([F.col('RESULT_DT_TM'), F.col('VALID_FROM_DT_TM'), F.col('VALID_UNTIL_DT_TM'), F.col('UPDT_DT_TM'), F.col('UPDT_CNT'), F.col('DATE_TYPE_FLAG'), F.col('RESULT_TZ'), F.col('ADC_UPDT'), F.col('ENCNTR_ID'), F.col('ORGANIZATION_ID'), F.col('Trust')])
    latest_window = Window.partitionBy('_EVENT_ID').orderBy(F.col('VALID_FROM_DT_TM').desc_nulls_last(), F.col('UPDT_CNT').desc_nulls_last(), F.col('UPDT_DT_TM').desc_nulls_last(), F.col('ADC_UPDT').desc_nulls_last(), F.col('VALID_UNTIL_DT_TM').desc_nulls_last(), tie_hash.desc())
    return raw.withColumn('_ROW_NUMBER', F.row_number().over(latest_window)).filter(F.col('_ROW_NUMBER') == 1).select(F.col('_EVENT_ID').alias('EVENT_ID'), F.col('RESULT_DT_TM'), _checked_int(F.col('DATE_TYPE_FLAG'), 'CE_DATE_RESULT.DATE_TYPE_FLAG').alias('DATE_TYPE_FLAG'), F.col('RESULT_TZ').cast('long').alias('RESULT_TZ'), F.col('VALID_FROM_DT_TM').alias('DATE_RESULT_VALID_FROM_DT_TM'), F.col('VALID_UNTIL_DT_TM').alias('DATE_RESULT_VALID_UNTIL_DT_TM'), F.col('UPDT_DT_TM').alias('DATE_RESULT_UPDT_DT_TM'), F.col('UPDT_CNT').cast('long').alias('DATE_RESULT_UPDT_CNT'), F.col('ADC_UPDT').alias('DATE_RESULT_ADC_UPDT'), F.coalesce(F.col('ADC_UPDT'), F.col('UPDT_DT_TM')).alias('DATE_RESULT_EFFECTIVE_UPDT_DT_TM'), F.col('ENCNTR_ID').cast('long').alias('DATE_RESULT_ENCNTR_ID'), F.col('ORGANIZATION_ID').cast('long').alias('DATE_RESULT_ORGANIZATION_ID'), F.col('Trust').alias('TRUST'))
_CLINICAL_EVENT_COLUMNS = ('CLINICAL_EVENT_ID', 'EVENT_ID', 'ENCNTR_ID', 'PERSON_ID', 'ORDER_ID', 'EVENT_CLASS_CD', 'PERFORMED_PRSNL_ID', 'EVENT_TITLE_TEXT', 'EVENT_CD', 'CATALOG_CD', 'CONTRIBUTOR_SYSTEM_CD', 'REFERENCE_NBR', 'PARENT_EVENT_ID', 'NORMALCY_CD', 'ENTRY_MODE_CD', 'PERFORMED_DT_TM', 'CLINSIG_UPDT_DT_TM', 'VALID_FROM_DT_TM', 'VALID_UNTIL_DT_TM', 'UPDT_DT_TM', 'UPDT_CNT', 'ADC_UPDT', 'EVENT_START_DT_TM', 'EVENT_END_DT_TM', 'RESULT_STATUS_CD', 'RECORD_STATUS_CD', 'AUTHENTIC_FLAG', 'PUBLISH_FLAG', 'EVENT_RELTN_CD', 'EVENT_TAG', 'SOURCE_CD', 'VERIFIED_DT_TM', 'VERIFIED_PRSNL_ID', 'ORGANIZATION_ID', 'Trust')

def _latest_clinical_events(config: MapDateEventsConfig, source_version: int, event_keys: DataFrame, prefix: str, key_output_name: str) -> DataFrame:
    keys = event_keys.select(F.col('EVENT_ID').cast('long').alias('_KEY_EVENT_ID')).dropDuplicates()
    raw = _read_snapshot(config.clinical_event_table, source_version).select(*_CLINICAL_EVENT_COLUMNS).withColumn('_CE_EVENT_ID', F.col('EVENT_ID').cast('long')).join(keys, F.col('_CE_EVENT_ID') == F.col('_KEY_EVENT_ID'), 'inner').drop('_KEY_EVENT_ID')
    tie_hash = _stable_hash_columns([F.col('CLINICAL_EVENT_ID'), F.col('VALID_FROM_DT_TM'), F.col('VALID_UNTIL_DT_TM'), F.col('UPDT_DT_TM'), F.col('UPDT_CNT'), F.col('ADC_UPDT')])
    latest_window = Window.partitionBy('_CE_EVENT_ID').orderBy(F.col('VALID_FROM_DT_TM').desc_nulls_last(), F.col('UPDT_CNT').desc_nulls_last(), F.col('UPDT_DT_TM').desc_nulls_last(), F.col('ADC_UPDT').desc_nulls_last(), F.col('CLINICAL_EVENT_ID').desc_nulls_last(), tie_hash.desc())
    ranked = raw.withColumn('_ROW_NUMBER', F.row_number().over(latest_window)).filter(F.col('_ROW_NUMBER') == 1)
    return ranked.select(F.col('_CE_EVENT_ID').alias(key_output_name), *[F.col(column_name).alias(f'{prefix}{column_name}') for column_name in _CLINICAL_EVENT_COLUMNS if column_name != 'EVENT_ID'])

def create_date_event_code_lookup(code_values: DataFrame, alias_suffix: str) -> DataFrame:
    """Backward-compatible lookup helper with a DISPLAY fallback."""
    return code_values.select(F.col('CODE_VALUE'), F.coalesce(_nonblank(F.col('DESCRIPTION')), _nonblank(F.col('DISPLAY'))).alias(f'{alias_suffix}_desc')).alias(alias_suffix)

def _code_lookup(config: MapDateEventsConfig, source_version: int, tag: str) -> DataFrame:
    raw = _read_snapshot(config.code_value_table, source_version)
    return F.broadcast(raw.select(_checked_double_id(F.col('CODE_VALUE'), f'CODE_VALUE.{tag}').alias(f'{tag}_CODE_VALUE'), F.col('DESCRIPTION').alias(f'{tag}_DESCRIPTION'), F.col('DISPLAY').alias(f'{tag}_DISPLAY'), F.col('CDF_MEANING').alias(f'{tag}_CDF_MEANING'), F.col('ADC_UPDT').alias(f'{tag}_ADC_UPDT')))

def _catalog_lookup(config: MapDateEventsConfig, source_version: int, tag: str) -> DataFrame:
    raw = _read_snapshot(config.order_catalog_table, source_version)
    return F.broadcast(raw.select(_checked_double_id(F.col('CATALOG_CD'), f'ORDER_CATALOG.{tag}.CATALOG_CD').alias(f'{tag}_CATALOG_CD'), _checked_int(F.col('CATALOG_TYPE_CD'), f'ORDER_CATALOG.{tag}.CATALOG_TYPE_CD').alias(f'{tag}_CATALOG_TYPE_CD'), F.col('DESCRIPTION').alias(f'{tag}_DESCRIPTION'), F.col('PRIMARY_MNEMONIC').alias(f'{tag}_PRIMARY_MNEMONIC'), F.col('DEPT_DISPLAY_NAME').alias(f'{tag}_DEPT_DISPLAY_NAME'), F.col('ACTIVE_IND').cast('long').alias(f'{tag}_ACTIVE_IND'), F.col('ADC_UPDT').alias(f'{tag}_ADC_UPDT')))

def _build_enriched_date_events(config: MapDateEventsConfig, date_rows: DataFrame, source_versions: Dict[str, int], run_id: str, run_timestamp: datetime) -> DataFrame:
    event_keys = date_rows.select('EVENT_ID').dropDuplicates()
    ce = _latest_clinical_events(config, source_versions[config.clinical_event_table], event_keys, prefix='CE_', key_output_name='EVENT_ID')
    base = date_rows.join(ce, 'EVENT_ID', 'left')
    parent_keys = base.select(F.col('CE_PARENT_EVENT_ID').cast('long').alias('EVENT_ID')).filter(F.col('EVENT_ID').isNotNull()).dropDuplicates()
    parent = _latest_clinical_events(config, source_versions[config.clinical_event_table], parent_keys, prefix='PARENT_', key_output_name='PARENT_LOOKUP_EVENT_ID')
    base = base.join(parent, F.col('CE_PARENT_EVENT_ID').cast('long') == F.col('PARENT_LOOKUP_EVENT_ID'), 'left')
    code_version = source_versions[config.code_value_table]
    catalog_version = source_versions[config.order_catalog_table]
    event_cv = _code_lookup(config, code_version, 'EVENT_CV')
    parent_event_cv = _code_lookup(config, code_version, 'PARENT_EVENT_CV')
    normalcy_cv = _code_lookup(config, code_version, 'NORMALCY_CV')
    contributor_cv = _code_lookup(config, code_version, 'CONTRIBUTOR_CV')
    entry_cv = _code_lookup(config, code_version, 'ENTRY_CV')
    catalog_type_cv = _code_lookup(config, code_version, 'CATALOG_TYPE_CV')
    parent_catalog_type_cv = _code_lookup(config, code_version, 'PARENT_CATALOG_TYPE_CV')
    order_catalog = _catalog_lookup(config, catalog_version, 'OC')
    parent_catalog = _catalog_lookup(config, catalog_version, 'POC')
    base = base.join(order_catalog, F.col('CE_CATALOG_CD').cast('long') == F.col('OC_CATALOG_CD'), 'left').join(parent_catalog, F.col('PARENT_CATALOG_CD').cast('long') == F.col('POC_CATALOG_CD'), 'left').join(event_cv, F.col('CE_EVENT_CD').cast('long') == F.col('EVENT_CV_CODE_VALUE'), 'left').join(parent_event_cv, F.col('PARENT_EVENT_CD').cast('long') == F.col('PARENT_EVENT_CV_CODE_VALUE'), 'left').join(normalcy_cv, F.col('CE_NORMALCY_CD').cast('long') == F.col('NORMALCY_CV_CODE_VALUE'), 'left').join(contributor_cv, F.col('CE_CONTRIBUTOR_SYSTEM_CD').cast('long') == F.col('CONTRIBUTOR_CV_CODE_VALUE'), 'left').join(entry_cv, F.col('CE_ENTRY_MODE_CD').cast('long') == F.col('ENTRY_CV_CODE_VALUE'), 'left').join(catalog_type_cv, F.col('OC_CATALOG_TYPE_CD').cast('long') == F.col('CATALOG_TYPE_CV_CODE_VALUE'), 'left').join(parent_catalog_type_cv, F.col('POC_CATALOG_TYPE_CD').cast('long') == F.col('PARENT_CATALOG_TYPE_CV_CODE_VALUE'), 'left')
    event_display = F.coalesce(_nonblank(F.col('EVENT_CV_DESCRIPTION')), _nonblank(F.col('EVENT_CV_DISPLAY')))
    parent_event_display = F.coalesce(_nonblank(F.col('PARENT_EVENT_CV_DESCRIPTION')), _nonblank(F.col('PARENT_EVENT_CV_DISPLAY')))
    normalcy_display = F.coalesce(_nonblank(F.col('NORMALCY_CV_DESCRIPTION')), _nonblank(F.col('NORMALCY_CV_DISPLAY')))
    contributor_display = F.coalesce(_nonblank(F.col('CONTRIBUTOR_CV_DESCRIPTION')), _nonblank(F.col('CONTRIBUTOR_CV_DISPLAY')))
    entry_display = F.coalesce(_nonblank(F.col('ENTRY_CV_DESCRIPTION')), _nonblank(F.col('ENTRY_CV_DISPLAY')))
    catalog_type_display = F.coalesce(_nonblank(F.col('CATALOG_TYPE_CV_DESCRIPTION')), _nonblank(F.col('CATALOG_TYPE_CV_DISPLAY')))
    parent_catalog_type_display = F.coalesce(_nonblank(F.col('PARENT_CATALOG_TYPE_CV_DESCRIPTION')), _nonblank(F.col('PARENT_CATALOG_TYPE_CV_DISPLAY')))
    catalog_display = F.coalesce(_nonblank(F.col('OC_DESCRIPTION')), _nonblank(F.col('OC_PRIMARY_MNEMONIC')), _nonblank(F.col('OC_DEPT_DISPLAY_NAME')))
    parent_catalog_display = F.coalesce(_nonblank(F.col('POC_DESCRIPTION')), _nonblank(F.col('POC_PRIMARY_MNEMONIC')), _nonblank(F.col('POC_DEPT_DISPLAY_NAME')))
    lookup_adc_updt = F.greatest(F.col('EVENT_CV_ADC_UPDT'), F.col('PARENT_EVENT_CV_ADC_UPDT'), F.col('NORMALCY_CV_ADC_UPDT'), F.col('CONTRIBUTOR_CV_ADC_UPDT'), F.col('ENTRY_CV_ADC_UPDT'), F.col('CATALOG_TYPE_CV_ADC_UPDT'), F.col('PARENT_CATALOG_TYPE_CV_ADC_UPDT'), F.col('OC_ADC_UPDT'), F.col('POC_ADC_UPDT'))
    adc_updt = F.greatest(F.col('DATE_RESULT_EFFECTIVE_UPDT_DT_TM'), F.col('CE_ADC_UPDT'), F.col('PARENT_ADC_UPDT'), lookup_adc_updt)
    event_label = F.coalesce(_nonblank(F.col('CE_EVENT_TITLE_TEXT')), _nonblank(event_display), _nonblank(catalog_display))
    encounter_match = F.when(F.col('DATE_RESULT_ENCNTR_ID').isNull() | F.col('CE_ENCNTR_ID').isNull(), F.lit(None).cast('boolean')).otherwise(F.col('DATE_RESULT_ENCNTR_ID') == F.col('CE_ENCNTR_ID'))
    source_current = F.when(F.col('DATE_RESULT_VALID_UNTIL_DT_TM').isNull(), F.lit(None).cast('boolean')).otherwise(F.col('DATE_RESULT_VALID_UNTIL_DT_TM') > F.lit(run_timestamp))
    selected = base.select(F.col('EVENT_ID'), F.coalesce(F.col('CE_ENCNTR_ID'), F.col('DATE_RESULT_ENCNTR_ID')).cast('long').alias('ENCNTR_ID'), F.col('CE_PERSON_ID').cast('long').alias('PERSON_ID'), F.col('CE_ORDER_ID').cast('long').alias('ORDER_ID'), _checked_int(F.col('CE_EVENT_CLASS_CD'), 'CLINICAL_EVENT.EVENT_CLASS_CD').alias('EVENT_CLASS_CD'), F.col('CE_PERFORMED_PRSNL_ID').cast('long').alias('PERFORMED_PRSNL_ID'), F.col('RESULT_DT_TM'), F.col('CE_EVENT_TITLE_TEXT').alias('EVENT_TITLE_TEXT'), _checked_int(F.col('CE_EVENT_CD'), 'CLINICAL_EVENT.EVENT_CD').alias('EVENT_CD'), event_display.alias('EVENT_CD_DISPLAY'), _checked_int(F.col('CE_CATALOG_CD'), 'CLINICAL_EVENT.CATALOG_CD').alias('CATALOG_CD'), catalog_display.alias('CATALOG_DISPLAY'), F.col('OC_CATALOG_TYPE_CD').alias('CATALOG_TYPE_CD'), catalog_type_display.alias('CATALOG_TYPE_DISPLAY'), _checked_int(F.col('CE_CONTRIBUTOR_SYSTEM_CD'), 'CLINICAL_EVENT.CONTRIBUTOR_SYSTEM_CD').alias('CONTRIBUTOR_SYSTEM_CD'), contributor_display.alias('CONTRIBUTOR_SYSTEM_DISPLAY'), F.col('CE_REFERENCE_NBR').alias('REFERENCE_NBR'), F.col('CE_PARENT_EVENT_ID').cast('long').alias('PARENT_EVENT_ID'), _checked_int(F.col('CE_NORMALCY_CD'), 'CLINICAL_EVENT.NORMALCY_CD').alias('NORMALCY_CD'), normalcy_display.alias('NORMALCY_DISPLAY'), _checked_int(F.col('CE_ENTRY_MODE_CD'), 'CLINICAL_EVENT.ENTRY_MODE_CD').alias('ENTRY_MODE_CD'), entry_display.alias('ENTRY_MODE_DISPLAY'), F.col('CE_PERFORMED_DT_TM').alias('PERFORMED_DT_TM'), F.col('CE_CLINSIG_UPDT_DT_TM').alias('CLINSIG_UPDT_DT_TM'), F.col('PARENT_EVENT_TITLE_TEXT').alias('PARENT_EVENT_TITLE_TEXT'), _checked_int(F.col('PARENT_EVENT_CD'), 'PARENT_CLINICAL_EVENT.EVENT_CD').alias('PARENT_EVENT_CD'), parent_event_display.alias('PARENT_EVENT_CD_DISPLAY'), _checked_int(F.col('PARENT_CATALOG_CD'), 'PARENT_CLINICAL_EVENT.CATALOG_CD').alias('PARENT_CATALOG_CD'), parent_catalog_display.alias('PARENT_CATALOG_DISPLAY'), F.col('POC_CATALOG_TYPE_CD').alias('PARENT_CATALOG_TYPE_CD'), parent_catalog_type_display.alias('PARENT_CATALOG_TYPE_DISPLAY'), F.col('PARENT_REFERENCE_NBR').alias('PARENT_REFERENCE_NBR'), adc_updt.alias('ADC_UPDT'), event_label.alias('EVENT_LABEL'), F.col('DATE_TYPE_FLAG'), F.col('RESULT_TZ'), F.col('DATE_RESULT_VALID_FROM_DT_TM'), F.col('DATE_RESULT_VALID_UNTIL_DT_TM'), F.col('DATE_RESULT_UPDT_DT_TM'), F.col('DATE_RESULT_UPDT_CNT'), F.col('DATE_RESULT_ADC_UPDT'), F.col('DATE_RESULT_EFFECTIVE_UPDT_DT_TM'), F.col('DATE_RESULT_ENCNTR_ID'), F.col('DATE_RESULT_ORGANIZATION_ID'), F.col('TRUST'), F.col('CE_CLINICAL_EVENT_ID').cast('long').alias('CLINICAL_EVENT_ID'), F.col('CE_ENCNTR_ID').cast('long').alias('CLINICAL_EVENT_ENCNTR_ID'), F.col('CE_ORGANIZATION_ID').cast('long').alias('CLINICAL_EVENT_ORGANIZATION_ID'), F.col('CE_Trust').alias('CLINICAL_EVENT_TRUST'), F.coalesce(F.col('CE_ORGANIZATION_ID'), F.col('DATE_RESULT_ORGANIZATION_ID')).cast('long').alias('ORGANIZATION_ID'), F.col('CE_VALID_FROM_DT_TM').alias('CLINICAL_EVENT_VALID_FROM_DT_TM'), F.col('CE_VALID_UNTIL_DT_TM').alias('CLINICAL_EVENT_VALID_UNTIL_DT_TM'), F.col('CE_UPDT_DT_TM').alias('CLINICAL_EVENT_UPDT_DT_TM'), F.col('CE_UPDT_CNT').cast('long').alias('CLINICAL_EVENT_UPDT_CNT'), F.col('CE_ADC_UPDT').alias('CLINICAL_EVENT_ADC_UPDT'), F.col('CE_EVENT_START_DT_TM').alias('EVENT_START_DT_TM'), F.col('CE_EVENT_END_DT_TM').alias('EVENT_END_DT_TM'), F.col('CE_RESULT_STATUS_CD').cast('long').alias('RESULT_STATUS_CD'), F.col('CE_RECORD_STATUS_CD').cast('long').alias('RECORD_STATUS_CD'), F.col('CE_AUTHENTIC_FLAG').cast('long').alias('AUTHENTIC_FLAG'), F.col('CE_PUBLISH_FLAG').cast('long').alias('PUBLISH_FLAG'), F.col('CE_EVENT_RELTN_CD').cast('long').alias('EVENT_RELTN_CD'), F.col('CE_EVENT_TAG').alias('EVENT_TAG'), F.col('CE_SOURCE_CD').cast('long').alias('SOURCE_CD'), F.col('CE_VERIFIED_DT_TM').alias('VERIFIED_DT_TM'), F.col('CE_VERIFIED_PRSNL_ID').cast('long').alias('VERIFIED_PRSNL_ID'), F.col('PARENT_CLINICAL_EVENT_ID').cast('long').alias('PARENT_CLINICAL_EVENT_ID'), F.col('PARENT_VALID_FROM_DT_TM').alias('PARENT_VALID_FROM_DT_TM'), F.col('PARENT_VALID_UNTIL_DT_TM').alias('PARENT_VALID_UNTIL_DT_TM'), F.col('PARENT_UPDT_DT_TM').alias('PARENT_UPDT_DT_TM'), F.col('PARENT_UPDT_CNT').cast('long').alias('PARENT_UPDT_CNT'), F.col('PARENT_ADC_UPDT').alias('PARENT_ADC_UPDT'), F.col('PARENT_RESULT_STATUS_CD').cast('long').alias('PARENT_RESULT_STATUS_CD'), F.col('OC_PRIMARY_MNEMONIC').alias('CATALOG_PRIMARY_MNEMONIC'), F.col('OC_DEPT_DISPLAY_NAME').alias('CATALOG_DEPT_DISPLAY_NAME'), F.col('OC_ACTIVE_IND').alias('CATALOG_ACTIVE_IND'), F.col('POC_PRIMARY_MNEMONIC').alias('PARENT_CATALOG_PRIMARY_MNEMONIC'), F.col('POC_DEPT_DISPLAY_NAME').alias('PARENT_CATALOG_DEPT_DISPLAY_NAME'), F.col('POC_ACTIVE_IND').alias('PARENT_CATALOG_ACTIVE_IND'), lookup_adc_updt.alias('LOOKUP_ADC_UPDT'), F.col('CE_CLINICAL_EVENT_ID').isNotNull().alias('CLINICAL_EVENT_FOUND_IND'), F.col('PARENT_CLINICAL_EVENT_ID').isNotNull().alias('PARENT_EVENT_FOUND_IND'), F.col('OC_CATALOG_CD').isNotNull().alias('ORDER_CATALOG_FOUND_IND'), F.col('POC_CATALOG_CD').isNotNull().alias('PARENT_ORDER_CATALOG_FOUND_IND'), encounter_match.alias('ENCOUNTER_ID_MATCH_IND'), source_current.alias('SOURCE_CURRENT_IND'), F.lit(False).alias('SOURCE_DELETED_IND'), F.col('DATE_RESULT_CDF_COMMIT_VERSION'), F.col('DATE_RESULT_CDF_COMMIT_TIMESTAMP'), F.col('DATE_RESULT_CDF_CHANGE_TYPE'), F.col('TRIGGER_SOURCES'), F.lit(source_versions[config.date_result_table]).cast('long').alias('DATE_RESULT_SOURCE_VERSION'), F.lit(source_versions[config.clinical_event_table]).cast('long').alias('CLINICAL_EVENT_SOURCE_VERSION'), F.lit(source_versions[config.code_value_table]).cast('long').alias('CODE_VALUE_SOURCE_VERSION'), F.lit(source_versions[config.order_catalog_table]).cast('long').alias('ORDER_CATALOG_SOURCE_VERSION'))
    hash_exclusions = {'ROW_HASH', 'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM', 'TRIGGER_SOURCES', 'DATE_RESULT_CDF_COMMIT_VERSION', 'DATE_RESULT_CDF_COMMIT_TIMESTAMP', 'DATE_RESULT_CDF_CHANGE_TYPE'}
    hash_columns = [F.col(field.name) for field in schema_map_date_events.fields if field.name not in hash_exclusions]
    final_df = selected.withColumn('ROW_HASH', _stable_hash_columns(hash_columns)).withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_UPDT_DT_TM', F.lit(run_timestamp))
    return _align_to_schema(final_df, schema_map_date_events)

def _empty_date_cdf_metadata() -> DataFrame:
    schema = T.StructType([T.StructField('EVENT_ID', T.LongType(), False), T.StructField('DATE_RESULT_CDF_COMMIT_VERSION', T.LongType(), True), T.StructField('DATE_RESULT_CDF_COMMIT_TIMESTAMP', T.TimestampType(), True), T.StructField('DATE_RESULT_CDF_CHANGE_TYPE', T.StringType(), True)])
    return spark.createDataFrame([], schema)

def _date_result_changed_keys(config: MapDateEventsConfig, previous_version: int, current_version: int) -> DataFrame:
    cdf = _read_cdf(config.date_result_table, previous_version + 1, current_version)
    if cdf is None:
        return _empty_date_cdf_metadata()
    prepared = cdf.select(_checked_double_id(F.col('EVENT_ID'), 'CE_DATE_RESULT.CDF.EVENT_ID').alias('EVENT_ID'), F.col('_commit_version').cast('long').alias('_commit_version'), F.col('_commit_timestamp').alias('_commit_timestamp'), F.col('_change_type').alias('_change_type'), F.when(F.col('_change_type') == 'delete', F.lit(4)).when(F.col('_change_type') == 'update_postimage', F.lit(3)).when(F.col('_change_type') == 'insert', F.lit(2)).otherwise(F.lit(1)).alias('_change_rank'))
    return prepared.groupBy('EVENT_ID').agg(F.max('_commit_version').alias('DATE_RESULT_CDF_COMMIT_VERSION'), F.max('_commit_timestamp').alias('DATE_RESULT_CDF_COMMIT_TIMESTAMP'), F.max_by('_change_type', F.struct('_commit_version', '_commit_timestamp', '_change_rank')).alias('DATE_RESULT_CDF_CHANGE_TYPE'))

def _simple_changed_ids(table_name: str, key_column: str, previous_version: int, current_version: int, key_is_double: bool) -> DataFrame:
    cdf = _read_cdf(table_name, previous_version + 1, current_version)
    if cdf is None:
        return spark.createDataFrame([], 'CHANGE_KEY long')
    key = _checked_double_id(F.col(key_column), f'{table_name}.{key_column}') if key_is_double else F.col(key_column).cast('long')
    return cdf.select(key.alias('CHANGE_KEY')).filter(F.col('CHANGE_KEY').isNotNull()).dropDuplicates()

def _target_events_for_changed_codes(target: DataFrame, changed_codes: DataFrame) -> DataFrame:
    candidate_columns = [column_name for column_name in ('EVENT_CD', 'NORMALCY_CD', 'CONTRIBUTOR_SYSTEM_CD', 'ENTRY_MODE_CD', 'CATALOG_TYPE_CD', 'PARENT_EVENT_CD', 'PARENT_CATALOG_TYPE_CD') if column_name in target.columns]
    if not candidate_columns:
        return spark.createDataFrame([], 'EVENT_ID long')
    exploded = target.select('EVENT_ID', F.explode_outer(F.array(*[F.col(c).cast('long') for c in candidate_columns])).alias('CHANGE_KEY')).filter(F.col('CHANGE_KEY').isNotNull())
    return exploded.join(F.broadcast(changed_codes), 'CHANGE_KEY', 'inner').select('EVENT_ID').dropDuplicates()

def _target_events_for_changed_catalogs(target: DataFrame, changed_catalogs: DataFrame) -> DataFrame:
    candidate_columns = [column_name for column_name in ('CATALOG_CD', 'PARENT_CATALOG_CD') if column_name in target.columns]
    if not candidate_columns:
        return spark.createDataFrame([], 'EVENT_ID long')
    exploded = target.select('EVENT_ID', F.explode_outer(F.array(*[F.col(c).cast('long') for c in candidate_columns])).alias('CHANGE_KEY')).filter(F.col('CHANGE_KEY').isNotNull())
    return exploded.join(F.broadcast(changed_catalogs), 'CHANGE_KEY', 'inner').select('EVENT_ID').dropDuplicates()

def _build_changed_event_keys(config: MapDateEventsConfig, previous_versions: Dict[str, int], current_versions: Dict[str, int]) -> DataFrame:
    target = spark.table(config.target_table)
    date_meta = _date_result_changed_keys(config, previous_versions[config.date_result_table], current_versions[config.date_result_table])
    trigger_frames = [date_meta.select('EVENT_ID').withColumn('TRIGGER_SOURCE', F.lit('DATE_RESULT'))]
    ce_ids = _simple_changed_ids(config.clinical_event_table, 'EVENT_ID', previous_versions[config.clinical_event_table], current_versions[config.clinical_event_table], key_is_double=False)
    direct_ce = target.select('EVENT_ID').join(ce_ids, F.col('EVENT_ID') == F.col('CHANGE_KEY'), 'inner').select('EVENT_ID').withColumn('TRIGGER_SOURCE', F.lit('CLINICAL_EVENT'))
    parent_ce = target.select('EVENT_ID', 'PARENT_EVENT_ID').join(ce_ids, F.col('PARENT_EVENT_ID') == F.col('CHANGE_KEY'), 'inner').select('EVENT_ID').withColumn('TRIGGER_SOURCE', F.lit('PARENT_CLINICAL_EVENT'))
    trigger_frames.extend([direct_ce, parent_ce])
    code_ids = _simple_changed_ids(config.code_value_table, 'CODE_VALUE', previous_versions[config.code_value_table], current_versions[config.code_value_table], key_is_double=True)
    code_events = _target_events_for_changed_codes(target, code_ids).withColumn('TRIGGER_SOURCE', F.lit('CODE_VALUE'))
    trigger_frames.append(code_events)
    catalog_ids = _simple_changed_ids(config.order_catalog_table, 'CATALOG_CD', previous_versions[config.order_catalog_table], current_versions[config.order_catalog_table], key_is_double=True)
    catalog_events = _target_events_for_changed_catalogs(target, catalog_ids).withColumn('TRIGGER_SOURCE', F.lit('ORDER_CATALOG'))
    trigger_frames.append(catalog_events)
    triggers = reduce(lambda left, right: left.unionByName(right), trigger_frames)
    grouped = triggers.groupBy('EVENT_ID').agg(F.concat_ws(',', F.sort_array(F.collect_set('TRIGGER_SOURCE'))).alias('TRIGGER_SOURCES'))
    return grouped.join(date_meta, 'EVENT_ID', 'left').select('EVENT_ID', 'TRIGGER_SOURCES', 'DATE_RESULT_CDF_COMMIT_VERSION', 'DATE_RESULT_CDF_COMMIT_TIMESTAMP', 'DATE_RESULT_CDF_CHANGE_TYPE')

def _apply_target_properties(config: MapDateEventsConfig) -> None:
    spark.sql(f"\n        ALTER TABLE {_sql_identifier(config.target_table)}\n        SET TBLPROPERTIES (\n            'delta.enableChangeDataFeed' = 'true',\n            'delta.enableDeletionVectors' = 'true'\n        )\n    ")
    spark.sql(f"COMMENT ON TABLE {_sql_identifier(config.target_table)} IS '{_escape_sql_string(map_date_events_comment)}'")

def _ensure_target_schema(config: MapDateEventsConfig) -> None:
    current_schema = spark.table(config.target_table).schema
    current_fields = {field.name: field for field in current_schema.fields}
    additions = []
    tick = chr(96)
    for field in bronze_contract_schema(config.target_table).fields:
        if field.name not in current_fields:
            comment = _escape_sql_string(field.metadata.get('comment', ''))
            definition = f'{tick}{field.name}{tick} {field.dataType.simpleString()}'
            if comment:
                definition += f" COMMENT '{comment}'"
            additions.append(definition)
        elif current_fields[field.name].dataType != field.dataType:
            raise RuntimeError(f'Existing column {field.name} has type {current_fields[field.name].dataType.simpleString()}, expected {field.dataType.simpleString()}. Run a full rebuild to apply the schema safely.')
    if additions:
        spark.sql(f"ALTER TABLE {_sql_identifier(config.target_table)} ADD COLUMNS ({', '.join(additions)})")
    _apply_target_properties(config)

def _validate_unique_event_grain(table_name: str) -> Dict[str, int]:
    row = spark.sql(f'\n        SELECT\n            COUNT(*) AS row_count,\n            COUNT(DISTINCT EVENT_ID) AS distinct_event_ids,\n            COUNT_IF(EVENT_ID IS NULL) AS null_event_ids\n        FROM {_sql_identifier(table_name)}\n    ').first()
    metrics = {'row_count': int(row['row_count']), 'distinct_event_ids': int(row['distinct_event_ids']), 'null_event_ids': int(row['null_event_ids'])}
    if metrics['null_event_ids'] != 0:
        raise RuntimeError(f'{table_name} contains null EVENT_ID values: {metrics}')
    if metrics['row_count'] != metrics['distinct_event_ids']:
        raise RuntimeError(f'{table_name} is not unique by EVENT_ID: {metrics}')
    return metrics

def _full_rebuild(config: MapDateEventsConfig, source_versions: Dict[str, int], run_id: str, run_timestamp: datetime, safe_rebuild: bool) -> Dict[str, object]:
    print('[INFO] Building an unfiltered full snapshot. This is expected to be a large one-time operation.')
    date_rows = _latest_date_results(config, source_versions[config.date_result_table], event_keys=None).withColumn('DATE_RESULT_CDF_COMMIT_VERSION', F.lit(None).cast('long')).withColumn('DATE_RESULT_CDF_COMMIT_TIMESTAMP', F.lit(None).cast('timestamp')).withColumn('DATE_RESULT_CDF_CHANGE_TYPE', F.lit(None).cast('string')).withColumn('TRIGGER_SOURCES', F.lit('FULL_REBUILD'))
    rebuilt = _build_enriched_date_events(config, date_rows, source_versions, run_id, run_timestamp)
    rebuilt = bronze_project_contract(rebuilt, config.target_table)
    if safe_rebuild:
        suffix = run_id.replace('-', '')
        staging_table = f'{config.target_table}__rebuild_{suffix}'
        print(f'[INFO] Writing rebuild staging table {staging_table}')
        rebuilt.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(staging_table)
        metrics = _validate_unique_event_grain(staging_table)
        print(f'[INFO] Staging validation passed: {metrics}')
        spark.table(staging_table).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(config.target_table)
        spark.sql(f'DROP TABLE {_sql_identifier(staging_table)}')
    else:
        rebuilt.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(config.target_table)
        metrics = _validate_unique_event_grain(config.target_table)
    _apply_target_properties(config)
    return {'mode': 'FULL_REBUILD', **metrics}

def _merge_active_updates(config: MapDateEventsConfig, updates: DataFrame) -> Dict[str, int]:
    cached = bronze_project_contract(updates, config.target_table)
    try:
        source_rows = cached.count()
        if source_rows == 0:
            return {'active_source_rows': 0}
        assignments = {
            column_name: F.col(f's.{column_name}')
            for column_name in cached.columns
        }
        comparisons = ' OR '.join(
            f'NOT (t.`{column_name}` <=> s.`{column_name}`)'
            for column_name in cached.columns
            if column_name != 'EVENT_ID'
        )
        DeltaTable.forName(spark, config.target_table).alias('t').merge(
            cached.alias('s'),
            't.EVENT_ID <=> s.EVENT_ID',
        ).whenMatchedUpdate(
            condition=comparisons or 'false',
            set=assignments,
        ).whenNotMatchedInsert(values=assignments).execute()
        return {'active_source_rows': int(source_rows)}
    finally:
        None

def _mark_deleted_keys(config: MapDateEventsConfig, deleted_keys: DataFrame, source_versions: Dict[str, int], run_id: str, run_timestamp: datetime) -> Dict[str, int]:
    prepared = deleted_keys.withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_UPDT_DT_TM', F.lit(run_timestamp)).withColumn('DATE_RESULT_SOURCE_VERSION', F.lit(source_versions[config.date_result_table]).cast('long')).withColumn('CLINICAL_EVENT_SOURCE_VERSION', F.lit(source_versions[config.clinical_event_table]).cast('long')).withColumn('CODE_VALUE_SOURCE_VERSION', F.lit(source_versions[config.code_value_table]).cast('long')).withColumn('ORDER_CATALOG_SOURCE_VERSION', F.lit(source_versions[config.order_catalog_table]).cast('long'))
    try:
        deleted_count = prepared.count()
        if deleted_count == 0:
            return {'deleted_source_keys': 0}
        requested_updates = {
            'SOURCE_DELETED_IND': 'true',
            'SOURCE_CURRENT_IND': 'false',
            'DATE_RESULT_CDF_COMMIT_VERSION': 's.DATE_RESULT_CDF_COMMIT_VERSION',
            'DATE_RESULT_CDF_COMMIT_TIMESTAMP': 's.DATE_RESULT_CDF_COMMIT_TIMESTAMP',
            'DATE_RESULT_CDF_CHANGE_TYPE': 's.DATE_RESULT_CDF_CHANGE_TYPE',
            'TRIGGER_SOURCES': 's.TRIGGER_SOURCES',
            'DATE_RESULT_SOURCE_VERSION': 's.DATE_RESULT_SOURCE_VERSION',
            'CLINICAL_EVENT_SOURCE_VERSION': 's.CLINICAL_EVENT_SOURCE_VERSION',
            'CODE_VALUE_SOURCE_VERSION': 's.CODE_VALUE_SOURCE_VERSION',
            'ORDER_CATALOG_SOURCE_VERSION': 's.ORDER_CATALOG_SOURCE_VERSION',
            'PIPELINE_RUN_ID': 's.PIPELINE_RUN_ID',
            'PIPELINE_UPDT_DT_TM': 's.PIPELINE_UPDT_DT_TM',
        }
        allowed = bronze_contract_column_set(config.target_table)
        retained_updates = {
            column_name: expression
            for column_name, expression in requested_updates.items()
            if column_name.lower() in allowed
        }
        DeltaTable.forName(spark, config.target_table).alias('t').merge(
            prepared.alias('s'),
            't.EVENT_ID <=> s.EVENT_ID',
        ).whenMatchedUpdate(set=retained_updates).execute()
        return {'deleted_source_keys': int(deleted_count)}
    finally:
        None

def _incremental_update(config: MapDateEventsConfig, previous_versions: Dict[str, int], source_versions: Dict[str, int], run_id: str, run_timestamp: datetime) -> Dict[str, object]:
    _ensure_target_schema(config)
    changed_keys = _build_changed_event_keys(config, previous_versions, source_versions)
    try:
        changed_count = changed_keys.count()
        if changed_count == 0:
            return {'mode': 'INCREMENTAL', 'changed_event_ids': 0, 'active_source_rows': 0, 'deleted_source_keys': 0}
        date_rows = _latest_date_results(config, source_versions[config.date_result_table], event_keys=changed_keys)
        active_date_rows = date_rows.join(changed_keys, 'EVENT_ID', 'inner')
        deleted_keys = changed_keys.join(date_rows.select('EVENT_ID'), 'EVENT_ID', 'left_anti')
        updates = _build_enriched_date_events(config, active_date_rows, source_versions, run_id, run_timestamp)
        metrics = {'mode': 'INCREMENTAL', 'changed_event_ids': int(changed_count)}
        metrics.update(_merge_active_updates(config, updates))
        metrics.update(_mark_deleted_keys(config, deleted_keys, source_versions, run_id, run_timestamp))
        missing_active = active_date_rows.select('EVENT_ID').dropDuplicates().join(spark.table(config.target_table).select('EVENT_ID'), 'EVENT_ID', 'left_anti').limit(1).count()
        if missing_active:
            raise RuntimeError('At least one active changed EVENT_ID was not present after the merge')
        return metrics
    finally:
        None

def process_date_events_incremental(force_full_rebuild: bool=False, bootstrap_if_state_missing: bool=True, safe_rebuild: bool=True, config: MapDateEventsConfig=MAP_DATE_EVENTS_CONFIG) -> Dict[str, object]:
    """
    Drop-in replacement for the original process_date_events_incremental().

    Behaviour:
      * first run or missing source-version state: full unfiltered rebuild;
      * later runs: CDF by stored Delta version for every mutable upstream source;
      * no timestamp fallback;
      * no validity, status, result, or Trust filters;
      * date-result, clinical-event, parent-event, code-value, and catalog changes
        can all trigger row refreshes;
      * physical source deletion marks an existing target row as deleted;
      * state advances only after the target write succeeds.
    """
    run_id = str(uuid.uuid4())
    run_timestamp = datetime.now(timezone.utc)
    mode = 'UNKNOWN'
    source_versions: Dict[str, int] = {}
    _ensure_metadata_tables(config)
    try:
        source_versions = _capture_source_versions(config)
        previous_versions = _read_pipeline_state(config)
        missing_state = [source for source in _source_tables(config) if source not in previous_versions]
        target_missing = not _table_exists(config.target_table)
        requires_bootstrap = target_missing or bool(missing_state)
        if requires_bootstrap and (not (bootstrap_if_state_missing or force_full_rebuild)):
            raise RuntimeError(f'Pipeline state is missing for one or more sources. Run with force_full_rebuild=True or bootstrap_if_state_missing=True. Missing state: {missing_state}')
        mode = 'FULL_REBUILD' if force_full_rebuild or requires_bootstrap else 'INCREMENTAL'
        _write_audit_event(config, run_id, status='STARTED', mode=mode, message='map_date_events load started', source_versions=source_versions)
        if mode == 'FULL_REBUILD':
            metrics = _full_rebuild(config, source_versions, run_id, run_timestamp, safe_rebuild=safe_rebuild)
        else:
            for source, previous_version in previous_versions.items():
                if previous_version > source_versions[source]:
                    raise RuntimeError(f'Stored state version {previous_version} is ahead of current version {source_versions[source]} for {source}')
            metrics = _incremental_update(config, previous_versions, source_versions, run_id, run_timestamp)
        completed_at = datetime.now(timezone.utc)
        _commit_pipeline_state(config, source_versions, run_id, completed_at)
        _write_audit_event(config, run_id, status='SUCCESS', mode=mode, message='map_date_events load completed', source_versions=source_versions, metrics=metrics)
        print(f'[SUCCESS] map_date_events {mode} completed: {metrics}')
        return metrics
    except Exception as exc:
        _write_audit_event(config, run_id, status='FAILED', mode=mode, message=str(exc), source_versions=source_versions)
        print(f'[ERROR] map_date_events {mode} failed: {exc}')
        raise

def validate_map_date_events(compare_to_source: bool=False, config: MapDateEventsConfig=MAP_DATE_EVENTS_CONFIG) -> Dict[str, object]:
    """Read-only validation over the retained single-layer date contract."""
    if not _table_exists(config.target_table):
        raise RuntimeError(f'Target table does not exist: {config.target_table}')
    metrics = _validate_unique_event_grain(config.target_table)
    quality = spark.sql(f"""
        SELECT
            COUNT_IF(DATE_RESULT_ADC_UPDT IS NULL) AS null_date_result_adc_updt,
            COUNT_IF(DATE_RESULT_EFFECTIVE_UPDT_DT_TM IS NULL) AS null_effective_source_updt,
            COUNT_IF(SOURCE_DELETED_IND = TRUE) AS source_deleted_rows,
            COUNT_IF(ENCOUNTER_ID_MATCH_IND = FALSE) AS encounter_disagreements,
            COUNT_IF(EVENT_LABEL IS NULL) AS missing_event_label
        FROM {_sql_identifier(config.target_table)}
    """).first()
    metrics.update({key: int(value or 0) for key, value in quality.asDict().items()})
    if compare_to_source:
        source_version = _latest_delta_version(config.date_result_table)
        source_ids = _latest_date_results(
            config, source_version
        ).select('EVENT_ID').dropDuplicates()
        target_ids = spark.table(config.target_table).select('EVENT_ID')
        metrics['missing_source_event_ids'] = source_ids.join(
            target_ids, 'EVENT_ID', 'left_anti'
        ).count()
        metrics['target_ids_absent_from_source'] = target_ids.join(
            source_ids, 'EVENT_ID', 'left_anti'
        ).count()
    return metrics
def configure_map_date_events_cdf_retention(retention_days: int=21, config: MapDateEventsConfig=MAP_DATE_EVENTS_CONFIG) -> None:
    """
    Optional operational helper.

    Version-based state prevents silent loss, but retaining more than one normal job
    interval reduces rebuild risk after delayed or failed runs. This function is not
    called automatically because it changes shared source-table retention settings.
    """
    if retention_days < 8:
        raise ValueError('retention_days must be at least 8')
    for source in _source_tables(config):
        spark.sql(f"\n            ALTER TABLE {_sql_identifier(source)}\n            SET TBLPROPERTIES (\n                'delta.deletedFileRetentionDuration' = 'interval {int(retention_days)} days',\n                'delta.logRetentionDuration' = 'interval {_pipeline_builtins.max(int(retention_days), 30)} days'\n            )\n        ")
        print(f'[INFO] Updated CDF-supporting retention for {source}')
print('map_date_events replacement functions loaded. Call process_date_events_incremental() to run; the first state-less run performs a full rebuild.')

try:
    _pipeline_run_recoverable('map_date_events', _PIPELINE_FULL_REFRESH, lambda: process_date_events_incremental(force_full_rebuild=False), lambda: process_date_events_incremental(force_full_rebuild=True))
    _PIPELINE_UPDATED_TARGETS.extend(['4_prod.bronze.map_date_events'])
    _pipeline_mark_component_complete('map_date_events', ['4_prod.bronze.map_date_events'])
    _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_date_events'})
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

_pipeline_component_start("map_nomen_events")
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
from typing import Dict, Iterable, List, Optional, Tuple
import uuid
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType, TimestampType
MAP_NOMEN_TARGET = '4_prod.bronze.map_nomen_events'
MAP_NOMEN_CHECKPOINT = '6_mgmt.bronze.map_nomen_events_checkpoints'
MAP_NOMEN_CURRENT_VIEW = '4_prod.bronze.map_nomen_events_current'
SRC_CODED_RESULT = '4_prod.raw.mill_ce_coded_result'
SRC_CLINICAL_EVENT = '4_prod.raw.mill_clinical_event'
SRC_NOMENCLATURE = '3_lookup.mill.map_nomenclature'
SRC_CODE_VALUE = '3_lookup.mill.mill_code_value'
SRC_ORDER_CATALOG = '3_lookup.mill.mill_order_catalog'
SRC_MANUAL_MAP = '3_lookup.omop.barts_new_maps'
SRC_OMOP_CONCEPT = '3_lookup.omop.concept'
MAP_NOMEN_SOURCES = [SRC_CODED_RESULT, SRC_CLINICAL_EVENT, SRC_NOMENCLATURE, SRC_CODE_VALUE, SRC_ORDER_CATALOG, SRC_MANUAL_MAP, SRC_OMOP_CONCEPT]
MAP_NOMEN_KEY = ['EVENT_ID', 'SEQUENCE_NBR']
MAP_NOMEN_ROW_KEY = 'SOURCE_ROW_KEY'
DEFAULT_TRUST = 'Barts'
DEFAULT_RETENTION_DAYS = 30
map_nomen_events_comment = 'Source-faithful bronze foundation for Cerner nomenclature-coded clinical-event results. The grain is one deterministic latest coded-result source row per EVENT_ID and SEQUENCE_NBR. Source validity and status attributes are retained rather than filtered. Clinical-event, parent-event, nomenclature, catalog, code-value and OMOP mapping attributes are supplied with source-specific provenance and ambiguity metadata.'

class FullRebuildRequired(RuntimeError):
    pass

@dataclass
class ChangeBatch:
    table_name: str
    start_version: int
    end_version: int
    end_timestamp: object
    changes: Optional[DataFrame]

def qname(table_name: str) -> str:
    quote = chr(96)
    return '.'.join((f'{quote}{part}{quote}' for part in table_name.split('.')))

def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _nonblank(column_name: str):
    return F.nullif(F.trim(F.col(column_name).cast('string')), F.lit(''))

def _greatest_existing(df: DataFrame, columns: Iterable[str]):
    available = [F.col(c) for c in columns]
    return F.greatest(*available) if available else F.lit(None).cast('timestamp')

def _empty_event_ids() -> DataFrame:
    schema = StructType([StructField('EVENT_ID', LongType(), False)])
    return spark.createDataFrame([], schema)

def _union_event_ids(frames: Iterable[Optional[DataFrame]]) -> DataFrame:
    valid = [frame.select(F.col('EVENT_ID').cast('long').alias('EVENT_ID')) for frame in frames if frame is not None]
    if not valid:
        return _empty_event_ids()
    return reduce(lambda left, right: left.unionByName(right), valid).filter(F.col('EVENT_ID').isNotNull()).distinct()

def _snapshot_table(table_name: str, versions: Optional[Dict[str, int]]=None) -> DataFrame:
    version = versions.get(table_name) if versions else None
    if version is not None:
        return _m30_hardening_read_pinned_snapshot(table_name, int(version))
    return spark.table(table_name)

def _latest_table_version(table_name: str) -> Tuple[int, object]:
    row = spark.sql(f'DESCRIBE HISTORY {qname(table_name)} LIMIT 1').select('version', 'timestamp').first()
    if row is None:
        raise RuntimeError(f'No Delta history found for {table_name}')
    return (int(row['version']), row['timestamp'])

def capture_source_versions() -> Dict[str, int]:
    return {table: _latest_table_version(table)[0] for table in MAP_NOMEN_SOURCES}

def _has_cdf(table_name: str) -> bool:
    detail = spark.sql(f'DESCRIBE DETAIL {qname(table_name)}').first()
    properties = detail['properties'] or {}
    return str(properties.get('delta.enableChangeDataFeed', 'false')).lower() == 'true'

def configure_cdf_retention(retention_days: int=DEFAULT_RETENTION_DAYS, apply: bool=False) -> None:
    statements = []
    for table_name in MAP_NOMEN_SOURCES + [MAP_NOMEN_TARGET]:
        if table_exists(table_name):
            statements.append(f"ALTER TABLE {qname(table_name)} SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval {int(retention_days)} days', 'delta.logRetentionDuration' = 'interval {int(retention_days)} days')")
    if not apply:
        print('[DRY RUN] CDF retention statements:')
        for statement in statements:
            print(statement)
        return
    for statement in statements:
        try:
            spark.sql(statement)
        except Exception as error:
            print(f'[WARN] Could not update CDF retention with statement {statement}: {error}')

def ensure_checkpoint_table() -> None:
    spark.sql(f"\n        CREATE TABLE IF NOT EXISTS {qname(MAP_NOMEN_CHECKPOINT)} (\n            source_table STRING NOT NULL,\n            last_version BIGINT NOT NULL,\n            last_commit_timestamp TIMESTAMP,\n            target_version BIGINT,\n            pipeline_run_id STRING,\n            updated_at TIMESTAMP NOT NULL\n        )\n        USING DELTA\n        TBLPROPERTIES (\n            'delta.enableChangeDataFeed' = 'true',\n            'delta.deletedFileRetentionDuration' = 'interval {DEFAULT_RETENTION_DAYS} days'\n        )\n        ")

def _checkpoint_versions() -> Dict[str, int]:
    if not table_exists(MAP_NOMEN_CHECKPOINT):
        return {}
    return {row['source_table']: int(row['last_version']) for row in spark.table(MAP_NOMEN_CHECKPOINT).select('source_table', 'last_version').collect()}

def write_checkpoints(versions: Dict[str, int], run_id: str) -> None:
    ensure_checkpoint_table()
    target_version = _latest_table_version(MAP_NOMEN_TARGET)[0]
    rows = []
    for table_name, version in versions.items():
        history_row = spark.sql(f'DESCRIBE HISTORY {qname(table_name)}').filter(F.col('version') == int(version)).select('timestamp').first()
        rows.append((table_name, int(version), history_row['timestamp'] if history_row else None, int(target_version), run_id, datetime.now(timezone.utc).replace(tzinfo=None)))
    schema = StructType([StructField('source_table', StringType(), False), StructField('last_version', LongType(), False), StructField('last_commit_timestamp', TimestampType(), True), StructField('target_version', LongType(), True), StructField('pipeline_run_id', StringType(), True), StructField('updated_at', TimestampType(), False)])
    updates = spark.createDataFrame(rows, schema)
    DeltaTable.forName(spark, MAP_NOMEN_CHECKPOINT).alias('t').merge(updates.alias('s'), 't.source_table = s.source_table').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def read_change_batches(end_versions: Dict[str, int]) -> Dict[str, ChangeBatch]:
    checkpoints = _checkpoint_versions()
    missing = [table for table in MAP_NOMEN_SOURCES if table not in checkpoints]
    if missing:
        raise FullRebuildRequired('No checkpoint exists for: ' + ', '.join(missing))
    batches: Dict[str, ChangeBatch] = {}
    for table_name in MAP_NOMEN_SOURCES:
        if not _has_cdf(table_name):
            raise FullRebuildRequired(f'CDF is not enabled for {table_name}')
        start_version = int(checkpoints[table_name]) + 1
        end_version = int(end_versions[table_name])
        end_timestamp = _latest_table_version(table_name)[1]
        if start_version > end_version:
            batches[table_name] = ChangeBatch(table_name, start_version, end_version, end_timestamp, None)
            continue
        try:
            changes = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', start_version).option('endingVersion', end_version).table(table_name)
            changes = _m30_hardening_validate_cdf(changes)
        except Exception as error:
            raise FullRebuildRequired(f'CDF could not be read for {table_name} versions {start_version}-{end_version}: {error}') from error
        batches[table_name] = ChangeBatch(table_name, start_version, end_version, end_timestamp, changes)
    return batches

def create_nomen_code_lookup(code_values: DataFrame, alias_suffix: str) -> DataFrame:
    """Drop-in replacement: DISPLAY is now a true display with DESCRIPTION fallback."""
    return code_values.select(F.col('CODE_VALUE').cast('long').alias('CODE_VALUE'), F.coalesce(F.nullif(F.trim(F.col('DISPLAY')), F.lit('')), F.nullif(F.trim(F.col('DESCRIPTION')), F.lit(''))).alias(f'{alias_suffix}_desc'), F.col('DESCRIPTION').alias(f'{alias_suffix}_description'), F.col('CDF_MEANING').alias(f'{alias_suffix}_cdf_meaning'), F.col('ACTIVE_IND').cast('long').alias(f'{alias_suffix}_active_ind'), F.col('ADC_UPDT').alias(f'{alias_suffix}_adc_updt')).alias(alias_suffix)

def _join_code_lookup(df: DataFrame, code_values: DataFrame, code_column: str, output_stem: str) -> DataFrame:
    suffix = 'lkp_' + output_stem.lower()
    prepared = create_nomen_code_lookup(code_values, suffix)
    lookup = F.broadcast(prepared.select(F.col('CODE_VALUE').alias(f'{suffix}_code_value'), F.col(f'{suffix}_desc'), F.col(f'{suffix}_description'), F.col(f'{suffix}_cdf_meaning'), F.col(f'{suffix}_active_ind'), F.col(f'{suffix}_adc_updt')))
    result = df.join(lookup, F.col(code_column).cast('long') == F.col(f'{suffix}_code_value'), 'left')
    return result.withColumn(f'{output_stem}_DISPLAY', F.col(f'{suffix}_desc')).withColumn(f'{output_stem}_DESCRIPTION', F.col(f'{suffix}_description')).withColumn(f'{output_stem}_CDF_MEANING', F.col(f'{suffix}_cdf_meaning')).withColumn(f'{output_stem}_ACTIVE_IND', F.col(f'{suffix}_active_ind')).withColumn(f'{output_stem}_LOOKUP_ADC_UPDT', F.col(f'{suffix}_adc_updt')).drop(f'{suffix}_code_value', f'{suffix}_desc', f'{suffix}_description', f'{suffix}_cdf_meaning', f'{suffix}_active_ind', f'{suffix}_adc_updt')

def _manual_rule_base(barts_mapfile: DataFrame, omop_fields: List[str]) -> DataFrame:
    return barts_mapfile.filter((F.col('SourceTable') == 'dbo.PI_CDE_CLINICAL_EVENT') & F.col('SourceField').isin('EVENT_CD', 'EVENT_RESULT_TXT') & F.col('OMOPField').isin(*omop_fields)).select('OMOPTable', 'OMOPField', 'SourceField', 'SourceValue', 'OMOPConceptId', 'EVENT_CD', 'EVENT_CLASS_CD', 'EVENT_RESULT_TXT', 'STANDARD_CONCEPT', 'ADC_UPDT').withColumn('MAP_CONCEPT_ID', F.expr('try_cast(OMOPConceptId as int)')).withColumn('DIRECT_EVENT_CD', F.when(F.col('SourceField') == 'EVENT_CD', F.expr('try_cast(SourceValue as bigint)'))).withColumn('DIRECT_RESULT_TXT', F.when(F.col('SourceField') == 'EVENT_RESULT_TXT', F.nullif(F.trim(F.col('SourceValue')), F.lit('')))).withColumn('CONTEXT_EVENT_CD', F.expr('try_cast(EVENT_CD as bigint)')).withColumn('CONTEXT_EVENT_CLASS_CD', F.expr('try_cast(EVENT_CLASS_CD as bigint)')).withColumn('CONTEXT_RESULT_TXT', F.nullif(F.trim(F.col('EVENT_RESULT_TXT')), F.lit(''))).withColumn('RULE_SPECIFICITY', F.when(F.col('CONTEXT_EVENT_CD').isNotNull(), 4).otherwise(0) + F.when(F.col('CONTEXT_EVENT_CLASS_CD').isNotNull(), 2).otherwise(0) + F.when(F.col('CONTEXT_RESULT_TXT').isNotNull(), 1).otherwise(0)).withColumn('RULE_KEY', F.sha2(F.concat_ws('||', F.coalesce(F.col('OMOPTable'), F.lit('<NULL>')), F.coalesce(F.col('OMOPField'), F.lit('<NULL>')), F.coalesce(F.col('SourceField'), F.lit('<NULL>')), F.coalesce(F.col('SourceValue'), F.lit('<NULL>')), F.coalesce(F.col('OMOPConceptId'), F.lit('<NULL>')), F.coalesce(F.col('EVENT_CD'), F.lit('<NULL>')), F.coalesce(F.col('EVENT_CLASS_CD'), F.lit('<NULL>')), F.coalesce(F.col('EVENT_RESULT_TXT'), F.lit('<NULL>'))), 256)).filter(F.col('MAP_CONCEPT_ID').isNotNull())

def _manual_mapping_candidates(inputs: DataFrame, rules: DataFrame) -> DataFrame:
    event_rules = F.broadcast(rules.filter(F.col('SourceField') == 'EVENT_CD')).alias('r')
    result_rules = F.broadcast(rules.filter(F.col('SourceField') == 'EVENT_RESULT_TXT')).alias('r')
    source = inputs.alias('i')
    context_condition = (F.col('r.CONTEXT_EVENT_CD').isNull() | (F.col('i.EVENT_CD') == F.col('r.CONTEXT_EVENT_CD'))) & (F.col('r.CONTEXT_EVENT_CLASS_CD').isNull() | (F.col('i.EVENT_CLASS_CD') == F.col('r.CONTEXT_EVENT_CLASS_CD'))) & (F.col('r.CONTEXT_RESULT_TXT').isNull() | (F.col('i.SOURCE_STRING_CLEAN') == F.col('r.CONTEXT_RESULT_TXT')))
    candidate_columns = [F.col('i.SOURCE_ROW_KEY'), F.col('r.MAP_CONCEPT_ID'), F.col('r.OMOPTable'), F.col('r.OMOPField'), F.col('r.SourceField'), F.col('r.SourceValue'), F.col('r.STANDARD_CONCEPT').alias('MAP_STANDARD_CONCEPT'), F.col('r.RULE_SPECIFICITY'), F.col('r.RULE_KEY'), F.col('r.ADC_UPDT').alias('MAP_ADC_UPDT')]
    by_event = source.join(event_rules, F.col('i.EVENT_CD') == F.col('r.DIRECT_EVENT_CD'), 'inner').filter(context_condition).select(*candidate_columns)
    by_result = source.join(result_rules, F.col('i.SOURCE_STRING_CLEAN') == F.col('r.DIRECT_RESULT_TXT'), 'inner').filter(context_condition).select(*candidate_columns)
    return by_event.unionByName(by_result)

def _domain_priority(column_name: str):
    return F.when(F.col(column_name) == 'Drug', 1).when(F.col(column_name) == 'Measurement', 2).when(F.col(column_name) == 'Procedure', 3).when(F.col(column_name) == 'Condition', 4).when(F.col(column_name) == 'Device', 5).when(F.col(column_name) == 'Observation', 6).otherwise(999)

def _best_manual_mapping(inputs: DataFrame, rules: DataFrame, concepts: DataFrame, prefix: str) -> DataFrame:
    candidates = _manual_mapping_candidates(inputs, rules)
    concept_details = concepts.filter(F.col('invalid_reason').isNull()).select(F.col('concept_id').alias('DETAIL_CONCEPT_ID'), F.col('concept_name').alias('DETAIL_CONCEPT_NAME'), F.col('standard_concept').alias('DETAIL_STANDARD_CONCEPT'), F.col('domain_id').alias('DETAIL_DOMAIN'), F.col('concept_class_id').alias('DETAIL_CONCEPT_CLASS'), F.col('valid_start_date').alias('DETAIL_VALID_START_DATE'), F.col('valid_end_date').alias('DETAIL_VALID_END_DATE'))
    candidates = candidates.join(concept_details, F.col('MAP_CONCEPT_ID') == F.col('DETAIL_CONCEPT_ID'), 'left')
    stats = candidates.groupBy('SOURCE_ROW_KEY').agg(F.countDistinct('MAP_CONCEPT_ID').alias(f'{prefix}_MATCH_COUNT'), F.countDistinct('RULE_KEY').alias(f'{prefix}_RULE_COUNT'), F.sort_array(F.collect_set('MAP_CONCEPT_ID')).alias(f'{prefix}_CANDIDATE_CONCEPT_IDS'), F.sort_array(F.collect_set('RULE_KEY')).alias(f'{prefix}_CANDIDATE_RULE_KEYS'))
    ranking = Window.partitionBy('SOURCE_ROW_KEY').orderBy(F.col('RULE_SPECIFICITY').desc(), F.when(F.col('DETAIL_STANDARD_CONCEPT') == 'S', 0).when(F.col('DETAIL_STANDARD_CONCEPT') == 'C', 1).otherwise(2).asc(), _domain_priority('DETAIL_DOMAIN').asc(), F.col('MAP_CONCEPT_ID').asc(), F.col('RULE_KEY').asc())
    selected = candidates.withColumn('_mapping_rank', F.row_number().over(ranking)).filter(F.col('_mapping_rank') == 1).drop('_mapping_rank').join(stats, 'SOURCE_ROW_KEY', 'left')
    return selected.select('SOURCE_ROW_KEY', F.col('MAP_CONCEPT_ID').alias(f'{prefix}_CONCEPT_ID'), F.col('DETAIL_CONCEPT_NAME').alias(f'{prefix}_CONCEPT_NAME'), F.col('DETAIL_STANDARD_CONCEPT').alias(f'{prefix}_STANDARD_CONCEPT'), F.col('DETAIL_DOMAIN').alias(f'{prefix}_CONCEPT_DOMAIN'), F.col('DETAIL_CONCEPT_CLASS').alias(f'{prefix}_CONCEPT_CLASS'), F.col('DETAIL_VALID_START_DATE').alias(f'{prefix}_VALID_START_DATE'), F.col('DETAIL_VALID_END_DATE').alias(f'{prefix}_VALID_END_DATE'), F.col('OMOPTable').alias(f'{prefix}_TABLE'), F.col('OMOPField').alias(f'{prefix}_COLUMN'), F.col('SourceField').alias(f'{prefix}_SOURCE_FIELD'), F.col('SourceValue').alias(f'{prefix}_SOURCE_VALUE'), F.col('RULE_KEY').alias(f'{prefix}_RULE_KEY'), F.col('RULE_SPECIFICITY').alias(f'{prefix}_RULE_SPECIFICITY'), F.col('MAP_ADC_UPDT').alias(f'{prefix}_MAP_ADC_UPDT'), F.col(f'{prefix}_MATCH_COUNT'), F.col(f'{prefix}_RULE_COUNT'), F.col(f'{prefix}_CANDIDATE_CONCEPT_IDS'), F.col(f'{prefix}_CANDIDATE_RULE_KEYS'), (F.col(f'{prefix}_MATCH_COUNT') > 1).alias(f'{prefix}_AMBIGUOUS_IND'))

def add_manual_omop_mappings_nomen(df: DataFrame, barts_mapfile: DataFrame, concepts: DataFrame) -> DataFrame:
    """Drop-in replacement that preserves input grain and exposes ambiguity/provenance."""
    inputs = df.select('SOURCE_ROW_KEY', F.col('EVENT_CD').cast('long').alias('EVENT_CD'), F.col('EVENT_CLASS_CD').cast('long').alias('EVENT_CLASS_CD'), F.nullif(F.trim(F.col('SOURCE_STRING')), F.lit('')).alias('SOURCE_STRING_CLEAN'))
    concept_rules = _manual_rule_base(barts_mapfile, ['measurement_concept_id', 'observation_concept_id'])
    value_rules = _manual_rule_base(barts_mapfile, ['value_as_concept_id'])
    selected_concepts = _best_manual_mapping(inputs, concept_rules, concepts, 'OMOP_MANUAL')
    selected_values = _best_manual_mapping(inputs, value_rules, concepts, 'OMOP_MANUAL_VALUE')
    result = df.join(selected_concepts, 'SOURCE_ROW_KEY', 'left').join(selected_values, 'SOURCE_ROW_KEY', 'left').withColumn('OMOP_MANUAL_CONCEPT', F.col('OMOP_MANUAL_CONCEPT_ID').cast('string')).withColumn('OMOP_MANUAL_VALUE_CONCEPT', F.col('OMOP_MANUAL_VALUE_CONCEPT_ID').cast('string')).withColumn('OMOP_MANUAL_MATCH_COUNT', F.coalesce(F.col('OMOP_MANUAL_MATCH_COUNT'), F.lit(0)).cast('long')).withColumn('OMOP_MANUAL_RULE_COUNT', F.coalesce(F.col('OMOP_MANUAL_RULE_COUNT'), F.lit(0)).cast('long')).withColumn('OMOP_MANUAL_AMBIGUOUS_IND', F.coalesce(F.col('OMOP_MANUAL_AMBIGUOUS_IND'), F.lit(False))).withColumn('OMOP_MANUAL_VALUE_MATCH_COUNT', F.coalesce(F.col('OMOP_MANUAL_VALUE_MATCH_COUNT'), F.lit(0)).cast('long')).withColumn('OMOP_MANUAL_VALUE_RULE_COUNT', F.coalesce(F.col('OMOP_MANUAL_VALUE_RULE_COUNT'), F.lit(0)).cast('long')).withColumn('OMOP_MANUAL_VALUE_AMBIGUOUS_IND', F.coalesce(F.col('OMOP_MANUAL_VALUE_AMBIGUOUS_IND'), F.lit(False))).withColumn('OMOP_MANUAL_CANDIDATE_CONCEPT_IDS', F.coalesce(F.col('OMOP_MANUAL_CANDIDATE_CONCEPT_IDS'), F.expr('CAST(array() AS ARRAY<INT>)'))).withColumn('OMOP_MANUAL_CANDIDATE_RULE_KEYS', F.coalesce(F.col('OMOP_MANUAL_CANDIDATE_RULE_KEYS'), F.expr('CAST(array() AS ARRAY<STRING>)'))).withColumn('OMOP_MANUAL_VALUE_CANDIDATE_CONCEPT_IDS', F.coalesce(F.col('OMOP_MANUAL_VALUE_CANDIDATE_CONCEPT_IDS'), F.expr('CAST(array() AS ARRAY<INT>)'))).withColumn('OMOP_MANUAL_VALUE_CANDIDATE_RULE_KEYS', F.coalesce(F.col('OMOP_MANUAL_VALUE_CANDIDATE_RULE_KEYS'), F.expr('CAST(array() AS ARRAY<STRING>)')))
    return result

def _coded_results(versions: Dict[str, int], event_ids: Optional[DataFrame], trust: str) -> DataFrame:
    source = _snapshot_table(SRC_CODED_RESULT, versions).filter(F.col('Trust') == trust)
    source = source.withColumn('EVENT_ID', F.col('EVENT_ID').cast('long')).withColumn('SEQUENCE_NBR', F.col('SEQUENCE_NBR').cast('long'))
    if event_ids is not None:
        source = source.alias('cr').join(event_ids.alias('scope'), F.col('cr.EVENT_ID') == F.col('scope.EVENT_ID'), 'left_semi')
    source = source.withColumn('_CURRENT_SOURCE_ROW', F.when((F.col('VALID_FROM_DT_TM') <= F.current_timestamp()) & (F.col('VALID_UNTIL_DT_TM') > F.current_timestamp()), 1).otherwise(0))
    source_window = Window.partitionBy('EVENT_ID', 'SEQUENCE_NBR').orderBy(F.col('_CURRENT_SOURCE_ROW').desc(), F.col('VALID_FROM_DT_TM').desc_nulls_last(), F.col('VALID_UNTIL_DT_TM').desc_nulls_last(), F.col('ADC_UPDT').desc_nulls_last(), F.col('UPDT_DT_TM').desc_nulls_last(), F.col('UPDT_CNT').desc_nulls_last(), F.col('NOMENCLATURE_ID').desc_nulls_last())
    source = source.withColumn('_SOURCE_ROW_NUMBER', F.row_number().over(source_window)).filter(F.col('_SOURCE_ROW_NUMBER') == 1).drop('_SOURCE_ROW_NUMBER', '_CURRENT_SOURCE_ROW').filter(F.col('NOMENCLATURE_ID').isNotNull() & (F.col('NOMENCLATURE_ID') != 0))
    result = source.select(F.col('EVENT_ID').cast('long').alias('EVENT_ID'), F.col('SEQUENCE_NBR').cast('long').alias('SEQUENCE_NBR'), F.col('NOMENCLATURE_ID').cast('long').alias('NOMENCLATURE_ID'), F.col('ENCNTR_ID').cast('long').alias('CR_ENCNTR_ID'), F.col('ORGANIZATION_ID').cast('long').alias('CR_ORGANIZATION_ID'), F.col('GROUP_NBR').cast('long').alias('GROUP_NBR'), F.col('DESCRIPTOR').alias('DESCRIPTOR'), F.col('ACR_CODE_STR').alias('ACR_CODE_STR'), F.col('RESULT_CD').cast('long').alias('CR_RESULT_CD'), F.col('RESULT_SET').alias('CR_RESULT_SET'), F.col('PATHOLOGY_STR').alias('PATHOLOGY_STR'), F.col('PROC_CODE_STR').alias('PROC_CODE_STR'), F.col('UNIT_OF_MEASURE_CD').cast('long').alias('CR_UNIT_OF_MEASURE_CD'), F.col('VALID_FROM_DT_TM').alias('CR_VALID_FROM_DT_TM'), F.col('VALID_UNTIL_DT_TM').alias('CR_VALID_UNTIL_DT_TM'), F.col('UPDT_DT_TM').alias('CR_UPDT_DT_TM'), F.col('UPDT_ID').cast('long').alias('CR_UPDT_ID'), F.col('UPDT_TASK').cast('long').alias('CR_UPDT_TASK'), F.col('UPDT_CNT').cast('long').alias('CR_UPDT_CNT'), F.col('UPDT_APPLCTX').cast('long').alias('CR_UPDT_APPLCTX'), F.col('LAST_UTC_TS').alias('CR_LAST_UTC_TS'), F.col('INST_ID').cast('long').alias('CR_INST_ID'), F.col('TXN_ID_TEXT').alias('CR_TXN_ID_TEXT'), F.col('ADC_UPDT').alias('CR_ADC_UPDT'), F.col('Trust').alias('TRUST'))
    return result.withColumn('SOURCE_ROW_KEY', F.sha2(F.concat_ws('||', F.coalesce(F.col('EVENT_ID').cast('string'), F.lit('<NULL>')), F.coalesce(F.col('SEQUENCE_NBR').cast('string'), F.lit('<NULL>'))), 256))

def _clinical_events(versions: Dict[str, int], event_ids: DataFrame, trust: str, prefix: str) -> DataFrame:
    ce = _snapshot_table(SRC_CLINICAL_EVENT, versions).filter(F.col('Trust') == trust)
    ce = ce.alias('ce').join(event_ids.alias('scope'), F.col('ce.EVENT_ID').cast('long') == F.col('scope.EVENT_ID'), 'left_semi')
    p = prefix
    return ce.select(F.col('EVENT_ID').cast('long').alias(f'{p}EVENT_ID'), F.col('CLINICAL_EVENT_ID').cast('long').alias(f'{p}CLINICAL_EVENT_ID'), F.col('ENCNTR_ID').cast('long').alias(f'{p}ENCNTR_ID'), F.col('PERSON_ID').cast('long').alias(f'{p}PERSON_ID'), F.col('ORDER_ID').cast('long').alias(f'{p}ORDER_ID'), F.col('ENCNTR_FINANCIAL_ID').cast('long').alias(f'{p}ENCNTR_FINANCIAL_ID'), F.col('EVENT_START_DT_TM').alias(f'{p}EVENT_START_DT_TM'), F.col('EVENT_END_DT_TM').alias(f'{p}EVENT_END_DT_TM'), F.col('VALID_FROM_DT_TM').alias(f'{p}VALID_FROM_DT_TM'), F.col('VALID_UNTIL_DT_TM').alias(f'{p}VALID_UNTIL_DT_TM'), F.col('EVENT_TITLE_TEXT').alias(f'{p}EVENT_TITLE_TEXT'), F.col('VIEW_LEVEL').cast('long').alias(f'{p}VIEW_LEVEL'), F.col('CATALOG_CD').cast('long').alias(f'{p}CATALOG_CD'), F.col('SERIES_REF_NBR').alias(f'{p}SERIES_REF_NBR'), F.col('ACCESSION_NBR').alias(f'{p}ACCESSION_NBR'), F.col('CONTRIBUTOR_SYSTEM_CD').cast('long').alias(f'{p}CONTRIBUTOR_SYSTEM_CD'), F.col('REFERENCE_NBR').alias(f'{p}REFERENCE_NBR'), F.col('PARENT_EVENT_ID').cast('long').alias(f'{p}PARENT_EVENT_ID'), F.col('EVENT_RELTN_CD').cast('long').alias(f'{p}EVENT_RELTN_CD'), F.col('EVENT_CLASS_CD').cast('long').alias(f'{p}EVENT_CLASS_CD'), F.col('EVENT_CD').cast('long').alias(f'{p}EVENT_CD'), F.col('EVENT_TAG').alias(f'{p}EVENT_TAG'), F.col('RESULT_VAL').alias(f'{p}RESULT_VAL'), F.col('RECORD_STATUS_CD').cast('long').alias(f'{p}RECORD_STATUS_CD'), F.col('RESULT_STATUS_CD').cast('long').alias(f'{p}RESULT_STATUS_CD'), F.col('AUTHENTIC_FLAG').cast('long').alias(f'{p}AUTHENTIC_FLAG'), F.col('PUBLISH_FLAG').cast('long').alias(f'{p}PUBLISH_FLAG'), F.col('NORMALCY_CD').cast('long').alias(f'{p}NORMALCY_CD'), F.col('NORMALCY_METHOD_CD').cast('long').alias(f'{p}NORMALCY_METHOD_CD'), F.col('VERIFIED_DT_TM').alias(f'{p}VERIFIED_DT_TM'), F.col('VERIFIED_PRSNL_ID').cast('long').alias(f'{p}VERIFIED_PRSNL_ID'), F.col('PERFORMED_DT_TM').alias(f'{p}PERFORMED_DT_TM'), F.col('PERFORMED_PRSNL_ID').cast('long').alias(f'{p}PERFORMED_PRSNL_ID'), F.col('CLINSIG_UPDT_DT_TM').alias(f'{p}CLINSIG_UPDT_DT_TM'), F.col('ENTRY_MODE_CD').cast('long').alias(f'{p}ENTRY_MODE_CD'), F.col('SOURCE_CD').cast('long').alias(f'{p}SOURCE_CD'), F.col('CLINICAL_SEQ').alias(f'{p}CLINICAL_SEQ'), F.col('SRC_EVENT_ID').cast('long').alias(f'{p}SRC_EVENT_ID'), F.col('NOMEN_STRING_FLAG').cast('long').alias(f'{p}NOMEN_STRING_FLAG'), F.col('DEVICE_FREE_TXT').alias(f'{p}DEVICE_FREE_TXT'), F.col('UPDT_DT_TM').alias(f'{p}UPDT_DT_TM'), F.col('UPDT_ID').cast('long').alias(f'{p}UPDT_ID'), F.col('UPDT_TASK').cast('long').alias(f'{p}UPDT_TASK'), F.col('UPDT_CNT').cast('long').alias(f'{p}UPDT_CNT'), F.col('ADC_UPDT').alias(f'{p}ADC_UPDT'), F.col('ORGANIZATION_ID').cast('long').alias(f'{p}ORGANIZATION_ID'))

def _align_child_clinical_event(coded: DataFrame, versions: Dict[str, int], trust: str) -> DataFrame:
    """Attach the child clinical-event context to each coded-result row.

    The validity window is a ranking preference, not a join predicate. As a join predicate it
    produced a LEFT-join miss whenever the coded-result anchor timestamp fell outside every
    clinical-event validity window for that EVENT_ID; rank 1 then kept the single all-null CE_*
    row the LEFT join manufactured, which is where the all-null clinical-event context came
    from. Every clinical-event row for the EVENT_ID now takes part in the ranking, ordered so
    that in-window rows win first: rows that matched before are selected exactly as before, and
    rows with no in-window candidate fall back to the best current clinical-event row for that
    EVENT_ID instead of nulls. _clinical_events() applies no validity filter, so those fallback
    candidates are available.

    Exactly one row per SOURCE_ROW_KEY is still emitted, which validate_map_nomen_build()
    requires. CE_CONTEXT_MATCH_MODE records which branch supplied the context:
    VALIDITY_WINDOW, FALLBACK_LATEST or NO_CANDIDATE.
    """
    event_ids = coded.select('EVENT_ID').distinct()
    ce = _clinical_events(versions, event_ids, trust, 'CE_')
    anchor = F.coalesce(F.col('cr.CR_VALID_FROM_DT_TM'), F.col('cr.CR_UPDT_DT_TM'), F.col('cr.CR_ADC_UPDT'))
    in_validity_window = anchor.isNull() | (anchor >= F.coalesce(F.col('ce.CE_VALID_FROM_DT_TM'), F.lit('1900-01-01').cast('timestamp'))) & (anchor < F.coalesce(F.col('ce.CE_VALID_UNTIL_DT_TM'), F.lit('9999-12-31').cast('timestamp')))
    condition = F.col('cr.EVENT_ID') == F.col('ce.CE_EVENT_ID')
    joined = coded.alias('cr').join(ce.alias('ce'), condition, 'left').withColumn('_ce_in_validity_window', F.when(F.col('ce.CE_CLINICAL_EVENT_ID').isNull(), F.lit(False)).otherwise(F.coalesce(in_validity_window, F.lit(False))))
    ranking = Window.partitionBy('SOURCE_ROW_KEY').orderBy(F.col('_ce_in_validity_window').desc_nulls_last(), F.col('CE_VALID_FROM_DT_TM').desc_nulls_last(), F.col('CE_UPDT_CNT').desc_nulls_last(), F.col('CE_ADC_UPDT').desc_nulls_last(), F.col('CE_CLINICAL_EVENT_ID').desc_nulls_last())
    selected = joined.withColumn('_ce_rank', F.row_number().over(ranking)).filter(F.col('_ce_rank') == 1)
    return selected.withColumn('CE_CONTEXT_MATCH_MODE', F.when(F.col('CE_CLINICAL_EVENT_ID').isNull(), F.lit('NO_CANDIDATE')).when(F.col('_ce_in_validity_window'), F.lit('VALIDITY_WINDOW')).otherwise(F.lit('FALLBACK_LATEST'))).drop('_ce_rank', '_ce_in_validity_window', 'CE_EVENT_ID')

def _align_parent_clinical_event(child: DataFrame, versions: Dict[str, int], trust: str) -> DataFrame:
    """Attach the parent clinical-event context to each coded-result row.

    Identical defect and identical fix to _align_child_clinical_event(): the validity window
    is a ranking preference rather than a join predicate, so a parent event whose validity
    windows do not span the anchor timestamp no longer yields an all-null PE_* row. One row
    per SOURCE_ROW_KEY is still emitted. PE_CONTEXT_MATCH_MODE records which branch supplied
    the context: VALIDITY_WINDOW, FALLBACK_LATEST or NO_CANDIDATE. Rows with no parent event
    at all report NO_CANDIDATE, because there is no parent row to attach.
    """
    parent_ids = child.select(F.col('CE_PARENT_EVENT_ID').alias('EVENT_ID')).filter(F.col('EVENT_ID').isNotNull()).distinct()
    pe = _clinical_events(versions, parent_ids, trust, 'PE_')
    anchor = F.coalesce(F.col('c.CE_VALID_FROM_DT_TM'), F.col('c.CR_VALID_FROM_DT_TM'), F.col('c.CR_UPDT_DT_TM'))
    in_validity_window = anchor.isNull() | (anchor >= F.coalesce(F.col('pe.PE_VALID_FROM_DT_TM'), F.lit('1900-01-01').cast('timestamp'))) & (anchor < F.coalesce(F.col('pe.PE_VALID_UNTIL_DT_TM'), F.lit('9999-12-31').cast('timestamp')))
    condition = F.col('c.CE_PARENT_EVENT_ID') == F.col('pe.PE_EVENT_ID')
    joined = child.alias('c').join(pe.alias('pe'), condition, 'left').withColumn('_pe_in_validity_window', F.when(F.col('pe.PE_CLINICAL_EVENT_ID').isNull(), F.lit(False)).otherwise(F.coalesce(in_validity_window, F.lit(False))))
    ranking = Window.partitionBy('SOURCE_ROW_KEY').orderBy(F.col('_pe_in_validity_window').desc_nulls_last(), F.col('PE_VALID_FROM_DT_TM').desc_nulls_last(), F.col('PE_UPDT_CNT').desc_nulls_last(), F.col('PE_ADC_UPDT').desc_nulls_last(), F.col('PE_CLINICAL_EVENT_ID').desc_nulls_last())
    selected = joined.withColumn('_pe_rank', F.row_number().over(ranking)).filter(F.col('_pe_rank') == 1)
    return selected.withColumn('PE_CONTEXT_MATCH_MODE', F.when(F.col('PE_CLINICAL_EVENT_ID').isNull(), F.lit('NO_CANDIDATE')).when(F.col('_pe_in_validity_window'), F.lit('VALIDITY_WINDOW')).otherwise(F.lit('FALLBACK_LATEST'))).drop('_pe_rank', '_pe_in_validity_window', 'PE_EVENT_ID')

def _join_catalogs(df: DataFrame, versions: Dict[str, int]) -> DataFrame:
    base = _snapshot_table(SRC_ORDER_CATALOG, versions)
    child = F.broadcast(base.select(F.col('CATALOG_CD').cast('long').alias('OC_CATALOG_CD'), F.col('CATALOG_TYPE_CD').cast('long').alias('OC_CATALOG_TYPE_CD'), F.col('DESCRIPTION').alias('OC_DESCRIPTION'), F.col('PRIMARY_MNEMONIC').alias('OC_PRIMARY_MNEMONIC'), F.col('DEPT_DISPLAY_NAME').alias('OC_DEPT_DISPLAY_NAME'), F.col('ACTIVITY_TYPE_CD').cast('long').alias('OC_ACTIVITY_TYPE_CD'), F.col('ACTIVITY_SUBTYPE_CD').cast('long').alias('OC_ACTIVITY_SUBTYPE_CD'), F.col('ACTIVE_IND').cast('long').alias('OC_ACTIVE_IND'), F.col('SOURCE_VOCAB_MEAN').alias('OC_SOURCE_VOCAB_MEAN'), F.col('SOURCE_VOCAB_IDENT').alias('OC_SOURCE_VOCAB_IDENT'), F.col('CONCEPT_CKI').alias('OC_CONCEPT_CKI'), F.col('ADC_UPDT').alias('OC_ADC_UPDT')))
    parent = child.select(*[F.col(c).alias(c.replace('OC_', 'POC_', 1)) for c in child.columns])
    return df.join(child, F.col('CE_CATALOG_CD') == F.col('OC_CATALOG_CD'), 'left').join(parent, F.col('PE_CATALOG_CD') == F.col('POC_CATALOG_CD'), 'left')

def _join_nomenclature(df: DataFrame, versions: Dict[str, int]) -> DataFrame:
    nom = _snapshot_table(SRC_NOMENCLATURE, versions).select(F.col('NOMENCLATURE_ID').cast('long').alias('NOM_NOMENCLATURE_ID'), 'SOURCE_IDENTIFIER', 'SOURCE_STRING', 'SOURCE_VOCABULARY_CD', 'VOCAB_AXIS_CD', F.col('IS_STANDARD_OMOP_CONCEPT').alias('OMOP_STANDARD_CONCEPT'), F.col('CONCEPT_DOMAIN').alias('OMOP_CONCEPT_DOMAIN'), F.col('CONCEPT_CLASS').alias('OMOP_CONCEPT_CLASS'), 'FOUND_CUI', 'CONCEPT_CKI', F.col('ADC_UPDT').alias('NOMENCLATURE_ADC_UPDT'), 'SNOMED_CODE', 'SNOMED_TYPE', F.col('SNOMED_MATCH_COUNT').cast('long').alias('SNOMED_MATCH_NUMBER'), 'SNOMED_TERM', 'ICD10_CODE', 'ICD10_CODE_TYPE', F.col('ICD10_CODE_MATCH_COUNT').cast('long').alias('ICD10_MATCH_NUMBER'), 'ICD10_TERM', 'OPCS4_CODE', 'OPCS4_CODE_TYPE', F.col('OPCS4_CODE_MATCH_COUNT').cast('long').alias('OPCS4_MATCH_NUMBER'), 'OPCS4_TERM', 'OMOP_CONCEPT_ID', 'OMOP_CONCEPT_NAME', F.col('NUMBER_OF_OMOP_MATCHES').cast('long').alias('OMOP_MATCH_NUMBER'), 'SNOMED_SIMILARITY', 'ICD10_SIMILARITY', 'OPCS4_SIMILARITY', 'OMOP_SIMILARITY', 'SIMILARITY_SOURCE_SNOMED', 'SIMILARITY_SOURCE_ICD10', 'SIMILARITY_SOURCE_OPCS4', 'SIMILARITY_SOURCE_OMOP', 'SIMILARITY_SNOMED_ICD10', 'SIMILARITY_SNOMED_OPCS4', 'SIMILARITY_SNOMED_OMOP', 'SIMILARITY_ICD10_OMOP', 'SIMILARITY_OPCS4_OMOP', 'SOURCE_CHANGE_TS', F.col('IS_ACTIVE').alias('NOMENCLATURE_IS_ACTIVE'))
    return df.join(nom, F.col('NOMENCLATURE_ID') == F.col('NOM_NOMENCLATURE_ID'), 'left').drop('NOM_NOMENCLATURE_ID')

def _legacy_and_foundation_columns(df: DataFrame, run_id: str) -> DataFrame:
    result = df.withColumn('ENCNTR_ID', F.col('CE_ENCNTR_ID')).withColumn('PERSON_ID', F.col('CE_PERSON_ID')).withColumn('ORDER_ID', F.col('CE_ORDER_ID')).withColumn('PERFORMED_PRSNL_ID', F.col('CE_PERFORMED_PRSNL_ID')).withColumn('EVENT_TITLE_TEXT', F.col('CE_EVENT_TITLE_TEXT')).withColumn('EVENT_CD', F.col('CE_EVENT_CD')).withColumn('CATALOG_CD', F.col('CE_CATALOG_CD')).withColumn('CATALOG_TYPE_CD', F.col('OC_CATALOG_TYPE_CD')).withColumn('CATALOG_DISPLAY', F.col('OC_DESCRIPTION')).withColumn('EVENT_CLASS_CD', F.col('CE_EVENT_CLASS_CD')).withColumn('CONTRIBUTOR_SYSTEM_CD', F.col('CE_CONTRIBUTOR_SYSTEM_CD')).withColumn('REFERENCE_NBR', F.col('CE_REFERENCE_NBR')).withColumn('PARENT_EVENT_ID', F.col('CE_PARENT_EVENT_ID')).withColumn('NORMALCY_CD', F.col('CE_NORMALCY_CD')).withColumn('ENTRY_MODE_CD', F.col('CE_ENTRY_MODE_CD')).withColumn('PERFORMED_DT_TM', F.col('CE_PERFORMED_DT_TM')).withColumn('CLINSIG_UPDT_DT_TM', F.col('CE_CLINSIG_UPDT_DT_TM')).withColumn('PARENT_EVENT_TITLE_TEXT', F.col('PE_EVENT_TITLE_TEXT')).withColumn('PARENT_EVENT_CD', F.col('PE_EVENT_CD')).withColumn('PARENT_CATALOG_CD', F.col('PE_CATALOG_CD')).withColumn('PARENT_CATALOG_TYPE_CD', F.col('POC_CATALOG_TYPE_CD')).withColumn('PARENT_CATALOG_DISPLAY', F.col('POC_DESCRIPTION')).withColumn('PARENT_REFERENCE_NBR', F.col('PE_REFERENCE_NBR')).withColumn('SOURCE_VOCABULARY_DESC', F.col('SOURCE_VOCABULARY_DESCRIPTION')).withColumn('VOCAB_AXIS_DESC', F.col('VOCAB_AXIS_DESCRIPTION')).withColumn('EVENT_START_DT_TM', F.col('CE_EVENT_START_DT_TM')).withColumn('EVENT_END_DT_TM', F.col('CE_EVENT_END_DT_TM')).withColumn('EVENT_TAG', F.col('CE_EVENT_TAG')).withColumn('ACCESSION_NBR', F.col('CE_ACCESSION_NBR')).withColumn('RESULT_STATUS_CD', F.col('CE_RESULT_STATUS_CD')).withColumn('RECORD_STATUS_CD', F.col('CE_RECORD_STATUS_CD')).withColumn('AUTHENTIC_FLAG', F.col('CE_AUTHENTIC_FLAG')).withColumn('PUBLISH_FLAG', F.col('CE_PUBLISH_FLAG')).withColumn('VERIFIED_DT_TM', F.col('CE_VERIFIED_DT_TM')).withColumn('VERIFIED_PRSNL_ID', F.col('CE_VERIFIED_PRSNL_ID')).withColumn('EVENT_RELTN_CD', F.col('CE_EVENT_RELTN_CD')).withColumn('SOURCE_CD', F.col('CE_SOURCE_CD')).withColumn('CLINICAL_EVENT_ID', F.col('CE_CLINICAL_EVENT_ID')).withColumn('CE_VALID_FROM_DT_TM', F.col('CE_VALID_FROM_DT_TM')).withColumn('CE_VALID_UNTIL_DT_TM', F.col('CE_VALID_UNTIL_DT_TM')).withColumn('CE_ADC_UPDT', F.col('CE_ADC_UPDT')).withColumn('PARENT_CE_ADC_UPDT', F.col('PE_ADC_UPDT')).withColumn('CATALOG_NAME', F.coalesce(F.nullif(F.trim(F.col('OC_DEPT_DISPLAY_NAME')), F.lit('')), F.nullif(F.trim(F.col('OC_DESCRIPTION')), F.lit('')), F.nullif(F.trim(F.col('OC_PRIMARY_MNEMONIC')), F.lit('')))).withColumn('PARENT_CATALOG_NAME', F.coalesce(F.nullif(F.trim(F.col('POC_DEPT_DISPLAY_NAME')), F.lit('')), F.nullif(F.trim(F.col('POC_DESCRIPTION')), F.lit('')), F.nullif(F.trim(F.col('POC_PRIMARY_MNEMONIC')), F.lit('')))).withColumn('CLINICAL_EVENT_DT_TM', F.coalesce(F.col('EVENT_END_DT_TM'), F.col('PERFORMED_DT_TM'), F.col('EVENT_START_DT_TM'), F.col('CR_VALID_FROM_DT_TM'))).withColumn('EVENT_NAME', F.coalesce(F.nullif(F.trim(F.col('EVENT_TITLE_TEXT')), F.lit('')), F.nullif(F.trim(F.col('EVENT_CD_DISPLAY')), F.lit('')), F.nullif(F.trim(F.col('CATALOG_NAME')), F.lit('')))).withColumn('NOMENCLATURE_CODE', F.coalesce(F.nullif(F.trim(F.col('SOURCE_IDENTIFIER')), F.lit('')), F.nullif(F.trim(F.col('CONCEPT_CKI')), F.lit('')), F.col('NOMENCLATURE_ID').cast('string'))).withColumn('RESOLVED_OMOP_CONCEPT_ID', F.coalesce(F.col('OMOP_MANUAL_CONCEPT_ID'), F.col('OMOP_CONCEPT_ID'))).withColumn('RESOLVED_OMOP_CONCEPT_NAME', F.coalesce(F.col('OMOP_MANUAL_CONCEPT_NAME'), F.col('OMOP_CONCEPT_NAME'))).withColumn('RESOLVED_OMOP_STANDARD_CONCEPT', F.coalesce(F.col('OMOP_MANUAL_STANDARD_CONCEPT'), F.col('OMOP_STANDARD_CONCEPT'))).withColumn('RESOLVED_OMOP_CONCEPT_DOMAIN', F.coalesce(F.col('OMOP_MANUAL_CONCEPT_DOMAIN'), F.col('OMOP_CONCEPT_DOMAIN'))).withColumn('RESOLVED_OMOP_CONCEPT_CLASS', F.coalesce(F.col('OMOP_MANUAL_CONCEPT_CLASS'), F.col('OMOP_CONCEPT_CLASS'))).withColumn('OMOP_MAPPING_SOURCE', F.when(F.col('OMOP_MANUAL_CONCEPT_ID').isNotNull(), F.lit('manual_rule')).when(F.col('OMOP_CONCEPT_ID').isNotNull(), F.lit('nomenclature'))).withColumn('RESOLVED_OMOP_VALUE_CONCEPT_ID', F.col('OMOP_MANUAL_VALUE_CONCEPT_ID')).withColumn('RESOLVED_OMOP_VALUE_CONCEPT_NAME', F.col('OMOP_MANUAL_VALUE_CONCEPT_NAME')).withColumn('OMOP_VALUE_MAPPING_SOURCE', F.when(F.col('OMOP_MANUAL_VALUE_CONCEPT_ID').isNotNull(), F.lit('manual_rule'))).withColumn('SOURCE_DELETED_IND', F.lit(False)).withColumn('SOURCE_CHANGE_TYPE', F.lit('snapshot_upsert'))
    result = result.withColumn('CODE_LOOKUP_ADC_UPDT', _greatest_existing(result, [f'{stem}_LOOKUP_ADC_UPDT' for stem in ('EVENT_CD', 'PARENT_EVENT_CD', 'NORMALCY', 'CONTRIBUTOR_SYSTEM', 'ENTRY_MODE', 'CATALOG_TYPE', 'PARENT_CATALOG_TYPE', 'SOURCE_VOCABULARY', 'VOCAB_AXIS', 'RESULT_STATUS', 'RECORD_STATUS', 'EVENT_RELTN', 'SOURCE')])).withColumn('ADC_UPDT', _greatest_existing(result, ['CR_ADC_UPDT', 'CE_ADC_UPDT', 'PARENT_CE_ADC_UPDT', 'NOMENCLATURE_ADC_UPDT', 'OC_ADC_UPDT', 'POC_ADC_UPDT', 'CODE_LOOKUP_ADC_UPDT', 'OMOP_MANUAL_MAP_ADC_UPDT', 'OMOP_MANUAL_VALUE_MAP_ADC_UPDT']))
    return result.withColumn('_ROW_HASH_PAYLOAD', F.to_json(F.struct('*'))).withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_PROCESSED_TS', F.current_timestamp()).withColumn('ROW_HASH', F.sha2(F.col('_ROW_HASH_PAYLOAD'), 256)).drop('_ROW_HASH_PAYLOAD')

def build_map_nomen_events(versions: Dict[str, int], event_ids: Optional[DataFrame]=None, trust: str=DEFAULT_TRUST, run_id: Optional[str]=None) -> DataFrame:
    run_id = run_id or str(uuid.uuid4())
    coded = _coded_results(versions, event_ids, trust)
    with_child = _align_child_clinical_event(coded, versions, trust)
    with_parent = _align_parent_clinical_event(with_child, versions, trust)
    with_catalog = _join_catalogs(with_parent, versions)
    result = _join_nomenclature(with_catalog, versions)
    code_values = _snapshot_table(SRC_CODE_VALUE, versions)
    lookups = [('CE_EVENT_CD', 'EVENT_CD'), ('PE_EVENT_CD', 'PARENT_EVENT_CD'), ('CE_NORMALCY_CD', 'NORMALCY'), ('CE_CONTRIBUTOR_SYSTEM_CD', 'CONTRIBUTOR_SYSTEM'), ('CE_ENTRY_MODE_CD', 'ENTRY_MODE'), ('OC_CATALOG_TYPE_CD', 'CATALOG_TYPE'), ('POC_CATALOG_TYPE_CD', 'PARENT_CATALOG_TYPE'), ('SOURCE_VOCABULARY_CD', 'SOURCE_VOCABULARY'), ('VOCAB_AXIS_CD', 'VOCAB_AXIS'), ('CE_RESULT_STATUS_CD', 'RESULT_STATUS'), ('CE_RECORD_STATUS_CD', 'RECORD_STATUS'), ('CE_EVENT_RELTN_CD', 'EVENT_RELTN'), ('CE_SOURCE_CD', 'SOURCE')]
    for code_column, output_stem in lookups:
        result = _join_code_lookup(result, code_values, code_column, output_stem)
    result = result.withColumn('EVENT_CD', F.col('CE_EVENT_CD')).withColumn('EVENT_CLASS_CD', F.col('CE_EVENT_CLASS_CD'))
    result = add_manual_omop_mappings_nomen(result, _snapshot_table(SRC_MANUAL_MAP, versions), _snapshot_table(SRC_OMOP_CONCEPT, versions))
    return _legacy_and_foundation_columns(result, run_id)

def _changed_event_ids_from_manual_rules(changes: DataFrame, target: DataFrame) -> DataFrame:
    rules = _manual_rule_base(changes, ['measurement_concept_id', 'observation_concept_id', 'value_as_concept_id']).select('SourceField', 'DIRECT_EVENT_CD', 'DIRECT_RESULT_TXT', 'CONTEXT_EVENT_CD', 'CONTEXT_EVENT_CLASS_CD', 'CONTEXT_RESULT_TXT').distinct()
    event_rules = F.broadcast(rules.filter(F.col('SourceField') == 'EVENT_CD')).alias('r')
    result_rules = F.broadcast(rules.filter(F.col('SourceField') == 'EVENT_RESULT_TXT')).alias('r')
    t = target.select('EVENT_ID', 'EVENT_CD', 'EVENT_CLASS_CD', 'SOURCE_STRING').withColumn('SOURCE_STRING_CLEAN', F.nullif(F.trim(F.col('SOURCE_STRING')), F.lit(''))).alias('t')
    context = (F.col('r.CONTEXT_EVENT_CD').isNull() | (F.col('t.EVENT_CD') == F.col('r.CONTEXT_EVENT_CD'))) & (F.col('r.CONTEXT_EVENT_CLASS_CD').isNull() | (F.col('t.EVENT_CLASS_CD') == F.col('r.CONTEXT_EVENT_CLASS_CD'))) & (F.col('r.CONTEXT_RESULT_TXT').isNull() | (F.col('t.SOURCE_STRING_CLEAN') == F.col('r.CONTEXT_RESULT_TXT')))
    by_event = t.join(event_rules, F.col('t.EVENT_CD') == F.col('r.DIRECT_EVENT_CD'), 'inner').filter(context).select(F.col('t.EVENT_ID'))
    by_result = t.join(result_rules, F.col('t.SOURCE_STRING_CLEAN') == F.col('r.DIRECT_RESULT_TXT'), 'inner').filter(context).select(F.col('t.EVENT_ID'))
    return by_event.unionByName(by_result).distinct()

def collect_affected_event_ids(batches: Dict[str, ChangeBatch], versions: Dict[str, int]) -> DataFrame:
    if not table_exists(MAP_NOMEN_TARGET):
        raise FullRebuildRequired(f'{MAP_NOMEN_TARGET} does not exist')
    target = spark.table(MAP_NOMEN_TARGET)
    frames: List[Optional[DataFrame]] = []
    coded_changes = batches[SRC_CODED_RESULT].changes
    if coded_changes is not None:
        frames.append(coded_changes.select(F.col('EVENT_ID').cast('long').alias('EVENT_ID')))
    clinical_changes = batches[SRC_CLINICAL_EVENT].changes
    if clinical_changes is not None:
        changed_ce_ids = clinical_changes.select(F.col('EVENT_ID').cast('long').alias('CHANGED_EVENT_ID')).filter(F.col('CHANGED_EVENT_ID').isNotNull()).distinct()
        frames.append(changed_ce_ids.select(F.col('CHANGED_EVENT_ID').alias('EVENT_ID')))
        frames.append(reduce(lambda left, right: left.unionByName(right), [target.alias('t').join(F.broadcast(changed_ce_ids).alias('c'), F.col('t.' + column) == F.col('c.CHANGED_EVENT_ID'), 'left_semi').select('EVENT_ID') for column in ('EVENT_ID', 'PARENT_EVENT_ID')]))
    nomen_changes = batches[SRC_NOMENCLATURE].changes
    if nomen_changes is not None:
        changed_nomen = nomen_changes.select(F.col('NOMENCLATURE_ID').cast('long').alias('NOMENCLATURE_ID')).filter(F.col('NOMENCLATURE_ID').isNotNull()).distinct()
        frames.append(_snapshot_table(SRC_CODED_RESULT, versions).filter(F.col('Trust') == DEFAULT_TRUST).alias('cr').join(F.broadcast(changed_nomen).alias('n'), 'NOMENCLATURE_ID', 'inner').select(F.col('cr.EVENT_ID').cast('long').alias('EVENT_ID')))
    code_changes = batches[SRC_CODE_VALUE].changes
    if code_changes is not None:
        changed_codes = code_changes.select(F.col('CODE_VALUE').cast('long').alias('CHANGED_CODE')).filter(F.col('CHANGED_CODE').isNotNull()).distinct()
        code_columns = [c for c in ['EVENT_CD', 'PARENT_EVENT_CD', 'NORMALCY_CD', 'CONTRIBUTOR_SYSTEM_CD', 'ENTRY_MODE_CD', 'CATALOG_TYPE_CD', 'PARENT_CATALOG_TYPE_CD', 'SOURCE_VOCABULARY_CD', 'VOCAB_AXIS_CD', 'RESULT_STATUS_CD', 'RECORD_STATUS_CD', 'EVENT_RELTN_CD', 'SOURCE_CD'] if c in target.columns]
        if code_columns:
            frames.append(reduce(lambda left, right: left.unionByName(right), [target.alias('t').join(F.broadcast(changed_codes).alias('v'), F.col('t.' + c).cast('long') == F.col('v.CHANGED_CODE'), 'left_semi').select('EVENT_ID') for c in code_columns]))
    catalog_changes = batches[SRC_ORDER_CATALOG].changes
    if catalog_changes is not None:
        changed_catalogs = catalog_changes.select(F.col('CATALOG_CD').cast('long').alias('CHANGED_CATALOG_CD')).filter(F.col('CHANGED_CATALOG_CD').isNotNull()).distinct()
        frames.append(reduce(lambda left, right: left.unionByName(right), [target.alias('t').join(F.broadcast(changed_catalogs).alias('c'), F.col('t.' + column) == F.col('c.CHANGED_CATALOG_CD'), 'left_semi').select('EVENT_ID') for column in ('CATALOG_CD', 'PARENT_CATALOG_CD')]))
    manual_changes = batches[SRC_MANUAL_MAP].changes
    if manual_changes is not None:
        frames.append(_changed_event_ids_from_manual_rules(manual_changes, target))
    concept_changes = batches[SRC_OMOP_CONCEPT].changes
    if concept_changes is not None:
        changed_concepts = concept_changes.select(F.col('concept_id').cast('int').alias('CHANGED_CONCEPT_ID')).filter(F.col('CHANGED_CONCEPT_ID').isNotNull()).distinct()
        concept_columns = [c for c in ['OMOP_CONCEPT_ID', 'OMOP_MANUAL_CONCEPT_ID', 'OMOP_MANUAL_VALUE_CONCEPT_ID'] if c in target.columns]
        if concept_columns:
            frames.append(reduce(lambda left, right: left.unionByName(right), [target.alias('t').join(F.broadcast(changed_concepts).alias('v'), F.col('t.' + c).cast('int') == F.col('v.CHANGED_CONCEPT_ID'), 'left_semi').select('EVENT_ID') for c in concept_columns]))
        candidate_array_columns = [c for c in ['OMOP_MANUAL_CANDIDATE_CONCEPT_IDS', 'OMOP_MANUAL_VALUE_CANDIDATE_CONCEPT_IDS'] if c in target.columns]
        if candidate_array_columns:
            candidate_events = target.select('EVENT_ID', F.explode_outer(F.flatten(F.array(*[F.coalesce(F.col(c), F.expr('CAST(array() AS ARRAY<INT>)')) for c in candidate_array_columns]))).alias('CANDIDATE_CONCEPT_ID')).filter(F.col('CANDIDATE_CONCEPT_ID').isNotNull())
            frames.append(candidate_events.join(F.broadcast(changed_concepts), F.col('CANDIDATE_CONCEPT_ID') == F.col('CHANGED_CONCEPT_ID'), 'left_semi').select('EVENT_ID'))
    return _union_event_ids(frames)
LEGACY_COLUMNS = ['EVENT_ID', 'ENCNTR_ID', 'PERSON_ID', 'ORDER_ID', 'NOMENCLATURE_ID', 'PERFORMED_PRSNL_ID', 'EVENT_TITLE_TEXT', 'EVENT_CD', 'EVENT_CD_DISPLAY', 'CATALOG_CD', 'CATALOG_DISPLAY', 'CATALOG_TYPE_CD', 'CATALOG_TYPE_DISPLAY', 'EVENT_CLASS_CD', 'CONTRIBUTOR_SYSTEM_CD', 'CONTRIBUTOR_SYSTEM_DISPLAY', 'REFERENCE_NBR', 'PARENT_EVENT_ID', 'NORMALCY_CD', 'NORMALCY_DISPLAY', 'ENTRY_MODE_CD', 'ENTRY_MODE_DISPLAY', 'PERFORMED_DT_TM', 'CLINSIG_UPDT_DT_TM', 'PARENT_EVENT_TITLE_TEXT', 'PARENT_EVENT_CD', 'PARENT_EVENT_CD_DISPLAY', 'PARENT_CATALOG_CD', 'PARENT_CATALOG_DISPLAY', 'PARENT_CATALOG_TYPE_CD', 'PARENT_CATALOG_TYPE_DISPLAY', 'PARENT_REFERENCE_NBR', 'SOURCE_IDENTIFIER', 'SOURCE_STRING', 'SOURCE_VOCABULARY_CD', 'SOURCE_VOCABULARY_DESC', 'VOCAB_AXIS_CD', 'VOCAB_AXIS_DESC', 'CONCEPT_CKI', 'OMOP_CONCEPT_ID', 'OMOP_CONCEPT_NAME', 'OMOP_STANDARD_CONCEPT', 'OMOP_MATCH_NUMBER', 'OMOP_CONCEPT_DOMAIN', 'OMOP_CONCEPT_CLASS', 'SNOMED_CODE', 'SNOMED_TYPE', 'SNOMED_MATCH_NUMBER', 'SNOMED_TERM', 'ADC_UPDT', 'OMOP_MANUAL_TABLE', 'OMOP_MANUAL_COLUMN', 'OMOP_MANUAL_CONCEPT', 'OMOP_MANUAL_VALUE_CONCEPT', 'OMOP_MANUAL_CONCEPT_NAME', 'OMOP_MANUAL_STANDARD_CONCEPT', 'OMOP_MANUAL_CONCEPT_DOMAIN', 'OMOP_MANUAL_CONCEPT_CLASS', 'OMOP_SIMILARITY', 'SNOMED_SIMILARITY']

def order_map_nomen_columns(df: DataFrame) -> DataFrame:
    quoted_legacy = [qname(column_name) for column_name in LEGACY_COLUMNS]
    return df.selectExpr(*quoted_legacy, f"* EXCEPT ({', '.join(quoted_legacy)})")

def validate_map_nomen_build(df: DataFrame, versions: Dict[str, int], trust: str, validation_level: str='FULL') -> Dict[str, int]:
    cached = df
    output_count = cached.count()
    expected_count = _coded_results(versions, event_ids=None, trust=trust).count()
    null_key_count = cached.filter(F.col('EVENT_ID').isNull() | F.col('SEQUENCE_NBR').isNull()).count()
    metrics = {'expected_source_rows': int(expected_count), 'output_rows': int(output_count), 'null_key_rows': int(null_key_count)}
    if output_count != expected_count:
        raise ValueError(f'Row preservation failed: source={expected_count}, output={output_count}')
    if null_key_count:
        raise ValueError(f'Found {null_key_count} rows with incomplete source keys')
    if validation_level.upper() == 'FULL':
        duplicate_keys = cached.groupBy(*MAP_NOMEN_KEY).count().filter(F.col('count') > 1).count()
        metrics['duplicate_keys'] = int(duplicate_keys)
        if duplicate_keys:
            raise ValueError(f'Found {duplicate_keys} duplicate source keys')
    print('[VALIDATION]', metrics)
    return metrics

def _apply_table_metadata(table_name: str) -> None:
    spark.sql(f"\n        ALTER TABLE {qname(table_name)} SET TBLPROPERTIES (\n            'delta.enableChangeDataFeed' = 'true',\n            'delta.enableRowTracking' = 'true',\n            'delta.enableDeletionVectors' = 'true',\n            'delta.autoOptimize.optimizeWrite' = 'true',\n            'delta.autoOptimize.autoCompact' = 'true',\n            'delta.deletedFileRetentionDuration' = 'interval {DEFAULT_RETENTION_DAYS} days',\n            'delta.logRetentionDuration' = 'interval {DEFAULT_RETENTION_DAYS} days',\n            'map_nomen_events.schemaVersion' = '2'\n        )\n        ")
    escaped_comment = map_nomen_events_comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {qname(table_name)} IS '{escaped_comment}'")
    column_comments = {'EVENT_ID': 'Clinical event identifier. Not unique by itself at the coded-result grain.', 'SEQUENCE_NBR': 'Coded-result sequence number; part of the source row grain.', 'CR_VALID_FROM_DT_TM': 'Valid-from timestamp retained from the selected coded-result source row.', 'SOURCE_ROW_KEY': 'SHA-256 key derived from EVENT_ID and SEQUENCE_NBR.', 'CLINICAL_EVENT_DT_TM': 'Clinically relevant event time: EVENT_END, then PERFORMED, EVENT_START and coded-result valid-from.', 'EVENT_NAME': 'Resolved event label from event title, corrected code display and catalog name.', 'SOURCE_DELETED_IND': 'True when a previously loaded source row is absent after a CDF-identified source change.', 'ROW_HASH': 'SHA-256 content hash used to avoid rewriting unchanged matched rows.', 'OMOP_MANUAL_AMBIGUOUS_IND': 'True when more than one distinct manual OMOP concept matched the source row.', 'OMOP_MANUAL_MATCH_COUNT': 'Number of distinct manual OMOP concepts matching the source row.', 'OMOP_MAPPING_SOURCE': 'Selected OMOP concept source: manual_rule or nomenclature.', 'ADC_UPDT': 'Informational greatest upstream update timestamp. Never used as a CDC checkpoint.', 'CE_CONTEXT_MATCH_MODE': 'How the child clinical-event context was chosen: VALIDITY_WINDOW when a row covered the coded-result anchor time, FALLBACK_LATEST when none did and the latest row was used, NO_CANDIDATE when no clinical-event row exists.', 'PE_CONTEXT_MATCH_MODE': 'How the parent clinical-event context was chosen: VALIDITY_WINDOW, FALLBACK_LATEST, or NO_CANDIDATE when there is no parent row.'}
    existing_columns = set(spark.table(table_name).columns)
    for column_name, comment in column_comments.items():
        if column_name in existing_columns:
            escaped = comment.replace("'", "''")
            spark.sql(f"ALTER TABLE {qname(table_name)} ALTER COLUMN {qname(column_name)} COMMENT '{escaped}'")
    try:
        spark.sql(f'ALTER TABLE {qname(table_name)} CLUSTER BY (EVENT_ID, SEQUENCE_NBR)')
    except Exception as error:
        print(f'[WARN] Could not enable liquid clustering on {table_name}: {error}')

def _create_current_view() -> None:
    spark.sql(f"""
        CREATE OR REPLACE VIEW {qname(MAP_NOMEN_CURRENT_VIEW)} AS
        SELECT *
        FROM {qname(MAP_NOMEN_TARGET)}
        WHERE SOURCE_DELETED_IND = false
          AND CR_VALID_UNTIL_DT_TM > current_timestamp()
        QUALIFY row_number() OVER (
            PARTITION BY EVENT_ID, SEQUENCE_NBR
            ORDER BY
                CR_VALID_FROM_DT_TM DESC,
                ADC_UPDT DESC,
                SOURCE_ROW_KEY DESC
        ) = 1
    """)
def process_nomen_events_full_rebuild(apply: bool=False, trust: str=DEFAULT_TRUST, validation_level: str='FULL', configure_source_retention: bool=False, create_backup: bool=False) -> Dict[str, object]:
    run_id = str(uuid.uuid4())
    versions = capture_source_versions()
    print(f'[INFO] Full rebuild run_id={run_id}')
    print(f'[INFO] Version-pinned sources: {versions}')
    if not apply:
        return {'mode': 'FULL_REBUILD_DRY_RUN', 'run_id': run_id, 'versions': versions}
    if configure_source_retention:
        configure_cdf_retention(DEFAULT_RETENTION_DAYS, apply=False)
    built = order_map_nomen_columns(build_map_nomen_events(versions, event_ids=None, trust=trust, run_id=run_id))
    validate_map_nomen_build(built, versions, trust, validation_level)
    built = bronze_project_contract(built, MAP_NOMEN_TARGET)
    backup_name = None
    if create_backup and table_exists(MAP_NOMEN_TARGET):
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        backup_name = f'{MAP_NOMEN_TARGET}__pre_v2_{timestamp}'
        spark.sql(f'CREATE TABLE {qname(backup_name)} SHALLOW CLONE {qname(MAP_NOMEN_TARGET)}')
    built.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('delta.enableChangeDataFeed', 'true').saveAsTable(MAP_NOMEN_TARGET)
    _apply_table_metadata(MAP_NOMEN_TARGET)
    _create_current_view()
    write_checkpoints(versions, run_id)
    None
    return {'mode': 'FULL_REBUILD', 'run_id': run_id, 'versions': versions, 'backup_table': backup_name, 'target_table': MAP_NOMEN_TARGET}

def _merge_rebuilt_rows(rebuilt: DataFrame) -> None:
    rebuilt = bronze_project_contract(rebuilt, MAP_NOMEN_TARGET)
    target = DeltaTable.forName(spark, MAP_NOMEN_TARGET)
    condition = ' AND '.join(
        [f't.{column} <=> s.{column}' for column in MAP_NOMEN_KEY]
    )
    assignments = {
        column_name: F.col(f's.{column_name}')
        for column_name in rebuilt.columns
    }
    comparisons = ' OR '.join(
        f'NOT (t.`{column_name}` <=> s.`{column_name}`)'
        for column_name in rebuilt.columns
        if column_name not in MAP_NOMEN_KEY
    )
    target.alias('t').merge(
        rebuilt.alias('s'), condition
    ).whenMatchedUpdate(
        condition=comparisons or 'false',
        set=assignments,
    ).whenNotMatchedInsert(values=assignments).execute()

def _mark_removed_source_rows(affected_event_ids: DataFrame, rebuilt: DataFrame, run_id: str) -> int:
    target_scope = spark.table(MAP_NOMEN_TARGET).filter(~F.col('SOURCE_DELETED_IND')).alias('t').join(affected_event_ids.alias('a'), F.col('t.EVENT_ID') == F.col('a.EVENT_ID'), 'inner').select(F.col('t.SOURCE_ROW_KEY'))
    active_keys = rebuilt.select('SOURCE_ROW_KEY').distinct()
    removed = _mp30_persist_stage(target_scope.join(active_keys, 'SOURCE_ROW_KEY', 'left_anti').distinct())
    removed_count = removed.count()
    if removed_count == 0:
        return 0
    requested_updates = {
        'SOURCE_DELETED_IND': 'true',
        'SOURCE_CHANGE_TYPE': "'delete_or_key_change'",
        'PIPELINE_RUN_ID': f"'{run_id}'",
        'PIPELINE_PROCESSED_TS': 'current_timestamp()',
        'ROW_HASH': "sha2(concat_ws('||', t.ROW_HASH, 'DELETED'), 256)",
    }
    allowed = bronze_contract_column_set(MAP_NOMEN_TARGET)
    retained_updates = {
        column_name: expression
        for column_name, expression in requested_updates.items()
        if column_name.lower() in allowed
    }
    DeltaTable.forName(spark, MAP_NOMEN_TARGET).alias('t').merge(
        removed.alias('s'),
        't.SOURCE_ROW_KEY = s.SOURCE_ROW_KEY',
    ).whenMatchedUpdate(set=retained_updates).execute()
    return int(removed_count)

def process_nomen_events_incremental_v2(apply: bool=True, trust: str=DEFAULT_TRUST) -> Dict[str, object]:
    if not table_exists(MAP_NOMEN_TARGET):
        raise FullRebuildRequired(f'{MAP_NOMEN_TARGET} does not exist')
    schema_property = spark.sql(f"SHOW TBLPROPERTIES {qname(MAP_NOMEN_TARGET)} ('map_nomen_events.schemaVersion')").first()
    property_values = schema_property.asDict() if schema_property is not None else {}
    schema_version = property_values.get('value')
    if schema_version is None and schema_property is not None and (len(schema_property) > 0):
        schema_version = schema_property[0]
    if str(schema_version) != '2':
        raise FullRebuildRequired('Target schema version 2 has not been deployed')
    run_id = str(uuid.uuid4())
    versions = capture_source_versions()
    batches = read_change_batches(versions)
    affected = _mp30_persist_stage(collect_affected_event_ids(batches, versions))
    has_affected_rows = bool(affected.take(1))
    print(f'[INFO] Incremental run_id={run_id}; affected_rows={has_affected_rows}')
    if not apply:
        return {'mode': 'INCREMENTAL_DRY_RUN', 'run_id': run_id, 'versions': versions, 'has_affected_rows': has_affected_rows}
    removed_count = 0
    if has_affected_rows:
        rebuilt = _mp30_persist_stage(order_map_nomen_columns(build_map_nomen_events(versions, event_ids=affected, trust=trust, run_id=run_id)))
        _merge_rebuilt_rows(rebuilt)
        removed_count = _mark_removed_source_rows(affected, rebuilt, run_id)
        _mp30_persist_stage_drop(rebuilt)
    write_checkpoints(versions, run_id)
    _mp30_persist_stage_drop(affected)
    return {'mode': 'INCREMENTAL', 'run_id': run_id, 'versions': versions, 'removed_rows_marked': removed_count}
schema_map_nomen_events = None

def rebuild_map_nomen_events_v2(trust: str=DEFAULT_TRUST, validation_level: str='FULL', create_backup: bool=False) -> Dict[str, object]:
    """Explicit one-time validated migration. Nothing calls this automatically."""
    return process_nomen_events_full_rebuild(apply=True, trust=trust, validation_level=validation_level, configure_source_retention=False, create_backup=create_backup)

def process_nomen_events_incremental(trust: str=DEFAULT_TRUST) -> Dict[str, object]:
    """Drop-in entry point that intentionally replaces the original pipeline function."""
    return process_nomen_events_incremental_v2(apply=True, trust=trust)

def deploy_map_nomen_events_improvements(trust: str=DEFAULT_TRUST, validation_level: str='FULL') -> Dict[str, object]:
    return rebuild_map_nomen_events_v2(trust=trust, validation_level=validation_level, create_backup=True)

try:
    _pipeline_run_recoverable('map_nomen_events', _PIPELINE_FULL_REFRESH, lambda: process_nomen_events_incremental(), lambda: process_nomen_events_full_rebuild(apply=True, validation_level='FULL', configure_source_retention=False, create_backup=False))
    _PIPELINE_UPDATED_TARGETS.extend(['4_prod.bronze.map_nomen_events'])
    _pipeline_mark_component_complete('map_nomen_events', ['4_prod.bronze.map_nomen_events'])
    _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_nomen_events'})
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

_pipeline_component_start("map_coded_events")
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
class MapCodedEventsConfig:
    target_table: str = '4_prod.bronze.map_coded_events'
    bridge_table: str = '4_prod.bronze.map_coded_events_omop_bridge'
    checkpoint_table: str = '6_mgmt.bronze.map_coded_events_checkpoints'
    pipeline_name: str = 'map_coded_events_v2'
    coded_result_source: str = '4_prod.raw.mill_ce_coded_result'
    clinical_event_source: str = '4_prod.raw.mill_clinical_event'
    code_value_source: str = '3_lookup.mill.mill_code_value'
    order_catalog_source: str = '3_lookup.mill.mill_order_catalog'
    manual_map_source: str = '3_lookup.omop.barts_new_maps'
    concept_source: str = '3_lookup.omop.concept'
    trust_scope: Optional[str] = 'Barts'
    cdf_retention: str = 'interval 30 days'
    broadcast_event_id_limit: int = 1000000

    @property
    def source_tables(self) -> Tuple[str, ...]:
        return (self.coded_result_source, self.clinical_event_source, self.code_value_source, self.order_catalog_source, self.manual_map_source, self.concept_source)
DEFAULT_CODED_EVENTS_CONFIG = MapCodedEventsConfig()
TARGET_KEYS = ['EVENT_ID', 'SEQUENCE_NBR']
BRIDGE_KEYS = ['EVENT_ID', 'SEQUENCE_NBR', 'MAPPING_GROUP', 'MAPPING_RULE_ID']
MAP_CODE_COLUMNS = ['RESULT_CD', 'EVENT_CD', 'CATALOG_TYPE_CD', 'EVENT_CLASS_CD', 'PARENT_EVENT_CLASS_CD', 'CONTRIBUTOR_SYSTEM_CD', 'NORMALCY_CD', 'NORMALCY_METHOD_CD', 'ENTRY_MODE_CD', 'PARENT_EVENT_CD', 'PARENT_CATALOG_TYPE_CD', 'PARENT_RESULT_STATUS_CD', 'RESULT_STATUS_CD', 'RECORD_STATUS_CD', 'EVENT_RELTN_CD', 'SOURCE_CD', 'RESULT_UNITS_CD', 'RESULT_TIME_UNITS_CD', 'UNIT_OF_MEASURE_CD']
MAPPING_SUMMARY_COLUMNS = ['OMOP_MANUAL_TABLE', 'OMOP_MANUAL_COLUMN', 'OMOP_MANUAL_CONCEPT', 'OMOP_MANUAL_VALUE_CONCEPT', 'OMOP_MANUAL_CONCEPT_NAME', 'OMOP_MANUAL_STANDARD_CONCEPT', 'OMOP_MANUAL_CONCEPT_DOMAIN', 'OMOP_MANUAL_CONCEPT_CLASS', 'OMOP_MAPPING_RULE_ID', 'OMOP_MAPPING_MATCH_TYPE', 'OMOP_MAPPING_CANDIDATE_COUNT', 'OMOP_MAPPING_DISTINCT_CONCEPT_COUNT', 'OMOP_MAPPING_AMBIGUOUS_IND', 'OMOP_MAPPING_CANDIDATE_CONCEPT_IDS', 'OMOP_VALUE_MAPPING_CANDIDATE_COUNT', 'OMOP_VALUE_MAPPING_AMBIGUOUS_IND', 'OMOP_VALUE_MAPPING_CANDIDATE_CONCEPT_IDS', 'OMOP_MAPPING_ADC_UPDT']
MAP_CODED_EVENTS_COMMENT = 'Curated coded clinical-event results at EVENT_ID + SEQUENCE_NBR grain. One row is kept per EVENT_ID + SEQUENCE_NBR, preferring the currently valid source version, and only rows with a populated RESULT_CD (not null and not the zero sentinel) are published, because a row without a result code carries no coded result. No validity or result-status filtering is applied within that scope, and 4_prod.raw.mill_ce_coded_result remains the retention layer holding every source row. The table preserves source provenance and status fields, decoded lookup values, quality indicators, and a deterministic compatibility OMOP mapping. All OMOP mapping candidates are retained in map_coded_events_omop_bridge.'
MAP_CODED_EVENTS_BRIDGE_COMMENT = 'Lossless OMOP mapping candidates for map_coded_events. One row represents one source coded-result row and one matching manual mapping rule. MAPPING_RANK records deterministic compatibility precedence without discarding alternative candidates.'

def _sql_name(table_name: str) -> str:
    """Quote a multipart Unity Catalog name safely for SQL statements."""
    return '.'.join((f"`{part.replace('`', '``')}`" for part in table_name.split('.')))

def _sql_string(value: str) -> str:
    return value.replace('\\', '\\\\').replace("'", "''")

def _table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _read_delta_snapshot(table_name: str, source_versions: Optional[Dict[str, int]]=None) -> DataFrame:
    """Read an exact source snapshot when a run has captured source versions."""
    if source_versions is not None and table_name in source_versions:
        return _m30_hardening_read_pinned_snapshot(table_name, int(source_versions[table_name]))
    return spark.table(table_name)

def _is_empty(df: DataFrame) -> bool:
    return df.limit(1).count() == 0

def _union_distinct(dataframes: Iterable[Optional[DataFrame]], column: str='EVENT_ID') -> Optional[DataFrame]:
    frames = [df.select(F.col(column).cast('long').alias(column)).where(F.col(column).isNotNull()) for df in dataframes if df is not None]
    if not frames:
        return None
    return reduce(lambda left, right: left.unionByName(right), frames).distinct()

def _latest_by_key(df: DataFrame, keys: Sequence[str], order_columns: Sequence[str]) -> DataFrame:
    ordering = [F.col(column).desc_nulls_last() for column in order_columns]
    window = Window.partitionBy(*keys).orderBy(*ordering)
    return df.withColumn('_LATEST_ROW_NUMBER', F.row_number().over(window)).where(F.col('_LATEST_ROW_NUMBER') == 1).drop('_LATEST_ROW_NUMBER')

def _nonblank(column: F.Column) -> F.Column:
    return column.isNotNull() & (F.trim(column.cast('string')) != '')

def _label_expression(display_column: str, description_column: str, meaning_column: str, code_column: str) -> F.Column:
    return F.coalesce(F.when(_nonblank(F.col(display_column)), F.trim(F.col(display_column))), F.when(_nonblank(F.col(description_column)), F.trim(F.col(description_column))), F.when(_nonblank(F.col(meaning_column)), F.trim(F.col(meaning_column))), F.col(code_column).cast('string'))

def _row_hash(df: DataFrame, excluded: Sequence[str]=()) -> F.Column:
    excluded_set = set(excluded) | {'ROW_HASH', 'PIPELINE_UPDT_DT_TM'}
    columns = [F.col(column) for column in sorted(df.columns) if column not in excluded_set]
    return F.sha2(F.to_json(F.struct(*columns)), 256)

def _get_table_comment(table_name: str) -> Optional[str]:
    if not _table_exists(table_name):
        return None
    try:
        return spark.catalog.getTable(table_name).description
    except Exception:
        return None

def _set_table_comment_if_changed(table_name: str, comment: str) -> None:
    if _get_table_comment(table_name) != comment:
        spark.sql(f"COMMENT ON TABLE {_sql_name(table_name)} IS '{_sql_string(comment)}'")

def _configure_delta_table(table_name: str, comment: str, cluster_columns: Sequence[str], retention: str) -> None:
    properties = {'delta.enableChangeDataFeed': 'true', 'delta.deletedFileRetentionDuration': retention, 'delta.logRetentionDuration': retention}
    property_sql = ', '.join((f"'{_sql_string(key)}' = '{_sql_string(value)}'" for key, value in properties.items()))
    spark.sql(f'ALTER TABLE {_sql_name(table_name)} SET TBLPROPERTIES ({property_sql})')
    if cluster_columns:
        cluster_sql = ', '.join((f'`{column}`' for column in cluster_columns))
        spark.sql(f'ALTER TABLE {_sql_name(table_name)} CLUSTER BY ({cluster_sql})')
    _set_table_comment_if_changed(table_name, comment)

def ensure_map_checkpoint_table(config: MapCodedEventsConfig=DEFAULT_CODED_EVENTS_CONFIG) -> None:
    spark.sql(f"\n        CREATE TABLE IF NOT EXISTS {_sql_name(config.checkpoint_table)} (\n            pipeline_name STRING,\n            source_table STRING,\n            last_processed_version BIGINT,\n            processed_at TIMESTAMP,\n            run_id STRING\n        )\n        USING DELTA\n        TBLPROPERTIES (\n            'delta.enableChangeDataFeed' = 'true',\n            'delta.deletedFileRetentionDuration' = '{_sql_string(config.cdf_retention)}'\n        )\n        ")

def get_latest_delta_version(table_name: str) -> int:
    rows = spark.sql(f'DESCRIBE HISTORY {_sql_name(table_name)} LIMIT 1').select('version').collect()
    if not rows:
        raise RuntimeError(f'No Delta history was returned for {table_name}')
    return int(rows[0]['version'])

def get_map_checkpoints(config: MapCodedEventsConfig=DEFAULT_CODED_EVENTS_CONFIG) -> Dict[str, int]:
    ensure_map_checkpoint_table(config)
    rows = spark.table(config.checkpoint_table).where(F.col('pipeline_name') == config.pipeline_name).select('source_table', 'last_processed_version').collect()
    return {row['source_table']: int(row['last_processed_version']) for row in rows}

def write_map_checkpoints(versions: Dict[str, int], run_id: str, config: MapCodedEventsConfig=DEFAULT_CODED_EVENTS_CONFIG) -> None:
    if not versions:
        return
    ensure_map_checkpoint_table(config)
    rows = [(config.pipeline_name, source, int(version), run_id) for source, version in versions.items()]
    updates = spark.createDataFrame(rows, 'pipeline_name string, source_table string, last_processed_version long, run_id string').withColumn('processed_at', F.current_timestamp())
    DeltaTable.forName(spark, config.checkpoint_table).alias('target').merge(updates.alias('source'), 'target.pipeline_name = source.pipeline_name AND target.source_table = source.source_table').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def read_cdf_versions(table_name: str, starting_version: int, ending_version: int) -> Optional[DataFrame]:
    if starting_version > ending_version:
        return None
    try:
        changes = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', int(starting_version)).option('endingVersion', int(ending_version)).table(table_name)
        return _m30_hardening_validate_cdf(changes)
    except Exception as exc:
        raise RuntimeError(f'CDF is unavailable for {table_name} from version {starting_version} to {ending_version}. Refusing timestamp fallback because it can lose late-arriving or backfilled rows. Run rebuild_map_coded_events_v2() and then resume incrementally.') from exc

def configure_map_source_cdf_retention(config: MapCodedEventsConfig=DEFAULT_CODED_EVENTS_CONFIG) -> None:
    """Increase the recovery window. This function performs DDL only when explicitly called."""
    for table_name in config.source_tables:
        spark.sql(f"ALTER TABLE {_sql_name(table_name)} SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '{_sql_string(config.cdf_retention)}', 'delta.logRetentionDuration' = '{_sql_string(config.cdf_retention)}')")

def _apply_trust_scope(df: DataFrame, config: MapCodedEventsConfig) -> DataFrame:
    if config.trust_scope is None or 'Trust' not in df.columns:
        return df
    return df.where(F.col('Trust') == F.lit(config.trust_scope))

def latest_coded_result_rows(event_ids: Optional[DataFrame]=None, config: MapCodedEventsConfig=DEFAULT_CODED_EVENTS_CONFIG, broadcast_event_ids: bool=True, source_versions: Optional[Dict[str, int]]=None) -> DataFrame:
    """Return the latest source snapshot at EVENT_ID + SEQUENCE_NBR grain, scoped to rows that carry a coded result.

    One row is kept per EVENT_ID + SEQUENCE_NBR, preferring the currently valid source
    version, with no validity or result-status filtering. RESULT_CD must then be populated:
    a coded-result row whose RESULT_CD is null or the zero sentinel records no coded result
    and only padded the canonical table. This mirrors the NOMENCLATURE_ID filter the nomen
    lane applies to the same source. 4_prod.raw.mill_ce_coded_result remains the retention
    layer and keeps every source row unfiltered.
    """
    df = _apply_trust_scope(_read_delta_snapshot(config.coded_result_source, source_versions), config)
    df = df.withColumn('EVENT_ID', F.col('EVENT_ID').cast('long')).withColumn('SEQUENCE_NBR', F.col('SEQUENCE_NBR').cast('long'))
    if event_ids is not None:
        ids = event_ids.select(F.col('EVENT_ID').cast('long').alias('EVENT_ID')).distinct()
        if broadcast_event_ids:
            ids = F.broadcast(ids)
        df = df.join(ids, 'EVENT_ID', 'left_semi')
    df = df.withColumn('_CURRENT_SOURCE_ROW', F.when((F.col('VALID_FROM_DT_TM') <= F.current_timestamp()) & (F.col('VALID_UNTIL_DT_TM') > F.current_timestamp()), 1).otherwise(0))
    df = _latest_by_key(df, TARGET_KEYS, ['_CURRENT_SOURCE_ROW', 'VALID_FROM_DT_TM', 'VALID_UNTIL_DT_TM', 'ADC_UPDT', 'UPDT_DT_TM', 'UPDT_CNT']).drop('_CURRENT_SOURCE_ROW')
    df = df.withColumn('RESULT_CD', F.col('RESULT_CD').cast('long')).filter(F.col('RESULT_CD').isNotNull() & (F.col('RESULT_CD') != 0))
    return df.select(F.col('EVENT_ID'), F.col('SEQUENCE_NBR'), F.col('VALID_FROM_DT_TM').alias('CR_VALID_FROM_DT_TM'), F.col('VALID_UNTIL_DT_TM').alias('CR_VALID_UNTIL_DT_TM'), F.col('NOMENCLATURE_ID').cast('long').alias('NOMENCLATURE_ID'), F.col('ACR_CODE_STR'), F.col('GROUP_NBR').cast('long').alias('GROUP_NBR'), F.col('DESCRIPTOR'), F.col('RESULT_CD').cast('long').alias('RESULT_CD'), F.col('RESULT_SET'), F.col('PATHOLOGY_STR'), F.col('PROC_CODE_STR'), F.col('UNIT_OF_MEASURE_CD').cast('long').alias('UNIT_OF_MEASURE_CD'), F.col('UPDT_DT_TM').alias('CR_UPDT_DT_TM'), F.col('UPDT_ID').cast('long').alias('CR_UPDT_ID'), F.col('UPDT_TASK').cast('long').alias('CR_UPDT_TASK'), F.col('UPDT_CNT').cast('long').alias('CR_UPDT_CNT'), F.col('UPDT_APPLCTX').cast('long').alias('CR_UPDT_APPLCTX'), F.col('LAST_UTC_TS').alias('CR_LAST_UTC_TS'), F.col('INST_ID').cast('long').alias('CR_INST_ID'), F.col('TXN_ID_TEXT').alias('CR_TXN_ID_TEXT'), F.col('ADC_UPDT').alias('CR_ADC_UPDT'), F.col('ENCNTR_ID').cast('long').alias('CR_ENCNTR_ID'), F.col('ORGANIZATION_ID').cast('long').alias('CR_ORGANIZATION_ID'), F.col('Trust').alias('CR_TRUST'))

def latest_clinical_event_rows(event_ids: DataFrame, config: MapCodedEventsConfig=DEFAULT_CODED_EVENTS_CONFIG, broadcast_event_ids: bool=True, source_versions: Optional[Dict[str, int]]=None) -> DataFrame:
    """Return one deterministic latest clinical-event row per logical EVENT_ID, with no status filter."""
    ids = event_ids.select(F.col('EVENT_ID').cast('long').alias('EVENT_ID')).distinct()
    if broadcast_event_ids:
        ids = F.broadcast(ids)
    df = _read_delta_snapshot(config.clinical_event_source, source_versions).withColumn('EVENT_ID', F.col('EVENT_ID').cast('long')).join(ids, 'EVENT_ID', 'left_semi')
    df = df.withColumn('_CURRENT_SOURCE_ROW', F.when((F.col('VALID_FROM_DT_TM') <= F.current_timestamp()) & (F.col('VALID_UNTIL_DT_TM') > F.current_timestamp()), 1).otherwise(0))
    df = _latest_by_key(df, ['EVENT_ID'], ['_CURRENT_SOURCE_ROW', 'VALID_FROM_DT_TM', 'UPDT_CNT', 'ADC_UPDT', 'UPDT_DT_TM', 'CLINICAL_EVENT_ID']).drop('_CURRENT_SOURCE_ROW')
    return df.select('EVENT_ID', F.col('CLINICAL_EVENT_ID').cast('long'), F.col('ENCNTR_ID').cast('long'), F.col('PERSON_ID').cast('long'), F.col('EVENT_START_DT_TM'), F.col('ENCNTR_FINANCIAL_ID').cast('long'), F.col('VALID_FROM_DT_TM').alias('CE_VALID_FROM_DT_TM'), F.col('VALID_UNTIL_DT_TM').alias('CE_VALID_UNTIL_DT_TM'), F.col('EVENT_TITLE_TEXT'), F.col('VIEW_LEVEL').cast('long'), F.col('ORDER_ID').cast('long'), F.col('CATALOG_CD').cast('long'), F.col('SERIES_REF_NBR'), F.col('ACCESSION_NBR'), F.col('CONTRIBUTOR_SYSTEM_CD').cast('long'), F.col('REFERENCE_NBR'), F.col('PARENT_EVENT_ID').cast('long'), F.col('EVENT_RELTN_CD').cast('long'), F.col('EVENT_CLASS_CD').cast('long'), F.col('EVENT_CD').cast('long'), F.col('EVENT_TAG'), F.col('EVENT_END_DT_TM'), F.col('RESULT_VAL'), F.col('RESULT_UNITS_CD').cast('long'), F.col('RESULT_TIME_UNITS_CD').cast('long'), F.col('TASK_ASSAY_CD').cast('long'), F.col('RECORD_STATUS_CD').cast('long'), F.col('RESULT_STATUS_CD').cast('long'), F.col('AUTHENTIC_FLAG').cast('long'), F.col('PUBLISH_FLAG').cast('long'), F.col('QC_REVIEW_CD').cast('long'), F.col('NORMALCY_CD').cast('long'), F.col('NORMALCY_METHOD_CD').cast('long'), F.col('INQUIRE_SECURITY_CD').cast('long'), F.col('RESOURCE_GROUP_CD').cast('long'), F.col('RESOURCE_CD').cast('long'), F.col('SUBTABLE_BIT_MAP').cast('long'), F.col('COLLATING_SEQ'), F.col('VERIFIED_DT_TM'), F.col('VERIFIED_PRSNL_ID').cast('long'), F.col('PERFORMED_DT_TM'), F.col('PERFORMED_PRSNL_ID').cast('long'), F.col('NORMAL_LOW'), F.col('NORMAL_HIGH'), F.col('CRITICAL_LOW'), F.col('CRITICAL_HIGH'), F.col('EXPIRATION_DT_TM'), F.col('CLINSIG_UPDT_DT_TM'), F.col('ORDER_ACTION_SEQUENCE').cast('long'), F.col('ENTRY_MODE_CD').cast('long'), F.col('SOURCE_CD').cast('long'), F.col('CLINICAL_SEQ'), F.col('EVENT_END_TZ').cast('long'), F.col('EVENT_START_TZ').cast('long'), F.col('PERFORMED_TZ').cast('long'), F.col('VERIFIED_TZ').cast('long'), F.col('SRC_EVENT_ID').cast('long'), F.col('SRC_CLINSIG_UPDT_DT_TM'), F.col('DEVICE_FREE_TXT'), F.col('UPDT_DT_TM').alias('CE_UPDT_DT_TM'), F.col('UPDT_ID').cast('long').alias('CE_UPDT_ID'), F.col('UPDT_TASK').cast('long').alias('CE_UPDT_TASK'), F.col('UPDT_CNT').cast('long').alias('CE_UPDT_CNT'), F.col('UPDT_APPLCTX').cast('long').alias('CE_UPDT_APPLCTX'), F.col('LAST_UTC_TS').alias('CE_LAST_UTC_TS'), F.col('TXN_ID_TEXT').alias('CE_TXN_ID_TEXT'), F.col('ADC_UPDT').alias('CE_ADC_UPDT'), F.col('ORGANIZATION_ID').cast('long').alias('CE_ORGANIZATION_ID'), F.col('Trust').alias('CE_TRUST'))

def create_coded_event_code_lookup(code_values: DataFrame, alias_suffix: str) -> DataFrame:
    """Compatibility replacement that preserves all three Cerner label representations."""
    prefix = alias_suffix.upper()
    return code_values.select(F.col('CODE_VALUE').cast('long').alias(f'_{prefix}_CODE_VALUE'), F.col('DESCRIPTION').alias(f'{prefix}_DESCRIPTION'), F.col('DISPLAY').alias(f'{prefix}_DISPLAY'), F.col('CDF_MEANING').alias(f'{prefix}_MEANING'), F.col('CODE_SET').cast('long').alias(f'{prefix}_CODE_SET'), F.col('ACTIVE_IND').cast('long').alias(f'{prefix}_ACTIVE_IND'), F.col('ADC_UPDT').alias(f'{prefix}_LOOKUP_ADC_UPDT'))

def _join_code_lookup(df: DataFrame, code_values: DataFrame, code_column: str, prefix: str) -> DataFrame:
    prefix = prefix.upper()
    lookup = create_coded_event_code_lookup(code_values, prefix)
    lookup_key = f'_{prefix}_CODE_VALUE'
    result = df.join(F.broadcast(lookup), F.col(code_column).cast('long') == F.col(lookup_key), 'left').drop(lookup_key).withColumn(f'{prefix}_LABEL', _label_expression(f'{prefix}_DISPLAY', f'{prefix}_DESCRIPTION', f'{prefix}_MEANING', code_column)).withColumn(f'{prefix}_LOOKUP_MATCHED_IND', F.col(f'{prefix}_DESCRIPTION').isNotNull() | F.col(f'{prefix}_DISPLAY').isNotNull() | F.col(f'{prefix}_MEANING').isNotNull())
    return result

def _join_order_catalog(df: DataFrame, order_catalog: DataFrame, code_column: str, prefix: str) -> DataFrame:
    prefix = prefix.upper()
    key = f'_{prefix}_CATALOG_CD'
    lookup = order_catalog.select(F.col('CATALOG_CD').cast('long').alias(key), F.col('CATALOG_TYPE_CD').cast('long').alias(f'{prefix}_CATALOG_TYPE_CD'), F.col('DESCRIPTION').alias(f'{prefix}_CATALOG_DESCRIPTION'), F.col('PRIMARY_MNEMONIC').alias(f'{prefix}_PRIMARY_MNEMONIC'), F.col('DEPT_DISPLAY_NAME').alias(f'{prefix}_DEPT_DISPLAY_NAME'), F.col('ACTIVE_IND').cast('long').alias(f'{prefix}_CATALOG_ACTIVE_IND'), F.col('EVENT_CD').cast('long').alias(f'{prefix}_CATALOG_EVENT_CD'), F.col('CONCEPT_CKI').alias(f'{prefix}_CATALOG_CONCEPT_CKI'), F.col('ADC_UPDT').alias(f'{prefix}_CATALOG_LOOKUP_ADC_UPDT'))
    return df.join(F.broadcast(lookup), F.col(code_column).cast('long') == F.col(key), 'left').drop(key).withColumn(f'{prefix}_CATALOG_LABEL', F.coalesce(F.when(_nonblank(F.col(f'{prefix}_DEPT_DISPLAY_NAME')), F.trim(F.col(f'{prefix}_DEPT_DISPLAY_NAME'))), F.when(_nonblank(F.col(f'{prefix}_CATALOG_DESCRIPTION')), F.trim(F.col(f'{prefix}_CATALOG_DESCRIPTION'))), F.when(_nonblank(F.col(f'{prefix}_PRIMARY_MNEMONIC')), F.trim(F.col(f'{prefix}_PRIMARY_MNEMONIC'))), F.col(code_column).cast('string')))

def build_coded_events_base(event_ids: Optional[DataFrame]=None, config: MapCodedEventsConfig=DEFAULT_CODED_EVENTS_CONFIG, source_versions: Optional[Dict[str, int]]=None, broadcast_event_ids: Optional[bool]=None) -> Tuple[DataFrame, List[DataFrame]]:
    """Build source-aligned rows. Returns the DataFrame and cache handles to unpersist."""
    full_build = event_ids is None
    use_broadcast = not full_build if broadcast_event_ids is None else broadcast_event_ids
    coded_results = latest_coded_result_rows(event_ids, config, broadcast_event_ids=use_broadcast, source_versions=source_versions)
    direct_event_ids = coded_results.select('EVENT_ID').distinct()
    clinical_events = latest_clinical_event_rows(direct_event_ids, config, broadcast_event_ids=use_broadcast, source_versions=source_versions)
    if full_build:
        clinical_events = clinical_events
    joined = coded_results.join(clinical_events, 'EVENT_ID', 'left')
    parent_ids = joined.select(F.col('PARENT_EVENT_ID').alias('EVENT_ID')).where(F.col('EVENT_ID').isNotNull()).distinct()
    parent_events = latest_clinical_event_rows(parent_ids, config, broadcast_event_ids=use_broadcast, source_versions=source_versions).select(F.col('EVENT_ID').alias('_PARENT_EVENT_JOIN_ID'), F.col('CLINICAL_EVENT_ID').alias('PARENT_CLINICAL_EVENT_ID'), F.col('EVENT_TITLE_TEXT').alias('PARENT_EVENT_TITLE_TEXT'), F.col('EVENT_TAG').alias('PARENT_EVENT_TAG'), F.col('EVENT_CD').alias('PARENT_EVENT_CD'), F.col('EVENT_CLASS_CD').alias('PARENT_EVENT_CLASS_CD'), F.col('CATALOG_CD').alias('PARENT_CATALOG_CD'), F.col('REFERENCE_NBR').alias('PARENT_REFERENCE_NBR'), F.col('RESULT_STATUS_CD').alias('PARENT_RESULT_STATUS_CD'), F.col('CE_VALID_FROM_DT_TM').alias('PARENT_CE_VALID_FROM_DT_TM'), F.col('CE_VALID_UNTIL_DT_TM').alias('PARENT_CE_VALID_UNTIL_DT_TM'), F.col('CE_ADC_UPDT').alias('PARENT_CE_ADC_UPDT'))
    joined = joined.join(parent_events, F.col('PARENT_EVENT_ID') == F.col('_PARENT_EVENT_JOIN_ID'), 'left').drop('_PARENT_EVENT_JOIN_ID')
    code_values = _read_delta_snapshot(config.code_value_source, source_versions).select('CODE_VALUE', 'CODE_SET', 'DESCRIPTION', 'DISPLAY', 'CDF_MEANING', 'ACTIVE_IND', 'ADC_UPDT')
    order_catalog = _read_delta_snapshot(config.order_catalog_source, source_versions).select('CATALOG_CD', 'CATALOG_TYPE_CD', 'DESCRIPTION', 'PRIMARY_MNEMONIC', 'DEPT_DISPLAY_NAME', 'ACTIVE_IND', 'EVENT_CD', 'CONCEPT_CKI', 'ADC_UPDT')
    joined = _join_order_catalog(joined, order_catalog, 'CATALOG_CD', 'MAIN')
    joined = _join_order_catalog(joined, order_catalog, 'PARENT_CATALOG_CD', 'PARENT')
    lookup_specs = [('RESULT_CD', 'RESULT'), ('EVENT_CD', 'EVENT_CD'), ('EVENT_CLASS_CD', 'EVENT_CLASS'), ('MAIN_CATALOG_TYPE_CD', 'CATALOG_TYPE'), ('CONTRIBUTOR_SYSTEM_CD', 'CONTRIBUTOR_SYSTEM'), ('NORMALCY_CD', 'NORMALCY'), ('NORMALCY_METHOD_CD', 'NORMALCY_METHOD'), ('ENTRY_MODE_CD', 'ENTRY_MODE'), ('PARENT_EVENT_CD', 'PARENT_EVENT_CD'), ('PARENT_EVENT_CLASS_CD', 'PARENT_EVENT_CLASS'), ('PARENT_CATALOG_TYPE_CD', 'PARENT_CATALOG_TYPE'), ('PARENT_RESULT_STATUS_CD', 'PARENT_RESULT_STATUS'), ('RESULT_STATUS_CD', 'RESULT_STATUS'), ('RECORD_STATUS_CD', 'RECORD_STATUS'), ('EVENT_RELTN_CD', 'EVENT_RELTN'), ('SOURCE_CD', 'SOURCE'), ('RESULT_UNITS_CD', 'RESULT_UNITS'), ('RESULT_TIME_UNITS_CD', 'RESULT_TIME_UNITS'), ('UNIT_OF_MEASURE_CD', 'UNIT_OF_MEASURE')]
    for code_column, prefix in lookup_specs:
        joined = _join_code_lookup(joined, code_values, code_column, prefix)
    joined = joined.withColumn('ENCNTR_ID', F.coalesce(F.col('ENCNTR_ID'), F.col('CR_ENCNTR_ID'))).withColumn('ORGANIZATION_ID', F.coalesce(F.col('CE_ORGANIZATION_ID'), F.col('CR_ORGANIZATION_ID'))).withColumn('Trust', F.coalesce(F.col('CR_TRUST'), F.col('CE_TRUST'))).withColumn('CLINICAL_EVENT_MATCHED_IND', F.col('CLINICAL_EVENT_ID').isNotNull()).withColumn('PARENT_EVENT_MATCHED_IND', F.col('PARENT_CLINICAL_EVENT_ID').isNotNull()).withColumn('CR_CURRENT_AT_LOAD_IND', F.when((F.col('CR_VALID_FROM_DT_TM') <= F.current_timestamp()) & (F.col('CR_VALID_UNTIL_DT_TM') > F.current_timestamp()), True).otherwise(False)).withColumn('CE_CURRENT_AT_LOAD_IND', F.when((F.col('CE_VALID_FROM_DT_TM') <= F.current_timestamp()) & (F.col('CE_VALID_UNTIL_DT_TM') > F.current_timestamp()), True).otherwise(False)).withColumn('RESULT_IN_ERROR_IND', F.when(F.col('RESULT_STATUS_CD').isin(29, 30, 31), True).otherwise(False)).withColumn('PERFORMED_DT_QUALITY_FLAG', F.when(F.col('PERFORMED_DT_TM').isNull(), F.lit('MISSING')).when(F.col('PERFORMED_DT_TM') < F.lit('1980-01-01').cast('timestamp'), F.lit('PRE_1980')).when(F.col('PERFORMED_DT_TM') > F.current_timestamp() + F.expr('INTERVAL 30 DAYS'), F.lit('FUTURE_GT_30D')).otherwise(F.lit('OK'))).withColumn('RESULT_LABEL', F.coalesce(F.col('RESULT_LABEL'), F.when(_nonblank(F.col('DESCRIPTOR')), F.trim(F.col('DESCRIPTOR'))), F.when(_nonblank(F.col('RESULT_VAL')), F.trim(F.col('RESULT_VAL'))), F.col('RESULT_CD').cast('string'))).withColumn('EVENT_LABEL', F.coalesce(F.when(_nonblank(F.col('EVENT_TITLE_TEXT')), F.trim(F.col('EVENT_TITLE_TEXT'))), F.when(_nonblank(F.col('EVENT_TAG')), F.trim(F.col('EVENT_TAG'))), F.col('EVENT_CD_LABEL'))).withColumn('PARENT_EVENT_LABEL', F.coalesce(F.when(_nonblank(F.col('PARENT_EVENT_TITLE_TEXT')), F.trim(F.col('PARENT_EVENT_TITLE_TEXT'))), F.when(_nonblank(F.col('PARENT_EVENT_TAG')), F.trim(F.col('PARENT_EVENT_TAG'))), F.col('PARENT_EVENT_CD_LABEL'))).withColumn('CATALOG_CD', F.col('CATALOG_CD').cast('long')).withColumn('CATALOG_DISPLAY', F.col('MAIN_CATALOG_LABEL')).withColumn('CATALOG_DESCRIPTION', F.col('MAIN_CATALOG_DESCRIPTION')).withColumn('CATALOG_PRIMARY_MNEMONIC', F.col('MAIN_PRIMARY_MNEMONIC')).withColumn('CATALOG_DEPT_DISPLAY_NAME', F.col('MAIN_DEPT_DISPLAY_NAME')).withColumn('CATALOG_TYPE_CD', F.col('MAIN_CATALOG_TYPE_CD')).withColumn('PARENT_CATALOG_DISPLAY', F.col('PARENT_CATALOG_LABEL')).withColumn('PARENT_CATALOG_DESCRIPTION', F.col('PARENT_CATALOG_DESCRIPTION')).withColumn('PARENT_CATALOG_TYPE_CD', F.col('PARENT_CATALOG_TYPE_CD')).withColumn('SOURCE_ADC_UPDT', F.greatest('CR_ADC_UPDT', 'CE_ADC_UPDT', 'PARENT_CE_ADC_UPDT'))
    lookup_update_columns = [f'{prefix}_LOOKUP_ADC_UPDT' for _, prefix in lookup_specs]
    joined = joined.withColumn('LOOKUP_ADC_UPDT', F.greatest(*[F.col(column) for column in lookup_update_columns])).withColumn('ADC_UPDT', F.greatest(F.col('SOURCE_ADC_UPDT'), F.col('LOOKUP_ADC_UPDT'))).withColumn('SOURCE_ROW_KEY', F.sha2(F.concat_ws('||', F.coalesce(F.col('EVENT_ID').cast('string'), F.lit('<NULL>')), F.coalesce(F.col('SEQUENCE_NBR').cast('string'), F.lit('<NULL>'))), 256))
    caches: List[DataFrame] = [code_values, order_catalog]
    if full_build:
        caches.append(clinical_events)
    return (joined, caches)

def _manual_mapping_rules(config: MapCodedEventsConfig, mapping_df: Optional[DataFrame]=None, concepts_df: Optional[DataFrame]=None, source_versions: Optional[Dict[str, int]]=None) -> DataFrame:
    maps = mapping_df if mapping_df is not None else _read_delta_snapshot(config.manual_map_source, source_versions)
    concepts = concepts_df if concepts_df is not None else _read_delta_snapshot(config.concept_source, source_versions)
    maps = maps.where(F.col('SourceTable') == 'dbo.PI_CDE_CLINICAL_EVENT').where(F.col('SourceField').isin('EVENT_CD', 'EVENT_RESULT_TXT')).where(F.col('OMOPField').isin('measurement_concept_id', 'observation_concept_id', 'value_as_concept_id')).select('OMOPTable', 'OMOPField', 'SourceTable', 'SourceField', 'SourceValue', 'OMOPConceptId', 'EVENT_CD', 'EVENT_CLASS_CD', 'EVENT_RESULT_TXT', F.col('CONCEPT_NAME').alias('MAP_CONCEPT_NAME'), F.col('DOMAIN_ID').alias('MAP_DOMAIN_ID'), F.col('VOCABULARY_ID').alias('MAP_VOCABULARY_ID'), F.col('CONCEPT_CLASS_ID').alias('MAP_CONCEPT_CLASS_ID'), F.col('STANDARD_CONCEPT').alias('MAP_STANDARD_CONCEPT'), F.col('ADC_UPDT').alias('MAPPING_ADC_UPDT')).withColumn('MAPPED_CONCEPT_ID', F.expr('try_cast(OMOPConceptId as bigint)')).withColumn('MAP_EVENT_CD', F.expr('try_cast(EVENT_CD as bigint)')).withColumn('MAP_EVENT_CLASS_CD', F.expr('try_cast(EVENT_CLASS_CD as bigint)')).withColumn('MAPPING_GROUP', F.when(F.col('OMOPField') == 'value_as_concept_id', 'VALUE').otherwise('CONCEPT')).withColumn('MAPPING_RULE_ID', F.sha2(F.concat_ws('||', *[F.coalesce(F.col(column).cast('string'), F.lit('<NULL>')) for column in ['OMOPTable', 'OMOPField', 'SourceTable', 'SourceField', 'SourceValue', 'OMOPConceptId', 'EVENT_CD', 'EVENT_CLASS_CD', 'EVENT_RESULT_TXT']]), 256)).withColumn('MAPPING_SPECIFICITY', F.lit(1) + F.when(F.col('EVENT_CD').isNotNull(), 1).otherwise(0) + F.when(F.col('EVENT_CLASS_CD').isNotNull(), 1).otherwise(0) + F.when(F.col('EVENT_RESULT_TXT').isNotNull(), 1).otherwise(0))
    maps = _latest_by_key(maps, ['MAPPING_RULE_ID'], ['MAPPING_ADC_UPDT'])
    concept_ids = maps.select(F.col('MAPPED_CONCEPT_ID').alias('concept_id')).where(F.col('concept_id').isNotNull()).distinct()
    concept_details = concepts.join(F.broadcast(concept_ids), 'concept_id', 'left_semi').select(F.col('concept_id').cast('long'), F.col('concept_name').alias('REF_CONCEPT_NAME'), F.col('standard_concept').alias('REF_STANDARD_CONCEPT'), F.col('domain_id').alias('REF_DOMAIN_ID'), F.col('concept_class_id').alias('REF_CONCEPT_CLASS_ID'), F.col('vocabulary_id').alias('REF_VOCABULARY_ID'), F.col('concept_code').alias('REF_CONCEPT_CODE'), F.col('valid_start_date').alias('REF_VALID_START_DATE'), F.col('valid_end_date').alias('REF_VALID_END_DATE'), F.col('invalid_reason').alias('REF_INVALID_REASON'))
    return maps.join(concept_details, maps.MAPPED_CONCEPT_ID == concept_details.concept_id, 'left').drop(concept_details.concept_id).withColumn('EFFECTIVE_CONCEPT_NAME', F.coalesce(F.col('REF_CONCEPT_NAME'), F.col('MAP_CONCEPT_NAME'))).withColumn('EFFECTIVE_STANDARD_CONCEPT', F.coalesce(F.col('REF_STANDARD_CONCEPT'), F.col('MAP_STANDARD_CONCEPT'))).withColumn('EFFECTIVE_DOMAIN_ID', F.coalesce(F.col('REF_DOMAIN_ID'), F.col('MAP_DOMAIN_ID'))).withColumn('EFFECTIVE_CONCEPT_CLASS_ID', F.coalesce(F.col('REF_CONCEPT_CLASS_ID'), F.col('MAP_CONCEPT_CLASS_ID'))).withColumn('EFFECTIVE_VOCABULARY_ID', F.coalesce(F.col('REF_VOCABULARY_ID'), F.col('MAP_VOCABULARY_ID')))

def build_omop_mapping_outputs(base_df: DataFrame, config: MapCodedEventsConfig=DEFAULT_CODED_EVENTS_CONFIG, mapping_df: Optional[DataFrame]=None, concepts_df: Optional[DataFrame]=None, source_versions: Optional[Dict[str, int]]=None) -> Tuple[DataFrame, DataFrame]:
    """Return a lossless candidate bridge and a deterministic compatibility summary."""
    rules = F.broadcast(_manual_mapping_rules(config, mapping_df, concepts_df, source_versions=source_versions)).alias('mapping')
    base = base_df.alias('base')
    result_text_columns = ['RESULT_DISPLAY', 'RESULT_DESCRIPTION', 'RESULT_MEANING', 'DESCRIPTOR', 'RESULT_VAL']
    source_value_matches = reduce(lambda left, right: left | right, [F.trim(F.col(f'base.{column}').cast('string')) == F.trim(F.col('mapping.SourceValue').cast('string')) for column in result_text_columns])
    context_result_matches = reduce(lambda left, right: left | right, [F.trim(F.col(f'base.{column}').cast('string')) == F.trim(F.col('mapping.EVENT_RESULT_TXT').cast('string')) for column in result_text_columns])
    event_code_rule = (F.col('mapping.SourceField') == 'EVENT_CD') & (F.col('base.EVENT_CD').cast('string') == F.col('mapping.SourceValue'))
    result_text_rule = (F.col('mapping.SourceField') == 'EVENT_RESULT_TXT') & source_value_matches
    context_matches = (F.col('mapping.MAP_EVENT_CD').isNull() | (F.col('base.EVENT_CD') == F.col('mapping.MAP_EVENT_CD'))) & (F.col('mapping.MAP_EVENT_CLASS_CD').isNull() | (F.col('base.EVENT_CLASS_CD') == F.col('mapping.MAP_EVENT_CLASS_CD'))) & (F.col('mapping.EVENT_RESULT_TXT').isNull() | context_result_matches)
    candidates = base.join(rules, (event_code_rule | result_text_rule) & context_matches, 'inner')
    matched_source_variant = F.when(F.col('mapping.SourceField') == 'EVENT_CD', F.lit('EVENT_CD')).when(F.trim(F.col('base.RESULT_DISPLAY')) == F.trim(F.col('mapping.SourceValue')), F.lit('RESULT_DISPLAY')).when(F.trim(F.col('base.RESULT_DESCRIPTION')) == F.trim(F.col('mapping.SourceValue')), F.lit('RESULT_DESCRIPTION')).when(F.trim(F.col('base.RESULT_MEANING')) == F.trim(F.col('mapping.SourceValue')), F.lit('RESULT_MEANING')).when(F.trim(F.col('base.DESCRIPTOR')) == F.trim(F.col('mapping.SourceValue')), F.lit('DESCRIPTOR')).when(F.trim(F.col('base.RESULT_VAL')) == F.trim(F.col('mapping.SourceValue')), F.lit('RESULT_VAL')).otherwise(F.lit('UNKNOWN'))
    domain_priority_map = F.create_map(F.lit('Drug'), F.lit(1), F.lit('Measurement'), F.lit(2), F.lit('Procedure'), F.lit(3), F.lit('Condition'), F.lit(4), F.lit('Device'), F.lit(5), F.lit('Observation'), F.lit(6))
    candidates = candidates.withColumn('MATCHED_SOURCE_VARIANT', matched_source_variant).withColumn('DOMAIN_PRIORITY', F.coalesce(F.element_at(domain_priority_map, F.col('mapping.EFFECTIVE_DOMAIN_ID')), F.lit(999))).withColumn('CONCEPT_VALIDITY_PRIORITY', F.when(F.col('mapping.REF_INVALID_REASON').isNull(), 1).otherwise(2)).withColumn('STANDARD_PRIORITY', F.when(F.col('mapping.EFFECTIVE_STANDARD_CONCEPT') == 'S', 1).when(F.col('mapping.EFFECTIVE_STANDARD_CONCEPT') == 'C', 2).otherwise(3))
    rank_window = Window.partitionBy(F.col('base.EVENT_ID'), F.col('base.SEQUENCE_NBR'), F.col('mapping.MAPPING_GROUP')).orderBy(F.col('CONCEPT_VALIDITY_PRIORITY').asc(), F.col('mapping.MAPPING_SPECIFICITY').desc(), F.col('STANDARD_PRIORITY').asc(), F.col('DOMAIN_PRIORITY').asc(), F.col('mapping.MAPPED_CONCEPT_ID').asc_nulls_last(), F.col('mapping.MAPPING_RULE_ID').asc())
    bridge = candidates.withColumn('MAPPING_RANK', F.row_number().over(rank_window)).select(F.col('base.EVENT_ID').alias('EVENT_ID'), F.col('base.SEQUENCE_NBR').alias('SEQUENCE_NBR'), F.col('base.SOURCE_ROW_KEY').alias('SOURCE_ROW_KEY'), F.col('mapping.MAPPING_GROUP').alias('MAPPING_GROUP'), F.col('mapping.MAPPING_RULE_ID').alias('MAPPING_RULE_ID'), F.col('mapping.MAPPING_SPECIFICITY').cast('int').alias('MAPPING_SPECIFICITY'), F.col('MAPPING_RANK').cast('int'), F.col('MATCHED_SOURCE_VARIANT'), F.col('mapping.SourceField').alias('SOURCE_FIELD'), F.col('mapping.SourceValue').alias('SOURCE_VALUE'), F.col('mapping.OMOPTable').alias('OMOP_TABLE'), F.col('mapping.OMOPField').alias('OMOP_FIELD'), F.col('mapping.MAPPED_CONCEPT_ID').cast('long').alias('OMOP_CONCEPT_ID'), F.col('mapping.EFFECTIVE_CONCEPT_NAME').alias('OMOP_CONCEPT_NAME'), F.col('mapping.EFFECTIVE_STANDARD_CONCEPT').alias('OMOP_STANDARD_CONCEPT'), F.col('mapping.EFFECTIVE_DOMAIN_ID').alias('OMOP_CONCEPT_DOMAIN'), F.col('mapping.EFFECTIVE_CONCEPT_CLASS_ID').alias('OMOP_CONCEPT_CLASS'), F.col('mapping.EFFECTIVE_VOCABULARY_ID').alias('OMOP_VOCABULARY_ID'), F.col('mapping.REF_CONCEPT_CODE').alias('OMOP_CONCEPT_CODE'), F.col('mapping.REF_VALID_START_DATE').alias('OMOP_VALID_START_DATE'), F.col('mapping.REF_VALID_END_DATE').alias('OMOP_VALID_END_DATE'), F.col('mapping.REF_INVALID_REASON').alias('OMOP_INVALID_REASON'), F.col('mapping.MAPPING_ADC_UPDT').alias('MAPPING_ADC_UPDT'), F.current_timestamp().alias('PIPELINE_UPDT_DT_TM'))
    summary = bridge.groupBy(*TARGET_KEYS).agg(F.first(F.when((F.col('MAPPING_GROUP') == 'CONCEPT') & (F.col('MAPPING_RANK') == 1), F.col('OMOP_TABLE')), ignorenulls=True).alias('OMOP_MANUAL_TABLE'), F.first(F.when((F.col('MAPPING_GROUP') == 'CONCEPT') & (F.col('MAPPING_RANK') == 1), F.col('OMOP_FIELD')), ignorenulls=True).alias('OMOP_MANUAL_COLUMN'), F.first(F.when((F.col('MAPPING_GROUP') == 'CONCEPT') & (F.col('MAPPING_RANK') == 1), F.col('OMOP_CONCEPT_ID')), ignorenulls=True).cast('long').alias('OMOP_MANUAL_CONCEPT'), F.first(F.when((F.col('MAPPING_GROUP') == 'VALUE') & (F.col('MAPPING_RANK') == 1), F.col('OMOP_CONCEPT_ID')), ignorenulls=True).cast('long').alias('OMOP_MANUAL_VALUE_CONCEPT'), F.first(F.when((F.col('MAPPING_GROUP') == 'CONCEPT') & (F.col('MAPPING_RANK') == 1), F.col('OMOP_CONCEPT_NAME')), ignorenulls=True).alias('OMOP_MANUAL_CONCEPT_NAME'), F.first(F.when((F.col('MAPPING_GROUP') == 'CONCEPT') & (F.col('MAPPING_RANK') == 1), F.col('OMOP_STANDARD_CONCEPT')), ignorenulls=True).alias('OMOP_MANUAL_STANDARD_CONCEPT'), F.first(F.when((F.col('MAPPING_GROUP') == 'CONCEPT') & (F.col('MAPPING_RANK') == 1), F.col('OMOP_CONCEPT_DOMAIN')), ignorenulls=True).alias('OMOP_MANUAL_CONCEPT_DOMAIN'), F.first(F.when((F.col('MAPPING_GROUP') == 'CONCEPT') & (F.col('MAPPING_RANK') == 1), F.col('OMOP_CONCEPT_CLASS')), ignorenulls=True).alias('OMOP_MANUAL_CONCEPT_CLASS'), F.first(F.when((F.col('MAPPING_GROUP') == 'CONCEPT') & (F.col('MAPPING_RANK') == 1), F.col('MAPPING_RULE_ID')), ignorenulls=True).alias('OMOP_MAPPING_RULE_ID'), F.first(F.when((F.col('MAPPING_GROUP') == 'CONCEPT') & (F.col('MAPPING_RANK') == 1), F.col('MATCHED_SOURCE_VARIANT')), ignorenulls=True).alias('OMOP_MAPPING_MATCH_TYPE'), F.sum(F.when(F.col('MAPPING_GROUP') == 'CONCEPT', 1).otherwise(0)).cast('int').alias('OMOP_MAPPING_CANDIDATE_COUNT'), F.countDistinct(F.when(F.col('MAPPING_GROUP') == 'CONCEPT', F.col('OMOP_CONCEPT_ID'))).cast('int').alias('OMOP_MAPPING_DISTINCT_CONCEPT_COUNT'), F.sort_array(F.collect_set(F.when(F.col('MAPPING_GROUP') == 'CONCEPT', F.col('OMOP_CONCEPT_ID')))).alias('OMOP_MAPPING_CANDIDATE_CONCEPT_IDS'), F.sum(F.when(F.col('MAPPING_GROUP') == 'VALUE', 1).otherwise(0)).cast('int').alias('OMOP_VALUE_MAPPING_CANDIDATE_COUNT'), F.countDistinct(F.when(F.col('MAPPING_GROUP') == 'VALUE', F.col('OMOP_CONCEPT_ID'))).cast('int').alias('_OMOP_VALUE_DISTINCT_COUNT'), F.sort_array(F.collect_set(F.when(F.col('MAPPING_GROUP') == 'VALUE', F.col('OMOP_CONCEPT_ID')))).alias('OMOP_VALUE_MAPPING_CANDIDATE_CONCEPT_IDS'), F.max('MAPPING_ADC_UPDT').alias('OMOP_MAPPING_ADC_UPDT')).withColumn('OMOP_MAPPING_AMBIGUOUS_IND', F.col('OMOP_MAPPING_DISTINCT_CONCEPT_COUNT') > 1).withColumn('OMOP_VALUE_MAPPING_AMBIGUOUS_IND', F.col('_OMOP_VALUE_DISTINCT_COUNT') > 1).drop('_OMOP_VALUE_DISTINCT_COUNT')
    return (bridge, summary)

def attach_omop_mapping_summary(base_df: DataFrame, summary_df: DataFrame) -> DataFrame:
    base = base_df.drop(*MAPPING_SUMMARY_COLUMNS)
    result = base.join(summary_df, TARGET_KEYS, 'left')
    empty_long_array = F.expr('CAST(array() AS ARRAY<BIGINT>)')
    result = result.withColumn('OMOP_MAPPING_CANDIDATE_COUNT', F.coalesce(F.col('OMOP_MAPPING_CANDIDATE_COUNT'), F.lit(0)).cast('int')).withColumn('OMOP_MAPPING_DISTINCT_CONCEPT_COUNT', F.coalesce(F.col('OMOP_MAPPING_DISTINCT_CONCEPT_COUNT'), F.lit(0)).cast('int')).withColumn('OMOP_MAPPING_AMBIGUOUS_IND', F.coalesce(F.col('OMOP_MAPPING_AMBIGUOUS_IND'), F.lit(False))).withColumn('OMOP_MAPPING_CANDIDATE_CONCEPT_IDS', F.coalesce(F.col('OMOP_MAPPING_CANDIDATE_CONCEPT_IDS'), empty_long_array)).withColumn('OMOP_VALUE_MAPPING_CANDIDATE_COUNT', F.coalesce(F.col('OMOP_VALUE_MAPPING_CANDIDATE_COUNT'), F.lit(0)).cast('int')).withColumn('OMOP_VALUE_MAPPING_AMBIGUOUS_IND', F.coalesce(F.col('OMOP_VALUE_MAPPING_AMBIGUOUS_IND'), F.lit(False))).withColumn('OMOP_VALUE_MAPPING_CANDIDATE_CONCEPT_IDS', F.coalesce(F.col('OMOP_VALUE_MAPPING_CANDIDATE_CONCEPT_IDS'), empty_long_array)).withColumn('ENRICHMENT_ADC_UPDT', F.greatest(F.col('LOOKUP_ADC_UPDT'), F.col('OMOP_MAPPING_ADC_UPDT'))).withColumn('ADC_UPDT', F.greatest(F.col('SOURCE_ADC_UPDT'), F.col('ENRICHMENT_ADC_UPDT')))
    return result.withColumn('_ROW_HASH_PAYLOAD', F.to_json(F.struct('*'))).withColumn('PIPELINE_UPDT_DT_TM', F.current_timestamp()).withColumn('ROW_HASH', F.sha2(F.col('_ROW_HASH_PAYLOAD'), 256)).drop('_ROW_HASH_PAYLOAD')

def add_manual_omop_mappings_coded(df: DataFrame, barts_mapfile: Optional[DataFrame]=None, concepts: Optional[DataFrame]=None, config: MapCodedEventsConfig=DEFAULT_CODED_EVENTS_CONFIG) -> DataFrame:
    """Compatibility replacement. Alternative candidates remain available via the bridge builder."""
    _, summary = build_omop_mapping_outputs(df, config, barts_mapfile, concepts)
    return attach_omop_mapping_summary(df, summary)

def validate_coded_events_dataframe(df: DataFrame, label: str='coded events') -> Dict[str, int]:
    stats = df.agg(F.count('*').alias('row_count'), F.countDistinct(F.struct(*[F.col(column) for column in TARGET_KEYS])).alias('distinct_key_count'), F.sum(F.when(reduce(lambda left, right: left | right, [F.col(column).isNull() for column in TARGET_KEYS]), 1).otherwise(0)).alias('null_key_count')).collect()[0]
    result = {key: int(stats[key] or 0) for key in stats.asDict()}
    if result['null_key_count']:
        raise ValueError(f"{label}: {result['null_key_count']} rows have a null source key")
    if result['row_count'] != result['distinct_key_count']:
        raise ValueError(f"{label}: key uniqueness failed: {result['row_count']} rows but {result['distinct_key_count']} distinct EVENT_ID + SEQUENCE_NBR keys")
    return result

def validate_omop_bridge_dataframe(df: DataFrame, label: str='OMOP bridge') -> Dict[str, int]:
    stats = df.agg(F.count('*').alias('row_count'), F.countDistinct(F.struct(*[F.col(column) for column in BRIDGE_KEYS])).alias('distinct_key_count')).collect()[0]
    result = {key: int(stats[key] or 0) for key in stats.asDict()}
    if result['row_count'] != result['distinct_key_count']:
        raise ValueError(f'{label}: duplicate candidate keys detected')
    return result

def _align_to_schema(df: DataFrame, schema: T.StructType) -> DataFrame:
    expressions = []
    for field in schema.fields:
        if field.name in df.columns:
            expressions.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            expressions.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(*expressions)

def _merge_changed_rows(source_df: DataFrame, target_table: str, key_columns: Sequence[str], delete_column: str='_DELETE_ROW') -> None:
    target = DeltaTable.forName(spark, target_table)
    target_columns = spark.table(target_table).columns
    assignments = {column: f'source.`{column}`' for column in target_columns}
    condition = ' AND '.join((f'target.`{column}` <=> source.`{column}`' for column in key_columns))
    merge = target.alias('target').merge(source_df.alias('source'), condition)
    merge = merge.whenMatchedDelete(condition=f'source.{delete_column} = true')
    if 'ROW_HASH' in target_columns and 'ROW_HASH' in source_df.columns:
        merge = merge.whenMatchedUpdate(condition=f'source.{delete_column} = false AND NOT (target.ROW_HASH <=> source.ROW_HASH)', set=assignments)
    else:
        merge = merge.whenMatchedUpdate(condition=f'source.{delete_column} = false', set=assignments)
    merge.whenNotMatchedInsert(condition=f'source.{delete_column} = false', values=assignments).execute()

def _synchronize_event_subset(new_rows: DataFrame, affected_event_ids: DataFrame, target_table: str, key_columns: Sequence[str]) -> None:
    target_df = spark.table(target_table)
    target_schema = target_df.schema
    new_aligned = _align_to_schema(new_rows, target_schema)
    ids = affected_event_ids.select(F.col('EVENT_ID').cast('long').alias('EVENT_ID')).distinct()
    existing_keys = target_df.join(ids, 'EVENT_ID', 'left_semi').select(*key_columns)
    stale_keys = existing_keys.join(new_aligned.select(*key_columns), list(key_columns), 'left_anti')
    tombstone_expressions = []
    for field in target_schema.fields:
        if field.name in key_columns:
            tombstone_expressions.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            tombstone_expressions.append(F.lit(None).cast(field.dataType).alias(field.name))
    tombstones = stale_keys.select(*tombstone_expressions).withColumn('_DELETE_ROW', F.lit(True))
    active = new_aligned.withColumn('_DELETE_ROW', F.lit(False))
    merge_source = active.unionByName(tombstones)
    try:
        _merge_changed_rows(merge_source, target_table, key_columns)
    finally:
        None

def _write_replacement_table(df: DataFrame, table_name: str) -> None:
    if bronze_is_primary_table(table_name):
        df = bronze_project_contract(df, table_name)
    df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('delta.enableChangeDataFeed', 'true').saveAsTable(table_name)

def rebuild_map_coded_events_v2(config: MapCodedEventsConfig=DEFAULT_CODED_EVENTS_CONFIG, create_backup: bool=True, configure_source_retention: bool=True) -> Dict[str, object]:
    """One-time validated migration. This is the only function that replaces the full target."""
    run_id = str(uuid4())
    started_at = datetime.now(timezone.utc)
    caches: List[DataFrame] = []
    base: Optional[DataFrame] = None
    final: Optional[DataFrame] = None
    bridge: Optional[DataFrame] = None
    if configure_source_retention:
        configure_map_source_cdf_retention(config)
    try:
        versions = {table: get_latest_delta_version(table) for table in config.source_tables}
        base, caches = build_coded_events_base(None, config, source_versions=versions)
        bridge, summary = build_omop_mapping_outputs(base, config, source_versions=versions)
        final = attach_omop_mapping_summary(base, summary)
        bridge = bridge
        target_stats = validate_coded_events_dataframe(final, 'map_coded_events rebuild')
        bridge_stats = validate_omop_bridge_dataframe(bridge, 'map_coded_events OMOP bridge rebuild')
        if create_backup and _table_exists(config.target_table):
            backup_suffix = started_at.strftime('%Y%m%d_%H%M%S')
            backup_table = f'{config.target_table}__pre_v2_{backup_suffix}'
            spark.sql(f'CREATE TABLE {_sql_name(backup_table)} SHALLOW CLONE {_sql_name(config.target_table)}')
        else:
            backup_table = None
        _write_replacement_table(final, config.target_table)
        _write_replacement_table(bridge, config.bridge_table)
        _configure_delta_table(config.target_table, MAP_CODED_EVENTS_COMMENT, TARGET_KEYS, config.cdf_retention)
        _configure_delta_table(config.bridge_table, MAP_CODED_EVENTS_BRIDGE_COMMENT, TARGET_KEYS + ['MAPPING_GROUP'], config.cdf_retention)
        write_map_checkpoints(versions, run_id, config)
        return {'run_id': run_id, 'started_at': started_at.isoformat(), 'target_stats': target_stats, 'bridge_stats': bridge_stats, 'backup_table': backup_table, 'checkpoint_versions': versions}
    finally:
        if final is not None:
            None
        if bridge is not None:
            None
        for cached in caches:
            None

def _changed_codes_to_events(changed_codes: DataFrame, config: MapCodedEventsConfig) -> Optional[DataFrame]:
    if changed_codes is None or _is_empty(changed_codes):
        return None
    target = spark.table(config.target_table)
    available_code_columns = [
        column for column in MAP_CODE_COLUMNS
        if column in target.columns
    ]
    if not available_code_columns:
        return None
    code_rows = target.select(
        'EVENT_ID',
        F.explode_outer(
            F.array(*[
                F.col(column).cast('long')
                for column in available_code_columns
            ])
        ).alias('CODE_VALUE'),
    ).where(F.col('CODE_VALUE').isNotNull())
    return code_rows.join(
        F.broadcast(changed_codes),
        'CODE_VALUE',
        'left_semi',
    ).select('EVENT_ID').distinct()

def _changed_catalogs_to_events(changed_catalogs: DataFrame, config: MapCodedEventsConfig) -> Optional[DataFrame]:
    if changed_catalogs is None or _is_empty(changed_catalogs):
        return None
    catalog_rows = spark.table(config.target_table).select('EVENT_ID', F.explode_outer(F.array(F.col('CATALOG_CD').cast('long'), F.col('PARENT_CATALOG_CD').cast('long'))).alias('CATALOG_CD')).where(F.col('CATALOG_CD').isNotNull())
    return catalog_rows.join(F.broadcast(changed_catalogs), 'CATALOG_CD', 'left_semi').select('EVENT_ID').distinct()

def refresh_all_coded_event_omop_mappings(config: MapCodedEventsConfig=DEFAULT_CODED_EVENTS_CONFIG, source_versions: Optional[Dict[str, int]]=None) -> Dict[str, int]:
    """Correctly propagate manual-map or OMOP-concept changes without rereading raw clinical events."""
    existing = spark.table(config.target_table)
    base = existing.drop(*[column for column in MAPPING_SUMMARY_COLUMNS + ['ROW_HASH', 'PIPELINE_UPDT_DT_TM'] if column in existing.columns])
    bridge, summary = build_omop_mapping_outputs(base, config, source_versions=source_versions)
    final = attach_omop_mapping_summary(base, summary)
    bridge = bridge
    try:
        target_stats = validate_coded_events_dataframe(final, 'OMOP mapping refresh')
        bridge_stats = validate_omop_bridge_dataframe(bridge, 'OMOP mapping bridge refresh')
        _write_replacement_table(bridge, config.bridge_table)
        source = _align_to_schema(final, existing.schema).withColumn('_DELETE_ROW', F.lit(False))
        _merge_changed_rows(source, config.target_table, TARGET_KEYS)
        return {'target_rows': target_stats['row_count'], 'bridge_rows': bridge_stats['row_count']}
    finally:
        None
        None

def process_coded_events_incremental_v2(config: MapCodedEventsConfig=DEFAULT_CODED_EVENTS_CONFIG) -> Dict[str, object]:
    """Version-checkpointed incremental replacement for the original coded-events function."""
    if not _table_exists(config.target_table) or not _table_exists(config.bridge_table):
        raise RuntimeError('The v2 target and bridge must be initialized once with rebuild_map_coded_events_v2().')
    ensure_map_checkpoint_table(config)
    checkpoints = get_map_checkpoints(config)
    missing = [table for table in config.source_tables if table not in checkpoints]
    if missing:
        raise RuntimeError('Missing source-version checkpoints for: ' + ', '.join(missing) + '. Run rebuild_map_coded_events_v2() once before enabling the incremental entry point.')
    current_versions = {table: get_latest_delta_version(table) for table in config.source_tables}
    changed_ranges = {table: (checkpoints[table] + 1, current_versions[table]) for table in config.source_tables if current_versions[table] > checkpoints[table]}
    if not changed_ranges:
        return {'status': 'NO_CHANGES', 'checkpoint_versions': current_versions}
    run_id = str(uuid4())
    cdf: Dict[str, DataFrame] = {table: read_cdf_versions(table, start, end) for table, (start, end) in changed_ranges.items()}
    coded_changed = cdf.get(config.coded_result_source)
    clinical_changed = cdf.get(config.clinical_event_source)
    code_changed = cdf.get(config.code_value_source)
    catalog_changed = cdf.get(config.order_catalog_source)
    map_changes = cdf.get(config.manual_map_source)
    concept_changes = cdf.get(config.concept_source)
    maps_changed = map_changes is not None and (not _is_empty(map_changes))
    concepts_changed = concept_changes is not None and (not _is_empty(concept_changes))
    coded_event_ids = None if coded_changed is None else coded_changed.select(F.col('EVENT_ID').cast('long').alias('EVENT_ID'))
    clinical_event_ids = None if clinical_changed is None else clinical_changed.select(F.col('EVENT_ID').cast('long').alias('EVENT_ID'))
    child_event_ids = None
    if clinical_event_ids is not None and (not _is_empty(clinical_event_ids)):
        changed_parents = clinical_event_ids.distinct().select(F.col('EVENT_ID').alias('_CHANGED_PARENT_EVENT_ID'))
        child_event_ids = spark.table(config.target_table).join(F.broadcast(changed_parents), F.col('PARENT_EVENT_ID') == F.col('_CHANGED_PARENT_EVENT_ID'), 'left_semi').select('EVENT_ID')
    changed_code_events = None
    if code_changed is not None:
        changed_codes = code_changed.select(F.col('CODE_VALUE').cast('long').alias('CODE_VALUE')).where(F.col('CODE_VALUE').isNotNull()).distinct()
        changed_code_events = _changed_codes_to_events(changed_codes, config)
    changed_catalog_events = None
    if catalog_changed is not None:
        changed_catalogs = catalog_changed.select(F.col('CATALOG_CD').cast('long').alias('CATALOG_CD')).where(F.col('CATALOG_CD').isNotNull()).distinct()
        changed_catalog_events = _changed_catalogs_to_events(changed_catalogs, config)
    affected_event_ids = _union_distinct([coded_event_ids, clinical_event_ids, child_event_ids, changed_code_events, changed_catalog_events])
    refreshed_rows = 0
    refreshed_bridge_rows = 0
    caches: List[DataFrame] = []
    final: Optional[DataFrame] = None
    bridge: Optional[DataFrame] = None
    affected_event_count = 0
    try:
        if affected_event_ids is not None:
            affected_event_ids = affected_event_ids
            affected_event_count = affected_event_ids.count()
        if affected_event_count > 0:
            use_broadcast = affected_event_count <= config.broadcast_event_id_limit
            base, caches = build_coded_events_base(affected_event_ids, config, source_versions=current_versions, broadcast_event_ids=use_broadcast)
            bridge, summary = build_omop_mapping_outputs(base, config, source_versions=current_versions)
            final = attach_omop_mapping_summary(base, summary)
            bridge = bridge
            stats = validate_coded_events_dataframe(final, 'incremental coded events')
            bridge_stats = validate_omop_bridge_dataframe(bridge, 'incremental OMOP bridge')
            refreshed_rows = stats['row_count']
            refreshed_bridge_rows = bridge_stats['row_count']
            _synchronize_event_subset(final, affected_event_ids, config.target_table, TARGET_KEYS)
            _synchronize_event_subset(bridge, affected_event_ids, config.bridge_table, BRIDGE_KEYS)
        if maps_changed or concepts_changed:
            mapping_stats = refresh_all_coded_event_omop_mappings(config, source_versions=current_versions)
            refreshed_rows = _pipeline_builtins.max(refreshed_rows, mapping_stats['target_rows'])
            refreshed_bridge_rows = _pipeline_builtins.max(refreshed_bridge_rows, mapping_stats['bridge_rows'])
        write_map_checkpoints(current_versions, run_id, config)
        return {'status': 'SUCCESS', 'run_id': run_id, 'changed_ranges': changed_ranges, 'affected_event_count': affected_event_count, 'refreshed_target_rows': refreshed_rows, 'refreshed_bridge_rows': refreshed_bridge_rows, 'full_mapping_refresh': bool(maps_changed or concepts_changed), 'checkpoint_versions': current_versions}
    finally:
        if final is not None:
            None
        if bridge is not None:
            None
        if affected_event_ids is not None:
            None
        for cached in caches:
            None

def process_coded_events_incremental(config: MapCodedEventsConfig=DEFAULT_CODED_EVENTS_CONFIG) -> Dict[str, object]:
    """Drop-in entry point that intentionally overrides the original Map Pipeline function."""
    return process_coded_events_incremental_v2(config)

def deploy_map_coded_events_improvements(config: MapCodedEventsConfig=DEFAULT_CODED_EVENTS_CONFIG, create_backup: bool=True) -> Dict[str, object]:
    """Explicit one-time deployment wrapper. Nothing calls this automatically."""
    return rebuild_map_coded_events_v2(config=config, create_backup=create_backup, configure_source_retention=False)

try:
    _pipeline_run_recoverable('map_coded_events', _PIPELINE_FULL_REFRESH, lambda: process_coded_events_incremental(), lambda: rebuild_map_coded_events_v2(create_backup=False, configure_source_retention=False))
    _PIPELINE_UPDATED_TARGETS.extend(['4_prod.bronze.map_coded_events'])
    _pipeline_mark_component_complete('map_coded_events', ['4_prod.bronze.map_coded_events'])
    _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_coded_events'})
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

