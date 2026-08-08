# Databricks notebook source
# MAGIC %md
# MAGIC # Map 40 — Maternity and Longitudinal
# MAGIC
# MAGIC Components: pregnancy, birth, VTE, family history, patient journey and implant details. This notebook is executed by `map_pipeline` in the shared map runtime.

# COMMAND ----------

if "_PIPELINE_RUN_ID" not in globals():
    raise RuntimeError("Run this component through map_pipeline so shared contracts, checkpoints and audit state are initialized.")


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

_pipeline_component_start("map_mat_pregnancy")
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
from typing import Dict, List, Optional, Sequence, Tuple
import json
import uuid
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

@dataclass(frozen=True)
class MapMatPregnancyConfig:
    mat_pregnancy_table: str = '4_prod.raw.mat_pregnancy'
    msds_table: str = '4_prod.raw.msds101pregbook'
    mat_birth_table: str = '4_prod.raw.mat_birth'
    target_table: str = '4_prod.bronze.map_mat_pregnancy'
    unmatched_msds_table: str = '4_prod.bronze.map_mat_pregnancy_msds_unmatched'
    audit_table: str = '6_mgmt.bronze.map_mat_pregnancy_pipeline_audit'
    schema_version: str = '2.0.0'
MAP_MAT_PREGNANCY_CONFIG = MapMatPregnancyConfig()
map_mat_pregnancy_comment = 'Lossless pregnancy foundation at one row per MAT_PREGNANCY source key. Source soft-deleted rows are retained and explicitly flagged. MAT, MSDS and birth-source values, provenance, conflict indicators and pipeline audit fields are retained without clinical row filtering.'
unmatched_msds_comment = 'Current MSDS101PREGBOOK rows that cannot be linked to a MAT_PREGNANCY source row by pregnancy ID. Rows are retained for reconciliation rather than being silently discarded.'

def _field(name: str, data_type: T.DataType, comment: str, nullable: bool=True) -> T.StructField:
    return T.StructField(name, data_type, nullable, metadata={'comment': comment})
schema_map_mat_pregnancy = T.StructType([_field('Person_ID', T.LongType(), 'Unique MAT person identifier for the mother.', nullable=False), _field('Pregnancy_ID', T.LongType(), 'Unique pregnancy episode identifier.', nullable=False), _field('MRN', T.StringType(), 'Local medical record number supplied by MAT_PREGNANCY.'), _field('NHS_Number', T.StringType(), 'NHS number supplied by MAT_PREGNANCY.'), _field('Mother_DOB', T.TimestampType(), 'Mother date/time of birth parsed losslessly from MAT_PREGNANCY.'), _field('Gravida_NBR', T.DoubleType(), 'Total pregnancies including the current pregnancy.'), _field('Parity', T.DoubleType(), 'Source parity value.'), _field('PrevLiveBirth_NBR', T.DoubleType(), 'Previous live births supplied by MSDS.'), _field('PrevMiscarriages_NBR', T.DoubleType(), 'Previous spontaneous abortions/miscarriages from HX_PREG_OUTCOME_SPONT_ABORT_NBR.'), _field('PrevStillBirth_NBR', T.DoubleType(), 'Previous stillbirths supplied by MSDS.'), _field('FirstAntenatalAPPTDate', T.TimestampType(), 'Best available booking/first antenatal assessment date; MAT preferred, then MSDS.'), _field('GestAgePregStart', T.StringType(), 'Unchanged source gestational-age-at-start text, including weeks and days.'), _field('GestAgePregEnd', T.StringType(), 'Unchanged source gestational-age-at-end text, including weeks and days.'), _field('LastMensPeriodDate', T.TimestampType(), 'Last menstrual period date supplied by MSDS.'), _field('AlcoholUnitsPerWeek', T.DoubleType(), 'Alcohol units per week supplied by MAT_PREGNANCY.'), _field('SmokingBooking_CD', T.IntegerType(), 'MAT smoking status at booking nomenclature identifier.'), _field('SmokingBooking_DESC', T.StringType(), 'MAT smoking status at booking description.'), _field('SmokingDelivery_CD', T.IntegerType(), 'MAT smoking status at delivery nomenclature identifier.'), _field('SmokingDelivery_DESC', T.StringType(), 'MAT smoking status at delivery description.'), _field('SubstanceUse_CD', T.StringType(), 'MAT recreational substance-use nomenclature identifier(s).'), _field('SubstanceUse_DESC', T.StringType(), 'MAT recreational substance-use description.'), _field('ExpectedDeliveryDate', T.TimestampType(), 'Best available estimated delivery date; MAT final EDD preferred, then MSDS agreed EDD.'), _field('Height_CM', T.DoubleType(), 'Booking height in centimetres; retained as DOUBLE to match the source.'), _field('Weight_KG', T.DoubleType(), 'Booking weight in kilograms; retained as DOUBLE to match the source.'), _field('BMI', T.DoubleType(), 'Booking BMI; retained as DOUBLE to match the source.'), _field('FolicAcidSupp_CD', T.StringType(), 'MSDS folic-acid supplement status code.'), _field('FolicAcidSupp_DESC', T.StringType(), 'Description of the MSDS folic-acid supplement status code.'), _field('LaborOnsetMethod_CD', T.IntegerType(), 'MAT method-of-labour-onset nomenclature identifier.'), _field('LaborOnsetMethod_DESC', T.StringType(), 'MAT method-of-labour-onset description.'), _field('Augmentation_CD', T.IntegerType(), 'MAT labour augmentation nomenclature identifier.'), _field('Augmentation_DESC', T.StringType(), 'MAT labour augmentation description.'), _field('AnalgesiaDelivery_CD', T.StringType(), 'MAT delivery analgesia nomenclature identifier(s).'), _field('AnalgesiaDelivery_DESC', T.StringType(), 'MAT delivery analgesia description.'), _field('AnalgesiaLabour_CD', T.StringType(), 'MAT labour analgesia nomenclature identifier(s).'), _field('AnalgesiaLabour_DESC', T.StringType(), 'MAT labour analgesia description.'), _field('AnaesthesiaLabour_CD', T.StringType(), 'MAT labour anaesthesia nomenclature identifier(s).'), _field('AnaesthesiaLabour_DESC', T.StringType(), 'MAT labour anaesthesia description.'), _field('PerinealTrauma_CD', T.StringType(), 'MAT perineal-trauma nomenclature identifier(s).'), _field('PerinealTrauma_DESC', T.StringType(), 'MAT perineal-trauma description.'), _field('Episiotomy_CD', T.IntegerType(), 'MAT episiotomy nomenclature identifier.'), _field('Episiotomy_DESC', T.StringType(), 'MAT episiotomy description.'), _field('BloodLoss', T.DoubleType(), 'MAT total blood loss in millilitres; retained as DOUBLE to match the source.'), _field('ADC_UPDT', T.TimestampType(), 'Greatest available MAT, MSDS or birth-source ADC update timestamp for the materialized row.'), _field('PregnancyType_CD', T.StringType(), 'Raw MAT SNOMED CT pregnancy-type code; retained without assuming it is authoritative.'), _field('PregnancyType_DESC', T.StringType(), 'Raw MAT pregnancy-type description.'), _field('FirstAntenatalAPPTDate_MAT', T.TimestampType(), 'MAT first antenatal assessment timestamp.'), _field('FirstAntenatalAPPTDate_MSDS', T.TimestampType(), 'MSDS booking appointment timestamp.'), _field('FirstAntenatalAPPTDate_Source', T.StringType(), 'Source used for FirstAntenatalAPPTDate: MAT, MSDS or null.'), _field('ExpectedDeliveryDate_MAT', T.TimestampType(), 'MAT final estimated delivery timestamp.'), _field('ExpectedDeliveryDate_MSDS', T.TimestampType(), 'MSDS agreed estimated delivery timestamp.'), _field('ExpectedDeliveryDate_Source', T.StringType(), 'Source used for ExpectedDeliveryDate: MAT, MSDS or null.'), _field('ROMDate', T.TimestampType(), 'MAT rupture-of-membranes timestamp.'), _field('LabOnsetDate', T.TimestampType(), 'MAT labour-onset timestamp.'), _field('FullDilatationDate', T.TimestampType(), 'MAT full-dilatation timestamp.'), _field('GestationalDeathConfirmedDate', T.TimestampType(), 'MAT timestamp at which gestational death was confirmed.'), _field('PregnancyFirstContactDate', T.TimestampType(), 'MSDS date of first contact for pregnancy care.'), _field('MaternityServiceDischargeDate', T.TimestampType(), 'MSDS date on which maternity-service care ended.'), _field('MaternityServiceDischargeReason', T.StringType(), 'MSDS primary reason maternity-service care ended.'), _field('EDDMethod', T.StringType(), 'MSDS method used to establish the agreed EDD.'), _field('ReasonLateBooking', T.StringType(), 'MSDS reason for booking after 12 weeks and 6 days.'), _field('FirstContactCareProfessionalType', T.StringType(), 'MSDS care-professional type at first pregnancy contact.'), _field('GestAgePregStartWeeks', T.IntegerType(), 'Parsed signed week component of GestAgePregStart.'), _field('GestAgePregStartDays', T.IntegerType(), 'Parsed signed day component of GestAgePregStart.'), _field('GestAgePregStartTotalDays', T.IntegerType(), 'Parsed signed total days from GestAgePregStart.'), _field('GestAgePregEndWeeks', T.IntegerType(), 'Parsed signed week component of GestAgePregEnd.'), _field('GestAgePregEndDays', T.IntegerType(), 'Parsed signed day component of GestAgePregEnd.'), _field('GestAgePregEndTotalDays', T.IntegerType(), 'Parsed signed total days from GestAgePregEnd.'), _field('PreviousCaesareanSections', T.DoubleType(), 'MSDS number of previous pregnancies delivered by caesarean.'), _field('PreviousLossesUnder24Weeks', T.DoubleType(), 'MSDS combined terminations and losses before 24 completed weeks; not synonymous with miscarriage.'), _field('PrevEctopicPregnancies', T.DoubleType(), 'MAT previous ectopic-pregnancy count.'), _field('PrevTherapeuticAbortionsMedical', T.DoubleType(), 'MAT previous medical therapeutic-abortion count.'), _field('PrevTherapeuticAbortionsSurgical', T.DoubleType(), 'MAT previous surgical therapeutic-abortion count.'), _field('PrevElectiveCaesareans', T.DoubleType(), 'MAT previous elective-caesarean count.'), _field('PrevEmergencyCaesareans', T.DoubleType(), 'MAT previous emergency-caesarean count.'), _field('PreviousPregnancyProblem_IND', T.DoubleType(), 'MAT indicator of problems in a previous pregnancy.'), _field('Unbooked_CD', T.IntegerType(), 'MAT unbooked-pregnancy nomenclature identifier.'), _field('Unbooked_DESC', T.StringType(), 'MAT unbooked-pregnancy description.'), _field('ROMMethod_CD', T.IntegerType(), 'MAT rupture-of-membranes method nomenclature identifier.'), _field('ROMMethod_DESC', T.StringType(), 'MAT rupture-of-membranes method description.'), _field('LabourTotalMinutes', T.DoubleType(), 'MAT total labour duration in minutes.'), _field('AnaesthesiaDelivery_CD', T.StringType(), 'MAT delivery anaesthesia nomenclature identifier(s).'), _field('AnaesthesiaDelivery_DESC', T.StringType(), 'MAT delivery anaesthesia description.'), _field('BirthingPoolUse_CD', T.StringType(), 'MAT birthing-pool-use nomenclature identifier(s).'), _field('BirthingPoolUse_DESC', T.StringType(), 'MAT birthing-pool-use description.'), _field('PlacentaAppearance_CD', T.IntegerType(), 'MAT placenta-appearance nomenclature identifier.'), _field('PlacentaAppearance_DESC', T.StringType(), 'MAT placenta-appearance description.'), _field('PlacentaManualRemoval_IND', T.DoubleType(), 'MAT manual-removal-of-placenta indicator.'), _field('PlannedDeliveryLocationPreg_CD', T.IntegerType(), 'MAT planned delivery location during pregnancy nomenclature identifier.'), _field('PlannedDeliveryLocationPreg_DESC', T.StringType(), 'MAT planned delivery location during pregnancy description.'), _field('PlannedDeliveryLocationLabour_CD', T.IntegerType(), 'MAT planned delivery location during labour nomenclature identifier.'), _field('PlannedDeliveryLocationLabour_DESC', T.StringType(), 'MAT planned delivery location during labour description.'), _field('PregnancyRelatedProblems_CD', T.StringType(), 'MAT pregnancy-related problem SNOMED CT code(s).'), _field('PregnancyRelatedProblems_DESC', T.StringType(), 'MAT pregnancy-related problem description.'), _field('NonPregnancyRelatedProblems_CD', T.StringType(), 'MAT non-pregnancy-related problem SNOMED CT code(s).'), _field('NonPregnancyRelatedProblems_DESC', T.StringType(), 'MAT non-pregnancy-related problem description.'), _field('TobaccoUsePerDay', T.DoubleType(), 'MAT tobacco-use quantity per day.'), _field('SmokeLivesWith_CD', T.IntegerType(), 'MAT nomenclature identifier for living with a smoker.'), _field('SmokeLivesWith_DESC', T.StringType(), 'MAT description for living with a smoker.'), _field('KnownSocialServices_CD', T.IntegerType(), 'MAT nomenclature identifier for being known to social services.'), _field('KnownSocialServices_DESC', T.StringType(), 'MAT description for being known to social services.'), _field('SupportStatus_CD', T.IntegerType(), 'MAT support-status nomenclature identifier.'), _field('SupportStatus_DESC', T.StringType(), 'MAT support-status description.'), _field('ComplexSocialFactors_CD', T.StringType(), 'MSDS complex-social-factors indicator.'), _field('MentalHealthPredictionDetection_CD', T.StringType(), 'MSDS indicator that recommended mental-health prediction/detection questions were asked.'), _field('DisabilityMother_CD', T.StringType(), 'MSDS mother disability indicator.'), _field('PreferredLanguage_CD', T.StringType(), 'MSDS preferred-language code.'), _field('SupportStatusMother_CD', T.StringType(), 'MSDS mother support-status indicator.'), _field('EmploymentStatusMother_CD', T.StringType(), 'MSDS mother employment-status code.'), _field('EmploymentStatusPartner_CD', T.StringType(), 'MSDS partner employment-status code.'), _field('MSDS_SubstanceUseStatus', T.StringType(), 'Unchanged MSDS substance-use status.'), _field('MSDS_SmokingStatus', T.StringType(), 'Unchanged MSDS smoking status.'), _field('MSDS_CigarettesPerDay', T.StringType(), 'Unchanged MSDS cigarettes-per-day value.'), _field('MSDS_AlcoholUnitsPerWeek', T.StringType(), 'Unchanged MSDS alcohol-units-per-week value.'), _field('MSDS_PersonWeight', T.StringType(), 'Unchanged MSDS booking-weight value.'), _field('MSDS_PersonHeight', T.StringType(), 'Unchanged MSDS booking-height value.'), _field('LPIDMother', T.StringType(), 'MSDS local patient identifier for the mother; not assumed to be an MRN.'), _field('MSDS_SourceReferral', T.StringType(), 'MSDS source of referral to maternity services.'), _field('BirthRecordCount', T.LongType(), 'Count of all MAT_BIRTH source rows for the pregnancy, including tombstones.'), _field('ActiveBirthRecordCount', T.LongType(), 'Count of MAT_BIRTH rows whose DELETE_IND is zero.'), _field('DistinctActiveChildCount', T.LongType(), 'Distinct active MAT_BIRTH pregnancy-child identifiers.'), _field('RecordedBirthCount', T.IntegerType(), 'Maximum active MAT_BIRTH BIRTH_NBR value.'), _field('MultipleBirth_IND', T.BooleanType(), 'True when MAT_BIRTH indicates more than one child or birth.'), _field('PregnancyTypeBirthConflict_IND', T.BooleanType(), 'True when MAT says singleton but MAT_BIRTH indicates a multiple birth.'), _field('BIRTH_ADC_UPDT', T.TimestampType(), 'Greatest MAT_BIRTH ADC update timestamp for the pregnancy.'), _field('MAT_DELETE_IND', T.IntegerType(), 'Unchanged MAT_PREGNANCY soft-delete indicator.'), _field('MAT_CTRL_ID', T.IntegerType(), 'MAT_PREGNANCY control identifier.'), _field('MAT_RECORD_UPDATED_DT', T.TimestampType(), 'MAT_PREGNANCY record-updated timestamp.'), _field('MAT_ADC_UPDT', T.TimestampType(), 'MAT_PREGNANCY ADC update timestamp.'), _field('MSDS_CTRL_ID', T.IntegerType(), 'MSDS control identifier.'), _field('MSDS_RECORD_UPDATED_DT', T.TimestampType(), 'MSDS record-updated timestamp.'), _field('MSDS_ADC_UPDT', T.TimestampType(), 'MSDS ADC update timestamp.'), _field('MSDS_SOURCE_SYSTEM', T.StringType(), 'MSDS source-system value.'), _field('MSDS_IS_VALID', T.StringType(), 'Unchanged MSDS validity value; no validity filter is applied.'), _field('MAT_SOURCE_PRESENT_IND', T.BooleanType(), 'True when the current MAT_PREGNANCY snapshot contains the key.'), _field('MSDS_SOURCE_PRESENT_IND', T.BooleanType(), 'True when the current MSDS snapshot contains the pregnancy ID.'), _field('BIRTH_SOURCE_PRESENT_IND', T.BooleanType(), 'True when MAT_BIRTH contains at least one row for the pregnancy.'), _field('SOURCE_DELETED_IND', T.BooleanType(), 'True for a MAT soft-delete or a source key that disappeared physically.'), _field('SOURCE_MISSING_IND', T.BooleanType(), 'True when a previously materialized MAT key is absent from the current source snapshot.'), _field('FirstAntenatalDateConflict_IND', T.BooleanType(), 'True when non-null MAT and MSDS antenatal dates differ by calendar date.'), _field('ExpectedDeliveryDateConflict_IND', T.BooleanType(), 'True when non-null MAT and MSDS EDDs differ by calendar date.'), _field('MSDSNewerThanMAT_IND', T.BooleanType(), 'True when the MSDS ADC timestamp is later than the MAT ADC timestamp.'), _field('GravidaParityConflict_IND', T.BooleanType(), 'True when source gravida/parity values are negative or parity exceeds gravida.'), _field('DateParseFailure_IND', T.BooleanType(), 'True when a nonblank MAT source date cannot be parsed as a timestamp.'), _field('MAT_SOURCE_VERSION', T.LongType(), 'MAT_PREGNANCY Delta version last materialized for this row.'), _field('MSDS_SOURCE_VERSION', T.LongType(), 'MSDS Delta version last materialized for this row.'), _field('MAT_BIRTH_SOURCE_VERSION', T.LongType(), 'MAT_BIRTH Delta version last materialized for this row.'), _field('TRIGGER_SOURCES', T.StringType(), 'Comma-separated source snapshots represented by the row.'), _field('ROW_HASH', T.LongType(), 'Stable hash of source-derived row content used to avoid unchanged rewrites.'), _field('PIPELINE_RUN_ID', T.StringType(), 'Pipeline run identifier that last materially changed the row.'), _field('PIPELINE_UPDT_DT_TM', T.TimestampType(), 'Timestamp when the row was last materially changed by this pipeline.'), _field('SCHEMA_VERSION', T.StringType(), 'Replacement schema version used to materialize the row.')])
schema_unmatched_msds = T.StructType([_field('UNMATCHED_KEY', T.StringType(), 'Stable key for the unmatched source row.', nullable=False), _field('UNMATCHED_REASON', T.StringType(), 'INVALID_PREGNANCY_ID or NO_MAT_PREGNANCY_MATCH.', nullable=False), _field('PREGNANCYID_RAW', T.StringType(), 'Unchanged MSDS pregnancy identifier.'), _field('Pregnancy_ID', T.LongType(), 'MSDS pregnancy identifier after checked numeric parsing.'), _field('LPIDMother', T.StringType(), 'MSDS local patient identifier for the mother.'), _field('AntenatalAppointmentDate', T.TimestampType(), 'MSDS booking appointment date.'), _field('PregnancyFirstContactDate', T.TimestampType(), 'MSDS first pregnancy-contact date.'), _field('ExpectedDeliveryDate', T.TimestampType(), 'MSDS agreed EDD.'), _field('LastMensPeriodDate', T.TimestampType(), 'MSDS last menstrual period date.'), _field('FolicAcidSupplement_CD', T.StringType(), 'MSDS folic-acid supplement code.'), _field('PreviousLiveBirths', T.DoubleType(), 'MSDS previous live-birth count.'), _field('PreviousStillBirths', T.DoubleType(), 'MSDS previous stillbirth count.'), _field('PreviousLossesUnder24Weeks', T.DoubleType(), 'MSDS terminations/losses under 24 weeks.'), _field('PreviousCaesareanSections', T.DoubleType(), 'MSDS previous-caesarean count.'), _field('SOURCE_SYSTEM', T.StringType(), 'MSDS source system.'), _field('IS_VALID', T.StringType(), 'Unchanged MSDS validity value.'), _field('RECORD_UPDATED_DT', T.TimestampType(), 'MSDS record-updated timestamp.'), _field('ADC_UPDT', T.TimestampType(), 'MSDS ADC update timestamp.'), _field('MSDS_SOURCE_VERSION', T.LongType(), 'MSDS Delta version used for the snapshot.'), _field('ROW_HASH', T.LongType(), 'Stable hash used to avoid unchanged rewrites.'), _field('PIPELINE_RUN_ID', T.StringType(), 'Pipeline run identifier that last changed the row.'), _field('PIPELINE_UPDT_DT_TM', T.TimestampType(), 'Pipeline materialization timestamp.')])
_PIPELINE_HASH_EXCLUSIONS = {'MAT_SOURCE_VERSION', 'MSDS_SOURCE_VERSION', 'MAT_BIRTH_SOURCE_VERSION', 'TRIGGER_SOURCES', 'ROW_HASH', 'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM', 'SCHEMA_VERSION'}

def _sql_identifier(name: str) -> str:
    tick = chr(96)
    return '.'.join((f'{tick}{part.replace(tick, tick + tick)}{tick}' for part in name.split('.')))

def _escape_sql_string(value: str) -> str:
    return value.replace('\\', '\\\\').replace("'", "''")

def _table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _latest_delta_version(table_name: str) -> int:
    row = spark.sql(f'DESCRIBE HISTORY {_sql_identifier(table_name)} LIMIT 1').select('version').first()
    if row is None:
        raise RuntimeError(f'No Delta history returned for {table_name}')
    return int(row['version'])

def _capture_source_versions(config: MapMatPregnancyConfig) -> Dict[str, int]:
    return {config.mat_pregnancy_table: _latest_delta_version(config.mat_pregnancy_table), config.msds_table: _latest_delta_version(config.msds_table), config.mat_birth_table: _latest_delta_version(config.mat_birth_table)}

def _m40_hardening_sql_identifier(table_name: str) -> str:
    return '.'.join('`' + part.replace('`', '``') + '`' for part in table_name.split('.'))

def _m40_hardening_snapshot_fallback_allowed(exc: BaseException) -> bool:
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

def _m40_hardening_read_pinned_snapshot(table_name: str, version: int):
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
        if not _m40_hardening_snapshot_fallback_allowed(exc):
            raise
        row = (
            spark.sql('DESCRIBE HISTORY ' + _m40_hardening_sql_identifier(table_name) + ' LIMIT 1')
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

def _m40_hardening_validate_cdf(changes):
    # Keep lazy Spark Connect failures inside the caller's recovery block.
    _ = changes.schema
    changes.limit(1).collect()
    return changes

def _read_snapshot(table_name: str, version: int) -> DataFrame:
    return _m40_hardening_read_pinned_snapshot(table_name, int(version))

def _get_table_comment(table_name: str) -> str:
    """Read the Unity Catalog description, not a non-existent Delta 'comment' property."""
    try:
        table = spark.catalog.getTable(table_name)
        return (getattr(table, 'description', None) or '').strip()
    except Exception:
        rows = spark.sql(f'DESCRIBE TABLE EXTENDED {_sql_identifier(table_name)}').collect()
        for row in rows:
            if str(row['col_name'] or '').strip().lower() == 'comment':
                return str(row['data_type'] or '').strip()
        return ''

def _set_table_comment_if_changed(table_name: str, comment: str) -> bool:
    if _get_table_comment(table_name) == (comment or '').strip():
        return False
    spark.sql(f"COMMENT ON TABLE {_sql_identifier(table_name)} IS '{_escape_sql_string(comment)}'")
    return True

def _get_table_properties(table_name: str) -> Dict[str, str]:
    return {str(row['key']): str(row['value']) for row in spark.sql(f'SHOW TBLPROPERTIES {_sql_identifier(table_name)}').collect()}

def _set_properties_if_needed(table_name: str, desired: Dict[str, str]) -> List[str]:
    current = _get_table_properties(table_name)
    changes = {key: value for key, value in desired.items() if current.get(key) != value}
    if not changes:
        return []
    assignments = ', '.join((f"'{_escape_sql_string(key)}' = '{_escape_sql_string(value)}'" for key, value in changes.items()))
    spark.sql(f'ALTER TABLE {_sql_identifier(table_name)} SET TBLPROPERTIES ({assignments})')
    return list(changes)

def _column_definition(field: T.StructField) -> str:
    tick = chr(96)
    name = field.name.replace(tick, tick + tick)
    sql = f'{tick}{name}{tick} {field.dataType.simpleString()}'
    comment = field.metadata.get('comment', '')
    if comment:
        sql += f" COMMENT '{_escape_sql_string(comment)}'"
    return sql

def _apply_changed_column_comments(table_name: str, schema: T.StructType) -> List[str]:
    current = {field.name: field for field in spark.table(table_name).schema.fields}
    changed: List[str] = []
    for field in schema.fields:
        if field.name not in current:
            continue
        wanted = field.metadata.get('comment', '')
        present = current[field.name].metadata.get('comment', '')
        if wanted != present:
            spark.sql(f"ALTER TABLE {_sql_identifier(table_name)} ALTER COLUMN {_sql_identifier(field.name)} COMMENT '{_escape_sql_string(wanted)}'")
            changed.append(field.name)
    return changed

def _ensure_delta_table(table_name: str, schema: T.StructType, comment: str) -> Dict[str, object]:
    desired_properties = {'delta.enableChangeDataFeed': 'true', 'delta.enableRowTracking': 'true', 'delta.enableTypeWidening': 'true', 'delta.parquet.compression.codec': 'zstd'}
    if not _table_exists(table_name):
        builder = DeltaTable.createIfNotExists(spark).tableName(table_name).addColumns(schema).comment(comment)
        for key, value in desired_properties.items():
            builder = builder.property(key, value)
        builder.execute()
        comments_changed = _apply_changed_column_comments(table_name, schema)
        table_comment_changed = _set_table_comment_if_changed(table_name, comment)
        return {'created': True, 'columns_added': [field.name for field in schema.fields], 'types_widened': [], 'comments_changed': comments_changed, 'properties_changed': list(desired_properties), 'table_comment_changed': table_comment_changed}
    properties_changed = _set_properties_if_needed(table_name, desired_properties)
    current_schema = spark.table(table_name).schema
    current = {field.name: field for field in current_schema.fields}
    target = {field.name: field for field in schema.fields}
    extra = sorted(set(current) - set(target))
    if extra:
        raise RuntimeError(f'{table_name} contains columns not present in the replacement schema: {extra}. Refusing to discard them automatically.')
    missing_fields = [field for field in schema.fields if field.name not in current]
    if missing_fields:
        definitions = ',\n  '.join((_column_definition(field) for field in missing_fields))
        spark.sql(f'ALTER TABLE {_sql_identifier(table_name)} ADD COLUMNS (\n  {definitions}\n)')
    types_widened: List[str] = []
    current = {field.name: field for field in spark.table(table_name).schema.fields}
    incompatible: List[str] = []
    for name, target_field in target.items():
        current_field = current[name]
        if current_field.dataType == target_field.dataType:
            continue
        if isinstance(current_field.dataType, T.FloatType) and isinstance(target_field.dataType, T.DoubleType):
            spark.sql(f'ALTER TABLE {_sql_identifier(table_name)} ALTER COLUMN {_sql_identifier(name)} TYPE DOUBLE')
            types_widened.append(name)
        else:
            incompatible.append(f'{name}: {current_field.dataType.simpleString()} -> {target_field.dataType.simpleString()}')
    if incompatible:
        raise RuntimeError('Unsafe automatic schema changes were detected: ' + '; '.join(incompatible) + '. Use run_map_mat_pregnancy_update(force_recreate_target=True) after review.')
    comments_changed = _apply_changed_column_comments(table_name, schema)
    table_comment_changed = _set_table_comment_if_changed(table_name, comment)
    return {'created': False, 'columns_added': [field.name for field in missing_fields], 'types_widened': types_widened, 'comments_changed': comments_changed, 'properties_changed': properties_changed, 'table_comment_changed': table_comment_changed}

def _align_to_schema(df: DataFrame, schema: T.StructType) -> DataFrame:
    expected = [field.name for field in schema.fields]
    missing = sorted(set(expected) - set(df.columns))
    extra = sorted(set(df.columns) - set(expected))
    if missing or extra:
        raise RuntimeError(f'Strict output-schema validation failed. Missing={missing}; extra={extra}')
    return df.select(*[F.col(field.name).cast(field.dataType).alias(field.name, metadata=field.metadata) for field in schema.fields])

def _nonblank(column: F.Column) -> F.Column:
    return F.when(F.length(F.trim(column.cast('string'))) > 0, column)

def _stable_row_hash(df: DataFrame, excluded: Sequence[str]) -> F.Column:
    columns = [F.col(name).alias(name) for name in sorted(df.columns) if name not in set(excluded)]
    return F.xxhash64(F.to_json(F.struct(*columns)))

def _parsed_gestation(text_column: F.Column) -> Tuple[F.Column, F.Column, F.Column]:
    text = F.lower(F.trim(text_column.cast('string')))
    week_token = F.regexp_extract(text, '(-?\\d+)\\s*weeks?', 1)
    day_token = F.regexp_extract(text, '(-?\\d+)\\s*days?', 1)
    numeric_only = text.rlike('^-?\\d+(?:\\.\\d+)?$')
    weeks = F.when(F.length(week_token) > 0, week_token.cast('int')).when(numeric_only, text.cast('double').cast('int')).otherwise(F.lit(None).cast('int'))
    days = F.when(F.length(day_token) > 0, day_token.cast('int')).when(numeric_only, F.lit(0)).otherwise(F.lit(None).cast('int'))
    total = F.when(weeks.isNotNull(), weeks * F.lit(7) + F.coalesce(days, F.lit(0)))
    return (weeks, days, total.cast('int'))

def _folic_acid_description(code_column: F.Column) -> F.Column:
    return F.when(code_column == '01', F.lit('Has been taking prior to becoming pregnant')).when(code_column == '02', F.lit('Started taking once pregnancy confirmed')).when(code_column == '03', F.lit('Not taking folic acid supplement')).when(code_column == 'ZZ', F.lit('Not stated: person declined to provide a response'))

def _assert_unique_non_null_keys(df: DataFrame, keys: Sequence[str], label: str) -> None:
    null_condition = None
    for key in keys:
        condition = F.col(key).isNull()
        null_condition = condition if null_condition is None else null_condition | condition
    if df.filter(null_condition).limit(1).count() > 0:
        raise RuntimeError(f'{label} contains a null business key in {list(keys)}')
    if df.groupBy(*keys).count().filter(F.col('count') > 1).limit(1).count() > 0:
        raise RuntimeError(f'{label} contains duplicate business keys in {list(keys)}')

def _try_timestamp(column_name: str) -> F.Column:
    tick = chr(96)
    escaped = column_name.replace(tick, tick + tick)
    return F.expr(f'try_cast({tick}{escaped}{tick} as timestamp)')

def _try_double(column_name: str) -> F.Column:
    tick = chr(96)
    escaped = column_name.replace(tick, tick + tick)
    return F.expr(f'try_cast({tick}{escaped}{tick} as double)')

def _try_long(column_name: str) -> F.Column:
    tick = chr(96)
    escaped = column_name.replace(tick, tick + tick)
    return F.expr(f'try_cast({tick}{escaped}{tick} as bigint)')

def _mat_pregnancy_snapshot(config: MapMatPregnancyConfig, source_version: int) -> DataFrame:
    raw = _read_snapshot(config.mat_pregnancy_table, source_version)
    selected = raw.select(F.col('PERSON_ID').cast('long').alias('Person_ID'), F.col('PREGNANCY_ID').cast('long').alias('Pregnancy_ID'), F.col('MRN').cast('string').alias('_MAT_MRN'), F.col('NHS').cast('string').alias('_MAT_NHS'), F.col('MOTHER_DOB_DT_TM').cast('string').alias('_MAT_MOTHER_DOB_RAW'), _try_timestamp('MOTHER_DOB_DT_TM').alias('_MAT_MOTHER_DOB'), F.col('GRAVIDA_NBR').cast('double').alias('_MAT_GRAVIDA'), F.col('PARA_NBR').cast('double').alias('_MAT_PARITY'), F.col('HX_PREG_OUTCOME_SPONT_ABORT_NBR').cast('double').alias('_MAT_PREV_MISCARRIAGES'), F.col('HX_PREG_OUTCOME_ECTOPIC_NBR').cast('double').alias('_MAT_PREV_ECTOPIC'), F.col('HX_PREG_OUTCOME_THERAP_ABORT_MED_NBR').cast('double').alias('_MAT_PREV_THERAP_ABORT_MED'), F.col('HX_PREG_OUTCOME_THERAP_ABORT_SURG_NBR').cast('double').alias('_MAT_PREV_THERAP_ABORT_SURG'), F.col('HX_PREG_OUTCOME_CS_EL_NBR').cast('double').alias('_MAT_PREV_CS_EL'), F.col('HX_PREG_OUTCOME_CS_EM_NBR').cast('double').alias('_MAT_PREV_CS_EM'), F.col('PREV_PREG_PROBLEM_IND').cast('double').alias('_MAT_PREV_PREG_PROBLEM'), F.col('FIRST_ANTENATAL_ASSESSMENT_DT_TM').cast('string').alias('_MAT_FIRST_ANTENATAL_RAW'), _try_timestamp('FIRST_ANTENATAL_ASSESSMENT_DT_TM').alias('_MAT_FIRST_ANTENATAL'), F.col('GEST_AGE_PREG_START').cast('string').alias('_MAT_GEST_START'), F.col('GEST_AGE_PREG_END').cast('string').alias('_MAT_GEST_END'), F.col('FINAL_EDD_DT_TM').cast('string').alias('_MAT_EDD_RAW'), _try_timestamp('FINAL_EDD_DT_TM').alias('_MAT_EDD'), F.col('ALCOHOL_USE_NBR').cast('double').alias('_MAT_ALCOHOL'), F.col('TOBAC_USE_NBR').cast('double').alias('_MAT_TOBACCO'), F.col('SMOKE_BOOKING_NM_ID').cast('int').alias('_MAT_SMOKE_BOOKING_CD'), F.col('SMOKE_BOOKING_DESC').cast('string').alias('_MAT_SMOKE_BOOKING_DESC'), F.col('SMOKE_LIVES_WITH_NM_ID').cast('int').alias('_MAT_SMOKE_LIVES_WITH_CD'), F.col('SMOKE_LIVES_WITH_DESC').cast('string').alias('_MAT_SMOKE_LIVES_WITH_DESC'), F.col('SMOKING_STATUS_DEL_NM_ID').cast('int').alias('_MAT_SMOKE_DELIVERY_CD'), F.col('SMOKING_STATUS_DEL_DESC').cast('string').alias('_MAT_SMOKE_DELIVERY_DESC'), F.col('REC_SUB_USE_NM_ID').cast('string').alias('_MAT_SUBSTANCE_CD'), F.col('REC_SUB_USE_DESC').cast('string').alias('_MAT_SUBSTANCE_DESC'), F.col('HT_BOOKING_CM').cast('double').alias('_MAT_HEIGHT'), F.col('WT_BOOKING_KG').cast('double').alias('_MAT_WEIGHT'), F.col('BMI_BOOKING_DESC').cast('double').alias('_MAT_BMI'), F.col('PREG_TYPE_SCT_CD').cast('string').alias('_MAT_PREG_TYPE_CD'), F.col('PREG_TYPE_DESC').cast('string').alias('_MAT_PREG_TYPE_DESC'), F.col('LAB_ONSET_METHOD_NM_ID').cast('int').alias('_MAT_LAB_ONSET_METHOD_CD'), F.col('LAB_ONSET_METHOD_DESC').cast('string').alias('_MAT_LAB_ONSET_METHOD_DESC'), F.col('AUGMENTATION_NM_ID').cast('int').alias('_MAT_AUGMENTATION_CD'), F.col('AUGMENTATION_DESC').cast('string').alias('_MAT_AUGMENTATION_DESC'), F.col('ANALGESIA_DEL_NM_ID').cast('string').alias('_MAT_ANALGESIA_DEL_CD'), F.col('ANALGESIA_DEL_DESC').cast('string').alias('_MAT_ANALGESIA_DEL_DESC'), F.col('ANALGESIA_LAB_NM_ID').cast('string').alias('_MAT_ANALGESIA_LAB_CD'), F.col('ANALGESIA_LAB_DESC').cast('string').alias('_MAT_ANALGESIA_LAB_DESC'), F.col('ANAESTHESIA_LAB_NM_ID').cast('string').alias('_MAT_ANAESTHESIA_LAB_CD'), F.col('ANAESTHESIA_LAB_DESC').cast('string').alias('_MAT_ANAESTHESIA_LAB_DESC'), F.col('ANAESTHESIA_DEL_NM_ID').cast('string').alias('_MAT_ANAESTHESIA_DEL_CD'), F.col('ANAESTHESIA_DEL_DESC').cast('string').alias('_MAT_ANAESTHESIA_DEL_DESC'), F.col('PERINEAL_TRAUMA_NM_ID').cast('string').alias('_MAT_PERINEAL_TRAUMA_CD'), F.col('PERINEAL_TRAUMA_DESC').cast('string').alias('_MAT_PERINEAL_TRAUMA_DESC'), F.col('EPISIOTOMY_NM_ID').cast('int').alias('_MAT_EPISIOTOMY_CD'), F.col('EPISIOTOMY_DESC').cast('string').alias('_MAT_EPISIOTOMY_DESC'), F.col('TOTAL_BLOOD_LOSS').cast('double').alias('_MAT_BLOOD_LOSS'), F.col('ROM_DT_TM').cast('string').alias('_MAT_ROM_RAW'), _try_timestamp('ROM_DT_TM').alias('_MAT_ROM'), F.col('LAB_ONSET_DT_TM').cast('string').alias('_MAT_LAB_ONSET_RAW'), _try_timestamp('LAB_ONSET_DT_TM').alias('_MAT_LAB_ONSET'), F.col('FULL_DIL_DT_TM').cast('string').alias('_MAT_FULL_DIL_RAW'), _try_timestamp('FULL_DIL_DT_TM').alias('_MAT_FULL_DIL'), F.col('GEST_DEATH_CONF_DT_TM').cast('string').alias('_MAT_GEST_DEATH_RAW'), _try_timestamp('GEST_DEATH_CONF_DT_TM').alias('_MAT_GEST_DEATH'), F.col('ROM_METHOD_NM_ID').cast('int').alias('_MAT_ROM_METHOD_CD'), F.col('ROM_METHOD_CD_DESC').cast('string').alias('_MAT_ROM_METHOD_DESC'), F.col('LAB_TOTAL').cast('double').alias('_MAT_LAB_TOTAL'), F.col('UNBOOKED_NM_ID').cast('int').alias('_MAT_UNBOOKED_CD'), F.col('UNBOOKED_DESC').cast('string').alias('_MAT_UNBOOKED_DESC'), F.col('BIRTHING_POOL_USE_NM_ID').cast('string').alias('_MAT_BIRTHING_POOL_CD'), F.col('BIRTHING_POOL_USE_DESC').cast('string').alias('_MAT_BIRTHING_POOL_DESC'), F.col('PLACENTA_APPEARANCE_NM_ID').cast('int').alias('_MAT_PLACENTA_CD'), F.col('PLACENTA_APPEARANCE_DESC').cast('string').alias('_MAT_PLACENTA_DESC'), F.col('PLACENTA_MAN_REMOVAL_IND').cast('double').alias('_MAT_PLACENTA_MANUAL'), F.col('PLANNED_DEL_LOC_PREG_NM_ID').cast('int').alias('_MAT_PLAN_LOC_PREG_CD'), F.col('PLANNED_DEL_LOC_PREG_DESC').cast('string').alias('_MAT_PLAN_LOC_PREG_DESC'), F.col('PLANNED_DEL_LOC_LAB_NM_ID').cast('int').alias('_MAT_PLAN_LOC_LAB_CD'), F.col('PLANNED_DEL_LOC_LAB_DESC').cast('string').alias('_MAT_PLAN_LOC_LAB_DESC'), F.col('PREG_REL_PROBLM_SCT_CD').cast('string').alias('_MAT_PREG_PROBLEM_CD'), F.col('PREG_REL_PROBLM_DESC').cast('string').alias('_MAT_PREG_PROBLEM_DESC'), F.col('NONPREG_REL_PROBLM_SCT_CD').cast('string').alias('_MAT_NONPREG_PROBLEM_CD'), F.col('NONPREG_REL_PROBLM_DESC').cast('string').alias('_MAT_NONPREG_PROBLEM_DESC'), F.col('KNOWN_SOC_SERV_NM_ID').cast('int').alias('_MAT_KNOWN_SOCIAL_CD'), F.col('KNOWN_SOC_SERV_DESC').cast('string').alias('_MAT_KNOWN_SOCIAL_DESC'), F.col('SUPPORT_STATUS_NM_ID').cast('int').alias('_MAT_SUPPORT_CD'), F.col('SUPPORT_STATUS_DESC').cast('string').alias('_MAT_SUPPORT_DESC'), F.col('DELETE_IND').cast('int').alias('_MAT_DELETE_IND'), F.col('Ctrl_Id').cast('int').alias('_MAT_CTRL_ID'), F.col('Record_Updated_Dt').cast('timestamp').alias('_MAT_RECORD_UPDATED_DT'), F.col('ADC_UPDT').cast('timestamp').alias('_MAT_ADC_UPDT'))
    parse_failure = _nonblank(F.col('_MAT_MOTHER_DOB_RAW')).isNotNull() & F.col('_MAT_MOTHER_DOB').isNull() | _nonblank(F.col('_MAT_FIRST_ANTENATAL_RAW')).isNotNull() & F.col('_MAT_FIRST_ANTENATAL').isNull() | _nonblank(F.col('_MAT_EDD_RAW')).isNotNull() & F.col('_MAT_EDD').isNull() | _nonblank(F.col('_MAT_ROM_RAW')).isNotNull() & F.col('_MAT_ROM').isNull() | _nonblank(F.col('_MAT_LAB_ONSET_RAW')).isNotNull() & F.col('_MAT_LAB_ONSET').isNull() | _nonblank(F.col('_MAT_FULL_DIL_RAW')).isNotNull() & F.col('_MAT_FULL_DIL').isNull() | _nonblank(F.col('_MAT_GEST_DEATH_RAW')).isNotNull() & F.col('_MAT_GEST_DEATH').isNull()
    result = selected.withColumn('_MAT_DATE_PARSE_FAILURE', parse_failure)
    _assert_unique_non_null_keys(result, ['Person_ID', 'Pregnancy_ID'], config.mat_pregnancy_table)
    return result

def _msds_snapshot(config: MapMatPregnancyConfig, source_version: int) -> DataFrame:
    result = _read_snapshot(config.msds_table, source_version).select(F.col('PREGNANCYID').cast('string').alias('_MSDS_PREGNANCYID_RAW'), _try_long('PREGNANCYID').alias('Pregnancy_ID'), F.col('LPIDMOTHER').cast('string').alias('_MSDS_LPID_MOTHER'), F.col('ANTENATALAPPDATE').cast('timestamp').alias('_MSDS_ANTENATAL_DATE'), F.col('PREGFIRSTCONDATE').cast('timestamp').alias('_MSDS_FIRST_CONTACT_DATE'), F.col('EDDAGREED').cast('timestamp').alias('_MSDS_EDD'), F.col('EDDMETHOD').cast('string').alias('_MSDS_EDD_METHOD'), F.col('SOURCEREFMAT').cast('string').alias('_MSDS_SOURCE_REFERRAL'), F.col('REASONLATEBOOKING').cast('string').alias('_MSDS_LATE_BOOKING_REASON'), F.col('PREGFIRSTCONTACTCAREPROFTYPE').cast('string').alias('_MSDS_FIRST_CONTACT_PROF'), F.col('LASTMENSTRUALPERIODDATE').cast('timestamp').alias('_MSDS_LMP'), F.col('FOLICACIDSUPPLEMENT').cast('string').alias('_MSDS_FOLIC_CD'), _try_double('PREVIOUSLIVEBIRTHS').alias('_MSDS_PREV_LIVE'), _try_double('PREVIOUSSTILLBIRTHS').alias('_MSDS_PREV_STILL'), _try_double('PREVIOUSLOSSESLESSTHAN24WEEKS').alias('_MSDS_PREV_LOSSES_LT24'), _try_double('PREVIOUSCAESAREANSECTIONS').alias('_MSDS_PREV_CS'), F.col('DISCHARGEDATEMATSERVICE').cast('timestamp').alias('_MSDS_DISCHARGE_DATE'), F.col('DISCHREASON').cast('string').alias('_MSDS_DISCHARGE_REASON'), F.col('DISABILITYINDMOTHER').cast('string').alias('_MSDS_DISABILITY'), F.col('LANGCODE').cast('string').alias('_MSDS_LANGUAGE'), F.col('MHPREDICTIONDETECTIONINDMOTHER').cast('string').alias('_MSDS_MH_PREDICTION'), F.col('COMPLEXSOCIALFACTORSIND').cast('string').alias('_MSDS_COMPLEX_SOCIAL'), F.col('SUPPORTSTATUSINDMOTHER').cast('string').alias('_MSDS_SUPPORT_MOTHER'), F.col('EMPLOYMENTSTATUSMOTHER').cast('string').alias('_MSDS_EMPLOYMENT_MOTHER'), F.col('EMPLOYMENTSTATUSPARTNER').cast('string').alias('_MSDS_EMPLOYMENT_PARTNER'), F.col('SUBSTANCEUSESTATUS').cast('string').alias('_MSDS_SUBSTANCE_USE'), F.col('SMOKINGSTATUS').cast('string').alias('_MSDS_SMOKING'), F.col('CIGARETTESPERDAY').cast('string').alias('_MSDS_CIGARETTES'), F.col('ALCOHOLUNITSPERWEEK').cast('string').alias('_MSDS_ALCOHOL'), F.col('PERSONWEIGHT').cast('string').alias('_MSDS_WEIGHT'), F.col('PERSONHEIGHT').cast('string').alias('_MSDS_HEIGHT'), F.col('SOURCE_SYSTEM').cast('string').alias('_MSDS_SOURCE_SYSTEM'), F.col('IS_VALID').cast('string').alias('_MSDS_IS_VALID'), F.col('CTRL_ID').cast('int').alias('_MSDS_CTRL_ID'), F.col('RECORD_UPDATED_DT').cast('timestamp').alias('_MSDS_RECORD_UPDATED_DT'), F.col('ADC_UPDT').cast('timestamp').alias('_MSDS_ADC_UPDT'))
    valid = result.filter(F.col('Pregnancy_ID').isNotNull())
    if valid.groupBy('Pregnancy_ID').count().filter(F.col('count') > 1).limit(1).count() > 0:
        raise RuntimeError(f'{config.msds_table} contains duplicate numeric PREGNANCYID values; refusing a non-deterministic many-to-one join.')
    return result

def _birth_aggregate(config: MapMatPregnancyConfig, source_version: int) -> DataFrame:
    raw = _read_snapshot(config.mat_birth_table, source_version).select(F.col('PREGNANCY_ID').cast('long').alias('Pregnancy_ID'), F.col('PREGNANCY_CHILD_ID').cast('long').alias('_BIRTH_CHILD_ID'), F.col('DELETE_IND').cast('int').alias('_BIRTH_DELETE_IND'), F.col('BIRTH_NBR').cast('int').alias('_BIRTH_NBR'), F.col('ADC_UPDT').cast('timestamp').alias('_BIRTH_ADC_UPDT'))
    return raw.filter(F.col('Pregnancy_ID').isNotNull()).groupBy('Pregnancy_ID').agg(F.count(F.lit(1)).cast('long').alias('_BIRTH_RECORD_COUNT'), F.sum(F.when(F.col('_BIRTH_DELETE_IND') == 0, F.lit(1)).otherwise(F.lit(0))).cast('long').alias('_ACTIVE_BIRTH_RECORD_COUNT'), F.countDistinct(F.when(F.col('_BIRTH_DELETE_IND') == 0, F.col('_BIRTH_CHILD_ID'))).cast('long').alias('_DISTINCT_ACTIVE_CHILD_COUNT'), F.max(F.when(F.col('_BIRTH_DELETE_IND') == 0, F.col('_BIRTH_NBR'))).cast('int').alias('_RECORDED_BIRTH_COUNT'), F.max('_BIRTH_ADC_UPDT').alias('_BIRTH_ADC_UPDT'))

def build_map_mat_pregnancy_snapshot(source_versions: Dict[str, int], run_id: str, run_timestamp: datetime, config: MapMatPregnancyConfig=MAP_MAT_PREGNANCY_CONFIG) -> DataFrame:
    mat = _mat_pregnancy_snapshot(config, source_versions[config.mat_pregnancy_table])
    msds = _msds_snapshot(config, source_versions[config.msds_table]).filter(F.col('Pregnancy_ID').isNotNull())
    births = _birth_aggregate(config, source_versions[config.mat_birth_table])
    joined = mat.join(msds, ['Pregnancy_ID'], 'left').join(births, ['Pregnancy_ID'], 'left')
    gest_start_weeks, gest_start_days, gest_start_total = _parsed_gestation(F.col('_MAT_GEST_START'))
    gest_end_weeks, gest_end_days, gest_end_total = _parsed_gestation(F.col('_MAT_GEST_END'))
    first_antenatal = F.coalesce(F.col('_MAT_FIRST_ANTENATAL'), F.col('_MSDS_ANTENATAL_DATE'))
    first_antenatal_source = F.when(F.col('_MAT_FIRST_ANTENATAL').isNotNull(), F.lit('MAT')).when(F.col('_MSDS_ANTENATAL_DATE').isNotNull(), F.lit('MSDS'))
    edd = F.coalesce(F.col('_MAT_EDD'), F.col('_MSDS_EDD'))
    edd_source = F.when(F.col('_MAT_EDD').isNotNull(), F.lit('MAT')).when(F.col('_MSDS_EDD').isNotNull(), F.lit('MSDS'))
    multiple_birth = F.greatest(F.coalesce(F.col('_RECORDED_BIRTH_COUNT'), F.lit(0)), F.coalesce(F.col('_DISTINCT_ACTIVE_CHILD_COUNT').cast('int'), F.lit(0))) > F.lit(1)
    first_conflict = F.col('_MAT_FIRST_ANTENATAL').isNotNull() & F.col('_MSDS_ANTENATAL_DATE').isNotNull() & (F.to_date('_MAT_FIRST_ANTENATAL') != F.to_date('_MSDS_ANTENATAL_DATE'))
    edd_conflict = F.col('_MAT_EDD').isNotNull() & F.col('_MSDS_EDD').isNotNull() & (F.to_date('_MAT_EDD') != F.to_date('_MSDS_EDD'))
    gravida_parity_conflict = (F.col('_MAT_GRAVIDA') < 0) | (F.col('_MAT_PARITY') < 0) | (F.col('_MAT_PARITY') > F.col('_MAT_GRAVIDA'))
    source_columns = joined.select(F.col('Person_ID'), F.col('Pregnancy_ID'), F.col('_MAT_MRN').alias('MRN'), F.col('_MAT_NHS').alias('NHS_Number'), F.col('_MAT_MOTHER_DOB').alias('Mother_DOB'), F.col('_MAT_GRAVIDA').alias('Gravida_NBR'), F.col('_MAT_PARITY').alias('Parity'), F.col('_MSDS_PREV_LIVE').alias('PrevLiveBirth_NBR'), F.col('_MAT_PREV_MISCARRIAGES').alias('PrevMiscarriages_NBR'), F.col('_MSDS_PREV_STILL').alias('PrevStillBirth_NBR'), first_antenatal.alias('FirstAntenatalAPPTDate'), F.col('_MAT_GEST_START').alias('GestAgePregStart'), F.col('_MAT_GEST_END').alias('GestAgePregEnd'), F.col('_MSDS_LMP').alias('LastMensPeriodDate'), F.col('_MAT_ALCOHOL').alias('AlcoholUnitsPerWeek'), F.col('_MAT_SMOKE_BOOKING_CD').alias('SmokingBooking_CD'), F.col('_MAT_SMOKE_BOOKING_DESC').alias('SmokingBooking_DESC'), F.col('_MAT_SMOKE_DELIVERY_CD').alias('SmokingDelivery_CD'), F.col('_MAT_SMOKE_DELIVERY_DESC').alias('SmokingDelivery_DESC'), F.col('_MAT_SUBSTANCE_CD').alias('SubstanceUse_CD'), F.col('_MAT_SUBSTANCE_DESC').alias('SubstanceUse_DESC'), edd.alias('ExpectedDeliveryDate'), F.col('_MAT_HEIGHT').alias('Height_CM'), F.col('_MAT_WEIGHT').alias('Weight_KG'), F.col('_MAT_BMI').alias('BMI'), F.col('_MSDS_FOLIC_CD').alias('FolicAcidSupp_CD'), _folic_acid_description(F.col('_MSDS_FOLIC_CD')).alias('FolicAcidSupp_DESC'), F.col('_MAT_LAB_ONSET_METHOD_CD').alias('LaborOnsetMethod_CD'), F.col('_MAT_LAB_ONSET_METHOD_DESC').alias('LaborOnsetMethod_DESC'), F.col('_MAT_AUGMENTATION_CD').alias('Augmentation_CD'), F.col('_MAT_AUGMENTATION_DESC').alias('Augmentation_DESC'), F.col('_MAT_ANALGESIA_DEL_CD').alias('AnalgesiaDelivery_CD'), F.col('_MAT_ANALGESIA_DEL_DESC').alias('AnalgesiaDelivery_DESC'), F.col('_MAT_ANALGESIA_LAB_CD').alias('AnalgesiaLabour_CD'), F.col('_MAT_ANALGESIA_LAB_DESC').alias('AnalgesiaLabour_DESC'), F.col('_MAT_ANAESTHESIA_LAB_CD').alias('AnaesthesiaLabour_CD'), F.col('_MAT_ANAESTHESIA_LAB_DESC').alias('AnaesthesiaLabour_DESC'), F.col('_MAT_PERINEAL_TRAUMA_CD').alias('PerinealTrauma_CD'), F.col('_MAT_PERINEAL_TRAUMA_DESC').alias('PerinealTrauma_DESC'), F.col('_MAT_EPISIOTOMY_CD').alias('Episiotomy_CD'), F.col('_MAT_EPISIOTOMY_DESC').alias('Episiotomy_DESC'), F.col('_MAT_BLOOD_LOSS').alias('BloodLoss'), F.greatest('_MAT_ADC_UPDT', '_MSDS_ADC_UPDT', '_BIRTH_ADC_UPDT').alias('ADC_UPDT'), F.col('_MAT_PREG_TYPE_CD').alias('PregnancyType_CD'), F.col('_MAT_PREG_TYPE_DESC').alias('PregnancyType_DESC'), F.col('_MAT_FIRST_ANTENATAL').alias('FirstAntenatalAPPTDate_MAT'), F.col('_MSDS_ANTENATAL_DATE').alias('FirstAntenatalAPPTDate_MSDS'), first_antenatal_source.alias('FirstAntenatalAPPTDate_Source'), F.col('_MAT_EDD').alias('ExpectedDeliveryDate_MAT'), F.col('_MSDS_EDD').alias('ExpectedDeliveryDate_MSDS'), edd_source.alias('ExpectedDeliveryDate_Source'), F.col('_MAT_ROM').alias('ROMDate'), F.col('_MAT_LAB_ONSET').alias('LabOnsetDate'), F.col('_MAT_FULL_DIL').alias('FullDilatationDate'), F.col('_MAT_GEST_DEATH').alias('GestationalDeathConfirmedDate'), F.col('_MSDS_FIRST_CONTACT_DATE').alias('PregnancyFirstContactDate'), F.col('_MSDS_DISCHARGE_DATE').alias('MaternityServiceDischargeDate'), F.col('_MSDS_DISCHARGE_REASON').alias('MaternityServiceDischargeReason'), F.col('_MSDS_EDD_METHOD').alias('EDDMethod'), F.col('_MSDS_LATE_BOOKING_REASON').alias('ReasonLateBooking'), F.col('_MSDS_FIRST_CONTACT_PROF').alias('FirstContactCareProfessionalType'), gest_start_weeks.alias('GestAgePregStartWeeks'), gest_start_days.alias('GestAgePregStartDays'), gest_start_total.alias('GestAgePregStartTotalDays'), gest_end_weeks.alias('GestAgePregEndWeeks'), gest_end_days.alias('GestAgePregEndDays'), gest_end_total.alias('GestAgePregEndTotalDays'), F.col('_MSDS_PREV_CS').alias('PreviousCaesareanSections'), F.col('_MSDS_PREV_LOSSES_LT24').alias('PreviousLossesUnder24Weeks'), F.col('_MAT_PREV_ECTOPIC').alias('PrevEctopicPregnancies'), F.col('_MAT_PREV_THERAP_ABORT_MED').alias('PrevTherapeuticAbortionsMedical'), F.col('_MAT_PREV_THERAP_ABORT_SURG').alias('PrevTherapeuticAbortionsSurgical'), F.col('_MAT_PREV_CS_EL').alias('PrevElectiveCaesareans'), F.col('_MAT_PREV_CS_EM').alias('PrevEmergencyCaesareans'), F.col('_MAT_PREV_PREG_PROBLEM').alias('PreviousPregnancyProblem_IND'), F.col('_MAT_UNBOOKED_CD').alias('Unbooked_CD'), F.col('_MAT_UNBOOKED_DESC').alias('Unbooked_DESC'), F.col('_MAT_ROM_METHOD_CD').alias('ROMMethod_CD'), F.col('_MAT_ROM_METHOD_DESC').alias('ROMMethod_DESC'), F.col('_MAT_LAB_TOTAL').alias('LabourTotalMinutes'), F.col('_MAT_ANAESTHESIA_DEL_CD').alias('AnaesthesiaDelivery_CD'), F.col('_MAT_ANAESTHESIA_DEL_DESC').alias('AnaesthesiaDelivery_DESC'), F.col('_MAT_BIRTHING_POOL_CD').alias('BirthingPoolUse_CD'), F.col('_MAT_BIRTHING_POOL_DESC').alias('BirthingPoolUse_DESC'), F.col('_MAT_PLACENTA_CD').alias('PlacentaAppearance_CD'), F.col('_MAT_PLACENTA_DESC').alias('PlacentaAppearance_DESC'), F.col('_MAT_PLACENTA_MANUAL').alias('PlacentaManualRemoval_IND'), F.col('_MAT_PLAN_LOC_PREG_CD').alias('PlannedDeliveryLocationPreg_CD'), F.col('_MAT_PLAN_LOC_PREG_DESC').alias('PlannedDeliveryLocationPreg_DESC'), F.col('_MAT_PLAN_LOC_LAB_CD').alias('PlannedDeliveryLocationLabour_CD'), F.col('_MAT_PLAN_LOC_LAB_DESC').alias('PlannedDeliveryLocationLabour_DESC'), F.col('_MAT_PREG_PROBLEM_CD').alias('PregnancyRelatedProblems_CD'), F.col('_MAT_PREG_PROBLEM_DESC').alias('PregnancyRelatedProblems_DESC'), F.col('_MAT_NONPREG_PROBLEM_CD').alias('NonPregnancyRelatedProblems_CD'), F.col('_MAT_NONPREG_PROBLEM_DESC').alias('NonPregnancyRelatedProblems_DESC'), F.col('_MAT_TOBACCO').alias('TobaccoUsePerDay'), F.col('_MAT_SMOKE_LIVES_WITH_CD').alias('SmokeLivesWith_CD'), F.col('_MAT_SMOKE_LIVES_WITH_DESC').alias('SmokeLivesWith_DESC'), F.col('_MAT_KNOWN_SOCIAL_CD').alias('KnownSocialServices_CD'), F.col('_MAT_KNOWN_SOCIAL_DESC').alias('KnownSocialServices_DESC'), F.col('_MAT_SUPPORT_CD').alias('SupportStatus_CD'), F.col('_MAT_SUPPORT_DESC').alias('SupportStatus_DESC'), F.col('_MSDS_COMPLEX_SOCIAL').alias('ComplexSocialFactors_CD'), F.col('_MSDS_MH_PREDICTION').alias('MentalHealthPredictionDetection_CD'), F.col('_MSDS_DISABILITY').alias('DisabilityMother_CD'), F.col('_MSDS_LANGUAGE').alias('PreferredLanguage_CD'), F.col('_MSDS_SUPPORT_MOTHER').alias('SupportStatusMother_CD'), F.col('_MSDS_EMPLOYMENT_MOTHER').alias('EmploymentStatusMother_CD'), F.col('_MSDS_EMPLOYMENT_PARTNER').alias('EmploymentStatusPartner_CD'), F.col('_MSDS_SUBSTANCE_USE').alias('MSDS_SubstanceUseStatus'), F.col('_MSDS_SMOKING').alias('MSDS_SmokingStatus'), F.col('_MSDS_CIGARETTES').alias('MSDS_CigarettesPerDay'), F.col('_MSDS_ALCOHOL').alias('MSDS_AlcoholUnitsPerWeek'), F.col('_MSDS_WEIGHT').alias('MSDS_PersonWeight'), F.col('_MSDS_HEIGHT').alias('MSDS_PersonHeight'), F.col('_MSDS_LPID_MOTHER').alias('LPIDMother'), F.col('_MSDS_SOURCE_REFERRAL').alias('MSDS_SourceReferral'), F.col('_BIRTH_RECORD_COUNT').alias('BirthRecordCount'), F.col('_ACTIVE_BIRTH_RECORD_COUNT').alias('ActiveBirthRecordCount'), F.col('_DISTINCT_ACTIVE_CHILD_COUNT').alias('DistinctActiveChildCount'), F.col('_RECORDED_BIRTH_COUNT').alias('RecordedBirthCount'), F.coalesce(multiple_birth, F.lit(False)).alias('MultipleBirth_IND'), (F.coalesce(multiple_birth, F.lit(False)) & (F.upper(F.trim(F.col('_MAT_PREG_TYPE_DESC'))) == F.lit('SINGLETON'))).alias('PregnancyTypeBirthConflict_IND'), F.col('_BIRTH_ADC_UPDT').alias('BIRTH_ADC_UPDT'), F.col('_MAT_DELETE_IND').alias('MAT_DELETE_IND'), F.col('_MAT_CTRL_ID').alias('MAT_CTRL_ID'), F.col('_MAT_RECORD_UPDATED_DT').alias('MAT_RECORD_UPDATED_DT'), F.col('_MAT_ADC_UPDT').alias('MAT_ADC_UPDT'), F.col('_MSDS_CTRL_ID').alias('MSDS_CTRL_ID'), F.col('_MSDS_RECORD_UPDATED_DT').alias('MSDS_RECORD_UPDATED_DT'), F.col('_MSDS_ADC_UPDT').alias('MSDS_ADC_UPDT'), F.col('_MSDS_SOURCE_SYSTEM').alias('MSDS_SOURCE_SYSTEM'), F.col('_MSDS_IS_VALID').alias('MSDS_IS_VALID'), F.lit(True).alias('MAT_SOURCE_PRESENT_IND'), F.col('_MSDS_PREGNANCYID_RAW').isNotNull().alias('MSDS_SOURCE_PRESENT_IND'), F.col('_BIRTH_RECORD_COUNT').isNotNull().alias('BIRTH_SOURCE_PRESENT_IND'), F.coalesce(F.col('_MAT_DELETE_IND') != 0, F.lit(False)).alias('SOURCE_DELETED_IND'), F.lit(False).alias('SOURCE_MISSING_IND'), F.coalesce(first_conflict, F.lit(False)).alias('FirstAntenatalDateConflict_IND'), F.coalesce(edd_conflict, F.lit(False)).alias('ExpectedDeliveryDateConflict_IND'), F.coalesce(F.col('_MSDS_ADC_UPDT') > F.col('_MAT_ADC_UPDT'), F.lit(False)).alias('MSDSNewerThanMAT_IND'), F.coalesce(gravida_parity_conflict, F.lit(False)).alias('GravidaParityConflict_IND'), F.coalesce(F.col('_MAT_DATE_PARSE_FAILURE'), F.lit(False)).alias('DateParseFailure_IND'), F.lit(source_versions[config.mat_pregnancy_table]).cast('long').alias('MAT_SOURCE_VERSION'), F.lit(source_versions[config.msds_table]).cast('long').alias('MSDS_SOURCE_VERSION'), F.lit(source_versions[config.mat_birth_table]).cast('long').alias('MAT_BIRTH_SOURCE_VERSION'), F.concat_ws(',', F.lit('MAT_PREGNANCY'), F.when(F.col('_MSDS_PREGNANCYID_RAW').isNotNull(), F.lit('MSDS101PREGBOOK')), F.when(F.col('_BIRTH_RECORD_COUNT').isNotNull(), F.lit('MAT_BIRTH'))).alias('TRIGGER_SOURCES'))
    with_hash = source_columns.withColumn('ROW_HASH', _stable_row_hash(source_columns, _PIPELINE_HASH_EXCLUSIONS))
    final_df = with_hash.withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_UPDT_DT_TM', F.lit(run_timestamp).cast('timestamp')).withColumn('SCHEMA_VERSION', F.lit(config.schema_version))
    return _align_to_schema(final_df, schema_map_mat_pregnancy)

def create_mat_pregnancy_mapping_incr(source_versions: Optional[Dict[str, int]]=None, run_id: Optional[str]=None, run_timestamp: Optional[datetime]=None, config: MapMatPregnancyConfig=MAP_MAT_PREGNANCY_CONFIG) -> DataFrame:
    """Compatibility name: returns the complete reconciled snapshot, not a timestamp slice."""
    versions = source_versions or _capture_source_versions(config)
    actual_run_id = run_id or str(uuid.uuid4())
    actual_timestamp = run_timestamp or datetime.now(timezone.utc)
    return build_map_mat_pregnancy_snapshot(versions, actual_run_id, actual_timestamp, config)

def build_unmatched_msds_snapshot(source_versions: Dict[str, int], run_id: str, run_timestamp: datetime, config: MapMatPregnancyConfig=MAP_MAT_PREGNANCY_CONFIG) -> DataFrame:
    mat_ids = _read_snapshot(config.mat_pregnancy_table, source_versions[config.mat_pregnancy_table]).select(F.col('PREGNANCY_ID').cast('long').alias('Pregnancy_ID')).filter(F.col('Pregnancy_ID').isNotNull()).dropDuplicates(['Pregnancy_ID'])
    msds = _msds_snapshot(config, source_versions[config.msds_table])
    invalid = msds.filter(F.col('Pregnancy_ID').isNull()).withColumn('_UNMATCHED_REASON', F.lit('INVALID_PREGNANCY_ID'))
    no_match = msds.filter(F.col('Pregnancy_ID').isNotNull()).join(mat_ids, ['Pregnancy_ID'], 'left_anti').withColumn('_UNMATCHED_REASON', F.lit('NO_MAT_PREGNANCY_MATCH'))
    unmatched = invalid.unionByName(no_match)
    source_columns = unmatched.select(F.sha2(F.concat_ws('|', F.coalesce(F.col('_MSDS_PREGNANCYID_RAW'), F.lit('<NULL>')), F.coalesce(F.col('_MSDS_LPID_MOTHER'), F.lit('<NULL>'))), 256).alias('UNMATCHED_KEY'), F.col('_UNMATCHED_REASON').alias('UNMATCHED_REASON'), F.col('_MSDS_PREGNANCYID_RAW').alias('PREGNANCYID_RAW'), F.col('Pregnancy_ID'), F.col('_MSDS_LPID_MOTHER').alias('LPIDMother'), F.col('_MSDS_ANTENATAL_DATE').alias('AntenatalAppointmentDate'), F.col('_MSDS_FIRST_CONTACT_DATE').alias('PregnancyFirstContactDate'), F.col('_MSDS_EDD').alias('ExpectedDeliveryDate'), F.col('_MSDS_LMP').alias('LastMensPeriodDate'), F.col('_MSDS_FOLIC_CD').alias('FolicAcidSupplement_CD'), F.col('_MSDS_PREV_LIVE').alias('PreviousLiveBirths'), F.col('_MSDS_PREV_STILL').alias('PreviousStillBirths'), F.col('_MSDS_PREV_LOSSES_LT24').alias('PreviousLossesUnder24Weeks'), F.col('_MSDS_PREV_CS').alias('PreviousCaesareanSections'), F.col('_MSDS_SOURCE_SYSTEM').alias('SOURCE_SYSTEM'), F.col('_MSDS_IS_VALID').alias('IS_VALID'), F.col('_MSDS_RECORD_UPDATED_DT').alias('RECORD_UPDATED_DT'), F.col('_MSDS_ADC_UPDT').alias('ADC_UPDT'), F.lit(source_versions[config.msds_table]).cast('long').alias('MSDS_SOURCE_VERSION'))
    with_hash = source_columns.withColumn('ROW_HASH', _stable_row_hash(source_columns, {'MSDS_SOURCE_VERSION', 'ROW_HASH', 'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM'}))
    final_df = with_hash.withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_UPDT_DT_TM', F.lit(run_timestamp).cast('timestamp'))
    return _align_to_schema(final_df, schema_unmatched_msds)

def _latest_operation_metrics(table_name: str) -> Dict[str, object]:
    row = spark.sql(f'DESCRIBE HISTORY {_sql_identifier(table_name)} LIMIT 1').select('operation', 'operationMetrics', 'version', 'timestamp').first()
    if row is None:
        return {}
    metrics = dict(row['operationMetrics'] or {})
    return {'operation': row['operation'], 'version': int(row['version']), 'timestamp': str(row['timestamp']), **metrics}

def _merge_current_snapshot(snapshot: DataFrame, table_name: str, schema: T.StructType, table_comment: str, key_condition: str, run_id: str, run_timestamp: datetime, schema_version: Optional[str]=None, mark_missing_mat_rows: bool=False) -> Dict[str, object]:
    if bronze_is_primary_table(table_name):
        schema = bronze_contract_schema(table_name)
        snapshot = bronze_project_contract(snapshot, table_name)
    schema_changes = _ensure_delta_table(table_name, schema, table_comment)
    target = DeltaTable.forName(spark, table_name)
    assignments = {
        column_name: F.col(f's.{column_name}')
        for column_name in snapshot.columns
    }
    comparisons = ' OR '.join(
        f'NOT (t.`{column_name}` <=> s.`{column_name}`)'
        for column_name in snapshot.columns
        if column_name not in {'Person_ID', 'Pregnancy_ID', 'UNMATCHED_KEY'}
    )
    merge = target.alias('t').merge(
        snapshot.alias('s'), key_condition
    ).whenMatchedUpdate(
        condition=comparisons or 'false',
        set=assignments,
    ).whenNotMatchedInsert(values=assignments)
    if not mark_missing_mat_rows:
        merge = merge.whenNotMatchedBySourceDelete()
    merge.execute()
    return {
        'schema_changes': schema_changes,
        'operation_metrics': _latest_operation_metrics(table_name),
    }

def _replace_target_table(snapshot: DataFrame, config: MapMatPregnancyConfig) -> Dict[str, object]:
    snapshot = bronze_project_contract(snapshot, config.target_table)
    snapshot.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('delta.enableChangeDataFeed', 'true').option('delta.enableRowTracking', 'true').option('delta.enableTypeWidening', 'true').option('delta.parquet.compression.codec', 'zstd').saveAsTable(config.target_table)
    schema_changes = _ensure_delta_table(
        config.target_table,
        bronze_contract_schema(config.target_table),
        map_mat_pregnancy_comment,
    )
    return {'schema_changes': schema_changes, 'operation_metrics': _latest_operation_metrics(config.target_table)}

def update_map_mat_pregnancy(snapshot: DataFrame, run_id: str, run_timestamp: datetime, config: MapMatPregnancyConfig=MAP_MAT_PREGNANCY_CONFIG, force_recreate_target: bool=False) -> Dict[str, object]:
    if force_recreate_target:
        return _replace_target_table(snapshot, config)
    return _merge_current_snapshot(snapshot=snapshot, table_name=config.target_table, schema=schema_map_mat_pregnancy, table_comment=map_mat_pregnancy_comment, key_condition='t.Person_ID <=> s.Person_ID AND t.Pregnancy_ID <=> s.Pregnancy_ID', run_id=run_id, run_timestamp=run_timestamp, schema_version=config.schema_version, mark_missing_mat_rows=True)

def update_unmatched_msds(snapshot: DataFrame, run_id: str, run_timestamp: datetime, config: MapMatPregnancyConfig=MAP_MAT_PREGNANCY_CONFIG) -> Dict[str, object]:
    return _merge_current_snapshot(snapshot=snapshot, table_name=config.unmatched_msds_table, schema=schema_unmatched_msds, table_comment=unmatched_msds_comment, key_condition='t.UNMATCHED_KEY = s.UNMATCHED_KEY', run_id=run_id, run_timestamp=run_timestamp, mark_missing_mat_rows=False)

def _ensure_audit_table(config: MapMatPregnancyConfig) -> None:
    spark.sql(f"\n        CREATE TABLE IF NOT EXISTS {_sql_identifier(config.audit_table)} (\n            run_id STRING,\n            event_timestamp TIMESTAMP,\n            status STRING,\n            mode STRING,\n            message STRING,\n            source_versions_json STRING,\n            metrics_json STRING\n        )\n        USING DELTA\n        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')\n    ")

def _write_audit_event(config: MapMatPregnancyConfig, run_id: str, status: str, mode: str, message: str, source_versions: Optional[Dict[str, int]]=None, metrics: Optional[Dict[str, object]]=None) -> None:
    try:
        row = [(run_id, datetime.now(timezone.utc), status, mode, message[:8000], json.dumps(source_versions or {}, sort_keys=True, default=str), json.dumps(metrics or {}, sort_keys=True, default=str))]
        schema = T.StructType([T.StructField('run_id', T.StringType(), False), T.StructField('event_timestamp', T.TimestampType(), False), T.StructField('status', T.StringType(), False), T.StructField('mode', T.StringType(), False), T.StructField('message', T.StringType(), True), T.StructField('source_versions_json', T.StringType(), True), T.StructField('metrics_json', T.StringType(), True)])
        spark.createDataFrame(row, schema).write.mode('append').saveAsTable(config.audit_table)
    except Exception as exc:
        print(f'[WARN] Could not write map_mat_pregnancy audit event: {exc}')

def _snapshot_summary(snapshot: DataFrame, unmatched_msds: DataFrame) -> Dict[str, int]:
    summary = snapshot.agg(F.count(F.lit(1)).alias('snapshot_rows'), F.count_if(F.col('SOURCE_DELETED_IND')).alias('soft_deleted_rows'), F.count_if(F.col('MSDS_SOURCE_PRESENT_IND')).alias('rows_with_msds'), F.count_if(F.col('BIRTH_SOURCE_PRESENT_IND')).alias('rows_with_birth'), F.count_if(F.col('FirstAntenatalAPPTDate_Source') == 'MSDS').alias('antenatal_msds_fills'), F.count_if(F.col('ExpectedDeliveryDate_Source') == 'MSDS').alias('edd_msds_fills'), F.count_if(F.col('FirstAntenatalDateConflict_IND')).alias('antenatal_conflicts'), F.count_if(F.col('ExpectedDeliveryDateConflict_IND')).alias('edd_conflicts'), F.count_if(F.col('PregnancyTypeBirthConflict_IND')).alias('pregnancy_type_birth_conflicts'), F.count_if(F.col('DateParseFailure_IND')).alias('date_parse_failures')).first().asDict()
    summary['unmatched_msds_rows'] = int(unmatched_msds.count())
    return {key: int(value or 0) for key, value in summary.items()}

def validate_map_mat_pregnancy_target(snapshot: DataFrame, config: MapMatPregnancyConfig=MAP_MAT_PREGNANCY_CONFIG) -> Dict[str, int]:
    target = spark.table(config.target_table)
    duplicate_groups = target.groupBy('Person_ID', 'Pregnancy_ID').count().filter(F.col('count') > 1).count()
    null_keys = target.filter(F.col('Person_ID').isNull() | F.col('Pregnancy_ID').isNull()).count()
    source_keys_missing = snapshot.select('Person_ID', 'Pregnancy_ID').join(target.select('Person_ID', 'Pregnancy_ID'), ['Person_ID', 'Pregnancy_ID'], 'left_anti').count()
    source_hash_mismatches = snapshot.alias('s').join(target.alias('t'), (F.col('s.Person_ID') == F.col('t.Person_ID')) & (F.col('s.Pregnancy_ID') == F.col('t.Pregnancy_ID')), 'inner').filter(~F.col('s.ROW_HASH').eqNullSafe(F.col('t.ROW_HASH'))).count()
    metrics = {
        'target_rows': int(target.count()),
        'duplicate_key_groups': int(duplicate_groups),
        'null_key_rows': int(null_keys),
        'source_keys_missing': int(source_keys_missing),
        'source_hash_mismatches': int(source_hash_mismatches),
    }
    failures = {key: value for key, value in metrics.items() if key != 'target_rows' and value != 0}
    if failures:
        raise RuntimeError(f'map_mat_pregnancy post-checks failed: {failures}')
    return metrics

def run_map_mat_pregnancy_update(config: MapMatPregnancyConfig=MAP_MAT_PREGNANCY_CONFIG, dry_run: bool=False, run_post_checks: bool=True, force_recreate_target: bool=False) -> Dict[str, object]:
    """Build and deploy the complete pregnancy snapshot. Importing this notebook does not call it."""
    run_id = str(uuid.uuid4())
    run_timestamp = datetime.now(timezone.utc)
    source_versions = _capture_source_versions(config)
    mode = 'DRY_RUN' if dry_run else 'RECREATE' if force_recreate_target else 'RECONCILE'
    snapshot: Optional[DataFrame] = None
    unmatched: Optional[DataFrame] = None
    if not dry_run:
        _ensure_audit_table(config)
        _write_audit_event(config, run_id, 'STARTED', mode, 'Started map_mat_pregnancy replacement run.', source_versions)
    try:
        snapshot = build_map_mat_pregnancy_snapshot(source_versions, run_id, run_timestamp, config)
        unmatched = build_unmatched_msds_snapshot(source_versions, run_id, run_timestamp, config)
        summary = _snapshot_summary(snapshot, unmatched)
        if dry_run:
            result = {'run_id': run_id, 'mode': mode, 'source_versions': source_versions, 'snapshot_summary': summary}
            print(json.dumps(result, indent=2, sort_keys=True, default=str))
            return result
        target_result = update_map_mat_pregnancy(snapshot, run_id, run_timestamp, config, force_recreate_target=force_recreate_target)
        unmatched_result = update_unmatched_msds(unmatched, run_id, run_timestamp, config)
        checks = validate_map_mat_pregnancy_target(snapshot, config) if run_post_checks else {}
        metrics: Dict[str, object] = {'snapshot_summary': summary, 'target_result': target_result, 'unmatched_msds_result': unmatched_result, 'post_checks': checks}
        _write_audit_event(config, run_id, 'SUCCESS', mode, 'map_mat_pregnancy replacement completed successfully.', source_versions, metrics)
        result = {'run_id': run_id, 'mode': mode, 'source_versions': source_versions, **metrics}
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return result
    except Exception as exc:
        if not dry_run:
            _write_audit_event(config, run_id, 'FAILED', mode, str(exc), source_versions)
        raise

try:
    _targets = ['4_prod.bronze.map_mat_pregnancy']
    if not _pipeline_resume_skip_component('map_mat_pregnancy', _targets):
        run_map_mat_pregnancy_update(run_post_checks=_PIPELINE_FULL_REFRESH, force_recreate_target=_PIPELINE_FULL_REFRESH)
        _PIPELINE_UPDATED_TARGETS.extend(_targets)
        _pipeline_mark_component_complete('map_mat_pregnancy', _targets)
        _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_mat_pregnancy'})
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

_pipeline_component_start("map_mat_birth")
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
from typing import Dict, Optional, Sequence, Tuple
import json
import uuid
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

@dataclass(frozen=True)
class MapMatBirthConfig:
    birth_source_table: str = '4_prod.raw.mat_birth'
    pregnancy_source_table: str = '4_prod.raw.mat_pregnancy'
    nnu_source_table: str = '4_prod.raw.nnu_episodes'
    target_table: str = '4_prod.bronze.map_mat_birth'
    audit_table: str = '6_mgmt.bronze.map_mat_birth_pipeline_audit'
    schema_version: str = '2.0.0'
MAP_MAT_BIRTH_CONFIG = MapMatBirthConfig()
map_mat_birth_comment = 'Lossless maternity-birth foundation at one row per MAT_BIRTH source row. Source identifiers, lifecycle fields, raw values and timestamps are retained without row filtering. MAT and NNU birth facts remain separately available; additive coalesced fields include provenance and quality flags. The table is synchronized from consistent Delta snapshots of MAT_BIRTH, MAT_PREGNANCY and NNU_EPISODES.'
_PENDING_MAP_MAT_BIRTH_PLAN: Dict[str, object] = {}

def _mmb_field(name: str, data_type: T.DataType, comment: str, nullable: bool=True) -> T.StructField:
    return T.StructField(name, data_type, nullable, metadata={'comment': comment})
schema_map_mat_birth = T.StructType([_mmb_field('BirthRow_ID', T.StringType(), 'Stable target key. Uses PREGNANCY_CHILD_SEQ_ID when present, otherwise a deterministic source-row hash.', False), _mmb_field('MotherPerson_ID', T.LongType(), 'Mother PERSON_ID selected deterministically from MAT_PREGNANCY.'), _mmb_field('Pregnancy_ID', T.LongType(), 'Source pregnancy identifier.'), _mmb_field('PregnancyChild_ID', T.LongType(), 'Source PREGNANCY_CHILD_ID; source zero sentinel values are retained.'), _mmb_field('PregnancyChildSeq_ID', T.LongType(), 'Source PREGNANCY_CHILD_SEQ_ID. Populated and unique in the reviewed source snapshot.'), _mmb_field('BabyPerson_ID', T.LongType(), 'Millennium person identifier for the baby.'), _mmb_field('Baby_MRN', T.StringType(), 'Raw local baby medical record number.'), _mmb_field('Baby_NHS', T.StringType(), 'Digits-only normalized baby NHS number used for linkage.'), _mmb_field('Baby_NHS_Raw', T.StringType(), 'Unchanged source NHS value from MAT_BIRTH.'), _mmb_field('BirthOrder', T.IntegerType(), 'Source birth order number; null is retained.'), _mmb_field('BirthNumber', T.IntegerType(), 'Source total number of babies born in the delivery.'), _mmb_field('FetusNumber', T.IntegerType(), 'Best available NNU fetus number; not substituted with BirthNumber.'), _mmb_field('BirthLocation_CD', T.IntegerType(), 'Source birth-location nomenclature identifier.'), _mmb_field('BirthLocation_DESC', T.StringType(), 'Source birth-location description.'), _mmb_field('BirthDateTime', T.TimestampType(), 'Best available birth timestamp, preferring MAT_BIRTH then NNU.'), _mmb_field('DeliveryMethod_CD', T.IntegerType(), 'Source delivery-method code.'), _mmb_field('DeliveryMethod_DESC', T.StringType(), 'Source delivery-method description.'), _mmb_field('DeliveryOutcome_CD', T.IntegerType(), 'Source delivery-outcome code.'), _mmb_field('DeliveryOutcome_DESC', T.StringType(), 'Source delivery-outcome description.'), _mmb_field('NeonatalOutcome_CD', T.IntegerType(), 'Source neonatal-outcome code.'), _mmb_field('NeonatalOutcome_DESC', T.StringType(), 'Source neonatal-outcome description.'), _mmb_field('PregOutcome_CD', T.IntegerType(), 'Source pregnancy-outcome code.'), _mmb_field('PregOutcome_DESC', T.StringType(), 'Source pregnancy-outcome description.'), _mmb_field('PresDel_CD', T.IntegerType(), 'Source presentation-at-delivery nomenclature identifier.'), _mmb_field('PresDel_DESC', T.StringType(), 'Source presentation-at-delivery description.'), _mmb_field('GestationWeeks', T.IntegerType(), 'Best available gestation weeks, preferring parsed MAT GEST_DEL, then recorded NNU, then calculated NNU.'), _mmb_field('GestationDays', T.IntegerType(), 'Best available additional gestation days with the same source precedence as GestationWeeks.'), _mmb_field('BirthWeight', T.StringType(), 'Compatibility field retaining the raw MAT_BIRTH BIRTH_WT text including units.'), _mmb_field('BirthWeightGrams', T.DecimalType(10, 2), 'Best available numeric birth weight in grams, preferring parsed MAT_BIRTH then NNU.'), _mmb_field('BirthSex', T.StringType(), 'Best available raw sex description, preferring MAT_BIRTH then NNU.'), _mmb_field('BirthSex_CD', T.IntegerType(), 'Source MAT_BIRTH newborn-sex code.'), _mmb_field('APGAR1Min', T.IntegerType(), 'Best available APGAR score at one minute, preferring MAT_BIRTH then NNU.'), _mmb_field('APGAR5Min', T.IntegerType(), 'Best available APGAR score at five minutes, preferring MAT_BIRTH then NNU.'), _mmb_field('APGAR10Min', T.IntegerType(), 'NNU APGAR score at ten minutes.'), _mmb_field('FeedingMethod', T.StringType(), 'Source MAT_BIRTH initial-feeding description.'), _mmb_field('FeedingMethod_CD', T.IntegerType(), 'Source MAT_BIRTH initial-feeding nomenclature identifier.'), _mmb_field('CongenitalAnomalies', T.StringType(), 'Latest non-null NNU congenital-anomalies value.'), _mmb_field('MotherComplications', T.StringType(), 'Source maternal-complication description.'), _mmb_field('MotherComplications_SCT_CD', T.StringType(), 'Source maternal-complication SNOMED CT code or code list.'), _mmb_field('FetalComplications', T.StringType(), 'Source fetal-complication description.'), _mmb_field('FetalComplications_SCT_CD', T.StringType(), 'Source fetal-complication SNOMED CT code or code list.'), _mmb_field('NeonatalComplications', T.StringType(), 'Source neonatal-complication description.'), _mmb_field('NeonatalComplications_SCT_CD', T.StringType(), 'Source neonatal-complication SNOMED CT code or code list.'), _mmb_field('ResMethod', T.StringType(), 'Best available resuscitation description, preferring MAT_BIRTH then NNU.'), _mmb_field('ResMethod_CD', T.StringType(), 'Source MAT_BIRTH resuscitation nomenclature identifier or identifier list.'), _mmb_field('MaritalStatusMother', T.StringType(), 'Latest non-null NNU maternal marital-status value.'), _mmb_field('NeonatalDeathDateTime', T.TimestampType(), 'Best available neonatal death timestamp, preferring MAT_BIRTH then NNU.'), _mmb_field('ADC_UPDT', T.TimestampType(), 'Greatest row-relevant ADC timestamp across MAT_BIRTH, MAT_PREGNANCY and NNU; not used as a pipeline checkpoint.'), _mmb_field('MAT_BirthDateTime', T.TimestampType(), 'Unchanged MAT_BIRTH birth timestamp.'), _mmb_field('NNU_BirthDateTime', T.TimestampType(), 'Latest non-null NNU birth timestamp.'), _mmb_field('BirthDateTimeSource', T.StringType(), 'Source supplying BirthDateTime: MAT_BIRTH, NNU or null.'), _mmb_field('BirthDateTimeConflict_IND', T.BooleanType(), 'True when populated MAT and NNU birth timestamps differ.'), _mmb_field('BirthDateTimeFuture_IND', T.BooleanType(), 'True when the effective birth timestamp is later than pipeline execution time; no row is filtered.'), _mmb_field('GestationRaw', T.StringType(), 'Unchanged MAT_BIRTH GEST_DEL text.'), _mmb_field('MAT_GestationWeeks', T.IntegerType(), 'Weeks parsed from MAT_BIRTH GEST_DEL.'), _mmb_field('MAT_GestationDays', T.IntegerType(), 'Additional days parsed from MAT_BIRTH GEST_DEL.'), _mmb_field('NNU_GestationWeeks', T.IntegerType(), 'Latest non-null recorded NNU gestation weeks.'), _mmb_field('NNU_GestationDays', T.IntegerType(), 'Latest non-null recorded NNU gestation days.'), _mmb_field('NNU_GestationWeeksCalculated', T.IntegerType(), 'Latest non-null calculated NNU gestation weeks.'), _mmb_field('NNU_GestationDaysCalculated', T.IntegerType(), 'Latest non-null calculated NNU gestation days.'), _mmb_field('GestationSource', T.StringType(), 'Source supplying canonical gestation: MAT_BIRTH_GEST_DEL, NNU_RECORDED, NNU_CALCULATED or null.'), _mmb_field('GestationConflict_IND', T.BooleanType(), 'True when populated parsed MAT and recorded NNU gestation values differ.'), _mmb_field('MAT_BirthWeightGrams', T.DecimalType(10, 2), 'Numeric grams parsed from MAT_BIRTH BIRTH_WT.'), _mmb_field('NNU_BirthWeightGrams', T.DecimalType(10, 2), 'Latest non-null NNU birth weight in grams.'), _mmb_field('BirthWeightSource', T.StringType(), 'Source supplying BirthWeightGrams: MAT_BIRTH, NNU or null.'), _mmb_field('BirthWeightParseable_IND', T.BooleanType(), 'Whether a populated MAT_BIRTH BIRTH_WT value could be parsed numerically.'), _mmb_field('BirthWeightImplausible_IND', T.BooleanType(), 'True when parsed/coalesced grams are outside 200 to 7000; no row is filtered.'), _mmb_field('BirthWeightConflict_IND', T.BooleanType(), 'True when populated MAT and NNU birth weights differ.'), _mmb_field('MAT_BirthSex', T.StringType(), 'Unchanged nonblank MAT_BIRTH newborn-sex description.'), _mmb_field('NNU_BirthSex', T.StringType(), 'Latest non-null NNU sex value.'), _mmb_field('BirthSexSource', T.StringType(), 'Source supplying BirthSex: MAT_BIRTH, NNU or null.'), _mmb_field('BirthSexConflict_IND', T.BooleanType(), 'True when populated normalized MAT and NNU sex strings differ.'), _mmb_field('MAT_APGAR1Min', T.IntegerType(), 'MAT_BIRTH APGAR score at one minute.'), _mmb_field('NNU_APGAR1Min', T.IntegerType(), 'Latest non-null NNU APGAR score at one minute.'), _mmb_field('APGAR1MinSource', T.StringType(), 'Source supplying APGAR1Min: MAT_BIRTH, NNU or null.'), _mmb_field('APGAR1Conflict_IND', T.BooleanType(), 'True when populated MAT and NNU APGAR-1 values differ.'), _mmb_field('MAT_APGAR5Min', T.IntegerType(), 'MAT_BIRTH APGAR score at five minutes.'), _mmb_field('NNU_APGAR5Min', T.IntegerType(), 'Latest non-null NNU APGAR score at five minutes.'), _mmb_field('APGAR5MinSource', T.StringType(), 'Source supplying APGAR5Min: MAT_BIRTH, NNU or null.'), _mmb_field('APGAR5Conflict_IND', T.BooleanType(), 'True when populated MAT and NNU APGAR-5 values differ.'), _mmb_field('MAT_ResMethod', T.StringType(), 'Unchanged nonblank MAT_BIRTH resuscitation description.'), _mmb_field('NNU_ResMethod', T.StringType(), 'Latest non-null NNU resuscitation value.'), _mmb_field('ResMethodSource', T.StringType(), 'Source supplying ResMethod: MAT_BIRTH, NNU or null.'), _mmb_field('MAT_NeonatalDeathDateTime', T.TimestampType(), 'Unchanged MAT_BIRTH neonatal-death timestamp.'), _mmb_field('NNU_NeonatalDeathDateTime', T.TimestampType(), 'Latest non-null NNU date/time of death.'), _mmb_field('NeonatalDeathDateTimeSource', T.StringType(), 'Source supplying NeonatalDeathDateTime: MAT_BIRTH, NNU or null.'), _mmb_field('BabyAliveLabourOnset_CD', T.IntegerType(), 'Source nomenclature identifier for whether the baby was alive at labour onset.'), _mmb_field('BabyAliveLabourOnset_DESC', T.StringType(), 'Source description for whether the baby was alive at labour onset.'), _mmb_field('BirthLocationDetail_CD', T.IntegerType(), 'Source detailed birth-location nomenclature identifier.'), _mmb_field('BirthLocationDetail_DESC', T.StringType(), 'Source detailed birth-location description.'), _mmb_field('MembraneRuptureDateTime', T.TimestampType(), 'Source membrane-rupture timestamp.'), _mmb_field('CaesareanUrgency_CD', T.IntegerType(), 'Source nomenclature identifier for caesarean urgency grade.'), _mmb_field('CaesareanUrgency_DESC', T.StringType(), 'Source caesarean urgency description.'), _mmb_field('CordPHResult', T.DoubleType(), 'Source cord blood pH result.'), _mmb_field('WaterBirth_CD', T.IntegerType(), 'Source waterbirth nomenclature identifier.'), _mmb_field('WaterBirth_DESC', T.StringType(), 'Source waterbirth description.'), _mmb_field('BornBeforeArrival_CD', T.IntegerType(), 'Source born-before-arrival nomenclature identifier.'), _mmb_field('BornBeforeArrival_DESC', T.StringType(), 'Source born-before-arrival description.'), _mmb_field('LabourStartDateTime', T.TimestampType(), 'Source MAT_BIRTH labour-start timestamp.'), _mmb_field('LabourLengthRaw', T.StringType(), 'Unchanged source total-labour-length text.'), _mmb_field('CaesareanIndication_CD', T.IntegerType(), 'Source caesarean-indication nomenclature identifier.'), _mmb_field('CaesareanIndication_DESC', T.StringType(), 'Source caesarean-indication description.'), _mmb_field('NeonatalCareLevel_CD', T.IntegerType(), 'Source neonatal-care-level nomenclature identifier.'), _mmb_field('NeonatalCareLevel_DESC', T.StringType(), 'Source neonatal-care-level description.'), _mmb_field('NewbornGestationalAssessment_CD', T.IntegerType(), 'Source newborn-gestational-assessment nomenclature identifier.'), _mmb_field('NewbornGestationalAssessment_DESC', T.StringType(), 'Source newborn-gestational-assessment description.'), _mmb_field('NNU_FinalOutcome', T.StringType(), 'Latest non-null NNU final outcome.'), _mmb_field('NNU_BirthLength', T.DecimalType(10, 2), 'Latest non-null NNU birth length.'), _mmb_field('NNU_BirthHeadCircumference', T.DecimalType(10, 2), 'Latest non-null NNU birth head circumference.'), _mmb_field('NNU_PlaceOfBirthName', T.StringType(), 'Latest non-null NNU place-of-birth name.'), _mmb_field('Baby_NHS_NormalizationChanged_IND', T.BooleanType(), 'True when digits-only normalization changed the nonblank raw NHS value.'), _mmb_field('Baby_NHS_ValidLength_IND', T.BooleanType(), 'True when normalized NHS contains ten digits; null when no normalized NHS exists.'), _mmb_field('BusinessKeyCollisionCount', T.LongType(), 'Count of MAT_BIRTH source rows sharing Pregnancy_ID and BirthOrder.'), _mmb_field('BusinessKeyCollision_IND', T.BooleanType(), 'True when Pregnancy_ID and BirthOrder identify more than one source row.'), _mmb_field('BirthOrderMissing_IND', T.BooleanType(), 'True when source BIRTH_ODR_NBR is null.'), _mmb_field('PregnancyChildIDZero_IND', T.BooleanType(), 'True when source PREGNANCY_CHILD_ID is the zero sentinel.'), _mmb_field('PregnancyJoinMatched_IND', T.BooleanType(), 'True when MAT_PREGNANCY supplied a selected enrichment row.'), _mmb_field('PregnancySourceRowCount', T.LongType(), 'Number of MAT_PREGNANCY rows sharing the selected Pregnancy_ID.'), _mmb_field('NNUJoinMatched_IND', T.BooleanType(), 'True when a normalized NHS match to NNU_EPISODES was found.'), _mmb_field('NNUEpisodeCount', T.LongType(), 'Number of NNU source rows sharing normalized NationalIDBaby.'), _mmb_field('BirthSource_DELETE_IND', T.IntegerType(), 'Unchanged MAT_BIRTH soft-delete indicator; no filtering is applied.'), _mmb_field('PregnancySource_DELETE_IND', T.IntegerType(), 'Soft-delete indicator on the selected MAT_PREGNANCY enrichment row.'), _mmb_field('BirthSourceCtrl_ID', T.IntegerType(), 'MAT_BIRTH source control identifier.'), _mmb_field('BirthSourceRecordUpdatedDateTime', T.TimestampType(), 'MAT_BIRTH source record-updated timestamp.'), _mmb_field('BirthSourceADC_UPDT', T.TimestampType(), 'MAT_BIRTH ADC update timestamp.'), _mmb_field('PregnancySourceCtrl_ID', T.IntegerType(), 'MAT_PREGNANCY source control identifier.'), _mmb_field('PregnancySourceRecordUpdatedDateTime', T.TimestampType(), 'MAT_PREGNANCY source record-updated timestamp.'), _mmb_field('PregnancySourceADC_UPDT', T.TimestampType(), 'MAT_PREGNANCY ADC update timestamp.'), _mmb_field('NNUSourceLastUpdate', T.TimestampType(), 'Greatest NNU LastUpdate for the linked normalized NHS identifier.'), _mmb_field('NNUSourceRecordTimestamp', T.TimestampType(), 'Greatest NNU RecordTimestamp for the linked normalized NHS identifier.'), _mmb_field('NNUSourceADC_UPDT', T.TimestampType(), 'Greatest NNU ADC_UPDT for the linked normalized NHS identifier.'), _mmb_field('BirthSourceVersion', T.LongType(), 'MAT_BIRTH Delta version used by the row build.'), _mmb_field('PregnancySourceVersion', T.LongType(), 'MAT_PREGNANCY Delta version used by the row build.'), _mmb_field('NNUSourceVersion', T.LongType(), 'NNU_EPISODES Delta version used by the row build.'), _mmb_field('ROW_HASH', T.LongType(), 'Stable hash of source-derived row content used to avoid unchanged rewrites.'), _mmb_field('PIPELINE_RUN_ID', T.StringType(), 'Run identifier that last materially changed the target row.'), _mmb_field('PIPELINE_UPDT_DT_TM', T.TimestampType(), 'Pipeline timestamp when the row was inserted or materially updated.')])

def _mmb_sql_identifier(table_name: str) -> str:
    tick = chr(96)
    return '.'.join((f'{tick}{part.replace(tick, tick + tick)}{tick}' for part in table_name.split('.')))

def _mmb_sql_column(column_name: str) -> str:
    tick = chr(96)
    return f'{tick}{column_name.replace(tick, tick + tick)}{tick}'

def _mmb_escape_sql_string(value: str) -> str:
    return value.replace('\\', '\\\\').replace("'", "''")

def _mmb_table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _mmb_latest_delta_version(table_name: str) -> int:
    row = spark.sql(f'DESCRIBE HISTORY {_mmb_sql_identifier(table_name)} LIMIT 1').select('version').first()
    if row is None:
        raise RuntimeError(f'No Delta history returned for {table_name}')
    return int(row['version'])

def _mmb_capture_source_versions(config: MapMatBirthConfig) -> Dict[str, int]:
    return {config.birth_source_table: _mmb_latest_delta_version(config.birth_source_table), config.pregnancy_source_table: _mmb_latest_delta_version(config.pregnancy_source_table), config.nnu_source_table: _mmb_latest_delta_version(config.nnu_source_table)}

def _mmb_read_snapshot(table_name: str, version: int) -> DataFrame:
    return _m40_hardening_read_pinned_snapshot(table_name, int(version))

def _mmb_nonblank(column: F.Column) -> F.Column:
    text = column.cast('string')
    return F.when(F.length(F.trim(text)) > 0, text)

def _mmb_normalize_nhs(column: F.Column) -> F.Column:
    digits = F.regexp_replace(F.trim(column.cast('string')), '[^0-9]', '')
    return F.when(F.length(digits) > 0, digits)

def _mmb_parse_gestation_weeks(column: F.Column) -> F.Column:
    extracted = F.regexp_extract(column.cast('string'), '(?i)([0-9]+)\\s*weeks?', 1)
    return F.when(F.length(extracted) > 0, extracted.cast('int'))

def _mmb_parse_gestation_days(column: F.Column) -> F.Column:
    extracted = F.regexp_extract(column.cast('string'), '(?i)([0-9]+)\\s*days?', 1)
    return F.when(F.length(extracted) > 0, extracted.cast('int'))

def _mmb_parse_birth_weight_grams(column: F.Column) -> F.Column:
    extracted = F.regexp_extract(column.cast('string'), '([0-9]+(?:\\.[0-9]+)?)', 1)
    return F.when(F.length(extracted) > 0, extracted.cast(T.DecimalType(10, 2)))

def _mmb_stable_hash(columns: Sequence[F.Column]) -> F.Column:
    return F.xxhash64(*[F.coalesce(column.cast('string'), F.lit('<NULL>')) for column in columns])

def _mmb_birth_row_id() -> F.Column:
    fallback_columns = ['PREGNANCY_ID', 'PREGNANCY_CHILD_ID', 'BIRTH_ODR_NBR', 'BABY_PERSON_ID', 'MRN', 'NHS', 'BIRTH_DT_TM']
    fallback_hash = F.sha2(F.to_json(F.struct(*[F.col(name) for name in fallback_columns])), 256)
    return F.when(F.col('PREGNANCY_CHILD_SEQ_ID').isNotNull(), F.concat(F.lit('SEQ:'), F.col('PREGNANCY_CHILD_SEQ_ID').cast('string'))).otherwise(F.concat(F.lit('FALLBACK:'), fallback_hash))

def _mmb_align_to_schema(df: DataFrame, schema: T.StructType=schema_map_mat_birth) -> DataFrame:
    missing = [field.name for field in schema.fields if field.name not in df.columns]
    if missing:
        raise RuntimeError(f'map_mat_birth output is missing required columns: {missing}')
    return df.select(*[F.col(field.name).cast(field.dataType).alias(field.name, metadata=field.metadata) for field in schema.fields])

def _mmb_boolean_conflict(left: F.Column, right: F.Column) -> F.Column:
    return left.isNotNull() & right.isNotNull() & (left != right)

def _mmb_source_label(primary: F.Column, secondary: F.Column, primary_label: str, secondary_label: str) -> F.Column:
    return F.when(primary.isNotNull(), F.lit(primary_label)).when(secondary.isNotNull(), F.lit(secondary_label))

def _mmb_prepare_birth_source(raw_birth: DataFrame) -> DataFrame:
    business_window = Window.partitionBy('PREGNANCY_ID', 'BIRTH_ODR_NBR')
    return raw_birth.withColumn('BirthRow_ID', _mmb_birth_row_id()).withColumn('_NHS_NORMALIZED', _mmb_normalize_nhs(F.col('NHS'))).withColumn('_MAT_GESTATION_WEEKS', _mmb_parse_gestation_weeks(F.col('GEST_DEL'))).withColumn('_MAT_GESTATION_DAYS', _mmb_parse_gestation_days(F.col('GEST_DEL'))).withColumn('_MAT_BIRTH_WEIGHT_GRAMS', _mmb_parse_birth_weight_grams(F.col('BIRTH_WT'))).withColumn('_BUSINESS_KEY_COLLISION_COUNT', F.count(F.lit(1)).over(business_window).cast('long'))

def _mmb_prepare_pregnancy_source(raw_pregnancy: DataFrame) -> DataFrame:
    partition = Window.partitionBy('PREGNANCY_ID')
    ordering = partition.orderBy((F.col('DELETE_IND') == 0).cast('int').desc(), F.col('Record_Updated_Dt').desc_nulls_last(), F.col('ADC_UPDT').desc_nulls_last(), F.col('Ctrl_Id').desc_nulls_last(), F.col('PERSON_ID').desc_nulls_last())
    tie_hash = _mmb_stable_hash([F.col('PREGNANCY_ID'), F.col('PERSON_ID'), F.col('DELETE_IND'), F.col('Record_Updated_Dt'), F.col('ADC_UPDT'), F.col('Ctrl_Id')])
    ranked = raw_pregnancy.withColumn('_PREG_TIE_HASH', tie_hash).withColumn('_PREG_SOURCE_ROW_COUNT', F.count(F.lit(1)).over(partition).cast('long')).withColumn('_PREG_RN', F.row_number().over(partition.orderBy((F.col('DELETE_IND') == 0).cast('int').desc(), F.col('Record_Updated_Dt').desc_nulls_last(), F.col('ADC_UPDT').desc_nulls_last(), F.col('Ctrl_Id').desc_nulls_last(), F.col('PERSON_ID').desc_nulls_last(), F.col('_PREG_TIE_HASH').desc())))
    return ranked.filter(F.col('_PREG_RN') == 1).select(F.col('PREGNANCY_ID').alias('_PREGNANCY_ID_JOIN'), F.col('PERSON_ID').cast('long').alias('_MOTHER_PERSON_ID'), F.col('DELETE_IND').cast('int').alias('_PREGNANCY_DELETE_IND'), F.col('Ctrl_Id').cast('int').alias('_PREGNANCY_CTRL_ID'), F.col('Record_Updated_Dt').cast('timestamp').alias('_PREGNANCY_RECORD_UPDATED'), F.col('ADC_UPDT').cast('timestamp').alias('_PREGNANCY_ADC_UPDT'), F.col('_PREG_SOURCE_ROW_COUNT'))
NNU_VALUE_COLUMNS: Dict[str, str] = {'BirthTimeBaby': '_NNU_BIRTH_DATETIME', 'GestationWeeks': '_NNU_GESTATION_WEEKS', 'GestationDays': '_NNU_GESTATION_DAYS', 'GestationWeeksCalculated': '_NNU_GESTATION_WEEKS_CALCULATED', 'GestationDaysCalculated': '_NNU_GESTATION_DAYS_CALCULATED', 'Birthweight': '_NNU_BIRTH_WEIGHT_GRAMS', 'BirthLength': '_NNU_BIRTH_LENGTH', 'BirthHeadCircumference': '_NNU_BIRTH_HEAD_CIRCUMFERENCE', 'Sex': '_NNU_BIRTH_SEX', 'FetusNumber': '_NNU_FETUS_NUMBER', 'Apgar1': '_NNU_APGAR_1', 'Apgar5': '_NNU_APGAR_5', 'Apgar10': '_NNU_APGAR_10', 'Resuscitation': '_NNU_RESUSCITATION', 'CongenitalAnomalies': '_NNU_CONGENITAL_ANOMALIES', 'MaritalStatusMother': '_NNU_MARITAL_STATUS_MOTHER', 'FinalNNUOutcome': '_NNU_FINAL_OUTCOME', 'DateTimeOfDeath': '_NNU_DATETIME_OF_DEATH', 'PlaceOfBirthName': '_NNU_PLACE_OF_BIRTH_NAME'}

def _mmb_prepare_nnu_source(raw_nnu: DataFrame) -> DataFrame:
    base = raw_nnu.withColumn('_NHS_NORMALIZED', _mmb_normalize_nhs(F.col('NationalIDBaby'))).withColumn('_NNU_TIE_HASH', _mmb_stable_hash([F.col('NationalIDBaby'), F.col('LastUpdate'), F.col('RecordTimestamp'), F.col('ADC_UPDT'), F.col('RowHash'), *[F.col(name) for name in NNU_VALUE_COLUMNS]]))
    partition = Window.partitionBy('_NHS_NORMALIZED')
    ordered = partition.orderBy(F.col('LastUpdate').desc_nulls_last(), F.col('RecordTimestamp').desc_nulls_last(), F.col('ADC_UPDT').desc_nulls_last(), F.col('RowHash').desc_nulls_last(), F.col('_NNU_TIE_HASH').desc())
    full_ordered = ordered.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    enriched = base.withColumn('_NNU_RN', F.row_number().over(ordered)).withColumn('_NNU_EPISODE_COUNT', F.count(F.lit(1)).over(partition).cast('long')).withColumn('_NNU_SOURCE_LAST_UPDATE', F.max('LastUpdate').over(partition)).withColumn('_NNU_SOURCE_RECORD_TIMESTAMP', F.max('RecordTimestamp').over(partition)).withColumn('_NNU_SOURCE_ADC_UPDT', F.max('ADC_UPDT').over(partition))
    for source_name, target_name in NNU_VALUE_COLUMNS.items():
        enriched = enriched.withColumn(target_name, F.first(F.col(source_name), ignorenulls=True).over(full_ordered))
    return enriched.filter(F.col('_NNU_RN') == 1).select('_NHS_NORMALIZED', *NNU_VALUE_COLUMNS.values(), '_NNU_EPISODE_COUNT', '_NNU_SOURCE_LAST_UPDATE', '_NNU_SOURCE_RECORD_TIMESTAMP', '_NNU_SOURCE_ADC_UPDT')

def _mmb_build_target_rows(birth_snapshot: DataFrame, pregnancy_snapshot: DataFrame, nnu_snapshot: DataFrame, source_versions: Dict[str, int], run_id: str, run_timestamp: datetime, config: MapMatBirthConfig=MAP_MAT_BIRTH_CONFIG) -> DataFrame:
    birth = _mmb_prepare_birth_source(birth_snapshot).alias('b')
    pregnancy = _mmb_prepare_pregnancy_source(pregnancy_snapshot).alias('p')
    nnu = _mmb_prepare_nnu_source(nnu_snapshot).alias('n')
    joined = birth.join(pregnancy, F.col('b.PREGNANCY_ID') == F.col('p._PREGNANCY_ID_JOIN'), 'left').join(nnu, F.col('b._NHS_NORMALIZED') == F.col('n._NHS_NORMALIZED'), 'left')

    def b(name: str) -> F.Column:
        return F.col(f'b.{name}')

    def p(name: str) -> F.Column:
        return F.col(f'p.{name}')

    def n(name: str) -> F.Column:
        return F.col(f'n.{name}')
    raw_nhs = _mmb_nonblank(b('NHS'))
    normalized_nhs = b('_NHS_NORMALIZED')
    mat_birth_datetime = b('BIRTH_DT_TM').cast('timestamp')
    nnu_birth_datetime = n('_NNU_BIRTH_DATETIME').cast('timestamp')
    birth_datetime = F.coalesce(mat_birth_datetime, nnu_birth_datetime)
    birth_datetime_source = _mmb_source_label(mat_birth_datetime, nnu_birth_datetime, 'MAT_BIRTH', 'NNU')
    gestation_raw = _mmb_nonblank(b('GEST_DEL'))
    mat_gestation_weeks = b('_MAT_GESTATION_WEEKS').cast('int')
    mat_gestation_days = b('_MAT_GESTATION_DAYS').cast('int')
    nnu_gestation_weeks = n('_NNU_GESTATION_WEEKS').cast('int')
    nnu_gestation_days = n('_NNU_GESTATION_DAYS').cast('int')
    nnu_gestation_weeks_calculated = n('_NNU_GESTATION_WEEKS_CALCULATED').cast('int')
    nnu_gestation_days_calculated = n('_NNU_GESTATION_DAYS_CALCULATED').cast('int')
    gestation_weeks = F.coalesce(mat_gestation_weeks, nnu_gestation_weeks, nnu_gestation_weeks_calculated)
    gestation_days = F.coalesce(mat_gestation_days, nnu_gestation_days, nnu_gestation_days_calculated)
    mat_gestation_available = mat_gestation_weeks.isNotNull() | mat_gestation_days.isNotNull()
    nnu_recorded_gestation_available = nnu_gestation_weeks.isNotNull() | nnu_gestation_days.isNotNull()
    nnu_calculated_gestation_available = nnu_gestation_weeks_calculated.isNotNull() | nnu_gestation_days_calculated.isNotNull()
    gestation_source = F.when(mat_gestation_available, F.lit('MAT_BIRTH_GEST_DEL')).when(nnu_recorded_gestation_available, F.lit('NNU_RECORDED')).when(nnu_calculated_gestation_available, F.lit('NNU_CALCULATED'))
    gestation_conflict = _mmb_boolean_conflict(mat_gestation_weeks, nnu_gestation_weeks) | _mmb_boolean_conflict(mat_gestation_days, nnu_gestation_days)
    birth_weight_raw = _mmb_nonblank(b('BIRTH_WT'))
    mat_birth_weight_grams = b('_MAT_BIRTH_WEIGHT_GRAMS').cast(T.DecimalType(10, 2))
    nnu_birth_weight_grams = n('_NNU_BIRTH_WEIGHT_GRAMS').cast(T.DecimalType(10, 2))
    birth_weight_grams = F.coalesce(mat_birth_weight_grams, nnu_birth_weight_grams)
    birth_weight_source = _mmb_source_label(mat_birth_weight_grams, nnu_birth_weight_grams, 'MAT_BIRTH', 'NNU')
    birth_weight_parseable = F.when(birth_weight_raw.isNull(), F.lit(None).cast('boolean')).otherwise(mat_birth_weight_grams.isNotNull())
    birth_weight_implausible = F.when(birth_weight_grams.isNull(), F.lit(None).cast('boolean')).otherwise((birth_weight_grams < F.lit(200)) | (birth_weight_grams > F.lit(7000)))
    mat_birth_sex = _mmb_nonblank(b('NB_SEX_DESC'))
    nnu_birth_sex = _mmb_nonblank(n('_NNU_BIRTH_SEX'))
    birth_sex = F.coalesce(mat_birth_sex, nnu_birth_sex)
    birth_sex_source = _mmb_source_label(mat_birth_sex, nnu_birth_sex, 'MAT_BIRTH', 'NNU')
    birth_sex_conflict = _mmb_boolean_conflict(F.lower(F.trim(mat_birth_sex)), F.lower(F.trim(nnu_birth_sex)))
    mat_apgar_1 = b('APGAR_1MIN').cast('int')
    nnu_apgar_1 = n('_NNU_APGAR_1').cast('int')
    apgar_1 = F.coalesce(mat_apgar_1, nnu_apgar_1)
    apgar_1_source = _mmb_source_label(mat_apgar_1, nnu_apgar_1, 'MAT_BIRTH', 'NNU')
    mat_apgar_5 = b('APGAR_5MIN').cast('int')
    nnu_apgar_5 = n('_NNU_APGAR_5').cast('int')
    apgar_5 = F.coalesce(mat_apgar_5, nnu_apgar_5)
    apgar_5_source = _mmb_source_label(mat_apgar_5, nnu_apgar_5, 'MAT_BIRTH', 'NNU')
    mat_resuscitation = _mmb_nonblank(b('RESUS_METHOD_DESC'))
    nnu_resuscitation = _mmb_nonblank(n('_NNU_RESUSCITATION'))
    resuscitation = F.coalesce(mat_resuscitation, nnu_resuscitation)
    resuscitation_source = _mmb_source_label(mat_resuscitation, nnu_resuscitation, 'MAT_BIRTH', 'NNU')
    mat_neonatal_death = b('NEO_DEATH_DT_TM').cast('timestamp')
    nnu_neonatal_death = n('_NNU_DATETIME_OF_DEATH').cast('timestamp')
    neonatal_death = F.coalesce(mat_neonatal_death, nnu_neonatal_death)
    neonatal_death_source = _mmb_source_label(mat_neonatal_death, nnu_neonatal_death, 'MAT_BIRTH', 'NNU')
    birth_source_adc = b('ADC_UPDT').cast('timestamp')
    pregnancy_source_adc = p('_PREGNANCY_ADC_UPDT').cast('timestamp')
    nnu_source_adc = n('_NNU_SOURCE_ADC_UPDT').cast('timestamp')
    row_adc_updt = F.greatest(birth_source_adc, pregnancy_source_adc, nnu_source_adc)
    run_timestamp_column = F.lit(run_timestamp).cast('timestamp')
    base = joined.select(b('BirthRow_ID').cast('string').alias('BirthRow_ID'), p('_MOTHER_PERSON_ID').cast('long').alias('MotherPerson_ID'), b('PREGNANCY_ID').cast('long').alias('Pregnancy_ID'), b('PREGNANCY_CHILD_ID').cast('long').alias('PregnancyChild_ID'), b('PREGNANCY_CHILD_SEQ_ID').cast('long').alias('PregnancyChildSeq_ID'), b('BABY_PERSON_ID').cast('long').alias('BabyPerson_ID'), b('MRN').cast('string').alias('Baby_MRN'), normalized_nhs.cast('string').alias('Baby_NHS'), b('NHS').cast('string').alias('Baby_NHS_Raw'), b('BIRTH_ODR_NBR').cast('int').alias('BirthOrder'), b('BIRTH_NBR').cast('int').alias('BirthNumber'), n('_NNU_FETUS_NUMBER').cast('int').alias('FetusNumber'), b('BIRTH_LOC_NM_ID').cast('int').alias('BirthLocation_CD'), b('BIRTH_LOC_DESC').cast('string').alias('BirthLocation_DESC'), birth_datetime.alias('BirthDateTime'), b('DEL_METHOD_CD').cast('int').alias('DeliveryMethod_CD'), b('DEL_METHOD_DESC').cast('string').alias('DeliveryMethod_DESC'), b('DEL_OUTCOME_CD').cast('int').alias('DeliveryOutcome_CD'), b('DEL_OUTCOME_DESC').cast('string').alias('DeliveryOutcome_DESC'), b('NEO_OUTCOME_CD').cast('int').alias('NeonatalOutcome_CD'), b('NEO_OUTCOME_DESC').cast('string').alias('NeonatalOutcome_DESC'), b('PREG_OUTCOME_CD').cast('int').alias('PregOutcome_CD'), b('PREG_OUTCOME_DESC').cast('string').alias('PregOutcome_DESC'), b('PRES_DEL_NM_ID').cast('int').alias('PresDel_CD'), b('PRES_DEL_DESC').cast('string').alias('PresDel_DESC'), gestation_weeks.alias('GestationWeeks'), gestation_days.alias('GestationDays'), b('BIRTH_WT').cast('string').alias('BirthWeight'), birth_weight_grams.alias('BirthWeightGrams'), birth_sex.alias('BirthSex'), b('NB_SEX_CD').cast('int').alias('BirthSex_CD'), apgar_1.alias('APGAR1Min'), apgar_5.alias('APGAR5Min'), n('_NNU_APGAR_10').cast('int').alias('APGAR10Min'), b('FEEDING_METHOD_DESC').cast('string').alias('FeedingMethod'), b('FEEDING_METHOD_NM_ID').cast('int').alias('FeedingMethod_CD'), n('_NNU_CONGENITAL_ANOMALIES').cast('string').alias('CongenitalAnomalies'), b('MOTHER_COMPLICATION_DESC').cast('string').alias('MotherComplications'), b('MOTHER_COMPLICATION_SCT_CD').cast('string').alias('MotherComplications_SCT_CD'), b('FETAL_COMPLICATION_DESC').cast('string').alias('FetalComplications'), b('FETAL_COMPLICATION_SCT_CD').cast('string').alias('FetalComplications_SCT_CD'), b('NEONATAL_COMPLICATION_DESC').cast('string').alias('NeonatalComplications'), b('NEONATAL_COMPLICATION_SCT_CD').cast('string').alias('NeonatalComplications_SCT_CD'), resuscitation.alias('ResMethod'), b('RESUS_METHOD_NM_ID').cast('string').alias('ResMethod_CD'), n('_NNU_MARITAL_STATUS_MOTHER').cast('string').alias('MaritalStatusMother'), neonatal_death.alias('NeonatalDeathDateTime'), row_adc_updt.alias('ADC_UPDT'), mat_birth_datetime.alias('MAT_BirthDateTime'), nnu_birth_datetime.alias('NNU_BirthDateTime'), birth_datetime_source.alias('BirthDateTimeSource'), _mmb_boolean_conflict(mat_birth_datetime, nnu_birth_datetime).alias('BirthDateTimeConflict_IND'), F.when(birth_datetime.isNull(), F.lit(None).cast('boolean')).otherwise(birth_datetime > run_timestamp_column).alias('BirthDateTimeFuture_IND'), gestation_raw.alias('GestationRaw'), mat_gestation_weeks.alias('MAT_GestationWeeks'), mat_gestation_days.alias('MAT_GestationDays'), nnu_gestation_weeks.alias('NNU_GestationWeeks'), nnu_gestation_days.alias('NNU_GestationDays'), nnu_gestation_weeks_calculated.alias('NNU_GestationWeeksCalculated'), nnu_gestation_days_calculated.alias('NNU_GestationDaysCalculated'), gestation_source.alias('GestationSource'), gestation_conflict.alias('GestationConflict_IND'), mat_birth_weight_grams.alias('MAT_BirthWeightGrams'), nnu_birth_weight_grams.alias('NNU_BirthWeightGrams'), birth_weight_source.alias('BirthWeightSource'), birth_weight_parseable.alias('BirthWeightParseable_IND'), birth_weight_implausible.alias('BirthWeightImplausible_IND'), _mmb_boolean_conflict(mat_birth_weight_grams, nnu_birth_weight_grams).alias('BirthWeightConflict_IND'), mat_birth_sex.alias('MAT_BirthSex'), nnu_birth_sex.alias('NNU_BirthSex'), birth_sex_source.alias('BirthSexSource'), birth_sex_conflict.alias('BirthSexConflict_IND'), mat_apgar_1.alias('MAT_APGAR1Min'), nnu_apgar_1.alias('NNU_APGAR1Min'), apgar_1_source.alias('APGAR1MinSource'), _mmb_boolean_conflict(mat_apgar_1, nnu_apgar_1).alias('APGAR1Conflict_IND'), mat_apgar_5.alias('MAT_APGAR5Min'), nnu_apgar_5.alias('NNU_APGAR5Min'), apgar_5_source.alias('APGAR5MinSource'), _mmb_boolean_conflict(mat_apgar_5, nnu_apgar_5).alias('APGAR5Conflict_IND'), mat_resuscitation.alias('MAT_ResMethod'), nnu_resuscitation.alias('NNU_ResMethod'), resuscitation_source.alias('ResMethodSource'), mat_neonatal_death.alias('MAT_NeonatalDeathDateTime'), nnu_neonatal_death.alias('NNU_NeonatalDeathDateTime'), neonatal_death_source.alias('NeonatalDeathDateTimeSource'), b('BABY_ALIVE_LABOUR_ONSET_NM_ID').cast('int').alias('BabyAliveLabourOnset_CD'), b('BABY_ALIVE_LABOUR_ONSET_DESC').cast('string').alias('BabyAliveLabourOnset_DESC'), b('BIRTH_LOC_DETAIL_NM_ID').cast('int').alias('BirthLocationDetail_CD'), b('BIRTH_LOC_DETAIL_DESC').cast('string').alias('BirthLocationDetail_DESC'), b('MEM_RUPTURE_DT_TM').cast('timestamp').alias('MembraneRuptureDateTime'), b('GRADE_OF_URGENCY_OF_THE_CAESAREAN_NM_ID').cast('int').alias('CaesareanUrgency_CD'), b('GRADE_OF_URGENCY_OF_THE_CAESAREAN_DESC').cast('string').alias('CaesareanUrgency_DESC'), b('CORD_PH_RESULT').cast('double').alias('CordPHResult'), b('WATERBIRTH_NM_ID').cast('int').alias('WaterBirth_CD'), b('WATERBIRTH_DESC').cast('string').alias('WaterBirth_DESC'), b('BBA_NM_ID').cast('int').alias('BornBeforeArrival_CD'), b('BBA_DESC').cast('string').alias('BornBeforeArrival_DESC'), b('LAB_START_DT_TM').cast('timestamp').alias('LabourStartDateTime'), b('LENGTH_LABOUR_TOT').cast('string').alias('LabourLengthRaw'), b('C_SECT_INDICATION_NM_ID').cast('int').alias('CaesareanIndication_CD'), b('C_SECT_INDICATION_DESC').cast('string').alias('CaesareanIndication_DESC'), b('NEO_CARE_LEVEL_NM_ID').cast('int').alias('NeonatalCareLevel_CD'), b('NEO_CARE_LEVEL_DESC').cast('string').alias('NeonatalCareLevel_DESC'), b('NB_GEST_ASSESS_NM_ID').cast('int').alias('NewbornGestationalAssessment_CD'), b('NB_GEST_ASSESS_DESC').cast('string').alias('NewbornGestationalAssessment_DESC'), n('_NNU_FINAL_OUTCOME').cast('string').alias('NNU_FinalOutcome'), n('_NNU_BIRTH_LENGTH').cast(T.DecimalType(10, 2)).alias('NNU_BirthLength'), n('_NNU_BIRTH_HEAD_CIRCUMFERENCE').cast(T.DecimalType(10, 2)).alias('NNU_BirthHeadCircumference'), n('_NNU_PLACE_OF_BIRTH_NAME').cast('string').alias('NNU_PlaceOfBirthName'), F.when(raw_nhs.isNull(), F.lit(None).cast('boolean')).otherwise(F.trim(raw_nhs) != normalized_nhs).alias('Baby_NHS_NormalizationChanged_IND'), F.when(normalized_nhs.isNull(), F.lit(None).cast('boolean')).otherwise(F.length(normalized_nhs) == 10).alias('Baby_NHS_ValidLength_IND'), b('_BUSINESS_KEY_COLLISION_COUNT').cast('long').alias('BusinessKeyCollisionCount'), (b('_BUSINESS_KEY_COLLISION_COUNT') > 1).alias('BusinessKeyCollision_IND'), b('BIRTH_ODR_NBR').isNull().alias('BirthOrderMissing_IND'), (b('PREGNANCY_CHILD_ID') == 0).alias('PregnancyChildIDZero_IND'), p('_PREGNANCY_ID_JOIN').isNotNull().alias('PregnancyJoinMatched_IND'), p('_PREG_SOURCE_ROW_COUNT').cast('long').alias('PregnancySourceRowCount'), n('_NHS_NORMALIZED').isNotNull().alias('NNUJoinMatched_IND'), n('_NNU_EPISODE_COUNT').cast('long').alias('NNUEpisodeCount'), b('DELETE_IND').cast('int').alias('BirthSource_DELETE_IND'), p('_PREGNANCY_DELETE_IND').cast('int').alias('PregnancySource_DELETE_IND'), b('Ctrl_Id').cast('int').alias('BirthSourceCtrl_ID'), b('Record_Updated_Dt').cast('timestamp').alias('BirthSourceRecordUpdatedDateTime'), birth_source_adc.alias('BirthSourceADC_UPDT'), p('_PREGNANCY_CTRL_ID').cast('int').alias('PregnancySourceCtrl_ID'), p('_PREGNANCY_RECORD_UPDATED').cast('timestamp').alias('PregnancySourceRecordUpdatedDateTime'), pregnancy_source_adc.alias('PregnancySourceADC_UPDT'), n('_NNU_SOURCE_LAST_UPDATE').cast('timestamp').alias('NNUSourceLastUpdate'), n('_NNU_SOURCE_RECORD_TIMESTAMP').cast('timestamp').alias('NNUSourceRecordTimestamp'), nnu_source_adc.alias('NNUSourceADC_UPDT'), F.lit(source_versions[config.birth_source_table]).cast('long').alias('BirthSourceVersion'), F.lit(source_versions[config.pregnancy_source_table]).cast('long').alias('PregnancySourceVersion'), F.lit(source_versions[config.nnu_source_table]).cast('long').alias('NNUSourceVersion'))
    hash_exclusions = {'BirthSourceVersion', 'PregnancySourceVersion', 'NNUSourceVersion', 'ROW_HASH', 'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM'}
    hash_columns = [F.col(name) for name in base.columns if name not in hash_exclusions]
    final_df = base.withColumn('ROW_HASH', _mmb_stable_hash(hash_columns)).withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_UPDT_DT_TM', run_timestamp_column)
    return _mmb_align_to_schema(final_df)

def _mmb_get_table_properties(table_name: str) -> Dict[str, str]:
    if not _mmb_table_exists(table_name):
        return {}
    return {row['key']: row['value'] for row in spark.sql(f'SHOW TBLPROPERTIES {_mmb_sql_identifier(table_name)}').collect()}

def _mmb_target_properties(config: MapMatBirthConfig) -> Dict[str, str]:
    return {'delta.enableChangeDataFeed': 'true', 'delta.enableDeletionVectors': 'true', 'delta.enableRowTracking': 'true', 'delta.autoOptimize.optimizeWrite': 'true', 'delta.autoOptimize.autoCompact': 'true', 'delta.deletedFileRetentionDuration': 'interval 30 days', 'pipeline.map_mat_birth.schema_version': config.schema_version, 'pipeline.map_mat_birth.load_mode': 'full_source_snapshot_merge', 'pipeline.map_mat_birth.row_key': 'BirthRow_ID', 'pipeline.map_mat_birth.row_filtering': 'none'}

def _mmb_set_table_properties_if_changed(table_name: str, properties: Dict[str, str]) -> None:
    current = _mmb_get_table_properties(table_name)
    changed = {key: value for key, value in properties.items() if current.get(key) != value}
    if not changed:
        return
    assignments = ', '.join((f"'{_mmb_escape_sql_string(key)}' = '{_mmb_escape_sql_string(value)}'" for key, value in changed.items()))
    spark.sql(f'ALTER TABLE {_mmb_sql_identifier(table_name)} SET TBLPROPERTIES ({assignments})')

def _mmb_table_description(table_name: str) -> Optional[str]:
    if not _mmb_table_exists(table_name):
        return None
    try:
        return spark.catalog.getTable(table_name).description
    except Exception:
        return None

def _mmb_set_table_comment_if_changed(table_name: str, comment: str) -> None:
    if _mmb_table_description(table_name) == comment:
        return
    spark.sql(f"COMMENT ON TABLE {_mmb_sql_identifier(table_name)} IS '{_mmb_escape_sql_string(comment)}'")

def _mmb_apply_column_comments_if_changed(table_name: str, schema: T.StructType) -> None:
    current = spark.table(table_name).schema
    current_comments = {field.name: field.metadata.get('comment', '') for field in current.fields}
    for field in schema.fields:
        desired = field.metadata.get('comment', '')
        if not desired or current_comments.get(field.name) == desired:
            continue
        spark.sql(f"ALTER TABLE {_mmb_sql_identifier(table_name)} ALTER COLUMN {_mmb_sql_column(field.name)} COMMENT '{_mmb_escape_sql_string(desired)}'")

def _mmb_target_requires_full_rebuild(config: MapMatBirthConfig) -> Tuple[bool, str]:
    if not _mmb_table_exists(config.target_table):
        return (True, 'target does not exist')
    current = [(field.name, field.dataType.simpleString()) for field in spark.table(config.target_table).schema.fields]
    desired = [
        (field.name, field.dataType.simpleString())
        for field in bronze_contract_schema(config.target_table).fields
    ]
    if current != desired:
        return (True, 'target schema differs from replacement contract')
    return (False, 'target schema is current')

def _mmb_ensure_audit_table(config: MapMatBirthConfig) -> None:
    spark.sql(f'\n        CREATE TABLE IF NOT EXISTS {_mmb_sql_identifier(config.audit_table)} (\n            run_id STRING,\n            event_timestamp TIMESTAMP,\n            status STRING,\n            mode STRING,\n            message STRING,\n            source_versions_json STRING,\n            metrics_json STRING\n        )\n        USING DELTA\n    ')

def _mmb_write_audit_event(config: MapMatBirthConfig, run_id: str, status: str, mode: str, message: str, source_versions: Optional[Dict[str, int]], metrics: Optional[Dict[str, object]]) -> None:
    _mmb_ensure_audit_table(config)
    row = [(run_id, datetime.now(timezone.utc), status, mode, message[:4000], json.dumps(source_versions or {}, sort_keys=True), json.dumps(metrics or {}, sort_keys=True, default=str))]
    schema = 'run_id string, event_timestamp timestamp, status string, mode string, message string, source_versions_json string, metrics_json string'
    spark.createDataFrame(row, schema).write.mode('append').saveAsTable(config.audit_table)

def _mmb_validate_snapshot_rows(df: DataFrame) -> Dict[str, int]:
    summary = df.agg(F.count(F.lit(1)).alias('source_rows'), F.sum(F.when(F.col('BirthRow_ID').isNull(), F.lit(1)).otherwise(F.lit(0))).alias('null_birth_row_ids')).first()
    duplicate_keys = df.groupBy('BirthRow_ID').count().filter(F.col('count') > 1).limit(1).count()
    results = {'source_rows': int(summary['source_rows']), 'null_birth_row_ids': int(summary['null_birth_row_ids'] or 0), 'duplicate_birth_row_ids': int(duplicate_keys)}
    if results['null_birth_row_ids']:
        raise AssertionError(f"BirthRow_ID is null for {results['null_birth_row_ids']} rows")
    if results['duplicate_birth_row_ids']:
        raise AssertionError('BirthRow_ID is not unique. No source rows were silently deduplicated; investigate the source-key contract.')
    return results

def _mmb_latest_operation_metrics(table_name: str) -> Dict[str, object]:
    row = spark.sql(f'DESCRIBE HISTORY {_mmb_sql_identifier(table_name)} LIMIT 1').select('operation', 'operationMetrics').first()
    if row is None:
        return {}
    operation_metrics = dict(row['operationMetrics']) if row['operationMetrics'] is not None else {}
    return {'delta_operation': row['operation'], 'delta_operation_metrics': operation_metrics}

def create_mat_birth_mapping_incr(force_full_rebuild: bool=False, run_id: Optional[str]=None, run_timestamp: Optional[datetime]=None, config: MapMatBirthConfig=MAP_MAT_BIRTH_CONFIG) -> DataFrame:
    """
    Build the complete source-aligned map_mat_birth snapshot without writing.

    The historical function name is retained for drop-in compatibility. The
    returned DataFrame is deliberately a full snapshot because a single target
    ADC watermark cannot safely represent three independently changing sources.
    """
    global _PENDING_MAP_MAT_BIRTH_PLAN
    run_id = run_id or str(uuid.uuid4())
    run_timestamp = run_timestamp or datetime.now(timezone.utc)
    source_versions = _mmb_capture_source_versions(config)
    automatic_full, reason = _mmb_target_requires_full_rebuild(config)
    full_rebuild = force_full_rebuild or automatic_full
    print(f"[INFO] map_mat_birth source snapshot; write mode={('FULL_REBUILD' if full_rebuild else 'FULL_SNAPSHOT_MERGE')}; reason={('explicit request' if force_full_rebuild else reason)}")
    print(f'[INFO] Captured source versions: {source_versions}')
    birth_snapshot = _mmb_read_snapshot(config.birth_source_table, source_versions[config.birth_source_table])
    pregnancy_snapshot = _mmb_read_snapshot(config.pregnancy_source_table, source_versions[config.pregnancy_source_table])
    nnu_snapshot = _mmb_read_snapshot(config.nnu_source_table, source_versions[config.nnu_source_table])
    result = _mmb_build_target_rows(birth_snapshot=birth_snapshot, pregnancy_snapshot=pregnancy_snapshot, nnu_snapshot=nnu_snapshot, source_versions=source_versions, run_id=run_id, run_timestamp=run_timestamp, config=config)
    _PENDING_MAP_MAT_BIRTH_PLAN = {'run_id': run_id, 'run_timestamp': run_timestamp, 'source_versions': source_versions, 'full_rebuild': full_rebuild, 'mode': 'FULL_REBUILD' if full_rebuild else 'FULL_SNAPSHOT_MERGE', 'config': config}
    return result

def create_mat_birth_mapping_full(**kwargs) -> DataFrame:
    """Explicitly named alias for create_mat_birth_mapping_incr."""
    return create_mat_birth_mapping_incr(**kwargs)

def update_map_mat_birth_table(source_df: DataFrame, config: MapMatBirthConfig=MAP_MAT_BIRTH_CONFIG) -> Dict[str, object]:
    """
    Synchronize the target from a complete source snapshot.

    Full rebuild is used automatically for the legacy schema. Later runs merge
    by BirthRow_ID, update only changed hashes, insert new rows, and delete rows
    no longer present in the captured MAT_BIRTH snapshot.
    """
    global _PENDING_MAP_MAT_BIRTH_PLAN
    plan = dict(_PENDING_MAP_MAT_BIRTH_PLAN)
    if not plan:
        raise RuntimeError('No pending map_mat_birth plan. Call create_mat_birth_mapping_incr() first or use run_map_mat_birth_update().')
    run_id = str(plan['run_id'])
    source_versions = dict(plan['source_versions'])
    full_rebuild = bool(plan['full_rebuild'])
    mode = str(plan['mode'])
    metrics: Dict[str, object] = {}
    source_df = bronze_project_contract(source_df, config.target_table)
    try:
        metrics.update(_mmb_validate_snapshot_rows(source_df))
        if full_rebuild:
            source_df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('delta.enableChangeDataFeed', 'true').option('delta.enableDeletionVectors', 'true').option('delta.enableRowTracking', 'true').option('delta.autoOptimize.optimizeWrite', 'true').option('delta.autoOptimize.autoCompact', 'true').saveAsTable(config.target_table)
        else:
            assignments = {
                column_name: F.col(f's.{column_name}')
                for column_name in source_df.columns
            }
            comparisons = ' OR '.join(
                f'NOT (t.`{column_name}` <=> s.`{column_name}`)'
                for column_name in source_df.columns
                if column_name != 'BirthRow_ID'
            )
            DeltaTable.forName(spark, config.target_table).alias('t').merge(
                source_df.alias('s'),
                't.BirthRow_ID = s.BirthRow_ID',
            ).whenMatchedUpdate(
                condition=comparisons or 'false',
                set=assignments,
            ).whenNotMatchedInsert(
                values=assignments
            ).whenNotMatchedBySourceDelete().execute()
        _mmb_set_table_properties_if_changed(config.target_table, _mmb_target_properties(config))
        _mmb_set_table_comment_if_changed(config.target_table, map_mat_birth_comment)
        _mmb_apply_column_comments_if_changed(
            config.target_table,
            bronze_contract_schema(config.target_table),
        )
        metrics.update(_mmb_latest_operation_metrics(config.target_table))
        metrics['target_rows'] = spark.table(config.target_table).count()
        metrics['full_rebuild'] = full_rebuild
        metrics['source_versions'] = source_versions
        if metrics['target_rows'] != metrics['source_rows']:
            raise AssertionError(f"Target/source row counts differ after synchronization: {metrics['target_rows']} != {metrics['source_rows']}")
        _mmb_write_audit_event(config=config, run_id=run_id, status='SUCCESS', mode=mode, message='map_mat_birth update completed', source_versions=source_versions, metrics=metrics)
        print(f"[SUCCESS] map_mat_birth completed: {metrics['target_rows']:,} target rows; full_rebuild={full_rebuild}")
        return metrics
    except Exception as exc:
        try:
            _mmb_write_audit_event(config=config, run_id=run_id, status='FAILED', mode=mode, message=str(exc), source_versions=source_versions, metrics=metrics)
        except Exception:
            pass
        raise
    finally:
        None
        _PENDING_MAP_MAT_BIRTH_PLAN = {}

def run_map_mat_birth_update(force_full_rebuild: bool=False, run_post_checks: bool=False, config: MapMatBirthConfig=MAP_MAT_BIRTH_CONFIG) -> Dict[str, object]:
    """Production entry point replacing the original map_mat_birth cells."""
    run_id = str(uuid.uuid4())
    run_timestamp = datetime.now(timezone.utc)
    rows = create_mat_birth_mapping_incr(force_full_rebuild=force_full_rebuild, run_id=run_id, run_timestamp=run_timestamp, config=config)
    metrics = update_map_mat_birth_table(rows, config=config)
    if run_post_checks:
        metrics['post_deployment_checks'] = run_map_mat_birth_post_deployment_checks(config)
    return metrics

def run_map_mat_birth_post_deployment_checks(config: MapMatBirthConfig=MAP_MAT_BIRTH_CONFIG) -> Dict[str, int]:
    """Full retained-contract checks for the single-layer birth target."""
    if not _mmb_table_exists(config.target_table):
        raise AssertionError(f'Target does not exist: {config.target_table}')
    target = spark.table(config.target_table)
    target_rows = int(target.count())
    target_duplicate_keys = int(
        target.groupBy('BirthRow_ID').count()
        .filter(F.col('count') > 1).count()
    )
    null_keys = int(target.filter(F.col('BirthRow_ID').isNull()).count())
    quality = target.agg(
        F.sum(
            F.when(F.col('BirthDateTimeFuture_IND'), 1).otherwise(0)
        ).alias('future_birth_datetime_rows'),
        F.sum(
            F.when(F.col('BirthWeightImplausible_IND'), 1).otherwise(0)
        ).alias('implausible_birth_weight_rows'),
        F.sum(
            F.when(F.col('GestationWeeks').isNull(), 1).otherwise(0)
        ).alias('missing_gestation_weeks'),
    ).first()
    results = {
        'target_rows': target_rows,
        'target_duplicate_birth_row_ids': target_duplicate_keys,
        'null_birth_row_ids': null_keys,
        'future_birth_datetime_rows': int(quality['future_birth_datetime_rows'] or 0),
        'implausible_birth_weight_rows': int(
            quality['implausible_birth_weight_rows'] or 0
        ),
        'missing_gestation_weeks': int(quality['missing_gestation_weeks'] or 0),
    }
    failures = {
        key: value for key, value in results.items()
        if key in {'target_duplicate_birth_row_ids', 'null_birth_row_ids'}
        and value != 0
    }
    target_types = {
        field.name: field.dataType.simpleString()
        for field in target.schema.fields
    }
    if target_types.get('BirthDateTime') != 'timestamp':
        failures['BirthDateTime_not_timestamp'] = 1
    if target_types.get('BirthRow_ID') != 'string':
        failures['BirthRow_ID_not_string'] = 1
    if target_types.get('PregnancyChildSeq_ID') != 'bigint':
        failures['PregnancyChildSeq_ID_not_bigint'] = 1
    if failures:
        raise AssertionError(f'map_mat_birth post-deployment checks failed: {failures}')
    print(f'[OK] map_mat_birth post-deployment checks passed: {results}')
    return results
try:
    _targets = ['4_prod.bronze.map_mat_birth']
    if not _pipeline_resume_skip_component('map_mat_birth', _targets):
        run_map_mat_birth_update(force_full_rebuild=_PIPELINE_FULL_REFRESH, run_post_checks=_PIPELINE_FULL_REFRESH)
        _PIPELINE_UPDATED_TARGETS.extend(_targets)
        _pipeline_mark_component_complete('map_mat_birth', _targets)
        _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_mat_birth'})
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

_pipeline_component_start("map_mat_vte_assessment")
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
from typing import Dict, List, Optional, Sequence, Tuple
import json
import uuid
from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

@dataclass(frozen=True)
class MapMatVTEConfig:
    response_source_table: str = '4_prod.raw.pi_cde_doc_response'
    reference_source_table: str = '3_lookup.dwh.pi_lkp_cde_doc_ref'
    pregnancy_table: str = '4_prod.bronze.map_mat_pregnancy'
    birth_table: str = '4_prod.bronze.map_mat_birth'
    target_table: str = '4_prod.bronze.map_mat_vte_assessment'
    audit_table: str = '6_mgmt.bronze.map_mat_vte_assessment_pipeline_audit'
    schema_version: str = '2.0.0'
    obstetric_section_ref_ids: Tuple[str, ...] = ('1497029943', '181045707', '7827242')
    complete_vte_form_ref_ids: Tuple[str, ...] = ('232991959',)
    include_complete_vte_form_sections: bool = False
    auto_full_rebuild_on_cdf_gap: bool = True
MAP_MAT_VTE_CONFIG = MapMatVTEConfig()
map_mat_vte_comment = 'Curated maternity/obstetric VTE assessment foundation at one row per DOC_RESPONSE_KEY. Scope is deliberately restricted to the curated obstetric VTE assessment sections (SECTION_REF_ID in obstetric_section_ref_ids); responses captured only under the general adult VTE assessment form (FORM_REF_ID 232991959) are NOT included, so this table is a curated obstetric subset and not a lossless copy of the source. The raw layer remains the retention layer for every excluded response. Within that scope the table retains raw identifiers, typed response values, lifecycle fields, form/section/element metadata, organisation/trust and source timestamps without active-state or element-label filtering. Pregnancy_ID is an audited deterministic enrichment and is not part of row identity.'

def _mmvte_field(name: str, data_type: T.DataType, comment: str, nullable: bool=True) -> T.StructField:
    return T.StructField(name, data_type, nullable, metadata={'comment': comment})
schema_map_mat_vte = T.StructType([_mmvte_field('Pregnancy_ID', T.LongType(), 'Deterministically selected pregnancy enrichment; not part of target row identity.'), _mmvte_field('PERSON_ID', T.LongType(), 'Person identifier parsed from the raw document response.'), _mmvte_field('Event_ID', T.LongType(), 'Compatibility alias parsed from source SECTION_EVENT_ID.'), _mmvte_field('ENCNTR_ID', T.LongType(), 'Encounter identifier parsed from the raw document response.'), _mmvte_field('FormDate', T.TimestampType(), 'Best available form timestamp; source and fallback provenance are retained in FormDateSource.'), _mmvte_field('Section', T.StringType(), 'Current reference section description.'), _mmvte_field('Element', T.StringType(), 'Current reference element label.'), _mmvte_field('Response', T.StringType(), 'Convenience display response from nonblank text, string, numeric or response datetime; raw typed values are retained.'), _mmvte_field('PERFORMED_PRSNL_ID', T.StringType(), 'Provider identifier associated with the source response.'), _mmvte_field('ADC_UPDT', T.TimestampType(), 'Greatest response/reference ADC timestamp; not used as a pipeline checkpoint.'), _mmvte_field('DOC_RESPONSE_KEY', T.StringType(), 'Stable primary key from PI_CDE_DOC_RESPONSE.'), _mmvte_field('DOC_INPUT_ID', T.StringType(), 'Source document-input identifier used to join the reference metadata.'), _mmvte_field('ELEMENT_EVENT_ID', T.StringType(), 'Source element-event identifier.'), _mmvte_field('FORM_EVENT_ID', T.StringType(), 'Source form-event identifier; normally the assessment instance.'), _mmvte_field('SECTION_EVENT_ID', T.StringType(), 'Source section-event identifier.'), _mmvte_field('GRID_EVENT_ID', T.StringType(), 'Source grid-event identifier.'), _mmvte_field('ROW_EVENT_ID', T.StringType(), 'Source grid-row event identifier.'), _mmvte_field('ACTIVE_IND', T.IntegerType(), 'Unchanged response ACTIVE_IND; no row filtering is applied.'), _mmvte_field('FORM_STATUS_CD', T.StringType(), 'Source form-status code.'), _mmvte_field('DOCUMENTATION_DT_TM', T.TimestampType(), 'Source documentation timestamp.'), _mmvte_field('FIRST_DOCUMENTED_DT_TM', T.TimestampType(), 'Source first-documented timestamp.'), _mmvte_field('LAST_DOCUMENTED_DT_TM', T.TimestampType(), 'Source last-documented timestamp.'), _mmvte_field('PERFORMED_DT_TM', T.TimestampType(), 'Unchanged source performed timestamp.'), _mmvte_field('Source_EXTRACT_DT_TM', T.TimestampType(), 'Response-source extraction timestamp.'), _mmvte_field('Source_Ctrl_ID', T.IntegerType(), 'Response-source control identifier.'), _mmvte_field('Source_Record_Updated_Dt', T.TimestampType(), 'Response-source record-updated timestamp.'), _mmvte_field('Source_ADC_UPDT', T.TimestampType(), 'Response-source ADC update timestamp.'), _mmvte_field('ORGANIZATION_ID', T.LongType(), 'Source organisation identifier.'), _mmvte_field('Trust', T.StringType(), 'Unchanged source Trust value; null is retained and not filtered.'), _mmvte_field('RESPONSE_VALUE_TXT', T.StringType(), 'Unchanged source response-value text.'), _mmvte_field('STRING_RESPONSE_TXT', T.StringType(), 'Unchanged typed string response.'), _mmvte_field('NUMERIC_RESPONSE_NBR', T.DoubleType(), 'Unchanged typed numeric response.'), _mmvte_field('RESPONSE_DT_TM', T.TimestampType(), 'Unchanged typed datetime response.'), _mmvte_field('RESPONSE_NOMEN_ID', T.StringType(), 'Unchanged response nomenclature identifier; zero sentinels are retained.'), _mmvte_field('RESPONSE_CODE_VALUE_CD', T.StringType(), 'Unchanged coded response value; zero sentinels are retained.'), _mmvte_field('ResponseSource', T.StringType(), 'Source field supplying the compatibility Response value.'), _mmvte_field('FormDateSource', T.StringType(), 'Source field supplying FormDate.'), _mmvte_field('FORM_REF_ID', T.StringType(), 'Stable form reference identifier.'), _mmvte_field('FORM_DESC_TXT', T.StringType(), 'Reference form description.'), _mmvte_field('FORM_DEFINITION_TXT', T.StringType(), 'Reference form definition text.'), _mmvte_field('SECTION_REF_ID', T.StringType(), 'Stable section reference identifier.'), _mmvte_field('SECTION_DESC_TXT', T.StringType(), 'Reference section description retained separately from compatibility Section.'), _mmvte_field('SECTION_DEF_TXT', T.StringType(), 'Reference section definition text.'), _mmvte_field('TASK_ASSAY_ID', T.StringType(), 'Reference task/assay identifier.'), _mmvte_field('ELEMENT_DESC_TXT', T.StringType(), 'Reference element description.'), _mmvte_field('ELEMENT_MNEMONIC_TXT', T.StringType(), 'Stable reference element mnemonic where populated.'), _mmvte_field('ELEMENT_LABEL_TXT', T.StringType(), 'Reference element label retained separately from compatibility Element.'), _mmvte_field('GRID_NAME_TXT', T.StringType(), 'Reference grid name.'), _mmvte_field('GRID_COLUMN_TASK_ASSAY_ID', T.StringType(), 'Reference grid-column task/assay identifier.'), _mmvte_field('GRID_COLUMN_DESC_TXT', T.StringType(), 'Reference grid-column description.'), _mmvte_field('GRID_COLUMN_MNEMONIC_TXT', T.StringType(), 'Reference grid-column mnemonic.'), _mmvte_field('GRID_ROW_TASK_ASSAY_ID', T.StringType(), 'Reference grid-row task/assay identifier.'), _mmvte_field('GRID_ROW_DESC_TXT', T.StringType(), 'Reference grid-row description.'), _mmvte_field('GRID_ROW_MNEMONIC_TXT', T.StringType(), 'Reference grid-row mnemonic.'), _mmvte_field('FORM_INSTANCE_ID', T.StringType(), 'Reference form-instance identifier.'), _mmvte_field('GRID_EVENT_CD', T.StringType(), 'Reference grid-event code.'), _mmvte_field('INPUT_TYPE_FLG', T.IntegerType(), 'Reference input-type flag.'), _mmvte_field('Reference_ACTIVE_IND', T.IntegerType(), 'ACTIVE_IND of the deterministically selected reference row.'), _mmvte_field('Reference_EXTRACT_DT_TM', T.TimestampType(), 'Reference extraction timestamp.'), _mmvte_field('Reference_Ctrl_ID', T.IntegerType(), 'Reference control identifier.'), _mmvte_field('Reference_Record_Updated_Dt', T.TimestampType(), 'Reference record-updated timestamp.'), _mmvte_field('Reference_ADC_UPDT', T.TimestampType(), 'Reference ADC update timestamp.'), _mmvte_field('ReferenceKeyRowCount', T.LongType(), 'Number of current reference rows sharing DOC_INPUT_KEY before deterministic selection.'), _mmvte_field('PregnancyMatched_IND', T.BooleanType(), 'True when one or more valid pregnancy intervals matched FormDate.'), _mmvte_field('PregnancyMatchMethod', T.StringType(), 'Method used to construct the selected pregnancy interval.'), _mmvte_field('PregnancyMatchCandidateCount', T.LongType(), 'Number of pregnancy intervals matching the response.'), _mmvte_field('PregnancyMatchAmbiguous_IND', T.BooleanType(), 'True when more than one pregnancy interval matched before ranking.'), _mmvte_field('PregnancyMatchStartDate', T.DateType(), 'Calculated start date of the selected pregnancy interval.'), _mmvte_field('PregnancyMatchEndDate', T.DateType(), 'Calculated end date including six weeks postpartum.'), _mmvte_field('PregnancyReferenceDate', T.DateType(), 'Selected birth/EDD/calculated pregnancy-end reference date used for ranking.'), _mmvte_field('PregnancyMatchDistanceDays', T.IntegerType(), 'Absolute days between FormDate and the selected pregnancy reference date.'), _mmvte_field('ResponseMissing_IND', T.BooleanType(), 'True when no nonblank text/string, numeric or response datetime is available.'), _mmvte_field('FormDateFallback_IND', T.BooleanType(), 'True when FormDate did not come from PERFORMED_DT_TM.'), _mmvte_field('TrustMissing_IND', T.BooleanType(), 'True when the unchanged source Trust value is null or blank.'), _mmvte_field('IdentifierCastFailure_IND', T.BooleanType(), 'True when a populated person, encounter or section-event source identifier cannot be parsed as BIGINT.'), _mmvte_field('TRIGGER_SOURCES', T.StringType(), 'Comma-separated sources that caused this row to be rebuilt.'), _mmvte_field('ROW_HASH', T.StringType(), 'SHA-256 hash of source-derived and linkage columns used to suppress unchanged updates.'), _mmvte_field('PIPELINE_RUN_ID', T.StringType(), 'Unique identifier of the pipeline run that last changed the row.'), _mmvte_field('PIPELINE_UPDT_DT_TM', T.TimestampType(), 'UTC timestamp when the pipeline last changed the row.'), _mmvte_field('SCHEMA_VERSION', T.StringType(), 'Version of this replacement schema and logic.')])
_MMVTE_PIPELINE_COLUMNS = {'TRIGGER_SOURCES', 'ROW_HASH', 'PIPELINE_RUN_ID', 'PIPELINE_UPDT_DT_TM', 'SCHEMA_VERSION'}

class MapMatVTECdfUnavailable(RuntimeError):
    pass

def _mmvte_sql_identifier(name: str) -> str:
    tick = chr(96)
    return '.'.join((f'{tick}{part.replace(tick, tick + tick)}{tick}' for part in name.split('.')))

def _mmvte_escape_sql_string(value: str) -> str:
    return (value or '').replace('\\', '\\\\').replace("'", "''")

def _mmvte_table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _mmvte_latest_delta_version(table_name: str) -> int:
    row = spark.sql(f'DESCRIBE HISTORY {_mmvte_sql_identifier(table_name)} LIMIT 1').select('version').first()
    if row is None:
        raise RuntimeError(f'No Delta history returned for {table_name}')
    return int(row['version'])

def _mmvte_capture_source_versions(config: MapMatVTEConfig) -> Dict[str, int]:
    return {config.response_source_table: _mmvte_latest_delta_version(config.response_source_table), config.reference_source_table: _mmvte_latest_delta_version(config.reference_source_table), config.pregnancy_table: _mmvte_latest_delta_version(config.pregnancy_table), config.birth_table: _mmvte_latest_delta_version(config.birth_table)}

def _mmvte_read_snapshot(table_name: str, version: int) -> DataFrame:
    return _m40_hardening_read_pinned_snapshot(table_name, int(version))

def _mmvte_read_cdf(table_name: str, previous_version: int, current_version: int) -> Optional[DataFrame]:
    if current_version <= previous_version:
        return None
    try:
        cdf = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', int(previous_version) + 1).option('endingVersion', int(current_version)).table(table_name)
        cdf.select('_commit_version').limit(1).collect()
        return cdf
    except Exception as exc:
        raise MapMatVTECdfUnavailable(f'CDF unavailable for {table_name} versions {previous_version + 1}..{current_version}: {exc}') from exc

def _mmvte_nonblank(column: Column) -> Column:
    return F.when(column.isNotNull() & (F.length(F.trim(column.cast('string'))) > 0), column)

def _mmvte_try_long(column_name: str) -> Column:
    escaped = column_name.replace('`', '``')
    return F.expr(f'try_cast(`{escaped}` as bigint)')

def _mmvte_greatest_timestamp(*columns: Column) -> Column:
    return F.greatest(*[column.cast('timestamp') for column in columns])

def _mmvte_align_to_schema(df: DataFrame, schema: T.StructType) -> DataFrame:
    expressions: List[Column] = []
    for field in schema.fields:
        if field.name in df.columns:
            expressions.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            expressions.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(*expressions)

_MMVTE_PERSIST_STAGES: Dict[int, Tuple[DataFrame, str]] = {}

def _mmvte_persist_stage(frame: Optional[DataFrame]) -> Optional[DataFrame]:
    """Materialize a frame that is read more than once.

    Serverless has no cache/persist, so an unmaterialized frame is recomputed in full at every
    reference. The VTE build counts each change-key set before joining it and counts, preflights
    and merges the finished snapshot, and the family-history build aggregates its joined snapshot
    before writing it. Round-tripping through a run-scoped Delta stage buys the same reuse a cache
    would.

    The stage name starts with map_ and carries __changes_stage_ so the _map_common
    startup sweep collects anything a crashed run leaves behind. The staged frame is the dict key,
    which also keeps it alive so its id() cannot be recycled onto a different object.
    """
    if frame is None:
        return frame
    if id(frame) in _MMVTE_PERSIST_STAGES:
        return frame
    columns = frame.columns
    if len(set(columns)) != len(columns):
        print('[WARN] persist stage skipped: frame has duplicate column names and cannot be staged')
        return frame
    prefix = MAP_MAT_VTE_CONFIG.target_table.rsplit('.', 1)[0]
    stage_table = f'{prefix}.map_persist__changes_stage_{uuid.uuid4().hex}'
    try:
        frame.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(stage_table)
    except Exception as stage_exc:
        print(f'[WARN] persist stage write failed for {stage_table}; continuing unmaterialized: {str(stage_exc)[:500]}')
        try:
            spark.sql(f'DROP TABLE IF EXISTS {_mmvte_sql_identifier(stage_table)}')
        except Exception:
            pass
        return frame
    staged = spark.table(stage_table)
    _MMVTE_PERSIST_STAGES[id(staged)] = (staged, stage_table)
    return staged

def _mmvte_persist_stage_drop(frame: Optional[DataFrame]) -> None:
    if frame is None:
        return
    entry = _MMVTE_PERSIST_STAGES.pop(id(frame), None)
    if entry is None:
        return
    try:
        spark.sql(f'DROP TABLE IF EXISTS {_mmvte_sql_identifier(entry[1])}')
    except Exception as drop_exc:
        print(f'[WARN] persist stage drop failed for {entry[1]}; the next pipeline startup sweep will retry: {str(drop_exc)[:500]}')

def _mmvte_maybe_persist(df: DataFrame) -> DataFrame:
    return _mmvte_persist_stage(df)

def _mmvte_maybe_unpersist(df: Optional[DataFrame]) -> None:
    _mmvte_persist_stage_drop(df)

def _mmvte_get_table_comment(table_name: str) -> str:
    try:
        table = spark.catalog.getTable(table_name)
        return (getattr(table, 'description', None) or '').strip()
    except Exception:
        rows = spark.sql(f'DESCRIBE TABLE EXTENDED {_mmvte_sql_identifier(table_name)}').collect()
        for row in rows:
            if str(row['col_name'] or '').strip().lower() == 'comment':
                return str(row['data_type'] or '').strip()
        return ''

def _mmvte_set_table_comment_if_changed(table_name: str, comment: str) -> bool:
    if _mmvte_get_table_comment(table_name) == (comment or '').strip():
        return False
    spark.sql(f"COMMENT ON TABLE {_mmvte_sql_identifier(table_name)} IS '{_mmvte_escape_sql_string(comment)}'")
    return True

def _mmvte_current_properties(table_name: str) -> Dict[str, str]:
    return {str(row['key']): str(row['value']) for row in spark.sql(f'SHOW TBLPROPERTIES {_mmvte_sql_identifier(table_name)}').collect()}

def _mmvte_set_properties_if_needed(table_name: str, desired: Dict[str, str]) -> List[str]:
    current = _mmvte_current_properties(table_name)
    changes = {key: value for key, value in desired.items() if current.get(key) != value}
    if not changes:
        return []
    assignments = ', '.join((f"'{_mmvte_escape_sql_string(key)}' = '{_mmvte_escape_sql_string(value)}'" for key, value in changes.items()))
    spark.sql(f'ALTER TABLE {_mmvte_sql_identifier(table_name)} SET TBLPROPERTIES ({assignments})')
    return list(changes)

def _mmvte_column_definition(field: T.StructField) -> str:
    tick = chr(96)
    name = field.name.replace(tick, tick + tick)
    definition = f'{tick}{name}{tick} {field.dataType.simpleString()}'
    comment = field.metadata.get('comment', '')
    if comment:
        definition += f" COMMENT '{_mmvte_escape_sql_string(comment)}'"
    return definition

def _mmvte_apply_column_comments(table_name: str, schema: T.StructType) -> List[str]:
    current = {field.name: field for field in spark.table(table_name).schema.fields}
    changed: List[str] = []
    for field in schema.fields:
        if field.name not in current:
            continue
        wanted = field.metadata.get('comment', '')
        present = current[field.name].metadata.get('comment', '')
        if wanted != present:
            spark.sql(f"ALTER TABLE {_mmvte_sql_identifier(table_name)} ALTER COLUMN {_mmvte_sql_identifier(field.name)} COMMENT '{_mmvte_escape_sql_string(wanted)}'")
            changed.append(field.name)
    return changed

def _mmvte_ensure_target_table(config: MapMatVTEConfig) -> Dict[str, object]:
    desired_properties = {'delta.enableChangeDataFeed': 'true', 'delta.enableRowTracking': 'true', 'delta.enableTypeWidening': 'true', 'delta.parquet.compression.codec': 'zstd'}
    if not _mmvte_table_exists(config.target_table):
        target_schema = bronze_contract_schema(config.target_table)
        builder = DeltaTable.createIfNotExists(spark).tableName(config.target_table).addColumns(target_schema).comment(map_mat_vte_comment)
        for key, value in desired_properties.items():
            builder = builder.property(key, value)
        builder.execute()
        return {
            'created': True,
            'columns_added': [field.name for field in target_schema.fields],
            'properties_changed': list(desired_properties),
            'comments_changed': _mmvte_apply_column_comments(
                config.target_table, target_schema
            ),
            'table_comment_changed': _mmvte_set_table_comment_if_changed(
                config.target_table, map_mat_vte_comment
            ),
        }
    target_schema = bronze_contract_schema(config.target_table)
    current_schema = spark.table(config.target_table).schema
    current = {field.name: field for field in current_schema.fields}
    missing = [
        field for field in target_schema.fields
        if field.name not in current
    ]
    incompatible = [
        (
            field.name,
            current[field.name].dataType.simpleString(),
            field.dataType.simpleString(),
        )
        for field in target_schema.fields
        if field.name in current and current[field.name].dataType != field.dataType
    ]
    if incompatible:
        raise RuntimeError(f'Target contains incompatible column types and requires force_full_rebuild=True: {incompatible}')
    for field in missing:
        spark.sql(f'ALTER TABLE {_mmvte_sql_identifier(config.target_table)} ADD COLUMN {_mmvte_column_definition(field)}')
    return {
        'created': False,
        'columns_added': [field.name for field in missing],
        'properties_changed': _mmvte_set_properties_if_needed(
            config.target_table, desired_properties
        ),
        'comments_changed': _mmvte_apply_column_comments(
            config.target_table, target_schema
        ),
        'table_comment_changed': _mmvte_set_table_comment_if_changed(
            config.target_table, map_mat_vte_comment
        ),
    }

def _mmvte_ensure_audit_table(config: MapMatVTEConfig) -> None:
    spark.sql(f"\n        CREATE TABLE IF NOT EXISTS {_mmvte_sql_identifier(config.audit_table)} (\n            run_id STRING,\n            event_timestamp TIMESTAMP,\n            status STRING,\n            mode STRING,\n            schema_version STRING,\n            message STRING,\n            source_versions_json STRING,\n            metrics_json STRING\n        )\n        USING DELTA\n        TBLPROPERTIES (\n            'delta.enableChangeDataFeed' = 'true',\n            'delta.parquet.compression.codec' = 'zstd'\n        )\n    ")

def _mmvte_write_audit_event(config: MapMatVTEConfig, run_id: str, status: str, mode: str, message: str, source_versions: Optional[Dict[str, int]]=None, metrics: Optional[Dict[str, object]]=None) -> None:
    row = [(run_id, datetime.now(timezone.utc), status, mode, config.schema_version, (message or '')[:8000], json.dumps(source_versions or {}, sort_keys=True, default=str), json.dumps(metrics or {}, sort_keys=True, default=str))]
    schema = T.StructType([T.StructField('run_id', T.StringType(), False), T.StructField('event_timestamp', T.TimestampType(), False), T.StructField('status', T.StringType(), False), T.StructField('mode', T.StringType(), False), T.StructField('schema_version', T.StringType(), False), T.StructField('message', T.StringType(), True), T.StructField('source_versions_json', T.StringType(), True), T.StructField('metrics_json', T.StringType(), True)])
    spark.createDataFrame(row, schema).write.mode('append').saveAsTable(config.audit_table)

def _mmvte_last_success_state(config: MapMatVTEConfig) -> Optional[Dict[str, object]]:
    if not _mmvte_table_exists(config.audit_table):
        return None
    row = spark.table(config.audit_table).filter(F.col('status') == 'SUCCESS').orderBy(F.col('event_timestamp').desc()).select('schema_version', 'source_versions_json', 'mode', 'run_id').first()
    if row is None:
        return None
    try:
        versions = {str(key): int(value) for key, value in json.loads(row['source_versions_json'] or '{}').items()}
    except Exception as exc:
        raise RuntimeError(f'Could not parse latest successful VTE checkpoint: {exc}') from exc
    return {'schema_version': row['schema_version'], 'source_versions': versions, 'mode': row['mode'], 'run_id': row['run_id']}

def _mmvte_latest_operation_metrics(table_name: str) -> Dict[str, object]:
    if not _mmvte_table_exists(table_name):
        return {}
    row = spark.sql(f'DESCRIBE HISTORY {_mmvte_sql_identifier(table_name)} LIMIT 1').select('operation', 'operationMetrics', 'version', 'timestamp').first()
    if row is None:
        return {}
    metrics = dict(row['operationMetrics'] or {})
    metrics.update({'operation': row['operation'], 'version': int(row['version']), 'timestamp': str(row['timestamp'])})
    return metrics

def _mmvte_reference_scope_condition(config: MapMatVTEConfig) -> Column:
    section_condition = F.col('SECTION_REF_ID').cast('string').isin(list(config.obstetric_section_ref_ids))
    if not config.include_complete_vte_form_sections:
        return section_condition
    form_condition = F.col('FORM_REF_ID').cast('string').isin(list(config.complete_vte_form_ref_ids))
    return section_condition | form_condition

def _mmvte_build_reference_snapshot(reference_version: int, config: MapMatVTEConfig) -> DataFrame:
    reference = _mmvte_read_snapshot(config.reference_source_table, reference_version)
    rank_window = Window.partitionBy('DOC_INPUT_KEY').orderBy(F.when(F.col('ACTIVE_IND') == 1, F.lit(1)).otherwise(F.lit(0)).desc(), F.col('ADC_UPDT').desc_nulls_last(), F.col('Record_Updated_Dt').desc_nulls_last(), F.col('EXTRACT_DT_TM').desc_nulls_last())
    count_window = Window.partitionBy('DOC_INPUT_KEY')
    return reference.withColumn('ReferenceKeyRowCount', F.count(F.lit(1)).over(count_window)).withColumn('_reference_rank', F.row_number().over(rank_window)).filter(F.col('_reference_rank') == 1).drop('_reference_rank').filter(_mmvte_reference_scope_condition(config))

def _mmvte_response_and_form_date_expressions() -> Dict[str, Column]:
    response_value = _mmvte_nonblank(F.col('d.RESPONSE_VALUE_TXT'))
    string_value = _mmvte_nonblank(F.col('d.STRING_RESPONSE_TXT'))
    numeric_value = F.when(F.col('d.NUMERIC_RESPONSE_NBR').isNotNull(), F.col('d.NUMERIC_RESPONSE_NBR').cast('string'))
    datetime_value = F.when(F.col('d.RESPONSE_DT_TM').isNotNull(), F.date_format(F.col('d.RESPONSE_DT_TM'), 'yyyy-MM-dd HH:mm:ss'))
    response = F.coalesce(response_value, string_value, numeric_value, datetime_value)
    response_source = F.when(response_value.isNotNull(), F.lit('RESPONSE_VALUE_TXT')).when(string_value.isNotNull(), F.lit('STRING_RESPONSE_TXT')).when(numeric_value.isNotNull(), F.lit('NUMERIC_RESPONSE_NBR')).when(datetime_value.isNotNull(), F.lit('RESPONSE_DT_TM'))
    form_date = F.coalesce(F.col('d.PERFORMED_DT_TM'), F.col('d.LAST_DOCUMENTED_DT_TM'), F.col('d.DOCUMENTATION_DT_TM'), F.col('d.FIRST_DOCUMENTED_DT_TM'), F.col('d.RESPONSE_DT_TM'))
    form_date_source = F.when(F.col('d.PERFORMED_DT_TM').isNotNull(), F.lit('PERFORMED_DT_TM')).when(F.col('d.LAST_DOCUMENTED_DT_TM').isNotNull(), F.lit('LAST_DOCUMENTED_DT_TM')).when(F.col('d.DOCUMENTATION_DT_TM').isNotNull(), F.lit('DOCUMENTATION_DT_TM')).when(F.col('d.FIRST_DOCUMENTED_DT_TM').isNotNull(), F.lit('FIRST_DOCUMENTED_DT_TM')).when(F.col('d.RESPONSE_DT_TM').isNotNull(), F.lit('RESPONSE_DT_TM'))
    return {'response': response, 'response_source': response_source, 'form_date': form_date, 'form_date_source': form_date_source}

def _mmvte_project_response_reference(responses: DataFrame, reference: DataFrame) -> DataFrame:
    expressions = _mmvte_response_and_form_date_expressions()
    joined = responses.alias('d').join(F.broadcast(reference.alias('r')), F.col('d.DOC_INPUT_ID') == F.col('r.DOC_INPUT_KEY'), 'inner')
    person_id = F.expr('try_cast(d.PERSON_ID as bigint)')
    encounter_id = F.expr('try_cast(d.ENCNTR_ID as bigint)')
    event_id = F.expr('try_cast(d.SECTION_EVENT_ID as bigint)')
    identifier_cast_failure = F.col('d.PERSON_ID').isNotNull() & person_id.isNull() | F.col('d.ENCNTR_ID').isNotNull() & encounter_id.isNull() | F.col('d.SECTION_EVENT_ID').isNotNull() & event_id.isNull()
    return joined.select(person_id.alias('PERSON_ID'), event_id.alias('Event_ID'), encounter_id.alias('ENCNTR_ID'), expressions['form_date'].cast('timestamp').alias('FormDate'), F.col('r.SECTION_DESC_TXT').cast('string').alias('Section'), F.col('r.ELEMENT_LABEL_TXT').cast('string').alias('Element'), expressions['response'].cast('string').alias('Response'), F.col('d.PERFORMED_PRSNL_ID').cast('string').alias('PERFORMED_PRSNL_ID'), _mmvte_greatest_timestamp(F.col('d.ADC_UPDT'), F.col('r.ADC_UPDT')).alias('ADC_UPDT'), F.col('d.DOC_RESPONSE_KEY').cast('string').alias('DOC_RESPONSE_KEY'), F.col('d.DOC_INPUT_ID').cast('string').alias('DOC_INPUT_ID'), F.col('d.ELEMENT_EVENT_ID').cast('string').alias('ELEMENT_EVENT_ID'), F.col('d.FORM_EVENT_ID').cast('string').alias('FORM_EVENT_ID'), F.col('d.SECTION_EVENT_ID').cast('string').alias('SECTION_EVENT_ID'), F.col('d.GRID_EVENT_ID').cast('string').alias('GRID_EVENT_ID'), F.col('d.ROW_EVENT_ID').cast('string').alias('ROW_EVENT_ID'), F.col('d.ACTIVE_IND').cast('int').alias('ACTIVE_IND'), F.col('d.FORM_STATUS_CD').cast('string').alias('FORM_STATUS_CD'), F.col('d.DOCUMENTATION_DT_TM').cast('timestamp').alias('DOCUMENTATION_DT_TM'), F.col('d.FIRST_DOCUMENTED_DT_TM').cast('timestamp').alias('FIRST_DOCUMENTED_DT_TM'), F.col('d.LAST_DOCUMENTED_DT_TM').cast('timestamp').alias('LAST_DOCUMENTED_DT_TM'), F.col('d.PERFORMED_DT_TM').cast('timestamp').alias('PERFORMED_DT_TM'), F.col('d.EXTRACT_DT_TM').cast('timestamp').alias('Source_EXTRACT_DT_TM'), F.col('d.Ctrl_Id').cast('int').alias('Source_Ctrl_ID'), F.col('d.Record_Updated_Dt').cast('timestamp').alias('Source_Record_Updated_Dt'), F.col('d.ADC_UPDT').cast('timestamp').alias('Source_ADC_UPDT'), F.col('d.ORGANIZATION_ID').cast('long').alias('ORGANIZATION_ID'), F.col('d.Trust').cast('string').alias('Trust'), F.col('d.RESPONSE_VALUE_TXT').cast('string').alias('RESPONSE_VALUE_TXT'), F.col('d.STRING_RESPONSE_TXT').cast('string').alias('STRING_RESPONSE_TXT'), F.col('d.NUMERIC_RESPONSE_NBR').cast('double').alias('NUMERIC_RESPONSE_NBR'), F.col('d.RESPONSE_DT_TM').cast('timestamp').alias('RESPONSE_DT_TM'), F.col('d.RESPONSE_NOMEN_ID').cast('string').alias('RESPONSE_NOMEN_ID'), F.col('d.RESPONSE_CODE_VALUE_CD').cast('string').alias('RESPONSE_CODE_VALUE_CD'), expressions['response_source'].alias('ResponseSource'), expressions['form_date_source'].alias('FormDateSource'), F.col('r.FORM_REF_ID').cast('string').alias('FORM_REF_ID'), F.col('r.FORM_DESC_TXT').cast('string').alias('FORM_DESC_TXT'), F.col('r.FORM_DEFINITION_TXT').cast('string').alias('FORM_DEFINITION_TXT'), F.col('r.SECTION_REF_ID').cast('string').alias('SECTION_REF_ID'), F.col('r.SECTION_DESC_TXT').cast('string').alias('SECTION_DESC_TXT'), F.col('r.SECTION_DEF_TXT').cast('string').alias('SECTION_DEF_TXT'), F.col('r.TASK_ASSAY_ID').cast('string').alias('TASK_ASSAY_ID'), F.col('r.ELEMENT_DESC_TXT').cast('string').alias('ELEMENT_DESC_TXT'), F.col('r.ELEMENT_MNEMONIC_TXT').cast('string').alias('ELEMENT_MNEMONIC_TXT'), F.col('r.ELEMENT_LABEL_TXT').cast('string').alias('ELEMENT_LABEL_TXT'), F.col('r.GRID_NAME_TXT').cast('string').alias('GRID_NAME_TXT'), F.col('r.GRID_COLUMN_TASK_ASSAY_ID').cast('string').alias('GRID_COLUMN_TASK_ASSAY_ID'), F.col('r.GRID_COLUMN_DESC_TXT').cast('string').alias('GRID_COLUMN_DESC_TXT'), F.col('r.GRID_COLUMN_MNEMONIC_TXT').cast('string').alias('GRID_COLUMN_MNEMONIC_TXT'), F.col('r.GRID_ROW_TASK_ASSAY_ID').cast('string').alias('GRID_ROW_TASK_ASSAY_ID'), F.col('r.GRID_ROW_DESC_TXT').cast('string').alias('GRID_ROW_DESC_TXT'), F.col('r.GRID_ROW_MNEMONIC_TXT').cast('string').alias('GRID_ROW_MNEMONIC_TXT'), F.col('r.FORM_INSTANCE_ID').cast('string').alias('FORM_INSTANCE_ID'), F.col('r.GRID_EVENT_CD').cast('string').alias('GRID_EVENT_CD'), F.col('r.INPUT_TYPE_FLG').cast('int').alias('INPUT_TYPE_FLG'), F.col('r.ACTIVE_IND').cast('int').alias('Reference_ACTIVE_IND'), F.col('r.EXTRACT_DT_TM').cast('timestamp').alias('Reference_EXTRACT_DT_TM'), F.col('r.Ctrl_Id').cast('int').alias('Reference_Ctrl_ID'), F.col('r.Record_Updated_Dt').cast('timestamp').alias('Reference_Record_Updated_Dt'), F.col('r.ADC_UPDT').cast('timestamp').alias('Reference_ADC_UPDT'), F.col('r.ReferenceKeyRowCount').cast('long').alias('ReferenceKeyRowCount'), expressions['response'].isNull().alias('ResponseMissing_IND'), (~expressions['form_date_source'].eqNullSafe(F.lit('PERFORMED_DT_TM'))).alias('FormDateFallback_IND'), (F.col('d.Trust').isNull() | (F.length(F.trim(F.col('d.Trust'))) == 0)).alias('TrustMissing_IND'), identifier_cast_failure.alias('IdentifierCastFailure_IND'))

def _mmvte_build_pregnancy_intervals(person_ids: DataFrame, source_versions: Dict[str, int], config: MapMatVTEConfig) -> DataFrame:
    persons = person_ids.select(F.col('PERSON_ID').cast('long').alias('Person_ID')).filter(F.col('Person_ID').isNotNull()).distinct()
    pregnancy = _mmvte_read_snapshot(config.pregnancy_table, source_versions[config.pregnancy_table])
    if 'SOURCE_MISSING_IND' in pregnancy.columns:
        pregnancy = pregnancy.filter(~F.coalesce(F.col('SOURCE_MISSING_IND'), F.lit(False)))
    pregnancy = pregnancy.join(F.broadcast(persons), 'Person_ID', 'inner')
    pregnancy_ids = pregnancy.select(F.col('Pregnancy_ID').cast('long').alias('Pregnancy_ID')).filter(F.col('Pregnancy_ID').isNotNull()).distinct()
    birth = _mmvte_read_snapshot(config.birth_table, source_versions[config.birth_table])
    if 'SOURCE_MISSING_IND' in birth.columns:
        birth = birth.filter(~F.coalesce(F.col('SOURCE_MISSING_IND'), F.lit(False)))
    birth = birth.join(F.broadcast(pregnancy_ids), 'Pregnancy_ID', 'inner')
    birth_timestamp = F.expr('try_to_timestamp(CAST(BirthDateTime AS STRING))')
    earliest_birth = birth.groupBy(F.col('Pregnancy_ID').cast('long').alias('Pregnancy_ID')).agg(F.min(birth_timestamp).alias('EarliestBirthDateTime'))
    gest_text = F.lower(F.coalesce(F.col('GestAgePregEnd'), F.lit('')))
    gest_weeks_text = F.regexp_extract(gest_text, '(-?\\d+)\\s*week', 1)
    gest_days_text = F.regexp_extract(gest_text, '(-?\\d+)\\s*day', 1)
    gest_weeks = F.when(F.length(gest_weeks_text) > 0, gest_weeks_text.cast('int'))
    gest_days_remainder = F.when(F.length(gest_days_text) > 0, gest_days_text.cast('int'))
    valid_gestation = gest_weeks.between(0, 45) & gest_days_remainder.between(0, 6)
    gestation_days = F.when(valid_gestation, gest_weeks * F.lit(7) + gest_days_remainder)
    intervals = pregnancy.alias('p').join(earliest_birth.alias('b'), 'Pregnancy_ID', 'left').withColumn('_gestation_days', gestation_days).withColumn('_lmp_date', F.to_date(F.col('LastMensPeriodDate'))).withColumn('_edd_date', F.to_date(F.col('ExpectedDeliveryDate'))).withColumn('_first_antenatal_date', F.to_date(F.col('FirstAntenatalAPPTDate'))).withColumn('_birth_date', F.to_date(F.col('EarliestBirthDateTime'))).withColumn('PregnancyReferenceDate', F.coalesce(F.col('_birth_date'), F.col('_edd_date'), F.when(F.col('_lmp_date').isNotNull() & F.col('_gestation_days').isNotNull(), F.date_add(F.col('_lmp_date'), F.col('_gestation_days'))))).withColumn('PregnancyMatchStartDate', F.coalesce(F.col('_lmp_date'), F.when(F.col('_edd_date').isNotNull(), F.date_sub(F.col('_edd_date'), 280)), F.when(F.col('PregnancyReferenceDate').isNotNull() & F.col('_gestation_days').isNotNull(), F.date_sub(F.col('PregnancyReferenceDate'), F.col('_gestation_days'))), F.col('_first_antenatal_date'))).withColumn('PregnancyMatchEndDate', F.when(F.col('PregnancyReferenceDate').isNotNull(), F.date_add(F.col('PregnancyReferenceDate'), 42))).withColumn('PregnancyMatchMethod', F.when(F.col('_lmp_date').isNotNull() & F.col('_birth_date').isNotNull(), F.lit('LMP_TO_EARLIEST_BIRTH')).when(F.col('_lmp_date').isNotNull() & F.col('_edd_date').isNotNull(), F.lit('LMP_TO_EDD')).when(F.col('_edd_date').isNotNull(), F.lit('EDD_DERIVED_START')).when(F.col('_birth_date').isNotNull() & F.col('_gestation_days').isNotNull(), F.lit('BIRTH_GESTATION_DERIVED_START')).when(F.col('_first_antenatal_date').isNotNull() & F.col('_birth_date').isNotNull(), F.lit('FIRST_ANTENATAL_TO_EARLIEST_BIRTH'))).withColumn('_match_priority', F.when(F.col('PregnancyMatchMethod') == 'LMP_TO_EARLIEST_BIRTH', 1).when(F.col('PregnancyMatchMethod') == 'LMP_TO_EDD', 2).when(F.col('PregnancyMatchMethod') == 'EDD_DERIVED_START', 3).when(F.col('PregnancyMatchMethod') == 'BIRTH_GESTATION_DERIVED_START', 4).when(F.col('PregnancyMatchMethod') == 'FIRST_ANTENATAL_TO_EARLIEST_BIRTH', 5).otherwise(99)).withColumn('_interval_days', F.datediff(F.col('PregnancyMatchEndDate'), F.col('PregnancyMatchStartDate'))).filter(F.col('PregnancyMatchStartDate').isNotNull() & F.col('PregnancyMatchEndDate').isNotNull() & F.col('_interval_days').between(0, 365)).select(F.col('Person_ID').cast('long').alias('Person_ID'), F.col('Pregnancy_ID').cast('long').alias('Pregnancy_ID'), 'PregnancyMatchStartDate', 'PregnancyMatchEndDate', 'PregnancyReferenceDate', 'PregnancyMatchMethod', '_match_priority', '_interval_days')
    return intervals

def _mmvte_link_pregnancy(base: DataFrame, source_versions: Dict[str, int], config: MapMatVTEConfig) -> DataFrame:
    person_ids = base.select('PERSON_ID').distinct()
    intervals = _mmvte_build_pregnancy_intervals(person_ids, source_versions, config)
    base_with_key = base.withColumn('_response_partition_key', F.coalesce(F.col('DOC_RESPONSE_KEY'), F.concat(F.lit('__NULL_KEY__'), F.monotonically_increasing_id().cast('string'))))
    candidates = base_with_key.alias('d').join(intervals.alias('p'), (F.col('d.PERSON_ID') == F.col('p.Person_ID')) & F.to_date(F.col('d.FormDate')).between(F.col('p.PregnancyMatchStartDate'), F.col('p.PregnancyMatchEndDate')), 'left').select('d.*', F.col('p.Pregnancy_ID').alias('_candidate_pregnancy_id'), F.col('p.PregnancyMatchStartDate').alias('_candidate_start_date'), F.col('p.PregnancyMatchEndDate').alias('_candidate_end_date'), F.col('p.PregnancyReferenceDate').alias('_candidate_reference_date'), F.col('p.PregnancyMatchMethod').alias('_candidate_method'), F.col('p._match_priority').alias('_candidate_priority'), F.col('p._interval_days').alias('_candidate_interval_days')).withColumn('_candidate_distance_days', F.abs(F.datediff(F.to_date(F.col('FormDate')), F.col('_candidate_reference_date'))))
    candidate_window = Window.partitionBy('_response_partition_key')
    rank_window = candidate_window.orderBy(F.when(F.col('_candidate_pregnancy_id').isNotNull(), 0).otherwise(1), F.col('_candidate_priority').asc_nulls_last(), F.col('_candidate_distance_days').asc_nulls_last(), F.col('_candidate_interval_days').asc_nulls_last(), F.col('_candidate_pregnancy_id').desc_nulls_last())
    return candidates.withColumn('PregnancyMatchCandidateCount', F.count(F.col('_candidate_pregnancy_id')).over(candidate_window).cast('long')).withColumn('_candidate_rank', F.row_number().over(rank_window)).filter(F.col('_candidate_rank') == 1).withColumn('Pregnancy_ID', F.col('_candidate_pregnancy_id').cast('long')).withColumn('PregnancyMatched_IND', F.col('_candidate_pregnancy_id').isNotNull()).withColumn('PregnancyMatchAmbiguous_IND', F.col('PregnancyMatchCandidateCount') > 1).withColumn('PregnancyMatchMethod', F.col('_candidate_method')).withColumn('PregnancyMatchStartDate', F.col('_candidate_start_date')).withColumn('PregnancyMatchEndDate', F.col('_candidate_end_date')).withColumn('PregnancyReferenceDate', F.col('_candidate_reference_date')).withColumn('PregnancyMatchDistanceDays', F.col('_candidate_distance_days').cast('int')).drop('_response_partition_key', '_candidate_pregnancy_id', '_candidate_start_date', '_candidate_end_date', '_candidate_reference_date', '_candidate_method', '_candidate_priority', '_candidate_interval_days', '_candidate_distance_days', '_candidate_rank')

def _mmvte_finalize_snapshot(linked: DataFrame, trigger_sources: str, run_id: str, run_timestamp: datetime, config: MapMatVTEConfig) -> DataFrame:
    source_columns = [field.name for field in schema_map_mat_vte.fields if field.name not in _MMVTE_PIPELINE_COLUMNS]
    with_hash = linked.withColumn('ROW_HASH', F.sha2(F.to_json(F.struct(*[F.col(name).alias(name) for name in source_columns])), 256))
    final = with_hash.withColumn('TRIGGER_SOURCES', F.lit(trigger_sources)).withColumn('PIPELINE_RUN_ID', F.lit(run_id)).withColumn('PIPELINE_UPDT_DT_TM', F.lit(run_timestamp).cast('timestamp')).withColumn('SCHEMA_VERSION', F.lit(config.schema_version))
    return _mmvte_align_to_schema(final, schema_map_mat_vte)

def _mmvte_build_full_snapshot(source_versions: Dict[str, int], run_id: str, run_timestamp: datetime, config: MapMatVTEConfig) -> DataFrame:
    reference = _mmvte_build_reference_snapshot(source_versions[config.reference_source_table], config)
    responses = _mmvte_read_snapshot(config.response_source_table, source_versions[config.response_source_table])
    base = _mmvte_project_response_reference(responses, reference)
    linked = _mmvte_link_pregnancy(base, source_versions, config)
    return _mmvte_finalize_snapshot(linked, 'FULL_REBUILD', run_id, run_timestamp, config)

def _mmvte_latest_response_cdf_rows(cdf: Optional[DataFrame], response_columns: Sequence[str], empty_source: DataFrame) -> Tuple[DataFrame, DataFrame]:
    empty_rows = empty_source.select(*response_columns).limit(0)
    empty_keys = empty_source.select(F.col('DOC_RESPONSE_KEY').cast('string').alias('DOC_RESPONSE_KEY')).limit(0)
    if cdf is None:
        return (empty_rows, empty_keys)
    changed_keys = cdf.select(F.col('DOC_RESPONSE_KEY').cast('string').alias('DOC_RESPONSE_KEY')).filter(F.col('DOC_RESPONSE_KEY').isNotNull()).distinct()
    priority = F.when(F.col('_change_type') == 'delete', 4).when(F.col('_change_type') == 'update_postimage', 3).when(F.col('_change_type') == 'insert', 2).when(F.col('_change_type') == 'update_preimage', 1).otherwise(0)
    rank_window = Window.partitionBy('DOC_RESPONSE_KEY').orderBy(F.col('_commit_version').desc(), priority.desc(), F.col('_commit_timestamp').desc())
    latest = cdf.withColumn('_cdf_rank', F.row_number().over(rank_window)).filter(F.col('_cdf_rank') == 1)
    current_rows = latest.filter(F.col('_change_type').isin('insert', 'update_postimage')).select(*[F.col(name) for name in response_columns])
    return (current_rows, changed_keys)

def _mmvte_changed_reference_keys(cdf: Optional[DataFrame], empty_reference: DataFrame) -> DataFrame:
    if cdf is None:
        return empty_reference.select(F.col('DOC_INPUT_KEY').cast('string').alias('DOC_INPUT_ID')).limit(0)
    return cdf.select(F.col('DOC_INPUT_KEY').cast('string').alias('DOC_INPUT_ID')).filter(F.col('DOC_INPUT_ID').isNotNull()).distinct()

def _mmvte_changed_person_ids(pregnancy_cdf: Optional[DataFrame], birth_cdf: Optional[DataFrame], pregnancy_snapshot: DataFrame) -> DataFrame:
    empty = pregnancy_snapshot.select(F.col('Person_ID').cast('long').alias('PERSON_ID')).limit(0)
    frames: List[DataFrame] = []
    if pregnancy_cdf is not None and 'Person_ID' in pregnancy_cdf.columns:
        frames.append(pregnancy_cdf.select(F.col('Person_ID').cast('long').alias('PERSON_ID')))
    if birth_cdf is not None:
        if 'MotherPerson_ID' in birth_cdf.columns:
            frames.append(birth_cdf.select(F.col('MotherPerson_ID').cast('long').alias('PERSON_ID')))
        if 'Pregnancy_ID' in birth_cdf.columns:
            changed_pregnancies = birth_cdf.select(F.col('Pregnancy_ID').cast('long').alias('Pregnancy_ID')).filter(F.col('Pregnancy_ID').isNotNull()).distinct()
            frames.append(pregnancy_snapshot.select(F.col('Pregnancy_ID').cast('long').alias('Pregnancy_ID'), F.col('Person_ID').cast('long').alias('PERSON_ID')).join(changed_pregnancies, 'Pregnancy_ID', 'inner').select('PERSON_ID'))
    if not frames:
        return empty
    changed = frames[0]
    for frame in frames[1:]:
        changed = changed.unionByName(frame, allowMissingColumns=True)
    return changed.filter(F.col('PERSON_ID').isNotNull()).distinct()

def _mmvte_triggered_current_response_rows(response_snapshot: DataFrame, changed_reference_keys: DataFrame, changed_person_ids: DataFrame, reference_key_count: int, person_count: int) -> DataFrame:
    if reference_key_count == 0 and person_count == 0:
        return response_snapshot.limit(0)
    current = response_snapshot.alias('d')
    if reference_key_count > 0:
        reference_flags = changed_reference_keys.withColumn('_reference_trigger', F.lit(True)).alias('rk')
        current = current.join(F.broadcast(reference_flags), F.col('d.DOC_INPUT_ID') == F.col('rk.DOC_INPUT_ID'), 'left')
    else:
        current = current.withColumn('_reference_trigger', F.lit(False))
    if person_count > 0:
        person_flags = changed_person_ids.withColumn('_person_trigger', F.lit(True)).alias('pk')
        current = current.join(F.broadcast(person_flags), F.expr('try_cast(d.PERSON_ID as bigint)') == F.col('pk.PERSON_ID'), 'left')
    else:
        current = current.withColumn('_person_trigger', F.lit(False))
    return current.filter(F.coalesce(F.col('_reference_trigger'), F.lit(False)) | F.coalesce(F.col('_person_trigger'), F.lit(False))).select('d.*')

def _mmvte_affected_target_keys(changed_response_keys: DataFrame, changed_reference_keys: DataFrame, reference_key_count: int, config: MapMatVTEConfig) -> DataFrame:
    affected = changed_response_keys.select('DOC_RESPONSE_KEY')
    if reference_key_count > 0 and _mmvte_table_exists(config.target_table) and ('DOC_INPUT_ID' in spark.table(config.target_table).columns):
        reference_affected = spark.table(config.target_table).alias('t').join(F.broadcast(changed_reference_keys.alias('r')), F.col('t.DOC_INPUT_ID') == F.col('r.DOC_INPUT_ID'), 'inner').select(F.col('t.DOC_RESPONSE_KEY').alias('DOC_RESPONSE_KEY'))
        affected = affected.unionByName(reference_affected)
    return affected.filter(F.col('DOC_RESPONSE_KEY').isNotNull()).distinct()

def _mmvte_build_incremental_snapshot(previous_versions: Dict[str, int], source_versions: Dict[str, int], run_id: str, run_timestamp: datetime, config: MapMatVTEConfig) -> Tuple[DataFrame, DataFrame, Dict[str, int]]:
    for table_name, current_version in source_versions.items():
        previous_version = previous_versions.get(table_name)
        if previous_version is None:
            raise MapMatVTECdfUnavailable(f'No prior checkpoint is available for {table_name}')
        if current_version < previous_version:
            raise MapMatVTECdfUnavailable(f'Delta version for {table_name} moved backwards from {previous_version} to {current_version}')
    response_snapshot = _mmvte_read_snapshot(config.response_source_table, source_versions[config.response_source_table])
    reference_source_snapshot = _mmvte_read_snapshot(config.reference_source_table, source_versions[config.reference_source_table])
    pregnancy_snapshot = _mmvte_read_snapshot(config.pregnancy_table, source_versions[config.pregnancy_table])
    reference = _mmvte_build_reference_snapshot(source_versions[config.reference_source_table], config)
    response_cdf = _mmvte_read_cdf(config.response_source_table, previous_versions[config.response_source_table], source_versions[config.response_source_table])
    reference_cdf = _mmvte_read_cdf(config.reference_source_table, previous_versions[config.reference_source_table], source_versions[config.reference_source_table])
    pregnancy_cdf = _mmvte_read_cdf(config.pregnancy_table, previous_versions[config.pregnancy_table], source_versions[config.pregnancy_table])
    birth_cdf = _mmvte_read_cdf(config.birth_table, previous_versions[config.birth_table], source_versions[config.birth_table])
    if response_cdf is not None:
        scoped_doc_inputs = reference.select(F.col('DOC_INPUT_KEY').cast('string').alias('_scope_doc_input_id')).distinct()
        response_cdf = response_cdf.alias('c').join(F.broadcast(scoped_doc_inputs.alias('scope')), F.col('c.DOC_INPUT_ID') == F.col('scope._scope_doc_input_id'), 'inner').select('c.*')
    changed_response_rows, changed_response_keys = _mmvte_latest_response_cdf_rows(response_cdf, response_snapshot.columns, response_snapshot)
    changed_reference_keys = _mmvte_changed_reference_keys(reference_cdf, reference_source_snapshot)
    changed_person_ids = _mmvte_changed_person_ids(pregnancy_cdf, birth_cdf, pregnancy_snapshot)
    changed_response_keys = _mmvte_maybe_persist(changed_response_keys)
    changed_reference_keys = _mmvte_maybe_persist(changed_reference_keys)
    changed_person_ids = _mmvte_maybe_persist(changed_person_ids)
    try:
        trigger_counts = {'changed_response_keys': int(changed_response_keys.count()), 'changed_reference_keys': int(changed_reference_keys.count()), 'changed_person_ids': int(changed_person_ids.count())}
        triggered_current = _mmvte_triggered_current_response_rows(response_snapshot, changed_reference_keys, changed_person_ids, trigger_counts['changed_reference_keys'], trigger_counts['changed_person_ids'])
        response_rows = changed_response_rows.unionByName(triggered_current, allowMissingColumns=True).dropDuplicates(['DOC_RESPONSE_KEY'])
        base = _mmvte_project_response_reference(response_rows, reference)
        linked = _mmvte_link_pregnancy(base, source_versions, config)
        trigger_names = [name for name, count in (('DOC_RESPONSE', trigger_counts['changed_response_keys']), ('DOC_REFERENCE', trigger_counts['changed_reference_keys']), ('PREGNANCY_OR_BIRTH', trigger_counts['changed_person_ids'])) if count > 0]
        snapshot = _mmvte_finalize_snapshot(linked, ','.join(trigger_names) or 'NO_RELEVANT_CHANGES', run_id, run_timestamp, config)
        affected_keys = _mmvte_affected_target_keys(changed_response_keys, changed_reference_keys, trigger_counts['changed_reference_keys'], config)
        delete_keys = affected_keys.join(snapshot.select('DOC_RESPONSE_KEY').distinct(), 'DOC_RESPONSE_KEY', 'left_anti')
        snapshot = _mmvte_maybe_persist(snapshot)
        delete_keys = _mmvte_maybe_persist(delete_keys)
        trigger_counts['incremental_snapshot_rows'] = int(snapshot.count())
        trigger_counts['incremental_delete_keys'] = int(delete_keys.count())
        return (snapshot, delete_keys, trigger_counts)
    finally:
        _mmvte_maybe_unpersist(changed_response_keys)
        _mmvte_maybe_unpersist(changed_reference_keys)
        _mmvte_maybe_unpersist(changed_person_ids)

def _mmvte_preflight_snapshot(snapshot: DataFrame) -> Dict[str, int]:
    summary_row = snapshot.agg(F.count(F.lit(1)).alias('snapshot_rows'), F.count_if(F.col('DOC_RESPONSE_KEY').isNull()).alias('null_source_keys'), F.count_if(F.col('PregnancyMatched_IND')).alias('pregnancy_matched_rows'), F.count_if(F.col('PregnancyMatchAmbiguous_IND')).alias('ambiguous_pregnancy_rows'), F.count_if(F.col('ResponseMissing_IND')).alias('missing_response_rows'), F.count_if(F.col('TrustMissing_IND')).alias('missing_trust_rows'), F.count_if(F.col('IdentifierCastFailure_IND')).alias('identifier_cast_failure_rows')).first()
    metrics = {key: int(value or 0) for key, value in summary_row.asDict().items()}
    duplicate_keys = snapshot.groupBy('DOC_RESPONSE_KEY').count().filter(F.col('DOC_RESPONSE_KEY').isNotNull() & (F.col('count') > 1)).count()
    metrics['duplicate_source_key_groups'] = int(duplicate_keys)
    failures = {key: metrics[key] for key in ('null_source_keys', 'duplicate_source_key_groups') if metrics[key] != 0}
    if failures:
        raise RuntimeError(f'VTE source-key preflight failed: {failures}. No rows were discarded or arbitrarily deduplicated.')
    return metrics

def _mmvte_replace_target(snapshot: DataFrame, config: MapMatVTEConfig) -> Dict[str, object]:
    snapshot = bronze_project_contract(snapshot, config.target_table)
    snapshot.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('delta.enableChangeDataFeed', 'true').option('delta.enableRowTracking', 'true').option('delta.enableTypeWidening', 'true').option('delta.parquet.compression.codec', 'zstd').saveAsTable(config.target_table)
    schema_result = _mmvte_ensure_target_table(config)
    return {'schema_result': schema_result, 'operation_metrics': _mmvte_latest_operation_metrics(config.target_table)}

def _mmvte_merge_incremental(snapshot: DataFrame, delete_keys: DataFrame, config: MapMatVTEConfig) -> Dict[str, object]:
    schema_result = _mmvte_ensure_target_table(config)
    operations: Dict[str, object] = {'schema_result': schema_result}
    if not snapshot.isEmpty():
        snapshot = bronze_project_contract(snapshot, config.target_table)
        target = DeltaTable.forName(spark, config.target_table)
        assignments = {
            column_name: F.col(f's.{column_name}')
            for column_name in snapshot.columns
        }
        comparisons = ' OR '.join(
            f'NOT (t.`{column_name}` <=> s.`{column_name}`)'
            for column_name in snapshot.columns
            if column_name != 'DOC_RESPONSE_KEY'
        )
        target.alias('t').merge(
            snapshot.alias('s'),
            't.DOC_RESPONSE_KEY <=> s.DOC_RESPONSE_KEY',
        ).whenMatchedUpdate(
            condition=comparisons or 'false',
            set=assignments,
        ).whenNotMatchedInsert(values=assignments).execute()
        operations['upsert_metrics'] = _mmvte_latest_operation_metrics(config.target_table)
    else:
        operations['upsert_metrics'] = {'operation': 'SKIPPED_EMPTY_UPSERT'}
    if not delete_keys.isEmpty():
        target = DeltaTable.forName(spark, config.target_table)
        target.alias('t').merge(delete_keys.select('DOC_RESPONSE_KEY').distinct().alias('d'), 't.DOC_RESPONSE_KEY <=> d.DOC_RESPONSE_KEY').whenMatchedDelete().execute()
        operations['delete_metrics'] = _mmvte_latest_operation_metrics(config.target_table)
    else:
        operations['delete_metrics'] = {'operation': 'SKIPPED_EMPTY_DELETE'}
    return operations

def validate_map_mat_vte_target(snapshot: DataFrame, full_rebuild: bool, config: MapMatVTEConfig=MAP_MAT_VTE_CONFIG) -> Dict[str, int]:
    target = spark.table(config.target_table)
    duplicate_keys = target.groupBy('DOC_RESPONSE_KEY').count().filter(
        F.col('DOC_RESPONSE_KEY').isNotNull() & (F.col('count') > 1)
    ).count()
    null_keys = target.filter(F.col('DOC_RESPONSE_KEY').isNull()).count()
    missing_snapshot_keys = snapshot.select('DOC_RESPONSE_KEY').join(
        target.select('DOC_RESPONSE_KEY'),
        'DOC_RESPONSE_KEY',
        'left_anti',
    ).count()
    hash_mismatches = (
        snapshot.alias('s')
        .join(
            target.select('DOC_RESPONSE_KEY', 'ROW_HASH').alias('t'),
            'DOC_RESPONSE_KEY',
            'inner',
        )
        .filter(~F.col('s.ROW_HASH').eqNullSafe(F.col('t.ROW_HASH')))
        .count()
    )
    metrics = {
        'target_rows': int(target.count()),
        'duplicate_source_key_groups': int(duplicate_keys),
        'null_source_keys': int(null_keys),
        'snapshot_keys_missing_from_target': int(missing_snapshot_keys),
        'snapshot_hash_mismatches': int(hash_mismatches),
        'target_null_pregnancy_rows': int(
            target.filter(F.col('Pregnancy_ID').isNull()).count()
        ),
    }
    if full_rebuild:
        metrics['target_keys_absent_from_full_snapshot'] = int(
            target.select('DOC_RESPONSE_KEY').join(
                snapshot.select('DOC_RESPONSE_KEY'),
                'DOC_RESPONSE_KEY',
                'left_anti',
            ).count()
        )
        metrics['full_snapshot_rows'] = int(snapshot.count())
    failure_keys = [
        'duplicate_source_key_groups', 'null_source_keys',
        'snapshot_keys_missing_from_target', 'snapshot_hash_mismatches',
    ]
    if full_rebuild:
        failure_keys.append('target_keys_absent_from_full_snapshot')
    failures = {
        key: metrics.get(key, 0)
        for key in failure_keys
        if metrics.get(key, 0) != 0
    }
    if failures:
        raise RuntimeError(f'map_mat_vte_assessment post-checks failed: {failures}')
    return metrics
def run_map_mat_vte_post_deployment_checks(config: MapMatVTEConfig=MAP_MAT_VTE_CONFIG) -> Dict[str, int]:
    if not _mmvte_table_exists(config.target_table):
        raise AssertionError(f'Target does not exist: {config.target_table}')
    target = spark.table(config.target_table)
    metrics = {
        'target_rows': int(target.count()),
        'distinct_source_keys': int(
            target.select('DOC_RESPONSE_KEY').distinct().count()
        ),
        'null_source_keys': int(
            target.filter(F.col('DOC_RESPONSE_KEY').isNull()).count()
        ),
        'null_pregnancy_rows': int(
            target.filter(F.col('Pregnancy_ID').isNull()).count()
        ),
        'distinct_forms': int(target.select('FORM_REF_ID').distinct().count()),
        'distinct_sections': int(target.select('SECTION_REF_ID').distinct().count()),
        'distinct_elements': int(target.select('DOC_INPUT_ID').distinct().count()),
    }
    if metrics['target_rows'] != metrics['distinct_source_keys']:
        raise AssertionError(
            f"Target is not one row per DOC_RESPONSE_KEY: "
            f"{metrics['target_rows']} rows vs {metrics['distinct_source_keys']} keys"
        )
    if metrics['null_source_keys'] != 0:
        raise AssertionError(
            f"Target contains {metrics['null_source_keys']} null DOC_RESPONSE_KEY rows"
        )
    return metrics
def build_map_mat_vte_snapshot(force_full_rebuild: bool=False, run_id: Optional[str]=None, run_timestamp: Optional[datetime]=None, captured_source_versions: Optional[Dict[str, int]]=None, config: MapMatVTEConfig=MAP_MAT_VTE_CONFIG) -> Dict[str, object]:
    """Build a full or checkpointed incremental plan without writing the target."""
    run_id = run_id or str(uuid.uuid4())
    run_timestamp = run_timestamp or datetime.now(timezone.utc)
    source_versions = dict(captured_source_versions) if captured_source_versions is not None else _mmvte_capture_source_versions(config)
    state = _mmvte_last_success_state(config)
    if force_full_rebuild:
        snapshot = _mmvte_build_full_snapshot(source_versions, run_id, run_timestamp, config)
        delete_keys = snapshot.select('DOC_RESPONSE_KEY').limit(0)
        return {'mode': 'FULL_REBUILD', 'full_rebuild': True, 'snapshot': snapshot, 'delete_keys': delete_keys, 'source_versions': source_versions, 'previous_versions': {}, 'trigger_counts': {}}
    if not _mmvte_table_exists(config.target_table):
        raise RuntimeError('Target table does not exist. Run with force_full_rebuild=True.')
    if 'DOC_RESPONSE_KEY' not in spark.table(config.target_table).columns:
        raise RuntimeError('Existing target predates the stable source key. Run with force_full_rebuild=True.')
    if state is None:
        raise RuntimeError('No successful source-version checkpoint exists. Run with force_full_rebuild=True.')
    if state['schema_version'] != config.schema_version:
        raise RuntimeError(f"Checkpoint schema version {state['schema_version']} does not match {config.schema_version}. Run with force_full_rebuild=True.")
    previous_versions = state['source_versions']
    snapshot, delete_keys, trigger_counts = _mmvte_build_incremental_snapshot(previous_versions, source_versions, run_id, run_timestamp, config)
    return {'mode': 'INCREMENTAL', 'full_rebuild': False, 'snapshot': snapshot, 'delete_keys': delete_keys, 'source_versions': source_versions, 'previous_versions': previous_versions, 'trigger_counts': trigger_counts}

def create_mat_vte_mapping_incr(force_full_rebuild: bool=False, config: MapMatVTEConfig=MAP_MAT_VTE_CONFIG) -> DataFrame:
    """
    Compatibility builder for interactive inspection.

    Production should call run_map_mat_vte_update so source checkpoints are advanced
    only after the target update succeeds.
    """
    plan = build_map_mat_vte_snapshot(force_full_rebuild=force_full_rebuild, config=config)
    return plan['snapshot']

def run_map_mat_vte_update(force_full_rebuild: bool=False, dry_run: bool=False, run_post_checks: bool=True, config: MapMatVTEConfig=MAP_MAT_VTE_CONFIG) -> Dict[str, object]:
    """Production entry point replacing the original VTE extraction and update cells."""
    run_id = str(uuid.uuid4())
    run_timestamp = datetime.now(timezone.utc)
    source_versions = _mmvte_capture_source_versions(config)
    mode = 'FULL_REBUILD' if force_full_rebuild else 'INCREMENTAL'
    metrics: Dict[str, object] = {}
    snapshot: Optional[DataFrame] = None
    delete_keys: Optional[DataFrame] = None
    if not dry_run:
        _mmvte_ensure_audit_table(config)
        _mmvte_write_audit_event(config, run_id, 'STARTED', mode, 'Started map_mat_vte_assessment replacement run.', source_versions)
    try:
        try:
            plan = build_map_mat_vte_snapshot(force_full_rebuild=force_full_rebuild, run_id=run_id, run_timestamp=run_timestamp, captured_source_versions=source_versions, config=config)
        except MapMatVTECdfUnavailable as exc:
            if not config.auto_full_rebuild_on_cdf_gap:
                raise
            mode = 'FULL_REBUILD_CDF_FALLBACK'
            print(f'[WARN] {exc}')
            print('[WARN] Falling back to a complete current-snapshot rebuild.')
            plan = build_map_mat_vte_snapshot(force_full_rebuild=True, run_id=run_id, run_timestamp=run_timestamp, captured_source_versions=source_versions, config=config)
        mode = mode if mode == 'FULL_REBUILD_CDF_FALLBACK' else plan['mode']
        full_rebuild = bool(plan['full_rebuild'])
        snapshot = _mmvte_maybe_persist(plan['snapshot'])
        delete_keys = _mmvte_maybe_persist(plan['delete_keys'])
        preflight = _mmvte_preflight_snapshot(snapshot)
        delete_key_count = int(delete_keys.count())
        metrics.update({'preflight': preflight, 'delete_key_count': delete_key_count, 'trigger_counts': plan.get('trigger_counts', {}), 'previous_versions': plan.get('previous_versions', {})})
        if dry_run:
            result = {'run_id': run_id, 'mode': f'DRY_RUN_{mode}', 'schema_version': config.schema_version, 'source_versions': source_versions, **metrics}
            print(json.dumps(result, indent=2, sort_keys=True, default=str))
            return result
        if full_rebuild:
            update_result = _mmvte_replace_target(snapshot, config)
        else:
            update_result = _mmvte_merge_incremental(snapshot, delete_keys, config)
        metrics['update_result'] = update_result
        if run_post_checks:
            metrics['post_checks'] = validate_map_mat_vte_target(snapshot, full_rebuild=full_rebuild, config=config)
        _mmvte_write_audit_event(config, run_id, 'SUCCESS', mode, 'map_mat_vte_assessment replacement completed successfully.', source_versions, metrics)
        result = {'run_id': run_id, 'mode': mode, 'schema_version': config.schema_version, 'source_versions': source_versions, **metrics}
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return result
    except Exception as exc:
        if not dry_run:
            try:
                _mmvte_write_audit_event(config, run_id, 'FAILED', mode, str(exc), source_versions, metrics)
            except Exception as audit_exc:
                print(f'[WARN] Could not write failure audit event: {audit_exc}')
        raise
    finally:
        _mmvte_maybe_unpersist(snapshot)
        _mmvte_maybe_unpersist(delete_keys)

try:
    _targets = ['4_prod.bronze.map_mat_vte_assessment']
    if not _pipeline_resume_skip_component('map_mat_vte_assessment', _targets):
        _pipeline_run_recoverable('map_mat_vte_assessment', _PIPELINE_FULL_REFRESH, lambda: run_map_mat_vte_update(force_full_rebuild=False, run_post_checks=False), lambda: run_map_mat_vte_update(force_full_rebuild=True, run_post_checks=True))
        _PIPELINE_UPDATED_TARGETS.extend(_targets)
        _pipeline_mark_component_complete('map_mat_vte_assessment', _targets)
        _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_mat_vte_assessment'})
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

_pipeline_component_start("map_family_history")
_pipeline_shared_update_table = globals().get('update_table')
_pipeline_shared_table_exists = globals().get('table_exists')
_pipeline_shared_get_max_timestamp = globals().get('get_max_timestamp')
_pipeline_shared_has_cdf_enabled = globals().get('has_cdf_enabled')
_pipeline_shared_get_incremental = globals().get('get_incremental_data_with_cdf')
globals().pop('_map_updates_original_update_table', None)
globals().pop('_map_medical_personnel_original_update_table', None)
globals().pop('_MAP_UPDATES_ORIGINAL_UPDATE_TABLE', None)

from functools import reduce
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window
MFH_TARGET = '4_prod.bronze.map_family_history'
MFH_CURRENT_VIEW = '4_prod.bronze.map_family_history_current'
MFH_FHX_SOURCE = '4_prod.raw.mill_fhx_activity'
MFH_RELATION_SOURCE = '4_prod.raw.mill_person_person_reltn'
MFH_NOMENCLATURE_SOURCE = '3_lookup.mill.map_nomenclature'
MFH_TEXT_LINK_SOURCE = '4_prod.raw.mill_fhx_long_text_r'
MFH_LONG_TEXT_SOURCE = '4_prod.raw.mill_long_text'
MFH_ALIAS_SOURCE = '4_prod.raw.mill_person_alias'
MFH_CODE_VALUE_SOURCE = '3_lookup.mill.mill_code_value'
MRN_POOL_PRIORITY = {683996: 1, 1115132483: 2, 6200990: 3, 6173940: 4}
MRN_POOL_LABELS = {683996: 'mrn', 1115132483: 'RF4 MRN', 6200990: 'RNJ 5C4 MRN', 6173940: '5C4 CMRN'}

def _quote_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"

def _quote_table(table_name: str) -> str:
    return '.'.join((_quote_identifier(part) for part in table_name.split('.')))

def _escape_sql_string(value: str) -> str:
    return (value or '').replace('\\', '\\\\').replace("'", "''")

def _normalise_comment(value: str) -> str:
    return ' '.join((value or '').split())

def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _get_table_comment(table_name: str) -> str:
    try:
        return spark.catalog.getTable(table_name).description or ''
    except Exception:
        rows = spark.sql(f'DESCRIBE TABLE EXTENDED {_quote_table(table_name)}').collect()
        for row in rows:
            if str(row['col_name'] or '').strip().lower() == 'comment':
                return row['data_type'] or ''
        return ''

def detect_schema_changes(target_table: str, target_schema: StructType=None, table_comment: str=None):
    changes = {'has_changes': False, 'columns_to_update': [], 'columns_to_add': [], 'table_comment_update': None}
    if target_schema:
        current_fields = {field.name: field for field in spark.table(target_table).schema.fields}
        for target_field in target_schema.fields:
            current_field = current_fields.get(target_field.name)
            if current_field is None:
                changes['columns_to_add'].append({'name': target_field.name, 'type': target_field.dataType.simpleString(), 'comment': target_field.metadata.get('comment', ''), 'nullable': target_field.nullable})
                changes['has_changes'] = True
                continue
            current_comment = current_field.metadata.get('comment', '')
            target_comment = target_field.metadata.get('comment', '')
            type_changed = current_field.dataType != target_field.dataType
            comment_changed = _normalise_comment(current_comment) != _normalise_comment(target_comment)
            if type_changed or comment_changed:
                changes['columns_to_update'].append({'name': target_field.name, 'current_type': current_field.dataType.simpleString(), 'type': target_field.dataType.simpleString(), 'comment': target_comment, 'type_changed': type_changed, 'comment_changed': comment_changed})
                changes['has_changes'] = True
    if table_comment is not None:
        current_comment = _get_table_comment(target_table)
        if _normalise_comment(current_comment) != _normalise_comment(table_comment):
            changes['table_comment_update'] = table_comment
            changes['has_changes'] = True
    return changes

def apply_schema_changes(target_table: str, changes: dict):
    type_changes = [item for item in changes['columns_to_update'] if item['type_changed']]
    if type_changes:
        details = ', '.join((f"{item['name']}: {item['current_type']} -> {item['type']}" for item in type_changes))
        raise TypeError(f'Refusing to drop/overwrite {target_table} for automatic type changes ({details}). Apply a reviewed widening migration separately.')
    quoted_table = _quote_table(target_table)
    additions = changes['columns_to_add']
    if additions:
        definitions = []
        for item in additions:
            definition = f"{_quote_identifier(item['name'])} {item['type']}"
            if item['comment']:
                definition += f" COMMENT '{_escape_sql_string(item['comment'])}'"
            definitions.append(definition)
        spark.sql(f"ALTER TABLE {quoted_table} ADD COLUMNS ({', '.join(definitions)})")
    for item in changes['columns_to_update']:
        if item['comment_changed']:
            spark.sql(f"ALTER TABLE {quoted_table} ALTER COLUMN {_quote_identifier(item['name'])} COMMENT '{_escape_sql_string(item['comment'])}'")
    if changes['table_comment_update'] is not None:
        spark.sql(f"COMMENT ON TABLE {quoted_table} IS '{_escape_sql_string(changes['table_comment_update'])}'")

def _align_df_to_schema(source_df: DataFrame, target_schema: StructType) -> DataFrame:
    expressions = []
    source_columns = set(source_df.columns)
    for field in target_schema.fields:
        if field.name in source_columns:
            expressions.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            expressions.append(F.lit(None).cast(field.dataType).alias(field.name))
    return source_df.select(*expressions)

def create_table_with_schema(source_df: DataFrame, target_table: str, target_schema: StructType=None, table_comment: str=None):
    if bronze_is_primary_table(target_table):
        target_schema = bronze_contract_schema(target_table)
        source_df = bronze_project_contract(source_df, target_table)
    if target_schema is None:
        source_df.write.format('delta').option('delta.enableChangeDataFeed', 'true').mode('errorifexists').saveAsTable(target_table)
        return
    builder = DeltaTable.createIfNotExists(spark).tableName(target_table).addColumns(target_schema)
    if table_comment:
        builder = builder.comment(table_comment)
    builder.property('delta.enableChangeDataFeed', 'true').property('delta.enableRowTracking', 'true').execute()
    _align_df_to_schema(source_df, target_schema).write.mode('append').saveAsTable(target_table)

def update_table(source_df: DataFrame, target_table: str, index_column, target_schema: StructType=None, table_comment: str=None, update_condition: str=None, source_is_full_snapshot: bool=False, delete_not_matched_by_source: bool=False, validate_unique_keys: bool=True, allow_empty_snapshot: bool=False):
    """Backward-compatible Delta merge helper with safe optional full-snapshot synchronization."""
    if source_df is None:
        print(f'[INFO] No source dataframe supplied for {target_table}; skipping.')
        return
    index_keys = [index_column] if isinstance(index_column, str) else list(index_column)
    if bronze_is_primary_table(target_table):
        target_schema = bronze_contract_schema(target_table)
        source_df = bronze_project_contract(source_df, target_table)
    elif target_schema:
        source_df = _align_df_to_schema(source_df, target_schema)
    has_rows = source_df.limit(1).count() > 0
    if not has_rows:
        if source_is_full_snapshot and (not allow_empty_snapshot):
            raise ValueError(f'Refusing an empty full-snapshot synchronization into {target_table}.')
        print(f'[INFO] Source dataframe is empty; skipping {target_table}.')
        return
    if validate_unique_keys:
        duplicate_rows = source_df.groupBy(*index_keys).count().where(F.col('count') > 1).limit(10).collect()
        if duplicate_rows:
            raise ValueError(f'Source for {target_table} is not unique on {index_keys}. Example duplicate keys: {duplicate_rows}')
    if not table_exists(target_table):
        create_table_with_schema(source_df, target_table, target_schema, table_comment)
        print(f'[INFO] Created and populated {target_table}.')
        return
    if target_schema or table_comment is not None:
        changes = detect_schema_changes(target_table, target_schema, table_comment)
        if changes['has_changes']:
            apply_schema_changes(target_table, changes)
    if target_schema:
        source_df = _align_df_to_schema(source_df, target_schema)
    quoted_keys = [f't.{_quote_identifier(key)} <=> s.{_quote_identifier(key)}' for key in index_keys]
    merge_condition = ' AND '.join(quoted_keys)
    if update_condition is None and 'ROW_HASH' in source_df.columns:
        update_condition = 'NOT (t.`ROW_HASH` <=> s.`ROW_HASH`)'
    merge_builder = DeltaTable.forName(spark, target_table).alias('t').merge(
        source_df.alias('s'), merge_condition
    )
    assignments = {
        column_name: F.col(f's.{column_name}')
        for column_name in source_df.columns
    }
    if update_condition:
        merge_builder = merge_builder.whenMatchedUpdate(
            condition=update_condition, set=assignments
        )
    else:
        comparisons = ' OR '.join(
            f'NOT (t.`{column_name}` <=> s.`{column_name}`)'
            for column_name in source_df.columns
            if column_name not in index_keys
        )
        merge_builder = merge_builder.whenMatchedUpdate(
            condition=comparisons or 'false', set=assignments
        )
    merge_builder = merge_builder.whenNotMatchedInsert(values=assignments)
    if delete_not_matched_by_source:
        if not source_is_full_snapshot:
            raise ValueError('delete_not_matched_by_source=True requires source_is_full_snapshot=True.')
        merge_builder = merge_builder.whenNotMatchedBySourceDelete()
    merge_builder.execute()
    print(f'[INFO] Successfully synchronized {target_table}.')

def _sf(name, data_type, comment):
    return StructField(name, data_type, True, metadata={'comment': comment})
family_history_comment_struct = StructType([StructField('COMMENT_DT_TM', TimestampType(), True), StructField('FHX_LONG_TEXT_R_ID', LongType(), True), StructField('LONG_TEXT_ID', LongType(), True), StructField('COMMENT_PRSNL_ID', LongType(), True), StructField('LONG_TEXT', StringType(), True), StructField('LINK_ADC_UPDT', TimestampType(), True), StructField('LONG_TEXT_ADC_UPDT', TimestampType(), True), StructField('LONG_TEXT_ACTIVE_IND', IntegerType(), True)])
family_history_alias_struct = StructType([StructField('BEG_EFFECTIVE_DT_TM', TimestampType(), True), StructField('ADC_UPDT', TimestampType(), True), StructField('PERSON_ALIAS_ID', LongType(), True), StructField('PERSON_ALIAS_TYPE_CD', LongType(), True), StructField('ALIAS_POOL_CD', LongType(), True), StructField('ALIAS', StringType(), True), StructField('ACTIVE_IND', IntegerType(), True), StructField('END_EFFECTIVE_DT_TM', TimestampType(), True)])
schema_map_family_history = StructType([_sf('FHX_ACTIVITY_ID', LongType(), 'Unique source FHX_ACTIVITY row identifier and target merge key.'), _sf('PERSON_ID', LongType(), 'Patient whose family history is recorded.'), _sf('MRN', StringType(), 'Deterministically selected compatibility MRN using documented alias-pool priority.'), _sf('NHS_Number', StringType(), 'Deterministically selected NHS number alias.'), _sf('RELATED_PERSON_ID', LongType(), 'Related family-member PERSON_ID; zero may represent an unnamed relative.'), _sf('RELATION_CD', DoubleType(), 'Person relationship code from PERSON_PERSON_RELTN.'), _sf('RELATION_DESC', StringType(), 'Current code-value display/description for RELATION_CD.'), _sf('RELATION_TYPE_CD', DoubleType(), 'Relationship category code from PERSON_PERSON_RELTN.'), _sf('RELATION_TYPE_DESC', StringType(), 'Current code-value display/description for RELATION_TYPE_CD.'), _sf('GENETIC_IND', IntegerType(), 'Whether the relationship is genetic.'), _sf('PERSON_PERSON_RELTN_ID', LongType(), 'Selected source PERSON_PERSON_RELTN row identifier.'), _sf('NOMENCLATURE_ID', LongType(), 'Source nomenclature identifier.'), _sf('CONDITION_DESC_CODED', StringType(), 'Current coded source term from nomenclature.'), _sf('CONDITION_DESC_FREETEXT', StringType(), 'Latest family-history comment retained for compatibility.'), _sf('CONDITION_DESC', StringType(), 'Coded condition term, falling back to the latest free-text comment.'), _sf('CONDITION_SOURCE', StringType(), 'BOTH, CODED, FREE_TEXT, or NONE based on populated source values.'), _sf('SOURCE_IDENTIFIER', StringType(), 'Identifier supplied by the source terminology.'), _sf('SOURCE_VOCABULARY_CD', DoubleType(), 'Source vocabulary code.'), _sf('SOURCE_VOCABULARY_DESC', StringType(), 'Current source vocabulary display/description.'), _sf('VOCAB_AXIS_CD', DoubleType(), 'Vocabulary axis code.'), _sf('VOCAB_AXIS_DESC', StringType(), 'Current vocabulary axis display/description.'), _sf('CONCEPT_CKI', StringType(), 'Parsed CKI identifier after the final ! delimiter.'), _sf('OMOP_CONCEPT_ID', IntegerType(), 'Current OMOP concept mapping.'), _sf('OMOP_CONCEPT_NAME', StringType(), 'Current OMOP concept name.'), _sf('OMOP_STANDARD_CONCEPT', StringType(), 'OMOP standard-concept flag.'), _sf('OMOP_MATCH_NUMBER', LongType(), 'Number of OMOP mapping candidates.'), _sf('OMOP_SIMILARITY', DoubleType(), 'OMOP similarity score.'), _sf('OMOP_CONCEPT_DOMAIN', StringType(), 'OMOP concept domain.'), _sf('SNOMED_CODE', LongType(), 'Current SNOMED CT code.'), _sf('SNOMED_TYPE', StringType(), 'SNOMED mapping method.'), _sf('SNOMED_MATCH_NUMBER', LongType(), 'Number of SNOMED mapping candidates.'), _sf('SNOMED_SIMILARITY', DoubleType(), 'SNOMED similarity score.'), _sf('SNOMED_TERM', StringType(), 'Current SNOMED term.'), _sf('ICD10_CODE', StringType(), 'Current ICD-10 code.'), _sf('ICD10_TYPE', StringType(), 'ICD-10 mapping method.'), _sf('ICD10_MATCH_NUMBER', LongType(), 'Number of ICD-10 mapping candidates.'), _sf('ICD10_SIMILARITY', DoubleType(), 'ICD-10 similarity score.'), _sf('ICD10_TERM', StringType(), 'Current ICD-10 term.'), _sf('FHX_TYPE', StringType(), 'PERSON, RELTN, or CONDITION source row type.'), _sf('FHX_VALUE_FLAG', IntegerType(), 'Family-history value flag from the source.'), _sf('ONSET_AGE', DoubleType(), 'Reported age at onset.'), _sf('ONSET_AGE_UNIT', StringType(), 'Decoded onset-age unit.'), _sf('SEVERITY', StringType(), 'Decoded severity.'), _sf('COURSE', StringType(), 'Decoded course/progression.'), _sf('LIFE_CYCLE_STATUS', StringType(), 'Decoded clinical lifecycle status.'), _sf('BEG_EFFECTIVE_DT_TM', TimestampType(), 'FHX_ACTIVITY effective start.'), _sf('END_EFFECTIVE_DT_TM', TimestampType(), 'FHX_ACTIVITY effective end.'), _sf('ADC_UPDT', TimestampType(), 'Greatest available update timestamp across every contributing source.'), _sf('FHX_ACTIVITY_GROUP_ID', LongType(), 'Stable revision-chain identifier for related FHX_ACTIVITY versions.'), _sf('ORGANIZATION_ID', LongType(), 'Source organization identifier.'), _sf('ORIGINATING_ENCNTR_ID', LongType(), 'Encounter in which the family-history record originated.'), _sf('TRUST', StringType(), 'Source trust classification; retained without filtering.'), _sf('FHX_ACTIVE_IND', IntegerType(), 'Raw FHX_ACTIVITY active indicator; history rows are not filtered out.'), _sf('FHX_ACTIVE_STATUS_CD', LongType(), 'Raw FHX_ACTIVITY row-status code.'), _sf('FHX_ACTIVE_STATUS_DESC', StringType(), 'Decoded FHX_ACTIVITY row status.'), _sf('FHX_ACTIVE_STATUS_DT_TM', TimestampType(), 'When the FHX_ACTIVITY row status was set.'), _sf('FHX_UPDT_DT_TM', TimestampType(), 'Source-system FHX_ACTIVITY update timestamp.'), _sf('FHX_UPDT_CNT', LongType(), 'Source-system FHX_ACTIVITY update counter.'), _sf('FHX_UPDT_ID', LongType(), 'Personnel identifier responsible for the source update.'), _sf('FHX_LAST_UTC_TS', TimestampType(), 'Source last-UTC timestamp.'), _sf('FHX_VALUE_DESC', StringType(), 'Decoded FHX_VALUE_FLAG meaning.'), _sf('ONSET_AGE_PREC_CD', LongType(), 'Raw onset-age precision code.'), _sf('ONSET_AGE_PREC_DESC', StringType(), 'Decoded onset-age precision.'), _sf('ONSET_AGE_UNIT_CD', LongType(), 'Raw onset-age unit code.'), _sf('SEVERITY_CD', LongType(), 'Raw severity code.'), _sf('COURSE_CD', LongType(), 'Raw course code.'), _sf('LIFE_CYCLE_STATUS_CD', LongType(), 'Raw clinical lifecycle-status code.'), _sf('RELATED_PERSON_NAME', StringType(), 'Free-text related-person name from PERSON_PERSON_RELTN.'), _sf('RELATED_PERSON_RELTN_CD', LongType(), 'Reciprocal relationship code when populated.'), _sf('RELATED_PERSON_RELTN_DESC', StringType(), 'Decoded reciprocal relationship.'), _sf('FAMILY_RELTN_SUB_TYPE_CD', LongType(), 'Specific family-relationship subtype code.'), _sf('FAMILY_RELTN_SUB_TYPE_DESC', StringType(), 'Decoded family-relationship subtype.'), _sf('RELATION_PRIORITY_SEQ', LongType(), 'Source relationship priority sequence.'), _sf('PPR_ACTIVE_IND', IntegerType(), 'Selected PERSON_PERSON_RELTN active indicator.'), _sf('PPR_DATA_STATUS_CD', LongType(), 'Selected relationship data-status code.'), _sf('PPR_DATA_STATUS_DESC', StringType(), 'Decoded relationship data status.'), _sf('PPR_BEG_EFFECTIVE_DT_TM', TimestampType(), 'Selected relationship effective start.'), _sf('PPR_END_EFFECTIVE_DT_TM', TimestampType(), 'Selected relationship effective end.'), _sf('PPR_UPDT_DT_TM', TimestampType(), 'Source-system relationship update timestamp.'), _sf('FHX_LONG_TEXT_R_ID', LongType(), 'Latest family-history-to-long-text link identifier.'), _sf('LONG_TEXT_ID', LongType(), 'Latest linked LONG_TEXT identifier.'), _sf('CONDITION_COMMENT', StringType(), 'Latest family-history comment.'), _sf('COMMENT_DT_TM', TimestampType(), 'Latest family-history comment timestamp.'), _sf('COMMENT_PRSNL_ID', LongType(), 'Personnel identifier associated with the latest comment.'), _sf('CONDITION_COMMENT_COUNT', LongType(), 'Number of linked comments retained in CONDITION_COMMENTS.'), _sf('CONDITION_COMMENTS', ArrayType(family_history_comment_struct), 'All linked family-history comments and provenance.'), _sf('MRN_GENERIC', StringType(), 'Latest deterministic alias in the generic MRN pool.'), _sf('MRN_RF4', StringType(), 'Latest deterministic RF4 MRN alias.'), _sf('MRN_RNJ_5C4', StringType(), 'Latest deterministic RNJ 5C4 MRN alias.'), _sf('CMRN_5C4', StringType(), 'Latest deterministic 5C4 CMRN alias.'), _sf('MRN_PERSON_ALIAS_ID', LongType(), 'Source PERSON_ALIAS_ID selected for MRN.'), _sf('MRN_ALIAS_POOL_CD', LongType(), 'Alias pool selected for MRN.'), _sf('MRN_ALIAS_POOL_DESC', StringType(), 'Description of the selected MRN alias pool.'), _sf('MRN_BEG_EFFECTIVE_DT_TM', TimestampType(), 'Selected MRN effective start.'), _sf('MRN_END_EFFECTIVE_DT_TM', TimestampType(), 'Selected MRN effective end.'), _sf('NHS_PERSON_ALIAS_ID', LongType(), 'Source PERSON_ALIAS_ID selected for NHS number.'), _sf('NHS_ALIAS_POOL_CD', LongType(), 'Alias pool selected for NHS number.'), _sf('NHS_ALIAS_POOL_DESC', StringType(), 'Description of the selected NHS alias pool.'), _sf('NHS_BEG_EFFECTIVE_DT_TM', TimestampType(), 'Selected NHS-number effective start.'), _sf('NHS_END_EFFECTIVE_DT_TM', TimestampType(), 'Selected NHS-number effective end.'), _sf('MRN_ALIASES', ArrayType(family_history_alias_struct), 'All relevant MRN aliases, including inactive history.'), _sf('NHS_ALIASES', ArrayType(family_history_alias_struct), 'All NHS-number aliases, including inactive history.'), _sf('CONCEPT_CKI_RAW', StringType(), 'Unmodified nomenclature CONCEPT_CKI.'), _sf('CONCEPT_CLASS', StringType(), 'OMOP/source concept class from nomenclature.'), _sf('FOUND_CUI', StringType(), 'Matched UMLS CUI where available.'), _sf('NOMENCLATURE_IS_ACTIVE', BooleanType(), 'Current nomenclature active flag.'), _sf('NOMENCLATURE_SOURCE_CHANGE_TS', TimestampType(), 'Nomenclature source-change timestamp.'), _sf('OPCS4_CODE', StringType(), 'Current OPCS-4 mapping.'), _sf('OPCS4_TYPE', StringType(), 'OPCS-4 mapping method.'), _sf('OPCS4_MATCH_NUMBER', LongType(), 'Number of OPCS-4 mapping candidates.'), _sf('OPCS4_SIMILARITY', DoubleType(), 'OPCS-4 similarity score.'), _sf('OPCS4_TERM', StringType(), 'Current OPCS-4 term.'), _sf('FHX_ADC_UPDT', TimestampType(), 'FHX_ACTIVITY ingestion/update timestamp.'), _sf('PPR_ADC_UPDT', TimestampType(), 'PERSON_PERSON_RELTN ingestion/update timestamp.'), _sf('NOMENCLATURE_ADC_UPDT', TimestampType(), 'Nomenclature update timestamp.'), _sf('FHX_LONG_TEXT_ADC_UPDT', TimestampType(), 'FHX_LONG_TEXT_R update timestamp.'), _sf('LONG_TEXT_ADC_UPDT', TimestampType(), 'LONG_TEXT update timestamp.'), _sf('MRN_ADC_UPDT', TimestampType(), 'Selected MRN alias update timestamp.'), _sf('NHS_ADC_UPDT', TimestampType(), 'Selected NHS alias update timestamp.'), _sf('LOOKUP_ADC_UPDT', TimestampType(), 'Latest code-value update contributing decoded text.'), _sf('MAPPED_AT', TimestampType(), 'Timestamp when this semantic row version was last materialized.'), _sf('ROW_HASH', StringType(), 'SHA-256 hash used to avoid rewriting unchanged matched rows.')])
map_family_history_comment = '\nBronze family-history foundation at one row per source FHX_ACTIVITY_ID. All source revisions,\nincluding inactive records, are retained and linked by FHX_ACTIVITY_GROUP_ID. The table preserves\nraw clinical/status codes, relationship and alias provenance, every family-history comment, current\nnomenclature mappings, and per-source freshness timestamps. map_family_history_current exposes the\ncurrent active revision without filtering this bronze history table.\n'

def create_family_history_code_lookup(code_values: DataFrame, alias_suffix: str) -> DataFrame:
    """Backward-compatible lookup helper with a display-to-description fallback."""
    return code_values.select(F.col('CODE_VALUE').cast('long').alias('CODE_VALUE'), F.col('DISPLAY').alias(f'{alias_suffix}_display'), F.col('DESCRIPTION').alias(f'{alias_suffix}_description'), F.coalesce(F.col('DISPLAY'), F.col('DESCRIPTION')).alias(f'{alias_suffix}_text'), F.col('ADC_UPDT').alias(f'{alias_suffix}_adc_updt')).dropDuplicates(['CODE_VALUE']).alias(alias_suffix)

def _build_family_history_base() -> DataFrame:
    return spark.table(MFH_FHX_SOURCE).select(F.col('FHX_ACTIVITY_ID').cast('long').alias('FHX_ACTIVITY_ID'), F.col('FHX_ACTIVITY_GROUP_ID').cast('long').alias('FHX_ACTIVITY_GROUP_ID'), F.col('PERSON_ID').cast('long').alias('PERSON_ID'), F.col('RELATED_PERSON_ID').cast('long').alias('RELATED_PERSON_ID'), F.col('NOMENCLATURE_ID').cast('long').alias('NOMENCLATURE_ID'), F.col('TYPE_MEAN').alias('FHX_TYPE'), F.col('FHX_VALUE_FLAG').cast('int').alias('FHX_VALUE_FLAG'), F.col('ONSET_AGE_PREC_CD').cast('long').alias('ONSET_AGE_PREC_CD'), F.col('ONSET_AGE').cast('double').alias('ONSET_AGE'), F.col('ONSET_AGE_UNIT_CD').cast('long').alias('ONSET_AGE_UNIT_CD'), F.col('LIFE_CYCLE_STATUS_CD').cast('long').alias('LIFE_CYCLE_STATUS_CD'), F.col('SEVERITY_CD').cast('long').alias('SEVERITY_CD'), F.col('COURSE_CD').cast('long').alias('COURSE_CD'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('BEG_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('END_EFFECTIVE_DT_TM'), F.col('ACTIVE_IND').cast('int').alias('FHX_ACTIVE_IND'), F.col('ACTIVE_STATUS_CD').cast('long').alias('FHX_ACTIVE_STATUS_CD'), F.col('ACTIVE_STATUS_DT_TM').cast('timestamp').alias('FHX_ACTIVE_STATUS_DT_TM'), F.col('UPDT_DT_TM').cast('timestamp').alias('FHX_UPDT_DT_TM'), F.col('UPDT_CNT').cast('long').alias('FHX_UPDT_CNT'), F.col('UPDT_ID').cast('long').alias('FHX_UPDT_ID'), F.col('LAST_UTC_TS').cast('timestamp').alias('FHX_LAST_UTC_TS'), F.col('ORGANIZATION_ID').cast('long').alias('ORGANIZATION_ID'), F.col('ORIGINATING_ENCNTR_ID').cast('long').alias('ORIGINATING_ENCNTR_ID'), F.col('Trust').cast('string').alias('TRUST'), F.col('ADC_UPDT').cast('timestamp').alias('FHX_ADC_UPDT'))

def _build_relationship_lookup(fhx: DataFrame) -> DataFrame:
    relationship_keys = fhx.where(F.col('RELATED_PERSON_ID').isNotNull() & (F.col('RELATED_PERSON_ID') != 0)).select('PERSON_ID', 'RELATED_PERSON_ID').distinct()
    relationships = spark.table(MFH_RELATION_SOURCE).select(F.col('PERSON_PERSON_RELTN_ID').cast('long').alias('PERSON_PERSON_RELTN_ID'), F.col('PERSON_ID').cast('long').alias('PERSON_ID'), F.col('RELATED_PERSON_ID').cast('long').alias('RELATED_PERSON_ID'), F.col('PERSON_RELTN_CD').cast('long').alias('PPR_PERSON_RELTN_CD'), F.col('PERSON_RELTN_TYPE_CD').cast('long').alias('PPR_PERSON_RELTN_TYPE_CD'), F.col('RELATED_PERSON_RELTN_CD').cast('long').alias('RELATED_PERSON_RELTN_CD'), F.col('GENETIC_RELATIONSHIP_IND').cast('int').alias('GENETIC_IND'), F.col('FT_REL_PERSON_NAME').cast('string').alias('RELATED_PERSON_NAME'), F.col('FAMILY_RELTN_SUB_TYPE_CD').cast('long').alias('FAMILY_RELTN_SUB_TYPE_CD'), F.col('PRIORITY_SEQ').cast('long').alias('RELATION_PRIORITY_SEQ'), F.col('ACTIVE_IND').cast('int').alias('PPR_ACTIVE_IND'), F.col('DATA_STATUS_CD').cast('long').alias('PPR_DATA_STATUS_CD'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('PPR_BEG_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('PPR_END_EFFECTIVE_DT_TM'), F.col('UPDT_DT_TM').cast('timestamp').alias('PPR_UPDT_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('PPR_ADC_UPDT')).join(F.broadcast(relationship_keys), ['PERSON_ID', 'RELATED_PERSON_ID'], 'inner')
    f = fhx.select('FHX_ACTIVITY_ID', 'PERSON_ID', 'RELATED_PERSON_ID', 'BEG_EFFECTIVE_DT_TM').alias('f')
    p = relationships.alias('p')
    joined = f.join(p, (F.col('f.PERSON_ID') == F.col('p.PERSON_ID')) & (F.col('f.RELATED_PERSON_ID') == F.col('p.RELATED_PERSON_ID')), 'left')
    temporal_match = F.col('p.PPR_BEG_EFFECTIVE_DT_TM').isNull() | (F.col('p.PPR_BEG_EFFECTIVE_DT_TM') <= F.col('f.BEG_EFFECTIVE_DT_TM')) & (F.col('p.PPR_END_EFFECTIVE_DT_TM').isNull() | (F.col('p.PPR_END_EFFECTIVE_DT_TM') > F.col('f.BEG_EFFECTIVE_DT_TM')))
    candidates = joined.select(F.col('f.FHX_ACTIVITY_ID').alias('FHX_ACTIVITY_ID'), F.when(temporal_match, F.lit(1)).otherwise(F.lit(0)).alias('_TEMPORAL_MATCH'), *[F.col(f'p.{column}') for column in relationships.columns if column not in {'PERSON_ID', 'RELATED_PERSON_ID'}])
    ranking = Window.partitionBy('FHX_ACTIVITY_ID').orderBy(F.col('_TEMPORAL_MATCH').desc(), F.col('PPR_ACTIVE_IND').desc_nulls_last(), F.col('PPR_BEG_EFFECTIVE_DT_TM').desc_nulls_last(), F.col('PPR_ADC_UPDT').desc_nulls_last(), F.col('PERSON_PERSON_RELTN_ID').desc_nulls_last())
    return candidates.withColumn('_RN', F.row_number().over(ranking)).where(F.col('_RN') == 1).drop('_RN', '_TEMPORAL_MATCH')

def _build_nomenclature_lookup(fhx: DataFrame) -> DataFrame:
    needed_ids = fhx.where(F.col('NOMENCLATURE_ID').isNotNull() & (F.col('NOMENCLATURE_ID') != 0)).select('NOMENCLATURE_ID').distinct()
    nomenclature = spark.table(MFH_NOMENCLATURE_SOURCE).select(F.col('NOMENCLATURE_ID').cast('long').alias('NOMENCLATURE_ID'), F.col('SOURCE_IDENTIFIER'), F.col('SOURCE_STRING'), F.col('SOURCE_VOCABULARY_CD').cast('long').alias('NOM_SOURCE_VOCABULARY_CD'), F.col('VOCAB_AXIS_CD').cast('long').alias('NOM_VOCAB_AXIS_CD'), F.col('IS_STANDARD_OMOP_CONCEPT'), F.col('CONCEPT_DOMAIN'), F.col('CONCEPT_CLASS'), F.col('FOUND_CUI'), F.col('CONCEPT_CKI').alias('CONCEPT_CKI_RAW'), F.substring_index(F.col('CONCEPT_CKI'), '!', -1).alias('CONCEPT_CKI_PROCESSED'), F.col('SNOMED_CODE').cast('long').alias('SNOMED_CODE'), F.col('SNOMED_TYPE'), F.col('SNOMED_MATCH_COUNT').cast('long').alias('SNOMED_MATCH_COUNT'), F.col('SNOMED_SIMILARITY').cast('double').alias('SNOMED_SIMILARITY'), F.col('SNOMED_TERM'), F.col('ICD10_CODE'), F.col('ICD10_CODE_TYPE'), F.col('ICD10_CODE_MATCH_COUNT').cast('long').alias('ICD10_CODE_MATCH_COUNT'), F.col('ICD10_SIMILARITY').cast('double').alias('ICD10_SIMILARITY'), F.col('ICD10_TERM'), F.col('OPCS4_CODE'), F.col('OPCS4_CODE_TYPE'), F.col('OPCS4_CODE_MATCH_COUNT').cast('long').alias('OPCS4_CODE_MATCH_COUNT'), F.col('OPCS4_SIMILARITY').cast('double').alias('OPCS4_SIMILARITY'), F.col('OPCS4_TERM'), F.col('OMOP_CONCEPT_ID').cast('int').alias('OMOP_CONCEPT_ID'), F.col('OMOP_CONCEPT_NAME'), F.col('NUMBER_OF_OMOP_MATCHES').cast('long').alias('NUMBER_OF_OMOP_MATCHES'), F.col('OMOP_SIMILARITY').cast('double').alias('OMOP_SIMILARITY'), F.col('SOURCE_CHANGE_TS').cast('timestamp').alias('NOMENCLATURE_SOURCE_CHANGE_TS'), F.col('IS_ACTIVE').cast('boolean').alias('NOMENCLATURE_IS_ACTIVE'), F.col('ADC_UPDT').cast('timestamp').alias('NOMENCLATURE_ADC_UPDT')).join(F.broadcast(needed_ids), 'NOMENCLATURE_ID', 'inner')
    ranking = Window.partitionBy('NOMENCLATURE_ID').orderBy(F.col('NOMENCLATURE_ADC_UPDT').desc_nulls_last(), F.col('NOMENCLATURE_SOURCE_CHANGE_TS').desc_nulls_last())
    return nomenclature.withColumn('_RN', F.row_number().over(ranking)).where(F.col('_RN') == 1).drop('_RN')

def _build_comment_lookup(fhx: DataFrame) -> DataFrame:
    fhx_ids = fhx.select('FHX_ACTIVITY_ID').distinct()
    links = spark.table(MFH_TEXT_LINK_SOURCE).select(F.col('FHX_ACTIVITY_ID').cast('long').alias('FHX_ACTIVITY_ID'), F.col('FHX_LONG_TEXT_R_ID').cast('long').alias('FHX_LONG_TEXT_R_ID'), F.col('LONG_TEXT_ID').cast('long').alias('LONG_TEXT_ID'), F.col('COMMENT_PRSNL_ID').cast('long').alias('COMMENT_PRSNL_ID'), F.col('COMMENT_DT_TM').cast('timestamp').alias('COMMENT_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('FHX_LONG_TEXT_ADC_UPDT')).join(F.broadcast(fhx_ids), 'FHX_ACTIVITY_ID', 'inner')
    text_ids = links.select('LONG_TEXT_ID').where(F.col('LONG_TEXT_ID').isNotNull()).distinct()
    long_text = spark.table(MFH_LONG_TEXT_SOURCE).where(F.col('PARENT_ENTITY_NAME') == 'FHX_ACTIVITY').select(F.col('LONG_TEXT_ID').cast('long').alias('LONG_TEXT_ID'), F.col('LONG_TEXT').cast('string').alias('LONG_TEXT'), F.col('ACTIVE_IND').cast('int').alias('LONG_TEXT_ACTIVE_IND'), F.col('ADC_UPDT').cast('timestamp').alias('LONG_TEXT_ADC_UPDT')).join(F.broadcast(text_ids), 'LONG_TEXT_ID', 'inner')
    comments = links.join(long_text, 'LONG_TEXT_ID', 'left')
    comment_struct = F.struct(F.col('COMMENT_DT_TM').alias('COMMENT_DT_TM'), F.col('FHX_LONG_TEXT_R_ID').alias('FHX_LONG_TEXT_R_ID'), F.col('LONG_TEXT_ID').alias('LONG_TEXT_ID'), F.col('COMMENT_PRSNL_ID').alias('COMMENT_PRSNL_ID'), F.col('LONG_TEXT').alias('LONG_TEXT'), F.col('FHX_LONG_TEXT_ADC_UPDT').alias('LINK_ADC_UPDT'), F.col('LONG_TEXT_ADC_UPDT').alias('LONG_TEXT_ADC_UPDT'), F.col('LONG_TEXT_ACTIVE_IND').alias('LONG_TEXT_ACTIVE_IND'))
    aggregated = comments.groupBy('FHX_ACTIVITY_ID').agg(F.sort_array(F.collect_list(comment_struct), False).alias('CONDITION_COMMENTS'), F.count(F.lit(1)).cast('long').alias('CONDITION_COMMENT_COUNT'), F.max('FHX_LONG_TEXT_ADC_UPDT').alias('FHX_LONG_TEXT_ADC_UPDT'), F.max('LONG_TEXT_ADC_UPDT').alias('LONG_TEXT_ADC_UPDT'))
    return aggregated.withColumn('_LATEST_COMMENT', F.element_at('CONDITION_COMMENTS', 1)).select('FHX_ACTIVITY_ID', F.col('_LATEST_COMMENT.FHX_LONG_TEXT_R_ID').alias('FHX_LONG_TEXT_R_ID'), F.col('_LATEST_COMMENT.LONG_TEXT_ID').alias('LONG_TEXT_ID'), F.col('_LATEST_COMMENT.LONG_TEXT').alias('CONDITION_COMMENT'), F.col('_LATEST_COMMENT.COMMENT_DT_TM').alias('COMMENT_DT_TM'), F.col('_LATEST_COMMENT.COMMENT_PRSNL_ID').alias('COMMENT_PRSNL_ID'), 'CONDITION_COMMENT_COUNT', 'CONDITION_COMMENTS', 'FHX_LONG_TEXT_ADC_UPDT', 'LONG_TEXT_ADC_UPDT')

def _mrn_pool_priority_expression():
    expression = F.lit(999)
    for pool_code, priority in sorted(MRN_POOL_PRIORITY.items(), key=lambda item: item[1], reverse=True):
        expression = F.when(F.col('ALIAS_POOL_CD') == pool_code, F.lit(priority)).otherwise(expression)
    return expression

def _build_alias_lookup(fhx: DataFrame) -> DataFrame:
    person_ids = fhx.select('PERSON_ID').where(F.col('PERSON_ID').isNotNull()).distinct()
    mrn_pools = list(MRN_POOL_PRIORITY)
    aliases = spark.table(MFH_ALIAS_SOURCE).where((F.col('PERSON_ALIAS_TYPE_CD') == 18) | ((F.col('PERSON_ALIAS_TYPE_CD') == 10) & F.col('ALIAS_POOL_CD').isin(mrn_pools))).select(F.col('PERSON_ID').cast('long').alias('PERSON_ID'), F.col('PERSON_ALIAS_ID').cast('long').alias('PERSON_ALIAS_ID'), F.col('PERSON_ALIAS_TYPE_CD').cast('long').alias('PERSON_ALIAS_TYPE_CD'), F.col('ALIAS_POOL_CD').cast('long').alias('ALIAS_POOL_CD'), F.col('ALIAS').cast('string').alias('ALIAS'), F.col('ACTIVE_IND').cast('int').alias('ACTIVE_IND'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('BEG_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('END_EFFECTIVE_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('ADC_UPDT')).join(F.broadcast(person_ids), 'PERSON_ID', 'inner').withColumn('_IS_CURRENT', F.when((F.col('ACTIVE_IND') == 1) & (F.col('BEG_EFFECTIVE_DT_TM').isNull() | (F.col('BEG_EFFECTIVE_DT_TM') <= F.current_timestamp())) & (F.col('END_EFFECTIVE_DT_TM').isNull() | (F.col('END_EFFECTIVE_DT_TM') > F.current_timestamp())), F.lit(1)).otherwise(F.lit(0))).withColumn('_POOL_PRIORITY', _mrn_pool_priority_expression())
    alias_struct = F.struct(F.col('BEG_EFFECTIVE_DT_TM').alias('BEG_EFFECTIVE_DT_TM'), F.col('ADC_UPDT').alias('ADC_UPDT'), F.col('PERSON_ALIAS_ID').alias('PERSON_ALIAS_ID'), F.col('PERSON_ALIAS_TYPE_CD').alias('PERSON_ALIAS_TYPE_CD'), F.col('ALIAS_POOL_CD').alias('ALIAS_POOL_CD'), F.col('ALIAS').alias('ALIAS'), F.col('ACTIVE_IND').alias('ACTIVE_IND'), F.col('END_EFFECTIVE_DT_TM').alias('END_EFFECTIVE_DT_TM'))
    mrn_arrays = aliases.where(F.col('PERSON_ALIAS_TYPE_CD') == 10).groupBy('PERSON_ID').agg(F.sort_array(F.collect_list(alias_struct), False).alias('MRN_ALIASES'))
    nhs_arrays = aliases.where(F.col('PERSON_ALIAS_TYPE_CD') == 18).groupBy('PERSON_ID').agg(F.sort_array(F.collect_list(alias_struct), False).alias('NHS_ALIASES'))
    mrn_ranking = Window.partitionBy('PERSON_ID').orderBy(F.col('_IS_CURRENT').desc(), F.col('ACTIVE_IND').desc_nulls_last(), F.col('_POOL_PRIORITY').asc(), F.col('BEG_EFFECTIVE_DT_TM').desc_nulls_last(), F.col('ADC_UPDT').desc_nulls_last(), F.col('PERSON_ALIAS_ID').desc_nulls_last())
    selected_mrn = aliases.where(F.col('PERSON_ALIAS_TYPE_CD') == 10).withColumn('_RN', F.row_number().over(mrn_ranking)).where(F.col('_RN') == 1).select('PERSON_ID', F.col('ALIAS').alias('MRN'), F.col('PERSON_ALIAS_ID').alias('MRN_PERSON_ALIAS_ID'), F.col('ALIAS_POOL_CD').alias('MRN_ALIAS_POOL_CD'), F.col('BEG_EFFECTIVE_DT_TM').alias('MRN_BEG_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM').alias('MRN_END_EFFECTIVE_DT_TM'), F.col('ADC_UPDT').alias('MRN_ADC_UPDT'))
    pool_ranking = Window.partitionBy('PERSON_ID', 'ALIAS_POOL_CD').orderBy(F.col('_IS_CURRENT').desc(), F.col('ACTIVE_IND').desc_nulls_last(), F.col('BEG_EFFECTIVE_DT_TM').desc_nulls_last(), F.col('ADC_UPDT').desc_nulls_last(), F.col('PERSON_ALIAS_ID').desc_nulls_last())
    latest_per_pool = aliases.where(F.col('PERSON_ALIAS_TYPE_CD') == 10).withColumn('_RN', F.row_number().over(pool_ranking)).where(F.col('_RN') == 1)
    mrn_by_pool = latest_per_pool.groupBy('PERSON_ID').agg(F.max(F.when(F.col('ALIAS_POOL_CD') == 683996, F.col('ALIAS'))).alias('MRN_GENERIC'), F.max(F.when(F.col('ALIAS_POOL_CD') == 1115132483, F.col('ALIAS'))).alias('MRN_RF4'), F.max(F.when(F.col('ALIAS_POOL_CD') == 6200990, F.col('ALIAS'))).alias('MRN_RNJ_5C4'), F.max(F.when(F.col('ALIAS_POOL_CD') == 6173940, F.col('ALIAS'))).alias('CMRN_5C4'))
    nhs_ranking = Window.partitionBy('PERSON_ID').orderBy(F.col('_IS_CURRENT').desc(), F.col('ACTIVE_IND').desc_nulls_last(), F.col('BEG_EFFECTIVE_DT_TM').desc_nulls_last(), F.col('ADC_UPDT').desc_nulls_last(), F.col('PERSON_ALIAS_ID').desc_nulls_last())
    selected_nhs = aliases.where(F.col('PERSON_ALIAS_TYPE_CD') == 18).withColumn('_RN', F.row_number().over(nhs_ranking)).where(F.col('_RN') == 1).select('PERSON_ID', F.col('ALIAS').alias('NHS_Number'), F.col('PERSON_ALIAS_ID').alias('NHS_PERSON_ALIAS_ID'), F.col('ALIAS_POOL_CD').alias('NHS_ALIAS_POOL_CD'), F.col('BEG_EFFECTIVE_DT_TM').alias('NHS_BEG_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM').alias('NHS_END_EFFECTIVE_DT_TM'), F.col('ADC_UPDT').alias('NHS_ADC_UPDT'))
    result = person_ids
    for lookup in [selected_mrn, mrn_by_pool, selected_nhs, mrn_arrays, nhs_arrays]:
        result = result.join(lookup, 'PERSON_ID', 'left')
    return result.withColumn('MRN_ALIAS_POOL_DESC', F.when(F.col('MRN_ALIAS_POOL_CD') == 683996, F.lit(MRN_POOL_LABELS[683996])).when(F.col('MRN_ALIAS_POOL_CD') == 1115132483, F.lit(MRN_POOL_LABELS[1115132483])).when(F.col('MRN_ALIAS_POOL_CD') == 6200990, F.lit(MRN_POOL_LABELS[6200990])).when(F.col('MRN_ALIAS_POOL_CD') == 6173940, F.lit(MRN_POOL_LABELS[6173940])))

def _union_code_columns(dataframes_and_columns) -> DataFrame:
    frames = [dataframe.select(F.col(column).cast('long').alias('CODE_VALUE')) for dataframe, column in dataframes_and_columns]
    return reduce(lambda left, right: left.unionByName(right), frames).where(F.col('CODE_VALUE').isNotNull() & (F.col('CODE_VALUE') != 0)).distinct()

def _build_code_lookup(fhx: DataFrame, relationships: DataFrame, nomenclature: DataFrame, aliases: DataFrame) -> DataFrame:
    used_codes = _union_code_columns([(fhx, 'FHX_ACTIVE_STATUS_CD'), (fhx, 'ONSET_AGE_PREC_CD'), (fhx, 'ONSET_AGE_UNIT_CD'), (fhx, 'SEVERITY_CD'), (fhx, 'COURSE_CD'), (fhx, 'LIFE_CYCLE_STATUS_CD'), (relationships, 'PPR_PERSON_RELTN_CD'), (relationships, 'PPR_PERSON_RELTN_TYPE_CD'), (relationships, 'RELATED_PERSON_RELTN_CD'), (relationships, 'FAMILY_RELTN_SUB_TYPE_CD'), (relationships, 'PPR_DATA_STATUS_CD'), (nomenclature, 'NOM_SOURCE_VOCABULARY_CD'), (nomenclature, 'NOM_VOCAB_AXIS_CD'), (aliases, 'NHS_ALIAS_POOL_CD')])
    return spark.table(MFH_CODE_VALUE_SOURCE).select(F.col('CODE_VALUE').cast('long').alias('CODE_VALUE'), F.coalesce(F.col('DISPLAY'), F.col('DESCRIPTION')).alias('CODE_TEXT'), F.col('ADC_UPDT').cast('timestamp').alias('CODE_ADC_UPDT')).join(F.broadcast(used_codes), 'CODE_VALUE', 'inner').dropDuplicates(['CODE_VALUE'])

def _join_code_description(dataframe: DataFrame, code_lookup: DataFrame, code_column: str, prefix: str) -> DataFrame:
    code_key = f'_{prefix}_CODE_VALUE'
    lookup = code_lookup.select(F.col('CODE_VALUE').alias(code_key), F.col('CODE_TEXT').alias(f'{prefix}_DECODE'), F.col('CODE_ADC_UPDT').alias(f'{prefix}_CODE_ADC_UPDT'))
    return dataframe.join(F.broadcast(lookup), F.col(code_column).cast('long') == F.col(code_key), 'left').drop(code_key)

def _nonblank(column):
    return column.isNotNull() & (F.length(F.trim(column)) > 0)

def create_family_history_mapping_incr() -> DataFrame:
    """
    Compatibility replacement for the old incremental builder.

    It returns a complete, one-row-per-FHX_ACTIVITY_ID snapshot so changes in nomenclature,
    aliases, relationships, comments, code values, inactive revisions, and source hard deletes
    are all reconciled on every run.
    """
    print('[INFO] Building complete family-history bronze snapshot.')
    fhx = _build_family_history_base()
    relationships = _build_relationship_lookup(fhx)
    nomenclature = _build_nomenclature_lookup(fhx)
    comments = _build_comment_lookup(fhx)
    aliases = _build_alias_lookup(fhx)
    code_lookup = _build_code_lookup(fhx, relationships, nomenclature, aliases)
    enriched = fhx.join(relationships, 'FHX_ACTIVITY_ID', 'left').join(nomenclature, 'NOMENCLATURE_ID', 'left').join(comments, 'FHX_ACTIVITY_ID', 'left').join(aliases, 'PERSON_ID', 'left')
    decode_columns = [('PPR_PERSON_RELTN_CD', 'RELATION'), ('PPR_PERSON_RELTN_TYPE_CD', 'RELATION_TYPE'), ('RELATED_PERSON_RELTN_CD', 'RELATED_PERSON_RELTN'), ('FAMILY_RELTN_SUB_TYPE_CD', 'FAMILY_RELTN_SUB_TYPE'), ('PPR_DATA_STATUS_CD', 'PPR_DATA_STATUS'), ('NOM_SOURCE_VOCABULARY_CD', 'SOURCE_VOCABULARY'), ('NOM_VOCAB_AXIS_CD', 'VOCAB_AXIS'), ('ONSET_AGE_PREC_CD', 'ONSET_AGE_PREC'), ('ONSET_AGE_UNIT_CD', 'ONSET_AGE_UNIT'), ('SEVERITY_CD', 'SEVERITY'), ('COURSE_CD', 'COURSE'), ('LIFE_CYCLE_STATUS_CD', 'LIFECYCLE'), ('FHX_ACTIVE_STATUS_CD', 'FHX_ACTIVE_STATUS'), ('NHS_ALIAS_POOL_CD', 'NHS_ALIAS_POOL')]
    for code_column, prefix in decode_columns:
        enriched = _join_code_description(enriched, code_lookup, code_column, prefix)
    coded_value = F.when(_nonblank(F.col('SOURCE_STRING')), F.col('SOURCE_STRING'))
    free_text_value = F.when(_nonblank(F.col('CONDITION_COMMENT')), F.col('CONDITION_COMMENT'))
    coded_present = coded_value.isNotNull()
    free_text_present = free_text_value.isNotNull()
    lookup_update_columns = [F.col(f'{prefix}_CODE_ADC_UPDT') for _, prefix in decode_columns]
    result = enriched.select('FHX_ACTIVITY_ID', 'PERSON_ID', 'MRN', 'NHS_Number', 'RELATED_PERSON_ID', F.col('PPR_PERSON_RELTN_CD').cast('double').alias('RELATION_CD'), F.col('RELATION_DECODE').alias('RELATION_DESC'), F.col('PPR_PERSON_RELTN_TYPE_CD').cast('double').alias('RELATION_TYPE_CD'), F.col('RELATION_TYPE_DECODE').alias('RELATION_TYPE_DESC'), 'GENETIC_IND', 'PERSON_PERSON_RELTN_ID', 'NOMENCLATURE_ID', F.col('SOURCE_STRING').alias('CONDITION_DESC_CODED'), F.col('CONDITION_COMMENT').alias('CONDITION_DESC_FREETEXT'), F.coalesce(coded_value, free_text_value).alias('CONDITION_DESC'), F.when(coded_present & free_text_present, F.lit('BOTH')).when(coded_present, F.lit('CODED')).when(free_text_present, F.lit('FREE_TEXT')).otherwise(F.lit('NONE')).alias('CONDITION_SOURCE'), 'SOURCE_IDENTIFIER', F.col('NOM_SOURCE_VOCABULARY_CD').cast('double').alias('SOURCE_VOCABULARY_CD'), F.col('SOURCE_VOCABULARY_DECODE').alias('SOURCE_VOCABULARY_DESC'), F.col('NOM_VOCAB_AXIS_CD').cast('double').alias('VOCAB_AXIS_CD'), F.col('VOCAB_AXIS_DECODE').alias('VOCAB_AXIS_DESC'), F.col('CONCEPT_CKI_PROCESSED').alias('CONCEPT_CKI'), 'OMOP_CONCEPT_ID', 'OMOP_CONCEPT_NAME', F.col('IS_STANDARD_OMOP_CONCEPT').alias('OMOP_STANDARD_CONCEPT'), F.col('NUMBER_OF_OMOP_MATCHES').alias('OMOP_MATCH_NUMBER'), 'OMOP_SIMILARITY', F.col('CONCEPT_DOMAIN').alias('OMOP_CONCEPT_DOMAIN'), 'SNOMED_CODE', 'SNOMED_TYPE', F.col('SNOMED_MATCH_COUNT').alias('SNOMED_MATCH_NUMBER'), 'SNOMED_SIMILARITY', 'SNOMED_TERM', 'ICD10_CODE', F.col('ICD10_CODE_TYPE').alias('ICD10_TYPE'), F.col('ICD10_CODE_MATCH_COUNT').alias('ICD10_MATCH_NUMBER'), 'ICD10_SIMILARITY', 'ICD10_TERM', 'FHX_TYPE', 'FHX_VALUE_FLAG', 'ONSET_AGE', F.col('ONSET_AGE_UNIT_DECODE').alias('ONSET_AGE_UNIT'), F.col('SEVERITY_DECODE').alias('SEVERITY'), F.col('COURSE_DECODE').alias('COURSE'), F.col('LIFECYCLE_DECODE').alias('LIFE_CYCLE_STATUS'), 'BEG_EFFECTIVE_DT_TM', 'END_EFFECTIVE_DT_TM', F.greatest(F.col('FHX_ADC_UPDT'), F.col('PPR_ADC_UPDT'), F.col('NOMENCLATURE_ADC_UPDT'), F.col('FHX_LONG_TEXT_ADC_UPDT'), F.col('LONG_TEXT_ADC_UPDT'), F.col('MRN_ADC_UPDT'), F.col('NHS_ADC_UPDT'), *lookup_update_columns).alias('ADC_UPDT'), 'FHX_ACTIVITY_GROUP_ID', 'ORGANIZATION_ID', 'ORIGINATING_ENCNTR_ID', 'TRUST', 'FHX_ACTIVE_IND', 'FHX_ACTIVE_STATUS_CD', F.col('FHX_ACTIVE_STATUS_DECODE').alias('FHX_ACTIVE_STATUS_DESC'), 'FHX_ACTIVE_STATUS_DT_TM', 'FHX_UPDT_DT_TM', 'FHX_UPDT_CNT', 'FHX_UPDT_ID', 'FHX_LAST_UTC_TS', F.when(F.col('FHX_VALUE_FLAG') == 0, F.lit('Negative')).when(F.col('FHX_VALUE_FLAG') == 1, F.lit('Positive')).when(F.col('FHX_VALUE_FLAG') == 2, F.lit('Unknown')).when(F.col('FHX_VALUE_FLAG') == 3, F.lit('Unable to Obtain')).when(F.col('FHX_VALUE_FLAG') == 4, F.lit('Patient Adopted')).alias('FHX_VALUE_DESC'), 'ONSET_AGE_PREC_CD', F.col('ONSET_AGE_PREC_DECODE').alias('ONSET_AGE_PREC_DESC'), 'ONSET_AGE_UNIT_CD', 'SEVERITY_CD', 'COURSE_CD', 'LIFE_CYCLE_STATUS_CD', 'RELATED_PERSON_NAME', 'RELATED_PERSON_RELTN_CD', F.col('RELATED_PERSON_RELTN_DECODE').alias('RELATED_PERSON_RELTN_DESC'), 'FAMILY_RELTN_SUB_TYPE_CD', F.col('FAMILY_RELTN_SUB_TYPE_DECODE').alias('FAMILY_RELTN_SUB_TYPE_DESC'), 'RELATION_PRIORITY_SEQ', 'PPR_ACTIVE_IND', 'PPR_DATA_STATUS_CD', F.col('PPR_DATA_STATUS_DECODE').alias('PPR_DATA_STATUS_DESC'), 'PPR_BEG_EFFECTIVE_DT_TM', 'PPR_END_EFFECTIVE_DT_TM', 'PPR_UPDT_DT_TM', 'FHX_LONG_TEXT_R_ID', 'LONG_TEXT_ID', 'CONDITION_COMMENT', 'COMMENT_DT_TM', 'COMMENT_PRSNL_ID', 'CONDITION_COMMENT_COUNT', 'CONDITION_COMMENTS', 'MRN_GENERIC', 'MRN_RF4', 'MRN_RNJ_5C4', 'CMRN_5C4', 'MRN_PERSON_ALIAS_ID', 'MRN_ALIAS_POOL_CD', 'MRN_ALIAS_POOL_DESC', 'MRN_BEG_EFFECTIVE_DT_TM', 'MRN_END_EFFECTIVE_DT_TM', 'NHS_PERSON_ALIAS_ID', 'NHS_ALIAS_POOL_CD', F.col('NHS_ALIAS_POOL_DECODE').alias('NHS_ALIAS_POOL_DESC'), 'NHS_BEG_EFFECTIVE_DT_TM', 'NHS_END_EFFECTIVE_DT_TM', 'MRN_ALIASES', 'NHS_ALIASES', 'CONCEPT_CKI_RAW', 'CONCEPT_CLASS', 'FOUND_CUI', 'NOMENCLATURE_IS_ACTIVE', 'NOMENCLATURE_SOURCE_CHANGE_TS', 'OPCS4_CODE', F.col('OPCS4_CODE_TYPE').alias('OPCS4_TYPE'), F.col('OPCS4_CODE_MATCH_COUNT').alias('OPCS4_MATCH_NUMBER'), 'OPCS4_SIMILARITY', 'OPCS4_TERM', 'FHX_ADC_UPDT', 'PPR_ADC_UPDT', 'NOMENCLATURE_ADC_UPDT', 'FHX_LONG_TEXT_ADC_UPDT', 'LONG_TEXT_ADC_UPDT', 'MRN_ADC_UPDT', 'NHS_ADC_UPDT', F.greatest(*lookup_update_columns).alias('LOOKUP_ADC_UPDT'), F.current_timestamp().alias('MAPPED_AT'))
    hash_columns = [field.name for field in schema_map_family_history.fields if field.name not in {'MAPPED_AT', 'ROW_HASH'}]
    result = result.withColumn('ROW_HASH', F.sha2(F.to_json(F.struct(*[F.col(column) for column in hash_columns]), options={'ignoreNullFields': 'false'}), 256))
    return _align_df_to_schema(result, schema_map_family_history)

def create_family_history_current_view():
    spark.sql(f"""
        CREATE OR REPLACE VIEW {_quote_table(MFH_CURRENT_VIEW)} AS
        SELECT * EXCEPT (_CURRENT_RN)
        FROM (
            SELECT
                source.*,
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(FHX_ACTIVITY_GROUP_ID, FHX_ACTIVITY_ID)
                    ORDER BY
                        BEG_EFFECTIVE_DT_TM DESC NULLS LAST,
                        FHX_UPDT_DT_TM DESC NULLS LAST,
                        FHX_ACTIVITY_ID DESC
                ) AS _CURRENT_RN
            FROM {_quote_table(MFH_TARGET)} source
            WHERE END_EFFECTIVE_DT_TM IS NULL
               OR END_EFFECTIVE_DT_TM > current_timestamp()
        ) ranked
        WHERE _CURRENT_RN = 1
    """)
    print(f'[INFO] Created/refreshed current-state view {MFH_CURRENT_VIEW}.')
def _persist_if_supported(dataframe: DataFrame):
    """Materialize the reconciliation snapshot so it is built once rather than per reference."""
    staged = _mmvte_persist_stage(dataframe)
    return (staged, staged is not dataframe)

def process_family_history_incremental():
    """Drop-in replacement processor: complete reconciliation with change-only Delta updates."""
    print('[INFO] Starting complete map_family_history reconciliation.')
    snapshot, snapshot_is_cached = _persist_if_supported(create_family_history_mapping_incr())
    try:
        stats = snapshot.agg(F.count(F.lit(1)).alias('rows'), F.countDistinct('FHX_ACTIVITY_ID').alias('distinct_ids'), F.sum(F.when(F.col('FHX_ACTIVITY_ID').isNull(), 1).otherwise(0)).alias('null_ids')).first()
        source_count = spark.table(MFH_FHX_SOURCE).count()
        if stats['null_ids']:
            raise ValueError(f"Snapshot contains {stats['null_ids']} null FHX_ACTIVITY_ID values.")
        if stats['rows'] != stats['distinct_ids']:
            raise ValueError(f"Snapshot is not unique on FHX_ACTIVITY_ID: rows={stats['rows']}, distinct={stats['distinct_ids']}.")
        if stats['rows'] != source_count:
            raise ValueError(f"Snapshot/source row-count mismatch: snapshot={stats['rows']}, source={source_count}.")
        update_table(snapshot, MFH_TARGET, 'FHX_ACTIVITY_ID', schema_map_family_history, map_family_history_comment, update_condition='NOT (t.`ROW_HASH` <=> s.`ROW_HASH`)', source_is_full_snapshot=True, delete_not_matched_by_source=True, validate_unique_keys=False)
        create_family_history_current_view()
        print(f"[INFO] map_family_history reconciliation complete: {stats['rows']} rows.")
    finally:
        if snapshot_is_cached:
            _mmvte_persist_stage_drop(snapshot)
print('[INFO] Map Updates loaded. No production refresh was executed.')

try:
    _targets = ['4_prod.bronze.map_family_history']
    if not _pipeline_resume_skip_component('map_family_history', _targets):
        process_family_history_incremental()
        _PIPELINE_UPDATED_TARGETS.extend(_targets)
        _pipeline_mark_component_complete('map_family_history', _targets)
        _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_family_history'})
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

_pipeline_component_start("map_patient_journey")
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
import hashlib
import json
import uuid
from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

@dataclass(frozen=True)
class MapPatientJourneyConfig:
    elh_table: str = '4_prod.raw.mill_encntr_loc_hist'
    encounter_table: str = '4_prod.raw.mill_encounter'
    person_alias_table: str = '4_prod.raw.mill_person_alias'
    code_value_table: str = '3_lookup.mill.mill_code_value'
    target_table: str = '4_prod.bronze.map_patient_journey'
    state_table: str = '6_mgmt.bronze.map_patient_journey_pipeline_state'
    audit_table: str = '6_mgmt.bronze.map_patient_journey_pipeline_audit'
    build_manifest_table: str = '6_mgmt.bronze.map_patient_journey_build_manifest'
    schema_version: str = '4.0.0'
    transform_version: str = '4.0.0-bucketed-stop-table'
    code_change_full_rebuild_threshold: int = 5000
    incremental_encounter_full_rebuild_threshold: int = 2000000
    end_effective_sentinel_cutoff: str = '2099-01-01 00:00:00'
    full_build_bucket_count: int = 32
    enable_liquid_clustering: bool = True
MAP_PATIENT_JOURNEY_CONFIG = MapPatientJourneyConfig()
map_patient_journey_comment = 'Lossless encounter-location-history foundation at one row per ENCNTR_LOC_HIST_ID. Rows are history events, which may represent location, service, encounter-type or care-level changes; separate columns identify physical location stops. The existing 98-column interface is retained and extended only with focused location, event, stop, quality and LOS semantics. Raw source audit, CDF and temporal-alias provenance live in control or sidecar datasets rather than being repeated on every history row. Exact Delta versions and a pinned build timestamp make full-build resume and later CDF incrementality deterministic.'
_MPJ_INTERNAL_BUCKET_COLUMN = '_BUILD_BUCKET'
_MPJ_MANIFEST_HEADER_STAGE = 'BUILD'
_MPJ_TEMPORAL_ALIAS_SIDECAR = 'map_patient_journey_identifier_asof'
MRN_POOL_PRIORITY: Dict[int, int] = {683996: 1, 1115132483: 2, 6200990: 3, 6173940: 4}
MRN_POOL_LABELS: Dict[int, str] = {683996: 'MRN_GENERIC', 1115132483: 'MRN_RF4', 6200990: 'MRN_RNJ_5C4', 6173940: 'CMRN_5C4'}

def _mpj_field(name: str, data_type: T.DataType, comment: str, nullable: bool=True) -> T.StructField:
    return T.StructField(name, data_type, nullable, metadata={'comment': comment})
schema_map_patient_journey = T.StructType([_mpj_field('ENCNTR_LOC_HIST_ID', T.LongType(), 'Source encounter-location-history primary key.', False), _mpj_field('ENCNTR_ID', T.LongType(), 'Source encounter identifier.', False), _mpj_field('PERSON_ID', T.LongType(), 'Person identifier supplied by the encounter row.'), _mpj_field('MRN', T.StringType(), 'Best current-preferred MRN; selection provenance is retained separately.'), _mpj_field('NHS_NUMBER', T.StringType(), 'Best current-preferred NHS number; selection provenance is retained separately.'), _mpj_field('ENCNTR_TYPE_CD', T.LongType(), 'Encounter-level encounter type code.'), _mpj_field('ENCNTR_TYPE_DESC', T.StringType(), 'Best available label for ENCNTR_TYPE_CD.'), _mpj_field('ENCNTR_TYPE_CLASS_CD', T.LongType(), 'Encounter-level broad encounter type-class code.'), _mpj_field('ENCNTR_TYPE_CLASS_DESC', T.StringType(), 'Best available label for ENCNTR_TYPE_CLASS_CD.'), _mpj_field('ENCNTR_STATUS_CD', T.LongType(), 'Encounter status code.'), _mpj_field('ENCNTR_STATUS_DESC', T.StringType(), 'Best available label for ENCNTR_STATUS_CD.'), _mpj_field('ADMIT_SRC_CD', T.LongType(), 'Encounter admission-source code.'), _mpj_field('ADMIT_SRC_DESC', T.StringType(), 'Best available label for ADMIT_SRC_CD.'), _mpj_field('ENCOUNTER_ADMIT_TYPE_CD', T.LongType(), 'Encounter-level admission-type code.'), _mpj_field('ENCOUNTER_ADMIT_TYPE_DESC', T.StringType(), 'Best available label for ENCOUNTER_ADMIT_TYPE_CD.'), _mpj_field('ADMIT_MODE_CD', T.LongType(), 'Encounter admission-mode code.'), _mpj_field('ADMIT_MODE_DESC', T.StringType(), 'Best available label for ADMIT_MODE_CD.'), _mpj_field('REFERRAL_SOURCE_CD', T.LongType(), 'Encounter referral-source code.'), _mpj_field('REFERRAL_SOURCE_DESC', T.StringType(), 'Best available label for REFERRAL_SOURCE_CD.'), _mpj_field('READMIT_CD', T.LongType(), 'Encounter readmission code.'), _mpj_field('READMIT_DESC', T.StringType(), 'Best available label for READMIT_CD.'), _mpj_field('REASON_FOR_VISIT', T.StringType(), 'Unchanged encounter reason-for-visit text.'), _mpj_field('LOC_FACILITY_CD', T.LongType(), 'Facility code recorded on the history event.'), _mpj_field('FACILITY_DESC', T.StringType(), 'Best available facility label.'), _mpj_field('LOC_BUILDING_CD', T.LongType(), 'Building code recorded on the history event.'), _mpj_field('BUILDING_DESC', T.StringType(), 'Best available building label.'), _mpj_field('LOC_NURSE_UNIT_CD', T.LongType(), 'Nurse-unit code recorded on the history event.'), _mpj_field('NURSE_UNIT_DESC', T.StringType(), 'Best available nurse-unit label.'), _mpj_field('LOC_ROOM_CD', T.LongType(), 'Room code recorded on the history event.'), _mpj_field('ROOM_DESC', T.StringType(), 'Best available room label.'), _mpj_field('LOC_BED_CD', T.LongType(), 'Bed code recorded on the history event.'), _mpj_field('BED_DESC', T.StringType(), 'Best available bed label.'), _mpj_field('LOC_ENCNTR_TYPE_CD', T.LongType(), 'Encounter type at the time of the history event.'), _mpj_field('LOC_ENCNTR_TYPE_DESC', T.StringType(), 'Best available label for LOC_ENCNTR_TYPE_CD.'), _mpj_field('LOC_ENCNTR_TYPE_CLASS_CD', T.LongType(), 'Encounter type class at the history event.'), _mpj_field('LOC_ENCNTR_TYPE_CLASS_DESC', T.StringType(), 'Best available label for LOC_ENCNTR_TYPE_CLASS_CD.'), _mpj_field('MED_SERVICE_CD', T.LongType(), 'Medical service at the history event.'), _mpj_field('MED_SERVICE_DESC', T.StringType(), 'Best available label for MED_SERVICE_CD.'), _mpj_field('LOC_ADMIT_TYPE_CD', T.LongType(), 'Admission type at the history event.'), _mpj_field('LOC_ADMIT_TYPE_DESC', T.StringType(), 'Best available label for LOC_ADMIT_TYPE_CD.'), _mpj_field('TRANSFER_REASON_CD', T.LongType(), 'Transfer-reason code recorded on the history event.'), _mpj_field('TRANSFER_REASON_DESC', T.StringType(), 'Best available label for TRANSFER_REASON_CD.'), _mpj_field('ACCOMMODATION_CD', T.LongType(), 'Accommodation code recorded on the history event.'), _mpj_field('ACCOMMODATION_DESC', T.StringType(), 'Best available label for ACCOMMODATION_CD.'), _mpj_field('ACCOMMODATION_REASON_CD', T.LongType(), 'Accommodation-reason code recorded on the history event.'), _mpj_field('ACCOMMODATION_REASON_DESC', T.StringType(), 'Best available label for ACCOMMODATION_REASON_CD.'), _mpj_field('ALT_LVL_CARE_CD', T.LongType(), 'Alternate-level-of-care code recorded on the history event.'), _mpj_field('ALT_LVL_CARE_DESC', T.StringType(), 'Best available label for ALT_LVL_CARE_CD.'), _mpj_field('ALC_REASON_CD', T.LongType(), 'Alternate-level-of-care reason code.'), _mpj_field('ALC_REASON_DESC', T.StringType(), 'Best available label for ALC_REASON_CD.'), _mpj_field('SERVICE_CATEGORY_CD', T.LongType(), 'Service category at the history event.'), _mpj_field('SERVICE_CATEGORY_DESC', T.StringType(), 'Best available label for SERVICE_CATEGORY_CD.'), _mpj_field('PROGRAM_SERVICE_CD', T.LongType(), 'Program service at the history event.'), _mpj_field('PROGRAM_SERVICE_DESC', T.StringType(), 'Best available label for PROGRAM_SERVICE_CD.'), _mpj_field('SPECIALTY_UNIT_CD', T.LongType(), 'Specialty unit at the history event.'), _mpj_field('SPECIALTY_UNIT_DESC', T.StringType(), 'Best available label for SPECIALTY_UNIT_CD.'), _mpj_field('ISOLATION_CD', T.LongType(), 'Isolation code at the history event.'), _mpj_field('ISOLATION_DESC', T.StringType(), 'Best available label for ISOLATION_CD.'), _mpj_field('LOC_ARRIVE_DT_TM', T.TimestampType(), 'Unchanged source location arrival timestamp.'), _mpj_field('LOC_DEPART_DT_TM', T.TimestampType(), 'Unchanged source location departure timestamp.'), _mpj_field('LOC_BEG_EFFECTIVE_DT_TM', T.TimestampType(), 'Unchanged source history-row effective start.'), _mpj_field('LOC_END_EFFECTIVE_DT_TM', T.TimestampType(), 'Unchanged source history-row effective end, including sentinels.'), _mpj_field('LOC_TRANSACTION_DT_TM', T.TimestampType(), 'Source transaction timestamp that triggered the history event.'), _mpj_field('LOC_ACTIVITY_DT_TM', T.TimestampType(), 'Source system insertion timestamp for the history event.'), _mpj_field('LOC_LENGTH_OF_STAY_HOURS', T.DoubleType(), 'Legacy location duration: the physical location-stop duration on rows where IS_PHYSICAL_LOCATION_CHANGE is true and null on rows that continue a stop; identical to LOCATION_STOP_DURATION_HOURS. Use HISTORY_STATE_DURATION_HOURS for the per-history-event duration.'), _mpj_field('PRE_REG_DT_TM', T.TimestampType(), 'Encounter pre-registration timestamp.'), _mpj_field('REG_DT_TM', T.TimestampType(), 'Encounter registration timestamp.'), _mpj_field('ENCNTR_ARRIVE_DT_TM', T.TimestampType(), 'Unchanged encounter actual arrival timestamp.'), _mpj_field('INPATIENT_ADMIT_DT_TM', T.TimestampType(), 'Encounter inpatient-admission timestamp.'), _mpj_field('DISCH_DT_TM', T.TimestampType(), 'Encounter clinical discharge timestamp.'), _mpj_field('ENCNTR_DEPART_DT_TM', T.TimestampType(), 'Encounter physical departure timestamp.'), _mpj_field('EST_LENGTH_OF_STAY', T.DoubleType(), 'Unchanged source estimated length of stay; fractional values are preserved.'), _mpj_field('ENCNTR_LENGTH_OF_STAY_DAYS', T.DoubleType(), 'Compatibility best-available encounter LOS; see ENCNTR_LOS_SOURCE and explicit LOS measures.'), _mpj_field('DISCH_DISPOSITION_CD', T.LongType(), 'Encounter discharge-disposition code.'), _mpj_field('DISCH_DISPOSITION_DESC', T.StringType(), 'Best available label for DISCH_DISPOSITION_CD.'), _mpj_field('DISCH_TO_LOCTN_CD', T.LongType(), 'Encounter discharge-destination code.'), _mpj_field('DISCH_TO_LOCTN_DESC', T.StringType(), 'Best available label for DISCH_TO_LOCTN_CD.'), _mpj_field('TRIAGE_CD', T.LongType(), 'Encounter triage code.'), _mpj_field('TRIAGE_DESC', T.StringType(), 'Best available label for TRIAGE_CD.'), _mpj_field('TRIAGE_DT_TM', T.TimestampType(), 'Encounter triage timestamp.'), _mpj_field('TRAUMA_CD', T.LongType(), 'Encounter trauma code.'), _mpj_field('TRAUMA_DESC', T.StringType(), 'Best available label for TRAUMA_CD.'), _mpj_field('AMBULATORY_COND_CD', T.LongType(), 'Encounter ambulatory-condition code.'), _mpj_field('AMBULATORY_COND_DESC', T.StringType(), 'Best available label for AMBULATORY_COND_CD.'), _mpj_field('FINANCIAL_CLASS_CD', T.LongType(), 'Encounter financial-class code.'), _mpj_field('FINANCIAL_CLASS_DESC', T.StringType(), 'Best available label for FINANCIAL_CLASS_CD.'), _mpj_field('VIP_CD', T.LongType(), 'Encounter VIP code.'), _mpj_field('VIP_DESC', T.StringType(), 'Best available label for VIP_CD.'), _mpj_field('CONFID_LEVEL_CD', T.LongType(), 'Encounter confidentiality-level code.'), _mpj_field('CONFID_LEVEL_DESC', T.StringType(), 'Best available label for CONFID_LEVEL_CD.'), _mpj_field('LOCATION_SEQUENCE', T.IntegerType(), 'Legacy location sequence: the physical location-stop number (LOCATION_STOP_SEQUENCE) on rows where IS_PHYSICAL_LOCATION_CHANGE is true and null on rows that continue a stop. Use HISTORY_EVENT_SEQUENCE for the history-event ordinal.'), _mpj_field('PREV_NURSE_UNIT', T.StringType(), "Previous history event's nurse-unit label."), _mpj_field('NEXT_NURSE_UNIT', T.StringType(), "Next history event's nurse-unit label."), _mpj_field('IS_FIRST_LOCATION', T.IntegerType(), 'Legacy flag: 1 only on the row that opens the first physical location stop of the encounter, otherwise 0. Use IS_FIRST_EVENT for the first history event.'), _mpj_field('IS_LAST_LOCATION', T.IntegerType(), 'Legacy flag: 1 only on the row that opens the last physical location stop of the encounter, otherwise 0; it does not imply current location. Use IS_LAST_EVENT for the last history event.'), _mpj_field('TOTAL_LOCATIONS_VISITED', T.IntegerType(), 'Legacy count: the number of consecutive physical location stops in the encounter; identical to TOTAL_LOCATION_STOPS. Use TOTAL_HISTORY_EVENTS for the count of history events.'), _mpj_field('ORGANIZATION_ID', T.LongType(), 'Organization identifier recorded on the history event.'), _mpj_field('ADC_UPDT', T.TimestampType(), 'Greatest source ADC update timestamp across location, encounter, aliases and code values; not a CDF checkpoint.'), _mpj_field('TRUST', T.StringType(), 'Source Trust value from encounter-location history; no Trust filter is applied.'), _mpj_field('LOCATION_CD', T.LongType(), 'Source lowest available location code for the history event.'), _mpj_field('LOCATION_DESC', T.StringType(), 'Best available label for LOCATION_CD.'), _mpj_field('MOST_SPECIFIC_LOCATION_CD', T.LongType(), 'Most specific non-zero hierarchy code: bed, room, nurse unit, building, facility, then LOCATION_CD.'), _mpj_field('MOST_SPECIFIC_LOCATION_DESC', T.StringType(), 'Most specific available location label.'), _mpj_field('MOST_SPECIFIC_LOCATION_LEVEL', T.StringType(), 'Hierarchy level supplying MOST_SPECIFIC_LOCATION_CD/DESC.'), _mpj_field('ELH_ACTIVE_IND', T.LongType(), 'Source location-history active indicator; retained without filtering.'), _mpj_field('ENC_ACTIVE_IND', T.LongType(), 'Encounter source active indicator; encounter rows are not filtered.'), _mpj_field('EVENT_ORDER_DT_TM', T.TimestampType(), 'Best available deterministic history-event ordering timestamp.'), _mpj_field('EVENT_ORDER_SOURCE', T.StringType(), 'Source field supplying EVENT_ORDER_DT_TM.'), _mpj_field('EVENT_ORDER_TIE_COUNT', T.IntegerType(), 'Number of rows in the encounter sharing EVENT_ORDER_DT_TM.'), _mpj_field('HISTORY_EVENT_SEQUENCE', T.IntegerType(), 'Deterministic sequence of all history events within the encounter.'), _mpj_field('TOTAL_HISTORY_EVENTS', T.IntegerType(), 'Count of all location-history events within the encounter.'), _mpj_field('IS_FIRST_EVENT', T.BooleanType(), 'True for the first deterministically ordered history event.'), _mpj_field('IS_LAST_EVENT', T.BooleanType(), 'True for the final deterministically ordered history event; not synonymous with current location.'), _mpj_field('PREV_LOCATION_CD', T.LongType(), "Previous history event's canonical LOCATION_CD."), _mpj_field('PREV_LOCATION_DESC', T.StringType(), 'Best available label for PREV_LOCATION_CD.'), _mpj_field('NEXT_LOCATION_CD', T.LongType(), "Next history event's canonical LOCATION_CD."), _mpj_field('NEXT_LOCATION_DESC', T.StringType(), 'Best available label for NEXT_LOCATION_CD.'), _mpj_field('PREV_NURSE_UNIT_CD', T.LongType(), "Previous history event's nurse-unit code."), _mpj_field('NEXT_NURSE_UNIT_CD', T.LongType(), "Next history event's nurse-unit code."), _mpj_field('IS_PHYSICAL_LOCATION_CHANGE', T.BooleanType(), 'True when the full location hierarchy differs from the previous event; first event is true.'), _mpj_field('LOCATION_STOP_SEQUENCE', T.IntegerType(), 'Sequence of consecutive physical location stops within the encounter.'), _mpj_field('TOTAL_LOCATION_STOPS', T.IntegerType(), 'Count of consecutive physical location stops within the encounter.'), _mpj_field('LOCATION_STOP_START_DT_TM', T.TimestampType(), 'Start timestamp of the physical location stop containing this history event.'), _mpj_field('LOCATION_STOP_END_DT_TM', T.TimestampType(), 'Earliest valid next-stop, departure or finite-effective boundary for the physical stop.'), _mpj_field('LOCATION_STOP_DURATION_HOURS', T.DoubleType(), 'Non-negative physical location-stop duration in hours; null when open or invalid.'), _mpj_field('HISTORY_STATE_END_DT_TM', T.TimestampType(), 'Earliest valid next-event, departure or finite-effective boundary for this history state.'), _mpj_field('HISTORY_STATE_DURATION_HOURS', T.DoubleType(), 'Non-negative history-state duration in hours; null when open or invalid.'), _mpj_field('END_EFFECTIVE_SENTINEL_IND', T.BooleanType(), 'True when LOC_END_EFFECTIVE_DT_TM is at/after the configured sentinel cutoff.'), _mpj_field('LOCATION_TIME_INVALID_IND', T.BooleanType(), 'True when a supplied location departure/effective end precedes the event start.'), _mpj_field('IS_CURRENT_LOCATION', T.BooleanType(), 'True when this was the active, open final event as of CURRENT_AS_OF_DT_TM.'), _mpj_field('CURRENT_AS_OF_DT_TM', T.TimestampType(), 'Pipeline timestamp at which IS_CURRENT_LOCATION and current aliases were evaluated.'), _mpj_field('BEST_AVAILABLE_ENCNTR_START_DT_TM', T.TimestampType(), 'Encounter start convenience timestamp: ARRIVE, REG, PRE_REG, then EST_ARRIVE.'), _mpj_field('BEST_AVAILABLE_ENCNTR_START_SOURCE', T.StringType(), 'Field supplying BEST_AVAILABLE_ENCNTR_START_DT_TM.'), _mpj_field('FACILITY_PHYSICAL_LOS_DAYS', T.DoubleType(), 'Non-negative ARRIVE-to-DEPART duration in days.'), _mpj_field('FACILITY_PHYSICAL_LOS_INVALID_IND', T.BooleanType(), 'True when physical encounter departure precedes arrival.'), _mpj_field('INPATIENT_CLINICAL_LOS_DAYS', T.DoubleType(), 'Non-negative INPATIENT_ADMIT-to-DISCH duration in days.'), _mpj_field('INPATIENT_CLINICAL_LOS_INVALID_IND', T.BooleanType(), 'True when discharge precedes inpatient admission.'), _mpj_field('REGISTRATION_TO_DISCHARGE_DAYS', T.DoubleType(), 'Non-negative REG-to-DISCH duration in days.'), _mpj_field('REGISTRATION_TO_DISCHARGE_INVALID_IND', T.BooleanType(), 'True when discharge precedes registration.'), _mpj_field('ENCNTR_LOS_SOURCE', T.StringType(), 'Explicit duration supplying ENCNTR_LENGTH_OF_STAY_DAYS.'), _mpj_field('ROW_HASH', T.LongType(), 'Stable hash of source-derived content used to suppress unchanged rewrites.'), _mpj_field('PIPELINE_UPDT_DT_TM', T.TimestampType(), 'Pipeline timestamp at which this row last materially changed.')])
'Restartable schema-v4 patient-journey runtime.\n\nThis file is concatenated after the reviewed compatibility schema by the Map\nPipeline compiler.  It intentionally contains no notebook execution tail.\n'
import builtins as _mpj_builtins
ELH_CODE_DESCRIPTION_COLUMNS: Dict[str, str] = {'LOC_FACILITY_CD': 'FACILITY_DESC', 'LOC_BUILDING_CD': 'BUILDING_DESC', 'LOC_NURSE_UNIT_CD': 'NURSE_UNIT_DESC', 'LOC_ROOM_CD': 'ROOM_DESC', 'LOC_BED_CD': 'BED_DESC', 'LOCATION_CD': 'LOCATION_DESC', 'LOC_ENCNTR_TYPE_CD': 'LOC_ENCNTR_TYPE_DESC', 'LOC_ENCNTR_TYPE_CLASS_CD': 'LOC_ENCNTR_TYPE_CLASS_DESC', 'MED_SERVICE_CD': 'MED_SERVICE_DESC', 'LOC_ADMIT_TYPE_CD': 'LOC_ADMIT_TYPE_DESC', 'TRANSFER_REASON_CD': 'TRANSFER_REASON_DESC', 'ACCOMMODATION_CD': 'ACCOMMODATION_DESC', 'ACCOMMODATION_REASON_CD': 'ACCOMMODATION_REASON_DESC', 'ALT_LVL_CARE_CD': 'ALT_LVL_CARE_DESC', 'ALC_REASON_CD': 'ALC_REASON_DESC', 'SERVICE_CATEGORY_CD': 'SERVICE_CATEGORY_DESC', 'PROGRAM_SERVICE_CD': 'PROGRAM_SERVICE_DESC', 'SPECIALTY_UNIT_CD': 'SPECIALTY_UNIT_DESC', 'ISOLATION_CD': 'ISOLATION_DESC', 'PREV_LOCATION_CD': 'PREV_LOCATION_DESC', 'NEXT_LOCATION_CD': 'NEXT_LOCATION_DESC', 'PREV_NURSE_UNIT_CD': 'PREV_NURSE_UNIT', 'NEXT_NURSE_UNIT_CD': 'NEXT_NURSE_UNIT'}
ENC_CODE_DESCRIPTION_COLUMNS: Dict[str, str] = {'ENCNTR_TYPE_CD': 'ENCNTR_TYPE_DESC', 'ENCNTR_TYPE_CLASS_CD': 'ENCNTR_TYPE_CLASS_DESC', 'ENCNTR_STATUS_CD': 'ENCNTR_STATUS_DESC', 'ADMIT_SRC_CD': 'ADMIT_SRC_DESC', 'ENCOUNTER_ADMIT_TYPE_CD': 'ENCOUNTER_ADMIT_TYPE_DESC', 'ADMIT_MODE_CD': 'ADMIT_MODE_DESC', 'REFERRAL_SOURCE_CD': 'REFERRAL_SOURCE_DESC', 'READMIT_CD': 'READMIT_DESC', 'DISCH_DISPOSITION_CD': 'DISCH_DISPOSITION_DESC', 'DISCH_TO_LOCTN_CD': 'DISCH_TO_LOCTN_DESC', 'TRIAGE_CD': 'TRIAGE_DESC', 'TRAUMA_CD': 'TRAUMA_DESC', 'AMBULATORY_COND_CD': 'AMBULATORY_COND_DESC', 'FINANCIAL_CLASS_CD': 'FINANCIAL_CLASS_DESC', 'VIP_CD': 'VIP_DESC', 'CONFID_LEVEL_CD': 'CONFID_LEVEL_DESC'}
ELH_RAW_CODE_COLUMNS: Sequence[str] = ('LOCATION_CD', 'LOC_FACILITY_CD', 'LOC_BUILDING_CD', 'LOC_NURSE_UNIT_CD', 'LOC_ROOM_CD', 'LOC_BED_CD', 'ENCNTR_TYPE_CD', 'ENCNTR_TYPE_CLASS_CD', 'MED_SERVICE_CD', 'ADMIT_TYPE_CD', 'TRANSFER_REASON_CD', 'ACCOMMODATION_CD', 'ACCOMMODATION_REASON_CD', 'ALT_LVL_CARE_CD', 'ALC_REASON_CD', 'SERVICE_CATEGORY_CD', 'PROGRAM_SERVICE_CD', 'SPECIALTY_UNIT_CD', 'ISOLATION_CD')
ENC_RAW_CODE_COLUMNS: Sequence[str] = ('ENCNTR_TYPE_CD', 'ENCNTR_TYPE_CLASS_CD', 'ENCNTR_STATUS_CD', 'ADMIT_SRC_CD', 'ADMIT_TYPE_CD', 'ADMIT_MODE_CD', 'REFERRAL_SOURCE_CD', 'READMIT_CD', 'DISCH_DISPOSITION_CD', 'DISCH_TO_LOCTN_CD', 'TRIAGE_CD', 'TRAUMA_CD', 'AMBULATORY_COND_CD', 'FINANCIAL_CLASS_CD', 'VIP_CD', 'CONFID_LEVEL_CD')
_MPJ_VOLATILE_HASH_COLUMNS = {'ROW_HASH', 'PIPELINE_UPDT_DT_TM', 'CURRENT_AS_OF_DT_TM'}

class _MPJFullRebuildRequired(RuntimeError):
    pass

def _mpj_sql_identifier(table_name: str) -> str:
    tick = chr(96)
    return '.'.join((f'{tick}{part.replace(tick, tick + tick)}{tick}' for part in table_name.split('.')))

def _mpj_sql_column(column_name: str) -> str:
    tick = chr(96)
    return f'{tick}{column_name.replace(tick, tick + tick)}{tick}'

def _mpj_escape_sql_string(value: str) -> str:
    return value.replace('\\', '\\\\').replace("'", "''")

def _mpj_table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _mpj_drop_table(table_name: str) -> None:
    spark.sql(f'DROP TABLE IF EXISTS {_mpj_sql_identifier(table_name)}')

def _mpj_latest_delta_version(table_name: str) -> int:
    row = spark.sql(f'DESCRIBE HISTORY {_mpj_sql_identifier(table_name)} LIMIT 1').select('version').first()
    if row is None:
        raise RuntimeError(f'No Delta history returned for {table_name}')
    return int(row['version'])

def _mpj_read_snapshot(table_name: str, version: int) -> DataFrame:
    return _m40_hardening_read_pinned_snapshot(table_name, int(version))

def _mpj_read_cdf(table_name: str, starting_version: int, ending_version: int) -> Optional[DataFrame]:
    if starting_version > ending_version:
        return None
    try:
        changes = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', int(starting_version)).option('endingVersion', int(ending_version)).table(table_name)
        return _m40_hardening_validate_cdf(changes)
    except Exception as exc:
        raise _MPJFullRebuildRequired(f'CDF unavailable for {table_name} from version {starting_version} through {ending_version}; a deterministic full rebuild is required') from exc

def _mpj_is_cdf_gap_exception(exc: BaseException) -> bool:
    messages: List[str] = []
    current: Optional[BaseException] = exc
    while current is not None and len(messages) < 8:
        messages.append(str(current).lower())
        current = current.__cause__ or current.__context__
    combined = ' '.join(messages)
    return any((marker in combined for marker in ('change data feed was not enabled', 'change data feed is not enabled', 'cdf could not be read', 'change data feed', 'delta_change', 'startingversion', 'starting version')))

def _mpj_capture_source_versions(config: MapPatientJourneyConfig) -> Dict[str, int]:
    return {config.elh_table: _mpj_latest_delta_version(config.elh_table), config.encounter_table: _mpj_latest_delta_version(config.encounter_table), config.person_alias_table: _mpj_latest_delta_version(config.person_alias_table), config.code_value_table: _mpj_latest_delta_version(config.code_value_table)}

def _mpj_checked_long(column: Column) -> Column:
    return column.cast('long')

def _mpj_nonblank(column: Column) -> Column:
    return F.when(column.isNotNull() & (F.length(F.trim(column.cast('string'))) > 0), F.trim(column.cast('string')))

def _mpj_nonzero(column: Column) -> Column:
    return F.when(column.isNotNull() & (column.cast('long') != 0), column.cast('long'))

def _mpj_schema_select(df: DataFrame, schema: T.StructType) -> DataFrame:
    expressions: List[Column] = []
    for field in schema.fields:
        expression = F.col(field.name).cast(field.dataType) if field.name in df.columns else F.lit(None).cast(field.dataType)
        expressions.append(expression.alias(field.name, metadata=field.metadata))
    return df.select(*expressions)

def _mpj_schema_hash() -> str:
    payload = [
        {
            'name': field.name,
            'type': field.dataType.simpleString(),
            'nullable': bool(field.nullable),
        }
        for field in bronze_contract_schema(
            MAP_PATIENT_JOURNEY_CONFIG.target_table
        ).fields
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode('utf-8')
    ).hexdigest()

def _mpj_manifest_config(config: MapPatientJourneyConfig) -> Dict[str, object]:
    return {'schema_version': config.schema_version, 'schema_hash': _mpj_schema_hash(), 'transform_version': config.transform_version, 'bucket_count': int(config.full_build_bucket_count), 'bucket_hash': 'pmod(xxhash64(ENCNTR_ID), bucket_count)', 'current_as_of_timezone': 'UTC', 'end_effective_sentinel_cutoff': config.end_effective_sentinel_cutoff, 'mrn_pool_priority': {str(pool): int(priority) for pool, priority in MRN_POOL_PRIORITY.items()}, 'code_label_policy': 'DISPLAY, DESCRIPTION, CDF_MEANING', 'code_change_full_rebuild_threshold': int(config.code_change_full_rebuild_threshold), 'incremental_encounter_full_rebuild_threshold': int(config.incremental_encounter_full_rebuild_threshold), 'temporal_alias_semantics': f'excluded from the core table; reserved for {_MPJ_TEMPORAL_ALIAS_SIDECAR}'}

def _mpj_control_schema(config: MapPatientJourneyConfig) -> str:
    return config.build_manifest_table.rsplit('.', 1)[0]

def _mpj_stage_tables(config: MapPatientJourneyConfig, build_id: str) -> Dict[str, str]:
    suffix = build_id.replace('-', '_')
    base = f'{_mpj_control_schema(config)}.map_patient_journey_build_{suffix}'
    return {'code': f'{base}__code', 'encounter': f'{base}__encounter', 'elh': f'{base}__elh', 'buckets': f'{base}__buckets', 'publish': f'{base}__publish'}

def _mpj_incremental_tables(config: MapPatientJourneyConfig, run_id: str) -> Dict[str, str]:
    suffix = run_id.replace('-', '_')
    base = f'{_mpj_control_schema(config)}.map_patient_journey_increment_{suffix}'
    return {'affected': f'{base}__affected', 'new_rows': f'{base}__new_rows', 'changes': f'{base}__changes'}

def _mpj_ensure_control_tables(config: MapPatientJourneyConfig) -> None:
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {_mpj_sql_identifier(_mpj_control_schema(config))}')
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_mpj_sql_identifier(config.state_table)} (
            source_table STRING NOT NULL,
            last_processed_version BIGINT NOT NULL,
            last_processed_at TIMESTAMP NOT NULL,
            last_run_id STRING NOT NULL
        )
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_mpj_sql_identifier(config.audit_table)} (
            run_id STRING NOT NULL,
            event_timestamp TIMESTAMP NOT NULL,
            status STRING NOT NULL,
            mode STRING NOT NULL,
            message STRING,
            source_versions_json STRING,
            metrics_json STRING
        )
        USING DELTA
    """)

def _mpj_ensure_build_manifest_table(config: MapPatientJourneyConfig) -> None:
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {_mpj_sql_identifier(_mpj_control_schema(config))}')
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_mpj_sql_identifier(config.build_manifest_table)} (
            build_id STRING NOT NULL,
            stage_name STRING NOT NULL,
            bucket_id INT NOT NULL,
            status STRING NOT NULL,
            schema_version STRING NOT NULL,
            schema_hash STRING NOT NULL,
            source_versions_json STRING NOT NULL,
            run_timestamp TIMESTAMP NOT NULL,
            config_json STRING NOT NULL,
            row_count BIGINT,
            metrics_json STRING,
            error_message STRING,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)

def _mpj_read_pipeline_state(config: MapPatientJourneyConfig) -> Dict[str, Dict[str, object]]:
    if not _mpj_table_exists(config.state_table):
        return {}
    required_sources = [config.elh_table, config.encounter_table, config.person_alias_table, config.code_value_table]
    rows = spark.table(config.state_table).filter(F.col('source_table').isin(required_sources)).select('source_table', 'last_processed_version', 'last_processed_at').collect()
    return {row['source_table']: {'version': int(row['last_processed_version']), 'processed_at': row['last_processed_at']} for row in rows if row['last_processed_version'] is not None}

def _mpj_commit_pipeline_state(config: MapPatientJourneyConfig, source_versions: Dict[str, int], run_id: str, completed_at: datetime) -> None:
    schema = T.StructType([T.StructField('source_table', T.StringType(), False), T.StructField('last_processed_version', T.LongType(), False), T.StructField('last_processed_at', T.TimestampType(), False), T.StructField('last_run_id', T.StringType(), False)])
    rows = [(source, int(version), completed_at, run_id) for source, version in source_versions.items()]
    updates = spark.createDataFrame(rows, schema)
    DeltaTable.forName(spark, config.state_table).alias('t').merge(updates.alias('s'), 't.source_table = s.source_table').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def _mpj_write_audit_event(config: MapPatientJourneyConfig, run_id: str, status: str, mode: str, message: str, source_versions: Optional[Dict[str, int]]=None, metrics: Optional[Dict[str, object]]=None) -> None:
    try:
        schema = T.StructType([T.StructField('run_id', T.StringType(), False), T.StructField('event_timestamp', T.TimestampType(), False), T.StructField('status', T.StringType(), False), T.StructField('mode', T.StringType(), False), T.StructField('message', T.StringType(), True), T.StructField('source_versions_json', T.StringType(), True), T.StructField('metrics_json', T.StringType(), True)])
        row = [(run_id, datetime.now(timezone.utc), status, mode, str(message)[:8000], json.dumps(source_versions or {}, sort_keys=True, default=str), json.dumps(metrics or {}, sort_keys=True, default=str))]
        spark.createDataFrame(row, schema).write.mode('append').saveAsTable(config.audit_table)
    except Exception as audit_exc:
        print(f'[WARN] Could not write map_patient_journey audit event: {audit_exc}')

def _mpj_manifest_upsert(config: MapPatientJourneyConfig, build_id: str, stage_name: str, bucket_id: int, status: str, schema_hash: str, source_versions: Dict[str, int], run_timestamp: datetime, manifest_config: Dict[str, object], row_count: Optional[int]=None, metrics: Optional[Dict[str, object]]=None, error_message: Optional[str]=None, created_at: Optional[datetime]=None) -> None:
    now = datetime.now(timezone.utc)
    schema = T.StructType([T.StructField('build_id', T.StringType(), False), T.StructField('stage_name', T.StringType(), False), T.StructField('bucket_id', T.IntegerType(), False), T.StructField('status', T.StringType(), False), T.StructField('schema_version', T.StringType(), False), T.StructField('schema_hash', T.StringType(), False), T.StructField('source_versions_json', T.StringType(), False), T.StructField('run_timestamp', T.TimestampType(), False), T.StructField('config_json', T.StringType(), False), T.StructField('row_count', T.LongType(), True), T.StructField('metrics_json', T.StringType(), True), T.StructField('error_message', T.StringType(), True), T.StructField('created_at', T.TimestampType(), False), T.StructField('updated_at', T.TimestampType(), False)])
    row = [(build_id, stage_name, int(bucket_id), status, config.schema_version, schema_hash, json.dumps(source_versions, sort_keys=True), run_timestamp, json.dumps(manifest_config, sort_keys=True, default=str), int(row_count) if row_count is not None else None, json.dumps(metrics or {}, sort_keys=True, default=str), str(error_message)[:8000] if error_message else None, created_at or now, now)]
    updates = spark.createDataFrame(row, schema)
    DeltaTable.forName(spark, config.build_manifest_table).alias('t').merge(updates.alias('s'), 't.build_id = s.build_id AND t.stage_name = s.stage_name AND t.bucket_id = s.bucket_id').whenMatchedUpdate(set={'status': 's.status', 'schema_version': 's.schema_version', 'schema_hash': 's.schema_hash', 'source_versions_json': 's.source_versions_json', 'run_timestamp': 's.run_timestamp', 'config_json': 's.config_json', 'row_count': 's.row_count', 'metrics_json': 's.metrics_json', 'error_message': 's.error_message', 'updated_at': 's.updated_at'}).whenNotMatchedInsertAll().execute()

def _mpj_manifest_rows(config: MapPatientJourneyConfig, build_id: str) -> List[Dict[str, object]]:
    return [row.asDict(recursive=True) for row in spark.table(config.build_manifest_table).where(F.col('build_id') == build_id).orderBy('stage_name', 'bucket_id').collect()]

def _mpj_active_manifest(config: MapPatientJourneyConfig) -> Optional[Dict[str, object]]:
    if not _mpj_table_exists(config.build_manifest_table):
        return None
    rows = spark.table(config.build_manifest_table).where((F.col('stage_name') == _MPJ_MANIFEST_HEADER_STAGE) & (F.col('bucket_id') == -1) & F.col('status').isin('BUILDING', 'PUBLISHING', 'PUBLISHED')).orderBy(F.col('updated_at').desc()).limit(2).collect()
    if not rows:
        return None
    if len(rows) > 1:
        raise RuntimeError('Multiple active patient-journey build manifests exist; an explicit reviewed abandonment is required')
    return rows[0].asDict(recursive=True)

def _mpj_start_manifest(config: MapPatientJourneyConfig, source_versions: Dict[str, int], run_timestamp: datetime) -> Dict[str, object]:
    build_id = str(uuid.uuid4())
    schema_hash = _mpj_schema_hash()
    manifest_config = _mpj_manifest_config(config)
    created_at = datetime.now(timezone.utc)
    _mpj_manifest_upsert(config, build_id, _MPJ_MANIFEST_HEADER_STAGE, -1, 'BUILDING', schema_hash, source_versions, run_timestamp, manifest_config, created_at=created_at)
    for stage_name in ('CODE', 'ENCOUNTER', 'ELH', 'PUBLISH'):
        _mpj_manifest_upsert(config, build_id, stage_name, -1, 'PENDING', schema_hash, source_versions, run_timestamp, manifest_config, created_at=created_at)
    for bucket_id in range(int(config.full_build_bucket_count)):
        _mpj_manifest_upsert(config, build_id, 'BUCKET', bucket_id, 'PENDING', schema_hash, source_versions, run_timestamp, manifest_config, created_at=created_at)
    return {'build_id': build_id, 'source_versions': source_versions, 'run_timestamp': run_timestamp, 'schema_hash': schema_hash, 'manifest_config': manifest_config, 'status': 'BUILDING'}

def _mpj_load_or_start_manifest(config: MapPatientJourneyConfig) -> Dict[str, object]:
    active = _mpj_active_manifest(config)
    expected_hash = _mpj_schema_hash()
    expected_config = json.loads(json.dumps(_mpj_manifest_config(config), sort_keys=True, default=str))
    if active is None:
        return _mpj_start_manifest(config, _mpj_capture_source_versions(config), datetime.now(timezone.utc))
    source_versions = json.loads(active['source_versions_json'])
    manifest_config = json.loads(active['config_json'])
    if active['schema_version'] != config.schema_version:
        raise RuntimeError(f"The active patient-journey manifest uses schema version {active['schema_version']}; explicitly abandon it before starting schema version {config.schema_version}")
    if active['schema_hash'] != expected_hash or manifest_config != expected_config:
        raise RuntimeError('The active patient-journey manifest is incompatible with the current schema/configuration; explicitly abandon it rather than mixing buckets')
    return {'build_id': active['build_id'], 'source_versions': {key: int(value) for key, value in source_versions.items()}, 'run_timestamp': active['run_timestamp'], 'schema_hash': active['schema_hash'], 'manifest_config': manifest_config, 'status': active['status']}

def _mpj_prepare_code_dimension(raw: DataFrame) -> DataFrame:
    return raw.select(_mpj_checked_long(F.col('CODE_VALUE')).alias('CODE_VALUE'), F.coalesce(_mpj_nonblank(F.col('DISPLAY')), _mpj_nonblank(F.col('DESCRIPTION')), _mpj_nonblank(F.col('CDF_MEANING'))).alias('CODE_LABEL'), F.col('ADC_UPDT').cast('timestamp').alias('CODE_ADC_UPDT')).where(F.col('CODE_VALUE').isNotNull())

def _mpj_prepare_elh(raw: DataFrame) -> DataFrame:
    return raw.select(_mpj_checked_long(F.col('ENCNTR_LOC_HIST_ID')).alias('ENCNTR_LOC_HIST_ID'), _mpj_checked_long(F.col('ENCNTR_ID')).alias('ENCNTR_ID'), _mpj_checked_long(F.col('LOCATION_CD')).alias('LOCATION_CD'), _mpj_checked_long(F.col('LOC_FACILITY_CD')).alias('LOC_FACILITY_CD'), _mpj_checked_long(F.col('LOC_BUILDING_CD')).alias('LOC_BUILDING_CD'), _mpj_checked_long(F.col('LOC_NURSE_UNIT_CD')).alias('LOC_NURSE_UNIT_CD'), _mpj_checked_long(F.col('LOC_ROOM_CD')).alias('LOC_ROOM_CD'), _mpj_checked_long(F.col('LOC_BED_CD')).alias('LOC_BED_CD'), _mpj_checked_long(F.col('ENCNTR_TYPE_CD')).alias('LOC_ENCNTR_TYPE_CD'), _mpj_checked_long(F.col('ENCNTR_TYPE_CLASS_CD')).alias('LOC_ENCNTR_TYPE_CLASS_CD'), _mpj_checked_long(F.col('MED_SERVICE_CD')).alias('MED_SERVICE_CD'), _mpj_checked_long(F.col('ADMIT_TYPE_CD')).alias('LOC_ADMIT_TYPE_CD'), _mpj_checked_long(F.col('TRANSFER_REASON_CD')).alias('TRANSFER_REASON_CD'), _mpj_checked_long(F.col('ACCOMMODATION_CD')).alias('ACCOMMODATION_CD'), _mpj_checked_long(F.col('ACCOMMODATION_REASON_CD')).alias('ACCOMMODATION_REASON_CD'), _mpj_checked_long(F.col('ALT_LVL_CARE_CD')).alias('ALT_LVL_CARE_CD'), _mpj_checked_long(F.col('ALC_REASON_CD')).alias('ALC_REASON_CD'), _mpj_checked_long(F.col('SERVICE_CATEGORY_CD')).alias('SERVICE_CATEGORY_CD'), _mpj_checked_long(F.col('PROGRAM_SERVICE_CD')).alias('PROGRAM_SERVICE_CD'), _mpj_checked_long(F.col('SPECIALTY_UNIT_CD')).alias('SPECIALTY_UNIT_CD'), _mpj_checked_long(F.col('ISOLATION_CD')).alias('ISOLATION_CD'), _mpj_checked_long(F.col('ORGANIZATION_ID')).alias('ORGANIZATION_ID'), F.col('ARRIVE_DT_TM').cast('timestamp').alias('LOC_ARRIVE_DT_TM'), F.col('DEPART_DT_TM').cast('timestamp').alias('LOC_DEPART_DT_TM'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('LOC_BEG_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('LOC_END_EFFECTIVE_DT_TM'), F.col('TRANSACTION_DT_TM').cast('timestamp').alias('LOC_TRANSACTION_DT_TM'), F.col('ACTIVITY_DT_TM').cast('timestamp').alias('LOC_ACTIVITY_DT_TM'), _mpj_checked_long(F.col('ACTIVE_IND')).alias('ELH_ACTIVE_IND'), F.col('ADC_UPDT').cast('timestamp').alias('ELH_ADC_UPDT'), F.col('Trust').cast('string').alias('TRUST'))

def _mpj_prepare_encounter(raw: DataFrame) -> DataFrame:
    return raw.select(_mpj_checked_long(F.col('ENCNTR_ID')).alias('ENCNTR_ID'), _mpj_checked_long(F.col('PERSON_ID')).alias('PERSON_ID'), _mpj_checked_long(F.col('ENCNTR_TYPE_CD')).alias('ENCNTR_TYPE_CD'), _mpj_checked_long(F.col('ENCNTR_TYPE_CLASS_CD')).alias('ENCNTR_TYPE_CLASS_CD'), _mpj_checked_long(F.col('ENCNTR_STATUS_CD')).alias('ENCNTR_STATUS_CD'), _mpj_checked_long(F.col('ADMIT_SRC_CD')).alias('ADMIT_SRC_CD'), _mpj_checked_long(F.col('ADMIT_TYPE_CD')).alias('ENCOUNTER_ADMIT_TYPE_CD'), _mpj_checked_long(F.col('ADMIT_MODE_CD')).alias('ADMIT_MODE_CD'), _mpj_checked_long(F.col('REFERRAL_SOURCE_CD')).alias('REFERRAL_SOURCE_CD'), _mpj_checked_long(F.col('READMIT_CD')).alias('READMIT_CD'), F.col('REASON_FOR_VISIT').cast('string').alias('REASON_FOR_VISIT'), F.col('PRE_REG_DT_TM').cast('timestamp').alias('PRE_REG_DT_TM'), F.col('REG_DT_TM').cast('timestamp').alias('REG_DT_TM'), F.col('ARRIVE_DT_TM').cast('timestamp').alias('ENCNTR_ARRIVE_DT_TM'), F.col('INPATIENT_ADMIT_DT_TM').cast('timestamp').alias('INPATIENT_ADMIT_DT_TM'), F.col('DISCH_DT_TM').cast('timestamp').alias('DISCH_DT_TM'), F.col('DEPART_DT_TM').cast('timestamp').alias('ENCNTR_DEPART_DT_TM'), F.col('EST_LENGTH_OF_STAY').cast('double').alias('EST_LENGTH_OF_STAY'), _mpj_checked_long(F.col('DISCH_DISPOSITION_CD')).alias('DISCH_DISPOSITION_CD'), _mpj_checked_long(F.col('DISCH_TO_LOCTN_CD')).alias('DISCH_TO_LOCTN_CD'), _mpj_checked_long(F.col('TRIAGE_CD')).alias('TRIAGE_CD'), F.col('TRIAGE_DT_TM').cast('timestamp').alias('TRIAGE_DT_TM'), _mpj_checked_long(F.col('TRAUMA_CD')).alias('TRAUMA_CD'), _mpj_checked_long(F.col('AMBULATORY_COND_CD')).alias('AMBULATORY_COND_CD'), _mpj_checked_long(F.col('FINANCIAL_CLASS_CD')).alias('FINANCIAL_CLASS_CD'), _mpj_checked_long(F.col('VIP_CD')).alias('VIP_CD'), _mpj_checked_long(F.col('CONFID_LEVEL_CD')).alias('CONFID_LEVEL_CD'), _mpj_checked_long(F.col('ACTIVE_IND')).alias('ENC_ACTIVE_IND'), F.col('EST_ARRIVE_DT_TM').cast('timestamp').alias('EST_ARRIVE_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('ENC_ADC_UPDT'))

def _mpj_prepare_aliases(raw: DataFrame) -> DataFrame:
    return raw.select(_mpj_checked_long(F.col('PERSON_ALIAS_ID')).alias('PERSON_ALIAS_ID'), _mpj_checked_long(F.col('PERSON_ID')).alias('PERSON_ID'), _mpj_checked_long(F.col('PERSON_ALIAS_TYPE_CD')).alias('PERSON_ALIAS_TYPE_CD'), _mpj_checked_long(F.col('ALIAS_POOL_CD')).alias('ALIAS_POOL_CD'), _mpj_nonblank(F.col('ALIAS')).alias('ALIAS'), _mpj_checked_long(F.col('ACTIVE_IND')).alias('ACTIVE_IND'), F.col('BEG_EFFECTIVE_DT_TM').cast('timestamp').alias('BEG_EFFECTIVE_DT_TM'), F.col('END_EFFECTIVE_DT_TM').cast('timestamp').alias('END_EFFECTIVE_DT_TM'), F.col('ADC_UPDT').cast('timestamp').alias('ALIAS_ADC_UPDT')).where(F.col('ALIAS').isNotNull())

def _mpj_mrn_pool_priority() -> Column:
    expression = F.lit(999)
    for pool, priority in sorted(MRN_POOL_PRIORITY.items(), key=lambda item: item[1], reverse=True):
        expression = F.when(F.col('ALIAS_POOL_CD') == F.lit(pool), F.lit(priority)).otherwise(expression)
    return expression

def _mpj_build_current_aliases(aliases: DataFrame, run_timestamp: datetime, person_ids: Optional[DataFrame]=None) -> DataFrame:
    if person_ids is not None:
        aliases = aliases.join(person_ids.select('PERSON_ID').distinct(), 'PERSON_ID', 'inner')
    candidates = aliases.where((F.col('PERSON_ALIAS_TYPE_CD') == 18) | ((F.col('PERSON_ALIAS_TYPE_CD') == 10) & F.col('ALIAS_POOL_CD').isin(list(MRN_POOL_PRIORITY)))).withColumn('_IS_CURRENT', (F.col('ACTIVE_IND') == 1) & (F.col('BEG_EFFECTIVE_DT_TM').isNull() | (F.col('BEG_EFFECTIVE_DT_TM') <= F.lit(run_timestamp))) & (F.col('END_EFFECTIVE_DT_TM').isNull() | (F.col('END_EFFECTIVE_DT_TM') > F.lit(run_timestamp)))).withColumn('_POOL_PRIORITY', _mpj_mrn_pool_priority())
    selectable = candidates.where(F.col('BEG_EFFECTIVE_DT_TM').isNull() | (F.col('BEG_EFFECTIVE_DT_TM') <= F.lit(run_timestamp)))
    mrn_window = Window.partitionBy('PERSON_ID').orderBy(F.col('_IS_CURRENT').desc(), F.col('ACTIVE_IND').desc_nulls_last(), F.col('_POOL_PRIORITY').asc(), F.col('BEG_EFFECTIVE_DT_TM').desc_nulls_last(), F.col('ALIAS_ADC_UPDT').desc_nulls_last(), F.col('PERSON_ALIAS_ID').desc_nulls_last())
    nhs_window = Window.partitionBy('PERSON_ID').orderBy(F.col('_IS_CURRENT').desc(), F.col('ACTIVE_IND').desc_nulls_last(), F.col('BEG_EFFECTIVE_DT_TM').desc_nulls_last(), F.col('ALIAS_ADC_UPDT').desc_nulls_last(), F.col('PERSON_ALIAS_ID').desc_nulls_last())
    mrn = selectable.where(F.col('PERSON_ALIAS_TYPE_CD') == 10).withColumn('_RN', F.row_number().over(mrn_window)).where(F.col('_RN') == 1).select('PERSON_ID', F.col('ALIAS').alias('MRN'), F.col('ALIAS_ADC_UPDT').alias('MRN_ADC_UPDT'))
    nhs = selectable.where(F.col('PERSON_ALIAS_TYPE_CD') == 18).withColumn('_RN', F.row_number().over(nhs_window)).where(F.col('_RN') == 1).select('PERSON_ID', F.col('ALIAS').alias('NHS_NUMBER'), F.col('ALIAS_ADC_UPDT').alias('NHS_ADC_UPDT'))
    people = aliases.select('PERSON_ID').where(F.col('PERSON_ID').isNotNull()).distinct()
    return people.join(mrn, 'PERSON_ID', 'left').join(nhs, 'PERSON_ID', 'left')

def _mpj_add_bucket(df: DataFrame, bucket_count: int) -> DataFrame:
    return df.withColumn(_MPJ_INTERNAL_BUCKET_COLUMN, F.pmod(F.xxhash64(F.col('ENCNTR_ID')), F.lit(int(bucket_count))).cast('int'))

def _mpj_enrich_code_column(frame: DataFrame, code_dimension: DataFrame, code_column: str, description_column: str, tag: str) -> DataFrame:
    key_column = f'__{tag}_CODE_VALUE'
    label_column = f'__{tag}_CODE_LABEL'
    update_column = f'__{tag}_CODE_ADC_UPDT'
    lookup = code_dimension.select(F.col('CODE_VALUE').alias(key_column), F.col('CODE_LABEL').alias(label_column), F.col('CODE_ADC_UPDT').alias(update_column))
    joined = frame.join(F.broadcast(lookup), F.col(code_column).cast('long') == F.col(key_column), 'left')
    previous_update = F.col('_CODE_ADC_UPDT') if '_CODE_ADC_UPDT' in joined.columns else F.lit(None).cast('timestamp')
    return joined.withColumn(description_column, F.col(label_column)).withColumn('_CODE_ADC_UPDT', F.greatest(previous_update, F.col(update_column))).drop(key_column, label_column, update_column)

def _mpj_enrich_code_columns(frame: DataFrame, code_dimension: DataFrame, mappings: Dict[str, str], tag_prefix: str) -> DataFrame:
    result = frame
    for index, (code_column, description_column) in enumerate(mappings.items()):
        if code_column not in result.columns:
            continue
        result = _mpj_enrich_code_column(result, code_dimension, code_column, description_column, f'{tag_prefix}_{index}')
    if '_CODE_ADC_UPDT' not in result.columns:
        result = result.withColumn('_CODE_ADC_UPDT', F.lit(None).cast('timestamp'))
    return result

def _mpj_add_event_and_stop_fields(df: DataFrame, run_timestamp: datetime, config: MapPatientJourneyConfig) -> DataFrame:
    result = df.withColumn('EVENT_ORDER_DT_TM', F.coalesce(F.col('LOC_ARRIVE_DT_TM'), F.col('LOC_BEG_EFFECTIVE_DT_TM'), F.col('LOC_TRANSACTION_DT_TM'), F.col('LOC_ACTIVITY_DT_TM'), F.col('ELH_ADC_UPDT'))).withColumn('EVENT_ORDER_SOURCE', F.when(F.col('LOC_ARRIVE_DT_TM').isNotNull(), F.lit('LOC_ARRIVE_DT_TM')).when(F.col('LOC_BEG_EFFECTIVE_DT_TM').isNotNull(), F.lit('LOC_BEG_EFFECTIVE_DT_TM')).when(F.col('LOC_TRANSACTION_DT_TM').isNotNull(), F.lit('LOC_TRANSACTION_DT_TM')).when(F.col('LOC_ACTIVITY_DT_TM').isNotNull(), F.lit('LOC_ACTIVITY_DT_TM')).when(F.col('ELH_ADC_UPDT').isNotNull(), F.lit('ELH_ADC_UPDT')))
    order_columns = [F.col('EVENT_ORDER_DT_TM').asc_nulls_last(), F.col('LOC_TRANSACTION_DT_TM').asc_nulls_last(), F.col('LOC_ACTIVITY_DT_TM').asc_nulls_last(), F.col('LOC_BEG_EFFECTIVE_DT_TM').asc_nulls_last(), F.col('ENCNTR_LOC_HIST_ID').asc()]
    event_window = Window.partitionBy('ENCNTR_ID').orderBy(*order_columns)
    encounter_window = Window.partitionBy('ENCNTR_ID')
    tie_window = Window.partitionBy('ENCNTR_ID', 'EVENT_ORDER_DT_TM')
    cumulative_window = event_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    location_columns = ['LOCATION_CD', 'LOC_FACILITY_CD', 'LOC_BUILDING_CD', 'LOC_NURSE_UNIT_CD', 'LOC_ROOM_CD', 'LOC_BED_CD']
    result = result.withColumn('HISTORY_EVENT_SEQUENCE', F.row_number().over(event_window).cast('int')).withColumn('TOTAL_HISTORY_EVENTS', F.count(F.lit(1)).over(encounter_window).cast('int')).withColumn('IS_FIRST_EVENT', F.col('HISTORY_EVENT_SEQUENCE') == 1).withColumn('IS_LAST_EVENT', F.col('HISTORY_EVENT_SEQUENCE') == F.col('TOTAL_HISTORY_EVENTS')).withColumn('EVENT_ORDER_TIE_COUNT', F.count(F.lit(1)).over(tie_window).cast('int')).withColumn('PREV_LOCATION_CD', F.lag('LOCATION_CD').over(event_window)).withColumn('NEXT_LOCATION_CD', F.lead('LOCATION_CD').over(event_window)).withColumn('PREV_NURSE_UNIT_CD', F.lag('LOC_NURSE_UNIT_CD').over(event_window)).withColumn('NEXT_NURSE_UNIT_CD', F.lead('LOC_NURSE_UNIT_CD').over(event_window)).withColumn('_NEXT_EVENT_START', F.lead('EVENT_ORDER_DT_TM').over(event_window))
    previous_columns = []
    for column_name in location_columns:
        previous_name = f'__PREV_{column_name}'
        result = result.withColumn(previous_name, F.lag(column_name).over(event_window))
        previous_columns.append(previous_name)
    equality_checks = [F.col(column_name).eqNullSafe(F.col(previous_name)) for column_name, previous_name in zip(location_columns, previous_columns)]
    same_location = reduce(lambda left, right: left & right, equality_checks)
    result = result.withColumn('IS_PHYSICAL_LOCATION_CHANGE', F.col('IS_FIRST_EVENT') | ~same_location).withColumn('LOCATION_STOP_SEQUENCE', F.sum(F.col('IS_PHYSICAL_LOCATION_CHANGE').cast('int')).over(cumulative_window).cast('int')).drop(*previous_columns)
    cutoff = F.to_timestamp(F.lit(config.end_effective_sentinel_cutoff))
    result = result.withColumn('END_EFFECTIVE_SENTINEL_IND', F.col('LOC_END_EFFECTIVE_DT_TM').isNotNull() & (F.col('LOC_END_EFFECTIVE_DT_TM') >= cutoff)).withColumn('_FINITE_END_EFFECTIVE', F.when(F.col('LOC_END_EFFECTIVE_DT_TM').isNotNull() & (F.col('LOC_END_EFFECTIVE_DT_TM') < cutoff), F.col('LOC_END_EFFECTIVE_DT_TM'))).withColumn('LOCATION_TIME_INVALID_IND', F.col('LOC_DEPART_DT_TM').isNotNull() & F.col('EVENT_ORDER_DT_TM').isNotNull() & (F.col('LOC_DEPART_DT_TM') < F.col('EVENT_ORDER_DT_TM')) | F.col('_FINITE_END_EFFECTIVE').isNotNull() & F.col('EVENT_ORDER_DT_TM').isNotNull() & (F.col('_FINITE_END_EFFECTIVE') < F.col('EVENT_ORDER_DT_TM'))).withColumn('_VALID_NEXT_EVENT_START', F.when(F.col('_NEXT_EVENT_START').isNotNull() & (F.col('_NEXT_EVENT_START') >= F.col('EVENT_ORDER_DT_TM')), F.col('_NEXT_EVENT_START'))).withColumn('_VALID_LOC_DEPART', F.when(F.col('LOC_DEPART_DT_TM').isNotNull() & (F.col('LOC_DEPART_DT_TM') >= F.col('EVENT_ORDER_DT_TM')), F.col('LOC_DEPART_DT_TM'))).withColumn('_VALID_FINITE_END', F.when(F.col('_FINITE_END_EFFECTIVE').isNotNull() & (F.col('_FINITE_END_EFFECTIVE') >= F.col('EVENT_ORDER_DT_TM')), F.col('_FINITE_END_EFFECTIVE'))).withColumn('HISTORY_STATE_END_DT_TM', F.least('_VALID_NEXT_EVENT_START', '_VALID_LOC_DEPART', '_VALID_FINITE_END')).withColumn('HISTORY_STATE_DURATION_HOURS', F.when(F.col('EVENT_ORDER_DT_TM').isNotNull() & F.col('HISTORY_STATE_END_DT_TM').isNotNull() & (F.col('HISTORY_STATE_END_DT_TM') >= F.col('EVENT_ORDER_DT_TM')), F.round((F.unix_timestamp('HISTORY_STATE_END_DT_TM') - F.unix_timestamp('EVENT_ORDER_DT_TM')) / F.lit(3600.0), 2)))
    stop_key = ['ENCNTR_ID', 'LOCATION_STOP_SEQUENCE']
    stops = result.groupBy(*stop_key).agg(F.min('EVENT_ORDER_DT_TM').alias('LOCATION_STOP_START_DT_TM'), F.min('LOC_DEPART_DT_TM').alias('_STOP_DEPART_RAW'), F.max('_FINITE_END_EFFECTIVE').alias('_STOP_FINITE_END_RAW'))
    stop_window = Window.partitionBy('ENCNTR_ID').orderBy(F.col('LOCATION_STOP_SEQUENCE').asc())
    stop_count_window = Window.partitionBy('ENCNTR_ID')
    stops = stops.withColumn('_NEXT_STOP_START', F.lead('LOCATION_STOP_START_DT_TM').over(stop_window)).withColumn('TOTAL_LOCATION_STOPS', F.count(F.lit(1)).over(stop_count_window).cast('int')).withColumn('_VALID_STOP_DEPART', F.when(F.col('_STOP_DEPART_RAW').isNotNull() & (F.col('_STOP_DEPART_RAW') >= F.col('LOCATION_STOP_START_DT_TM')), F.col('_STOP_DEPART_RAW'))).withColumn('_VALID_STOP_FINITE_END', F.when(F.col('_STOP_FINITE_END_RAW').isNotNull() & (F.col('_STOP_FINITE_END_RAW') >= F.col('LOCATION_STOP_START_DT_TM')), F.col('_STOP_FINITE_END_RAW'))).withColumn('LOCATION_STOP_END_DT_TM', F.least('_NEXT_STOP_START', '_VALID_STOP_DEPART', '_VALID_STOP_FINITE_END')).withColumn('LOCATION_STOP_DURATION_HOURS', F.when(F.col('LOCATION_STOP_START_DT_TM').isNotNull() & F.col('LOCATION_STOP_END_DT_TM').isNotNull() & (F.col('LOCATION_STOP_END_DT_TM') >= F.col('LOCATION_STOP_START_DT_TM')), F.round((F.unix_timestamp('LOCATION_STOP_END_DT_TM') - F.unix_timestamp('LOCATION_STOP_START_DT_TM')) / F.lit(3600.0), 2))).select(*stop_key, 'TOTAL_LOCATION_STOPS', 'LOCATION_STOP_START_DT_TM', 'LOCATION_STOP_END_DT_TM', 'LOCATION_STOP_DURATION_HOURS')
    result = result.join(stops, stop_key, 'left')
    return result.withColumn('IS_CURRENT_LOCATION', F.col('IS_LAST_EVENT') & (F.col('ELH_ACTIVE_IND') == 1) & F.col('LOC_DEPART_DT_TM').isNull() & (F.col('LOC_BEG_EFFECTIVE_DT_TM').isNull() | (F.col('LOC_BEG_EFFECTIVE_DT_TM') <= F.lit(run_timestamp))) & (F.col('LOC_END_EFFECTIVE_DT_TM').isNull() | (F.col('LOC_END_EFFECTIVE_DT_TM') > F.lit(run_timestamp)))).withColumn('CURRENT_AS_OF_DT_TM', F.lit(run_timestamp).cast('timestamp')).drop('_NEXT_EVENT_START', '_FINITE_END_EFFECTIVE', '_VALID_NEXT_EVENT_START', '_VALID_LOC_DEPART', '_VALID_FINITE_END')

def _mpj_add_location_hierarchy(df: DataFrame) -> DataFrame:
    return df.withColumn('MOST_SPECIFIC_LOCATION_CD', F.coalesce(_mpj_nonzero(F.col('LOC_BED_CD')), _mpj_nonzero(F.col('LOC_ROOM_CD')), _mpj_nonzero(F.col('LOC_NURSE_UNIT_CD')), _mpj_nonzero(F.col('LOC_BUILDING_CD')), _mpj_nonzero(F.col('LOC_FACILITY_CD')), _mpj_nonzero(F.col('LOCATION_CD')))).withColumn('MOST_SPECIFIC_LOCATION_DESC', F.coalesce(_mpj_nonblank(F.col('BED_DESC')), _mpj_nonblank(F.col('ROOM_DESC')), _mpj_nonblank(F.col('NURSE_UNIT_DESC')), _mpj_nonblank(F.col('BUILDING_DESC')), _mpj_nonblank(F.col('FACILITY_DESC')), _mpj_nonblank(F.col('LOCATION_DESC')))).withColumn('MOST_SPECIFIC_LOCATION_LEVEL', F.when(_mpj_nonzero(F.col('LOC_BED_CD')).isNotNull(), F.lit('BED')).when(_mpj_nonzero(F.col('LOC_ROOM_CD')).isNotNull(), F.lit('ROOM')).when(_mpj_nonzero(F.col('LOC_NURSE_UNIT_CD')).isNotNull(), F.lit('NURSE_UNIT')).when(_mpj_nonzero(F.col('LOC_BUILDING_CD')).isNotNull(), F.lit('BUILDING')).when(_mpj_nonzero(F.col('LOC_FACILITY_CD')).isNotNull(), F.lit('FACILITY')).when(_mpj_nonzero(F.col('LOCATION_CD')).isNotNull(), F.lit('LOCATION_CD')))

def _mpj_add_encounter_timing(df: DataFrame) -> DataFrame:
    result = df.withColumn('BEST_AVAILABLE_ENCNTR_START_DT_TM', F.coalesce(F.col('ENCNTR_ARRIVE_DT_TM'), F.col('REG_DT_TM'), F.col('PRE_REG_DT_TM'), F.col('EST_ARRIVE_DT_TM'))).withColumn('BEST_AVAILABLE_ENCNTR_START_SOURCE', F.when(F.col('ENCNTR_ARRIVE_DT_TM').isNotNull(), F.lit('ENCNTR_ARRIVE_DT_TM')).when(F.col('REG_DT_TM').isNotNull(), F.lit('REG_DT_TM')).when(F.col('PRE_REG_DT_TM').isNotNull(), F.lit('PRE_REG_DT_TM')).when(F.col('EST_ARRIVE_DT_TM').isNotNull(), F.lit('EST_ARRIVE_DT_TM'))).withColumn('FACILITY_PHYSICAL_LOS_INVALID_IND', F.col('ENCNTR_ARRIVE_DT_TM').isNotNull() & F.col('ENCNTR_DEPART_DT_TM').isNotNull() & (F.col('ENCNTR_DEPART_DT_TM') < F.col('ENCNTR_ARRIVE_DT_TM'))).withColumn('FACILITY_PHYSICAL_LOS_DAYS', F.when(F.col('ENCNTR_ARRIVE_DT_TM').isNotNull() & F.col('ENCNTR_DEPART_DT_TM').isNotNull() & (F.col('ENCNTR_DEPART_DT_TM') >= F.col('ENCNTR_ARRIVE_DT_TM')), F.round((F.unix_timestamp('ENCNTR_DEPART_DT_TM') - F.unix_timestamp('ENCNTR_ARRIVE_DT_TM')) / F.lit(86400.0), 2))).withColumn('INPATIENT_CLINICAL_LOS_INVALID_IND', F.col('INPATIENT_ADMIT_DT_TM').isNotNull() & F.col('DISCH_DT_TM').isNotNull() & (F.col('DISCH_DT_TM') < F.col('INPATIENT_ADMIT_DT_TM'))).withColumn('INPATIENT_CLINICAL_LOS_DAYS', F.when(F.col('INPATIENT_ADMIT_DT_TM').isNotNull() & F.col('DISCH_DT_TM').isNotNull() & (F.col('DISCH_DT_TM') >= F.col('INPATIENT_ADMIT_DT_TM')), F.round((F.unix_timestamp('DISCH_DT_TM') - F.unix_timestamp('INPATIENT_ADMIT_DT_TM')) / F.lit(86400.0), 2))).withColumn('REGISTRATION_TO_DISCHARGE_INVALID_IND', F.col('REG_DT_TM').isNotNull() & F.col('DISCH_DT_TM').isNotNull() & (F.col('DISCH_DT_TM') < F.col('REG_DT_TM'))).withColumn('REGISTRATION_TO_DISCHARGE_DAYS', F.when(F.col('REG_DT_TM').isNotNull() & F.col('DISCH_DT_TM').isNotNull() & (F.col('DISCH_DT_TM') >= F.col('REG_DT_TM')), F.round((F.unix_timestamp('DISCH_DT_TM') - F.unix_timestamp('REG_DT_TM')) / F.lit(86400.0), 2)))
    return result.withColumn('ENCNTR_LENGTH_OF_STAY_DAYS', F.coalesce(F.col('FACILITY_PHYSICAL_LOS_DAYS'), F.col('INPATIENT_CLINICAL_LOS_DAYS'), F.col('REGISTRATION_TO_DISCHARGE_DAYS'))).withColumn('ENCNTR_LOS_SOURCE', F.when(F.col('FACILITY_PHYSICAL_LOS_DAYS').isNotNull(), F.lit('FACILITY_PHYSICAL_LOS_DAYS')).when(F.col('INPATIENT_CLINICAL_LOS_DAYS').isNotNull(), F.lit('INPATIENT_CLINICAL_LOS_DAYS')).when(F.col('REGISTRATION_TO_DISCHARGE_DAYS').isNotNull(), F.lit('REGISTRATION_TO_DISCHARGE_DAYS')))

def _mpj_add_compatibility_columns(df: DataFrame) -> DataFrame:
    """Legacy LOC_*/LOCATION_* compatibility columns, computed over physical location stops.

    These five columns predate the history-event model and are named and documented as
    physical-location facts, but they used to be plain aliases of the history-event columns,
    which also advance on service, encounter-type and care-level changes. They are now derived
    from the physical-location-change subset instead: IS_PHYSICAL_LOCATION_CHANGE marks the row
    that opens each stop, LOCATION_STOP_SEQUENCE renumbers the stops, TOTAL_LOCATION_STOPS is
    the per-encounter count and LOCATION_STOP_DURATION_HOURS is the stop duration. Rows that
    continue an existing stop carry NULL for the duration and the sequence, so that neither can
    be double counted, and 0 for the two flags. The HISTORY_* columns are unchanged and remain
    the per-history-event view.
    """
    is_location_change = F.col('IS_PHYSICAL_LOCATION_CHANGE')
    return df.withColumn('LOC_LENGTH_OF_STAY_HOURS', F.when(is_location_change, F.col('LOCATION_STOP_DURATION_HOURS'))).withColumn('LOCATION_SEQUENCE', F.when(is_location_change, F.col('LOCATION_STOP_SEQUENCE')).cast('int')).withColumn('IS_FIRST_LOCATION', (is_location_change & (F.col('LOCATION_STOP_SEQUENCE') == F.lit(1))).cast('int')).withColumn('IS_LAST_LOCATION', (is_location_change & (F.col('LOCATION_STOP_SEQUENCE') == F.col('TOTAL_LOCATION_STOPS'))).cast('int')).withColumn('TOTAL_LOCATIONS_VISITED', F.col('TOTAL_LOCATION_STOPS').cast('int'))

def _mpj_add_row_hash(df: DataFrame) -> DataFrame:
    hash_columns = [field.name for field in schema_map_patient_journey.fields if field.name not in _MPJ_VOLATILE_HASH_COLUMNS and field.name in df.columns]
    chunk_hashes: List[Column] = []
    for start in range(0, len(hash_columns), 32):
        parts: List[Column] = []
        for column_name in hash_columns[start:start + 32]:
            parts.extend([F.col(column_name).isNull(), F.col(column_name)])
        chunk_hashes.append(F.xxhash64(*parts))
    if not chunk_hashes:
        return df.withColumn('ROW_HASH', F.lit(42).cast('long'))
    return df.withColumn('ROW_HASH', F.xxhash64(*chunk_hashes))

def _mpj_build_rows(elh: DataFrame, encounter: DataFrame, code_dimension: DataFrame, run_timestamp: datetime, config: MapPatientJourneyConfig) -> DataFrame:
    events = _mpj_add_event_and_stop_fields(elh, run_timestamp, config)
    events = _mpj_enrich_code_columns(events, code_dimension, ELH_CODE_DESCRIPTION_COLUMNS, 'ELH').withColumnRenamed('_CODE_ADC_UPDT', '_ELH_CODE_ADC_UPDT')
    events = _mpj_add_location_hierarchy(events)
    encounter = _mpj_enrich_code_columns(encounter, code_dimension, ENC_CODE_DESCRIPTION_COLUMNS, 'ENC').withColumnRenamed('_CODE_ADC_UPDT', '_ENC_CODE_ADC_UPDT')
    encounter_columns = [column_name for column_name in encounter.columns if column_name != 'ENCNTR_ID']
    result = events.alias('elh').join(encounter.alias('enc'), 'ENCNTR_ID', 'left').select('elh.*', *[F.col(f'enc.{column_name}').alias(column_name) for column_name in encounter_columns])
    result = _mpj_add_encounter_timing(result)
    result = result.withColumn('ADC_UPDT', F.greatest(F.col('ELH_ADC_UPDT'), F.col('ENC_ADC_UPDT'), F.col('MRN_ADC_UPDT'), F.col('NHS_ADC_UPDT'), F.col('_ELH_CODE_ADC_UPDT'), F.col('_ENC_CODE_ADC_UPDT')))
    result = _mpj_add_compatibility_columns(result).withColumn('PIPELINE_UPDT_DT_TM', F.lit(run_timestamp).cast('timestamp'))
    result = _mpj_add_row_hash(result)
    return _mpj_schema_select(result, schema_map_patient_journey)

def _mpj_manifest_lookup(config: MapPatientJourneyConfig, build_id: str, stage_name: str, bucket_id: int=-1) -> Optional[Dict[str, object]]:
    rows = spark.table(config.build_manifest_table).where((F.col('build_id') == build_id) & (F.col('stage_name') == stage_name) & (F.col('bucket_id') == int(bucket_id))).limit(1).collect()
    return rows[0].asDict(recursive=True) if rows else None

def _mpj_validate_source_keys(elh: DataFrame) -> int:
    row = elh.agg(F.count(F.lit(1)).alias('row_count'), F.count_if(F.col('ENCNTR_LOC_HIST_ID').isNull()).alias('null_history_keys'), F.count_if(F.col('ENCNTR_ID').isNull()).alias('null_encounter_keys')).first()
    if row is None:
        raise RuntimeError('Location-history source returned no validation row')
    null_history = int(row['null_history_keys'] or 0)
    null_encounters = int(row['null_encounter_keys'] or 0)
    if null_history or null_encounters:
        raise ValueError(f'Patient-journey staging requires non-null keys: null ENCNTR_LOC_HIST_ID={null_history}, null ENCNTR_ID={null_encounters}')
    return int(row['row_count'])

def _mpj_validate_code_dimension(code_dimension: DataFrame) -> Dict[str, int]:
    row_count = int(code_dimension.count())
    distinct_count = int(code_dimension.select('CODE_VALUE').distinct().count())
    if row_count != distinct_count:
        raise ValueError(f'MILL_CODE_VALUE snapshot is not unique by CODE_VALUE: rows={row_count}, distinct={distinct_count}')
    return {'rows': row_count, 'distinct_codes': distinct_count}

def _mpj_write_partitioned_stage(df: DataFrame, table_name: str, bucket_count: int) -> None:
    df.repartition(int(bucket_count) * 8, F.col(_MPJ_INTERNAL_BUCKET_COLUMN), F.col('ENCNTR_ID')).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').partitionBy(_MPJ_INTERNAL_BUCKET_COLUMN).saveAsTable(table_name)

def _mpj_prepare_full_build_stages(config: MapPatientJourneyConfig, manifest: Dict[str, object]) -> Dict[str, object]:
    build_id = str(manifest['build_id'])
    source_versions = dict(manifest['source_versions'])
    run_timestamp = manifest['run_timestamp']
    schema_hash = str(manifest['schema_hash'])
    manifest_config = dict(manifest['manifest_config'])
    bucket_count = int(config.full_build_bucket_count)
    tables = _mpj_stage_tables(config, build_id)
    code_state = _mpj_manifest_lookup(config, build_id, 'CODE')
    if not code_state or code_state['status'] != 'COMPLETE' or (not _mpj_table_exists(tables['code'])):
        _mpj_manifest_upsert(config, build_id, 'CODE', -1, 'RUNNING', schema_hash, source_versions, run_timestamp, manifest_config)
        code_dimension = _mpj_prepare_code_dimension(_mpj_read_snapshot(config.code_value_table, source_versions[config.code_value_table]))
        metrics = _mpj_validate_code_dimension(code_dimension)
        code_dimension.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(tables['code'])
        _mpj_manifest_upsert(config, build_id, 'CODE', -1, 'COMPLETE', schema_hash, source_versions, run_timestamp, manifest_config, row_count=metrics['rows'], metrics=metrics)
    encounter_state = _mpj_manifest_lookup(config, build_id, 'ENCOUNTER')
    if not encounter_state or encounter_state['status'] != 'COMPLETE' or (not _mpj_table_exists(tables['encounter'])):
        _mpj_manifest_upsert(config, build_id, 'ENCOUNTER', -1, 'RUNNING', schema_hash, source_versions, run_timestamp, manifest_config)
        encounter = _mpj_prepare_encounter(_mpj_read_snapshot(config.encounter_table, source_versions[config.encounter_table]))
        null_encounters = int(encounter.where(F.col('ENCNTR_ID').isNull()).limit(1).count())
        if null_encounters:
            raise ValueError('MILL_ENCOUNTER contains a null ENCNTR_ID')
        aliases = _mpj_prepare_aliases(_mpj_read_snapshot(config.person_alias_table, source_versions[config.person_alias_table]))
        current_aliases = _mpj_build_current_aliases(aliases, run_timestamp)
        encounter = encounter.join(current_aliases, 'PERSON_ID', 'left')
        encounter = _mpj_add_bucket(encounter, bucket_count)
        _mpj_write_partitioned_stage(encounter, tables['encounter'], bucket_count)
        encounter_count = int(spark.table(tables['encounter']).count())
        _mpj_manifest_upsert(config, build_id, 'ENCOUNTER', -1, 'COMPLETE', schema_hash, source_versions, run_timestamp, manifest_config, row_count=encounter_count, metrics={'current_alias_semantics': 'pinned_run_timestamp'})
    elh_state = _mpj_manifest_lookup(config, build_id, 'ELH')
    if not elh_state or elh_state['status'] != 'COMPLETE' or (not _mpj_table_exists(tables['elh'])):
        _mpj_manifest_upsert(config, build_id, 'ELH', -1, 'RUNNING', schema_hash, source_versions, run_timestamp, manifest_config)
        elh = _mpj_prepare_elh(_mpj_read_snapshot(config.elh_table, source_versions[config.elh_table]))
        source_count = _mpj_validate_source_keys(elh)
        if not _mpj_table_exists(tables['elh']):
            elh = _mpj_add_bucket(elh, bucket_count)
            null_buckets = int(elh.where(F.col(_MPJ_INTERNAL_BUCKET_COLUMN).isNull()).limit(1).count())
            if null_buckets:
                raise ValueError('BUILD_BUCKET is null for at least one source row')
            _mpj_write_partitioned_stage(elh, tables['elh'], bucket_count)
        else:
            print(f"[RESUME] map_patient_journey reusing completed ELH staging table {tables['elh']}")
        staged_elh = spark.table(tables['elh'])
        null_buckets = int(staged_elh.where(F.col(_MPJ_INTERNAL_BUCKET_COLUMN).isNull()).limit(1).count())
        if null_buckets:
            raise ValueError('BUILD_BUCKET is null for at least one staged row')
        bucket_counts = {int(row[_MPJ_INTERNAL_BUCKET_COLUMN]): int(row['count']) for row in staged_elh.groupBy(_MPJ_INTERNAL_BUCKET_COLUMN).count().collect()}
        if set(bucket_counts) != set(range(bucket_count)):
            raise ValueError(f'ELH staging did not produce every expected bucket: found={sorted(bucket_counts)}')
        staged_row_count = _mpj_builtins.sum(bucket_counts.values())
        if staged_row_count != source_count:
            raise ValueError(f'ELH bucket counts do not sum to the pinned source count: buckets={staged_row_count}, source={source_count}')
        for bucket_id, expected_rows in bucket_counts.items():
            bucket_state = _mpj_manifest_lookup(config, build_id, 'BUCKET', bucket_id)
            previous_metrics = json.loads(bucket_state['metrics_json'] or '{}') if bucket_state else {}
            previous_metrics['expected_rows'] = expected_rows
            _mpj_manifest_upsert(config, build_id, 'BUCKET', bucket_id, bucket_state['status'] if bucket_state else 'PENDING', schema_hash, source_versions, run_timestamp, manifest_config, row_count=int(bucket_state['row_count']) if bucket_state and bucket_state['row_count'] is not None else None, metrics=previous_metrics, error_message=bucket_state['error_message'] if bucket_state else None)
        _mpj_manifest_upsert(config, build_id, 'ELH', -1, 'COMPLETE', schema_hash, source_versions, run_timestamp, manifest_config, row_count=source_count, metrics={'bucket_counts': bucket_counts})
    return {'tables': tables, 'source_count': int(_mpj_manifest_lookup(config, build_id, 'ELH')['row_count'])}

def _mpj_validate_bucket_rows(rows: DataFrame, expected_rows: int) -> Dict[str, int]:
    actual_rows = int(rows.count())
    null_history = int(rows.where(F.col('ENCNTR_LOC_HIST_ID').isNull()).limit(1).count())
    null_encounter = int(rows.where(F.col('ENCNTR_ID').isNull()).limit(1).count())
    duplicate = rows.groupBy('ENCNTR_LOC_HIST_ID').count().where(F.col('count') > 1).limit(1).collect()
    invalid_journey = rows.groupBy('ENCNTR_ID').agg(F.count(F.lit(1)).alias('actual_rows'), F.count_if(F.col('IS_FIRST_EVENT')).alias('first_rows'), F.count_if(F.col('IS_LAST_EVENT')).alias('last_rows'), F.min('HISTORY_EVENT_SEQUENCE').alias('min_sequence'), F.max('HISTORY_EVENT_SEQUENCE').alias('max_sequence'), F.min('TOTAL_HISTORY_EVENTS').alias('min_total'), F.max('TOTAL_HISTORY_EVENTS').alias('max_total'), F.min('TOTAL_LOCATION_STOPS').alias('min_stops'), F.max('TOTAL_LOCATION_STOPS').alias('max_stops')).where((F.col('first_rows') != 1) | (F.col('last_rows') != 1) | (F.col('min_sequence') != 1) | (F.col('max_sequence') != F.col('actual_rows')) | (F.col('min_total') != F.col('actual_rows')) | (F.col('max_total') != F.col('actual_rows')) | (F.col('min_stops') < 1) | (F.col('max_stops') > F.col('actual_rows'))).limit(1).collect()
    if actual_rows != int(expected_rows):
        raise ValueError(f'Bucket row count mismatch: actual={actual_rows}, expected={expected_rows}')
    if null_history or null_encounter or duplicate or invalid_journey:
        raise ValueError(f'Bucket validation failed: null_history={null_history}, null_encounter={null_encounter}, duplicate={duplicate[:1]}, invalid_journey={invalid_journey[:1]}')
    return {'rows': actual_rows, 'null_history_keys': null_history, 'null_encounter_keys': null_encounter}

def _mpj_write_bucket(config: MapPatientJourneyConfig, manifest: Dict[str, object], tables: Dict[str, str], bucket_id: int) -> Dict[str, int]:
    build_id = str(manifest['build_id'])
    source_versions = dict(manifest['source_versions'])
    run_timestamp = manifest['run_timestamp']
    schema_hash = str(manifest['schema_hash'])
    manifest_config = dict(manifest['manifest_config'])
    state = _mpj_manifest_lookup(config, build_id, 'BUCKET', bucket_id)
    if state is None:
        raise RuntimeError(f'Missing manifest row for bucket {bucket_id}')
    metrics = json.loads(state['metrics_json'] or '{}')
    expected_rows = int(metrics['expected_rows'])
    if state['status'] == 'COMPLETE':
        if not _mpj_table_exists(tables['buckets']):
            raise RuntimeError(f'Bucket {bucket_id} is marked complete but the bucket table is missing')
        return {'rows': int(state['row_count']), 'resumed': 1}
    _mpj_manifest_upsert(config, build_id, 'BUCKET', bucket_id, 'RUNNING', schema_hash, source_versions, run_timestamp, manifest_config, metrics=metrics)
    try:
        elh = spark.table(tables['elh']).where(F.col(_MPJ_INTERNAL_BUCKET_COLUMN) == bucket_id).drop(_MPJ_INTERNAL_BUCKET_COLUMN)
        encounter = spark.table(tables['encounter']).where(F.col(_MPJ_INTERNAL_BUCKET_COLUMN) == bucket_id).drop(_MPJ_INTERNAL_BUCKET_COLUMN)
        code_dimension = spark.table(tables['code'])
        rows = _mpj_build_rows(elh, encounter, code_dimension, run_timestamp, config).withColumn(_MPJ_INTERNAL_BUCKET_COLUMN, F.lit(bucket_id).cast('int'))
        if not _mpj_table_exists(tables['buckets']):
            rows.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').partitionBy(_MPJ_INTERNAL_BUCKET_COLUMN).saveAsTable(tables['buckets'])
        else:
            rows.write.format('delta').mode('overwrite').option('replaceWhere', f'{_MPJ_INTERNAL_BUCKET_COLUMN} = {int(bucket_id)}').saveAsTable(tables['buckets'])
        written = spark.table(tables['buckets']).where(F.col(_MPJ_INTERNAL_BUCKET_COLUMN) == bucket_id)
        validation = _mpj_validate_bucket_rows(written, expected_rows)
        validation['expected_rows'] = expected_rows
        _mpj_manifest_upsert(config, build_id, 'BUCKET', bucket_id, 'COMPLETE', schema_hash, source_versions, run_timestamp, manifest_config, row_count=validation['rows'], metrics=validation)
        return validation
    except Exception as exc:
        _mpj_manifest_upsert(config, build_id, 'BUCKET', bucket_id, 'FAILED', schema_hash, source_versions, run_timestamp, manifest_config, metrics=metrics, error_message=str(exc))
        raise

def _mpj_get_table_properties(table_name: str) -> Dict[str, str]:
    if not _mpj_table_exists(table_name):
        return {}
    return {row['key']: row['value'] for row in spark.sql(f'SHOW TBLPROPERTIES {_mpj_sql_identifier(table_name)}').collect()}

def _mpj_set_table_properties(table_name: str, properties: Dict[str, str]) -> None:
    current = _mpj_get_table_properties(table_name)
    changed = {key: str(value) for key, value in properties.items() if current.get(key) != str(value)}
    if not changed:
        return
    assignments = ', '.join((f"'{_mpj_escape_sql_string(key)}' = '{_mpj_escape_sql_string(value)}'" for key, value in changed.items()))
    spark.sql(f'ALTER TABLE {_mpj_sql_identifier(table_name)} SET TBLPROPERTIES ({assignments})')

def _mpj_target_properties(config: MapPatientJourneyConfig, build_id: Optional[str]=None, source_versions: Optional[Dict[str, int]]=None, run_timestamp: Optional[datetime]=None) -> Dict[str, str]:
    properties = {'delta.enableChangeDataFeed': 'true', 'delta.enableDeletionVectors': 'true', 'delta.enableRowTracking': 'true', 'delta.autoOptimize.optimizeWrite': 'true', 'delta.autoOptimize.autoCompact': 'true', 'delta.deletedFileRetentionDuration': 'interval 30 days', 'pipeline.map_patient_journey.schema_version': config.schema_version, 'pipeline.map_patient_journey.schema_hash': _mpj_schema_hash(), 'pipeline.map_patient_journey.transform_version': config.transform_version, 'pipeline.map_patient_journey.row_semantics': 'location_history_event_with_physical_stop_fields', 'pipeline.map_patient_journey.cdf_checkpoint': 'delta_commit_version', 'pipeline.map_patient_journey.filters': 'none', 'pipeline.map_patient_journey.temporal_alias_semantics': f'sidecar:{_MPJ_TEMPORAL_ALIAS_SIDECAR}'}
    if build_id:
        properties['pipeline.map_patient_journey.build_id'] = build_id
    if source_versions is not None:
        properties['pipeline.map_patient_journey.source_versions'] = json.dumps(source_versions, sort_keys=True)
    if run_timestamp is not None:
        properties['pipeline.map_patient_journey.current_as_of'] = run_timestamp.isoformat()
    return properties

def _mpj_apply_column_comments(table_name: str, schema: T.StructType) -> None:
    current = spark.table(table_name).schema
    current_comments = {field.name: field.metadata.get('comment', '') for field in current.fields}
    for field in schema.fields:
        desired = field.metadata.get('comment', '')
        if not desired or current_comments.get(field.name) == desired:
            continue
        spark.sql(f"ALTER TABLE {_mpj_sql_identifier(table_name)} ALTER COLUMN {_mpj_sql_column(field.name)} COMMENT '{_mpj_escape_sql_string(desired)}'")

def _mpj_set_target_metadata(table_name: str, config: MapPatientJourneyConfig, build_id: Optional[str]=None, source_versions: Optional[Dict[str, int]]=None, run_timestamp: Optional[datetime]=None) -> None:
    _mpj_set_table_properties(table_name, _mpj_target_properties(config, build_id, source_versions, run_timestamp))
    spark.sql(f"COMMENT ON TABLE {_mpj_sql_identifier(table_name)} IS '{_mpj_escape_sql_string(map_patient_journey_comment)}'")
    _mpj_apply_column_comments(
        table_name,
        (
            bronze_contract_schema(config.target_table)
            if bronze_is_primary_table(table_name)
            else spark.table(table_name).schema
        ),
    )
    if config.enable_liquid_clustering:
        try:
            spark.sql(f'ALTER TABLE {_mpj_sql_identifier(table_name)} CLUSTER BY (ENCNTR_ID, ENCNTR_LOC_HIST_ID)')
        except Exception as cluster_exc:
            print(f'[WARN] Could not configure liquid clustering: {cluster_exc}')

def _mpj_target_requires_full_rebuild(config: MapPatientJourneyConfig, state: Dict[str, Dict[str, object]], source_versions: Dict[str, int]) -> Tuple[bool, str]:
    if not _mpj_table_exists(config.target_table):
        return (True, 'target table does not exist')
    properties = _mpj_get_table_properties(config.target_table)
    if properties.get('pipeline.map_patient_journey.schema_version') != config.schema_version:
        return (True, 'schema-version property is absent or outdated')
    if properties.get('pipeline.map_patient_journey.schema_hash') != _mpj_schema_hash():
        return (True, 'schema-hash property is absent or outdated')
    expected = {
        field.name.lower(): field.dataType.simpleString()
        for field in bronze_contract_schema(config.target_table).fields
    }
    actual = {field.name.lower(): field.dataType.simpleString() for field in spark.table(config.target_table).schema.fields}
    if actual != expected:
        return (True, 'target schema does not exactly match schema v4')
    if not set(source_versions).issubset(state):
        return (True, 'source-version state is incomplete')
    for source, ending_version in source_versions.items():
        if int(state[source]['version']) > int(ending_version):
            return (True, f'stored checkpoint is ahead of {source}')
    return (False, 'target and source-version state are compatible')

def _mpj_target_has_build(config: MapPatientJourneyConfig, build_id: str) -> bool:
    if not _mpj_table_exists(config.target_table):
        return False
    properties = _mpj_get_table_properties(config.target_table)
    if properties.get('pipeline.map_patient_journey.build_id') == build_id:
        return True
    try:
        row = spark.sql(f'DESCRIBE HISTORY {_mpj_sql_identifier(config.target_table)} LIMIT 1').select('userMetadata').first()
        if row and row['userMetadata']:
            payload = json.loads(row['userMetadata'])
            return payload.get('map_patient_journey_build_id') == build_id
    except Exception:
        pass
    return False

def _mpj_publish_candidate(config: MapPatientJourneyConfig, manifest: Dict[str, object], tables: Dict[str, str]) -> Dict[str, int]:
    build_id = str(manifest['build_id'])
    source_versions = dict(manifest['source_versions'])
    run_timestamp = manifest['run_timestamp']
    schema_hash = str(manifest['schema_hash'])
    manifest_config = dict(manifest['manifest_config'])
    publish_state = _mpj_manifest_lookup(config, build_id, 'PUBLISH')
    source_count = int(_mpj_manifest_lookup(config, build_id, 'ELH')['row_count'])
    if _mpj_target_has_build(config, build_id):
        return {'rows': source_count, 'already_published': 1}
    if not publish_state or publish_state['status'] != 'CANDIDATE_COMPLETE' or (not _mpj_table_exists(tables['publish'])):
        _mpj_manifest_upsert(config, build_id, 'PUBLISH', -1, 'RUNNING', schema_hash, source_versions, run_timestamp, manifest_config)
        candidate = spark.table(tables['buckets']).drop(_MPJ_INTERNAL_BUCKET_COLUMN)
        _mpj_schema_select(candidate, schema_map_patient_journey).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('delta.enableChangeDataFeed', 'true').option('delta.enableDeletionVectors', 'true').option('delta.enableRowTracking', 'true').saveAsTable(tables['publish'])
        candidate_table = spark.table(tables['publish'])
        candidate_count = int(candidate_table.count())
        null_history = int(candidate_table.where(F.col('ENCNTR_LOC_HIST_ID').isNull()).limit(1).count())
        null_encounters = int(candidate_table.where(F.col('ENCNTR_ID').isNull()).limit(1).count())
        duplicate = candidate_table.groupBy('ENCNTR_LOC_HIST_ID').count().where(F.col('count') > 1).limit(1).collect()
        if candidate_count != source_count or null_history or null_encounters or duplicate:
            raise ValueError(f'Publish candidate validation failed: rows={candidate_count}/{source_count}, null_history={null_history}, null_encounters={null_encounters}, duplicate={duplicate[:1]}')
        _mpj_set_target_metadata(tables['publish'], config, build_id, source_versions, run_timestamp)
        _mpj_manifest_upsert(config, build_id, 'PUBLISH', -1, 'CANDIDATE_COMPLETE', schema_hash, source_versions, run_timestamp, manifest_config, row_count=candidate_count, metrics={'null_history_keys': null_history, 'null_encounter_keys': null_encounters, 'duplicate_keys': len(duplicate)})
    _mpj_manifest_upsert(config, build_id, _MPJ_MANIFEST_HEADER_STAGE, -1, 'PUBLISHING', schema_hash, source_versions, run_timestamp, manifest_config, row_count=source_count)
    metadata = json.dumps({'map_patient_journey_build_id': build_id, 'schema_version': config.schema_version, 'source_versions': source_versions}, sort_keys=True)
    commit_metadata_key = 'spark.databricks.delta.commitInfo.userMetadata'
    previous_metadata: Optional[str]
    try:
        previous_metadata = spark.conf.get(commit_metadata_key)
    except Exception:
        previous_metadata = None
    commit_metadata_applied = False
    try:
        spark.conf.set(commit_metadata_key, metadata)
        commit_metadata_applied = True
    except Exception as metadata_exc:
        metadata_reason = str(metadata_exc).splitlines()[0]
        print(
            '[WARN] Delta commit user metadata is unavailable; publication will '
            f'rely on the durable table build-id property: {metadata_reason}'
        )
    try:
        bronze_project_contract(
            spark.table(tables['publish']),
            config.target_table,
        ).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').option('delta.enableChangeDataFeed', 'true').option('delta.enableDeletionVectors', 'true').option('delta.enableRowTracking', 'true').saveAsTable(config.target_table)
        _mpj_set_target_metadata(
            config.target_table,
            config,
            build_id,
            source_versions,
            run_timestamp,
        )
    finally:
        if commit_metadata_applied:
            try:
                if previous_metadata is None:
                    spark.conf.unset(commit_metadata_key)
                else:
                    spark.conf.set(commit_metadata_key, previous_metadata)
            except Exception as restore_exc:
                restore_reason = str(restore_exc).splitlines()[0]
                print(f'[WARN] Could not restore Delta commit metadata: {restore_reason}')
    if not _mpj_target_has_build(config, build_id):
        raise RuntimeError('Canonical target publication completed without a detectable build identity')
    _mpj_manifest_upsert(config, build_id, _MPJ_MANIFEST_HEADER_STAGE, -1, 'PUBLISHED', schema_hash, source_versions, run_timestamp, manifest_config, row_count=source_count)
    return {'rows': source_count, 'already_published': 0}

def _mpj_cleanup_full_build(config: MapPatientJourneyConfig, manifest: Dict[str, object]) -> None:
    tables = _mpj_stage_tables(config, str(manifest['build_id']))
    for table_name in tables.values():
        try:
            _mpj_drop_table(table_name)
        except Exception as cleanup_exc:
            print(f'[WARN] Could not drop staging table {table_name}: {cleanup_exc}')
    try:
        _mpj_drop_table(config.build_manifest_table)
    except Exception as cleanup_exc:
        print(f'[WARN] Could not drop completed full-build manifest {config.build_manifest_table}: {cleanup_exc}')

def _mpj_run_full_build(config: MapPatientJourneyConfig, run_post_checks: bool) -> Dict[str, object]:
    _mpj_ensure_build_manifest_table(config)
    manifest = _mpj_load_or_start_manifest(config)
    build_id = str(manifest['build_id'])
    source_versions = dict(manifest['source_versions'])
    run_timestamp = manifest['run_timestamp']
    schema_hash = str(manifest['schema_hash'])
    manifest_config = dict(manifest['manifest_config'])
    _mpj_write_audit_event(config, build_id, 'STARTED', 'FULL_REBUILD', 'Starting or resuming the temporary schema-v4 full-build workspace', source_versions, {'run_timestamp': run_timestamp.isoformat()})
    if _mpj_target_has_build(config, build_id):
        completed_at = datetime.now(timezone.utc)
        _mpj_commit_pipeline_state(config, source_versions, build_id, completed_at)
        recovered_metrics: Dict[str, object] = {'full_rebuild': True, 'build_id': build_id, 'resumed_after_publish': True}
        if run_post_checks:
            recovered_metrics['post_deployment_checks'] = run_map_patient_journey_post_deployment_checks(config)
        _mpj_manifest_upsert(config, build_id, _MPJ_MANIFEST_HEADER_STAGE, -1, 'SUCCESS', schema_hash, source_versions, run_timestamp, manifest_config)
        _mpj_cleanup_full_build(config, manifest)
        return recovered_metrics
    try:
        stage_metrics = _mpj_prepare_full_build_stages(config, manifest)
        tables = stage_metrics['tables']
        completed_buckets = 0
        resumed_buckets = 0
        for bucket_id in range(int(config.full_build_bucket_count)):
            bucket_metrics = _mpj_write_bucket(config, manifest, tables, bucket_id)
            completed_buckets += 1
            resumed_buckets += int(bucket_metrics.get('resumed', 0))
            print(f"[PATIENT JOURNEY] bucket {bucket_id + 1}/{config.full_build_bucket_count} complete; rows={bucket_metrics['rows']:,}")
        publish_metrics = _mpj_publish_candidate(config, manifest, tables)
        completed_at = datetime.now(timezone.utc)
        _mpj_commit_pipeline_state(config, source_versions, build_id, completed_at)
        final_metrics: Dict[str, object] = {'full_rebuild': True, 'build_id': build_id, 'rows': int(publish_metrics['rows']), 'completed_buckets': completed_buckets, 'resumed_buckets': resumed_buckets, 'source_versions': source_versions, 'current_as_of': run_timestamp.isoformat()}
        if run_post_checks:
            final_metrics['post_deployment_checks'] = run_map_patient_journey_post_deployment_checks(config)
        _mpj_manifest_upsert(config, build_id, _MPJ_MANIFEST_HEADER_STAGE, -1, 'SUCCESS', schema_hash, source_versions, run_timestamp, manifest_config, row_count=int(publish_metrics['rows']), metrics=final_metrics)
        _mpj_write_audit_event(config, build_id, 'SUCCESS', 'FULL_REBUILD', 'Schema-v4 full build published; temporary recovery artifacts removed', source_versions, final_metrics)
        _mpj_cleanup_full_build(config, manifest)
        return final_metrics
    except Exception as exc:
        _mpj_manifest_upsert(config, build_id, _MPJ_MANIFEST_HEADER_STAGE, -1, 'BUILDING', schema_hash, source_versions, run_timestamp, manifest_config, error_message=str(exc))
        _mpj_write_audit_event(config, build_id, 'FAILED', 'FULL_REBUILD', str(exc), source_versions, {'staging_retained': True})
        raise

def _mpj_empty_encounter_keys() -> DataFrame:
    return spark.createDataFrame([], 'ENCNTR_ID long')

def _mpj_union_encounter_keys(frames: Sequence[Optional[DataFrame]]) -> DataFrame:
    valid = [frame.select(F.col('ENCNTR_ID').cast('long').alias('ENCNTR_ID')) for frame in frames if frame is not None]
    if not valid:
        return _mpj_empty_encounter_keys()
    return reduce(lambda left, right: left.unionByName(right), valid).where(F.col('ENCNTR_ID').isNotNull()).distinct()

def _mpj_source_code_matches(raw: DataFrame, encounter_column: str, available_code_columns: Sequence[str], changed_codes: Sequence[int]) -> DataFrame:
    code_columns = [F.coalesce(F.col(column_name).cast('long'), F.lit(-9223372036854775807)) for column_name in available_code_columns if column_name in raw.columns]
    if not code_columns or not changed_codes:
        return _mpj_empty_encounter_keys()
    changed_array = F.array(*[F.lit(int(value)).cast('long') for value in changed_codes])
    return raw.where(F.size(F.array_intersect(F.array(*code_columns), changed_array)) > 0).select(F.col(encounter_column).cast('long').alias('ENCNTR_ID')).where(F.col('ENCNTR_ID').isNotNull()).distinct()

def _mpj_collect_affected_encounters(config: MapPatientJourneyConfig, state: Dict[str, Dict[str, object]], source_versions: Dict[str, int]) -> Tuple[DataFrame, Dict[str, object]]:
    starts = {source: int(state[source]['version']) + 1 for source in source_versions}
    elh_cdf = _mpj_read_cdf(config.elh_table, starts[config.elh_table], source_versions[config.elh_table])
    enc_cdf = _mpj_read_cdf(config.encounter_table, starts[config.encounter_table], source_versions[config.encounter_table])
    alias_cdf = _mpj_read_cdf(config.person_alias_table, starts[config.person_alias_table], source_versions[config.person_alias_table])
    code_cdf = _mpj_read_cdf(config.code_value_table, starts[config.code_value_table], source_versions[config.code_value_table])
    elh_keys = elh_cdf.select(F.col('ENCNTR_ID').cast('long').alias('ENCNTR_ID')) if elh_cdf is not None else None
    enc_keys = enc_cdf.select(F.col('ENCNTR_ID').cast('long').alias('ENCNTR_ID')) if enc_cdf is not None else None
    alias_keys: Optional[DataFrame] = None
    if alias_cdf is not None:
        changed_people = alias_cdf.select(F.col('PERSON_ID').cast('long').alias('PERSON_ID')).where(F.col('PERSON_ID').isNotNull()).distinct()
        encounter_snapshot = _mpj_read_snapshot(config.encounter_table, source_versions[config.encounter_table])
        alias_keys = encounter_snapshot.alias('enc').join(changed_people.alias('people'), F.col('enc.PERSON_ID').cast('long') == F.col('people.PERSON_ID'), 'inner').select(F.col('enc.ENCNTR_ID').cast('long').alias('ENCNTR_ID'))
    changed_codes: List[int] = []
    code_keys: Optional[DataFrame] = None
    if code_cdf is not None:
        changed_codes = [int(row['CODE_VALUE']) for row in code_cdf.select(F.col('CODE_VALUE').cast('long').alias('CODE_VALUE')).where(F.col('CODE_VALUE').isNotNull()).distinct().collect()]
        if len(changed_codes) > int(config.code_change_full_rebuild_threshold):
            raise _MPJFullRebuildRequired(f'{len(changed_codes)} code values changed, exceeding the threshold {config.code_change_full_rebuild_threshold}')
        if changed_codes:
            elh_snapshot = _mpj_read_snapshot(config.elh_table, source_versions[config.elh_table])
            enc_snapshot = _mpj_read_snapshot(config.encounter_table, source_versions[config.encounter_table])
            code_keys = _mpj_union_encounter_keys([_mpj_source_code_matches(elh_snapshot, 'ENCNTR_ID', ELH_RAW_CODE_COLUMNS, changed_codes), _mpj_source_code_matches(enc_snapshot, 'ENCNTR_ID', ENC_RAW_CODE_COLUMNS, changed_codes)])
    affected = _mpj_union_encounter_keys([elh_keys, enc_keys, alias_keys, code_keys])
    return (affected, {'changed_codes': len(changed_codes), 'starting_versions': starts, 'ending_versions': source_versions})

def _mpj_filter_by_affected(raw: DataFrame, affected: DataFrame, encounter_column: str='ENCNTR_ID') -> DataFrame:
    return raw.alias('source').join(affected.alias('affected'), F.col(f'source.{encounter_column}').cast('long') == F.col('affected.ENCNTR_ID'), 'inner').select('source.*')

def _mpj_build_incremental_rows(config: MapPatientJourneyConfig, source_versions: Dict[str, int], affected: DataFrame, run_timestamp: datetime) -> DataFrame:
    elh = _mpj_prepare_elh(_mpj_filter_by_affected(_mpj_read_snapshot(config.elh_table, source_versions[config.elh_table]), affected))
    encounter = _mpj_prepare_encounter(_mpj_filter_by_affected(_mpj_read_snapshot(config.encounter_table, source_versions[config.encounter_table]), affected))
    person_ids = encounter.select('PERSON_ID').where(F.col('PERSON_ID').isNotNull()).distinct()
    aliases = _mpj_prepare_aliases(_mpj_read_snapshot(config.person_alias_table, source_versions[config.person_alias_table]))
    current_aliases = _mpj_build_current_aliases(aliases, run_timestamp, person_ids)
    encounter = encounter.join(current_aliases, 'PERSON_ID', 'left')
    code_dimension = _mpj_prepare_code_dimension(_mpj_read_snapshot(config.code_value_table, source_versions[config.code_value_table]))
    return _mpj_build_rows(elh, encounter, code_dimension, run_timestamp, config)

def _mpj_delete_change_rows(keys: DataFrame) -> DataFrame:
    return _mpj_schema_select(keys.select(F.col('ENCNTR_LOC_HIST_ID').cast('long').alias('ENCNTR_LOC_HIST_ID'), F.col('ENCNTR_ID').cast('long').alias('ENCNTR_ID')), schema_map_patient_journey).withColumn('_source_change_type', F.lit('delete'))

def _mpj_validate_change_set(changes: DataFrame) -> Dict[str, int]:
    action_counts = {str(row['_source_change_type']): int(row['count']) for row in changes.groupBy('_source_change_type').count().collect()}
    invalid_action = changes.where(~F.col('_source_change_type').isin('upsert', 'delete')).limit(1).collect()
    duplicate = changes.groupBy('ENCNTR_LOC_HIST_ID').count().where(F.col('count') > 1).limit(1).collect()
    null_keys = changes.where(F.col('ENCNTR_LOC_HIST_ID').isNull() | F.col('ENCNTR_ID').isNull()).limit(1).collect()
    if invalid_action or duplicate or null_keys:
        raise ValueError(f'Incremental change-set validation failed: invalid_action={invalid_action[:1]}, duplicate={duplicate[:1]}, null_keys={null_keys[:1]}')
    return {'upsert_rows': action_counts.get('upsert', 0), 'delete_rows': action_counts.get('delete', 0)}

def _mpj_merge_target(changes: DataFrame, config: MapPatientJourneyConfig) -> None:
    target = DeltaTable.forName(spark, config.target_table)
    delete_keys = (
        changes.filter(F.col('_source_change_type') == 'delete')
        .select('ENCNTR_LOC_HIST_ID')
        .dropDuplicates(['ENCNTR_LOC_HIST_ID'])
    )
    upserts = bronze_project_contract(
        changes.filter(F.col('_source_change_type') == 'upsert')
        .drop('_source_change_type'),
        config.target_table,
    )
    if delete_keys.take(1):
        target.alias('t').merge(
            delete_keys.alias('s'),
            't.ENCNTR_LOC_HIST_ID = s.ENCNTR_LOC_HIST_ID',
        ).whenMatchedDelete().execute()
    if upserts.take(1):
        assignments = {
            column_name: F.col(f's.{column_name}')
            for column_name in upserts.columns
        }
        comparisons = ' OR '.join(
            f'NOT (t.`{column_name}` <=> s.`{column_name}`)'
            for column_name in upserts.columns
            if column_name != 'ENCNTR_LOC_HIST_ID'
        )
        target.alias('t').merge(
            upserts.alias('s'),
            't.ENCNTR_LOC_HIST_ID = s.ENCNTR_LOC_HIST_ID',
        ).whenMatchedUpdate(
            condition=comparisons or 'false',
            set=assignments,
        ).whenNotMatchedInsert(values=assignments).execute()

def _mpj_run_incremental(config: MapPatientJourneyConfig) -> Dict[str, object]:
    run_id = str(uuid.uuid4())
    run_timestamp = datetime.now(timezone.utc)
    source_versions = _mpj_capture_source_versions(config)
    state = _mpj_read_pipeline_state(config)
    affected, trigger_metrics = _mpj_collect_affected_encounters(config, state, source_versions)
    tables = _mpj_incremental_tables(config, run_id)
    try:
        affected.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(tables['affected'])
        affected = spark.table(tables['affected'])
        affected_count = int(affected.count())
        if affected_count > int(config.incremental_encounter_full_rebuild_threshold):
            raise _MPJFullRebuildRequired(f'Incremental blast radius is {affected_count:,} encounters, exceeding {config.incremental_encounter_full_rebuild_threshold:,}')
        if affected_count == 0:
            completed_at = datetime.now(timezone.utc)
            _mpj_commit_pipeline_state(config, source_versions, run_id, completed_at)
            metrics = {'full_rebuild': False, 'affected_encounters': 0, 'upsert_rows': 0, 'delete_rows': 0, **trigger_metrics}
            _mpj_write_audit_event(config, run_id, 'SUCCESS', 'INCREMENTAL', 'No affected encounters; checkpoints advanced', source_versions, metrics)
            return metrics
        new_rows = _mpj_build_incremental_rows(config, source_versions, affected, run_timestamp)
        new_rows.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(tables['new_rows'])
        new_rows = spark.table(tables['new_rows'])
        existing_keys = spark.table(config.target_table).join(affected, 'ENCNTR_ID', 'inner').select('ENCNTR_LOC_HIST_ID', 'ENCNTR_ID')
        new_keys = new_rows.select('ENCNTR_LOC_HIST_ID').distinct()
        deletes = _mpj_delete_change_rows(existing_keys.join(new_keys, 'ENCNTR_LOC_HIST_ID', 'left_anti'))
        changes = new_rows.withColumn('_source_change_type', F.lit('upsert')).unionByName(deletes, allowMissingColumns=True)
        changes.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(tables['changes'])
        changes = spark.table(tables['changes'])
        change_metrics = _mpj_validate_change_set(changes)
        if change_metrics['upsert_rows'] or change_metrics['delete_rows']:
            _mpj_merge_target(changes, config)
        completed_at = datetime.now(timezone.utc)
        _mpj_commit_pipeline_state(config, source_versions, run_id, completed_at)
        metrics = {'full_rebuild': False, 'affected_encounters': affected_count, **change_metrics, **trigger_metrics}
        _mpj_write_audit_event(config, run_id, 'SUCCESS', 'INCREMENTAL', 'Incremental affected-encounter rebuild completed', source_versions, metrics)
        return metrics
    except Exception as exc:
        _mpj_write_audit_event(config, run_id, 'FAILED', 'INCREMENTAL', str(exc), source_versions, trigger_metrics)
        if not isinstance(exc, _MPJFullRebuildRequired) and _mpj_is_cdf_gap_exception(exc):
            raise _MPJFullRebuildRequired('A CDF retention/configuration gap surfaced during incremental materialization; route to the restartable full build') from exc
        raise
    finally:
        for table_name in tables.values():
            try:
                _mpj_drop_table(table_name)
            except Exception as cleanup_exc:
                print(f'[WARN] Could not drop incremental stage {table_name}: {cleanup_exc}')

def run_map_patient_journey_post_deployment_checks(config: MapPatientJourneyConfig=MAP_PATIENT_JOURNEY_CONFIG) -> Dict[str, object]:
    if not _mpj_table_exists(config.target_table):
        raise AssertionError('Single-layer patient-journey target does not exist')
    state = _mpj_read_pipeline_state(config)
    required_sources = {
        config.elh_table, config.encounter_table,
        config.person_alias_table, config.code_value_table,
    }
    if not required_sources.issubset(state):
        raise AssertionError('Patient-journey source-version state is incomplete')
    target = spark.table(config.target_table)
    target_schema = {
        field.name: field.dataType.simpleString()
        for field in target.schema.fields
    }
    expected_schema = {
        field.name: field.dataType.simpleString()
        for field in bronze_contract_schema(config.target_table).fields
    }
    if target_schema != expected_schema:
        raise AssertionError(
            'Patient-journey schema differs from the retained-column contract'
        )
    duplicate_keys = int(
        target.groupBy('ENCNTR_LOC_HIST_ID').count()
        .filter(F.col('count') > 1).count()
    )
    null_keys = int(
        target.filter(F.col('ENCNTR_LOC_HIST_ID').isNull()).count()
    )
    if duplicate_keys or null_keys:
        raise AssertionError(
            f'Patient-journey key failure: duplicates={duplicate_keys}, nulls={null_keys}'
        )
    properties = _mpj_get_table_properties(config.target_table)
    if properties.get('pipeline.map_patient_journey.schema_version') != config.schema_version:
        raise AssertionError('Patient-journey schema-version property is wrong')
    if properties.get('pipeline.map_patient_journey.schema_hash') != _mpj_schema_hash():
        raise AssertionError('Patient-journey schema-hash property is wrong')
    build_id = properties.get('pipeline.map_patient_journey.build_id')
    if not build_id:
        raise AssertionError('Patient-journey build identity is absent')
    target_count = int(target.count())
    result: Dict[str, object] = {
        'schema_columns': len(target_schema),
        'schema_version': config.schema_version,
        'schema_hash': _mpj_schema_hash(),
        'state_sources': len(state),
        'build_id': build_id,
        'target_rows': target_count,
        'duplicate_keys': duplicate_keys,
        'null_keys': null_keys,
        'source_reconciliation': run_map_patient_journey_legacy_reconciliation(
            config
        ),
    }
    print(f'[OK] map_patient_journey post-deployment checks passed: {result}')
    return result
def run_map_patient_journey_legacy_reconciliation(config: MapPatientJourneyConfig=MAP_PATIENT_JOURNEY_CONFIG, legacy_table: Optional[str]=None) -> Dict[str, object]:
    """Reconcile the single-layer target to the pinned active Barts source scope."""
    if not _mpj_table_exists(config.target_table):
        raise AssertionError('Patient-journey target does not exist')
    state = _mpj_read_pipeline_state(config)
    if config.elh_table not in state or config.encounter_table not in state:
        raise AssertionError('Pinned source versions are unavailable for reconciliation')
    elh = _mpj_read_snapshot(
        config.elh_table, int(state[config.elh_table]['version'])
    ).select(
        F.col('ENCNTR_LOC_HIST_ID').cast('long').alias('ENCNTR_LOC_HIST_ID'),
        F.col('ENCNTR_ID').cast('long').alias('ENCNTR_ID'),
        F.col('ACTIVE_IND').cast('long').alias('ELH_ACTIVE_IND'),
        F.col('Trust').cast('string').alias('TRUST'),
    )
    encounter = _mpj_read_snapshot(
        config.encounter_table, int(state[config.encounter_table]['version'])
    ).select(
        F.col('ENCNTR_ID').cast('long').alias('ENCNTR_ID'),
        F.col('ACTIVE_IND').cast('long').alias('ENC_ACTIVE_IND'),
    )
    expected = (
        elh.where(
            F.col('TRUST').eqNullSafe(F.lit('Barts'))
            & (F.col('ELH_ACTIVE_IND') == 1)
        )
        .join(
            encounter.where(F.col('ENC_ACTIVE_IND') == 1).select('ENCNTR_ID'),
            'ENCNTR_ID',
            'inner',
        )
        .select('ENCNTR_LOC_HIST_ID')
        .dropDuplicates()
    )
    actual = (
        spark.table(config.target_table)
        .select('ENCNTR_LOC_HIST_ID')
        .dropDuplicates()
    )
    missing = int(expected.join(
        actual, 'ENCNTR_LOC_HIST_ID', 'left_anti'
    ).count())
    extra = int(actual.join(
        expected, 'ENCNTR_LOC_HIST_ID', 'left_anti'
    ).count())
    result = {
        'expected_active_source_keys': int(expected.count()),
        'target_keys': int(actual.count()),
        'missing_target_keys': missing,
        'extra_target_keys': extra,
    }
    if missing or extra:
        raise AssertionError(f'Patient-journey source reconciliation failed: {result}')
    return result
def process_patient_journey_incremental(force_full_rebuild: bool=False, run_post_checks: bool=False, config: MapPatientJourneyConfig=MAP_PATIENT_JOURNEY_CONFIG) -> Dict[str, object]:
    _mpj_ensure_control_tables(config)
    active_manifest = _mpj_active_manifest(config)
    if active_manifest is not None:
        print(f"[RESUME] map_patient_journey continuing build {active_manifest['build_id']} from its first incomplete stage/bucket")
        return _mpj_run_full_build(config, run_post_checks)
    source_versions = _mpj_capture_source_versions(config)
    state = _mpj_read_pipeline_state(config)
    requires_full, reason = _mpj_target_requires_full_rebuild(config, state, source_versions)
    if force_full_rebuild or requires_full:
        print(f"[FULL REBUILD] map_patient_journey: {('explicit request' if force_full_rebuild else reason)}")
        return _mpj_run_full_build(config, run_post_checks)
    try:
        return _mpj_run_incremental(config)
    except _MPJFullRebuildRequired as exc:
        print(f'[RECOVERY] map_patient_journey: {exc}')
        return _mpj_run_full_build(config, run_post_checks=True)

def run_map_patient_journey_update(force_full_rebuild: bool=False, run_post_checks: bool=False, config: MapPatientJourneyConfig=MAP_PATIENT_JOURNEY_CONFIG) -> Dict[str, object]:
    return process_patient_journey_incremental(force_full_rebuild=force_full_rebuild, run_post_checks=run_post_checks, config=config)

try:
    _targets = ['4_prod.bronze.map_patient_journey']
    if not _pipeline_resume_skip_component('map_patient_journey', _targets):
        _pipeline_run_recoverable('map_patient_journey', _PIPELINE_FULL_REFRESH, lambda: run_map_patient_journey_update(force_full_rebuild=False, run_post_checks=False), lambda: run_map_patient_journey_update(force_full_rebuild=True, run_post_checks=True))
        _PIPELINE_UPDATED_TARGETS.extend(_targets)
        _pipeline_mark_component_complete('map_patient_journey', _targets)
        _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_patient_journey'})
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

_pipeline_component_start("map_implant_details")
_pipeline_shared_update_table = globals().get('update_table')
_pipeline_shared_table_exists = globals().get('table_exists')
_pipeline_shared_get_max_timestamp = globals().get('get_max_timestamp')
_pipeline_shared_has_cdf_enabled = globals().get('has_cdf_enabled')
_pipeline_shared_get_incremental = globals().get('get_incremental_data_with_cdf')
globals().pop('_map_updates_original_update_table', None)
globals().pop('_map_medical_personnel_original_update_table', None)
globals().pop('_MAP_UPDATES_ORIGINAL_UPDATE_TABLE', None)

from typing import Dict, List, Optional
from uuid import uuid4
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, DecimalType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType
IMPLANT_SOURCE_TABLE = '4_prod.raw.mill_clinical_event'
IMPLANT_TARGET_TABLE = '4_prod.bronze.map_implant_details'
IMPLANT_EVENT_TARGET_TABLE = '4_prod.bronze.map_implant_detail_events'
DEVICE_MAPPING_TABLE = '4_prod.bronze.map_device_mapping'
CHECKPOINT_TABLE = '6_mgmt.bronze.map_pipeline_checkpoints'
PIPELINE_NAME = 'map_implant_details_v2'
TRUST_NAME = 'Barts'
IMPLANT_DESCRIPTION_CD = 71837900
IMPLANT_FORM_CD = 71839410
PROCEDURE_CONFIRM_CD = 3016928
PROCEDURE_DESCRIPTION_CD = 3016930
ATTRIBUTE_CODE_TO_COLUMN: Dict[int, str] = {71837626: 'QUANTITY', 325617013: 'SIDE_OF_PROCEDURE', 325617132: 'COMPATIBILITY', 71837688: 'EXPIRY_DATE_RAW', 325617070: 'SCRUB_NURSE_READ', 325617147: 'SURGEON_CONFIRM', 71837522: 'MANUFACTURER', 325617106: 'CONFIRM_MANUFACTURER', 71837900: 'IMPLANT_DESCRIPTION', 325617081: 'SCRUB_NURSE_CONFIRMS', 71837685: 'STERILISATION_DATE_RAW', 325617123: 'CONFIRM_STERILITY', 71837524: 'CATALOGUE_NUMBER', 71837624: 'SERIAL_NUMBER', 71837686: 'SIZE'}
ATTRIBUTE_TITLE_TO_COLUMN: Dict[str, str] = {'SN - IM - Quantity': 'QUANTITY', 'SN - IM - Confirm Side of Procedure': 'SIDE_OF_PROCEDURE', 'SN - IM - Confirm Compatibility': 'COMPATIBILITY', 'SN - IM - Expiry Date': 'EXPIRY_DATE_RAW', 'SN - IM - Scrub Nurse Read': 'SCRUB_NURSE_READ', 'SN - IM - Surgeon Confirm Implant': 'SURGEON_CONFIRM', 'SN - IM - Manufacturer': 'MANUFACTURER', 'SN - IM - Confirm Manufacturer': 'CONFIRM_MANUFACTURER', 'SN - IM - Implant Description': 'IMPLANT_DESCRIPTION', 'SN - IM - Scrub Nurse Confirms': 'SCRUB_NURSE_CONFIRMS', 'SN - IM - Sterilisation Date': 'STERILISATION_DATE_RAW', 'SN - IM - Confirm Sterility': 'CONFIRM_STERILITY', 'SN - IM - Catalogue Number': 'CATALOGUE_NUMBER', 'SN - IM - Serial Number': 'SERIAL_NUMBER', 'SN - IM - Size': 'SIZE'}
ATTRIBUTE_COLUMNS: List[str] = list(dict.fromkeys(ATTRIBUTE_CODE_TO_COLUMN.values()))
IMPLANT_RELEVANT_CODES = list(ATTRIBUTE_CODE_TO_COLUMN) + [IMPLANT_FORM_CD]
CLINICAL_CHANGE_CODES = IMPLANT_RELEVANT_CODES + [PROCEDURE_CONFIRM_CD, PROCEDURE_DESCRIPTION_CD]
GROUP_KEYS = ['ENCNTR_ID', 'PARENT_EVENT_ID', 'COLLATING_SEQ']
MAPPING_COLUMNS = ['SNOMED_DEVICE_CONCEPT_ID', 'SNOMED_DEVICE_CONCEPT_NAME', 'DEVICE_TYPE', 'MAPPING_CONFIDENCE', 'GMDN_CODE', 'GMDN_NAME', 'MAPPING_LAYER', 'CONFIDENCE_TIER', 'MAPPING_SOURCE', 'MAPPING_CANDIDATE_COUNT', 'MAPPING_CONFLICT_IND', 'MAPPING_ROW_HASH']
SOURCE_ATTRIBUTE_EVENT_SCHEMA = StructType([StructField('CLINICAL_EVENT_ID', LongType(), True), StructField('EVENT_ID', LongType(), True), StructField('EVENT_CD', LongType(), True), StructField('EVENT_TITLE_TEXT', StringType(), True), StructField('EVENT_TAG', StringType(), True), StructField('RESULT_VAL', StringType(), True), StructField('ATTRIBUTE_NAME', StringType(), True), StructField('ATTRIBUTE_VALUE', StringType(), True), StructField('VALUE_SOURCE', StringType(), True), StructField('VALUE_CONFLICT_IND', BooleanType(), True), StructField('ADC_UPDT', TimestampType(), True), StructField('VALID_FROM_DT_TM', TimestampType(), True), StructField('VALID_UNTIL_DT_TM', TimestampType(), True), StructField('RESULT_STATUS_CD', LongType(), True), StructField('RECORD_STATUS_CD', LongType(), True)])
schema_map_implant_details_v2 = StructType([StructField('EVENT_ID', LongType(), False), StructField('ENCNTR_ID', LongType(), True), StructField('PERSON_ID', LongType(), True), StructField('PRIMARY_PROCEDURE', StringType(), True), StructField('PROCEDURE_CAT', LongType(), True), StructField('PERFORMED_PRSNL_ID', LongType(), True), StructField('CLINSIG_UPDT_DT_TM', TimestampType(), True), StructField('CATALOGUE_NUMBER', StringType(), True), StructField('COMPATIBILITY', StringType(), True), StructField('CONFIRM_MANUFACTURER', StringType(), True), StructField('CONFIRM_STERILITY', StringType(), True), StructField('EXPIRY_DATE', TimestampType(), True), StructField('IMPLANT_DESCRIPTION', StringType(), True), StructField('MANUFACTURER', StringType(), True), StructField('QUANTITY', StringType(), True), StructField('SCRUB_NURSE_CONFIRMS', StringType(), True), StructField('SCRUB_NURSE_READ', StringType(), True), StructField('SERIAL_NUMBER', StringType(), True), StructField('SIDE_OF_PROCEDURE', StringType(), True), StructField('SIZE', StringType(), True), StructField('STERILISATION_DATE', TimestampType(), True), StructField('SURGEON_CONFIRM', StringType(), True), StructField('GS1_IDENTIFIER', StringType(), True), StructField('GS1_PRODUCTION_DATE', TimestampType(), True), StructField('GS1_EXPIRY_DATE', TimestampType(), True), StructField('GS1_BATCH_NUMBER', StringType(), True), StructField('GS1_SERIAL_NUMBER', StringType(), True), StructField('HIBCC_DEVICE_ID', StringType(), True), StructField('HIBCC_PROD_ID', StringType(), True), StructField('ADC_UPDT', TimestampType(), True), StructField('SNOMED_DEVICE_CONCEPT_ID', LongType(), True), StructField('SNOMED_DEVICE_CONCEPT_NAME', StringType(), True), StructField('DEVICE_TYPE', StringType(), True), StructField('MAPPING_CONFIDENCE', DoubleType(), True), StructField('BASE_CLINICAL_EVENT_ID', LongType(), True), StructField('PARENT_EVENT_ID', LongType(), True), StructField('COLLATING_SEQ', StringType(), True), StructField('IMPLANT_SEQUENCE', IntegerType(), True), StructField('IMPLANT_FORM_EVENT_ID', LongType(), True), StructField('EVENT_START_DT_TM', TimestampType(), True), StructField('EVENT_END_DT_TM', TimestampType(), True), StructField('IMPLANT_DT_TM', TimestampType(), True), StructField('VALID_FROM_DT_TM', TimestampType(), True), StructField('VALID_UNTIL_DT_TM', TimestampType(), True), StructField('RESULT_STATUS_CD', LongType(), True), StructField('RECORD_STATUS_CD', LongType(), True), StructField('BASE_EVENT_ADC_UPDT', TimestampType(), True), StructField('SOURCE_MAX_ADC_UPDT', TimestampType(), True), StructField('CONTRIBUTOR_SYSTEM_CD', LongType(), True), StructField('ORGANIZATION_ID', LongType(), True), StructField('UPDT_CNT', LongType(), True), StructField('BASE_CURRENT_VERSION_COUNT', IntegerType(), True), StructField('QUANTITY_RAW', StringType(), True), StructField('QUANTITY_NUMERIC', DecimalType(18, 4), True), StructField('QUANTITY_PARSE_STATUS', StringType(), True), StructField('EXPIRY_DATE_RAW', StringType(), True), StructField('STERILISATION_DATE_RAW', StringType(), True), StructField('GROUPING_METHOD', StringType(), True), StructField('GROUPING_AMBIGUOUS_IND', BooleanType(), True), StructField('GROUPING_CANDIDATE_COUNT', IntegerType(), True), StructField('VALUE_CONFLICT_COUNT', IntegerType(), True), StructField('SOURCE_ATTRIBUTE_EVENTS', ArrayType(SOURCE_ATTRIBUTE_EVENT_SCHEMA), True), StructField('PROCEDURE_EVENT_ID', LongType(), True), StructField('PROCEDURE_PARENT_EVENT_ID', LongType(), True), StructField('PROCEDURE_MATCH_METHOD', StringType(), True), StructField('GMDN_CODE', LongType(), True), StructField('GMDN_NAME', StringType(), True), StructField('MAPPING_LAYER', StringType(), True), StructField('CONFIDENCE_TIER', StringType(), True), StructField('MAPPING_SOURCE', StringType(), True), StructField('MAPPING_CANDIDATE_COUNT', IntegerType(), True), StructField('MAPPING_CONFLICT_IND', BooleanType(), True), StructField('UDI_DI', StringType(), True), StructField('UDI_STANDARD', StringType(), True), StructField('BEST_EXPIRY_DATE', TimestampType(), True), StructField('EXPIRY_DATE_SOURCE', StringType(), True), StructField('EXPIRY_DATE_CONFLICT_IND', BooleanType(), True), StructField('SOURCE_ROW_HASH', StringType(), True), StructField('MAPPING_ROW_HASH', StringType(), True)])
schema_map_implant_detail_events = StructType([StructField('CLINICAL_EVENT_ID', LongType(), False), StructField('EVENT_ID', LongType(), True), StructField('ENCNTR_ID', LongType(), True), StructField('PERSON_ID', LongType(), True), StructField('PARENT_EVENT_ID', LongType(), True), StructField('COLLATING_SEQ', StringType(), True), StructField('EVENT_CD', LongType(), True), StructField('EVENT_TITLE_TEXT', StringType(), True), StructField('EVENT_TAG', StringType(), True), StructField('RESULT_VAL', StringType(), True), StructField('CATALOG_CD', LongType(), True), StructField('ATTRIBUTE_NAME', StringType(), True), StructField('ATTRIBUTE_VALUE', StringType(), True), StructField('VALUE_SOURCE', StringType(), True), StructField('VALUE_CONFLICT_IND', BooleanType(), True), StructField('EVENT_START_DT_TM', TimestampType(), True), StructField('EVENT_END_DT_TM', TimestampType(), True), StructField('PERFORMED_DT_TM', TimestampType(), True), StructField('CLINSIG_UPDT_DT_TM', TimestampType(), True), StructField('PERFORMED_PRSNL_ID', LongType(), True), StructField('VALID_FROM_DT_TM', TimestampType(), True), StructField('VALID_UNTIL_DT_TM', TimestampType(), True), StructField('RESULT_STATUS_CD', LongType(), True), StructField('RECORD_STATUS_CD', LongType(), True), StructField('CONTRIBUTOR_SYSTEM_CD', LongType(), True), StructField('ORGANIZATION_ID', LongType(), True), StructField('UPDT_CNT', LongType(), True), StructField('ADC_UPDT', TimestampType(), True), StructField('TRUST', StringType(), True), StructField('CURRENT_VERSION_CANDIDATE_COUNT', IntegerType(), True), StructField('SOURCE_ROW_HASH', StringType(), True)])

def _sql_name(table_name: str) -> str:
    return '.'.join((chr(96) + part + chr(96) for part in table_name.split('.')))

def _table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def _nonblank(column):
    return F.when(column.isNotNull() & (F.length(F.trim(column)) > 0), column)

def _coalesced_result_value(result_column, tag_column):
    return F.coalesce(_nonblank(result_column), _nonblank(tag_column))

def _table_properties(table_name: str) -> Dict[str, str]:
    if not _table_exists(table_name):
        return {}
    return {row['key']: row['value'] for row in spark.sql(f'SHOW TBLPROPERTIES {_sql_name(table_name)}').collect()}

def _current_delta_version(table_name: str) -> int:
    row = DeltaTable.forName(spark, table_name).history(1).select('version').first()
    return int(row['version'])

def _align_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    expressions = []
    source_columns = set(df.columns)
    for field in schema.fields:
        if field.name in source_columns:
            expressions.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            expressions.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(*expressions)

def _hash_columns(df: DataFrame, columns: List[str], output_column: str) -> DataFrame:
    available = [F.col(name) for name in columns if name in df.columns]
    return df.withColumn(output_column, F.sha2(F.to_json(F.struct(*available)), 256))

def ensure_checkpoint_table() -> None:
    spark.sql(f"\n        CREATE TABLE IF NOT EXISTS {_sql_name(CHECKPOINT_TABLE)} (\n            PIPELINE_NAME STRING NOT NULL,\n            SOURCE_TABLE STRING NOT NULL,\n            LAST_PROCESSED_VERSION BIGINT NOT NULL,\n            UPDATED_AT TIMESTAMP NOT NULL,\n            RUN_ID STRING\n        )\n        USING DELTA\n        TBLPROPERTIES (\n            'delta.enableChangeDataFeed' = 'true',\n            'comment' = 'Delta CDF checkpoints for Map Pipeline replacement functions'\n        )\n    ")

def get_checkpoint_version(source_table: str) -> Optional[int]:
    ensure_checkpoint_table()
    row = spark.table(CHECKPOINT_TABLE).filter((F.col('PIPELINE_NAME') == PIPELINE_NAME) & (F.col('SOURCE_TABLE') == source_table)).orderBy(F.col('UPDATED_AT').desc()).select('LAST_PROCESSED_VERSION').first()
    return None if row is None else int(row['LAST_PROCESSED_VERSION'])

def set_checkpoint_version(source_table: str, version: int, run_id: str) -> None:
    ensure_checkpoint_table()
    update_df = spark.createDataFrame([(PIPELINE_NAME, source_table, int(version), run_id)], 'PIPELINE_NAME string, SOURCE_TABLE string, LAST_PROCESSED_VERSION long, RUN_ID string').withColumn('UPDATED_AT', F.current_timestamp())
    DeltaTable.forName(spark, CHECKPOINT_TABLE).alias('t').merge(update_df.alias('s'), 't.PIPELINE_NAME = s.PIPELINE_NAME AND t.SOURCE_TABLE = s.SOURCE_TABLE').whenMatchedUpdate(set={'LAST_PROCESSED_VERSION': 's.LAST_PROCESSED_VERSION', 'UPDATED_AT': 's.UPDATED_AT', 'RUN_ID': 's.RUN_ID'}).whenNotMatchedInsertAll().execute()

def ensure_implant_target_schema() -> None:
    target_schema = bronze_contract_schema(IMPLANT_TARGET_TABLE)
    if not _table_exists(IMPLANT_TARGET_TABLE):
        DeltaTable.createIfNotExists(spark).tableName(IMPLANT_TARGET_TABLE).addColumns(target_schema).comment('Current surgical implant records with deterministic source grouping, parsed identifiers, and event-level device mapping.').property('delta.enableChangeDataFeed', 'true').property('delta.enableRowTracking', 'true').property('delta.enableTypeWidening', 'true').execute()
        return
    existing = {
        field.name: field
        for field in spark.table(IMPLANT_TARGET_TABLE).schema.fields
    }
    for field in target_schema.fields:
        if field.name not in existing:
            spark.sql(f'ALTER TABLE {_sql_name(IMPLANT_TARGET_TABLE)} ADD COLUMN {field.name} {field.dataType.simpleString()}')
    snomed_field = existing.get('SNOMED_DEVICE_CONCEPT_ID')
    if snomed_field is not None and isinstance(snomed_field.dataType, IntegerType):
        properties = _table_properties(IMPLANT_TARGET_TABLE)
        if properties.get('delta.enableTypeWidening') != 'true':
            spark.sql(f"ALTER TABLE {_sql_name(IMPLANT_TARGET_TABLE)} SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
        spark.sql(f'ALTER TABLE {_sql_name(IMPLANT_TARGET_TABLE)} ALTER COLUMN SNOMED_DEVICE_CONCEPT_ID TYPE BIGINT')

def ensure_implant_event_target_schema() -> None:
    if not _table_exists(IMPLANT_EVENT_TARGET_TABLE):
        DeltaTable.createIfNotExists(spark).tableName(IMPLANT_EVENT_TARGET_TABLE).addColumns(schema_map_implant_detail_events).comment('Current long-form SurgiNet implant events. Preserves orphan, ambiguous, and unpivoted source attributes for audit and reprocessing.').property('delta.enableChangeDataFeed', 'true').property('delta.enableRowTracking', 'true').execute()
        return
    existing = {field.name for field in spark.table(IMPLANT_EVENT_TARGET_TABLE).schema.fields}
    for field in schema_map_implant_detail_events.fields:
        if field.name not in existing:
            spark.sql(f'ALTER TABLE {_sql_name(IMPLANT_EVENT_TARGET_TABLE)} ADD COLUMN {field.name} {field.dataType.simpleString()}')

def _attribute_name_expression():
    code_items = []
    for key, value in ATTRIBUTE_CODE_TO_COLUMN.items():
        code_items.extend([F.lit(int(key)), F.lit(value)])
    title_items = []
    for key, value in ATTRIBUTE_TITLE_TO_COLUMN.items():
        title_items.extend([F.lit(key), F.lit(value)])
    code_map = F.create_map(*code_items)
    title_map = F.create_map(*title_items)
    return F.when(F.col('EVENT_CD') == IMPLANT_FORM_CD, F.lit('IMPLANT_FORM')).otherwise(F.coalesce(F.element_at(code_map, F.col('EVENT_CD')), F.element_at(title_map, F.col('EVENT_TITLE_TEXT'))))

def _latest_current_versions(df: DataFrame) -> DataFrame:
    current = df.filter(F.col('VALID_UNTIL_DT_TM').isNull() | (F.col('VALID_UNTIL_DT_TM') > F.current_timestamp()))
    window = Window.partitionBy('EVENT_ID').orderBy(F.col('VALID_FROM_DT_TM').desc_nulls_last(), F.col('ADC_UPDT').desc_nulls_last(), F.col('CLINSIG_UPDT_DT_TM').desc_nulls_last(), F.col('CLINICAL_EVENT_ID').desc_nulls_last())
    count_window = Window.partitionBy('EVENT_ID')
    return current.withColumn('_CURRENT_VERSION_COUNT', F.count(F.lit(1)).over(count_window)).withColumn('_CURRENT_RN', F.row_number().over(window)).filter(F.col('_CURRENT_RN') == 1).drop('_CURRENT_RN')

def _scope_to_encounters(df: DataFrame, affected_encounters: Optional[DataFrame]) -> DataFrame:
    if affected_encounters is None:
        return df
    return df.join(F.broadcast(affected_encounters.select('ENCNTR_ID').distinct()), 'ENCNTR_ID', 'left_semi')

def get_clinical_event_change_scope(force_full_refresh: bool=False) -> Dict:
    current_version = _current_delta_version(IMPLANT_SOURCE_TABLE)
    checkpoint_version = get_checkpoint_version(IMPLANT_SOURCE_TABLE)
    if force_full_refresh or checkpoint_version is None:
        return {'full_refresh': True, 'has_changes': True, 'affected_encounters': None, 'ending_version': current_version, 'checkpoint_version': checkpoint_version, 'reason': 'forced_full_refresh' if force_full_refresh else 'checkpoint_not_found'}
    if checkpoint_version >= current_version:
        return {'full_refresh': False, 'has_changes': False, 'affected_encounters': None, 'ending_version': current_version, 'checkpoint_version': checkpoint_version, 'reason': 'already_current'}
    try:
        changes = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', checkpoint_version + 1).option('endingVersion', current_version).table(IMPLANT_SOURCE_TABLE)
        relevant = changes.filter((F.col('Trust') == TRUST_NAME) & (F.col('EVENT_CD').isin(CLINICAL_CHANGE_CODES) | F.col('EVENT_TITLE_TEXT').isin(list(ATTRIBUTE_TITLE_TO_COLUMN))))
        affected = relevant.filter(F.col('ENCNTR_ID').isNotNull()).select('ENCNTR_ID').distinct()
        if _table_exists(IMPLANT_TARGET_TABLE) and 'VALID_UNTIL_DT_TM' in spark.table(IMPLANT_TARGET_TABLE).columns:
            due_expiry = spark.table(IMPLANT_TARGET_TABLE).filter(F.col('VALID_UNTIL_DT_TM').isNotNull() & (F.col('VALID_UNTIL_DT_TM') <= F.current_timestamp())).select('ENCNTR_ID').distinct()
            affected = affected.unionByName(due_expiry).distinct()
        has_changes = len(affected.take(1)) > 0
        return {'full_refresh': False, 'has_changes': has_changes, 'affected_encounters': affected, 'ending_version': current_version, 'checkpoint_version': checkpoint_version, 'reason': 'cdf_changes'}
    except Exception as exc:
        print(f'[WARN] Clinical-event CDF range is unavailable; using a full reconciliation. Checkpoint={checkpoint_version}, current={current_version}, error={exc}')
        return {'full_refresh': True, 'has_changes': True, 'affected_encounters': None, 'ending_version': current_version, 'checkpoint_version': checkpoint_version, 'reason': 'cdf_unavailable_full_reconciliation'}

def build_implant_detail_events(affected_encounters: Optional[DataFrame]=None) -> DataFrame:
    source = spark.table(IMPLANT_SOURCE_TABLE).filter(F.col('Trust') == TRUST_NAME)
    source = _scope_to_encounters(source, affected_encounters)
    source = source.filter(F.col('EVENT_CD').isin(IMPLANT_RELEVANT_CODES) | F.col('EVENT_TITLE_TEXT').isin(list(ATTRIBUTE_TITLE_TO_COLUMN)))
    source = _latest_current_versions(source)
    result_value = _nonblank(F.col('RESULT_VAL'))
    tag_value = _nonblank(F.col('EVENT_TAG'))
    result = source.withColumn('ATTRIBUTE_NAME', _attribute_name_expression()).withColumn('ATTRIBUTE_VALUE', F.coalesce(result_value, tag_value)).withColumn('VALUE_SOURCE', F.when(result_value.isNotNull(), F.lit('RESULT_VAL')).when(tag_value.isNotNull(), F.lit('EVENT_TAG'))).withColumn('VALUE_CONFLICT_IND', result_value.isNotNull() & tag_value.isNotNull() & (result_value != tag_value)).withColumnRenamed('Trust', 'TRUST').withColumn('CURRENT_VERSION_CANDIDATE_COUNT', F.col('_CURRENT_VERSION_COUNT').cast('int')).drop('_CURRENT_VERSION_COUNT')
    hash_columns = ['CLINICAL_EVENT_ID', 'EVENT_ID', 'ENCNTR_ID', 'PERSON_ID', 'PARENT_EVENT_ID', 'COLLATING_SEQ', 'EVENT_CD', 'EVENT_TITLE_TEXT', 'EVENT_TAG', 'RESULT_VAL', 'ATTRIBUTE_NAME', 'ATTRIBUTE_VALUE', 'VALUE_SOURCE', 'VALUE_CONFLICT_IND', 'EVENT_START_DT_TM', 'EVENT_END_DT_TM', 'PERFORMED_DT_TM', 'CLINSIG_UPDT_DT_TM', 'VALID_FROM_DT_TM', 'VALID_UNTIL_DT_TM', 'RESULT_STATUS_CD', 'RECORD_STATUS_CD', 'ADC_UPDT']
    result = _hash_columns(result, hash_columns, 'SOURCE_ROW_HASH')
    return _align_to_schema(result, schema_map_implant_detail_events)

def sync_implant_detail_events(source_df: DataFrame, affected_encounters: Optional[DataFrame], full_refresh: bool) -> Dict[str, int]:
    ensure_implant_event_target_schema()
    source_count = source_df.count()
    duplicate_exists = source_df.groupBy('CLINICAL_EVENT_ID').count().filter(F.col('count') > 1).limit(1).count() > 0
    if duplicate_exists:
        raise ValueError('Duplicate CLINICAL_EVENT_ID values found in implant event source')
    target = DeltaTable.forName(spark, IMPLANT_EVENT_TARGET_TABLE)
    if full_refresh:
        target.alias('t').merge(source_df.alias('s'), 't.CLINICAL_EVENT_ID = s.CLINICAL_EVENT_ID').whenMatchedUpdateAll(condition='NOT (t.SOURCE_ROW_HASH <=> s.SOURCE_ROW_HASH)').whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute()
    else:
        affected = affected_encounters.select('ENCNTR_ID').distinct()
        target.alias('t').merge(affected.alias('s'), 't.ENCNTR_ID = s.ENCNTR_ID').whenMatchedDelete().execute()
        if source_count:
            source_df.write.mode('append').saveAsTable(IMPLANT_EVENT_TARGET_TABLE)
    return {'event_source_rows': source_count}

def _source_event_array():
    return F.struct(F.col('CLINICAL_EVENT_ID'), F.col('EVENT_ID'), F.col('EVENT_CD'), F.col('EVENT_TITLE_TEXT'), F.col('EVENT_TAG'), F.col('RESULT_VAL'), F.col('ATTRIBUTE_NAME'), F.col('ATTRIBUTE_VALUE'), F.col('VALUE_SOURCE'), F.col('VALUE_CONFLICT_IND'), F.col('ADC_UPDT'), F.col('VALID_FROM_DT_TM'), F.col('VALID_UNTIL_DT_TM'), F.col('RESULT_STATUS_CD'), F.col('RECORD_STATUS_CD'))

def _parse_encoded_date(column_name: str):
    digits = F.regexp_extract(F.col(column_name), '1:(\\d{8})', 1)
    return F.when(F.length(digits) == 8, F.try_to_timestamp(digits, F.lit("yyyyMMdd")))

def _add_quantity_columns(df: DataFrame) -> DataFrame:
    normalized_column = "_QUANTITY_NORMALIZED"
    result = df.withColumn(
        normalized_column,
        F.regexp_replace(F.trim(F.col("QUANTITY_RAW")), ",", "."),
    )
    parsed = F.when(
        F.col(normalized_column).rlike(r"^[+-]?\d+(?:\.\d+)?$"),
        F.expr(f"try_cast(`{normalized_column}` AS DECIMAL(18, 4))"),
    )
    return (
        result
        .withColumn("QUANTITY", F.col("QUANTITY_RAW"))
        .withColumn("QUANTITY_NUMERIC", parsed)
        .withColumn(
            "QUANTITY_PARSE_STATUS",
            F.when(
                F.col("QUANTITY_RAW").isNull()
                | (F.length(F.trim(F.col("QUANTITY_RAW"))) == 0),
                F.lit("MISSING"),
            )
            .when(parsed.isNotNull(), F.lit("PARSED"))
            .otherwise(F.lit("UNPARSED")),
        )
        .drop(normalized_column)
    )
def _normalize_barcode(column):
    return F.regexp_replace(F.regexp_replace(F.regexp_replace(F.trim(column), '^\\]C1', ''), '\\((\\d{2})\\)', '$1'), '[\\r\\n]', '')

def _regex_value(column, pattern: str):
    extracted = F.regexp_extract(column, pattern, 1)
    return F.when(F.length(extracted) > 0, extracted)

def _parse_gs1_date(value_column):
    return F.when(F.length(value_column) == 6, F.try_to_timestamp(value_column, F.lit("yyMMdd")))

def add_udi_columns(df: DataFrame) -> DataFrame:
    serial = _normalize_barcode(F.col('SERIAL_NUMBER'))
    catalogue = _normalize_barcode(F.col('CATALOGUE_NUMBER'))
    gs1_di = F.coalesce(_regex_value(serial, '(?:^|[^0-9])01([0-9]{14})'), _regex_value(catalogue, '(?:^|[^0-9])01([0-9]{14})'))
    gs1_production_raw = F.coalesce(_regex_value(serial, '(?:^|\\x1D|01[0-9]{14}|17[0-9]{6})11([0-9]{6})(?=\\x1D|10|17|21|$)'), _regex_value(catalogue, '(?:^|\\x1D|01[0-9]{14}|17[0-9]{6})11([0-9]{6})(?=\\x1D|10|17|21|$)'))
    gs1_expiry_raw = F.coalesce(_regex_value(serial, '(?:^|\\x1D|01[0-9]{14}|11[0-9]{6})17([0-9]{6})(?=\\x1D|10|11|21|$)'), _regex_value(catalogue, '(?:^|\\x1D|01[0-9]{14}|11[0-9]{6})17([0-9]{6})(?=\\x1D|10|11|21|$)'))
    gs1_batch = F.coalesce(_regex_value(serial, '(?:^|\\x1D|01[0-9]{14}|11[0-9]{6}|17[0-9]{6})10([^\\x1D]+?)(?=\\x1D|$)'), _regex_value(catalogue, '(?:^|\\x1D|01[0-9]{14}|11[0-9]{6}|17[0-9]{6})10([^\\x1D]+?)(?=\\x1D|$)'))
    gs1_serial = F.coalesce(_regex_value(serial, '(?:^|\\x1D|01[0-9]{14}|11[0-9]{6}|17[0-9]{6})21([^\\x1D]+?)(?=\\x1D|$)'), _regex_value(catalogue, '(?:^|\\x1D|01[0-9]{14}|11[0-9]{6}|17[0-9]{6})21([^\\x1D]+?)(?=\\x1D|$)'))
    hibcc_di = F.coalesce(_regex_value(serial, '^\\+(?!\\$\\$)(.+)$'), _regex_value(catalogue, '^\\+(?!\\$\\$)(.+)$'))
    hibcc_pi = F.coalesce(_regex_value(serial, '^\\+\\$\\$(.+)$'), _regex_value(catalogue, '^\\+\\$\\$(.+)$'))
    result = df.withColumn('GS1_IDENTIFIER', gs1_di).withColumn('GS1_PRODUCTION_DATE', _parse_gs1_date(gs1_production_raw)).withColumn('GS1_EXPIRY_DATE', _parse_gs1_date(gs1_expiry_raw)).withColumn('GS1_BATCH_NUMBER', gs1_batch).withColumn('GS1_SERIAL_NUMBER', gs1_serial).withColumn('HIBCC_DEVICE_ID', hibcc_di).withColumn('HIBCC_PROD_ID', hibcc_pi).withColumn('UDI_DI', F.coalesce(gs1_di, hibcc_di)).withColumn('UDI_STANDARD', F.when(gs1_di.isNotNull(), F.lit('GS1')).when(hibcc_di.isNotNull(), F.lit('HIBCC')))
    expiry_conflict = F.col('EXPIRY_DATE').isNotNull() & F.col('GS1_EXPIRY_DATE').isNotNull() & (F.to_date('EXPIRY_DATE') != F.to_date('GS1_EXPIRY_DATE'))
    return result.withColumn('BEST_EXPIRY_DATE', F.coalesce(F.col('EXPIRY_DATE'), F.col('GS1_EXPIRY_DATE'))).withColumn('EXPIRY_DATE_SOURCE', F.when(F.col('EXPIRY_DATE').isNotNull() & F.col('GS1_EXPIRY_DATE').isNotNull() & ~expiry_conflict, F.lit('SOURCE_AND_GS1')).when(F.col('EXPIRY_DATE').isNotNull(), F.lit('SOURCE_FIELD')).when(F.col('GS1_EXPIRY_DATE').isNotNull(), F.lit('GS1'))).withColumn('EXPIRY_DATE_CONFLICT_IND', expiry_conflict)

def _build_procedure_candidates(affected_encounters: Optional[DataFrame]) -> DataFrame:
    source = spark.table(IMPLANT_SOURCE_TABLE).filter(F.col('Trust') == TRUST_NAME)
    source = _scope_to_encounters(source, affected_encounters)
    source = source.filter(F.col('EVENT_CD').isin([PROCEDURE_CONFIRM_CD, PROCEDURE_DESCRIPTION_CD]))
    source = _latest_current_versions(source)
    confirmations = source.filter(F.col('EVENT_CD') == PROCEDURE_CONFIRM_CD).withColumn('_CONFIRM_VALUE', _coalesced_result_value(F.col('RESULT_VAL'), F.col('EVENT_TAG'))).filter(F.upper(F.trim(F.col('_CONFIRM_VALUE'))) == 'YES').select('ENCNTR_ID', F.col('EVENT_ID').alias('CONFIRM_EVENT_ID'), F.col('PARENT_EVENT_ID').alias('PROCEDURE_PARENT_EVENT_ID'), F.col('EVENT_END_DT_TM').alias('CONFIRM_EVENT_DT_TM'), F.col('ADC_UPDT').alias('CONFIRM_ADC_UPDT'))
    descriptions = source.filter(F.col('EVENT_CD') == PROCEDURE_DESCRIPTION_CD).select('ENCNTR_ID', F.col('EVENT_ID').alias('PROCEDURE_EVENT_ID'), F.col('PARENT_EVENT_ID').alias('DESCRIPTION_PARENT_EVENT_ID'), F.col('CATALOG_CD').alias('PROCEDURE_CAT'), _coalesced_result_value(F.col('RESULT_VAL'), F.col('EVENT_TAG')).alias('PRIMARY_PROCEDURE'), F.col('EVENT_END_DT_TM').alias('PROCEDURE_EVENT_DT_TM'), F.col('ADC_UPDT').alias('PROCEDURE_ADC_UPDT'))
    pair_window = Window.partitionBy('ENCNTR_ID', 'CONFIRM_EVENT_ID').orderBy(F.col('PROCEDURE_EVENT_ID').desc_nulls_last())
    return confirmations.alias('c').join(descriptions.alias('d'), (F.col('c.ENCNTR_ID') == F.col('d.ENCNTR_ID')) & (F.col('c.PROCEDURE_PARENT_EVENT_ID') == F.col('d.DESCRIPTION_PARENT_EVENT_ID')) & (F.col('d.PROCEDURE_EVENT_ID') < F.col('c.CONFIRM_EVENT_ID')), 'inner').select(F.col('c.ENCNTR_ID').alias('ENCNTR_ID'), 'CONFIRM_EVENT_ID', F.col('c.PROCEDURE_PARENT_EVENT_ID').alias('PROCEDURE_PARENT_EVENT_ID'), 'CONFIRM_EVENT_DT_TM', 'CONFIRM_ADC_UPDT', 'PROCEDURE_EVENT_ID', 'PROCEDURE_CAT', 'PRIMARY_PROCEDURE', 'PROCEDURE_EVENT_DT_TM', 'PROCEDURE_ADC_UPDT').withColumn('_PAIR_RN', F.row_number().over(pair_window)).filter(F.col('_PAIR_RN') == 1).drop('_PAIR_RN')

def _attach_primary_procedure(base_df: DataFrame, affected_encounters: Optional[DataFrame]) -> DataFrame:
    candidates = _build_procedure_candidates(affected_encounters)
    joined = base_df.alias('b').join(candidates.alias('p'), 'ENCNTR_ID', 'left').withColumn('_SAME_PARENT', (F.col('b.PARENT_EVENT_ID') == F.col('p.PROCEDURE_PARENT_EVENT_ID')).cast('int')).withColumn('_TIME_DISTANCE', F.abs(F.unix_timestamp(F.col('b.EVENT_END_DT_TM')) - F.unix_timestamp(F.col('p.PROCEDURE_EVENT_DT_TM'))))
    selection_window = Window.partitionBy(F.col('b.EVENT_ID')).orderBy(F.col('_SAME_PARENT').desc_nulls_last(), F.col('_TIME_DISTANCE').asc_nulls_last(), F.col('p.CONFIRM_EVENT_ID').desc_nulls_last(), F.col('p.PROCEDURE_EVENT_ID').desc_nulls_last())
    return joined.withColumn('_PROCEDURE_RN', F.row_number().over(selection_window)).filter(F.col('_PROCEDURE_RN') == 1).select('b.*', F.col('p.PRIMARY_PROCEDURE').alias('PRIMARY_PROCEDURE'), F.col('p.PROCEDURE_CAT').alias('PROCEDURE_CAT'), F.col('p.PROCEDURE_EVENT_ID').alias('PROCEDURE_EVENT_ID'), F.col('p.PROCEDURE_PARENT_EVENT_ID').alias('PROCEDURE_PARENT_EVENT_ID'), F.greatest(F.col('p.PROCEDURE_ADC_UPDT'), F.col('p.CONFIRM_ADC_UPDT')).alias('_PROCEDURE_MAX_ADC_UPDT'), F.when(F.col('p.PROCEDURE_EVENT_ID').isNull(), F.lit(None).cast('string')).when(F.col('_SAME_PARENT') == 1, F.lit('SAME_PARENT')).when(F.col('_TIME_DISTANCE').isNotNull(), F.lit('NEAREST_EVENT_TIME')).otherwise(F.lit('LATEST_CONFIRMATION_EVENT')).alias('PROCEDURE_MATCH_METHOD'))

def build_implant_details_base(implant_events_df: DataFrame, affected_encounters: Optional[DataFrame]) -> DataFrame:
    base = implant_events_df.filter(F.col('EVENT_CD') == IMPLANT_DESCRIPTION_CD)
    base_key_counts = base.groupBy(*GROUP_KEYS).agg(F.count(F.lit(1)).cast('int').alias('GROUPING_CANDIDATE_COUNT'))
    base = base.join(base_key_counts, GROUP_KEYS, 'left')
    attributes = implant_events_df.filter(F.col('ATTRIBUTE_NAME').isin(ATTRIBUTE_COLUMNS))
    latest_attribute_window = Window.partitionBy(*GROUP_KEYS, 'ATTRIBUTE_NAME').orderBy(F.col('ADC_UPDT').desc_nulls_last(), F.col('CLINSIG_UPDT_DT_TM').desc_nulls_last(), F.col('VALID_FROM_DT_TM').desc_nulls_last(), F.col('CLINICAL_EVENT_ID').desc_nulls_last())
    latest_attributes = attributes.withColumn('_ATTRIBUTE_RN', F.row_number().over(latest_attribute_window)).filter(F.col('_ATTRIBUTE_RN') == 1).drop('_ATTRIBUTE_RN')
    pivoted = latest_attributes.groupBy(*GROUP_KEYS).pivot('ATTRIBUTE_NAME', ATTRIBUTE_COLUMNS).agg(F.first('ATTRIBUTE_VALUE'))
    attribute_summary = attributes.groupBy(*GROUP_KEYS).agg(F.max('ADC_UPDT').alias('_ATTRIBUTE_MAX_ADC_UPDT'), F.sum(F.col('VALUE_CONFLICT_IND').cast('int')).cast('int').alias('VALUE_CONFLICT_COUNT'), F.sort_array(F.collect_list(_source_event_array())).alias('SOURCE_ATTRIBUTE_EVENTS'))
    form_events = implant_events_df.filter(F.col('EVENT_CD') == IMPLANT_FORM_CD).select(F.col('EVENT_ID').alias('IMPLANT_FORM_EVENT_ID'), F.col('ENCNTR_ID').alias('_FORM_ENCNTR_ID')).dropDuplicates(['IMPLANT_FORM_EVENT_ID'])
    result = base.alias('b').join(pivoted.alias('a'), GROUP_KEYS, 'left').join(attribute_summary.alias('s'), GROUP_KEYS, 'left').join(form_events.alias('f'), (F.col('b.PARENT_EVENT_ID') == F.col('f.IMPLANT_FORM_EVENT_ID')) & (F.col('b.ENCNTR_ID') == F.col('f._FORM_ENCNTR_ID')), 'left').select(F.col('b.EVENT_ID').alias('EVENT_ID'), F.col('b.ENCNTR_ID').alias('ENCNTR_ID'), F.col('b.PERSON_ID').alias('PERSON_ID'), F.col('b.PERFORMED_PRSNL_ID').alias('PERFORMED_PRSNL_ID'), F.col('b.CLINSIG_UPDT_DT_TM').alias('CLINSIG_UPDT_DT_TM'), *[F.coalesce(F.col(f'a.{name}'), F.col('b.ATTRIBUTE_VALUE') if name == 'IMPLANT_DESCRIPTION' else F.lit(None)).alias(name) for name in ATTRIBUTE_COLUMNS], F.col('b.CLINICAL_EVENT_ID').alias('BASE_CLINICAL_EVENT_ID'), F.col('b.PARENT_EVENT_ID').alias('PARENT_EVENT_ID'), F.col('b.COLLATING_SEQ').alias('COLLATING_SEQ'), F.expr('try_cast(b.COLLATING_SEQ as int)').alias('IMPLANT_SEQUENCE'), F.col('f.IMPLANT_FORM_EVENT_ID').alias('IMPLANT_FORM_EVENT_ID'), F.col('b.EVENT_START_DT_TM').alias('EVENT_START_DT_TM'), F.col('b.EVENT_END_DT_TM').alias('EVENT_END_DT_TM'), F.col('b.EVENT_END_DT_TM').alias('IMPLANT_DT_TM'), F.col('b.VALID_FROM_DT_TM').alias('VALID_FROM_DT_TM'), F.col('b.VALID_UNTIL_DT_TM').alias('VALID_UNTIL_DT_TM'), F.col('b.RESULT_STATUS_CD').alias('RESULT_STATUS_CD'), F.col('b.RECORD_STATUS_CD').alias('RECORD_STATUS_CD'), F.col('b.ADC_UPDT').alias('BASE_EVENT_ADC_UPDT'), F.col('b.CONTRIBUTOR_SYSTEM_CD').alias('CONTRIBUTOR_SYSTEM_CD'), F.col('b.ORGANIZATION_ID').alias('ORGANIZATION_ID'), F.col('b.UPDT_CNT').alias('UPDT_CNT'), F.col('b.CURRENT_VERSION_CANDIDATE_COUNT').alias('BASE_CURRENT_VERSION_COUNT'), F.col('b.GROUPING_CANDIDATE_COUNT').alias('GROUPING_CANDIDATE_COUNT'), F.when(F.col('b.GROUPING_CANDIDATE_COUNT') == 1, F.lit('PARENT_COLLATING_SEQ')).when(F.col('b.GROUPING_CANDIDATE_COUNT') > 1, F.lit('AMBIGUOUS_PARENT_COLLATING_SEQ')).otherwise(F.lit('BASE_ONLY')).alias('GROUPING_METHOD'), (F.coalesce(F.col('b.GROUPING_CANDIDATE_COUNT'), F.lit(0)) != 1).alias('GROUPING_AMBIGUOUS_IND'), F.coalesce(F.col('s.VALUE_CONFLICT_COUNT'), F.lit(0)).cast('int').alias('VALUE_CONFLICT_COUNT'), F.col('s.SOURCE_ATTRIBUTE_EVENTS').alias('SOURCE_ATTRIBUTE_EVENTS'), F.col('s._ATTRIBUTE_MAX_ADC_UPDT').alias('_ATTRIBUTE_MAX_ADC_UPDT'))
    result = _attach_primary_procedure(result, affected_encounters)
    result = result.withColumn('QUANTITY_RAW', F.col('QUANTITY')).withColumn('EXPIRY_DATE', _parse_encoded_date('EXPIRY_DATE_RAW')).withColumn('STERILISATION_DATE', _parse_encoded_date('STERILISATION_DATE_RAW'))
    result = _add_quantity_columns(result)
    result = add_udi_columns(result)
    result = result.withColumn('SOURCE_MAX_ADC_UPDT', F.greatest(F.col('BASE_EVENT_ADC_UPDT'), F.col('_ATTRIBUTE_MAX_ADC_UPDT'), F.col('_PROCEDURE_MAX_ADC_UPDT'))).withColumn('ADC_UPDT', F.col('SOURCE_MAX_ADC_UPDT')).drop('_ATTRIBUTE_MAX_ADC_UPDT', '_PROCEDURE_MAX_ADC_UPDT')
    for field_name, data_type in [('SNOMED_DEVICE_CONCEPT_ID', LongType()), ('SNOMED_DEVICE_CONCEPT_NAME', StringType()), ('DEVICE_TYPE', StringType()), ('MAPPING_CONFIDENCE', DoubleType()), ('GMDN_CODE', LongType()), ('GMDN_NAME', StringType()), ('MAPPING_LAYER', StringType()), ('CONFIDENCE_TIER', StringType()), ('MAPPING_SOURCE', StringType()), ('MAPPING_CANDIDATE_COUNT', IntegerType()), ('MAPPING_CONFLICT_IND', BooleanType()), ('MAPPING_ROW_HASH', StringType())]:
        result = result.withColumn(field_name, F.lit(None).cast(data_type))
    base_hash_columns = [field.name for field in schema_map_implant_details_v2.fields if field.name not in MAPPING_COLUMNS + ['SOURCE_ROW_HASH']]
    result = _hash_columns(result, base_hash_columns, 'SOURCE_ROW_HASH')
    return _align_to_schema(result, schema_map_implant_details_v2)

def merge_implant_details_base(source_df: DataFrame, affected_encounters: Optional[DataFrame], full_refresh: bool) -> Dict[str, int]:
    ensure_implant_target_schema()
    source_df = bronze_project_contract(
        source_df, IMPLANT_TARGET_TABLE
    )
    source_count = source_df.count()
    duplicate_exists = source_df.groupBy('EVENT_ID').count().filter(F.col('count') > 1).limit(1).count() > 0
    if duplicate_exists:
        None
        raise ValueError('Duplicate EVENT_ID values found in implant detail source')
    target = DeltaTable.forName(spark, IMPLANT_TARGET_TABLE)
    target_columns = spark.table(IMPLANT_TARGET_TABLE).columns
    base_managed_columns = [name for name in target_columns if name not in MAPPING_COLUMNS and name != 'EVENT_ID']
    update_values = {name: f's.{name}' for name in base_managed_columns}
    insert_values = {name: f's.{name}' for name in target_columns}
    merge_builder = target.alias('t').merge(source_df.alias('s'), 't.EVENT_ID = s.EVENT_ID').whenMatchedUpdate(condition='NOT (t.SOURCE_ROW_HASH <=> s.SOURCE_ROW_HASH)', set=update_values).whenNotMatchedInsert(values=insert_values)
    if full_refresh:
        merge_builder = merge_builder.whenNotMatchedBySourceDelete()
    merge_builder.execute()
    deleted_count = 0
    if not full_refresh and affected_encounters is not None:
        affected_target_ids = spark.table(IMPLANT_TARGET_TABLE).join(affected_encounters.select('ENCNTR_ID').distinct(), 'ENCNTR_ID', 'inner').select('EVENT_ID')
        stale_ids = affected_target_ids.join(source_df.select('EVENT_ID'), 'EVENT_ID', 'left_anti').distinct()
        deleted_count = stale_ids.count()
        if deleted_count:
            DeltaTable.forName(spark, IMPLANT_TARGET_TABLE).alias('t').merge(stale_ids.alias('s'), 't.EVENT_ID = s.EVENT_ID').whenMatchedDelete().execute()
        None
    None
    return {'implant_source_rows': source_count, 'stale_target_rows_deleted': deleted_count}

def get_device_mapping_lookup(event_ids: Optional[DataFrame]=None) -> DataFrame:
    mapping = spark.table(DEVICE_MAPPING_TABLE)
    if event_ids is not None:
        mapping = mapping.join(event_ids.select('EVENT_ID').distinct(), 'EVENT_ID', 'inner')
    layer_rank = F.when(F.col('mapping_layer') == 'LAYER1_GS1_GUDID', 1).when(F.col('mapping_layer').isin('LAYER2_EXACT_ANCHOR', 'LAYER2_ANCHOR_EXACT'), 2).when(F.col('mapping_layer').isin('LAYER3_CLEANED_ANCHOR', 'LAYER3_ANCHOR_CLEANED'), 3).when(F.col('mapping_layer') == 'LAYER4_OPCS_SNOMED', 4).when(F.col('mapping_layer') == 'LAYER5_GMDN_EMBEDDING', 5).when(F.col('mapping_layer') == 'UNMAPPED', 99).otherwise(50)
    stats = mapping.groupBy('EVENT_ID').agg(F.count(F.lit(1)).cast('int').alias('MAPPING_CANDIDATE_COUNT'), F.countDistinct('snomed_concept_id').cast('int').alias('_MAPPING_CONCEPT_COUNT'))
    selection_window = Window.partitionBy('EVENT_ID').orderBy(F.when(F.col('snomed_concept_id').isNull(), 1).otherwise(0), layer_rank, F.col('mapping_confidence').desc_nulls_last(), F.col('snomed_concept_id').desc_nulls_last(), F.col('gmdncode').desc_nulls_last())
    return mapping.withColumn('_MAPPING_RN', F.row_number().over(selection_window)).filter(F.col('_MAPPING_RN') == 1).drop('_MAPPING_RN').join(stats, 'EVENT_ID', 'left').select('EVENT_ID', F.col('snomed_concept_id').cast('long').alias('SNOMED_DEVICE_CONCEPT_ID'), F.col('snomed_name').alias('SNOMED_DEVICE_CONCEPT_NAME'), F.col('device_type').alias('DEVICE_TYPE'), F.col('mapping_confidence').alias('MAPPING_CONFIDENCE'), F.col('gmdncode').cast('long').alias('GMDN_CODE'), F.col('gmdn_name').alias('GMDN_NAME'), F.col('mapping_layer').alias('MAPPING_LAYER'), F.col('confidence_tier').alias('CONFIDENCE_TIER'), F.concat(F.lit('EVENT_ID:'), F.coalesce(F.col('mapping_layer'), F.lit('UNKNOWN'))).alias('MAPPING_SOURCE'), 'MAPPING_CANDIDATE_COUNT', (F.col('_MAPPING_CONCEPT_COUNT') > 1).alias('MAPPING_CONFLICT_IND'))

def refresh_implant_mapping_enrichment(event_ids: Optional[DataFrame]=None) -> Dict[str, int]:
    ensure_implant_target_schema()
    if not spark.catalog.tableExists(DEVICE_MAPPING_TABLE):
        print(f'[WARN] {DEVICE_MAPPING_TABLE} is unavailable; preserving the existing implant mapping enrichment.')
        return {'mapping_rows_evaluated': 0, 'mapping_refresh_skipped': 1}
    if spark.table(DEVICE_MAPPING_TABLE).limit(1).count() == 0:
        print(f'[WARN] {DEVICE_MAPPING_TABLE} is empty; preserving the existing implant mapping enrichment rather than nulling it.')
        return {'mapping_rows_evaluated': 0, 'mapping_refresh_skipped': 1}
    target_ids = spark.table(IMPLANT_TARGET_TABLE).select('EVENT_ID')
    if event_ids is not None:
        target_ids = target_ids.join(event_ids.select('EVENT_ID').distinct(), 'EVENT_ID', 'inner')
    best_mapping = get_device_mapping_lookup(target_ids)
    source = target_ids.distinct().join(best_mapping, 'EVENT_ID', 'inner')
    mapping_hash_columns = [name for name in MAPPING_COLUMNS if name != 'MAPPING_ROW_HASH']
    source = _hash_columns(source, mapping_hash_columns, 'MAPPING_ROW_HASH')
    source_count = source.count()
    if source_count == 0:
        print('[WARN] No current device mappings matched the requested implant scope; preserving existing enrichment values.')
        None
        return {'mapping_rows_evaluated': 0, 'mapping_refresh_skipped': 1}
    update_values = {name: f's.{name}' for name in MAPPING_COLUMNS if name in source.columns}
    DeltaTable.forName(spark, IMPLANT_TARGET_TABLE).alias('t').merge(source.alias('s'), 't.EVENT_ID = s.EVENT_ID').whenMatchedUpdate(condition='NOT (t.MAPPING_ROW_HASH <=> s.MAPPING_ROW_HASH)', set=update_values).execute()
    None
    return {'mapping_rows_evaluated': source_count}

def create_implant_details_incr(force_full_refresh: bool=False, return_context: bool=False):
    context = get_clinical_event_change_scope(force_full_refresh)
    if not context['has_changes']:
        return (None, context, None) if return_context else None
    implant_events = build_implant_detail_events(context['affected_encounters'])
    implant_details = build_implant_details_base(implant_events, context['affected_encounters'])
    if return_context:
        return (implant_details, context, implant_events)
    None
    return implant_details

def process_implant_details_incremental(force_full_refresh: bool=False, sync_event_foundation: bool=True, refresh_mapping: bool=True, run_validation: bool=True) -> Dict:
    run_id = str(uuid4())
    context = get_clinical_event_change_scope(force_full_refresh)
    if not context['has_changes']:
        set_checkpoint_version(IMPLANT_SOURCE_TABLE, context['ending_version'], run_id)
        mapping_metrics = refresh_implant_mapping_enrichment() if refresh_mapping else {}
        return {'run_id': run_id, 'status': 'NO_CLINICAL_EVENT_CHANGES', 'context': context, **mapping_metrics}
    implant_events = build_implant_detail_events(context['affected_encounters'])
    implant_events.count()
    implant_details = build_implant_details_base(implant_events, context['affected_encounters'])
    metrics = {'run_id': run_id, 'status': 'SUCCESS', 'context': context}
    try:
        if sync_event_foundation:
            metrics.update(sync_implant_detail_events(implant_events, context['affected_encounters'], context['full_refresh']))
        metrics.update(merge_implant_details_base(implant_details, context['affected_encounters'], context['full_refresh']))
        if refresh_mapping:
            affected_event_ids = implant_details.select('EVENT_ID')
            metrics.update(refresh_implant_mapping_enrichment(affected_event_ids))
        if run_validation:
            metrics['validation'] = validate_implant_details(raise_on_error=True)
        set_checkpoint_version(IMPLANT_SOURCE_TABLE, context['ending_version'], run_id)
        return metrics
    finally:
        None

def run_implant_details_full_backfill(refresh_mapping: bool=True, run_validation: bool=True) -> Dict:
    return process_implant_details_incremental(force_full_refresh=True, sync_event_foundation=True, refresh_mapping=refresh_mapping, run_validation=run_validation)

def run_post_mapping_enrichment(run_validation: bool=True) -> Dict:
    metrics = refresh_implant_mapping_enrichment()
    if run_validation:
        metrics['validation'] = validate_implant_details(raise_on_error=True)
    return metrics

def validate_implant_details(raise_on_error: bool=True) -> Dict[str, int]:
    target = spark.table(IMPLANT_TARGET_TABLE)
    structural = target.agg(
        F.count(F.lit(1)).alias('row_count'),
        F.countDistinct('EVENT_ID').alias('distinct_event_ids'),
        F.sum(F.col('EVENT_ID').isNull().cast('long')).alias('null_event_ids'),
        F.sum(F.col('SOURCE_ROW_HASH').isNull().cast('long')).alias(
            'missing_source_hash'
        ),
    ).first().asDict()
    structural['duplicate_event_ids'] = (
        structural['row_count'] - structural['distinct_event_ids']
    )
    if _table_exists(IMPLANT_EVENT_TARGET_TABLE):
        structural.update(
            spark.table(IMPLANT_EVENT_TARGET_TABLE).agg(
                F.count(F.lit(1)).alias('event_foundation_rows'),
                F.sum(F.col('VALUE_CONFLICT_IND').cast('long')).alias(
                    'event_value_conflicts'
                ),
            ).first().asDict()
        )
    fatal = {
        'duplicate_event_ids': structural.get('duplicate_event_ids', 0),
        'null_event_ids': structural.get('null_event_ids', 0),
        'missing_source_hash': structural.get('missing_source_hash', 0),
    }
    if raise_on_error and any((value or 0) > 0 for value in fatal.values()):
        raise ValueError(f'Implant detail validation failed: {fatal}')
    return {key: int(value or 0) for key, value in structural.items()}
try:
    _targets = ['4_prod.bronze.map_implant_details']
    if not _pipeline_resume_skip_component('map_implant_details', _targets):
        run_implant_details_full_backfill(refresh_mapping=False, run_validation=True) if _PIPELINE_FULL_REFRESH else process_implant_details_incremental(refresh_mapping=False, run_validation=True)
        _PIPELINE_UPDATED_TARGETS.extend(_targets)
        _pipeline_mark_component_complete('map_implant_details', _targets)
        _pipeline_audit(None, 'COMPONENT_END', {'component': 'map_implant_details'})
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

