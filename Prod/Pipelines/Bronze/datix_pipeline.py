# Databricks notebook source
# PMS_P1_T1_V3
# PMS_P1_T0_SCHEMA_V1
# PMS_P1_T2_V1
# PMS_P1_T3_V1
# BRONZE_FIX_857469999366132_V1
# BRONZE_FIX_946452877034658_V1
# TDX_P4_SAFETY_LINKAGE_V1
# TDX Datix bronze writer. Default is an incident-linked bounded sample; full mode is guarded.

# COMMAND ----------

from datetime import datetime, timezone
from functools import reduce
import json
import re

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

for _name,_default in {"target_schema":"8_dev.tdx_bronze","sample_mode":"true","sample_incidents":"5000","reuse_sample_tables":"false","force_sample_targets":"","allow_production_write":"false","pipeline_run_id":""}.items():
    try: dbutils.widgets.get(_name)
    except Exception: dbutils.widgets.text(_name,_default)
TARGET_SCHEMA=dbutils.widgets.get("target_schema")
SAMPLE_MODE=dbutils.widgets.get("sample_mode").lower()=="true"
SAMPLE_INCIDENTS=int(dbutils.widgets.get("sample_incidents"))
REUSE_SAMPLE_TABLES=dbutils.widgets.get("reuse_sample_tables").lower()=="true"
FORCE_SAMPLE_TARGETS={x.strip() for x in dbutils.widgets.get("force_sample_targets").split(",") if x.strip()}
ALLOW_PRODUCTION_WRITE=dbutils.widgets.get("allow_production_write").lower()=="true"
RUN_ID=dbutils.widgets.get("pipeline_run_id") or f"tdx-datix-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
assert not TARGET_SCHEMA.lower().startswith("4_prod") or ALLOW_PRODUCTION_WRITE
if SAMPLE_MODE: assert TARGET_SCHEMA.startswith("8_dev.")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {'.'.join(f'`{p}`' for p in TARGET_SCHEMA.split('.'))}")

RAW="4_prod.raw"
SRC_INCIDENTS=f"{RAW}.datix_incidents_main"; SRC_CONTACTS=f"{RAW}.datix_contacts_main"; SRC_LINKS=f"{RAW}.datix_link_contacts"
SRC_INJURIES=f"{RAW}.datix_inc_injuries"; SRC_STATUS=f"{RAW}.datix_inc_status_audit"; SRC_ACTIONS=f"{RAW}.datix_ca_actions"
SRC_LFPSE=f"{RAW}.datix_lfpse_main"; SRC_LFPSE_PATIENT=f"{RAW}.datix_lfpse_patients_details"
SRC_IDENTIFIERS="4_prod.bronze.map_patient_identifier"; SRC_PERSON="4_prod.bronze.map_person"; SRC_ENCOUNTER="4_prod.bronze.map_encounter"
dbutils.widgets.text("drug_lexicon_table","3_lookup.omop.drug_name_lexicon"); SRC_DRUG_LEXICON=dbutils.widgets.get("drug_lexicon_table")
INCIDENT=f"{TARGET_SCHEMA}.map_safety_incident"; PARTICIPANT=f"{TARGET_SCHEMA}.map_safety_incident_participant"
INJURY=f"{TARGET_SCHEMA}.map_safety_incident_injury"; FACTOR=f"{TARGET_SCHEMA}.map_safety_incident_factor"
LFPSE=f"{TARGET_SCHEMA}.map_safety_incident_lfpse"; ACTION=f"{TARGET_SCHEMA}.map_safety_incident_action"
STATUS=f"{TARGET_SCHEMA}.map_safety_incident_status"; FRAGMENT=f"{TARGET_SCHEMA}.map_safety_text_fragment"
ANON_STATE=["anon_status","anon_redactor_version","anon_source_text_sha","anon_identity_fingerprint","anon_context_fingerprint","anon_redaction_count","anon_processed_at"]
SNAPSHOT_SOURCES=[SRC_LINKS,SRC_INJURIES,SRC_LFPSE,SRC_LFPSE_PATIENT,SRC_STATUS]
ANON_TEXT_STATE_REATTACH_V3_2 = "preserve anon state during full source reconciliation"

def datix_source(table):
    frame = spark.table(table)
    # Identical raw LFPSE rows recur in all three consumers. Collapse only exact
    # copies; conflicting versions of the same key still fail publish validation.
    return frame.dropDuplicates() if table == SRC_LFPSE else frame

def qname(n): return ".".join(f"`{p.replace('`','``')}`" for p in n.split("."))
def has_col(table,col): return col.lower() in {c.lower() for c in spark.table(table).columns}
def nonblank(c): return F.length(F.trim(F.col(c).cast("string")))>0
def active_incident_filter(df): return df.join(F.broadcast(INC_IDS),F.col("inc_id").cast("long")==F.col("_inc_id"),"inner").drop("_inc_id")

# DATIX_SNAPSHOT_BASELINE_V2: week-over-week baseline from retained Delta history.
# The raw Datix tables are replaced in full each week (CREATE OR REPLACE TABLE AS SELECT).
# V1 pinned a fixed September version with no stored count and re-derived the count from
# history; once Delta's 30-day log retention dropped that version the guard failed closed
# on every run. V2 compares against the previous full snapshot, which weekly writes keep
# inside retention indefinitely.
FULL_SNAPSHOT_OPERATIONS = {"CREATE TABLE AS SELECT", "CREATE OR REPLACE TABLE AS SELECT", "REPLACE TABLE AS SELECT"}
NON_DATA_OPERATIONS = {"SET TBLPROPERTIES", "UNSET TBLPROPERTIES", "CHANGE COLUMN", "ADD COLUMNS",
                       "DROP COLUMNS", "RENAME COLUMN", "OPTIMIZE", "VACUUM START", "VACUUM END",
                       "UPGRADE PROTOCOL", "ROW TRACKING BACKFILL"}

def _is_full_snapshot_write(row):
    """True when a history row replaced the whole table (CTAS/RTAS or an unpredicated overwrite)."""
    operation = row["operation"]
    parameters = row["operationParameters"] or {}
    if operation in FULL_SNAPSHOT_OPERATIONS:
        return True
    return operation == "WRITE" and parameters.get("mode", "").lower() == "overwrite" and not parameters.get("predicate")

def _previous_snapshot_rows(table):
    """Row count of the full snapshot before the current one, read from retained Delta history.

    Walks history newest-first. Commits after the current snapshot must be metadata-only,
    otherwise the table is no longer a pure snapshot and the guard fails closed. The count
    is the previous snapshot write's exact numOutputRows.
    """
    history = spark.sql(f"DESCRIBE HISTORY {qname(table)}").orderBy(F.col("version").desc()).collect()
    snapshots = [row for row in history if _is_full_snapshot_write(row)]
    if len(snapshots) < 2:
        raise RuntimeError(f"{table}: retained history holds {len(snapshots)} full snapshot(s); need 2 for a baseline")
    current_snapshot, previous_snapshot = snapshots[0], snapshots[1]
    for row in history:
        if int(row["version"]) <= int(current_snapshot["version"]):
            break
        if row["operation"] not in NON_DATA_OPERATIONS:
            raise RuntimeError(f"{table}: data-changing {row['operation']} at version {row['version']} after the current snapshot")
    value = (previous_snapshot["operationMetrics"] or {}).get("numOutputRows")
    if value is None or not str(value).isdigit():
        raise RuntimeError(f"{table}: previous snapshot (version {previous_snapshot['version']}) has no exact numOutputRows")
    return int(previous_snapshot["version"]), int(value)

def assert_snapshot_not_shrunk(table):
    """Fail if a raw snapshot lost more than 20% of its rows since the previous weekly snapshot.

    Snapshot sources drive SOURCE_PRESENT_IND=false in bronze, so a truncated raw load would
    mark live rows absent. Sample mode reads a bounded cohort and skips the check.
    """
    if SAMPLE_MODE:
        return
    baseline_version, baseline = _previous_snapshot_rows(table)
    current = spark.table(table).count()
    if current * 5 < baseline * 4:
        raise RuntimeError(f"{table}: snapshot shrink guard: current={current}, baseline={baseline} (version {baseline_version})")
    print(f"Snapshot count verified: {table}: baseline={baseline} (version {baseline_version}), current={current}")
for _table in SNAPSHOT_SOURCES: assert_snapshot_not_shrunk(_table)

def with_anon_state(df,outputs=("anon_text",)):
    for c in outputs: df=df.withColumn(c,F.lit(None).cast("string"))
    for c in ANON_STATE:
        dtype="bigint" if c=="anon_redaction_count" else ("timestamp" if c=="anon_processed_at" else "string")
        df=df.withColumn(c,F.lit(None).cast(dtype))
    return df

def finalise(df,key_cols,source_table,source_row_col):
    df=(df.withColumn("SOURCE_TABLE",F.lit(source_table)).withColumn("SOURCE_ROW_ID",F.col(source_row_col).cast("string"))
        .withColumn("PIPELINE_RUN_ID",F.lit(RUN_ID)).withColumn("SOURCE_PRESENT_IND",F.lit(True)).withColumn("SOURCE_ABSENT_DETECTED_TS",F.lit(None).cast("timestamp")))
    excluded={"ROW_HASH","ADC_UPDT","PIPELINE_RUN_ID","SOURCE_ABSENT_DETECTED_TS",*ANON_STATE}
    payload=sorted(c for c in df.columns if c not in excluded and not c.startswith("anon_"))
    return df.withColumn("ROW_HASH",F.sha2(F.to_json(F.struct(*[F.col(c) for c in payload])),256)).withColumn("ADC_UPDT",F.current_timestamp())

# INSR_W8_COMMENTS_V1 (W8.04): canonical bronze comments, generated from the INSR W8.02 dictionary. Edit the dictionary,
# never this literal. Column names are the bronze (upper-case) names.
INSR_W8_BRONZE_COMMENTS = {
 "map_safety_incident": {
  "ADC_UPDT": "When bronze last wrote the row.",
  "AFFECTED_PATIENT_COUNT": "Count of patients recorded as affected by the clinical safety incident.",
  "CONTEXT_FINGERPRINT_CURRENT": "SHA-256 of the incident's current identifying context (names, birth date, postcode, reporter, investigator and participants); anonymised narrative text is released only while its anon_context_fingerprint matches this value.",
  "ENCNTR_ID": "Unique identifier for the Encounter table.",
  "ENCOUNTER_LINK_CONFIDENCE": "HIGH when the linked encounter has HIGH arrival and departure confidence; LOW when an inpatient/ED departure was unresolved and bounded at arrival + 1 day; else MEDIUM.",
  "ENCOUNTER_LINK_METHOD": "Method by which the clinical safety incident was linked to an encounter record.",
  "ENCOUNTER_LINK_RULE": "Rule that set encounter_id (TDX-E2 incident-time overlap with Millennium encounter *_BEST bounds).",
  "INCIDENT_DT_TM": "Date and time the clinical safety incident occurred, derived from the Datix incident date and incident time fields.",
  "INCIDENT_DT_TM_CLEAN": "Cleaned version of the date and time the clinical safety incident occurred, derived from the Datix incident date and incident time fields.",
  "INCIDENT_TIME_VALID_IND": "Incident time was recorded (an undated incident time spans the whole incident day in TDX-E2).",
  "INC_ACCTYPE": "Type of accident recorded for the incident.",
  "INC_BODYPART": "Part of the body affected or injured in the incident.",
  "INC_CARESTAGE": "Code for the stage of care at which the incident occurred, as recorded in Datix.",
  "INC_CATEGORY": "Coded category of the incident.",
  "INC_CATEGORY_DISPLAY": "Descriptive label of the Datix incident category associated with the code.",
  "INC_CAUSE": "Recorded cause of the incident in Datix.",
  "INC_CLINGROUP": "Code for the clinical group or division associated with the incident in Datix.",
  "INC_CLINOUTCOME": "Clinical outcome recorded for the incident in Datix.",
  "INC_CLINOUTCOME2": "Secondary clinical outcome recorded for the incident in Datix.",
  "INC_CLINTYPE": "Code for the clinical incident type recorded in Datix.",
  "INC_CONSEQUENCE": "Coded consequence score used in the risk grading of the incident.",
  "INC_DIRECTORATE": "Code for the directorate associated with the incident.",
  "INC_DOB": "Date of birth of the person involved in the incident.",
  "INC_ETHNICITY": "Ethnicity of the person affected by the incident, as recorded in Datix.",
  "INC_FURTHER_INV": "Datix incident field recording whether further investigation of the incident is required.",
  "INC_GENDER": "Gender of the person involved in the incident as recorded in Datix.",
  "INC_GRADE": "Overall risk grade of the incident derived from consequence and likelihood.",
  "INC_GRADE_DISPLAY": "Descriptive label for the Datix incident grade code.",
  "INC_HEAD": "Code identifying the head or senior manager associated with the incident record in Datix.",
  "INC_IMPACT": "Impact classification of the incident.",
  "INC_INJURY": "Type or nature of injury sustained in the incident.",
  "INC_INVESTIGATOR": "Person assigned to investigate the incident, as recorded in Datix.",
  "INC_IS_RIDDOR": "Flag indicating whether the incident is reportable under RIDDOR (Reporting of Injuries, Diseases and Dangerous Occurrences Regulations).",
  "INC_LIKELIHOOD": "Coded likelihood of recurrence used in the risk grading of the incident.",
  "INC_LOCACTUAL": "Code for the specific location where the incident occurred.",
  "INC_LOCATION": "Current location of the equipment involved in the incident.",
  "INC_LOCTYPE": "Coded type of location where the incident occurred.",
  "INC_MGR": "Code identifying the handler or manager responsible for the incident record in Datix.",
  "INC_NAME": "Name of the person affected by or associated with the incident as recorded in Datix.",
  "INC_NOTIFY": "Notification details recorded for the incident, such as who was notified.",
  "INC_NSPECIALTY": "Specialty recorded for the incident, likely an alternative or national specialty classification in Datix.",
  "INC_ORGANISATION": "Code for the organisation in which the incident occurred, as recorded in Datix.",
  "INC_OURREF": "Local incident reference number assigned by the organisation in Datix.",
  "INC_OUTCOMECODE": "Code recording the outcome of the incident.",
  "INC_PAT_TYPE": "Type of patient involved in the incident, as recorded in Datix.",
  "INC_POSTCODE": "Postcode recorded against the incident.",
  "INC_RATING": "Numeric risk rating of the incident derived from risk grading.",
  "INC_RC_REQUIRED": "Datix incident field recording whether a root cause analysis is required for the incident.",
  "INC_REPNAME": "Name of the person who reported the incident.",
  "INC_REPORTEDBY": "Role or staff category of the person who reported the incident, as recorded in Datix.",
  "INC_RESULT": "Recorded result or outcome of the incident in Datix.",
  "INC_RIDDOR": "RIDDOR-related classification recorded for the incident.",
  "INC_RIDDORNO": "RIDDOR reference number assigned to the incident report.",
  "INC_RIDLOC": "RIDDOR location classification for where the incident occurred.",
  "INC_SEVERITY": "Coded severity or harm level assigned to the incident.",
  "INC_SEVERITY_DISPLAY": "Descriptive label for the incident severity code shown to Datix users.",
  "INC_SPECIALTY": "Code for the clinical specialty associated with the incident.",
  "INC_SUBCATEGORY": "Coded subcategory of the incident within its category.",
  "INC_SUBCATEGORY_DISPLAY": "Descriptive label for the Datix incident subcategory code.",
  "INC_TREATMENT": "Treatment received as a result of the incident.",
  "INC_TYPE": "Coded incident type indicating who or what was affected, such as patient or staff.",
  "INC_TYPE_DISPLAY": "Descriptive label for the Datix incident type code.",
  "INC_UNIT": "Code for the organisational unit or site where the incident occurred.",
  "INC_UNIT_TYPE": "Code recording the type of unit associated with the Datix incident.",
  "INVESTIGATION_COMPLETE_DT_TM": "Date and time the investigation of the clinical safety incident was completed, sourced from Datix.",
  "INVESTIGATION_START_DT_TM": "Date and time the investigation of the clinical safety incident was started, sourced from Datix.",
  "NOTIFIED_DT_TM": "Date and time of notification recorded against the clinical safety incident in Datix, populated for very few records.",
  "OPENED_DT_TM": "Date and time the clinical safety incident record was opened in Datix.",
  "PERSON_ID": "This is the value of the unique primary identifier of the person table.  It is an internal system assigned number.",
  "PERSON_LINK_METHOD": "Method by which the clinical safety incident was linked to a person record.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "RECORDID": "Datix record identifier for the incident, uniquely identifying each incident record in incidents_main.",
  "REPORTED_DT_TM": "Date and time the clinical safety incident was reported, sourced from Datix.",
  "REP_APPROVED": "Approval status code of the incident record within the Datix review workflow.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SECGROUP": "Security group associated with the incident record for Datix access control.",
  "SECGROUP_DISPLAY": "Descriptive text for the Datix security group code.",
  "SECLEVEL": "Security level assigned to the incident record for Datix access control.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from.",
  "SOURCE_UPDT_DT_TM": "Last-update time of the row in the source system (for Datix, updateddate); NULL when the source supplies none."
 },
 "map_safety_incident_action": {
  "ACT_BY_INITS": "Datix staff initials code of the user who completed the action.",
  "ACT_CHAIN_ID": "Integer identifier of the action chain that the action belongs to, where applicable.",
  "ACT_CLINGROUP": "Code for the clinical group associated with the action.",
  "ACT_COST": "Cost estimate recorded for the action.",
  "ACT_COST_MAX": "Maximum cost estimate recorded for the action.",
  "ACT_COST_MIN": "Minimum cost estimate recorded for the action.",
  "ACT_COST_TYPE": "Code indicating the type of cost recorded for the action.",
  "ACT_DDONE": "Date on which the action was recorded as done.",
  "ACT_DDUE": "Date by which the action is due to be completed.",
  "ACT_DIRECTORATE": "Code for the directorate associated with the action.",
  "ACT_DSTART": "Start date of the action.",
  "ACT_FROM_INITS": "Datix staff initials code of the user who assigned the action.",
  "ACT_LOCACTUAL": "Code for the specific location associated with the action.",
  "ACT_LOCTYPE": "Code for the type of location associated with the action.",
  "ACT_ORGANISATION": "Code for the organisation associated with the action.",
  "ACT_PRIORITY": "Priority assigned to the action.",
  "ACT_SCORE": "Integer score recorded against the action; its scoring scheme is not documented in the available metadata.",
  "ACT_SPECIALTY": "Code for the specialty associated with the action.",
  "ACT_STEP_NO": "Step number of the action within its action chain.",
  "ACT_TO_INITS": "Datix staff initials code of the user to whom the action is assigned.",
  "ACT_TYPE": "Code indicating the type of action.",
  "ACT_UNIT": "Code for the site or unit associated with the action.",
  "ADC_UPDT": "When bronze last wrote the row.",
  "INC_ID": "Identifier of the safety incident to which the action relates, sourced from the incident identifier in the incident action mapping and the case identifier on Datix action records.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "RECORDID": "Datix record identifier for the action, with one row per action.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SECGROUP": "Datix security group associated with the action record.",
  "SECLEVEL": "Datix security level applied to the action record.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from.",
  "SOURCE_UPDT_DT_TM": "Last-update time of the row in the source system (for Datix, updateddate); NULL when the source supplies none.",
  "UPDATEDDATE": "Date and time the action record was last updated in Datix."
 },
 "map_safety_incident_factor": {
  "ADC_UPDT": "When bronze last wrote the row.",
  "DEVICE_BATCH_NUMBER": "Batch or lot number of the medical device involved in the clinical safety incident factor, as recorded in the source.",
  "DEVICE_EQUIPMENT": "Medical device or equipment recorded as involved in the safety incident factor.",
  "DEVICE_INVOLVEMENT_FACTORS": "Recorded factors describing how the medical device was involved in the clinical safety incident.",
  "DEVICE_MANUFACTURER": "Manufacturer of the medical device involved in the safety incident factor.",
  "DEVICE_MODEL": "Model of the medical device involved in the safety incident factor.",
  "DEVICE_SERIAL_NUMBER": "Serial number of the medical device involved in the clinical safety incident factor, as recorded in the source.",
  "DEVICE_SUPPLIER": "Supplier of the medical device recorded as a factor in the clinical safety incident, as captured in the source.",
  "DEVICE_TYPE": "Type or category of the medical device involved in the safety incident factor.",
  "FACTOR_ATTRIBUTES_JSON": "JSON of the raw Datix factor attributes (drug, route, dose, …) as recorded.",
  "FACTOR_CODE": "Code identifying a contributing factor recorded against a clinical safety incident, derived from the Datix equipment or medication drug fields via the incident factor mapping.",
  "FACTOR_DISPLAY": "Display text describing the contributing factor recorded against a clinical safety incident, derived from the Datix equipment or medication drug fields.",
  "FACTOR_MAP_CONFIDENCE": "Numeric confidence score for the mapping applied to the clinical safety incident factor, populated only where a mapping was made.",
  "FACTOR_MAP_METHOD": "Method used to map the clinical safety incident factor to a target concept or code.",
  "FACTOR_MAP_RULE_ID": "Identifier of the mapping rule applied to map the clinical safety incident factor.",
  "FACTOR_MAP_VERSION": "Version of the mapping resource or rules used to map the clinical safety incident factor.",
  "FACTOR_ORDINAL": "Sequence number of the factor within its clinical safety incident.",
  "FACTOR_ROW_ID": "Internal identifier for each incident factor row, derived from the Datix incident record or LFPSE incident identifier.",
  "FACTOR_SOURCE_RECORD_ID": "Identifier of the source Datix incident or LFPSE record from which the incident factor row was derived.",
  "FACTOR_SOURCE_TABLE": "Name of the source table from which the clinical safety incident factor was derived.",
  "FACTOR_TARGET_CODE": "SNOMED code the Datix drug text maps to via 3_lookup.omop.drug_name_lexicon; null when unmapped.",
  "FACTOR_TARGET_DISPLAY": "Display of factor_target_code.",
  "FACTOR_TARGET_SYSTEM": "Code system of the mapped medication factor (SNOMED CT dm+d VTM or Substance).",
  "FACTOR_TYPE": "Type or category of the contributing factor recorded for the clinical safety incident.",
  "INC_ID": "Identifier of the Datix incident record that this LFPSE row relates to.",
  "LFPSE_ID": "Numeric identifier of the associated Learn From Patient Safety Events (LFPSE) record for the incident factor, where one exists.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_FIELDS": "Source field or fields from which the clinical safety incident factor was extracted.",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from."
 },
 "map_safety_incident_injury": {
  "ADC_UPDT": "When bronze last wrote the row.",
  "CON_ID": "Identifier of the Datix contact (person involved in the incident) who sustained the injury.",
  "INC_BODYPART": "Datix code identifying the body part affected by the recorded injury, which may be blank.",
  "INC_BODYPART_DISPLAY": "Descriptive label of the body part represented by the code.",
  "INC_ID": "Identifier of the Datix incident to which the injury entry relates.",
  "INC_INJURY": "Datix code identifying the type of injury recorded against the incident contact.",
  "INC_INJURY_DISPLAY": "Descriptive label for the injury type code.",
  "LISTORDER": "Display or sequence order of the injury entry within the incident-contact combination.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "RECORDID": "Datix record identifier for the injury entry row in the inc_injuries table.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from.",
  "SOURCE_UPDT_DT_TM": "Last-update time of the row in the source system (for Datix, updateddate); NULL when the source supplies none."
 },
 "map_safety_incident_lfpse": {
  "ADC_UPDT": "When bronze last wrote the row.",
  "CON_ID": "Identifier of the Datix contact record that this LFPSE row relates to, where applicable.",
  "INC_ID": "Identifier of the Datix incident record that this LFPSE row relates to.",
  "LFPSE_BLOOD_INVOLVED": "LFPSE response recording the blood involved in the reported event.",
  "LFPSE_BLOOD_PROBLEM": "LFPSE response describing the problem with blood involved in the reported event.",
  "LFPSE_BLOOD_PRODUCTS_BATCH": "LFPSE response recording the batch of blood products involved in the reported event.",
  "LFPSE_BLOOD_PRODUCTS_BRAND": "LFPSE response recording the brand of blood products involved in the reported event.",
  "LFPSE_BLOOD_PRODUCTS_INVOLVED": "LFPSE response recording the blood products involved in the reported event.",
  "LFPSE_BLOOD_PRODUCTS_NOT_USED": "LFPSE response relating to blood products not being used when they should have been in the reported event.",
  "LFPSE_BLOOD_PRODUCTS_PROBLEM": "LFPSE response describing the problem with blood products involved in the reported event.",
  "LFPSE_BLOOD_WAS_NOT_USED": "LFPSE response relating to blood not being used when it should have been in the reported event.",
  "LFPSE_BUILDINGS_INFRASTRUCTURE": "LFPSE response recording the buildings or infrastructure involved in the reported event.",
  "LFPSE_BUILDINGS_INFRASTRUCTURE_INVOLVED": "LFPSE response indicating whether buildings or infrastructure were involved in the reported event.",
  "LFPSE_BUILDINGS_INFRASTRUCTURE_NOT_USED": "LFPSE response relating to buildings or infrastructure not being used when they should have been in the reported event.",
  "LFPSE_BUILDINGS_INFRASTRUCTURE_PROBLEM": "LFPSE response describing the problem with buildings or infrastructure involved in the reported event.",
  "LFPSE_CQC_CRITERIA": "LFPSE response recording the Care Quality Commission notification criteria applicable to the reported event.",
  "LFPSE_CQC_NOTIFY": "LFPSE response indicating whether the Care Quality Commission should be notified of the reported event.",
  "LFPSE_DAMAGED_BLOOD": "LFPSE response relating to damaged blood involved in the reported event.",
  "LFPSE_DAMAGED_BLOOD_PRODUCTS": "LFPSE response relating to damaged blood products involved in the reported event.",
  "LFPSE_DATA_SHARING_OPT_OUT": "LFPSE response relating to data-sharing opt-out for the reported event.",
  "LFPSE_DATE": "Date recorded for the LFPSE event.",
  "LFPSE_DEPRIVATION_OF_LIBERTY": "LFPSE response relating to deprivation of liberty for the reported event.",
  "LFPSE_DESIGNATIONS_MENTAL_HEALTH": "LFPSE field recording mental health designations relevant to the reported event.",
  "LFPSE_DETECTION_POINT": "LFPSE field recording the point at which the event was detected.",
  "LFPSE_DEVICE_INVOLVEMENT_FACTORS": "LFPSE field recording the factors describing how a device was involved in the event.",
  "LFPSE_DEVICE_TYPE": "LFPSE field recording the type of device involved in the event.",
  "LFPSE_DEVICE_USED_UNNECESSARILY": "LFPSE field giving details where a device was used unnecessarily in the event.",
  "LFPSE_DRUGS_INVOLVED": "LFPSE field recording the medications involved in the event.",
  "LFPSE_DRUG_INVOLVEMENT_FACTORS": "LFPSE field recording the factors describing how medication was involved in the event.",
  "LFPSE_DRUG_REACTION": "LFPSE field recording a drug reaction associated with the event.",
  "LFPSE_DRUG_USED_TOO_MUCH": "LFPSE field giving details where too much of a medication was involved in the event.",
  "LFPSE_DUTY_OF_CANDOUR": "LFPSE field recording whether duty of candour applies to the event.",
  "LFPSE_ESTATES_SERVICES": "LFPSE field recording estates services relevant to the event.",
  "LFPSE_ESTATES_SERVICES_INVOLVED": "LFPSE field recording whether or which estates services were involved in the event.",
  "LFPSE_ESTATES_SERVICES_NOT_USED": "LFPSE field giving details where estates services were not used in relation to the event.",
  "LFPSE_ESTATES_SERVICES_PROBLEM": "LFPSE field describing the problem with estates services involved in the event.",
  "LFPSE_ESTIMATED_TIME": "LFPSE field recording the estimated time of the event, stored as a string.",
  "LFPSE_FROM_ONLINE_FORMS": "LFPSE field indicating whether the record originated from Datix online forms.",
  "LFPSE_GOOD_CARE_DETECTION_FACTOR": "LFPSE field recording the factor or factors through which good care was detected in the reported event.",
  "LFPSE_HSIB_NOTIFY": "LFPSE field indicating whether the event is notifiable to the Healthcare Safety Investigation Branch (HSIB).",
  "LFPSE_IDENTIFIED_LOCATION": "LFPSE field recording the location identified as where the reported event occurred.",
  "LFPSE_INCIDENT_OCCURRED_TODAY": "LFPSE field indicating whether the reported incident occurred on the day of reporting.",
  "LFPSE_INVOLVED_AGENTS": "LFPSE field recording the agents involved in the event, such as medication, devices, blood products, tissues and organs, estates, IT systems or people.",
  "LFPSE_INVOLVED_PERSONS_ACTIONS": "LFPSE field recording the actions of people involved that contributed to the reported event.",
  "LFPSE_INVOLVED_PROCESSES": "LFPSE field recording the care processes involved in the reported event.",
  "LFPSE_IS_ANONYMOUS": "LFPSE field indicating whether the report was submitted anonymously.",
  "LFPSE_IT_SYSTEMS_INVOLVEMENT_FACTORS": "LFPSE field recording how IT systems or software were involved in the reported event.",
  "LFPSE_LEVEL_OF_CONCERN": "LFPSE field recording the reporter's level of concern about the reported event or risk.",
  "LFPSE_LOCAL_AUTHORITY_SAFEGUARDING": "LFPSE field indicating whether the event involves or was referred for local authority safeguarding.",
  "LFPSE_LOCATION_AT_RISK": "LFPSE field recording the location affected by an identified risk.",
  "LFPSE_LOCATION_KNOWN": "LFPSE field indicating whether the location of the reported event is known.",
  "LFPSE_LOCATION_WITHIN_SERVICE": "LFPSE field recording the location within the service where the reported event occurred.",
  "LFPSE_MANUFACTURER": "LFPSE field recording the manufacturer of a product, such as a device or medication, involved in the reported event.",
  "LFPSE_MARVIN_REFERENCE_NUMBER": "LFPSE field holding an external MARVIN reference number associated with the reported event.",
  "LFPSE_MEDICATION_ADMINISTERED_INCORRECTLY": "LFPSE field recording details of medication administered incorrectly in the reported event.",
  "LFPSE_MEDICATION_ADMINISTRATOR": "LFPSE field recording who administered the medication involved in the reported event.",
  "LFPSE_MODEL": "LFPSE field recording the model of a device or product involved in the reported event.",
  "LFPSE_NEVER_EVENT": "LFPSE field recorded in Datix indicating whether the patient safety event was considered a Never Event.",
  "LFPSE_NEVER_EVENT_TYPE": "LFPSE field recorded in Datix giving the type or category of Never Event associated with the patient safety event.",
  "LFPSE_NHSBT_REPORT_NUMBER": "External reference number of a related report made to NHS Blood and Transplant (NHSBT), recorded for the LFPSE event.",
  "LFPSE_ORGANISATION": "LFPSE field recording the organisation associated with the reported patient safety event.",
  "LFPSE_OUTCOME_TYPE": "LFPSE field recording the type of outcome of the reported patient safety event.",
  "LFPSE_PATIENT_EVENT": "LFPSE field indicating or describing whether the recorded event is a patient event.",
  "LFPSE_PATIENT_SAFETY_INCIDENT_HAS_OCCURRED": "LFPSE field indicating whether a patient safety incident has occurred, as opposed to a risk or other event type.",
  "LFPSE_PEOPLE_ACTION_FACTORS": "LFPSE field recording the factors relating to people's actions that were involved in the event.",
  "LFPSE_PEOPLE_ACTION_TOO_MUCH": "LFPSE field recording details where people involved in the event took too much of an action.",
  "LFPSE_PEOPLE_INVOLVEMENT_FACTOR": "LFPSE field recording how people were involved as a factor in the event.",
  "LFPSE_PRECISE_TIME": "LFPSE field recording the precise time at which the event occurred, stored as text.",
  "LFPSE_PROBLEM_MEDICATION_PACKAGING": "LFPSE field recording problems with medication packaging involved in the event.",
  "LFPSE_RADIOTHERAPY_INCIDENT_CODE": "LFPSE field recording the radiotherapy incident classification code for the event.",
  "LFPSE_RECORD_LVL_ONE": "LFPSE level-one record field held on the Datix LFPSE record.",
  "LFPSE_RECORD_LVL_TWO": "LFPSE level-two record field held on the Datix LFPSE record.",
  "LFPSE_REFERENCE_NUMBER": "LFPSE reference number assigned to the reported event.",
  "LFPSE_REPORTER_INVOLVEMENT": "LFPSE field recording how the reporter was involved in the event.",
  "LFPSE_REPORTER_ORGANISATION": "Organisation of the person reporting the LFPSE event as captured in Datix.",
  "LFPSE_REPORTER_ROLE": "Role of the person reporting the LFPSE event as captured in Datix.",
  "LFPSE_REPORTER_TYPE": "Type of reporter submitting the LFPSE event record as captured in Datix.",
  "LFPSE_RESPONSIBLE_SPECIALTY": "Clinical specialty responsible for the care associated with the LFPSE event.",
  "LFPSE_RISK_IDENTIFIED_LOCATION": "Location where the patient safety risk was identified for an LFPSE risk record.",
  "LFPSE_RISK_IMMINENT": "Indicator of whether the recorded patient safety risk is imminent.",
  "LFPSE_RISK_POPULATION": "Population or patient group potentially affected by the recorded patient safety risk.",
  "LFPSE_RISK_SERVICE_AREA": "Service area associated with the recorded patient safety risk.",
  "LFPSE_RISK_THEME": "Theme or category assigned to the recorded patient safety risk.",
  "LFPSE_RISK_TIMEFRAME": "Timeframe within which the recorded patient safety risk is expected to materialise.",
  "LFPSE_SABRE_REPORT_NUMBER": "External SABRE report reference number linked to the LFPSE event.",
  "LFPSE_SAFETY_CHALLENGES": "Safety challenges recorded as relevant to the LFPSE event.",
  "LFPSE_SERIOUS_INCIDENT": "Indicator of whether the LFPSE event is classed as a serious incident.",
  "LFPSE_SERVICE_AREA": "Service area in which the LFPSE event occurred as captured in Datix.",
  "LFPSE_SHOT_REPORT_NUMBER": "External SHOT haemovigilance report reference number linked to the LFPSE event.",
  "LFPSE_TISSUE_ORGANS_INVOLVEMENT_FACTOR": "Factor describing how tissues or organs were involved in the LFPSE event.",
  "LFPSE_TISSUE_ORGANS_NOT_USED": "Details recorded where tissues or organs were not used when they should have been in the LFPSE event.",
  "LFPSE_TISSUE_ORGANS_USED_TOO_MUCH": "Details recorded where too much tissue or organ material was used in the LFPSE event.",
  "LFPSE_TOO_LITTLE_BLOOD_PRODUCTS": "LFPSE reporting field capturing information on too little of a blood product being used in the recorded event.",
  "LFPSE_TOO_LITTLE_BLOOD_USED": "LFPSE reporting field capturing information on too little blood being used in the recorded event.",
  "LFPSE_TOO_MUCH_BLOOD_PRODUCTS": "LFPSE reporting field capturing information on too much of a blood product being used in the recorded event.",
  "LFPSE_TOO_MUCH_BLOOD_USED": "LFPSE reporting field capturing information on too much blood being used in the recorded event.",
  "LFPSE_TYPE": "LFPSE event type recorded for the Datix record.",
  "LFPSE_WRONG_BLOOD": "LFPSE reporting field capturing information on the wrong blood being involved in the recorded event.",
  "LFPSE_WRONG_BLOOD_PRODUCTS": "LFPSE reporting field capturing information on the wrong blood product being involved in the recorded event.",
  "LFPSE_WRONG_BUILDINGS_INFRASTRUCTURE": "LFPSE reporting field capturing information on wrong buildings or infrastructure involved in the recorded event.",
  "LFPSE_WRONG_ESTATES_SERVICES": "LFPSE reporting field capturing information on wrong estates services involved in the recorded event.",
  "LFPSE_YELLOW_CARD_REFERENCE": "External Yellow Card reference number associated with the LFPSE record.",
  "MED_ID": "Identifier of the Datix medication record that this LFPSE row relates to, where applicable.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "RECORDID": "Record identifier for the row in the Datix lfpse_main table.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from.",
  "SOURCE_UPDT_DT_TM": "Last-update time of the row in the source system (for Datix, updateddate); NULL when the source supplies none."
 },
 "map_safety_incident_participant": {
  "ADC_UPDT": "When bronze last wrote the row.",
  "CON_ADDRESS": "Address of the contact.",
  "CON_CLINGROUP": "Clinical group code associated with the Datix contact's organisational placement.",
  "CON_DIRECTORATE": "Directorate code associated with the Datix contact's organisational placement.",
  "CON_DISABILITY": "Disability status or details recorded for the Datix contact.",
  "CON_DOB": "Date of birth of the contact.",
  "CON_DOD": "Date of death recorded for the Datix contact.",
  "CON_EMAIL": "Email address recorded for the Datix contact.",
  "CON_EMPL_GRADE": "Employment grade recorded for the Datix contact.",
  "CON_ETHNICITY": "Ethnicity of the contact.",
  "CON_FAX": "Fax number of the contact.",
  "CON_FORENAMES": "Forename(s) of the contact.",
  "CON_GENDER": "Gender of the contact.",
  "CON_ID": "Identifier of the Datix contact record for the patient associated with this LFPSE submission.",
  "CON_JOBTITLE": "Job title of the contact as free text.",
  "CON_LANGUAGE": "Language recorded for the Datix contact.",
  "CON_LOCACTUAL": "Actual location code or codes associated with the Datix contact.",
  "CON_MRN": "Medical record number recorded for the Datix contact participating in the safety incident, sourced from the Datix contact number field.",
  "CON_NHSNO": "NHS number recorded for the Datix contact.",
  "CON_ORGCODE": "Organisation code associated with the Datix contact's organisational placement.",
  "CON_POSTCODE": "Postcode of the contact's address.",
  "CON_RELIGION": "Religion or belief recorded for the contact.",
  "CON_SEX_ORIENTATION": "Sexual orientation recorded for the contact.",
  "CON_SPECIALTY": "Specialty code associated with the Datix contact's organisational placement.",
  "CON_SUBTYPE": "Code for the subtype of the Datix contact, refining the contact type.",
  "CON_SURNAME": "Surname of the contact.",
  "CON_TEL1": "Primary telephone number of the contact.",
  "CON_TEL2": "Secondary telephone number of the contact.",
  "CON_TITLE": "Title or honorific of the contact.",
  "CON_TYPE": "Code indicating the type of contact, such as patient or staff category.",
  "CON_UNIT": "Unit code associated with the Datix contact's organisational placement.",
  "DOB_AGREES_IND": "Indicator of whether the date of birth recorded on the Datix contact agrees with the birth date of the resolved person record.",
  "INC_ID": "Identifier of the Datix incident record associated with this LFPSE patient detail record.",
  "LFPSE_AGE_AT_TIME_OF_INCIDENTS_DAYS": "Patient's age at the time of the incident, expressed in days, as recorded for the LFPSE submission.",
  "LFPSE_AGE_BRACKET": "Raw code for the patient's age bracket at the time of the incident, as recorded for the LFPSE submission.",
  "LFPSE_GENDER": "Raw code for the patient's gender as recorded for the LFPSE submission.",
  "LFPSE_ID": "Identifier of the Learn from Patient Safety Events (LFPSE) submission to which this patient detail record belongs.",
  "LFPSE_PATIENT_ETHNICITY": "Raw code for the patient's ethnicity as recorded for the LFPSE submission.",
  "LFPSE_PATIENT_RECORDID": "Record identifier of the linked patient details record in the Datix LFPSE patient details source, where one applies.",
  "LFPSE_PATIENT_SEQUENCE": "Sequence number distinguishing individual patients within an LFPSE submission.",
  "LFPSE_PHYSICAL_HARM": "Raw code for the level of physical harm to the patient recorded for the LFPSE submission.",
  "LFPSE_PSYCHOLOGICAL_HARM": "Raw code for the level of psychological harm to the patient recorded for the LFPSE submission.",
  "LFPSE_STRENGTH_OF_ASSOCIATION": "Raw code for the strength of association between the incident and the patient harm recorded for the LFPSE submission.",
  "LINK_ABS_END": "End date of the contact's absence recorded against this link.",
  "LINK_ABS_START": "Start date of the contact's absence recorded against this link.",
  "LINK_AGE": "Age of the linked contact recorded against the link to the Datix record.",
  "LINK_AGE_BAND": "Age band of the linked contact recorded against this link.",
  "LINK_ATTEMPTED_ASSAULT": "Indicator of whether the incident involving the linked contact involved an attempted assault.",
  "LINK_BECOME_UNCONSCIOUS": "Indicator of whether the linked contact became unconscious as a result of the incident.",
  "LINK_BODYPART1": "Primary body part injured for the linked contact in relation to the linked Datix record.",
  "LINK_CLIN_FACTORS": "Clinical factors recorded for the linked contact in relation to the incident.",
  "LINK_DATE_ADMISSION": "Date of admission recorded for the linked contact on the Datix record.",
  "LINK_DAYSAWAY": "Number of days the linked contact was away from work as a result of the event.",
  "LINK_DECEASED": "Indicator of whether the linked contact is deceased.",
  "LINK_DIRECT_INDIRECT": "Indicator of whether the linked contact was directly or indirectly involved in, or affected by, the incident.",
  "LINK_DISCOMFORT_CAUSED": "Indicator of whether discomfort was caused to the linked contact in the incident.",
  "LINK_DOD": "Date of death of the linked contact as recorded for this link.",
  "LINK_HARASSMENT": "Indicator of whether the incident involving the linked contact involved harassment.",
  "LINK_HOSPITAL_24HOURS": "Indicator of whether the linked contact was admitted to hospital for more than 24 hours following the incident.",
  "LINK_INJURIES": "Injury details recorded for the linked contact in relation to the linked Datix record.",
  "LINK_INJURY1": "Primary injury recorded for the linked contact in relation to the linked Datix record.",
  "LINK_INJURY_CAUSED": "Indicator of whether injury was caused to the linked contact in the incident.",
  "LINK_IS_RIDDOR": "Indicator of whether the event is reportable under RIDDOR for the linked contact.",
  "LINK_MHACT": "Indicator of whether the linked contact was detained under the Mental Health Act.",
  "LINK_MHACT_SECTION": "Mental Health Act section applicable to the linked contact.",
  "LINK_MHCPA": "Mental health Care Programme Approach status recorded for the linked contact.",
  "LINK_MH_OBSERVE": "Mental health observation level recorded for the linked contact.",
  "LINK_NPSA": "NPSA-related classification recorded for the linked contact on the Datix record.",
  "LINK_NPSA_ROLE": "NPSA-related role of the linked contact in the Datix record.",
  "LINK_PATRELATION": "Relationship of the linked contact to the patient involved in the Datix record.",
  "LINK_PRIMARY": "Flag for whether this contact link is the primary link for the contact on the associated Datix record.",
  "LINK_RECORDID": "Datix identifier for the contact link record within the link_contacts table.",
  "LINK_REQ_RESUSCITATION": "Indicator of whether the linked contact required resuscitation as a result of the incident.",
  "LINK_RIDDOR": "RIDDOR reporting detail recorded for the contact on the linked Datix record.",
  "LINK_ROLE": "Code indicating the role the contact plays in relation to the linked Datix record.",
  "LINK_ROLE_DISPLAY": "Descriptive label for the Datix link role code.",
  "LINK_STATUS": "Status of the contact in relation to the linked Datix record.",
  "LINK_TREATMENT": "Treatment received by the linked contact in connection with the Datix record.",
  "LINK_TYPE": "Code indicating the type of link between the contact and the Datix record.",
  "LINK_VERBAL_ABUSE": "Indicator of whether the incident involving the linked contact involved verbal abuse.",
  "LINK_WORKED_ALONE": "Indicator of whether the linked contact was working alone at the time of the incident.",
  "LOGIN": "Datix user account login name for the contact.",
  "PARTICIPANT_CLASS": "Classification of the incident participant derived from the Datix contact type and contact link role.",
  "PARTICIPANT_SOURCE": "Indicates the source from which the safety incident participant record was derived, carried from the bronze safety incident participant mapping.",
  "PERSON_ID": "This is the value of the unique primary identifier of the person table.  It is an internal system assigned number.",
  "PERSON_RESOLUTION_RULE": "Rule used to resolve the incident participant to a person record through the patient identifier mapping.",
  "PERSON_RESOLUTION_STATUS": "Outcome status of the attempt to resolve the incident participant to a person record.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from.",
  "SOURCE_UPDT_DT_TM": "Last-update time of the row in the source system (for Datix, updateddate); NULL when the source supplies none.",
  "TOTAL_ABSENCE": "Total absence recorded for the linked contact, likely related to the absence start and end dates; units are not documented."
 },
 "map_safety_incident_status": {
  "ADC_UPDT": "When bronze last wrote the row.",
  "INC_ID": "Identifier of the clinical safety incident to which this status audit record relates, sourced from the Datix incident record ID.",
  "LOGIN": "Login of the user who recorded or changed the incident status in Datix.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from.",
  "STATUS": "Status value assigned to the safety incident at this point in its audit history.",
  "STATUS_DT_TM": "Date and time at which the incident status was recorded.",
  "STATUS_ROW_ID": "Derived identifier for an individual incident status audit row, built from the status date and user login.",
  "STATUS_SEQUENCE": "Order of this status change within the incident's status history, derived from the status date and login."
 },
 "map_safety_text_fragment": {
  "ADC_UPDT": "When bronze last wrote the row.",
  "AFFECTED_PATIENT_COUNT": "Number of patients affected by the associated safety incident.",
  "ANON_INPUT_DIGEST": "SHA-256 digest of the fragment text as presented to the redactor; a changed digest marks the row for re-anonymisation.",
  "CONTEXT_FINGERPRINT_CURRENT": "SHA-256 of the incident's current identifying context (names, birth date, postcode, reporter, investigator and participants); anonymised narrative text is released only while its anon_context_fingerprint matches this value.",
  "FIELD_NAME": "Name of the source field from which the safety text fragment was taken.",
  "FRAGMENT_ID": "Identifier of the safety text fragment, carried from the bronze safety text fragment mapping and derived from Datix action or incident record identifiers.",
  "FRAGMENT_ROLE": "Role or category of the safety text fragment within the incident documentation.",
  "INCIDENT_DT_TM": "Date and time at which the patient safety incident occurred, carried through from the source incident record via the bronze incident mappings.",
  "INCIDENT_PERSON_ID": "Internal person identifier for the patient associated with the safety incident.",
  "INC_ID": "Identifier of the Datix incident record that this LFPSE row relates to.",
  "INC_OURREF": "Local incident reference number assigned by the organisation in Datix.",
  "PIPELINE_RUN_ID": "Bronze pipeline run that last wrote the row.",
  "PRIORITY_BASIS": "Basis or rationale used to assign priority to the safety text document.",
  "PRIORITY_DT_TM": "Timestamp used to prioritise or order the safety text fragment, taken from the fragment mapping and derived from Datix action update date fields.",
  "ROW_HASH": "SHA-256 of the row's payload columns, used by the bronze MERGE to detect changes.",
  "SOURCE_ABSENT_DETECTED_TS": "When bronze first found the row absent from the source snapshot; NULL while present.",
  "SOURCE_ADC_UPDT": "When the source row was landed in raw (raw ADC_UPDT).",
  "SOURCE_OBJECT_TABLE": "Name of the source table from which the safety text fragment originated.",
  "SOURCE_PRESENT_IND": "True while the row is present in the latest full source snapshot.",
  "SOURCE_RECORD_ID": "Record identifier of the originating Datix source record, either an action or an incident, from which the text fragment was taken.",
  "SOURCE_ROW_ID": "Native source row identifier, as text.",
  "SOURCE_TABLE": "Bronze or raw source table the row came from.",
  "SOURCE_UPDT_DT_TM": "Last-update time of the row in the source system (for Datix, updateddate); NULL when the source supplies none.",
  "TEXT": "Free-text content of the safety fragment, drawn from Datix action description and action monitoring narrative fields.",
  "TEXT_LENGTH": "Character length of TEXT, the source narrative fragment before anonymisation.",
  "TEXT_SHA256": "SHA-256 of TEXT, the source narrative fragment before anonymisation.",
  "anon_context_fingerprint": "Hash of the contextual inputs the redactor used for this row; the anonymised text is released only while it matches the current context.",
  "anon_identity_fingerprint": "Hash of the patient identity details the record-driven redactor used for this row.",
  "anon_processed_at": "When the redactor last processed this row's text.",
  "anon_redaction_count": "Number of redactions the redactor applied to the source text.",
  "anon_redactor_version": "Version of the redactor that produced the anonymised text.",
  "anon_source_text_sha": "SHA-256 digest of the text the redactor processed; when it no longer matches the current input digest the anonymised text is stale.",
  "anon_status": "Outcome of the anonymisation run for this row's free text (anonymized when it succeeded).",
  "anon_text": "Anonymised version of text: identifiers detected in the free text are replaced by placeholders; published alongside the source column, which gold releases only when anonymisation succeeded."
 }
}
INSR_W8_BRONZE_TABLE_COMMENTS = {
 "map_safety_incident": "One row per Datix patient-safety incident (incident reports, not incidence rates or proven causality), keyed by Datix RECORDID. Curated bronze input to silver clinical_safety_incident; source-faithful columns, decodes in silver.",
 "map_safety_incident_action": "One row per action recorded against a Datix incident, keyed by RECORDID. Curated bronze input to silver clinical_safety_incident_action; source-faithful columns, decodes in silver.",
 "map_safety_incident_factor": "One row per contributory factor (medication, device or process) recorded on a Datix incident. Curated bronze input to silver clinical_safety_incident_factor; source-faithful columns, decodes in silver.",
 "map_safety_incident_injury": "One row per recorded injury on a Datix incident, keyed by RECORDID. Curated bronze input to silver clinical_safety_incident_injury; source-faithful columns, decodes in silver.",
 "map_safety_incident_lfpse": "One row per LFPSE (Learn from Patient Safety Events) submission linked to a Datix incident, keyed by RECORDID. Curated bronze input to silver clinical_safety_incident_lfpse; source-faithful columns, decodes in silver.",
 "map_safety_incident_participant": "One row per person linked to a Datix incident (patient, staff or other), keyed by the Datix link record id. Curated bronze input to silver clinical_safety_incident_participant; source-faithful columns, decodes in silver.",
 "map_safety_incident_status": "One row per status change of a Datix incident. Curated bronze input to silver clinical_safety_incident_status; source-faithful columns, decodes in silver.",
 "map_safety_text_fragment": "One row per Datix incident narrative fragment (description, action, outcome and similar free text), with the released anonymised text. Curated bronze input to silver text_safety_document; source-faithful columns, decodes in silver."
}


def _insr_w8_sql(text):
    """Escape a comment for a single-quoted SQL literal."""
    return text.replace("\\", "\\\\").replace("'", "\\'")


def _insr_w8_comment(table, column):
    """Dictionary comment for one bronze column; KeyError for a column the dictionary does not know."""
    return INSR_W8_BRONZE_COMMENTS[table.split('.')[-1]][column]


def _insr_w8_assert_covered(table, columns):
    """Refuse to publish a table with a column the comment dictionary does not cover (checked before any write)."""
    known = INSR_W8_BRONZE_COMMENTS[table.split('.')[-1]]
    missing = [c for c in columns if c not in known]
    if missing:
        raise KeyError(f"INSR_W8_COMMENTS_V1: {table} columns missing from the comment dictionary: {missing}")


def governance(table):
    spark.sql(f"COMMENT ON TABLE {qname(table)} IS '{_insr_w8_sql(INSR_W8_BRONZE_TABLE_COMMENTS[table.split('.')[-1]])}'")
    # INSR_W1_TAG_HEURISTIC_RETIRED_V1: ig_risk/ig_severity come from Table Metadata Compliance (estate-copied), never from column names.
    for c in spark.table(table).columns:
        spark.sql(f"ALTER TABLE {qname(table)} ALTER COLUMN `{c.replace('`','``')}` COMMENT '{_insr_w8_sql(_insr_w8_comment(table, c))}'")

def publish(df,target,keys,snapshot=False):
    _insr_w8_assert_covered(target, df.columns)  # INSR_W8_COMMENTS_V1: before any write
    if SAMPLE_MODE and REUSE_SAMPLE_TABLES and spark.catalog.tableExists(target) and target.split(".")[-1] not in FORCE_SAMPLE_TARGETS:
        return {"target":target,"operation":"REUSED_SAMPLE","rows":spark.table(target).count()}
    def validate(candidate):
        assert candidate.where(reduce(lambda a,b:a|b,[F.col(k).isNull() for k in keys])).limit(1).count()==0,f"{target}: null key"
        assert candidate.groupBy(*keys).count().where("count>1").limit(1).count()==0,f"{target}: duplicate key"
    if SAMPLE_MODE:
        df.write.format("delta").mode("overwrite").option("overwriteSchema","true").option("delta.enableChangeDataFeed","true").option("delta.enableRowTracking","true").saveAsTable(target)
        validate(spark.table(target)); op="SAMPLE_OVERWRITE"
    elif not spark.catalog.tableExists(target):
        validate(df)
        df.write.format("delta").mode("overwrite").option("overwriteSchema","true").option("delta.enableChangeDataFeed","true").option("delta.enableRowTracking","true").saveAsTable(target)
        op="CREATE"
    else:
        validate(df)
        # PMS_P1_T0_SCHEMA_V1: additive schema evolution; the explicit MERGE set cannot add columns.
        have={c.lower() for c in spark.table(target).columns}
        add=[f for f in df.schema.fields if f.name.lower() not in have]
        if add:
            spark.sql(f"ALTER TABLE {qname(target)} ADD COLUMNS ("+", ".join(f"`{f.name.replace('`','``')}` {f.dataType.simpleString()}" for f in add)+")")
        dt=DeltaTable.forName(spark,target); cond=" AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
        mutable=[c for c in df.columns if c not in keys and not c.startswith("anon_")]
        m=(dt.alias("t").merge(df.alias("s"),cond).whenMatchedUpdate(condition="NOT (t.ROW_HASH <=> s.ROW_HASH)",set={c:f"s.`{c}`" for c in mutable}).whenNotMatchedInsertAll())
        if snapshot: m=m.whenNotMatchedBySourceUpdate(condition="t.SOURCE_PRESENT_IND=true",set={"SOURCE_PRESENT_IND":"false","SOURCE_ABSENT_DETECTED_TS":"current_timestamp()","ADC_UPDT":"current_timestamp()"})
        m.execute(); op="FULL_MERGE"
    spark.sql(f"ALTER TABLE {qname(target)} SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true','delta.enableRowTracking'='true','delta.enableDeletionVectors'='true')")
    governance(target)
    return {"target":target,"operation":op,"rows":spark.table(target).count()}

def datix_decode(df,code_col,code_table,out_col=None):
    out_col=out_col or code_col+"_DISPLAY"; key=f"__{out_col}_key"
    lk=spark.table(f"{RAW}.{code_table}").select(F.upper(F.trim("code")).alias(key),F.col("description").alias(out_col)).dropDuplicates([key])
    return df.join(F.broadcast(lk),F.upper(F.trim(F.col(code_col)))==F.col(key),"left").drop(key)

# COMMAND ----------

incident_source=spark.table(SRC_INCIDENTS)
if SAMPLE_MODE:
    # Stratify on incident type so the bounded cohort includes patient, staff, environment and security events.
    per=max(1,SAMPLE_INCIDENTS//5)
    incident_source=reduce(lambda a,b:a.unionByName(b),[incident_source.where(F.upper(F.trim("inc_type"))==t).limit(per) for t in ["PATIE1","PATIE2","STAFF1","ENVIR1","SECUR1"]]).dropDuplicates(["recordid"])
INC_IDS=incident_source.select(F.col("recordid").cast("long").alias("_inc_id"))
links=active_incident_filter(spark.table(SRC_LINKS))
contact_ids=links.select(F.col("con_id").cast("long").alias("_con_id")).where("_con_id IS NOT NULL").distinct()
contacts=spark.table(SRC_CONTACTS).alias("c").join(F.broadcast(contact_ids),F.col("c.recordid").cast("long")==F.col("_con_id"),"inner").drop("_con_id")

# PMS_P1_T1_V3: resolve the whole contact cohort by join; the former collect()/limit(10000) lookup
# silently capped resolution at 10k aliases in full mode.
# Keys: MRN compared letters+digits only on both sides (Datix holds spaced/dashed/slashed forms);
# current MRN aliases win, historic aliases are used only for a key no current alias holds. The NHS key
# is con_nhsno, else a mod-11-valid 10-digit con_number (NHS numbers typed into the MRN field).
# DOB refutation: a single-identifier link (MRN-only or NHS-only) is withheld as conflict_dob when both
# DOBs exist and disagree beyond Datix entry noise (+/-1 day for UTC births, one of year/month/day
# mistyped, or day/month swapped). Measured 2026-09-29: MRN-only links carried ~3x the wholly-different-DOB
# rate of MRN+NHS-agreeing links, i.e. MRN collisions. MRN+NHS agreement is not refuted.
# Core rescue: an unresolved, ambiguous or DOB-refuted contact takes the one person whose MRN digit core
# (digits, leading zeros dropped, >=5 long) matches and whose DOB agrees within +/-1 day, unless a unique
# NHS match names someone else. Measured 2026-09-29: 24.6k rescued; a 400-day-shifted DOB null model gave 2.
person_dob=spark.table(SRC_PERSON).select(F.col("person_id").cast("long").alias("_pid"),F.col("birth_date").alias("_birth_date"))
def _alnum(c): return F.upper(F.regexp_replace(c,"[^A-Za-z0-9]",""))
def _core(c):
    k=F.regexp_replace(F.regexp_replace(c,"[^0-9]",""),"^0+","")
    return F.when(F.length(k)>=5,k)
_mrn_digits=F.regexp_replace("con_number","[^0-9]","")
_nhs_ok=(F.length(_mrn_digits)==10)&F.upper(F.trim("con_number")).rlike(r"^[0-9][0-9 \-]*$")&(
    F.pmod(F.lit(11)-F.pmod(reduce(lambda a,b:a+b,[F.substring(_mrn_digits,i,1).cast("int")*(11-i) for i in range(1,10)]),F.lit(11)),F.lit(11))
    ==F.substring(_mrn_digits,10,1).cast("int"))
keys=contacts.select(F.col("recordid").cast("long").alias("CON_ID"),_alnum("con_number").alias("_mrn"),
    F.coalesce(F.nullif(F.regexp_replace("con_nhsno","[^0-9]",""),F.lit("")),F.when(_nhs_ok,_mrn_digits)).alias("_nhs"),F.to_date("con_dob").alias("_dob"),_core("con_number").alias("_core"))
ids=spark.table(SRC_IDENTIFIERS)
mrn_keys=keys.select("_mrn").where("_mrn IS NOT NULL AND _mrn<>''").distinct()
nhs_keys=keys.select("_nhs").where("_nhs IS NOT NULL AND _nhs<>''").distinct()
mrn=(ids.where(F.col("ALIAS_TYPE")=="MRN").select(_alnum("ALIAS_VALUE").alias("_mrn"),F.col("PERSON_ID").cast("long").alias("_p"),F.col("CURRENT_IND").alias("_cur"))
    .join(mrn_keys,"_mrn","left_semi").where("_p IS NOT NULL")
    # Current aliases hold the key when any exist; otherwise its historic aliases do.
    .withColumn("_tier",F.max(F.col("_cur").cast("int")).over(Window.partitionBy("_mrn"))).where(F.coalesce(F.col("_cur").cast("int"),F.lit(0))==F.col("_tier"))
    .groupBy("_mrn")
    .agg(F.countDistinct("_p").alias("_mrn_n"),F.max("_p").alias("_mrn_person"),F.collect_set("_p").alias("_mrn_cands")))
nhs=(ids.where((F.col("ALIAS_TYPE")=="NHS")&F.col("CURRENT_IND")).select(F.regexp_replace("ALIAS_VALUE","[^0-9]","").alias("_nhs"),F.col("PERSON_ID").cast("long").alias("_p"))
    .join(nhs_keys,"_nhs","left_semi").groupBy("_nhs")
    .agg(F.countDistinct("_p").alias("_nhs_n"),F.max("_p").alias("_nhs_person")))

core_keys=keys.select("_core").where("_core IS NOT NULL").distinct()
mrn_core=(ids.where(F.col("ALIAS_TYPE")=="MRN").select(_core("ALIAS_VALUE").alias("_core"),F.col("PERSON_ID").cast("long").alias("_cp"))
    .join(core_keys,"_core","left_semi").where("_cp IS NOT NULL").distinct())
core_pick=(keys.where("_core IS NOT NULL AND _dob IS NOT NULL").select("CON_ID","_core","_dob").join(mrn_core,"_core")
    .join(person_dob.select(F.col("_pid").alias("_cp"),F.col("_birth_date").alias("_cbd")),"_cp")
    .where(F.abs(F.datediff("_cbd","_dob"))<=1)
    .groupBy("CON_ID").agg(F.countDistinct("_cp").alias("_core_n"),F.max("_cp").alias("_core_any"))
    .select("CON_ID",F.when(F.col("_core_n")==1,F.col("_core_any")).alias("_core_person")))

def dob_consistent(a,b):
    ya,yb,ma,mb,da,db=F.year(a),F.year(b),F.month(a),F.month(b),F.dayofmonth(a),F.dayofmonth(b)
    return ((F.abs(F.datediff(a,b))<=1)|((ya==yb)&(ma==mb))|((ya==yb)&(da==db))|((ma==mb)&(da==db))|((ya==yb)&(ma==db)&(da==mb)))

def resolved_contacts():
    j=keys.join(mrn,"_mrn","left").join(nhs,"_nhs","left")
    # A multi-person MRN is rescued only when exactly one candidate's DOB agrees within +/-1 day
    # (mill BIRTH_DT_TM is UTC, so BST births land a day early).
    dob_pick=(j.where(F.col("_mrn_n")>1).select("CON_ID","_dob",F.explode("_mrn_cands").alias("_cand"))
        .join(person_dob,F.col("_cand")==F.col("_pid"))
        .where(F.abs(F.datediff("_birth_date","_dob"))<=1)
        .groupBy("CON_ID").agg(F.countDistinct("_cand").alias("_dob_n"),F.max("_cand").alias("_dob_person_any"))
        .select("CON_ID",F.when(F.col("_dob_n")==1,F.col("_dob_person_any")).alias("_dob_person")))
    j=j.join(dob_pick,"CON_ID","left")
    mrn_multi=F.col("_mrn_n")>1; nhs_multi=F.col("_nhs_n")>1; mrn_ok=F.col("_mrn_n")==1; nhs_ok=F.col("_nhs_n")==1
    agree=mrn_ok&nhs_ok&(F.col("_mrn_person")==F.col("_nhs_person")); conflict=mrn_ok&nhs_ok&(F.col("_mrn_person")!=F.col("_nhs_person"))
    dob_rescue=mrn_multi&F.col("_dob_person").isNotNull()&(~F.coalesce(nhs_ok,F.lit(False))|(F.col("_nhs_person")==F.col("_dob_person")))
    status=(F.when(conflict,"ambiguous").when(agree,"resolved_mrn_nhs").when(dob_rescue,"resolved_mrn_dob")
        .when(mrn_multi|nhs_multi,"ambiguous").when(mrn_ok,"resolved_mrn").when(nhs_ok,"resolved_nhs").otherwise("unresolved"))
    person=(F.when(status=="resolved_mrn_dob",F.col("_dob_person"))
        .when(status.startswith("resolved"),F.coalesce(F.when(mrn_ok,F.col("_mrn_person")),F.when(nhs_ok,F.col("_nhs_person")))))
    j=(j.withColumn("_status",status).withColumn("_person",person.cast("long"))
        .join(person_dob,F.col("_person")==F.col("_pid"),"left"))
    refuted=F.col("_status").isin("resolved_mrn","resolved_nhs")&F.col("_dob").isNotNull()&F.col("_birth_date").isNotNull()&~dob_consistent(F.col("_dob"),F.col("_birth_date"))
    j=j.join(core_pick,"CON_ID","left")
    base=F.when(refuted,"conflict_dob").otherwise(F.col("_status"))
    core_ok=(base.isin("unresolved","ambiguous","conflict_dob")&F.col("_core_person").isNotNull()
        &~(F.coalesce(nhs_ok,F.lit(False))&(F.col("_nhs_person")!=F.col("_core_person"))))
    return j.select("CON_ID",F.when(core_ok,F.col("_core_person")).when(~refuted,F.col("_person")).alias("PERSON_ID"),
        F.when(core_ok,"resolved_mrn_core_dob").otherwise(base).alias("PERSON_RESOLUTION_STATUS"),
        F.lit("TDX-P3 full-cohort join; alnum MRN (current aliases, else historic); NHS from con_nhsno else mod-11 10-digit con_number; multi-match-first except DOB+/-1d-unique MRN; single-identifier links withheld on DOB conflict beyond one-field typo; else unique MRN digit-core + DOB+/-1d; mrn>nhs unique-only 2026-09-29").alias("PERSON_RESOLUTION_RULE"),
        F.col("_dob"))
# TDX_P4_SAFETY_LINKAGE_V1 (2026-10-25) — corroborated second pass over Datix patient contacts.
# TDX-P3 above is unchanged. This pass only touches PATC contacts that P3 left 'unresolved' or withheld as
# 'conflict_dob'; 'ambiguous' (MRN and NHS name different people) stays as it is. Every link it adds carries
# its own PERSON_RESOLUTION_STATUS and a per-row PERSON_RESOLUTION_RULE naming key rule, alias pool and evidence.
# Evidence is name-free: DOB agreement, an encounter of the candidate person overlapping the incident date
# (+/-1 day), and contact sex vs Cerner sex as a refuter only.
# Measured 2026-10-25 on prod bronze (distinct PATC contacts; sex agreement is the precision proxy: true
# links ~0.99, random links ~0.50):
#  - conflict_dob with an exact MRN person: 11,100. With an encounter on the incident date 7,384 (sex 0.94);
#    without one 3,716 (sex 0.58, i.e. mostly wrong people). Random MRN pairings meet an encounter ~0.6%.
#    Within the encounter group the DOB pattern matters: a placeholder/future Datix DOB (sex 0.99) or one
#    within a year of Cerner's (0.97) is a typing error; a DOB near the incident date (0.89, typically a
#    baby recorded under the mother's MRN) or 15-50 years away (0.90, typically a parent/child) is a
#    different person, so those stay conflict_dob.
#  - unresolved with a normalised MRN key: DOB +/-1d or one-field typo -> sex 0.97-1.00; DOB missing but an
#    encounter on the incident date -> 0.94-1.00; DOB conflicting even with an encounter -> 0.80-0.92
#    (rejected); dropping a Datix letter prefix to hit a bare-digit alias -> 0.48 (rule not used: it mostly
#    lands on BHRUT 8-digit numbers).
#  - BHRUT RF4 MRNs never collide with a Barts MRN on an exact key; for normalised keys Barts aliases win and
#    BHRUT aliases are only a fallback when no Barts alias carries the key.
P4_RULE_VERSION = "TDX-P4 2026-10-25"
P4_POOL_TIER = {6200990: 1, 1115132483: 2}          # Barts RNJ MRN pool first, BHRUT RF4 MRN pool second
P4_POOL_NAME = {1: "barts", 2: "bhrut", 3: "other"}
P4_LABEL_PREFIX = "^(MRN|HOSPNO|HOSPITALNO|HOSPNUMBER|RNJ|PAS)"   # typed labels seen in front of real MRNs


def p4_mrn_parts(key):
    # Split an upper-case alphanumeric MRN key into (letter prefix, digit body with leading zeros dropped,
    # letter suffix). All three are NULL when the key is not shaped letters-digits-letters.
    shaped = key.rlike("^[A-Z]*[0-9]+[A-Z]*$")
    prefix = F.when(shaped, F.regexp_extract(key, "^([A-Z]*)[0-9]", 1))
    body = F.when(shaped, F.regexp_replace(F.regexp_extract(key, "^[A-Z]*([0-9]+)[A-Z]*$", 1), "^0+", ""))
    suffix = F.when(shaped, F.regexp_extract(key, "^[A-Z]*[0-9]+([A-Z]*)$", 1))
    return prefix, body, suffix


def p4_alias_parts():
    # All MRN aliases (current and historic) with their key parts and pool tier.
    tier = F.coalesce(*[F.when(F.col("ALIAS_POOL_CD") == pool, F.lit(t)) for pool, t in P4_POOL_TIER.items()], F.lit(3))
    aliases = (ids.where(F.col("ALIAS_TYPE") == "MRN")
        .select(_alnum("ALIAS_VALUE").alias("_v"), F.col("PERSON_ID").cast("long").alias("_cand"), tier.alias("_tier"))
        .where("_cand IS NOT NULL"))
    prefix, body, suffix = p4_mrn_parts(F.col("_v"))
    return aliases.select("_v", "_cand", "_tier", prefix.alias("_aL"), body.alias("_aD"), suffix.alias("_aS"))


def p4_target_contacts(p3):
    # PATC contacts that P3 left unresolved or withheld for a DOB conflict, with their keys, DOB, sex and the
    # unique-MRN / unique-NHS persons P3 saw (used as the exact candidate and as the NHS veto).
    patc = contacts.where(F.upper(F.trim("con_type")) == "PATC").select(
        F.col("recordid").cast("long").alias("CON_ID"),
        F.when(F.upper(F.trim("con_gender")).isin("M", "F"), F.upper(F.trim("con_gender"))).alias("_sex"))
    stripped = F.regexp_replace(F.col("_mrn"), P4_LABEL_PREFIX, "")
    prefix, body, suffix = p4_mrn_parts(stripped)
    return (p3.where(F.col("PERSON_RESOLUTION_STATUS").isin("unresolved", "conflict_dob")).select("CON_ID", "PERSON_RESOLUTION_STATUS")
        .join(patc, "CON_ID")
        .join(keys, "CON_ID")
        .join(mrn.select("_mrn", F.when(F.col("_mrn_n") == 1, F.col("_mrn_person")).alias("_exact_mrn_person")), "_mrn", "left")
        .join(nhs.select("_nhs", F.when(F.col("_nhs_n") == 1, F.col("_nhs_person")).alias("_exact_nhs_person")), "_nhs", "left")
        .select("CON_ID", "PERSON_RESOLUTION_STATUS", "_sex", "_dob", "_mrn", "_exact_mrn_person", "_exact_nhs_person",
                stripped.alias("_k"), prefix.alias("_L"), body.alias("_D"), suffix.alias("_S")))


def p4_candidates(targets):
    # One row per (contact, rule, candidate person, pool tier). Rules in precedence order:
    #  1 exact_mrn / exact_nhs: the unique person P3 found but refuted on DOB (conflict_dob only)
    #  2 label_strip:   key equals an alias after removing a typed label such as 'MRN'
    #  3 zero_pad:      same prefix, digits and suffix once leading zeros are ignored on both sides
    #  4 alias_suffix:  Datix carries prefix+digits only; the alias adds trailing check letters
    #  5 alias_prefix:  Datix carries digits(+suffix) only; the alias adds a site letter prefix
    #  6 digit_core:    digits alone (>= 5 long, leading zeros ignored)
    a = p4_alias_parts()
    t = targets.alias("t")
    def joined(rule, order, condition):
        # Candidates for one normalisation rule, labelled with its name and precedence.
        return (t.join(a, condition).select("t.CON_ID", F.lit(rule).alias("_rule"), F.lit(order).alias("_order"), "_cand", "_tier"))
    exact = (t.where("PERSON_RESOLUTION_STATUS = 'conflict_dob'")
        .select("CON_ID", F.when(F.col("_exact_mrn_person").isNotNull(), "exact_mrn").otherwise("exact_nhs").alias("_rule"), F.lit(1).alias("_order"),
                F.coalesce("_exact_mrn_person", "_exact_nhs_person").alias("_cand"), F.lit(0).alias("_tier"))
        .where("_cand IS NOT NULL"))
    parts = [
        exact,
        joined("label_strip", 2, (F.col("t._k") != F.col("t._mrn")) & (F.col("_v") == F.col("t._k"))),
        joined("zero_pad", 3, (F.col("_aL") == F.col("t._L")) & (F.col("_aD") == F.col("t._D")) & (F.col("_aS") == F.col("t._S"))),
        joined("alias_suffix", 4, (F.col("_aL") == F.col("t._L")) & (F.col("_aD") == F.col("t._D")) & (F.col("t._S") == "") & (F.col("_aS") != "")),
        joined("alias_prefix", 5, (F.col("t._L") == "") & (F.col("_aL") != "") & (F.col("_aD") == F.col("t._D")) & (F.col("_aS") == F.col("t._S"))),
        joined("digit_core", 6, (F.length(F.col("t._D")) >= 5) & (F.col("_aD") == F.col("t._D"))),
    ]
    found = reduce(lambda x, y: x.unionByName(y), parts).dropDuplicates(["CON_ID", "_rule", "_cand"])
    # Within a rule keep only the best pool tier present (Barts before BHRUT), then require one person.
    best = Window.partitionBy("CON_ID", "_rule")
    found = found.withColumn("_tier_best", F.min("_tier").over(best)).where(F.col("_tier") == F.col("_tier_best"))
    return (found.groupBy("CON_ID", "_rule", "_order").agg(F.countDistinct("_cand").alias("_n"), F.max("_cand").alias("_cand"), F.min("_tier").alias("_tier"))
        .where("_n = 1").drop("_n"))


def p4_incident_dates():
    # The (contact, incident date) pairs: classic links plus LFPSE patient rows, on clean incident dates.
    lfpse_links = active_incident_filter(spark.table(SRC_LFPSE_PATIENT)).select("con_id", "inc_id")
    pairs = links.select("con_id", "inc_id").unionByName(lfpse_links)
    dates = incident_source.select(F.col("recordid").cast("long").alias("_iid"), F.to_date("inc_dincident").alias("_inc_date")).where(F.col("_inc_date") >= F.lit("1990-01-01").cast("date"))
    return (pairs.select(F.col("con_id").cast("long").alias("CON_ID"), F.col("inc_id").cast("long").alias("_iid"))
        .join(dates, "_iid").select("CON_ID", "_inc_date").distinct())


def p4_encounter_hits(cands):
    # (contact, candidate) pairs where the candidate has an IP/ED/OP/Recurring encounter overlapping one of the
    # contact's incident dates, +/-1 day, on map_encounter *_BEST bounds converted to Europe/London.
    people = cands.select(F.col("_cand").alias("_epid")).distinct()
    enc_days = (spark.table(SRC_ENCOUNTER).where(F.col("encntr_type_class_desc").isin("Inpatient", "Emergency", "Outpatient", "Recurring"))
        .select(F.col("PERSON_ID").cast("long").alias("_epid"),
                F.to_date(F.from_utc_timestamp("ARRIVAL_DT_TM_BEST", "Europe/London")).alias("_a"),
                F.to_date(F.from_utc_timestamp("DEPARTURE_DT_TM_BEST", "Europe/London")).alias("_d"))
        .where("_a IS NOT NULL").join(people, "_epid", "left_semi")
        .withColumn("_d", F.greatest(F.coalesce("_d", "_a"), F.col("_a"))))
    pairs = cands.select("CON_ID", "_cand").distinct().join(p4_incident_dates(), "CON_ID")
    return (pairs.join(enc_days, (F.col("_epid") == F.col("_cand")) & (F.col("_a") <= F.date_add("_inc_date", 1)) & (F.col("_d") >= F.date_sub("_inc_date", 1)))
        .select("CON_ID", "_cand").distinct().withColumn("_enc", F.lit(True)))


def extend_with_tdx_p4(p3):
    # Apply the corroborated second pass and return P3's frame with the same columns, where rescued contacts
    # gain PERSON_ID, a new status and a per-row rule. Acceptance (first rule in precedence order that passes):
    #  exact_mrn / exact_nhs -> encounter on the incident date, no sex refutation, and the DOB conflict looks like
    #                           a typing error: placeholder/future Datix DOB, or within a year of Cerner's
    #                           (status resolved_<key>_encounter)
    #  normalised rules      -> no sex refutation and (DOB +/-1d, or DOB one-field typo, or a DOB missing and an
    #                           encounter on the incident date); a conflicting DOB is never accepted
    #                           (status resolved_mrn_normalised)
    # The NHS veto from P3's core rescue also applies: a unique NHS match naming someone else blocks the link.
    targets = p4_target_contacts(p3)
    cands = p4_candidates(targets)
    person_sex = spark.table(SRC_PERSON).select(F.col("person_id").cast("long").alias("_cand"),
        F.when(F.upper(F.substring("gender_display", 1, 1)).isin("M", "F"), F.upper(F.substring("gender_display", 1, 1))).alias("_csex"))
    ev = (cands.join(targets.select("CON_ID", "_dob", "_sex", "_exact_nhs_person"), "CON_ID")
        .join(person_dob.select(F.col("_pid").alias("_cand"), F.col("_birth_date").alias("_cbd")), "_cand", "left")
        .join(person_sex, "_cand", "left")
        .join(p4_encounter_hits(cands), ["CON_ID", "_cand"], "left")
        .withColumn("_enc", F.coalesce("_enc", F.lit(False))))
    dob_known = F.col("_dob").isNotNull() & F.col("_cbd").isNotNull()
    dob_1d = dob_known & (F.abs(F.datediff("_cbd", "_dob")) <= 1)
    dob_typo = dob_known & ~dob_1d & dob_consistent(F.col("_dob"), F.col("_cbd"))
    dob_placeholder = (F.col("_dob") < F.lit("1901-01-01").cast("date")) | (F.col("_dob") > F.current_date())
    dob_within_1y = dob_known & ~dob_1d & ~dob_typo & (F.abs(F.datediff("_cbd", "_dob")) <= 366)
    sex_refuted = F.col("_sex").isNotNull() & F.col("_csex").isNotNull() & (F.col("_sex") != F.col("_csex"))
    nhs_veto = F.col("_exact_nhs_person").isNotNull() & (F.col("_exact_nhs_person") != F.col("_cand")) & ~F.col("_rule").isin("exact_nhs")
    is_exact = F.col("_rule").startswith("exact_")
    exact_ok = F.col("_enc") & (dob_placeholder | dob_within_1y)
    normalised_ok = dob_1d | dob_typo | (~dob_known & F.col("_enc"))
    accepted = ~sex_refuted & ~nhs_veto & F.when(is_exact, exact_ok).otherwise(normalised_ok)
    evidence = (F.when(dob_1d, "dob_1d").when(dob_typo, "dob_one_field_typo").when(dob_known & dob_placeholder, "dob_placeholder")
        .when(dob_within_1y, "dob_within_1y").when(dob_known, "dob_conflict").otherwise("dob_missing"))
    ev = ev.where(accepted).withColumn("_evidence", F.concat(evidence, F.when(F.col("_enc"), F.lit("+encounter_on_incident_date")).otherwise(F.lit(""))))
    first = Window.partitionBy("CON_ID").orderBy(F.col("_order").asc(), F.col("_cand").asc())
    pick = ev.withColumn("_rn", F.row_number().over(first)).where("_rn = 1")
    pool_name = F.when(F.col("_tier") == 0, "exact").otherwise(F.coalesce(*[F.when(F.col("_tier") == t, F.lit(n)) for t, n in P4_POOL_NAME.items()]))
    pick = pick.select("CON_ID", F.col("_cand").alias("_p4_person"),
        F.when(F.col("_rule") == "exact_mrn", "resolved_mrn_encounter").when(F.col("_rule") == "exact_nhs", "resolved_nhs_encounter")
         .otherwise("resolved_mrn_normalised").alias("_p4_status"),
        F.concat_ws("; ", F.lit(P4_RULE_VERSION), F.concat(F.lit("key="), F.col("_rule")), F.concat(F.lit("pool="), pool_name),
                    F.concat(F.lit("evidence="), F.col("_evidence"))).alias("_p4_rule"))
    out = p3.join(pick, "CON_ID", "left")
    return out.select("CON_ID",
        F.coalesce("_p4_person", "PERSON_ID").alias("PERSON_ID"),
        F.coalesce("_p4_status", "PERSON_RESOLUTION_STATUS").alias("PERSON_RESOLUTION_STATUS"),
        F.coalesce("_p4_rule", "PERSON_RESOLUTION_RULE").alias("PERSON_RESOLUTION_RULE"),
        "_dob")

resolved=extend_with_tdx_p4(resolved_contacts())

# COMMAND ----------

l=links.alias("l"); c=contacts.alias("c"); r=resolved.alias("r")
classic=l.join(c,F.col("l.con_id").cast("long")==F.col("c.recordid").cast("long"),"left").join(r,F.col("l.con_id").cast("long")==F.col("r.CON_ID"),"left").join(person_dob,F.col("r.PERSON_ID")==F.col("_pid"),"left")
classic=classic.select(
    F.concat(F.lit("C:"),F.col("l.link_recordid").cast("long")).alias("LINK_RECORDID"),F.lit("classic").alias("PARTICIPANT_SOURCE"),F.col("l.inc_id").cast("long").alias("INC_ID"),F.col("l.con_id").cast("long").alias("CON_ID"),
    F.col("l.link_type").alias("LINK_TYPE"),F.upper(F.trim("l.link_role")).alias("LINK_ROLE"),F.col("c.con_type").alias("CON_TYPE"),
    F.when((F.upper(F.col("c.con_type"))=="PATC")| (F.upper(F.col("l.link_role"))=="PATIEN"),"patient").when(F.upper(F.col("c.con_type")).endswith("C"),"staff").otherwise("other").alias("PARTICIPANT_CLASS"),
    F.col("r.PERSON_ID"),F.col("r.PERSON_RESOLUTION_STATUS"),F.col("r.PERSON_RESOLUTION_RULE"),(F.to_date("c.con_dob")==F.col("_birth_date")).alias("DOB_AGREES_IND"),
    *[F.col(f"c.{x}").alias(x.upper() if x!="con_number" else "CON_MRN") for x in ["con_surname","con_forenames","con_title","con_dob","con_gender","con_ethnicity","con_nhsno","con_number","con_postcode","con_dod","con_language","con_disability","con_religion","con_sex_orientation","con_address","con_tel1","con_tel2","con_fax","con_email","login","con_jobtitle","con_empl_grade","con_specialty","con_directorate","con_clingroup","con_unit","con_locactual","con_orgcode","con_subtype"]],
    *[F.col(f"l.{x}").alias(x.upper()) for x in ["link_age","link_age_band","link_deceased","link_dod","link_date_admission","link_patrelation","link_injuries","link_injury1","link_bodypart1","link_treatment","link_become_unconscious","link_req_resuscitation","link_hospital_24hours","link_clin_factors","link_direct_indirect","link_injury_caused","link_discomfort_caused","link_status","link_mhact","link_mhact_section","link_mhcpa","link_mh_observe","link_npsa","link_npsa_role","link_riddor","link_is_riddor","link_daysaway","link_abs_start","link_abs_end","total_absence","link_worked_alone","link_harassment","link_verbal_abuse","link_attempted_assault","link_primary"]],
    F.col("l.updateddate").alias("SOURCE_UPDT_DT_TM"),F.col("l.ADC_UPDT").alias("SOURCE_ADC_UPDT"),
)
classic=datix_decode(classic,"LINK_ROLE","datix_code_link_role","LINK_ROLE_DISPLAY")

lp=active_incident_filter(spark.table(SRC_LFPSE_PATIENT)).alias("lp")
lfp_contact=lp.join(c,F.col("lp.con_id").cast("long")==F.col("c.recordid").cast("long"),"left").join(r,F.col("lp.con_id").cast("long")==F.col("r.CON_ID"),"left")
lfp=lfp_contact.select(
    F.concat(F.lit("L:"),F.col("lp.recordid").cast("long")).alias("LINK_RECORDID"),F.lit("lfpse").alias("PARTICIPANT_SOURCE"),F.col("lp.inc_id").cast("long").alias("INC_ID"),F.col("lp.con_id").cast("long").alias("CON_ID"),
    F.lit(None).cast("string").alias("LINK_TYPE"),F.lit("PATIEN").alias("LINK_ROLE"),F.lit(None).cast("string").alias("LINK_ROLE_DISPLAY"),F.col("c.con_type").alias("CON_TYPE"),F.lit("patient").alias("PARTICIPANT_CLASS"),
    F.col("r.PERSON_ID"),F.col("r.PERSON_RESOLUTION_STATUS"),F.col("r.PERSON_RESOLUTION_RULE"),F.lit(None).cast("boolean").alias("DOB_AGREES_IND"),
    *[F.col(f"c.{x}").alias(x.upper() if x!="con_number" else "CON_MRN") for x in ["con_surname","con_forenames","con_title","con_dob","con_gender","con_ethnicity","con_nhsno","con_number","con_postcode","con_dod","con_language","con_disability","con_religion","con_sex_orientation","con_address","con_tel1","con_tel2","con_fax","con_email","login","con_jobtitle","con_empl_grade","con_specialty","con_directorate","con_clingroup","con_unit","con_locactual","con_orgcode","con_subtype"]],
    *[F.lit(None).cast("string").alias(x.upper()) for x in ["link_age","link_age_band","link_deceased","link_dod","link_date_admission","link_patrelation","link_injuries","link_injury1","link_bodypart1","link_treatment","link_become_unconscious","link_req_resuscitation","link_hospital_24hours","link_clin_factors","link_direct_indirect","link_injury_caused","link_discomfort_caused","link_status","link_mhact","link_mhact_section","link_mhcpa","link_mh_observe","link_npsa","link_npsa_role","link_riddor","link_is_riddor","link_daysaway","link_abs_start","link_abs_end","total_absence","link_worked_alone","link_harassment","link_verbal_abuse","link_attempted_assault","link_primary"]],
    F.lit(None).cast("timestamp").alias("SOURCE_UPDT_DT_TM"),F.col("lp.ADC_UPDT").alias("SOURCE_ADC_UPDT"),
    F.col("lp.recordid").cast("long").alias("LFPSE_PATIENT_RECORDID"),F.col("lp.lfpse_id").cast("long").alias("LFPSE_ID"),
    *[F.col(f"lp.{x}").alias(x.upper()) for x in ["lfpse_physical_harm","lfpse_psychological_harm","lfpse_strength_of_association","lfpse_age_at_time_of_incidents_days","lfpse_age_bracket","lfpse_gender","lfpse_patient_ethnicity","lfpse_patient_sequence"]],
)
participant=classic.unionByName(lfp,allowMissingColumns=True)
participant=finalise(participant,["LINK_RECORDID"],SRC_LINKS+"|"+SRC_LFPSE_PATIENT,"LINK_RECORDID")
participant_result=publish(participant,PARTICIPANT,["LINK_RECORDID"],snapshot=True)

# COMMAND ----------

pctx=(spark.table(PARTICIPANT).where("SOURCE_PRESENT_IND").groupBy("INC_ID").agg(
    F.sort_array(F.collect_set(F.concat_ws("~",F.col("LINK_RECORDID"),F.coalesce("CON_SURNAME",F.lit("")),F.coalesce("CON_FORENAMES",F.lit("")),F.coalesce(F.col("CON_DOB").cast("string"),F.lit("")),F.coalesce("CON_NHSNO",F.lit("")),F.coalesce("CON_MRN",F.lit("")),F.coalesce("CON_POSTCODE",F.lit("")),F.coalesce(F.col("PERSON_ID").cast("string"),F.lit(""))))).alias("_participants"),
    F.countDistinct(F.when((F.col("PARTICIPANT_CLASS")=="patient") & F.col("LINK_ROLE").isin("AFFECT","PATIEN") & F.col("PERSON_ID").isNotNull(),F.col("PERSON_ID"))).alias("AFFECTED_PATIENT_COUNT"),
    F.max(F.when((F.col("PARTICIPANT_CLASS")=="patient") & F.col("LINK_ROLE").isin("AFFECT","PATIEN") & F.col("PERSON_ID").isNotNull(),F.col("PERSON_ID"))).alias("_affected_person"),
))
i=incident_source.alias("i").join(pctx,F.col("i.recordid").cast("long")==F.col("INC_ID"),"left")
valid_time=F.col("i.inc_time").rlike(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
incident_dt=F.when(valid_time,F.to_timestamp(F.concat_ws(" ",F.date_format("i.inc_dincident","yyyy-MM-dd"),F.col("i.inc_time")),"yyyy-MM-dd HH:mm")).otherwise(F.col("i.inc_dincident"))
incident_clean=F.when(incident_dt>=F.lit("1990-01-01").cast("timestamp"),incident_dt)
header_fields=["inc_ourref","inc_type","inc_category","inc_subcategory","inc_severity","inc_grade","inc_consequence","inc_likelihood","inc_rating","inc_impact","inc_outcomecode","inc_result","inc_cause","inc_acctype","inc_riddor","inc_is_riddor","inc_riddorno","inc_ridloc","inc_notify","inc_treatment","inc_injury","inc_bodypart","inc_pat_type","inc_carestage","inc_clinoutcome","inc_clinoutcome2","inc_clintype","inc_further_inv","inc_rc_required","inc_organisation","inc_unit","inc_directorate","inc_clingroup","inc_specialty","inc_nspecialty","inc_loctype","inc_locactual","inc_location","inc_unit_type","inc_head","inc_mgr","rep_approved","seclevel","secgroup","inc_name","inc_dob","inc_gender","inc_ethnicity","inc_postcode","inc_repname","inc_reportedby","inc_investigator"]
header_fields=[x for x in header_fields if has_col(SRC_INCIDENTS,x)]
incident=i.select(F.col("i.recordid").cast("long").alias("RECORDID"),*[F.col(f"i.{x}").alias(x.upper()) for x in header_fields],incident_dt.alias("INCIDENT_DT_TM"),incident_clean.alias("INCIDENT_DT_TM_CLEAN"),valid_time.alias("INCIDENT_TIME_VALID_IND"),F.col("i.inc_dreported").alias("REPORTED_DT_TM"),F.col("i.inc_dopened").alias("OPENED_DT_TM"),F.col("i.inc_dnotified").alias("NOTIFIED_DT_TM"),F.col("i.inc_inv_dstart").alias("INVESTIGATION_START_DT_TM"),F.col("i.inc_inv_dcomp").alias("INVESTIGATION_COMPLETE_DT_TM"),F.col("i.updateddate").alias("SOURCE_UPDT_DT_TM"),F.coalesce("AFFECTED_PATIENT_COUNT",F.lit(0)).cast("long").alias("AFFECTED_PATIENT_COUNT"),F.when(F.col("AFFECTED_PATIENT_COUNT")==1,F.col("_affected_person")).cast("long").alias("PERSON_ID"),F.when(F.col("AFFECTED_PATIENT_COUNT")==1,"resolved_affected_participant").otherwise("none_or_multiple").alias("PERSON_LINK_METHOD"),F.lit(None).cast("long").alias("ENCNTR_ID"),F.when(F.col("AFFECTED_PATIENT_COUNT")==1,"sample_not_evaluated" if SAMPLE_MODE else "pending_overlap_resolution").otherwise("no_person").alias("ENCOUNTER_LINK_METHOD"),F.lit("TDX-E1 2026-09-13").alias("ENCOUNTER_LINK_RULE"),F.col("i.ADC_UPDT").alias("SOURCE_ADC_UPDT"),"_participants")
incident=incident.withColumn("CONTEXT_FINGERPRINT_CURRENT",F.sha2(F.concat_ws("|",F.coalesce("INC_NAME",F.lit("")),F.coalesce(F.col("INC_DOB").cast("string"),F.lit("")),F.coalesce("INC_POSTCODE",F.lit("")),F.coalesce("INC_REPNAME",F.lit("")),F.coalesce("INC_INVESTIGATOR",F.lit("")),F.to_json(F.coalesce("_participants",F.array().cast("array<string>")))),256)).drop("_participants")
for code,table in [("INC_TYPE","datix_code_inc_type"),("INC_CATEGORY","datix_code_inc_cat"),("INC_SUBCATEGORY","datix_code_inc_subcat"),("INC_SEVERITY","datix_code_inc_severity"),("INC_GRADE","datix_code_inc_grades"),("SECGROUP","datix_code_secgroup")]:
    if code in incident.columns: incident=datix_decode(incident,code,table,code+"_DISPLAY")
# PMS_P1_T2_V1 — TDX-E2: link an incident to the person's containing encounter. Datix times are
# Europe/London wall clock; map_encounter *_BEST bounds are UTC (the *_BEST_LOCAL columns are unpopulated).
# BEST, not raw DISCH/DEPART: BEST is NULL where the departure was rejected (ADMINISTRATIVE_CLOSE,
# FIXED_WINDOW_CLOSE, BEFORE_ARRIVAL), so rejected closure stamps never widen a window.
# Unresolved end policy: a NULL (or pre-arrival) BEST departure is bounded at arrival + 1 day. For
# Outpatient/Recurring (same-day attendances) that bound is the natural window; for Inpatient/Emergency
# it is a truncation, so those links are LOW confidence.
# An undated-time incident spans its whole day.
DAY=F.expr("INTERVAL 1 DAY")
enc=(spark.table(SRC_ENCOUNTER)
     .where(F.col("encntr_type_class_desc").isin("Inpatient","Emergency","Outpatient","Recurring"))
     .select(F.col("ENCNTR_ID").cast("long").alias("_eid"),F.col("PERSON_ID").cast("long").alias("_epid"),
        F.from_utc_timestamp("ARRIVAL_DT_TM_BEST","Europe/London").alias("_beg"),
        F.from_utc_timestamp("DEPARTURE_DT_TM_BEST","Europe/London").alias("_end_raw"),
        F.col("ARRIVAL_CONFIDENCE").alias("_aconf"),F.col("DEPARTURE_CONFIDENCE").alias("_dconf"),
        F.col("encntr_type_class_desc").isin("Inpatient","Emergency").alias("_stay"))
     .where("_eid IS NOT NULL AND _beg IS NOT NULL")
     .withColumn("_open",F.col("_end_raw").isNull()|(F.col("_end_raw")<F.col("_beg")))
     .withColumn("_end",F.when(F.col("_open"),F.col("_beg")+DAY).otherwise(F.col("_end_raw")))
     .dropDuplicates(["_eid"]))
inc_keys=(incident.select(F.col("RECORDID").alias("_rid"),F.col("PERSON_ID").alias("_ipid"),F.col("INCIDENT_DT_TM_CLEAN").alias("_t0"),
        F.when(F.col("INCIDENT_TIME_VALID_IND"),F.col("INCIDENT_DT_TM_CLEAN")).otherwise(F.col("INCIDENT_DT_TM_CLEAN")+DAY).alias("_t1"))
     .where("_ipid IS NOT NULL AND _t0 IS NOT NULL"))
cand=(inc_keys.join(enc,(F.col("_epid")==F.col("_ipid"))&(F.col("_beg")<=F.col("_t1"))&(F.col("_end")>=F.col("_t0")))
     .withColumn("_span",F.col("_end").cast("long")-F.col("_beg").cast("long")))
n_all=cand.groupBy("_rid").agg(F.countDistinct("_eid").alias("_n_all"))
narrow=(cand.withColumn("_min",F.min("_span").over(Window.partitionBy("_rid"))).where(F.col("_span")==F.col("_min"))
     .groupBy("_rid").agg(F.countDistinct("_eid").alias("_n_best"),F.max("_eid").alias("_best_eid"),
        F.max((F.col("_open")&F.col("_stay")).cast("int")).alias("_best_open"),
        F.min(F.when((F.col("_aconf")=="HIGH")&(F.col("_dconf")=="HIGH"),1).otherwise(0)).alias("_best_high")))
link=n_all.join(narrow,"_rid")
incident=(incident.join(link,F.col("RECORDID")==F.col("_rid"),"left")
    .withColumn("ENCNTR_ID",F.when(F.col("_n_best")==1,F.col("_best_eid")).cast("long"))
    .withColumn("ENCOUNTER_LINK_METHOD",F.when(F.col("PERSON_ID").isNull(),"no_person").when(F.col("_n_all").isNull(),"no_overlap")
        .when(F.col("_n_all")==1,"overlap_unique").when(F.col("_n_best")==1,"overlap_narrowest").otherwise("ambiguous"))
    .withColumn("ENCOUNTER_LINK_CONFIDENCE",F.when(F.col("ENCNTR_ID").isNull(),F.lit(None).cast("string"))
        .when(F.col("_best_open")==1,"LOW").when(F.col("_best_high")==1,"HIGH").otherwise("MEDIUM"))
    .withColumn("ENCOUNTER_LINK_RULE",F.lit("TDX-E2 containing encounter on map_encounter *_BEST bounds; IP/ED/OP/Recurring; open end=arrival+1d (LOW for IP/ED); narrowest span unique; Europe/London 2026-09-29"))
    .drop("_rid","_n_all","_n_best","_best_eid","_best_open","_best_high"))
incident=finalise(incident,["RECORDID"],SRC_INCIDENTS,"RECORDID")
incident_result=publish(incident,INCIDENT,["RECORDID"])

# COMMAND ----------

inj=active_incident_filter(spark.table(SRC_INJURIES)).select(F.col("recordid").cast("long").alias("RECORDID"),F.col("inc_id").cast("long").alias("INC_ID"),F.col("con_id").cast("long").alias("CON_ID"),F.upper(F.trim("inc_injury")).alias("INC_INJURY"),F.upper(F.trim("inc_bodypart")).alias("INC_BODYPART"),F.col("listorder").cast("int").alias("LISTORDER"),F.col("updateddate").alias("SOURCE_UPDT_DT_TM"),F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"))
inj=datix_decode(inj,"INC_INJURY","datix_code_inc_injury","INC_INJURY_DISPLAY"); inj=datix_decode(inj,"INC_BODYPART","datix_code_inc_bodypart","INC_BODYPART_DISPLAY")
inj=finalise(inj,["RECORDID"],SRC_INJURIES,"RECORDID"); injury_result=publish(inj,INJURY,["RECORDID"],snapshot=True)

action_fields=["ACT_TYPE","ACT_PRIORITY","ACT_DSTART","ACT_DDUE","ACT_DDONE","ACT_SCORE","ACT_COST","ACT_COST_MIN","ACT_COST_MAX","ACT_COST_TYPE","ACT_ORGANISATION","ACT_UNIT","ACT_DIRECTORATE","ACT_SPECIALTY","ACT_CLINGROUP","ACT_LOCTYPE","ACT_LOCACTUAL","ACT_CHAIN_ID","ACT_STEP_NO","ACT_FROM_INITS","ACT_TO_INITS","ACT_BY_INITS","SECLEVEL","SECGROUP","UPDATEDDATE"]
act_src=(spark.table(SRC_ACTIONS).where("upper(trim(ACT_MODULE))='INC'")
    .join(F.broadcast(INC_IDS),F.col("ACT_CAS_ID").cast("long")==F.col("_inc_id"),"inner").drop("_inc_id"))
act=act_src.select(F.col("RECORDID").cast("long").alias("RECORDID"),F.col("ACT_CAS_ID").cast("long").alias("INC_ID"),*[F.col(c) for c in action_fields if has_col(SRC_ACTIONS,c)],F.col("UPDATEDDATE").alias("SOURCE_UPDT_DT_TM"),F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"))
act=finalise(act,["RECORDID"],SRC_ACTIONS,"RECORDID"); action_result=publish(act,ACTION,["RECORDID"])

st_src=spark.table(SRC_STATUS).join(F.broadcast(INC_IDS),F.col("recordid").cast("long")==F.col("_inc_id"),"inner").drop("_inc_id")
st=st_src.select(F.col("recordid").cast("long").alias("INC_ID"),F.col("date").alias("STATUS_DT_TM"),F.col("status").alias("STATUS"),F.col("login").alias("LOGIN"),F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"))
st=st.withColumn("STATUS_SEQUENCE",F.row_number().over(Window.partitionBy("INC_ID").orderBy(F.col("STATUS_DT_TM").asc_nulls_last(),F.col("STATUS").asc_nulls_last(),F.col("LOGIN").asc_nulls_last())))
st=st.withColumn("STATUS_ROW_ID",F.sha2(F.concat_ws("|",F.col("INC_ID"),F.coalesce(F.col("STATUS_DT_TM").cast("string"),F.lit("")),F.coalesce("STATUS",F.lit("")),F.coalesce("LOGIN",F.lit("")),F.col("STATUS_SEQUENCE")),256))
st=finalise(st,["STATUS_ROW_ID"],SRC_STATUS,"STATUS_ROW_ID"); status_result=publish(st,STATUS,["STATUS_ROW_ID"],snapshot=True)

# COMMAND ----------

NARRATIVE_BASE={"lfpse_description","lfpse_immediate_actions","lfpse_how_future_occurrence","lfpse_went_well","lfpse_risk_description"}
lfp_columns=datix_source(SRC_LFPSE).columns
lfp_narrative=sorted(c for c in lfp_columns if c.lower() in NARRATIVE_BASE or c.lower().startswith("lfpse_problem_description_") or (c.lower().startswith("lfpse_") and (c.lower().endswith("_details") or c.lower().endswith("_other"))))
lfp_structured=[c for c in lfp_columns if c not in lfp_narrative and c.lower()!="lfpse_reporter_contact"]
lfp=active_incident_filter(datix_source(SRC_LFPSE)).select(
    *[F.col(c).alias(c.upper()) for c in lfp_structured if c.upper() != "ADC_UPDT"],
    F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"),
)
lfp=lfp.withColumnRenamed("RECORDID","RECORDID").withColumn("SOURCE_UPDT_DT_TM",F.lit(None).cast("timestamp"))
lfp=finalise(lfp,["RECORDID"],SRC_LFPSE,"RECORDID"); lfpse_result=publish(lfp,LFPSE,["RECORDID"],snapshot=True)

FACTOR_CONFIG=[
    ("device",SRC_INCIDENTS,"recordid",["inc_equipment","inc_eqpt_type","inc_manufacturer","inc_model","inc_supplier","inc_serialno","inc_batchno"]),
    ("medication",SRC_INCIDENTS,"recordid",["inc_med_drug","inc_med_drug_rt","inc_med_trade","inc_med_form","inc_med_dose","inc_med_route","inc_med_bnf"]),
    ("process",SRC_INCIDENTS,"recordid",["inc_processfail","inc_failnature","inc_guidelines","inc_root_causes"]),
    ("device",SRC_LFPSE,"recordid",["lfpse_device_type","lfpse_manufacturer","lfpse_model","lfpse_device_involvement_factors"]),
    ("medication",SRC_LFPSE,"recordid",["lfpse_drugs_involved","lfpse_drug_involvement_factors","lfpse_drug_reaction"]),
    ("blood",SRC_LFPSE,"recordid",["lfpse_blood_involved","lfpse_blood_problem","lfpse_blood_products_involved"]),
    ("tissue_organ",SRC_LFPSE,"recordid",["lfpse_tissue_organs_involvement_factor","lfpse_tissue_organs_not_used"]),
    ("it_system",SRC_LFPSE,"recordid",["lfpse_it_systems_involvement_factors"]),
    ("estates",SRC_LFPSE,"recordid",["lfpse_estates_services","lfpse_buildings_infrastructure"]),
    ("people_action",SRC_LFPSE,"recordid",["lfpse_people_action_factors","lfpse_people_involvement_factor"]),
    ("process",SRC_LFPSE,"recordid",["lfpse_involved_processes","lfpse_safety_challenges"]),
]
# PMS_DATIX_DEVICE_V1: typed device columns; FACTOR_CODE keeps only the first configured field, so the rest were dropped.
PMS_DEVICE_SOURCES={
    SRC_INCIDENTS:{"DEVICE_EQUIPMENT":"inc_equipment","DEVICE_TYPE":"inc_eqpt_type","DEVICE_MANUFACTURER":"inc_manufacturer","DEVICE_MODEL":"inc_model","DEVICE_SUPPLIER":"inc_supplier","DEVICE_SERIAL_NUMBER":"inc_serialno","DEVICE_BATCH_NUMBER":"inc_batchno"},
    SRC_LFPSE:{"DEVICE_TYPE":"lfpse_device_type","DEVICE_MANUFACTURER":"lfpse_manufacturer","DEVICE_MODEL":"lfpse_model","DEVICE_INVOLVEMENT_FACTORS":"lfpse_device_involvement_factors"},
}
PMS_DEVICE_NAMES=['DEVICE_EQUIPMENT', 'DEVICE_TYPE', 'DEVICE_MANUFACTURER', 'DEVICE_MODEL', 'DEVICE_SUPPLIER', 'DEVICE_SERIAL_NUMBER', 'DEVICE_BATCH_NUMBER', 'DEVICE_INVOLVEMENT_FACTORS']
def pms_device_columns(factor_type,table,available):
    src=PMS_DEVICE_SOURCES.get(table,{}) if factor_type=="device" else {}
    return [(F.when(nonblank(src[n]),F.trim(F.col(src[n]).cast("string"))) if src.get(n) in available else F.lit(None).cast("string")).alias(n) for n in PMS_DEVICE_NAMES]
# PMS_DATIX_DEVICE_V1 end
factor_parts=[]
for factor_type,table,key,fields in FACTOR_CONFIG:
    available=[c for c in fields if has_col(table,c)]
    if not available: continue
    src=incident_source if table==SRC_INCIDENTS else active_incident_filter(datix_source(table))
    inc_col="recordid" if table==SRC_INCIDENTS else "inc_id"
    pred=reduce(lambda a,b:a|b,[nonblank(c) for c in available])
    factor_parts.append(src.where(pred).select(F.col(inc_col).cast("long").alias("INC_ID"),F.when(F.lit(table==SRC_LFPSE),F.col("recordid").cast("long")).alias("LFPSE_ID"),F.lit(factor_type).alias("FACTOR_TYPE"),F.lit(table.split(".")[-1]).alias("FACTOR_SOURCE_TABLE"),F.col(key).cast("string").alias("FACTOR_SOURCE_RECORD_ID"),F.lit(1).alias("FACTOR_ORDINAL"),F.col(available[0]).cast("string").alias("FACTOR_CODE"),F.col(available[0]).cast("string").alias("FACTOR_DISPLAY"),F.to_json(F.array(*[F.lit(c) for c in available])).alias("SOURCE_FIELDS"),F.to_json(F.struct(*[F.col(c).cast("string").alias(c) for c in available])).alias("FACTOR_ATTRIBUTES_JSON"),F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT"),*pms_device_columns(factor_type,table,available)))  # PMS_DATIX_DEVICE_V1
factor=reduce(lambda a,b:a.unionByName(b),factor_parts).withColumn("FACTOR_ROW_ID",F.sha2(F.concat_ws("|",F.col("INC_ID"),"FACTOR_TYPE","FACTOR_SOURCE_TABLE","FACTOR_SOURCE_RECORD_ID",F.col("FACTOR_ORDINAL")),256))
# PMS_P1_T3_V1 — medication factors map by exact normalised name to the drug lexicon. FACTOR_CODE keeps
# the raw source value; the mapped concept lands in FACTOR_TARGET_*. Unmatched medication rows say so.
lex=spark.table(SRC_DRUG_LEXICON).select(F.col("name_normalised").alias("_lk"),F.col("target_code").alias("_lc"),F.col("target_display").alias("_ld"),F.col("mapping_method").alias("_lm"),F.col("mapping_version").alias("_lv"),F.col("mapping_rule_id").alias("_lr"))
is_med=F.col("FACTOR_TYPE")=="medication"
factor=factor.withColumn("_lk",F.when(is_med,F.lower(F.trim(F.regexp_replace("FACTOR_DISPLAY",r"\s+"," "))))).join(F.broadcast(lex),"_lk","left")
factor=(factor.withColumn("FACTOR_TARGET_SYSTEM",F.when(F.col("_lc").isNotNull(),F.lit("http://snomed.info/sct")))
    .withColumn("FACTOR_TARGET_CODE",F.col("_lc")).withColumn("FACTOR_TARGET_DISPLAY",F.col("_ld"))
    .withColumn("FACTOR_MAP_METHOD",F.when(is_med,F.coalesce(F.col("_lm"),F.lit("unmapped"))))
    .withColumn("FACTOR_MAP_VERSION",F.col("_lv")).withColumn("FACTOR_MAP_RULE_ID",F.col("_lr"))
    .withColumn("FACTOR_MAP_CONFIDENCE",F.when(F.col("_lc").isNotNull(),F.lit(1.0)).cast("double"))
    .drop("_lk","_lc","_ld","_lm","_lv","_lr"))
factor=finalise(factor,["FACTOR_ROW_ID"],"typed Datix factor projection","FACTOR_ROW_ID"); factor_result=publish(factor,FACTOR,["FACTOR_ROW_ID"])

# COMMAND ----------

FRAGMENT_SOURCES=[
    (SRC_INCIDENTS,"recordid","recordid",None,"updateddate",["INC_NOTES","INC_INV_ACTION","INC_ACTIONTAKEN","INC_DESCRIPTION","INC_RECOMMEND","inc_inv_lessons","INC_DEFECT","INC_EQPT_DESCR","INC_OUTCOME","INC_IMPRSTRATS","inc_imprstrats2","INC_EXTRAINFO","inc_clin_detail","inc_problems","inc_user_action","inc_agg_issues"]),
    (SRC_LINKS,"link_recordid","inc_id",None,"updateddate",["LINK_NOTES"]),
    (SRC_ACTIONS,"RECORDID","ACT_CAS_ID","upper(trim(ACT_MODULE))='INC'","UPDATEDDATE",["ACT_DESCR","ACT_SYNOPSIS","ACT_PROGRESS","ACT_RESOURCES","ACT_MONITORING"]),
    (SRC_LFPSE,"recordid","inc_id",None,None,lfp_narrative),
    (SRC_LFPSE_PATIENT,"recordid","inc_id",None,None,["lfpse_clinical_outcome"]),
]
parts=[]
for table,id_col,inc_col,admit_sql,updt_col,fields in FRAGMENT_SOURCES:
    src=datix_source(table)
    if admit_sql: src=src.where(admit_sql)
    if table==SRC_INCIDENTS:
        src=src.join(F.broadcast(INC_IDS),F.col("recordid").cast("long")==F.col("_inc_id"),"inner").drop("_inc_id")
    elif table==SRC_ACTIONS:
        src=src.join(F.broadcast(INC_IDS),F.col("ACT_CAS_ID").cast("long")==F.col("_inc_id"),"inner").drop("_inc_id")
    else:
        src=active_incident_filter(src)
    for field in fields:
        if not has_col(table,field): continue
        updt=F.col(updt_col) if updt_col else F.lit(None).cast("timestamp")
        parts.append(src.where(nonblank(field)).select(F.lit(table.split(".")[-1]).alias("SOURCE_OBJECT_TABLE"),F.col(id_col).cast("string").alias("SOURCE_RECORD_ID"),F.lit(field.upper()).alias("FIELD_NAME"),F.col(inc_col).cast("long").alias("INC_ID"),F.col(field).cast("string").alias("TEXT"),updt.alias("SOURCE_UPDT_DT_TM"),F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT")))
frags=reduce(lambda a,b:a.unionByName(b),parts)
header=spark.table(INCIDENT).select(F.col("RECORDID").alias("INC_ID"),F.col("PERSON_ID").alias("INCIDENT_PERSON_ID"),"AFFECTED_PATIENT_COUNT","CONTEXT_FINGERPRINT_CURRENT","INC_OURREF","INCIDENT_DT_TM")
frags=(frags.join(header,"INC_ID","left").withColumn("FRAGMENT_ID",F.sha2(F.concat_ws("|","SOURCE_OBJECT_TABLE","SOURCE_RECORD_ID","FIELD_NAME"),256)).withColumn("FRAGMENT_ROLE",F.col("FIELD_NAME"))
    .withColumn("TEXT_SHA256",F.sha2("TEXT",256)).withColumn("TEXT_LENGTH",F.length("TEXT")).withColumn("ANON_INPUT_DIGEST",F.sha2(F.to_json(F.struct(F.coalesce(F.col("TEXT"),F.lit("")).alias("TEXT"))),256)).withColumn("PRIORITY_DT_TM",F.coalesce("SOURCE_UPDT_DT_TM","SOURCE_ADC_UPDT"))
    # INSR_W1_DATIX_PRIORITY_V1: the anonymisation queue drains newest PRIORITY_DT_TM first. About 13% of fragments
    # have no source update time, so they queue by ADC load time instead; PRIORITY_BASIS says which. It does not
    # enter ANON_INPUT_DIGEST, so no fragment is re-anonymised because of it.
    .withColumn("PRIORITY_BASIS",F.when(F.col("SOURCE_UPDT_DT_TM").isNotNull(),F.lit("SOURCE_UPDT_DT_TM")).otherwise(F.lit("ADC_LOAD_FALLBACK"))))
frags=with_anon_state(frags,["anon_text"]); frags=finalise(frags,["FRAGMENT_ID"],"Datix admitted narrative fragments","FRAGMENT_ID")
fragment_result=publish(frags,FRAGMENT,["FRAGMENT_ID"],snapshot=True)

# COMMAND ----------

results=[incident_result,participant_result,injury_result,factor_result,lfpse_result,action_result,status_result,fragment_result]
spark.createDataFrame([(RUN_ID,SAMPLE_MODE,SAMPLE_INCIDENTS,json.dumps(results,sort_keys=True),datetime.now(timezone.utc).replace(tzinfo=None))],"run_id string, sample_mode boolean, sample_incidents long, result_json string, recorded_at timestamp").write.mode("append").saveAsTable("6_mgmt.bronze.datix_pipeline_runs")
dbutils.notebook.exit(json.dumps({"sample_mode":SAMPLE_MODE,"sample_incidents":SAMPLE_INCIDENTS,"results":results},sort_keys=True))
