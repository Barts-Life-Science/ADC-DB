# Databricks notebook source
# BRONZE_FIX_857469999366132_V1
# BRONZE_FIX_946452877034658_V1
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

def _snapshot_rows_from_history(table, version):
    """Recover an exact full-snapshot count through contiguous metadata commits."""
    history = (spark.sql(f"DESCRIBE HISTORY {qname(table)}")
               .where(F.col("version") <= int(version)).orderBy(F.col("version").desc()).collect())
    expected = int(version)
    metadata_only = {"SET TBLPROPERTIES", "UNSET TBLPROPERTIES", "CHANGE COLUMN",
                     "ADD COLUMNS", "DROP COLUMNS", "RENAME COLUMN", "OPTIMIZE",
                     "VACUUM START", "VACUUM END"}
    for row in history:
        if int(row["version"]) != expected:
            raise RuntimeError(f"{table}: incomplete snapshot-count history at {expected}")
        operation = row["operation"]
        parameters = row["operationParameters"] or {}
        full_write = operation in {"CREATE TABLE AS SELECT", "CREATE OR REPLACE TABLE AS SELECT", "REPLACE TABLE AS SELECT"}
        full_write = full_write or (operation == "WRITE" and parameters.get("mode", "").lower() == "overwrite" and not parameters.get("predicate"))
        if full_write:
            value = (row["operationMetrics"] or {}).get("numOutputRows")
            if value is None or not str(value).isdigit():
                raise RuntimeError(f"{table}: full snapshot has no exact numOutputRows")
            return int(value)
        if operation not in metadata_only:
            raise RuntimeError(f"{table}: cannot derive snapshot baseline through {operation}")
        expected -= 1
    raise RuntimeError(f"{table}: retained history does not prove baseline version {version}")

def assert_snapshot_not_shrunk(table):
    if SAMPLE_MODE:
        return
    pins = (spark.table("8_dev.tdx_evidence.source_pins")
            .where(F.col("source_table") == table).select("row_count", "delta_version").collect())
    if len(pins) != 1:
        raise RuntimeError(f"{table}: expected one saved snapshot baseline, found {len(pins)}")
    prior = pins[0]
    baseline = prior["row_count"]
    if baseline is None:
        baseline = _snapshot_rows_from_history(table, prior["delta_version"])
    baseline = int(baseline)
    if baseline < 0:
        raise RuntimeError(f"{table}: negative baseline count")
    current = spark.table(table).count()
    if current * 5 < baseline * 4:
        raise RuntimeError(f"{table}: snapshot shrink guard: current={current}, baseline={baseline}")
    print(f"Snapshot count verified: {table}: baseline={baseline}, current={current}")
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

def governance(table):
    spark.sql(f"COMMENT ON TABLE {qname(table)} IS 'TDX Datix curated bronze product. Reported incidents and associations are not incidence rates or proven causality.'")
    ids=("PERSON","ENCNTR","NHS","MRN","NAME","DOB","DOD","POSTCODE","ADDRESS","TEL","PHONE","FAX","EMAIL","LOGIN","INITIAL","SERIAL")
    texts=("TEXT","NOTES","DESCR","ACTION","OUTCOME","PROGRESS","SYNOPSIS","MONITOR","RESOURCE","PROBLEM")
    for c in spark.table(table).columns:
        spark.sql(f"ALTER TABLE {qname(table)} ALTER COLUMN `{c.replace('`','``')}` COMMENT 'TDX Datix bronze field {c}.'")
        u=c.upper()
        if any(x in u for x in ids): risk,severity="4","2"
        elif any(x in u for x in texts) or u.startswith("ANON_"): risk,severity="3","2"
        elif u in {"SECLEVEL","SECGROUP","SOURCE_SECURITY_LEVEL","SOURCE_SECURITY_GROUP"}: risk,severity="2","1"
        else: risk,severity="1","1"
        spark.sql(f"ALTER TABLE {qname(table)} ALTER COLUMN `{c.replace('`','``')}` SET TAGS ('ig_risk'='{risk}','ig_severity'='{severity}')")

def publish(df,target,keys,snapshot=False):
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

# Limit the identifier lookup to aliases actually present in the bounded contact cohort.
mrn_values=[r[0] for r in contacts.select(F.upper(F.trim("con_number")).alias("value")).where("value IS NOT NULL AND value<>''").distinct().limit(10000).collect() if r[0]]
nhs_values=[r[0] for r in contacts.select(F.regexp_replace("con_nhsno","[^0-9]","").alias("value")).where("value IS NOT NULL AND value<>''").distinct().limit(10000).collect() if r[0]]
ids=spark.table(SRC_IDENTIFIERS).where("CURRENT_IND")
mrn=(ids.where((F.col("ALIAS_TYPE")=="MRN") & F.upper(F.trim("ALIAS_VALUE")).isin(mrn_values or ["__NONE__"]))
    .groupBy(F.upper(F.trim("ALIAS_VALUE")).alias("_mrn")).agg(F.countDistinct("PERSON_ID").alias("_mrn_n"),F.max("PERSON_ID").cast("long").alias("_mrn_person")))
nhs=(ids.where((F.col("ALIAS_TYPE")=="NHS") & F.regexp_replace("ALIAS_VALUE","[^0-9]","").isin(nhs_values or ["__NONE__"]))
    .groupBy(F.regexp_replace("ALIAS_VALUE","[^0-9]","").alias("_nhs")).agg(F.countDistinct("PERSON_ID").alias("_nhs_n"),F.max("PERSON_ID").cast("long").alias("_nhs_person")))

def resolved_contacts():
    c=contacts.select(F.col("recordid").cast("long").alias("CON_ID"),F.upper(F.trim("con_number")).alias("_mrn"),F.regexp_replace("con_nhsno","[^0-9]","").alias("_nhs"),F.col("con_dob").alias("_dob"))
    j=c.join(mrn,"_mrn","left").join(nhs,"_nhs","left")
    mrn_multi=F.col("_mrn_n")>1; nhs_multi=F.col("_nhs_n")>1; mrn_ok=F.col("_mrn_n")==1; nhs_ok=F.col("_nhs_n")==1
    agree=mrn_ok&nhs_ok&(F.col("_mrn_person")==F.col("_nhs_person")); conflict=mrn_ok&nhs_ok&(F.col("_mrn_person")!=F.col("_nhs_person"))
    status=(F.when(mrn_multi|nhs_multi|conflict,"ambiguous").when(agree,"resolved_mrn_nhs").when(mrn_ok,"resolved_mrn").when(nhs_ok,"resolved_nhs").otherwise("unresolved"))
    person=F.when(status.startswith("resolved"),F.coalesce(F.when(mrn_ok,F.col("_mrn_person")),F.when(nhs_ok,F.col("_nhs_person"))))
    return j.select("CON_ID",person.cast("long").alias("PERSON_ID"),status.alias("PERSON_RESOLUTION_STATUS"),F.lit("TDX-P1 multi-match-first, mrn>nhs unique-only 2026-09-13").alias("PERSON_RESOLUTION_RULE"),"_dob")
resolved=resolved_contacts()
person_dob=spark.table(SRC_PERSON).select(F.col("person_id").cast("long").alias("_pid"),F.col("birth_date").alias("_birth_date"))

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
incident=i.select(F.col("i.recordid").cast("long").alias("RECORDID"),*[F.col(f"i.{x}").alias(x.upper()) for x in header_fields],incident_dt.alias("INCIDENT_DT_TM"),incident_clean.alias("INCIDENT_DT_TM_CLEAN"),F.col("i.inc_dreported").alias("REPORTED_DT_TM"),F.col("i.inc_dopened").alias("OPENED_DT_TM"),F.col("i.inc_dnotified").alias("NOTIFIED_DT_TM"),F.col("i.inc_inv_dstart").alias("INVESTIGATION_START_DT_TM"),F.col("i.inc_inv_dcomp").alias("INVESTIGATION_COMPLETE_DT_TM"),F.col("i.updateddate").alias("SOURCE_UPDT_DT_TM"),F.coalesce("AFFECTED_PATIENT_COUNT",F.lit(0)).cast("long").alias("AFFECTED_PATIENT_COUNT"),F.when(F.col("AFFECTED_PATIENT_COUNT")==1,F.col("_affected_person")).cast("long").alias("PERSON_ID"),F.when(F.col("AFFECTED_PATIENT_COUNT")==1,"resolved_affected_participant").otherwise("none_or_multiple").alias("PERSON_LINK_METHOD"),F.lit(None).cast("long").alias("ENCNTR_ID"),F.when(F.col("AFFECTED_PATIENT_COUNT")==1,"sample_not_evaluated" if SAMPLE_MODE else "pending_overlap_resolution").otherwise("no_person").alias("ENCOUNTER_LINK_METHOD"),F.lit("TDX-E1 2026-09-13").alias("ENCOUNTER_LINK_RULE"),F.col("i.ADC_UPDT").alias("SOURCE_ADC_UPDT"),"_participants")
incident=incident.withColumn("CONTEXT_FINGERPRINT_CURRENT",F.sha2(F.concat_ws("|",F.coalesce("INC_NAME",F.lit("")),F.coalesce(F.col("INC_DOB").cast("string"),F.lit("")),F.coalesce("INC_POSTCODE",F.lit("")),F.coalesce("INC_REPNAME",F.lit("")),F.coalesce("INC_INVESTIGATOR",F.lit("")),F.to_json(F.coalesce("_participants",F.array().cast("array<string>")))),256)).drop("_participants")
for code,table in [("INC_TYPE","datix_code_inc_type"),("INC_CATEGORY","datix_code_inc_cat"),("INC_SUBCATEGORY","datix_code_inc_subcat"),("INC_SEVERITY","datix_code_inc_severity"),("INC_GRADE","datix_code_inc_grades"),("SECGROUP","datix_code_secgroup")]:
    if code in incident.columns: incident=datix_decode(incident,code,table,code+"_DISPLAY")
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
factor_parts=[]
for factor_type,table,key,fields in FACTOR_CONFIG:
    available=[c for c in fields if has_col(table,c)]
    if not available: continue
    src=incident_source if table==SRC_INCIDENTS else active_incident_filter(datix_source(table))
    inc_col="recordid" if table==SRC_INCIDENTS else "inc_id"
    pred=reduce(lambda a,b:a|b,[nonblank(c) for c in available])
    factor_parts.append(src.where(pred).select(F.col(inc_col).cast("long").alias("INC_ID"),F.when(F.lit(table==SRC_LFPSE),F.col("recordid").cast("long")).alias("LFPSE_ID"),F.lit(factor_type).alias("FACTOR_TYPE"),F.lit(table.split(".")[-1]).alias("FACTOR_SOURCE_TABLE"),F.col(key).cast("string").alias("FACTOR_SOURCE_RECORD_ID"),F.lit(1).alias("FACTOR_ORDINAL"),F.col(available[0]).cast("string").alias("FACTOR_CODE"),F.col(available[0]).cast("string").alias("FACTOR_DISPLAY"),F.to_json(F.array(*[F.lit(c) for c in available])).alias("SOURCE_FIELDS"),F.col("ADC_UPDT").alias("SOURCE_ADC_UPDT")))
factor=reduce(lambda a,b:a.unionByName(b),factor_parts).withColumn("FACTOR_ROW_ID",F.sha2(F.concat_ws("|",F.col("INC_ID"),"FACTOR_TYPE","FACTOR_SOURCE_TABLE","FACTOR_SOURCE_RECORD_ID",F.col("FACTOR_ORDINAL")),256))
for c,d in [("FACTOR_MAP_METHOD","string"),("FACTOR_MAP_VERSION","string"),("FACTOR_MAP_CONFIDENCE","double"),("FACTOR_MAP_RULE_ID","string")]: factor=factor.withColumn(c,F.lit(None).cast(d))
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
    .withColumn("TEXT_SHA256",F.sha2("TEXT",256)).withColumn("TEXT_LENGTH",F.length("TEXT")).withColumn("ANON_INPUT_DIGEST",F.sha2(F.to_json(F.struct(F.coalesce(F.col("TEXT"),F.lit("")).alias("TEXT"))),256)).withColumn("PRIORITY_DT_TM",F.coalesce("SOURCE_UPDT_DT_TM","SOURCE_ADC_UPDT")))
frags=with_anon_state(frags,["anon_text"]); frags=finalise(frags,["FRAGMENT_ID"],"Datix admitted narrative fragments","FRAGMENT_ID")
fragment_result=publish(frags,FRAGMENT,["FRAGMENT_ID"],snapshot=True)

# COMMAND ----------

results=[incident_result,participant_result,injury_result,factor_result,lfpse_result,action_result,status_result,fragment_result]
spark.createDataFrame([(RUN_ID,SAMPLE_MODE,SAMPLE_INCIDENTS,json.dumps(results,sort_keys=True),datetime.now(timezone.utc).replace(tzinfo=None))],"run_id string, sample_mode boolean, sample_incidents long, result_json string, recorded_at timestamp").write.mode("append").saveAsTable("8_dev.tdx_evidence.datix_pipeline_runs")
dbutils.notebook.exit(json.dumps({"sample_mode":SAMPLE_MODE,"sample_incidents":SAMPLE_INCIDENTS,"results":results},sort_keys=True))


