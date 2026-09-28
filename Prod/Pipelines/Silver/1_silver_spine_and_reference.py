# Databricks notebook source
# MAGIC %md
# MAGIC # Spine and reference
# MAGIC The person and encounter spine, patient event index, terminology, practitioners, locations and supporting reference data.
# MAGIC
# MAGIC
# MAGIC Reading order: 1 of 8. Numbers guide navigation; Lakeflow schedules datasets by their dependencies.
# MAGIC Shared helpers live in `silver_journey_shared.py`, an importable Python file.

# COMMAND ----------

# Shared projections also serve the patient event index.
from silver_journey_shared import (
    F,
    INTERNAL_SCHEMA,
    LIFECYCLE_COLUMN_COMMENTS,
    SNOMED_URI,
    SRC_APPOINTMENT,
    SRC_APPOINTMENT_RESOURCE,
    SRC_CODED_EVENTS,
    SRC_DATE_EVENTS,
    SRC_DIAGNOSIS,
    SRC_ENCOUNTER,
    SRC_FORM_ACTIVITY,
    SRC_IMPLANT_DETAILS,
    SRC_MEDICATION_ORDER,
    SRC_MEDICATION_ORDER_ACTION,
    SRC_MEDICONNECT_TYPE_MAP,
    SRC_MED_ADMIN,
    SRC_MILL_BLOB_TEXT,
    SRC_NOMEN_EVENTS,
    SRC_NUMERIC_EVENTS,
    SRC_PATHOLOGY_REPORT_VERSIONS,
    SRC_PHARMACY_ISSUE,
    SRC_PROBLEM,
    SRC_TEXT_EVENTS,
    SRC_THEATRE_CASE,
    SRC_THEATRE_CASE_PROCEDURE,
    Window,
    _allergy_canonical,
    _allergy_canonical_pregate,
    _appointment_canonical,
    _artifact_asset_canonical,
    _baby_delivery_canonical,
    _cancer_treatment_canonical,
    _cancer_treatment_canonical_pregate,
    _cc_daily_score_canonical,
    _cc_daily_score_canonical_pregate,
    _cc_procedure_canonical,
    _cc_procedure_canonical_pregate,
    _clamped_ts,
    _clinical_score_canonical,
    _coded_finding_canonical,
    _coded_finding_typed,
    _community_care_activity_canonical,
    _community_care_activity_canonical_pregate,
    _community_care_contact_canonical,
    _condition_diagnosis_canonical,
    _condition_diagnosis_canonical_pregate,
    _condition_problem_canonical,
    _condition_problem_canonical_pregate,
    _condition_stage_canonical,
    _condition_stage_canonical_pregate,
    _costed_activity_canonical,
    _critical_care_activity_canonical,
    _critical_care_activity_canonical_pregate,
    _critical_care_admission_canonical,
    _critical_care_period_canonical,
    _date_finding_canonical,
    _date_finding_typed,
    _device_canonical,
    _device_canonical_pregate,
    _document_canonical,
    _drug_expenditure_canonical,
    _elective_access_comment_document_canonical,
    _elective_access_entry_canonical,
    _encounter_canonical,
    _endobase_document_canonical,
    _endobase_procedure_canonical,
    _endobase_procedure_canonical_pregate,
    _endoscopy_finding_canonical,
    _endoscopy_finding_canonical_pregate,
    _family_history_canonical,
    _family_history_canonical_pregate,
    _form_canonical,
    _form_canonical_pregate,
    _genomic_result_canonical,
    _genomic_result_canonical_pregate,
    _genomic_test_canonical,
    _genomic_test_canonical_pregate,
    _hrg_grouping_canonical,
    _imaging_exam_canonical,
    _imaging_exam_canonical_pregate,
    _implant_procedure_canonical,
    _implant_procedure_canonical_pregate,
    _indication_canonical,
    _indication_canonical_pregate,
    _labour_delivery_canonical,
    _lifecycle_source_encounter,
    _lifecycle_source_episode_encounter,
    _maternity_care_contact_canonical,
    _maternity_diagnosis_canonical,
    _maternity_diagnosis_canonical_pregate,
    _medication_admin_canonical,
    _medication_admin_canonical_pregate,
    _medication_dispense_canonical,
    _medication_dispense_canonical_pregate,
    _medication_order_canonical,
    _medication_order_canonical_pregate,
    _medication_supply_canonical,
    _medication_supply_canonical_pregate,
    _micro_isolate_canonical,
    _micro_isolate_canonical_pregate,
    _mill_radiology_exam_canonical,
    _mill_radiology_exam_canonical_pregate,
    _n,
    _neonatal_care_day_canonical,
    _neonatal_care_day_canonical_pregate,
    _neonatal_episode_canonical,
    _neonatal_examination_canonical,
    _neonatal_examination_canonical_pregate,
    _neonatal_narrative_document_canonical,
    _nomen_finding_canonical,
    _nomen_finding_typed,
    _numeric_route_expr,
    _numeric_score_canonical,
    _numeric_vital_canonical,
    _order_comment_document_canonical,
    _pathology_report_document_base,
    _pathology_report_document_canonical,
    _pathology_report_document_history_canonical,
    _pathology_report_series_canonical,
    _pathology_requested_test_canonical,
    _pathology_requested_test_canonical_pregate,
    _pathology_result_canonical,
    _pathology_result_canonical_pregate,
    _pathology_specimen_canonical,
    _pathway_tracking_canonical,
    _pregnancy_reconciliation_source,
    _present,
    _presenting_complaint_canonical,
    _procedure_canonical,
    _procedure_canonical_pregate,
    _promoted_score_from_stage,
    _promoted_vital_from_stage,
    _referral_canonical,
    _registry_entry_canonical,
    _rtt_activity_canonical,
    _rtt_pathway_canonical,
    _s3_direct_axis,
    _s3_lookup_axis,
    _susceptibility_canonical,
    _susceptibility_canonical_pregate,
    _text_finding_canonical,
    _text_finding_typed,
    _theatre_procedure_canonical,
    _theatre_procedure_canonical_pregate,
    _transfusion_canonical,
    _transfusion_canonical_pregate,
    _usable_code,
    _vital_sign_canonical,
    _waiting_list_index_representative,
    axis_comments,
    canonical_coding_system,
    materialized_view,
    read_source,
    stable_id,
    subject_key_with_system,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Naming the source a row came from

# COMMAND ----------

# ==== Naming the source a row came from ====

SOURCE_SYSTEM_DISPLAY = {
    "millennium": "Millennium",
    "millennium-pm": "Millennium (patient management)",
    "millennium-scheduling": "Millennium (scheduling)",
    "surginet": "Millennium SurgiNet",
    "luna": "LUNA",
    "jac": "JAC",
    "laboratory": "WinPath",
    "sectra-pacs": "Sectra PACS",
}

def _source_system_display(token_col):
    # Translate a source-system token to its configured display name, with a readable fallback.
    mapping = F.create_map(*[F.lit(x) for kv in SOURCE_SYSTEM_DISPLAY.items() for x in kv])
    return F.coalesce(mapping[token_col], F.initcap(token_col))

def _source_object_display(table_col):
    # Turn a source table's leaf name into a display label by removing fixture and source
    # prefixes.
    tail = F.element_at(F.split(table_col, r"\."), -1)
    unfixtured = F.regexp_replace(tail, r"_s\d+$", "")
    unprefixed = F.regexp_replace(unfixtured, r"^(map_|mill_|ancil_)", "")
    return F.regexp_replace(unprefixed, "_", " ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference /  snomed concept crosswalk

# COMMAND ----------

SRC_S3B_ETHNICITY_MAP = "3_lookup.omop.ethnicity_snomed_map"

@materialized_view(
    name=_n("journey_reference._snomed_concept_crosswalk"),
    private=True,
    comment="Internal S3 OMOP-to-SNOMED crosswalk restricted to valid SNOMED concepts.",
    refresh_policy="incremental",
)
def _s3_snomed_concept_crosswalk():
    # Build the declared dataset: Internal S3 OMOP-to-SNOMED crosswalk restricted to valid
    # SNOMED concepts.
    return (read_source("3_lookup.omop.concept")
            .where("vocabulary_id='SNOMED' AND invalid_reason IS NULL")
            .select(F.col("concept_id").cast("bigint").alias("concept_id"),
                    F.col("concept_code").cast("string").alias("concept_code"),
                    F.col("concept_name").alias("concept_name"),
                    F.col("standard_concept").alias("standard_concept")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spine / person

# COMMAND ----------

# ==== Person identity and reference dimensions ====

SRC_PERSON         = "4_prod.bronze.map_person"

SRC_PATIENT_IDENTIFIER = "4_prod.bronze.map_patient_identifier"

def _person_alias_selection(alias_type):
    """Pick the current MRN or NHS number from the governed alias feed.
    Ranking: active alias first (CURRENT_IND), then latest valid end-effective (open-ended
    sorts last via the 2100 sentinel), NULLS LAST via coalesce floors, deterministic
    SOURCE_PK tiebreak. MULTI_ACTIVE pools are never tiebroken: the whole (person, type)
    pool resolves to 'ambiguous' with NULL value."""
    a = read_source(SRC_PATIENT_IDENTIFIER).where(F.col("ALIAS_TYPE") == alias_type)
    ranked = a.select(
        a.PERSON_ID.cast("string").alias("_pid"),
        F.coalesce(a.MULTI_ACTIVE_IND.cast("int"), F.lit(0)).alias("_multi"),
        a.PIPELINE_UPDT_DT_TM.alias("_alias_loaded_at"),
        a.SOURCE_ADC_UPDT.alias("_alias_source_updt"),
        F.struct(
            F.coalesce(a.CURRENT_IND.cast("int"), F.lit(0)).alias("o_current"),
            F.coalesce(a.END_EFFECTIVE_DT_TM_CLEAN,
                       F.lit("2100-01-01").cast("timestamp")).alias("o_valid_to"),
            F.coalesce(a.BEG_EFFECTIVE_DT_TM_CLEAN,
                       F.lit("1900-01-01").cast("timestamp")).alias("o_valid_from"),
            F.coalesce(a.SOURCE_PK, F.lit(-1)).alias("o_tiebreak"),
            a.ALIAS_VALUE.alias("v_value"),
            F.coalesce(a.CURRENT_IND, F.lit(False)).alias("v_current"),
        ).alias("ranked"),
    )
    won = ranked.groupBy("_pid").agg(
        F.max("ranked").alias("w"),
        F.max("_multi").alias("_m"),
        F.max("_alias_loaded_at").alias("_alias_loaded_at"),
        F.max("_alias_source_updt").alias("_alias_source_updt"),
    )
    ambiguous = F.col("_m") == 1
    return won.select(
        F.col("_pid"),
        F.when(ambiguous, F.lit(None).cast("string")).otherwise(F.col("w.v_value")).alias("_value"),
        F.when(ambiguous, F.lit(None).cast("string"))
         .when(F.col("w.v_current"), F.lit("active")).otherwise(F.lit("inactive")).alias("_value_status"),
        F.when(ambiguous, F.lit("ambiguous"))
         .when(F.col("w.v_current"), F.lit("active_alias"))
         .otherwise(F.lit("latest_valid_alias")).alias("_selection_status"),
        F.col("_alias_loaded_at"),
        F.col("_alias_source_updt"),
    )

PERSON_COLUMN_COMMENTS = {
    "person_id": "Millennium person identifier and table primary key.",
    "active": "Source active indicator.",
    "gender_code": "Source administrative gender code.",
    "gender_display": "Source administrative gender display.",
    "ethnicity_code": "Source ethnicity code.",
    "ethnicity_display": "Source ethnicity display.",
    "birth_date": "Calendar birth date.",
    "birth_datetime": "Source-compatible birth timestamp.",
    "birth_precision_code": "Source code describing birth-date precision.",
    "birth_precision_display": "Display for the source birth precision.",
    "birth_precision_flag": "Raw source birth precision flag.",
    "language_code": "Preferred language source code.",
    "language_display": "Preferred language display.",
    "marital_status_code": "Source marital-status code.",
    "marital_status_display": "Source marital-status display.",
    "religion_code": "Source religion code.",
    "religion_display": "Source religion display.",
    "deceased_ind": "Whether the source indicates the person is deceased.",
    "deceased_datetime": "Source death timestamp.",
    "deceased_datetime_precision": "Raw source death-time precision.",
    "confidentiality_code": "Source confidentiality level carried for downstream access control.",
    "vip_ind": "Source VIP indicator carried as data.",
    "current_address_id": "Current source address reference when available.",
    "latest_known_address_id": "Latest-known source address reference.",
    "address_selection_status": "Provenance for current versus latest-known address selection.",
    "current_mrn": "Current hospital MRN selected from map_patient_identifier: active alias first, then latest valid end-effective, NULLS LAST, deterministic SOURCE_PK tiebreak; NULL when selection is ambiguous or no alias exists.",
    "current_mrn_status": "Lifecycle status of the selected hospital MRN alias; NULL when selection is ambiguous or no alias exists.",
    "mrn_selection_status": "Selection outcome for the governed hospital MRN alias pool; ambiguous = multiple active aliases",
    "nhs_number": "Current NHS number selected from map_patient_identifier: active alias first, then latest valid end-effective, NULLS LAST, deterministic SOURCE_PK tiebreak; NULL when selection is ambiguous or no alias exists.",
    "nhs_number_status": "Lifecycle status of the selected NHS-number alias; NULL when selection is ambiguous or no alias exists.",
    "nhs_number_selection_status": "Selection outcome for the governed NHS-number alias pool; ambiguous = multiple active aliases",
    "record_status": "Derived person-row status: active only when map_person.active_ind is 1 and end_effective_dt_tm is null or not earlier than 2100-01-01; otherwise superseded, including a missing active_ind. This is a normalized row-status label, not the source numeric ACTIVE_IND.",
    "record_status_effective_from": "Source person active-status timestamp carried from `active_status_dt_tm` without a fallback timestamp.",
    "record_status_effective_to": "Source `end_effective_dt_tm` when earlier than 2100-01-01; otherwise null, including missing or open-ended sentinel timestamps.",
    "source_update_timestamp": "Greatest of map_person.source_updt_dt_tm and the maximum map_patient_identifier.SOURCE_ADC_UPDT in each matching MRN and NHS alias pool. The pool maxima include all aliases, even when the displayed identifier is ambiguous; missing alias clocks fall back to the person clock. This is not the Silver refresh time; null only when all contributing clocks are null.",
    "loaded_at": "Greatest of map_person.ADC_UPDT and the maximum map_patient_identifier.PIPELINE_UPDT_DT_TM in each matching MRN and NHS alias pool. These are pool-wide clocks, not just the displayed winning alias; missing alias clocks fall back to the person clock. This is contributing ingestion provenance, not the Silver refresh time; null only when all contributing clocks are null.",
}

PERSON_LIFECYCLE_FIELDS = [
    'load_batch_id',
]

PERSON_RETIRED_COLUMNS = [

]

PERSON_LIFECYCLE_COLUMNS = [
    'person_id',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_person():
    # Assemble person rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    s = read_source(SRC_PERSON)
    mrn = _person_alias_selection("MRN").alias("mrn")
    nhs = _person_alias_selection("NHS").alias("nhs")
    ended = F.coalesce(
        s.end_effective_dt_tm < F.lit("2100-01-01").cast("timestamp"),
        F.lit(False),
    )
    joined = (
        s.join(mrn, s.person_id.cast("string") == F.col("mrn._pid"), "left")
         .join(nhs, s.person_id.cast("string") == F.col("nhs._pid"), "left")
    )
    # contract v2: publish Millennium PERSON_ID as the BIGINT person primary key
    return joined.select(
        s.person_id.cast("bigint").alias("person_id"),
        (s.active_ind == F.lit(1)).alias("active"),
        s.gender_cd.cast("string").alias("gender_code"),
        s.gender_display.alias("gender_display"),
        s.ethnicity_cd.cast("string").alias("ethnicity_code"),
        s.ethnicity_display.alias("ethnicity_display"),
        s.birth_date.alias("birth_date"),
        s.birth_datetime.alias("birth_datetime"),
        s.birth_dt_cd.cast("string").alias("birth_precision_code"),
        s.birth_dt_display.alias("birth_precision_display"),
        s.birth_precision_flag.cast("string").alias("birth_precision_flag"),
        s.language_cd.cast("string").alias("language_code"),
        s.language_display.alias("language_display"),
        s.marital_type_cd.cast("string").alias("marital_status_code"),
        s.marital_type_display.alias("marital_status_display"),
        s.religion_cd.cast("string").alias("religion_code"),
        s.religion_display.alias("religion_display"),
        (s.deceased_dt_tm.isNotNull() | (F.coalesce(s.deceased_cd, F.lit(0)) != 0)).alias("deceased_ind"),
        s.deceased_dt_tm.alias("deceased_datetime"),
        s.deceased_dt_tm_precision_flag.cast("string").alias("deceased_datetime_precision"),
        s.confid_level_cd.cast("string").alias("confidentiality_code"),
        (F.coalesce(s.vip_cd, F.lit(0)) != 0).alias("vip_ind"),
        s.current_address_id.cast("string").alias("current_address_id"),
        s.latest_known_address_id.cast("string").alias("latest_known_address_id"),
        s.address_selection_status.alias("address_selection_status"),
        F.col("mrn._value").alias("current_mrn"),
        F.col("mrn._value_status").alias("current_mrn_status"),
        F.coalesce(F.col("mrn._selection_status"), F.lit("no_alias")).alias("mrn_selection_status"),
        F.col("nhs._value").alias("nhs_number"),
        F.col("nhs._value_status").alias("nhs_number_status"),
        F.coalesce(F.col("nhs._selection_status"), F.lit("no_alias")).alias("nhs_number_selection_status"),
        F.when((s.active_ind == 1) & ~ended, F.lit("active"))
         .otherwise(F.lit("superseded")).alias("record_status"),
        s.active_status_dt_tm.alias("record_status_effective_from"),
        F.when(ended, s.end_effective_dt_tm).alias("record_status_effective_to"),
        F.greatest(
            s.source_updt_dt_tm,
            F.coalesce(F.col("mrn._alias_source_updt"), s.source_updt_dt_tm),
            F.coalesce(F.col("nhs._alias_source_updt"), s.source_updt_dt_tm),
        ).alias("source_update_timestamp"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        F.greatest(
            s.ADC_UPDT,
            F.coalesce(F.col("mrn._alias_loaded_at"), s.ADC_UPDT),
            F.coalesce(F.col("nhs._alias_loaded_at"), s.ADC_UPDT),
        ).alias("loaded_at"),
    )

@materialized_view(
    name=_n("journey_spine.person"),
    comment="One resolved Millennium person. Silver retains inactive, implausible-looking, and deceased rows. Current MRN / NHS number selected deterministically from map_patient_identifier; multi-active pools publish as ambiguous, never tiebroken.",
    refresh_policy="incremental",
    column_comments={**PERSON_COLUMN_COMMENTS, **axis_comments("ethnicity", "ethnic category"), 'record_status': "Derived person-row status: active only when map_person.active_ind is 1 and end_effective_dt_tm is null or not earlier than 2100-01-01; otherwise superseded, including a missing active_ind. This is a normalized row-status label, not the source numeric ACTIVE_IND.", 'record_status_effective_from': "Source person active-status timestamp carried from `active_status_dt_tm` without a fallback timestamp.", 'record_status_effective_to': "Source `end_effective_dt_tm` when earlier than 2100-01-01; otherwise null, including missing or open-ended sentinel timestamps.", 'source_update_timestamp': "Greatest of map_person.source_updt_dt_tm and the maximum map_patient_identifier.SOURCE_ADC_UPDT in each matching MRN and NHS alias pool. The pool maxima include all aliases, even when the displayed identifier is ambiguous; missing alias clocks fall back to the person clock. This is not the Silver refresh time; null only when all contributing clocks are null.", 'loaded_at': "Greatest of map_person.ADC_UPDT and the maximum map_patient_identifier.PIPELINE_UPDT_DT_TM in each matching MRN and NHS alias pool. These are pool-wide clocks, not just the displayed winning alias; missing alias clocks fall back to the person clock. This is contributing ingestion provenance, not the Silver refresh time; null only when all contributing clocks are null."},
)
def person():
    # Build the declared dataset: One resolved Millennium person. Silver retains inactive,
    # implausible-looking, and deceased rows. Current MRN / NHS number selected
    # deterministically from map_patient_identifier; multi-active pools publish as ambiguous,
    # never tiebroken.
    df = _lifecycle_source_person().drop(*PERSON_LIFECYCLE_FIELDS, *PERSON_RETIRED_COLUMNS)
    df = _s3_direct_axis(
        df, "ethnicity", F.lit("urn:cerner:ethnicity"),
        F.col("ethnicity_code"), F.col("ethnicity_display"),
    )
    return _s3_lookup_axis(
        df, "ethnicity", SRC_S3B_ETHNICITY_MAP, "urn:cerner:ethnicity",
        F.col("ethnicity_code"), allow_split=False, model_name="person", row_key="person_id",
    )

@materialized_view(
    name=_n('journey_spine._person_metadata'),
    comment='Internal quality and batch metadata for spine_person; same row grain as the research table. Join keys: person_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def person_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for spine_person; same row
    # grain as the research table. Join keys: person_id.
    return (_lifecycle_source_person()).select(*PERSON_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spine / person identifier

# COMMAND ----------

SRC_COMMUNITY_LINK = "4_prod.bronze.map_community_patient_link"

SRC_PACS_PATIENT_LINK = "4_prod.bronze.map_pacs_patient_link"

SRC_ENDOBASE_PATIENT = "4_prod.bronze.map_endobase_patient"

SRC_CANCER_PTL_LINKAGE = "4_prod.bronze.map_cancer_ptl_linkage"

PERSON_IDENTIFIER_COLUMN_COMMENTS = {
    "person_identifier_key": "B3 bronze field ROW_ID; source value retained unless documented as derived.",
    "subject_key": "deterministic join key over the strongest available subject evidence.",
    "subject_id_system": "Identifier system used to derive subject_key.",
    "person_id": "Resolved Millennium person identifier when available.",
    "identifier_system": "Namespace for the source identifier.",
    "identifier_value": "Source identifier value.",
    "identifier_type_code": "Text label of the alias type derived from the alias type code, distinguishing medical record number from NHS number rows.",
    "status": "Source assignment or matching status.",
    "valid_from": "Identifier validity start.",
    "valid_to": "Identifier validity end.",
    "source_system": "Source system name.",
    "source_table": "Fully qualified bronze source table.",
    "source_row_id": "Stable source row identifier.",
    "source_object": "Value describing source object for the person identifier record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_identifier_id": "B3 bronze field ROW_ID; source value retained unless documented as derived.",
    "loaded_at": "Arm-specific clock: map_person.ADC_UPDT for Millennium person IDs; always null for both community key and community number assignments; map_pacs_patient_link.ADC_UPDT for PACS; map_patient_identifier.PIPELINE_UPDT_DT_TM for Millennium aliases; map_endobase_patient.ADC_UPDT for EndoBase; map_cancer_ptl_linkage.ADC_UPDT for both Pathfinder MRN and NHS assignments. The eight arms are unioned without a cross-arm maximum or Silver refresh timestamp.",
}

PERSON_IDENTIFIER_LIFECYCLE_FIELDS = [
    'identity_status',
    'load_batch_id',
]

PERSON_IDENTIFIER_RETIRED_COLUMNS = [

]

PERSON_IDENTIFIER_LIFECYCLE_COLUMNS = [
    'person_identifier_key',
    'source_identifier_id',
    'identity_status',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_person_identifier():
    # Assemble person identifier rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    p = read_source(SRC_PERSON)
    p_skey, p_ssys = subject_key_with_system(
        [("urn:cerner:person_id", p.person_id)],
        SRC_PERSON,
        p.person_id,
    )
    # contract v2: rename the deterministic assignment hash, publish source arm and native mill_alias row id, and type person_id as BIGINT
    p_rows = p.select(
        stable_id("person_identifier:mill:person_id", p.person_id).alias("person_identifier_key"),
        F.lit("mill:person_id").alias("source_object"),
        F.lit(None).cast("string").alias("source_identifier_id"),
        p_skey.alias("subject_key"),
        p_ssys.alias("subject_id_system"),
        p.person_id.cast("bigint").alias("person_id"),
        F.lit("urn:cerner:person_id").alias("identifier_system"),
        p.person_id.cast("string").alias("identifier_value"),
        F.lit("PI").alias("identifier_type_code"),
        F.when(p.active_ind == 1, F.lit("active")).otherwise(F.lit("inactive")).alias("status"),
        p.beg_effective_dt_tm.alias("valid_from"),
        p.end_effective_dt_tm.alias("valid_to"),
        F.lit("millennium").alias("source_system"),
        F.lit(SRC_PERSON).alias("source_table"),
        p.person_id.cast("string").alias("source_row_id"),
        F.lit("resolved").alias("identity_status"),
        F.date_format(p.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        p.ADC_UPDT.alias("loaded_at"),
    )

    c = read_source(SRC_COMMUNITY_LINK)
    c_skey, c_ssys = subject_key_with_system(
        [("urn:cerner:person_id", c.person_id),
         ("urn:barts:community_patient_key", c.community_patient_key)],
        SRC_COMMUNITY_LINK,
        c.community_patient_key,
    )
    common = [
        c_skey.alias("subject_key"),
        c_ssys.alias("subject_id_system"),
        c.person_id.cast("bigint").alias("person_id"),
        c.person_match_status.alias("status"),
        c.community_registration_date.cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.lit("bh-community").alias("source_system"),
        F.lit(SRC_COMMUNITY_LINK).alias("source_table"),
        c.community_patient_key.alias("source_row_id"),
        F.when(c.person_id.isNotNull(), F.lit("resolved"))
         .when(c.community_patient_key.isNotNull(), F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("string").alias("load_batch_id"),
        F.lit(None).cast("timestamp").alias("loaded_at"),
    ]
    c_key_rows = c.select(
        stable_id("person_identifier:community:key", c.community_patient_key).alias("person_identifier_key"),
        F.lit("community:key").alias("source_object"),
        F.lit(None).cast("string").alias("source_identifier_id"),
        *common[:3],
        F.lit("urn:barts:community_patient_key").alias("identifier_system"),
        c.community_patient_key.alias("identifier_value"),
        F.lit("COMMUNITY_REGISTRATION").alias("identifier_type_code"),
        *common[3:],
    )
    c_number_rows = c.where(c.source_patient_number.isNotNull()).select(
        stable_id("person_identifier:community:number", c.source_database_id, c.source_patient_number)
          .alias("person_identifier_key"),
        F.lit("community:number").alias("source_object"),
        F.lit(None).cast("string").alias("source_identifier_id"),
        *common[:3],
        F.concat(F.lit("urn:barts:community_patient_number:"), c.source_database_id.cast("string"))
          .alias("identifier_system"),
        c.source_patient_number.cast("string").alias("identifier_value"),
        F.lit("MR").alias("identifier_type_code"),
        *common[3:],
    )
    pacs = read_source(SRC_PACS_PATIENT_LINK)
    pacs_skey, pacs_ssys = subject_key_with_system(
        [("urn:cerner:person_id", pacs.PERSON_ID),
         ("urn:sectra:pacs-patient-id", pacs.PACS_PATIENT_ID)],
        SRC_PACS_PATIENT_LINK,
        pacs.PACS_PATIENT_ID,
    )
    pacs_rows = pacs.select(
        stable_id("person_identifier:pacs:patient", pacs.PACS_PATIENT_ID)
        .alias("person_identifier_key"),
        F.lit("pacs:patient").alias("source_object"),
        F.lit(None).cast("string").alias("source_identifier_id"),
        pacs_skey.alias("subject_key"), pacs_ssys.alias("subject_id_system"),
        pacs.PERSON_ID.cast("bigint").alias("person_id"),
        F.lit("urn:sectra:pacs-patient-id").alias("identifier_system"),
        pacs.PACS_PATIENT_ID.cast("string").alias("identifier_value"),
        F.lit("PACS_INTERNAL").alias("identifier_type_code"),
        pacs.PERSON_MATCH_STATUS.alias("status"),
        F.lit(None).cast("timestamp").alias("valid_from"),
        F.when(~F.coalesce(pacs.SOURCE_PRESENT_IND, F.lit(True)), pacs.ADC_UPDT)
        .alias("valid_to"),
        F.lit("sectra-pacs").alias("source_system"),
        F.lit(SRC_PACS_PATIENT_LINK).alias("source_table"),
        pacs.PACS_PATIENT_ID.cast("string").alias("source_row_id"),
        F.when(pacs.PERSON_ID.isNotNull(), F.lit("resolved"))
        .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.date_format(pacs.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        pacs.ADC_UPDT.alias("loaded_at"),
    )

    ali = read_source(SRC_PATIENT_IDENTIFIER)
    ali_skey, ali_ssys = subject_key_with_system(
        [("urn:cerner:person_id", ali.PERSON_ID)], SRC_PATIENT_IDENTIFIER, ali.SOURCE_PK
    )
    ali_rows = ali.select(
        stable_id("person_identifier:mill_alias", ali.SOURCE_PK).alias("person_identifier_key"),
        F.lit("mill_alias").alias("source_object"),
        ali.SOURCE_PK.cast("string").alias("source_identifier_id"),
        ali_skey.alias("subject_key"),
        ali_ssys.alias("subject_id_system"),
        ali.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(ali.ALIAS_TYPE == F.lit("NHS"), F.lit("https://fhir.nhs.uk/Id/nhs-number"))
         .otherwise(F.lit("urn:barts:mrn")).alias("identifier_system"),
        ali.ALIAS_VALUE.alias("identifier_value"),
        F.when(ali.ALIAS_TYPE == F.lit("NHS"), F.lit("NH")).otherwise(F.lit("MR")).alias("identifier_type_code"),
        F.when(F.coalesce(ali.CURRENT_IND, F.lit(False)), F.lit("active"))
         .otherwise(F.lit("inactive")).alias("status"),
        ali.BEG_EFFECTIVE_DT_TM.alias("valid_from"),
        F.when(ali.END_EFFECTIVE_DT_TM < F.lit("2100-01-01").cast("timestamp"),
               ali.END_EFFECTIVE_DT_TM).alias("valid_to"),
        F.lit("millennium").alias("source_system"),
        F.lit(SRC_PATIENT_IDENTIFIER).alias("source_table"),
        ali.SOURCE_PK.cast("string").alias("source_row_id"),
        F.lit("resolved").alias("identity_status"),
        F.date_format(ali.PIPELINE_UPDT_DT_TM, "yyyyMMddHHmmss").alias("load_batch_id"),
        ali.PIPELINE_UPDT_DT_TM.alias("loaded_at"),
    )

    e = read_source(SRC_ENDOBASE_PATIENT)
    e_skey, e_ssys = subject_key_with_system(
        [("urn:cerner:person_id", e.PERSON_ID),
         ("urn:barts:endobase:patient-id", e.ENDOBASE_PATIENT_ID)],
        SRC_ENDOBASE_PATIENT,
        e.ENDOBASE_PATIENT_ID,
    )
    e_present = F.coalesce(e.SOURCE_PRESENT_IND, F.lit(True))
    e_rows = e.select(
        stable_id("person_identifier:endobase", e.ENDOBASE_PATIENT_ID)
        .alias("person_identifier_key"),
        F.lit("endobase").alias("source_object"),
        F.lit(None).cast("string").alias("source_identifier_id"),
        e_skey.alias("subject_key"),
        e_ssys.alias("subject_id_system"),
        e.PERSON_ID.cast("bigint").alias("person_id"),
        F.lit("urn:barts:endobase:patient-id").alias("identifier_system"),
        e.ENDOBASE_PATIENT_ID.cast("string").alias("identifier_value"),
        F.lit("ENDOBASE_INTERNAL").alias("identifier_type_code"),
        F.when(e_present, F.lit("active")).otherwise(F.lit("inactive")).alias("status"),
        e.SOURCE_CREATE_DT.cast("timestamp").alias("valid_from"),
        F.when(~e_present, e.ADC_UPDT).alias("valid_to"),
        F.lit("endobase").alias("source_system"),
        F.lit(SRC_ENDOBASE_PATIENT).alias("source_table"),
        e.ENDOBASE_PATIENT_ID.cast("string").alias("source_row_id"),
        F.when(e.PERSON_ID.isNotNull(), F.lit("resolved"))
         .when(e.PERSON_LINK_STATUS.isin("CONSENSUS", "MRN_ONLY", "NHS_ONLY"),
               F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.date_format(e.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        e.ADC_UPDT.alias("loaded_at"),
    )

    lk = read_source(SRC_CANCER_PTL_LINKAGE)
    lk_skey, lk_ssys = subject_key_with_system(
        [("urn:cerner:person_id", lk.PERSON_ID)],
        SRC_CANCER_PTL_LINKAGE,
        lk.ROW_ID.cast("string"),
    )

    def _ptl_identifier(namespace, system, value_col, type_code):
        # Publish nonblank PTL patient identifiers with stable keys and the supplied subject-
        # linkage evidence.
        return lk.where(F.trim(F.coalesce(value_col.cast("string"), F.lit(""))) != "").select(
            stable_id(namespace, lk.ROW_ID).alias("person_identifier_key"),
            F.regexp_extract(F.lit(namespace), r"^person_identifier:(.*)$", 1).alias("source_object"),
            F.lit(None).cast("string").alias("source_identifier_id"),
            lk_skey.alias("subject_key"),
            lk_ssys.alias("subject_id_system"),
            lk.PERSON_ID.cast("bigint").alias("person_id"),
            F.lit(system).alias("identifier_system"),
            value_col.cast("string").alias("identifier_value"),
            F.lit(type_code).alias("identifier_type_code"),
            F.when(F.coalesce(lk.PERSON_VALID_IND, F.lit(False)), F.lit("MATCHED"))
             .otherwise(F.lit("UNVALIDATED")).alias("status"),
            lk.REFERRAL_DATE_CLEAN.cast("timestamp").alias("valid_from"),
            F.lit(None).cast("timestamp").alias("valid_to"),
            F.lit("luna-pathfinder").alias("source_system"),
            F.lit(SRC_CANCER_PTL_LINKAGE).alias("source_table"),
            lk.ROW_ID.cast("string").alias("source_row_id"),
            F.lit("resolved").alias("identity_status"),
            F.date_format(lk.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
            lk.ADC_UPDT.alias("loaded_at"),
        )

    ptl_mrn_rows = _ptl_identifier(
        "person_identifier:luna_ptl:mrn", "urn:barts:mrn", lk.HOSPITAL_NUMBER, "MR"
    )
    ptl_nhs_rows = _ptl_identifier(
        "person_identifier:luna_ptl:nhs",
        "https://fhir.nhs.uk/Id/nhs-number",
        lk.NHS_NUMBER,
        "NH",
    )
    return (
        p_rows.unionByName(c_key_rows).unionByName(c_number_rows)
        .unionByName(pacs_rows).unionByName(ali_rows).unionByName(e_rows)
        .unionByName(ptl_mrn_rows).unionByName(ptl_nhs_rows)
    )

@materialized_view(
    name=_n("journey_spine.person_identifier"),
    comment="One source identifier assignment. subject_key is person-keyed where resolution exists and source-keyed otherwise.",
    refresh_policy="incremental",
    column_comments=PERSON_IDENTIFIER_COLUMN_COMMENTS,
)
def person_identifier():
    # Build the declared dataset: One source identifier assignment. subject_key is person-keyed
    # where resolution exists and source-keyed otherwise.
    return _lifecycle_source_person_identifier().drop(*PERSON_IDENTIFIER_LIFECYCLE_FIELDS, *PERSON_IDENTIFIER_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_spine._person_identifier_metadata'),
    comment='Internal quality and batch metadata for spine_person_identifier; same row grain as the research table. Join keys: person_identifier_key, source_identifier_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def person_identifier_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # spine_person_identifier; same row grain as the research table. Join keys:
    # person_identifier_key, source_identifier_id.
    return (_lifecycle_source_person_identifier()).select(*PERSON_IDENTIFIER_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spine / episode

# COMMAND ----------

# ==== Episode spine containers — source-backed only, never event-indexed ====

SRC_EPISODE = "4_prod.bronze.map_episode"

EPISODE_COLUMN_COMMENTS = {
    "episode_key": "Deterministic SHA-256 key minted from Millennium EPISODE_ID.",
    "episode_id": "Millennium EPISODE_ID; primary key of this table.",
    "person_id": "Millennium PERSON_ID as BIGINT.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "episode_display": "Source episode display name.",
    "episode_type_code": "Source episode type code.",
    "episode_type_display": "Source episode type display.",
    "status_code": "Source episode status code.",
    "status_display": "Source episode status display.",
    "period_start": "Sentinel-cleaned episode begin timestamp (bronze DQ triplet).",
    "period_end": "Sentinel-cleaned episode end timestamp when supplied.",
    "breach_datetime": "Sentinel-cleaned episode breach timestamp when supplied.",
    "pause_days": "Source pause-day count.",
    "close_reason_code": "Source close-reason code.",
    "close_reason_display": "Source close-reason display.",
    "service_category_code": "Source service-category code.",
    "service_category_display": "Source service-category display.",
    "referring_facility_code": "Source referring-facility code.",
    "referring_facility_display": "Source referring-facility display.",
    "contributor_system_code": "Source contributor-system code.",
    "contributor_system_display": "Source contributor-system display.",
    "direct_encounter_key": "Deterministic encounter key for the agreement-gated direct reference; evidence only.",
    "direct_encounter_id": "Millennium ENCNTR_ID for the agreement-gated direct reference; evidence only.",
    "source_table": "Registered bronze source table.",
    "source_row_id": "Native source row identifier.",
    "record_status": "Derived map_episode row status: superseded when SOURCE_PRESENT_IND is false or ACTIVE_IND is not 1; otherwise active. A null SOURCE_PRESENT_IND defaults to present and a null ACTIVE_IND defaults to 1. Source absence maps to superseded here, not retracted; this is not the clinical episode status code.",
    "record_status_effective_from": "map_episode.ACTIVE_STATUS_DT_TM carried unchanged as the source active-status timestamp. No episode-start or ingestion-clock fallback is applied; a missing source value remains null.",
    "record_status_effective_to": "For a superseded episode, the first available of map_episode.END_EFFECTIVE_DT_TM earlier than 2100-01-01, ACTIVE_STATUS_DT_TM, and ADC_UPDT. Null for an active episode or when every fallback is unavailable. This is a derived end boundary and may use ingestion time; it is not a Boolean active indicator.",
    "loaded_at": "map_episode.ADC_UPDT carried unchanged from the contributing bronze episode row. This is ingestion provenance, not the Silver refresh time or the clinical episode start/end; a missing source timestamp remains null.",
}

EPISODE_LIFECYCLE_FIELDS = [
    'load_batch_id',
]

EPISODE_RETIRED_COLUMNS = [

]

EPISODE_LIFECYCLE_COLUMNS = [
    'episode_key',
    'episode_id',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_episode():
    # Assemble episode rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    s = read_source(SRC_EPISODE)
    skey, _ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_EPISODE, s.EPISODE_ID
    )
    superseded = (~F.coalesce(s.SOURCE_PRESENT_IND.cast("boolean"), F.lit(True))) | (
        F.coalesce(s.ACTIVE_IND, F.lit(1)) != 1
    )
    # contract v2: retain the episode and direct-encounter SHA values as keys and publish their Millennium ids
    return s.select(
        stable_id("episode:mill", s.EPISODE_ID).alias("episode_key"),
        s.EPISODE_ID.cast("bigint").alias("episode_id"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        skey.alias("subject_key"),
        s.DISPLAY.alias("episode_display"),
        s.EPISODE_TYPE_CD.cast("string").alias("episode_type_code"),
        s.EPISODE_TYPE_DESC.alias("episode_type_display"),
        s.EPISODE_STATUS_CD.cast("string").alias("status_code"),
        s.EPISODE_STATUS_DESC.alias("status_display"),
        s.EPISODE_START_DT_TM_CLEAN.alias("period_start"),
        s.EPISODE_STOP_DT_TM_CLEAN.alias("period_end"),
        s.EPISODE_BREACH_DT_TM_CLEAN.alias("breach_datetime"),
        s.EPISODE_PAUSE_DAYS_CNT.cast("long").alias("pause_days"),
        s.EPISODE_CLOSE_REASON_CD.cast("string").alias("close_reason_code"),
        s.EPISODE_CLOSE_REASON_DESC.alias("close_reason_display"),
        s.SERVICE_CATEGORY_CD.cast("string").alias("service_category_code"),
        s.SERVICE_CATEGORY_DESC.alias("service_category_display"),
        s.REFER_FACILITY_CD.cast("string").alias("referring_facility_code"),
        s.REFER_FACILITY_DESC.alias("referring_facility_display"),
        s.CONTRIBUTOR_SYSTEM_CD.cast("string").alias("contributor_system_code"),
        s.CONTRIBUTOR_SYSTEM_DESC.alias("contributor_system_display"),
        F.when(s.ENCNTR_ID.isNotNull(),
               stable_id("encounter:mill", s.ENCNTR_ID)).alias("direct_encounter_key"),
        s.ENCNTR_ID.cast("bigint").alias("direct_encounter_id"),
        F.when(superseded, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        s.ACTIVE_STATUS_DT_TM.alias("record_status_effective_from"),
        F.when(
            superseded,
            F.coalesce(
                F.when(s.END_EFFECTIVE_DT_TM < F.lit("2100-01-01").cast("timestamp"),
                       s.END_EFFECTIVE_DT_TM),
                s.ACTIVE_STATUS_DT_TM,
                s.ADC_UPDT,
            ),
        ).alias("record_status_effective_to"),
        F.lit(SRC_EPISODE).alias("source_table"),
        s.EPISODE_ID.cast("string").alias("source_row_id"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.ADC_UPDT.alias("loaded_at"),
    )

@materialized_view(
    name=_n("journey_spine.episode"),
    comment="One Millennium episode container (map_episode). Source-backed only; the direct "
            "encounter reference is agreement-gated evidence, never containment.",
    cluster_by=["person_id", "period_start"],
    refresh_policy="incremental",
    column_comments=EPISODE_COLUMN_COMMENTS,
)
def episode():
    # Build the declared dataset: One Millennium episode container (map_episode). Source-backed
    # only; the direct encounter reference is agreement-gated evidence, never containment.
    return _lifecycle_source_episode().drop(*EPISODE_LIFECYCLE_FIELDS, *EPISODE_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_spine._episode_metadata'),
    comment='Internal quality and batch metadata for spine_episode; same row grain as the research table. Join keys: episode_key, episode_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def episode_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for spine_episode; same
    # row grain as the research table. Join keys: episode_key, episode_id.
    return (_lifecycle_source_episode()).select(*EPISODE_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spine / episode encounter

# COMMAND ----------

EPISODE_ENCOUNTER_COLUMN_COMMENTS = {
    "episode_encounter_key": "Deterministic SHA-256 relationship key.",
    "episode_encounter_reltn_id": "Millennium EPISODE_ENCNTR_RELTN_ID; primary key of this table.",
    "episode_id": "Millennium EPISODE_ID; joins to spine_episode.episode_id.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "relation_status_code": "Source relation classification including the orphan marker for episode IDs absent from mill_episode.",
    "source_duplicate_count": "Raw byte-identical duplicate rows collapsed into this canonical row by bronze.",
    "valid_from": "Membership effective-from timestamp.",
    "valid_to": "Membership effective-to timestamp when supplied.",
    "source_table": "Registered bronze source table.",
    "source_row_id": "Native source row identifier.",
    "record_status": "Derived map_episode_encounter relationship-row status: superseded when SOURCE_PRESENT_IND is false or ACTIVE_IND is not 1; otherwise active. Null presence and activity flags default to present and 1 respectively. Source absence maps to superseded, not retracted; this does not describe the encounter's clinical status.",
    "loaded_at": "map_episode_encounter.ADC_UPDT carried unchanged from the contributing bronze relationship row. This is ingestion provenance, not the Silver refresh time or relationship-validity time; a missing source timestamp remains null.",
}

EPISODE_ENCOUNTER_LIFECYCLE_FIELDS = [
    'load_batch_id',
]

EPISODE_ENCOUNTER_RETIRED_COLUMNS = [

]

EPISODE_ENCOUNTER_LIFECYCLE_COLUMNS = [
    'episode_encounter_key',
    'episode_encounter_reltn_id',
    'load_batch_id',
]

@materialized_view(
    name=_n("journey_spine.episode_encounter"),
    comment="N:M episode-encounter membership (map_episode_encounter). Multi-relation pairs "
            "preserved; orphan episode references retained with their source classification.",
    cluster_by=["episode_id"],
    refresh_policy="incremental",
    column_comments=EPISODE_ENCOUNTER_COLUMN_COMMENTS,
)
def episode_encounter():
    # Build the declared dataset: N:M episode-encounter membership (map_episode_encounter).
    # Multi-relation pairs preserved; orphan episode references retained with their source
    # classification.
    return _lifecycle_source_episode_encounter().drop(*EPISODE_ENCOUNTER_LIFECYCLE_FIELDS, *EPISODE_ENCOUNTER_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_spine._episode_encounter_metadata'),
    comment='Internal quality and batch metadata for spine_episode_encounter; same row grain as the research table. Join keys: episode_encounter_key, episode_encounter_reltn_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def episode_encounter_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # spine_episode_encounter; same row grain as the research table. Join keys:
    # episode_encounter_key, episode_encounter_reltn_id.
    return (_lifecycle_source_episode_encounter()).select(*EPISODE_ENCOUNTER_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spine / encounter identifier

# COMMAND ----------

SRC_ENCOUNTER_IDENTIFIER = "4_prod.bronze.map_encounter_identifier"

ENCOUNTER_IDENTIFIER_COLUMN_COMMENTS = {
    "encounter_identifier_key": "Deterministic SHA-256 key minted from the source alias row.",
    "source_identifier_id": "Native map_encounter_identifier SOURCE_PK rendered as text.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "identifier_system": "Type-scoped identifier system URI (urn:cerner:encntr_alias:<normalized alias type>) — 23.17M values live under multiple alias types",
    "identifier_type_code": "Source alias type verbatim.",
    "identifier_value": "Identifier value; published and IG-governed at serve time",
    "status": "Alias lifecycle status.",
    "current_ind": "Bronze current-alias indicator.",
    "multi_active_ind": "Bronze multiple-active-aliases indicator for this encounter and type.",
    "valid_from": "Alias effective-from timestamp.",
    "valid_to": "Alias effective-to timestamp when supplied.",
    "source_table": "Registered bronze source table.",
    "source_row_id": "Native source row identifier.",
    "loaded_at": "map_encounter_identifier.PIPELINE_UPDT_DT_TM carried unchanged from the contributing bronze alias row. This records bronze processing provenance, not identifier validity or the Silver refresh time; a missing source timestamp remains null.",
}

ENCOUNTER_IDENTIFIER_LIFECYCLE_FIELDS = [
    'load_batch_id',
]

ENCOUNTER_IDENTIFIER_RETIRED_COLUMNS = [

]

ENCOUNTER_IDENTIFIER_LIFECYCLE_COLUMNS = [
    'encounter_identifier_key',
    'source_identifier_id',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_encounter_identifier():
    # 22 live rows carry a NULL ALIAS_VALUE (8 of them current). An identifier row with
    # no value says nothing, so those rows are excluded and the row-count check compares
    # against the non-null source count.
    # Assemble encounter identifier rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    a = read_source(SRC_ENCOUNTER_IDENTIFIER).where(F.col("ALIAS_VALUE").isNotNull())
    # contract v2: rename the assignment hash and publish native SOURCE_PK and ENCNTR_ID values
    return a.select(
        stable_id("encounter_identifier:mill_alias", a.SOURCE_PK).alias("encounter_identifier_key"),
        a.SOURCE_PK.cast("string").alias("source_identifier_id"),
        a.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        # Type-scoped system: 23.17M ALIAS_VALUEs live under MULTIPLE alias types — a single
        # system string would collide (system, value) across types.
        F.concat(
            F.lit("urn:cerner:encntr_alias:"),
            F.lower(F.regexp_replace(F.coalesce(a.ALIAS_TYPE, F.lit("unknown")),
                                     "[^A-Za-z0-9]+", "_")),
        ).alias("identifier_system"),
        a.ALIAS_TYPE.alias("identifier_type_code"),
        a.ALIAS_VALUE.alias("identifier_value"),
        F.when(F.coalesce(a.CURRENT_IND, F.lit(False)), F.lit("active"))
         .otherwise(F.lit("inactive")).alias("status"),
        a.CURRENT_IND.alias("current_ind"),
        a.MULTI_ACTIVE_IND.alias("multi_active_ind"),
        a.BEG_EFFECTIVE_DT_TM.alias("valid_from"),
        F.when(a.END_EFFECTIVE_DT_TM < F.lit("2100-01-01").cast("timestamp"),
               a.END_EFFECTIVE_DT_TM).alias("valid_to"),
        F.lit(SRC_ENCOUNTER_IDENTIFIER).alias("source_table"),
        a.SOURCE_PK.cast("string").alias("source_row_id"),
        F.date_format(a.PIPELINE_UPDT_DT_TM, "yyyyMMddHHmmss").alias("load_batch_id"),
        a.PIPELINE_UPDT_DT_TM.alias("loaded_at"),
    )

@materialized_view(
    name=_n("journey_spine.encounter_identifier"),
    comment="One source encounter identifier assignment (map_encounter_identifier); reference "
            "surface parallel to person_identifier. Feed family has no ADC_UPDT: loaded_at rides "
            "the bronze pipeline write timestamp.",
    refresh_policy="incremental",
    column_comments=ENCOUNTER_IDENTIFIER_COLUMN_COMMENTS,
)
def encounter_identifier():
    # Build the declared dataset: One source encounter identifier assignment
    # (map_encounter_identifier); reference surface parallel to person_identifier. Feed family
    # has no ADC_UPDT: loaded_at rides the bronze pipeline write timestamp.
    return _lifecycle_source_encounter_identifier().drop(*ENCOUNTER_IDENTIFIER_LIFECYCLE_FIELDS, *ENCOUNTER_IDENTIFIER_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_spine._encounter_identifier_metadata'),
    comment='Internal quality and batch metadata for spine_encounter_identifier; same row grain as the research table. Join keys: encounter_identifier_key, source_identifier_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def encounter_identifier_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # spine_encounter_identifier; same row grain as the research table. Join keys:
    # encounter_identifier_key, source_identifier_id.
    return (_lifecycle_source_encounter_identifier()).select(*ENCOUNTER_IDENTIFIER_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Location projection

# COMMAND ----------

SRC_CARE_SITE      = "4_prod.bronze.map_care_site"

def _location_projection():
    # Derive the facility, building, unit and room location hierarchy from care-site evidence.
    s = read_source(SRC_CARE_SITE)

    # contract v2: retain SHA hierarchy keys while publishing level-qualified source location codes and native organization ids
    facilities = (
        s.where(s.facility_cd.isNotNull())
         .groupBy("facility_cd")
         .agg(
             F.max("facility_name").alias("name"),
             F.max("facility_code_active_ind").alias("active_ind"),
             F.min("facility_relation_beg_effective_dt_tm").alias("valid_from"),
             F.max("facility_relation_end_effective_dt_tm").alias("valid_to"),
             F.max("ORGANIZATION_ID").alias("organization_id"),
             F.max("masked_zipcode").alias("address_postcode_masked"),
             F.max("city").alias("address_city"),
             F.max("latitude").alias("latitude"),
             F.max("longitude").alias("longitude"),
             F.max("ADC_UPDT").alias("loaded_at"),
         )
         .select(
             stable_id("location:mill:facility", F.col("facility_cd")).alias("location_key"),
             F.col("facility_cd").cast("string").alias("location_code"),
             F.lit(None).cast("string").alias("parent_location_key"),
             F.lit("facility").alias("location_level"),
             F.col("facility_cd").cast("string").alias("source_location_code"),
             F.col("name"),
             F.when(F.col("active_ind") == 1, F.lit("active")).otherwise(F.lit("inactive")).alias("status"),
             F.col("organization_id").cast("bigint").alias("organization_id"),
             F.lit("si").alias("physical_type_code"),
             F.col("valid_from"), F.col("valid_to"),
             F.col("latitude"), F.col("longitude"),
             F.col("address_city"), F.col("address_postcode_masked"),
             F.lit(SRC_CARE_SITE).alias("source_table"),
             F.col("facility_cd").cast("string").alias("source_row_id"),
             F.date_format("loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"),
             F.col("loaded_at"),
         )
    )

    buildings = (
        s.where(s.building_cd.isNotNull())
         .groupBy("building_cd")
         .agg(
             F.max("building_name").alias("name"),
             F.max("building_code_active_ind").alias("active_ind"),
             F.min("building_relation_beg_effective_dt_tm").alias("valid_from"),
             F.max("building_relation_end_effective_dt_tm").alias("valid_to"),
             F.min("facility_cd").alias("facility_cd"),
             F.max("ORGANIZATION_ID").alias("organization_id"),
             F.max("masked_zipcode").alias("address_postcode_masked"),
             F.max("city").alias("address_city"),
             F.max("latitude").alias("latitude"),
             F.max("longitude").alias("longitude"),
             F.max("ADC_UPDT").alias("loaded_at"),
         )
         .select(
             stable_id("location:mill:building", F.col("building_cd")).alias("location_key"),
             F.col("building_cd").cast("string").alias("location_code"),
             F.when(F.col("facility_cd").isNotNull(),
                    stable_id("location:mill:facility", F.col("facility_cd"))).alias("parent_location_key"),
             F.lit("building").alias("location_level"),
             F.col("building_cd").cast("string").alias("source_location_code"),
             F.col("name"),
             F.when(F.col("active_ind") == 1, F.lit("active")).otherwise(F.lit("inactive")).alias("status"),
             F.col("organization_id").cast("bigint").alias("organization_id"),
             F.lit("bu").alias("physical_type_code"),
             F.col("valid_from"), F.col("valid_to"),
             F.col("latitude"), F.col("longitude"),
             F.col("address_city"), F.col("address_postcode_masked"),
             F.lit(SRC_CARE_SITE).alias("source_table"),
             F.col("building_cd").cast("string").alias("source_row_id"),
             F.date_format("loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"),
             F.col("loaded_at"),
         )
    )

    units = s.select(
        stable_id("location:mill:nurse_unit", s.care_site_cd).alias("location_key"),
        s.care_site_cd.cast("string").alias("location_code"),
        F.when(s.building_cd.isNotNull(), stable_id("location:mill:building", s.building_cd))
         .when(s.facility_cd.isNotNull(), stable_id("location:mill:facility", s.facility_cd))
         .alias("parent_location_key"),
        F.lit("nurse_unit").alias("location_level"),
        s.care_site_cd.cast("string").alias("source_location_code"),
        s.care_site_name.alias("name"),
        F.when(s.location_active_ind == 1, F.lit("active")).otherwise(F.lit("inactive")).alias("status"),
        s.ORGANIZATION_ID.cast("bigint").alias("organization_id"),
        F.lit("wa").alias("physical_type_code"),
        s.location_beg_effective_dt_tm.alias("valid_from"),
        s.location_end_effective_dt_tm.alias("valid_to"),
        s.latitude, s.longitude,
        s.city.alias("address_city"),
        s.masked_zipcode.alias("address_postcode_masked"),
        F.lit(SRC_CARE_SITE).alias("source_table"),
        s.care_site_cd.cast("string").alias("source_row_id"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.ADC_UPDT.alias("loaded_at"),
    )
    return facilities.unionByName(buildings).unionByName(units)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / location

# COMMAND ----------

LOCATION_COLUMN_COMMENTS = {
    "location_key": "Deterministic SHA-256 key across location levels.",
    "location_code": "Native Millennium location code; primary key together with location_level.",
    "parent_location_key": "Deterministic SHA-256 key of the parent facility or building.",
    "location_level": "Derived hierarchy level.",
    "source_location_code": "Source Millennium location code.",
    "name": "Source location name.",
    "status": "Source-derived location status.",
    "organization_id": "Millennium ORGANIZATION_ID as BIGINT when available.",
    "physical_type_code": "FHIR physical location type code.",
    "valid_from": "Source validity start.",
    "valid_to": "Source validity end.",
    "latitude": "Source address latitude.",
    "longitude": "Source address longitude.",
    "address_city": "Source address city.",
    "address_postcode_masked": "Privacy-aware source postcode.",
    "source_table": "Fully qualified bronze source table.",
    "source_row_id": "Stable source row identifier.",
    "loaded_at": "From map_care_site: maximum ADC_UPDT per facility_cd for facility rows, maximum ADC_UPDT per building_cd for building rows, and the individual source row's ADC_UPDT for nurse-unit rows. The hierarchy union adds no parent clock or cross-level aggregation; this is bronze ingestion provenance, not location validity time or Silver refresh time.",
}

LOCATION_LIFECYCLE_FIELDS = [
    'load_batch_id',
]

LOCATION_RETIRED_COLUMNS = [

]

LOCATION_LIFECYCLE_COLUMNS = [
    'location_key',
    'location_code',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_location():
    # contract v2: document location_key, level-qualified location_code, parent key, and native organization id
    # Assemble location rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    return _location_projection()

@materialized_view(
    name=_n("journey_reference.location"),
    comment="Effective-dated facility, building, and nurse-unit hierarchy derived from bronze care sites.",
    refresh_policy="incremental",
    column_comments=LOCATION_COLUMN_COMMENTS,
)
def location():
    # Build the declared dataset: Effective-dated facility, building, and nurse-unit hierarchy
    # derived from bronze care sites.
    return _lifecycle_source_location().drop(*LOCATION_LIFECYCLE_FIELDS, *LOCATION_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_reference._location_metadata'),
    comment='Internal quality and batch metadata for reference_location; same row grain as the research table. Join keys: location_key, location_code.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def location_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for reference_location;
    # same row grain as the research table. Join keys: location_key, location_code.
    return (_lifecycle_source_location()).select(*LOCATION_LIFECYCLE_COLUMNS)

# COMMAND ----------

SRC_PRACTITIONER   = "4_prod.bronze.map_medical_personnel"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / practitioner

# COMMAND ----------

PRACTITIONER_COLUMN_COMMENTS = {
    "practitioner_key": "Deterministic SHA-256 practitioner key.",
    "practitioner_id": "Millennium personnel PERSON_ID; primary key of this table.",
    "source_practitioner_id": "Millennium personnel PERSON_ID as BIGINT.",
    "active": "Source active indicator.",
    "name": "Source formatted practitioner name.",
    "physician_ind": "Source physician indicator.",
    "position_code": "Source position code.",
    "position_display": "Source position display.",
    "practitioner_type_code": "Source personnel type code.",
    "practitioner_type_display": "Source personnel type display.",
    "primary_location_key": "Deterministic key of the primary assigned or inferred location.",
    "medical_service_key": "Deterministic key of the selected medical service.",
    "npi": "Selected NPI identifier.",
    "doctor_number": "Selected organization doctor number.",
    "gdp_number": "Selected dental practitioner number.",
    "external_provider_id": "Selected external provider identifier.",
    "valid_from": "Source validity start.",
    "valid_to": "Source validity end.",
    "source_table": "Fully qualified bronze source table.",
    "source_row_id": "Stable source row identifier.",
    "record_status": "Directory rows are active when map_medical_personnel.ACTIVE_IND equals 1, otherwise superseded, including when the flag is null. Personnel referenced by clinical records but absent from the directory are published as placeholders with literal active status. Neither arm emits retracted; placeholder active status is not evidence of an active personnel-directory record.",
    "loaded_at": "Directory rows carry map_medical_personnel.ADC_UPDT only; clinical-reference clocks are not added to known personnel. For personnel absent from that directory, placeholders use the maximum clock across their contributing encounter, diagnosis, problem, implant, form, event, medication, text, appointment and theatre references. Implant references first coalesce SOURCE_MAX_ADC_UPDT with ADC_UPDT; other references use ADC_UPDT. This is arm-specific ingestion provenance, not a uniform implant clock or Silver refresh time.",
}

PRACTITIONER_LIFECYCLE_FIELDS = [
    'load_batch_id',
]

PRACTITIONER_RETIRED_COLUMNS = [

]

PRACTITIONER_LIFECYCLE_COLUMNS = [
    'practitioner_key',
    'practitioner_id',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_practitioner():
    # Assemble practitioner rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    s = read_source(SRC_PRACTITIONER)
    # contract v2: retain practitioner, location, and service hashes as keys while publishing personnel ids as BIGINT
    directory = s.select(
        stable_id("practitioner:mill", s.PERSON_ID).alias("practitioner_key"),
        s.PERSON_ID.cast("bigint").alias("practitioner_id"),
        s.PERSON_ID.cast("bigint").alias("source_practitioner_id"),
        (s.ACTIVE_IND == 1).alias("active"),
        s.NAME_FULL_FORMATTED.alias("name"),
        s.PHYSICIAN_IND.alias("physician_ind"),
        s.POSITION_CD.cast("string").alias("position_code"),
        s.position_name.alias("position_display"),
        s.PRSNL_TYPE_CD.cast("string").alias("practitioner_type_code"),
        s.prsnl_type_name.alias("practitioner_type_display"),
        F.when(s.primary_care_site_cd.isNotNull(),
               stable_id("location:mill:nurse_unit", s.primary_care_site_cd)).alias("primary_location_key"),
        F.when(s.MEDSERVICE_GROUP_ID.isNotNull(),
               stable_id("service:mill:medical", s.MEDSERVICE_GROUP_ID)).alias("medical_service_key"),
        s.NPI.alias("npi"),
        s.DOCNBR.alias("doctor_number"),
        s.GDP_NUMBER.alias("gdp_number"),
        s.EXTERNAL_PROVIDER_ID.alias("external_provider_id"),
        F.when(s.ACTIVE_IND == 1, F.lit("active")).otherwise(F.lit("superseded")).alias("record_status"),
        s.BEG_EFFECTIVE_DT_TM.alias("valid_from"),
        s.END_EFFECTIVE_DT_TM.alias("valid_to"),
        F.lit(SRC_PRACTITIONER).alias("source_table"),
        s.PERSON_ID.cast("string").alias("source_row_id"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.ADC_UPDT.alias("loaded_at"),
    )
    encounter = read_source(SRC_ENCOUNTER)
    referenced = (
        encounter.where(encounter.REG_PRSNL_ID.isNotNull())
                 .select(encounter.REG_PRSNL_ID.cast("bigint").alias("source_practitioner_id"),
                         encounter.ADC_UPDT.alias("loaded_at"))
        .unionByName(
            encounter.where(encounter.CREATE_PRSNL_ID.isNotNull())
                     .select(encounter.CREATE_PRSNL_ID.cast("bigint").alias("source_practitioner_id"),
                             encounter.ADC_UPDT.alias("loaded_at"))
        )
        .unionByName(
            encounter.where(encounter.DISCH_PRSNL_ID.isNotNull())
                     .select(encounter.DISCH_PRSNL_ID.cast("bigint").alias("source_practitioner_id"),
                             encounter.ADC_UPDT.alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_DIAGNOSIS).where(F.col("DIAG_PRSNL_ID").isNotNull())
              .select(F.col("DIAG_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_PROBLEM).where(F.col("ACTIVE_STATUS_PRSNL_ID").isNotNull())
              .select(F.col("ACTIVE_STATUS_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_IMPLANT_DETAILS).where(F.col("PERFORMED_PRSNL_ID").isNotNull())
              .select(F.col("PERFORMED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.coalesce(F.col("SOURCE_MAX_ADC_UPDT"), F.col("ADC_UPDT")).alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_FORM_ACTIVITY).where(F.col("PERFORMED_PRSNL_ID_LONG").isNotNull())
              .select(F.col("PERFORMED_PRSNL_ID_LONG").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_NUMERIC_EVENTS).where(F.col("PERFORMED_PRSNL_ID").isNotNull())
              .select(F.col("PERFORMED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MED_ADMIN).where(F.col("PRSNL_ID").isNotNull())
              .select(F.col("PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MED_ADMIN).where(F.col("PERFORMED_PRSNL_ID").isNotNull())
              .select(F.col("PERFORMED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MED_ADMIN).where(F.col("CE_VERIFIED_PRSNL_ID").isNotNull())
              .select(F.col("CE_VERIFIED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MED_ADMIN).where(F.col("MAE_VERIFIED_PRSNL_ID").isNotNull())
              .select(F.col("MAE_VERIFIED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MEDICATION_ORDER).where(F.col("LAST_UPDATE_PROVIDER_ID").isNotNull())
              .select(F.col("LAST_UPDATE_PROVIDER_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MEDICATION_ORDER_ACTION).where(F.col("ACTION_PERSONNEL_ID").isNotNull())
              .select(F.col("ACTION_PERSONNEL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MEDICATION_ORDER_ACTION).where(F.col("ORDER_PROVIDER_ID").isNotNull())
              .select(F.col("ORDER_PROVIDER_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MEDICATION_ORDER_ACTION).where(F.col("SUPERVISING_PROVIDER_ID").isNotNull())
              .select(F.col("SUPERVISING_PROVIDER_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_CODED_EVENTS).where(F.col("PERFORMED_PRSNL_ID").isNotNull())
              .select(F.col("PERFORMED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_CODED_EVENTS).where(F.col("VERIFIED_PRSNL_ID").isNotNull())
              .select(F.col("VERIFIED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_NOMEN_EVENTS).where(F.col("PERFORMED_PRSNL_ID").isNotNull())
              .select(F.col("PERFORMED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_NOMEN_EVENTS).where(F.col("VERIFIED_PRSNL_ID").isNotNull())
              .select(F.col("VERIFIED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_DATE_EVENTS).where(F.col("PERFORMED_PRSNL_ID").isNotNull())
              .select(F.col("PERFORMED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_DATE_EVENTS).where(F.col("VERIFIED_PRSNL_ID").isNotNull())
              .select(F.col("VERIFIED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_TEXT_EVENTS).where(F.col("PERFORMED_PRSNL_ID").isNotNull())
              .select(F.col("PERFORMED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_TEXT_EVENTS).where(F.col("VERIFIED_PRSNL_ID").isNotNull())
              .select(F.col("VERIFIED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MILL_BLOB_TEXT).where(F.col("UPDT_ID").isNotNull())
              .select(F.col("UPDT_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_APPOINTMENT).where(F.col("REQUESTED_PERSONNEL_ID").isNotNull())
              .select(F.col("REQUESTED_PERSONNEL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_APPOINTMENT_RESOURCE).where(F.col("ALLOCATED_PERSONNEL_ID").isNotNull())
              .select(F.col("ALLOCATED_PERSONNEL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_THEATRE_CASE).where(F.col("SURGEON_PERSONNEL_ID").isNotNull())
              .select(F.col("SURGEON_PERSONNEL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_THEATRE_CASE).where(F.col("ANAESTHETIST_PERSONNEL_ID").isNotNull())
              .select(F.col("ANAESTHETIST_PERSONNEL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_THEATRE_CASE_PROCEDURE).where(F.col("PRIMARY_SURGEON_ID").isNotNull())
              .select(F.col("PRIMARY_SURGEON_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .groupBy("source_practitioner_id")
        .agg(F.max("loaded_at").alias("loaded_at"))
    )
    known = s.select(s.PERSON_ID.cast("bigint").alias("source_practitioner_id"))
    missing = referenced.join(known, "source_practitioner_id", "left_anti")
    placeholders = missing.select(
        stable_id("practitioner:mill", F.col("source_practitioner_id")).alias("practitioner_key"),
        F.col("source_practitioner_id").cast("bigint").alias("practitioner_id"),
        F.col("source_practitioner_id"),
        F.lit(None).cast("boolean").alias("active"),
        F.lit(None).cast("string").alias("name"),
        F.lit(None).cast("boolean").alias("physician_ind"),
        F.lit(None).cast("string").alias("position_code"),
        F.lit(None).cast("string").alias("position_display"),
        F.lit(None).cast("string").alias("practitioner_type_code"),
        F.lit(None).cast("string").alias("practitioner_type_display"),
        F.lit(None).cast("string").alias("primary_location_key"),
        F.lit(None).cast("string").alias("medical_service_key"),
        F.lit(None).cast("string").alias("npi"),
        F.lit(None).cast("string").alias("doctor_number"),
        F.lit(None).cast("string").alias("gdp_number"),
        F.lit(None).cast("string").alias("external_provider_id"),
        F.lit("active").alias("record_status"),
        F.lit(None).cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.lit(SRC_ENCOUNTER).alias("source_table"),
        F.col("source_practitioner_id").alias("source_row_id"),
        F.date_format("loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("loaded_at"),
    )
    return directory.unionByName(placeholders)

@materialized_view(
    name=_n("journey_reference.practitioner"),
    comment="One effective-dated Millennium practitioner/personnel row; inactive rows are retained.",
    refresh_policy="incremental",
    column_comments=PRACTITIONER_COLUMN_COMMENTS,
)
def practitioner():
    # Build the declared dataset: One effective-dated Millennium practitioner/personnel row;
    # inactive rows are retained.
    return _lifecycle_source_practitioner().drop(*PRACTITIONER_LIFECYCLE_FIELDS, *PRACTITIONER_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_reference._practitioner_metadata'),
    comment='Internal quality and batch metadata for reference_practitioner; same row grain as the research table. Join keys: practitioner_key, practitioner_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def practitioner_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # reference_practitioner; same row grain as the research table. Join keys: practitioner_key,
    # practitioner_id.
    return (_lifecycle_source_practitioner()).select(*PRACTITIONER_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / service

# COMMAND ----------

SERVICE_COLUMN_COMMENTS = {
    "service_key": "Deterministic SHA-256 service key.",
    "service_id": "Millennium MEDSERVICE_GROUP_ID; primary key of this table.",
    "source_service_code": "Source personnel-group service code.",
    "name": "Source service name.",
    "status": "Source-derived service status.",
    "source_table": "Fully qualified bronze source table.",
    "source_row_id": "Stable source service identifier.",
    "loaded_at": "Maximum map_medical_personnel.ADC_UPDT over source rows sharing the non-null MEDSERVICE_GROUP_ID. This includes all contributing group assignments, not only active assignments or the row supplying the selected service name; it is bronze ingestion provenance, not Silver refresh time.",
}

SERVICE_LIFECYCLE_FIELDS = [
    'load_batch_id',
]

SERVICE_RETIRED_COLUMNS = [

]

SERVICE_LIFECYCLE_COLUMNS = [
    'service_key',
    'service_id',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_service():
    # Assemble service rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    s = read_source(SRC_PRACTITIONER)
    return (
        s.where(s.MEDSERVICE_GROUP_ID.isNotNull())
         .groupBy("MEDSERVICE_GROUP_ID")
         .agg(
             F.max(F.coalesce("MEDSERVICE_GROUP_NAME", "MEDSERVICE")).alias("name"),
             F.max("MEDSERVICE_ACTIVE_ASSIGNMENT_COUNT").alias("active_assignment_count"),
             F.max(F.col("MEDSERVICE_SELECTED_INACTIVE_IND").cast("int")).alias("selected_inactive_ind"),
             F.max("ADC_UPDT").alias("loaded_at"),
         )
         # contract v2: retain the service SHA as service_key and publish MEDSERVICE_GROUP_ID as BIGINT
         .select(
             stable_id("service:mill:medical", F.col("MEDSERVICE_GROUP_ID")).alias("service_key"),
             F.col("MEDSERVICE_GROUP_ID").cast("bigint").alias("service_id"),
             F.col("MEDSERVICE_GROUP_ID").cast("string").alias("source_service_code"),
             F.col("name"),
             F.when((F.coalesce("active_assignment_count", F.lit(0)) > 0) &
                    (F.coalesce("selected_inactive_ind", F.lit(0)) == 0),
                    F.lit("active")).otherwise(F.lit("inactive")).alias("status"),
             F.lit(SRC_PRACTITIONER).alias("source_table"),
             F.col("MEDSERVICE_GROUP_ID").cast("string").alias("source_row_id"),
             F.date_format("loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"),
             F.col("loaded_at"),
         )
    )

@materialized_view(
    name=_n("journey_reference.service"),
    comment="Thin v1 medical-service dimension from practitioner group assignments.",
    refresh_policy="incremental",
    column_comments=SERVICE_COLUMN_COMMENTS,
)
def service():
    # Build the declared dataset: Thin v1 medical-service dimension from practitioner group
    # assignments.
    return _lifecycle_source_service().drop(*SERVICE_LIFECYCLE_FIELDS, *SERVICE_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_reference._service_metadata'),
    comment='Internal quality and batch metadata for reference_service; same row grain as the research table. Join keys: service_key, service_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def service_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for reference_service;
    # same row grain as the research table. Join keys: service_key, service_id.
    return (_lifecycle_source_service()).select(*SERVICE_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / organization

# COMMAND ----------

ORGANIZATION_COLUMN_COMMENTS = {
    "organization_key": "Deterministic SHA-256 organization key.",
    "organization_id": "Millennium ORGANIZATION_ID; primary key for the Millennium arm.",
    "source_organization_id": "Millennium organization identifier.",
    "name": "Source organization name.",
    "status": "Source-derived organization status.",
    "valid_from": "Source validity start.",
    "valid_to": "Source validity end.",
    "address_city": "Source organization address city.",
    "address_postcode_masked": "Privacy-aware source postcode.",
    "source_table": "Fully qualified bronze source table.",
    "source_row_id": "Stable source organization identifier.",
    "loaded_at": "Maximum map_care_site.ADC_UPDT over source rows sharing the non-null ORGANIZATION_ID. The maximum is not restricted to active source rows or the row supplying the selected name/address; it records contributing bronze ingestion, not organization validity time or Silver refresh time.",
}

ORGANIZATION_LIFECYCLE_FIELDS = [
    'load_batch_id',
]

ORGANIZATION_RETIRED_COLUMNS = [

]

ORGANIZATION_LIFECYCLE_COLUMNS = [
    'organization_key',
    'organization_id',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_organization():
    # Assemble organization rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    s = read_source(SRC_CARE_SITE)
    return (
        s.where(s.ORGANIZATION_ID.isNotNull())
         .groupBy("ORGANIZATION_ID")
         .agg(
             F.max("organization_name").alias("name"),
             F.max("organization_active_ind").alias("active_ind"),
             F.min("organization_beg_effective_dt_tm").alias("valid_from"),
             F.max("organization_end_effective_dt_tm").alias("valid_to"),
             F.max("masked_zipcode").alias("address_postcode_masked"),
             F.max("city").alias("address_city"),
             F.max("ADC_UPDT").alias("loaded_at"),
         )
         # contract v2: retain the organization SHA as organization_key and publish ORGANIZATION_ID as BIGINT
         .select(
             stable_id("organization:mill", F.col("ORGANIZATION_ID")).alias("organization_key"),
             F.col("ORGANIZATION_ID").cast("bigint").alias("organization_id"),
             F.col("ORGANIZATION_ID").cast("string").alias("source_organization_id"),
             F.col("name"),
             F.when(F.col("active_ind") == 1, F.lit("active")).otherwise(F.lit("inactive")).alias("status"),
             F.col("valid_from"), F.col("valid_to"),
             F.col("address_city"), F.col("address_postcode_masked"),
             F.lit(SRC_CARE_SITE).alias("source_table"),
             F.col("ORGANIZATION_ID").cast("string").alias("source_row_id"),
             F.date_format("loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"),
             F.col("loaded_at"),
         )
    )

@materialized_view(
    name=_n("journey_reference.organization"),
    comment="Thin v1 provider organization dimension derived from bronze care sites.",
    refresh_policy="incremental",
    column_comments=ORGANIZATION_COLUMN_COMMENTS,
)
def organization():
    # Build the declared dataset: Thin v1 provider organization dimension derived from bronze
    # care sites.
    return _lifecycle_source_organization().drop(*ORGANIZATION_LIFECYCLE_FIELDS, *ORGANIZATION_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_reference._organization_metadata'),
    comment='Internal quality and batch metadata for reference_organization; same row grain as the research table. Join keys: organization_key, organization_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def organization_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # reference_organization; same row grain as the research table. Join keys: organization_key,
    # organization_id.
    return (_lifecycle_source_organization()).select(*ORGANIZATION_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Events /  event index

# COMMAND ----------

def _event_index_projection(
    df,
    event_type,
    fact_table,
    fact_category,
    source_system=None,
    source_object=None,
    source_row_key=None,
):
    # Project a typed fact into the common patient-event index, including its source and storage
    # references.
    sys_col = _source_system_display(F.col("_source_system")) if source_system is None else source_system
    obj_col = _source_object_display(F.col("_source_table")) if source_object is None else source_object
    key_col = F.col("_source_row_id").cast("string") if source_row_key is None else source_row_key
    # contract v2: carry the v2 event key and derive each typed-fact row key from that same immutable key
    return df.select(
        "patient_event_key", "subject_key", "subject_id_system", "person_id", "identity_status",
        "encounter_id", "event_datetime", "event_end_datetime",
        F.lit(event_type).alias("event_type"),
        F.lit(fact_category).alias("fact_category"),
        sys_col.alias("source_system"), obj_col.alias("source_object"),
        key_col.alias("source_row_key"),
        "source_coding_system", "source_code", "source_display",
        "record_status", "confidentiality_code", "vip_ind", "withheld_identity_ind",
        F.lit(fact_table).alias("fact_table"), F.col("patient_event_key").alias("fact_row_key"),
        "load_batch_id", "loaded_at",
    )

def _s3b_public_event_index_projection(df, event_type, fact_table, axis, source_object):
    """Project component and registry rows that have no canonical event-index row."""
    columns = set(df.columns)

    def optional(name, data_type):
        # Return a typed source column when present, or a typed null for a missing optional
        # field.
        return F.col(name).cast(data_type) if name in columns else F.lit(None).cast(data_type)

    source_object_col = optional("source_object", "string")
    source_system_col = optional("source_system", "string")
    source_row_key = (
        optional("source_patient_event_key", "string")
        if "source_patient_event_key" in columns
        else F.col("patient_event_key").cast("string")
    )
    source_coding_system = (
        optional("source_coding_system", "string")
        if "source_coding_system" in columns
        else optional(f"{axis}_source_system", "string")
    )
    source_code = (
        optional("source_code", "string")
        if "source_code" in columns
        else optional(f"{axis}_source_code", "string")
    )
    source_display = (
        optional("source_display", "string")
        if "source_display" in columns
        else optional(f"{axis}_source_display", "string")
    )
    return df.select(
        F.col("patient_event_key").cast("string").alias("patient_event_key"),
        optional("subject_key", "string").alias("subject_key"),
        optional("subject_id_system", "string").alias("subject_id_system"),
        optional("person_id", "bigint").alias("person_id"),
        F.when(optional("person_id", "bigint").isNotNull(), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        optional("encounter_id", "string").alias("encounter_id"),
        optional("event_datetime", "timestamp").alias("event_datetime"),
        optional("event_end_datetime", "timestamp").alias("event_end_datetime"),
        F.lit(event_type).alias("event_type"), F.lit("clinical").alias("fact_category"),
        F.coalesce(source_system_col, F.lit("S3b Silver component")).alias("source_system"),
        F.coalesce(source_object_col, F.lit(source_object)).alias("source_object"),
        source_row_key.alias("source_row_key"),
        source_coding_system.alias("source_coding_system"), source_code.alias("source_code"),
        source_display.alias("source_display"), optional("record_status", "string").alias("record_status"),
        optional("confidentiality_code", "string").alias("confidentiality_code"),
        optional("vip_ind", "boolean").alias("vip_ind"),
        optional("withheld_identity_ind", "boolean").alias("withheld_identity_ind"),
        F.lit(fact_table).alias("fact_table"),
        F.col("patient_event_key").cast("string").alias("fact_row_key"),
        optional("load_batch_id", "string").alias("load_batch_id"),
        optional("loaded_at", "timestamp").alias("loaded_at"),
    )

def _s3b_event_index_supplement():
    """Add split component events absent from the canonical event index."""
    split_specs = (
        ("journey_clinical.condition", "condition", "journey_clinical.condition", "condition", "condition"),
        ("journey_clinical.family_history", "family_history", "journey_clinical.family_history", "condition", "family_history"),
        ("journey_clinical.procedure", "procedure", "journey_clinical.procedure", "procedure", "procedure"),
        ("journey_clinical.medication_admin", "medication_admin", "journey_clinical.medication_admin", "medication", "medication_admin"),
        ("journey_clinical.medication_order", "medication_order", "journey_clinical.medication_order", "medication", "medication_order"),
        ("journey_clinical.medication_dispense", "medication_dispense", "journey_clinical.medication_dispense", "medication", "medication_dispense"),
        ("journey_clinical.presenting_complaint", "presenting_complaint", "journey_clinical.presenting_complaint", "complaint", "presenting_complaint"),
    )
    frames = []
    for relation, event_type, fact_table, axis, source_object in split_specs:
        source = spark.read.table(_n(relation))
        if "source_patient_event_key" not in source.columns:
            continue
        components = source.where(
            ~F.col("patient_event_key").eqNullSafe(F.col("source_patient_event_key"))
        )
        frames.append(_s3b_public_event_index_projection(
            components, event_type, fact_table, axis, source_object
        ))
    registry_findings = spark.read.table(_n("journey_clinical.clinical_finding")).where(
        F.col("source_object").startswith("registry:")
    )
    frames.append(_s3b_public_event_index_projection(
        registry_findings, "clinical_finding", "journey_clinical.clinical_finding",
        "value", "registry_observation"
    ))
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame)
    return out.dropDuplicates(["patient_event_key"])

@materialized_view(
    name=_n("journey_events._event_index"),
    private=True,
    comment="INTERNAL event-grain stage (one row per admitted event) behind patient_event. "
            "Internal consumers needing event grain read THIS, never the public N-row table.",
    refresh_policy="incremental",
)
def _event_index():
    # Build the declared dataset: INTERNAL event-grain stage (one row per admitted event) behind
    # patient_event. Internal consumers needing event grain read THIS, never the public N-row
    # table.
    lanes = [
        _event_index_projection(_family_history_canonical(), "family_history",
                                "journey_clinical.family_history", "clinical"),
        _event_index_projection(_presenting_complaint_canonical(), "presenting_complaint",
                                "journey_clinical.presenting_complaint", "clinical"),
        _event_index_projection(_allergy_canonical(), "allergy_intolerance",
                                "journey_clinical.allergy_intolerance", "clinical"),
        _event_index_projection(_transfusion_canonical(), "transfusion",
                                "journey_clinical.transfusion", "clinical"),
        _event_index_projection(_cancer_treatment_canonical(), "cancer_treatment",
                                "journey_clinical.cancer_treatment", "clinical"),
        _event_index_projection(_condition_stage_canonical(), "condition_stage",
                                "journey_clinical.condition_stage", "clinical"),
        _event_index_projection(_endoscopy_finding_canonical(), "endoscopy_finding",
                                "journey_clinical.endoscopy_finding", "clinical"),
        _event_index_projection(_registry_entry_canonical(), "registry_entry",
                                "journey_clinical.registry_entry", "clinical"),
        _event_index_projection(_device_canonical(), "device",
                                "journey_clinical.device", "clinical"),
        _event_index_projection(_condition_diagnosis_canonical(), "condition",
                                "journey_clinical.condition", "clinical"),
        _event_index_projection(_condition_problem_canonical(), "condition",
                                "journey_clinical.condition", "clinical"),
        _event_index_projection(_procedure_canonical(), "procedure",
                                "journey_clinical.procedure", "clinical"),
        _event_index_projection(_implant_procedure_canonical(), "procedure",
                                "journey_clinical.procedure", "clinical"),
        _event_index_projection(_theatre_procedure_canonical(), "procedure",
                                "journey_clinical.procedure", "clinical"),
        _event_index_projection(_endobase_procedure_canonical(), "procedure",
                                "journey_clinical.procedure", "clinical"),
        _event_index_projection(_pathology_requested_test_canonical(), "pathology_order",
                                "journey_clinical.pathology_order", "clinical"),
        _event_index_projection(_pathology_specimen_canonical(), "specimen",
                                "journey_clinical.specimen", "clinical"),
        _event_index_projection(_pathology_report_series_canonical(), "pathology_report",
                                "journey_clinical.pathology_report", "clinical"),
        _event_index_projection(_pathology_result_canonical(), "pathology_result",
                                "journey_clinical.pathology_result", "clinical"),
        _event_index_projection(_genomic_test_canonical(), "genomic_test",
                                "journey_clinical.genomic_test", "clinical"),
        _event_index_projection(_genomic_result_canonical(), "genomic_result",
                                "journey_clinical.genomic_result", "clinical"),
        _event_index_projection(_indication_canonical(), "indication",
                                "journey_clinical.indication", "clinical"),
        _event_index_projection(_micro_isolate_canonical(), "microbiology_isolate",
                                "journey_clinical.microbiology_isolate", "clinical"),
        _event_index_projection(_susceptibility_canonical(), "susceptibility_result",
                                "journey_clinical.susceptibility_result", "clinical"),
        _event_index_projection(_form_canonical(), "form",
                                "journey_clinical.form", "clinical"),
        _event_index_projection(_vital_sign_canonical(), "vital_sign",
                                "journey_clinical.vital_sign", "clinical"),
        _event_index_projection(_clinical_score_canonical(), "clinical_score",
                                "journey_clinical.clinical_score", "clinical"),
        _event_index_projection(_medication_admin_canonical(), "medication_admin",
                                "journey_clinical.medication_admin", "clinical"),
        _event_index_projection(_medication_order_canonical(), "medication_order",
                                "journey_clinical.medication_order", "clinical"),
        _event_index_projection(_medication_dispense_canonical(), "medication_dispense",
                                "journey_clinical.medication_dispense", "clinical"),
        _event_index_projection(_coded_finding_typed(), "clinical_finding",
                                "journey_clinical.clinical_finding", "clinical"),
        _event_index_projection(_nomen_finding_typed(), "clinical_finding",
                                "journey_clinical.clinical_finding", "clinical"),
        _event_index_projection(_date_finding_typed(), "clinical_finding",
                                "journey_clinical.clinical_finding", "clinical"),
        _event_index_projection(_text_finding_typed(), "clinical_finding",
                                "journey_clinical.clinical_finding", "clinical"),
        _event_index_projection(_imaging_exam_canonical(), "imaging_exam",
                                "journey_clinical.imaging_exam", "clinical"),
        _event_index_projection(_artifact_asset_canonical(), "artifact",
                                "journey_artifact.asset", "clinical"),
        _event_index_projection(_document_canonical(), "document",
                                "journey_text.document", "clinical"),
        _event_index_projection(_appointment_canonical(), "appointment",
                                "journey_clinical.appointment", "administrative"),
        _event_index_projection(_referral_canonical(), "referral",
                                "journey_clinical.referral", "administrative"),
        _event_index_projection(_rtt_pathway_canonical(), "rtt_pathway",
                                "journey_clinical.rtt_pathway", "administrative"),
        _event_index_projection(_rtt_activity_canonical(), "rtt_activity",
                                "journey_clinical.rtt_activity", "administrative"),
        _event_index_projection(_waiting_list_index_representative(), "waiting_list_entry",
                                "journey_clinical.waiting_list_entry", "administrative"),
        _event_index_projection(_community_care_activity_canonical(), "community_care_activity",
                                "journey_clinical.community_care_activity", "clinical"),
        _event_index_projection(_community_care_contact_canonical(), "community_care_contact",
                                "journey_clinical.community_care_contact", "administrative"),
        _event_index_projection(_hrg_grouping_canonical(), "hrg_grouping",
                                "journey_clinical.hrg_grouping", "administrative"),
        _event_index_projection(_costed_activity_canonical(), "costed_activity",
                                "journey_clinical.costed_activity", "administrative"),
        _event_index_projection(_drug_expenditure_canonical(), "drug_expenditure",
                                "journey_clinical.drug_expenditure", "administrative"),
        _event_index_projection(_medication_supply_canonical(), "medication_supply",
                                "journey_clinical.medication_supply", "clinical"),
        _event_index_projection(_elective_access_entry_canonical(), "elective_access_entry",
                                "journey_clinical.elective_access_entry", "administrative"),
        _event_index_projection(_pathway_tracking_canonical(), "pathway_tracking",
                                "journey_clinical.pathway_tracking", "administrative"),
        _event_index_projection(_critical_care_period_canonical(), "critical_care_period",
                                "journey_clinical.critical_care_period", "administrative"),
        _event_index_projection(_critical_care_activity_canonical(), "critical_care_activity",
                                "journey_clinical.critical_care_activity", "clinical"),
        _event_index_projection(_cc_procedure_canonical(), "procedure",
                                "journey_clinical.procedure", "clinical"),
        _event_index_projection(_critical_care_admission_canonical(), "critical_care_admission",
                                "journey_clinical.critical_care_admission", "administrative"),
        _event_index_projection(_cc_daily_score_canonical(), "critical_care_daily_score",
                                "journey_clinical.critical_care_daily_score", "clinical"),
        _event_index_projection(_neonatal_episode_canonical(), "neonatal_episode",
                                "journey_clinical.neonatal_episode", "administrative"),
        _event_index_projection(_neonatal_care_day_canonical(), "neonatal_care_day",
                                "journey_clinical.neonatal_care_day", "clinical"),
        _event_index_projection(_neonatal_examination_canonical(), "neonatal_examination",
                                "journey_clinical.neonatal_examination", "clinical"),
        _event_index_projection(_baby_delivery_canonical(), "baby_delivery",
                                "journey_clinical.baby_delivery", "clinical"),
        _event_index_projection(_labour_delivery_canonical(), "labour_delivery",
                                "journey_clinical.labour_delivery", "clinical"),
        _event_index_projection(_maternity_care_contact_canonical(), "maternity_care_contact",
                                "journey_clinical.maternity_care_contact", "administrative"),
        _event_index_projection(_maternity_diagnosis_canonical(), "condition",
                                "journey_clinical.condition", "clinical"),
        _event_index_projection(_research_enrollment_canonical(), "research_enrollment",
                                "journey_clinical.research_enrollment", "administrative"),
        _event_index_projection(_mill_radiology_exam_canonical(), "imaging_exam",
                                "journey_clinical.imaging_exam", "clinical"),
        _event_index_projection(spark.read.table(INTERNAL_SCHEMA + "._clinical_safety_incident_index_lane"), "safety_incident", "journey_clinical.safety_incident", "clinical"),
    ]
    out = lanes[0]
    for lane in lanes[1:]:
        out = out.unionByName(lane)
    return out.unionByName(_s3b_event_index_supplement())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Iweb target system

# COMMAND ----------

def _iweb_target_system(vocabulary):
    # Map an iWeb vocabulary label to its coding-system URI, preserving unknown vocabularies in
    # an iWeb namespace.
    return (
        F.when(vocabulary == "SNOMED", F.lit("http://snomed.info/sct"))
        .when(vocabulary == "OPCS4", F.lit("http://fhir.hl7.org.uk/CodeSystem/OPCS-4"))
        .when(vocabulary == "ICD10", F.lit("http://hl7.org/fhir/sid/icd-10"))
        .when(vocabulary == "LOINC", F.lit("http://loinc.org"))
        .otherwise(F.concat(F.lit("urn:iweb:"),
                            F.lower(F.coalesce(vocabulary, F.lit("unmapped")))))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Events /  event map

# COMMAND ----------

SRC_CODED_EVENT_FINDING_MAPPING = "4_prod.bronze.map_coded_events_omop_bridge"

SRC_IWEB_MULTISELECT = "4_prod.bronze.iweb_multiselect_value"

S3_EVENT_AXIS_SOURCES = [
    ("journey_clinical.presenting_complaint", "complaint"),
    ("journey_clinical.condition", "condition"),
    ("journey_clinical.family_history", "condition"),
    ("journey_clinical.allergy_intolerance", "substance"),
    ("journey_clinical.cancer_treatment", "drug"),
    ("journey_clinical.clinical_finding", "finding"),
    ("journey_clinical.clinical_finding", "value"),
    ("journey_clinical.clinical_score", "score"),
    ("journey_clinical.condition_stage", "stage_diagnosis"),
    ("journey_clinical.condition_stage", "stage_group"),
    ("journey_clinical.device", "device"),
    ("journey_clinical.endoscopy_finding", "finding"),
    ("journey_clinical.imaging_exam", "exam"),
    ("journey_clinical.medication_admin", "medication"),
    ("journey_clinical.medication_dispense", "medication"),
    ("journey_clinical.medication_order", "medication"),
    ("journey_clinical.pathology_result", "test"),
    ("journey_clinical.pathology_result", "result"),
    ("journey_clinical.procedure", "procedure"),
    ("journey_clinical.procedure", "device"),
    ("journey_clinical.specimen", "specimen_type"),
    ("journey_clinical.vital_sign", "vital"),
    ("journey_clinical.vital_sign", "unit"),
    ("journey_clinical.vital_sign", "method"),
    ("journey_clinical.vital_sign", "body_site"),
    ("journey_clinical.vital_sign", "interpretation"),
    ("journey_clinical.clinical_score_component", "component"),
    ("journey_clinical.form_response", "question"),
    ("journey_clinical.form_response", "answer"),
    ("journey_clinical.form_response", "unit"),
    ("journey_clinical.form_smoking_status", "smoking_status"),
    ("journey_clinical.form_smoking_status", "cessation_advice"),
    ("journey_clinical.form_smoking_status", "cessation_referral"),
    ("journey_clinical.form_smoking_status", "nicotine_treatment"),
    ("journey_clinical.form_smoking_status", "dependence_level"),
]

def _s3_axis_event_map():
    """Carry every selected flat SNOMED target into the one-to-many event child."""
    frames = []
    for relation, axis in S3_EVENT_AXIS_SOURCES:
        source = spark.read.table(_n(relation))
        code = F.col(f"{axis}_snomed_code")
        method = F.col(f"{axis}_map_method")
        rule = F.col(f"{axis}_map_rule_id")
        version = F.col(f"{axis}_map_version")
        confidence = F.col(f"{axis}_map_confidence")
        candidates = F.col(f"{axis}_map_candidate_count")
        cosine = F.col(f"{axis}_map_cosine")
        scoring_model = F.col(f"{axis}_map_scoring_model")
        status = F.col(f"{axis}_map_status")
        competing = F.col(f"{axis}_map_competing_count")
        candidate_set = F.col(f"{axis}_map_candidate_set_id")
        component_index = F.col(f"{axis}_map_component_index")
        component_count = F.col(f"{axis}_map_component_count")
        rollup_levels = F.col(f"{axis}_map_rollup_levels")
        provenance = F.concat(
            F.lit("s3_axis:"),
            F.to_json(F.struct(method.alias("method"), confidence.alias("confidence"),
                               cosine.alias("cosine"), scoring_model.alias("scoring_model"),
                               status.alias("status"), rule.alias("rule"),
                               candidates.alias("candidate_count"), competing.alias("competing_count"),
                               candidate_set.alias("candidate_set_id"), component_index.alias("component_index"),
                               component_count.alias("component_count"), rollup_levels.alias("rollup_levels"))),
        )
        frames.append(source.where(code.isNotNull()).select(
            stable_id("patient_event_map:s3_axis", F.col("patient_event_key"), F.lit(axis), code, rule).alias("patient_event_map_id"),
            "patient_event_key", F.lit(SNOMED_URI).alias("mapped_coding_system"),
            code.cast("string").alias("mapped_code"), F.col(f"{axis}_snomed_display").alias("mapped_display"),
            F.lit(axis).alias("target_domain"), provenance.alias("map_source"),
            version.cast("string").alias("map_version"), F.lit(None).cast("timestamp").alias("valid_from"),
            F.lit(None).cast("timestamp").alias("valid_to"), F.col("loaded_at"),
        ))
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame)
    return out.dropDuplicates(["patient_event_map_id"])

# contract v2: carry patient_event_key through every internal coding-map projection
def _map_projection(df, code_col, display_col, system, target_domain, map_source):
    # Create patient-event mapping rows for non-null mapped codes in a fixed target domain.
    filtered = df.where(F.col(code_col).isNotNull())
    return filtered.select(
        stable_id(
            "patient_event_map",
            F.col("patient_event_key"), F.lit(system), F.col(code_col),
            F.lit(target_domain), F.lit(map_source), F.lit(None),
        ).alias("patient_event_map_id"),
        "patient_event_key",
        F.lit(system).alias("mapped_coding_system"),
        F.col(code_col).cast("string").alias("mapped_code"),
        F.col(display_col).alias("mapped_display"),
        F.lit(target_domain).alias("target_domain"),
        F.lit(map_source).alias("map_source"),
        F.lit(None).cast("string").alias("map_version"),
        F.lit(None).cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        "loaded_at",
    )

def _transfusion_blood_group_event_map():
    """Publish unit mapping plus a distinct patient mapping at event-map grain."""
    transfusion = _transfusion_canonical()
    unit = _map_projection(
        transfusion, "unit_group_concept_id", "_unit_group_concept_name",
        "http://snomed.info/sct", "observation",
        "lookup.bloodtrack_blood_group_map:applied",
    )
    distinct_patient = transfusion.where(
        F.col("patient_group_concept_id").isNotNull()
        & ~F.col("patient_group_concept_id").eqNullSafe(F.col("unit_group_concept_id"))
    )
    patient = _map_projection(
        distinct_patient, "patient_group_concept_id", "_patient_group_concept_name",
        "http://snomed.info/sct", "observation",
        "lookup.bloodtrack_blood_group_map:applied",
    )
    return unit.unionByName(patient)

@materialized_view(
    name=_n("journey_events._event_map"),
    private=True,
    comment="INTERNAL mapping-lane union (one row per event x mapping) behind patient_event.",
    refresh_policy="incremental",
)
def _event_map():
    # Build the declared dataset: INTERNAL mapping-lane union (one row per event x mapping)
    # behind patient_event.
    family = _family_history_canonical()
    base = (
        _map_projection(family, "_snomed_code", "_snomed_display",
                        "http://snomed.info/sct", "family_history", "bronze.map_family_history")
        .unionByName(
            _map_projection(family, "_icd10_code", "_icd10_display",
                            "http://hl7.org/fhir/sid/icd-10", "family_history", "bronze.map_family_history")
        )
        .unionByName(
            _map_projection(family, "_omop_code", "_omop_display",
                            "urn:omop:concept_id", "family_history", "bronze.map_family_history")
        )
        .unionByName(
            _map_projection(_medication_dispense_canonical(), "_dmd_code", "_dmd_display",
                            "http://snomed.info/sct", "drug", "bronze.map_pharmacy_issue")
        )
        .unionByName(
            _map_projection(_condition_diagnosis_canonical(), "_snomed_code", "_snomed_display",
                            "http://snomed.info/sct", "condition", "bronze.map_diagnosis")
        )
        .unionByName(
            _map_projection(_condition_diagnosis_canonical(), "_icd10_code", "_icd10_display",
                            "http://hl7.org/fhir/sid/icd-10", "condition", "bronze.map_diagnosis")
        )
        .unionByName(
            _map_projection(_condition_diagnosis_canonical(), "_omop_code", "_omop_display",
                            "urn:omop:concept_id", "condition", "bronze.map_diagnosis")
        )
        .unionByName(
            _map_projection(_condition_problem_canonical(), "_snomed_code", "_snomed_display",
                            "http://snomed.info/sct", "condition", "bronze.map_problem")
        )
        .unionByName(
            _map_projection(_condition_problem_canonical(), "_icd10_code", "_icd10_display",
                            "http://hl7.org/fhir/sid/icd-10", "condition", "bronze.map_problem")
        )
        .unionByName(
            _map_projection(_condition_problem_canonical(), "_omop_code", "_omop_display",
                            "urn:omop:concept_id", "condition", "bronze.map_problem")
        )
        .unionByName(
            _map_projection(_procedure_canonical(), "_snomed_code", "_snomed_display",
                            "http://snomed.info/sct", "procedure", "bronze.map_procedure")
        )
        .unionByName(
            _map_projection(_procedure_canonical(), "_opcs4_code", "_opcs4_display",
                            "http://fhir.hl7.org.uk/CodeSystem/OPCS-4", "procedure", "bronze.map_procedure")
        )
        .unionByName(
            _map_projection(_cc_procedure_canonical(), "_opcs4_code", "_opcs4_display",
                            "http://fhir.hl7.org.uk/CodeSystem/OPCS-4", "procedure",
                            "bronze.map_critical_care_procedure")
        )
        .unionByName(
            _map_projection(_maternity_diagnosis_canonical(), "_snomed_code", "_snomed_display",
                            "http://snomed.info/sct", "condition",
                            "bronze.map_maternity_diagnosis")
        )
        .unionByName(
            _map_projection(_procedure_canonical(), "_omop_code", "_omop_display",
                            "urn:omop:concept_id", "procedure", "bronze.map_procedure")
        )
        .unionByName(
            _map_projection(_implant_procedure_canonical(), "_device_snomed_code", "_device_snomed_display",
                            "http://snomed.info/sct", "device", "bronze.map_implant_details")
        )
        .unionByName(
            _map_projection(_pathology_requested_test_canonical(), "_test_snomed_code", "_test_snomed_display",
                            "http://snomed.info/sct", "measurement", "bronze.map_pathology_requested_test")
        )
        .unionByName(
            _map_projection(_pathology_requested_test_canonical(), "_test_omop_code", "_test_omop_display",
                            "urn:omop:concept_id", "measurement", "bronze.map_pathology_requested_test")
        )
        .unionByName(
            _map_projection(_genomic_test_canonical(), "_gt_snomed_code", "_gt_snomed_display",
                            "http://snomed.info/sct", "measurement", "bronze.map_pathology_genetic_test")
        )
        .unionByName(
            _map_projection(_genomic_test_canonical(), "_gt_loinc_code", "_gt_loinc_display",
                            "http://loinc.org", "measurement", "bronze.map_pathology_genetic_test")
        )
        .unionByName(
            _map_projection(_genomic_test_canonical(), "_gt_omop_code", "_gt_omop_display",
                            "urn:omop:concept_id", "measurement", "bronze.map_pathology_genetic_test")
        )
        .unionByName(
            _map_projection(_genomic_result_canonical(), "_gr_snomed_code", "_gr_snomed_display",
                            "http://snomed.info/sct", "measurement", "bronze.map_pathology_genetic_result")
        )
        .unionByName(
            _map_projection(_genomic_result_canonical(), "_gr_clinvar_code", "_gr_clinvar_display",
                            "urn:clinvar", "measurement", "bronze.map_pathology_genetic_result:clinvar")
        )
        .unionByName(
            _map_projection(_genomic_result_canonical(), "_gr_omop_code", "_gr_omop_display",
                            "urn:omop:concept_id", "measurement", "bronze.map_pathology_genetic_result:omop-genomic")
        )
        .unionByName(
            _map_projection(_indication_canonical(), "_ind_snomed_code", "_ind_snomed_display",
                            "http://snomed.info/sct", "condition", "bronze.map_pathology_indication")
        )
        .unionByName(
            _map_projection(_indication_canonical(), "_ind_omop_code", "_ind_omop_display",
                            "urn:omop:concept_id", "condition", "bronze.map_pathology_indication")
        )
        .unionByName(
            _map_projection(_micro_isolate_canonical(), "_iso_snomed_code", "_iso_snomed_display",
                            "http://snomed.info/sct", "measurement_value", "bronze.map_pathology_microbiology_isolate")
        )
        .unionByName(
            _map_projection(_micro_isolate_canonical(), "_iso_omop_code", "_iso_omop_display",
                            "urn:omop:concept_id", "measurement_value", "bronze.map_pathology_microbiology_isolate")
        )
        .unionByName(
            _map_projection(_susceptibility_canonical(), "_sus_code_code", "_sus_code_display",
                            "urn:barts:pathology:antimicrobial", "drug", "bronze.map_pathology_antimicrobial_susceptibility")
        )
        .unionByName(
            _map_projection(_susceptibility_canonical(), "_sus_omop_code", "_sus_omop_display",
                            "urn:omop:concept_id", "drug", "bronze.map_pathology_antimicrobial_susceptibility")
        )
        .unionByName(
            _map_projection(_pathology_result_canonical(), "_test_snomed_code", "_test_snomed_display",
                            "http://snomed.info/sct", "measurement", "bronze.map_pathology")
        )
        .unionByName(
            _map_projection(_pathology_result_canonical(), "_test_loinc_code", "_test_loinc_display",
                            "http://loinc.org", "measurement", "bronze.map_pathology")
        )
        .unionByName(
            _map_projection(_pathology_result_canonical(), "_test_omop_code", "_test_omop_display",
                            "urn:omop:concept_id", "measurement", "bronze.map_pathology")
        )
        .unionByName(
            _map_projection(_pathology_result_canonical(), "_result_snomed_code", "_result_snomed_display",
                            "http://snomed.info/sct", "measurement_value", "bronze.map_pathology")
        )
        .unionByName(
            _map_projection(_pathology_result_canonical(), "_result_loinc_code", "_result_loinc_display",
                            "http://loinc.org", "measurement_value", "bronze.map_pathology")
        )
        .unionByName(
            _map_projection(_pathology_result_canonical(), "_result_omop_code", "_result_omop_display",
                            "urn:omop:concept_id", "measurement_value", "bronze.map_pathology")
        )
        .unionByName(
            _map_projection(_vital_sign_canonical(), "_omop_code", "_omop_display",
                            "urn:omop:concept_id", "measurement", "bronze.map_numeric_events_or_form")
        )
        .unionByName(
            _map_projection(_clinical_score_canonical(), "_omop_code", "_omop_display",
                            "urn:omop:concept_id", "observation", "bronze.map_numeric_events_or_form")
        )
        .unionByName(
            _map_projection(_medication_admin_canonical(), "_snomed_code", "_snomed_display",
                            "http://snomed.info/sct", "drug", "bronze.map_med_admin")
        )
        .unionByName(
            _map_projection(_medication_admin_canonical(), "_omop_code", "_omop_display",
                            "urn:omop:concept_id", "drug", "bronze.map_med_admin")
        )
        .unionByName(
            _map_projection(_medication_admin_canonical(), "_rxnorm_code", "_rxnorm_display",
                            "http://www.nlm.nih.gov/research/umls/rxnorm", "drug",
                            "bronze.map_med_admin")
        )
        .unionByName(_coded_bridge_event_map())
        .unionByName(
            _dynamic_map_projection(
                _coded_finding_typed(), "_omop_code", "_omop_display",
                "urn:omop:concept_id", "_omop_domain", "bronze.map_coded_events:manual",
            )
        )
        .unionByName(
            _dynamic_map_projection(
                _nomen_finding_typed(), "_omop_code", "_omop_display",
                "urn:omop:concept_id", "_omop_domain", "bronze.map_nomen_events",
            )
        )
        .unionByName(
            _map_projection(
                _nomen_finding_typed(), "_snomed_code", "_snomed_display",
                "http://snomed.info/sct", "observation", "bronze.map_nomen_events",
            )
        )
        .unionByName(
            _dynamic_map_projection(
                _text_finding_typed(), "_omop_code", "_omop_display",
                "urn:omop:concept_id", "_omop_domain", "bronze.map_text_events:concept",
            )
        )
        .unionByName(
            _dynamic_map_projection(
                _text_finding_typed(), "_omop_value_code", "_omop_value_display",
                "urn:omop:concept_id", "_omop_value_domain", "bronze.map_text_events:value",
            )
        )
        .unionByName(_luna_code_event_map(
            _referral_canonical(), "treatment_function_code",
            "urn:barts:luna:treatment-function"))
        .unionByName(_luna_code_event_map(
            _rtt_pathway_canonical(), "treatment_function_code",
            "urn:barts:luna:treatment-function"))
        .unionByName(_luna_code_event_map(
            _rtt_activity_canonical(), "treatment_function_code",
            "urn:barts:luna:treatment-function"))
        .unionByName(_luna_code_event_map(
            _rtt_pathway_canonical(), "current_status_code",
            "urn:barts:luna:rtt-status"))
        .unionByName(_luna_code_event_map(
            _rtt_activity_canonical(), "status_code",
            "urn:barts:luna:rtt-status"))
        .unionByName(_seed_code_event_map(
            _waiting_list_index_representative(), "status_code",
            "urn:cerner:code_value", "wl_status_cdf_meaning"))
        .unionByName(
            _map_projection(_allergy_canonical(), "_snomed_code", "_snomed_display",
                            "http://snomed.info/sct", "condition", "bronze.map_allergy")
        )
        .unionByName(
            _map_projection(_transfusion_canonical(), "product_concept_id", "product_concept_name",
                            "http://snomed.info/sct", "device",
                            "lookup.bloodtrack_product_group_map:applied")
        )
        .unionByName(
            _map_projection(_transfusion_canonical(), "_product_proc_concept_id",
                            "_product_proc_concept_name", "http://snomed.info/sct", "procedure",
                            "lookup.bloodtrack_product_group_map:applied")
        )
        .unionByName(_transfusion_blood_group_event_map())
        .unionByName(
            _map_projection(_cancer_treatment_canonical(), "_drug_omop_code", "_drug_omop_display",
                            "urn:omop:concept_id", "drug",
                            "lookup.cancer_treatment_term_map:applied")
        )
        .unionByName(
            _map_projection(_cancer_treatment_canonical(), "_procurement_omop_code",
                            "_procurement_omop_display", "urn:omop:concept_id", "procedure",
                            "bronze.map_cancer_treatment:procurement")
        )
        .unionByName(
            _map_projection(_cancer_treatment_canonical(), "_procurement_opcs4_code",
                            "_procurement_opcs4_display",
                            "http://fhir.hl7.org.uk/CodeSystem/OPCS-4", "procedure",
                            "bronze.map_cancer_treatment:procurement")
        )
        .unionByName(
            _map_projection(_cancer_treatment_canonical(), "_delivery_omop_code",
                            "_delivery_omop_display", "urn:omop:concept_id", "procedure",
                            "bronze.map_cancer_treatment:delivery")
        )
        .unionByName(
            _map_projection(_cancer_treatment_canonical(), "_delivery_opcs4_code",
                            "_delivery_opcs4_display",
                            "http://fhir.hl7.org.uk/CodeSystem/OPCS-4", "procedure",
                            "bronze.map_cancer_treatment:delivery")
        )
        .unionByName(
            _map_projection(_condition_stage_canonical(), "_omop_code", "_omop_display",
                            "urn:omop:concept_id", "condition",
                            "bronze.map_aria_diagnosis_staging")
        )
        .unionByName(
            _map_projection(_endoscopy_finding_canonical(), "_snomed_code", "_snomed_display",
                            "http://snomed.info/sct", "observation",
                            "bronze.map_endobase_exam_term:c4")
        )
        .unionByName(
            _map_projection(_endoscopy_finding_canonical(), "_omop_code", "_omop_display",
                            "urn:omop:concept_id", "observation",
                            "bronze.map_endobase_exam_term:c4")
        )
        .unionByName(
            _map_projection(_device_canonical(), "_snomed_code", "_snomed_display",
                            "http://snomed.info/sct", "device",
                            "lookup.mediconnect_device_type_map:applied")
        )
        .unionByName(
            _map_projection(_medication_supply_canonical(), "_dmd_code", "_dmd_display",
                            "http://snomed.info/sct", "drug", "bronze.map_homecare_request")
        )
        .unionByName(
            _map_projection(_drug_expenditure_canonical(), "_dmd_code", "_dmd_display",
                            "http://snomed.info/sct", "drug",
                            "bronze.map_finance_hcd_expenditure")
        )
        .unionByName(
            _map_projection(_drug_expenditure_canonical(), "_omop_code", "_omop_display",
                            "urn:omop:concept_id", "drug",
                            "bronze.map_finance_hcd_expenditure")
        )
        .unionByName(
            _map_projection(_community_care_activity_canonical(),
                            "_snomed_candidate_code", "_snomed_candidate_name",
                            "http://snomed.info/sct", "community_activity",
                            "bronze.map_community_care_activity")
        )
        .unionByName(
            _map_projection(_community_care_activity_canonical(),
                            "_snomed_candidate_omop_code", "_snomed_candidate_omop_display",
                            "urn:omop:concept_id", "community_activity",
                            "bronze.map_community_care_activity")
        )
        .unionByName(
            _map_projection(_elective_access_entry_canonical(), "_opcs4_code", "_opcs4_display",
                            "http://fhir.hl7.org.uk/CodeSystem/OPCS-4", "procedure",
                            "bronze.map_elective_access_list_procedure")
        )
        .unionByName(_iweb_multiselect_event_map())
    )
    out = base.unionByName(_s3_axis_event_map())
    s3_json = F.regexp_replace(F.col("map_source"), r"^s3_axis:", "")
    is_s3 = F.col("map_source").startswith("s3_axis:")
    return (out.withColumn("map_method", F.when(is_s3, F.get_json_object(s3_json, "$.method")))
               .withColumn("map_confidence", F.when(is_s3, F.expr("try_cast(get_json_object(regexp_replace(map_source, '^s3_axis:', ''), '$.confidence') as double)")))
               .withColumn("map_rule_id", F.when(is_s3, F.get_json_object(s3_json, "$.rule")))
               .withColumn("map_candidate_count", F.when(is_s3, F.expr("try_cast(get_json_object(regexp_replace(map_source, '^s3_axis:', ''), '$.candidate_count') as int)"))))

def _dynamic_map_projection(df, code_col, display_col, system, domain_col, map_source):
    # Create patient-event mapping rows using each row's mapped target domain, defaulting to
    # observation.
    filtered = df.where(F.col(code_col).isNotNull())
    domain = F.lower(F.coalesce(F.col(domain_col), F.lit("observation")))
    return filtered.select(
        stable_id(
            "patient_event_map", F.col("patient_event_key"), F.lit(system), F.col(code_col),
            domain, F.lit(map_source), F.lit(None),
        ).alias("patient_event_map_id"),
        "patient_event_key", F.lit(system).alias("mapped_coding_system"),
        F.col(code_col).cast("string").alias("mapped_code"),
        F.col(display_col).alias("mapped_display"), domain.alias("target_domain"),
        F.lit(map_source).alias("map_source"), F.lit(None).cast("string").alias("map_version"),
        F.lit(None).cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"), "loaded_at",
    )

def _luna_code_event_map(fact_df, code_col, source_system_urn):
    # Join LUNA facts to source-system-specific terminology mappings and retain the mapping rule
    # evidence.
    m = (
        read_source(SRC_LUNA_CODE_MAPPING)
        .where(F.col("source_coding_system") == F.lit(source_system_urn))
        .select(
            F.col("source_code").alias("_m_source_code"),
            F.col("target_coding_system").alias("_m_target_system"),
            F.col("target_code").alias("_m_target_code"),
            F.col("target_display").alias("_m_target_display"),
            F.col("target_domain").alias("_m_target_domain"),
            F.col("mapping_rule_id").alias("_m_rule"),
        )
    )
    facts = fact_df.where(F.col(code_col).isNotNull()).select(
        "patient_event_key", F.col(code_col).alias("_f_code"),
        "loaded_at",
    )
    joined = facts.join(m, facts["_f_code"] == m["_m_source_code"], "inner")
    map_source = F.concat(F.lit("lookup.luna_code_map:"), F.col("_m_rule"))
    return joined.select(
        stable_id(
            "patient_event_map", F.col("patient_event_key"), F.col("_m_target_system"),
            F.col("_m_target_code"), F.col("_m_target_domain"), map_source,
            F.lit(None),
        ).alias("patient_event_map_id"),
        "patient_event_key",
        F.col("_m_target_system").alias("mapped_coding_system"),
        F.col("_m_target_code").cast("string").alias("mapped_code"),
        F.col("_m_target_display").alias("mapped_display"),
        F.col("_m_target_domain").alias("target_domain"),
        map_source.alias("map_source"),
        F.lit(None).cast("string").alias("map_version"),
        F.lit(None).cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        "loaded_at",
    )

def _seed_code_event_map(fact_df, code_col, source_system_urn, rule_id):
    # Join facts to seed mappings restricted to the requested source coding system and mapping
    # rule.
    m = (
        read_source(SRC_SEED_CODE_MAPPING)
        .where(
            (F.col("source_coding_system") == F.lit(source_system_urn))
            & (F.col("mapping_rule_id") == F.lit(rule_id))
        )
        .select(
            F.col("source_code").alias("_m_source_code"),
            F.col("target_coding_system").alias("_m_target_system"),
            F.col("target_code").alias("_m_target_code"),
            F.col("target_display").alias("_m_target_display"),
            F.col("target_domain").alias("_m_target_domain"),
            F.col("mapping_rule_id").alias("_m_rule"),
        )
    )
    facts = fact_df.where(F.col(code_col).isNotNull()).select(
        "patient_event_key", F.col(code_col).alias("_f_code"),
        "loaded_at",
    )
    joined = facts.join(m, facts["_f_code"] == m["_m_source_code"], "inner")
    map_source = F.concat(F.lit("lookup.seed_code_map:"), F.col("_m_rule"))
    return joined.select(
        stable_id(
            "patient_event_map", F.col("patient_event_key"), F.col("_m_target_system"),
            F.col("_m_target_code"), F.col("_m_target_domain"), map_source,
            F.lit(None),
        ).alias("patient_event_map_id"),
        "patient_event_key",
        F.col("_m_target_system").alias("mapped_coding_system"),
        F.col("_m_target_code").cast("string").alias("mapped_code"),
        F.col("_m_target_display").alias("mapped_display"),
        F.col("_m_target_domain").alias("target_domain"),
        map_source.alias("map_source"),
        F.lit(None).cast("string").alias("map_version"),
        F.lit(None).cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        "loaded_at",
    )

def _coded_bridge_event_map():
    # Project the coded-event finding bridge into patient-event mapping rows with its target
    # domain.
    b = read_source(SRC_CODED_EVENT_FINDING_MAPPING).alias("b")
    event_id = stable_id("coded_event:mill", b.SOURCE_ROW_KEY)
    domain = F.lower(F.coalesce(b.OMOP_CONCEPT_DOMAIN, F.lit("observation")))
    return b.where(b.OMOP_CONCEPT_ID.isNotNull()).select(
        stable_id(
            "patient_event_map:coded_bridge", event_id, b.OMOP_CONCEPT_ID,
            b.MAPPING_RULE_ID, b.MAPPING_RANK, b.SOURCE_FIELD, F.lit(None),
        ).alias("patient_event_map_id"),
        event_id.alias("patient_event_key"),
        F.lit("urn:omop:concept_id").alias("mapped_coding_system"),
        b.OMOP_CONCEPT_ID.cast("string").alias("mapped_code"),
        b.OMOP_CONCEPT_NAME.alias("mapped_display"), domain.alias("target_domain"),
        F.lit("bronze.map_coded_events_omop_bridge").alias("map_source"),
        F.lit(None).cast("string").alias("map_version"),
        b.OMOP_VALID_START_DATE.cast("timestamp").alias("valid_from"),
        b.OMOP_VALID_END_DATE.cast("timestamp").alias("valid_to"),
        F.coalesce(b.PIPELINE_UPDT_DT_TM, b.MAPPING_ADC_UPDT).alias("loaded_at"),
    )

def _iweb_multiselect_event_map():
    # Expand mapped iWeb multiselect answers into patient-event mapping rows while retaining
    # vocabulary and rule evidence.
    v = read_source(SRC_IWEB_VALUE_MAP).where(
        F.col("TARGET_CODE").isNotNull() | F.col("TARGET_CONCEPT_ID").isNotNull()
    ).select(
        F.col("SOURCE_TABLE").alias("_v_table"),
        F.col("FIELD_NAME").alias("_v_field"),
        F.col("CODE").alias("_v_code"),
        F.col("TARGET_VOCABULARY").alias("_v_vocabulary"),
        F.col("TARGET_CODE").alias("_v_target_code"),
        F.col("TARGET_CONCEPT_ID").alias("_v_target_concept_id"),
        F.col("TARGET_CONCEPT_NAME").alias("_v_target_name"),
        F.col("MAPPING_STATUS").alias("_v_mapping_status"),
    )
    m = read_source(SRC_IWEB_MULTISELECT).join(
        v,
        (F.col("SOURCE_TABLE") == F.col("_v_table"))
        & (F.col("FIELD_NAME") == F.col("_v_field"))
        & (F.col("CODE") == F.col("_v_code")),
        "inner",
    )
    family = (
        F.when(F.col("SOURCE_TABLE") == "reg_coronary_subprocedure", F.lit("coronary_lesion"))
        .when(F.col("SOURCE_TABLE").isin("reg_cs2010g_pre1", "reg_cs2010g_pre2",
                                         "reg_cs2010g_post1", "reg_cs2010g_post2"),
              F.lit("surgery_episode"))
        .when(F.col("SOURCE_TABLE") == "reg_cs2010g_subprocedure", F.lit("surgery_procedure"))
        .when(F.col("SOURCE_TABLE") == "reg_cs2010g_followup", F.lit("surgery_followup"))
        .when(F.col("SOURCE_TABLE") == "reg_dghminap", F.lit("acs_transfer"))
        .when(F.col("SOURCE_TABLE") == "reg_eracs", F.lit("eracs_episode"))
        .when(F.col("SOURCE_TABLE") == "reg_mort", F.lit("mortality_review"))
        .when(F.col("SOURCE_TABLE") == "reg_noncoronary", F.lit("noncoronary_procedure"))
        .when(F.col("SOURCE_TABLE").isin("reg_mdt", "reg_ctmdt"), F.lit("cardiac_mdt"))
    )
    registry_type = (
        F.when(F.col("SOURCE_TABLE") == "reg_mdt", F.lit("CORONARY_REVASC"))
        .when(F.col("SOURCE_TABLE") == "reg_ctmdt", F.lit("CT_AORTIC"))
        .otherwise(F.lit("~"))
    )
    m = m.withColumn("_family", family).withColumn("_registry_type", registry_type) \
        .where(F.col("_family").isNotNull())
    target_system = _iweb_target_system(F.col("_v_vocabulary"))
    target_code = F.coalesce(F.col("_v_target_code"),
                             F.col("_v_target_concept_id").cast("string"))
    parent_event_id = stable_id("registry_entry:iweb", F.col("_family"),
                                F.col("_registry_type"), F.col("ENTRY_ID"))
    map_source = F.concat(F.lit("lookup.iweb_value_to_concept:"),
                          F.coalesce(F.col("_v_mapping_status"), F.lit("UNKNOWN")))
    return m.select(
        stable_id("patient_event_map", parent_event_id, target_system, target_code,
                  F.lit("registry"), map_source, F.lit(None))
        .alias("patient_event_map_id"),
        parent_event_id.alias("patient_event_key"), target_system.alias("mapped_coding_system"),
        target_code.alias("mapped_code"), F.col("_v_target_name").alias("mapped_display"),
        F.lit("registry").alias("target_domain"), map_source.alias("map_source"),
        F.lit(None).cast("string").alias("map_version"),
        F.lit(None).cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.col("ADC_UPDT").alias("loaded_at"),
    ).dropDuplicates(["patient_event_map_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Events / patient event

# COMMAND ----------

PATIENT_EVENT_COLUMN_COMMENTS = {
    "patient_event_row_key": "Deterministic row key (the mapping id for mapped rows; a namespaced unmapped id otherwise).",
    "patient_event_key": "Immutable deterministic fact-grain event key shared across a mapped event's N rows.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system behind subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT when available.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Clinical or administrative event time.",
    "event_end_datetime": "Event end when the source supplies one.",
    "event_type": "Fact kind (condition",
    "fact_category": "Clinical versus administrative fact.",
    "fact_table": "Typed fact table holding the event's values.",
    "fact_row_key": "Deterministic row key inside fact_table.",
    "source_system": "Plain-language source system (Millennium",
    "source_object": "Native source entity in plain language",
    "source_row_key": "Native source record key (BIGINT ids rendered as text); the typed native id lives on the fact table.",
    "source_coding_system": "The description for the code value",
    "source_code": "Source code as recorded.",
    "source_display": "Source display as recorded.",
    "confidentiality_code": "Source confidentiality label.",
    "vip_ind": "Source VIP indicator carried as data.",
    "withheld_identity_ind": "Source withheld-identity indicator.",
    "mapped_coding_system": "Standard coding system for this mapping row; null on the unmapped row.",
    "mapped_code": "Unique valid standard OMOP concept reached from exact ICD code semantics; NULL when absent or ambiguous.",
    "mapped_display": "Standard display or definition.",
    "target_domain": "Downstream routing domain for this mapping.",
    "map_source": "Governed mapping product that produced the row.",
    "map_version": "Mapping content version.",
    "map_method": "How the mapping was reached: source_native or rule for legacy lanes; S3 lookup lanes retain their explicit method.",
    "map_confidence": "Mapping confidence in [0,1] where the lane measured one.",
    "map_rule_id": "Rule or lane identifier that produced this candidate.",
    "map_candidate_count": "Number of candidates for the event, coding system and mapping source.",
    "record_status": "Status label carried unchanged from the contributing canonical arm through internal_event_index and the public event-to-mapping left join. It is not a Boolean mirror of IS_CURRENT_IN_SOURCE or a verbatim closure flag: each arm supplies its own active, superseded or retracted recipe. Every mapping row for an event inherits the event-side status; mapping fields do not replace it. The input is the canonical event-index row, not a promise of equality with a later typed public product's history or latest-version selection.",
    "loaded_at": "Per-event loaded_at carried unchanged from the contributing canonical arm through internal_event_index, then selected from the event side of the public mapping join. Source arms have different clock expressions, including typed nulls; this is not uniformly one bronze row's write time. Mapping clocks do not contribute, and mapping fan-out does not calculate a new maximum. Later typed-product enrichment, such as implant attribute clocks or additional document-history rows, is not folded back into this event-index clock. No current Silver refresh timestamp is substituted.",
}

# Physical person/time clustering is applied by _jm_pipeline_admin post-update, exactly as for
# the v1 UNION-backed index (the join-backed MV keeps the same repair pathway).

PATIENT_EVENT_LIFECYCLE_FIELDS = [
    'identity_status',
    'load_batch_id',
]

PATIENT_EVENT_RETIRED_COLUMNS = [

]

PATIENT_EVENT_LIFECYCLE_COLUMNS = [
    'patient_event_row_key',
    'patient_event_key',
    'identity_status',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_patient_event():
    # Assemble patient event rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    e = spark.read.table(_n("journey_events._event_index")).alias("e")
    m = spark.read.table(_n("journey_events._event_map")).alias("m")
    # contract v2: publish patient-event and row hashes under key names and join the event index on patient_event_key
    return (
        e.join(m, F.col("e.patient_event_key") == F.col("m.patient_event_key"), "left")
        .select(
            F.coalesce(
                F.col("m.patient_event_map_id"),
                stable_id("patient_event:unmapped", F.col("e.patient_event_key")),
            ).alias("patient_event_row_key"),
            F.col("e.patient_event_key").alias("patient_event_key"),
            F.col("e.subject_key").alias("subject_key"),
            F.col("e.subject_id_system").alias("subject_id_system"),
            F.col("e.person_id").alias("person_id"),
            F.col("e.identity_status").alias("identity_status"),
            F.col("e.encounter_id").alias("encounter_id"),
            F.col("e.event_datetime").alias("event_datetime"),
            F.col("e.event_end_datetime").alias("event_end_datetime"),
            F.col("e.event_type").alias("event_type"),
            F.col("e.fact_category").alias("fact_category"),
            F.col("e.fact_table").alias("fact_table"),
            F.col("e.fact_row_key").alias("fact_row_key"),
            F.col("e.source_system").alias("source_system"),
            F.col("e.source_object").alias("source_object"),
            F.col("e.source_row_key").alias("source_row_key"),
            canonical_coding_system(F.col("e.source_coding_system")).alias("source_coding_system"),
            F.col("e.source_code").alias("source_code"),
            F.col("e.source_display").alias("source_display"),
            F.col("e.record_status").alias("record_status"),
            F.col("e.confidentiality_code").alias("confidentiality_code"),
            F.col("e.vip_ind").alias("vip_ind"),
            F.col("e.withheld_identity_ind").alias("withheld_identity_ind"),
            canonical_coding_system(F.col("m.mapped_coding_system")).alias("mapped_coding_system"),
            F.col("m.mapped_code").alias("mapped_code"),
            F.col("m.mapped_display").alias("mapped_display"),
            F.col("m.target_domain").alias("target_domain"),
            F.col("m.map_source").alias("map_source"),
            F.col("m.map_version").alias("map_version"),
            F.coalesce(
                F.col("m.map_method"),
                F.when(F.col("m.mapped_code").isNull(), F.lit(None).cast("string"))
                 .when(canonical_coding_system(F.col("m.mapped_coding_system")) ==
                       canonical_coding_system(F.col("e.source_coding_system")), F.lit("source_native"))
                 .otherwise(F.lit("rule")),
            ).alias("map_method"),
            F.col("m.map_confidence").alias("map_confidence"),
            F.coalesce(F.col("m.map_rule_id"), F.col("m.map_source")).alias("map_rule_id"),
            F.coalesce(
                F.col("m.map_candidate_count"),
                F.when(F.col("m.mapped_code").isNotNull(),
                       F.count(F.col("m.mapped_code")).over(Window.partitionBy(
                           F.col("e.patient_event_key"), F.col("m.mapped_coding_system"), F.col("m.map_source"))))
                 .otherwise(F.lit(0)),
            ).cast("int").alias("map_candidate_count"),
            F.col("e.load_batch_id").alias("load_batch_id"),
            F.col("e.loaded_at").alias("loaded_at"),
        )
    )

@materialized_view(
    name=_n("journey_events.patient_event"),
    comment="One row per admitted event x standard mapping (unmapped events carry one row with "
            "a null mapping block). Who/when/what, plain-language source, and standard-terms "
            "meaning in a single read. Timeline counts use COUNT(DISTINCT patient_event_key).",
    refresh_policy="incremental",
    column_comments=PATIENT_EVENT_COLUMN_COMMENTS,
)
def patient_event():
    # Build the declared dataset: One row per admitted event x standard mapping (unmapped events
    # carry one row with a null mapping block). Who/when/what, plain-language source, and
    # standard-terms meaning in a single read. Timeline counts use COUNT(DISTINCT
    # patient_event_key).
    return _lifecycle_source_patient_event().drop(*PATIENT_EVENT_LIFECYCLE_FIELDS, *PATIENT_EVENT_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_events._patient_event_metadata'),
    comment='Internal quality and batch metadata for events_patient_event; same row grain as the research table. Join keys: patient_event_row_key, patient_event_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def patient_event_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for events_patient_event;
    # same row grain as the research table. Join keys: patient_event_row_key, patient_event_key.
    return (_lifecycle_source_patient_event()).select(*PATIENT_EVENT_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / admission metrics

# COMMAND ----------

def _pharmacy_issue_canonical():
    # Assemble normalized pharmacy issue rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_PHARMACY_ISSUE)
    event_id = stable_id("pharmacy_issue:jac", s.PHARMACY_ISSUE_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID), ("urn:jac:lnkpid", s.LNKPID)],
        SRC_PHARMACY_ISSUE,
        s.PHARMACY_ISSUE_ID,
    )
    retracted = F.coalesce(~s.SOURCE_PRESENT_IND, F.lit(False))
    promoted = F.upper(F.trim(F.coalesce(s.ISSUE_CATEGORY, F.lit("")))) == F.lit("ISSUE")
    payload = F.parse_json(F.to_json(F.struct(*[s[c] for c in s.columns])))
    return s.select(
        event_id.alias("patient_event_id"),
        event_id.alias("fact_row_id"),
        skey.alias("subject_key"),
        ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("long").cast("string").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
         .when(s.LNKPID.isNotNull(), F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("string").alias("encounter_id"),
        s.ISSUE_DTTM.alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.lit("urn:jac:issue_type").alias("source_coding_system"),
        s.ISSUE_TYPE.alias("source_code"),
        s.ISSUE_CATEGORY.alias("source_display"),
        s.DRUG_FULL.alias("value_text"),
        s.TOTAL_UNITS.cast("decimal(38,4)").alias("value_number"),
        F.lit(None).cast("timestamp").alias("value_datetime"),
        s.DRUG_DOSEUNIT.alias("unit"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active")).alias("record_status"),
        s.SOURCE_RECORD_UPDATED_DT.alias("record_status_effective_from"),
        F.when(retracted, s.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.parse_json(F.lit("null")).alias("sensitivity_labels"),
        F.lit("pharmacy_issue").alias("feed_id"),
        payload.alias("payload"),
        F.when(promoted, F.lit("journey_clinical.medication_dispense"))
        .alias("promoted_to_table"),
        F.when(promoted, event_id).alias("promoted_to_row_id"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.SOURCE_RECORD_UPDATED_DT.alias("source_update_timestamp"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("jac").alias("_source_system"),
        F.lit(SRC_PHARMACY_ISSUE).alias("_source_table"),
        s.PHARMACY_ISSUE_ID.alias("_source_row_id"),
        s.DMD_VTM_CODE.alias("_dmd_code"),
        s.DMD_VTM_NAME.alias("_dmd_display"),
    )

ADMISSION_METRICS_COLUMN_COMMENTS = {
    "feed_id": "Source-system identifier for the feed associated with each admission metrics record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "route_id": "Route or exclusion reason.",
    "admitted": "Whether these rows entered silver.",
    "load_date": "Bronze load date bucket for per-load accounting.",
    "row_count": "Rows in this bucket.",
    "latest_loaded_at": "Latest bronze load time observed in the bucket.",
}

@materialized_view(
    name=_n("journey_reference.admission_metrics"),
    comment="Admission-gate accounting: rows seen per registered route with admitted flag. "
            "route_id doubles as the exclusion reason for admitted=false rows. Product-wide "
            "usable-code gate: all source_code-bearing fact lanes report "
            "excluded_unusable_code; document (narrative route) and the structured admin facts "
            "without source_code are exempt; registry_entry is exempt as a "
            "code-free class. Exclusions are counted here and "
            "retained nowhere.",
    refresh_policy="incremental",
    column_comments=ADMISSION_METRICS_COLUMN_COMMENTS,
)
def admission_metrics():
    # contract v2: admission metrics remain identifier-free and require no identifier projection change
    # Build the declared dataset: Admission-gate accounting: rows seen per registered route with
    # admitted flag. route_id doubles as the exclusion reason for admitted=false rows. Product-
    # wide usable-code gate: all source_code-bearing fact lanes report excluded_unusable_code;
    # document (narrative route) and the structured admin facts without source_code are exempt;
    # registry_entry is exempt as a code-free class. Exclusions are counted here and retained
    # nowhere.
    def lane(df, feed_id, route_col, admitted_col):
        # Project feed, route, admission decision and load date into the common coverage-metric
        # schema.
        return df.select(
            F.lit(feed_id).alias("feed_id"),
            route_col.cast("string").alias("route_id"),
            admitted_col.alias("admitted"),
            F.to_date(F.col("loaded_at")).alias("load_date"),
            F.col("loaded_at"),
        )

    def typed_route(df):
        # Classify finding rows as admitted, untyped or unusably coded for coverage accounting.
        gated = F.col("_typed_route") & _usable_code(F.col("source_code"))
        route = (
            F.when(gated, F.lit("clinical_finding"))
             .when(F.col("_typed_route"), F.lit("excluded_unusable_code"))
             .otherwise(F.lit("excluded_untyped"))
        )
        return df.withColumn("_metric_route", route), gated

    def fact_lane(df, feed_id, fact_route):
        # Mark fact rows as admitted or excluded for unusable source codes, then project their
        # coverage metrics.
        ok = _usable_code(F.col("source_code"))
        route = F.when(ok, F.lit(fact_route)).otherwise(F.lit("excluded_unusable_code"))
        return lane(df, feed_id, route, ok)

    coded_df, coded_ok = typed_route(_coded_finding_canonical())
    nomen_df, nomen_ok = typed_route(_nomen_finding_canonical())
    date_df, date_ok = typed_route(_date_finding_canonical())
    text_gated = _usable_code(F.col("source_code"))
    text_route = (
        F.when(F.col("_route") == "annex_catch_all", F.lit("excluded_untyped"))
         .when(~text_gated, F.lit("excluded_unusable_code"))
         .otherwise(F.col("_route"))
    )
    rows = (
        lane(coded_df, "coded_events", F.col("_metric_route"), coded_ok)
        .unionByName(lane(nomen_df, "nomen_events", F.col("_metric_route"), nomen_ok))
        .unionByName(lane(date_df, "date_events", F.col("_metric_route"), date_ok))
        .unionByName(lane(
            _text_finding_canonical(), "text_events", text_route,
            (F.col("_route") != "annex_catch_all") & text_gated,
        ))
        .unionByName(lane(
            _numeric_vital_canonical(), "numeric_events",
            F.when(_usable_code(F.col("source_code")), F.lit("vital_sign"))
             .otherwise(F.lit("excluded_unusable_code")),
            _usable_code(F.col("source_code")),
        ))
        .unionByName(lane(
            _numeric_score_canonical(), "numeric_events",
            F.when(_usable_code(F.col("source_code")), F.lit("clinical_score"))
             .otherwise(F.lit("excluded_unusable_code")),
            _usable_code(F.col("source_code")),
        ))
        .unionByName(fact_lane(
            _promoted_vital_from_stage(), "form_promotion", "vital_sign"
        ))
        .unionByName(fact_lane(
            _promoted_score_from_stage(), "form_promotion", "clinical_score"
        ))
        .unionByName(lane(
            _numeric_excluded_canonical(), "numeric_events",
            F.lit("excluded_residual"), F.lit(False),
        ))
        .unionByName(lane(
            _pharmacy_issue_canonical().where(
                F.upper(F.trim(F.coalesce(F.col("source_display"), F.lit("")))) != "ISSUE"
            ),
            "pharmacy_issue", F.lit("excluded_non_issue_audit"), F.lit(False),
        ))
        .unionByName(fact_lane(
            _medication_dispense_canonical_pregate(), "pharmacy_issue", "medication_dispense"
        ))
        .unionByName(fact_lane(
            _family_history_canonical_pregate(), "family_history", "family_history"
        ))
        .unionByName(fact_lane(
            _allergy_canonical_pregate(), "allergy", "allergy_intolerance"
        ))
        .unionByName(fact_lane(
            _transfusion_canonical_pregate(), "bloodtrack", "transfusion"
        ))
        .unionByName(fact_lane(
            _cancer_treatment_canonical_pregate(), "cancer_treatment", "cancer_treatment"
        ))
        .unionByName(fact_lane(
            _condition_stage_canonical_pregate(), "aria_staging", "condition_stage"
        ))
        .unionByName(fact_lane(
            _endobase_procedure_canonical_pregate(), "endobase_exam", "procedure"
        ))
        .unionByName(fact_lane(
            _endoscopy_finding_canonical_pregate(), "endobase_exam_term", "endoscopy_finding"
        ))
        .unionByName(lane(
            _endobase_document_canonical(), "endobase_exam",
            F.lit("document"), F.lit(True),
        ))
        .unionByName(lane(
            _order_comment_document_canonical(), "order_comment",
            F.lit("document"), F.lit(True),
        ))
        .unionByName(lane(
            _elective_access_comment_document_canonical(), "elective_access_comment",
            F.lit("document"), F.lit(True),
        ))
        .unionByName(lane(
            _registry_entry_canonical(), "iweb_registry",
            F.col("registry_family"), F.lit(True),
        ))
        .unionByName(fact_lane(
            _device_canonical_pregate(), "mediconnect_device", "device"
        ))
        .unionByName(fact_lane(
            _condition_diagnosis_canonical_pregate(), "diagnosis", "condition"
        ))
        .unionByName(fact_lane(
            _condition_problem_canonical_pregate(), "problem", "condition"
        ))
        .unionByName(fact_lane(
            _procedure_canonical_pregate(), "procedure", "procedure"
        ))
        .unionByName(fact_lane(
            _implant_procedure_canonical_pregate(), "implant_details", "procedure"
        ))
        .unionByName(fact_lane(
            _theatre_procedure_canonical_pregate(), "theatre_case", "procedure"
        ))
        .unionByName(fact_lane(
            _pathology_result_canonical_pregate(), "pathology", "pathology_result"
        ))
        .unionByName(fact_lane(
            _pathology_requested_test_canonical_pregate(),
            "pathology_requested_test", "pathology_order"
        ))
        .unionByName(fact_lane(
            _genomic_test_canonical_pregate(), "pathology_genetic_test", "genomic_test"
        ))
        .unionByName(fact_lane(
            _genomic_result_canonical_pregate(), "pathology_genetic_result", "genomic_result"
        ))
        .unionByName(fact_lane(
            _indication_canonical_pregate(), "pathology_indication", "indication"
        ))
        .unionByName(fact_lane(
            _micro_isolate_canonical_pregate(), "pathology_microbiology_isolate", "microbiology_isolate"
        ))
        .unionByName(fact_lane(
            _susceptibility_canonical_pregate(), "pathology_antimicrobial_susceptibility", "susceptibility_result"
        ))
        .unionByName(lane(
            _pathology_specimen_canonical(), "pathology_accession",
            F.lit("specimen"), F.lit(True)
        ))
        .unionByName(lane(
            _pathology_report_series_canonical(), "pathology_report",
            F.lit("pathology_report"), F.lit(True)
        ))
        .unionByName(lane(
            _pathology_report_document_canonical(), "pathology_report",
            F.lit("document"), F.lit(True)
        ))
        .unionByName(lane(
            _pathology_report_document_history_canonical(), "pathology_report",
            F.lit("document_history"), F.lit(True)
        ))
        .unionByName(lane(
            _pathology_report_document_base(
                read_source(SRC_PATHOLOGY_REPORT_VERSIONS).where(
                    F.col("report_text").isNull() | (F.trim(F.col("report_text")) == "")
                )
            ),
            "pathology_report", F.lit("excluded_no_text"), F.lit(False)
        ))
        .unionByName(fact_lane(
            _form_canonical_pregate(), "form_activity", "form"
        ))
        .unionByName(fact_lane(
            _medication_admin_canonical_pregate(), "med_admin", "medication_admin"
        ))
        .unionByName(fact_lane(
            _medication_order_canonical_pregate(), "medication_order", "medication_order"
        ))
        .unionByName(fact_lane(
            _imaging_exam_canonical_pregate(), "pacs_examination", "imaging_exam"
        ))
        .unionByName(fact_lane(
            _community_care_activity_canonical_pregate(),
            "community_care_activity", "community_care_activity"
        ))
        .unionByName(lane(
            _community_care_contact_canonical(), "community_care_contact",
            F.lit("community_care_contact"), F.lit(True)
        ))
        .unionByName(lane(
            _hrg_grouping_canonical(), "slam_apc_hrg", F.col("source_feed"), F.lit(True)
        ).where(F.col("route_id") == "slam_apc_hrg"))
        .unionByName(lane(
            _hrg_grouping_canonical(), "slam_op_hrg", F.col("source_feed"), F.lit(True)
        ).where(F.col("route_id") == "slam_op_hrg"))
        .unionByName(lane(
            _costed_activity_canonical(), "slam_costed_activity",
            F.lit("costed_activity"), F.lit(True)
        ))
        .unionByName(lane(
            _drug_expenditure_canonical(), "finance_hcd_expenditure",
            F.lit("drug_expenditure"), F.lit(True)
        ))
        .unionByName(fact_lane(
            _medication_supply_canonical_pregate(), "homecare_request", "medication_supply"
        ))
        .unionByName(lane(
            _elective_access_entry_canonical(), "elective_access_list",
            F.lit("elective_access_entry"), F.lit(True)
        ))
        .unionByName(lane(
            _pathway_tracking_canonical(), "cancer_ptl",
            F.lit("pathway_tracking"), F.lit(True)
        ))
        .unionByName(lane(
            _critical_care_period_canonical(), "critical_care_period",
            F.lit("critical_care_period"), F.lit(True)
        ))
        .unionByName(fact_lane(
            _critical_care_activity_canonical_pregate(),
            "critical_care_activity", "critical_care_activity"
        ))
        .unionByName(fact_lane(
            _cc_procedure_canonical_pregate(),
            "critical_care_procedure", "procedure"
        ))
        .unionByName(lane(
            _critical_care_admission_canonical(), "critical_care_admission",
            F.lit("critical_care_admission"), F.lit(True)
        ))
        .unionByName(fact_lane(
            _cc_daily_score_canonical_pregate(),
            "critical_care_daily_score", "critical_care_daily_score"
        ))
        .unionByName(lane(
            _neonatal_episode_canonical(), "neonatal_episode",
            F.lit("neonatal_episode"), F.lit(True)
        ))
        .unionByName(fact_lane(
            _neonatal_care_day_canonical_pregate(),
            "neonatal_critical_care", "neonatal_care_day"
        ))
        .unionByName(fact_lane(
            _neonatal_examination_canonical_pregate(),
            "neonatal_examination", "neonatal_examination"
        ))
        .unionByName(lane(
            _neonatal_narrative_document_canonical(), "neonatal_episode_narrative",
            F.lit("document"), F.lit(True)
        ))
        .unionByName(lane(
            _baby_delivery_canonical(), "maternity_baby_delivery",
            F.lit("baby_delivery"), F.lit(True)
        ))
        .unionByName(lane(
            _labour_delivery_canonical(), "maternity_labour_delivery",
            F.lit("labour_delivery"), F.lit(True)
        ))
        .unionByName(lane(
            _maternity_care_contact_canonical(), "maternity_care_contact",
            F.lit("maternity_care_contact"), F.lit(True)
        ))
        .unionByName(fact_lane(
            _maternity_diagnosis_canonical_pregate(),
            "maternity_diagnosis", "condition"
        ))
        .unionByName(lane(
            _research_enrollment_canonical(), "research_subject",
            F.lit("research_enrollment"), F.lit(True)
        ))
        .unionByName(fact_lane(
            _mill_radiology_exam_canonical_pregate(),
            "radiology_event", "imaging_exam"
        ))
        .unionByName(lane(
            _pregnancy_reconciliation_source(), "mat_pregnancy_msds_unmatched",
            F.lit("reconciliation"), F.lit(True)
        ))
    )
    return rows.groupBy("feed_id", "route_id", "admitted", "load_date").agg(
        F.count(F.lit(1)).alias("row_count"),
        F.max("loaded_at").alias("latest_loaded_at"),
    )

def _numeric_excluded_canonical():
    # Assemble normalized numeric excluded rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_NUMERIC_EVENTS)
    s = s.where(_numeric_route_expr(s) == "excluded")
    event_id = stable_id("numeric_event:mill", s.EVENT_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_NUMERIC_EVENTS, s.EVENT_ID
    )
    payload = F.parse_json(F.to_json(F.struct(*[s[c] for c in s.columns])))
    deleted = F.coalesce(s.SOURCE_DELETED_IND, F.lit(False))
    ended = s.CLINICAL_EVENT_VALID_UNTIL_DT_TM.isNotNull() & (
        s.CLINICAL_EVENT_VALID_UNTIL_DT_TM < F.lit("2100-01-01").cast("timestamp")
    )
    return s.select(
        event_id.alias("patient_event_id"), event_id.alias("fact_row_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("string").alias("person_id"),
        F.when(_present(s.PERSON_ID), F.lit("resolved")).otherwise(F.lit("unresolved"))
         .alias("identity_status"),
        F.when(s.ENCNTR_ID.isNotNull(), stable_id("encounter:mill", s.ENCNTR_ID)).alias("encounter_id"),
        F.coalesce(s.PERFORMED_DT_TM, s.EVENT_START_DT_TM).alias("event_datetime"),
        s.EVENT_END_DT_TM.alias("event_end_datetime"),
        F.lit("urn:cerner:event_cd").alias("source_coding_system"),
        s.EVENT_CD.cast("string").alias("source_code"),
        F.coalesce(s.EVENT_LABEL, s.EVENT_CD_DISPLAY).alias("source_display"),
        s.RESULT_TEXT_EFFECTIVE.alias("value_text"),
        s.NUMERIC_RESULT.cast("decimal(38,10)").alias("value_number"),
        F.lit(None).cast("timestamp").alias("value_datetime"),
        F.coalesce(s.UNIT_OF_MEASURE_DISPLAY, s.RESULT_UNITS_DISPLAY).alias("unit"),
        F.when(deleted, F.lit("retracted")).when(ended, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        s.CLINICAL_EVENT_VALID_FROM_DT_TM.alias("record_status_effective_from"),
        F.when(deleted | ended, s.CLINICAL_EVENT_VALID_UNTIL_DT_TM).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"), F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.parse_json(F.lit("null")).alias("sensitivity_labels"),
        F.lit("numeric_event").alias("feed_id"), payload.alias("payload"),
        F.lit(None).cast("string").alias("promoted_to_table"),
        F.lit(None).cast("string").alias("promoted_to_row_id"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        F.greatest(s.STRING_RESULT_UPDT_DT_TM, s.CLINICAL_EVENT_UPDT_DT_TM, s.ADC_UPDT)
         .alias("source_update_timestamp"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_NUMERIC_EVENTS).alias("_source_table"),
        s.EVENT_ID.cast("string").alias("_source_row_id"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spine / encounter

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
ENCOUNTER_PUBLIC_COLUMNS = [
    'encounter_key',
    'encounter_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'parent_encounter_id',
    'parentage_status',
    'encounter_level',
    'class_code',
    'class_display',
    'type_code',
    'type_display',
    'type_class_code',
    'type_class_display',
    'status_code',
    'status_display',
    'period_start',
    'period_end',
    'arrival_method',
    'arrival_confidence',
    'departure_method',
    'departure_confidence',
    'length_of_stay_minutes',
    'scheduled_start',
    'scheduled_end',
    'registration_datetime',
    'inpatient_admit_datetime',
    'discharge_datetime',
    'workflow_complete_datetime',
    'raw_arrival_datetime',
    'raw_departure_datetime',
    'admission_source_code',
    'admission_source_display',
    'discharge_destination_code',
    'discharge_destination_display',
    'responsible_service_code',
    'responsible_service_display',
    'specialty_code',
    'specialty_display',
    'current_location_key',
    'organization_id',
    'service_provider_organization_key',
    'reason_for_visit',
    'attendance_evidence',
    'attendance_witness_count',
    'confidentiality_code',
    'vip_ind',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

ENCOUNTER_LIFECYCLE_COLUMNS = [
    'encounter_key',
    'encounter_id',
    'load_batch_id',
]

ENCOUNTER_COLUMN_COMMENTS = {
    "encounter_key": "Source encounter primary key.",
    "encounter_id": "Deterministic encounter primary key.",
    "subject_key": "Always-populated subject join key.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium person identifier.",
    "parent_encounter_id": "Governed containment parent when supplied by source evidence.",
    "parentage_status": "Provenance state for encounter containment.",
    "encounter_level": "Rules-light source-classified encounter level.",
    "class_code": "Source encounter class code.",
    "class_display": "Source encounter class display.",
    "type_code": "Source encounter type code.",
    "type_display": "Source encounter type display.",
    "type_class_code": "Source encounter type-class code.",
    "type_class_display": "Source encounter type-class display.",
    "status_code": "Native encounter status code.",
    "status_display": "Native encounter status display.",
    "period_start": "Best observed encounter start.",
    "period_end": "Best observed encounter end.",
    "arrival_method": "Method selecting period_start.",
    "arrival_confidence": "Source-derived confidence for period_start.",
    "departure_method": "Method selecting period_end.",
    "departure_confidence": "Source-derived confidence for period_end.",
    "length_of_stay_minutes": "Source-productised encounter duration.",
    "scheduled_start": "Scheduled arrival timestamp.",
    "scheduled_end": "Scheduled departure timestamp.",
    "registration_datetime": "Registration timestamp retained as source evidence.",
    "inpatient_admit_datetime": "Inpatient admission timestamp.",
    "discharge_datetime": "Source actual discharge timestamp.",
    "workflow_complete_datetime": "Administrative workflow completion timestamp.",
    "raw_arrival_datetime": "Raw ARRIVE_DT_TM retained without asserting observability.",
    "raw_departure_datetime": "Raw DEPART_DT_TM retained without asserting clinical meaning.",
    "admission_source_code": "Source admission source code.",
    "admission_source_display": "Best available label for ADMIT_SRC_CD.",
    "discharge_destination_code": "Source discharge destination code.",
    "discharge_destination_display": "Best available label for DISCH_TO_LOCTN_CD.",
    "responsible_service_code": "Source medical service code.",
    "responsible_service_display": "Best available label for MED_SERVICE_CD.",
    "specialty_code": "Source specialty-unit code.",
    "specialty_display": "Source specialty-unit display.",
    "current_location_key": "Source current nurse-unit code.",
    "organization_id": "Source encounter organization reference.",
    "service_provider_organization_key": "Source organization primarily responsible for the encounter.",
    "reason_for_visit": "Verbatim source reason for visit.",
    "attendance_evidence": "Productised attendance evidence classification.",
    "attendance_witness_count": "Number of attendance witnesses in the source product.",
    "confidentiality_code": "Source confidentiality code.",
    "vip_ind": "Source VIP indicator.",
    "record_status": "Derived encounter-row status: superseded when map_encounter.END_EFFECTIVE_DT_TM is earlier than 2100-01-01; otherwise active, including a null or open-ended source timestamp. This classification does not use the current wall clock or ACTIVE_IND and is distinct from encounter workflow/discharge status.",
    "record_status_effective_from": "map_encounter.ACTIVE_STATUS_DT_TM carried unchanged as the source active-status timestamp. No arrival, registration or ingestion-clock fallback is applied; a missing source value remains null.",
    "record_status_effective_to": "map_encounter.END_EFFECTIVE_DT_TM when it is earlier than 2100-01-01; otherwise null, including missing values and open-ended sentinels. This is the row-version end boundary, not the encounter departure or discharge time.",
    "source_update_timestamp": "map_encounter.UPDT_DT_TM carried unchanged from the source encounter row. It is separate from the bronze ADC_UPDT ingestion clock and is not the Silver refresh time; a missing source timestamp remains null.",
    "loaded_at": "map_encounter.ADC_UPDT carried unchanged from the contributing bronze encounter row. This is ingestion provenance, not the Silver refresh time or encounter arrival/departure; a missing source timestamp remains null.",
}

@materialized_view(
    name=_n("journey_spine.encounter"),
    comment="One source encounter with evidence-based timestamps. Parent remains null where bronze supplies no governed containment key.",
    cluster_by=["person_id", "period_start"],
    refresh_policy="incremental",
    column_comments=ENCOUNTER_COLUMN_COMMENTS,
)
def encounter():
    # Build the declared dataset: One source encounter with evidence-based timestamps. Parent
    # remains null where bronze supplies no governed containment key.
    return _lifecycle_source_encounter().select(*ENCOUNTER_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_spine._encounter_metadata'),
    comment='Internal quality and batch metadata for spine_encounter; same row grain as the research table. Join keys: encounter_key, encounter_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def encounter_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for spine_encounter; same
    # row grain as the research table. Join keys: encounter_key, encounter_id.
    return (_lifecycle_source_encounter()).select(*ENCOUNTER_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spine / location stay

# COMMAND ----------

SRC_LOCATION_HISTORY = "4_prod.bronze.map_patient_journey"

def _location_stay_canonical():
    # Assemble normalized location stay rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_LOCATION_HISTORY)
    staged = s.withColumn(
        "_stop_discriminator",
        F.when(s.LOCATION_STOP_SEQUENCE.isNotNull(),
               F.concat(F.lit("stop:"), s.LOCATION_STOP_SEQUENCE.cast("string")))
         .otherwise(F.concat(F.lit("event:"), s.ENCNTR_LOC_HIST_ID.cast("string"))),
    )
    grouped = (
        staged.groupBy("ENCNTR_ID", "_stop_discriminator")
        .agg(
            F.max("PERSON_ID").alias("PERSON_ID"),
            F.min("ENCNTR_LOC_HIST_ID").alias("source_row_id_min"),
            F.max("ENCNTR_LOC_HIST_ID").alias("source_row_id_max"),
            F.count(F.lit(1)).alias("source_history_row_count"),
            F.min("HISTORY_EVENT_SEQUENCE").alias("first_history_event_sequence"),
            F.max("HISTORY_EVENT_SEQUENCE").alias("last_history_event_sequence"),
            F.min("LOCATION_STOP_START_DT_TM").alias("stay_start"),
            F.max("LOCATION_STOP_END_DT_TM").alias("stay_end"),
            F.max("LOC_NURSE_UNIT_CD").alias("nurse_unit_cd"),
            F.max("NURSE_UNIT_DESC").alias("nurse_unit_display"),
            F.max("LOC_BUILDING_CD").alias("building_cd"),
            F.max("BUILDING_DESC").alias("building_display"),
            F.max("LOC_FACILITY_CD").alias("facility_cd"),
            F.max("FACILITY_DESC").alias("facility_display"),
            F.max("LOC_ROOM_CD").alias("room_cd"),
            F.max("ROOM_DESC").alias("room_display"),
            F.max("LOC_BED_CD").alias("bed_cd"),
            F.max("BED_DESC").alias("bed_display"),
            F.max("MED_SERVICE_CD").alias("service_code"),
            F.max("MED_SERVICE_DESC").alias("service_display"),
            F.max("TRANSFER_REASON_CD").alias("transfer_reason_code"),
            F.max("TRANSFER_REASON_DESC").alias("transfer_reason_display"),
            F.max("CONFID_LEVEL_CD").alias("confidentiality_code"),
            F.max("VIP_CD").alias("vip_code"),
            F.max("ADC_UPDT").alias("loaded_at"),
        )
    )
    source_row_id = F.concat_ws(
        ":",
        F.col("ENCNTR_ID").cast("string"),
        F.col("_stop_discriminator"),
        F.col("source_row_id_min").cast("string"),
        F.col("source_row_id_max").cast("string"),
    )
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("PERSON_ID"))],
        SRC_LOCATION_HISTORY,
        source_row_id,
    )
    ended = F.coalesce(
        F.col("stay_end") < F.lit("2100-01-01").cast("timestamp"),
        F.lit(False),
    )
    # contract v2: publish native encounter/person/location values while retaining only the modelled stop hash as a key
    projected = grouped.select(
        stable_id("location_stay:mill", F.col("ENCNTR_ID"), F.col("_stop_discriminator"))
          .alias("location_stay_key"),
        F.col("ENCNTR_ID").cast("bigint").alias("encounter_id"),
        skey.alias("subject_key"),
        ssys.alias("subject_id_system"),
        F.col("PERSON_ID").cast("bigint").alias("person_id"),
        F.when(F.col("nurse_unit_cd").isNotNull(),
               stable_id("location:mill:nurse_unit", F.col("nurse_unit_cd")))
         .when(F.col("building_cd").isNotNull(),
               stable_id("location:mill:building", F.col("building_cd")))
         .when(F.col("facility_cd").isNotNull(),
               stable_id("location:mill:facility", F.col("facility_cd")))
         .alias("_candidate_location_key"),
        F.coalesce(F.col("nurse_unit_cd"), F.col("building_cd"), F.col("facility_cd"))
         .cast("string").alias("_candidate_location_code"),
        F.col("stay_start").alias("period_start"),
        F.col("stay_end").alias("period_end"),
        F.col("nurse_unit_cd").cast("string").alias("nurse_unit_code"),
        F.col("nurse_unit_display"),
        F.col("building_cd").cast("string").alias("building_code"),
        F.col("building_display"),
        F.col("facility_cd").cast("string").alias("facility_code"),
        F.col("facility_display"),
        F.col("room_cd").cast("string").alias("room_code"),
        F.col("room_display"),
        F.col("bed_cd").cast("string").alias("bed_code"),
        F.col("bed_display"),
        F.col("service_code").cast("string").alias("service_code"),
        F.col("service_display"),
        F.col("transfer_reason_code").cast("string").alias("transfer_reason_code"),
        F.col("transfer_reason_display"),
        F.col("source_history_row_count").cast("long").alias("source_history_row_count"),
        F.col("first_history_event_sequence").cast("int").alias("first_history_event_sequence"),
        F.col("last_history_event_sequence").cast("int").alias("last_history_event_sequence"),
        F.col("confidentiality_code").cast("string").alias("confidentiality_code"),
        (F.coalesce(F.col("vip_code"), F.lit(0)) != 0).alias("vip_ind"),
        F.when(ended, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        F.when(ended, F.col("stay_end")).alias("record_status_effective_to"),
        F.lit(SRC_LOCATION_HISTORY).alias("source_table"),
        source_row_id.alias("source_row_id"),
        F.date_format("loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("loaded_at"),
    )
    valid_locations = _location_projection().select(
        F.col("location_key").alias("_valid_location_key"),
        F.col("location_code").alias("_valid_location_code"),
    )
    joined = projected.join(
        valid_locations,
        projected._candidate_location_key == valid_locations._valid_location_key,
        "left",
    )
    return joined.select(*[
        F.col("_valid_location_code").alias("location_code")
        if c == "_candidate_location_code" else F.col(c)
        for c in projected.columns if c != "_candidate_location_key"
    ])

LOCATION_STAY_COLUMN_COMMENTS = {
    "location_stay_key": "Deterministic SHA-256 modelled location-stop key.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "location_code": "Native Millennium location code resolved against reference_location.",
    "period_start": "Earliest `LOCATION_STOP_START_DT_TM` across the source history rows grouped into this encounter location stop.",
    "period_end": "Latest `LOCATION_STOP_END_DT_TM` across the source history rows grouped into this encounter location stop; the grouped source timestamp is retained without sentinel removal.",
    "nurse_unit_code": "Historical nurse-unit code.",
    "nurse_unit_display": "Historical nurse-unit display.",
    "building_code": "Historical building code.",
    "building_display": "Historical building display.",
    "facility_code": "Historical facility code.",
    "facility_display": "Historical facility display.",
    "room_code": "Historical room code.",
    "room_display": "Historical room display.",
    "bed_code": "Historical bed code.",
    "bed_display": "Historical bed display.",
    "service_code": "Service code during the stop.",
    "service_display": "Service display during the stop.",
    "transfer_reason_code": "Source transfer-reason code.",
    "transfer_reason_display": "Source transfer-reason display.",
    "source_history_row_count": "Number of history rows grouped into the stop.",
    "first_history_event_sequence": "First source history sequence in the stop.",
    "last_history_event_sequence": "Last source history sequence in the stop.",
    "confidentiality_code": "Source confidentiality code.",
    "vip_ind": "Source VIP indicator.",
    "source_table": "Fully qualified bronze source table.",
    "source_row_id": "Deterministic grouped source-row identity.",
    "record_status": "Derived location-stop status: superseded when the grouped maximum map_patient_journey.LOCATION_STOP_END_DT_TM is earlier than 2100-01-01; otherwise active, including an all-null or open-ended maximum. Grouping is by ENCNTR_ID and the stop discriminator. This is a status label, not an end timestamp or a comparison with the current wall clock.",
    "record_status_effective_to": "Maximum map_patient_journey.LOCATION_STOP_END_DT_TM within the encounter/stop-discriminator group, returned only when that maximum is earlier than 2100-01-01. Otherwise null, including all-null or open-ended maxima. This uses the grouped stop end, not the earliest contributing boundary.",
    "loaded_at": "Maximum map_patient_journey.ADC_UPDT within the ENCNTR_ID and stop-discriminator group. The discriminator uses LOCATION_STOP_SEQUENCE when present, otherwise ENCNTR_LOC_HIST_ID. This is contributing ingestion provenance, not the Silver refresh time; null when all contributing ADC_UPDT values are null.",
}

LOCATION_STAY_LIFECYCLE_FIELDS = [
    'load_batch_id',
]

LOCATION_STAY_RETIRED_COLUMNS = [

]

LOCATION_STAY_LIFECYCLE_COLUMNS = [
    'location_stay_key',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_location_stay():
    # Assemble location stay rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return _location_stay_canonical()

@materialized_view(
    name=_n("journey_spine.location_stay"),
    comment="One physical location stop per encounter, anchored on the source encounter and preserving bed/room detail.",
    cluster_by=["person_id", "period_start"],
    refresh_policy="incremental",
    column_comments=LOCATION_STAY_COLUMN_COMMENTS,
)
def location_stay():
    # Build the declared dataset: One physical location stop per encounter, anchored on the
    # source encounter and preserving bed/room detail.
    return _lifecycle_source_location_stay().drop(*LOCATION_STAY_LIFECYCLE_FIELDS, *LOCATION_STAY_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_spine._location_stay_metadata'),
    comment='Internal quality and batch metadata for spine_location_stay; same row grain as the research table. Join keys: location_stay_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def location_stay_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for spine_location_stay;
    # same row grain as the research table. Join keys: location_stay_key.
    return (_lifecycle_source_location_stay()).select(*LOCATION_STAY_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spine / care participation

# COMMAND ----------

def _care_participation_projection(df, practitioner_col, role, start_col):
    # Create encounter-level practitioner participation records for rows with a practitioner in
    # the specified role.
    return (
        # contract v2: rename modelled hashes as keys and publish the native practitioner id
        df.where(F.col(practitioner_col).isNotNull())
          .select(
              stable_id(
                  "care_participation:mill",
                  F.col("_source_encounter_id"), F.col(practitioner_col), F.lit(role),
              ).alias("care_participation_key"),
              "subject_key", "subject_id_system", "person_id",
              F.col("encounter_id"),
              F.lit(None).cast("string").alias("journey_key"),
              F.lit(None).cast("string").alias("service_id"),
              F.col(practitioner_col).cast("bigint").alias("practitioner_id"),
              F.lit(role).alias("role"),
              F.col(start_col).alias("valid_from"),
              F.lit(None).cast("timestamp").alias("valid_to"),
              F.lit("source_encounter_personnel_field").alias("construction_rule"),
              F.lit("0.4.0").alias("construction_version"),
              F.lit(SRC_ENCOUNTER).alias("source_table"),
              F.col("_source_encounter_id").cast("string").alias("source_row_id"),
              "load_batch_id", "loaded_at",
          )
    )

CARE_PARTICIPATION_COLUMN_COMMENTS = {
    "care_participation_key": "Deterministic SHA-256 participation key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT.",
    "journey_key": "Deterministic journey key when available.",
    "service_id": "Service-registration context when available.",
    "practitioner_id": "Millennium personnel PERSON_ID as BIGINT.",
    "role": "Source-field-derived participation role.",
    "valid_from": "Participation validity start.",
    "valid_to": "Participation validity end.",
    "construction_rule": "Governed derivation rule.",
    "construction_version": "Governed derivation-rule version.",
    "source_table": "Fully qualified bronze source table.",
    "source_row_id": "Source encounter row identity.",
    "loaded_at": "map_encounter.ADC_UPDT inherited unchanged through the canonical encounter row for each registrar, recorder or discharger participation. This is encounter ingestion provenance, not a practitioner update, role-validity timestamp or Silver refresh time; a missing source timestamp remains null.",
}

CARE_PARTICIPATION_LIFECYCLE_FIELDS = [
    'load_batch_id',
]

CARE_PARTICIPATION_RETIRED_COLUMNS = [

]

CARE_PARTICIPATION_LIFECYCLE_COLUMNS = [
    'care_participation_key',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_care_participation():
    # Assemble care participation rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    e = _encounter_canonical()
    return (
        _care_participation_projection(e, "_registration_practitioner_id", "registrar", "_registration_role_start")
        .unionByName(
            _care_participation_projection(e, "_creator_practitioner_id", "recorder", "_creator_role_start")
        )
        .unionByName(
            _care_participation_projection(e, "_discharge_practitioner_id", "discharger", "_discharge_role_start")
        )
    )

@materialized_view(
    name=_n("journey_spine.care_participation"),
    comment="Encounter-scoped practitioner participation from registration, creation, and discharge source fields.",
    refresh_policy="incremental",
    column_comments=CARE_PARTICIPATION_COLUMN_COMMENTS,
)
def care_participation():
    # Build the declared dataset: Encounter-scoped practitioner participation from registration,
    # creation, and discharge source fields.
    return _lifecycle_source_care_participation().drop(*CARE_PARTICIPATION_LIFECYCLE_FIELDS, *CARE_PARTICIPATION_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_spine._care_participation_metadata'),
    comment='Internal quality and batch metadata for spine_care_participation; same row grain as the research table. Join keys: care_participation_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def care_participation_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # spine_care_participation; same row grain as the research table. Join keys:
    # care_participation_key.
    return (_lifecycle_source_care_participation()).select(*CARE_PARTICIPATION_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / concept map

# COMMAND ----------

# ==== Terminology and mapping plane ====

SRC_LUNA_CODE_MAPPING = "3_lookup.omop.luna_code_map"

SRC_SEED_CODE_MAPPING = "3_lookup.omop.seed_code_map"

SRC_IWEB_VALUE_MAP = "3_lookup.omop.iweb_value_to_concept"

SRC_DIAGNOSIS_MAPPING = "4_prod.bronze.map_diagnosis"

SRC_CODED_EVENT_MAPPING = "4_prod.bronze.map_coded_events_omop_bridge"

SRC_PATHOLOGY_MAPPING = "4_prod.bronze.map_pathology"

SRC_PATHOLOGY_CROSS_ARM_TEST_MAP = "3_lookup.omop.pathology_cross_arm_test_map"

SRC_PATHOLOGY_HGNC_GENE = "3_lookup.omop.pathology_hgnc_gene"

SRC_PATHOLOGY_HGNC_ALIAS = "3_lookup.omop.pathology_hgnc_alias"

SRC_PATHOLOGY_ANTIMICROBIAL_MAP = "3_lookup.omop.pathology_antimicrobial_map"

SRC_PATHOLOGY_PANEL_DEFINITION = "3_lookup.omop.pathology_panel_definition"

SRC_PATHOLOGY_PANEL_GENE = "3_lookup.omop.pathology_panel_gene"

SRC_CANCER_DRUG_MAP = "3_lookup.omop.cancer_treatment_term_map"

SRC_BLOODTRACK_BLOOD_GROUP_MAP = "3_lookup.omop.bloodtrack_blood_group_map"

SRC_BLOODTRACK_PRODUCT_GROUP_MAP = "3_lookup.omop.bloodtrack_product_group_map"

def _concept_map_projection(
    df,
    source_system,
    source_code,
    source_display,
    target_system,
    target_code,
    target_display,
    target_domain,
    map_source,
    source_table,
    mapping_rule_id,
    mapping_rank,
    valid_from,
    valid_to,
    loaded_at,
    *,
    review_status_col=None,
):
    # Normalize source and target coding systems and project mapping evidence into the shared
    # concept-map schema.
    staged = df.select(
        canonical_coding_system(source_system).alias("source_coding_system"),
        source_code.cast("string").alias("source_code"),
        source_display.cast("string").alias("source_display"),
        canonical_coding_system(target_system).alias("target_coding_system"),
        target_code.cast("string").alias("target_code"),
        target_display.cast("string").alias("target_display"),
        target_domain.cast("string").alias("target_domain"),
        map_source.cast("string").alias("map_source"),
        source_table.cast("string").alias("source_table"),
        F.lit(None).cast("string").alias("map_version"),
        (F.lit("source_carried") if review_status_col is None else review_status_col)
        .cast("string").alias("review_status"),
        mapping_rule_id.cast("string").alias("mapping_rule_id"),
        mapping_rank.cast("int").alias("mapping_rank"),
        valid_from.cast("date").alias("valid_from"),
        valid_to.cast("date").alias("valid_to"),
        loaded_at.cast("timestamp").alias("loaded_at"),
    ).where(
        F.col("source_coding_system").isNotNull()
        & F.col("source_code").isNotNull()
        & F.col("target_coding_system").isNotNull()
        & F.col("target_code").isNotNull()
    )
    grouped = (
        staged.groupBy(
            "source_coding_system", "source_code",
            "target_coding_system", "target_code", "target_domain",
            "map_source", "source_table", "map_version", "mapping_rule_id", "mapping_rank",
            "review_status", "valid_from", "valid_to",
        )
        .agg(
            F.max("source_display").alias("source_display"),
            F.max("target_display").alias("target_display"),
            F.count(F.lit(1)).cast("long").alias("source_row_count"),
            F.max("loaded_at").alias("loaded_at"),
        )
    )
    grouped = (grouped
        .withColumn("map_method",
            F.when((F.col("source_coding_system") == F.col("target_coding_system")) &
                   (F.col("source_code") == F.col("target_code")), F.lit("source_native"))
             .otherwise(F.lit("rule")))
        .withColumn("map_confidence", F.lit(None).cast("double"))
        .withColumn("map_rule_id", F.col("mapping_rule_id"))
        .withColumn("map_candidate_count", F.count(F.lit(1)).over(Window.partitionBy(
            "source_coding_system", "source_code", "map_source")).cast("int")))
    # contract v2: name the deterministic terminology-map identity concept_map_key
    return grouped.select(
        stable_id(
            "terminology-map",
            F.col("source_coding_system"), F.col("source_code"),
            F.col("target_coding_system"), F.col("target_code"),
            F.col("target_domain"), F.col("map_source"),
            F.col("map_version"), F.col("mapping_rule_id"), F.col("mapping_rank"),
            F.col("valid_from"), F.col("valid_to"),
        ).alias("concept_map_key"),
        "source_coding_system", "source_code", "source_display",
        "target_coding_system", "target_code", "target_display", "target_domain",
        F.lit(None).cast("string").alias("equivalence"),
        "map_source", "map_version", "mapping_rule_id", "mapping_rank",
        "map_method", "map_confidence", "map_rule_id", "map_candidate_count",
        "review_status",
        "valid_from", "valid_to",
        "source_table",
        "source_row_count", "loaded_at",
        )

def _union_frames(frames):
    # Stack DataFrames by column name, preserving all rows from every supplied frame.
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result

def _concept_map_projection_all():
    # Combine the supported diagnosis, procedure and other terminology feeds into the common
    # concept-map projection.
    d = read_source(SRC_DIAGNOSIS_MAPPING)
    diagnosis_source_system = F.coalesce(
        d.source_vocabulary_desc,
        d.CONCEPT_CKI_SOURCE,
        F.lit("urn:cerner:nomenclature"),
    )
    diagnosis_source_code = F.coalesce(
        d.SOURCE_IDENTIFIER,
        d.CONCEPT_CKI_IDENTIFIER,
        d.NOMENCLATURE_ID.cast("string"),
    )
    diagnosis_source_display = F.coalesce(
        d.SOURCE_STRING, d.DIAGNOSIS_DISPLAY, d.DIAGNOSIS_TEXT
    )
    no_date = F.lit(None).cast("date")
    no_rank = F.lit(None).cast("int")
    mappings = [
        _concept_map_projection(
            d, diagnosis_source_system, diagnosis_source_code, diagnosis_source_display,
            F.lit("http://snomed.info/sct"), d.SNOMED_CODE, d.SNOMED_TERM,
            F.lit("condition"), F.lit("bronze.map_diagnosis:snomed"),
            F.lit(SRC_DIAGNOSIS_MAPPING),
            F.lit("diagnosis_snomed"), d.SNOMED_MATCH_NUMBER, no_date, no_date, d.ADC_UPDT,
        ),
        _concept_map_projection(
            d, diagnosis_source_system, diagnosis_source_code, diagnosis_source_display,
            F.lit("http://hl7.org/fhir/sid/icd-10"), d.ICD10_CODE, d.ICD10_TERM,
            F.lit("condition"), F.lit("bronze.map_diagnosis:icd10"),
            F.lit(SRC_DIAGNOSIS_MAPPING),
            F.lit("diagnosis_icd10"), d.ICD10_MATCH_NUMBER, no_date, no_date, d.ADC_UPDT,
        ),
        _concept_map_projection(
            d, diagnosis_source_system, diagnosis_source_code, diagnosis_source_display,
            F.lit("urn:omop:concept_id"), d.OMOP_CONCEPT_ID, d.OMOP_CONCEPT_NAME,
            F.coalesce(F.lower(d.OMOP_CONCEPT_DOMAIN), F.lit("condition")),
            F.lit("bronze.map_diagnosis:omop"), F.lit(SRC_DIAGNOSIS_MAPPING),
            F.lit("diagnosis_omop"),
            d.OMOP_MATCH_NUMBER, no_date, no_date, d.ADC_UPDT,
        ),
    ]

    b = read_source(SRC_CODED_EVENT_MAPPING)
    mappings.append(
        _concept_map_projection(
            b,
            F.concat(F.lit("urn:cerner:coded-event:"),
                     F.lower(F.coalesce(b.SOURCE_FIELD, F.lit("unknown")))),
            b.SOURCE_VALUE,
            b.MATCHED_SOURCE_VARIANT,
            F.lit("urn:omop:concept_id"),
            b.OMOP_CONCEPT_ID,
            b.OMOP_CONCEPT_NAME,
            F.coalesce(F.lower(b.OMOP_CONCEPT_DOMAIN), F.lower(b.OMOP_TABLE), F.lit("unknown")),
            F.lit("bronze.map_coded_events_omop_bridge:omop"),
            F.lit(SRC_CODED_EVENT_MAPPING),
            b.MAPPING_RULE_ID,
            b.MAPPING_RANK,
            b.OMOP_VALID_START_DATE,
            b.OMOP_VALID_END_DATE,
            F.coalesce(b.MAPPING_ADC_UPDT, b.PIPELINE_UPDT_DT_TM),
        )
    )

    p = read_source(SRC_PATHOLOGY_MAPPING)
    pathology_source_system = F.coalesce(
        p.code_system, F.lit("urn:barts:pathology:event-code")
    )
    pathology_source_code = F.coalesce(p.code, p.EVENT_CD.cast("string"))
    pathology_source_display = F.coalesce(p.description, p.EVENT_CD_DISPLAY)
    result_source_system = F.concat(
        F.lit("urn:barts:pathology:result-value:"), pathology_source_code
    )
    pathology_loaded_at = F.coalesce(p.mapping_updated_at, p.ADC_UPDT, p.source_adc_updt)
    mappings.extend([
        _concept_map_projection(
            p, pathology_source_system, pathology_source_code, pathology_source_display,
            F.lit("http://snomed.info/sct"), p.test_snomed_code, p.description,
            F.lit("measurement"), F.lit("bronze.map_pathology:test-snomed"),
            F.lit(SRC_PATHOLOGY_MAPPING),
            p.test_confidence_tier, no_rank, no_date, no_date, pathology_loaded_at,
        ),
        _concept_map_projection(
            p, pathology_source_system, pathology_source_code, pathology_source_display,
            F.lit("http://loinc.org"), p.test_loinc_code, p.description,
            F.lit("measurement"), F.lit("bronze.map_pathology:test-loinc"),
            F.lit(SRC_PATHOLOGY_MAPPING),
            p.test_confidence_tier, no_rank, no_date, no_date, pathology_loaded_at,
        ),
        _concept_map_projection(
            p, pathology_source_system, pathology_source_code, pathology_source_display,
            F.lit("urn:omop:concept_id"), p.test_omop_concept_id, p.measurement_concept_name,
            F.lit("measurement"), F.lit("bronze.map_pathology:test-omop"),
            F.lit(SRC_PATHOLOGY_MAPPING),
            p.test_confidence_tier, no_rank, no_date, no_date, pathology_loaded_at,
        ),
        _concept_map_projection(
            p, result_source_system, p.value_source_value, p.value_source_value,
            F.lit("http://snomed.info/sct"), p.result_snomed_code, p.result_concept_name,
            F.lit("measurement_value"), F.lit("bronze.map_pathology:result-snomed"),
            F.lit(SRC_PATHOLOGY_MAPPING),
            p.result_mapping_match_type, no_rank, no_date, no_date, pathology_loaded_at,
        ),
        _concept_map_projection(
            p, result_source_system, p.value_source_value, p.value_source_value,
            F.lit("http://loinc.org"), p.result_loinc_code, p.result_concept_name,
            F.lit("measurement_value"), F.lit("bronze.map_pathology:result-loinc"),
            F.lit(SRC_PATHOLOGY_MAPPING),
            p.result_mapping_match_type, no_rank, no_date, no_date, pathology_loaded_at,
        ),
        _concept_map_projection(
            p, result_source_system, p.value_source_value, p.value_source_value,
            F.lit("urn:omop:concept_id"), p.result_omop_concept_id, p.result_concept_name,
            F.lit("measurement_value"), F.lit("bronze.map_pathology:result-omop"),
            F.lit(SRC_PATHOLOGY_MAPPING),
            p.result_mapping_match_type, no_rank, no_date, no_date, pathology_loaded_at,
        ),
        _concept_map_projection(
            p, F.lit("urn:barts:pathology:unit"), p.unit_source_value, p.unit_source_value,
            F.lit("http://unitsofmeasure.org"), p.ucum_code, p.ucum_code,
            F.lit("unit"), F.lit("bronze.map_pathology:unit-ucum"),
            F.lit(SRC_PATHOLOGY_MAPPING),
            p.unit_mapping_match_type, no_rank, no_date, no_date, pathology_loaded_at,
        ),
        _concept_map_projection(
            p, F.lit("urn:barts:pathology:unit"), p.unit_source_value, p.unit_source_value,
            F.lit("urn:omop:concept_id"), p.unit_concept_id, p.ucum_code,
            F.lit("unit"), F.lit("bronze.map_pathology:unit-omop"),
            F.lit(SRC_PATHOLOGY_MAPPING),
            p.unit_mapping_match_type, no_rank, no_date, no_date, pathology_loaded_at,
        ),
    ])
    xa = read_source(SRC_PATHOLOGY_CROSS_ARM_TEST_MAP)
    mappings.append(
        _concept_map_projection(
            xa, F.lit("urn:barts:pathology:wkg-tlc"), xa.wkg_code, xa.wkg_code,
            F.lit("urn:cerner:order-mnemonic"), xa.order_mnemonic, xa.order_mnemonic,
            F.lit("measurement"), F.lit("lookup.pathology_cross_arm_test_map"),
            F.lit(SRC_PATHOLOGY_CROSS_ARM_TEST_MAP),
            F.concat(F.lit("tfc:"), F.coalesce(xa.tfc_code.cast("string"), F.lit("~"))),
            no_rank, xa.valid_from, xa.valid_to, xa.ADC_UPDT,
            review_status_col=xa.status,
        )
    )
    hg = read_source(SRC_PATHOLOGY_HGNC_GENE)
    mappings.append(
        _concept_map_projection(
            hg, F.lit("urn:barts:pathology:gene-symbol"), hg.approved_symbol,
            hg.approved_name, F.lit("urn:hgnc"), hg.hgnc_id, hg.approved_symbol,
            F.lit("gene"), F.lit("lookup.pathology_hgnc_gene"),
            F.lit(SRC_PATHOLOGY_HGNC_GENE),
            hg.reference_release, no_rank, no_date, no_date, hg.ADC_UPDT,
        )
    )
    ha = read_source(SRC_PATHOLOGY_HGNC_ALIAS)
    mappings.append(
        _concept_map_projection(
            ha, F.lit("urn:barts:pathology:gene-symbol"), ha.alias_symbol,
            ha.alias_symbol, F.lit("urn:hgnc"), ha.hgnc_id, ha.approved_symbol,
            F.lit("gene"), F.lit("lookup.pathology_hgnc_alias"),
            F.lit(SRC_PATHOLOGY_HGNC_ALIAS),
            ha.alias_type, no_rank, no_date, no_date, ha.ADC_UPDT,
            review_status_col=F.when(F.coalesce(ha.ambiguous_ind, F.lit(False)),
                                     F.lit("ambiguous")).otherwise(F.lit("source_carried")),
        )
    )
    am = read_source(SRC_PATHOLOGY_ANTIMICROBIAL_MAP)
    mappings.append(
        _concept_map_projection(
            am, am.code_system, am.code, am.antimicrobial_text,
            F.lit("urn:omop:concept_id"), am.antimicrobial_omop_concept_id,
            am.antimicrobial_text, F.lit("drug"),
            F.lit("lookup.pathology_antimicrobial_map"),
            F.lit(SRC_PATHOLOGY_ANTIMICROBIAL_MAP),
            am.method, no_rank, no_date, no_date, am.ADC_UPDT,
            review_status_col=am.status,
        )
    )
    pdef = read_source(SRC_PATHOLOGY_PANEL_DEFINITION)
    mappings.append(
        _concept_map_projection(
            pdef, F.lit("urn:barts:pathology:panel-code"),
            F.concat_ws("@", pdef.panel_code, pdef.panel_version), pdef.panel_name,
            F.lit("urn:barts:pathology:assay-code"), pdef.source_assay_code,
            pdef.panel_name, F.lit("measurement"),
            F.lit("lookup.pathology_panel_definition"),
            F.lit(SRC_PATHOLOGY_PANEL_DEFINITION),
            pdef.analysis_context, no_rank, pdef.effective_from, pdef.effective_to, pdef.ADC_UPDT,
        )
    )
    pgn = read_source(SRC_PATHOLOGY_PANEL_GENE)
    mappings.append(
        _concept_map_projection(
            pgn, F.lit("urn:barts:pathology:panel-code"),
            F.concat_ws("@", pgn.panel_code, pgn.panel_version), pgn.panel_code,
            F.lit("urn:hgnc"), pgn.hgnc_id, pgn.gene_symbol,
            F.lit("gene"), F.lit("lookup.pathology_panel_gene"),
            F.lit(SRC_PATHOLOGY_PANEL_GENE),
            pgn.test_scope, no_rank, no_date, no_date, pgn.ADC_UPDT,
        )
    )
    l = read_source(SRC_LUNA_CODE_MAPPING)
    mappings.append(
        _concept_map_projection(
            l, l.source_coding_system, l.source_code, l.source_display,
            l.target_coding_system, l.target_code, l.target_display, l.target_domain,
            F.concat(F.lit("lookup.luna_code_map:"), l.mapping_rule_id),
            F.lit(SRC_LUNA_CODE_MAPPING),
            l.mapping_rule_id, l.mapping_rank, no_date, no_date, l.updated_at,
        )
    )
    sm = read_source(SRC_SEED_CODE_MAPPING)
    mappings.append(
        _concept_map_projection(
            sm, sm.source_coding_system, sm.source_code, sm.source_display,
            sm.target_coding_system, sm.target_code, sm.target_display, sm.target_domain,
            F.concat(F.lit("lookup.seed_code_map:"), sm.mapping_rule_id),
            F.lit(SRC_SEED_CODE_MAPPING),
            sm.mapping_rule_id, sm.mapping_rank, no_date, no_date, sm.updated_at,
            review_status_col=sm.review_status,
        )
    )
    iw = read_source(SRC_IWEB_VALUE_MAP)
    mappings.append(
        _concept_map_projection(
            iw,
            F.concat(F.lit("urn:iweb:"), F.lower(iw.SOURCE_TABLE), F.lit(":"),
                     F.lower(iw.FIELD_NAME)),
            iw.CODE, iw.LABEL,
            _iweb_target_system(iw.TARGET_VOCABULARY),
            F.coalesce(iw.TARGET_CODE, iw.TARGET_CONCEPT_ID.cast("string")),
            iw.TARGET_CONCEPT_NAME, F.lit("registry"),
            F.concat(F.lit("lookup.iweb_value_to_concept:"), iw.MAPPING_STATUS),
            F.lit(SRC_IWEB_VALUE_MAP), iw.MAPPING_METHOD, no_rank,
            no_date, no_date, iw.CURATED_AT,
            review_status_col=iw.MAPPING_STATUS,
        )
    )
    ct = read_source(SRC_CANCER_DRUG_MAP)
    mappings.append(
        _concept_map_projection(
            ct, F.lit("urn:barts:sact:drug-token"), ct.drug_token, ct.drug_token,
            F.lit("urn:omop:concept_id"), ct.drug_concept_id, ct.drug_concept_name,
            F.lit("drug"),
            F.concat(F.lit("lookup.cancer_treatment_term_map:"), ct.mapping_status),
            F.lit(SRC_CANCER_DRUG_MAP), ct.mapping_status, no_rank,
            no_date, no_date, ct.ADC_UPDT,
            review_status_col=ct.mapping_status,
        )
    )
    bg = read_source(SRC_BLOODTRACK_BLOOD_GROUP_MAP)
    mappings.append(
        _concept_map_projection(
            bg, F.lit("urn:bloodtrack:blood-group"), bg.BLOOD_GROUP_SOURCE_VALUE,
            bg.BLOOD_GROUP_SOURCE_VALUE, F.lit("http://snomed.info/sct"),
            bg.SNOMED_CONCEPT_ID, bg.SNOMED_CONCEPT_NAME, F.lit("observation"),
            F.concat(F.lit("lookup.bloodtrack_blood_group_map:"), bg.MAPPING_STATUS),
            F.lit(SRC_BLOODTRACK_BLOOD_GROUP_MAP), bg.MAPPING_METHOD, no_rank,
            no_date, no_date, bg.CURATED_AT,
            review_status_col=bg.MAPPING_STATUS,
        )
    )
    pg = read_source(SRC_BLOODTRACK_PRODUCT_GROUP_MAP)
    mappings.extend([
        _concept_map_projection(
            pg, F.lit("urn:bloodtrack:product-group"), pg.BLOOD_PRODUCT_GROUP,
            pg.BLOOD_PRODUCT_GROUP, F.lit("http://snomed.info/sct"),
            pg.PRODUCT_CONCEPT_ID, pg.PRODUCT_CONCEPT_NAME, F.lit("device"),
            F.concat(F.lit("lookup.bloodtrack_product_group_map:"), pg.MAPPING_STATUS),
            F.lit(SRC_BLOODTRACK_PRODUCT_GROUP_MAP), pg.MAPPING_METHOD, no_rank,
            no_date, no_date, pg.CURATED_AT,
            review_status_col=pg.MAPPING_STATUS,
        ),
        _concept_map_projection(
            pg, F.lit("urn:bloodtrack:product-group"), pg.BLOOD_PRODUCT_GROUP,
            pg.BLOOD_PRODUCT_GROUP, F.lit("http://snomed.info/sct"),
            pg.PROC_CONCEPT_ID, pg.PROC_CONCEPT_NAME, F.lit("procedure"),
            F.concat(F.lit("lookup.bloodtrack_product_group_map:"), pg.MAPPING_STATUS),
            F.lit(SRC_BLOODTRACK_PRODUCT_GROUP_MAP), pg.MAPPING_METHOD, no_rank,
            no_date, no_date, pg.CURATED_AT,
            review_status_col=pg.MAPPING_STATUS,
        ),
    ])
    md = read_source(SRC_MEDICONNECT_TYPE_MAP)
    mappings.append(
        _concept_map_projection(
            md, F.lit("urn:barts:mediconnect:device-type"), md.DEVICE_TYPE,
            md.SOURCE_LABEL, F.lit("http://snomed.info/sct"),
            md.SNOMED_CONCEPT_ID, md.SNOMED_CONCEPT_NAME, F.lit("device"),
            F.concat(F.lit("lookup.mediconnect_device_type_map:"), md.MAPPING_STATUS),
            F.lit(SRC_MEDICONNECT_TYPE_MAP), md.MAPPING_METHOD, no_rank,
            no_date, no_date, md.CURATED_AT,
            review_status_col=md.MAPPING_STATUS,
        )
    )
    return _union_frames(mappings)

CONCEPT_MAP_COLUMN_COMMENTS = {
    "concept_map_key": "Deterministic SHA-256 terminology-map row key.",
    "source_coding_system": "Source code-system namespace.",
    "source_code": "Source code or mapped source value.",
    "source_display": "Source display carried from bronze.",
    "target_coding_system": "Target code-system namespace.",
    "target_code": "Target code or concept identifier.",
    "target_display": "Target display carried from bronze.",
    "target_domain": "Intended downstream semantic domain.",
    "equivalence": "FHIR equivalence when explicitly supplied; null rather than inferred.",
    "map_source": "Governed mapping product and route.",
    "map_version": "Pinned mapping release.",
    "mapping_rule_id": "Source mapping rule or confidence discriminator.",
    "mapping_rank": "Source-supplied candidate rank where available.",
    "map_method": "How the mapping was reached: source_native or rule for legacy reference mappings.",
    "map_confidence": "Mapping confidence in [0,1] where the source supplied one.",
    "map_rule_id": "Stable rule identifier copied from mapping_rule_id for the S3 flat contract.",
    "map_candidate_count": "Number of target candidates for the source code and mapping source.",
    "review_status": "Review state carried verbatim from the mapping source; source_carried rows are bronze-carried without review",
    "valid_from": "Target mapping validity start where supplied.",
    "valid_to": "Target mapping validity end where supplied.",
    "source_table": "Configured bronze source or explicitly authorized development fixture.",
    "source_row_count": "Number of bronze mapping observations represented by the row.",
    "loaded_at": "Maximum contributing clock within each source/target mapping group, with no further clock aggregation across the unioned mapping arms. Diagnosis and the ADC-backed lookup arms use ADC_UPDT; coded-event mappings first coalesce MAPPING_ADC_UPDT with PIPELINE_UPDT_DT_TM; pathology first coalesces mapping_updated_at, ADC_UPDT and source_adc_updt. LUNA and seed maps use updated_at; iWeb, BloodTrack and Mediconnect maps use CURATED_AT. Each chosen clock is cast to TIMESTAMP before the group maximum. These are mixed ingestion/mapping-update clocks, not a uniform native modification time or Silver refresh time.",
}

CONCEPT_MAP_LIFECYCLE_FIELDS = [
]

CONCEPT_MAP_RETIRED_COLUMNS = [

]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_concept_map():
    # Assemble concept map rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return _concept_map_projection_all()

@materialized_view(
    name=_n("journey_reference.concept_map"),
    comment="Versioned one-to-many source-to-target mappings observed in governed bronze products; no winner is selected.",
    refresh_policy="incremental",
    column_comments=CONCEPT_MAP_COLUMN_COMMENTS,
)
def concept_map():
    # Build the declared dataset: Versioned one-to-many source-to-target mappings observed in
    # governed bronze products; no winner is selected.
    return _lifecycle_source_concept_map().drop(*CONCEPT_MAP_LIFECYCLE_FIELDS, *CONCEPT_MAP_RETIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / concept registry

# COMMAND ----------

CONCEPT_REGISTRY_COLUMN_COMMENTS = {
    "concept_registry_key": "Deterministic SHA-256 observed-concept key.",
    "coding_system": "Identifier namespace for the observed source or target code, taken from event or mapping evidence; event codes with no source namespace use urn:unknown.",
    "code": "OMOP standard vocabulary concept identifier assigned to the mapped source value.",
    "preferred_display": "Deterministically selected observed display.",
    "status": "Registry observation status.",
    "source_use_count": "Source-side event and mapping observation count.",
    "target_use_count": "Target-side mapping observation count.",
    "first_observed_at": "Earliest represented bronze observation timestamp.",
    "last_observed_at": "Latest represented bronze observation timestamp.",
}

@materialized_view(
    name=_n("journey_reference.concept_registry"),
    comment="Observed source and target coding-system/code pairs across the event index and concept map.",
    refresh_policy="incremental",
    column_comments=CONCEPT_REGISTRY_COLUMN_COMMENTS,
)
def concept_registry():
    # Build the declared dataset: Observed source and target coding-system/code pairs across the
    # event index and concept map.
    mappings = _lifecycle_source_concept_map()
    events = spark.read.table(_n("journey_events._event_index"))
    event_mappings = spark.read.table(_n("journey_events._event_map"))
    event_codes = events.where(events.source_code.isNotNull()).select(
        F.coalesce(events.source_coding_system, F.lit("urn:unknown")).alias("coding_system"),
        events.source_code.alias("code"),
        events.source_display.alias("display"),
        F.lit(1).cast("long").alias("source_use_count"),
        F.lit(0).cast("long").alias("target_use_count"),
        events.loaded_at.alias("observed_at"),
    )
    map_sources = mappings.select(
        F.col("source_coding_system").alias("coding_system"),
        F.col("source_code").alias("code"),
        F.col("source_display").alias("display"),
        F.col("source_row_count").cast("long").alias("source_use_count"),
        F.lit(0).cast("long").alias("target_use_count"),
        F.col("loaded_at").alias("observed_at"),
    )
    map_targets = mappings.select(
        F.col("target_coding_system").alias("coding_system"),
        F.col("target_code").alias("code"),
        F.col("target_display").alias("display"),
        F.lit(0).cast("long").alias("source_use_count"),
        F.col("source_row_count").cast("long").alias("target_use_count"),
        F.col("loaded_at").alias("observed_at"),
    )
    event_map_targets = event_mappings.select(
        F.col("mapped_coding_system").alias("coding_system"),
        F.col("mapped_code").alias("code"),
        F.col("mapped_display").alias("display"),
        F.lit(0).cast("long").alias("source_use_count"),
        F.lit(1).cast("long").alias("target_use_count"),
        F.col("loaded_at").alias("observed_at"),
    )
    grouped = (
        event_codes.unionByName(map_sources).unionByName(map_targets).unionByName(event_map_targets)
        .withColumn("coding_system", canonical_coding_system(F.col("coding_system")))
        .groupBy("coding_system", "code")
        .agg(
            F.max("display").alias("preferred_display"),
            F.sum("source_use_count").cast("long").alias("source_use_count"),
            F.sum("target_use_count").cast("long").alias("target_use_count"),
            F.min("observed_at").alias("first_observed_at"),
            F.max("observed_at").alias("last_observed_at"),
        )
    )
    # contract v2: name the deterministic observed-concept identity concept_registry_key
    return grouped.select(
        stable_id("terminology-concept", F.col("coding_system"), F.col("code"))
          .alias("concept_registry_key"),
        "coding_system", "code", "preferred_display",
        F.lit("observed").alias("status"),
        "source_use_count", "target_use_count",
        "first_observed_at", "last_observed_at",
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / value set

# COMMAND ----------

SRC_VALUE_SET_RELEASE = "3_lookup.omop.value_set_release"

VALUE_SET_COLUMN_COMMENTS = {
    "value_set_key": "Deterministic SHA-256 value-set membership key.",
    "canonical_url": "Canonical value-set URL.",
    "version": "Pinned value-set version.",
    "member_system": "Member coding system.",
    "member_code": "Source-system code representing member for the value set record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "member_display": "Human-readable member label for the value set record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "status": "Value-set membership status.",
    "source_table": "Governed bronze value-set source.",
    "source_row_id": "Stable source membership identity.",
    "loaded_at": "3_lookup.omop.value_set_release.updated_at carried unchanged for the value-set membership row. This is the lookup's update timestamp, not a bronze ADC_UPDT clock, release identifier or Silver refresh time; a missing value remains null.",
}

VALUE_SET_LIFECYCLE_FIELDS = [
    'load_batch_id',
]

VALUE_SET_RETIRED_COLUMNS = [

]

VALUE_SET_LIFECYCLE_COLUMNS = [
    'value_set_key',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_value_set():
    # Assemble value set rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    r = read_source(SRC_VALUE_SET_RELEASE)
    # contract v2: name the deterministic value-set membership identity value_set_key
    return r.select(
        stable_id(
            "value-set-member", r.canonical_url, r.value_set_version,
            r.member_system, r.member_code,
        ).alias("value_set_key"),
        r.canonical_url.alias("canonical_url"),
        r.value_set_version.alias("version"),
        r.member_system.alias("member_system"),
        r.member_code.alias("member_code"),
        r.member_display.alias("member_display"),
        r.membership_status.alias("status"),
        F.lit(SRC_VALUE_SET_RELEASE).alias("source_table"),
        F.concat_ws("|", r.canonical_url, r.member_system, r.member_code)
         .alias("source_row_id"),
        r.release_id.alias("load_batch_id"),
        r.updated_at.alias("loaded_at"),
    )

@materialized_view(
    name=_n("journey_reference.value_set"),
    comment="Pinned value-set membership from the governed value_set_release lookup asset; "
            "v3-ServiceDeliveryLocationRoleType is a recorded v1 exclusion.",
    refresh_policy="incremental",
    column_comments=VALUE_SET_COLUMN_COMMENTS,
)
def value_set():
    # Build the declared dataset: Pinned value-set membership from the governed
    # value_set_release lookup asset; v3-ServiceDeliveryLocationRoleType is a recorded v1
    # exclusion.
    return _lifecycle_source_value_set().drop(*VALUE_SET_LIFECYCLE_FIELDS, *VALUE_SET_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_reference._value_set_metadata'),
    comment='Internal quality and batch metadata for reference_value_set; same row grain as the research table. Join keys: value_set_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def value_set_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for reference_value_set;
    # same row grain as the research table. Join keys: value_set_key.
    return (_lifecycle_source_value_set()).select(*VALUE_SET_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / research study

# COMMAND ----------

SRC_RESEARCH_STUDY = "4_prod.bronze.map_research_study"

RESEARCH_STUDY_COLUMN_COMMENTS = {
    "research_study_key": "Deterministic SHA-256 key for the Cerner research protocol.",
    "source_protocol_id": "Source-system identifier for the source protocol associated with each research study record. It is derived from bronze field `PROT_MASTER_ID` in `4_prod.bronze.map_research_study`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "study_mnemonic": "Mnemonic assigned to the protocol",
    "study_mnemonic_key": "Mnemonic of the protocol stripped of special characters",
    "protocol_type_code": "This field contains a code identifying the type of protocol; therapeutic, non-therapeutic, BCM guidelines etc.",
    "protocol_type_desc": "The description for the code value",
    "protocol_phase_code": "This field contains a code for the phase of the protocol.  Examples of phases would include, but not be limited to, phase 1, phase 1A, phase 1B, phase 2, phase 3, etc.",
    "protocol_phase_desc": "The description for the code value",
    "protocol_status_code": "This field contains a code identifying the status of the protocol/study: approved, open, suspended (temporarily closed to accrual), closed to accrual, closed and terminated.",
    "protocol_status_desc": "The description for the code value",
    "protocol_purpose_code": "This field contains a code identifying the purpose for the protocol/study.  Examples of purposes would include, but not be limited to, cancer control, epidemiology, etc.",
    "protocol_purpose_desc": "The description for the code value",
    "parent_protocol_id": "Source-system identifier for the parent protocol associated with each research study record. It is derived from bronze field `PARENT_PROT_MASTER_ID` in `4_prod.bronze.map_research_study`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "previous_protocol_id": "Source-system identifier for the previous protocol associated with each research study record. It is derived from bronze field `PREV_PROT_MASTER_ID` in `4_prod.bronze.map_research_study`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "root_protocol_ind": "Indicator of whether root protocol applies to the research study record. It is derived from bronze field `PARENT_PROT_MASTER_ID` in `4_prod.bronze.map_research_study`, selected as the provenance anchor from 2 recorded bronze source fields. Null means the source did not state the indicator and must not be interpreted as false.",
    "beg_effective": "The date and time for which this table row becomes effective.  Normally, this will be the date and time the row is added, but could be a past or future date and time.",
    "end_effective": "The date/time after which the row is no longer valid as active current data.  This may be valued with the date that the row became inactive.",
    "open_ended_ind": "The date/time after which the row is no longer valid as active current data.  This may be valued with the date that the row became inactive.",
    "display_ind": "This field is an indicator that tells whether the protocol enrollment information for this protocol will be displayed in the electronic medical record.",
    "_source_system": "Value describing source system for the research study record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the research study record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Source-system identifier for the source row associated with each research study record. It is derived from bronze field `PROT_MASTER_ID` in `4_prod.bronze.map_research_study`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "record_status": "Always active in this projection. Protocol status, display flag, begin/end-effective dates and amendment parent/previous links do not determine this literal label; it does not assert that a study is open or recruiting.",
    "loaded_at": "map_research_study.ADC_UPDT carried unchanged for the protocol/amendment row. No parent or previous-protocol timestamp is joined; this is bronze ingestion provenance, not protocol validity time or Silver refresh time.",
}

RESEARCH_STUDY_LIFECYCLE_FIELDS = [
]

RESEARCH_STUDY_RETIRED_COLUMNS = [

]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_research_study():
    # Assemble research study rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    s = read_source(SRC_RESEARCH_STUDY)
    # contract v2: rename the deterministic research-study identity as a key
    return s.select(
        stable_id("research_study:cerner", s.PROT_MASTER_ID).alias("research_study_key"),
        s.PROT_MASTER_ID.cast("string").alias("source_protocol_id"),
        s.STUDY_MNEMONIC.alias("study_mnemonic"), s.STUDY_MNEMONIC_KEY.alias("study_mnemonic_key"),
        s.PROT_TYPE_CD.cast("long").alias("protocol_type_code"),
        s.PROT_TYPE_DESC.alias("protocol_type_desc"),
        s.PROT_PHASE_CD.cast("long").alias("protocol_phase_code"),
        s.PROT_PHASE_DESC.alias("protocol_phase_desc"),
        s.PROT_STATUS_CD.cast("long").alias("protocol_status_code"),
        s.PROT_STATUS_DESC.alias("protocol_status_desc"),
        s.PROT_PURPOSE_CD.cast("long").alias("protocol_purpose_code"),
        s.PROT_PURPOSE_DESC.alias("protocol_purpose_desc"),
        s.PARENT_PROT_MASTER_ID.cast("string").alias("parent_protocol_id"),
        s.PREV_PROT_MASTER_ID.cast("string").alias("previous_protocol_id"),
        (s.PARENT_PROT_MASTER_ID == s.PROT_MASTER_ID).alias("root_protocol_ind"),
        s.BEG_EFFECTIVE_DT_TM.alias("beg_effective"), s.END_EFFECTIVE_DT_TM.alias("end_effective"),
        (s.END_EFFECTIVE_DT_TM >= F.lit("2100-01-01").cast("timestamp")).alias("open_ended_ind"),
        s.DISPLAY_IND.cast("long").alias("display_ind"), F.lit("active").alias("record_status"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("cerner-research").alias("_source_system"),
        F.lit(SRC_RESEARCH_STUDY).alias("_source_table"),
        s.PROT_MASTER_ID.cast("string").alias("_source_row_id"),
    )

@materialized_view(
    name=_n("journey_reference.research_study"),
    comment="Cerner research protocol amendments with parent and previous protocol lineage.",
    refresh_policy="incremental",
    column_comments=RESEARCH_STUDY_COLUMN_COMMENTS,
)
def research_study():
    # Build the declared dataset: Cerner research protocol amendments with parent and previous
    # protocol lineage.
    return _lifecycle_source_research_study().drop(*RESEARCH_STUDY_LIFECYCLE_FIELDS, *RESEARCH_STUDY_RETIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Research enrollment canonical

# COMMAND ----------

def _research_enrollment_canonical():
    # Assemble normalized research enrollment rows for downstream dataset builders, preserving
    # the existing source and identity rules.
    s = read_source(SRC_RESEARCH_SUBJECT)
    event_id = stable_id("research_enrollment:cerner", s.PT_PROT_REG_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_RESEARCH_SUBJECT, s.PT_PROT_REG_ID
    )
    # contract v2: publish the native Cerner registration id and native person/encounter ids while retaining SHA event and study keys
    return s.select(
        event_id.alias("patient_event_key"),
        s.PT_PROT_REG_ID.cast("bigint").alias("pt_prot_reg_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"), F.lit("resolved").alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        s.ON_STUDY_DT_TM_CLEAN.alias("event_datetime"),
        s.OFF_STUDY_DT_TM_CLEAN.alias("event_end_datetime"),
        F.lit("urn:cerner:research-status").alias("source_coding_system"),
        s.STATUS_ENUM.cast("long").cast("string").alias("source_code"),
        s.STATUS_DESC.alias("source_display"),
        stable_id("research_study:cerner", s.PROT_MASTER_ID).alias("research_study_key"),
        s.REG_ID.cast("string").alias("registration_id"),
        s.PROT_ACCESSION_NBR.alias("protocol_accession_number"),
        s.PROT_ARM_ID.cast("string").alias("protocol_arm_id"),
        s.STATUS_ENUM.cast("long").alias("status_code"), s.STATUS_DESC.alias("status_desc"),
        s.OFF_STUDY_DT_TM_CLEAN.alias("off_study_datetime"),
        s.TX_START_DT_TM_CLEAN.alias("treatment_start_datetime"),
        s.TX_COMPLETION_DT_TM_CLEAN.alias("treatment_completion_datetime"),
        s.REMOVAL_REASON_DESC_CV.alias("removal_reason_desc"),
        s.REMOVAL_REASON_FT.alias("removal_reason_text"),
        s.REASON_OFF_TX_DESC_CV.alias("off_treatment_reason_desc"),
        s.REASON_OFF_TX_FT.alias("off_treatment_reason_text"),
        s.EPISODE_ID.cast("string").alias("source_episode_id"),
        s.ENROLLING_ORGANIZATION_ID.cast("string").alias("enrolling_organization_id"),
        F.lit("active").alias("record_status"),
        s.ON_STUDY_DT_TM_CLEAN.alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("administrative").alias("fact_category"),
        F.lit("research_subject").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.PIPELINE_UPDT_DT_TM.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
        F.lit("cerner-research").alias("_source_system"),
        F.lit(SRC_RESEARCH_SUBJECT).alias("_source_table"),
        s.PT_PROT_REG_ID.cast("string").alias("_source_row_id"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / research enrollment

# COMMAND ----------

SRC_RESEARCH_SUBJECT = "4_prod.bronze.map_research_subject"

# contract v2: retain the full canonical shape for internal reuse.
RESEARCH_ENROLLMENT_SOURCE_COLUMNS = [
    "patient_event_key",
    "pt_prot_reg_id",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "source_coding_system",
    "source_code",
    "source_display",
    "research_study_key",
    "registration_id",
    "protocol_accession_number",
    "protocol_arm_id",
    "status_code",
    "status_desc",
    "off_study_datetime",
    "treatment_start_datetime",
    "treatment_completion_datetime",
    "removal_reason_desc",
    "removal_reason_text",
    "off_treatment_reason_desc",
    "off_treatment_reason_text",
    "source_episode_id",
    "enrolling_organization_id",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "fact_category",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
RESEARCH_ENROLLMENT_PUBLIC_COLUMNS = [
    'patient_event_key',
    'pt_prot_reg_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'research_study_key',
    'registration_id',
    'protocol_accession_number',
    'protocol_arm_id',
    'status_code',
    'status_desc',
    'off_study_datetime',
    'treatment_start_datetime',
    'treatment_completion_datetime',
    'removal_reason_desc',
    'removal_reason_text',
    'off_treatment_reason_desc',
    'off_treatment_reason_text',
    'source_episode_id',
    'enrolling_organization_id',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'fact_category',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

RESEARCH_ENROLLMENT_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'pt_prot_reg_id',
    'identity_status',
    'load_batch_id',
]

RESEARCH_ENROLLMENT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "pt_prot_reg_id": "Cerner PT_PROT_REG_ID; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace used to interpret the subject identifier on this record for each research enrollment record. It is derived from bronze field `PERSON_ID` in `4_prod.bronze.map_research_subject`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date and time when the represented clinical or administrative event occurred for each research enrollment record. It is derived from bronze field `ON_STUDY_DT_TM_CLEAN` in `4_prod.bronze.map_research_subject`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each research enrollment record. It is derived from bronze field `OFF_STUDY_DT_TM_CLEAN` in `4_prod.bronze.map_research_subject`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each research enrollment record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Number to indicate the current status of the enrolled patient(1: On Study,2:OnTreatment, 3:Off Treatment,4-OnFollowup,5-OffStudy)",
    "source_display": "Human-readable label supplied by the source system for the source code for each research enrollment record. It is derived from bronze field `STATUS_DESC` in `4_prod.bronze.map_research_subject`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "research_study_key": "Deterministic SHA-256 key for the related research study.",
    "registration_id": "Source-system identifier for the registration associated with each research enrollment record. It is derived from bronze field `REG_ID` in `4_prod.bronze.map_research_subject`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "protocol_accession_number": "This field contains the patient's accession number (order of enrollment) for this protocol.",
    "protocol_arm_id": "Source-system identifier for the protocol arm associated with each research enrollment record. It is derived from bronze field `PROT_ARM_ID` in `4_prod.bronze.map_research_subject`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "status_code": "Number to indicate the current status of the enrolled patient(1: On Study,2:OnTreatment, 3:Off Treatment,4-OnFollowup,5-OffStudy)",
    "status_desc": "Value describing status desc for the research enrollment record. It is carried from bronze field `STATUS_DESC` in `4_prod.bronze.map_research_subject`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "off_study_datetime": "Date and time associated with off study for the research enrollment record. It is derived from bronze field `OFF_STUDY_DT_TM_CLEAN` in `4_prod.bronze.map_research_subject`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "treatment_start_datetime": "Date and time associated with treatment start for the research enrollment record. It is derived from bronze field `TX_START_DT_TM_CLEAN` in `4_prod.bronze.map_research_subject`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "treatment_completion_datetime": "Date and time associated with treatment completion for the research enrollment record. It is derived from bronze field `TX_COMPLETION_DT_TM_CLEAN` in `4_prod.bronze.map_research_subject`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "removal_reason_desc": "The description for the code value",
    "removal_reason_text": "Textual reason patient is taken off study.",
    "off_treatment_reason_desc": "The description for the code value",
    "off_treatment_reason_text": "Textual reason patient is taken off treatment.",
    "source_episode_id": "Source-system identifier for the source episode associated with each research enrollment record. It is derived from bronze field `EPISODE_ID` in `4_prod.bronze.map_research_subject`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "enrolling_organization_id": "Source-system identifier for the enrolling organization associated with each research enrollment record. It is carried from bronze field `ENROLLING_ORGANIZATION_ID` in `4_prod.bronze.map_research_subject`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each research enrollment record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each research enrollment record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each research enrollment record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each research enrollment record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each research enrollment record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Always active in the current map_research_subject projection. No superseded or retracted status is emitted; STATUS_ENUM, off-study/treatment dates and removal reasons remain separate clinical fields and do not change this literal label.",
    "record_status_effective_from": "map_research_subject.ON_STUDY_DT_TM_CLEAN carried unchanged. This study-entry-time proxy is not an independently observed source-status transition; there is no registration, treatment-start or ingestion-time fallback.",
    "record_status_effective_to": "Always null as a TIMESTAMP in the current research-enrollment projection. OFF_STUDY_DT_TM_CLEAN and treatment/removal fields are not used as history ends; a null here must not be interpreted as evidence that study participation is ongoing.",
    "source_update_timestamp": "Bronze pipeline processing timestamp carried from `PIPELINE_UPDT_DT_TM` in `4_prod.bronze.map_research_subject`. It records when bronze processed the source row, not a native clinical-system update timestamp, clinical event time, or the current Silver refresh time.",
    "loaded_at": "map_research_subject.ADC_UPDT carried unchanged for the contributing research-subject row. The study key is derived without joining a study load clock. This input load provenance is distinct from PIPELINE_UPDT_DT_TM published as source_update_timestamp and from Silver refresh time.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_research_enrollment():
    # Assemble research enrollment rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _research_enrollment_canonical().select(*RESEARCH_ENROLLMENT_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.research_enrollment"),
    comment="One Cerner research-subject registration using clean sentinel-safe dates.",
    cluster_by=["person_id", "event_datetime"], refresh_policy="incremental",
    column_comments=RESEARCH_ENROLLMENT_COLUMN_COMMENTS,
)
def research_enrollment():
    # Build the declared dataset: One Cerner research-subject registration using clean sentinel-
    # safe dates.
    return _lifecycle_source_research_enrollment().select(*RESEARCH_ENROLLMENT_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._research_enrollment_metadata'),
    comment='Internal quality and batch metadata for clinical_research_enrollment; same row grain as the research table. Join keys: patient_event_key, pt_prot_reg_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def research_enrollment_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_research_enrollment; same row grain as the research table. Join keys:
    # patient_event_key, pt_prot_reg_id.
    return (_lifecycle_source_research_enrollment()).select(*RESEARCH_ENROLLMENT_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / person address

# COMMAND ----------

SRC_ADDRESS = "4_prod.bronze.map_address"

SRC_ADDRESS_EPC = "4_prod.bronze.map_address_epc"

PERSON_ADDRESS_COLUMN_COMMENTS = {
    "person_address_key": "Deterministic SHA-256 key for the Millennium address row.",
    "source_address_id": "The address ID is the primary key of the address table.",
    "parent_entity": "The upper case name of the table to which this address row is related (i.e., PERSON, PRSNL, ORGANIZATION, etc.)",
    "person_id": "Native Millennium PERSON_ID as BIGINT for person-owned addresses.",
    "organization_id": "Native Millennium ORGANIZATION_ID as BIGINT for organization-owned addresses.",
    "address_type_code": "Code value identifying the type of address recorded on the source address row.",
    "active_ind": "The table row is active or inactive. A row is generally active unless it is in an inactive state such as logically deleted, combined away, pending purge, etc.",
    "beg_effective": "The date and time for which this table row becomes effective. Normally, this will be the date and time the row is added, but could be a past or future date and time.",
    "end_effective": "The date/time after which the row is no longer valid as active current data.  This may be valued with the date that the row became inactive.",
    "open_ended_ind": "The date/time after which the row is no longer valid as active current data.  This may be valued with the date that the row became inactive.",
    "street_address": "Concatenated street address.",
    "city": "The city field is the text name of the city associated with the address row.",
    "postcode": "Postcode value as held in the source address record, retained for provenance.",
    "postcode_masked": "Partially masked version of the postcode for privacy protection.",
    "postcode_outward": "Outward (first) part of the postcode derived from the source address postcode for privacy-aware geographic analysis.",
    "uprn": "Unique Property Reference Number - unique identifier for every spatial address in Great Britain (1-999999999999).",
    "lsoa": "LSOA stands for Lower Layer Super Output Area, which is a geographic area used for small area statistics in the UK.",
    "msoa": "2021 Census Middle Layer Super Output Area code derived from the address postcode geography lookup.",
    "local_authority_code": "Local Authority District code for the address, derived from the postcode geography lookup.",
    "imd_decile": "IMD_Decile is used to store the Index of Multiple Deprivation (IMD) decile value.",
    "imd_quintile": "IMD_Quintile is used to store the Index of Multiple Deprivation (IMD) quintile value.",
    "latitude": "Latitude coordinate of the matched address property from the reference address data.",
    "longitude": "Longitude coordinate of the matched address property from the reference address data.",
    "uprn_match_quality": "It provides a descriptive label indicating the type or quality of the address match, based on the matching algorithm used to link the address to the reference data.",
    "epc_current_energy_rating": "EPC band A-G. Core fuel poverty indicator; bands D-G flag risk under LILEE definition.",
    "epc_potential_energy_rating": "Achievable EPC band after recommended improvements.",
    "epc_property_type": "Property type recorded in the linked Energy Performance Certificate, such as house, flat, bungalow, maisonette or park home; carried from `PROPERTY_TYPE`.",
    "epc_built_form": "Detached/Semi/Terrace etc. Affects heat loss via surface-area-to-volume ratio.",
    "epc_construction_age_band": "Building age band. Strongest single predictor of fabric quality. Pre-1919 homes avg SAP ~45.",
    "epc_tenure": "Tenure at time of EPC: Owner-occupied / Rented (private) / Rented (social).",
    "epc_mains_gas_flag": "Whether mains gas is available. Off-gas-grid homes face higher fuel costs.",
    "epc_total_floor_area": "Total floor area in m². Overcrowding proxy when linked to household size.",
    "epc_inspection_date": "Date the EPC assessment was carried out.",
    "epc_lodgement_date": "Date the EPC was lodged on the register.",
    "epc_fuel_poverty_risk": "True if EPC band D-G. Flags fuel poverty risk under LILEE definition.",
    "epc_cold_hazard_proxy": "True if EPC band F or G. Approximates HHSRS Category 1 excessive cold hazard.",
    "epc_spatial_heating_poverty": "True if heated rooms < habitable rooms, indicating partial home heating.",
    "epc_off_gas_grid": "True if no mains gas connection. Off-grid homes face higher fuel costs.",
    "_source_system": "Value describing source system for the person address record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the person address record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "The address ID is the primary key of the address table.",
    "address_id": "Millennium ADDRESS_ID as BIGINT; primary key of this table.",
    "loaded_at": "map_address.ADC_UPDT carried unchanged for the address row. The EPC enrichment join contributes no load timestamp, so this field does not track changes to the EPC payload or its inspection/lodgement dates. It is address-source bronze provenance, not Silver refresh time.",
}

PERSON_ADDRESS_LIFECYCLE_FIELDS = [
]

PERSON_ADDRESS_RETIRED_COLUMNS = [

]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_person_address():
    # Assemble person address rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    a = read_source(SRC_ADDRESS).alias("a")
    e = read_source(SRC_ADDRESS_EPC).select(
        F.col("ADDRESS_ID").alias("_epc_address_id"),
        F.col("CURRENT_ENERGY_RATING").alias("epc_current_energy_rating"),
        F.col("POTENTIAL_ENERGY_RATING").alias("epc_potential_energy_rating"),
        F.col("PROPERTY_TYPE").alias("epc_property_type"),
        F.col("BUILT_FORM").alias("epc_built_form"),
        F.col("CONSTRUCTION_AGE_BAND").alias("epc_construction_age_band"),
        F.col("TENURE").alias("epc_tenure"),
        F.col("MAINS_GAS_FLAG").alias("epc_mains_gas_flag"),
        F.col("TOTAL_FLOOR_AREA").alias("epc_total_floor_area"),
        F.col("EPC_INSPECTION_DATE").alias("epc_inspection_date"),
        F.col("EPC_LODGEMENT_DATE").alias("epc_lodgement_date"),
        F.col("fuel_poverty_risk").alias("epc_fuel_poverty_risk"),
        F.col("hhsrs_cold_hazard_proxy").alias("epc_cold_hazard_proxy"),
        F.col("spatial_heating_poverty").alias("epc_spatial_heating_poverty"),
        F.col("off_gas_grid").alias("epc_off_gas_grid"),
    ).alias("e")
    d = a.join(e, a.ADDRESS_ID == e._epc_address_id, "left")
    # contract v2: rename the address hash as a key and publish native address, person, and organization ids
    return d.select(
        stable_id("person_address:mill", a.ADDRESS_ID).alias("person_address_key"),
        a.ADDRESS_ID.cast("bigint").alias("address_id"),
        a.ADDRESS_ID.cast("string").alias("source_address_id"),
        a.PARENT_ENTITY_NAME.alias("parent_entity"),
        F.when(a.PARENT_ENTITY_NAME == "PERSON", a.PARENT_ENTITY_ID.cast("bigint"))
         .alias("person_id"),
        F.when(a.PARENT_ENTITY_NAME == "ORGANIZATION", a.PARENT_ENTITY_ID.cast("bigint"))
         .alias("organization_id"),
        a.ADDRESS_TYPE_CODE.cast("string").alias("address_type_code"),
        a.ACTIVE_IND.alias("active_ind"), a.BEG_EFFECTIVE_DT_TM.alias("beg_effective"),
        a.END_EFFECTIVE_DT_TM.alias("end_effective"),
        (a.END_EFFECTIVE_DT_TM >= F.lit("2100-01-01").cast("timestamp"))
         .alias("open_ended_ind"),
        a.full_street_address.alias("street_address"), a.CITY.alias("city"),
        a.SOURCE_ZIPCODE.alias("postcode"), a.masked_zipcode.alias("postcode_masked"),
        a.POSTCODE_OUTWARD.alias("postcode_outward"), a.UPRN.cast("string").alias("uprn"),
        a.LSOA.alias("lsoa"), a.MSOA21CD.alias("msoa"),
        a.LADCD.alias("local_authority_code"), a.IMD_Decile.alias("imd_decile"),
        a.IMD_Quintile.alias("imd_quintile"), a.LATITUDE.alias("latitude"),
        a.LONGITUDE.alias("longitude"), a.match_quality.alias("uprn_match_quality"),
        F.col("epc_current_energy_rating"), F.col("epc_potential_energy_rating"),
        F.col("epc_property_type"), F.col("epc_built_form"),
        F.col("epc_construction_age_band"), F.col("epc_tenure"),
        F.col("epc_mains_gas_flag"), F.col("epc_total_floor_area"),
        F.col("epc_inspection_date"), F.col("epc_lodgement_date"),
        F.col("epc_fuel_poverty_risk"), F.col("epc_cold_hazard_proxy"),
        F.col("epc_spatial_heating_poverty"), F.col("epc_off_gas_grid"),
        a.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_ADDRESS).alias("_source_table"),
        a.ADDRESS_ID.cast("string").alias("_source_row_id"),
    )

@materialized_view(
    name=_n("journey_reference.person_address"),
    comment="Address assignments with EPC extension; direct address/postcode/UPRN fields are IG-sensitive.",
    cluster_by=["person_id"], refresh_policy="incremental",
    column_comments=PERSON_ADDRESS_COLUMN_COMMENTS,
)
def person_address():
    # Build the declared dataset: Address assignments with EPC extension; direct
    # address/postcode/UPRN fields are IG-sensitive.
    return _lifecycle_source_person_address().drop(*PERSON_ADDRESS_LIFECYCLE_FIELDS, *PERSON_ADDRESS_RETIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Journey_reference.person_address_air_quality

# COMMAND ----------

# ==== journey_reference.person_address_air_quality ====

SRC_ADDRESS_AIR_QUALITY_CELL = "4_prod.bronze.map_address_air_quality_cell"

SRC_AIR_QUALITY_SITE_MONTH = "4_prod.bronze.map_air_quality_site_month"

PERSON_ADDRESS_AIR_QUALITY_COLUMN_COMMENTS = {
    "person_address_key": "Deterministic key of the address assignment this estimate belongs to; joins to reference_person_address.person_address_key.",
    "address_id": "Native Millennium ADDRESS_ID for the address assignment.",
    "person_id": "Native Millennium PERSON_ID of the address holder.",
    "month": "First day of the calendar month the estimate covers; the person lived at the address during this month.",
    "pollutant": "Pollutant measure reported by bronze map_air_quality_site_month, such as NO2 or PM2.5.",
    "nearest_within_2km_value": "Monthly mean at the nearest reporting site when that site is within 2 km; NULL otherwise. DAR059 method nearest_within_2km.",
    "nearest_site_id": "Site identifier of the nearest site that reported this pollutant in this month within 25 km.",
    "nearest_site_distance_km": "Great-circle distance from the address to nearest_site_id, in kilometres.",
    "tri_weighted_25km_value": "Inverse-square-distance-weighted mean of up to the three nearest reporting sites within 25 km, using weight 1/max(distance_km, 0.1)^2. DAR059 method tri_weighted_25km.",
    "tri_weighted_site_count": "Number of reporting sites, from one to three, that contributed to tri_weighted_25km_value.",
    "min_data_capture_pct": "Lowest source data-capture percentage among the contributing sites; a completeness caveat rather than a quality gate.",
}

from functools import reduce

AQ_LOCATION = ["address_latitude", "address_longitude", "address_lat_cell", "address_lon_cell"]

AQ_SITE_LOCATION = ["site_id", "site_latitude", "site_longitude", "site_lat_cell", "site_lon_cell"]

AQ_PUBLIC = [
    "person_address_key", "address_id", "person_id", "month", "pollutant",
    "nearest_within_2km_value", "nearest_site_id", "nearest_site_distance_km",
    "tri_weighted_25km_value", "tri_weighted_site_count", "min_data_capture_pct",
]

def aq_addresses(addresses, cells):
    # Select usable patient addresses and express their residency periods as inclusive start and
    # end months.
    a = addresses.where(
        F.col("person_address_key").isNotNull() & F.col("address_id").isNotNull()
        & F.col("person_id").isNotNull() & F.col("beg_effective").isNotNull()
    ).select(
        "person_address_key", F.col("address_id").cast("bigint").alias("address_id"),
        F.col("person_id").cast("bigint").alias("person_id"),
        F.trunc(F.to_date("beg_effective"), "month").alias("_residency_start_month"),
        F.trunc(F.to_date(F.coalesce("end_effective", F.lit("2100-01-01").cast("timestamp"))), "month").alias("_residency_end_month"),
    )
    c = cells.select(
        F.col("ADDRESS_ID").cast("bigint").alias("address_id"),
        F.col("LATITUDE").cast("double").alias("address_latitude"),
        F.col("LONGITUDE").cast("double").alias("address_longitude"),
        F.col("lat_cell").alias("address_lat_cell"), F.col("lon_cell").alias("address_lon_cell"),
    ).where(reduce(lambda x, y: x & y, [F.col(c).isNotNull() for c in AQ_LOCATION]))
    return a.join(c, "address_id", "inner")

def aq_observations(source):
    # Normalize monthly air-quality observations and monitoring-site coordinates to the expected
    # types.
    return source.select(
        F.col("site_id").cast("string").alias("site_id"),
        F.to_date("month_start").alias("month_start"),
        F.col("pollutant").cast("string").alias("pollutant"),
        F.col("measurement_value").cast("double").alias("measurement_value"),
        F.col("data_capture_pct").cast("double").alias("data_capture_pct"),
        F.col("site_latitude").cast("double").alias("site_latitude"),
        F.col("site_longitude").cast("double").alias("site_longitude"),
        "site_lat_cell", "site_lon_cell",
    ).where(reduce(lambda x, y: x & y, [F.col(c).isNotNull() for c in AQ_SITE_LOCATION + ["month_start", "pollutant", "measurement_value"]]))

def aq_distance():
    # Operation order and constants intentionally match the parity-proven query.
    # Calculate great-circle distance in kilometres between address and monitoring-site
    # coordinates.
    return F.lit(2.0) * F.lit(6371.0088) * F.asin(F.sqrt(
        F.pow(F.sin(F.radians(F.col("site_latitude") - F.col("address_latitude")) / F.lit(2.0)), F.lit(2.0))
        + F.cos(F.radians("address_latitude")) * F.cos(F.radians("site_latitude"))
        * F.pow(F.sin(F.radians(F.col("site_longitude") - F.col("address_longitude")) / F.lit(2.0)), F.lit(2.0))
    ))

def aq_geometry(addresses, observations):
    # Generate nearby address/site candidates using neighbouring spatial cells and calculate
    # their distances.
    locations = addresses.select(*AQ_LOCATION).distinct()
    sites = observations.select(*AQ_SITE_LOCATION).distinct()
    # The compact geometry relation is independent of month and person. Retain all
    # sites within the original 5x5 neighbourhood and 25km radius, not a static top 3.
    # Expand the small station-geometry side (currently 1,339 x 25), not millions
    # of assignments. Equality keys avoid a location x all-sites nested-loop join.
    site_neighbours = sites.withColumn("_cell", F.explode(F.array(*[
        F.struct((F.col("site_lat_cell") + dx).alias("address_lat_cell"),
                 (F.col("site_lon_cell") + dy).alias("address_lon_cell"))
        for dx in (-2, -1, 0, 1, 2) for dy in (-2, -1, 0, 1, 2)
    ]))).select(*AQ_SITE_LOCATION, "_cell.*")
    nearby = locations.join(site_neighbours, ["address_lat_cell", "address_lon_cell"], "inner").withColumn("distance_km", aq_distance()).where(F.col("distance_km") <= 25.0)
    return nearby.select(*AQ_LOCATION, *AQ_SITE_LOCATION, "distance_km")

def aq_attach_residency(addresses, estimates):
    # Join location estimates to patient addresses and retain only months within each residency
    # period.
    return addresses.join(estimates, AQ_LOCATION, "inner").where(
        (F.col("month") >= F.col("_residency_start_month"))
        & (F.col("month") <= F.col("_residency_end_month"))
    ).select(*AQ_PUBLIC)

def aq_location_estimates_join(addresses, geometry, observations):
    """Native relational comparator: reuse geometry before monthly expansion.

    Unlike the array prototype, all joins/aggregates are Photon-native. Person
    identities and their repetition are absent until the final residency join.
    """
    scopes = addresses.groupBy(*AQ_LOCATION).agg(
        F.min("_residency_start_month").alias("_first_month"),
        F.max("_residency_end_month").alias("_last_month"),
    )
    pairs = geometry.join(scopes, AQ_LOCATION, "inner")
    candidates = pairs.join(observations, AQ_SITE_LOCATION, "inner").where(
        (F.col("month_start") >= F.col("_first_month")) & (F.col("month_start") <= F.col("_last_month")))
    ranked = candidates.groupBy(*AQ_LOCATION, "month_start", "pollutant").agg(
        F.slice(F.array_sort(F.collect_list(F.struct("distance_km", "site_id", "measurement_value", "data_capture_pct"))), 1, 3).alias("nearest3"))
    weight = lambda site: F.lit(1.0) / F.pow(F.greatest(site["distance_km"], F.lit(0.1)), F.lit(2.0))
    return ranked.select(
        *AQ_LOCATION, F.col("month_start").alias("month"), "pollutant",
        F.when(F.col("nearest3")[0]["distance_km"] <= 2.0, F.col("nearest3")[0]["measurement_value"]).alias("nearest_within_2km_value"),
        F.col("nearest3")[0]["site_id"].alias("nearest_site_id"),
        F.col("nearest3")[0]["distance_km"].alias("nearest_site_distance_km"),
        (F.aggregate("nearest3", F.lit(0.0), lambda total, site: total + site["measurement_value"] * weight(site))
         / F.aggregate("nearest3", F.lit(0.0), lambda total, site: total + weight(site))).alias("tri_weighted_25km_value"),
        F.size("nearest3").alias("tri_weighted_site_count"),
        F.array_min(F.transform("nearest3", lambda site: site["data_capture_pct"])).alias("min_data_capture_pct"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference /  air quality assignments

# COMMAND ----------

# Reusable implementation tables live only in the configured internal schema.
# Public lineage (native assignment/person IDs and chosen monitoring site) stays
# on reference_person_address_air_quality; no companion research view is added.

@materialized_view(
    name=_n("journey_reference._air_quality_assignments"), private=True,
    comment="Internal eligible address assignments with exact residency months and air-quality coordinates; supports location-level estimate reuse.",
    refresh_policy="incremental",
)
def _air_quality_assignments():
    # Build the declared dataset: Internal eligible address assignments with exact residency
    # months and air-quality coordinates; supports location-level estimate reuse.
    return aq_addresses(
        spark.read.table(_n("journey_reference.person_address")),
        read_source(SRC_ADDRESS_AIR_QUALITY_CELL),
    )

@materialized_view(
    name=_n("journey_reference._air_quality_geometry"), private=True,
    comment="Internal great-circle distances for distinct address locations and monitoring-site positions within the original 5x5 cells and 25 km; retains all eligible sites.",
    refresh_policy="incremental",
)
def _air_quality_geometry():
    # Build the declared dataset: Internal great-circle distances for distinct address locations
    # and monitoring-site positions within the original 5x5 cells and 25 km; retains all
    # eligible sites.
    return aq_geometry(
        spark.read.table(_n("journey_reference._air_quality_assignments")),
        aq_observations(read_source(SRC_AIR_QUALITY_SITE_MONTH)),
    )

@materialized_view(
    name=_n("journey_reference._air_quality_estimates"), private=True,
    comment="Internal location/month/pollutant estimates, using the nearest three reporting observations and parity-proven inverse-square weights; exact person residency is attached by the research product.",
    refresh_policy="incremental",
)
def _air_quality_estimates():
    # Build the declared dataset: Internal location/month/pollutant estimates, using the nearest
    # three reporting observations and parity-proven inverse-square weights; exact person
    # residency is attached by the research product.
    return aq_location_estimates_join(
        spark.read.table(_n("journey_reference._air_quality_assignments")),
        spark.read.table(_n("journey_reference._air_quality_geometry")),
        aq_observations(read_source(SRC_AIR_QUALITY_SITE_MONTH)),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / person address air quality

# COMMAND ----------

@materialized_view(
    name=_n("journey_reference.person_address_air_quality"),
    comment=("Monthly ambient air-quality estimates for the months a person lived at an address, "
             "using sites that reported each pollutant-month. Publishes nearest-site within 2 km "
             "and inverse-square-distance weighted nearest-three within 25 km DAR059 methods."),
    column_comments=PERSON_ADDRESS_AIR_QUALITY_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def person_address_air_quality():
    # Geometry and location estimates are reused internally. Exact original
    # person-address residency bounds are applied here, including month endpoints.
    # Build the declared dataset: Monthly ambient air-quality estimates for the months a person
    # lived at an address, using sites that reported each pollutant-month. Publishes nearest-
    # site within 2 km and inverse-square-distance weighted nearest-three within 25 km DAR059
    # methods.
    return aq_attach_residency(
        spark.read.table(_n("journey_reference._air_quality_assignments")),
        spark.read.table(_n("journey_reference._air_quality_estimates")),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / person death evidence

# COMMAND ----------

SRC_DEATH = "4_prod.bronze.map_death"

PERSON_DEATH_EVIDENCE_COLUMN_COMMENTS = {
    "person_death_evidence_key": "Deterministic SHA-256 key for the person death-evidence row.",
    "person_id": "Native Millennium PERSON_ID as BIGINT.",
    "deceased_datetime_raw": "Date and time of death.",
    "deceased_datetime": "Date and time of death.",
    "calculated_death_date": "Calendar date associated with calculated death for the person death evidence record. It is derived from bronze field `CALC_DEATH_DATE` in `4_prod.bronze.map_death`. The value has no time-of-day component; null means the source date was unavailable.",
    "precision_flag": "Source precision flag for DECEASED_DT_TM.",
    "precision_desc": "Decoded death timestamp precision: unknown, full date/time, month, year or day.",
    "source_desc": "Description of the code.",
    "method_desc": "Description of the code.",
    "death_date_estimate_source": "RECORDED_DEATH, LAST_ENCOUNTER, LAST_CLINICAL_EVENT or NONE.",
    "cause_of_death": "Raw free-text cause of death.",
    "autopsy_desc": "Resolved autopsy code description/display.",
    "age_at_death": "Raw reported age at death; source zero values are retained.",
    "last_encounter_datetime": "It represents the timestamp of the last encounter associated with a deceased individual.",
    "last_clinical_event_datetime": "Latest EVENT_END_DT_TM across selected latest source versions, without result-status filtering.",
    "last_known_activity_datetime": "Greatest of LAST_ENCNTR_DT_TM and LAST_NON_ERROR_CLINICAL_EVENT_DT_TM. This is activity, not a confirmed death timestamp.",
    "clinical_event_count": "Count of selected latest logical clinical-event source states for the person.",
    "_source_system": "Value describing source system for the person death evidence record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the person death evidence record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "This is the value of the unique primary identifier of the person table. It is an internal system assigned number.",
    "record_status": "Always active in this projection. The label is a literal, not derived from death evidence, source presence, clinical activity or effective dates; it does not assert that the person is alive.",
    "loaded_at": "map_death.ADC_UPDT carried unchanged for the contributing death-evidence row. This is bronze load provenance, not recorded/calculated death time, last-known activity or Silver refresh time.",
}

PERSON_DEATH_EVIDENCE_LIFECYCLE_FIELDS = [
]

PERSON_DEATH_EVIDENCE_RETIRED_COLUMNS = [

]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_person_death_evidence():
    # Assemble person death evidence rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    s = read_source(SRC_DEATH)
    # contract v2: rename the deterministic death-evidence identity as a key and publish native person_id
    return s.select(
        stable_id("person_death_evidence:mill", s.PERSON_ID).alias("person_death_evidence_key"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        s.DECEASED_DT_TM.alias("deceased_datetime_raw"),
        _clamped_ts(s.DECEASED_DT_TM).alias("deceased_datetime"),
        s.CALC_DEATH_DATE.alias("calculated_death_date"),
        s.DECEASED_DT_TM_PREC_FLAG.alias("precision_flag"),
        s.DECEASED_DT_TM_PREC_DESC.alias("precision_desc"),
        s.DECEASED_SOURCE_DESC.alias("source_desc"),
        s.DECEASED_METHOD_DESC.alias("method_desc"),
        s.DEATH_DATE_ESTIMATE_SOURCE.alias("death_date_estimate_source"),
        s.CAUSE_OF_DEATH.alias("cause_of_death"), s.AUTOPSY_DESC.alias("autopsy_desc"),
        s.AGE_AT_DEATH.alias("age_at_death"), s.LAST_ENCNTR_DT_TM.alias("last_encounter_datetime"),
        s.LAST_CLINICAL_EVENT_DT_TM.alias("last_clinical_event_datetime"),
        s.LAST_KNOWN_ACTIVITY_DT_TM.alias("last_known_activity_datetime"),
        s.CLINICAL_EVENT_COUNT.alias("clinical_event_count"),
        F.lit("active").alias("record_status"), s.ADC_UPDT.alias("loaded_at"), F.lit("millennium").alias("_source_system"),
        F.lit(SRC_DEATH).alias("_source_table"), s.PERSON_ID.cast("string").alias("_source_row_id"),
    )

@materialized_view(
    name=_n("journey_reference.person_death_evidence"),
    comment="Corroborating death-register evidence; person-spine deceased fields remain authoritative.",
    cluster_by=["person_id"], refresh_policy="incremental",
    column_comments=PERSON_DEATH_EVIDENCE_COLUMN_COMMENTS,
)
def person_death_evidence():
    # Build the declared dataset: Corroborating death-register evidence; person-spine deceased
    # fields remain authoritative.
    return _lifecycle_source_person_death_evidence().drop(*PERSON_DEATH_EVIDENCE_LIFECYCLE_FIELDS, *PERSON_DEATH_EVIDENCE_RETIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / device mapping

# COMMAND ----------

SRC_DEVICE_MAPPING = "4_prod.bronze.map_device_mapping"

DEVICE_MAPPING_COLUMN_COMMENTS = {
    "device_mapping_key": "Deterministic SHA-256 key for the Millennium device-mapping row.",
    "source_event_id": "Source implant description event identifier.",
    "person_id": "Native Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT.",
    "implant_description": "Raw source implant description.",
    "normalized_description": "Versioned punctuation/whitespace normalized description.",
    "cleaned_description": "Versioned fallback-matching description.",
    "udi_di": "Coalesced source UDI-DI; issuer is retained separately.",
    "udi_issuer": "GS1, HIBCC or null.",
    "gs1_identifier": "Source GS1 UDI device identifier.",
    "hibcc_device_id": "Source HIBCC UDI device identifier.",
    "serial_number": "GS1 serial number coalesced with raw serial number.",
    "expiry_date": "GS1 expiry date coalesced with raw expiry date.",
    "gmdn_code": "Selected GMDN code.",
    "gmdn_name": "Selected GMDN preferred term name.",
    "snomed_concept_id": "Compatibility field populated only for SNOMED concepts.",
    "snomed_name": "Compatibility field populated only for SNOMED concepts.",
    "standard_concept_id": "Selected standard OMOP device concept identifier.",
    "standard_concept_name": "Selected standard OMOP device concept name.",
    "standard_vocabulary_id": "Vocabulary of STANDARD_CONCEPT_ID.",
    "device_type": "Selected device category/type.",
    "mapping_layer": "Selected mapping evidence layer.",
    "mapping_status": "MAPPED, AMBIGUOUS_SELECTED or UNMAPPED.",
    "mapping_confidence": "Deterministic evidence score between zero and one.",
    "confidence_tier": "HIGH, MEDIUM, LOW or UNMAPPED.",
    "mapping_rule_id": "Rule or algorithm identifier for the selected candidate.",
    "matched_field": "Source field that produced the selected candidate.",
    "matched_value": "Normalized source value used for the selected candidate.",
    "mapping_candidate_count": "Number of distinct mapping candidates retained.",
    "mapping_distinct_concept_count": "Number of distinct selected standard/GMDN tuples.",
    "mapping_ambiguous_ind": "Whether multiple credible candidates existed.",
    "matched_opcs_code": "OPCS code supporting the selected candidate.",
    "procedure_support_ind": "Whether procedure context supports the selected candidate.",
    "mapping_schema_version": "Output schema and mapping implementation version.",
    "normalization_version": "Description normalization version.",
    "brand_rules_version": "Boundary-aware phrase-rule version.",
    "mapped_at": "Timestamp when this event was last evaluated.",
    "_source_system": "Value describing source system for the device mapping record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the device mapping record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Source implant description event identifier.",
    "loaded_at": "map_device_mapping.PIPELINE_LOADED_AT carried unchanged. This is the contributing bronze row-write clock, distinct from MAPPED_AT evaluation time and from Silver refresh time; a missing value remains null.",
}

DEVICE_MAPPING_LIFECYCLE_FIELDS = [
]

DEVICE_MAPPING_RETIRED_COLUMNS = [

]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_device_mapping():
    # Assemble device mapping rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    s = read_source(SRC_DEVICE_MAPPING)
    # contract v2: rename the mapping hash as a key and publish native person and encounter ids
    return s.select(
        stable_id("device_mapping:mill", s.EVENT_ID).alias("device_mapping_key"),
        s.EVENT_ID.cast("string").alias("source_event_id"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        s.IMPLANT_DESCRIPTION.alias("implant_description"),
        s.NORMALIZED_DESCRIPTION.alias("normalized_description"),
        s.CLEANED_DESCRIPTION.alias("cleaned_description"),
        s.UDI_DI.alias("udi_di"), s.UDI_ISSUER.alias("udi_issuer"),
        s.SOURCE_GS1_IDENTIFIER.alias("gs1_identifier"),
        s.SOURCE_HIBCC_DEVICE_ID.alias("hibcc_device_id"),
        s.EFFECTIVE_SERIAL_NUMBER.alias("serial_number"),
        s.EFFECTIVE_EXPIRY_DATE.alias("expiry_date"),
        s.gmdncode.cast("string").alias("gmdn_code"), s.gmdn_name.alias("gmdn_name"),
        s.snomed_concept_id.cast("string").alias("snomed_concept_id"),
        s.snomed_name.alias("snomed_name"),
        s.STANDARD_CONCEPT_ID.cast("string").alias("standard_concept_id"),
        s.STANDARD_CONCEPT_NAME.alias("standard_concept_name"),
        s.STANDARD_VOCABULARY_ID.alias("standard_vocabulary_id"),
        s.device_type.alias("device_type"), s.mapping_layer.alias("mapping_layer"),
        s.MAPPING_STATUS.alias("mapping_status"), s.mapping_confidence.alias("mapping_confidence"),
        s.confidence_tier.alias("confidence_tier"), s.MAPPING_RULE_ID.alias("mapping_rule_id"),
        s.MATCHED_FIELD.alias("matched_field"), s.MATCHED_VALUE.alias("matched_value"),
        s.MAPPING_CANDIDATE_COUNT.alias("mapping_candidate_count"),
        s.MAPPING_DISTINCT_CONCEPT_COUNT.alias("mapping_distinct_concept_count"),
        s.MAPPING_AMBIGUOUS_IND.alias("mapping_ambiguous_ind"),
        s.MATCHED_OPCS_CODE.alias("matched_opcs_code"),
        s.PROCEDURE_SUPPORT_IND.alias("procedure_support_ind"),
        s.MAPPING_SCHEMA_VERSION.alias("mapping_schema_version"),
        s.NORMALIZATION_VERSION.alias("normalization_version"),
        s.BRAND_RULES_VERSION.alias("brand_rules_version"), s.MAPPED_AT.alias("mapped_at"),
        s.PIPELINE_LOADED_AT.alias("loaded_at"), F.lit("millennium").alias("_source_system"),
        F.lit(SRC_DEVICE_MAPPING).alias("_source_table"),
        s.EVENT_ID.cast("string").alias("_source_row_id"),
    )

@materialized_view(
    name=_n("journey_reference.device_mapping"),
    comment="Best-effort implant-device decode with complete mapping provenance; identifiers are IG-sensitive.",
    cluster_by=["person_id"], refresh_policy="incremental",
    column_comments=DEVICE_MAPPING_COLUMN_COMMENTS,
)
def device_mapping():
    # Build the declared dataset: Best-effort implant-device decode with complete mapping
    # provenance; identifiers are IG-sensitive.
    return _lifecycle_source_device_mapping().drop(*DEVICE_MAPPING_LIFECYCLE_FIELDS, *DEVICE_MAPPING_RETIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / encounter bounds

# COMMAND ----------

SRC_ENC_ACTIVITY_BOUNDS = "4_prod.bronze.map_encounter_activity_bounds"

SRC_ENC_EVENT_BOUNDS = "4_prod.bronze.map_encounter_event_bounds"

ENCOUNTER_BOUNDS_COLUMN_COMMENTS = {
    "encounter_bounds_key": "Deterministic SHA-256 key for the encounter-bounds row.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT; joins to spine_encounter.encounter_id.",
    "source_encounter_id": "Millennium encounter identifier. One row per encounter with some recorded clinical activity; encounters with none are absent from this table.",
    "first_clinical_event_datetime": "Earliest plausible mill_clinical_event.EVENT_END_DT_TM for the encounter, over current rows only and excluding cancelled, in-error and not-done results. EVENT_END_DT_TM is clinical time and is freely backdated by retrospective documentation, so this can precede a known arrival by years. It is evidence that the encounter happened, not a usable arrival.",
    "last_clinical_event_datetime": "Latest plausible mill_clinical_event.EVENT_END_DT_TM for the encounter under the same filters.",
    "clinical_event_count": "Number of qualifying clinical events, zero when the encounter has none.",
    "first_contemporaneous_event_datetime": "Earliest clinical event whose clinical time is no more than seven days before its documentation time (EVENT_END_DT_TM at or after VALID_FROM_DT_TM minus seven days), which strips most retrospectively backdated entries. This is the only event-derived timestamp permitted to set a best-guess arrival, and then only at low confidence: measured against known arrivals it still precedes them 27.0 per cent of the time, with a 1st percentile of minus 48.9 days.",
    "last_contemporaneous_event_datetime": "Latest clinical event under the same contemporaneity filter.",
    "contemporaneous_event_count": "Number of clinical events passing the contemporaneity filter, zero when the encounter has none.",
    "first_order_datetime": "Earliest plausible mill_orders.ORIG_ORDER_DT_TM for the encounter, excluding order templates and cancelled, voided-without-results and future orders. Orders sit a median 1.4 minutes after a known arrival but precede it 9.9 per cent of the time, because orders can be placed at referral.",
    "last_order_datetime": "Latest plausible mill_orders.ORIG_ORDER_DT_TM for the encounter under the same filters.",
    "order_count": "Number of qualifying orders, zero when the encounter has none.",
    "ward_move_count": "Number of distinct nurse-unit placements recorded in mill_encntr_loc_hist, deduplicated on nurse unit and the placement arrival and departure pair. Zero when the encounter has none.",
    "ward_occupancy_minutes": "Total minutes spent in nurse units, summed over placements that have both an arrival and a later departure. Null when no placement is closed. Overlapping placements are summed as recorded, so read this as evidence of inpatient duration rather than an exact bed-occupancy measure.",
    "last_ward_in_datetime": "Latest plausible nurse-unit arrival recorded in mill_encntr_loc_hist.",
    "last_ward_out_datetime": "Latest plausible nurse-unit departure recorded in mill_encntr_loc_hist. Location history mirrors the encounter DISCH_DT_TM to the minute 99.6 per cent of the time, so this contributes a departure only for the few thousand encounters that have neither DEPART_DT_TM nor DISCH_DT_TM.",
    "last_ward_still_open_ind": "True when the most recent nurse-unit placement has no recorded departure after it, meaning the patient is still in situ. When true, LAST_WARD_OUT_DT_TM must not be read as a departure.",
    "event_first_datetime": "Earliest plausible mill_clinical_event.CLINSIG_UPDT_DT_TM recorded against the encounter.",
    "event_last_datetime": "Latest plausible mill_clinical_event.CLINSIG_UPDT_DT_TM recorded against the encounter.",
    "in_activity_bounds_ind": "Millennium encounter identifier. One row per encounter with some recorded clinical activity; encounters with none are absent from this table.",
    "in_event_bounds_ind": "Millennium encounter identifier.",
    "_source_system": "Value describing source system for the encounter bounds record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the encounter bounds record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Millennium encounter identifier. One row per encounter with some recorded clinical activity; encounters with none are absent from this table.",
    "loaded_at": "Always NULL as a TIMESTAMP in the encounter-bounds projection. No load clock is supplied from map_encounter_activity_bounds or map_encounter_event_bounds, and the pipeline does not substitute event-boundary timestamps or the Silver refresh time.",
}

ENCOUNTER_BOUNDS_LIFECYCLE_FIELDS = [
]

ENCOUNTER_BOUNDS_RETIRED_COLUMNS = [

]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_encounter_bounds():
    # Assemble encounter bounds rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    a = read_source(SRC_ENC_ACTIVITY_BOUNDS).alias("a")
    b = read_source(SRC_ENC_EVENT_BOUNDS).alias("b")
    d = a.join(b, a.ENCNTR_ID == b.ENCNTR_ID, "full")
    source_id = F.coalesce(a.ENCNTR_ID, b.ENCNTR_ID)
    # contract v2: rename the bounds hash as a key and publish native encounter_id
    return d.select(
        stable_id("encounter_bounds:mill", source_id).alias("encounter_bounds_key"),
        source_id.cast("bigint").alias("encounter_id"),
        source_id.cast("string").alias("source_encounter_id"),
        a.FIRST_CLINICAL_EVENT_DT_TM.alias("first_clinical_event_datetime"),
        a.LAST_CLINICAL_EVENT_DT_TM.alias("last_clinical_event_datetime"),
        a.CLINICAL_EVENT_COUNT.alias("clinical_event_count"),
        a.FIRST_CONTEMPORANEOUS_EVENT_DT_TM.alias("first_contemporaneous_event_datetime"),
        a.LAST_CONTEMPORANEOUS_EVENT_DT_TM.alias("last_contemporaneous_event_datetime"),
        a.CONTEMPORANEOUS_EVENT_COUNT.alias("contemporaneous_event_count"),
        a.FIRST_ORDER_DT_TM.alias("first_order_datetime"),
        a.LAST_ORDER_DT_TM.alias("last_order_datetime"), a.ORDER_COUNT.alias("order_count"),
        a.WARD_MOVE_COUNT.alias("ward_move_count"),
        a.WARD_OCCUPANCY_MINUTES.alias("ward_occupancy_minutes"),
        a.LAST_WARD_IN_DT_TM.alias("last_ward_in_datetime"),
        a.LAST_WARD_OUT_DT_TM.alias("last_ward_out_datetime"),
        a.LAST_WARD_STILL_OPEN_IND.alias("last_ward_still_open_ind"),
        b.FIRST_EVENT_DT_TM.alias("event_first_datetime"),
        b.LAST_EVENT_DT_TM.alias("event_last_datetime"),
        a.ENCNTR_ID.isNotNull().alias("in_activity_bounds_ind"),
        b.ENCNTR_ID.isNotNull().alias("in_event_bounds_ind"),
        F.lit(None).cast("timestamp").alias("loaded_at"),
        F.lit("millennium").alias("_source_system"),
        F.concat_ws("|", F.lit(SRC_ENC_ACTIVITY_BOUNDS), F.lit(SRC_ENC_EVENT_BOUNDS))
         .alias("_source_table"), source_id.cast("string").alias("_source_row_id"),
    )

@materialized_view(
    name=_n("journey_reference.encounter_bounds"),
    comment="Full-outer encounter activity/event bounds with source-arm presence flags.",
    cluster_by=["encounter_id"], refresh_policy="incremental",
    column_comments=ENCOUNTER_BOUNDS_COLUMN_COMMENTS,
)
def encounter_bounds():
    # Build the declared dataset: Full-outer encounter activity/event bounds with source-arm
    # presence flags.
    return _lifecycle_source_encounter_bounds().drop(*ENCOUNTER_BOUNDS_LIFECYCLE_FIELDS, *ENCOUNTER_BOUNDS_RETIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / practitioner identifier

# COMMAND ----------

SRC_PRSNL_ALIAS = "4_prod.bronze.map_medical_personnel_alias"

PRACTITIONER_IDENTIFIER_COLUMN_COMMENTS = {
    "practitioner_identifier_key": "Deterministic SHA-256 key for the practitioner alias row.",
    "practitioner_id": "Native Millennium personnel PERSON_ID as BIGINT; joins to reference_practitioner.practitioner_id.",
    "practitioner_person_id": "Source-system identifier for the practitioner person associated with each practitioner identifier record. It is derived from bronze field `PERSON_ID` in `4_prod.bronze.map_medical_personnel_alias`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "source_alias_id": "Native personnel-alias record identifier from `PRSNL_ALIAS_ID`, retained as a string.",
    "alias_type_meaning": "CDF meaning for the alias type.",
    "alias_type_display": "Display for the alias type.",
    "alias": "Raw personnel alias value.",
    "active_ind": "Source numeric active indicator for the personnel alias, carried from `ACTIVE_IND` without boolean conversion.",
    "effective_now_ind": "Whether the alias is effective at pipeline time.",
    "beg_effective": "Alias begin-effective timestamp.",
    "end_effective": "Alias end-effective timestamp.",
    "contributor_system_code": "Alias contributor-system code.",
    "_source_system": "Value describing source system for the practitioner identifier record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the practitioner identifier record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Native personnel-alias record identifier from `PRSNL_ALIAS_ID`, retained as a string.",
    "record_status": "Retracted when map_medical_personnel_alias.SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag defaults true. ACTIVE_IND, EFFECTIVE_NOW_IND and alias validity dates do not determine this label, and no superseded status is emitted.",
    "loaded_at": "map_medical_personnel_alias.ADC_UPDT carried unchanged for the alias row. This is bronze load provenance, not the alias begin/end-effective timestamp or Silver refresh time.",
}

PRACTITIONER_IDENTIFIER_LIFECYCLE_FIELDS = [
]

PRACTITIONER_IDENTIFIER_RETIRED_COLUMNS = [

]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_practitioner_identifier():
    # Assemble practitioner identifier rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    s = read_source(SRC_PRSNL_ALIAS)
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    # contract v2: rename the row hash as a key and publish native practitioner_id
    return s.select(
        stable_id("practitioner_identifier:mill", s.PRSNL_ALIAS_ID)
         .alias("practitioner_identifier_key"),
        s.PERSON_ID.cast("bigint").alias("practitioner_id"),
        s.PERSON_ID.cast("string").alias("practitioner_person_id"),
        s.PRSNL_ALIAS_ID.cast("string").alias("source_alias_id"),
        s.ALIAS_TYPE_CDF_MEANING.alias("alias_type_meaning"),
        s.ALIAS_TYPE_DISPLAY.alias("alias_type_display"), s.ALIAS.alias("alias"),
        s.ACTIVE_IND.alias("active_ind"), s.EFFECTIVE_NOW_IND.alias("effective_now_ind"),
        s.BEG_EFFECTIVE_DT_TM.alias("beg_effective"), s.END_EFFECTIVE_DT_TM.alias("end_effective"),
        s.CONTRIBUTOR_SYSTEM_CD.cast("string").alias("contributor_system_code"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active")).alias("record_status"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_PRSNL_ALIAS).alias("_source_table"),
        s.PRSNL_ALIAS_ID.cast("string").alias("_source_row_id"),
    )

@materialized_view(
    name=_n("journey_reference.practitioner_identifier"),
    comment="Staff aliases including postcode-type identifiers; alias values are IG-sensitive.",
    cluster_by=["practitioner_id"], refresh_policy="incremental",
    column_comments=PRACTITIONER_IDENTIFIER_COLUMN_COMMENTS,
)
def practitioner_identifier():
    # Build the declared dataset: Staff aliases including postcode-type identifiers; alias
    # values are IG-sensitive.
    return _lifecycle_source_practitioner_identifier().drop(*PRACTITIONER_IDENTIFIER_LIFECYCLE_FIELDS, *PRACTITIONER_IDENTIFIER_RETIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / practitioner group

# COMMAND ----------

SRC_PRSNL_GROUP = "4_prod.bronze.map_medical_personnel_group"

PRACTITIONER_GROUP_COLUMN_COMMENTS = {
    "practitioner_group_key": "Deterministic SHA-256 key for the practitioner-group membership.",
    "practitioner_id": "Native Millennium personnel PERSON_ID as BIGINT; joins to reference_practitioner.practitioner_id.",
    "practitioner_person_id": "Source-system identifier for the practitioner person associated with each practitioner group record. It is derived from bronze field `PERSON_ID` in `4_prod.bronze.map_medical_personnel_group`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "in_practitioner_dimension_ind": "Exact Millennium personnel identifier.",
    "source_group_id": "Native personnel-group identifier from `PRSNL_GROUP_ID`, retained as a string.",
    "group_name": "Raw personnel-group name.",
    "group_label": "Best label from group name, description and code metadata.",
    "group_type_meaning": "CDF meaning for the group type code.",
    "group_type_display": "Code-value display for the group type.",
    "primary_ind": "Source relationship primary indicator.",
    "relation_active_ind": "Source numeric active indicator for the personnel-to-group relationship, carried from `RELATION_ACTIVE_IND` without boolean conversion.",
    "relation_beg_effective": "Relationship begin-effective timestamp.",
    "relation_end_effective": "Relationship end-effective timestamp.",
    "group_active_ind": "Source numeric active indicator for the personnel group, carried from `GROUP_ACTIVE_IND` without boolean conversion.",
    "group_beg_effective": "Group begin-effective timestamp.",
    "group_end_effective": "Group end-effective timestamp.",
    "_source_system": "Value describing source system for the practitioner group record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the practitioner group record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Personnel-group relationship identifier.",
    "record_status": "Retracted when map_medical_personnel_group.SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag defaults true. Relation/group activity, validity dates and personnel-directory membership do not determine this label, and no superseded status is emitted.",
    "loaded_at": "map_medical_personnel_group.ADC_UPDT carried unchanged for the membership row. The personnel-directory existence join supplies only a person identifier and contributes no timestamp; this is bronze load provenance, not relationship validity time or Silver refresh time.",
}

PRACTITIONER_GROUP_LIFECYCLE_FIELDS = [
]

PRACTITIONER_GROUP_RETIRED_COLUMNS = [

]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_practitioner_group():
    # Assemble practitioner group rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    s = read_source(SRC_PRSNL_GROUP).alias("s")
    p = read_source(SRC_PRACTITIONER).select(
        F.col("PERSON_ID").alias("_practitioner_person_id")
    ).distinct().alias("p")
    d = s.join(p, s.PERSON_ID == p._practitioner_person_id, "left")
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    # contract v2: rename the membership hash as a key and publish native practitioner_id
    return d.select(
        stable_id("practitioner_group:mill", s.PRSNL_GROUP_RELTN_ID)
         .alias("practitioner_group_key"),
        s.PERSON_ID.cast("bigint").alias("practitioner_id"),
        s.PERSON_ID.cast("string").alias("practitioner_person_id"),
        F.col("_practitioner_person_id").isNotNull().alias("in_practitioner_dimension_ind"),
        s.PRSNL_GROUP_ID.cast("string").alias("source_group_id"),
        s.PRSNL_GROUP_NAME.alias("group_name"), s.GROUP_LABEL.alias("group_label"),
        s.CDF_MEANING.alias("group_type_meaning"), s.GROUP_TYPE_DISPLAY.alias("group_type_display"),
        s.PRIMARY_IND.alias("primary_ind"), s.RELATION_ACTIVE_IND.alias("relation_active_ind"),
        s.RELATION_BEG_EFFECTIVE_DT_TM.alias("relation_beg_effective"),
        s.RELATION_END_EFFECTIVE_DT_TM.alias("relation_end_effective"),
        s.GROUP_ACTIVE_IND.alias("group_active_ind"),
        s.GROUP_BEG_EFFECTIVE_DT_TM.alias("group_beg_effective"),
        s.GROUP_END_EFFECTIVE_DT_TM.alias("group_end_effective"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active")).alias("record_status"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_PRSNL_GROUP).alias("_source_table"),
        s.PRSNL_GROUP_RELTN_ID.cast("string").alias("_source_row_id"),
    )

@materialized_view(
    name=_n("journey_reference.practitioner_group"),
    comment="Staff-group memberships; personnel-dimension orphans are retained and flagged.",
    cluster_by=["practitioner_id"], refresh_policy="incremental",
    column_comments=PRACTITIONER_GROUP_COLUMN_COMMENTS,
)
def practitioner_group():
    # Build the declared dataset: Staff-group memberships; personnel-dimension orphans are
    # retained and flagged.
    return _lifecycle_source_practitioner_group().drop(*PRACTITIONER_GROUP_LIFECYCLE_FIELDS, *PRACTITIONER_GROUP_RETIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / practitioner location evidence

# COMMAND ----------

SRC_PRSNL_LOC_EVIDENCE = "4_prod.bronze.map_medical_personnel_location_evidence"

PRACTITIONER_LOCATION_EVIDENCE_COLUMN_COMMENTS = {
    "practitioner_location_evidence_key": "Deterministic SHA-256 key for practitioner-location evidence.",
    "practitioner_id": "Native Millennium personnel PERSON_ID as BIGINT; joins to reference_practitioner.practitioner_id.",
    "practitioner_person_id": "Source-system identifier for the practitioner person associated with each practitioner location evidence record. It is derived from bronze field `PERSON_ID` in `4_prod.bronze.map_medical_personnel_location_evidence`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "location_code": "Observed nurse-unit code.",
    "event_count": "Current canonical logical-event count at this location.",
    "first_event_datetime_raw": "Earliest supporting event timestamp.",
    "first_event_datetime": "Earliest supporting event timestamp.",
    "last_event_datetime_raw": "Latest supporting event timestamp.",
    "last_event_datetime": "Latest supporting event timestamp.",
    "location_rank": "Deterministic personnel location rank.",
    "top_count_tie_count": "Number of locations tied on the maximum event count.",
    "_source_system": "Value describing source system for the practitioner location evidence record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the practitioner location evidence record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Observed nurse-unit code.",
    "loaded_at": "map_medical_personnel_location_evidence.EVIDENCE_ADC_UPDT carried unchanged. It is the supplied supporting-evidence clock, not FIRST_EVENT_DT_TM, LAST_EVENT_DT_TM or Silver refresh time; Silver performs no further timestamp aggregation or clamping on this field.",
}

PRACTITIONER_LOCATION_EVIDENCE_LIFECYCLE_FIELDS = [
]

PRACTITIONER_LOCATION_EVIDENCE_RETIRED_COLUMNS = [

]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_practitioner_location_evidence():
    # Assemble practitioner location evidence rows with lifecycle and source evidence for the
    # public product and its internal metadata.
    s = read_source(SRC_PRSNL_LOC_EVIDENCE)
    # contract v2: rename the evidence hash as a key and publish native practitioner_id
    return s.select(
        stable_id("practitioner_location_evidence:mill", s.PERSON_ID, s.LOC_NURSE_UNIT_CD)
         .alias("practitioner_location_evidence_key"),
        s.PERSON_ID.cast("bigint").alias("practitioner_id"),
        s.PERSON_ID.cast("string").alias("practitioner_person_id"),
        s.LOC_NURSE_UNIT_CD.cast("string").alias("location_code"),
        s.EVENT_COUNT.alias("event_count"), s.FIRST_EVENT_DT_TM.alias("first_event_datetime_raw"),
        _clamped_ts(s.FIRST_EVENT_DT_TM).alias("first_event_datetime"),
        s.LAST_EVENT_DT_TM.alias("last_event_datetime_raw"),
        _clamped_ts(s.LAST_EVENT_DT_TM).alias("last_event_datetime"),
        s.LOCATION_RANK.alias("location_rank"), s.TOP_COUNT_TIE_COUNT.alias("top_count_tie_count"),
        s.EVIDENCE_ADC_UPDT.alias("loaded_at"), F.lit("millennium").alias("_source_system"),
        F.lit(SRC_PRSNL_LOC_EVIDENCE).alias("_source_table"),
        F.concat_ws("|", s.PERSON_ID, s.LOC_NURSE_UNIT_CD).alias("_source_row_id"),
    )

@materialized_view(
    name=_n("journey_reference.practitioner_location_evidence"),
    comment="Staff location evidence with raw and sentinel-clamped event bounds.",
    cluster_by=["practitioner_id"], refresh_policy="incremental",
    column_comments=PRACTITIONER_LOCATION_EVIDENCE_COLUMN_COMMENTS,
)
def practitioner_location_evidence():
    # Build the declared dataset: Staff location evidence with raw and sentinel-clamped event
    # bounds.
    return _lifecycle_source_practitioner_location_evidence().drop(*PRACTITIONER_LOCATION_EVIDENCE_LIFECYCLE_FIELDS, *PRACTITIONER_LOCATION_EVIDENCE_RETIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attribute reference

# COMMAND ----------

def _attribute_reference(slot, namespace, id_name, entity_kind):
    # Project a configured attribute feed using its entity identifier and source attribute
    # evidence.
    s = read_source(slot)
    entity_id = s.ENTITY_ID
    # contract v2: publish native BIGINT entity ids from the shared attribute helper
    entity_ref = entity_id.cast("bigint")
    cols = [
        stable_id(namespace, s.SOURCE_PK).alias(id_name),
        entity_ref.alias(f"{entity_kind}_id"),
        entity_id.cast("string").alias(f"source_{entity_kind}_id"),
        s.ATTRIBUTE_NAME.alias("attribute_name"), s.VALUE_KIND.alias("value_kind"),
        s.VALUE_CD.cast("string").alias("value_code"), s.VALUE_DISPLAY.alias("value_display"),
        s.VALUE_CODE_SET.cast("string").alias("value_code_set"),
        s.VALUE_DT_TM_CLEAN.alias("value_datetime"), s.VALUE_NUMERIC.alias("value_numeric"),
        s.VALUE_ANSWERED_IND.alias("answered_ind"), s.ACTIVE_IND.alias("active_ind"),
        s.CURRENT_IND.alias("current_ind"),
        s.BEG_EFFECTIVE_DT_TM_CLEAN.alias("beg_effective"),
        s.END_EFFECTIVE_DT_TM_CLEAN.alias("end_effective"), s.LINK_STATUS.alias("link_status"),
        s.PIPELINE_UPDT_DT_TM.alias("loaded_at"), F.lit("millennium").alias("_source_system"),
        F.lit(slot).alias("_source_table"), s.SOURCE_PK.cast("string").alias("_source_row_id"),
    ]
    return s.select(*cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / encounter attribute

# COMMAND ----------

SRC_ENCOUNTER_ATTRIBUTE = "4_prod.bronze.map_encounter_attribute"

ENCOUNTER_ATTRIBUTE_COLUMN_COMMENTS = {
    "encounter_attribute_key": "Deterministic SHA-256 key for the encounter attribute row.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT; joins to spine_encounter.encounter_id.",
    "source_encounter_id": "Internal encounter identifier from the source encounter information row (ENCNTR_ID), linking the attribute to its encounter.",
    "attribute_name": "Standardised curated label for the encounter attribute, derived from the source information sub-type code.",
    "value_kind": "Indicator of which value representation applies to the row, such as a coded, date/time or numeric value.",
    "value_code": "Coded value of the encounter information row, drawn from the code set applicable to the attribute, where zero denotes an unanswered value.",
    "value_display": "The display string for the code_value",
    "value_code_set": "Code set to which VALUE_CD actually belongs, resolved from the code value lookup.",
    "value_datetime": "VALUE_DT_TM with future-dated and sentinel values excluded, leaving only usable date and time values.",
    "value_numeric": "The numeric (non-codified) value of the encounter information.  If the value of the row necessitates storing a numeric (non-codified) value, it is placed in this column.",
    "answered_ind": "Flag indicating whether the row carries an answered coded value rather than the unanswered zero code.",
    "active_ind": "Source indicator of whether the row is active or in an inactive state such as logically deleted or combined away.",
    "current_ind": "Flag marking the latest active row for each encounter and information subtype within the retained history.",
    "beg_effective": "BEG_EFFECTIVE_DT_TM with future-dated and sentinel values excluded, leaving only usable effective-from date and time values.",
    "end_effective": "END_EFFECTIVE_DT_TM with future-dated and sentinel values excluded, leaving only usable effective-to date and time values.",
    "link_status": "Indicator of whether the row's encounter identifier resolves to an encounter in the curated encounter foundation.",
    "_source_system": "Value describing source system for the encounter attribute record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the encounter attribute record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Primary key of the source mill_encntr_info row (ENCNTR_INFO_ID), giving one row per allowlisted encounter information record.",
    "loaded_at": "map_encounter_attribute.PIPELINE_UPDT_DT_TM carried unchanged through the shared attribute projection. This is the contributing bronze write clock, not the attribute's effective dates, clinical value time or Silver refresh time.",
}

ENCOUNTER_ATTRIBUTE_LIFECYCLE_FIELDS = [
]

ENCOUNTER_ATTRIBUTE_RETIRED_COLUMNS = [

]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_encounter_attribute():
    # contract v2: rename the deterministic encounter-attribute identity as a key
    # Assemble encounter attribute rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _attribute_reference(
        SRC_ENCOUNTER_ATTRIBUTE, "encounter_attribute:mill", "encounter_attribute_key", "encounter"
    )

@materialized_view(
    name=_n("journey_reference.encounter_attribute"),
    comment="Allowlisted encounter attributes with source-record identifiers and current or effective-period evidence, linked to the native encounter.",
    cluster_by=["encounter_id"], refresh_policy="incremental",
    column_comments=ENCOUNTER_ATTRIBUTE_COLUMN_COMMENTS,
)
def encounter_attribute():
    # Build the declared dataset: Allowlisted encounter attributes with source-record
    # identifiers and current or effective-period evidence, linked to the native encounter.
    return _lifecycle_source_encounter_attribute().drop(*ENCOUNTER_ATTRIBUTE_LIFECYCLE_FIELDS, *ENCOUNTER_ATTRIBUTE_RETIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / person attribute

# COMMAND ----------

SRC_PERSON_ATTRIBUTE = "4_prod.bronze.map_person_attribute"

PERSON_ATTRIBUTE_COLUMN_COMMENTS = {
    "person_attribute_key": "Deterministic SHA-256 key for the person attribute row.",
    "person_id": "Native Millennium PERSON_ID as BIGINT.",
    "source_person_id": "Internal system-assigned person identifier from the source person table, linking the attribute row to a person.",
    "attribute_name": "Standardised label for the person attribute represented by the row, derived from the source information sub-type code.",
    "value_kind": "Indicator of the value representation expected for the attribute, such as a coded value, date or numeric value.",
    "value_code": "Coded value recorded for the attribute, where zero indicates the attribute was not answered and is never decoded.",
    "value_display": "The display string for the code_value",
    "value_code_set": "Code set of the decoded coded value as held in the reference code value lookup.",
    "value_datetime": "Attribute date/time after removal of sentinel and implausible values, leaving null where no usable value remains.",
    "value_numeric": "If the comment is of a numeric type, it is stored in this attribute",
    "answered_ind": "Flag indicating whether a coded value was actually recorded for the attribute rather than left unanswered.",
    "active_ind": "Indicator of whether the source row is in an active state or has been made inactive, such as logically deleted or combined away.",
    "current_ind": "Flag marking the latest active row for each person and attribute sub-type within the retained history.",
    "beg_effective": "Row begin effective date/time after removal of sentinel and implausible values, leaving null where no usable value remains.",
    "end_effective": "Row end effective date/time after removal of sentinel and open-ended placeholder values, leaving null where no usable value remains.",
    "link_status": "Indicator of whether the attribute row's person identifier matched a person in the curated person foundation table.",
    "_source_system": "Value describing source system for the person attribute record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the person attribute record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Unique source primary key of the originating mill_person_info row, carried through as the row grain of this table.",
    "loaded_at": "map_person_attribute.PIPELINE_UPDT_DT_TM carried unchanged through the shared attribute projection. This is the contributing bronze write clock, not the attribute's effective dates, clinical value time or Silver refresh time.",
}

PERSON_ATTRIBUTE_LIFECYCLE_FIELDS = [
]

PERSON_ATTRIBUTE_RETIRED_COLUMNS = [

]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_person_attribute():
    # contract v2: rename the row identity as a key and enforce native BIGINT person_id at publication
    # Assemble person attribute rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    return (
        _attribute_reference(
            SRC_PERSON_ATTRIBUTE, "person_attribute:mill", "person_attribute_key", "person"
        )
        .withColumn("person_id", F.col("person_id").cast("bigint"))
    )

@materialized_view(
    name=_n("journey_reference.person_attribute"),
    comment="Allowlisted person attributes; NO_FIXED_ABODE and CHILD_IN_PUBLIC_CARE are IG-sensitive.",
    cluster_by=["person_id"], refresh_policy="incremental",
    column_comments=PERSON_ATTRIBUTE_COLUMN_COMMENTS,
)
def person_attribute():
    # Build the declared dataset: Allowlisted person attributes; NO_FIXED_ABODE and
    # CHILD_IN_PUBLIC_CARE are IG-sensitive.
    return _lifecycle_source_person_attribute().drop(*PERSON_ATTRIBUTE_LIFECYCLE_FIELDS, *PERSON_ATTRIBUTE_RETIRED_COLUMNS)
