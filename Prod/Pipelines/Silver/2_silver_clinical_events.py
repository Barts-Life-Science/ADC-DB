# Databricks notebook source
# MAGIC %md
# MAGIC # Clinical events
# MAGIC Conditions, procedures, pathology, genomics, microbiology, medication, forms, vital signs, scores, imaging and other clinical facts.

# MAGIC
# MAGIC Reading order: 2 of 8. Numbers guide navigation; Lakeflow schedules datasets by their dependencies.
# MAGIC Shared helpers live in `silver_journey_shared.py`, an importable Python file.

# COMMAND ----------

# Shared projections also serve the patient event index.
from silver_journey_shared import (
    CONDITION_PRIMITIVE_COLUMNS,
    F,
    IMAGING_ACCESSION_MEMBER_COLUMNS,
    IMAGING_ACCESSION_MEMBER_COLUMN_COMMENTS,
    IMAGING_INTEGRATION_COLUMNS,
    IMAGING_INTEGRATION_COLUMN_COMMENTS,
    IMAGING_REPORT_LINK_COLUMNS,
    IMAGING_REPORT_LINK_COLUMN_COMMENTS,
    LIFECYCLE_COLUMN_COMMENTS,
    OMOP_URI,
    PROCEDURE_PRIMITIVE_COLUMNS,
    PROMOTED_SCORE_STAGE_COLUMNS,
    PROMOTED_VITAL_STAGE_COLUMNS,
    RESULT_PRIMITIVE_COLUMNS,
    S3_TABLE_AXIS_SPECS,
    SCORE_STAGE_COLUMNS,
    SNOMED_URI,
    SRC_APPOINTMENT,
    SRC_FORM_ACTIVITY,
    SRC_MEDICATION_ORDER,
    SRC_MEDICATION_ORDER_ACTION,
    SRC_MED_ADMIN,
    SRC_PATHOLOGY,
    SRC_PATHOLOGY_ACCESSION,
    SRC_PATHOLOGY_REPORT_VERSIONS,
    SRC_PATHOLOGY_REQUESTED_TEST,
    SRC_PROBLEM,
    SRC_S3C_RXNORM_MULTUM_MAP,
    SRC_WAITING_LIST,
    VITAL_STAGE_COLUMNS,
    _alias_resolved_accession,
    _allergy_canonical,
    _cancer_treatment_canonical,
    _cc_procedure_canonical,
    _clinical_score_canonical,
    _coded_finding_typed,
    _condition_diagnosis_canonical,
    _condition_problem_canonical,
    _condition_stage_canonical,
    _critical_care_period_canonical,
    _cross_qc_primitive,
    _cross_qc_public,
    _date_finding_typed,
    _endobase_procedure_canonical,
    _endoscopy_finding_canonical,
    _family_history_canonical,
    _form_canonical,
    _genomic_result_canonical,
    _genomic_test_canonical,
    _imaging_accession_member_canonical,
    _imaging_exam_canonical,
    _imaging_report_link_canonical,
    _implant_procedure_canonical,
    _indication_canonical,
    _maternity_diagnosis_canonical,
    _medication_admin_canonical,
    _medication_dispense_canonical,
    _medication_order_canonical,
    _micro_isolate_canonical,
    _mill_radiology_exam_canonical,
    _n,
    _nomen_finding_typed,
    _numeric_score_canonical,
    _numeric_vital_canonical,
    _pathology_report_series_canonical,
    _pathology_requested_test_canonical,
    _pathology_result_canonical,
    _pathology_specimen_canonical,
    _present,
    _presenting_complaint_canonical,
    _procedure_canonical,
    _s3_direct_axis,
    _s3_flatten_codeable_json,
    _s3_lookup_axis,
    _s3_replace_public_variant,
    _susceptibility_canonical,
    _text_finding_typed,
    _theatre_procedure_canonical,
    _transfusion_canonical,
    _usable_code,
    _vital_sign_canonical,
    _waiting_list_index_representative,
    axis_columns,
    axis_comments,
    canonical_coding_system,
    codeable_concept,
    codeable_concept_json,
    coding_obj,
    materialized_view,
    read_source,
    stable_id,
    subject_key_with_system,
)

# COMMAND ----------

SRC_S3_ENDOBASE_TERM_MAP = "3_lookup.omop.endobase_term_snomed_map"

SRC_S3B_SCORE_COMPONENT_MAP = "3_lookup.omop.score_component_snomed_map"

SRC_S3B_REGISTRY_ROUTE = "3_lookup.omop.registry_field_route"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc family history

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_family_history"),
    comment="Private JSON bridge and Gold cross-rule flags for family_history.",
    refresh_policy="incremental",
)
def _qc_family_history():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # family_history.
    return _cross_qc_primitive(
        _family_history_canonical(),
        "family_history",
        {"condition_code": "_qc_condition_code_json"},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / family history

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
FAMILY_HISTORY_PUBLIC_COLUMNS = [
    'patient_event_key',
    'fhx_activity_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'condition_code',
    'relationship_code',
    'relationship_display',
    'relationship_type_code',
    'relationship_type_display',
    'onset_age',
    'onset_age_unit',
    'severity_code',
    'severity_display',
    'source_lifecycle_status',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'asserter_practitioner_id',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

FAMILY_HISTORY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'fhx_activity_id',
    'identity_status',
    'event_after_death_30d',
    'load_batch_id',
]

FAMILY_HISTORY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "fhx_activity_id": "Millennium FHX_ACTIVITY_ID; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ORIGINATING_ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "event_datetime": "Source family-history effective start.",
    "event_end_datetime": "Source effective end when the assertion is no longer current.",
    "source_coding_system": "Verbatim source code system.",
    "source_code": "Source condition code",
    "source_display": "Verbatim source condition display.",
    "condition_code": "Source and mapped condition codings as a one-level CodeableConcept VARIANT.",
    "relationship_code": "Source family relationship code.",
    "relationship_display": "Source family relationship display.",
    "relationship_type_code": "Source relationship record type code.",
    "relationship_type_display": "Source relationship record type display.",
    "onset_age": "Source recorded age at onset.",
    "onset_age_unit": "Unit for recorded onset age.",
    "severity_code": "Source severity code.",
    "severity_display": "Source severity display.",
    "source_lifecycle_status": "Verbatim source life-cycle status.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "asserter_practitioner_id": "Millennium personnel PERSON_ID when supplied.",
    "record_status": "Superseded when map_family_history.END_EFFECTIVE_DT_TM is non-null and earlier than 2100-01-01; otherwise active. No retracted status is emitted. The separately retained LIFE_CYCLE_STATUS does not determine this derived label.",
    "record_status_effective_from": "Source-supplied timestamp from which the normalized record status applies for each family history record. It is derived from bronze field `FHX_ACTIVE_STATUS_DT_TM` in `4_prod.bronze.map_family_history`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "record_status_effective_to": "map_family_history.END_EFFECTIVE_DT_TM only when non-null and earlier than 2100-01-01; otherwise null. This is the same predicate that sets superseded status. No 1950 lower clamp, event-end substitution or ingestion-time fallback is applied.",
    "source_update_timestamp": "map_family_history.ADC_UPDT carried unchanged, identical to loaded_at. No independent native application-update clock is projected for this field, and the current Silver refresh time is not substituted.",
    "loaded_at": "map_family_history.ADC_UPDT carried unchanged through usable-code filtering and QC/public projections, identical to source_update_timestamp. QC identity/encounter joins add no load clocks; neither clinical event time nor the current Silver refresh time replaces it.",
}

@materialized_view(
    name=_n("journey_clinical.family_history"),
    comment="One family-history assertion with the Journey Model standard event block and CodeableConcept mappings.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=FAMILY_HISTORY_COLUMN_COMMENTS,
)
def family_history():
    # Build the declared dataset: One family-history assertion with the Journey Model standard
    # event block and CodeableConcept mappings.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_family_history")),
        "family_history",
        {"condition_code": "_qc_condition_code_json"},
        FAMILY_HISTORY_PUBLIC_COLUMNS,
        FAMILY_HISTORY_LIFECYCLE_COLUMNS,
    )

def _s3_component_metadata(public_relation, qc_relation, columns):
    # Join each published component back to its source event's quality metadata using
    # source_patient_event_key.
    public = spark.read.table(_n(public_relation)).select("patient_event_key", "source_patient_event_key")
    qc = spark.read.table(_n(qc_relation)).alias("q")
    return (public.alias("p").join(qc, F.col("p.source_patient_event_key") == F.col("q.patient_event_key"))
            .select(F.col("p.patient_event_key").alias("patient_event_key"),
                    *[F.col("q." + c).alias(c) for c in columns if c != "patient_event_key"]))

@materialized_view(
    name=_n('journey_clinical._family_history_metadata'),
    comment='Internal quality and batch metadata for clinical_family_history; same row grain as the research table. Join keys: patient_event_key, fhx_activity_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def family_history_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_family_history; same row grain as the research table. Join keys:
    # patient_event_key, fhx_activity_id.
    return _s3_component_metadata("journey_clinical.family_history", "journey_clinical._qc_family_history", FAMILY_HISTORY_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / presenting complaint

# COMMAND ----------

# ==== journey_clinical.presenting_complaint (S3b, P2) ====

PRESENTING_COMPLAINT_PUBLIC_COLUMNS = [
    "patient_event_key", "source_patient_event_key", "encounter_key", "encounter_id", "subject_key",
    "subject_id_system", "person_id", "source_object", "source_event_id", "statement_sequence",
    "event_datetime", "encounter_class", "source_coding_system", "source_code", "source_display",
    "complaint_text_normalised", "complaint_group_display", *axis_columns("complaint"),
    "record_status", "record_status_effective_from", "record_status_effective_to", "confidentiality_code",
    "vip_ind", "withheld_identity_ind", "source_update_timestamp", "loaded_at",
]

PRESENTING_COMPLAINT_LIFECYCLE_COLUMNS = [
    "patient_event_key", "source_patient_event_key", "identity_status", "person_id_resolved",
    "encounter_id_resolved", "event_before_birth", "event_after_death_30d", "load_batch_id",
]

PRESENTING_COMPLAINT_COLUMN_COMMENTS = {
    column: "S3b presenting-complaint field retained at encounter, source-statement and component grain."
    for column in PRESENTING_COMPLAINT_PUBLIC_COLUMNS
}

PRESENTING_COMPLAINT_COLUMN_COMMENTS.update(axis_comments("complaint", "presenting complaint"))

PRESENTING_COMPLAINT_COLUMN_COMMENTS["complaint_map_cosine"] = (
    "Cosine similarity between the normalised source text and selected SNOMED target, measured by complaint_map_scoring_model."
)

PRESENTING_COMPLAINT_COLUMN_COMMENTS["complaint_map_component_index"] = (
    "One-based source-derived component index; later components receive a stable component event key."
)

@materialized_view(name=_n("journey_clinical._qc_presenting_complaint"), private=True,
                   comment="Private source-grain stage for presenting complaint and reason for visit.", refresh_policy="incremental")
def _qc_presenting_complaint():
    # Build the declared dataset: Private source-grain stage for presenting complaint and reason
    # for visit.
    return _cross_qc_primitive(_presenting_complaint_canonical(), "presenting_complaint", {})

@materialized_view(name=_n("journey_clinical.presenting_complaint"),
                   comment="One presenting-complaint or reason-for-visit component per encounter and source statement.",
                   cluster_by=["person_id", "event_datetime"], refresh_policy="incremental",
                   column_comments=PRESENTING_COMPLAINT_COLUMN_COMMENTS)
def presenting_complaint():
    # Build the declared dataset: One presenting-complaint or reason-for-visit component per
    # encounter and source statement.
    return _cross_qc_public(spark.read.table(_n("journey_clinical._qc_presenting_complaint")),
                            "presenting_complaint", {}, PRESENTING_COMPLAINT_PUBLIC_COLUMNS,
                            PRESENTING_COMPLAINT_LIFECYCLE_COLUMNS)

@materialized_view(name=_n("journey_clinical._presenting_complaint_metadata"),
                   comment="Internal quality metadata at presenting-complaint component grain.",
                   refresh_policy="incremental", column_comments=LIFECYCLE_COLUMN_COMMENTS)
def presenting_complaint_lifecycle():
    # Build the declared dataset: Internal quality metadata at presenting-complaint component
    # grain.
    public = spark.read.table(_n("journey_clinical.presenting_complaint")).select(
        "patient_event_key", "source_patient_event_key")
    qc = spark.read.table(_n("journey_clinical._qc_presenting_complaint")).alias("q")
    return (public.alias("p").join(qc, F.col("p.source_patient_event_key") == F.col("q.patient_event_key"))
            .select(F.col("p.patient_event_key").alias("patient_event_key"),
                    F.col("p.source_patient_event_key").alias("source_patient_event_key"),
                    *[F.col("q." + c).alias(c) for c in PRESENTING_COMPLAINT_LIFECYCLE_COLUMNS
                      if c not in ("patient_event_key", "source_patient_event_key")]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  problem revision grouped

# COMMAND ----------

# ==== Typed clinical facts — condition and procedure ====

SRC_PROBLEM_HISTORY = "4_prod.bronze.map_problem_history"

def _problem_revision_grouped_query():
    # Group revision history for problem identifiers that are present in the current problem
    # source.
    s = read_source(SRC_PROBLEM_HISTORY)
    current = read_source(SRC_PROBLEM).select("PROBLEM_ID").distinct()
    s = s.join(current, "PROBLEM_ID", "inner")
    revision = F.struct(
        F.coalesce(s.PROBLEM_REVISION_RANK, F.lit(0)).cast("long").alias("sequence"),
        s.PROBLEM_REVISION_RANK.cast("long").alias("revision_rank"),
        s.IS_CURRENT_PROBLEM_REVISION.alias("is_current_revision"),
        s.LIFE_CYCLE_STATUS_CD.cast("string").alias("clinical_status_code"),
        s.life_cycle_status_desc.alias("clinical_status_display"),
        s.LIFE_CYCLE_DT_TM.alias("clinical_status_datetime"),
        s.CONFIRMATION_STATUS_CD.cast("string").alias("verification_status_code"),
        s.confirmation_status_desc.alias("verification_status_display"),
        F.coalesce(s.ANNOTATED_DISPLAY, s.PROBLEM_DISPLAY, s.SOURCE_STRING).alias("display"),
        s.SEVERITY_CD.cast("string").alias("severity_code"),
        s.severity_desc.alias("severity_display"),
        s.ONSET_DT_TM.alias("onset_datetime"),
        s.ASSERTED_DT_TM.alias("asserted_datetime"),
        s.BEG_EFFECTIVE_DT_TM.alias("effective_start"),
        s.END_EFFECTIVE_DT_TM.alias("effective_end"),
        F.coalesce(s.SOURCE_DELETED_IND, F.lit(False)).alias("source_tombstone_ind"),
        s.UPDT_DT_TM.alias("source_update_timestamp"),
    )
    history = (
        s.groupBy("PROBLEM_ID")
        .agg(
            F.to_json(F.sort_array(F.collect_list(revision))).alias("_revision_history_json"),
            F.count(F.lit(1)).cast("long").alias("_revision_history_count"),
            F.max(s.ADC_UPDT).alias("_evidence_loaded_at"),
        )
        .select(
            stable_id("condition:mill:problem", F.col("PROBLEM_ID"))
            .alias("_revision_condition_event_id"),
            F.col("_revision_history_json"),
            F.col("_revision_history_count"),
            F.col("_evidence_loaded_at"),
        )
    )
    # contract v2: join problem revision history by patient_event_key after fact_row_id removal
    problem = (
        _condition_problem_canonical()
        .join(
            history,
            F.col("patient_event_key") == history["_revision_condition_event_id"],
            "left",
        )
        .withColumn(
            "_revision_history_count",
            F.coalesce(F.col("_revision_history_count"), F.lit(0).cast("long")),
        )
        .withColumn(
            "loaded_at", F.greatest(F.col("loaded_at"), F.col("_evidence_loaded_at"))
        )
    )
    diagnosis = (
        _condition_diagnosis_canonical()
        .withColumn("_revision_history_json", F.lit(None).cast("string"))
        .withColumn("_revision_history_count", F.lit(0).cast("long"))
    )
    maternity = (
        _maternity_diagnosis_canonical()
        .withColumn("_revision_history_json", F.lit(None).cast("string"))
        .withColumn("_revision_history_count", F.lit(0).cast("long"))
    )
    return (
        diagnosis.select(*CONDITION_PRIMITIVE_COLUMNS)
        .unionByName(problem.select(*CONDITION_PRIMITIVE_COLUMNS))
        .unionByName(maternity.select(*CONDITION_PRIMITIVE_COLUMNS))
    )

@materialized_view(
    name=_n("journey_clinical._problem_revision_grouped"),
    private=True,
    comment="Internal condition primitive with ordered problem-revision history and deterministic JSON boundaries.",
    refresh_policy="incremental",
)
def _problem_revision_grouped():
    # Build the declared dataset: Internal condition primitive with ordered problem-revision
    # history and deterministic JSON boundaries.
    return _problem_revision_grouped_query()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc condition

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_condition"),
    comment="Private JSON bridge and Gold cross-rule flags for condition.",
    refresh_policy="incremental",
)
def _qc_condition():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for condition.
    source = (
        spark.read.table(_n("journey_clinical._problem_revision_grouped"))
        .withColumn("revision_history_count", F.col("_revision_history_count"))
    )
    return _cross_qc_primitive(
        source,
        "condition",
        {"condition_code": "_condition_code_json"},
    )

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
CONDITION_PUBLIC_COLUMNS = [
    'patient_event_key',
    'source_object',
    'diagnosis_id',
    'problem_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'condition_code',
    'category_code',
    'category_display',
    'clinical_status_code',
    'clinical_status_display',
    'verification_status_code',
    'verification_status_display',
    'onset_datetime',
    'abatement_datetime',
    'body_site_code',
    'body_site_display',
    'severity_code',
    'severity_display',
    'laterality_code',
    'laterality_display',
    'asserted_datetime',
    'asserter_practitioner_id',
    'recorder_practitioner_id',
    'revision_history_count',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

CONDITION_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'diagnosis_id',
    'problem_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / condition

# COMMAND ----------

CONDITION_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "source_object": "Source arm: diagnosis, problem, or maternity.",
    "diagnosis_id": "Millennium DIAGNOSIS_ID; primary key for the diagnosis arm.",
    "problem_id": "Millennium PROBLEM_ID; primary key for the problem arm.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Primary assertion timestamp.",
    "event_end_datetime": "Source effective end where supplied.",
    "source_coding_system": "Verbatim source coding system.",
    "source_code": "Source condition code",
    "source_display": "Verbatim source condition display.",
    "condition_code": "Source and mapped condition codings.",
    "category_code": "Source condition category code.",
    "category_display": "Source condition category display.",
    "clinical_status_code": "Source clinical-status code.",
    "clinical_status_display": "Source clinical-status display.",
    "verification_status_code": "Source verification-status code.",
    "verification_status_display": "Source verification-status display.",
    "onset_datetime": "Source onset timestamp.",
    "abatement_datetime": "Source abatement or effective-end timestamp.",
    "body_site_code": "Body-site code where supplied.",
    "body_site_display": "Body-site display where supplied.",
    "severity_code": "Source severity code.",
    "severity_display": "Source severity display.",
    "laterality_code": "Source laterality code.",
    "laterality_display": "Source laterality display.",
    "asserted_datetime": "Source assertion timestamp.",
    "asserter_practitioner_id": "Millennium personnel PERSON_ID as BIGINT when available.",
    "recorder_practitioner_id": "Millennium personnel PERSON_ID as BIGINT when available.",
    "revision_history_count": "Number of retained problem revision rows.",
    "confidentiality_code": "Source confidentiality code.",
    "vip_ind": "Source VIP indicator.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "source_feed": "Registered source feed owning the assertion.",
    "record_status": "Diagnosis and problem arms are superseded when their END_EFFECTIVE_DT_TM is before 2100-01-01, otherwise active; a null end defaults to not-ended. The maternity-diagnosis arm is always active. No arm emits retracted here, and clinical status, source-presence flags and problem-history revision flags do not determine this label.",
    "record_status_effective_from": "ACTIVE_STATUS_DT_TM from map_diagnosis or map_problem on the corresponding Millennium arm; DIAGDATE_CLEAN from map_maternity_diagnosis on the maternity arm. Values are carried unchanged without a fallback. The maternity value is a diagnosis-date proxy rather than an independently observed row-status change.",
    "record_status_effective_to": "END_EFFECTIVE_DT_TM from the diagnosis or problem row only when it is before 2100-01-01; otherwise null. Always null on the maternity-diagnosis arm. No problem-history end, pregnancy end or ingestion timestamp is substituted.",
    "source_update_timestamp": "Source application update datetime.",
    "loaded_at": "Diagnosis and maternity arms carry their own ADC_UPDT. The problem arm takes greatest(map_problem.ADC_UPDT, maximum map_problem_history.ADC_UPDT for that PROBLEM_ID), across all retained revisions rather than only the latest. Problem-history UPDT_DT_TM is not added to the parent's source_update_timestamp, which remains its own UPDT_DT_TM; diagnosis uses its own UPDT_DT_TM and maternity uses RECORD_UPDATED_DT. The maternity identity join adds no pregnancy-spine clock, and the union adds no cross-arm timestamp maximum.",
}

@materialized_view(
    name=_n("journey_clinical.condition"),
    comment="Diagnosis and problem assertions from registered bronze feeds; no cross-feed deduplication or active-only filtering.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=CONDITION_COLUMN_COMMENTS,
)
def condition():
    # Build the declared dataset: Diagnosis and problem assertions from registered bronze feeds;
    # no cross-feed deduplication or active-only filtering.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_condition")),
        "condition",
        {"condition_code": "_condition_code_json"},
        CONDITION_PUBLIC_COLUMNS,
        CONDITION_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._condition_metadata'),
    comment='Internal quality and batch metadata for clinical_condition; same row grain as the research table. Join keys: patient_event_key, source_object, diagnosis_id, problem_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def condition_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_condition;
    # same row grain as the research table. Join keys: patient_event_key, source_object,
    # diagnosis_id, problem_id.
    return _s3_component_metadata("journey_clinical.condition", "journey_clinical._qc_condition", CONDITION_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  implant attribute grouped

# COMMAND ----------

SRC_IMPLANT_EVENTS = "4_prod.bronze.map_implant_detail_events"

def _implant_attribute_grouped_query():
    # Link implant events to their procedure or parent form, then group the implant attribute
    # evidence.
    e = read_source(SRC_IMPLANT_EVENTS)
    # contract v2: key implant attribute aggregation and joins by patient_event_key
    implants = _implant_procedure_canonical().select(
        "patient_event_key", "_base_clinical_event_id", "_implant_form_event_id"
    )
    joined = e.join(
        implants,
        (e.CLINICAL_EVENT_ID == implants._base_clinical_event_id)
        | (e.PARENT_EVENT_ID == implants._implant_form_event_id),
        "inner",
    )
    attribute = F.struct(
        F.coalesce(e.PERFORMED_DT_TM, e.EVENT_START_DT_TM).cast("long").alias("sequence"),
        e.CLINICAL_EVENT_ID.cast("string").alias("clinical_event_id"),
        e.ATTRIBUTE_NAME.alias("attribute_name"),
        e.ATTRIBUTE_VALUE.alias("attribute_value"),
        e.EVENT_TAG.alias("event_tag"),
        e.EVENT_TITLE_TEXT.alias("event_title"),
        e.RESULT_VAL.alias("result_value"),
        e.VALUE_SOURCE.alias("value_source"),
        e.VALUE_CONFLICT_IND.alias("value_conflict_ind"),
        e.PERFORMED_DT_TM.alias("performed_datetime"),
        e.VALID_FROM_DT_TM.alias("valid_from"),
    )
    return (
        joined.groupBy("patient_event_key")
        .agg(
            F.to_json(F.sort_array(F.collect_list(attribute)))
            .alias("_implant_attribute_json"),
            F.count(F.lit(1)).cast("long").alias("_implant_attribute_count"),
            F.max(e.ADC_UPDT).alias("_evidence_loaded_at"),
        )
        .select(
            F.col("patient_event_key").alias("_implant_attribute_event_key"),
            F.col("_implant_attribute_json"),
            F.col("_implant_attribute_count"),
            F.col("_evidence_loaded_at"),
        )
    )

@materialized_view(
    name=_n("journey_clinical._implant_attribute_grouped"),
    private=True,
    comment="Internal ordered implant-attribute aggregate keyed by implant procedure event.",
    refresh_policy="incremental",
)
def _implant_attribute_grouped():
    # Build the declared dataset: Internal ordered implant-attribute aggregate keyed by implant
    # procedure event.
    return _implant_attribute_grouped_query()

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
PROCEDURE_PUBLIC_COLUMNS = [
    'patient_event_key',
    'source_object',
    'procedure_id',
    'endobase_exam_id',
    'implant_event_id',
    'implant_sequence',
    'surg_case_proc_id',
    'source_row_hash',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'procedure_code',
    'status_code',
    'status_display',
    'performed_start',
    'performed_end',
    'body_site_code',
    'body_site_display',
    'laterality_code',
    'laterality_display',
    'performer_practitioner_id',
    'procedure_location_code',
    'procedure_location_display',
    'procedure_note',
    'implant_description',
    'device_code',
    'device_display',
    'manufacturer',
    'serial_number',
    'batch_number',
    'udi_di',
    'udi_standard',
    'quantity',
    'implant_attribute_count',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

PROCEDURE_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'endobase_exam_id',
    'implant_event_id',
    'implant_sequence',
    'procedure_id',
    'surg_case_proc_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  procedure primitive

# COMMAND ----------

def _procedure_primitive_query():
    # Combine procedure sources with grouped implant attributes, retaining JSON across the join-
    # shaped internal stage.
    attributes = spark.read.table(_n("journey_clinical._implant_attribute_grouped")).alias("a")
    procedure_rows = (
        _procedure_canonical()
        .withColumn("_implant_attribute_json", F.lit(None).cast("string"))
        .withColumn("_implant_attribute_count", F.lit(0).cast("long"))
    )
    # contract v2: join implant attributes to procedure rows by patient_event_key
    implant_rows = (
        _implant_procedure_canonical()
        .join(
            attributes,
            F.col("patient_event_key") == F.col("a._implant_attribute_event_key"),
            "left",
        )
        .withColumn("_implant_attribute_json", F.col("a._implant_attribute_json"))
        .withColumn(
            "_implant_attribute_count",
            F.coalesce(F.col("a._implant_attribute_count"), F.lit(0).cast("long")),
        )
        .withColumn(
            "loaded_at", F.greatest(F.col("loaded_at"), F.col("a._evidence_loaded_at"))
        )
    )
    theatre_rows = (
        _theatre_procedure_canonical()
        .withColumn("_implant_attribute_json", F.lit(None).cast("string"))
        .withColumn("_implant_attribute_count", F.lit(0).cast("long"))
    )
    cc_rows = (
        _cc_procedure_canonical()
        .withColumn("_implant_attribute_json", F.lit(None).cast("string"))
        .withColumn("_implant_attribute_count", F.lit(0).cast("long"))
    )
    endobase_rows = (
        _endobase_procedure_canonical()
        .withColumn("_implant_attribute_json", F.lit(None).cast("string"))
        .withColumn("_implant_attribute_count", F.lit(0).cast("long"))
    )
    return (
        procedure_rows.select(*PROCEDURE_PRIMITIVE_COLUMNS)
        .unionByName(implant_rows.select(*PROCEDURE_PRIMITIVE_COLUMNS))
        .unionByName(theatre_rows.select(*PROCEDURE_PRIMITIVE_COLUMNS))
        .unionByName(cc_rows.select(*PROCEDURE_PRIMITIVE_COLUMNS))
        .unionByName(endobase_rows.select(*PROCEDURE_PRIMITIVE_COLUMNS))
    )

@materialized_view(
    name=_n("journey_clinical._procedure_primitive"),
    private=True,
    comment="Internal orderable procedure union; CodeableConcept JSON crosses the incremental boundary.",
    refresh_policy="incremental",
)
def _procedure_primitive():
    # Build the declared dataset: Internal orderable procedure union; CodeableConcept JSON
    # crosses the incremental boundary.
    return _procedure_primitive_query()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc procedure

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_procedure"),
    comment="Private JSON bridge and Gold cross-rule flags for procedure.",
    refresh_policy="incremental",
)
def _qc_procedure():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for procedure.
    source = (
        spark.read.table(_n("journey_clinical._procedure_primitive"))
        .withColumn("implant_attribute_count", F.col("_implant_attribute_count"))
    )
    return _cross_qc_primitive(
        source,
        "procedure",
        {"procedure_code": "_procedure_code_json"},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / procedure

# COMMAND ----------

PROCEDURE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "source_object": "Source arm: millennium_procedure, implant, theatre, endobase_exam, or nccmds.",
    "procedure_id": "Millennium PROCEDURE_ID; primary key for the millennium_procedure arm.",
    "endobase_exam_id": "Endobase ENDOBASE_EXAM_ID; primary key for the endobase_exam arm.",
    "implant_event_id": "Millennium implant EVENT_ID; first component of the implant-arm primary key.",
    "implant_sequence": "Millennium IMPLANT_SEQUENCE; second component of the implant-arm primary key.",
    "surg_case_proc_id": "SurgiNet SURG_CASE_PROC_ID; primary key for the theatre arm.",
    "source_row_hash": "Native ROW_HASH for the nccmds arm; NULL for other arms.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Primary performed timestamp.",
    "event_end_datetime": "Procedure end timestamp.",
    "source_coding_system": "Verbatim source coding system.",
    "source_code": "Source procedure code or implant label",
    "source_display": "Verbatim source procedure display.",
    "procedure_code": "Source and mapped procedure codings.",
    "status_code": "Source-derived FHIR procedure status.",
    "status_display": "Source status display.",
    "performed_start": "Procedure performed start.",
    "performed_end": "Procedure performed end.",
    "body_site_code": "Body-site code where supplied.",
    "body_site_display": "Body-site display where supplied.",
    "laterality_code": "Source laterality code.",
    "laterality_display": "Source laterality display.",
    "performer_practitioner_id": "Millennium personnel PERSON_ID as BIGINT when available.",
    "procedure_location_code": "Source procedure-location code retained without asserting a location-dimension FK.",
    "procedure_location_display": "Source procedure-location display.",
    "procedure_note": "Source procedure note.",
    "implant_description": "Implant description for implant-placement facts.",
    "device_code": "Device concept identifier for implant-placement facts.",
    "device_display": "Device concept display.",
    "manufacturer": "Implant manufacturer name",
    "serial_number": "Implant serial number.",
    "batch_number": "Implant batch number.",
    "udi_di": "Unique device identifier device identifier.",
    "udi_standard": "UDI issuing standard.",
    "quantity": "Parsed implant quantity.",
    "implant_attribute_count": "Number of retained implant attribute rows.",
    "confidentiality_code": "Source confidentiality code.",
    "vip_ind": "Source VIP indicator.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "source_feed": "Registered source feed owning the fact.",
    "record_status": "Arm-specific label: Millennium procedures are superseded when lower(coalesce(active_status_desc, empty string)) contains the substring inactive, otherwise active. Implants are always active. Theatre procedures are superseded for source presence false (null defaults true) or ACTIVE_IND cast to LONG zero (null defaults one), otherwise active. EndoBase exams are superseded for source presence false (null defaults true), otherwise active. NCCMDS procedures instead map source absence to retracted, otherwise active. Clinical completion/cancellation fields remain separate.",
    "record_status_effective_from": "Millennium: ACTIVE_STATUS_DT_TM. Implant: VALID_FROM_DT_TM. Theatre: first non-null procedure PROC_START_DT_TM then parent case SCHED_START_DT_TM, not the wider performed-time fallback. NCCMDS: OPCS_Proc_Dt_CLEAN. EndoBase: first non-null PERFORMED_TS_CLEAN, EXAM_TS_CLEAN, TRUE_START_TS_CLEAN then START_TS_CLEAN. These arm-specific values are carried unchanged; no ingestion-time or cross-arm fallback is added.",
    "record_status_effective_to": "Millennium: ACTIVE_STATUS_DT_TM only when inactive. Implant: always null. Theatre: first non-null procedure SOURCE_ABSENT_DETECTED_TS then procedure ADC_UPDT only when inactive; the parent case clock does not contribute. NCCMDS: own ADC_UPDT only when retracted. EndoBase: own ADC_UPDT only when source-absent/superseded. All other rows have null ends; clinical procedure/exam end timestamps are not substituted.",
    "source_update_timestamp": "Source-arm-specific timestamp; interpret with source_object. Cerner procedures carry map_procedure.UPDT_DT_TM; implants carry map_implant_details.CLINSIG_UPDT_DT_TM; theatre carries the greatest SOURCE_ADC_UPDT from its case and case-procedure rows. EndoBase exams and NCCMDS procedures carry their bronze PIPELINE_UPDT_DT_TM. This mixes source and processing clocks and is not a uniform clinical event time or native-system update time.",
    "loaded_at": "Millennium, NCCMDS and EndoBase arms use their own ADC_UPDT. Theatre uses greatest of procedure ADC_UPDT and joined case ADC_UPDT. Implant first takes coalesce(SOURCE_MAX_ADC_UPDT, ADC_UPDT, BASE_EVENT_ADC_UPDT), in that order, then greatest of this result and maximum ADC_UPDT from matching map_implant_detail_events grouped by patient_event_key. Attribute matching uses the base clinical-event ID or implant-form parent-event ID. The ordered implant fallback is not itself a maximum; the five-arm union and QC add no further clocks or Silver refresh timestamp.",
}

@materialized_view(
    name=_n("journey_clinical.procedure"),
    comment="Performed procedure and implant-placement evidence from registered bronze feeds; source facts remain separate.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=PROCEDURE_COLUMN_COMMENTS,
)
def procedure():
    # Build the declared dataset: Performed procedure and implant-placement evidence from
    # registered bronze feeds; source facts remain separate.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_procedure")),
        "procedure",
        {"procedure_code": "_procedure_code_json"},
        PROCEDURE_PUBLIC_COLUMNS,
        PROCEDURE_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._procedure_metadata'),
    comment='Internal quality and batch metadata for clinical_procedure; same row grain as the research table. Join keys: patient_event_key, source_object, endobase_exam_id, implant_event_id, implant_sequence, procedure_id, surg_case_proc_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def procedure_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_procedure;
    # same row grain as the research table. Join keys: patient_event_key, source_object,
    # endobase_exam_id, implant_event_id, implant_sequence, procedure_id, surg_case_proc_id.
    return _s3_component_metadata("journey_clinical.procedure", "journey_clinical._qc_procedure", PROCEDURE_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / condition revision

# COMMAND ----------

# ==== journey_clinical.condition_revision ====

# contract v2: retain the full canonical shape for internal reuse.
CONDITION_REVISION_SOURCE_COLUMNS = [
    "patient_event_key",
    "problem_id",
    "sequence",
    "revision_rank",
    "is_current_revision",
    "clinical_status_code",
    "clinical_status_display",
    "clinical_status_datetime",
    "verification_status_code",
    "verification_status_display",
    "display",
    "severity_code",
    "severity_display",
    "onset_datetime",
    "asserted_datetime",
    "effective_start",
    "effective_end",
    "source_tombstone_ind",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
CONDITION_REVISION_PUBLIC_COLUMNS = [
    'patient_event_key',
    'problem_id',
    'sequence',
    'revision_rank',
    'is_current_revision',
    'clinical_status_code',
    'clinical_status_display',
    'clinical_status_datetime',
    'verification_status_code',
    'verification_status_display',
    'display',
    'severity_code',
    'severity_display',
    'onset_datetime',
    'asserted_datetime',
    'effective_start',
    'effective_end',
    'source_tombstone_ind',
    'loaded_at',
    'source_update_timestamp',
]

CONDITION_REVISION_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent clinical_condition row.",
    "problem_id": "Millennium PROBLEM_ID of the parent problem condition.",
    "sequence": "Problem revision sequence, defaulted to zero when the source rank is null.",
    "revision_rank": "Source problem revision rank.",
    "is_current_revision": "Whether the source marks this as the current problem revision.",
    "clinical_status_code": "Source lifecycle-status code for this revision.",
    "clinical_status_display": "Source lifecycle-status display for this revision.",
    "clinical_status_datetime": "Source lifecycle-status timestamp for this revision.",
    "verification_status_code": "Source confirmation-status code for this revision.",
    "verification_status_display": "Source confirmation-status display for this revision.",
    "display": "Best available source problem display for this revision.",
    "severity_code": "Source severity code for this revision.",
    "severity_display": "Source severity display for this revision.",
    "onset_datetime": "Source onset timestamp carried by this revision.",
    "asserted_datetime": "Source assertion timestamp carried by this revision.",
    "effective_start": "Source revision effective-start timestamp.",
    "effective_end": "Source revision effective-end timestamp.",
    "source_tombstone_ind": "Whether the source revision is marked deleted.",
    "loaded_at": "Bronze load timestamp of the problem-history row.",
    "source_update_timestamp": "Source application update datetime.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_condition_revision():
    # Assemble condition revision rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    s = read_source(SRC_PROBLEM_HISTORY)
    # Contract v2 parent/child conservation: clinical_condition publishes only the
    # problems whose code-or-display passes Gate B, so the revision child applies
    # the same gate. Taking the eligible ids from the parent's own canonical
    # expression keeps the child from drifting if that rule ever changes. Without
    # it the child carried 22 rows that no reader could reach through the parent.
    current = (
        _condition_problem_canonical()
        .select(F.col("problem_id").alias("PROBLEM_ID"))
        .distinct()
    )
    s = s.join(current, "PROBLEM_ID", "inner")
    return s.select(
        stable_id("condition:mill:problem", s.PROBLEM_ID).alias("patient_event_key"),
        s.PROBLEM_ID.cast("bigint").alias("problem_id"),
        F.coalesce(s.PROBLEM_REVISION_RANK, F.lit(0)).cast("long").alias("sequence"),
        s.PROBLEM_REVISION_RANK.cast("long").alias("revision_rank"),
        s.IS_CURRENT_PROBLEM_REVISION.alias("is_current_revision"),
        s.LIFE_CYCLE_STATUS_CD.cast("string").alias("clinical_status_code"),
        s.life_cycle_status_desc.alias("clinical_status_display"),
        s.LIFE_CYCLE_DT_TM.alias("clinical_status_datetime"),
        s.CONFIRMATION_STATUS_CD.cast("string").alias("verification_status_code"),
        s.confirmation_status_desc.alias("verification_status_display"),
        F.coalesce(s.ANNOTATED_DISPLAY, s.PROBLEM_DISPLAY, s.SOURCE_STRING).alias("display"),
        s.SEVERITY_CD.cast("string").alias("severity_code"),
        s.severity_desc.alias("severity_display"),
        s.ONSET_DT_TM.alias("onset_datetime"),
        s.ASSERTED_DT_TM.alias("asserted_datetime"),
        s.BEG_EFFECTIVE_DT_TM.alias("effective_start"),
        s.END_EFFECTIVE_DT_TM.alias("effective_end"),
        F.coalesce(s.SOURCE_DELETED_IND, F.lit(False)).alias("source_tombstone_ind"),
        s.UPDT_DT_TM.alias("source_update_timestamp"),
        s.ADC_UPDT.alias("loaded_at"),
    ).select(*CONDITION_REVISION_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.condition_revision"),
    comment="One Millennium problem-history revision per parent clinical_condition problem row.",
    column_comments=CONDITION_REVISION_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def condition_revision():
    # Build the declared dataset: One Millennium problem-history revision per parent
    # clinical_condition problem row.
    return _lifecycle_source_condition_revision().select(*CONDITION_REVISION_PUBLIC_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / procedure implant attribute

# COMMAND ----------

# ==== journey_clinical.procedure_implant_attribute ====

PROCEDURE_IMPLANT_ATTRIBUTE_PUBLIC_COLUMNS = [
    "patient_event_key", "implant_event_id", "implant_sequence", "sequence",
    "clinical_event_id", "attribute_name", "attribute_value", "event_tag",
    "event_title", "result_value", "value_source", "value_conflict_ind",
    "performed_datetime", "valid_from", "loaded_at",
]

PROCEDURE_IMPLANT_ATTRIBUTE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent implant clinical_procedure row.",
    "implant_event_id": "Millennium implant EVENT_ID; first native parent-key component.",
    "implant_sequence": "Millennium IMPLANT_SEQUENCE; second native parent-key component.",
    "sequence": "Deterministic source ordering value derived from performed or event-start time.",
    "clinical_event_id": "Source clinical-event identifier for the implant attribute.",
    "attribute_name": "Source implant attribute name.",
    "attribute_value": "Source implant attribute value.",
    "event_tag": "Source clinical-event tag.",
    "event_title": "Source clinical-event title.",
    "result_value": "Source result value.",
    "value_source": "Source route that supplied the retained value.",
    "value_conflict_ind": "Whether source evidence for the value conflicts.",
    "performed_datetime": "Source performed timestamp.",
    "valid_from": "Source validity-start timestamp.",
    "loaded_at": "Bronze load timestamp of the implant attribute row.",
}

@materialized_view(
    name=_n("journey_clinical.procedure_implant_attribute"),
    comment="One source implant attribute per parent implant clinical_procedure row.",
    column_comments=PROCEDURE_IMPLANT_ATTRIBUTE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def procedure_implant_attribute():
    # Build the declared dataset: One source implant attribute per parent implant
    # clinical_procedure row.
    e = read_source(SRC_IMPLANT_EVENTS)
    implants = _implant_procedure_canonical().select(
        "patient_event_key", "implant_event_id", "implant_sequence",
        "_base_clinical_event_id", "_implant_form_event_id",
    )
    joined = e.join(
        implants,
        (e.CLINICAL_EVENT_ID == implants._base_clinical_event_id)
        | (e.PARENT_EVENT_ID == implants._implant_form_event_id),
        "inner",
    )
    return joined.select(
        implants.patient_event_key.alias("patient_event_key"),
        implants.implant_event_id.alias("implant_event_id"),
        implants.implant_sequence.alias("implant_sequence"),
        F.coalesce(e.PERFORMED_DT_TM, e.EVENT_START_DT_TM).cast("long").alias("sequence"),
        e.CLINICAL_EVENT_ID.cast("string").alias("clinical_event_id"),
        e.ATTRIBUTE_NAME.alias("attribute_name"),
        e.ATTRIBUTE_VALUE.alias("attribute_value"),
        e.EVENT_TAG.alias("event_tag"),
        e.EVENT_TITLE_TEXT.alias("event_title"),
        e.RESULT_VAL.alias("result_value"),
        e.VALUE_SOURCE.alias("value_source"),
        e.VALUE_CONFLICT_IND.alias("value_conflict_ind"),
        e.PERFORMED_DT_TM.alias("performed_datetime"),
        e.VALID_FROM_DT_TM.alias("valid_from"),
        e.ADC_UPDT.alias("loaded_at"),
    ).select(*PROCEDURE_IMPLANT_ATTRIBUTE_PUBLIC_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pathology, reports and specimens

# COMMAND ----------

# ==== Pathology, reports and specimens ====

SRC_PATHOLOGY_ACCESSION_SOURCE = "4_prod.bronze.map_pathology_accession_source"

def _pathology_parent_link_canonical():
    # Assemble normalized pathology parent link rows for downstream dataset builders, preserving
    # the existing source and identity rules.
    src = _alias_resolved_accession(read_source(SRC_PATHOLOGY_ACCESSION_SOURCE))
    ver = read_source(SRC_PATHOLOGY_REPORT_VERSIONS).select(
        F.col("source_record_key").alias("source_parent_key"),
        F.col("report_series_id"),
    )
    return (
        src.select(
            "source_parent_key", "source_system",
            F.col("_resolved_accession_id").alias("pathology_accession_id"),
            F.col("encounter_id").alias("source_encounter_id"),
            F.col("ADC_UPDT").alias("loaded_at"),
        )
        .join(ver, "source_parent_key", "left")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Events /  pathology parent link

# COMMAND ----------

@materialized_view(
    name=_n("journey_events._pathology_parent_link"),
    private=True,
    comment="INTERNAL source-parent linkage: source_parent_key -> alias-resolved accession, "
            "report series (1:1 parent->version->series, preflight-verified), and parent encounter.",
    refresh_policy="incremental",
)
def _pathology_parent_link():
    # Build the declared dataset: INTERNAL source-parent linkage: source_parent_key -> alias-
    # resolved accession, report series (1:1 parent->version->series, preflight-verified), and
    # parent encounter.
    return _pathology_parent_link_canonical()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Events /  pathology accession identity

# COMMAND ----------

@materialized_view(
    name=_n("journey_events._pathology_accession_identity"),
    private=True,
    comment="INTERNAL accession identity: status-honest registry person columns plus MRN/NHS "
            "evidence aggregated from accession_source. Survivor accessions only; aliases resolve "
            "before grouping so retired evidence follows the survivor.",
    refresh_policy="incremental",
)
def _pathology_accession_identity():
    # Build the declared dataset: INTERNAL accession identity: status-honest registry person
    # columns plus MRN/NHS evidence aggregated from accession_source. Survivor accessions only;
    # aliases resolve before grouping so retired evidence follows the survivor.
    acc = _alias_resolved_accession(read_source(SRC_PATHOLOGY_ACCESSION))
    survivors = acc.where(F.col("pathology_accession_id") == F.col("_resolved_accession_id"))
    ev = (
        _alias_resolved_accession(read_source(SRC_PATHOLOGY_ACCESSION_SOURCE))
        .groupBy(F.col("_resolved_accession_id").alias("pathology_accession_id"))
        .agg(
            F.max("mrn").alias("evidence_mrn"),
            F.max("nhs_number").alias("evidence_nhs"),
            F.count(F.lit(1)).cast("long").alias("source_row_count"),
            F.max("ADC_UPDT").alias("evidence_loaded_at"),
        )
    )
    return survivors.select(
        "pathology_accession_id", "primary_source_accession_id", "canonical_accession_status",
        "canonical_person_id", "person_resolution_status", "normalized_lab_no",
        "lab_series", "discipline", "request_dt", "sample_dt", "report_dt",
        "clinical_details", "tlcs_requested", "conditions", "reason", "urgent_flag",
        "body_site_code", "body_site_snomed_code", "specimen_type_code",
        "specimen_type_snomed_code", "source_site_code", "lifecycle_status",
        "research_qi_only", "created_at", "ADC_UPDT",
    ).join(ev, "pathology_accession_id", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc specimen

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_specimen"),
    comment="Private JSON bridge and Gold cross-rule flags for specimen.",
    refresh_policy="incremental",
)
def _qc_specimen():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for specimen.
    return _cross_qc_primitive(
        _pathology_specimen_canonical(),
        "specimen",
        {"specimen_type": "_qc_specimen_type_json"},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / specimen

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
SPECIMEN_PUBLIC_COLUMNS = [
    'patient_event_key',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'specimen_type',
    'accession_identifier',
    'resolved_accession_key',
    'normalized_lab_no',
    'canonical_accession_status',
    'person_resolution_status',
    'lab_series',
    'discipline',
    'urgent_flag',
    'research_qi_only',
    'clinical_details',
    'tlcs_requested',
    'conditions',
    'reason',
    'body_site_code',
    'body_site_snomed_code',
    'specimen_type_code',
    'specimen_type_snomed_code',
    'sample_datetime',
    'request_datetime',
    'report_datetime',
    'source_history_row_count',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

SPECIMEN_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'identity_status',
    'person_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

SPECIMEN_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Collection or receipt timestamp.",
    "event_end_datetime": "Event end when supplied.",
    "source_coding_system": "Source specimen type system.",
    "source_code": "Source specimen type code.",
    "source_display": "Source specimen type display.",
    "specimen_type": "Source specimen type CodeableConcept.",
    "accession_identifier": "Canonical accession identifier (pathology_accession_id); never LabNo-derived.",
    "resolved_accession_key": "Alias-resolved accession key retained as derivation evidence.",
    "normalized_lab_no": "Matching evidence only — reused lab numbers reach 3",
    "canonical_accession_status": "Canonical accession-link state.",
    "person_resolution_status": "Person-projection eligibility state.",
    "lab_series": "Source laboratory series.",
    "discipline": "Source pathology discipline.",
    "urgent_flag": "Source urgent-request indicator code (Y",
    "research_qi_only": "Registry doctrine flag; true on 100% of rows today — describe-only",
    "clinical_details": "Source clinical details. Identifiable free text; ig_risk 4",
    "tlcs_requested": "Requested TLC context. Identifiable free text; ig_risk 4",
    "conditions": "Source request conditions. Identifiable free text; ig_risk 4",
    "reason": "Source request reason. Identifiable free text; ig_risk 4",
    "body_site_code": "Source collection body-site code.",
    "body_site_snomed_code": "Source-provided SNOMED body-site code.",
    "specimen_type_code": "Native specimen-type code.",
    "specimen_type_snomed_code": "Source-provided SNOMED specimen-type code.",
    "sample_datetime": "Clamped source sample timestamp.",
    "request_datetime": "Clamped source request timestamp.",
    "report_datetime": "Clamped source report timestamp.",
    "source_history_row_count": "accession_source rows behind this accession.",
    "confidentiality_code": "Source confidentiality code.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each specimen record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Registered owning feed.",
    "record_status": "Always active in the current specimen projection of surviving pathology accessions. The input is restricted to accessions whose ID equals the alias-resolved survivor ID; canonical_accession_status and lifecycle_status do not change this literal label.",
    "record_status_effective_from": "Time this immutable accession identifier was first assigned.",
    "record_status_effective_to": "Always null as a TIMESTAMP in the current specimen projection. Sample, request and report dates and accession lifecycle fields are not used as status ends.",
    "source_update_timestamp": "Time this immutable accession identifier was first assigned.",
    "loaded_at": "ADC_UPDT of the surviving map_pathology_accession row, carried unchanged through identity and QC/public projections. The separately aggregated maximum evidence_loaded_at from alias-resolved map_pathology_accession_source rows is not used here. This is the accession row's load clock, not created_at, specimen collection time or Silver refresh time.",
}

@materialized_view(
    name=_n("journey_clinical.specimen"),
    comment="One canonical pathology accession with collection and request evidence.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=SPECIMEN_COLUMN_COMMENTS,
)
def specimen():
    # Build the declared dataset: One canonical pathology accession with collection and request
    # evidence.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_specimen")),
        "specimen",
        {"specimen_type": "_qc_specimen_type_json"},
        SPECIMEN_PUBLIC_COLUMNS,
        SPECIMEN_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._specimen_metadata'),
    comment='Internal quality and batch metadata for clinical_specimen; same row grain as the research table. Join keys: patient_event_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def specimen_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_specimen;
    # same row grain as the research table. Join keys: patient_event_key.
    return (spark.read.table(_n("journey_clinical._qc_specimen"))).select(*SPECIMEN_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc pathology report

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_pathology_report"),
    comment="Private JSON bridge and Gold cross-rule flags for pathology_report.",
    refresh_policy="incremental",
)
def _qc_pathology_report():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # pathology_report.
    return _cross_qc_primitive(
        _pathology_report_series_canonical(),
        "pathology_report",
        {"report_code": "_qc_report_code_json"},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / pathology report

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
REPORT_PUBLIC_COLUMNS = [
    'patient_event_key',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'report_code',
    'report_role',
    'discipline',
    'report_section',
    'lifecycle_status',
    'version_ordinal',
    'version_count',
    'report_version_key',
    'supersedes_report_version_key',
    'is_current_present',
    'document_key',
    'issued_datetime',
    'specimen_key',
    'accession_identifier',
    'report_text_hash',
    'research_qi_only',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

REPORT_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

PATHOLOGY_REPORT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Report issue or result timestamp.",
    "event_end_datetime": "Report event end.",
    "source_coding_system": "Source test coding system.",
    "source_code": "Source test code.",
    "source_display": "Source test display.",
    "report_code": "Source and mapped report codings.",
    "report_role": "Latest-version report role.",
    "discipline": "Latest-version pathology discipline.",
    "report_section": "Latest-version report section.",
    "lifecycle_status": "Latest-version lifecycle status.",
    "version_ordinal": "Latest version ordinal in the report series.",
    "version_count": "Number of report versions in the series.",
    "report_version_key": "Deterministic report-version key.",
    "supersedes_report_version_key": "Deterministic prior report-version key.",
    "is_current_present": "True when the series has a current version row.",
    "document_key": "Deterministic text-document key.",
    "issued_datetime": "Report issue timestamp.",
    "specimen_key": "Deterministic specimen key.",
    "accession_identifier": "Alias-resolved canonical pathology accession identifier carried by the selected report-series version; not the source result-record key.",
    "report_text_hash": "Bronze hash for the latest report version",
    "research_qi_only": "Registry doctrine flag; true on 100% of rows today — describe-only",
    "confidentiality_code": "Source confidentiality classification attached to the record for each pathology report record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each pathology report record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Registered owning feed.",
    "record_status": "Derived from lifecycle_status of the selected map_pathology_report version: retracted for cancelled or entered_in_error, otherwise active, including a null lifecycle status. Selection is the grouped maximum full version struct, beginning with version_ordinal, report_version_id and source_record_key. This label is not the is_current_present flag and does not emit superseded.",
    "record_status_effective_from": "valid_from of the selected map_pathology_report version, carried unchanged as the report-series history start. The selected full version struct is ranked first by version_ordinal, report_version_id and source_record_key. This is not issued_datetime, a group-wide maximum validity timestamp or Silver refresh time; a missing value remains null.",
    "record_status_effective_to": "Always NULL as a TIMESTAMP in the pathology-report-series projection, including retracted reports. No cancellation, successor-version or ingestion timestamp is substituted as a status end.",
    "source_update_timestamp": "valid_from of the selected map_pathology_report version, carried unchanged. This field is a selected-version validity timestamp, not an independent source modification clock, the group-wide maximum ADC_UPDT or Silver refresh time; a missing value remains null.",
    "loaded_at": "Maximum map_pathology_report.ADC_UPDT across all contributing versions in the report_series_id group, not only the version selected for display. Accession aliases and identity/link lookups supply no additional load clock. This is ingestion provenance, not report issue time or Silver refresh time; null when all contributing clocks are null.",
}

@materialized_view(
    name=_n("journey_clinical.pathology_report"),
    comment="One pathology report series; latest-version projection; versions in journey_text.document.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=PATHOLOGY_REPORT_COLUMN_COMMENTS,
)
def pathology_report():
    # Build the declared dataset: One pathology report series; latest-version projection;
    # versions in journey_text.document.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_pathology_report")),
        "pathology_report",
        {"report_code": "_qc_report_code_json"},
        REPORT_PUBLIC_COLUMNS,
        REPORT_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._pathology_report_metadata'),
    comment='Internal quality and batch metadata for clinical_pathology_report; same row grain as the research table. Join keys: patient_event_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def pathology_report_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_pathology_report; same row grain as the research table. Join keys:
    # patient_event_key.
    return (spark.read.table(_n("journey_clinical._qc_pathology_report"))).select(*REPORT_LIFECYCLE_COLUMNS)

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
RESULT_PUBLIC_COLUMNS = [
    'patient_event_key',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'result_code',
    'pathology_report_key',
    'specimen_key',
    'equivalence_group_key',
    'representation_role',
    'preferred_result_ind',
    'person_projection_status',
    'value_number',
    'value_text',
    'value_datetime',
    'value_concept_id',
    'value_concept_display',
    'operator_concept_id',
    'unit_source_value',
    'ucum_code',
    'unit_concept_id',
    'reference_range_low',
    'reference_range_high',
    'interpretation_code',
    'polarity',
    'finding_axis',
    'result_status',
    'body_site_code',
    'clinician_code',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

RESULT_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  pathology result primitive

# COMMAND ----------

SRC_PATHOLOGY_RESULT_EQUIVALENCE = "4_prod.bronze.map_pathology_result_equivalence"

@materialized_view(
    name=_n("journey_clinical._pathology_result_primitive"),
    private=True,
    comment="Internal repointed result rows; CodeableConcept JSON crosses the incremental "
            "boundary — a VARIANT output column on a join-shaped flow crashes Enzyme planning "
            "(GROUP_EXPRESSION_TYPE_IS_NOT_ORDERABLE) and forces complete recompute every update.",
    refresh_policy="incremental",
)
def _pathology_result_primitive():
    # Build the declared dataset: Internal repointed result rows; CodeableConcept JSON crosses
    # the incremental boundary — a VARIANT output column on a join-shaped flow crashes Enzyme
    # planning (GROUP_EXPRESSION_TYPE_IS_NOT_ORDERABLE) and forces complete recompute every
    # update.
    raw = read_source(SRC_PATHOLOGY).alias("s")
    link = _pathology_parent_link_canonical().select(
        F.col("source_parent_key").alias("_source_parent_key"),
        F.col("pathology_accession_id").alias("_link_accession_id"),
        F.col("report_series_id").alias("_link_series_id"),
    ).alias("l")
    eq = read_source(SRC_PATHOLOGY_RESULT_EQUIVALENCE).select(
        F.col("source_record_key").alias("_eq_key"),
        F.col("canonical_result_id").alias("_eq_group"),
        F.col("representation_role").alias("_eq_role"),
        F.col("preferred_result_ind").alias("_eq_preferred"),
        F.col("person_projection_status").alias("_eq_person_projection"),
    ).alias("e")
    joined = (
        raw.join(link, raw.source_parent_key == F.col("l._source_parent_key"), "left")
        .join(eq, raw.source_record_key == F.col("e._eq_key"), "left")
        .select("s.*", "l._link_accession_id", "l._link_series_id",
                "e._eq_group", "e._eq_role", "e._eq_preferred", "e._eq_person_projection")
    )
    return _pathology_result_canonical(
        joined, include_parent_links=True
    ).select(*RESULT_PRIMITIVE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc pathology result

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_pathology_result"),
    comment="Private JSON bridge and Gold cross-rule flags for pathology_result.",
    refresh_policy="incremental",
)
def _qc_pathology_result():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # pathology_result.
    return _cross_qc_primitive(
        spark.read.table(_n("journey_clinical._pathology_result_primitive")),
        "pathology_result",
        {"result_code": "_result_code_json"},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / pathology result

# COMMAND ----------

PATHOLOGY_RESULT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date and time when the represented clinical or administrative event occurred for each pathology result record. It is derived from bronze field `measurement_datetime` in `4_prod.bronze.map_pathology`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "event_end_datetime": "Measurement end timestamp.",
    "source_coding_system": "Source test coding system.",
    "source_code": "Source test code.",
    "source_display": "Source test display.",
    "result_code": "Source and mapped test codings.",
    "pathology_report_key": "Deterministic pathology-report series key.",
    "specimen_key": "Deterministic specimen key.",
    "equivalence_group_key": "Derived cross-arm result-equivalence key.",
    "representation_role": "Result representation role within the equivalence group.",
    "preferred_result_ind": "Whether this is the preferred representation.",
    "person_projection_status": "Equivalence-layer person projection state.",
    "value_number": "Numeric result value.",
    "value_text": "Verbatim result value.",
    "value_datetime": "Datetime result value.",
    "value_concept_id": "Coded result concept identifier.",
    "value_concept_display": "Coded result concept display.",
    "operator_concept_id": "Result comparison operator concept.",
    "unit_source_value": "Verbatim source unit.",
    "ucum_code": "UCUM unit code.",
    "unit_concept_id": "OMOP unit concept identifier.",
    "reference_range_low": "Reference-range lower bound.",
    "reference_range_high": "Reference-range upper bound.",
    "interpretation_code": "Source interpretation or normalcy.",
    "polarity": "Source result polarity or growth grade.",
    "finding_axis": "Source finding/result axis.",
    "result_status": "Source result status.",
    "body_site_code": "Source body-site code.",
    "clinician_code": "Source clinician code.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each pathology result record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each pathology result record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Registered owning feed.",
    "record_status": "Superseded when map_pathology.valid_until_dt_tm is non-null and earlier than 2100-01-01; otherwise active. No retracted status is emitted. Result-status text, equivalence preference and parent linkage do not determine this label; the usable-code filter controls eligibility separately.",
    "record_status_effective_from": "Timestamp from which the normalised record status applies, when the source supplies one.",
    "record_status_effective_to": "map_pathology.valid_until_dt_tm only when non-null and earlier than 2100-01-01; otherwise null. The predicate is identical to the superseded-status test. Unlike event timestamps, this history end has no 1950 lower clamp; neither event_end_dt_tm nor ingestion time is substituted.",
    "source_update_timestamp": "Greatest contributing source update timestamp; never changed by mapping-only backfills.",
    "loaded_at": "map_pathology.ADC_UPDT carried unchanged through parent-link and result-equivalence joins, usable-code filtering and QC/public projections. Those joins select linkage/equivalence fields, not additional clocks; parent-link loaded_at is not propagated. This differs from source_adc_updt used for source_update_timestamp and from Silver refresh time.",
}

@materialized_view(
    name=_n("journey_clinical.pathology_result"),
    comment="One pathology result with typed values, units, ranges and retained mapping evidence.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=PATHOLOGY_RESULT_COLUMN_COMMENTS,
)
def pathology_result():
    # Build the declared dataset: One pathology result with typed values, units, ranges and
    # retained mapping evidence.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_pathology_result")),
        "pathology_result",
        {"result_code": "_result_code_json"},
        RESULT_PUBLIC_COLUMNS,
        RESULT_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._pathology_result_metadata'),
    comment='Internal quality and batch metadata for clinical_pathology_result; same row grain as the research table. Join keys: patient_event_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def pathology_result_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_pathology_result; same row grain as the research table. Join keys:
    # patient_event_key.
    return (spark.read.table(_n("journey_clinical._qc_pathology_result"))).select(*RESULT_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / pathology order

# COMMAND ----------

PATHOLOGY_ORDER_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic pathology-order event key minted from requested_test_occurrence_id.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Requested or source-validity timestamp.",
    "event_end_datetime": "Order event end when supplied.",
    "source_coding_system": "Native order catalogue coding system.",
    "source_code": "Native order catalogue code.",
    "source_display": "Native order description.",
    "source_object": "Source arm normalized from the existing source_system value.",
    "wkg_code": "TFC work-group code.",
    "tlc_code": "TFC test-level code.",
    "order_id": "CERNER ORDER_ID as BIGINT; NULL on the TFC_LIMS arm.",
    "order_mnemonic": "CERNER order mnemonic.",
    "raw_request_text": "Native request wording. Identifiable free text; ig_risk 4",
    "test_description": "Native requested-test description.",
    "test_snomed_code": "Source-provided requested-test SNOMED code.",
    "test_omop_concept_id": "Source-provided requested-test OMOP concept identifier.",
    "mapping_status": "'mapped' means a rule ran, not that codes landed — CERNER arm carries zero baked codes.",
    "request_ordinal": "Source request ordinal within the accession.",
    "specimen_key": "Alias-resolved accession-minted specimen key.",
    "confidentiality_code": "Source confidentiality code.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each pathology order record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "record_status": "Literal active for every retained requested-test occurrence from map_pathology_requested_test. This projection does not evaluate source presence, cancellation or result/report status; active is not evidence that an order has never been withdrawn.",
    "record_status_effective_from": "Always NULL as a TIMESTAMP in the pathology-order projection. No status-start clock is supplied, and accession request/sample times are not substituted.",
    "record_status_effective_to": "Always NULL as a TIMESTAMP in the pathology-order projection. No status-end clock is supplied, and result, report or ingestion timestamps are not substituted.",
    "source_update_timestamp": "Always NULL as a TIMESTAMP in the pathology-order projection. The contributing ADC_UPDT is published separately as loaded_at and is not copied into this field.",
    "loaded_at": "map_pathology_requested_test.ADC_UPDT carried unchanged for the requested-test occurrence. Accession alias and identity joins do not contribute another clock. This is ingestion provenance, not the request/sample event time or Silver refresh time; a missing timestamp remains null.",
}

PATHOLOGY_ORDER_LIFECYCLE_FIELDS = [
    'identity_status',
    'load_batch_id',
]

PATHOLOGY_ORDER_RETIRED_COLUMNS = [

]

PATHOLOGY_ORDER_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'identity_status',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_pathology_order():
    # Assemble pathology order rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    s = _pathology_requested_test_canonical()
    # contract v2: publish the direct pathology-order projection with source_object and key names
    return s.select(
        "patient_event_key", "subject_key", "subject_id_system", "person_id",
        "identity_status", "encounter_id", "event_datetime", "event_end_datetime",
        "source_coding_system", "source_code", "source_display",
        "source_object", "wkg_code", "tlc_code", "order_id", "order_mnemonic",
        "raw_request_text", "test_description", "test_snomed_code", "test_omop_concept_id",
        "mapping_status", "request_ordinal", "specimen_key",
        "record_status", "record_status_effective_from", "record_status_effective_to",
        "confidentiality_code", "vip_ind", "withheld_identity_ind",
        "load_batch_id", "source_update_timestamp", "loaded_at",
    )

@materialized_view(
    name=_n("journey_clinical.pathology_order"),
    comment="One requested-test occurrence across TFC_LIMS and CERNER; anchors accession-scoped request threads.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=PATHOLOGY_ORDER_COLUMN_COMMENTS,
)
def pathology_order():
    # Build the declared dataset: One requested-test occurrence across TFC_LIMS and CERNER;
    # anchors accession-scoped request threads.
    return _lifecycle_source_pathology_order().drop(*PATHOLOGY_ORDER_LIFECYCLE_FIELDS, *PATHOLOGY_ORDER_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._pathology_order_metadata'),
    comment='Internal quality and batch metadata for clinical_pathology_order; same row grain as the research table. Join keys: patient_event_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def pathology_order_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_pathology_order; same row grain as the research table. Join keys:
    # patient_event_key.
    return (_lifecycle_source_pathology_order()).select(*PATHOLOGY_ORDER_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / genomic test

# COMMAND ----------

# ==== Genomics, indication and microbiology/AMR ====

# contract v2: retain the full canonical shape for internal reuse.
GENOMIC_TEST_SOURCE_COLUMNS = [
    "patient_event_key",
    "genetic_test_id",
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
    "assay_code",
    "assay_name",
    "method",
    "analysis_context",
    "overall_result_status",
    "panel_code",
    "panel_version",
    "panel_version_inferred",
    "parser_profile_id",
    "report_version_key",
    "pathology_report_key",
    "specimen_key",
    "accession_identifier",
    "test_snomed_code",
    "test_loinc_code",
    "test_omop_concept_id",
    "is_current",
    "research_qi_only",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
GENOMIC_TEST_PUBLIC_COLUMNS = [
    'patient_event_key',
    'genetic_test_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'assay_code',
    'assay_name',
    'method',
    'analysis_context',
    'overall_result_status',
    'panel_code',
    'panel_version',
    'panel_version_inferred',
    'parser_profile_id',
    'report_version_key',
    'pathology_report_key',
    'specimen_key',
    'accession_identifier',
    'test_snomed_code',
    'test_loinc_code',
    'test_omop_concept_id',
    'is_current',
    'research_qi_only',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

GENOMIC_TEST_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'genetic_test_id',
    'identity_status',
    'load_batch_id',
]

GENOMIC_TEST_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "genetic_test_id": "Pathology genetic_test_id; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace selected for `subject_key`: Millennium person identifier first, then MRN, then NHS number; nosubject denotes the per-source-record fallback when none is present.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Report issue time with accession fallback.",
    "event_end_datetime": "Event end time.",
    "source_coding_system": "Assay coding namespace.",
    "source_code": "Source assay code.",
    "source_display": "Source assay display.",
    "assay_code": "Source assay or report code.",
    "assay_name": "Resolved assay name.",
    "method": "Reported assay method.",
    "analysis_context": "Somatic or germline analysis context.",
    "overall_result_status": "Source assay-level result status.",
    "panel_code": "Governed panel code.",
    "panel_version": "Explicit or inferred panel version.",
    "panel_version_inferred": "Whether panel version was inferred.",
    "parser_profile_id": "Deterministic parser profile.",
    "report_version_key": "Deterministic report-version key.",
    "pathology_report_key": "Deterministic pathology-report series key.",
    "specimen_key": "Deterministic specimen key.",
    "accession_identifier": "Alias-resolved accession identifier.",
    "test_snomed_code": "Approved SNOMED assay code.",
    "test_loinc_code": "Approved LOINC assay code.",
    "test_omop_concept_id": "Standard OMOP assay concept.",
    "is_current": "Whether the source report version is current.",
    "research_qi_only": "Research/QI release flag.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each genomic test record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each genomic test record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Originating data feed responsible for the record for each genomic test record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Superseded when map_pathology_genetic_test.is_current is false, otherwise active; missing is_current defaults true. No retracted status is emitted. The assay's overall_result_status is separate, and joined report/identity fields do not set this label.",
    "record_status_effective_from": "Always null as a TIMESTAMP in the current genomic-test projection. No source-status start is supplied here; the clinical event's report/sample/request date fallbacks and ADC_UPDT are not used for this field.",
    "record_status_effective_to": "Always null as a TIMESTAMP in the current genomic-test projection, including when its status or source-current flag changes. No report, clinical-event or ingestion timestamp is substituted as a status end.",
    "source_update_timestamp": "map_pathology_genetic_test.ADC_UPDT carried unchanged from the contributing row, identical to loaded_at. No independent native application-update clock is projected here, and Silver refresh time is not substituted.",
    "loaded_at": "map_pathology_genetic_test.ADC_UPDT carried unchanged through its joins, usable-code filtering and source/public projections. Alias resolution and accession identity add no load clocks; neither joined report/parent timestamps nor the separately aggregated accession evidence clock replace this value. It is identical to source_update_timestamp, not the clinical event time or Silver refresh time.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_genomic_test():
    # Assemble genomic test rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return _genomic_test_canonical().select(*GENOMIC_TEST_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.genomic_test"),
    comment="One molecular/cytogenetic assay per report version (all versions published; "
            "record_status supersedes on is_current). Vocab and panel columns are all-NULL "
            "in the current build and lanes are pre-wired.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=GENOMIC_TEST_COLUMN_COMMENTS,
)
def genomic_test():
    # Build the declared dataset: One molecular/cytogenetic assay per report version (all
    # versions published; record_status supersedes on is_current). Vocab and panel columns are
    # all-NULL in the current build and lanes are pre-wired.
    return _lifecycle_source_genomic_test().select(*GENOMIC_TEST_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._genomic_test_metadata'),
    comment='Internal quality and batch metadata for clinical_genomic_test; same row grain as the research table. Join keys: patient_event_key, genetic_test_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def genomic_test_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_genomic_test;
    # same row grain as the research table. Join keys: patient_event_key, genetic_test_id.
    return (_lifecycle_source_genomic_test()).select(*GENOMIC_TEST_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / genomic result

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
GENOMIC_RESULT_SOURCE_COLUMNS = [
    "patient_event_key",
    "genetic_result_id",
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
    "genomic_test_key",
    "report_version_key",
    "hgnc_id",
    "reported_gene_symbol",
    "normalized_gene_symbol",
    "partner_hgnc_id",
    "partner_gene_symbol",
    "alteration_type",
    "detection_status",
    "hgvs_c_raw",
    "hgvs_c_parsed",
    "hgvs_p_raw",
    "hgvs_p_parsed",
    "transcript",
    "hgvs_validation_status",
    "genome_build",
    "chromosome",
    "position_start",
    "position_end",
    "vaf_raw",
    "vaf",
    "zygosity",
    "reported_classification",
    "reported_tier",
    "copy_number",
    "ratio_raw",
    "iscn_raw",
    "clinvar_concept_id",
    "omop_genomic_concept_id",
    "snomed_code",
    "evidence_text",
    "evidence_start",
    "evidence_end",
    "parser_profile_id",
    "parser_version",
    "review_status",
    "lifecycle_status",
    "is_current",
    "research_qi_only",
    "specimen_key",
    "accession_identifier",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
GENOMIC_RESULT_PUBLIC_COLUMNS = [
    'patient_event_key',
    'genetic_result_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'genomic_test_key',
    'report_version_key',
    'hgnc_id',
    'reported_gene_symbol',
    'normalized_gene_symbol',
    'partner_hgnc_id',
    'partner_gene_symbol',
    'alteration_type',
    'detection_status',
    'hgvs_c_raw',
    'hgvs_c_parsed',
    'hgvs_p_raw',
    'hgvs_p_parsed',
    'transcript',
    'hgvs_validation_status',
    'genome_build',
    'chromosome',
    'position_start',
    'position_end',
    'vaf_raw',
    'vaf',
    'zygosity',
    'reported_classification',
    'reported_tier',
    'copy_number',
    'ratio_raw',
    'iscn_raw',
    'clinvar_concept_id',
    'omop_genomic_concept_id',
    'snomed_code',
    'evidence_text',
    'evidence_start',
    'evidence_end',
    'parser_profile_id',
    'parser_version',
    'review_status',
    'lifecycle_status',
    'is_current',
    'research_qi_only',
    'specimen_key',
    'accession_identifier',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

GENOMIC_RESULT_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'genetic_result_id',
    'identity_status',
    'load_batch_id',
]

GENOMIC_RESULT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "genetic_result_id": "Pathology genetic_result_id; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace selected for `subject_key`: Millennium person identifier first, then MRN, then NHS number; nosubject denotes the per-source-record fallback when none is present.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT through the shared pathology identity projection.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Report issue time with accession fallback.",
    "event_end_datetime": "Event end time.",
    "source_coding_system": "Gene-symbol or alteration coding namespace.",
    "source_code": "SNV, indel, fusion, CNV, rearrangement, repeat, karyotype, or other.",
    "source_display": "SNV, indel, fusion, CNV, rearrangement, repeat, karyotype, or other.",
    "genomic_test_key": "Deterministic SHA-256 key of the parent genomic test; equals clinical_genomic_test.patient_event_key.",
    "report_version_key": "Deterministic report-version key.",
    "hgnc_id": "Primary HGNC identifier.",
    "reported_gene_symbol": "Gene symbol as reported.",
    "normalized_gene_symbol": "Approved HGNC symbol.",
    "partner_hgnc_id": "Partner HGNC identifier.",
    "partner_gene_symbol": "Fusion/rearrangement partner symbol.",
    "alteration_type": "Reported alteration type.",
    "detection_status": "Finding detection status.",
    "hgvs_c_raw": "Raw coding HGVS.",
    "hgvs_c_parsed": "Parsed coding HGVS.",
    "hgvs_p_raw": "Raw protein HGVS.",
    "hgvs_p_parsed": "Parsed protein HGVS.",
    "transcript": "Reported transcript accession.",
    "hgvs_validation_status": "HGVS validation state.",
    "genome_build": "Explicit genome build.",
    "chromosome": "Explicitly reported chromosome.",
    "position_start": "Genomic start coordinate.",
    "position_end": "Genomic end coordinate.",
    "vaf_raw": "Raw variant allele frequency.",
    "vaf": "Parsed variant allele fraction.",
    "zygosity": "Value describing zygosity for the genomic result record. It is carried from bronze field `zygosity` in `4_prod.bronze.map_pathology_genetic_result`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "reported_classification": "Classification or tier reported by the laboratory.",
    "reported_tier": "Reported clinical/actionability tier.",
    "copy_number": "Reported copy number.",
    "ratio_raw": "Raw molecular ratio.",
    "iscn_raw": "Raw ISCN notation.",
    "clinvar_concept_id": "ClinVar concept identifier.",
    "omop_genomic_concept_id": "OMOP Genomic concept identifier.",
    "snomed_code": "SNOMED finding code.",
    "evidence_text": "Minimal narrative evidence span.",
    "evidence_start": "Evidence start offset.",
    "evidence_end": "Evidence end offset.",
    "parser_profile_id": "Deterministic parser profile identifier.",
    "parser_version": "Parser implementation version.",
    "review_status": "Source finding review status.",
    "lifecycle_status": "Inherited report lifecycle.",
    "is_current": "Whether the source report version is current.",
    "research_qi_only": "Research/QI release flag.",
    "specimen_key": "Deterministic specimen key.",
    "accession_identifier": "Alias-resolved accession identifier.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each genomic result record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each genomic result record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Originating data feed responsible for the record for each genomic result record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_pathology_genetic_result.lifecycle_status is exactly cancelled or entered_in_error; otherwise superseded when is_current is false (null defaults true), otherwise active. Comparisons are case-sensitive without trimming; a null lifecycle_status does not select retraction. Retraction takes priority over supersession; clinical findings and joined identity/link fields are separate.",
    "record_status_effective_from": "Always null as a TIMESTAMP in the current genomic-result projection. No source-status start is supplied here; the clinical event's report/sample/request date fallbacks and ADC_UPDT are not used for this field.",
    "record_status_effective_to": "Always null as a TIMESTAMP in the current genomic-result projection, including when its status or source-current flag changes. No report, clinical-event or ingestion timestamp is substituted as a status end.",
    "source_update_timestamp": "map_pathology_genetic_result.ADC_UPDT carried unchanged from the contributing row, identical to loaded_at. No independent native application-update clock is projected here, and Silver refresh time is not substituted.",
    "loaded_at": "map_pathology_genetic_result.ADC_UPDT carried unchanged through its joins, usable-code filtering and source/public projections. Alias resolution and accession identity add no load clocks; neither joined report/parent timestamps nor the separately aggregated accession evidence clock replace this value. It is identical to source_update_timestamp, not the clinical event time or Silver refresh time.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_genomic_result():
    # Assemble genomic result rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return _genomic_result_canonical().select(*GENOMIC_RESULT_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.genomic_result"),
    comment="One reportable molecular/cytogenetic finding. Schema-first: zero rows in both "
            "catalogs as of 2026-08-18 (empty_by_design — no detected assays upstream).",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=GENOMIC_RESULT_COLUMN_COMMENTS,
)
def genomic_result():
    # Build the declared dataset: One reportable molecular/cytogenetic finding. Schema-first:
    # zero rows in both catalogs as of 2026-08-18 (empty_by_design — no detected assays
    # upstream).
    return _lifecycle_source_genomic_result().select(*GENOMIC_RESULT_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._genomic_result_metadata'),
    comment='Internal quality and batch metadata for clinical_genomic_result; same row grain as the research table. Join keys: patient_event_key, genetic_result_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def genomic_result_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_genomic_result; same row grain as the research table. Join keys:
    # patient_event_key, genetic_result_id.
    return (_lifecycle_source_genomic_result()).select(*GENOMIC_RESULT_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / gene tested

# COMMAND ----------

SRC_PATHOLOGY_GENE_TESTED = "4_prod.bronze.map_pathology_gene_tested"

GENE_TESTED_COLUMN_COMMENTS = {
    "gene_tested_row_key": "Deterministic SHA-256 assay-gene row key.",
    "source_gene_tested_id": "Bronze gene-tested identifier.",
    "genomic_test_key": "Deterministic SHA-256 parent genomic-test key.",
    "source_genetic_test_id": "Bronze parent genetic-test identifier.",
    "hgnc_id": "Governed HGNC identifier.",
    "reported_gene_symbol": "Gene symbol as reported or configured.",
    "normalized_gene_symbol": "Current approved HGNC symbol.",
    "alias_match_type": "HGNC symbol-resolution route.",
    "evidence_type": "Source of assay-gene membership.",
    "test_scope": "Reported exon/region/coverage scope when available.",
    "panel_version_inferred": "Whether membership used an inferred panel version.",
    "confidence": "Deterministic normalization confidence.",
    "loaded_at": "map_pathology_gene_tested.ADC_UPDT carried unchanged for the assay-gene membership row. The parent assay key is derived without joining or adding its timestamp; this is bronze load provenance, not a genomic-test event time or Silver refresh time.",
}

GENE_TESTED_LIFECYCLE_FIELDS = [
]

GENE_TESTED_RETIRED_COLUMNS = [

]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_gene_tested():
    # Assemble gene tested rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    g = read_source(SRC_PATHOLOGY_GENE_TESTED)
    # contract v2: name assay-gene and parent-assay deterministic identities as keys
    return g.select(
        stable_id("gene_tested:assay-gene", g.gene_tested_id).alias("gene_tested_row_key"),
        g.gene_tested_id.alias("source_gene_tested_id"),
        stable_id("genomic_test:assay", g.genetic_test_id).alias("genomic_test_key"),
        g.genetic_test_id.alias("source_genetic_test_id"),
        g.hgnc_id, g.reported_gene_symbol, g.normalized_gene_symbol,
        g.alias_match_type, g.evidence_type, g.test_scope,
        g.panel_version_inferred, g.confidence,
        g.ADC_UPDT.alias("loaded_at"),
    )

@materialized_view(
    name=_n("journey_reference.gene_tested"),
    comment="Assay-gene denominator, HGNC-normalized; reference surface, no event index. "
            "Covers 42.6% of assays in the current build (BCRL/FLT3 complete; "
            "MNGS 10.5%, TNGS 16.6%) — recorded, not gated.",
    refresh_policy="incremental",
    column_comments=GENE_TESTED_COLUMN_COMMENTS,
)
def gene_tested():
    # Build the declared dataset: Assay-gene denominator, HGNC-normalized; reference surface, no
    # event index. Covers 42.6% of assays in the current build (BCRL/FLT3 complete; MNGS 10.5%,
    # TNGS 16.6%) — recorded, not gated.
    return _lifecycle_source_gene_tested().drop(*GENE_TESTED_LIFECYCLE_FIELDS, *GENE_TESTED_RETIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / indication

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
INDICATION_SOURCE_COLUMNS = [
    "patient_event_key",
    "indication_id",
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
    "relation_type",
    "source_field",
    "source_text",
    "evidence_text",
    "evidence_start",
    "evidence_end",
    "snomed_code",
    "snomed_term",
    "omop_concept_id",
    "assertion",
    "temporality",
    "experiencer",
    "rule_id",
    "rule_version",
    "confidence",
    "mapping_status",
    "ig_release_status",
    "is_current",
    "research_qi_only",
    "specimen_key",
    "accession_identifier",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
INDICATION_PUBLIC_COLUMNS = [
    'patient_event_key',
    'indication_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'relation_type',
    'source_field',
    'source_text',
    'evidence_text',
    'evidence_start',
    'evidence_end',
    'snomed_code',
    'snomed_term',
    'omop_concept_id',
    'assertion',
    'temporality',
    'experiencer',
    'rule_id',
    'rule_version',
    'confidence',
    'mapping_status',
    'ig_release_status',
    'is_current',
    'research_qi_only',
    'specimen_key',
    'accession_identifier',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

INDICATION_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'indication_id',
    'identity_status',
    'load_batch_id',
]

INDICATION_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "indication_id": "Pathology indication_id; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace selected for `subject_key`: Millennium person identifier first, then MRN, then NHS number; nosubject denotes the per-source-record fallback when none is present.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT through the shared pathology identity projection.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Accession report/sample/request fallback time.",
    "event_end_datetime": "Event end time.",
    "source_coding_system": "Indication text namespace.",
    "source_code": "Coded diagnosis display string.",
    "source_display": "Coded diagnosis display string.",
    "relation_type": "Evidence relation type.",
    "source_field": "Source evidence field.",
    "source_text": "Unmodified coded diagnosis display string.",
    "evidence_text": "Exact evidence span.",
    "evidence_start": "Evidence start offset.",
    "evidence_end": "Evidence end offset.",
    "snomed_code": "SNOMED condition code.",
    "snomed_term": "SNOMED display term.",
    "omop_concept_id": "Standard OMOP condition concept.",
    "assertion": "present, absent, possible, family_history, or unknown.",
    "temporality": "current, historical, future, or unknown.",
    "experiencer": "patient, family, or unknown.",
    "rule_id": "Deterministic rule identifier.",
    "rule_version": "Deterministic rule version.",
    "confidence": "Confidence value supplied by the pathology indication rule, carried unchanged from bronze `confidence`; Silver does not recalibrate this value.",
    "mapping_status": "Source mapping status.",
    "ig_release_status": "IG release status published verbatim.",
    "is_current": "Whether evidence remains current.",
    "research_qi_only": "Research/QI release flag.",
    "specimen_key": "Deterministic specimen key.",
    "accession_identifier": "Alias-resolved accession identifier.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each indication record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each indication record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Originating data feed responsible for the record for each indication record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Always active in the current map_pathology_indication projection. is_current, assertion, temporality, experiencer and mapping/release status do not change this literal label; usable-code filtering separately controls publication eligibility.",
    "record_status_effective_from": "Always null as a TIMESTAMP in the current indication projection. No source-status start is supplied here; the clinical event's report/sample/request date fallbacks and ADC_UPDT are not used for this field.",
    "record_status_effective_to": "Always null as a TIMESTAMP in the current indication projection, including when its status or source-current flag changes. No report, clinical-event or ingestion timestamp is substituted as a status end.",
    "source_update_timestamp": "map_pathology_indication.ADC_UPDT carried unchanged from the contributing row, identical to loaded_at. No independent native application-update clock is projected here, and Silver refresh time is not substituted.",
    "loaded_at": "map_pathology_indication.ADC_UPDT carried unchanged through its joins, usable-code filtering and source/public projections. Alias resolution and accession identity add no load clocks; neither joined report/parent timestamps nor the separately aggregated accession evidence clock replace this value. It is identical to source_update_timestamp, not the clinical event time or Silver refresh time.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_indication():
    # Assemble indication rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    return _indication_canonical().select(*INDICATION_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.indication"),
    comment="Accession-scoped diagnosis-context evidence (single deterministic lane "
            "diagnosis_context_window_v1). Source/evidence text are coded diagnosis display "
            "strings; ig_release_status published verbatim; prod activation is gated.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=INDICATION_COLUMN_COMMENTS,
)
def indication():
    # Build the declared dataset: Accession-scoped diagnosis-context evidence (single
    # deterministic lane diagnosis_context_window_v1). Source/evidence text are coded diagnosis
    # display strings; ig_release_status published verbatim; prod activation is gated.
    return _lifecycle_source_indication().select(*INDICATION_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._indication_metadata'),
    comment='Internal quality and batch metadata for clinical_indication; same row grain as the research table. Join keys: patient_event_key, indication_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def indication_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_indication;
    # same row grain as the research table. Join keys: patient_event_key, indication_id.
    return (_lifecycle_source_indication()).select(*INDICATION_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / microbiology isolate

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
MICRO_ISOLATE_SOURCE_COLUMNS = [
    "patient_event_key",
    "microbiology_isolate_id",
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
    "pathology_result_id",
    "report_version_key",
    "specimen_type_code",
    "organism_text",
    "organism_snomed_code",
    "organism_omop_concept_id",
    "suspected_ind",
    "growth_grade",
    "organism_code",
    "panel_code",
    "isolate_ordinal",
    "isolate_comment",
    "lims_no",
    "parse_status",
    "parser_version",
    "lifecycle_status",
    "is_current",
    "research_qi_only",
    "specimen_key",
    "accession_identifier",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
MICRO_ISOLATE_PUBLIC_COLUMNS = [
    'patient_event_key',
    'microbiology_isolate_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'pathology_result_id',
    'report_version_key',
    'specimen_type_code',
    'organism_text',
    'organism_snomed_code',
    'organism_omop_concept_id',
    'suspected_ind',
    'growth_grade',
    'organism_code',
    'panel_code',
    'isolate_ordinal',
    'isolate_comment',
    'lims_no',
    'parse_status',
    'parser_version',
    'lifecycle_status',
    'is_current',
    'research_qi_only',
    'specimen_key',
    'accession_identifier',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

MICRO_ISOLATE_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'microbiology_isolate_id',
    'identity_status',
    'load_batch_id',
]

MICROBIOLOGY_ISOLATE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "microbiology_isolate_id": "Pathology microbiology_isolate_id; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace selected for `subject_key`: Millennium person identifier first, then MRN, then NHS number; nosubject denotes the per-source-record fallback when none is present.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT through the shared pathology identity projection.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Accession sample/report/request fallback time.",
    "event_end_datetime": "Event end time.",
    "source_coding_system": "Organism coding namespace.",
    "source_code": "Organism text or code.",
    "source_display": "Approved SNOMED organism code.",
    "pathology_result_id": "Linked pathology-result fact.",
    "report_version_key": "Deterministic report-version key.",
    "specimen_type_code": "Source specimen type.",
    "organism_text": "Organism as reported.",
    "organism_snomed_code": "SNOMED organism code.",
    "organism_omop_concept_id": "Standard OMOP organism concept.",
    "suspected_ind": "Whether the organism is hedged or suspected.",
    "growth_grade": "Reported growth grade.",
    "organism_code": "WinPath organism code exactly as packed in the antibiogram (e.g. ESCOL).",
    "panel_code": "WinPath susceptibility panel code following the organism (e.g. GU5, 3P); null when blank.",
    "isolate_ordinal": "Zero-based position of this isolate block within the source result value.",
    "isolate_comment": "Text of the [~ annotation lines attached to this isolate, joined with single spaces; null when none.",
    "lims_no": "TFC/LIMS instance number the source row came from; dictionaries are keyed per instance.",
    "parse_status": "ok, or unterminated_block when this isolate block had no terminator before the next opener; row-level orphan_continuation is recorded in validation.",
    "parser_version": "pathology_antibiogram parser version that produced the row.",
    "lifecycle_status": "Inherited source lifecycle.",
    "is_current": "Whether the isolate remains current.",
    "research_qi_only": "Research/QI release flag.",
    "specimen_key": "Deterministic specimen key.",
    "accession_identifier": "Alias-resolved accession identifier.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each microbiology isolate record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each microbiology isolate record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Originating data feed responsible for the record for each microbiology isolate record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_pathology_microbiology_isolate.lifecycle_status is exactly cancelled or entered_in_error; otherwise superseded when is_current is false (null defaults true), otherwise active. Comparisons are case-sensitive without trimming; a null lifecycle_status does not select retraction. Retraction takes priority over supersession; clinical findings and joined identity/link fields are separate.",
    "record_status_effective_from": "Always null as a TIMESTAMP in the current microbiology-isolate projection. No source-status start is supplied here; the clinical event's report/sample/request date fallbacks and ADC_UPDT are not used for this field.",
    "record_status_effective_to": "Always null as a TIMESTAMP in the current microbiology-isolate projection, including when its status or source-current flag changes. No report, clinical-event or ingestion timestamp is substituted as a status end.",
    "source_update_timestamp": "map_pathology_microbiology_isolate.ADC_UPDT carried unchanged from the contributing row, identical to loaded_at. No independent native application-update clock is projected here, and Silver refresh time is not substituted.",
    "loaded_at": "map_pathology_microbiology_isolate.ADC_UPDT carried unchanged through its joins, usable-code filtering and source/public projections. Alias resolution and accession identity add no load clocks; neither joined report/parent timestamps nor the separately aggregated accession evidence clock replace this value. It is identical to source_update_timestamp, not the clinical event time or Silver refresh time.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_microbiology_isolate():
    # Assemble microbiology isolate rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _micro_isolate_canonical().select(*MICRO_ISOLATE_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.microbiology_isolate"),
    comment="One organism/isolate finding per accession, parsed from the packed WinPath antibiogram in map_pathology (S2.5, parser 1.0.0). Person linkage follows the accession identity; unresolved rows stay here and drop at gold.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=MICROBIOLOGY_ISOLATE_COLUMN_COMMENTS,
)
def microbiology_isolate():
    # Build the declared dataset: One organism/isolate finding per accession, parsed from the
    # packed WinPath antibiogram in map_pathology (S2.5, parser 1.0.0). Person linkage follows
    # the accession identity; unresolved rows stay here and drop at gold.
    return _lifecycle_source_microbiology_isolate().select(*MICRO_ISOLATE_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._microbiology_isolate_metadata'),
    comment='Internal quality and batch metadata for clinical_microbiology_isolate; same row grain as the research table. Join keys: patient_event_key, microbiology_isolate_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def microbiology_isolate_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_microbiology_isolate; same row grain as the research table. Join keys:
    # patient_event_key, microbiology_isolate_id.
    return (_lifecycle_source_microbiology_isolate()).select(*MICRO_ISOLATE_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / susceptibility result

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
SUSCEPTIBILITY_SOURCE_COLUMNS = [
    "patient_event_key",
    "susceptibility_result_id",
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
    "microbiology_isolate_id",
    "isolate_event_key",
    "pathology_result_id",
    "link_status",
    "antimicrobial_text",
    "antimicrobial_code",
    "antimicrobial_omop_concept_id",
    "interpretation_raw",
    "interpretation",
    "mic_raw",
    "mic",
    "unit_source_value",
    "method",
    "token_class",
    "token_ordinal",
    "parser_version",
    "lifecycle_status",
    "is_current",
    "research_qi_only",
    "specimen_key",
    "accession_identifier",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
SUSCEPTIBILITY_PUBLIC_COLUMNS = [
    'patient_event_key',
    'susceptibility_result_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'microbiology_isolate_id',
    'isolate_event_key',
    'pathology_result_id',
    'link_status',
    'antimicrobial_text',
    'antimicrobial_code',
    'antimicrobial_omop_concept_id',
    'interpretation_raw',
    'interpretation',
    'mic_raw',
    'mic',
    'unit_source_value',
    'method',
    'token_class',
    'token_ordinal',
    'parser_version',
    'lifecycle_status',
    'is_current',
    'research_qi_only',
    'specimen_key',
    'accession_identifier',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

SUSCEPTIBILITY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'susceptibility_result_id',
    'identity_status',
    'load_batch_id',
]

SUSCEPTIBILITY_RESULT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "susceptibility_result_id": "Pathology susceptibility_result_id; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace selected for `subject_key`: Millennium person identifier first, then MRN, then NHS number; nosubject denotes the per-source-record fallback when none is present.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT through the shared pathology identity projection.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Accession sample/report/request fallback time.",
    "event_end_datetime": "Event end time.",
    "source_coding_system": "Antimicrobial coding namespace.",
    "source_code": "Antimicrobial code or text.",
    "source_display": "Source antimicrobial result/test code.",
    "microbiology_isolate_id": "Native bronze microbiology isolate identifier; foreign key to clinical_microbiology_isolate.microbiology_isolate_id.",
    "isolate_event_key": "Deterministic event key for the linked microbiology isolate.",
    "pathology_result_id": "Linked pathology-result fact.",
    "link_status": "Isolate-link resolution state.",
    "antimicrobial_text": "Antimicrobial as reported.",
    "antimicrobial_code": "Source antimicrobial code.",
    "antimicrobial_omop_concept_id": "Standard OMOP antimicrobial concept.",
    "interpretation_raw": "Raw susceptibility result.",
    "interpretation": "S, I, R or indeterminate for antimicrobials; positive, negative or indeterminate for mechanism markers. Case of the source flag is preserved in interpretation_raw and is not interpreted.",
    "mic_raw": "Raw MIC text.",
    "mic": "Parsed MIC value.",
    "unit_source_value": "Raw MIC unit.",
    "method": "Reported susceptibility method.",
    "token_class": "antimicrobial, mechanism or unparsed token class assigned by pathology_antibiogram.",
    "token_ordinal": "Zero-based position of this token within its isolate block.",
    "parser_version": "pathology_antibiogram parser version that produced the row.",
    "lifecycle_status": "Inherited source lifecycle.",
    "is_current": "Whether the result remains current.",
    "research_qi_only": "Research/QI release flag.",
    "specimen_key": "Deterministic specimen key.",
    "accession_identifier": "Alias-resolved accession identifier.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each susceptibility result record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each susceptibility result record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Originating data feed responsible for the record for each susceptibility result record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_pathology_antimicrobial_susceptibility.lifecycle_status is exactly cancelled or entered_in_error; otherwise superseded when is_current is false (null defaults true), otherwise active. Comparisons are case-sensitive without trimming; a null lifecycle_status does not select retraction. Retraction takes priority over supersession; clinical findings and joined identity/link fields are separate.",
    "record_status_effective_from": "Always null as a TIMESTAMP in the current susceptibility-result projection. No source-status start is supplied here; the clinical event's report/sample/request date fallbacks and ADC_UPDT are not used for this field.",
    "record_status_effective_to": "Always null as a TIMESTAMP in the current susceptibility-result projection, including when its status or source-current flag changes. No report, clinical-event or ingestion timestamp is substituted as a status end.",
    "source_update_timestamp": "map_pathology_antimicrobial_susceptibility.ADC_UPDT carried unchanged from the contributing row, identical to loaded_at. No independent native application-update clock is projected here, and Silver refresh time is not substituted.",
    "loaded_at": "map_pathology_antimicrobial_susceptibility.ADC_UPDT carried unchanged through its joins, usable-code filtering and source/public projections. Alias resolution and accession identity add no load clocks; neither joined report/parent timestamps nor the separately aggregated accession evidence clock replace this value. It is identical to source_update_timestamp, not the clinical event time or Silver refresh time.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_susceptibility_result():
    # Assemble susceptibility result rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _susceptibility_canonical().select(*SUSCEPTIBILITY_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.susceptibility_result"),
    comment="One antimicrobial, mechanism-marker or unparsed token per isolate, parsed from the packed WinPath antibiogram (S2.5, parser 1.0.0). Categorical S/I/R only; MIC columns are null by source.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=SUSCEPTIBILITY_RESULT_COLUMN_COMMENTS,
)
def susceptibility_result():
    # Build the declared dataset: One antimicrobial, mechanism-marker or unparsed token per
    # isolate, parsed from the packed WinPath antibiogram (S2.5, parser 1.0.0). Categorical
    # S/I/R only; MIC columns are null by source.
    return _lifecycle_source_susceptibility_result().select(*SUSCEPTIBILITY_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._susceptibility_result_metadata'),
    comment='Internal quality and batch metadata for clinical_susceptibility_result; same row grain as the research table. Join keys: patient_event_key, susceptibility_result_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def susceptibility_result_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_susceptibility_result; same row grain as the research table. Join keys:
    # patient_event_key, susceptibility_result_id.
    return (_lifecycle_source_susceptibility_result()).select(*SUSCEPTIBILITY_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spine / request thread

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
REQUEST_THREAD_SOURCE_COLUMNS = [
    "request_thread_key",
    "source_object",
    "source_patient_event_key",
    "target_patient_event_key",
    "link_type_code",
    "subject_key",
    "person_id",
    "encounter_id",
    "request_key",
    "response_key",
    "requested_datetime",
    "responded_datetime",
    "source_history_row_count",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "source_system",
    "source_table",
    "source_row_id",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
REQUEST_THREAD_PUBLIC_COLUMNS = [
    'request_thread_key',
    'source_object',
    'source_patient_event_key',
    'target_patient_event_key',
    'link_type_code',
    'subject_key',
    'person_id',
    'encounter_id',
    'request_key',
    'response_key',
    'requested_datetime',
    'responded_datetime',
    'source_history_row_count',
    'source_system',
    'source_table',
    'source_row_id',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

REQUEST_THREAD_LIFECYCLE_COLUMNS = [
    'request_thread_key',
    'load_batch_id',
]

def _pathology_request_thread_canonical():
    # contract v2: publish all request-thread arms with key names, source_object, and native person/encounter types
    # Assemble normalized pathology request thread rows for downstream dataset builders,
    # preserving the existing source and identity rules.
    t = _pathology_requested_test_canonical().select(
        F.col("patient_event_key").alias("source_patient_event_key"),
        F.col("_source_row_id").alias("request_key"),
        "subject_key", "person_id", "specimen_key",
        F.col("event_datetime").alias("requested_datetime"),
        "load_batch_id", "loaded_at",
    )
    r = _pathology_report_series_canonical().select(
        F.col("patient_event_key").alias("target_patient_event_key"),
        F.col("_source_row_id").alias("response_key"),
        F.col("specimen_key").alias("_r_specimen_key"),
        F.col("event_datetime").alias("responded_datetime"),
        F.col("loaded_at").alias("_r_loaded_at"),
    )
    j = t.join(r, t["specimen_key"] == r["_r_specimen_key"], "inner")
    return j.select(
        stable_id(
            "request_thread:pathology", F.col("source_patient_event_key"),
            F.col("target_patient_event_key"), F.lit("order_to_report"),
        ).alias("request_thread_key"),
        "source_patient_event_key", "target_patient_event_key",
        F.lit("order_to_report").alias("link_type_code"),
        F.lit("pathology").alias("source_object"),
        "subject_key", "person_id", F.lit(None).cast("bigint").alias("encounter_id"),
        "request_key", "response_key", "requested_datetime",
        "responded_datetime", F.lit(1).cast("long").alias("source_history_row_count"),
        F.lit("active").alias("record_status"),
        F.lit(None).cast("timestamp").alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit("laboratory").alias("source_system"),
        F.lit(SRC_PATHOLOGY_REQUESTED_TEST).alias("source_table"),
        F.concat_ws("|", "request_key", "response_key").alias("source_row_id"),
        "load_batch_id", F.lit(None).cast("timestamp").alias("source_update_timestamp"),
        F.greatest("loaded_at", "_r_loaded_at").alias("loaded_at"),
    )

REQUEST_THREAD_COLUMN_COMMENTS = {
    "request_thread_key": "Deterministic request-edge primary key.",
    "source_object": "Request-thread arm: pathology, medication, or waiting_list.",
    "source_patient_event_key": "Request-side patient event identifier.",
    "target_patient_event_key": "Response-side patient event identifier.",
    "link_type_code": "Controlled directed relationship type.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT when available.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "request_key": "Request-side source identifier retained under the governed key name.",
    "response_key": "Response-side source identifier retained under the governed key name.",
    "requested_datetime": "Source request timestamp.",
    "responded_datetime": "Source report or measurement timestamp.",
    "source_history_row_count": "Number of source rows supporting the edge.",
    "source_system": "Source system identifier.",
    "source_table": "Fully qualified configured source table.",
    "source_row_id": "Representative stable source row identifier.",
    "request_identifier": "Verbatim source order identifier.",
    "response_identifier": "Verbatim source report-parent identifier.",
    "record_status": "Arm-specific edge status: pathology is literal active. Medication is retracted when the order SOURCE_PRESENT_IND is false (null defaults to present), otherwise superseded when administration CE_VALID_UNTIL_DT_TM is before 2100-01-01, otherwise active. Waiting-list edges copy the selected representative version's status: superseded for source absence or non-current status (null presence defaults true; null IS_CURRENT defaults false), otherwise active. This is not a single source workflow-status code.",
    "record_status_effective_from": "Arm-specific history start: null for pathology edges; map_medication_order.ORIG_ORDER_DT_TM for medication edges; selected map_waiting_list representative BEG_EFFECTIVE_DT_TM for waiting-list edges. These are request/version-time proxies, not a uniformly observed status transition. Missing timestamps remain null.",
    "record_status_effective_to": "Pathology edges always have a null end. Medication uses order SOURCE_ABSENT_DETECTED_TS when retracted, without fallback even if null; otherwise finite administration CE_VALID_UNTIL_DT_TM when superseded, else null. Waiting-list edges copy their representative's end: for superseded versions, coalesce(SOURCE_ABSENT_DETECTED_TS, END_EFFECTIVE_DT_TM, ADC_UPDT), with no far-future sentinel filter; active versions have a null end.",
    "source_update_timestamp": "Arm-specific provenance: null for pathology edges; greatest of map_med_admin.ADC_UPDT, map_medication_order.SOURCE_ADC_UPDT and map_medication_order.ADC_UPDT for medication edges; selected map_waiting_list representative SOURCE_ADC_UPDT for waiting-list edges. This is not uniformly a native source modification clock. The appointment existence/person join contributes no timestamp; no Silver refresh timestamp is substituted.",
    "loaded_at": "Arm-specific contributing load clock: pathology takes the greatest requested-test ADC_UPDT and the linked report-series maximum ADC_UPDT across all its versions; medication takes the greatest administration ADC_UPDT and order ADC_UPDT; waiting-list takes ADC_UPDT of the selected whole representative version. The appointment join adds no clock, and the union does not aggregate across arms. Null when that edge has no contributing load timestamp; not Silver refresh time.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_request_thread():
    # Assemble request thread rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    pathology = _pathology_request_thread_canonical().select(*REQUEST_THREAD_SOURCE_COLUMNS)
    medication = _medication_request_thread_canonical().select(*REQUEST_THREAD_SOURCE_COLUMNS)
    waiting_list = _waiting_list_appointment_thread_canonical().select(*REQUEST_THREAD_SOURCE_COLUMNS)
    return pathology.unionByName(medication).unionByName(waiting_list)

@materialized_view(
    name=_n("journey_spine.request_thread"),
    comment="Directed request-to-response edges: pathology order-to-report, medication order-to-administration, waiting-list entry-to-appointment.",
    refresh_policy="incremental",
    column_comments=REQUEST_THREAD_COLUMN_COMMENTS,
)
def request_thread():
    # Build the declared dataset: Directed request-to-response edges: pathology order-to-report,
    # medication order-to-administration, waiting-list entry-to-appointment.
    return _lifecycle_source_request_thread().select(*REQUEST_THREAD_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_spine._request_thread_metadata'),
    comment='Internal quality and batch metadata for spine_request_thread; same row grain as the research table. Join keys: request_thread_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def request_thread_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for spine_request_thread;
    # same row grain as the research table. Join keys: request_thread_key.
    return (_lifecycle_source_request_thread()).select(*REQUEST_THREAD_LIFECYCLE_COLUMNS)

def _medication_request_thread_canonical():
    # Assemble normalized medication request thread rows for downstream dataset builders,
    # preserving the existing source and identity rules.
    a = read_source(SRC_MED_ADMIN).alias("a")
    o = read_source(SRC_MEDICATION_ORDER).alias("o")
    joined = a.where(F.col("a.ORDER_ID").isNotNull()).join(
        o, F.col("a.ORDER_ID") == F.col("o.ORDER_ID"), "inner"
    )
    source_event_id = stable_id("medication_order:mill", F.col("o.ORDER_ID"))
    target_event_id = stable_id("medication_admin:mill", F.col("a.EVENT_ID"))
    skey, _ = subject_key_with_system(
        [("urn:cerner:person_id", F.coalesce(F.col("a.PERSON_ID"), F.col("o.PERSON_ID")))],
        SRC_MED_ADMIN, F.col("a.EVENT_ID"),
    )
    retracted = F.coalesce(F.col("o.SOURCE_PRESENT_IND"), F.lit(True)) == F.lit(False)
    ended = F.col("a.CE_VALID_UNTIL_DT_TM").isNotNull() & (
        F.col("a.CE_VALID_UNTIL_DT_TM") < F.lit("2100-01-01").cast("timestamp")
    )
    loaded_at = F.greatest(F.col("a.ADC_UPDT"), F.col("o.ADC_UPDT"))
    source_update = F.greatest(F.col("a.ADC_UPDT"), F.col("o.SOURCE_ADC_UPDT"),
                               F.col("o.ADC_UPDT"))
    return joined.select(
        stable_id("request_thread:medication", source_event_id, target_event_id,
                  F.lit("order_to_administration")).alias("request_thread_key"),
        source_event_id.alias("source_patient_event_key"),
        target_event_id.alias("target_patient_event_key"),
        F.lit("order_to_administration").alias("link_type_code"),
        F.lit("medication").alias("source_object"), skey.alias("subject_key"),
        F.coalesce(F.col("a.PERSON_ID"), F.col("o.PERSON_ID")).cast("bigint").alias("person_id"),
        F.coalesce(F.col("a.ENCNTR_ID"), F.col("o.ENCNTR_ID")).cast("bigint").alias("encounter_id"),
        F.col("o.ORDER_ID").cast("string").alias("request_key"),
        F.col("a.EVENT_ID").cast("string").alias("response_key"),
        F.col("o.ORIG_ORDER_DT_TM").alias("requested_datetime"),
        F.coalesce(F.col("a.ADMIN_START_DT_TM"), F.col("a.PERFORMED_DT_TM"))
         .alias("responded_datetime"),
        F.lit(2).cast("long").alias("source_history_row_count"),
        F.when(retracted, F.lit("retracted")).when(ended, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        F.col("o.ORIG_ORDER_DT_TM").alias("record_status_effective_from"),
        F.when(retracted, F.col("o.SOURCE_ABSENT_DETECTED_TS"))
         .when(ended, F.col("a.CE_VALID_UNTIL_DT_TM")).alias("record_status_effective_to"),
        F.lit("millennium").alias("source_system"),
        F.concat_ws("|", F.lit(SRC_MEDICATION_ORDER), F.lit(SRC_MED_ADMIN)).alias("source_table"),
        F.col("a.EVENT_ID").cast("string").alias("source_row_id"),
        F.date_format(loaded_at, "yyyyMMddHHmmss").alias("load_batch_id"),
        source_update.alias("source_update_timestamp"),
        loaded_at.alias("loaded_at"),
    )

def _waiting_list_appointment_thread_canonical():
    # One edge per entry: representative (CURRENT wins) with a non-zero SCH_EVENT_ID,
    # existence-gated against the appointment source so no dangling target id is minted,
    # AND person-gated so an edge never links two different patients (the edge copies the
    # waiting-list subject; an id-only join would silently cross patients).
    # Full-volume 2026-08-12: 14,312,122/14,333,454 distinct ids resolve (99.85%);
    # of 14,324,720 matched pairs 14,271,836 are person-concordant (99.63%),
    # 52,841 appointment-person NULL and 43 discordant are excluded here.
    # Assemble normalized waiting list appointment thread rows for downstream dataset builders,
    # preserving the existing source and identity rules.
    rep = _waiting_list_index_representative().where(
        F.col("sch_event_id").isNotNull() & (F.col("sch_event_id") != 0)
    ).alias("rep")
    appt = (
        read_source(SRC_APPOINTMENT)
        .where(F.col("SCH_EVENT_ID").isNotNull() & F.col("PERSON_ID").isNotNull())
        .select(
            F.col("SCH_EVENT_ID").cast("long").alias("_appt_sch_event_id"),
            F.col("PERSON_ID").cast("bigint").alias("_appt_person_id"),
        )
        .distinct()
        .alias("appt")
    )
    joined = rep.join(
        appt,
        (F.col("rep.sch_event_id") == F.col("_appt_sch_event_id"))
        & (F.col("rep.person_id") == F.col("_appt_person_id")),
        "inner",
    )
    target_id = stable_id("appointment:mill_scheduling", F.col("_appt_sch_event_id"))
    return joined.select(
        stable_id(
            "request_thread:waiting_list", F.col("rep.patient_event_key"), target_id,
            F.lit("waiting_list_to_appointment"),
        ).alias("request_thread_key"),
        F.col("rep.patient_event_key").alias("source_patient_event_key"),
        target_id.alias("target_patient_event_key"),
        F.lit("waiting_list_to_appointment").alias("link_type_code"),
        F.lit("waiting_list").alias("source_object"),
        F.col("rep.subject_key").alias("subject_key"),
        F.col("rep.person_id").alias("person_id"),
        F.col("rep.encounter_id").alias("encounter_id"),
        F.col("rep.pm_wait_list_id").cast("string").alias("request_key"),
        F.col("rep.sch_event_id").cast("string").alias("response_key"),
        F.col("rep.event_datetime").alias("requested_datetime"),
        F.col("rep.scheduled_datetime").alias("responded_datetime"),
        F.lit(1).cast("long").alias("source_history_row_count"),
        F.col("rep.record_status").alias("record_status"),
        F.col("rep.record_status_effective_from").alias("record_status_effective_from"),
        F.col("rep.record_status_effective_to").alias("record_status_effective_to"),
        F.lit("millennium-pm").alias("source_system"),
        F.lit(SRC_WAITING_LIST).alias("source_table"),
        F.concat_ws(
            ":", F.col("rep.pm_wait_list_id").cast("string"), F.col("rep.row_source"),
            F.col("rep.source_version_id").cast("string"),
        ).alias("source_row_id"),
        F.col("rep.load_batch_id").alias("load_batch_id"),
        F.col("rep.source_update_timestamp").alias("source_update_timestamp"),
        F.col("rep.loaded_at").alias("loaded_at"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  form grouped

# COMMAND ----------

SRC_FORM_ITEM = "4_prod.bronze.map_powerform_assessment_item"

SRC_FORM_ASSESSMENT = "4_prod.bronze.map_powerform_assessment"

SRC_FORM_VTE = "4_prod.bronze.map_mat_vte_assessment"

def _form_grouped_query():
    # Join form activity, item and assessment records and group their responses into the
    # internal form payload.
    a = read_source(SRC_FORM_ACTIVITY).alias("a")
    i = read_source(SRC_FORM_ITEM).alias("i")
    g = read_source(SRC_FORM_ASSESSMENT).alias("g")
    v = read_source(SRC_FORM_VTE).select(
        "DOC_RESPONSE_KEY", "Pregnancy_ID", "PregnancyMatchMethod", "Source_ADC_UPDT"
    ).alias("v")
    joined = (
        a.join(i, a.DOC_RESPONSE_KEY == i.DOC_RESPONSE_KEY, "left")
         .join(g, a.DCP_FORMS_ACTIVITY_ID == g.DCP_FORMS_ACTIVITY_ID, "left")
         .join(v, a.DOC_RESPONSE_KEY == v.DOC_RESPONSE_KEY, "left")
    )
    response = F.struct(
        F.coalesce(a.RESPONSE_SEQUENCE_NBR, F.lit(0)).cast("long").alias("sequence"),
        a.DOC_RESPONSE_KEY.alias("response_id"),
        a.SECTION_DESC_TXT.alias("section"),
        a.SECTION_REF_ID.cast("string").alias("section_id"),
        a.DCP_INPUT_REF_ID.cast("string").alias("element_id"),
        a.ELEMENT_LABEL_TXT.alias("element_label"),
        a.GRID_NAME_TXT.alias("grid_name"),
        a.GRID_ROW_DESC_TXT.alias("grid_row"),
        i.RESPONSE_KIND.alias("response_kind"),
        F.coalesce(i.CANONICAL_TEXT_RESULT, i.RESPONSE_TEXT_TXT,
                   a.RESPONSE_VALUE_TXT, a.STRING_RESPONSE_TXT).alias("response_value_text"),
        F.coalesce(i.CANONICAL_NUMERIC_RESULT, a.NUMERIC_RESPONSE_NBR)
         .cast("decimal(38,10)").alias("response_value_number"),
        F.coalesce(i.CANONICAL_DATE_RESULT, i.RESPONSE_DT_TM).alias("response_value_datetime"),
        F.when(i.RESPONSE_NOMENCLATURE_ID.isNotNull(), F.lit("urn:cerner:nomenclature"))
         .when(i.RESPONSE_CODE_VALUE_ID.isNotNull(), F.lit("urn:cerner:code_value"))
         .alias("response_coding_system"),
        F.coalesce(i.RESPONSE_NOMENCLATURE_ID, i.RESPONSE_CODE_VALUE_ID)
         .cast("string").alias("response_coding_code"),
        i.RESPONSE_TEXT_TXT.alias("response_coding_display"),
        i.QUESTION_CONCEPT_ID.cast("string").alias("question_concept_id"),
        i.VALUE_CONCEPT_ID.cast("string").alias("value_concept_id"),
        i.UNIT_CONCEPT_ID.cast("string").alias("unit_concept_id"),
        v.Pregnancy_ID.cast("string").alias("pregnancy_id"),
        v.PregnancyMatchMethod.alias("pregnancy_match_method"),
        (a.ACTIVE_IND == 1).alias("active_ind"),
        i.SOURCE_PRESENT_IND.alias("source_present_ind"),
        i.CANONICAL_SOURCE_DELETED_IND.alias("source_deleted_ind"),
        i.CANONICAL_MATCH_STATUS.alias("canonical_match_status"),
    )
    empty_response = (
        a.RESPONSE_VALUE_TXT.isNull() & a.STRING_RESPONSE_TXT.isNull()
        & a.NUMERIC_RESPONSE_NBR.isNull() & a.RESPONSE_NOMENCLATURE_ID.isNull()
        & a.RESPONSE_CODE_VALUE_ID.isNull()
    )
    grouped = joined.groupBy(a.DCP_FORMS_ACTIVITY_ID).agg(
        F.max(a.PERSON_ID_LONG).alias("PERSON_ID"),
        F.max(a.ENCNTR_ID_LONG).alias("ENCNTR_ID"),
        F.max(a.ORGANIZATION_ID).alias("ORGANIZATION_ID"),
        F.max(a.FORM_REF_ID).alias("FORM_REF_ID"),
        F.max(a.FORM_STATUS_CD).alias("FORM_STATUS_CD"),
        F.max(a.STATUS).alias("STATUS"),
        F.max(a.FORM_DESC_TXT).alias("FORM_DESC_TXT"),
        F.min(a.DOCUMENTATION_DT_TM).alias("authored_datetime"),
        F.max(F.coalesce(a.LAST_DOCUMENTED_DT_TM, a.PERFORMED_DT_TM))
         .alias("completed_datetime"),
        F.max(a.PERFORMED_PRSNL_ID_LONG).alias("PERFORMED_PRSNL_ID"),
        F.parse_json(F.to_json(F.sort_array(F.collect_list(response)))).alias("responses"),
        F.count(F.lit(1)).cast("long").alias("response_row_count"),
        F.sum(F.when(a.ACTIVE_IND == 1, F.lit(1)).otherwise(F.lit(0)))
         .cast("long").alias("active_response_row_count"),
        F.sum(F.when(empty_response, F.lit(1)).otherwise(F.lit(0)))
         .cast("long").alias("empty_response_row_count"),
        F.max(g.INVALID_ROW_COUNT).cast("long").alias("invalid_response_row_count"),
        F.max(g.CANONICAL_MATCHED_ROW_COUNT).cast("long").alias("matched_response_row_count"),
        F.max(g.CANONICAL_UNMATCHED_ROW_COUNT).cast("long").alias("unmatched_response_row_count"),
        F.max(g.CONTEXT_CONFLICT_IND.cast("int")).alias("context_conflict_int"),
        F.max(g.CONTEXT_QUARANTINED_IND.cast("int")).alias("context_quarantined_int"),
        F.max(g.ASSESSMENT_ACTIVE_IND.cast("int")).alias("assessment_active_int"),
        F.max(g.SOURCE_PRESENT_IND.cast("int")).alias("source_present_int"),
        F.max(F.greatest(a.ADC_UPDT, a.SOURCE_ACTIVITY_ADC_UPDT,
                         a.SOURCE_EVENT_ADC_UPDT, a.SOURCE_LABEL_ADC_UPDT))
         .alias("source_update_timestamp"),
        F.max(a.ADC_UPDT).alias("activity_loaded_at"),
        F.max(i.ADC_UPDT).alias("item_loaded_at"),
        F.max(g.ADC_UPDT).alias("assessment_loaded_at"),
        F.max(v.Source_ADC_UPDT).alias("vte_loaded_at"),
    )
    return grouped

@materialized_view(
    name=_n("journey_clinical._form_grouped"),
    private=True,
    comment="Internal top-level aggregate for incrementally maintained ordered PowerForm responses.",
    refresh_policy="incremental",
)
def _form_grouped():
    # Build the declared dataset: Internal top-level aggregate for incrementally maintained
    # ordered PowerForm responses.
    return _form_grouped_query()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc form

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_form"),
    comment="Private JSON bridge and Gold cross-rule flags for form.",
    refresh_policy="incremental",
)
def _qc_form():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for form.
    return _cross_qc_primitive(
        _form_canonical(),
        "form",
        {"responses": "_qc_responses_json"},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / form

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
FORM_PUBLIC_COLUMNS = [
    'patient_event_key',
    'dcp_forms_activity_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'form_type_code',
    'form_type_display',
    'form_status_code',
    'form_status_display',
    'authored_datetime',
    'completed_datetime',
    'performed_practitioner_id',
    'organization_id',
    # contract v2: the ordered payload is decomposed into clinical_form_response,
    # one row per response, so the parent no longer publishes it. The reconciliation
    # counters below stay: they are how a reader checks the child is complete.
    'response_row_count',
    'active_response_row_count',
    'empty_response_row_count',
    'invalid_response_row_count',
    'matched_response_row_count',
    'unmatched_response_row_count',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

FORM_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'dcp_forms_activity_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

FORM_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "dcp_forms_activity_id": "Millennium DCP_FORMS_ACTIVITY_ID; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "event_datetime": "First documented timestamp.",
    "event_end_datetime": "Last documented or performed timestamp.",
    "source_coding_system": "Source form coding system.",
    "source_code": "Source form reference identifier.",
    "source_display": "Source form description.",
    "form_type_code": "Source form type code.",
    "form_type_display": "Source form type display.",
    "form_status_code": "Source form status code.",
    "form_status_display": "Source form status display.",
    "authored_datetime": "Earliest form documentation timestamp.",
    "completed_datetime": "Latest documentation or performed timestamp.",
    "performed_practitioner_id": "Millennium personnel PERSON_ID for the performer.",
    "organization_id": "Millennium ORGANIZATION_ID for the source organization.",
    "response_row_count": "Number of response rows grouped into the form.",
    "active_response_row_count": "Number of active response rows.",
    "empty_response_row_count": "Number of retained empty response rows.",
    "invalid_response_row_count": "Number of source-classified invalid response rows.",
    "matched_response_row_count": "Number of canonical matched response rows.",
    "unmatched_response_row_count": "Number of canonical unmatched response rows.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Registered owning feed.",
    "record_status": "Derived PowerForm row status: retracted when the grouped maximum of map_powerform_assessment.SOURCE_PRESENT_IND is 0; otherwise superseded when the grouped maximum of ASSESSMENT_ACTIVE_IND is 0 or no mill_form_activity response has ACTIVE_IND = 1; otherwise active. Missing assessment flags alone do not trigger retraction or supersession. This is a status label, not the raw Boolean assessment flag.",
    "record_status_effective_from": "Earliest mill_form_activity.DOCUMENTATION_DT_TM for the form instance, carried from authored_datetime. This is the form documentation start used by the research status projection, not an independently recorded status-change timestamp; null when all contributing documentation timestamps are null.",
    "record_status_effective_to": "For a retracted or superseded form, completed_datetime: the maximum of coalesce(mill_form_activity.LAST_DOCUMENTED_DT_TM, mill_form_activity.PERFORMED_DT_TM) across its responses. Null for active forms or when no contributing completion timestamp is available. This is a documentation/completion boundary, not a Boolean assessment flag or an independently recorded status-change time.",
    "source_update_timestamp": "Maximum, across the form's mill_form_activity rows, of the greatest ADC_UPDT, SOURCE_ACTIVITY_ADC_UPDT, SOURCE_EVENT_ADC_UPDT and SOURCE_LABEL_ADC_UPDT. This combines the contributing activity, event, label and bronze-ingestion clocks; it is not uniformly a native clinical-system update time. Null only when every contributing clock is null.",
    "loaded_at": "Greatest of the form-level maximum ADC_UPDT values from mill_form_activity, map_powerform_assessment_item and map_powerform_assessment, and maximum Source_ADC_UPDT from map_mat_vte_assessment. This is the contributing-source ingestion/provenance clock, not the Silver refresh time or form event time; null only when every contributing clock is null.",
}

@materialized_view(
    name=_n("journey_clinical.form"),
    comment="One PowerForm instance with ordered lossless responses and reconciliation evidence.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=FORM_COLUMN_COMMENTS,
)
def form():
    # Build the declared dataset: One PowerForm instance with ordered lossless responses and
    # reconciliation evidence.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_form")),
        "form",
        {},
        FORM_PUBLIC_COLUMNS,
        FORM_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._form_metadata'),
    comment='Internal quality and batch metadata for clinical_form; same row grain as the research table. Join keys: patient_event_key, dcp_forms_activity_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def form_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_form; same
    # row grain as the research table. Join keys: patient_event_key, dcp_forms_activity_id.
    return (spark.read.table(_n("journey_clinical._qc_form"))).select(*FORM_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  registry observations

# COMMAND ----------

# S3b Task 15: unpivot governed registry fields once, preserving the registry record,
# field identity and the registry's own event time. Public registry products remain wide.
S3B_REGISTRY_PRODUCTS = (
    "clinical_registry_acs_transfer", "clinical_registry_cardiac_mdt",
    "clinical_registry_coronary_lesion", "clinical_registry_coronary_procedure",
    "clinical_registry_eracs_episode", "clinical_registry_mortality_review",
    "clinical_registry_noncoronary_procedure", "clinical_registry_surgery_episode",
    "clinical_registry_surgery_followup", "clinical_registry_surgery_procedure",
)

def _registry_observation_query():
    # Use registry-field routing rules and parent registry entries to construct configured
    # observations.
    route = read_source(SRC_S3B_REGISTRY_ROUTE).select(
        "registry_field_id", "product", "field", "route", "question_display", "unit_column"
    )
    parent = spark.read.table(_n("journey_clinical.registry_entry")).select(
        F.col("patient_event_key").alias("registry_entry_key"), "subject_key", "subject_id_system",
        "person_id", "encounter_id", "event_datetime", "event_end_datetime", "record_status",
        "record_status_effective_from", "record_status_effective_to", "source_update_timestamp", "loaded_at",
    )
    frames = []
    for product in S3B_REGISTRY_PRODUCTS:
        relation = product.removeprefix("clinical_")
        source = spark.read.table(_n(f"journey_clinical.{relation}"))
        values = []
        for name in source.columns:
            values.extend((F.lit(name), source[name].cast("string")))
        long = (source.select(
                    F.col("patient_event_key").alias("registry_entry_key"),
                    F.create_map(*values).alias("_values"))
                .select("registry_entry_key", "_values", F.explode(F.map_entries("_values")).alias("_field_value"))
                .select("registry_entry_key", "_values", F.col("_field_value.key").alias("field"),
                        F.col("_field_value.value").alias("answer_text"))
                .where(F.col("answer_text").isNotNull() & (F.trim("answer_text") != ""))
                .withColumn("product", F.lit(product)))
        frames.append(long)
    observations = frames[0]
    for frame in frames[1:]:
        observations = observations.unionByName(frame)
    observations = observations.join(F.broadcast(route), ["product", "field"], "inner").join(parent, "registry_entry_key", "left")
    return (observations
        .withColumn("patient_event_key", stable_id("registry_observation:iweb", F.col("registry_entry_key"), F.col("registry_field_id")))
        .withColumn("registry_product", F.col("product"))
        .withColumn("registry_field", F.col("field"))
        .withColumn("value_number", F.when(F.col("route") == "measurement", F.expr("try_cast(answer_text as decimal(38,10))")))
        .withColumn("unit", F.element_at(F.col("_values"), F.col("unit_column")))
        .withColumn("event_datetime_status", F.when(F.col("event_datetime").isNull(), F.lit("registry_undated")).otherwise(F.lit("registry_dated"))))

@materialized_view(
    name=_n("journey_clinical._registry_observations"),
    private=True,
    comment="Source-linked long observations for governed fields in the ten typed iWeb registry products.",
    refresh_policy="incremental",
)
def _registry_observations():
    # Build the declared dataset: Source-linked long observations for governed fields in the ten
    # typed iWeb registry products.
    return _registry_observation_query().select(
        "patient_event_key", "registry_entry_key", "registry_field_id", "registry_product", "registry_field",
        "route", "question_display", "answer_text", "value_number", "unit", "event_datetime_status",
        "subject_key", "subject_id_system", "person_id", "encounter_id", "event_datetime", "event_end_datetime",
        "record_status", "record_status_effective_from", "record_status_effective_to", "source_update_timestamp", "loaded_at",
    )

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
VITAL_PUBLIC_COLUMNS = [
    'patient_event_key',
    'event_id',
    'doc_response_key',
    'source_dcp_forms_activity_id',
    'source_object',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'vital_code',
    'value_number',
    'value_text',
    'unit_source_value',
    'unit_concept_id',
    'reference_range_low',
    'reference_range_high',
    'method_code',
    'method_display',
    'body_site_code',
    'body_site_display',
    'interpretation_code',
    'interpretation_display',
    'result_status_code',
    'result_status_display',
    'performer_practitioner_id',
    'source_form_key',
    'promotion_rule_id',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
    'registry_field_id',
    'event_datetime_status',
]

for _s3b_axis in ("unit", "method", "body_site", "interpretation"):
    VITAL_PUBLIC_COLUMNS.extend(axis_columns(_s3b_axis))

VITAL_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'doc_response_key',
    'event_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  numeric vital sign

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._numeric_vital_sign"),
    private=True,
    comment="Internal incrementally maintained native numeric-event vital route.",
    refresh_policy="incremental",
)
def _numeric_vital_sign():
    # Build the declared dataset: Internal incrementally maintained native numeric-event vital
    # route.
    return _numeric_vital_canonical().select(*VITAL_STAGE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Form promotion source

# COMMAND ----------

def _form_promotion_source(target_fact_type):
    # Join form items to active promotion rules for the requested vital-sign or score fact type.
    i = read_source(SRC_FORM_ITEM).alias("i")
    c = read_source(SRC_FORM_PROMOTION_CONFIG).where(
        F.col("active_ind") & (F.col("target_fact_type") == target_fact_type)
    ).alias("c")
    return i.join(c, i.DCP_INPUT_REF_ID == c.dcp_input_ref_id, "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  promoted vital sign primitive

# COMMAND ----------

SRC_FORM_PROMOTION_CONFIG = "3_lookup.omop.form_promotion_config"

def _promoted_vital_canonical():
    # Assemble normalized promoted vital rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = _form_promotion_source("vital_sign")
    event_id = stable_id("form_promotion:vital_sign", F.col("i.DOC_RESPONSE_KEY"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("i.PERSON_ID_LONG"))],
        SRC_FORM_ITEM, F.col("i.DOC_RESPONSE_KEY"),
    )
    deleted = F.coalesce(F.col("i.CANONICAL_SOURCE_DELETED_IND"), F.lit(False))
    absent = ~F.coalesce(F.col("i.SOURCE_PRESENT_IND"), F.lit(False))
    inactive = F.coalesce(F.col("i.ACTIVE_IND"), F.lit(0)) == 0
    # contract v2: publish PowerForm response identity and native foreign keys for the promoted vital arm
    return s.select(
        event_id.alias("patient_event_key"),
        F.lit(None).cast("bigint").alias("event_id"),
        F.col("i.DOC_RESPONSE_KEY").cast("string").alias("doc_response_key"),
        F.col("i.DCP_FORMS_ACTIVITY_ID").cast("bigint").alias("source_dcp_forms_activity_id"),
        F.lit("form_promotion").alias("source_object"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        F.col("i.PERSON_ID_LONG").cast("bigint").alias("person_id"),
        F.when(_present(F.col("i.PERSON_ID_LONG")), F.lit("resolved")).otherwise(F.lit("unresolved"))
         .alias("identity_status"),
        F.col("i.ENCNTR_ID_LONG").cast("bigint").alias("encounter_id"),
        F.coalesce(F.col("i.RESPONSE_DT_TM"), F.col("i.PERFORMED_DT_TM"),
                   F.col("i.DOCUMENTATION_DT_TM")).alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.col("c.source_coding_system").alias("source_coding_system"),
        F.col("c.source_code").alias("source_code"), F.col("c.source_display").alias("source_display"),
        codeable_concept(
            coding_obj(F.col("c.source_coding_system"), F.col("c.source_code"),
                       F.col("c.source_display"), True)
        ).alias("vital_code"),
        F.coalesce(F.col("i.CANONICAL_NUMERIC_RESULT"), F.col("i.NUMERIC_RESPONSE_NBR"))
         .cast("decimal(38,10)").alias("value_number"),
        F.coalesce(F.col("i.CANONICAL_TEXT_RESULT"), F.col("i.RESPONSE_TEXT_TXT"))
         .alias("value_text"),
        F.lit(None).cast("string").alias("unit_source_value"),
        F.col("i.UNIT_CONCEPT_ID").cast("string").alias("unit_concept_id"),
        F.lit(None).cast("decimal(38,10)").alias("reference_range_low"),
        F.lit(None).cast("decimal(38,10)").alias("reference_range_high"),
        F.lit(None).cast("string").alias("method_code"), F.lit(None).cast("string").alias("method_display"),
        F.lit(None).cast("string").alias("body_site_code"), F.lit(None).cast("string").alias("body_site_display"),
        F.lit(None).cast("string").alias("interpretation_code"),
        F.lit(None).cast("string").alias("interpretation_display"),
        F.col("i.FORM_STATUS_CD").cast("string").alias("result_status_code"),
        F.col("i.STATUS").alias("result_status_display"),
        F.col("i.PERFORMED_PRSNL_ID_LONG").cast("bigint").alias("performer_practitioner_id"),
        stable_id("form:mill:powerform", F.col("i.DCP_FORMS_ACTIVITY_ID")).alias("source_form_key"),
        F.col("c.promotion_rule_id"),
        F.when(deleted | absent, F.lit("retracted")).when(inactive, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        F.col("i.FIRST_DOCUMENTED_DT_TM").alias("record_status_effective_from"),
        F.when(deleted | absent | inactive, F.col("i.LAST_DOCUMENTED_DT_TM"))
         .alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"), F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("form_promotion").alias("source_feed"),
        F.date_format(F.col("i.ADC_UPDT"), "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("i.ADC_UPDT").alias("source_update_timestamp"),
        F.col("i.ADC_UPDT").alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_FORM_ITEM).alias("_source_table"),
        F.col("i.DOC_RESPONSE_KEY").alias("_source_row_id"),
        F.col("i.QUESTION_CONCEPT_ID").cast("string").alias("_omop_code"),
        F.col("c.source_display").alias("_omop_display"),
    )

@materialized_view(
    name=_n("journey_clinical._promoted_vital_sign_primitive"),
    private=True,
    comment="Internal incrementally maintained governed form-promotion vital route.",
    refresh_policy="incremental",
)
def _promoted_vital_sign_primitive():
    # Build the declared dataset: Internal incrementally maintained governed form-promotion
    # vital route.
    return _promoted_vital_canonical().select(*PROMOTED_VITAL_STAGE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc vital sign

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_vital_sign"),
    comment="Private JSON bridge and Gold cross-rule flags for vital_sign.",
    refresh_policy="incremental",
)
def _qc_vital_sign():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for vital_sign.
    return _cross_qc_primitive(
        _vital_sign_canonical(),
        "vital_sign",
        {"vital_code": "_qc_vital_code_json"},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / vital sign

# COMMAND ----------

VITAL_SIGN_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "event_id": "Millennium numeric EVENT_ID; native key for the numeric_event arm.",
    "doc_response_key": "PowerForm DOC_RESPONSE_KEY; native key for the form_promotion arm.",
    "source_dcp_forms_activity_id": "Millennium DCP_FORMS_ACTIVITY_ID of the source form for promoted rows.",
    "source_object": "Native source arm: numeric_event or form_promotion.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "event_datetime": "Clinical start date and time of the event as recorded on the Millennium CLINICAL_EVENT row.",
    "event_end_datetime": "Measurement end timestamp.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each vital sign record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Source vital code.",
    "source_display": "Source vital display.",
    "vital_code": "Source and mapped vital CodeableConcept.",
    "value_number": "Numeric vital value.",
    "value_text": "Verbatim result text.",
    "unit_source_value": "Source result unit.",
    "unit_concept_id": "Mapped unit concept identifier.",
    "reference_range_low": "Reference-range lower bound.",
    "reference_range_high": "Reference-range upper bound.",
    "method_code": "Source measurement method code.",
    "method_display": "Source measurement method display.",
    "body_site_code": "Body-site code when supplied.",
    "body_site_display": "Body-site display when supplied.",
    "interpretation_code": "Source interpretation code.",
    "interpretation_display": "Source interpretation display.",
    "result_status_code": "Source result status code.",
    "result_status_display": "Source result status display.",
    "performer_practitioner_id": "Millennium personnel PERSON_ID for the performer.",
    "source_form_key": "Deterministic SHA-256 key of the source PowerForm instance for promoted rows.",
    "promotion_rule_id": "Governed promotion-rule identifier.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Registered source route.",
    "registry_field_id": "Governed SHA-256 registry product and field identifier for registry observations; null on native rows.",
    "event_datetime_status": "registry_dated or registry_undated for registry observations; null on native rows.",
    "record_status": "Arm-specific row status. Numeric events are retracted when map_numeric_events.SOURCE_DELETED_IND is true (null defaults false), otherwise superseded when CLINICAL_EVENT_VALID_UNTIL_DT_TM is before 2100-01-01, otherwise active. Form-promoted rows are retracted when CANONICAL_SOURCE_DELETED_IND is true or SOURCE_PRESENT_IND is false or missing; otherwise superseded when ACTIVE_IND is zero or missing, otherwise active. Retraction has priority over supersession; this is not the clinical result-status code.",
    "record_status_effective_from": "map_numeric_events.CLINICAL_EVENT_VALID_FROM_DT_TM on the numeric-event arm; map_powerform_assessment_item.FIRST_DOCUMENTED_DT_TM on the form-promotion arm. Each is carried unchanged, with no performed/response/ingestion-time fallback; a missing value remains null.",
    "record_status_effective_to": "Numeric events return CLINICAL_EVENT_VALID_UNTIL_DT_TM when deleted or ended, otherwise null; deletion can therefore retain a far-future end value. Form-promoted rows return LAST_DOCUMENTED_DT_TM when deleted, absent or inactive, otherwise null. No missing-end fallback is applied, and the form documentation end is not an independently observed status-transition time.",
    "source_update_timestamp": "For numeric events, greatest of map_numeric_events.STRING_RESULT_UPDT_DT_TM, CLINICAL_EVENT_UPDT_DT_TM and ADC_UPDT. For form-promoted rows, map_powerform_assessment_item.ADC_UPDT directly. This mixes source and ingestion provenance rather than uniformly representing a native modification clock; the promotion lookup adds no clock and Silver refresh time is not substituted.",
    "loaded_at": "map_numeric_events.ADC_UPDT for numeric-event rows, or map_powerform_assessment_item.ADC_UPDT for form-promoted rows. Each arm's bronze load clock is carried unchanged through its staging, union and quality wrappers. No form-parent or promotion-configuration clock is added; missing values remain null and this is not Silver refresh time.",
}

@materialized_view(
    name=_n("journey_clinical.vital_sign"),
    comment="Native numeric-event measurements and governed form-promoted vital signs, retaining source identifiers, measurement values and units.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=VITAL_SIGN_COLUMN_COMMENTS,
)
def vital_sign():
    # Build the declared dataset: Native numeric-event measurements and governed form-promoted
    # vital signs, retaining source identifiers, measurement values and units.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_vital_sign")),
        "vital_sign",
        {"vital_code": "_qc_vital_code_json"},
        VITAL_PUBLIC_COLUMNS,
        VITAL_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._vital_sign_metadata'),
    comment='Internal quality and batch metadata for clinical_vital_sign; same row grain as the research table. Join keys: patient_event_key, source_object, doc_response_key, event_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def vital_sign_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_vital_sign;
    # same row grain as the research table. Join keys: patient_event_key, source_object,
    # doc_response_key, event_id.
    return (spark.read.table(_n("journey_clinical._qc_vital_sign"))).select(*VITAL_LIFECYCLE_COLUMNS)

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
SCORE_PUBLIC_COLUMNS = [
    'patient_event_key',
    'event_id',
    'doc_response_key',
    'source_dcp_forms_activity_id',
    'source_object',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'score_code',
    'score_name',
    'value_number',
    'value_text',
    'unit_source_value',
    'unit_concept_id',
    'component_count',
    'interpretation_code',
    'interpretation_display',
    'result_status_code',
    'result_status_display',
    'performer_practitioner_id',
    'source_form_key',
    'promotion_rule_id',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

SCORE_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'doc_response_key',
    'event_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  numeric clinical score

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._numeric_clinical_score"),
    private=True,
    comment="Internal incrementally maintained native numeric-event clinical-score route.",
    refresh_policy="incremental",
)
def _numeric_clinical_score():
    # Build the declared dataset: Internal incrementally maintained native numeric-event
    # clinical-score route.
    return _numeric_score_canonical().select(*SCORE_STAGE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  promoted clinical score primitive

# COMMAND ----------

def _promoted_score_canonical():
    # Assemble normalized promoted score rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = _form_promotion_source("clinical_score")
    event_id = stable_id("form_promotion:clinical_score", F.col("i.DOC_RESPONSE_KEY"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("i.PERSON_ID_LONG"))],
        SRC_FORM_ITEM, F.col("i.DOC_RESPONSE_KEY"),
    )
    deleted = F.coalesce(F.col("i.CANONICAL_SOURCE_DELETED_IND"), F.lit(False))
    absent = ~F.coalesce(F.col("i.SOURCE_PRESENT_IND"), F.lit(False))
    inactive = F.coalesce(F.col("i.ACTIVE_IND"), F.lit(0)) == 0
    component = F.struct(
        F.lit(1).cast("long").alias("sequence"),
        F.col("i.DOC_RESPONSE_KEY").alias("component_id"),
        F.col("c.source_coding_system").alias("coding_system"),
        F.col("c.source_code").alias("coding_code"), F.col("c.source_display").alias("coding_display"),
        F.coalesce(F.col("i.CANONICAL_NUMERIC_RESULT"), F.col("i.NUMERIC_RESPONSE_NBR"))
         .cast("decimal(38,10)").alias("value_number"),
        F.coalesce(F.col("i.CANONICAL_TEXT_RESULT"), F.col("i.RESPONSE_TEXT_TXT")).alias("value_text"),
        F.lit(None).cast("string").alias("unit"),
    )
    # contract v2: publish PowerForm response identity and native foreign keys for the promoted score arm
    return s.select(
        event_id.alias("patient_event_key"),
        F.lit(None).cast("bigint").alias("event_id"),
        F.col("i.DOC_RESPONSE_KEY").cast("string").alias("doc_response_key"),
        F.col("i.DCP_FORMS_ACTIVITY_ID").cast("bigint").alias("source_dcp_forms_activity_id"),
        F.lit("form_promotion").alias("source_object"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        F.col("i.PERSON_ID_LONG").cast("bigint").alias("person_id"),
        F.when(_present(F.col("i.PERSON_ID_LONG")), F.lit("resolved")).otherwise(F.lit("unresolved"))
         .alias("identity_status"),
        F.col("i.ENCNTR_ID_LONG").cast("bigint").alias("encounter_id"),
        F.coalesce(F.col("i.RESPONSE_DT_TM"), F.col("i.PERFORMED_DT_TM"),
                   F.col("i.DOCUMENTATION_DT_TM")).alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.col("c.source_coding_system").alias("source_coding_system"),
        F.col("c.source_code").alias("source_code"), F.col("c.source_display").alias("source_display"),
        codeable_concept(
            coding_obj(F.col("c.source_coding_system"), F.col("c.source_code"),
                       F.col("c.source_display"), True)
        ).alias("score_code"),
        F.col("c.source_display").alias("score_name"),
        F.coalesce(F.col("i.CANONICAL_NUMERIC_RESULT"), F.col("i.NUMERIC_RESPONSE_NBR"))
         .cast("decimal(38,10)").alias("value_number"),
        F.coalesce(F.col("i.CANONICAL_TEXT_RESULT"), F.col("i.RESPONSE_TEXT_TXT")).alias("value_text"),
        F.lit(None).cast("string").alias("unit_source_value"),
        F.col("i.UNIT_CONCEPT_ID").cast("string").alias("unit_concept_id"),
        F.to_json(F.array(component)).alias("_components_json"),
        F.lit(1).cast("long").alias("component_count"),
        F.lit(None).cast("string").alias("interpretation_code"),
        F.lit(None).cast("string").alias("interpretation_display"),
        F.col("i.FORM_STATUS_CD").cast("string").alias("result_status_code"),
        F.col("i.STATUS").alias("result_status_display"),
        F.col("i.PERFORMED_PRSNL_ID_LONG").cast("bigint").alias("performer_practitioner_id"),
        stable_id("form:mill:powerform", F.col("i.DCP_FORMS_ACTIVITY_ID")).alias("source_form_key"),
        F.col("c.promotion_rule_id"),
        F.when(deleted | absent, F.lit("retracted")).when(inactive, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        F.col("i.FIRST_DOCUMENTED_DT_TM").alias("record_status_effective_from"),
        F.when(deleted | absent | inactive, F.col("i.LAST_DOCUMENTED_DT_TM"))
         .alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"), F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("form_promotion").alias("source_feed"),
        F.date_format(F.col("i.ADC_UPDT"), "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("i.ADC_UPDT").alias("source_update_timestamp"),
        F.col("i.ADC_UPDT").alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_FORM_ITEM).alias("_source_table"),
        F.col("i.DOC_RESPONSE_KEY").alias("_source_row_id"),
        F.col("i.QUESTION_CONCEPT_ID").cast("string").alias("_omop_code"),
        F.col("c.source_display").alias("_omop_display"),
    )

@materialized_view(
    name=_n("journey_clinical._promoted_clinical_score_primitive"),
    private=True,
    comment="Internal incrementally maintained governed form-promotion clinical-score route.",
    refresh_policy="incremental",
)
def _promoted_clinical_score_primitive():
    # Build the declared dataset: Internal incrementally maintained governed form-promotion
    # clinical-score route.
    return _promoted_score_canonical().select(*PROMOTED_SCORE_STAGE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc clinical score

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_clinical_score"),
    comment="Private JSON bridge and Gold cross-rule flags for clinical_score.",
    refresh_policy="incremental",
)
def _qc_clinical_score():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # clinical_score.
    return _cross_qc_primitive(
        _clinical_score_canonical(),
        "clinical_score",
        {
            "score_code": "_qc_score_code_json",
        },
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / clinical score

# COMMAND ----------

CLINICAL_SCORE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "event_id": "Millennium numeric EVENT_ID; native key for the numeric_event arm.",
    "doc_response_key": "PowerForm DOC_RESPONSE_KEY; native key for the form_promotion arm.",
    "source_dcp_forms_activity_id": "Millennium DCP_FORMS_ACTIVITY_ID of the source form for promoted rows.",
    "source_object": "Native source arm: numeric_event or form_promotion.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "event_datetime": "Clinical start date and time of the event as recorded on the Millennium CLINICAL_EVENT row.",
    "event_end_datetime": "Score end timestamp.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each clinical score record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Source score code.",
    "source_display": "Source score display.",
    "score_code": "Source and mapped score CodeableConcept.",
    "score_name": "Human-readable score or scale name.",
    "value_number": "Numeric score value.",
    "value_text": "Verbatim score text.",
    "unit_source_value": "Source unit when supplied.",
    "unit_concept_id": "Mapped unit concept identifier.",
    "component_count": "Number of ordered component objects.",
    "interpretation_code": "Source interpretation code.",
    "interpretation_display": "Source interpretation display.",
    "result_status_code": "Source result status code.",
    "result_status_display": "Source result status display.",
    "performer_practitioner_id": "Millennium personnel PERSON_ID for the performer.",
    "source_form_key": "Deterministic SHA-256 key of the source PowerForm instance for promoted rows.",
    "promotion_rule_id": "Governed promotion-rule identifier.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Registered source route.",
    "record_status": "Arm-specific row status. Numeric events are retracted when map_numeric_events.SOURCE_DELETED_IND is true (null defaults false), otherwise superseded when CLINICAL_EVENT_VALID_UNTIL_DT_TM is before 2100-01-01, otherwise active. Form-promoted rows are retracted when CANONICAL_SOURCE_DELETED_IND is true or SOURCE_PRESENT_IND is false or missing; otherwise superseded when ACTIVE_IND is zero or missing, otherwise active. Retraction has priority over supersession; this is not the clinical result-status code.",
    "record_status_effective_from": "map_numeric_events.CLINICAL_EVENT_VALID_FROM_DT_TM on the numeric-event arm; map_powerform_assessment_item.FIRST_DOCUMENTED_DT_TM on the form-promotion arm. Each is carried unchanged, with no performed/response/ingestion-time fallback; a missing value remains null.",
    "record_status_effective_to": "Numeric events return CLINICAL_EVENT_VALID_UNTIL_DT_TM when deleted or ended, otherwise null; deletion can therefore retain a far-future end value. Form-promoted rows return LAST_DOCUMENTED_DT_TM when deleted, absent or inactive, otherwise null. No missing-end fallback is applied, and the form documentation end is not an independently observed status-transition time.",
    "source_update_timestamp": "For numeric events, greatest of map_numeric_events.STRING_RESULT_UPDT_DT_TM, CLINICAL_EVENT_UPDT_DT_TM and ADC_UPDT. For form-promoted rows, map_powerform_assessment_item.ADC_UPDT directly. This mixes source and ingestion provenance rather than uniformly representing a native modification clock; the promotion lookup adds no clock and Silver refresh time is not substituted.",
    "loaded_at": "map_numeric_events.ADC_UPDT for numeric-event rows, or map_powerform_assessment_item.ADC_UPDT for form-promoted rows. Each arm's bronze load clock is carried unchanged through its staging, union and quality wrappers. No form-parent or promotion-configuration clock is added; missing values remain null and this is not Silver refresh time.",
}

@materialized_view(
    name=_n("journey_clinical.clinical_score"),
    comment="Native numeric-event and governed form-promoted clinical score results.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=CLINICAL_SCORE_COLUMN_COMMENTS,
)
def clinical_score():
    # Build the declared dataset: Native numeric-event and governed form-promoted clinical score
    # results.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_clinical_score")),
        "clinical_score",
        {
            "score_code": "_qc_score_code_json",
        },
        SCORE_PUBLIC_COLUMNS,
        SCORE_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._clinical_score_metadata'),
    comment='Internal quality and batch metadata for clinical_clinical_score; same row grain as the research table. Join keys: patient_event_key, source_object, doc_response_key, event_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def clinical_score_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_clinical_score; same row grain as the research table. Join keys:
    # patient_event_key, source_object, doc_response_key, event_id.
    return (spark.read.table(_n("journey_clinical._qc_clinical_score"))).select(*SCORE_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / clinical score component

# COMMAND ----------

# ==== journey_clinical.clinical_score_component ====

CLINICAL_SCORE_COMPONENT_PUBLIC_COLUMNS = [
    "patient_event_key", "event_id", "doc_response_key", "source_object",
    "sequence", "component_id", "coding_system", "coding_code",
    "coding_display", "value_number", "value_text", "unit", "loaded_at",
] + axis_columns("component")

CLINICAL_SCORE_COMPONENT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent clinical_clinical_score row.",
    "event_id": "Millennium numeric EVENT_ID for the numeric-event parent arm.",
    "doc_response_key": "PowerForm DOC_RESPONSE_KEY for the form-promotion parent arm.",
    "source_object": "Parent source arm: numeric_event or form_promotion.",
    "sequence": "Component sequence within the parent score.",
    "component_id": "Native source component identifier.",
    "coding_system": "Source coding-system namespace for the component.",
    "coding_code": "Source component code.",
    "coding_display": "Source component display.",
    "value_number": "Numeric component value when supplied.",
    "value_text": "Verbatim component text when supplied.",
    "unit": "Source component unit when supplied.",
    "loaded_at": "Bronze load timestamp inherited from the parent source row.",
}

CLINICAL_SCORE_COMPONENT_COLUMN_COMMENTS.update(axis_comments("component", "score component or categorical score response"))

@materialized_view(
    name=_n("journey_clinical.clinical_score_component"),
    comment="One ordered component per native or governed promoted clinical score.",
    column_comments=CLINICAL_SCORE_COMPONENT_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def clinical_score_component():
    # Build the declared dataset: One ordered component per native or governed promoted clinical
    # score.
    numeric = spark.read.table(
        _n("journey_clinical._numeric_clinical_score")
    ).select(
        "patient_event_key", "event_id", "doc_response_key", "source_object",
        F.col("source_code").alias("_score_source_code"), "_components_json", "loaded_at",
    )
    promoted = spark.read.table(
        _n("journey_clinical._promoted_clinical_score_primitive")
    ).select(
        "patient_event_key", "event_id", "doc_response_key", "source_object",
        F.col("source_code").alias("_score_source_code"), "_components_json", "loaded_at",
    )
    parents = numeric.unionByName(promoted).where(_usable_code(F.col("_score_source_code")))
    component_schema = (
        "array<struct<sequence:bigint,component_id:string,coding_system:string,"
        "coding_code:string,coding_display:string,value_number:decimal(38,10),"
        "value_text:string,unit:string>>"
    )
    exploded = parents.select(
        "patient_event_key", "event_id", "doc_response_key", "source_object", "_score_source_code", "loaded_at",
        F.explode(F.from_json(F.col("_components_json"), component_schema)).alias("_component"),
    )
    result = exploded.select(
        "patient_event_key", "event_id", "doc_response_key", "source_object",
        "_score_source_code",
        F.col("_component.sequence").alias("sequence"),
        F.col("_component.component_id").alias("component_id"),
        F.col("_component.coding_system").alias("coding_system"),
        F.col("_component.coding_code").alias("coding_code"),
        F.col("_component.coding_display").alias("coding_display"),
        F.col("_component.value_number").alias("value_number"),
        F.col("_component.value_text").alias("value_text"),
        F.col("_component.unit").alias("unit"),
        "loaded_at",
    )
    component_key = F.sha2(F.concat_ws("|", F.coalesce("_score_source_code", F.lit("")),
        F.coalesce("coding_system", F.lit("")), F.coalesce("coding_code", F.lit("")),
        F.lower(F.trim(F.regexp_replace(F.coalesce("coding_display", F.lit("")), r"\s+", " "))),
        F.when(F.col("value_number").isNull(),
               F.lower(F.trim(F.regexp_replace(F.coalesce("value_text", F.lit("")), r"\s+", " "))))
         .otherwise(F.lit(""))), 256)
    result = _s3_direct_axis(
        result, "component", F.lit("urn:barts:score-component"), component_key,
        F.coalesce("coding_display", "value_text"),
    )
    result = _s3_lookup_axis(
        result, "component", SRC_S3B_SCORE_COMPONENT_MAP, "urn:barts:score-component",
        component_key, allow_split=False, model_name="clinical_score_component",
        row_key="patient_event_key",
    )
    return result.drop("_score_source_code").select(*CLINICAL_SCORE_COMPONENT_PUBLIC_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  med admin ingredient grouped

# COMMAND ----------

SRC_MED_ADMIN_INGREDIENT = "4_prod.bronze.map_med_admin_ingredient"

def _med_admin_ingredient_grouped_query():
    # Group medication-administration ingredient components in their action and component
    # sequence.
    s = read_source(SRC_MED_ADMIN_INGREDIENT)
    ingredient = F.struct(
        F.coalesce(s.ACTION_SEQUENCE, F.lit(0)).cast("long").alias("action_sequence"),
        F.coalesce(s.COMP_SEQUENCE, F.lit(0)).cast("long").alias("component_sequence"),
        F.concat_ws(":", s.INGREDIENT_ORDER_ID.cast("string"),
                    s.ACTION_SEQUENCE.cast("string"), s.COMP_SEQUENCE.cast("string"))
         .alias("component_id"),
        F.lit("urn:cerner:synonym_id").alias("coding_system"),
        s.SYNONYM_ID.cast("string").alias("coding_code"),
        F.coalesce(s.ORDERED_AS_MNEMONIC, s.SUPPLIED_AS_MNEMONIC,
                   s.ORDER_MNEMONIC, s.HNA_ORDER_MNEMONIC).alias("coding_display"),
        s.STRENGTH.cast("decimal(38,10)").alias("strength_value"),
        s.STRENGTH_UNIT_CD.cast("string").alias("strength_unit"),
        s.VOLUME.cast("decimal(38,10)").alias("volume_value"),
        s.VOLUME_UNIT_CD.cast("string").alias("volume_unit"),
        F.coalesce(s.ORDERED_DOSE, s.DOSE_QUANTITY).cast("decimal(38,10)").alias("dose_value"),
        F.coalesce(s.ORDERED_DOSE_UNIT_CD, s.DOSE_QUANTITY_UNIT_CD).cast("string").alias("dose_unit"),
        s.NORMALIZED_RATE.cast("decimal(38,10)").alias("rate_value"),
        s.NORMALIZED_RATE_UNIT_CD.cast("string").alias("rate_unit"),
        s.CONCENTRATION.cast("decimal(38,10)").alias("concentration_value"),
        s.CONCENTRATION_UNIT_CD.cast("string").alias("concentration_unit"),
        s.INGREDIENT_TYPE_FLAG.cast("string").alias("ingredient_type_code"),
        (s.CLINICALLY_SIGNIFICANT_FLAG == 1).alias("clinically_significant_ind"),
        (s.INCLUDE_IN_TOTAL_VOLUME_FLAG == 1).alias("include_in_total_volume_ind"),
        s.FREETEXT_DOSE.alias("freetext_dose"),
        F.lit(True).alias("source_present_ind"),
    )
    return s.groupBy("EVENT_ID").agg(
        F.to_json(F.sort_array(F.collect_list(ingredient))).alias("ingredients_json"),
        F.count(F.lit(1)).cast("long").alias("ingredient_count"),
        F.max(F.greatest(s.UPDT_DT_TM, s.ADC_UPDT)).alias("ingredient_source_update_timestamp"),
        F.max(s.ADC_UPDT).alias("ingredient_loaded_at"),
    )

@materialized_view(
    name=_n("journey_clinical._med_admin_ingredient_grouped"),
    private=True,
    comment="Internal ordered ingredient aggregate for medication administrations.",
    refresh_policy="incremental",
)
def _med_admin_ingredient_grouped():
    # Build the declared dataset: Internal ordered ingredient aggregate for medication
    # administrations.
    return _med_admin_ingredient_grouped_query()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  med order action grouped

# COMMAND ----------

def _med_order_action_grouped_query():
    # Group medication-order actions with their status and effective-time evidence.
    s = read_source(SRC_MEDICATION_ORDER_ACTION)
    action = F.struct(
        F.coalesce(s.ACTION_SEQUENCE, F.lit(0)).cast("long").alias("sequence"),
        F.coalesce(s.ACTION_TYPE_DESCRIPTION, s.ACTION_TYPE_CD.cast("string")).alias("status_kind"),
        s.ORDER_STATUS_CD.cast("string").alias("status_code"),
        F.coalesce(s.ORDER_STATUS_DESCRIPTION, s.ACTION_TYPE_DESCRIPTION).alias("status_display"),
        F.coalesce(s.ACTION_DT_TM, s.EFFECTIVE_DT_TM, s.ORDER_DT_TM).alias("effective_datetime"),
        F.when(s.ACTION_PERSONNEL_ID.isNotNull(),
               stable_id("practitioner:mill", s.ACTION_PERSONNEL_ID)).alias("practitioner_id"),
        s.ACTION_SEQUENCE.cast("long").alias("source_action_sequence"),
        s.ACTION_TYPE_CD.cast("string").alias("action_type_code"),
        s.ACTION_TYPE_DESCRIPTION.alias("action_type_display"),
        s.ACTION_QUALIFIER_CD.cast("string").alias("action_qualifier_code"),
        s.ACTION_QUALIFIER_DESCRIPTION.alias("action_qualifier_display"),
        (s.ACTION_REJECTED_IND == 1).alias("action_rejected_ind"),
        s.HISTORICAL_FEED_IND.alias("historical_feed_ind"),
        s.SOURCE_PRESENT_IND.alias("source_present_ind"),
    )
    return s.groupBy("ORDER_ID").agg(
        F.to_json(F.sort_array(F.collect_list(action))).alias("status_history_json"),
        F.count(F.lit(1)).cast("long").alias("status_history_count"),
        F.max(F.greatest(s.SOURCE_ADC_UPDT, s.ORDER_SOURCE_ADC_UPDT, s.ADC_UPDT))
         .alias("action_source_update_timestamp"),
        F.max(s.ADC_UPDT).alias("action_loaded_at"),
    )

@materialized_view(
    name=_n("journey_clinical._med_order_action_grouped"),
    private=True,
    comment="Internal ordered status-history aggregate for medication orders.",
    refresh_policy="incremental",
)
def _med_order_action_grouped():
    # Build the declared dataset: Internal ordered status-history aggregate for medication
    # orders.
    return _med_order_action_grouped_query()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  med order ingredient grouped

# COMMAND ----------

SRC_MEDICATION_ORDER_INGREDIENT = "4_prod.bronze.map_medication_order_ingredient"

def _med_order_ingredient_grouped_query():
    # Group medication-order ingredients with their action and component sequence.
    s = read_source(SRC_MEDICATION_ORDER_INGREDIENT)
    ingredient = F.struct(
        F.coalesce(s.ACTION_SEQUENCE, F.lit(0)).cast("long").alias("action_sequence"),
        F.coalesce(s.COMP_SEQUENCE, F.lit(0)).cast("long").alias("component_sequence"),
        F.concat_ws(":", s.ORDER_ID.cast("string"), s.ACTION_SEQUENCE.cast("string"),
                    s.COMP_SEQUENCE.cast("string")).alias("component_id"),
        F.lit("urn:cerner:synonym_id").alias("coding_system"),
        s.SYNONYM_ID.cast("string").alias("coding_code"),
        F.coalesce(s.ORDERED_AS_MNEMONIC, s.SUPPLIED_AS_MNEMONIC,
                   s.ORDER_MNEMONIC, s.HNA_ORDER_MNEMONIC).alias("coding_display"),
        s.STRENGTH.cast("decimal(38,10)").alias("strength_value"),
        s.STRENGTH_UNIT_DESCRIPTION.alias("strength_unit"),
        s.VOLUME.cast("decimal(38,10)").alias("volume_value"),
        s.VOLUME_UNIT_DESCRIPTION.alias("volume_unit"),
        F.coalesce(s.ORDERED_DOSE, s.DOSE_QUANTITY).cast("decimal(38,10)").alias("dose_value"),
        F.coalesce(s.ORDERED_DOSE_UNIT_DESCRIPTION,
                   s.DOSE_QUANTITY_UNIT_DESCRIPTION).alias("dose_unit"),
        s.NORMALIZED_RATE.cast("decimal(38,10)").alias("rate_value"),
        s.NORMALIZED_RATE_UNIT_DESCRIPTION.alias("rate_unit"),
        s.CONCENTRATION.cast("decimal(38,10)").alias("concentration_value"),
        s.CONCENTRATION_UNIT_DESCRIPTION.alias("concentration_unit"),
        s.INGREDIENT_TYPE_FLAG.cast("string").alias("ingredient_type_code"),
        (s.CLINICALLY_SIGNIFICANT_FLAG == 1).alias("clinically_significant_ind"),
        (s.INCLUDE_IN_TOTAL_VOLUME_FLAG == 1).alias("include_in_total_volume_ind"),
        s.FREETEXT_DOSE.alias("freetext_dose"),
        s.SOURCE_PRESENT_IND.alias("source_present_ind"),
    )
    return s.groupBy("ORDER_ID").agg(
        F.to_json(F.sort_array(F.collect_list(ingredient))).alias("ingredients_json"),
        F.count(F.lit(1)).cast("long").alias("ingredient_count"),
        F.max(F.greatest(s.SOURCE_ADC_UPDT, s.ORDER_SOURCE_ADC_UPDT, s.ADC_UPDT))
         .alias("ingredient_source_update_timestamp"),
        F.max(s.ADC_UPDT).alias("ingredient_loaded_at"),
    )

@materialized_view(
    name=_n("journey_clinical._med_order_ingredient_grouped"),
    private=True,
    comment="Internal ordered ingredient aggregate for medication orders.",
    refresh_policy="incremental",
)
def _med_order_ingredient_grouped():
    # Build the declared dataset: Internal ordered ingredient aggregate for medication orders.
    return _med_order_ingredient_grouped_query()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  med order detail grouped

# COMMAND ----------

SRC_MEDICATION_ORDER_DETAIL = "4_prod.bronze.map_medication_order_detail"

def _med_order_detail_grouped_query():
    # Group medication-order detail fields with their sequence, text and numeric values.
    s = read_source(SRC_MEDICATION_ORDER_DETAIL)
    detail = F.struct(
        F.coalesce(s.ACTION_SEQUENCE, F.lit(0)).cast("long").alias("action_sequence"),
        F.coalesce(s.DETAIL_SEQUENCE, F.lit(0)).cast("long").alias("detail_sequence"),
        s.OE_FIELD_ID.cast("string").alias("field_id"),
        s.OE_FIELD_MEANING.alias("field_meaning"),
        F.coalesce(s.OE_FIELD_DISPLAY_VALUE_EXTEND,
                   s.OE_FIELD_DISPLAY_VALUE).alias("value_text"),
        s.OE_FIELD_VALUE.cast("decimal(38,10)").alias("value_number"),
        s.OE_FIELD_DT_TM_VALUE.alias("value_datetime"),
        s.DETAIL_HISTORY_CONTRACT.alias("history_contract"),
        s.PARENT_ACTION_SEQUENCE.cast("long").alias("parent_action_sequence"),
        s.LAST_ACTION_SEQUENCE.cast("long").alias("last_action_sequence"),
        s.SOURCE_PRESENT_IND.alias("source_present_ind"),
    )
    return s.groupBy("ORDER_ID").agg(
        F.to_json(F.sort_array(F.collect_list(detail))).alias("order_details_json"),
        F.count(F.lit(1)).cast("long").alias("order_detail_count"),
        F.max(F.greatest(s.SOURCE_ADC_UPDT, s.ORDER_SOURCE_ADC_UPDT, s.ADC_UPDT))
         .alias("detail_source_update_timestamp"),
        F.max(s.ADC_UPDT).alias("detail_loaded_at"),
    )

@materialized_view(
    name=_n("journey_clinical._med_order_detail_grouped"),
    private=True,
    comment="Internal ordered latest-action-detail aggregate for medication orders.",
    refresh_policy="incremental",
)
def _med_order_detail_grouped():
    # Build the declared dataset: Internal ordered latest-action-detail aggregate for medication
    # orders.
    return _med_order_detail_grouped_query()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  medication admin primitive

# COMMAND ----------

def _med_admin_status_entry(sequence, status_kind, status_code, status_display,
                            effective_datetime, practitioner_id):
    # Construct one consistently typed medication-administration status-history entry.
    return F.struct(
        F.lit(sequence).cast("long").alias("sequence"),
        status_kind.cast("string").alias("status_kind"),
        status_code.cast("string").alias("status_code"),
        status_display.cast("string").alias("status_display"),
        effective_datetime.cast("timestamp").alias("effective_datetime"),
        practitioner_id.cast("string").alias("practitioner_id"),
        F.lit(None).cast("long").alias("source_action_sequence"),
        F.lit(True).alias("source_present_ind"),
    )

def _medication_admin_primitive_query():
    # Enrich administration rows with grouped ingredients and linked order evidence for the
    # internal stage.
    a = read_source(SRC_MED_ADMIN).alias("a")
    ingredients = spark.read.table(_n("journey_clinical._med_admin_ingredient_grouped")).alias("i")
    orders = read_source(SRC_MEDICATION_ORDER).select(
        F.col("ORDER_ID").alias("_linked_order_id")
    ).alias("o")
    joined = (
        a.join(ingredients, F.col("a.EVENT_ID") == F.col("i.EVENT_ID"), "left")
         .join(orders, F.col("a.ORDER_ID") == F.col("o._linked_order_id"), "left")
    )
    event_id = stable_id("medication_admin:mill", F.col("a.EVENT_ID"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("a.PERSON_ID"))], SRC_MED_ADMIN, F.col("a.EVENT_ID")
    )
    performer = F.coalesce(F.col("a.PERFORMED_PRSNL_ID"), F.col("a.PRSNL_ID"))
    verifier = F.coalesce(F.col("a.MAE_VERIFIED_PRSNL_ID"), F.col("a.CE_VERIFIED_PRSNL_ID"))
    status_entries = F.filter(F.array(
        _med_admin_status_entry(
            10, F.lit("scheduled"),
            F.when(F.col("a.SCHEDULED_DT_TM").isNotNull(), F.lit("scheduled")),
            F.when(F.col("a.SCHEDULED_DT_TM").isNotNull(), F.lit("Scheduled")),
            F.col("a.SCHEDULED_DT_TM"), F.lit(None).cast("string"),
        ),
        _med_admin_status_entry(
            20, F.lit("administration_event"), F.col("a.EVENT_TYPE_CD"),
            F.col("a.EVENT_TYPE_DISPLAY"),
            F.coalesce(F.col("a.PERFORMED_DT_TM"), F.col("a.ADMIN_START_DT_TM")),
            F.when(performer.isNotNull(), stable_id("practitioner:mill", performer)),
        ),
        _med_admin_status_entry(
            30, F.lit("result"), F.col("a.RESULT_STATUS_CD"),
            F.col("a.RESULT_STATUS_DISPLAY"),
            F.coalesce(F.col("a.ADMIN_END_DT_TM"), F.col("a.ADMIN_START_DT_TM")),
            F.when(verifier.isNotNull(), stable_id("practitioner:mill", verifier)),
        ),
        _med_admin_status_entry(
            40, F.lit("verified"),
            F.when(F.coalesce(F.col("a.VERIFICATION_DT_TM"),
                              F.col("a.VERIFIED_DT_TM")).isNotNull(), F.lit("verified")),
            F.when(F.coalesce(F.col("a.VERIFICATION_DT_TM"),
                              F.col("a.VERIFIED_DT_TM")).isNotNull(), F.lit("Verified")),
            F.coalesce(F.col("a.VERIFICATION_DT_TM"), F.col("a.VERIFIED_DT_TM")),
            F.when(verifier.isNotNull(), stable_id("practitioner:mill", verifier)),
        ),
        _med_admin_status_entry(
            50, F.lit("order_status"), F.col("a.ORDER_STATUS_CD"),
            F.col("a.ORDER_STATUS_DISPLAY"), F.col("a.ORDER_STATUS_DT_TM"),
            F.lit(None).cast("string"),
        ),
        _med_admin_status_entry(
            60, F.lit("suspended"),
            F.when((F.col("a.SUSPEND_IND") == 1) | F.col("a.SUSPEND_EFFECTIVE_DT_TM").isNotNull(),
                   F.lit("suspended")),
            F.when((F.col("a.SUSPEND_IND") == 1) | F.col("a.SUSPEND_EFFECTIVE_DT_TM").isNotNull(),
                   F.lit("Suspended")),
            F.col("a.SUSPEND_EFFECTIVE_DT_TM"), F.lit(None).cast("string"),
        ),
        _med_admin_status_entry(
            70, F.lit("resumed"),
            F.when((F.col("a.RESUME_IND") == 1) | F.col("a.RESUME_EFFECTIVE_DT_TM").isNotNull(),
                   F.lit("resumed")),
            F.when((F.col("a.RESUME_IND") == 1) | F.col("a.RESUME_EFFECTIVE_DT_TM").isNotNull(),
                   F.lit("Resumed")),
            F.col("a.RESUME_EFFECTIVE_DT_TM"), F.lit(None).cast("string"),
        ),
        _med_admin_status_entry(
            80, F.lit("discontinued"),
            F.when((F.col("a.DISCONTINUE_IND") == 1)
                   | F.col("a.DISCONTINUE_EFFECTIVE_DT_TM").isNotNull(), F.lit("discontinued")),
            F.when((F.col("a.DISCONTINUE_IND") == 1)
                   | F.col("a.DISCONTINUE_EFFECTIVE_DT_TM").isNotNull(), F.lit("Discontinued")),
            F.col("a.DISCONTINUE_EFFECTIVE_DT_TM"), F.lit(None).cast("string"),
        ),
    ), lambda x: x["status_code"].isNotNull() | x["effective_datetime"].isNotNull())
    ended = F.col("a.CE_VALID_UNTIL_DT_TM").isNotNull() & (
        F.col("a.CE_VALID_UNTIL_DT_TM") < F.lit("2100-01-01").cast("timestamp")
    )
    superseded = ended | (F.coalesce(F.col("a.MR_IS_CURRENT_IND"), F.lit(True)) == F.lit(False))
    loaded_at = F.greatest(F.col("a.ADC_UPDT"), F.col("i.ingredient_loaded_at"))
    source_update = F.greatest(
        F.col("a.MAE_ADC_UPDT"), F.col("a.MR_ADC_UPDT"), F.col("a.ORDERS_ADC_UPDT"),
        F.col("a.OI_ADC_UPDT"), F.col("a.SYNONYM_ADC_UPDT"), F.col("a.LOOKUP_ADC_UPDT"),
        F.col("a.ADC_UPDT"), F.col("i.ingredient_source_update_timestamp"),
    )
    with_status = joined.withColumn("_status_history_array", status_entries)
    # contract v2: publish the administration EVENT_ID and native relationship identifiers while retaining SHA relationship keys explicitly
    return with_status.select(
        event_id.alias("patient_event_key"),
        F.col("a.EVENT_ID").cast("bigint").alias("event_id"),
        F.when(F.col("o._linked_order_id").isNotNull(), F.col("a.ORDER_ID").cast("bigint")).alias("order_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        F.col("a.PERSON_ID").cast("bigint").alias("person_id"),
        F.when(_present(F.col("a.PERSON_ID")), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.col("a.ENCNTR_ID").cast("bigint").alias("encounter_id"),
        F.coalesce(F.col("a.ADMIN_START_DT_TM"), F.col("a.PERFORMED_DT_TM"),
                   F.col("a.SCHEDULED_DT_TM")).alias("event_datetime"),
        F.col("a.ADMIN_END_DT_TM").alias("event_end_datetime"),
        F.lit("urn:cerner:order_synonym_id").alias("source_coding_system"),
        F.col("a.ORDER_SYNONYM_ID").cast("string").alias("source_code"),
        F.coalesce(F.col("a.ORDER_MNEMONIC"), F.col("a.ORDERED_AS_MNEMONIC"),
                   F.col("a.HNA_ORDER_MNEMONIC")).alias("source_display"),
        codeable_concept_json(
            coding_obj(F.lit("urn:cerner:order_synonym_id"), F.col("a.ORDER_SYNONYM_ID"),
                       F.coalesce(F.col("a.ORDER_MNEMONIC"), F.col("a.ORDERED_AS_MNEMONIC")), True),
            coding_obj(F.lit("http://www.nlm.nih.gov/research/umls/rxnorm"), F.col("a.RXNORM_CUI"),
                       F.col("a.RXNORM_STR"), False, "bronze.map_med_admin", None),
            coding_obj(F.lit("http://snomed.info/sct"),
                       F.coalesce(F.col("a.SNOMED_VALIDATED_CODE"), F.col("a.SNOMED_CODE"),
                                  F.col("a.LOOKUP_SNOMED_CODE")),
                       F.coalesce(F.col("a.SNOMED_VALIDATED_STR"), F.col("a.SNOMED_STR"),
                                  F.col("a.LOOKUP_SNOMED_FROM_OMOP")),
                       False, "bronze.map_med_admin", None),
            coding_obj(F.lit("urn:omop:concept_id"),
                       F.coalesce(F.col("a.OMOP_STANDARD_CONCEPT_ID"), F.col("a.OMOP_CONCEPT_ID")),
                       F.coalesce(F.col("a.OMOP_STANDARD_CONCEPT_NAME"), F.col("a.OMOP_CONCEPT_NAME")),
                       False, "bronze.map_med_admin", None),
        ).alias("_medication_code_json"),
        F.when(F.col("o._linked_order_id").isNotNull(),
               stable_id("medication_order:mill", F.col("a.ORDER_ID"))).alias("medication_order_key"),
        F.col("a.RESULT_STATUS_CD").cast("string").alias("administration_status_code"),
        F.col("a.RESULT_STATUS_DISPLAY").alias("administration_status_display"),
        F.col("a.EVENT_TYPE_CD").cast("string").alias("source_event_type_code"),
        F.col("a.EVENT_TYPE_DISPLAY").alias("source_event_type_display"),
        F.to_json(F.col("_status_history_array")).alias("_status_history_json"),
        F.size(F.col("_status_history_array")).cast("long").alias("status_history_count"),
        F.coalesce(F.col("a.ADMIN_DOSAGE"), F.col("a.DOSE_VALUE_EFFECTIVE"))
         .cast("decimal(38,10)").alias("dose_value"),
        F.coalesce(F.col("a.ADMIN_DOSAGE_UNIT_DISPLAY"), F.col("a.DOSE_UNIT_NORMALIZED"))
         .alias("dose_unit"),
        F.col("a.INITIAL_DOSAGE").cast("decimal(38,10)").alias("initial_dose_value"),
        F.col("a.INITIAL_DOSAGE_UNIT_DISPLAY").alias("initial_dose_unit"),
        F.col("a.DOSE_IN_MG").cast("decimal(38,10)").alias("dose_in_mg"),
        F.col("a.DOSE_IN_ML").cast("decimal(38,10)").alias("dose_in_ml"),
        F.col("a.DOSE_STANDARDIZATION_STATUS").alias("dose_standardization_status"),
        F.col("a.ADMIN_ROUTE_CD").cast("string").alias("route_code"),
        F.col("a.ADMIN_ROUTE_DISPLAY").alias("route_display"),
        F.col("a.ADMIN_SITE_CD").cast("string").alias("site_code"),
        F.col("a.ADMIN_SITE_DISPLAY").alias("site_display"),
        F.col("a.INFUSED_VOLUME").cast("decimal(38,10)").alias("infused_volume"),
        F.col("a.INFUSED_VOLUME_UNIT_DISPLAY").alias("infused_volume_unit"),
        F.col("a.INFUSION_RATE").cast("decimal(38,10)").alias("infusion_rate"),
        F.col("a.INFUSION_UNIT_DISPLAY").alias("infusion_rate_unit"),
        F.col("i.ingredients_json").alias("_ingredients_json"),
        F.coalesce(F.col("i.ingredient_count"), F.lit(0)).cast("long").alias("ingredient_count"),
        performer.cast("bigint").alias("performer_practitioner_id"),
        verifier.cast("bigint").alias("verifier_practitioner_id"),
        F.col("a.NURSE_UNIT_CD").cast("string").alias("location_code"),
        F.when(F.col("a.NURSE_UNIT_CD").isNotNull(),
               stable_id("location:mill:nurse_unit", F.col("a.NURSE_UNIT_CD"))).alias("location_key"),
        F.col("a.ORGANIZATION_ID").cast("bigint").alias("organization_id"),
        F.col("a.SCHEDULED_DT_TM").alias("scheduled_datetime"),
        F.coalesce(F.col("a.PERFORMED_DT_TM"), F.col("a.ADMIN_START_DT_TM"))
         .alias("performed_datetime"),
        F.coalesce(F.col("a.VERIFICATION_DT_TM"), F.col("a.VERIFIED_DT_TM"))
         .alias("verified_datetime"),
        F.col("a.ORDER_STATUS_CD").cast("string").alias("order_status_code"),
        F.col("a.ORDER_STATUS_DISPLAY").alias("order_status_display"),
        (F.col("a.PRN_IND") == 1).alias("prn_ind"), (F.col("a.IV_IND") == 1).alias("iv_ind"),
        F.when(superseded, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        F.coalesce(F.col("a.CE_VALID_FROM_DT_TM"), F.col("a.MR_VALID_FROM_DT_TM"),
                   F.col("a.ADMIN_START_DT_TM")).alias("record_status_effective_from"),
        F.when(superseded, F.coalesce(F.col("a.CE_VALID_UNTIL_DT_TM"),
                                     F.col("a.MR_VALID_UNTIL_DT_TM")))
         .alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("med_admin").alias("source_feed"),
        F.date_format(loaded_at, "yyyyMMddHHmmss").alias("load_batch_id"),
        source_update.alias("source_update_timestamp"),
        loaded_at.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_MED_ADMIN).alias("_source_table"),
        F.col("a.EVENT_ID").cast("string").alias("_source_row_id"),
        F.coalesce(F.col("a.SNOMED_VALIDATED_CODE"), F.col("a.SNOMED_CODE"),
                   F.col("a.LOOKUP_SNOMED_CODE")).cast("string").alias("_snomed_code"),
        F.coalesce(F.col("a.SNOMED_VALIDATED_STR"), F.col("a.SNOMED_STR"),
                   F.col("a.LOOKUP_SNOMED_FROM_OMOP")).alias("_snomed_display"),
        F.coalesce(F.col("a.OMOP_STANDARD_CONCEPT_ID"), F.col("a.OMOP_CONCEPT_ID"))
         .cast("string").alias("_omop_code"),
        F.coalesce(F.col("a.OMOP_STANDARD_CONCEPT_NAME"), F.col("a.OMOP_CONCEPT_NAME"))
         .alias("_omop_display"),
        F.col("a.RXNORM_CUI").cast("string").alias("_rxnorm_code"),
        F.col("a.RXNORM_STR").alias("_rxnorm_display"),
    )

@materialized_view(
    name=_n("journey_clinical._medication_admin_primitive"),
    private=True,
    comment="Internal primitive medication-administration join with deterministic JSON boundaries.",
    refresh_policy="incremental",
)
def _medication_admin_primitive():
    # Build the declared dataset: Internal primitive medication-administration join with
    # deterministic JSON boundaries.
    return _medication_admin_primitive_query()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  medication order primitive

# COMMAND ----------

def _medication_order_primitive_query():
    # Join orders to grouped actions, ingredients and details for the internal medication-order
    # stage.
    o = read_source(SRC_MEDICATION_ORDER).alias("o")
    actions = spark.read.table(_n("journey_clinical._med_order_action_grouped")).alias("a")
    ingredients = spark.read.table(_n("journey_clinical._med_order_ingredient_grouped")).alias("i")
    details = spark.read.table(_n("journey_clinical._med_order_detail_grouped")).alias("d")
    joined = (
        o.join(actions, F.col("o.ORDER_ID") == F.col("a.ORDER_ID"), "left")
         .join(ingredients, F.col("o.ORDER_ID") == F.col("i.ORDER_ID"), "left")
         .join(details, F.col("o.ORDER_ID") == F.col("d.ORDER_ID"), "left")
    )
    event_id = stable_id("medication_order:mill", F.col("o.ORDER_ID"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("o.PERSON_ID"))], SRC_MEDICATION_ORDER, F.col("o.ORDER_ID")
    )
    retracted = F.coalesce(F.col("o.SOURCE_PRESENT_IND"), F.lit(True)) == F.lit(False)
    superseded = (F.coalesce(F.col("o.INACTIVE_ORDER_FLAG"), F.lit(0)) == 1) | (
        F.coalesce(F.col("o.ACTIVE_IND"), F.lit(1)) == 0
    )
    loaded_at = F.greatest(F.col("o.ADC_UPDT"), F.col("a.action_loaded_at"),
                           F.col("i.ingredient_loaded_at"), F.col("d.detail_loaded_at"))
    source_update = F.greatest(
        F.col("o.SOURCE_ADC_UPDT"), F.col("o.ADC_UPDT"),
        F.col("a.action_source_update_timestamp"),
        F.col("i.ingredient_source_update_timestamp"),
        F.col("d.detail_source_update_timestamp"),
    )
    # contract v2: publish native medication ORDER_ID and native foreign keys while retaining the event SHA as patient_event_key
    return joined.select(
        event_id.alias("patient_event_key"),
        F.col("o.ORDER_ID").cast("bigint").alias("order_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        F.col("o.PERSON_ID").cast("bigint").alias("person_id"),
        F.when(_present(F.col("o.PERSON_ID")), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.col("o.ENCNTR_ID").cast("bigint").alias("encounter_id"),
        F.coalesce(F.col("o.ORIG_ORDER_DT_TM"), F.col("o.CURRENT_START_DT_TM"))
         .alias("event_datetime"),
        F.coalesce(F.col("o.PROJECTED_STOP_DT_TM"), F.col("o.SOFT_STOP_DT_TM"),
                   F.col("o.DISCONTINUE_EFFECTIVE_DT_TM")).alias("event_end_datetime"),
        F.lit("urn:cerner:synonym_id").alias("source_coding_system"),
        F.col("o.SYNONYM_ID").cast("string").alias("source_code"),
        F.coalesce(F.col("o.ORDER_MNEMONIC"), F.col("o.ORDERED_AS_MNEMONIC"),
                   F.col("o.HNA_ORDER_MNEMONIC")).alias("source_display"),
        codeable_concept_json(
            coding_obj(F.lit("urn:cerner:synonym_id"), F.col("o.SYNONYM_ID"),
                       F.coalesce(F.col("o.ORDER_MNEMONIC"), F.col("o.ORDERED_AS_MNEMONIC"),
                                  F.col("o.HNA_ORDER_MNEMONIC")), True),
            coding_obj(F.lit(SNOMED_URI), F.col("o.SNOMED_CODE"),
                       F.coalesce(F.col("o.OMOP_STANDARD_CONCEPT_NAME"), F.col("o.OMOP_CONCEPT_NAME")),
                       False, "bronze.map_medication_order", None),
            coding_obj(F.lit(OMOP_URI), F.col("o.OMOP_STANDARD_CONCEPT_ID"),
                       F.col("o.OMOP_STANDARD_CONCEPT_NAME"), False,
                       "bronze.map_medication_order", None),
        ).alias("_medication_code_json"),
        F.col("o.ORDER_STATUS_CD").cast("string").alias("order_status_code"),
        F.col("o.ORDER_STATUS_DESCRIPTION").alias("order_status_display"),
        F.col("o.DEPT_STATUS_CD").cast("string").alias("department_status_code"),
        F.col("o.DEPT_STATUS_DESCRIPTION").alias("department_status_display"),
        F.col("o.ACTIVE_STATUS_CD").cast("string").alias("active_status_code"),
        F.col("o.ACTIVE_STATUS_DESCRIPTION").alias("active_status_display"),
        F.lit("order").alias("intent_code"),
        F.col("o.MED_ORDER_TYPE_CD").cast("string").alias("medication_order_type_code"),
        F.col("o.MED_ORDER_TYPE_DESCRIPTION").alias("medication_order_type_display"),
        F.col("o.ORIG_ORDER_DT_TM").alias("authored_datetime"),
        F.col("o.CURRENT_START_DT_TM").alias("effective_start_datetime"),
        F.col("o.PROJECTED_STOP_DT_TM").alias("projected_stop_datetime"),
        F.col("o.DISCONTINUE_EFFECTIVE_DT_TM").alias("discontinued_datetime"),
        F.col("o.FREQUENCY_ID").cast("string").alias("frequency_id"),
        (F.col("o.PRN_IND") == 1).alias("prn_ind"), (F.col("o.IV_IND") == 1).alias("iv_ind"),
        (F.col("o.SUSPEND_IND") == 1).alias("suspend_ind"),
        (F.col("o.RESUME_IND") == 1).alias("resume_ind"),
        (F.col("o.DISCONTINUE_IND") == 1).alias("discontinue_ind"),
        F.col("o.LAST_UPDATE_PROVIDER_ID").cast("bigint").alias("requester_practitioner_id"),
        F.col("o.ORGANIZATION_ID").cast("bigint").alias("organization_id"),
        F.col("o.CLINICAL_DISPLAY_LINE").alias("clinical_display_line"),
        F.col("o.ORDER_DETAIL_DISPLAY_LINE").alias("order_detail_display_line"),
        F.col("a.status_history_json").alias("_status_history_json"),
        F.coalesce(F.col("a.status_history_count"), F.lit(0)).cast("long")
         .alias("status_history_count"),
        F.col("i.ingredients_json").alias("_ingredients_json"),
        F.coalesce(F.col("i.ingredient_count"), F.lit(0)).cast("long").alias("ingredient_count"),
        F.col("d.order_details_json").alias("_order_details_json"),
        F.coalesce(F.col("d.order_detail_count"), F.lit(0)).cast("long")
         .alias("order_detail_count"),
        F.when(retracted, F.lit("retracted")).when(superseded, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        F.col("o.ORIG_ORDER_DT_TM").alias("record_status_effective_from"),
        F.when(retracted, F.col("o.SOURCE_ABSENT_DETECTED_TS"))
         .when(superseded, F.coalesce(F.col("o.DISCONTINUE_EFFECTIVE_DT_TM"),
                                     F.col("o.STATUS_DT_TM"))).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("medication_order").alias("source_feed"),
        F.date_format(loaded_at, "yyyyMMddHHmmss").alias("load_batch_id"),
        source_update.alias("source_update_timestamp"),
        loaded_at.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"),
        F.lit(SRC_MEDICATION_ORDER).alias("_source_table"),
        F.col("o.ORDER_ID").cast("string").alias("_source_row_id"),
    )

@materialized_view(
    name=_n("journey_clinical._medication_order_primitive"),
    private=True,
    comment="Internal primitive medication-order join with deterministic JSON boundaries.",
    refresh_policy="incremental",
)
def _medication_order_primitive():
    # Build the declared dataset: Internal primitive medication-order join with deterministic
    # JSON boundaries.
    return _medication_order_primitive_query()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc medication admin

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_medication_admin"),
    comment="Private JSON bridge and Gold cross-rule flags for medication_admin.",
    refresh_policy="incremental",
)
def _qc_medication_admin():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # medication_admin.
    return _cross_qc_primitive(
        _medication_admin_canonical(),
        "medication_admin",
        {
            "ingredients": "_qc_ingredients_json",
            "medication_code": "_qc_medication_code_json",
            "status_history": "_qc_status_history_json",
        },
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / medication admin

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
MEDICATION_ADMIN_PUBLIC_COLUMNS = [
    'patient_event_key',
    'event_id',
    'order_id',
    'location_key',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'medication_code',
    'medication_order_key',
    'administration_status_code',
    'administration_status_display',
    'source_event_type_code',
    'source_event_type_display',
    'status_history_count',
    'dose_value',
    'dose_unit',
    'initial_dose_value',
    'initial_dose_unit',
    'dose_in_mg',
    'dose_in_ml',
    'dose_standardization_status',
    'route_code',
    'route_display',
    'site_code',
    'site_display',
    'infused_volume',
    'infused_volume_unit',
    'infusion_rate',
    'infusion_rate_unit',
    'ingredient_count',
    'performer_practitioner_id',
    'verifier_practitioner_id',
    'location_code',
    'organization_id',
    'scheduled_datetime',
    'performed_datetime',
    'verified_datetime',
    'order_status_code',
    'order_status_display',
    'prn_ind',
    'iv_ind',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

MEDICATION_ADMIN_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'event_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

MEDICATION_ADMIN_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "event_id": "Millennium medication-administration EVENT_ID; primary key of this table.",
    "order_id": "Millennium medication ORDER_ID when the linked order exists.",
    "location_key": "Deterministic SHA-256 nurse-unit key retained beside location_code.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Subject-key identifier system.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "event_datetime": "Administration start or performed time.",
    "event_end_datetime": "Administration end time.",
    "source_coding_system": "Source medication coding system.",
    "source_code": "Source order-synonym identifier.",
    "source_display": "Source medication display.",
    "medication_code": "Source and mapped medication codings.",
    "medication_order_key": "Deterministic SHA-256 key of the linked medication order.",
    "administration_status_code": "Current source result status code.",
    "administration_status_display": "Current source result status display.",
    "source_event_type_code": "Source medication event type code.",
    "source_event_type_display": "Source medication event type display.",
    "status_history_count": "Number of retained status milestones.",
    "dose_value": "Effective administered dose.",
    "dose_unit": "Administered dose unit.",
    "initial_dose_value": "Initially documented dose.",
    "initial_dose_unit": "Initially documented dose unit.",
    "dose_in_mg": "Bronze-standardized milligram dose.",
    "dose_in_ml": "Bronze-standardized millilitre dose.",
    "dose_standardization_status": "Bronze dose-standardization status.",
    "route_code": "Administration route code.",
    "route_display": "Administration route display.",
    "site_code": "Administration site code.",
    "site_display": "Administration site display.",
    "infused_volume": "The volume at any one point in time that remains in the IV Bag.",
    "infused_volume_unit": "Text description of the unit of measure for infused volume.",
    "infusion_rate": "For continuously administered medications, IV or IVP, the infusion rate and unit is used to capture the flow rate of the medication into the patient.",
    "infusion_rate_unit": "Text description of the unit of measure for volume or quantity of the medication.",
    "ingredient_count": "Number of retained ingredient rows.",
    "performer_practitioner_id": "Millennium personnel PERSON_ID for the administration performer.",
    "verifier_practitioner_id": "Millennium personnel PERSON_ID for the administration verifier.",
    "location_code": "Millennium NURSE_UNIT_CD for the administration location.",
    "organization_id": "Millennium ORGANIZATION_ID for the source organization.",
    "scheduled_datetime": "Scheduled administration time.",
    "performed_datetime": "Performed administration time.",
    "verified_datetime": "Date and time at which the medication administration event was verified.",
    "order_status_code": "Linked source order status code.",
    "order_status_display": "Linked source order status display.",
    "prn_ind": "True when source `PRN_IND` equals 1, indicating as-needed medication; other non-null values become false and null remains null.",
    "iv_ind": "Indicator showing whether the order is an intravenous (IV) medication order.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each medication admin record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each medication admin record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Registered source feed.",
    "record_status": "Derived administration-row status: superseded when map_med_admin.CE_VALID_UNTIL_DT_TM is before 2100-01-01 or MR_IS_CURRENT_IND is false; otherwise active. Null MR_IS_CURRENT_IND defaults true and a null clinical-event end alone does not supersede the row. This projection does not emit retracted or derive status from the administration result/workflow code.",
    "record_status_effective_from": "First non-null map_med_admin.CE_VALID_FROM_DT_TM, MR_VALID_FROM_DT_TM, then ADMIN_START_DT_TM. This is a source validity start with an administration-time fallback, not a uniform independent status-change timestamp; null when all three are missing.",
    "record_status_effective_to": "For a superseded administration, first non-null map_med_admin.CE_VALID_UNTIL_DT_TM then MR_VALID_UNTIL_DT_TM; null for active rows or when both are missing. The end selection does not remove far-future sentinels, so supersession caused by MR_IS_CURRENT_IND=false can retain an open-ended clinical-event timestamp.",
    "source_update_timestamp": "Greatest of map_med_admin.MAE_ADC_UPDT, MR_ADC_UPDT, ORDERS_ADC_UPDT, OI_ADC_UPDT, SYNONYM_ADC_UPDT, LOOKUP_ADC_UPDT and ADC_UPDT, plus the per-EVENT_ID maximum of greatest(UPDT_DT_TM, ADC_UPDT) from map_med_admin_ingredient. This combines contributing source/lookup/ingestion clocks, not a single application update time or Silver refresh time; null only when all contributing clocks are null.",
    "loaded_at": "Greatest of map_med_admin.ADC_UPDT and the maximum map_med_admin_ingredient.ADC_UPDT for the same EVENT_ID. The order-ID existence join contributes no separate load timestamp. This combines administration and ingredient ingestion provenance, not administration event time or Silver refresh time; null only when all contributing load clocks are null.",
}

@materialized_view(
    name=_n("journey_clinical.medication_admin"),
    comment="One Millennium medication-administration event with current state and ordered history.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=MEDICATION_ADMIN_COLUMN_COMMENTS,
)
def medication_admin():
    # Build the declared dataset: One Millennium medication-administration event with current
    # state and ordered history.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_medication_admin")),
        "medication_admin",
        {
            "medication_code": "_qc_medication_code_json",
        },
        MEDICATION_ADMIN_PUBLIC_COLUMNS,
        MEDICATION_ADMIN_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._medication_admin_metadata'),
    comment='Internal quality and batch metadata for clinical_medication_admin; same row grain as the research table. Join keys: patient_event_key, event_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def medication_admin_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_medication_admin; same row grain as the research table. Join keys:
    # patient_event_key, event_id.
    return _s3_component_metadata("journey_clinical.medication_admin", "journey_clinical._qc_medication_admin", MEDICATION_ADMIN_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc medication order

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_medication_order"),
    comment="Private JSON bridge and Gold cross-rule flags for medication_order.",
    refresh_policy="incremental",
)
def _qc_medication_order():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # medication_order.
    return _cross_qc_primitive(
        _medication_order_canonical(),
        "medication_order",
        {
            "ingredients": "_qc_ingredients_json",
            "medication_code": "_qc_medication_code_json",
            "order_details": "_qc_order_details_json",
            "status_history": "_qc_status_history_json",
        },
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / medication order

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
MEDICATION_ORDER_PUBLIC_COLUMNS = [
    'patient_event_key',
    'order_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'medication_code',
    'order_status_code',
    'order_status_display',
    'department_status_code',
    'department_status_display',
    'active_status_code',
    'active_status_display',
    'intent_code',
    'medication_order_type_code',
    'medication_order_type_display',
    'authored_datetime',
    'effective_start_datetime',
    'projected_stop_datetime',
    'discontinued_datetime',
    'frequency_id',
    'prn_ind',
    'iv_ind',
    'suspend_ind',
    'resume_ind',
    'discontinue_ind',
    'requester_practitioner_id',
    'organization_id',
    'clinical_display_line',
    'order_detail_display_line',
    'status_history_count',
    'ingredient_count',
    'order_detail_count',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

MEDICATION_ORDER_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'order_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

MEDICATION_ORDER_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "order_id": "Millennium ORDER_ID; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Subject-key identifier system.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "event_datetime": "Order authored or start time.",
    "event_end_datetime": "Projected stop or discontinue time.",
    "source_coding_system": "Source medication coding system.",
    "source_code": "Source medication synonym identifier.",
    "source_display": "Source medication display.",
    "medication_code": "Source medication CodeableConcept.",
    "order_status_code": "Current source order status code.",
    "order_status_display": "Current source order status display.",
    "department_status_code": "Department workflow status code.",
    "department_status_display": "Department workflow status display.",
    "active_status_code": "Source active-status code.",
    "active_status_display": "Source active-status display.",
    "intent_code": "Medication request intent.",
    "medication_order_type_code": "Source medication-order type code.",
    "medication_order_type_display": "Source medication-order type display.",
    "authored_datetime": "Original order time.",
    "effective_start_datetime": "Current effective start.",
    "projected_stop_datetime": "Projected stop time.",
    "discontinued_datetime": "Discontinue effective time.",
    "frequency_id": "Source frequency identifier.",
    "prn_ind": "True when source `PRN_IND` equals 1, indicating as-needed medication; other non-null values become false and null remains null.",
    "iv_ind": "S10 medication_order; retained as a source-faithful bronze input or Journey standard-block field.",
    "suspend_ind": "Source suspended indicator.",
    "resume_ind": "Source resumed indicator.",
    "discontinue_ind": "Source discontinued indicator.",
    "requester_practitioner_id": "Millennium personnel PERSON_ID for the requesting practitioner.",
    "organization_id": "Millennium ORGANIZATION_ID for the source organization.",
    "clinical_display_line": "Source clinical display line.",
    "order_detail_display_line": "Source order-detail display line.",
    "status_history_count": "Number of retained action rows.",
    "ingredient_count": "Number of retained ingredient rows.",
    "order_detail_count": "Number of retained detail rows.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each medication order record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each medication order record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Withheld identity indicator.",
    "source_feed": "Registered source feed.",
    "record_status": "Retracted when SOURCE_PRESENT_IND is false (null defaults true); otherwise superseded when INACTIVE_ORDER_FLAG equals 1 (null defaults 0) or ACTIVE_IND equals 0 (null defaults 1); otherwise active. Retraction takes priority. Clinical order status, discontinue flags and child-source presence do not directly determine this label.",
    "record_status_effective_from": "map_medication_order.ORIG_ORDER_DT_TM carried unchanged as the history start. Unlike event_datetime, it has no CURRENT_START_DT_TM fallback; this authored-order time is not an independently observed status-transition timestamp.",
    "record_status_effective_to": "SOURCE_ABSENT_DETECTED_TS for retracted orders, even if null; otherwise first non-null DISCONTINUE_EFFECTIVE_DT_TM then STATUS_DT_TM for superseded orders; otherwise null. There is no fallback between branches, no projected/soft-stop or ingestion fallback, and no sentinel range clamp.",
    "source_update_timestamp": "Greatest of the order's SOURCE_ADC_UPDT and ADC_UPDT plus each action, ingredient and detail group's maximum of greatest(SOURCE_ADC_UPDT, ORDER_SOURCE_ADC_UPDT, ADC_UPDT), grouped by ORDER_ID across all contributing rows. This mixes source and ingestion clocks; it is not solely a native/raw-source update timestamp or Silver refresh time.",
    "loaded_at": "Greatest of map_medication_order.ADC_UPDT and the per-ORDER_ID maximum ADC_UPDT from map_medication_order_action, map_medication_order_ingredient and map_medication_order_detail. All contributing child rows participate, not only the latest action; JSON decoding and quality/public projections preserve this combined ingestion clock.",
}

@materialized_view(
    name=_n("journey_clinical.medication_order"),
    comment="One Millennium medication order with current state and ordered action, ingredient, and detail evidence.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=MEDICATION_ORDER_COLUMN_COMMENTS,
)
def medication_order():
    # Build the declared dataset: One Millennium medication order with current state and ordered
    # action, ingredient, and detail evidence.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_medication_order")),
        "medication_order",
        {
            "medication_code": "_qc_medication_code_json",
        },
        MEDICATION_ORDER_PUBLIC_COLUMNS,
        MEDICATION_ORDER_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._medication_order_metadata'),
    comment='Internal quality and batch metadata for clinical_medication_order; same row grain as the research table. Join keys: patient_event_key, order_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def medication_order_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_medication_order; same row grain as the research table. Join keys:
    # patient_event_key, order_id.
    return _s3_component_metadata("journey_clinical.medication_order", "journey_clinical._qc_medication_order", MEDICATION_ORDER_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / medication order ingredient

# COMMAND ----------

# ==== journey_clinical.medication_order_ingredient ====

MEDICATION_ORDER_INGREDIENT_PUBLIC_COLUMNS = [
    "order_id", "action_sequence", "component_sequence", "component_id",
    "coding_system", "coding_code", "coding_display", "strength_value",
    "strength_unit", "volume_value", "volume_unit", "dose_value", "dose_unit",
    "rate_value", "rate_unit", "concentration_value", "concentration_unit",
    "ingredient_type_code", "clinically_significant_ind",
    "include_in_total_volume_ind", "freetext_dose", "source_present_ind",
    "loaded_at",
] + axis_columns("ingredient")

MEDICATION_ORDER_INGREDIENT_COLUMN_COMMENTS = {
    "order_id": "Millennium ORDER_ID of the parent medication order.",
    "action_sequence": "Order action sequence the ingredient row belongs to; zero when absent.",
    "component_sequence": "Ingredient component sequence within the action; zero when absent.",
    "component_id": "Source composite ORDER_ID:ACTION_SEQUENCE:COMP_SEQUENCE.",
    "coding_system": "Cerner synonym identifier namespace.",
    "coding_code": "Millennium SYNONYM_ID of the ingredient.",
    "coding_display": "Ordered-as, supplied-as, order, or HNA mnemonic; first available.",
    "strength_value": "Ingredient strength as ordered.",
    "strength_unit": "Strength unit description.",
    "volume_value": "Ingredient volume as ordered.",
    "volume_unit": "Volume unit description.",
    "dose_value": "Ordered dose, falling back to dose quantity.",
    "dose_unit": "Ordered-dose unit, falling back to dose-quantity unit.",
    "rate_value": "S10 medication_order medication/dosage; retained as a source-faithful bronze input or Journey standard-block field.",
    "rate_unit": "Normalized rate unit description.",
    "concentration_value": "Concentration as ordered.",
    "concentration_unit": "Concentration unit description.",
    "ingredient_type_code": "Millennium ingredient type flag.",
    "clinically_significant_ind": "Whether the source flags the ingredient as clinically significant.",
    "include_in_total_volume_ind": "Whether the ingredient counts toward total volume.",
    "freetext_dose": "Free-text dose when supplied.",
    "source_present_ind": "Whether the source row remains present.",
    "loaded_at": "Bronze load timestamp of the ingredient row.",
}

MEDICATION_ORDER_INGREDIENT_COLUMN_COMMENTS.update(axis_comments("ingredient", "medication-order ingredient"))

@materialized_view(
    name=_n("journey_clinical.medication_order_ingredient"),
    comment="One ordered ingredient component per medication order, linked to its parent by the retained event key.",
    column_comments=MEDICATION_ORDER_INGREDIENT_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def medication_order_ingredient():
    # Build the declared dataset: One ordered ingredient component per medication order, linked
    # to its parent by the retained event key.
    s = read_source(SRC_MEDICATION_ORDER_INGREDIENT)
    df = s.select(
        s.ORDER_ID.cast("bigint").alias("order_id"),
        F.coalesce(s.ACTION_SEQUENCE, F.lit(0)).cast("long").alias("action_sequence"),
        F.coalesce(s.COMP_SEQUENCE, F.lit(0)).cast("long").alias("component_sequence"),
        F.concat_ws(
            ":", s.ORDER_ID.cast("string"), s.ACTION_SEQUENCE.cast("string"),
            s.COMP_SEQUENCE.cast("string"),
        ).alias("component_id"),
        F.lit("urn:cerner:synonym_id").alias("coding_system"),
        s.SYNONYM_ID.cast("string").alias("coding_code"),
        F.coalesce(
            s.ORDERED_AS_MNEMONIC, s.SUPPLIED_AS_MNEMONIC,
            s.ORDER_MNEMONIC, s.HNA_ORDER_MNEMONIC,
        ).alias("coding_display"),
        s.STRENGTH.cast("decimal(38,10)").alias("strength_value"),
        s.STRENGTH_UNIT_DESCRIPTION.alias("strength_unit"),
        s.VOLUME.cast("decimal(38,10)").alias("volume_value"),
        s.VOLUME_UNIT_DESCRIPTION.alias("volume_unit"),
        F.coalesce(s.ORDERED_DOSE, s.DOSE_QUANTITY)
         .cast("decimal(38,10)").alias("dose_value"),
        F.coalesce(
            s.ORDERED_DOSE_UNIT_DESCRIPTION, s.DOSE_QUANTITY_UNIT_DESCRIPTION,
        ).alias("dose_unit"),
        s.NORMALIZED_RATE.cast("decimal(38,10)").alias("rate_value"),
        s.NORMALIZED_RATE_UNIT_DESCRIPTION.alias("rate_unit"),
        s.CONCENTRATION.cast("decimal(38,10)").alias("concentration_value"),
        s.CONCENTRATION_UNIT_DESCRIPTION.alias("concentration_unit"),
        s.INGREDIENT_TYPE_FLAG.cast("string").alias("ingredient_type_code"),
        (s.CLINICALLY_SIGNIFICANT_FLAG == 1).alias("clinically_significant_ind"),
        (s.INCLUDE_IN_TOTAL_VOLUME_FLAG == 1).alias("include_in_total_volume_ind"),
        s.FREETEXT_DOSE.alias("freetext_dose"),
        s.SOURCE_PRESENT_IND.alias("source_present_ind"),
        s.ADC_UPDT.alias("loaded_at"),
        s.MULTUM_CODE.cast("string").alias("multum_code"),
    )
    df = _s3_direct_axis(df, "ingredient", F.col("coding_system"), F.col("coding_code"), F.col("coding_display"))
    df = _s3_lookup_axis(df, "ingredient", SRC_S3C_RXNORM_MULTUM_MAP, "urn:cerner:multum:ingredient",
                         F.col("multum_code"), model_name="medication_order_ingredient", row_key="component_id")
    return df.select(*MEDICATION_ORDER_INGREDIENT_PUBLIC_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / medication order status

# COMMAND ----------

# ==== journey_clinical.medication_order_status ====

MEDICATION_ORDER_STATUS_PUBLIC_COLUMNS = [
    "order_id", "sequence", "status_kind", "status_code", "status_display",
    "effective_datetime", "practitioner_key", "source_action_sequence",
    "action_type_code", "action_type_display", "action_qualifier_code",
    "action_qualifier_display", "action_rejected_ind", "historical_feed_ind",
    "source_present_ind", "loaded_at",
]

MEDICATION_ORDER_STATUS_COLUMN_COMMENTS = {
    "order_id": "Millennium ORDER_ID of the parent medication order.",
    "sequence": "Deterministic action sequence, defaulted to zero when absent.",
    "status_kind": "Source action type display, falling back to its code.",
    "status_code": "Source order-status code at this action.",
    "status_display": "Source order-status display, falling back to action type.",
    "effective_datetime": "Action, effective, or order timestamp; first available.",
    "practitioner_key": "Deterministic practitioner key derived from ACTION_PERSONNEL_ID.",
    "source_action_sequence": "Verbatim source action sequence.",
    "action_type_code": "Source action-type code.",
    "action_type_display": "Source action-type display.",
    "action_qualifier_code": "Source action-qualifier code.",
    "action_qualifier_display": "Source action-qualifier display.",
    "action_rejected_ind": "Whether the source marks the action rejected.",
    "historical_feed_ind": "Whether the action came from the historical feed.",
    "source_present_ind": "Whether the source row remains present.",
    "loaded_at": "Bronze load timestamp of the action row.",
}

@materialized_view(
    name=_n("journey_clinical.medication_order_status"),
    comment="One ordered source action or status row per medication order.",
    column_comments=MEDICATION_ORDER_STATUS_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def medication_order_status():
    # Build the declared dataset: One ordered source action or status row per medication order.
    s = read_source(SRC_MEDICATION_ORDER_ACTION)
    return s.select(
        s.ORDER_ID.cast("bigint").alias("order_id"),
        F.coalesce(s.ACTION_SEQUENCE, F.lit(0)).cast("long").alias("sequence"),
        F.coalesce(s.ACTION_TYPE_DESCRIPTION, s.ACTION_TYPE_CD.cast("string"))
         .alias("status_kind"),
        s.ORDER_STATUS_CD.cast("string").alias("status_code"),
        F.coalesce(s.ORDER_STATUS_DESCRIPTION, s.ACTION_TYPE_DESCRIPTION)
         .alias("status_display"),
        F.coalesce(s.ACTION_DT_TM, s.EFFECTIVE_DT_TM, s.ORDER_DT_TM)
         .alias("effective_datetime"),
        F.when(
            s.ACTION_PERSONNEL_ID.isNotNull(),
            stable_id("practitioner:mill", s.ACTION_PERSONNEL_ID),
        ).alias("practitioner_key"),
        s.ACTION_SEQUENCE.cast("long").alias("source_action_sequence"),
        s.ACTION_TYPE_CD.cast("string").alias("action_type_code"),
        s.ACTION_TYPE_DESCRIPTION.alias("action_type_display"),
        s.ACTION_QUALIFIER_CD.cast("string").alias("action_qualifier_code"),
        s.ACTION_QUALIFIER_DESCRIPTION.alias("action_qualifier_display"),
        (s.ACTION_REJECTED_IND == 1).alias("action_rejected_ind"),
        s.HISTORICAL_FEED_IND.alias("historical_feed_ind"),
        s.SOURCE_PRESENT_IND.alias("source_present_ind"),
        s.ADC_UPDT.alias("loaded_at"),
    ).select(*MEDICATION_ORDER_STATUS_PUBLIC_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / medication order detail

# COMMAND ----------

# ==== journey_clinical.medication_order_detail ====

MEDICATION_ORDER_DETAIL_PUBLIC_COLUMNS = [
    "order_id", "action_sequence", "detail_sequence", "field_id",
    "field_meaning", "value_text", "value_number", "value_datetime",
    "history_contract", "parent_action_sequence", "last_action_sequence",
    "source_present_ind", "loaded_at",
]

MEDICATION_ORDER_DETAIL_COLUMN_COMMENTS = {
    "order_id": "Millennium ORDER_ID of the parent medication order.",
    "action_sequence": "Order action sequence the detail belongs to; zero when absent.",
    "detail_sequence": "Detail sequence within the action; zero when absent.",
    "field_id": "Millennium order-entry field identifier.",
    "field_meaning": "Source order-entry field meaning.",
    "value_text": "Extended display value, falling back to display value.",
    "value_number": "Numeric order-detail value when supplied.",
    "value_datetime": "Datetime order-detail value when supplied.",
    "history_contract": "Source detail-history contract marker.",
    "parent_action_sequence": "Source parent action sequence.",
    "last_action_sequence": "Source last action sequence.",
    "source_present_ind": "Whether the source row remains present.",
    "loaded_at": "Bronze load timestamp of the order-detail row.",
}

@materialized_view(
    name=_n("journey_clinical.medication_order_detail"),
    comment="One latest-action detail row per medication order field.",
    column_comments=MEDICATION_ORDER_DETAIL_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def medication_order_detail():
    # Build the declared dataset: One latest-action detail row per medication order field.
    s = read_source(SRC_MEDICATION_ORDER_DETAIL)
    return s.select(
        s.ORDER_ID.cast("bigint").alias("order_id"),
        F.coalesce(s.ACTION_SEQUENCE, F.lit(0)).cast("long").alias("action_sequence"),
        F.coalesce(s.DETAIL_SEQUENCE, F.lit(0)).cast("long").alias("detail_sequence"),
        s.OE_FIELD_ID.cast("string").alias("field_id"),
        s.OE_FIELD_MEANING.alias("field_meaning"),
        F.coalesce(s.OE_FIELD_DISPLAY_VALUE_EXTEND, s.OE_FIELD_DISPLAY_VALUE)
         .alias("value_text"),
        s.OE_FIELD_VALUE.cast("decimal(38,10)").alias("value_number"),
        s.OE_FIELD_DT_TM_VALUE.alias("value_datetime"),
        s.DETAIL_HISTORY_CONTRACT.alias("history_contract"),
        s.PARENT_ACTION_SEQUENCE.cast("long").alias("parent_action_sequence"),
        s.LAST_ACTION_SEQUENCE.cast("long").alias("last_action_sequence"),
        s.SOURCE_PRESENT_IND.alias("source_present_ind"),
        s.ADC_UPDT.alias("loaded_at"),
    ).select(*MEDICATION_ORDER_DETAIL_PUBLIC_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / medication admin ingredient

# COMMAND ----------

# ==== journey_clinical.medication_admin_ingredient ====

MEDICATION_ADMIN_INGREDIENT_PUBLIC_COLUMNS = [
    "event_id", "action_sequence", "component_sequence", "component_id",
    "coding_system", "coding_code", "coding_display", "strength_value",
    "strength_unit", "volume_value", "volume_unit", "dose_value", "dose_unit",
    "rate_value", "rate_unit", "concentration_value", "concentration_unit",
    "ingredient_type_code", "clinically_significant_ind",
    "include_in_total_volume_ind", "freetext_dose", "source_present_ind",
    "loaded_at",
] + axis_columns("ingredient")

MEDICATION_ADMIN_INGREDIENT_COLUMN_COMMENTS = {
    "event_id": "Millennium EVENT_ID of the parent medication administration.",
    "action_sequence": "Administration action sequence; zero when absent.",
    "component_sequence": "Ingredient component sequence within the action; zero when absent.",
    "component_id": "Source composite INGREDIENT_ORDER_ID:ACTION_SEQUENCE:COMP_SEQUENCE.",
    "coding_system": "Cerner synonym identifier namespace.",
    "coding_code": "Millennium SYNONYM_ID of the ingredient.",
    "coding_display": "Ordered-as, supplied-as, order, or HNA mnemonic; first available.",
    "strength_value": "Numeric strength value of the ingredient component, interpreted with STRENGTH_UNIT_CD.",
    "strength_unit": "Source strength-unit code.",
    "volume_value": "Numeric volume value for the ingredient component, interpreted with VOLUME_UNIT_CD.",
    "volume_unit": "Source volume-unit code.",
    "dose_value": "Ordered dose, falling back to dose quantity.",
    "dose_unit": "Ordered-dose unit code, falling back to dose-quantity unit code.",
    "rate_value": "Administration or infusion rate for this ingredient component normalised by the source system, interpreted with NORMALIZED_RATE_UNIT_CD.",
    "rate_unit": "Normalized rate-unit code.",
    "concentration_value": "Concentration value of this ingredient component within the order, interpreted with CONCENTRATION_UNIT_CD.",
    "concentration_unit": "Source concentration-unit code.",
    "ingredient_type_code": "Millennium ingredient type flag.",
    "clinically_significant_ind": "Whether the source flags the ingredient as clinically significant.",
    "include_in_total_volume_ind": "Whether the ingredient counts toward total volume.",
    "freetext_dose": "Free-text dose when supplied.",
    "source_present_ind": "True for rows retained by the current administration ingredient source.",
    "loaded_at": "Bronze load timestamp of the ingredient row.",
}

MEDICATION_ADMIN_INGREDIENT_COLUMN_COMMENTS.update(axis_comments("ingredient", "medication-administration ingredient"))

@materialized_view(
    name=_n("journey_clinical.medication_admin_ingredient"),
    comment="One ingredient component per medication administration, retaining source component order and its parent event key.",
    column_comments=MEDICATION_ADMIN_INGREDIENT_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def medication_admin_ingredient():
    # Contract v2 parent/child conservation: medication_admin publishes only the
    # administrations whose ORDER_SYNONYM_ID passes Gate B, so this child applies
    # the same gate. The eligible EVENT_IDs come from the parent's own primitive,
    # so the child cannot drift if the parent's rule changes. Without it the child
    # carried 7,435 rows over 2,989 events that no reader could reach through the
    # parent.
    # Build the declared dataset: One ingredient component per medication administration,
    # retaining source component order and its parent event key.
    admitted = (
        spark.read.table(_n("journey_clinical._medication_admin_primitive"))
        .where(_usable_code(F.col("source_code")))
        .select(F.col("event_id").alias("EVENT_ID"))
        .distinct()
    )
    s = read_source(SRC_MED_ADMIN_INGREDIENT).join(admitted, "EVENT_ID", "left_semi")
    df = s.select(
        s.EVENT_ID.cast("bigint").alias("event_id"),
        F.coalesce(s.ACTION_SEQUENCE, F.lit(0)).cast("long").alias("action_sequence"),
        F.coalesce(s.COMP_SEQUENCE, F.lit(0)).cast("long").alias("component_sequence"),
        F.concat_ws(
            ":", s.INGREDIENT_ORDER_ID.cast("string"),
            s.ACTION_SEQUENCE.cast("string"), s.COMP_SEQUENCE.cast("string"),
        ).alias("component_id"),
        F.lit("urn:cerner:synonym_id").alias("coding_system"),
        s.SYNONYM_ID.cast("string").alias("coding_code"),
        F.coalesce(
            s.ORDERED_AS_MNEMONIC, s.SUPPLIED_AS_MNEMONIC,
            s.ORDER_MNEMONIC, s.HNA_ORDER_MNEMONIC,
        ).alias("coding_display"),
        s.STRENGTH.cast("decimal(38,10)").alias("strength_value"),
        s.STRENGTH_UNIT_CD.cast("string").alias("strength_unit"),
        s.VOLUME.cast("decimal(38,10)").alias("volume_value"),
        s.VOLUME_UNIT_CD.cast("string").alias("volume_unit"),
        F.coalesce(s.ORDERED_DOSE, s.DOSE_QUANTITY)
         .cast("decimal(38,10)").alias("dose_value"),
        F.coalesce(s.ORDERED_DOSE_UNIT_CD, s.DOSE_QUANTITY_UNIT_CD)
         .cast("string").alias("dose_unit"),
        s.NORMALIZED_RATE.cast("decimal(38,10)").alias("rate_value"),
        s.NORMALIZED_RATE_UNIT_CD.cast("string").alias("rate_unit"),
        s.CONCENTRATION.cast("decimal(38,10)").alias("concentration_value"),
        s.CONCENTRATION_UNIT_CD.cast("string").alias("concentration_unit"),
        s.INGREDIENT_TYPE_FLAG.cast("string").alias("ingredient_type_code"),
        (s.CLINICALLY_SIGNIFICANT_FLAG == 1).alias("clinically_significant_ind"),
        (s.INCLUDE_IN_TOTAL_VOLUME_FLAG == 1).alias("include_in_total_volume_ind"),
        s.FREETEXT_DOSE.alias("freetext_dose"),
        F.lit(True).alias("source_present_ind"),
        s.ADC_UPDT.alias("loaded_at"),
        s.MULTUM_CODE.cast("string").alias("multum_code"),
    )
    df = _s3_direct_axis(df, "ingredient", F.col("coding_system"), F.col("coding_code"), F.col("coding_display"))
    df = _s3_lookup_axis(df, "ingredient", SRC_S3C_RXNORM_MULTUM_MAP, "urn:cerner:multum:ingredient",
                         F.col("multum_code"), model_name="medication_admin_ingredient", row_key="component_id")
    return df.select(*MEDICATION_ADMIN_INGREDIENT_PUBLIC_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / medication admin status

# COMMAND ----------

# ==== journey_clinical.medication_admin_status ====

MEDICATION_ADMIN_STATUS_PUBLIC_COLUMNS = [
    "event_id", "sequence", "status_kind", "status_code", "status_display",
    "effective_datetime", "practitioner_key", "source_action_sequence",
    "source_present_ind", "loaded_at",
]

MEDICATION_ADMIN_STATUS_COLUMN_COMMENTS = {
    "event_id": "Millennium EVENT_ID of the parent medication administration.",
    "sequence": "Deterministic status milestone sequence from the parent expression.",
    "status_kind": "Milestone kind such as scheduled, result, verified, or discontinued.",
    "status_code": "Source status code synthesized by the existing parent expression.",
    "status_display": "Source status display synthesized by the existing parent expression.",
    "effective_datetime": "Effective timestamp of the status milestone.",
    "practitioner_key": "Deterministic practitioner key when the milestone identifies personnel.",
    "source_action_sequence": "Source action sequence when supplied; null for synthesized milestones.",
    "source_present_ind": "True for status rows synthesized from a retained administration row.",
    "loaded_at": "Bronze load timestamp inherited from the parent source row.",
}

@materialized_view(
    name=_n("journey_clinical.medication_admin_status"),
    comment="One status milestone per medication administration, retaining source sequence and the parent event key.",
    column_comments=MEDICATION_ADMIN_STATUS_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def medication_admin_status():
    # Contract v2 parent/child conservation: medication_admin publishes only the
    # administrations whose ORDER_SYNONYM_ID passes Gate B, so this child applies
    # the same gate. Without it the child carried 11,722 rows over 3,020 events
    # that no reader could reach through the parent.
    # Build the declared dataset: One status milestone per medication administration, retaining
    # source sequence and the parent event key.
    parents = spark.read.table(
        _n("journey_clinical._medication_admin_primitive")
    ).where(_usable_code(F.col("source_code"))).select(
        "event_id", "_status_history_json", "loaded_at"
    )
    status_schema = (
        "array<struct<sequence:bigint,status_kind:string,status_code:string,"
        "status_display:string,effective_datetime:timestamp,practitioner_id:string,"
        "source_action_sequence:bigint,source_present_ind:boolean>>"
    )
    exploded = parents.select(
        "event_id", "loaded_at",
        F.explode(F.from_json(F.col("_status_history_json"), status_schema)).alias("_status"),
    )
    return exploded.select(
        "event_id",
        F.col("_status.sequence").alias("sequence"),
        F.col("_status.status_kind").alias("status_kind"),
        F.col("_status.status_code").alias("status_code"),
        F.col("_status.status_display").alias("status_display"),
        F.col("_status.effective_datetime").alias("effective_datetime"),
        F.col("_status.practitioner_id").alias("practitioner_key"),
        F.col("_status.source_action_sequence").alias("source_action_sequence"),
        F.col("_status.source_present_ind").alias("source_present_ind"),
        "loaded_at",
    ).select(*MEDICATION_ADMIN_STATUS_PUBLIC_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc medication dispense

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_medication_dispense"),
    comment="Private JSON bridge and Gold cross-rule flags for medication_dispense.",
    refresh_policy="incremental",
)
def _qc_medication_dispense():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # medication_dispense.
    return _cross_qc_primitive(
        _medication_dispense_canonical(),
        "medication_dispense",
        {"medication_code": "_qc_medication_code_json"},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / medication dispense

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
MEDICATION_DISPENSE_PUBLIC_COLUMNS = [
    'patient_event_key',
    'pharmacy_issue_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'medication_code',
    'status_code',
    'issue_type',
    'issue_category',
    'quantity',
    'quantity_unit',
    'issued_containers',
    'units_per_container',
    'drug_form',
    'drug_strength',
    'location_code',
    'issue_value_gbp',
    'source_transaction_identifier',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

MEDICATION_DISPENSE_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'pharmacy_issue_id',
    'identity_status',
    'event_before_birth',
    'load_batch_id',
]

MEDICATION_DISPENSE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "pharmacy_issue_id": "JAC PHARMACY_ISSUE_ID; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "encounter_id": "Nullable Millennium ENCNTR_ID; JAC does not supply encounter context.",
    "event_datetime": "Source issue timestamp.",
    "event_end_datetime": "Dispense end timestamp when supplied.",
    "source_coding_system": "Verbatim JAC drug coding system.",
    "source_code": "Verbatim JAC drug identifier.",
    "source_display": "Verbatim JAC drug description.",
    "medication_code": "Source JAC and mapped dm+d medication codings.",
    "status_code": "Source-presence-derived dispense status.",
    "issue_type": "Raw JAC issue type.",
    "issue_category": "Governed JAC transaction category; only ISSUE is typed here.",
    "quantity": "Best-effort parsed total units.",
    "quantity_unit": "Source dose-unit description.",
    "issued_containers": "Parsed source container count.",
    "units_per_container": "Parsed source units per container.",
    "drug_form": "Source drug form.",
    "drug_strength": "Source drug strength.",
    "location_code": "Millennium care-site code supplied by the JAC crosswalk.",
    "issue_value_gbp": "Source-recorded issue value including legitimate negative values outside this route.",
    "source_transaction_identifier": "Raw JAC dailyissues traceability key.",
    "confidentiality_code": "Source confidentiality code when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "source_feed": "Registered source feed owning the typed fact.",
    "record_status": "Retracted when map_pharmacy_issue.SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag does not retract the row. No superseded status is emitted. This is distinct from clinical status_code stopped/completed. Only normalized ISSUE-category rows with usable medication codes are eligible.",
    "record_status_effective_from": "Upstream mirror timestamp; snapshot-level, not a reliable row watermark.",
    "record_status_effective_to": "map_pharmacy_issue.ADC_UPDT only when SOURCE_PRESENT_IND is false; otherwise null. This is the input-load-time end proxy, not ISSUE_DTTM or a clinical dispensing completion time; a missing ADC_UPDT remains null.",
    "source_update_timestamp": "Upstream mirror timestamp; snapshot-level, not a reliable row watermark.",
    "loaded_at": "map_pharmacy_issue.ADC_UPDT carried unchanged through ISSUE-category and usable-code filtering and the QC/public projection. It is distinct from SOURCE_RECORD_UPDATED_DT used for the source-update/history-start fields and from ISSUE_DTTM. QC adds no parent clocks; Silver refresh time is not substituted.",
}

@materialized_view(
    name=_n("journey_clinical.medication_dispense"),
    comment="JAC ISSUE transactions admitted directly as typed dispense facts.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=MEDICATION_DISPENSE_COLUMN_COMMENTS,
)
def medication_dispense():
    # Build the declared dataset: JAC ISSUE transactions admitted directly as typed dispense
    # facts.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_medication_dispense")),
        "medication_dispense",
        {"medication_code": "_qc_medication_code_json"},
        MEDICATION_DISPENSE_PUBLIC_COLUMNS,
        MEDICATION_DISPENSE_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._medication_dispense_metadata'),
    comment='Internal quality and batch metadata for clinical_medication_dispense; same row grain as the research table. Join keys: patient_event_key, pharmacy_issue_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def medication_dispense_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_medication_dispense; same row grain as the research table. Join keys:
    # patient_event_key, pharmacy_issue_id.
    return _s3_component_metadata("journey_clinical.medication_dispense", "journey_clinical._qc_medication_dispense", MEDICATION_DISPENSE_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assertions and generic event routing

# COMMAND ----------

# ==== Assertions and generic event routing ====

# contract v2: retain the full canonical shape for internal reuse.
CLINICAL_FINDING_SOURCE_COLUMNS = [
    "patient_event_key",
    "event_id",
    "sequence_nbr",
    "source_object",
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
    "finding_code",
    "finding_kind",
    "value_text",
    "value_datetime",
    "value_code",
    "value_display",
    "normalcy_code",
    "normalcy_display",
    "result_status_code",
    "result_status_display",
    "order_id",
    "parent_event_id",
    "performer_practitioner_id",
    "verifier_practitioner_id",
    "organization_id",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
CLINICAL_FINDING_PUBLIC_COLUMNS = [
    'patient_event_key',
    'event_id',
    'sequence_nbr',
    'source_object',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'finding_code',
    'finding_kind',
    'value_text',
    'value_datetime',
    'value_code',
    'value_display',
    'normalcy_code',
    'normalcy_display',
    'result_status_code',
    'result_status_display',
    'order_id',
    'parent_event_id',
    'performer_practitioner_id',
    'verifier_practitioner_id',
    'organization_id',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
    'registry_field_id',
    'event_datetime_status',
]

CLINICAL_FINDING_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'event_id',
    'sequence_nbr',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

def _registry_finding_canonical():
    # Assemble normalized registry finding rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = spark.read.table(_n("journey_clinical._registry_observations")).where("route='finding'")
    return s.select(
        "patient_event_key", F.lit(None).cast("bigint").alias("event_id"), F.lit(1).cast("bigint").alias("sequence_nbr"),
        F.concat(F.lit("registry:"), F.col("registry_product")).alias("source_object"), "subject_key", "subject_id_system", "person_id",
        F.when(F.col("person_id").isNotNull(), F.lit("resolved")).otherwise(F.lit("unresolved")).alias("identity_status"),
        "encounter_id", "event_datetime", "event_end_datetime", F.lit("urn:barts:registry-field").alias("source_coding_system"),
        F.col("registry_field_id").alias("source_code"), F.col("question_display").alias("source_display"),
        codeable_concept(coding_obj(F.lit("urn:barts:registry-field"), F.col("registry_field_id"), F.col("question_display"), True)).alias("finding_code"),
        F.lit("registry_categorical").alias("finding_kind"), F.col("answer_text").alias("value_text"),
        F.lit(None).cast("timestamp").alias("value_datetime"), F.lit(None).cast("string").alias("value_code"),
        F.col("answer_text").alias("value_display"), F.lit(None).cast("string").alias("normalcy_code"),
        F.lit(None).cast("string").alias("normalcy_display"), F.lit(None).cast("string").alias("result_status_code"),
        F.lit(None).cast("string").alias("result_status_display"), F.lit(None).cast("bigint").alias("order_id"),
        F.lit(None).cast("bigint").alias("parent_event_id"), F.lit(None).cast("bigint").alias("performer_practitioner_id"),
        F.lit(None).cast("bigint").alias("verifier_practitioner_id"), F.lit(None).cast("bigint").alias("organization_id"),
        "record_status", "record_status_effective_from", "record_status_effective_to",
        F.lit(None).cast("string").alias("confidentiality_code"), F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"), F.lit("iweb_registry").alias("source_feed"),
        F.date_format("loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"), "source_update_timestamp", "loaded_at",
        "registry_field_id", "event_datetime_status",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc clinical finding

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_clinical_finding"),
    comment="Private JSON bridge and Gold cross-rule flags for clinical_finding.",
    refresh_policy="incremental",
)
def _qc_clinical_finding():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # clinical_finding.
    source = (
        _coded_finding_typed().select(*CLINICAL_FINDING_SOURCE_COLUMNS)
        .unionByName(_nomen_finding_typed().select(*CLINICAL_FINDING_SOURCE_COLUMNS))
        .unionByName(_date_finding_typed().select(*CLINICAL_FINDING_SOURCE_COLUMNS))
        .unionByName(_text_finding_typed().select(*CLINICAL_FINDING_SOURCE_COLUMNS))
        .unionByName(_registry_finding_canonical(), allowMissingColumns=True)
    )
    return _cross_qc_primitive(
        source,
        "clinical_finding",
        {"finding_code": "_qc_finding_code_json"},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / clinical finding

# COMMAND ----------

CLINICAL_FINDING_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "event_id": "Millennium EVENT_ID; native key for coded, nomenclature, date, and text event arms.",
    "sequence_nbr": "Sequence number that makes the coded result primary key unique when a single result spans more than one row.",
    "source_object": "Native source arm: coded_event, nomen_event, date_event, or text_event.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "event_datetime": "Clinically relevant finding timestamp.",
    "event_end_datetime": "Source effective end timestamp.",
    "source_coding_system": "Verbatim source event coding system.",
    "source_code": "Verbatim source event code.",
    "source_display": "Verbatim source event display.",
    "finding_code": "Source finding CodeableConcept.",
    "finding_kind": "Generic result representation carried by the source feed.",
    "value_text": "Verbatim text result or descriptor.",
    "value_datetime": "Date or date-time result value.",
    "value_code": "Source coded result value.",
    "value_display": "Source coded result display.",
    "normalcy_code": "Source normalcy or interpretation code.",
    "normalcy_display": "Source normalcy or interpretation display.",
    "result_status_code": "Source result-status code.",
    "result_status_display": "Source result-status display.",
    "order_id": "Source order identifier when supplied.",
    "parent_event_id": "Source parent-event identifier.",
    "performer_practitioner_id": "Millennium personnel PERSON_ID for the performer.",
    "verifier_practitioner_id": "Millennium personnel PERSON_ID for the verifier.",
    "organization_id": "Millennium ORGANIZATION_ID for the source organization.",
    "confidentiality_code": "Source confidentiality code.",
    "vip_ind": "Source VIP indicator.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "source_feed": "Registered generic-event feed.",
    "registry_field_id": "Governed SHA-256 registry product and field identifier for registry observations; null on native rows.",
    "event_datetime_status": "registry_dated or registry_undated for registry observations; null on native rows.",
    "record_status": "Coded-event rows are superseded only when the least CR_VALID_UNTIL_DT_TM and CE_VALID_UNTIL_DT_TM is non-null and before 2100-01-01; otherwise active, never retracted. Nomenclature, date and text rows are retracted for SOURCE_DELETED_IND cast to BOOLEAN true (null defaults false); otherwise superseded for the least result/event validity end being non-null and before 2100-01-01 or AUTHENTIC_FLAG cast to LONG zero (null defaults one); otherwise active. Retraction takes priority, and clinical result-status codes are separate.",
    "record_status_effective_from": "First non-null result-valid-from, event-valid-from, then clinical event time. Coded/nomenclature use CR_VALID_FROM_DT_TM and CE_VALID_FROM_DT_TM; date uses DATE_RESULT_VALID_FROM_DT_TM and CLINICAL_EVENT_VALID_FROM_DT_TM; text substitutes STRING_RESULT_VALID_FROM_DT_TM. Coded event time falls back through PERFORMED_DT_TM, EVENT_END_DT_TM, EVENT_START_DT_TM. Nomenclature uses CLINICAL_EVENT_DT_TM, EVENT_END_DT_TM, PERFORMED_DT_TM, EVENT_START_DT_TM. Date/text use RESULT_DT_TM, PERFORMED_DT_TM, EVENT_END_DT_TM, EVENT_START_DT_TM. No status-bound clamp or ingestion-time fallback is added.",
    "record_status_effective_to": "Coded rows use the least CR_VALID_UNTIL_DT_TM and CE_VALID_UNTIL_DT_TM only when superseded, otherwise null. Nomenclature, date and text rows use their computed source_update_timestamp when retracted; otherwise their least result/event validity end when superseded, otherwise null. Nomenclature uses CR/CE ends, date DATE_RESULT/CLINICAL_EVENT ends and text STRING_RESULT/CLINICAL_EVENT ends. An AUTHENTIC_FLAG-only supersession can therefore retain a null or future/sentinel end; no extra end clamp or fallback is applied.",
    "source_update_timestamp": "Per-arm greatest contributing clock, including own ADC_UPDT: coded adds SOURCE_ADC_UPDT, CR_UPDT_DT_TM and CE_UPDT_DT_TM; nomenclature adds SOURCE_CHANGE_TS and NOMENCLATURE_ADC_UPDT; date adds DATE_RESULT_EFFECTIVE_UPDT_DT_TM, CLINICAL_EVENT_ADC_UPDT and LOOKUP_ADC_UPDT; text adds STRING_RESULT_EFFECTIVE_UPDT_DT_TM, CLINICAL_EVENT_ADC_UPDT, LONG_TEXT_ADC_UPDT and LOOKUP_ADC_UPDT. Null inputs are ignored and all null gives null. These are not uniformly native application-update clocks, and the union takes no cross-arm maximum.",
    "loaded_at": "Own ADC_UPDT from map_coded_events, map_nomen_events, map_date_events or map_text_events, carried per arm through routing/usable-code filters, the union and QC/public projections. The additional result, clinical-event, lookup and long-text clocks used by source_update_timestamp are not added here; no current Silver refresh timestamp is substituted.",
}

@materialized_view(
    name=_n("journey_clinical.clinical_finding"),
    comment="Governed residual clinical observations from exhaustive generic-event routes.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=CLINICAL_FINDING_COLUMN_COMMENTS,
)
def clinical_finding():
    # Build the declared dataset: Governed residual clinical observations from exhaustive
    # generic-event routes.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_clinical_finding")),
        "clinical_finding",
        {"finding_code": "_qc_finding_code_json"},
        CLINICAL_FINDING_PUBLIC_COLUMNS,
        CLINICAL_FINDING_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._clinical_finding_metadata'),
    comment='Internal quality and batch metadata for clinical_clinical_finding; same row grain as the research table. Join keys: patient_event_key, source_object, event_id, sequence_nbr.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def clinical_finding_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_clinical_finding; same row grain as the research table. Join keys:
    # patient_event_key, source_object, event_id, sequence_nbr.
    return (spark.read.table(_n("journey_clinical._qc_clinical_finding"))).select(*CLINICAL_FINDING_LIFECYCLE_COLUMNS)

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
IMAGING_EXAM_SOURCE_COLUMNS = [
    "patient_event_key",
    "event_id",
    "pacs_examination_id",
    "organization_key",
    "source_object",
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
    "exam_code",
    "status_code",
    "accession_identifier",
    "study_instance_uid",
    "modality_code",
    "body_site_code",
    "report_patient_event_key",
    "requester_practitioner_id",
    "performer_practitioner_id",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
IMAGING_EXAM_PUBLIC_COLUMNS = [
    'patient_event_key',
    'event_id',
    'pacs_examination_id',
    'organization_key',
    'source_object',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'exam_code',
    'status_code',
    'accession_identifier',
    'study_instance_uid',
    'modality_code',
    'body_site_code',
    'report_patient_event_key',
    'requester_practitioner_id',
    'performer_practitioner_id',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

# PACS_INTEGRATION_SILVER_CLINICAL_V1
# PACS integration v1: accession, performed-evidence, report-link and request-text fields.
# accession_identifier keeps its meaning; sectra_accession_number is added beside it.
_at = IMAGING_EXAM_SOURCE_COLUMNS.index("record_status")
IMAGING_EXAM_SOURCE_COLUMNS[_at:_at] = IMAGING_INTEGRATION_COLUMNS
_at = IMAGING_EXAM_PUBLIC_COLUMNS.index("performer_practitioner_id") + 1
IMAGING_EXAM_PUBLIC_COLUMNS[_at:_at] = IMAGING_INTEGRATION_COLUMNS

IMAGING_EXAM_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'event_id',
    'pacs_examination_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc imaging exam

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_imaging_exam"),
    comment="Private JSON bridge and Gold cross-rule flags for imaging_exam.",
    refresh_policy="incremental",
)
def _qc_imaging_exam():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # imaging_exam.
    source = (
        _imaging_exam_canonical(integration=True).select(*IMAGING_EXAM_SOURCE_COLUMNS)
        .unionByName(
            _mill_radiology_exam_canonical(integration=True).select(*IMAGING_EXAM_SOURCE_COLUMNS)
        )
    )
    return _cross_qc_primitive(
        source,
        "imaging_exam",
        {"exam_code": "_qc_exam_code_json"},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / imaging exam

# COMMAND ----------

IMAGING_EXAM_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "event_id": "Millennium radiology EVENT_ID; native key for the millennium arm.",
    "pacs_examination_id": "PACS_EXAMINATION_ID; native key for the pacs arm.",
    "organization_key": "Nullable deterministic organization key; no organization expression is present in the imaging source cells.",
    "source_object": "Native source arm: millennium or pacs.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID when supplied; native encounter foreign key.",
    "event_datetime": "Imaging study start timestamp.",
    "event_end_datetime": "Imaging study end timestamp.",
    "source_coding_system": "Source exam coding system.",
    "source_code": "Source exam code",
    "source_display": "Source exam display.",
    "exam_code": "Source and mapped imaging-exam CodeableConcept.",
    "status_code": "Imaging study status.",
    "accession_identifier": "Imaging accession identifier, unchanged: PACS MILL_LINK_REF, Millennium REFERENCE_NBR. For the Barts/Sectra extraction accession use sectra_accession_number.",
    "study_instance_uid": "DICOM study instance UID.",
    "modality_code": "Imaging modality code.",
    "body_site_code": "Coded body part or anatomical region examined.",
    "report_patient_event_key": "Deterministic SHA-256 key of the linked report event.",
    "requester_practitioner_id": "Nullable Millennium personnel PERSON_ID for the requester.",
    "performer_practitioner_id": "Nullable Millennium personnel PERSON_ID for the performer.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each imaging exam record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each imaging exam record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "source_feed": "Registered PACS or Millennium radiology source feed.",
    "record_status": "PACS rows are retracted when map_pacs_examination.SOURCE_PRESENT_IND is false, otherwise active; missing presence does not retract. Millennium rows are superseded when map_radiology_event.IN_ERROR_IND is true, otherwise active; missing error defaults false. The arms are unioned without reconciling their labels; a Millennium validity end does not itself supersede a row.",
    "record_status_effective_from": "PACS: map_pacs_examination.SRC_ADC_UPDT. Millennium: map_radiology_event.VALID_FROM_DT_TM_CLEAN. Each arm carries its value through the union and QC/public projections; no cross-arm maximum or clinical-event timestamp is substituted.",
    "record_status_effective_to": "PACS: map_pacs_examination.ADC_UPDT only when source presence is false, otherwise null. Millennium: UPDT_DT_TM when IN_ERROR_IND is true, otherwise VALID_UNTIL_DT_TM_CLEAN directly, including a finite or sentinel end on an active row. No sentinel filtering or fallback between Millennium branches is applied.",
    "source_update_timestamp": "PACS: map_pacs_examination.SRC_ADC_UPDT. Millennium: map_radiology_event.UPDT_DT_TM. These distinct clocks are carried per arm through the union, not combined as a maximum. Neither joined report clocks nor the current Silver refresh time replaces them.",
    "loaded_at": "PACS: map_pacs_examination.ADC_UPDT. Millennium: map_radiology_event.PIPELINE_UPDT_DT_TM, not its ADC_UPDT. These are arm-specific load/processing clocks, carried unchanged through the union and QC/public projections; no report or cross-arm clock maximum is taken and the current Silver refresh time is not substituted.",
}

IMAGING_EXAM_COLUMN_COMMENTS.update(IMAGING_INTEGRATION_COLUMN_COMMENTS)

@materialized_view(
    name=_n("journey_clinical.imaging_exam"),
    comment="One PACS examination/study from the governed bronze landing with report linkage.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=IMAGING_EXAM_COLUMN_COMMENTS,
)
def imaging_exam():
    # Build the declared dataset: One PACS examination/study from the governed bronze landing
    # with report linkage.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_imaging_exam")),
        "imaging_exam",
        {"exam_code": "_qc_exam_code_json"},
        IMAGING_EXAM_PUBLIC_COLUMNS,
        IMAGING_EXAM_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._imaging_exam_metadata'),
    comment='Internal quality and batch metadata for clinical_imaging_exam; same row grain as the research table. Join keys: patient_event_key, source_object, event_id, pacs_examination_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def imaging_exam_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_imaging_exam;
    # same row grain as the research table. Join keys: patient_event_key, source_object,
    # event_id, pacs_examination_id.
    return (spark.read.table(_n("journey_clinical._qc_imaging_exam"))).select(*IMAGING_EXAM_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / imaging accession member and report link
# MAGIC Accession-level selection support (PACS integration v1). One member row per accession-bearing
# MAGIC PACS examination or Millennium examination event, with the default-selection eligibility
# MAGIC decision and its blocking reasons; one link row per examination/accession-to-report
# MAGIC relationship. Concepts are the imaging_exam S3 axis, mapped once there and joined here.

# COMMAND ----------

_EXAM_AXIS = axis_columns("exam", False)
_at = IMAGING_ACCESSION_MEMBER_COLUMNS.index("source_display") + 1
IMAGING_ACCESSION_MEMBER_PUBLIC_COLUMNS = (
    IMAGING_ACCESSION_MEMBER_COLUMNS[:_at] + _EXAM_AXIS + IMAGING_ACCESSION_MEMBER_COLUMNS[_at:]
)
IMAGING_ACCESSION_MEMBER_PUBLIC_COMMENTS = {
    **IMAGING_ACCESSION_MEMBER_COLUMN_COMMENTS,
    **axis_comments("exam", "imaging examination"),
}

@materialized_view(
    name=_n("journey_clinical.imaging_accession_member"),
    comment="One row per accession-bearing imaging examination member (PACS examination or Millennium examination event) with default-selection eligibility and blocking reasons. Group by accession_key for accession-level selection.",
    cluster_by=["accession_key"],
    refresh_policy="incremental",
    column_comments=IMAGING_ACCESSION_MEMBER_PUBLIC_COMMENTS,
)
def imaging_accession_member():
    # Build the declared dataset: accession members with eligibility, carrying the imaging_exam
    # mapped exam axis (NULL for examinations imaging_exam does not admit).
    concepts = spark.read.table(_n("journey_clinical.imaging_exam")).select(
        F.col("patient_event_key").alias("_concept_key"), *_EXAM_AXIS
    )
    members = _imaging_accession_member_canonical()
    return (
        members.join(concepts, members.patient_event_key == concepts._concept_key, "left")
        .select(*IMAGING_ACCESSION_MEMBER_PUBLIC_COLUMNS)
    )

@materialized_view(
    name=_n("journey_clinical.imaging_report_link"),
    comment="Relational imaging report links: examination-, request- and Cerner-event-scoped report documents with version decisions, text and approved-anonymisation availability. Multiple reports and addenda stay separate rows.",
    cluster_by=["accession_key"],
    refresh_policy="incremental",
    column_comments=IMAGING_REPORT_LINK_COLUMN_COMMENTS,
)
def imaging_report_link():
    # Build the declared dataset: imaging report links over silver text_document versions.
    return _imaging_report_link_canonical().select(*IMAGING_REPORT_LINK_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc allergy intolerance

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_allergy_intolerance"),
    comment="Private JSON bridge and Gold cross-rule flags for allergy_intolerance.",
    refresh_policy="incremental",
)
def _qc_allergy_intolerance():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # allergy_intolerance.
    return _cross_qc_primitive(
        _allergy_canonical(),
        "allergy_intolerance",
        {"substance_code": "_qc_substance_code_json"},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / allergy intolerance

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
ALLERGY_PUBLIC_COLUMNS = [
    'patient_event_key',
    'allergy_instance_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'substance_code',
    'substance_type_code',
    'substance_type_display',
    'reaction_class_code',
    'reaction_class_display',
    'reaction_status_code',
    'reaction_status_display',
    'severity_code',
    'severity_display',
    'absence_assertion_ind',
    'onset_precision_display',
    'source_of_info_display',
    'verified_status_flag',
    'reviewed_datetime',
    'cancel_reason_display',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

ALLERGY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'allergy_instance_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

ALLERGY_INTOLERANCE_COLUMN_COMMENTS = {
    "patient_event_key": "Stable silver-v2 key identifying the patient event associated with each allergy intolerance record. It is derived from bronze field `ALLERGY_INSTANCE_ID` in `4_prod.bronze.map_allergy`. This is a contract identifier rather than a display value; null means the source identifiers were insufficient to derive the key.",
    "allergy_instance_id": "Source-system identifier for the allergy instance associated with each allergy intolerance record. It is carried from bronze field `ALLERGY_INSTANCE_ID` in `4_prod.bronze.map_allergy`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "subject_key": "Always-populated deterministic subject key.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium person identifier when available.",
    "encounter_id": "Recording encounter reference when supplied.",
    "event_datetime": "Allergy onset timestamp from `ONSET_DT_TM_CLEAN`, falling back to `CREATED_DT_TM_CLEAN` when onset is absent; null when both cleaned timestamps are absent.",
    "event_end_datetime": "Reaction-status end when resolved or cancelled.",
    "source_coding_system": "Verbatim substance code system.",
    "source_code": "Substance code (SNOMED then source identifier then nomenclature id)",
    "source_display": "Verbatim substance display text.",
    "substance_code": "Source and mapped substance codings as a one-level CodeableConcept VARIANT.",
    "substance_type_code": "Source substance type code.",
    "substance_type_display": "Substance type (Drug/Food/Environment/...).",
    "reaction_class_code": "Source reaction class code.",
    "reaction_class_display": "Reaction class (Allergy/Intolerance/Side Effect/...).",
    "reaction_status_code": "Source reaction status code.",
    "reaction_status_display": "Reaction status (Active/Cancelled/Resolved/Proposed).",
    "severity_code": "Source severity code.",
    "severity_display": "Source allergy severity description carried from `SEVERITY_DESC`, for example mild, moderate or severe.",
    "absence_assertion_ind": "True when the row asserts ABSENCE of allergy (e.g. no known allergies) rather than a positive assertion.",
    "onset_precision_display": "Source onset precision.",
    "source_of_info_display": "Allergy information-source description from `SOURCE_OF_INFO_DESC`, falling back to the recorded free text in `SOURCE_OF_INFO_FT` when the description is null.",
    "verified_status_flag": "Pharmacy-verified flag verbatim.",
    "reviewed_datetime": "Sentinel-cleaned last review timestamp.",
    "cancel_reason_display": "Cancel reason when cancelled.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "record_status": "Retracted when upper(coalesce(REACTION_STATUS_DESC,'')) equals CANCELLED, without trimming; otherwise superseded when END_EFFECTIVE_DT_TM is before 2100-01-01 or ACTIVE_IND cast to LONG equals zero; otherwise active. A null end defaults to not-ended and a null activity flag defaults to 1. Cancellation takes priority, and other nonzero activity values do not trigger supersession.",
    "record_status_effective_from": "First non-null map_allergy.REACTION_STATUS_DT_TM_CLEAN, ACTIVE_STATUS_DT_TM, then BEG_EFFECTIVE_DT_TM. This source-status/validity timestamp is distinct from the onset/created fallbacks used for event_datetime; null when all three are missing.",
    "record_status_effective_to": "CANCEL_DT_TM_CLEAN for cancelled allergy rows, even if null; otherwise END_EFFECTIVE_DT_TM for ended/inactive rows; otherwise null. There is no fallback between those branches. Supersession caused by ACTIVE_IND=0 can retain a far-future end value because the selected end is not range-clamped.",
    "source_update_timestamp": "Bronze pipeline processing timestamp carried from `PIPELINE_UPDT_DT_TM` in `4_prod.bronze.map_allergy`. It records when bronze processed the source row, not a native clinical-system update timestamp, clinical event time, or the current Silver refresh time.",
    "loaded_at": "map_allergy.ADC_UPDT carried unchanged through usable-code filtering and quality/public projections. This is the contributing bronze load clock, distinct from PIPELINE_UPDT_DT_TM published as source_update_timestamp; it is not allergy onset, status-change time or Silver refresh time.",
}

@materialized_view(
    name=_n("journey_clinical.allergy_intolerance"),
    comment="One Millennium allergy or intolerance assertion, with recorded substance, reaction and source-history information.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=ALLERGY_INTOLERANCE_COLUMN_COMMENTS,
)
def allergy_intolerance():
    # Build the declared dataset: One Millennium allergy or intolerance assertion, with recorded
    # substance, reaction and source-history information.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_allergy_intolerance")),
        "allergy_intolerance",
        {"substance_code": "_qc_substance_code_json"},
        ALLERGY_PUBLIC_COLUMNS,
        ALLERGY_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._allergy_intolerance_metadata'),
    comment='Internal quality and batch metadata for clinical_allergy_intolerance; same row grain as the research table. Join keys: patient_event_key, allergy_instance_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def allergy_intolerance_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_allergy_intolerance; same row grain as the research table. Join keys:
    # patient_event_key, allergy_instance_id.
    return (spark.read.table(_n("journey_clinical._qc_allergy_intolerance"))).select(*ALLERGY_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / transfusion

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
TRANSFUSION_SOURCE_COLUMNS = [
    "patient_event_key",
    "transfusion_key",
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
    "begin_datetime",
    "end_datetime",
    "transfusion_status",
    "elapsed_minutes",
    "unit_number",
    "blood_product_group",
    "blood_unit_group",
    "patient_blood_group",
    "quantity_value",
    "quantity_raw",
    "begin_location",
    "end_location",
    "unit_is_irradiated",
    "unit_is_cmv_neg",
    "requires_irradiated",
    "requires_cmv_neg",
    "ambiguity_ind",
    "product_concept_id",
    "product_concept_name",
    "unit_group_concept_id",
    "patient_group_concept_id",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
TRANSFUSION_PUBLIC_COLUMNS = [
    'patient_event_key',
    'transfusion_key',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'begin_datetime',
    'end_datetime',
    'transfusion_status',
    'elapsed_minutes',
    'unit_number',
    'blood_product_group',
    'blood_unit_group',
    'patient_blood_group',
    'quantity_value',
    'quantity_raw',
    'begin_location',
    'end_location',
    'unit_is_irradiated',
    'unit_is_cmv_neg',
    'requires_irradiated',
    'requires_cmv_neg',
    'ambiguity_ind',
    'product_concept_id',
    'product_concept_name',
    'unit_group_concept_id',
    'patient_group_concept_id',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

TRANSFUSION_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'transfusion_key',
    'identity_status',
    'load_batch_id',
]

TRANSFUSION_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "transfusion_key": "BloodTrack TRANSFUSION_KEY; native primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "encounter_id": "Nullable Millennium ENCNTR_ID; BloodTrack transfusion rows do not supply encounter context.",
    "event_datetime": "Transfusion begin timestamp falling back to end.",
    "event_end_datetime": "Transfusion end timestamp.",
    "source_coding_system": "BloodTrack product coding system.",
    "source_code": "ISBT product code with product display fallback.",
    "source_display": "Blood product description.",
    "begin_datetime": "Paired transfusion begin timestamp.",
    "end_datetime": "Paired transfusion end timestamp.",
    "transfusion_status": "Pairing and clock-quality outcome.",
    "elapsed_minutes": "Elapsed transfusion minutes when calculable.",
    "unit_number": "Blood unit number.",
    "blood_product_group": "Normalized blood product group.",
    "blood_unit_group": "Blood group recorded on the unit.",
    "patient_blood_group": "Patient blood group at transfusion.",
    "quantity_value": "Parsed transfused quantity.",
    "quantity_raw": "Verbatim quantity text.",
    "begin_location": "Begin workflow location.",
    "end_location": "End workflow location.",
    "unit_is_irradiated": "Unit irradiation flag.",
    "unit_is_cmv_neg": "Unit CMV-negative flag.",
    "requires_irradiated": "Patient requires irradiated product.",
    "requires_cmv_neg": "Patient requires CMV-negative product.",
    "ambiguity_ind": "Pairing ambiguity indicator.",
    "product_concept_id": "Mapped SNOMED device concept identifier.",
    "product_concept_name": "Mapped product concept display.",
    "unit_group_concept_id": "Mapped unit blood-group concept identifier.",
    "patient_group_concept_id": "Mapped patient blood-group concept identifier.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "record_status": "Superseded when map_bloodtrack_transfusion.IS_CURRENT_IN_SOURCE is false, otherwise active; a missing flag defaults true. No retracted status is emitted. TRANSFUSION_STATUS and ambiguity indicators are separate, while usable-code filtering controls publication eligibility.",
    "record_status_effective_from": "First non-null map_bloodtrack_transfusion.BEGIN_TS then END_TS, also used as this row's clinical event time. This is a begin/end-time proxy, not an independently observed status transition; no event-time clamp is applied.",
    "record_status_effective_to": "map_bloodtrack_transfusion.PIPELINE_LOADED_AT when IS_CURRENT_IN_SOURCE is false; otherwise null. A missing processing timestamp remains null; neither BEGIN_TS nor END_TS is used as a fallback.",
    "source_update_timestamp": "Always null as a TIMESTAMP in the current transfusion projection. PIPELINE_LOADED_AT is published separately as loaded_at and is not copied into this field; no native update, clinical-event or Silver refresh timestamp is substituted.",
    "loaded_at": "Timestamp when the pipeline wrote this row to the bronze table.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_transfusion():
    # Assemble transfusion rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return _transfusion_canonical().select(*TRANSFUSION_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.transfusion"),
    comment="One paired BloodTrack unit-recipient transfusion episode, retaining available timing and source-event linkage.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=TRANSFUSION_COLUMN_COMMENTS,
)
def transfusion():
    # Build the declared dataset: One paired BloodTrack unit-recipient transfusion episode,
    # retaining available timing and source-event linkage.
    return _lifecycle_source_transfusion().select(*TRANSFUSION_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._transfusion_metadata'),
    comment='Internal quality and batch metadata for clinical_transfusion; same row grain as the research table. Join keys: patient_event_key, transfusion_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def transfusion_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_transfusion;
    # same row grain as the research table. Join keys: patient_event_key, transfusion_key.
    return (_lifecycle_source_transfusion()).select(*TRANSFUSION_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / transfusion event

# COMMAND ----------

SRC_BLOODTRACK_TRANSACTION = "4_prod.bronze.map_bloodtrack_transaction"

# contract v2: rename direct-publication scan-event and source-row comment keys
TRANSFUSION_EVENT_COLUMN_COMMENTS = {
    "transfusion_event_key": "Deterministic SHA-256 BloodTrack scan-event key.",
    "bloodtrack_transaction_key": "BloodTrack transaction key; native primary key of this table.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "event_datetime": "Effective transaction timestamp.",
    "workflow_step": "BloodTrack workflow step.",
    "transaction_success_ind": "Whether the transaction succeeded.",
    "unit_number": "Blood unit number.",
    "product_code": "Product code scanned.",
    "product_description": "Source BloodTrack textual description of the blood component product.",
    "bloodtrack_unit_id": "BloodTrack unit identifier.",
    "device_name": "Scanning device name.",
    "source_location": "Source workflow location name.",
    "linkage_status": "Patient linkage status.",
    "response_code": "Device or workflow response code.",
    "response_text": "Device or workflow response text.",
    "blood_unit_state": "Source BloodTrack status of the blood unit within the storage and issue workflow at the time of the transaction.",
    "blood_unit_fate": "Source BloodTrack final fate recorded for the blood unit, such as confirmed use or non-use.",
    "alert_present_ind": "Alert presence indicator.",
    "comment_present_ind": "Comment presence indicator.",
    "source_table": "Registered source table.",
    "source_row_key": "Verbatim BloodTrack transaction key retained as source-row provenance.",
    "record_status": "Superseded when map_bloodtrack_transaction.IS_CURRENT_IN_SOURCE is false, otherwise active; a missing flag defaults true. No retracted status is emitted. Workflow step, transaction success, response and blood-unit state/fate do not set this label.",
    "loaded_at": "map_bloodtrack_transaction.ADC_UPDT carried unchanged for the transaction row. No join to the transfusion aggregate contributes its PIPELINE_LOADED_AT; device/event times and the current Silver refresh time do not replace this value.",
}

TRANSFUSION_EVENT_LIFECYCLE_FIELDS = [
    'identity_status',
    'load_batch_id',
]

TRANSFUSION_EVENT_RETIRED_COLUMNS = [

]

TRANSFUSION_EVENT_LIFECYCLE_COLUMNS = [
    'transfusion_event_key',
    'bloodtrack_transaction_key',
    'identity_status',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_transfusion_event():
    # Assemble transfusion event rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    s = read_source(SRC_BLOODTRACK_TRANSACTION)
    event_id = stable_id("transfusion_event:bloodtrack", s.BLOODTRACK_TRANSACTION_KEY)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID),
         ("urn:bloodtrack:source-patient-number", s.SOURCE_PATIENT_NUMBER)],
        SRC_BLOODTRACK_TRANSACTION,
        s.BLOODTRACK_TRANSACTION_KEY,
    )
    inactive = ~F.coalesce(s.IS_CURRENT_IN_SOURCE, F.lit(True))
    # contract v2: publish the native BloodTrack transaction key and rename SHA/source-row identities explicitly
    return s.select(
        event_id.alias("transfusion_event_key"),
        s.BLOODTRACK_TRANSACTION_KEY.cast("string").alias("bloodtrack_transaction_key"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
        .when(_present(s.SOURCE_PATIENT_NUMBER), F.lit("provisional"))
        .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.coalesce(s.EVENT_TS_EFFECTIVE, s.SERVER_TRANSACTION_TS).alias("event_datetime"),
        s.WORKFLOW_STEP.alias("workflow_step"),
        s.TRANSACTION_SUCCESS_IND.alias("transaction_success_ind"),
        s.UNIT_NUMBER.alias("unit_number"), s.PRODUCT_CODE.alias("product_code"),
        s.PRODUCT_DESCRIPTION.alias("product_description"),
        s.BLOODTRACK_UNIT_ID.cast("string").alias("bloodtrack_unit_id"),
        s.DEVICE_NAME.alias("device_name"), s.SOURCE_LOCATION_NAME.alias("source_location"),
        s.LINKAGE_STATUS.alias("linkage_status"), s.RESPONSE_CODE.alias("response_code"),
        s.RESPONSE_TEXT.alias("response_text"), s.BLOOD_UNIT_STATE.alias("blood_unit_state"),
        s.BLOOD_UNIT_FATE.alias("blood_unit_fate"),
        s.ALERT_PRESENT_IND.alias("alert_present_ind"),
        s.COMMENT_PRESENT_IND.alias("comment_present_ind"),
        F.when(inactive, F.lit("superseded")).otherwise(F.lit("active"))
        .alias("record_status"),
        F.lit(SRC_BLOODTRACK_TRANSACTION).alias("source_table"),
        s.BLOODTRACK_TRANSACTION_KEY.cast("string").alias("source_row_key"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.ADC_UPDT.alias("loaded_at"),
    )

@materialized_view(
    name=_n("journey_clinical.transfusion_event"),
    comment="BloodTrack scan-grain workflow evidence including failed and safety-check attempts.",
    refresh_policy="incremental",
    column_comments=TRANSFUSION_EVENT_COLUMN_COMMENTS,
)
def transfusion_event():
    # Build the declared dataset: BloodTrack scan-grain workflow evidence including failed and
    # safety-check attempts.
    return _lifecycle_source_transfusion_event().drop(*TRANSFUSION_EVENT_LIFECYCLE_FIELDS, *TRANSFUSION_EVENT_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._transfusion_event_metadata'),
    comment='Internal quality and batch metadata for clinical_transfusion_event; same row grain as the research table. Join keys: transfusion_event_key, bloodtrack_transaction_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def transfusion_event_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_transfusion_event; same row grain as the research table. Join keys:
    # transfusion_event_key, bloodtrack_transaction_key.
    return (_lifecycle_source_transfusion_event()).select(*TRANSFUSION_EVENT_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc cancer treatment

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_cancer_treatment"),
    comment="Private JSON bridge and Gold cross-rule flags for cancer_treatment.",
    refresh_policy="incremental",
)
def _qc_cancer_treatment():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # cancer_treatment.
    return _cross_qc_primitive(
        _cancer_treatment_canonical(),
        "cancer_treatment",
        {"drug_code": "_qc_drug_code_json"},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / cancer treatment

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
CANCER_TREATMENT_PUBLIC_COLUMNS = [
    'patient_event_key',
    'treatment_key',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'drug_code',
    'treatment_plan',
    'regimen_name',
    'indication',
    'record_type',
    'dose_value',
    'dose_total',
    'dose_unit_code',
    'route_code',
    'start_date',
    'end_date',
    'final_treatment_date',
    'course_finished',
    'planned_cycles',
    'default_cycles',
    'chemo_radiation',
    'procurement_opcs_code',
    'delivery_opcs_code',
    'drug_similarity',
    'iqemo_course_id',
    'aria_rx_key',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

CANCER_TREATMENT_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'treatment_key',
    'identity_status',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

CANCER_TREATMENT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "treatment_key": "SACT TREATMENT_KEY; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Treatment start timestamp.",
    "event_end_datetime": "Treatment end or final-treatment timestamp.",
    "source_coding_system": "SACT drug-token coding system.",
    "source_code": "Normalized drug token with agent-name display fallback.",
    "source_display": "Source agent name.",
    "drug_code": "Source and mapped drug codings.",
    "treatment_plan": "Source treatment plan.",
    "regimen_name": "Treatment regimen name.",
    "indication": "Clinical indication text held against the iQemo regimen definition for which the regimen is used.",
    "record_type": "ARIA/iQemo linkage class.",
    "dose_value": "Constituent dose value.",
    "dose_total": "Total planned or delivered dose.",
    "dose_unit_code": "ARIA coded value for the dosage unit used for administration of the agent.",
    "route_code": "Administration route code.",
    "start_date": "Source start date.",
    "end_date": "Source end date.",
    "final_treatment_date": "Date of the final treatment administered within the iQemo chemotherapy course.",
    "course_finished": "Source indication that the treatment course has finished, from `CourseFinished` cast to boolean.",
    "planned_cycles": "Planned cycle count.",
    "default_cycles": "Default regimen cycle count.",
    "chemo_radiation": "Concurrent chemo-radiation indicator.",
    "procurement_opcs_code": "OPCS procurement code.",
    "delivery_opcs_code": "OPCS delivery code.",
    "drug_similarity": "Drug-link similarity score.",
    "iqemo_course_id": "iQemo course identifier.",
    "aria_rx_key": "ARIA prescription key.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "record_status": "Superseded when map_cancer_treatment.SOURCE_PRESENT_IND cast to BOOLEAN is false, otherwise active; missing presence defaults true. No retracted status is emitted. CourseFinished, record_type and treatment end dates do not set this row-history label.",
    "record_status_effective_from": "Start date of the treatment record, taken from the ARIA administration start date or the iQemo chemotherapy course start date.",
    "record_status_effective_to": "map_cancer_treatment.ADC_UPDT when source presence is false; otherwise null. Neither EndDate nor FinalTreatmentDate is substituted, and a missing ADC_UPDT remains null.",
    "source_update_timestamp": "Timestamp of the last update to the underlying ARIA or iQemo source record as recorded in the raw layer.",
    "loaded_at": "map_cancer_treatment.ADC_UPDT carried unchanged through usable-code filtering and QC/public projections. No joined parent or QC clock is added. This differs from SRC_ADC_UPDT used for source_update_timestamp and from the current Silver refresh time.",
}

@materialized_view(
    name=_n("journey_clinical.cancer_treatment"),
    comment="One SACT constituent-drug treatment fact, separate from Millennium medication administration.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=CANCER_TREATMENT_COLUMN_COMMENTS,
)
def cancer_treatment():
    # Build the declared dataset: One SACT constituent-drug treatment fact, separate from
    # Millennium medication administration.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_cancer_treatment")),
        "cancer_treatment",
        {"drug_code": "_qc_drug_code_json"},
        CANCER_TREATMENT_PUBLIC_COLUMNS,
        CANCER_TREATMENT_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._cancer_treatment_metadata'),
    comment='Internal quality and batch metadata for clinical_cancer_treatment; same row grain as the research table. Join keys: patient_event_key, treatment_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def cancer_treatment_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_cancer_treatment; same row grain as the research table. Join keys:
    # patient_event_key, treatment_key.
    return (spark.read.table(_n("journey_clinical._qc_cancer_treatment"))).select(*CANCER_TREATMENT_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / cancer treatment cycle

# COMMAND ----------

SRC_CANCER_TREATMENT_CYCLE = "4_prod.bronze.map_cancer_treatment_cycle"

CANCER_TREATMENT_CYCLE_COLUMN_COMMENTS = {
    "cancer_treatment_cycle_key": "Deterministic SHA-256 key for the iQemo course-cycle pair.",
    "iqemo_course_id": "iQemo chemotherapy course identifier.",
    "cycle_sequence_id": "Verbatim treatment-cycle sequence token.",
    "regimen_cycle_id": "Regimen cycle identifier.",
    "cycle_code": "Source cycle code.",
    "prescribed_datetime": "Sentinel-cleaned prescribed timestamp.",
    "pharmacy_confirmed_datetime": "Sentinel-cleaned pharmacy-confirmed timestamp.",
    "cycle_start_datetime": "Sentinel-cleaned cycle start timestamp.",
    "cancellation_datetime": "Sentinel-cleaned cancellation timestamp.",
    "cycle_status_code": "Source cycle status code.",
    "treatment_response_id": "Source treatment-response identifier from `TreatmentResponseID`, retained as a string on the treatment cycle.",
    "line_of_treatment": "Coded lookup identifier for the line of treatment represented by the parent chemotherapy course.",
    "regimen_number": "Sequential number identifying the regimen within the parent chemotherapy course.",
    "course_link_status": "Parent course-link status.",
    "outcome_comments": "Source outcome comments.",
    "source_table": "Registered source table.",
    "source_row_id": "Composite source row identifier.",
    "chemotherapy_course_id": "iQemo CHEMOTHERAPY_COURSE_ID as BIGINT; first component of the primary key.",
    "treatment_cycle_id": "iQemo TREATMENT_CYCLE_ID as BIGINT; second component of the primary key.",
    "record_status": "Always active in the current map_cancer_treatment_cycle projection. CycleStatus and CANCELLATION_DATE_CLEAN remain separate clinical fields and do not change this literal label; no superseded or retracted status is emitted.",
    "loaded_at": "map_cancer_treatment_cycle.PIPELINE_UPDT_DT_TM carried unchanged. No treatment-parent clock is joined, and ADC_UPDT, prescription dates, cancellation time and the current Silver refresh time are not substituted.",
}

CANCER_TREATMENT_CYCLE_LIFECYCLE_FIELDS = [
    'load_batch_id',
]

CANCER_TREATMENT_CYCLE_RETIRED_COLUMNS = [

]

CANCER_TREATMENT_CYCLE_LIFECYCLE_COLUMNS = [
    'cancer_treatment_cycle_key',
    'chemotherapy_course_id',
    'treatment_cycle_id',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_cancer_treatment_cycle():
    # Assemble cancer treatment cycle rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    s = read_source(SRC_CANCER_TREATMENT_CYCLE)
    row_key = F.concat_ws(":", s.CHEMOTHERAPY_COURSE_ID.cast("string"),
                          s.TREATMENT_CYCLE_ID.cast("string"))
    # contract v2: retain the composite SHA as cancer_treatment_cycle_key and publish the two native iQemo key components
    return s.select(
        stable_id("cancer_cycle:iqemo", s.CHEMOTHERAPY_COURSE_ID,
                  s.TREATMENT_CYCLE_ID).alias("cancer_treatment_cycle_key"),
        s.CHEMOTHERAPY_COURSE_ID.cast("bigint").alias("chemotherapy_course_id"),
        s.TREATMENT_CYCLE_ID.cast("bigint").alias("treatment_cycle_id"),
        s.CHEMOTHERAPY_COURSE_ID.cast("string").alias("iqemo_course_id"),
        s.TREATMENT_CYCLE_ID.cast("string").alias("cycle_sequence_id"),
        s.RegimenCycleID.cast("string").alias("regimen_cycle_id"),
        s.TreatmentCycleCode.alias("cycle_code"),
        s.PRESCRIBED_DATE_CLEAN.alias("prescribed_datetime"),
        s.PHARMACY_CONFIRMED_DATE_CLEAN.alias("pharmacy_confirmed_datetime"),
        s.START_DATE_CLEAN.alias("cycle_start_datetime"),
        s.CANCELLATION_DATE_CLEAN.alias("cancellation_datetime"),
        s.CycleStatus.cast("string").alias("cycle_status_code"),
        s.TreatmentResponseID.cast("string").alias("treatment_response_id"),
        s.LINE_OF_TREATMENT_ID.cast("string").alias("line_of_treatment"),
        s.REGIMEN_NUMBER.cast("string").alias("regimen_number"),
        s.MAP_CANCER_TREATMENT_LINK_STATUS.alias("course_link_status"),
        s.OutcomeComments.alias("outcome_comments"),
        F.lit("active").alias("record_status"),
        F.lit(SRC_CANCER_TREATMENT_CYCLE).alias("source_table"),
        row_key.alias("source_row_id"),
        F.date_format(s.PIPELINE_UPDT_DT_TM, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.PIPELINE_UPDT_DT_TM.alias("loaded_at"),
    )

@materialized_view(
    name=_n("journey_clinical.cancer_treatment_cycle"),
    comment="One iQemo chemotherapy cycle record linked to its parent treatment course, with recorded cycle timing.",
    refresh_policy="incremental",
    column_comments=CANCER_TREATMENT_CYCLE_COLUMN_COMMENTS,
)
def cancer_treatment_cycle():
    # Build the declared dataset: One iQemo chemotherapy cycle record linked to its parent
    # treatment course, with recorded cycle timing.
    return _lifecycle_source_cancer_treatment_cycle().drop(*CANCER_TREATMENT_CYCLE_LIFECYCLE_FIELDS, *CANCER_TREATMENT_CYCLE_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._cancer_treatment_cycle_metadata'),
    comment='Internal quality and batch metadata for clinical_cancer_treatment_cycle; same row grain as the research table. Join keys: cancer_treatment_cycle_key, chemotherapy_course_id, treatment_cycle_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def cancer_treatment_cycle_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_cancer_treatment_cycle; same row grain as the research table. Join keys:
    # cancer_treatment_cycle_key, chemotherapy_course_id, treatment_cycle_id.
    return (_lifecycle_source_cancer_treatment_cycle()).select(*CANCER_TREATMENT_CYCLE_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc condition stage

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_condition_stage"),
    comment="Private JSON bridge and Gold cross-rule flags for condition_stage.",
    refresh_policy="incremental",
)
def _qc_condition_stage():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # condition_stage.
    return _cross_qc_primitive(
        _condition_stage_canonical(),
        "condition_stage",
        {"stage_code": "_qc_stage_code_json"},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / condition stage

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
CONDITION_STAGE_PUBLIC_COLUMNS = [
    'patient_event_key',
    'aria_pt_id',
    'aria_dx_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'stage_code',
    'stage_of_disease',
    'stage_criteria',
    'dx_type',
    'dx_confirmed',
    'dx_method',
    'history_ind',
    'current_entry_ind',
    'cause_of_death_ind',
    'onset_datetime',
    'resolution_datetime',
    'clinical_description',
    'dx_comment',
    'person_link_status',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

CONDITION_STAGE_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'aria_pt_id',
    'aria_dx_id',
    'identity_status',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

CONDITION_STAGE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "aria_pt_id": "ARIA patient identifier; first component of the primary key.",
    "aria_dx_id": "ARIA diagnosis identifier; second component of the primary key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Sentinel-cleaned diagnosis onset timestamp.",
    "event_end_datetime": "Sentinel-cleaned resolution timestamp.",
    "source_coding_system": "ICD-10 coding system.",
    "source_code": "ICD diagnosis code with diagnosis-name fallback.",
    "source_display": "Diagnosis name or description.",
    "stage_code": "Source ICD and mapped OMOP diagnosis codings.",
    "stage_of_disease": "Source disease stage.",
    "stage_criteria": "Staging criteria description.",
    "dx_type": "Code indicating the type or category of the diagnosis entry.",
    "dx_confirmed": "Diagnosis confirmation state.",
    "dx_method": "Coded value recording the method by which the diagnosis was made.",
    "history_ind": "Indicator showing whether the entry represents a history of the condition rather than an active diagnosis.",
    "current_entry_ind": "True when the ARIA current-entry indicator `CUR_ENTRY_IND` is Y after uppercasing; null source values and other values become false.",
    "cause_of_death_ind": "True when the ARIA cause-of-death indicator `CS_OF_DTH_IND` is Y after uppercasing; null source values and other values become false.",
    "onset_datetime": "Source onset timestamp.",
    "resolution_datetime": "Source resolution timestamp.",
    "clinical_description": "Clinical diagnosis description.",
    "dx_comment": "Free-text comment recorded against the diagnosis entry.",
    "person_link_status": "Bronze person-link status.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "record_status": "Superseded when coalesce(CUR_ENTRY_IND,'Y') differs from the exact case-sensitive value Y, without trimming, or SOURCE_PRESENT_IND is false (null defaults true); otherwise active. No retracted status is emitted. This differs from current_entry_ind, which uppercases CUR_ENTRY_IND and defaults a missing value to N.",
    "record_status_effective_from": "map_aria_diagnosis_staging.EVLV_TSTAMP_CLEAN carried unchanged as the history start. It is the source-supplied cleaned timestamp, not a fallback to diagnosis onset, resolution or ingestion time; a missing value remains null.",
    "record_status_effective_to": "map_aria_diagnosis_staging.ADC_UPDT only when the row is superseded; otherwise null. This is an ingestion-time proxy, not RESOLUTION_DATE_CLEAN or an independently observed status-transition timestamp; a missing ADC_UPDT remains null.",
    "source_update_timestamp": "Bronze pipeline processing timestamp carried from `PIPELINE_UPDT_DT_TM` in `4_prod.bronze.map_aria_diagnosis_staging`. It records when bronze processed the source row, not a native clinical-system update timestamp, clinical event time, or the current Silver refresh time.",
    "loaded_at": "map_aria_diagnosis_staging.ADC_UPDT carried unchanged through code filtering and quality/public projections. This is the contributing bronze load clock, distinct from PIPELINE_UPDT_DT_TM published as source_update_timestamp; it is not disease onset/resolution or Silver refresh time.",
}

@materialized_view(
    name=_n("journey_clinical.condition_stage"),
    comment="One diagnosis-and-staging assertion from the frozen ARIA source, retaining its composite identifiers and staging details.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=CONDITION_STAGE_COLUMN_COMMENTS,
)
def condition_stage():
    # Build the declared dataset: One diagnosis-and-staging assertion from the frozen ARIA
    # source, retaining its composite identifiers and staging details.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_condition_stage")),
        "condition_stage",
        {"stage_code": "_qc_stage_code_json"},
        CONDITION_STAGE_PUBLIC_COLUMNS,
        CONDITION_STAGE_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._condition_stage_metadata'),
    comment='Internal quality and batch metadata for clinical_condition_stage; same row grain as the research table. Join keys: patient_event_key, aria_pt_id, aria_dx_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def condition_stage_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_condition_stage; same row grain as the research table. Join keys:
    # patient_event_key, aria_pt_id, aria_dx_id.
    return (spark.read.table(_n("journey_clinical._qc_condition_stage"))).select(*CONDITION_STAGE_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  endoscopy finding stage

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._endoscopy_finding_stage"),
    private=True,
    comment="Incremental boundary for joined Endobase finding scalars and deterministic coding JSON.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
)
def _endoscopy_finding_stage():
    # Build the declared dataset: Incremental boundary for joined Endobase finding scalars and
    # deterministic coding JSON.
    return _endoscopy_finding_canonical()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / endoscopy finding

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
ENDOSCOPY_FINDING_SOURCE_COLUMNS = [
    "patient_event_key",
    "endobase_exam_term_id",
    "endobase_exam_id",
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
    "finding_code",
    "endoscopy_exam_event_key",
    "section_id",
    "subsection_id",
    "parent_term_id",
    "display_order",
    "confirmed_ind",
    "text_changed_ind",
    "free_text_ind",
    "term_mapping_status",
    "person_link_status",
    "authored_datetime",
    "event_time_source",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
ENDOSCOPY_FINDING_PUBLIC_COLUMNS = [
    'patient_event_key',
    'endobase_exam_term_id',
    'endobase_exam_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'finding_code',
    'endoscopy_exam_event_key',
    'section_id',
    'subsection_id',
    'parent_term_id',
    'display_order',
    'confirmed_ind',
    'text_changed_ind',
    'free_text_ind',
    'term_mapping_status',
    'person_link_status',
    'authored_datetime',
    'event_time_source',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

ENDOSCOPY_FINDING_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'endobase_exam_term_id',
    'identity_status',
    'load_batch_id',
]

ENDOSCOPY_FINDING_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "endobase_exam_term_id": "Endobase ENDOBASE_EXAM_TERM_ID; primary key of this table.",
    "endobase_exam_id": "Endobase ENDOBASE_EXAM_ID; native parent examination identifier.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Parent exam time",
    "event_end_datetime": "Finding end timestamp when supplied.",
    "source_coding_system": "DGVS term coding system.",
    "source_code": "DGVS term identifier with term-text fallback.",
    "source_display": "Endobase term text.",
    "finding_code": "Source DGVS and mapped SNOMED/OMOP codings.",
    "endoscopy_exam_event_key": "Deterministic SHA-256 key for the parent Endobase examination.",
    "section_id": "Report section identifier.",
    "subsection_id": "Report subsection identifier.",
    "parent_term_id": "Parent term identifier.",
    "display_order": "Source display order.",
    "confirmed_ind": "Finding confirmation indicator.",
    "text_changed_ind": "Source text-changed indicator.",
    "free_text_ind": "Free-text route indicator.",
    "term_mapping_status": "C4 term-mapping status.",
    "person_link_status": "Bronze person-link status.",
    "authored_datetime": "Creation timestamp of the EndoBase examination-term record, carried from `CREATED_TS`; not a report-level authoring timestamp.",
    "event_time_source": "Whether event time came from the exam or authored fallback.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "record_status": "Superseded when map_endobase_exam_term.SOURCE_PRESENT_IND is false, otherwise active; missing presence defaults true. No retracted status is emitted. The non-free-text and usable-code filters control eligibility separately, while parent-exam fields do not determine this label.",
    "record_status_effective_from": "map_endobase_exam_term.CREATED_TS carried directly as the record-status start. This differs from clinical event time, which can use the joined parent exam's performed/exam/start timestamps before falling back to term creation time.",
    "record_status_effective_to": "map_endobase_exam_term.ADC_UPDT when its source-presence flag is false; otherwise null. The parent-exam load clock and clinical event time are not used as status ends, even when they contribute to other fields.",
    "source_update_timestamp": "map_endobase_exam_term.ADC_UPDT carried unchanged. The parent-exam ADC_UPDT contributes to loaded_at only, not this field; no independent native application-update clock or current Silver refresh timestamp is substituted.",
    "loaded_at": "Greatest of map_endobase_exam_term.ADC_UPDT and the left-joined map_endobase_exam.ADC_UPDT, with missing parent time first replaced by term time. Parent lookup uses ENDOBASE_EXAM_ID. A non-null parent time survives a null term time; both null gives null. This can differ from source_update_timestamp and the status end, which use only the term clock; it is not clinical event time or the current Silver refresh time.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_endoscopy_finding():
    # Assemble endoscopy finding rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    return (spark.read.table(_n("journey_clinical._endoscopy_finding_stage"))
            .withColumn("finding_code", F.parse_json(F.col("_finding_code_json")))
            .select(*ENDOSCOPY_FINDING_SOURCE_COLUMNS))

@materialized_view(
    name=_n("journey_clinical.endoscopy_finding"),
    comment="One coded Endobase report term with parent-exam clinical time.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=ENDOSCOPY_FINDING_COLUMN_COMMENTS,
)
def endoscopy_finding():
    # Build the declared dataset: One coded Endobase report term with parent-exam clinical time.
    df = (_lifecycle_source_endoscopy_finding()
          .withColumn("source_coding_system", canonical_coding_system(F.col("source_coding_system")))
          .withColumn("_finding_code_json", F.to_json(F.col("finding_code"))))
    df = _s3_flatten_codeable_json(df, "_finding_code_json", "finding", False)
    df = _s3_lookup_axis(
        df, "finding", SRC_S3_ENDOBASE_TERM_MAP, "urn:barts:endobase:dgvs-term",
        F.concat_ws("|", F.col("source_code"),
                    F.lower(F.trim(F.regexp_replace(F.col("source_display"), r"\s+", " ")))))
    return df.select(*ENDOSCOPY_FINDING_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._endoscopy_finding_metadata'),
    comment='Internal quality and batch metadata for clinical_endoscopy_finding; same row grain as the research table. Join keys: patient_event_key, endobase_exam_term_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def endoscopy_finding_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_endoscopy_finding; same row grain as the research table. Join keys:
    # patient_event_key, endobase_exam_term_id.
    return (_lifecycle_source_endoscopy_finding()).select(*ENDOSCOPY_FINDING_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / critical care period

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
CRITICAL_CARE_PERIOD_SOURCE_COLUMNS = [
    "patient_event_key",
    'crit_care_period_id',
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
    "period_end_datetime",
    "period_business_key",
    "no_current_version_ind",
    "business_key_status",
    "source_valid_ind",
    "care_type",
    "unit_function",
    "unit_id",
    "cds_source_system",
    "level2_days",
    "level3_days",
    "organ_systems_supported",
    "gestation_length",
    "discharge_status",
    "discharge_destination",
    "source_encounter_id",
    "cc_encounter_key",
    "cc_encounter_id",
    "cds_apc_id",
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
CRITICAL_CARE_PERIOD_PUBLIC_COLUMNS = [
    'patient_event_key',
    'crit_care_period_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'period_end_datetime',
    'period_business_key',
    'no_current_version_ind',
    'business_key_status',
    'source_valid_ind',
    'care_type',
    'unit_function',
    'unit_id',
    'cds_source_system',
    'level2_days',
    'level3_days',
    'organ_systems_supported',
    'gestation_length',
    'discharge_status',
    'discharge_destination',
    'source_encounter_id',
    'cc_encounter_key',
    'cc_encounter_id',
    'cds_apc_id',
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

CRITICAL_CARE_PERIOD_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'period_business_key',
    'identity_status',
    'load_batch_id',
]

CRITICAL_CARE_PERIOD_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "crit_care_period_id": "Source Crit_Care_Period_Id of the selected CCMDS revision, exposed as BIGINT. Identifies the representative source row chosen for this business-period record; period_business_key and patient_event_key retain the existing group and event linkage.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace used to interpret the subject identifier on this record for each critical care period record. It is derived from bronze field `PERSON_ID` in `4_prod.bronze.map_critical_care_period`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date and time when the represented clinical or administrative event occurred for each critical care period record. It is derived from bronze field `CC_Period_Start_Dt_Tm_CLEAN` in `4_prod.bronze.map_critical_care_period`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each critical care period record. It is derived from bronze field `CC_Period_Disch_Dt_Tm_CLEAN` in `4_prod.bronze.map_critical_care_period`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each critical care period record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Code supplied by the originating source system for the represented concept for each critical care period record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_display": "Human-readable label supplied by the source system for the source code for each critical care period record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "period_end_datetime": "Date and time associated with period end for the critical care period record. It is derived from bronze field `CC_Period_Disch_Dt_Tm_CLEAN` in `4_prod.bronze.map_critical_care_period`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "period_business_key": "Bronze-generated 64-bit xxhash64 of CC_Period_Local_Id and normalized MRN. Identifies the business-key group, not a native source revision; consult business_key_status for ambiguous or missing-MRN groups.",
    "no_current_version_ind": "Indicator of whether no current version applies to the critical care period record. It is derived from bronze field `CURRENT_IND` in `4_prod.bronze.map_critical_care_period`. Null means the source did not state the indicator and must not be interpreted as false.",
    "business_key_status": "Processing or clinical status of business key for the critical care period record. It is carried from bronze field `BUSINESS_KEY_STATUS` in `4_prod.bronze.map_critical_care_period`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_valid_ind": "Flag indicating whether the critical care period record passed source validation checks.",
    "care_type": "Textual description of the meaning of the NHS Data Dictionary code value for the element.",
    "unit_function": "Textual description of the meaning of the NHS Data Dictionary code value for the element.",
    "unit_id": "Identifier of the critical care unit to which the period of care relates.",
    "cds_source_system": "Code identifying the source system or organisation that supplied the critical care period record.",
    "level2_days": "Number of calendar days of level 2 critical care delivered during the critical care period.",
    "level3_days": "Number of days within the critical care period during which the patient received level 3 (intensive care) support.",
    "organ_systems_supported": "Maximum number of organ systems supported at any one time during the critical care period.",
    "gestation_length": "Recorded gestation length in completed weeks for the patient where relevant to the critical care period.",
    "discharge_status": "Textual description of the meaning of the NHS Data Dictionary code value for the element.",
    "discharge_destination": "Textual description of the meaning of the NHS Data Dictionary code value for the element.",
    "source_encounter_id": "Source-system identifier for the source encounter associated with each critical care period record. It is derived from bronze field `ENCNTR_ID` in `4_prod.bronze.map_critical_care_period`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "cc_encounter_key": "Deterministic SHA-256 key for the linked critical-care encounter.",
    "cc_encounter_id": "Native Millennium CC_ENCNTR_ID as BIGINT; joins to spine_encounter.encounter_id.",
    "cds_apc_id": "Identifier linking the critical care period to the associated Commissioning Data Set admitted patient care record.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each critical care period record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each critical care period record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each critical care period record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each critical care period record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each critical care period record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "For the selected CCMDS period revision, retracted when SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag defaults true. No superseded status is emitted. CURRENT_IND influences representative-row selection but does not itself determine this status; no_current_version_ind and source_valid_ind are separate.",
    "record_status_effective_from": "CC_Period_Start_Dt_Tm_CLEAN of the selected map_critical_care_period revision, carried unchanged. This care-period start is a history proxy, not an independently observed status change; no Record_Updated_Dt or ingestion-time fallback is applied.",
    "record_status_effective_to": "ADC_UPDT of the selected map_critical_care_period revision only when it is retracted; otherwise null. This is an ingestion-time end proxy, not CC_Period_Disch_Dt_Tm_CLEAN or an independent status-transition timestamp.",
    "source_update_timestamp": "Bronze pipeline processing timestamp carried from `PIPELINE_UPDT_DT_TM` in `4_prod.bronze.map_critical_care_period`. It records when bronze processed the source row, not a native clinical-system update timestamp, clinical event time, or the current Silver refresh time.",
    "loaded_at": "ADC_UPDT of the selected map_critical_care_period revision, not the maximum load time across the business-period group. Selection maximizes the struct of CURRENT_IND cast to INT (null 0), Record_Updated_Dt cast to TIMESTAMP (null 1900-01-01), then Crit_Care_Period_Id (null -1), within PERIOD_BUSINESS_KEY. The chosen row's clock is carried unchanged; it is distinct from PIPELINE_UPDT_DT_TM and Silver refresh time.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_critical_care_period():
    # Assemble critical care period rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _critical_care_period_canonical().select(*CRITICAL_CARE_PERIOD_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.critical_care_period"),
    comment="One critical-care period per CCMDS business key; version history stays in bronze.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=CRITICAL_CARE_PERIOD_COLUMN_COMMENTS,
)
def critical_care_period():
    # Build the declared dataset: One critical-care period per CCMDS business key; version
    # history stays in bronze.
    return _lifecycle_source_critical_care_period().select(*CRITICAL_CARE_PERIOD_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._critical_care_period_metadata'),
    comment='Internal quality and batch metadata for clinical_critical_care_period; same row grain as the research table. Join keys: patient_event_key, period_business_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def critical_care_period_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_critical_care_period; same row grain as the research table. Join keys:
    # patient_event_key, period_business_key.
    return (_lifecycle_source_critical_care_period()).select(*CRITICAL_CARE_PERIOD_LIFECYCLE_COLUMNS)

# COMMAND ----------

# Finalise the public column lists used by this notebook.
if "source_patient_event_key" not in FAMILY_HISTORY_PUBLIC_COLUMNS:
    FAMILY_HISTORY_PUBLIC_COLUMNS.insert(FAMILY_HISTORY_PUBLIC_COLUMNS.index("patient_event_key") + 1, "source_patient_event_key")
for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_family_history"].items():
    _s3_replace_public_variant(FAMILY_HISTORY_PUBLIC_COLUMNS, old_name, axes)

if "source_patient_event_key" not in CONDITION_PUBLIC_COLUMNS:
    CONDITION_PUBLIC_COLUMNS.insert(CONDITION_PUBLIC_COLUMNS.index("patient_event_key") + 1, "source_patient_event_key")
for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_condition"].items():
    _s3_replace_public_variant(CONDITION_PUBLIC_COLUMNS, old_name, axes)

if "source_patient_event_key" not in PROCEDURE_PUBLIC_COLUMNS:
    PROCEDURE_PUBLIC_COLUMNS.insert(PROCEDURE_PUBLIC_COLUMNS.index("patient_event_key") + 1, "source_patient_event_key")
for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_procedure"].items():
    _s3_replace_public_variant(PROCEDURE_PUBLIC_COLUMNS, old_name, axes)

for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_specimen"].items():
    _s3_replace_public_variant(SPECIMEN_PUBLIC_COLUMNS, old_name, axes)

for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_pathology_report"].items():
    _s3_replace_public_variant(REPORT_PUBLIC_COLUMNS, old_name, axes)

for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_pathology_result"].items():
    _s3_replace_public_variant(RESULT_PUBLIC_COLUMNS, old_name, axes)

for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_vital_sign"].items():
    _s3_replace_public_variant(VITAL_PUBLIC_COLUMNS, old_name, axes)

for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_clinical_score"].items():
    _s3_replace_public_variant(SCORE_PUBLIC_COLUMNS, old_name, axes)

if "source_patient_event_key" not in MEDICATION_ADMIN_PUBLIC_COLUMNS:
    MEDICATION_ADMIN_PUBLIC_COLUMNS.insert(MEDICATION_ADMIN_PUBLIC_COLUMNS.index("patient_event_key") + 1, "source_patient_event_key")
for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_medication_admin"].items():
    _s3_replace_public_variant(MEDICATION_ADMIN_PUBLIC_COLUMNS, old_name, axes)

if "source_patient_event_key" not in MEDICATION_ORDER_PUBLIC_COLUMNS:
    MEDICATION_ORDER_PUBLIC_COLUMNS.insert(MEDICATION_ORDER_PUBLIC_COLUMNS.index("patient_event_key") + 1, "source_patient_event_key")
for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_medication_order"].items():
    _s3_replace_public_variant(MEDICATION_ORDER_PUBLIC_COLUMNS, old_name, axes)

if "source_patient_event_key" not in MEDICATION_DISPENSE_PUBLIC_COLUMNS:
    MEDICATION_DISPENSE_PUBLIC_COLUMNS.insert(MEDICATION_DISPENSE_PUBLIC_COLUMNS.index("patient_event_key") + 1, "source_patient_event_key")
for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_medication_dispense"].items():
    _s3_replace_public_variant(MEDICATION_DISPENSE_PUBLIC_COLUMNS, old_name, axes)

for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_clinical_finding"].items():
    _s3_replace_public_variant(CLINICAL_FINDING_PUBLIC_COLUMNS, old_name, axes)

for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_imaging_exam"].items():
    _s3_replace_public_variant(IMAGING_EXAM_PUBLIC_COLUMNS, old_name, axes)

for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_allergy_intolerance"].items():
    _s3_replace_public_variant(ALLERGY_PUBLIC_COLUMNS, old_name, axes)

for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_cancer_treatment"].items():
    _s3_replace_public_variant(CANCER_TREATMENT_PUBLIC_COLUMNS, old_name, axes)

for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_condition_stage"].items():
    _s3_replace_public_variant(CONDITION_STAGE_PUBLIC_COLUMNS, old_name, axes)

for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_endoscopy_finding"].items():
    _s3_replace_public_variant(ENDOSCOPY_FINDING_PUBLIC_COLUMNS, old_name, axes)

