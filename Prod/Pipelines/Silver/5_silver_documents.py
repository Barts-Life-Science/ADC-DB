# Databricks notebook source
# MAGIC %md
# MAGIC # Documents
# MAGIC Clinical text, reports, imaging assets, files and their sensitivity labels.

# MAGIC
# MAGIC Reading order: 5 of 8. Numbers guide navigation; Lakeflow schedules datasets by their dependencies.
# MAGIC Shared helpers live in `silver_journey_shared.py`, an importable Python file.

# COMMAND ----------

# Shared projections also serve the patient event index.
from silver_journey_shared import (
    DOCUMENT_LANE_COLUMNS,
    DOCUMENT_PRIMITIVE_COLUMNS,
    DOCUMENT_THREADED_COLUMNS,
    DOCUMENT_VERSION_COLUMNS,
    F,
    LIFECYCLE_COLUMN_COMMENTS,
    S3_TABLE_AXIS_SPECS,
    _artifact_asset_canonical,
    _cross_qc_primitive,
    _cross_qc_public,
    _dicom_file_artifact_canonical,
    _document_canonical,
    _document_lane,
    _document_prsb_enrich,
    _n,
    _pacs_study_artifact_canonical,
    _pathology_report_document_history_canonical,
    _s3_flatten_codeable_json,
    _s3_replace_public_variant,
    canonical_coding_system,
    materialized_view,
    stable_id,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Artifact / asset

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
ARTIFACT_ASSET_SOURCE_COLUMNS = [
    "patient_event_key",
    "pacs_examination_id",
    "dicom_path",
    "organization_key",
    "source_object",
    "artifact_key",
    "parent_artifact_key",
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
    "artifact_type",
    "artifact_class",
    "artifact_level",
    "acquisition_datetime",
    "modality_code",
    "body_site_code",
    "locator_system",
    "locator_value",
    "storage_uri",
    "availability_status",
    "study_instance_uid",
    "series_instance_uid",
    "sop_instance_uid",
    "series_count",
    "object_count",
    "folder_count",
    "payload_present_ind",
    "file_name",
    "content_type",
    "byte_size",
    "sha256",
    "source_artifact_id",
    "ingest_run_id",
    "last_accessed_datetime",
    "archive_status_code",
    "burned_in_pii_tier",
    "status_code",
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
ARTIFACT_ASSET_PUBLIC_COLUMNS = [
    'patient_event_key',
    'pacs_examination_id',
    'dicom_path',
    'organization_key',
    'source_object',
    'artifact_key',
    'parent_artifact_key',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'artifact_type',
    'artifact_class',
    'artifact_level',
    'acquisition_datetime',
    'modality_code',
    'body_site_code',
    'locator_system',
    'locator_value',
    'storage_uri',
    'availability_status',
    'study_instance_uid',
    'series_instance_uid',
    'sop_instance_uid',
    'series_count',
    'object_count',
    'folder_count',
    'payload_present_ind',
    'file_name',
    'content_type',
    'byte_size',
    'sha256',
    'source_artifact_id',
    'ingest_run_id',
    'last_accessed_datetime',
    'archive_status_code',
    'burned_in_pii_tier',
    'status_code',
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

ARTIFACT_ASSET_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'dicom_path',
    'pacs_examination_id',
    'identity_status',
    'load_batch_id',
]

ARTIFACT_ASSET_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "pacs_examination_id": "PACS_EXAMINATION_ID; native key for the pacs_study arm.",
    "dicom_path": "DICOM_PATH; native key for the dicom_file arm.",
    "organization_key": "Nullable deterministic organization key; no organization expression is present in the artifact source cells.",
    "source_object": "Native source arm: pacs_study or dicom_file.",
    "artifact_key": "Deterministic SHA-256 artifact identity.",
    "parent_artifact_key": "Deterministic SHA-256 key of the containing artifact.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system behind subject_key.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "encounter_id": "Nullable Millennium ENCNTR_ID; these artifact lanes do not supply encounter context.",
    "event_datetime": "Artifact acquisition or creation timestamp.",
    "event_end_datetime": "Artifact acquisition end timestamp when supplied.",
    "source_coding_system": "Verbatim source procedure or artifact coding system.",
    "source_code": "Verbatim source procedure or artifact code.",
    "source_display": "Verbatim source procedure or artifact display.",
    "artifact_type": "Source clinical-artifact CodeableConcept.",
    "artifact_class": "High-level payload class.",
    "artifact_level": "Artifact granularity represented by the row.",
    "acquisition_datetime": "Source acquisition timestamp.",
    "modality_code": "Imaging or acquisition modality when supplied.",
    "body_site_code": "Body-site code when supplied.",
    "locator_system": "Namespace of locator_value.",
    "locator_value": "Stable source locator such as a DICOM Study Instance UID.",
    "storage_uri": "Governed retrievable location when a serving manifest supplies one; never invented by Silver.",
    "availability_status": "Whether Silver has metadata only or a governed retrievable payload.",
    "study_instance_uid": "DICOM Study Instance UID when supplied.",
    "series_instance_uid": "DICOM Series Instance UID when supplied.",
    "sop_instance_uid": "DICOM SOP Instance UID when supplied.",
    "series_count": "Number of series represented by the artifact when supplied.",
    "object_count": "Number of image or file objects represented when supplied.",
    "folder_count": "Source folder count when supplied.",
    "payload_present_ind": "Source evidence that binary or pixel payload exists.",
    "file_name": "Source file name when a governed manifest supplies one.",
    "content_type": "MIME type or governed source format.",
    "byte_size": "Represented payload size in bytes when supplied.",
    "sha256": "Payload SHA-256 when a governed serving manifest supplies one.",
    "source_artifact_id": "Native source identifier for the represented artifact or study.",
    "ingest_run_id": "Upstream manifest or ingest-run identifier when supplied.",
    "last_accessed_datetime": "Source-reported last-access timestamp when supplied.",
    "archive_status_code": "Source archive-state code when supplied.",
    "burned_in_pii_tier": "Upstream burned-in-PII classification when supplied.",
    "status_code": "Source artifact status.",
    "confidentiality_code": "Source confidentiality classification.",
    "vip_ind": "Source VIP indicator.",
    "withheld_identity_ind": "Source withheld-identity indicator.",
    "source_feed": "Registered artifact source route.",
    "record_status": "Derived separately for PACS-study and DICOM-file assets: retracted when that arm's SOURCE_PRESENT_IND is false, otherwise active; a null presence flag does not retract the row. Neither arm emits superseded. This row-status label is distinct from pixel/file availability.",
    "record_status_effective_from": "Source provenance timestamp used as the history start: map_pacs_examination.SRC_ADC_UPDT for PACS studies, or map_dicom_file_attribute.SOURCE_MAX_EXTRACTION_TS for DICOM files. It is not the image acquisition time or an independently recorded status transition; missing values remain null.",
    "record_status_effective_to": "For retracted assets only, map_pacs_examination.ADC_UPDT on the PACS-study arm or map_dicom_file_attribute.PIPELINE_UPDT_DT_TM on the DICOM-file arm. Null for active assets or when that arm's processing clock is unavailable. This processing-time end proxy is not a Boolean presence indicator.",
    "source_update_timestamp": "map_pacs_examination.SRC_ADC_UPDT for PACS-study assets; map_dicom_file_attribute.SOURCE_MAX_EXTRACTION_TS for DICOM-file assets. These arm-specific source provenance clocks are carried unchanged, not combined across the union or replaced by Silver refresh time; missing values remain null.",
    "loaded_at": "map_pacs_examination.ADC_UPDT for PACS-study assets; map_dicom_file_attribute.PIPELINE_UPDT_DT_TM for DICOM-file assets. The union carries each arm's ingestion/processing clock unchanged; it does not compute a maximum across studies and files. This is not image acquisition or Silver refresh time.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_artifact_asset():
    # Assemble artifact asset rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return _artifact_asset_canonical().select(*ARTIFACT_ASSET_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_artifact.asset"),
    comment="One governed clinical artifact or artifact-collection locator; payloads remain outside Silver.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=ARTIFACT_ASSET_COLUMN_COMMENTS,
)
def artifact_asset():
    # Build the declared dataset: One governed clinical artifact or artifact-collection locator;
    # payloads remain outside Silver.
    df = (_lifecycle_source_artifact_asset()
          .withColumn("source_coding_system", canonical_coding_system(F.col("source_coding_system")))
          .withColumn("_artifact_type_json", F.to_json(F.col("artifact_type"))))
    return _s3_flatten_codeable_json(df, "_artifact_type_json", "artifact_type", True).select(
        *ARTIFACT_ASSET_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_artifact._asset_metadata'),
    comment='Internal quality and batch metadata for artifact_asset; same row grain as the research table. Join keys: patient_event_key, source_object, dicom_path, pacs_examination_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def artifact_asset_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for artifact_asset; same
    # row grain as the research table. Join keys: patient_event_key, source_object, dicom_path,
    # pacs_examination_id.
    return (_lifecycle_source_artifact_asset()).select(*ARTIFACT_ASSET_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Artifact / asset sensitivity label

# COMMAND ----------

# ==== journey_artifact.asset_sensitivity_label ====

ARTIFACT_ASSET_SENSITIVITY_LABEL_PUBLIC_COLUMNS = [
    "patient_event_key", "pacs_examination_id", "dicom_path", "artifact_key",
    "sequence", "sensitivity_label", "loaded_at",
]

ARTIFACT_ASSET_SENSITIVITY_LABEL_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent artifact_asset row.",
    "pacs_examination_id": "PACS_EXAMINATION_ID for the pacs_study parent arm.",
    "dicom_path": "DICOM_PATH for the dicom_file parent arm.",
    "artifact_key": "Deterministic artifact key carried by the parent.",
    "sequence": "One-based order of the source sensitivity label.",
    "sensitivity_label": "Verbatim source sensitivity or security label.",
    "loaded_at": "Bronze load timestamp inherited from the parent source row.",
}

@materialized_view(
    name=_n("journey_artifact.asset_sensitivity_label"),
    comment="One ordered sensitivity label per governed artifact asset.",
    column_comments=ARTIFACT_ASSET_SENSITIVITY_LABEL_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def artifact_asset_sensitivity_label():
    # Build the declared dataset: One ordered sensitivity label per governed artifact asset.
    source = _artifact_asset_canonical().select(
        "patient_event_key", "pacs_examination_id", "dicom_path", "artifact_key",
        "_sensitivity_labels_json", "loaded_at",
    )
    return (
        source.select(
            "patient_event_key", "pacs_examination_id", "dicom_path",
            "artifact_key", "loaded_at",
            F.posexplode_outer(
                F.from_json(F.col("_sensitivity_labels_json"), "array<string>")
            ).alias("_position", "sensitivity_label"),
        )
        .where(F.col("sensitivity_label").isNotNull())
        .select(
            "patient_event_key", "pacs_examination_id", "dicom_path", "artifact_key",
            (F.col("_position") + 1).cast("long").alias("sequence"),
            F.col("sensitivity_label").cast("string").alias("sensitivity_label"),
            "loaded_at",
        )
        .select(*ARTIFACT_ASSET_SENSITIVITY_LABEL_PUBLIC_COLUMNS)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Artifact / link

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
ARTIFACT_LINK_SOURCE_COLUMNS = [
    "artifact_link_key",
    "artifact_key",
    "patient_event_key",
    "fact_table",
    "role_code",
    "role_display",
    "source_system",
    "source_table",
    "source_row_id",
    "load_batch_id",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
ARTIFACT_LINK_PUBLIC_COLUMNS = [
    'artifact_link_key',
    'artifact_key',
    'patient_event_key',
    'fact_table',
    'role_code',
    'role_display',
    'source_system',
    'source_table',
    'source_row_id',
    'loaded_at',
]

ARTIFACT_LINK_LIFECYCLE_COLUMNS = [
    'artifact_link_key',
    'patient_event_key',
    'load_batch_id',
]

def _artifact_link_canonical():
    # contract v2: use explicit artifact and event key names and remove duplicate fact-row hashes from every link arm
    # Assemble normalized artifact link rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    studies = _pacs_study_artifact_canonical()
    imaging = studies.select(
        stable_id(
            "artifact_link:pacs", F.col("artifact_key"), F.lit("content_of"),
            F.col("_imaging_patient_event_id"),
        ).alias("artifact_link_key"),
        "artifact_key",
        F.col("_imaging_patient_event_id").alias("patient_event_key"),
        F.lit("journey_clinical.imaging_exam").alias("fact_table"),
        F.lit("content_of").alias("role_code"),
        F.lit("Imaging content of examination").alias("role_display"),
        F.col("_source_system").alias("source_system"),
        F.col("_source_table").alias("source_table"),
        F.col("_source_row_id").alias("source_row_id"),
        "load_batch_id", "loaded_at",
    )
    report = studies.where(F.col("_report_patient_event_id").isNotNull()).select(
        stable_id(
            "artifact_link:pacs", F.col("artifact_key"), F.lit("associated_report"),
            F.col("_report_patient_event_id"),
        ).alias("artifact_link_key"),
        "artifact_key",
        F.col("_report_patient_event_id").alias("patient_event_key"),
        F.lit("journey_text.document").alias("fact_table"),
        F.lit("associated_report").alias("role_code"),
        F.lit("Diagnostic report for artifact").alias("role_display"),
        F.col("_source_system").alias("source_system"),
        F.col("_source_table").alias("source_table"),
        F.col("_source_row_id").alias("source_row_id"),
        "load_batch_id", "loaded_at",
    )
    files = _dicom_file_artifact_canonical()
    file_exam = files.where(F.col("_imaging_patient_event_id").isNotNull()).select(
        stable_id(
            "artifact_link:dicom_file", F.col("artifact_key"), F.lit("content_of"),
            F.col("_imaging_patient_event_id"),
        ).alias("artifact_link_key"),
        "artifact_key",
        F.col("_imaging_patient_event_id").alias("patient_event_key"),
        F.lit("journey_clinical.imaging_exam").alias("fact_table"),
        F.lit("content_of").alias("role_code"),
        F.lit("DICOM file content of examination").alias("role_display"),
        F.col("_source_system").alias("source_system"),
        F.col("_source_table").alias("source_table"),
        F.col("_source_row_id").alias("source_row_id"),
        "load_batch_id", "loaded_at",
    )
    file_parent = files.where(F.col("parent_artifact_key").isNotNull()).select(
        stable_id(
            "artifact_link:dicom_file", F.col("artifact_key"), F.lit("part_of"),
            F.col("parent_artifact_key"),
        ).alias("artifact_link_key"),
        "artifact_key",
        F.col("parent_artifact_key").alias("patient_event_key"),
        F.lit("journey_artifact.asset").alias("fact_table"),
        F.lit("part_of").alias("role_code"),
        F.lit("DICOM file contained by imaging study").alias("role_display"),
        F.col("_source_system").alias("source_system"),
        F.col("_source_table").alias("source_table"),
        F.col("_source_row_id").alias("source_row_id"),
        "load_batch_id", "loaded_at",
    )
    return imaging.unionByName(report).unionByName(file_exam).unionByName(file_parent)

ARTIFACT_LINK_COLUMN_COMMENTS = {
    "artifact_link_key": "Deterministic SHA-256 relationship key.",
    "artifact_key": "Deterministic SHA-256 key of the linked artifact.",
    "patient_event_key": "Deterministic SHA-256 key of the linked event or parent artifact.",
    "fact_table": "Logical typed table or parent artifact holding the linked row.",
    "role_code": "Coded relationship role.",
    "role_display": "Display label describing the artifact relationship, such as examination content, diagnostic report or file containment.",
    "source_system": "Source-system label carried from the artifact record that supplies this relationship.",
    "source_table": "Governed Bronze source table.",
    "source_row_id": "Native source row identifier.",
    "loaded_at": "PACS-study links carry map_pacs_examination.ADC_UPDT; DICOM-file links carry map_dicom_file_attribute.PIPELINE_UPDT_DT_TM. Both imaging/report study links and exam/parent file links retain their own artifact arm's clock. Linked report, examination and parent-artifact clocks are not joined or maximized, and the union does not combine clocks across arms.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_artifact_link():
    # Assemble artifact link rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return _artifact_link_canonical().select(*ARTIFACT_LINK_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_artifact.link"),
    comment="Traceable relationships from clinical artifacts to typed facts and documents.",
    refresh_policy="incremental",
    column_comments=ARTIFACT_LINK_COLUMN_COMMENTS,
)
def artifact_link():
    # Build the declared dataset: Traceable relationships from clinical artifacts to typed facts
    # and documents.
    return _lifecycle_source_artifact_link().select(*ARTIFACT_LINK_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_artifact._link_metadata'),
    comment='Internal quality and batch metadata for artifact_link; same row grain as the research table. Join keys: artifact_link_key, patient_event_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def artifact_link_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for artifact_link; same
    # row grain as the research table. Join keys: artifact_link_key, patient_event_key.
    return (_lifecycle_source_artifact_link()).select(*ARTIFACT_LINK_LIFECYCLE_COLUMNS)

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
DOCUMENT_PUBLIC_COLUMNS = [
    'patient_event_key',
    'event_id',
    'update_count',
    'valid_from_datetime',
    'pacs_report_id',
    'endobase_exam_id',
    'report_version_id',
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
    'document_type',
    'title',
    'author_practitioner_id',
    'author_role',
    'service_id',
    'status_code',
    'version_id',
    'document_thread_key',
    'supersedes_document_key',
    'version_ordinal',
    'is_latest_version',
    'document_text',
    'parser_version',
    'decompressor_version',
    'post_processor_version',
    'content_type',
    'encoding',
    'language',
    'text_sha256',
    'raw_content_sha256',
    'text_length',
    'content_class',
    'date_quality',
    'text_is_truncated',
    'linkage_route',
    'source_class',
    'assembly_status',
    'chunk_count',
    'corpus_frequency',
    'is_boilerplate',
    'source_link_event_id',
    'source_link_system',
    'author_id_system',
    'author_source_id',
    'verified_practitioner_id',
    'verified_datetime',
    'source_organization_key',
    'source_organization_display',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'document_class',
    'contributor_system',
    'succession_status',
    'source_parent_event_id',
    'parent_relation',
    'source_parent_display',
    'source_parent_title',
    'source_parent_tag',
    'source_tag',
    'source_record_status',
    'document_text_anonymised',
    'text_is_anonymised',
    'prsb_document_type',
    'prsb_subtype',
    'prsb_standard',
    'prsb_setting',
    'prsb_map_method',
    'prsb_map_score',
    'prsb_map_version',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

DOCUMENT_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'endobase_exam_id',
    'event_id',
    'update_count',
    'valid_from_datetime',
    'pacs_report_id',
    'report_version_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Text /  document primitive

# COMMAND ----------

# contract v2: rename document thread and supersession SHA identities to explicit key columns
def _document_thread_metadata(df):
    """Attach source-derived thread identity before the batch-built head join."""
    return (
        df.withColumn(
            "document_thread_key",
            F.coalesce(F.col("_document_thread_id"), F.col("patient_event_key")),
        )
        .withColumn("supersedes_document_key", F.col("_supersedes_document_id"))
        .withColumn(
            "version_ordinal",
            F.coalesce(F.col("_version_ordinal"), F.lit(1).cast("long")),
        )
    )

@materialized_view(
    name=_n("journey_text._document_primitive"),
    private=True,
    comment="Internal document union with in-graph corpus frequency and thread metadata.",
)
def _document_primitive():
    # Build the declared dataset: Internal document union with in-graph corpus frequency and
    # thread metadata.
    history = _document_prsb_enrich(
        _document_lane(_pathology_report_document_history_canonical())
        .select(*DOCUMENT_LANE_COLUMNS)
    )
    versions = (
        _document_canonical().select(*DOCUMENT_VERSION_COLUMNS)
        .unionByName(history.select(*DOCUMENT_VERSION_COLUMNS))
    )
    versions = _document_thread_metadata(versions)

    # Formerly built by a separate post-refresh notebook. Keeping it here removes
    # the circular operational dependency while preserving the sparse contract:
    # counts below 100 remain NULL and 1000+ is marked as boilerplate.
    frequencies = (
        versions.where(F.col("text_sha256").isNotNull())
        .groupBy("text_sha256")
        .agg(F.count(F.lit(1)).cast("long").alias("_corpus_frequency"))
        .where(F.col("_corpus_frequency") >= 100)
    )
    return (
        versions.drop("corpus_frequency", "is_boilerplate")
        .join(frequencies, "text_sha256", "left")
        .withColumn("corpus_frequency", F.col("_corpus_frequency"))
        .withColumn(
            "is_boilerplate",
            F.coalesce(F.col("_corpus_frequency"), F.lit(0)) >= 1000,
        )
        .drop("_corpus_frequency")
        .select(*DOCUMENT_PRIMITIVE_COLUMNS)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Text /  document threaded

# COMMAND ----------

@materialized_view(
    name=_n("journey_text._document_threaded"),
    private=True,
    comment="Internal document versions with the latest version selected deterministically in-graph.",
)
# contract v2: use renamed document and thread keys for deterministic latest-version selection
def _document_threaded():
    # Build the declared dataset: Internal document versions with the latest version selected
    # deterministically in-graph.
    versions = spark.read.table(_n("journey_text._document_primitive"))
    effective_from = F.coalesce(
        F.col("record_status_effective_from"),
        F.col("event_datetime"),
        F.col("source_update_timestamp"),
        F.col("loaded_at"),
    )
    head_key = F.struct(
        F.when(
            F.col("document_text").isNotNull()
            & (F.trim(F.col("document_text")) != ""),
            F.lit(1),
        ).otherwise(F.lit(0)).alias("has_data"),
        F.coalesce(effective_from.cast("double"), F.lit(-1.0e308))
        .alias("effective_from_epoch"),
        F.coalesce(F.col("version_ordinal"), F.lit(-1).cast("long"))
        .alias("version_ordinal"),
        F.coalesce(F.col("version_id"), F.lit("")).alias("version_id"),
    )
    ranked = versions.select(
        "patient_event_key",
        "document_thread_key",
        head_key.alias("head_key"),
    ).alias("r")
    heads = ranked.groupBy("document_thread_key").agg(
        F.max("head_key").alias("selected_head_key")
    ).alias("h")
    selected = ranked.join(
        heads,
        (F.col("r.document_thread_key") == F.col("h.document_thread_key"))
        & (F.col("r.head_key") == F.col("h.selected_head_key")),
        "inner",
    ).select(
        F.col("r.document_thread_key").alias("_head_thread_key"),
        F.col("r.patient_event_key").alias("_latest_patient_event_key"),
    )
    return (
        versions.join(
            selected,
            versions["document_thread_key"] == selected["_head_thread_key"],
            "left",
        )
        .withColumn(
            "is_latest_version",
            F.coalesce(
                F.col("patient_event_key") == F.col("_latest_patient_event_key"),
                F.lit(False),
            ),
        )
        .drop("_head_thread_key", "_latest_patient_event_key")
        .select(*DOCUMENT_THREADED_COLUMNS)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Text /  qc document

# COMMAND ----------

@materialized_view(
    name=_n("journey_text._qc_document"),
    comment="Private JSON bridge and Gold cross-rule flags for document.",
    refresh_policy="incremental",
)
def _qc_document():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for document.
    return _cross_qc_primitive(
        spark.read.table(_n("journey_text._document_threaded")),
        "document",
        {
            "document_type": "_document_type_json",
            "sections": "_sections_json",
            "sensitivity_labels": "_sensitivity_labels_json",
        },
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Text / document

# COMMAND ----------

DOCUMENT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "event_id": "Millennium document EVENT_ID; native key for the mill_blob arm.",
    "update_count": "De-identified version of the decoded blob text in which detected patient identifiers have been replaced with placeholder tokens.",
    "valid_from_datetime": "De-identified version of the decoded blob text in which detected patient identifiers have been replaced with placeholder tokens.",
    "pacs_report_id": "PACS_REPORT_ID; native key for the pacs_report arm.",
    "endobase_exam_id": "ENDOBASE_EXAM_ID; native key for the endobase_exam arm.",
    "report_version_id": "Pathology report_version_id; native key for the pathology_report arm.",
    "organization_key": "Deterministic organization key retained for mixed Millennium and PACS organization evidence.",
    "source_object": "Native document source arm; contract arms are endobase_exam, mill_blob, pacs_report, and pathology_report.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID when supplied; native encounter foreign key.",
    "event_datetime": "Document clinical or version timestamp.",
    "event_end_datetime": "Source version-validity end.",
    "source_coding_system": "Verbatim source document-type system.",
    "source_code": "Verbatim source document-type code.",
    "source_display": "Verbatim source document-type display.",
    "document_type": "Source document-type CodeableConcept.",
    "title": "Source document title or label.",
    "author_practitioner_id": "Millennium personnel PERSON_ID when the author resolves to Millennium personnel.",
    "author_role": "Source author role when supplied.",
    "service_id": "Service reference when supplied.",
    "status_code": "Verbatim document extraction or source status.",
    "version_id": "Source document-version token.",
    "document_thread_key": "Deterministic SHA-256 key for all versions of one logical document.",
    "supersedes_document_key": "Deterministic SHA-256 key of the directly superseded document version.",
    "version_ordinal": "Source-supplied version ordinal when available; one for single-version lanes.",
    "is_latest_version": "Exactly one row per document_thread_id selected by non-empty text then effective-from and source version ordinal descending with version_id descending as deterministic tiebreak.",
    "document_text": "Identifiable source document text under default-deny access.",
    "parser_version": "Parser version that produced document_text or sections. PACS reports whose text comes from the report-text bridge (v4, re-sourced from mill_blob_text) carry the bridge BRIDGED_TEXT_PARSER_VERSION, formatted mill_blob_text:d<decompressor>/p<parser>/pp<post-processor>; pacs-bridge-v1 marks v3 bridge text.",
    "decompressor_version": "Decompressor version used by bronze.",
    "post_processor_version": "Post-processor version used by bronze.",
    "content_type": "Source MIME or content type.",
    "encoding": "Source text encoding",
    "language": "Document language defaulted to English (`en`) because no source language field is available; method=default.",
    "text_sha256": "SHA-256 of retained document_text only; the anonymous alternative never changes this hash or any stable identifier. NULL is represented by the empty-string digest.",
    "raw_content_sha256": "SHA-256 of the original binary payload when supplied by the blob source; distinct from text_sha256.",
    "text_length": "Retained document-text character count recomputed after parsing and post-processing.",
    "content_class": "Deterministic retained-text quality class; rows are flagged rather than dropped.",
    "date_quality": "Event-date quality relative to source provenance: null, pre-1975 epoch sentinel, future beyond source/load time plus one day, or ok.",
    "text_is_truncated": "True when retained text length equals a known source or parser cap (1000000, 65535, 32767, or 32000 characters).",
    "linkage_route": "Deterministic provenance route used to resolve document subject or encounter linkage.",
    "source_class": "Fail-closed source-content class; the public document product contains clinical text only.",
    "assembly_status": "Source-row assembly outcome; NULL for parked SCD working-copy rows.",
    "chunk_count": "Number of source chunks represented by this document row; NULL for parked SCD working-copy rows.",
    "corpus_frequency": "Number of current Journey document rows sharing text_sha256 when the governed release-built frequency is at least 100; NULL means below that storage floor.",
    "is_boilerplate": "True exactly when governed corpus_frequency is at least 1000; absent frequency is false.",
    "source_link_event_id": "Verbatim linked source event identifier when a governed cross-feed bridge supplies one. PACS reports: the Cerner report document EVENT_ID of the version-safe bridge row; source-absent bridge tombstones are not read. Imaging report relationships are in clinical_imaging_report_link.",
    "source_link_system": "Identifier system for source_link_event_id.",
    "author_id_system": "Identifier system for the verbatim source author identifier.",
    "author_source_id": "Verbatim source author identifier retained separately from any resolved practitioner reference.",
    "verified_practitioner_id": "Millennium personnel PERSON_ID when the verifier resolves to Millennium personnel.",
    "verified_datetime": "Source verification timestamp associated with verified_practitioner_id.",
    "source_organization_key": "Deterministic source-organization key from governed source evidence.",
    "source_organization_display": "Source-organization display from the governed organization dimension or verbatim PACS institution.",
    "confidentiality_code": "Source confidentiality classification.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "document_class": "Source event-class display such as Document or mdoc.",
    "contributor_system": "Source contributor-system display such as PowerChart or BLT_TIE_RAD.",
    "succession_status": "Blob succession status display such as Interim or Final.",
    "source_parent_event_id": "Parent clinical-event identifier for document threading when supplied.",
    "parent_relation": "Event relation display such as Root or Child.",
    "source_parent_display": "Parent event-code description; the legacy MainEventDesc document-type label.",
    "source_parent_title": "Parent event title text; the legacy MainTitleText.",
    "source_parent_tag": "Parent event tag text; the legacy MainTagText.",
    "source_tag": "Own event tag text; the legacy ChildTagText.",
    "source_record_status": "Clinical-event record-status display such as Active or Deleted; the legacy Status.",
    "document_text_anonymised": "Reserved NULL field; de-identification is applied at serve time.",
    "text_is_anonymised": "Always false in silver; de-identification is applied at serve time.",
    "prsb_document_type": "PRSB-aligned canonical document type from the governed doc_type_prsb_map lookup.",
    "prsb_subtype": "Canonical subtype qualifier when the mapping supplies one.",
    "prsb_standard": "Source PRSB or Royal-College standard label for the canonical type.",
    "prsb_setting": "Care-setting bucket of the canonical type.",
    "prsb_map_method": "Mapping method provenance.",
    "prsb_map_score": "Embedder cosine score for embedder-method rows.",
    "prsb_map_version": "doc_type_prsb_map version label.",
    "source_feed": "Registered owning source route; all document text is IG-sensitive clinical content.",
    "record_status": "Arm-specific status, with a final text override: after document text hygiene, lower(trim(document_text)) equal to deleted forces retracted. Otherwise text-event rows use SOURCE_DELETED_IND (null false) for retraction, then a validity end before 2100-01-01 or AUTHENTIC_FLAG zero (null one) for supersession. Blob rows retract for result-status codes 29, 30 or 31, otherwise supersede for a validity end before 2100. PACS and neonatal narratives retract for false source presence (null true). EndoBase supersedes only when a joined parent exam exists and is source-absent. Order comments supersede for TEXT_ACTIVE_IND zero (null one). Elective-access comments retract for source absence, otherwise supersede for inactive status (both null true). Pathology retracts cancelled/entered_in_error versions, otherwise uses active for is_current true and superseded for false/null. Other rows are active. This is not a verbatim closure flag or the independently selected latest-version marker.",
    "record_status_effective_from": "Source-arm history start, unchanged by text hygiene and thread-head selection. Text events: first STRING_RESULT_VALID_FROM_DT_TM, CLINICAL_EVENT_VALID_FROM_DT_TM, then clinical time falling back through RESULT_DT_TM, PERFORMED_DT_TM, EVENT_END_DT_TM and EVENT_START_DT_TM. Blob versions: first VALID_FROM_DT_TM, UPDT_DT_TM, ADC_UPDT. PACS: SRC_ADC_UPDT. EndoBase: parent exam SOURCE_CREATE_TS then earliest retained term CREATED_TS. Order comments: first COMMENT_DT_TM_CLEAN, COMMENT_UPDT_DT_TM_CLEAN, parent ORIG_ORDER_DT_TM_CLEAN, parent CURRENT_START_DT_TM_CLEAN, COMMENT_DT_TM, COMMENT_UPDT_DT_TM. Elective-access comments: CREATED_DT_TM. Neonatal narratives: parent episode DischTime_CLEAN then AdmitTime_CLEAN. Pathology versions: valid_from. No common clinical-time clamp or current refresh timestamp is added.",
    "record_status_effective_to": "Arm-specific end before text hygiene: text-event retractions use their computed source_update_timestamp; supersessions use the least string-result/event validity end, which can be null or a sentinel for AUTHENTIC_FLAG-only supersession. Blob rows use VALID_UNTIL_DT_TM only when it is non-null and before 2100-01-01; a result-status-only retraction can have no end. Source-absent PACS and neonatal rows use own ADC_UPDT. Source-absent joined EndoBase exams use the maximum retained-term, exam and encounter ADC_UPDT. Inactive order comments use TEXT_UPDT_DT_TM_CLEAN. Absent/inactive elective-access comments use first MODIFIED_DT_TM, CREATED_DT_TM, WAITING_LIST_STATUS_CHANGE_DT_TM_CLEAN, DECIDED_TO_ADMIT_DT_TM_CLEAN, TCI_DT_TM_CLEAN. Pathology carries valid_to directly. Other arm cases have null ends. Finally, text equal to deleted after hygiene forces first existing end, source_update_timestamp, loaded_at; it does not guarantee a non-null end. This is not a clinical document completion time.",
    "source_update_timestamp": "Source-arm-specific timestamp; interpret with source_feed. text_event takes the greatest STRING_RESULT_EFFECTIVE_UPDT_DT_TM, CLINICAL_EVENT_ADC_UPDT, LONG_TEXT_ADC_UPDT, LOOKUP_ADC_UPDT and ADC_UPDT from map_text_events. mill_blob_text uses UPDT_DT_TM; pacs_report uses REPORT_MODIFIED_UTC. endobase_exam takes the greatest contributing term ADC_UPDT and parent-exam PIPELINE_UPDT_DT_TM. order_comment uses TEXT_UPDT_DT_TM_CLEAN, falling back to COMMENT_UPDT_DT_TM_CLEAN. elective_access_comment uses MODIFIED_DT_TM; neonatal_episode_narrative uses ADC_UPDT; pathology_report uses valid_from. This mixes source, version and processing clocks; it is not a uniform clinical event time or native-system update time.",
    "loaded_at": "Per-arm contributing load clock: text events use own map_text_events.ADC_UPDT; deduplicated blob versions use the selected mill_blob_text.ADC_UPDT; PACS reports use own report ADC_UPDT, excluding bridge, examination and linkage clocks; neonatal narratives and pathology versions use own ADC_UPDT, excluding joined episode, accession, identity and alias clocks. EndoBase takes greatest of maximum ADC_UPDT across retained nonblank source-present terms, joined exam ADC_UPDT and joined encounter ADC_UPDT. Order comments take greatest of SOURCE_COMMENT_ADC_UPDT, SOURCE_TEXT_ADC_UPDT, PIPELINE_UPDT_DT_TM and joined order SOURCE_ADC_UPDT. Elective-access comments use own ADC_UPDT. Text hygiene, PRSB/organization enrichment, corpus frequency, thread-head selection and QC/public projections add no clock; current and historical pathology versions retain their own clocks. Null inputs follow the arm expression, and no current Silver refresh time is substituted.",
}

@materialized_view(
    name=_n("journey_text.document"),
    comment="Identifiable clinical document versions with source thread and parser provenance.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=DOCUMENT_COLUMN_COMMENTS,
)
def document():
    # Build the declared dataset: Identifiable clinical document versions with source thread and
    # parser provenance.
    return _cross_qc_public(
        spark.read.table(_n("journey_text._qc_document")),
        "document",
        {
            "document_type": "_document_type_json",
            "sections": "_sections_json",
        },
        DOCUMENT_PUBLIC_COLUMNS,
        DOCUMENT_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_text._document_metadata'),
    comment='Internal quality and batch metadata for text_document; same row grain as the research table. Join keys: patient_event_key, source_object, endobase_exam_id, event_id, update_count, valid_from_datetime, pacs_report_id, report_version_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def document_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for text_document; same
    # row grain as the research table. Join keys: patient_event_key, source_object,
    # endobase_exam_id, event_id, update_count, valid_from_datetime, pacs_report_id,
    # report_version_id.
    return (spark.read.table(_n("journey_text._qc_document"))).select(*DOCUMENT_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Text / document sensitivity label

# COMMAND ----------

# ==== journey_text.document_sensitivity_label ====

TEXT_DOCUMENT_SENSITIVITY_LABEL_PUBLIC_COLUMNS = [
    "patient_event_key", "sequence", "sensitivity_label", "loaded_at",
]

TEXT_DOCUMENT_SENSITIVITY_LABEL_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent text_document row.",
    "sequence": "One-based order of the source sensitivity label.",
    "sensitivity_label": "Verbatim source sensitivity or security label.",
    "loaded_at": "Bronze load timestamp inherited from the parent document row.",
}

@materialized_view(
    name=_n("journey_text.document_sensitivity_label"),
    comment="One ordered sensitivity label per clinical text document.",
    column_comments=TEXT_DOCUMENT_SENSITIVITY_LABEL_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def text_document_sensitivity_label():
    # Build the declared dataset: One ordered sensitivity label per clinical text document.
    source = spark.read.table(
        _n("journey_text._document_threaded")
    ).select("patient_event_key", "_sensitivity_labels_json", "loaded_at")
    return (
        source.select(
            "patient_event_key", "loaded_at",
            F.posexplode_outer(
                F.from_json(F.col("_sensitivity_labels_json"), "array<string>")
            ).alias("_position", "sensitivity_label"),
        )
        .where(F.col("sensitivity_label").isNotNull())
        .select(
            "patient_event_key",
            (F.col("_position") + 1).cast("long").alias("sequence"),
            F.col("sensitivity_label").cast("string").alias("sensitivity_label"),
            "loaded_at",
        )
        .select(*TEXT_DOCUMENT_SENSITIVITY_LABEL_PUBLIC_COLUMNS)
    )

# COMMAND ----------

# Finalise the public column lists used by this notebook.
for old_name, axes in S3_TABLE_AXIS_SPECS["artifact_asset"].items():
    _s3_replace_public_variant(ARTIFACT_ASSET_PUBLIC_COLUMNS, old_name, axes)

for old_name, axes in S3_TABLE_AXIS_SPECS["text_document"].items():
    _s3_replace_public_variant(DOCUMENT_PUBLIC_COLUMNS, old_name, axes)

