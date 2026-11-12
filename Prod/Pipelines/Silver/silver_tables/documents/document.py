"""Silver documents.document: 4 dataset(s).

- datasets: _text_document_primitive, _text_qc_document, text_document, _text_document_metadata
- moved verbatim from: 5_silver_documents:472, 5_silver_documents:563, 5_silver_documents:588, 5_silver_documents:602, 5_silver_documents:717, 5_silver_documents:832, 5_silver_documents:853, 5_silver_documents:920

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, spark
from silver_core.value_policy import mill_fk
from silver_tables._common.journey import (
    DOCUMENT_LANE_COLUMNS,
    DOCUMENT_PRIMITIVE_COLUMNS,
    DOCUMENT_THREAD_COLUMNS,
    DOCUMENT_THREAD_PRIVATE_TYPES,
    S3_TABLE_AXIS_SPECS,
    _document_canonical,
    _document_lane,
    _document_prsb_enrich,
    _pathology_report_document_base,
    _pathology_report_document_text_rows,
    _s3_replace_public_variant,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


def _pathology_report_document_history_canonical():
    # Assemble normalized pathology report document history rows for downstream dataset
    # builders, preserving the existing source and identity rules.
    return _pathology_report_document_base(
        _pathology_report_document_text_rows().where(
            ~F.coalesce(F.col("is_current"), F.lit(False))
        )
    )


DOCUMENT_VERSION_COLUMNS = [
    c for c in DOCUMENT_PRIMITIVE_COLUMNS if c not in DOCUMENT_THREAD_COLUMNS
] + list(DOCUMENT_THREAD_PRIVATE_TYPES)


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
    'event_datetime_local',
    'event_end_datetime_local',
    'source_coding_system',
    'source_code',
    'source_display',
    'document_type',
    'title',
    'author_practitioner_id',
    'author_role',
    'service_id',
    'status_code',
    'result_status_code',  # BBR_FOLDIN_V1 BBR-17
    'result_status_display',  # BBR_FOLDIN_V1 BBR-17
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


# Text /  document primitive

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


# Text /  qc document
@materialized_view(
    name=_n("journey_text._qc_document"),
    refresh_policy="auto",
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


@materialized_view(
    name=_n("journey_text.document"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
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
    refresh_policy="auto",
)
def document_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for text_document; same
    # row grain as the research table. Join keys: patient_event_key, source_object,
    # endobase_exam_id, event_id, update_count, valid_from_datetime, pacs_report_id,
    # report_version_id.
    return (spark.read.table(_n("journey_text._qc_document"))).select(*DOCUMENT_LIFECYCLE_COLUMNS)


for old_name, axes in S3_TABLE_AXIS_SPECS["text_document"].items():
    _s3_replace_public_variant(DOCUMENT_PUBLIC_COLUMNS, old_name, axes)


CROSS_QC = {
    'document': CrossQc(model='document', fk=('encounter_id', 'person_id'), dates=('event_after_death_30d', 'event_before_birth'), coded_axes={'document_type': ('document_type', True)}, promote_lifecycle=['identity_status', 'person_id_resolved', 'encounter_id_resolved']),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {
    'text_document': ValuePolicy(table='text_document', enabled=True, zero_as_null=('author_practitioner_id', 'author_source_id'), system_user_columns={'author_practitioner_id': 'author'}),
}


# W2.15(b) (J31, critic 2.5): text_document.encounter_id STRING -> BIGINT, 0 -> NULL. The text chain recomputes
# in the BI refresh anyway, so this costs nothing extra. Prod gate 2026-10-30 (W2.15 Step 1): 331,677,677 rows,
# cast_fail 0, zeros 578,261 (those become NULL).
VALUE_POLICIES["text_document"].value_rule("encounter_id", ["encounter_id"], lambda F: mill_fk("encounter_id"))


WRAP_POLICY = {'text_document'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_text_document_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged from the '
                                                          "public parent; combine it with the companion's "
                                                          'other key columns when joining.',
                                         'activity_record_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                         'admission_key': 'Deterministic v2 key carried unchanged from the '
                                                          'public parent; joins one lifecycle row to that '
                                                          'research row.',
                                         'allergy_instance_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                         'aria_dx_id': 'Native source identifier carried unchanged from the '
                                                       'public parent; joins one lifecycle row to that '
                                                       'research row.',
                                         'aria_pt_id': 'Native source identifier carried unchanged from the '
                                                       'public parent; joins one lifecycle row to that '
                                                       'research row.',
                                         'artifact_link_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                         'birth_row_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                         'bloodtrack_transaction_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                         'cancer_treatment_cycle_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                         'care_contact_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                         'care_participation_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                         'cds_id': 'Native source identifier carried unchanged from the '
                                                   'public parent; joins one lifecycle row to that research '
                                                   'row.',
                                         'chemotherapy_course_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                         'community_care_activity_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                         'community_care_contact_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                         'concept_map_key': 'Deterministic v2 key carried unchanged from the '
                                                            'public parent; joins one lifecycle row to that '
                                                            'research row.',
                                         'cost_line_item_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                         'dcp_forms_activity_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                         'device_mapping_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                         'diagnosis_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                         'dicom_path': 'Primary-key component carried unchanged from the '
                                                       "public parent; combine it with the companion's other "
                                                       'key columns when joining.',
                                         'doc_response_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
                                         'elective_access_procedure_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                         'encounter_attribute_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                         'encounter_bounds_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                         'encounter_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                         'encounter_id_resolved': 'True when encounter_id is null or '
                                                                  'resolves to the governed encounter spine; '
                                                                  'false records unresolved linkage without '
                                                                  'dropping the row.',
                                         'encounter_identifier_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                         'encounter_key': 'Deterministic v2 key carried unchanged from the '
                                                          'public parent; joins one lifecycle row to that '
                                                          'research row.',
                                         'endobase_exam_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                         'endobase_exam_term_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                         'entity_id': 'Native source identifier carried unchanged from the '
                                                      'public parent; joins one lifecycle row to that '
                                                      'research row.',
                                         'entry_id': 'Native source identifier carried unchanged from the '
                                                     'public parent; joins one lifecycle row to that '
                                                     'research row.',
                                         'episode_encounter_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                         'episode_encounter_reltn_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                         'episode_id': 'Native source identifier carried unchanged from the '
                                                       'public parent; joins one lifecycle row to that '
                                                       'research row.',
                                         'episode_id_resolved': 'True when episode_id is null or resolves to '
                                                                'the governed episode spine; false records '
                                                                'unresolved linkage without dropping the '
                                                                'row.',
                                         'episode_key': 'Deterministic v2 key carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                         'event_after_death_30d': 'True when the event timestamp is more '
                                                                  "than 30 days after the resolved person's "
                                                                  'death timestamp; silver records the doubt '
                                                                  'and does not filter the row.',
                                         'event_before_birth': 'True when the event timestamp precedes the '
                                                               "resolved person's birth timestamp; silver "
                                                               'records the doubt and does not filter the '
                                                               'row.',
                                         'event_id': 'Native source identifier carried unchanged from the '
                                                     'public parent; joins one lifecycle row to that '
                                                     'research row.',
                                         'extract_cd': 'Primary-key component carried unchanged from the '
                                                       "public parent; combine it with the companion's other "
                                                       'key columns when joining.',
                                         'fhx_activity_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                         'gene_tested_row_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                         'genetic_result_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                         'genetic_test_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                         'homecare_request_item_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                         'identity_status': 'Subject identity resolution state recorded by '
                                                            'the silver canonical query.',
                                         'implant_event_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                         'implant_sequence': 'Primary-key component carried unchanged from '
                                                             'the public parent; combine it with the '
                                                             "companion's other key columns when joining.",
                                         'indication_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                         'is_illogical': 'Source RTT chronology flag indicating an illogical '
                                                         'activity sequence; retained for the gold drop '
                                                         'rule.',
                                         'journey_key': 'Deterministic v2 key carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                         'journey_link_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
                                         'journey_participant_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                         'labour_delivery_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                         'load_batch_id': 'Deterministic batch token derived from the source '
                                                          "row's bronze load timestamp.",
                                         'loaded_at': 'Latest bronze load timestamp contributing to the '
                                                      "public row; never the lifecycle pipeline's wall-clock "
                                                      'time.',
                                         'location_code': 'Primary-key component carried unchanged from the '
                                                          "public parent; combine it with the companion's "
                                                          'other key columns when joining.',
                                         'location_key': 'Deterministic v2 key carried unchanged from the '
                                                         'public parent; joins one lifecycle row to that '
                                                         'research row.',
                                         'location_stay_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                         'mc_device_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                         'microbiology_isolate_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                         'order_id': 'Native source identifier carried unchanged from the '
                                                     'public parent; joins one lifecycle row to that '
                                                     'research row.',
                                         'organization_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                         'organization_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
                                         'pacs_examination_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                         'pacs_report_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                         'pathway_oid': 'Native source identifier carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                         'patient_event_key': 'Deterministic SHA-256 event key carried '
                                                              'unchanged from the public parent; joins one '
                                                              'lifecycle row to that event row.',
                                         'patient_event_row_key': 'Deterministic row-level key carried '
                                                                  'unchanged from events_patient_event; '
                                                                  'joins one lifecycle row to one '
                                                                  'event-index row.',
                                         'period_business_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                         'period_oid': 'Native source identifier carried unchanged from the '
                                                       'public parent; joins one lifecycle row to that '
                                                       'research row.',
                                         'person_address_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                         'person_attribute_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                         'person_death_evidence_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                         'person_id': 'Native source identifier carried unchanged from the '
                                                      'public parent; joins one lifecycle row to that '
                                                      'research row.',
                                         'person_id_resolved': 'True when person_id is null or resolves to '
                                                               'the governed person spine; false records '
                                                               'unresolved linkage without dropping the row.',
                                         'person_identifier_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                         'person_relationship_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                         'pharmacy_issue_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                         'pm_wait_list_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                         'practitioner_group_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                         'practitioner_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                         'practitioner_identifier_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                         'practitioner_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
                                         'practitioner_location_evidence_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                         'pregnancy_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                         'pregnancy_reconciliation_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                         'problem_id': 'Native source identifier carried unchanged from the '
                                                       'public parent; joins one lifecycle row to that '
                                                       'research row.',
                                         'procedure_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                         'pt_prot_reg_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                         'ptl_unique_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                         'record_status': 'active, superseded or retracted, normalised from '
                                                          "the source's own status fields; superseded rows "
                                                          'are earlier versions the source kept.',
                                         'record_status_effective_from': 'Timestamp from which the '
                                                                         'normalised record status applies, '
                                                                         'when the source supplies one.',
                                         'record_status_effective_to': 'Timestamp until which the normalised '
                                                                       'record status applies; null means no '
                                                                       'source end is known.',
                                         'referral_oid': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                         'report_version_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                         'request_thread_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                         'research_study_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                         'row_source': 'Physical bronze row source, CURRENT or HIST, carried '
                                                       'unchanged from the public parent; a key component '
                                                       'because the research table is at physical version '
                                                       'grain and patient_event_key is shared across an '
                                                       "entry's versions.",
                                         'rtt_activity_oid': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                         'sch_event_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                         'score_type': 'Primary-key component carried unchanged from the '
                                                       "public parent; combine it with the companion's other "
                                                       'key columns when joining.',
                                         'sequence_nbr': 'Primary-key component carried unchanged from the '
                                                         "public parent; combine it with the companion's "
                                                         'other key columns when joining.',
                                         'service_id': 'Native source identifier carried unchanged from the '
                                                       'public parent; joins one lifecycle row to that '
                                                       'research row.',
                                         'service_key': 'Deterministic v2 key carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                         'snapshot_date': 'Primary-key component carried unchanged from the '
                                                          "public parent; combine it with the companion's "
                                                          'other key columns when joining.',
                                         'source_daily_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                         'source_identifier_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                         'source_object': 'Source-arm discriminator carried unchanged from '
                                                          'the public parent; use it with arm-specific '
                                                          'native identifiers when joining.',
                                         'source_site': 'Primary-key component carried unchanged from the '
                                                        "public parent; combine it with the companion's "
                                                        'other key columns when joining.',
                                         'source_system_oid': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                         'source_unit': 'Primary-key component carried unchanged from the '
                                                        "public parent; combine it with the companion's "
                                                        'other key columns when joining.',
                                         'source_update_timestamp': 'Native source-system update timestamp '
                                                                    'carried through the canonical silver '
                                                                    'query.',
                                         'source_version_id': 'Native source version identifier carried '
                                                              'unchanged from the public parent; CURRENT '
                                                              'rows carry -1. A key component alongside '
                                                              'row_source.',
                                         'surg_case_proc_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                         'susceptibility_result_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                         'theatre_attendance_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                         'theatre_case_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
                                         'theatre_case_milestone_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                         'theatre_implant_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                         'transfusion_event_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                         'transfusion_key': 'Deterministic v2 key carried unchanged from the '
                                                            'public parent; joins one lifecycle row to that '
                                                            'research row.',
                                         'treatment_cycle_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                         'treatment_key': 'Deterministic v2 key carried unchanged from the '
                                                          'public parent; joins one lifecycle row to that '
                                                          'research row.',
                                         'update_count': 'Primary-key component carried unchanged from the '
                                                         "public parent; combine it with the companion's "
                                                         'other key columns when joining.',
                                         'valid_from_datetime': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                         'value_set_key': 'Deterministic v2 key carried unchanged from the '
                                                          'public parent; joins one lifecycle row to that '
                                                          'research row.',
                                         'waiting_list_oid': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                         'waiting_list_snapshot_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.'},
                             'table': 'Internal quality and batch metadata for text_document; same row grain '
                                      'as the research table. Join keys: patient_event_key, source_object, '
                                      'endobase_exam_id, event_id, update_count, valid_from_datetime, '
                                      'pacs_report_id, report_version_id.'},
 '_text_document_primitive': {'columns': {},
                              'table': 'Internal document union with in-graph corpus frequency and thread '
                                       'metadata.'},
 '_text_qc_document': {'columns': {}, 'table': 'Private JSON bridge and Gold cross-rule flags for document.'},
 'text_document': {'columns': {'assembly_status': 'Source-row assembly outcome; NULL for parked SCD '
                                                  'working-copy rows.',
                               'author_id_system': 'Identifier system for the verbatim source author '
                                                   'identifier.',
                               'author_practitioner_id': 'Millennium personnel PERSON_ID when the author '
                                                         'resolves to Millennium personnel.',
                               'author_role': 'Source author role when supplied.',
                               'author_source_id': 'Verbatim source author identifier retained separately '
                                                   'from any resolved practitioner reference.',
                               'author_system_ind': 'True when the source author_practitioner_id was 1, the '
                                                    'Cerner system user (J5); the id is then published as '
                                                    'NULL. NULL when the source id was NULL. INSR_W2.',
                               'chunk_count': 'Number of source chunks represented by this document row; '
                                              'NULL for parked SCD working-copy rows.',
                               'confidentiality_code': 'Source confidentiality classification.',
                               'content_class': 'Deterministic retained-text quality class; rows are flagged '
                                                'rather than dropped.',
                               'content_type': 'Source MIME or content type.',
                               'contributor_system': 'Source contributor-system display such as PowerChart '
                                                     'or BLT_TIE_RAD.',
                               'corpus_frequency': 'Number of current Journey document rows sharing '
                                                   'text_sha256 when the governed release-built frequency is '
                                                   'at least 100; NULL means below that storage floor.',
                               'date_quality': 'Event-date quality relative to source provenance: null, '
                                               'pre-1975 epoch sentinel, future beyond source/load time plus '
                                               'one day, or ok.',
                               'decompressor_version': 'Decompressor version used by bronze.',
                               'document_class': 'Source event-class display such as Document or mdoc.',
                               'document_text': 'Identifiable source document text under default-deny '
                                                'access.',
                               'document_text_anonymised': 'Reserved NULL field; de-identification is '
                                                           'applied at serve time.',
                               'document_thread_key': 'Deterministic SHA-256 key for all versions of one '
                                                      'logical document.',
                               'document_type_source_code': 'Source code for the document type, retained '
                                                            'without de-identification.',
                               'document_type_source_display': 'Source display for the document type.',
                               'document_type_source_system': 'Canonical coding-system URI for the source '
                                                              'document type.',
                               'encoding': 'Source text encoding',
                               'encounter_id': 'Millennium ENCNTR_ID when supplied; native encounter foreign '
                                               'key.',
                               'endobase_exam_id': 'ENDOBASE_EXAM_ID; native key for the endobase_exam arm.',
                               'event_datetime': 'Document clinical or version timestamp.',
                               'event_datetime_local': 'event_datetime in Europe/London wall-clock time '
                                                       '(GMT/BST). Millennium rows take the bronze *_LOCAL '
                                                       'companions of the same source columns and fallback '
                                                       'order; rows from local-clock sources (LUNA, '
                                                       'PACS/DICOM, TFC LIMS, CCMDS, MSDS, BadgerNet, '
                                                       'EndoBase, SLAM, iWeb, Datix and similar) carry '
                                                       'event_datetime unchanged. event_datetime is a UTC '
                                                       'instant for Millennium rows, so take local calendar '
                                                       'dates and clock times from this column.',
                               'event_end_datetime': 'Source version-validity end.',
                               'event_end_datetime_local': 'event_end_datetime in Europe/London wall-clock '
                                                           'time (GMT/BST), by the same rule as '
                                                           'event_datetime_local; null where '
                                                           'event_end_datetime is null.',
                               'event_id': 'Millennium document EVENT_ID; native key for the mill_blob arm.',
                               'is_boilerplate': 'True exactly when governed corpus_frequency is at least '
                                                 '1000; absent frequency is false.',
                               'is_latest_version': 'Exactly one row per document_thread_id selected by '
                                                    'non-empty text then effective-from and source version '
                                                    'ordinal descending with version_id descending as '
                                                    'deterministic tiebreak.',
                               'language': 'Document language defaulted to English (`en`) because no source '
                                           'language field is available; method=default.',
                               'linkage_route': 'Deterministic provenance route used to resolve document '
                                                'subject or encounter linkage.',
                               'loaded_at': 'Per-arm contributing load clock: text events use own '
                                            'map_text_events.ADC_UPDT; deduplicated blob versions use the '
                                            'selected mill_blob_text.ADC_UPDT; PACS reports use own report '
                                            'ADC_UPDT, excluding bridge, examination and linkage clocks; '
                                            'neonatal narratives and pathology versions use own ADC_UPDT, '
                                            'excluding joined episode, accession, identity and alias clocks. '
                                            'EndoBase takes greatest of maximum ADC_UPDT across retained '
                                            'nonblank source-present terms, joined exam ADC_UPDT and joined '
                                            'encounter ADC_UPDT. Order comments take greatest of '
                                            'SOURCE_COMMENT_ADC_UPDT, SOURCE_TEXT_ADC_UPDT, '
                                            'PIPELINE_UPDT_DT_TM and joined order SOURCE_ADC_UPDT. '
                                            'Elective-access comments use own ADC_UPDT. Text hygiene, '
                                            'PRSB/organization enrichment, corpus frequency, thread-head '
                                            'selection and QC/public projections add no clock; current and '
                                            'historical pathology versions retain their own clocks. Null '
                                            'inputs follow the arm expression, and no current Silver refresh '
                                            'time is substituted.',
                               'organization_key': 'Deterministic organization key retained for mixed '
                                                   'Millennium and PACS organization evidence.',
                               'pacs_report_id': 'PACS_REPORT_ID; native key for the pacs_report arm.',
                               'parent_relation': 'Event relation display such as Root or Child.',
                               'parser_version': 'Parser version that produced document_text or sections. '
                                                 'PACS reports whose text comes from the report-text bridge '
                                                 '(v4, re-sourced from mill_blob_text) carry the bridge '
                                                 'BRIDGED_TEXT_PARSER_VERSION, formatted '
                                                 'mill_blob_text:d<decompressor>/p<parser>/pp<post-processor>; '
                                                 'pacs-bridge-v1 marks v3 bridge text.',
                               'patient_event_key': 'Deterministic SHA-256 event identity; stable cross-feed '
                                                    'join key.',
                               'person_id': 'Millennium PERSON_ID when resolved; native person foreign key.',
                               'post_processor_version': 'Post-processor version used by bronze.',
                               'prsb_document_type': 'PRSB-aligned canonical document type from the governed '
                                                     'doc_type_prsb_map lookup.',
                               'prsb_map_method': 'Mapping method provenance.',
                               'prsb_map_score': 'Embedder cosine score for embedder-method rows.',
                               'prsb_map_version': 'doc_type_prsb_map version label.',
                               'prsb_setting': 'Care-setting bucket of the canonical type.',
                               'prsb_standard': 'Source PRSB or Royal-College standard label for the '
                                                'canonical type.',
                               'prsb_subtype': 'Canonical subtype qualifier when the mapping supplies one.',
                               'raw_content_sha256': 'SHA-256 of the original binary payload when supplied '
                                                     'by the blob source; distinct from text_sha256.',
                               'record_status': 'Arm-specific status, with a final text override: after '
                                                'document text hygiene, lower(trim(document_text)) equal to '
                                                'deleted forces retracted. Otherwise text-event rows use '
                                                'SOURCE_DELETED_IND (null false) for retraction, then a '
                                                'validity end before 2100-01-01 or AUTHENTIC_FLAG zero (null '
                                                'one) for supersession. Blob rows retract for result-status '
                                                'codes 29, 30 or 31, otherwise supersede for a validity end '
                                                'before 2100. PACS and neonatal narratives retract for false '
                                                'source presence (null true). EndoBase supersedes only when '
                                                'a joined parent exam exists and is source-absent. Order '
                                                'comments supersede for TEXT_ACTIVE_IND zero (null one). '
                                                'Elective-access comments retract for source absence, '
                                                'otherwise supersede for inactive status (both null true). '
                                                'Pathology retracts cancelled/entered_in_error versions, '
                                                'otherwise uses active for is_current true and superseded '
                                                'for false/null. Other rows are active. This is not a '
                                                'verbatim closure flag or the independently selected '
                                                'latest-version marker.',
                               'record_status_effective_from': 'Source-arm history start, unchanged by text '
                                                               'hygiene and thread-head selection. Text '
                                                               'events: first '
                                                               'STRING_RESULT_VALID_FROM_DT_TM, '
                                                               'CLINICAL_EVENT_VALID_FROM_DT_TM, then '
                                                               'clinical time falling back through '
                                                               'RESULT_DT_TM, PERFORMED_DT_TM, '
                                                               'EVENT_END_DT_TM and EVENT_START_DT_TM. Blob '
                                                               'versions: first VALID_FROM_DT_TM, '
                                                               'UPDT_DT_TM, ADC_UPDT. PACS: SRC_ADC_UPDT. '
                                                               'EndoBase: parent exam SOURCE_CREATE_TS then '
                                                               'earliest retained term CREATED_TS. Order '
                                                               'comments: first COMMENT_DT_TM_CLEAN, '
                                                               'COMMENT_UPDT_DT_TM_CLEAN, parent '
                                                               'ORIG_ORDER_DT_TM_CLEAN, parent '
                                                               'CURRENT_START_DT_TM_CLEAN, COMMENT_DT_TM, '
                                                               'COMMENT_UPDT_DT_TM. Elective-access '
                                                               'comments: CREATED_DT_TM. Neonatal '
                                                               'narratives: parent episode DischTime_CLEAN '
                                                               'then AdmitTime_CLEAN. Pathology versions: '
                                                               'valid_from. No common clinical-time clamp or '
                                                               'current refresh timestamp is added.',
                               'record_status_effective_to': 'Arm-specific end before text hygiene: '
                                                             'text-event retractions use their computed '
                                                             'source_update_timestamp; supersessions use the '
                                                             'least string-result/event validity end, which '
                                                             'can be null or a sentinel for '
                                                             'AUTHENTIC_FLAG-only supersession. Blob rows '
                                                             'use VALID_UNTIL_DT_TM only when it is non-null '
                                                             'and before 2100-01-01; a result-status-only '
                                                             'retraction can have no end. Source-absent PACS '
                                                             'and neonatal rows use own ADC_UPDT. '
                                                             'Source-absent joined EndoBase exams use the '
                                                             'maximum retained-term, exam and encounter '
                                                             'ADC_UPDT. Inactive order comments use '
                                                             'TEXT_UPDT_DT_TM_CLEAN. Absent/inactive '
                                                             'elective-access comments use first '
                                                             'MODIFIED_DT_TM, CREATED_DT_TM, '
                                                             'WAITING_LIST_STATUS_CHANGE_DT_TM_CLEAN, '
                                                             'DECIDED_TO_ADMIT_DT_TM_CLEAN, TCI_DT_TM_CLEAN. '
                                                             'Pathology carries valid_to directly. Other arm '
                                                             'cases have null ends. Finally, text equal to '
                                                             'deleted after hygiene forces first existing '
                                                             'end, source_update_timestamp, loaded_at; it '
                                                             'does not guarantee a non-null end. This is not '
                                                             'a clinical document completion time.',
                               'report_version_id': 'Pathology report_version_id; native key for the '
                                                    'pathology_report arm.',
                               'result_status_code': 'Millennium result status code of the document event '
                                                     '(code set 8): mill_blob EVENT_RESULT_STATUS_CD, '
                                                     'text_event RESULT_STATUS_CD; NULL on other arms. '
                                                     'BBR_FOLDIN_V1 BBR-17.',
                               'result_status_display': 'Result status display (Auth (Verified), Modified, '
                                                        "In Error ...): the document's finality. mill_blob "
                                                        'carries the source display; text_event codes are '
                                                        'decoded through code set 8. BBR_FOLDIN_V1 BBR-17.',
                               'service_id': 'Service reference when supplied.',
                               'source_class': 'Fail-closed source-content class; the public document '
                                               'product contains clinical text only.',
                               'source_code': 'Verbatim source document-type code.',
                               'source_coding_system': 'Verbatim source document-type system.',
                               'source_display': 'Verbatim source document-type display.',
                               'source_feed': 'Registered owning source route; all document text is '
                                              'IG-sensitive clinical content.',
                               'source_link_event_id': 'Verbatim linked source event identifier when a '
                                                       'governed cross-feed bridge supplies one. PACS '
                                                       'reports: the Cerner report document EVENT_ID of the '
                                                       'version-safe bridge row; source-absent bridge '
                                                       'tombstones are not read. Imaging report '
                                                       'relationships are in clinical_imaging_report_link.',
                               'source_link_system': 'Identifier system for source_link_event_id.',
                               'source_object': 'Native document source arm; contract arms are '
                                                'endobase_exam, mill_blob, pacs_report, and '
                                                'pathology_report.',
                               'source_organization_display': 'Source-organization display from the governed '
                                                              'organization dimension or verbatim PACS '
                                                              'institution.',
                               'source_organization_key': 'Deterministic source-organization key from '
                                                          'governed source evidence.',
                               'source_parent_display': 'Parent event-code description; the legacy '
                                                        'MainEventDesc document-type label.',
                               'source_parent_event_id': 'Parent clinical-event identifier for document '
                                                         'threading when supplied.',
                               'source_parent_tag': 'Parent event tag text; the legacy MainTagText.',
                               'source_parent_title': 'Parent event title text; the legacy MainTitleText.',
                               'source_record_status': 'Clinical-event record-status display such as Active '
                                                       'or Deleted; the legacy Status.',
                               'source_tag': 'Own event tag text; the legacy ChildTagText.',
                               'source_update_timestamp': 'Source-arm-specific timestamp; interpret with '
                                                          'source_feed. text_event takes the greatest '
                                                          'STRING_RESULT_EFFECTIVE_UPDT_DT_TM, '
                                                          'CLINICAL_EVENT_ADC_UPDT, LONG_TEXT_ADC_UPDT, '
                                                          'LOOKUP_ADC_UPDT and ADC_UPDT from '
                                                          'map_text_events. mill_blob_text uses UPDT_DT_TM; '
                                                          'pacs_report uses REPORT_MODIFIED_UTC. '
                                                          'endobase_exam takes the greatest contributing '
                                                          'term ADC_UPDT and parent-exam '
                                                          'PIPELINE_UPDT_DT_TM. order_comment uses '
                                                          'TEXT_UPDT_DT_TM_CLEAN, falling back to '
                                                          'COMMENT_UPDT_DT_TM_CLEAN. elective_access_comment '
                                                          'uses MODIFIED_DT_TM; neonatal_episode_narrative '
                                                          'uses ADC_UPDT; pathology_report uses valid_from. '
                                                          'This mixes source, version and processing clocks; '
                                                          'it is not a uniform clinical event time or '
                                                          'native-system update time.',
                               'status_code': "mill_blob arm: decoder status, always 'Decoded'; text_event "
                                              'arm: raw Millennium result status code (see '
                                              'result_status_display); other arms: the source artifact '
                                              'status. BBR_FOLDIN_V1 BBR-17.',
                               'subject_id_system': 'Identifier system used for subject_key.',
                               'subject_key': 'Deterministic SHA-256 over the strongest source identifier. '
                                              'Not salted, not secret; a stable join key across feeds where '
                                              'person_id is unresolved.',
                               'succession_status': "Cerner succession type display; '? Unknown' is a source "
                                                    'value. Not document finality: use '
                                                    'result_status_display. BBR_FOLDIN_V1 BBR-17.',
                               'supersedes_document_key': 'Deterministic SHA-256 key of the directly '
                                                          'superseded document version.',
                               'text_is_anonymised': 'Always false in silver; de-identification is applied '
                                                     'at serve time.',
                               'text_is_truncated': 'True when retained text length equals a known source or '
                                                    'parser cap (1000000, 65535, 32767, or 32000 '
                                                    'characters).',
                               'text_length': 'Retained document-text character count recomputed after '
                                              'parsing and post-processing.',
                               'text_sha256': 'SHA-256 of retained document_text only; the anonymous '
                                              'alternative never changes this hash or any stable identifier. '
                                              'NULL is represented by the empty-string digest.',
                               'title': 'Source document title or label.',
                               'update_count': 'De-identified version of the decoded blob text in which '
                                               'detected patient identifiers have been replaced with '
                                               'placeholder tokens.',
                               'valid_from_datetime': 'De-identified version of the decoded blob text in '
                                                      'which detected patient identifiers have been replaced '
                                                      'with placeholder tokens.',
                               'verified_datetime': 'Source verification timestamp associated with '
                                                    'verified_practitioner_id.',
                               'verified_practitioner_id': 'Millennium personnel PERSON_ID when the verifier '
                                                           'resolves to Millennium personnel.',
                               'version_id': 'Source document-version token.',
                               'version_ordinal': 'Source-supplied version ordinal when available; one for '
                                                  'single-version lanes.',
                               'vip_ind': 'VIP indicator when supplied.',
                               'withheld_identity_ind': 'Withheld-identity indicator when supplied.'},
                   'table': 'Identifiable clinical document versions with source thread and parser '
                            'provenance.'}}
