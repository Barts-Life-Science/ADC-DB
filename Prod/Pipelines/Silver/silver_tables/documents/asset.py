"""Silver documents.asset: 2 dataset(s).

- datasets: artifact_asset, _artifact_asset_metadata
- moved verbatim from: 5_silver_documents:107, 5_silver_documents:164, 5_silver_documents:231, 5_silver_documents:236, 5_silver_documents:252, 5_silver_documents:46, 5_silver_documents:917

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.cross_qc import _s3_flatten_codeable_json
from silver_core.journey_core import _n, canonical_coding_system
from silver_tables._common.journey import (
    S3_TABLE_AXIS_SPECS,
    _artifact_asset_canonical,
    _s3_replace_public_variant,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Artifact / asset

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
    "event_datetime_local",
    "event_end_datetime_local",
    # TZ_SILVER_LOCAL_V1: Europe/London companions (bronze *_LOCAL for Millennium; local-clock sources unchanged)
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
    'event_datetime_local',
    'event_end_datetime_local',
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


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_artifact_asset():
    # Assemble artifact asset rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return _artifact_asset_canonical().select(*ARTIFACT_ASSET_SOURCE_COLUMNS)


@materialized_view(
    name=_n("journey_artifact.asset"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
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
    refresh_policy="auto",
)
def artifact_asset_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for artifact_asset; same
    # row grain as the research table. Join keys: patient_event_key, source_object, dicom_path,
    # pacs_examination_id.
    return (_lifecycle_source_artifact_asset()).select(*ARTIFACT_ASSET_LIFECYCLE_COLUMNS)


# Finalise the public column lists used by this notebook.
for old_name, axes in S3_TABLE_AXIS_SPECS["artifact_asset"].items():
    _s3_replace_public_variant(ARTIFACT_ASSET_PUBLIC_COLUMNS, old_name, axes)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_artifact_asset_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged from the '
                                                           "public parent; combine it with the companion's "
                                                           'other key columns when joining.',
                                          'activity_record_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                          'admission_key': 'Deterministic v2 key carried unchanged from the '
                                                           'public parent; joins one lifecycle row to that '
                                                           'research row.',
                                          'allergy_instance_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                          'aria_dx_id': 'Native source identifier carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                          'aria_pt_id': 'Native source identifier carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                          'artifact_link_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                          'birth_row_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                          'bloodtrack_transaction_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                          'cancer_treatment_cycle_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                          'care_contact_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
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
                                          'concept_map_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
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
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                          'dicom_path': 'Primary-key component carried unchanged from the '
                                                        "public parent; combine it with the companion's "
                                                        'other key columns when joining.',
                                          'doc_response_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                          'elective_access_procedure_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                          'encounter_attribute_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                          'encounter_bounds_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                          'encounter_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                          'encounter_id_resolved': 'True when encounter_id is null or '
                                                                   'resolves to the governed encounter '
                                                                   'spine; false records unresolved linkage '
                                                                   'without dropping the row.',
                                          'encounter_identifier_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
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
                                          'episode_id_resolved': 'True when episode_id is null or resolves '
                                                                 'to the governed episode spine; false '
                                                                 'records unresolved linkage without '
                                                                 'dropping the row.',
                                          'episode_key': 'Deterministic v2 key carried unchanged from the '
                                                         'public parent; joins one lifecycle row to that '
                                                         'research row.',
                                          'event_after_death_30d': 'True when the event timestamp is more '
                                                                   "than 30 days after the resolved person's "
                                                                   'death timestamp; silver records the '
                                                                   'doubt and does not filter the row.',
                                          'event_before_birth': 'True when the event timestamp precedes the '
                                                                "resolved person's birth timestamp; silver "
                                                                'records the doubt and does not filter the '
                                                                'row.',
                                          'event_id': 'Native source identifier carried unchanged from the '
                                                      'public parent; joins one lifecycle row to that '
                                                      'research row.',
                                          'extract_cd': 'Primary-key component carried unchanged from the '
                                                        "public parent; combine it with the companion's "
                                                        'other key columns when joining.',
                                          'fhx_activity_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                          'gene_tested_row_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                          'genetic_result_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                          'genetic_test_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
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
                                          'is_illogical': 'Source RTT chronology flag indicating an '
                                                          'illogical activity sequence; retained for the '
                                                          'gold drop rule.',
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
                                          'load_batch_id': 'Deterministic batch token derived from the '
                                                           "source row's bronze load timestamp.",
                                          'loaded_at': 'Latest bronze load timestamp contributing to the '
                                                       "public row; never the lifecycle pipeline's "
                                                       'wall-clock time.',
                                          'location_code': 'Primary-key component carried unchanged from the '
                                                           "public parent; combine it with the companion's "
                                                           'other key columns when joining.',
                                          'location_key': 'Deterministic v2 key carried unchanged from the '
                                                          'public parent; joins one lifecycle row to that '
                                                          'research row.',
                                          'location_stay_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                          'mc_device_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                          'microbiology_isolate_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                          'order_id': 'Native source identifier carried unchanged from the '
                                                      'public parent; joins one lifecycle row to that '
                                                      'research row.',
                                          'organization_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                          'organization_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                          'pacs_examination_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                          'pacs_report_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                          'pathway_oid': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                          'patient_event_key': 'Deterministic SHA-256 event key carried '
                                                               'unchanged from the public parent; joins one '
                                                               'lifecycle row to that event row.',
                                          'patient_event_row_key': 'Deterministic row-level key carried '
                                                                   'unchanged from events_patient_event; '
                                                                   'joins one lifecycle row to one '
                                                                   'event-index row.',
                                          'period_business_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                                                'unresolved linkage without dropping the '
                                                                'row.',
                                          'person_identifier_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                          'person_relationship_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                          'pharmacy_issue_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                          'pm_wait_list_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                          'practitioner_group_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                          'practitioner_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                          'practitioner_identifier_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                          'practitioner_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                          'practitioner_location_evidence_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                          'pregnancy_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                          'pregnancy_reconciliation_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                          'problem_id': 'Native source identifier carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
                                                        'research row.',
                                          'procedure_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
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
                                          'record_status_effective_to': 'Timestamp until which the '
                                                                        'normalised record status applies; '
                                                                        'null means no source end is known.',
                                          'referral_oid': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                          'report_version_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                          'request_thread_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                          'research_study_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                          'row_source': 'Physical bronze row source, CURRENT or HIST, '
                                                        'carried unchanged from the public parent; a key '
                                                        'component because the research table is at physical '
                                                        'version grain and patient_event_key is shared '
                                                        "across an entry's versions.",
                                          'rtt_activity_oid': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                          'sch_event_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                          'score_type': 'Primary-key component carried unchanged from the '
                                                        "public parent; combine it with the companion's "
                                                        'other key columns when joining.',
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
                                          'source_daily_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                          'source_identifier_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
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
                                          'theatre_implant_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                          'transfusion_event_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                          'transfusion_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
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
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
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
                              'table': 'Internal quality and batch metadata for artifact_asset; same row '
                                       'grain as the research table. Join keys: patient_event_key, '
                                       'source_object, dicom_path, pacs_examination_id.'},
 'artifact_asset': {'columns': {'acquisition_datetime': 'Source acquisition timestamp.',
                                'archive_status_code': 'Source archive-state code when supplied.',
                                'artifact_class': 'High-level payload class.',
                                'artifact_key': 'Deterministic SHA-256 artifact identity.',
                                'artifact_level': 'Artifact granularity represented by the row.',
                                'artifact_type_source_code': 'Source code for the artifact type, retained '
                                                             'without de-identification.',
                                'artifact_type_source_display': 'Source display for the artifact type.',
                                'artifact_type_source_system': 'Canonical coding-system URI for the source '
                                                               'artifact type.',
                                'availability_status': 'Whether Silver has metadata only or a governed '
                                                       'retrievable payload.',
                                'body_site_code': 'Body-site code when supplied.',
                                'burned_in_pii_tier': 'Upstream burned-in-PII classification when supplied.',
                                'byte_size': 'Represented payload size in bytes when supplied.',
                                'confidentiality_code': 'Source confidentiality classification.',
                                'content_type': 'MIME type or governed source format.',
                                'dicom_path': 'DICOM_PATH; native key for the dicom_file arm.',
                                'encounter_id': 'Nullable Millennium ENCNTR_ID; these artifact lanes do not '
                                                'supply encounter context.',
                                'event_datetime': 'Artifact acquisition or creation timestamp.',
                                'event_datetime_local': 'event_datetime in Europe/London wall-clock time '
                                                        '(GMT/BST). Millennium rows take the bronze *_LOCAL '
                                                        'companions of the same source columns and fallback '
                                                        'order; rows from local-clock sources (LUNA, '
                                                        'PACS/DICOM, TFC LIMS, CCMDS, MSDS, BadgerNet, '
                                                        'EndoBase, SLAM, iWeb, Datix and similar) carry '
                                                        'event_datetime unchanged. event_datetime is a UTC '
                                                        'instant for Millennium rows, so take local calendar '
                                                        'dates and clock times from this column.',
                                'event_end_datetime': 'Artifact acquisition end timestamp when supplied.',
                                'event_end_datetime_local': 'event_end_datetime in Europe/London wall-clock '
                                                            'time (GMT/BST), by the same rule as '
                                                            'event_datetime_local; null where '
                                                            'event_end_datetime is null.',
                                'file_name': 'Source file name when a governed manifest supplies one.',
                                'folder_count': 'Source folder count when supplied.',
                                'ingest_run_id': 'Upstream manifest or ingest-run identifier when supplied.',
                                'last_accessed_datetime': 'Source-reported last-access timestamp when '
                                                          'supplied.',
                                'loaded_at': 'map_pacs_examination.ADC_UPDT for PACS-study assets; '
                                             'map_dicom_file_attribute.PIPELINE_UPDT_DT_TM for DICOM-file '
                                             "assets. The union carries each arm's ingestion/processing "
                                             'clock unchanged; it does not compute a maximum across studies '
                                             'and files. This is not image acquisition or Silver refresh '
                                             'time.',
                                'locator_system': 'Namespace of locator_value.',
                                'locator_value': 'Stable source locator such as a DICOM Study Instance UID.',
                                'modality_code': 'Imaging or acquisition modality when supplied.',
                                'object_count': 'Number of image or file objects represented when supplied.',
                                'organization_key': 'Nullable deterministic organization key; no '
                                                    'organization expression is present in the artifact '
                                                    'source cells.',
                                'pacs_examination_id': 'PACS_EXAMINATION_ID; native key for the pacs_study '
                                                       'arm.',
                                'parent_artifact_key': 'Deterministic SHA-256 key of the containing '
                                                       'artifact.',
                                'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                     'cross-feed join key.',
                                'payload_present_ind': 'Source evidence that binary or pixel payload exists.',
                                'person_id': 'Millennium PERSON_ID when resolved; native person foreign key.',
                                'record_status': 'Derived separately for PACS-study and DICOM-file assets: '
                                                 "retracted when that arm's SOURCE_PRESENT_IND is false, "
                                                 'otherwise active; a null presence flag does not retract '
                                                 'the row. Neither arm emits superseded. This row-status '
                                                 'label is distinct from pixel/file availability.',
                                'record_status_effective_from': 'Source provenance timestamp used as the '
                                                                'history start: '
                                                                'map_pacs_examination.SRC_ADC_UPDT for PACS '
                                                                'studies, or '
                                                                'map_dicom_file_attribute.SOURCE_MAX_EXTRACTION_TS '
                                                                'for DICOM files. It is not the image '
                                                                'acquisition time or an independently '
                                                                'recorded status transition; missing values '
                                                                'remain null.',
                                'record_status_effective_to': 'For retracted assets only, '
                                                              'map_pacs_examination.ADC_UPDT on the '
                                                              'PACS-study arm or '
                                                              'map_dicom_file_attribute.PIPELINE_UPDT_DT_TM '
                                                              'on the DICOM-file arm. Null for active assets '
                                                              "or when that arm's processing clock is "
                                                              'unavailable. This processing-time end proxy '
                                                              'is not a Boolean presence indicator.',
                                'series_count': 'Number of series represented by the artifact when supplied.',
                                'series_instance_uid': 'DICOM Series Instance UID when supplied.',
                                'sha256': 'Payload SHA-256 when a governed serving manifest supplies one.',
                                'sop_instance_uid': 'DICOM SOP Instance UID when supplied.',
                                'source_artifact_id': 'Native source identifier for the represented artifact '
                                                      'or study.',
                                'source_code': 'Verbatim source procedure or artifact code.',
                                'source_coding_system': 'Verbatim source procedure or artifact coding '
                                                        'system.',
                                'source_display': 'Verbatim source procedure or artifact display.',
                                'source_feed': 'Registered artifact source route.',
                                'source_object': 'Native source arm: pacs_study or dicom_file.',
                                'source_update_timestamp': 'map_pacs_examination.SRC_ADC_UPDT for PACS-study '
                                                           'assets; '
                                                           'map_dicom_file_attribute.SOURCE_MAX_EXTRACTION_TS '
                                                           'for DICOM-file assets. These arm-specific source '
                                                           'provenance clocks are carried unchanged, not '
                                                           'combined across the union or replaced by Silver '
                                                           'refresh time; missing values remain null.',
                                'status_code': 'Source artifact status.',
                                'storage_uri': 'Governed retrievable location when a serving manifest '
                                               'supplies one; never invented by Silver.',
                                'study_instance_uid': 'DICOM Study Instance UID when supplied.',
                                'subject_id_system': 'Identifier system behind subject_key.',
                                'subject_key': 'Deterministic SHA-256 over the strongest source identifier. '
                                               'Not salted, not secret; a stable join key across feeds where '
                                               'person_id is unresolved.',
                                'vip_ind': 'Source VIP indicator.',
                                'withheld_identity_ind': 'Source withheld-identity indicator.'},
                    'table': 'One governed clinical artifact or artifact-collection locator; payloads remain '
                             'outside Silver.'}}

# INSR_R4_SILVER_V1 (module_list): generated by r4/release/gen_silver_components.py; edit the generator, not this.
ARTIFACT_ASSET_PUBLIC_COLUMNS = list(ARTIFACT_ASSET_PUBLIC_COLUMNS) + ['identity_status']  # decision 1: contract columns, additions last
