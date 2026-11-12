"""Silver clinical.endoscopy_finding: 3 dataset(s).

- datasets: _clinical_endoscopy_finding_stage, clinical_endoscopy_finding, _clinical_endoscopy_finding_metadata
- moved verbatim from: 2_silver_clinical_events:113, 2_silver_clinical_events:7921, 2_silver_clinical_events:7941, 2_silver_clinical_events:7982, 2_silver_clinical_events:8020, 2_silver_clinical_events:8066, 2_silver_clinical_events:8073, 2_silver_clinical_events:8092

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.cross_qc import _s3_flatten_codeable_json
from silver_core.journey_core import _n, canonical_coding_system, spark
from silver_tables._common.journey import (
    S3_TABLE_AXIS_SPECS,
    _endoscopy_finding_canonical,
    _s3_lookup_axis,
    _s3_replace_public_variant,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


SRC_S3_ENDOBASE_TERM_MAP = "3_lookup.omop.endobase_term_snomed_map"


# Clinical /  endoscopy finding stage
@materialized_view(
    name=_n("journey_clinical._endoscopy_finding_stage"),
    private=True,
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def _endoscopy_finding_stage():
    # Build the declared dataset: Incremental boundary for joined Endobase finding scalars and
    # deterministic coding JSON.
    return _endoscopy_finding_canonical()


# Clinical / endoscopy finding

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
    "event_datetime_local",
    "event_end_datetime_local",
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
    'event_datetime_local',
    'event_end_datetime_local',
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


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_endoscopy_finding():
    # Assemble endoscopy finding rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    return (spark.read.table(_n("journey_clinical._endoscopy_finding_stage"))
            .withColumn("finding_code", F.parse_json(F.col("_finding_code_json")))
            .select(*ENDOSCOPY_FINDING_SOURCE_COLUMNS))


@materialized_view(
    name=_n("journey_clinical.endoscopy_finding"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
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
    refresh_policy="auto",
)
def endoscopy_finding_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_endoscopy_finding; same row grain as the research table. Join keys:
    # patient_event_key, endobase_exam_term_id.
    return (_lifecycle_source_endoscopy_finding()).select(*ENDOSCOPY_FINDING_LIFECYCLE_COLUMNS)


for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_endoscopy_finding"].items():
    _s3_replace_public_variant(ENDOSCOPY_FINDING_PUBLIC_COLUMNS, old_name, axes)


VALUE_POLICIES = {
    'clinical_endoscopy_finding': ValuePolicy(table='clinical_endoscopy_finding', enabled=True, zero_as_null=('encounter_id',)),
}


WRAP_POLICY = {'clinical_endoscopy_finding'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_endoscopy_finding_metadata': {'columns': {'activity_date': 'Primary-key component carried '
                                                                       'unchanged from the public parent; '
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                      'activity_record_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                      'admission_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                      'allergy_instance_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                      'aria_dx_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                      'aria_pt_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                      'artifact_link_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'birth_row_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'bloodtrack_transaction_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                      'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                      'care_contact_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'care_participation_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                      'cds_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                      'chemotherapy_course_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                      'community_care_activity_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                      'community_care_contact_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                      'concept_map_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'cost_line_item_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                      'dcp_forms_activity_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                      'device_mapping_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                      'diagnosis_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'dicom_path': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                                      'doc_response_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'elective_access_procedure_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                      'encounter_attribute_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                      'encounter_bounds_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                      'encounter_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'encounter_id_resolved': 'True when encounter_id is '
                                                                               'null or resolves to the '
                                                                               'governed encounter spine; '
                                                                               'false records unresolved '
                                                                               'linkage without dropping the '
                                                                               'row.',
                                                      'encounter_identifier_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                      'encounter_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                      'endobase_exam_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'endobase_exam_term_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                      'entity_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                      'entry_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                      'episode_encounter_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                      'episode_encounter_reltn_id': 'Native source '
                                                                                    'identifier carried '
                                                                                    'unchanged from the '
                                                                                    'public parent; joins '
                                                                                    'one lifecycle row to '
                                                                                    'that research row.',
                                                      'episode_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                      'episode_id_resolved': 'True when episode_id is null '
                                                                             'or resolves to the governed '
                                                                             'episode spine; false records '
                                                                             'unresolved linkage without '
                                                                             'dropping the row.',
                                                      'episode_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                      'event_after_death_30d': 'True when the event '
                                                                               'timestamp is more than 30 '
                                                                               'days after the resolved '
                                                                               "person's death timestamp; "
                                                                               'silver records the doubt and '
                                                                               'does not filter the row.',
                                                      'event_before_birth': 'True when the event timestamp '
                                                                            "precedes the resolved person's "
                                                                            'birth timestamp; silver records '
                                                                            'the doubt and does not filter '
                                                                            'the row.',
                                                      'event_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                      'extract_cd': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                                      'fhx_activity_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'gene_tested_row_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                      'genetic_result_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'genetic_test_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'homecare_request_item_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                      'identity_status': 'Subject identity resolution state '
                                                                         'recorded by the silver canonical '
                                                                         'query.',
                                                      'implant_event_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'implant_sequence': 'Primary-key component carried '
                                                                          'unchanged from the public parent; '
                                                                          "combine it with the companion's "
                                                                          'other key columns when joining.',
                                                      'indication_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                      'is_illogical': 'Source RTT chronology flag indicating '
                                                                      'an illogical activity sequence; '
                                                                      'retained for the gold drop rule.',
                                                      'journey_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                      'journey_link_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'journey_participant_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                      'labour_delivery_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                      'load_batch_id': 'Deterministic batch token derived '
                                                                       "from the source row's bronze load "
                                                                       'timestamp.',
                                                      'loaded_at': 'Latest bronze load timestamp '
                                                                   'contributing to the public row; never '
                                                                   "the lifecycle pipeline's wall-clock "
                                                                   'time.',
                                                      'location_code': 'Primary-key component carried '
                                                                       'unchanged from the public parent; '
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                      'location_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'location_stay_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'mc_device_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'microbiology_isolate_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                      'order_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                      'organization_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'organization_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'pacs_examination_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                      'pacs_report_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                      'pathway_oid': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                      'patient_event_key': 'Deterministic SHA-256 event key '
                                                                           'carried unchanged from the '
                                                                           'public parent; joins one '
                                                                           'lifecycle row to that event row.',
                                                      'patient_event_row_key': 'Deterministic row-level key '
                                                                               'carried unchanged from '
                                                                               'events_patient_event; joins '
                                                                               'one lifecycle row to one '
                                                                               'event-index row.',
                                                      'period_business_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                      'period_oid': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                      'person_address_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                      'person_attribute_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                      'person_death_evidence_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                      'person_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                      'person_id_resolved': 'True when person_id is null or '
                                                                            'resolves to the governed person '
                                                                            'spine; false records unresolved '
                                                                            'linkage without dropping the '
                                                                            'row.',
                                                      'person_identifier_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                      'person_relationship_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                      'pharmacy_issue_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'pm_wait_list_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'practitioner_group_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                      'practitioner_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'practitioner_identifier_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                      'practitioner_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'practitioner_location_evidence_key': 'Deterministic '
                                                                                            'v2 key carried '
                                                                                            'unchanged from '
                                                                                            'the public '
                                                                                            'parent; joins '
                                                                                            'one lifecycle '
                                                                                            'row to that '
                                                                                            'research row.',
                                                      'pregnancy_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'pregnancy_reconciliation_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                      'problem_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                      'procedure_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'pt_prot_reg_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                      'ptl_unique_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                      'record_status': 'active, superseded or retracted, '
                                                                       "normalised from the source's own "
                                                                       'status fields; superseded rows are '
                                                                       'earlier versions the source kept.',
                                                      'record_status_effective_from': 'Timestamp from which '
                                                                                      'the normalised record '
                                                                                      'status applies, when '
                                                                                      'the source supplies '
                                                                                      'one.',
                                                      'record_status_effective_to': 'Timestamp until which '
                                                                                    'the normalised record '
                                                                                    'status applies; null '
                                                                                    'means no source end is '
                                                                                    'known.',
                                                      'referral_oid': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'report_version_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'request_thread_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                      'research_study_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                      'row_source': 'Physical bronze row source, CURRENT or '
                                                                    'HIST, carried unchanged from the public '
                                                                    'parent; a key component because the '
                                                                    'research table is at physical version '
                                                                    'grain and patient_event_key is shared '
                                                                    "across an entry's versions.",
                                                      'rtt_activity_oid': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'sch_event_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'score_type': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                                      'sequence_nbr': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                      'service_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                      'service_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                      'snapshot_date': 'Primary-key component carried '
                                                                       'unchanged from the public parent; '
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                      'source_daily_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'source_identifier_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                      'source_object': 'Source-arm discriminator carried '
                                                                       'unchanged from the public parent; '
                                                                       'use it with arm-specific native '
                                                                       'identifiers when joining.',
                                                      'source_site': 'Primary-key component carried '
                                                                     'unchanged from the public parent; '
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
                                                      'source_system_oid': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'source_unit': 'Primary-key component carried '
                                                                     'unchanged from the public parent; '
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
                                                      'source_update_timestamp': 'Native source-system '
                                                                                 'update timestamp carried '
                                                                                 'through the canonical '
                                                                                 'silver query.',
                                                      'source_version_id': 'Native source version identifier '
                                                                           'carried unchanged from the '
                                                                           'public parent; CURRENT rows '
                                                                           'carry -1. A key component '
                                                                           'alongside row_source.',
                                                      'surg_case_proc_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'susceptibility_result_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                      'theatre_attendance_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                      'theatre_case_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'theatre_case_milestone_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                      'theatre_implant_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                      'transfusion_event_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                      'transfusion_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'treatment_cycle_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                      'treatment_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                      'update_count': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                      'valid_from_datetime': 'Primary-key component carried '
                                                                             'unchanged from the public '
                                                                             'parent; combine it with the '
                                                                             "companion's other key columns "
                                                                             'when joining.',
                                                      'value_set_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                      'waiting_list_oid': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'waiting_list_snapshot_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.'},
                                          'table': 'Internal quality and batch metadata for '
                                                   'clinical_endoscopy_finding; same row grain as the '
                                                   'research table. Join keys: patient_event_key, '
                                                   'endobase_exam_term_id.'},
 '_clinical_endoscopy_finding_stage': {'columns': {},
                                       'table': 'Incremental boundary for joined Endobase finding scalars '
                                                'and deterministic coding JSON.'},
 'clinical_endoscopy_finding': {'columns': {'authored_datetime': 'Creation timestamp of the EndoBase '
                                                                 'examination-term record, carried from '
                                                                 '`CREATED_TS`; not a report-level authoring '
                                                                 'timestamp.',
                                            'confidentiality_code': 'Security classification when supplied.',
                                            'confirmed_ind': 'Finding confirmation indicator.',
                                            'display_order': 'Source display order.',
                                            'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                            'available.',
                                            'endobase_exam_id': 'Endobase ENDOBASE_EXAM_ID; native parent '
                                                                'examination identifier.',
                                            'endobase_exam_term_id': 'Endobase ENDOBASE_EXAM_TERM_ID; '
                                                                     'primary key of this table.',
                                            'endoscopy_exam_event_key': 'Deterministic SHA-256 key for the '
                                                                        'parent Endobase examination.',
                                            'event_datetime': 'Parent exam time',
                                            'event_datetime_local': 'event_datetime in Europe/London '
                                                                    'wall-clock time (GMT/BST). Millennium '
                                                                    'rows take the bronze *_LOCAL companions '
                                                                    'of the same source columns and fallback '
                                                                    'order; rows from local-clock sources '
                                                                    '(LUNA, PACS/DICOM, TFC LIMS, CCMDS, '
                                                                    'MSDS, BadgerNet, EndoBase, SLAM, iWeb, '
                                                                    'Datix and similar) carry event_datetime '
                                                                    'unchanged. event_datetime is a UTC '
                                                                    'instant for Millennium rows, so take '
                                                                    'local calendar dates and clock times '
                                                                    'from this column.',
                                            'event_end_datetime': 'Finding end timestamp when supplied.',
                                            'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                        'wall-clock time (GMT/BST), by the '
                                                                        'same rule as event_datetime_local; '
                                                                        'null where event_end_datetime is '
                                                                        'null.',
                                            'event_time_source': 'Whether event time came from the exam or '
                                                                 'authored fallback.',
                                            'finding_map_candidate_count': 'Number of candidate targets '
                                                                           'observed; a mapped parent is '
                                                                           'unambiguous when this is 1.',
                                            'finding_map_candidate_set_id': 'Stable identifier of the '
                                                                            'retained candidate set.',
                                            'finding_map_competing_count': 'Candidates within 0.05 cosine of '
                                                                           'the selected candidate.',
                                            'finding_map_component_count': 'Number of source-derived '
                                                                           'components.',
                                            'finding_map_component_index': 'One-based source-derived '
                                                                           'component index.',
                                            'finding_map_confidence': 'Mapping confidence in [0,1] where the '
                                                                      'lane measured one.',
                                            'finding_map_cosine': 'Measured cosine between the normalised '
                                                                  'source and selected target; null when '
                                                                  'scoring did not run.',
                                            'finding_map_method': 'Mapping method: source_native, rule, '
                                                                  'exact_synonym or embedding_proposed.',
                                            'finding_map_rollup_levels': 'Maximum hierarchy distance to a '
                                                                         'selected common ancestor.',
                                            'finding_map_rule_id': 'Stable rule or lane identifier that '
                                                                   'produced the mapping.',
                                            'finding_map_scoring_model': 'Embedding model that produced '
                                                                         'map_cosine.',
                                            'finding_map_status': 'Explicit mapping outcome from the S3b '
                                                                  'status vocabulary.',
                                            'finding_map_status_reason': 'Reason for the mapping outcome or '
                                                                         'abstention.',
                                            'finding_map_version': 'Version of the source, terminology '
                                                                   'release or model used by the mapping.',
                                            'finding_omop_concept_id': 'OMOP concept identifier selected '
                                                                       'with the endoscopy finding mapping.',
                                            'finding_omop_source_concept_id': 'Non-standard OMOP source '
                                                                              'concept identifier, kept '
                                                                              'separate from a standard '
                                                                              'target.',
                                            'finding_snomed_code': 'SNOMED CT code selected for the '
                                                                   'endoscopy finding; null when no '
                                                                   'unambiguous mapping is available.',
                                            'finding_snomed_display': 'SNOMED CT preferred display selected '
                                                                      'for the endoscopy finding.',
                                            'finding_source_code': 'Source code for the endoscopy finding, '
                                                                   'retained without de-identification.',
                                            'finding_source_display': 'Source display for the endoscopy '
                                                                      'finding.',
                                            'finding_source_system': 'Canonical coding-system URI for the '
                                                                     'source endoscopy finding.',
                                            'finding_target_code': 'Code of the selected target in its own '
                                                                   'vocabulary.',
                                            'finding_target_display': 'Display of the selected target in its '
                                                                      'own vocabulary.',
                                            'finding_target_system': 'Coding-system URI of the selected '
                                                                     'target.',
                                            'free_text_ind': 'Free-text route indicator.',
                                            'loaded_at': 'Greatest of map_endobase_exam_term.ADC_UPDT and '
                                                         'the left-joined map_endobase_exam.ADC_UPDT, with '
                                                         'missing parent time first replaced by term time. '
                                                         'Parent lookup uses ENDOBASE_EXAM_ID. A non-null '
                                                         'parent time survives a null term time; both null '
                                                         'gives null. This can differ from '
                                                         'source_update_timestamp and the status end, which '
                                                         'use only the term clock; it is not clinical event '
                                                         'time or the current Silver refresh time.',
                                            'parent_term_id': 'Parent term identifier.',
                                            'patient_event_key': 'Deterministic event key derived from '
                                                                 'source identifiers; retained as a stable '
                                                                 'join key.',
                                            'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                         'resolved.',
                                            'person_link_status': 'Bronze person-link status.',
                                            'record_status': 'Superseded when '
                                                             'map_endobase_exam_term.SOURCE_PRESENT_IND is '
                                                             'false, otherwise active; missing presence '
                                                             'defaults true. No retracted status is emitted. '
                                                             'The non-free-text and usable-code filters '
                                                             'control eligibility separately, while '
                                                             'parent-exam fields do not determine this '
                                                             'label.',
                                            'record_status_effective_from': 'map_endobase_exam_term.CREATED_TS '
                                                                            'carried directly as the '
                                                                            'record-status start. This '
                                                                            'differs from clinical event '
                                                                            'time, which can use the joined '
                                                                            "parent exam's "
                                                                            'performed/exam/start timestamps '
                                                                            'before falling back to term '
                                                                            'creation time.',
                                            'record_status_effective_to': 'map_endobase_exam_term.ADC_UPDT '
                                                                          'when its source-presence flag is '
                                                                          'false; otherwise null. The '
                                                                          'parent-exam load clock and '
                                                                          'clinical event time are not used '
                                                                          'as status ends, even when they '
                                                                          'contribute to other fields.',
                                            'section_id': 'Report section identifier.',
                                            'source_code': 'DGVS term identifier with term-text fallback.',
                                            'source_coding_system': 'DGVS term coding system.',
                                            'source_display': 'Endobase term text.',
                                            'source_update_timestamp': 'map_endobase_exam_term.ADC_UPDT '
                                                                       'carried unchanged. The parent-exam '
                                                                       'ADC_UPDT contributes to loaded_at '
                                                                       'only, not this field; no independent '
                                                                       'native application-update clock or '
                                                                       'current Silver refresh timestamp is '
                                                                       'substituted.',
                                            'subject_id_system': 'Identifier system used for subject_key.',
                                            'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                           'identifier. Not salted, not secret; a stable '
                                                           'join key across feeds where person_id is '
                                                           'unresolved.',
                                            'subsection_id': 'Report subsection identifier.',
                                            'term_mapping_status': 'C4 term-mapping status.',
                                            'text_changed_ind': 'Source text-changed indicator.',
                                            'vip_ind': 'VIP indicator when supplied.',
                                            'withheld_identity_ind': 'Identity-withholding status is not '
                                                                     'populated by the current source '
                                                                     'projections; null does not mean '
                                                                     'identity was not withheld.'},
                                'table': 'One coded Endobase report term with parent-exam clinical time.'}}

# INSR_R4_SILVER_V1 (module_list): generated by r4/release/gen_silver_components.py; edit the generator, not this.
ENDOSCOPY_FINDING_PUBLIC_COLUMNS = list(ENDOSCOPY_FINDING_PUBLIC_COLUMNS) + ['identity_status']  # decision 1: contract columns, additions last
