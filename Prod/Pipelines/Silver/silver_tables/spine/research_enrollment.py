"""Silver spine.research_enrollment: 2 dataset(s).

- datasets: clinical_research_enrollment, _clinical_research_enrollment_metadata
- moved verbatim from: 1_silver_spine_and_reference:5681, 1_silver_spine_and_reference:5735, 1_silver_spine_and_reference:5746

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n
from silver_tables._common.spine_and_reference import (
    RESEARCH_ENROLLMENT_PUBLIC_COLUMNS,
    _lifecycle_source_research_enrollment,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


RESEARCH_ENROLLMENT_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'pt_prot_reg_id',
    'identity_status',
    'load_batch_id',
]


@materialized_view(
    name=_n("journey_clinical.research_enrollment"),
    cluster_by=["person_id", "event_datetime"], refresh_policy="auto",
)
def research_enrollment():
    # Build the declared dataset: One Cerner research-subject registration using clean sentinel-
    # safe dates.
    return _lifecycle_source_research_enrollment().select(*RESEARCH_ENROLLMENT_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._research_enrollment_metadata'),
    refresh_policy="auto",
)
def research_enrollment_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_research_enrollment; same row grain as the research table. Join keys:
    # patient_event_key, pt_prot_reg_id.
    return (_lifecycle_source_research_enrollment()).select(*RESEARCH_ENROLLMENT_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'clinical_research_enrollment': ValuePolicy(table='clinical_research_enrollment', enabled=True, zero_as_null=('encounter_id', 'protocol_arm_id', 'source_episode_id')),
}


WRAP_POLICY = {'clinical_research_enrollment'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_research_enrollment_metadata': {'columns': {'activity_date': 'Primary-key component carried '
                                                                         'unchanged from the public parent; '
                                                                         "combine it with the companion's "
                                                                         'other key columns when joining.',
                                                        'activity_record_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                        'admission_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                        'allergy_instance_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                        'aria_dx_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                        'aria_pt_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                        'artifact_link_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                        'birth_row_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                        'bloodtrack_transaction_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                        'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                        'care_contact_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                        'care_participation_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                        'cds_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                        'chemotherapy_course_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                        'community_care_activity_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                        'community_care_contact_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                        'concept_map_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                        'cost_line_item_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                        'dcp_forms_activity_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                        'device_mapping_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                        'diagnosis_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                        'dicom_path': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                        'doc_response_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                        'elective_access_procedure_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                        'encounter_attribute_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                        'encounter_bounds_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                        'encounter_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                        'encounter_id_resolved': 'True when encounter_id is '
                                                                                 'null or resolves to the '
                                                                                 'governed encounter spine; '
                                                                                 'false records unresolved '
                                                                                 'linkage without dropping '
                                                                                 'the row.',
                                                        'encounter_identifier_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                        'encounter_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                        'endobase_exam_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                        'endobase_exam_term_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                        'entity_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                        'entry_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                        'episode_encounter_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                        'episode_encounter_reltn_id': 'Native source '
                                                                                      'identifier carried '
                                                                                      'unchanged from the '
                                                                                      'public parent; joins '
                                                                                      'one lifecycle row to '
                                                                                      'that research row.',
                                                        'episode_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                        'episode_id_resolved': 'True when episode_id is null '
                                                                               'or resolves to the governed '
                                                                               'episode spine; false records '
                                                                               'unresolved linkage without '
                                                                               'dropping the row.',
                                                        'episode_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                        'event_after_death_30d': 'True when the event '
                                                                                 'timestamp is more than 30 '
                                                                                 'days after the resolved '
                                                                                 "person's death timestamp; "
                                                                                 'silver records the doubt '
                                                                                 'and does not filter the '
                                                                                 'row.',
                                                        'event_before_birth': 'True when the event timestamp '
                                                                              'precedes the resolved '
                                                                              "person's birth timestamp; "
                                                                              'silver records the doubt and '
                                                                              'does not filter the row.',
                                                        'event_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                        'extract_cd': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                        'fhx_activity_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                        'gene_tested_row_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                        'genetic_result_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                        'genetic_test_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                        'homecare_request_item_id': 'Native source '
                                                                                    'identifier carried '
                                                                                    'unchanged from the '
                                                                                    'public parent; joins '
                                                                                    'one lifecycle row to '
                                                                                    'that research row.',
                                                        'identity_status': 'Subject identity resolution '
                                                                           'state recorded by the silver '
                                                                           'canonical query.',
                                                        'implant_event_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                        'implant_sequence': 'Primary-key component carried '
                                                                            'unchanged from the public '
                                                                            'parent; combine it with the '
                                                                            "companion's other key columns "
                                                                            'when joining.',
                                                        'indication_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                        'is_illogical': 'Source RTT chronology flag '
                                                                        'indicating an illogical activity '
                                                                        'sequence; retained for the gold '
                                                                        'drop rule.',
                                                        'journey_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                        'journey_link_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                        'journey_participant_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                        'labour_delivery_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
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
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                        'mc_device_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                        'microbiology_isolate_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                        'order_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                        'organization_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                        'organization_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                        'pacs_examination_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                        'pacs_report_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                        'pathway_oid': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                        'patient_event_key': 'Deterministic SHA-256 event '
                                                                             'key carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that event '
                                                                             'row.',
                                                        'patient_event_row_key': 'Deterministic row-level '
                                                                                 'key carried unchanged from '
                                                                                 'events_patient_event; '
                                                                                 'joins one lifecycle row to '
                                                                                 'one event-index row.',
                                                        'period_business_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                        'period_oid': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                        'person_address_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                        'person_attribute_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                        'person_death_evidence_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                        'person_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                        'person_id_resolved': 'True when person_id is null '
                                                                              'or resolves to the governed '
                                                                              'person spine; false records '
                                                                              'unresolved linkage without '
                                                                              'dropping the row.',
                                                        'person_identifier_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                        'person_relationship_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                        'pharmacy_issue_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                        'pm_wait_list_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                        'practitioner_group_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                        'practitioner_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                        'practitioner_identifier_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                        'practitioner_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                        'practitioner_location_evidence_key': 'Deterministic '
                                                                                              'v2 key '
                                                                                              'carried '
                                                                                              'unchanged '
                                                                                              'from the '
                                                                                              'public '
                                                                                              'parent; joins '
                                                                                              'one lifecycle '
                                                                                              'row to that '
                                                                                              'research row.',
                                                        'pregnancy_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                        'pregnancy_reconciliation_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                        'problem_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
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
                                                        'record_status_effective_from': 'Timestamp from '
                                                                                        'which the '
                                                                                        'normalised record '
                                                                                        'status applies, '
                                                                                        'when the source '
                                                                                        'supplies one.',
                                                        'record_status_effective_to': 'Timestamp until which '
                                                                                      'the normalised record '
                                                                                      'status applies; null '
                                                                                      'means no source end '
                                                                                      'is known.',
                                                        'referral_oid': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                        'report_version_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                        'request_thread_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                        'research_study_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                        'row_source': 'Physical bronze row source, CURRENT '
                                                                      'or HIST, carried unchanged from the '
                                                                      'public parent; a key component '
                                                                      'because the research table is at '
                                                                      'physical version grain and '
                                                                      'patient_event_key is shared across an '
                                                                      "entry's versions.",
                                                        'rtt_activity_oid': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                        'sch_event_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                        'score_type': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                        'sequence_nbr': 'Primary-key component carried '
                                                                        'unchanged from the public parent; '
                                                                        "combine it with the companion's "
                                                                        'other key columns when joining.',
                                                        'service_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                        'service_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                        'snapshot_date': 'Primary-key component carried '
                                                                         'unchanged from the public parent; '
                                                                         "combine it with the companion's "
                                                                         'other key columns when joining.',
                                                        'source_daily_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                        'source_system_oid': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                        'source_unit': 'Primary-key component carried '
                                                                       'unchanged from the public parent; '
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                        'source_update_timestamp': 'Native source-system '
                                                                                   'update timestamp carried '
                                                                                   'through the canonical '
                                                                                   'silver query.',
                                                        'source_version_id': 'Native source version '
                                                                             'identifier carried unchanged '
                                                                             'from the public parent; '
                                                                             'CURRENT rows carry -1. A key '
                                                                             'component alongside '
                                                                             'row_source.',
                                                        'surg_case_proc_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                        'susceptibility_result_id': 'Native source '
                                                                                    'identifier carried '
                                                                                    'unchanged from the '
                                                                                    'public parent; joins '
                                                                                    'one lifecycle row to '
                                                                                    'that research row.',
                                                        'theatre_attendance_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                        'theatre_case_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                        'theatre_case_milestone_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                        'theatre_implant_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                        'transfusion_event_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                        'transfusion_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                        'treatment_cycle_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                        'treatment_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                        'update_count': 'Primary-key component carried '
                                                                        'unchanged from the public parent; '
                                                                        "combine it with the companion's "
                                                                        'other key columns when joining.',
                                                        'valid_from_datetime': 'Primary-key component '
                                                                               'carried unchanged from the '
                                                                               'public parent; combine it '
                                                                               "with the companion's other "
                                                                               'key columns when joining.',
                                                        'value_set_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                        'waiting_list_oid': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                        'waiting_list_snapshot_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.'},
                                            'table': 'Internal quality and batch metadata for '
                                                     'clinical_research_enrollment; same row grain as the '
                                                     'research table. Join keys: patient_event_key, '
                                                     'pt_prot_reg_id.'},
 'clinical_research_enrollment': {'columns': {'confidentiality_code': 'Source confidentiality classification '
                                                                      'attached to the record for each '
                                                                      'research enrollment record. It is '
                                                                      'produced by the silver transformation '
                                                                      'and has no direct bronze-column '
                                                                      'lineage entry. Source code meanings '
                                                                      'and sentinel values are retained '
                                                                      'unless the pipeline explicitly maps '
                                                                      'them; null means no code was '
                                                                      'supplied.',
                                              'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                              'available.',
                                              'enrolling_organization_id': 'Source-system identifier for the '
                                                                           'enrolling organization '
                                                                           'associated with each research '
                                                                           'enrollment record. It is carried '
                                                                           'from bronze field '
                                                                           '`ENROLLING_ORGANIZATION_ID` in '
                                                                           '`4_prod.bronze.map_research_subject`. '
                                                                           'Its uniqueness is limited to the '
                                                                           'originating source namespace; '
                                                                           'null means no identifier was '
                                                                           'supplied.',
                                              'event_datetime': 'Date and time when the represented clinical '
                                                                'or administrative event occurred for each '
                                                                'research enrollment record. It is derived '
                                                                'from bronze field `ON_STUDY_DT_TM_CLEAN` in '
                                                                '`4_prod.bronze.map_research_subject`. '
                                                                'Source precision and timezone handling '
                                                                'follow the pipeline expression; null means '
                                                                'the time was unavailable.',
                                              'event_datetime_local': 'event_datetime in Europe/London '
                                                                      'wall-clock time (GMT/BST). Millennium '
                                                                      'rows take the bronze *_LOCAL '
                                                                      'companions of the same source columns '
                                                                      'and fallback order; rows from '
                                                                      'local-clock sources (LUNA, '
                                                                      'PACS/DICOM, TFC LIMS, CCMDS, MSDS, '
                                                                      'BadgerNet, EndoBase, SLAM, iWeb, '
                                                                      'Datix and similar) carry '
                                                                      'event_datetime unchanged. '
                                                                      'event_datetime is a UTC instant for '
                                                                      'Millennium rows, so take local '
                                                                      'calendar dates and clock times from '
                                                                      'this column.',
                                              'event_end_datetime': 'Date and time when the represented '
                                                                    'clinical or administrative event ended '
                                                                    'for each research enrollment record. It '
                                                                    'is derived from bronze field '
                                                                    '`OFF_STUDY_DT_TM_CLEAN` in '
                                                                    '`4_prod.bronze.map_research_subject`. '
                                                                    'Source precision and timezone handling '
                                                                    'follow the pipeline expression; null '
                                                                    'means the time was unavailable.',
                                              'event_end_datetime_local': 'event_end_datetime in '
                                                                          'Europe/London wall-clock time '
                                                                          '(GMT/BST), by the same rule as '
                                                                          'event_datetime_local; null where '
                                                                          'event_end_datetime is null.',
                                              'fact_category': 'Contract category used to group the record '
                                                               'with comparable clinical facts for each '
                                                               'research enrollment record. It is produced '
                                                               'by the silver transformation and has no '
                                                               'direct bronze-column lineage entry. '
                                                               'Whitespace and source sentinel text are '
                                                               'retained unless the pipeline explicitly '
                                                               'normalizes them; null means no value was '
                                                               'supplied.',
                                              'loaded_at': 'map_research_subject.ADC_UPDT carried unchanged '
                                                           'for the contributing research-subject row. The '
                                                           'study key is derived without joining a study '
                                                           'load clock. This input load provenance is '
                                                           'distinct from PIPELINE_UPDT_DT_TM published as '
                                                           'source_update_timestamp and from Silver refresh '
                                                           'time.',
                                              'off_study_datetime': 'Date and time associated with off study '
                                                                    'for the research enrollment record. It '
                                                                    'is derived from bronze field '
                                                                    '`OFF_STUDY_DT_TM_CLEAN` in '
                                                                    '`4_prod.bronze.map_research_subject`. '
                                                                    'Source precision and timezone handling '
                                                                    'follow the pipeline expression; null '
                                                                    'means the time was unavailable.',
                                              'off_treatment_reason_desc': 'The description for the code '
                                                                           'value',
                                              'off_treatment_reason_text': 'Textual reason patient is taken '
                                                                           'off treatment.',
                                              'patient_event_key': 'Deterministic event key derived from '
                                                                   'source identifiers; retained as a stable '
                                                                   'join key.',
                                              'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                           'resolved.',
                                              'protocol_accession_number': 'This field contains the '
                                                                           "patient's accession number "
                                                                           '(order of enrollment) for this '
                                                                           'protocol.',
                                              'protocol_arm_id': 'Source-system identifier for the protocol '
                                                                 'arm associated with each research '
                                                                 'enrollment record. It is derived from '
                                                                 'bronze field `PROT_ARM_ID` in '
                                                                 '`4_prod.bronze.map_research_subject`. Its '
                                                                 'uniqueness is limited to the originating '
                                                                 'source namespace; null means no identifier '
                                                                 'was supplied.',
                                              'pt_prot_reg_id': 'Cerner PT_PROT_REG_ID; primary key of this '
                                                                'table.',
                                              'record_status': 'Always active in the current '
                                                               'map_research_subject projection. No '
                                                               'superseded or retracted status is emitted; '
                                                               'STATUS_ENUM, off-study/treatment dates and '
                                                               'removal reasons remain separate clinical '
                                                               'fields and do not change this literal label.',
                                              'record_status_effective_from': 'map_research_subject.ON_STUDY_DT_TM_CLEAN '
                                                                              'carried unchanged. This '
                                                                              'study-entry-time proxy is not '
                                                                              'an independently observed '
                                                                              'source-status transition; '
                                                                              'there is no registration, '
                                                                              'treatment-start or '
                                                                              'ingestion-time fallback.',
                                              'record_status_effective_to': 'Always null as a TIMESTAMP in '
                                                                            'the current research-enrollment '
                                                                            'projection. '
                                                                            'OFF_STUDY_DT_TM_CLEAN and '
                                                                            'treatment/removal fields are '
                                                                            'not used as history ends; a '
                                                                            'null here must not be '
                                                                            'interpreted as evidence that '
                                                                            'study participation is ongoing.',
                                              'registration_id': 'Source-system identifier for the '
                                                                 'registration associated with each research '
                                                                 'enrollment record. It is derived from '
                                                                 'bronze field `REG_ID` in '
                                                                 '`4_prod.bronze.map_research_subject`. Its '
                                                                 'uniqueness is limited to the originating '
                                                                 'source namespace; null means no identifier '
                                                                 'was supplied.',
                                              'removal_reason_desc': 'The description for the code value',
                                              'removal_reason_text': 'Textual reason patient is taken off '
                                                                     'study.',
                                              'research_study_key': 'Deterministic SHA-256 key for the '
                                                                    'related research study.',
                                              'source_code': 'Number to indicate the current status of the '
                                                             'enrolled patient(1: On Study,2:OnTreatment, '
                                                             '3:Off Treatment,4-OnFollowup,5-OffStudy)',
                                              'source_coding_system': 'Coding system or source namespace in '
                                                                      'which the source code is defined for '
                                                                      'each research enrollment record. It '
                                                                      'is produced by the silver '
                                                                      'transformation and has no direct '
                                                                      'bronze-column lineage entry. Source '
                                                                      'code meanings and sentinel values are '
                                                                      'retained unless the pipeline '
                                                                      'explicitly maps them; null means no '
                                                                      'code was supplied.',
                                              'source_display': 'Human-readable label supplied by the source '
                                                                'system for the source code for each '
                                                                'research enrollment record. It is derived '
                                                                'from bronze field `STATUS_DESC` in '
                                                                '`4_prod.bronze.map_research_subject`. '
                                                                'Whitespace and source sentinel text are '
                                                                'retained unless the pipeline explicitly '
                                                                'normalizes them; null means no value was '
                                                                'supplied.',
                                              'source_episode_id': 'Source-system identifier for the source '
                                                                   'episode associated with each research '
                                                                   'enrollment record. It is derived from '
                                                                   'bronze field `EPISODE_ID` in '
                                                                   '`4_prod.bronze.map_research_subject`. '
                                                                   'Its uniqueness is limited to the '
                                                                   'originating source namespace; null means '
                                                                   'no identifier was supplied.',
                                              'source_feed': 'Originating data feed responsible for the '
                                                             'record for each research enrollment record. It '
                                                             'is produced by the silver transformation and '
                                                             'has no direct bronze-column lineage entry. '
                                                             'Whitespace and source sentinel text are '
                                                             'retained unless the pipeline explicitly '
                                                             'normalizes them; null means no value was '
                                                             'supplied.',
                                              'source_update_timestamp': 'Bronze pipeline processing '
                                                                         'timestamp carried from '
                                                                         '`PIPELINE_UPDT_DT_TM` in '
                                                                         '`4_prod.bronze.map_research_subject`. '
                                                                         'It records when bronze processed '
                                                                         'the source row, not a native '
                                                                         'clinical-system update timestamp, '
                                                                         'clinical event time, or the '
                                                                         'current Silver refresh time.',
                                              'status_code': 'Number to indicate the current status of the '
                                                             'enrolled patient(1: On Study,2:OnTreatment, '
                                                             '3:Off Treatment,4-OnFollowup,5-OffStudy)',
                                              'status_desc': 'Value describing status desc for the research '
                                                             'enrollment record. It is carried from bronze '
                                                             'field `STATUS_DESC` in '
                                                             '`4_prod.bronze.map_research_subject`. '
                                                             'Whitespace and source sentinel text are '
                                                             'retained unless the pipeline explicitly '
                                                             'normalizes them; null means no value was '
                                                             'supplied.',
                                              'subject_id_system': 'Identifier namespace used to interpret '
                                                                   'the subject identifier on this record '
                                                                   'for each research enrollment record. It '
                                                                   'is derived from bronze field `PERSON_ID` '
                                                                   'in `4_prod.bronze.map_research_subject`. '
                                                                   'Its uniqueness is limited to the '
                                                                   'originating source namespace; null means '
                                                                   'no identifier was supplied.',
                                              'subject_key': 'Deterministic SHA-256 over the strongest '
                                                             'source identifier. Not salted, not secret; a '
                                                             'stable join key across feeds where person_id '
                                                             'is unresolved.',
                                              'treatment_completion_datetime': 'Date and time associated '
                                                                               'with treatment completion '
                                                                               'for the research enrollment '
                                                                               'record. It is derived from '
                                                                               'bronze field '
                                                                               '`TX_COMPLETION_DT_TM_CLEAN` '
                                                                               'in '
                                                                               '`4_prod.bronze.map_research_subject`. '
                                                                               'Source precision and '
                                                                               'timezone handling follow the '
                                                                               'pipeline expression; null '
                                                                               'means the time was '
                                                                               'unavailable.',
                                              'treatment_start_datetime': 'Date and time associated with '
                                                                          'treatment start for the research '
                                                                          'enrollment record. It is derived '
                                                                          'from bronze field '
                                                                          '`TX_START_DT_TM_CLEAN` in '
                                                                          '`4_prod.bronze.map_research_subject`. '
                                                                          'Source precision and timezone '
                                                                          'handling follow the pipeline '
                                                                          'expression; null means the time '
                                                                          'was unavailable.',
                                              'vip_ind': 'Indicator that the source record carries VIP '
                                                         'handling status for each research enrollment '
                                                         'record. It is produced by the silver '
                                                         'transformation and has no direct bronze-column '
                                                         'lineage entry. Null means the source did not state '
                                                         'the indicator and must not be interpreted as '
                                                         'false.',
                                              'withheld_identity_ind': 'Indicator that the source identity '
                                                                       'was deliberately withheld for each '
                                                                       'research enrollment record. It is '
                                                                       'produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Null '
                                                                       'means the source did not state the '
                                                                       'indicator and must not be '
                                                                       'interpreted as false.'},
                                  'table': 'One Cerner research-subject registration using clean '
                                           'sentinel-safe dates.'}}

# INSR_R4_SILVER_V1 (module_list): generated by r4/release/gen_silver_components.py; edit the generator, not this.
RESEARCH_ENROLLMENT_PUBLIC_COLUMNS = list(RESEARCH_ENROLLMENT_PUBLIC_COLUMNS) + ['identity_status']  # decision 1: contract columns, additions last
