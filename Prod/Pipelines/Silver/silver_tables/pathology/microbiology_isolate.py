"""Silver pathology.microbiology_isolate: 2 dataset(s).

- datasets: clinical_microbiology_isolate, _clinical_microbiology_isolate_metadata
- moved verbatim from: 2_silver_clinical_events:3327, 2_silver_clinical_events:3386, 2_silver_clinical_events:3399

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n
from silver_tables.pathology._shared import MICRO_ISOLATE_PUBLIC_COLUMNS, _lifecycle_source_microbiology_isolate


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


MICRO_ISOLATE_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'microbiology_isolate_id',
    'identity_status',
    'load_batch_id',
]


@materialized_view(
    name=_n("journey_clinical.microbiology_isolate"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def microbiology_isolate():
    # Build the declared dataset: One organism/isolate finding per accession, parsed from the
    # packed WinPath antibiogram in map_pathology (S2.5, parser 1.0.0). Person linkage follows
    # the accession identity; unresolved rows stay here and drop at gold.
    return _lifecycle_source_microbiology_isolate().select(*MICRO_ISOLATE_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._microbiology_isolate_metadata'),
    refresh_policy="auto",
)
def microbiology_isolate_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_microbiology_isolate; same row grain as the research table. Join keys:
    # patient_event_key, microbiology_isolate_id.
    return (_lifecycle_source_microbiology_isolate()).select(*MICRO_ISOLATE_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_microbiology_isolate_metadata': {'columns': {'activity_date': 'Primary-key component carried '
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
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                         'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                         'care_contact_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                         'care_participation_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                         'cds_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                         'chemotherapy_course_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                         'community_care_activity_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                         'community_care_contact_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                         'concept_map_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                         'cost_line_item_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                         'dcp_forms_activity_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                         'doc_response_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                         'elective_access_procedure_key': 'Deterministic v2 '
                                                                                          'key carried '
                                                                                          'unchanged from '
                                                                                          'the public '
                                                                                          'parent; joins one '
                                                                                          'lifecycle row to '
                                                                                          'that research '
                                                                                          'row.',
                                                         'encounter_attribute_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
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
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
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
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                         'entity_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                         'entry_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                         'episode_encounter_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                         'episode_id_resolved': 'True when episode_id is '
                                                                                'null or resolves to the '
                                                                                'governed episode spine; '
                                                                                'false records unresolved '
                                                                                'linkage without dropping '
                                                                                'the row.',
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
                                                         'event_before_birth': 'True when the event '
                                                                               'timestamp precedes the '
                                                                               "resolved person's birth "
                                                                               'timestamp; silver records '
                                                                               'the doubt and does not '
                                                                               'filter the row.',
                                                         'event_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                         'extract_cd': 'Primary-key component carried '
                                                                       'unchanged from the public parent; '
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                         'fhx_activity_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                         'gene_tested_row_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                         'genetic_result_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                         'genetic_test_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                         'journey_participant_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
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
                                                         'microbiology_isolate_id': 'Native source '
                                                                                    'identifier carried '
                                                                                    'unchanged from the '
                                                                                    'public parent; joins '
                                                                                    'one lifecycle row to '
                                                                                    'that research row.',
                                                         'order_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                         'organization_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                         'organization_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                         'pacs_examination_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                         'pacs_report_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                         'pathway_oid': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                         'patient_event_key': 'Deterministic SHA-256 event '
                                                                              'key carried unchanged from '
                                                                              'the public parent; joins one '
                                                                              'lifecycle row to that event '
                                                                              'row.',
                                                         'patient_event_row_key': 'Deterministic row-level '
                                                                                  'key carried unchanged '
                                                                                  'from '
                                                                                  'events_patient_event; '
                                                                                  'joins one lifecycle row '
                                                                                  'to one event-index row.',
                                                         'period_business_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
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
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
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
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                         'person_relationship_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                         'pharmacy_issue_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                         'pm_wait_list_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                         'practitioner_group_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                         'practitioner_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                         'practitioner_identifier_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                         'practitioner_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                         'practitioner_location_evidence_key': 'Deterministic '
                                                                                               'v2 key '
                                                                                               'carried '
                                                                                               'unchanged '
                                                                                               'from the '
                                                                                               'public '
                                                                                               'parent; '
                                                                                               'joins one '
                                                                                               'lifecycle '
                                                                                               'row to that '
                                                                                               'research '
                                                                                               'row.',
                                                         'pregnancy_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                         'pregnancy_reconciliation_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                         'problem_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                         'procedure_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                         'pt_prot_reg_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                         'ptl_unique_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                         'record_status': 'active, superseded or retracted, '
                                                                          "normalised from the source's own "
                                                                          'status fields; superseded rows '
                                                                          'are earlier versions the source '
                                                                          'kept.',
                                                         'record_status_effective_from': 'Timestamp from '
                                                                                         'which the '
                                                                                         'normalised record '
                                                                                         'status applies, '
                                                                                         'when the source '
                                                                                         'supplies one.',
                                                         'record_status_effective_to': 'Timestamp until '
                                                                                       'which the normalised '
                                                                                       'record status '
                                                                                       'applies; null means '
                                                                                       'no source end is '
                                                                                       'known.',
                                                         'referral_oid': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                         'report_version_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
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
                                                                       'patient_event_key is shared across '
                                                                       "an entry's versions.",
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
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
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
                                                         'source_daily_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                         'source_unit': 'Primary-key component carried '
                                                                        'unchanged from the public parent; '
                                                                        "combine it with the companion's "
                                                                        'other key columns when joining.',
                                                         'source_update_timestamp': 'Native source-system '
                                                                                    'update timestamp '
                                                                                    'carried through the '
                                                                                    'canonical silver query.',
                                                         'source_version_id': 'Native source version '
                                                                              'identifier carried unchanged '
                                                                              'from the public parent; '
                                                                              'CURRENT rows carry -1. A key '
                                                                              'component alongside '
                                                                              'row_source.',
                                                         'surg_case_proc_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                         'susceptibility_result_id': 'Native source '
                                                                                     'identifier carried '
                                                                                     'unchanged from the '
                                                                                     'public parent; joins '
                                                                                     'one lifecycle row to '
                                                                                     'that research row.',
                                                         'theatre_attendance_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                         'theatre_case_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                         'theatre_case_milestone_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                         'theatre_implant_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                         'transfusion_event_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.'},
                                             'table': 'Internal quality and batch metadata for '
                                                      'clinical_microbiology_isolate; same row grain as the '
                                                      'research table. Join keys: patient_event_key, '
                                                      'microbiology_isolate_id.'},
 'clinical_microbiology_isolate': {'columns': {'accession_identifier': 'Alias-resolved accession identifier.',
                                               'confidentiality_code': 'Source confidentiality '
                                                                       'classification attached to the '
                                                                       'record for each microbiology isolate '
                                                                       'record. It is produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Source '
                                                                       'code meanings and sentinel values '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly maps them; null means no '
                                                                       'code was supplied.',
                                               'encounter_id': "Millennium ENCNTR_ID as BIGINT: the row's "
                                                               'own Millennium source encounter, else the '
                                                               'single encounter shared by every Millennium '
                                                               'source of its accession; see '
                                                               'encounter_link_method.',
                                               'encounter_link_method': 'How encounter_id was set '
                                                                        '(PATH_ENCOUNTER_V1). '
                                                                        "source_accession: the row's own "
                                                                        'Millennium source names the '
                                                                        'encounter. accession_single: every '
                                                                        'current Millennium source of the '
                                                                        'alias-resolved accession names this '
                                                                        'one encounter (how TFC LIMS rows '
                                                                        'inherit one once their accession is '
                                                                        'merged with the Millennium order). '
                                                                        'accession_multiple: those sources '
                                                                        'name different encounters, so '
                                                                        'encounter_id is NULL. none: the '
                                                                        'accession has no Millennium source, '
                                                                        'so encounter_id is NULL.',
                                               'event_datetime': 'Accession sample/report/request fallback '
                                                                 'time.',
                                               'event_datetime_local': 'event_datetime in Europe/London '
                                                                       'wall-clock time (GMT/BST). '
                                                                       'Millennium rows take the bronze '
                                                                       '*_LOCAL companions of the same '
                                                                       'source columns and fallback order; '
                                                                       'rows from local-clock sources (LUNA, '
                                                                       'PACS/DICOM, TFC LIMS, CCMDS, MSDS, '
                                                                       'BadgerNet, EndoBase, SLAM, iWeb, '
                                                                       'Datix and similar) carry '
                                                                       'event_datetime unchanged. '
                                                                       'event_datetime is a UTC instant for '
                                                                       'Millennium rows, so take local '
                                                                       'calendar dates and clock times from '
                                                                       'this column.',
                                               'event_end_datetime': 'Event end time.',
                                               'event_end_datetime_local': 'event_end_datetime in '
                                                                           'Europe/London wall-clock time '
                                                                           '(GMT/BST), by the same rule as '
                                                                           'event_datetime_local; null where '
                                                                           'event_end_datetime is null.',
                                               'growth_grade': 'Reported growth grade.',
                                               'is_current': 'Whether the isolate remains current.',
                                               'isolate_comment': 'Text of the [~ annotation lines attached '
                                                                  'to this isolate, joined with single '
                                                                  'spaces; null when none.',
                                               'isolate_ordinal': 'Zero-based position of this isolate block '
                                                                  'within the source result value.',
                                               'lifecycle_status': 'Inherited source lifecycle.',
                                               'lims_no': 'TFC/LIMS instance number the source row came '
                                                          'from; dictionaries are keyed per instance.',
                                               'loaded_at': 'map_pathology_microbiology_isolate.ADC_UPDT '
                                                            'carried unchanged through its joins, '
                                                            'usable-code filtering and source/public '
                                                            'projections. Alias resolution and accession '
                                                            'identity add no load clocks; neither joined '
                                                            'report/parent timestamps nor the separately '
                                                            'aggregated accession evidence clock replace '
                                                            'this value. It is identical to '
                                                            'source_update_timestamp, not the clinical event '
                                                            'time or Silver refresh time.',
                                               'microbiology_isolate_id': 'Pathology '
                                                                          'microbiology_isolate_id; primary '
                                                                          'key of this table.',
                                               'organism_code': 'WinPath organism code exactly as packed in '
                                                                'the antibiogram (e.g. ESCOL).',
                                               'organism_omop_concept_id': 'Standard OMOP organism concept.',
                                               'organism_snomed_code': 'SNOMED organism code.',
                                               'organism_text': 'Organism as reported.',
                                               'panel_code': 'WinPath susceptibility panel code following '
                                                             'the organism (e.g. GU5, 3P); null when blank.',
                                               'parse_status': 'ok, or unterminated_block when this isolate '
                                                               'block had no terminator before the next '
                                                               'opener; row-level orphan_continuation is '
                                                               'recorded in validation.',
                                               'parser_version': 'pathology_antibiogram parser version that '
                                                                 'produced the row.',
                                               'pathology_result_id': 'Linked pathology-result fact.',
                                               'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                    'stable cross-feed join key.',
                                               'person_id': 'Resolved Millennium PERSON_ID as BIGINT through '
                                                            'the shared pathology identity projection.',
                                               'record_status': 'Retracted when '
                                                                'map_pathology_microbiology_isolate.lifecycle_status '
                                                                'is exactly cancelled or entered_in_error; '
                                                                'otherwise superseded when is_current is '
                                                                'false (null defaults true), otherwise '
                                                                'active. Comparisons are case-sensitive '
                                                                'without trimming; a null lifecycle_status '
                                                                'does not select retraction. Retraction '
                                                                'takes priority over supersession; clinical '
                                                                'findings and joined identity/link fields '
                                                                'are separate.',
                                               'record_status_effective_from': 'Always null as a TIMESTAMP '
                                                                               'in the current '
                                                                               'microbiology-isolate '
                                                                               'projection. No source-status '
                                                                               'start is supplied here; the '
                                                                               "clinical event's "
                                                                               'report/sample/request date '
                                                                               'fallbacks and ADC_UPDT are '
                                                                               'not used for this field.',
                                               'record_status_effective_to': 'Always null as a TIMESTAMP in '
                                                                             'the current '
                                                                             'microbiology-isolate '
                                                                             'projection, including when its '
                                                                             'status or source-current flag '
                                                                             'changes. No report, '
                                                                             'clinical-event or ingestion '
                                                                             'timestamp is substituted as a '
                                                                             'status end.',
                                               'report_version_key': 'Deterministic report-version key.',
                                               'research_qi_only': 'Research/QI release flag.',
                                               'source_code': 'Organism text or code.',
                                               'source_coding_system': 'Organism coding namespace.',
                                               'source_display': 'Approved SNOMED organism code.',
                                               'source_feed': 'Originating data feed responsible for the '
                                                              'record for each microbiology isolate record. '
                                                              'It is produced by the silver transformation '
                                                              'and has no direct bronze-column lineage '
                                                              'entry. Whitespace and source sentinel text '
                                                              'are retained unless the pipeline explicitly '
                                                              'normalizes them; null means no value was '
                                                              'supplied.',
                                               'source_update_timestamp': 'map_pathology_microbiology_isolate.ADC_UPDT '
                                                                          'carried unchanged from the '
                                                                          'contributing row, identical to '
                                                                          'loaded_at. No independent native '
                                                                          'application-update clock is '
                                                                          'projected here, and Silver '
                                                                          'refresh time is not substituted.',
                                               'specimen_key': 'Deterministic specimen key.',
                                               'specimen_type_code': 'Source specimen type.',
                                               'subject_id_system': 'Identifier namespace selected for '
                                                                    '`subject_key`: Millennium person '
                                                                    'identifier first, then MRN, then NHS '
                                                                    'number; nosubject denotes the '
                                                                    'per-source-record fallback when none is '
                                                                    'present.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'suspected_ind': 'Whether the organism is hedged or '
                                                                'suspected.',
                                               'vip_ind': 'Indicator that the source record carries VIP '
                                                          'handling status for each microbiology isolate '
                                                          'record. It is produced by the silver '
                                                          'transformation and has no direct bronze-column '
                                                          'lineage entry. Null means the source did not '
                                                          'state the indicator and must not be interpreted '
                                                          'as false.',
                                               'withheld_identity_ind': 'Withheld identity indicator.'},
                                   'table': 'One organism/isolate finding per accession, parsed from the '
                                            'packed WinPath antibiogram in map_pathology (S2.5, parser '
                                            '1.0.0). Person linkage follows the accession identity; '
                                            'unresolved rows stay here and drop at gold.'}}

# INSR_R4_SILVER_V1 (module_list): generated by r4/release/gen_silver_components.py; edit the generator, not this.
MICRO_ISOLATE_PUBLIC_COLUMNS = list(MICRO_ISOLATE_PUBLIC_COLUMNS) + ['identity_status']  # decision 1: contract columns, additions last
