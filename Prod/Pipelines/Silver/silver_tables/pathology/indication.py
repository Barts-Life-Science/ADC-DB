"""Silver pathology.indication: 2 dataset(s).

- datasets: clinical_indication, _clinical_indication_metadata
- moved verbatim from: 2_silver_clinical_events:3138, 2_silver_clinical_events:3198, 2_silver_clinical_events:3213

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n
from silver_tables._common.clinical_events import INDICATION_PUBLIC_COLUMNS, _lifecycle_source_indication


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


INDICATION_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'indication_id',
    'identity_status',
    'load_batch_id',
]


@materialized_view(
    name=_n("journey_clinical.indication"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def indication():
    # Build the declared dataset: Accession-scoped diagnosis-context evidence (single
    # deterministic lane diagnosis_context_window_v1). Source/evidence text are coded diagnosis
    # display strings; ig_release_status published verbatim; prod activation is gated.
    return _lifecycle_source_indication().select(*INDICATION_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._indication_metadata'),
    refresh_policy="auto",
)
def indication_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_indication;
    # same row grain as the research table. Join keys: patient_event_key, indication_id.
    return (_lifecycle_source_indication()).select(*INDICATION_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_indication_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                               'activity_record_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                               'admission_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                               'allergy_instance_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                               'aria_dx_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                               'aria_pt_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                               'artifact_link_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                               'birth_row_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                               'bloodtrack_transaction_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                               'cancer_treatment_cycle_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                               'care_contact_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                               'care_participation_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                               'cds_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                               'chemotherapy_course_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                               'community_care_activity_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                               'community_care_contact_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                               'concept_map_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                               'cost_line_item_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                               'dcp_forms_activity_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                               'device_mapping_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                               'diagnosis_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                               'dicom_path': 'Primary-key component carried unchanged from '
                                                             'the public parent; combine it with the '
                                                             "companion's other key columns when joining.",
                                               'doc_response_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                               'elective_access_procedure_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                               'encounter_attribute_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                               'encounter_bounds_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                               'encounter_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                               'encounter_id_resolved': 'True when encounter_id is null or '
                                                                        'resolves to the governed encounter '
                                                                        'spine; false records unresolved '
                                                                        'linkage without dropping the row.',
                                               'encounter_identifier_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                               'encounter_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                               'endobase_exam_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                               'endobase_exam_term_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                               'entity_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                               'entry_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                               'episode_encounter_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                               'episode_encounter_reltn_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                               'episode_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                               'episode_id_resolved': 'True when episode_id is null or '
                                                                      'resolves to the governed episode '
                                                                      'spine; false records unresolved '
                                                                      'linkage without dropping the row.',
                                               'episode_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                               'event_after_death_30d': 'True when the event timestamp is '
                                                                        'more than 30 days after the '
                                                                        "resolved person's death timestamp; "
                                                                        'silver records the doubt and does '
                                                                        'not filter the row.',
                                               'event_before_birth': 'True when the event timestamp precedes '
                                                                     "the resolved person's birth timestamp; "
                                                                     'silver records the doubt and does not '
                                                                     'filter the row.',
                                               'event_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                               'extract_cd': 'Primary-key component carried unchanged from '
                                                             'the public parent; combine it with the '
                                                             "companion's other key columns when joining.",
                                               'fhx_activity_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                               'gene_tested_row_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                               'genetic_result_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                               'genetic_test_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                               'homecare_request_item_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                               'identity_status': 'Subject identity resolution state '
                                                                  'recorded by the silver canonical query.',
                                               'implant_event_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                               'implant_sequence': 'Primary-key component carried unchanged '
                                                                   'from the public parent; combine it with '
                                                                   "the companion's other key columns when "
                                                                   'joining.',
                                               'indication_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                               'is_illogical': 'Source RTT chronology flag indicating an '
                                                               'illogical activity sequence; retained for '
                                                               'the gold drop rule.',
                                               'journey_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                               'journey_link_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                               'journey_participant_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                               'labour_delivery_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                               'load_batch_id': 'Deterministic batch token derived from the '
                                                                "source row's bronze load timestamp.",
                                               'loaded_at': 'Latest bronze load timestamp contributing to '
                                                            "the public row; never the lifecycle pipeline's "
                                                            'wall-clock time.',
                                               'location_code': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                               'location_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                               'location_stay_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                               'mc_device_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                               'microbiology_isolate_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                               'order_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                               'organization_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                               'organization_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                               'pacs_examination_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                               'pacs_report_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                               'pathway_oid': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                               'patient_event_key': 'Deterministic SHA-256 event key carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that event row.',
                                               'patient_event_row_key': 'Deterministic row-level key carried '
                                                                        'unchanged from '
                                                                        'events_patient_event; joins one '
                                                                        'lifecycle row to one event-index '
                                                                        'row.',
                                               'period_business_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                               'period_oid': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                               'person_address_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                               'person_attribute_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                               'person_death_evidence_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                               'person_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                               'person_id_resolved': 'True when person_id is null or '
                                                                     'resolves to the governed person spine; '
                                                                     'false records unresolved linkage '
                                                                     'without dropping the row.',
                                               'person_identifier_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                               'person_relationship_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                               'pharmacy_issue_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                               'pm_wait_list_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                               'practitioner_group_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                               'practitioner_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                               'practitioner_identifier_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                               'practitioner_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                               'practitioner_location_evidence_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                               'pregnancy_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                               'pregnancy_reconciliation_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                               'problem_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                               'procedure_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                               'pt_prot_reg_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                               'ptl_unique_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                               'record_status': 'active, superseded or retracted, normalised '
                                                                "from the source's own status fields; "
                                                                'superseded rows are earlier versions the '
                                                                'source kept.',
                                               'record_status_effective_from': 'Timestamp from which the '
                                                                               'normalised record status '
                                                                               'applies, when the source '
                                                                               'supplies one.',
                                               'record_status_effective_to': 'Timestamp until which the '
                                                                             'normalised record status '
                                                                             'applies; null means no source '
                                                                             'end is known.',
                                               'referral_oid': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                               'report_version_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                               'request_thread_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                               'research_study_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                               'row_source': 'Physical bronze row source, CURRENT or HIST, '
                                                             'carried unchanged from the public parent; a '
                                                             'key component because the research table is at '
                                                             'physical version grain and patient_event_key '
                                                             "is shared across an entry's versions.",
                                               'rtt_activity_oid': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                               'sch_event_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                               'score_type': 'Primary-key component carried unchanged from '
                                                             'the public parent; combine it with the '
                                                             "companion's other key columns when joining.",
                                               'sequence_nbr': 'Primary-key component carried unchanged from '
                                                               'the public parent; combine it with the '
                                                               "companion's other key columns when joining.",
                                               'service_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                               'service_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                               'snapshot_date': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                               'source_daily_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                               'source_identifier_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                               'source_object': 'Source-arm discriminator carried unchanged '
                                                                'from the public parent; use it with '
                                                                'arm-specific native identifiers when '
                                                                'joining.',
                                               'source_site': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                               'source_system_oid': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                               'source_unit': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                               'source_update_timestamp': 'Native source-system update '
                                                                          'timestamp carried through the '
                                                                          'canonical silver query.',
                                               'source_version_id': 'Native source version identifier '
                                                                    'carried unchanged from the public '
                                                                    'parent; CURRENT rows carry -1. A key '
                                                                    'component alongside row_source.',
                                               'surg_case_proc_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                               'susceptibility_result_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                               'theatre_attendance_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                               'theatre_case_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                               'theatre_case_milestone_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                               'theatre_implant_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                               'transfusion_event_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                               'transfusion_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                               'treatment_cycle_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                               'treatment_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                               'update_count': 'Primary-key component carried unchanged from '
                                                               'the public parent; combine it with the '
                                                               "companion's other key columns when joining.",
                                               'valid_from_datetime': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                               'value_set_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                               'waiting_list_oid': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                               'waiting_list_snapshot_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.'},
                                   'table': 'Internal quality and batch metadata for clinical_indication; '
                                            'same row grain as the research table. Join keys: '
                                            'patient_event_key, indication_id.'},
 'clinical_indication': {'columns': {'accession_identifier': 'Alias-resolved accession identifier.',
                                     'assertion': 'present, absent, possible, family_history, or unknown.',
                                     'confidence': 'Confidence value supplied by the pathology indication '
                                                   'rule, carried unchanged from bronze `confidence`; Silver '
                                                   'does not recalibrate this value.',
                                     'confidentiality_code': 'Source confidentiality classification attached '
                                                             'to the record for each indication record. It '
                                                             'is produced by the silver transformation and '
                                                             'has no direct bronze-column lineage entry. '
                                                             'Source code meanings and sentinel values are '
                                                             'retained unless the pipeline explicitly maps '
                                                             'them; null means no code was supplied.',
                                     'encounter_id': "Millennium ENCNTR_ID as BIGINT: the row's own "
                                                     'Millennium source encounter, else the single encounter '
                                                     'shared by every Millennium source of its accession; '
                                                     'see encounter_link_method.',
                                     'encounter_link_method': 'How encounter_id was set (PATH_ENCOUNTER_V1). '
                                                              "source_accession: the row's own Millennium "
                                                              'source names the encounter. accession_single: '
                                                              'every current Millennium source of the '
                                                              'alias-resolved accession names this one '
                                                              'encounter (how TFC LIMS rows inherit one once '
                                                              'their accession is merged with the Millennium '
                                                              'order). accession_multiple: those sources '
                                                              'name different encounters, so encounter_id is '
                                                              'NULL. none: the accession has no Millennium '
                                                              'source, so encounter_id is NULL.',
                                     'event_datetime': 'Accession report/sample/request fallback time.',
                                     'event_datetime_local': 'event_datetime in Europe/London wall-clock '
                                                             'time (GMT/BST). Millennium rows take the '
                                                             'bronze *_LOCAL companions of the same source '
                                                             'columns and fallback order; rows from '
                                                             'local-clock sources (LUNA, PACS/DICOM, TFC '
                                                             'LIMS, CCMDS, MSDS, BadgerNet, EndoBase, SLAM, '
                                                             'iWeb, Datix and similar) carry event_datetime '
                                                             'unchanged. event_datetime is a UTC instant for '
                                                             'Millennium rows, so take local calendar dates '
                                                             'and clock times from this column.',
                                     'event_end_datetime': 'Event end time.',
                                     'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                 'wall-clock time (GMT/BST), by the same '
                                                                 'rule as event_datetime_local; null where '
                                                                 'event_end_datetime is null.',
                                     'evidence_end': 'Evidence end offset.',
                                     'evidence_start': 'Evidence start offset.',
                                     'evidence_text': 'Exact evidence span.',
                                     'experiencer': 'patient, family, or unknown.',
                                     'ig_release_status': 'IG release status published verbatim.',
                                     'indication_id': 'Pathology indication_id; primary key of this table.',
                                     'is_current': 'Whether evidence remains current.',
                                     'loaded_at': 'map_pathology_indication.ADC_UPDT carried unchanged '
                                                  'through its joins, usable-code filtering and '
                                                  'source/public projections. Alias resolution and accession '
                                                  'identity add no load clocks; neither joined report/parent '
                                                  'timestamps nor the separately aggregated accession '
                                                  'evidence clock replace this value. It is identical to '
                                                  'source_update_timestamp, not the clinical event time or '
                                                  'Silver refresh time.',
                                     'mapping_status': 'Source mapping status.',
                                     'omop_concept_id': 'Standard OMOP condition concept.',
                                     'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                          'cross-feed join key.',
                                     'person_id': 'Resolved Millennium PERSON_ID as BIGINT through the '
                                                  'shared pathology identity projection.',
                                     'record_status': 'Always active in the current map_pathology_indication '
                                                      'projection. is_current, assertion, temporality, '
                                                      'experiencer and mapping/release status do not change '
                                                      'this literal label; usable-code filtering separately '
                                                      'controls publication eligibility.',
                                     'record_status_effective_from': 'Always null as a TIMESTAMP in the '
                                                                     'current indication projection. No '
                                                                     'source-status start is supplied here; '
                                                                     "the clinical event's "
                                                                     'report/sample/request date fallbacks '
                                                                     'and ADC_UPDT are not used for this '
                                                                     'field.',
                                     'record_status_effective_to': 'Always null as a TIMESTAMP in the '
                                                                   'current indication projection, including '
                                                                   'when its status or source-current flag '
                                                                   'changes. No report, clinical-event or '
                                                                   'ingestion timestamp is substituted as a '
                                                                   'status end.',
                                     'relation_type': 'Evidence relation type.',
                                     'research_qi_only': 'Research/QI release flag.',
                                     'rule_id': 'Deterministic rule identifier.',
                                     'rule_version': 'Deterministic rule version.',
                                     'snomed_code': 'SNOMED condition code.',
                                     'snomed_term': 'SNOMED display term.',
                                     'source_code': 'Coded diagnosis display string.',
                                     'source_coding_system': 'Indication text namespace.',
                                     'source_display': 'Coded diagnosis display string.',
                                     'source_feed': 'Originating data feed responsible for the record for '
                                                    'each indication record. It is produced by the silver '
                                                    'transformation and has no direct bronze-column lineage '
                                                    'entry. Whitespace and source sentinel text are retained '
                                                    'unless the pipeline explicitly normalizes them; null '
                                                    'means no value was supplied.',
                                     'source_field': 'Source evidence field.',
                                     'source_text': 'Unmodified coded diagnosis display string.',
                                     'source_update_timestamp': 'map_pathology_indication.ADC_UPDT carried '
                                                                'unchanged from the contributing row, '
                                                                'identical to loaded_at. No independent '
                                                                'native application-update clock is '
                                                                'projected here, and Silver refresh time is '
                                                                'not substituted.',
                                     'specimen_key': 'Deterministic specimen key.',
                                     'subject_id_system': 'Identifier namespace selected for `subject_key`: '
                                                          'Millennium person identifier first, then MRN, '
                                                          'then NHS number; nosubject denotes the '
                                                          'per-source-record fallback when none is present.',
                                     'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                    'identifier. Not salted, not secret; a stable join key '
                                                    'across feeds where person_id is unresolved.',
                                     'temporality': 'current, historical, future, or unknown.',
                                     'vip_ind': 'Indicator that the source record carries VIP handling '
                                                'status for each indication record. It is produced by the '
                                                'silver transformation and has no direct bronze-column '
                                                'lineage entry. Null means the source did not state the '
                                                'indicator and must not be interpreted as false.',
                                     'withheld_identity_ind': 'Withheld identity indicator.'},
                         'table': 'Accession-scoped diagnosis-context evidence (single deterministic lane '
                                  'diagnosis_context_window_v1). Source/evidence text are coded diagnosis '
                                  'display strings; ig_release_status published verbatim; prod activation is '
                                  'gated.'}}

# INSR_R4_SILVER_V1 (module_list): generated by r4/release/gen_silver_components.py; edit the generator, not this.
INDICATION_PUBLIC_COLUMNS = list(INDICATION_PUBLIC_COLUMNS) + ['identity_status']  # decision 1: contract columns, additions last
