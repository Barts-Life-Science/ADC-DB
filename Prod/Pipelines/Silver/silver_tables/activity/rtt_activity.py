"""Silver activity.rtt_activity: 2 dataset(s).

- datasets: clinical_rtt_activity, _clinical_rtt_activity_metadata
- moved verbatim from: 4_silver_care_pathways:1828, 4_silver_care_pathways:1834, 4_silver_care_pathways:1838, 4_silver_care_pathways:1848, 4_silver_care_pathways:1853, 4_silver_care_pathways:1865

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n
from silver_tables._common.journey import _rtt_activity_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


RTT_ACTIVITY_LIFECYCLE_FIELDS = [
    'identity_status',
    'is_illogical',
    'load_batch_id',
]


RTT_ACTIVITY_RETIRED_COLUMNS = [

]


RTT_ACTIVITY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_system_oid',
    'rtt_activity_oid',
    'identity_status',
    'is_illogical',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_rtt_activity():
    # Assemble rtt activity rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return _rtt_activity_canonical().drop("_source_system", "_source_table", "_source_row_id")


@materialized_view(
    name=_n("journey_clinical.rtt_activity"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def rtt_activity():
    # Build the declared dataset: LUNA clock-affecting RTT activity/status events. IS_ILLOGICAL
    # is carried as data, never filtered.
    return _lifecycle_source_rtt_activity().drop(*RTT_ACTIVITY_LIFECYCLE_FIELDS, *RTT_ACTIVITY_RETIRED_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._rtt_activity_metadata'),
    refresh_policy="auto",
)
def rtt_activity_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_rtt_activity;
    # same row grain as the research table. Join keys: patient_event_key, source_system_oid,
    # rtt_activity_oid.
    return (_lifecycle_source_rtt_activity()).select(*RTT_ACTIVITY_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'clinical_rtt_activity': ValuePolicy(table='clinical_rtt_activity', enabled=True, zero_as_null=('appointment_oid',)),
}


WRAP_POLICY = {'clinical_rtt_activity'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_rtt_activity_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
                                                                  'from the public parent; combine it with '
                                                                  "the companion's other key columns when "
                                                                  'joining.',
                                                 'activity_record_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                 'admission_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
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
                                                 'artifact_link_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                 'birth_row_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                 'cds_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                                 'chemotherapy_course_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                 'community_care_activity_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                 'community_care_contact_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                 'concept_map_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                                 'cost_line_item_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                 'dcp_forms_activity_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                 'device_mapping_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                 'diagnosis_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                 'dicom_path': 'Primary-key component carried unchanged from '
                                                               'the public parent; combine it with the '
                                                               "companion's other key columns when joining.",
                                                 'doc_response_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                 'elective_access_procedure_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                 'encounter_attribute_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                 'encounter_bounds_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                 'encounter_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                 'encounter_id_resolved': 'True when encounter_id is null or '
                                                                          'resolves to the governed '
                                                                          'encounter spine; false records '
                                                                          'unresolved linkage without '
                                                                          'dropping the row.',
                                                 'encounter_identifier_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                 'encounter_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                                 'endobase_exam_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                 'endobase_exam_term_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                 'entity_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                 'entry_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                                 'episode_encounter_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                 'episode_encounter_reltn_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                 'episode_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                                 'episode_id_resolved': 'True when episode_id is null or '
                                                                        'resolves to the governed episode '
                                                                        'spine; false records unresolved '
                                                                        'linkage without dropping the row.',
                                                 'episode_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                                 'event_after_death_30d': 'True when the event timestamp is '
                                                                          'more than 30 days after the '
                                                                          "resolved person's death "
                                                                          'timestamp; silver records the '
                                                                          'doubt and does not filter the '
                                                                          'row.',
                                                 'event_before_birth': 'True when the event timestamp '
                                                                       "precedes the resolved person's birth "
                                                                       'timestamp; silver records the doubt '
                                                                       'and does not filter the row.',
                                                 'event_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
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
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                 'genetic_test_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                 'homecare_request_item_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                 'identity_status': 'Subject identity resolution state '
                                                                    'recorded by the silver canonical query.',
                                                 'implant_event_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                 'implant_sequence': 'Primary-key component carried '
                                                                     'unchanged from the public parent; '
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
                                                 'indication_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                 'is_illogical': 'Source RTT chronology flag indicating an '
                                                                 'illogical activity sequence; retained for '
                                                                 'the gold drop rule.',
                                                 'journey_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                                 'journey_link_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                 'journey_participant_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                 'labour_delivery_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                 'load_batch_id': 'Deterministic batch token derived from '
                                                                  "the source row's bronze load timestamp.",
                                                 'loaded_at': 'Latest bronze load timestamp contributing to '
                                                              'the public row; never the lifecycle '
                                                              "pipeline's wall-clock time.",
                                                 'location_code': 'Primary-key component carried unchanged '
                                                                  'from the public parent; combine it with '
                                                                  "the companion's other key columns when "
                                                                  'joining.',
                                                 'location_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                 'location_stay_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                 'mc_device_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                 'microbiology_isolate_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                 'order_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
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
                                                 'pacs_report_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                 'pathway_oid': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                 'patient_event_key': 'Deterministic SHA-256 event key '
                                                                      'carried unchanged from the public '
                                                                      'parent; joins one lifecycle row to '
                                                                      'that event row.',
                                                 'patient_event_row_key': 'Deterministic row-level key '
                                                                          'carried unchanged from '
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
                                                 'person_address_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                 'person_attribute_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                 'person_death_evidence_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                 'person_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                 'person_id_resolved': 'True when person_id is null or '
                                                                       'resolves to the governed person '
                                                                       'spine; false records unresolved '
                                                                       'linkage without dropping the row.',
                                                 'person_identifier_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                 'person_relationship_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                 'pharmacy_issue_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                 'pm_wait_list_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                 'practitioner_group_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                 'practitioner_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                 'practitioner_identifier_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                 'practitioner_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                 'practitioner_location_evidence_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                 'pregnancy_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                 'pregnancy_reconciliation_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                 'problem_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                                 'procedure_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                 'pt_prot_reg_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                 'ptl_unique_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                 'record_status': 'active, superseded or retracted, '
                                                                  "normalised from the source's own status "
                                                                  'fields; superseded rows are earlier '
                                                                  'versions the source kept.',
                                                 'record_status_effective_from': 'Timestamp from which the '
                                                                                 'normalised record status '
                                                                                 'applies, when the source '
                                                                                 'supplies one.',
                                                 'record_status_effective_to': 'Timestamp until which the '
                                                                               'normalised record status '
                                                                               'applies; null means no '
                                                                               'source end is known.',
                                                 'referral_oid': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                 'report_version_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                 'request_thread_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                 'research_study_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                 'row_source': 'Physical bronze row source, CURRENT or HIST, '
                                                               'carried unchanged from the public parent; a '
                                                               'key component because the research table is '
                                                               'at physical version grain and '
                                                               'patient_event_key is shared across an '
                                                               "entry's versions.",
                                                 'rtt_activity_oid': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                 'sch_event_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                 'score_type': 'Primary-key component carried unchanged from '
                                                               'the public parent; combine it with the '
                                                               "companion's other key columns when joining.",
                                                 'sequence_nbr': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
                                                 'service_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                                 'service_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                                 'snapshot_date': 'Primary-key component carried unchanged '
                                                                  'from the public parent; combine it with '
                                                                  "the companion's other key columns when "
                                                                  'joining.',
                                                 'source_daily_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                 'source_identifier_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                 'source_object': 'Source-arm discriminator carried '
                                                                  'unchanged from the public parent; use it '
                                                                  'with arm-specific native identifiers when '
                                                                  'joining.',
                                                 'source_site': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                                 'source_system_oid': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                 'source_unit': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                                 'source_update_timestamp': 'Native source-system update '
                                                                            'timestamp carried through the '
                                                                            'canonical silver query.',
                                                 'source_version_id': 'Native source version identifier '
                                                                      'carried unchanged from the public '
                                                                      'parent; CURRENT rows carry -1. A key '
                                                                      'component alongside row_source.',
                                                 'surg_case_proc_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                 'susceptibility_result_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                 'theatre_attendance_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                 'treatment_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                                 'update_count': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
                                                 'valid_from_datetime': 'Primary-key component carried '
                                                                        'unchanged from the public parent; '
                                                                        "combine it with the companion's "
                                                                        'other key columns when joining.',
                                                 'value_set_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                                 'waiting_list_oid': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                 'waiting_list_snapshot_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.'},
                                     'table': 'Internal quality and batch metadata for '
                                              'clinical_rtt_activity; same row grain as the research table. '
                                              'Join keys: patient_event_key, source_system_oid, '
                                              'rtt_activity_oid.'},
 'clinical_rtt_activity': {'columns': {'activity_code': 'Source RTT activity code.',
                                       'activity_datetime_quality': 'Bronze quality classification for the '
                                                                    'activity timestamp.',
                                       'activity_display': 'Source RTT activity display.',
                                       'activity_sequence_asc': 'Ascending RTT-activity sequence.',
                                       'activity_sequence_desc': 'Descending RTT-activity sequence.',
                                       'activity_type_code': 'Source RTT activity-type code.',
                                       'activity_type_display': 'Source RTT activity-type display.',
                                       'appointment_oid': 'Raw appointment linkage evidence; resolution is '
                                                          'gated and no edge is emitted.',
                                       'confidentiality_code': 'Source confidentiality code when supplied.',
                                       'encounter_id': 'Nullable Millennium ENCNTR_ID; the LUNA activity '
                                                       'feed supplies no encounter.',
                                       'event_datetime': 'RTT activity timestamp.',
                                       'event_datetime_local': 'event_datetime in Europe/London wall-clock '
                                                               'time (GMT/BST). Millennium rows take the '
                                                               'bronze *_LOCAL companions of the same source '
                                                               'columns and fallback order; rows from '
                                                               'local-clock sources (LUNA, PACS/DICOM, TFC '
                                                               'LIMS, CCMDS, MSDS, BadgerNet, EndoBase, '
                                                               'SLAM, iWeb, Datix and similar) carry '
                                                               'event_datetime unchanged. event_datetime is '
                                                               'a UTC instant for Millennium rows, so take '
                                                               'local calendar dates and clock times from '
                                                               'this column.',
                                       'event_end_datetime': 'Event end timestamp; not supplied by LUNA RTT '
                                                             'activities.',
                                       'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                   'wall-clock time (GMT/BST), by the same '
                                                                   'rule as event_datetime_local; null where '
                                                                   'event_end_datetime is null.',
                                       'fact_category': 'Whether this fact is clinical or administrative in '
                                                        'the v2 plane merge.',
                                       'loaded_at': 'map_rtt_activity.ADC_UPDT carried unchanged through the '
                                                    'source and public projections. No parent or other '
                                                    'source clock is joined into this value; it differs from '
                                                    'SOURCE_ADC_UPDT used for source_update_timestamp and '
                                                    'from the current Silver refresh time.',
                                       'pathway_oid': 'Raw LUNA pathway object identifier.',
                                       'patient_event_key': 'Deterministic SHA-256 RTT activity identity.',
                                       'person_id': 'Millennium PERSON_ID when resolved; native person '
                                                    'foreign key.',
                                       'record_status': 'Superseded when map_rtt_activity.SOURCE_PRESENT_IND '
                                                        'or ACTIVE_IND is false; each missing flag defaults '
                                                        'true. Otherwise active. Source absence and '
                                                        'inactivity both map to superseded, not retracted; '
                                                        'the clinical status code does not set this '
                                                        'row-history label.',
                                       'record_status_effective_from': 'Source system timestamp recording '
                                                                       'when the RTT activity record was '
                                                                       'created.',
                                       'record_status_effective_to': 'For a superseded rtt activity row, the '
                                                                     'first non-null '
                                                                     'map_rtt_activity.SOURCE_ABSENT_DETECTED_TS, '
                                                                     'MODIFIED_DATETIME or ADC_UPDT, in that '
                                                                     'order; otherwise null. The same '
                                                                     'source-presence/inactivity predicate '
                                                                     'sets record_status. No event-time '
                                                                     'clamp or clinical-event timestamp is '
                                                                     'substituted.',
                                       'referral_key': 'Deterministic SHA-256 key of the linked referral.',
                                       'rtt_activity_oid': 'Raw LUNA RTT activity object identifier.',
                                       'site_code': 'Source site code.',
                                       'site_display': 'Source site display.',
                                       'source_code': 'Verbatim RTT-status code.',
                                       'source_coding_system': 'Verbatim LUNA RTT-status coding system.',
                                       'source_display': 'Verbatim RTT-status display.',
                                       'source_feed': 'Registered feed owning the fact.',
                                       'source_system_oid': 'LUNA SOURCE_SYSTEM_OID; component of the native '
                                                            'primary key.',
                                       'source_update_timestamp': 'map_rtt_activity.SOURCE_ADC_UPDT carried '
                                                                  'unchanged to the research row, distinct '
                                                                  'from ADC_UPDT used for loaded_at. It is '
                                                                  'not the clinical event timestamp or the '
                                                                  'current Silver refresh time.',
                                       'status_code': 'Source RTT-status code.',
                                       'status_display': 'Source RTT-status display.',
                                       'status_sequence_asc': 'Ascending RTT-status sequence.',
                                       'status_sequence_desc': 'Descending RTT-status sequence.',
                                       'subject_id_system': 'Identifier system used for subject_key.',
                                       'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                      'identifier. Not salted, not secret; a stable join key '
                                                      'across feeds where person_id is unresolved.',
                                       'treatment_function_code': 'Source treatment-function code.',
                                       'treatment_function_display': 'Source treatment-function display.',
                                       'vip_ind': 'Source VIP indicator when supplied.',
                                       'withheld_identity_ind': 'Withheld-identity indicator when supplied.'},
                           'table': 'LUNA clock-affecting RTT activity/status events. IS_ILLOGICAL is '
                                    'carried as data, never filtered.'}}

# INSR_R4_SILVER_V1 (module_drop): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import with_columns_last  # noqa: E402
RTT_ACTIVITY_LIFECYCLE_FIELDS = [c for c in RTT_ACTIVITY_LIFECYCLE_FIELDS if c not in ['identity_status', 'is_illogical']]  # decision 1: published
SPECS = with_columns_last(SPECS, 'clinical_rtt_activity', ('identity_status', 'is_illogical'))  # additions last
