"""Silver activity.pathway_tracking: 2 dataset(s).

- datasets: clinical_pathway_tracking, _clinical_pathway_tracking_metadata
- moved verbatim from: 4_silver_care_pathways:4366, 4_silver_care_pathways:4431, 4_silver_care_pathways:4493, 4_silver_care_pathways:4563, 4_silver_care_pathways:4568, 4_silver_care_pathways:4580

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n
from silver_tables._common.journey import _pathway_tracking_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / pathway tracking

# contract v2: retain the full canonical shape for internal reuse.
PATHWAY_TRACKING_SOURCE_COLUMNS = [
    "patient_event_key",
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
    "ptl_unique_id",
    "ptl_group_id",
    "archived_pathway",
    "pathway_oid",
    "referral_oid",
    "patient_oid",
    "ptl_activity_oid",
    "parent_ptl_unique_id",
    "parent_ptl_activity_oid",
    "parent_present_ind",
    "latest_activity_type",
    "latest_activity_oid",
    "latest_activity_date_future_ind",
    "days_waited",
    "specialty",
    "treatment_function",
    "treatment_function_code",
    "site",
    "site_group",
    "division",
    "lead_clinician",
    "lead_clinician_prid",
    "site_rvid",
    "source_key_status",
    "patient_spine_ind",
    "pathway_spine_ind",
    "referral_spine_ind",
    "patient_spine_link_status",
    "pathway_spine_link_status",
    "referral_spine_link_status",
    "nhs_number_valid_ind",
    "linkage_historical_fallback_ind",
    "linkage_fallback_conflict_ind",
    "person_link_status",
    "person_link_method",
    "identifier_link_status",
    "source_system_oid",
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
PATHWAY_TRACKING_PUBLIC_COLUMNS = [
    'patient_event_key',
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
    'ptl_unique_id',
    'ptl_group_id',
    'archived_pathway',
    'pathway_oid',
    'referral_oid',
    'patient_oid',
    'ptl_activity_oid',
    'parent_ptl_unique_id',
    'parent_ptl_activity_oid',
    'parent_present_ind',
    'latest_activity_type',
    'latest_activity_oid',
    'latest_activity_date_future_ind',
    'days_waited',
    'specialty',
    'treatment_function',
    'treatment_function_code',
    'site',
    'site_group',
    'division',
    'lead_clinician',
    'lead_clinician_prid',
    'site_rvid',
    'source_key_status',
    'patient_spine_ind',
    'pathway_spine_ind',
    'referral_spine_ind',
    'patient_spine_link_status',
    'pathway_spine_link_status',
    'referral_spine_link_status',
    'nhs_number_valid_ind',
    'linkage_historical_fallback_ind',
    'linkage_fallback_conflict_ind',
    'person_link_status',
    'person_link_method',
    'identifier_link_status',
    'source_system_oid',
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


PATHWAY_TRACKING_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'ptl_unique_id',
    'identity_status',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_pathway_tracking():
    # Assemble pathway tracking rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    return _pathway_tracking_canonical().select(*PATHWAY_TRACKING_SOURCE_COLUMNS)


@materialized_view(
    name=_n("journey_clinical.pathway_tracking"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def pathway_tracking():
    # Build the declared dataset: Whole-trust Pathfinder PTL activity rows; scope is
    # PTL_GROUP_ID, not cancer.
    return _lifecycle_source_pathway_tracking().select(*PATHWAY_TRACKING_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._pathway_tracking_metadata'),
    refresh_policy="auto",
)
def pathway_tracking_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_pathway_tracking; same row grain as the research table. Join keys:
    # patient_event_key, ptl_unique_id.
    return (_lifecycle_source_pathway_tracking()).select(*PATHWAY_TRACKING_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_pathway_tracking_metadata': {'columns': {'activity_date': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                     'activity_record_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                     'birth_row_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                     'bloodtrack_transaction_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                     'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                     'care_contact_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                     'care_participation_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
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
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                     'community_care_contact_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
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
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
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
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
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
                                                     'entry_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                     'episode_encounter_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                     'episode_encounter_reltn_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                     'episode_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                     'episode_id_resolved': 'True when episode_id is null or '
                                                                            'resolves to the governed '
                                                                            'episode spine; false records '
                                                                            'unresolved linkage without '
                                                                            'dropping the row.',
                                                     'episode_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                                     'event_after_death_30d': 'True when the event timestamp '
                                                                              'is more than 30 days after '
                                                                              "the resolved person's death "
                                                                              'timestamp; silver records the '
                                                                              'doubt and does not filter the '
                                                                              'row.',
                                                     'event_before_birth': 'True when the event timestamp '
                                                                           "precedes the resolved person's "
                                                                           'birth timestamp; silver records '
                                                                           'the doubt and does not filter '
                                                                           'the row.',
                                                     'event_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                     'genetic_result_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                     'genetic_test_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                     'homecare_request_item_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
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
                                                     'labour_delivery_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                     'load_batch_id': 'Deterministic batch token derived '
                                                                      "from the source row's bronze load "
                                                                      'timestamp.',
                                                     'loaded_at': 'Latest bronze load timestamp contributing '
                                                                  'to the public row; never the lifecycle '
                                                                  "pipeline's wall-clock time.",
                                                     'location_code': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                     'location_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                     'location_stay_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                     'mc_device_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                     'microbiology_isolate_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                     'order_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                     'patient_event_key': 'Deterministic SHA-256 event key '
                                                                          'carried unchanged from the public '
                                                                          'parent; joins one lifecycle row '
                                                                          'to that event row.',
                                                     'patient_event_row_key': 'Deterministic row-level key '
                                                                              'carried unchanged from '
                                                                              'events_patient_event; joins '
                                                                              'one lifecycle row to one '
                                                                              'event-index row.',
                                                     'period_business_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
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
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
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
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                     'pm_wait_list_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                     'practitioner_group_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                     'practitioner_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                     'practitioner_identifier_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                     'practitioner_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                     'practitioner_location_evidence_key': 'Deterministic v2 '
                                                                                           'key carried '
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
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
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
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
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
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                     'source_daily_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                     'source_identifier_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                     'source_object': 'Source-arm discriminator carried '
                                                                      'unchanged from the public parent; use '
                                                                      'it with arm-specific native '
                                                                      'identifiers when joining.',
                                                     'source_site': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                                     'source_system_oid': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                     'source_unit': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                                     'source_update_timestamp': 'Native source-system update '
                                                                                'timestamp carried through '
                                                                                'the canonical silver query.',
                                                     'source_version_id': 'Native source version identifier '
                                                                          'carried unchanged from the public '
                                                                          'parent; CURRENT rows carry -1. A '
                                                                          'key component alongside '
                                                                          'row_source.',
                                                     'surg_case_proc_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                     'susceptibility_result_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                     'theatre_attendance_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                     'theatre_case_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                     'theatre_case_milestone_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                     'theatre_implant_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                     'transfusion_event_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                     'transfusion_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                     'treatment_cycle_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                                  'one lifecycle row to that '
                                                                                  'research row.'},
                                         'table': 'Internal quality and batch metadata for '
                                                  'clinical_pathway_tracking; same row grain as the research '
                                                  'table. Join keys: patient_event_key, ptl_unique_id.'},
 'clinical_pathway_tracking': {'columns': {'archived_pathway': 'Source archived-pathway state retained '
                                                               'verbatim; never used as a publication gate.',
                                           'confidentiality_code': 'Source confidentiality classification '
                                                                   'attached to the record for each pathway '
                                                                   'tracking record. It is produced by the '
                                                                   'silver transformation and has no direct '
                                                                   'bronze-column lineage entry. Source code '
                                                                   'meanings and sentinel values are '
                                                                   'retained unless the pipeline explicitly '
                                                                   'maps them; null means no code was '
                                                                   'supplied.',
                                           'days_waited': 'B3 bronze field DAYS_WAITED; source value '
                                                          'retained unless documented as derived.',
                                           'division': 'B3 bronze field DIVISION; source value retained '
                                                       'unless documented as derived.',
                                           'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                           'available.',
                                           'event_datetime': 'B3 bronze field LATEST_ACTIVITY_DATE_CLEAN; '
                                                             'source value retained unless documented as '
                                                             'derived.',
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
                                           'event_end_datetime': 'Date and time when the represented '
                                                                 'clinical or administrative event ended for '
                                                                 'each pathway tracking record. It is '
                                                                 'produced by the silver transformation and '
                                                                 'has no direct bronze-column lineage entry. '
                                                                 'Source precision and timezone handling '
                                                                 'follow the pipeline expression; null means '
                                                                 'the time was unavailable.',
                                           'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                       'wall-clock time (GMT/BST), by the '
                                                                       'same rule as event_datetime_local; '
                                                                       'null where event_end_datetime is '
                                                                       'null.',
                                           'fact_category': 'Contract category used to group the record with '
                                                            'comparable clinical facts for each pathway '
                                                            'tracking record. It is produced by the silver '
                                                            'transformation and has no direct bronze-column '
                                                            'lineage entry. Whitespace and source sentinel '
                                                            'text are retained unless the pipeline '
                                                            'explicitly normalizes them; null means no value '
                                                            'was supplied.',
                                           'identifier_link_status': 'B3 bronze field '
                                                                     'IDENTIFIER_LINK_STATUS; source value '
                                                                     'retained unless documented as derived.',
                                           'latest_activity_date_future_ind': 'B3 bronze field '
                                                                              'LATEST_ACTIVITY_DATE_FUTURE_IND; '
                                                                              'source value retained unless '
                                                                              'documented as derived.',
                                           'latest_activity_oid': 'B3 bronze field LATEST_ACTIVITY_OID; '
                                                                  'source value retained unless documented '
                                                                  'as derived.',
                                           'latest_activity_type': 'B3 bronze field LATEST_ACTIVITY_TYPE; '
                                                                   'source value retained unless documented '
                                                                   'as derived.',
                                           'lead_clinician': 'B3 bronze field LEAD_CLINICIAN; source value '
                                                             'retained unless documented as derived.',
                                           'lead_clinician_prid': 'B3 bronze field LEAD_CLINICIAN_PRID; '
                                                                  'source value retained unless documented '
                                                                  'as derived.',
                                           'linkage_fallback_conflict_ind': 'B3 bronze field '
                                                                            'LINKAGE_FALLBACK_CONFLICT_IND; '
                                                                            'source value retained unless '
                                                                            'documented as derived.',
                                           'linkage_historical_fallback_ind': 'B3 bronze field '
                                                                              'LINKAGE_HISTORICAL_FALLBACK_IND; '
                                                                              'source value retained unless '
                                                                              'documented as derived.',
                                           'loaded_at': 'map_cancer_ptl.ADC_UPDT carried unchanged, distinct '
                                                        'from the PIPELINE_UPDT_DT_TM used for '
                                                        'source_update_timestamp. Parent and pathway keys '
                                                        'are derived without adding parent load clocks; this '
                                                        'is not the current Silver refresh time.',
                                           'nhs_number_valid_ind': 'B3 bronze field NHS_NUMBER_VALID_IND; '
                                                                   'source value retained unless documented '
                                                                   'as derived.',
                                           'parent_present_ind': 'Whether a non-null parent PTL_UNIQUE_ID '
                                                                 'exists in the pinned source snapshot.',
                                           'parent_ptl_activity_oid': 'B3 bronze field '
                                                                      'PARENT_PTL_ACTIVITY_OID; source value '
                                                                      'retained unless documented as '
                                                                      'derived.',
                                           'parent_ptl_unique_id': 'B3 bronze field PARENT_PTL_UNIQUE_ID; '
                                                                   'source value retained unless documented '
                                                                   'as derived.',
                                           'pathway_oid': 'B3 bronze field PATHWAY_OID; source value '
                                                          'retained unless documented as derived.',
                                           'pathway_spine_ind': 'B3 bronze field PATHWAY_SPINE_IND; source '
                                                                'value retained unless documented as '
                                                                'derived.',
                                           'pathway_spine_link_status': 'B3 bronze field '
                                                                        'PATHWAY_SPINE_LINK_STATUS; source '
                                                                        'value retained unless documented as '
                                                                        'derived.',
                                           'patient_event_key': 'Deterministic event key derived from source '
                                                                'identifiers; retained as a stable join key.',
                                           'patient_oid': 'B3 bronze field PATIENT_OID; source value '
                                                          'retained unless documented as derived.',
                                           'patient_spine_ind': 'B3 bronze field PATIENT_SPINE_IND; source '
                                                                'value retained unless documented as '
                                                                'derived.',
                                           'patient_spine_link_status': 'B3 bronze field '
                                                                        'PATIENT_SPINE_LINK_STATUS; source '
                                                                        'value retained unless documented as '
                                                                        'derived.',
                                           'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                        'resolved.',
                                           'person_link_method': 'B3 bronze field PERSON_LINK_METHOD; source '
                                                                 'value retained unless documented as '
                                                                 'derived.',
                                           'person_link_status': 'Consensus-safe published linkage status; '
                                                                 'conflicting arms never publish PERSON_ID.',
                                           'ptl_activity_oid': 'B3 bronze field PTL_ACTIVITY_OID; source '
                                                               'value retained unless documented as derived.',
                                           'ptl_group_id': 'B3 bronze field PTL_GROUP_ID; source value '
                                                           'retained unless documented as derived.',
                                           'ptl_unique_id': 'Pathfinder PTL_UNIQUE_ID as BIGINT; primary key '
                                                            'of this table.',
                                           'record_status': 'Retracted when '
                                                            'map_cancer_ptl.SOURCE_PRESENT_IND is false, '
                                                            'otherwise active; missing presence defaults '
                                                            'true. No superseded status is emitted. '
                                                            'ARCHIVED_PATHWAY, parent-presence and linkage '
                                                            'flags do not set this label.',
                                           'record_status_effective_from': 'map_cancer_ptl.LATEST_ACTIVITY_DATE_CLEAN '
                                                                           'carried directly as the '
                                                                           'record-status start. The '
                                                                           'projection does not replace it '
                                                                           'with the pipeline clock or a '
                                                                           'joined parent timestamp.',
                                           'record_status_effective_to': 'map_cancer_ptl.SOURCE_ABSENT_DETECTED_TS '
                                                                         'when the source-presence flag is '
                                                                         'false; otherwise null. A missing '
                                                                         'absence-detection timestamp stays '
                                                                         'null, without an ADC_UPDT '
                                                                         'fallback.',
                                           'referral_oid': 'B3 bronze field REFERRAL_OID; source value '
                                                           'retained unless documented as derived.',
                                           'referral_spine_ind': 'B3 bronze field REFERRAL_SPINE_IND; source '
                                                                 'value retained unless documented as '
                                                                 'derived.',
                                           'referral_spine_link_status': 'B3 bronze field '
                                                                         'REFERRAL_SPINE_LINK_STATUS; source '
                                                                         'value retained unless documented '
                                                                         'as derived.',
                                           'site': 'B3 bronze field SITE; source value retained unless '
                                                   'documented as derived.',
                                           'site_group': 'B3 bronze field SITE_GROUP; source value retained '
                                                         'unless documented as derived.',
                                           'site_rvid': 'B3 bronze field SITE_RVID; source value retained '
                                                        'unless documented as derived.',
                                           'source_code': 'Code supplied by the originating source system '
                                                          'for the represented concept for each pathway '
                                                          'tracking record. It is produced by the silver '
                                                          'transformation and has no direct bronze-column '
                                                          'lineage entry. Source code meanings and sentinel '
                                                          'values are retained unless the pipeline '
                                                          'explicitly maps them; null means no code was '
                                                          'supplied.',
                                           'source_coding_system': 'Coding system or source namespace in '
                                                                   'which the source code is defined for '
                                                                   'each pathway tracking record. It is '
                                                                   'produced by the silver transformation '
                                                                   'and has no direct bronze-column lineage '
                                                                   'entry. Source code meanings and sentinel '
                                                                   'values are retained unless the pipeline '
                                                                   'explicitly maps them; null means no code '
                                                                   'was supplied.',
                                           'source_display': 'Human-readable label supplied by the source '
                                                             'system for the source code for each pathway '
                                                             'tracking record. It is produced by the silver '
                                                             'transformation and has no direct bronze-column '
                                                             'lineage entry. Whitespace and source sentinel '
                                                             'text are retained unless the pipeline '
                                                             'explicitly normalizes them; null means no '
                                                             'value was supplied.',
                                           'source_feed': 'Originating data feed responsible for the record '
                                                          'for each pathway tracking record. It is produced '
                                                          'by the silver transformation and has no direct '
                                                          'bronze-column lineage entry. Whitespace and '
                                                          'source sentinel text are retained unless the '
                                                          'pipeline explicitly normalizes them; null means '
                                                          'no value was supplied.',
                                           'source_key_status': 'COMPLETE or NAMESPACE_MISSING; missing '
                                                                'namespaces publish and are never dropped.',
                                           'source_system_oid': 'B3 bronze field SOURCE_SYSTEM_OID; source '
                                                                'value retained unless documented as '
                                                                'derived.',
                                           'source_update_timestamp': 'Bronze run-as-of timestamp carried '
                                                                      'from '
                                                                      'map_cancer_ptl.PIPELINE_UPDT_DT_TM '
                                                                      'for the whole-trust Pathfinder '
                                                                      'tracker. The bronze writer stamps '
                                                                      'inserted, changed, revived or newly '
                                                                      'absent rows; unchanged present rows '
                                                                      'retain their prior stamp. Run-as-of '
                                                                      'defaults to processing time but can '
                                                                      'be supplied explicitly. This is not a '
                                                                      'clinical event time or native-system '
                                                                      'update timestamp.',
                                           'specialty': 'B3 bronze field SPECIALTY; source value retained '
                                                        'unless documented as derived.',
                                           'subject_id_system': 'B3 bronze field PATIENT_OID; source value '
                                                                'retained unless documented as derived.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'treatment_function': 'B3 bronze field TREATMENT_FUNCTION; source '
                                                                 'value retained unless documented as '
                                                                 'derived.',
                                           'treatment_function_code': 'B3 bronze field '
                                                                      'TREATMENT_FUNCTION_CODE; source value '
                                                                      'retained unless documented as '
                                                                      'derived.',
                                           'vip_ind': 'Indicator that the source record carries VIP handling '
                                                      'status for each pathway tracking record. It is '
                                                      'produced by the silver transformation and has no '
                                                      'direct bronze-column lineage entry. Null means the '
                                                      'source did not state the indicator and must not be '
                                                      'interpreted as false.',
                                           'withheld_identity_ind': 'Indicator that the source identity was '
                                                                    'deliberately withheld for each pathway '
                                                                    'tracking record. It is produced by the '
                                                                    'silver transformation and has no direct '
                                                                    'bronze-column lineage entry. Null means '
                                                                    'the source did not state the indicator '
                                                                    'and must not be interpreted as false.'},
                               'table': 'Whole-trust Pathfinder PTL activity rows; scope is PTL_GROUP_ID, '
                                        'not cancer.'}}

# INSR_R4_SILVER_V1 (module_list): generated by r4/release/gen_silver_components.py; edit the generator, not this.
PATHWAY_TRACKING_PUBLIC_COLUMNS = list(PATHWAY_TRACKING_PUBLIC_COLUMNS) + ['identity_status']  # decision 1: contract columns, additions last
