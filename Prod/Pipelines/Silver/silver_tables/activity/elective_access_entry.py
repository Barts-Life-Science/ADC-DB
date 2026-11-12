"""Silver activity.elective_access_entry: 2 dataset(s).

- datasets: clinical_elective_access_entry, _clinical_elective_access_entry_metadata
- moved verbatim from: 4_silver_care_pathways:4019, 4_silver_care_pathways:4098, 4_silver_care_pathways:4174, 4_silver_care_pathways:4259, 4_silver_care_pathways:4264, 4_silver_care_pathways:4276

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n
from silver_tables._common.journey import _elective_access_entry_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / elective access entry

# contract v2: retain the full canonical shape for internal reuse.
ELECTIVE_ACCESS_ENTRY_SOURCE_COLUMNS = [
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
    "waiting_list_oid",
    "source_system_oid",
    "pathway_oid",
    "referral_oid",
    "patient_oid",
    "waiting_list_id",
    "legacy_waiting_list_id",
    "waiting_list_name",
    "waiting_list_code",
    "department",
    "division",
    "business_unit",
    "clinical_priority_code",
    "clinical_priority_recorded_datetime",
    "site_rvid",
    "treatment_function_rvid",
    "admin_category_rvid",
    "intended_management_rvid",
    "admit_method_rvid",
    "priority_rvid",
    "status_rvid",
    "elective_admission_type_rvid",
    "encounter_type_rvid",
    "removal_reason_rvid",
    "division_rvid",
    "tci_location_rvid",
    "admit_offer_outcome_rvid",
    "lead_clinician_prid",
    "status_reason",
    "status_change_datetime",
    "decided_to_admit_datetime",
    "tci_datetime",
    "tci_future_ind",
    "tci_created_datetime",
    "guaranteed_activity_datetime",
    "actual_guaranteed_activity_datetime",
    "planned_datetime",
    "earliest_reasonable_offer_datetime",
    "admit_datetime",
    "comments",
    "active_ind",
    "created_datetime",
    "created_by_prid",
    "modified_datetime",
    "modified_by_prid",
    "person_link_status",
    "person_link_method",
    "identifier_link_status",
    "linkage_historical_fallback_ind",
    "linkage_fallback_conflict_ind",
    "nhs_number_valid_ind",
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
ELECTIVE_ACCESS_ENTRY_PUBLIC_COLUMNS = [
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
    'waiting_list_oid',
    'source_system_oid',
    'pathway_oid',
    'referral_oid',
    'patient_oid',
    'waiting_list_id',
    'legacy_waiting_list_id',
    'waiting_list_name',
    'waiting_list_code',
    'department',
    'division',
    'business_unit',
    'clinical_priority_code',
    'clinical_priority_recorded_datetime',
    'site_rvid',
    'treatment_function_rvid',
    'admin_category_rvid',
    'intended_management_rvid',
    'admit_method_rvid',
    'priority_rvid',
    'status_rvid',
    'elective_admission_type_rvid',
    'encounter_type_rvid',
    'removal_reason_rvid',
    'division_rvid',
    'tci_location_rvid',
    'admit_offer_outcome_rvid',
    'lead_clinician_prid',
    'status_reason',
    'status_change_datetime',
    'decided_to_admit_datetime',
    'tci_datetime',
    'tci_future_ind',
    'tci_created_datetime',
    'guaranteed_activity_datetime',
    'actual_guaranteed_activity_datetime',
    'planned_datetime',
    'earliest_reasonable_offer_datetime',
    'admit_datetime',
    'comments',
    'active_ind',
    'created_datetime',
    'created_by_prid',
    'modified_datetime',
    'modified_by_prid',
    'person_link_status',
    'person_link_method',
    'identifier_link_status',
    'linkage_historical_fallback_ind',
    'linkage_fallback_conflict_ind',
    'nhs_number_valid_ind',
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


ELECTIVE_ACCESS_ENTRY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_system_oid',
    'waiting_list_oid',
    'identity_status',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_elective_access_entry():
    # Assemble elective access entry rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _elective_access_entry_canonical().select(*ELECTIVE_ACCESS_ENTRY_SOURCE_COLUMNS)


@materialized_view(
    name=_n("journey_clinical.elective_access_entry"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def elective_access_entry():
    # Build the declared dataset: One LUNA elective-access entry with primary OPCS and clinical-
    # priority folds.
    return _lifecycle_source_elective_access_entry().select(*ELECTIVE_ACCESS_ENTRY_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._elective_access_entry_metadata'),
    refresh_policy="auto",
)
def elective_access_entry_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_elective_access_entry; same row grain as the research table. Join keys:
    # patient_event_key, source_system_oid, waiting_list_oid.
    return (_lifecycle_source_elective_access_entry()).select(*ELECTIVE_ACCESS_ENTRY_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'clinical_elective_access_entry': ValuePolicy(table='clinical_elective_access_entry', enabled=True),
}


# W2.10 (L2637): elective access event time: the decision to admit, else the entry's creation time.
VALUE_POLICIES["clinical_elective_access_entry"].register("event_datetime_derived", ["decided_to_admit_datetime", "created_datetime"],
         lambda F: F.coalesce(F.col("decided_to_admit_datetime"), F.col("created_datetime")),
         "Clinical event time for the entry: decided_to_admit_datetime, else created_datetime (event_datetime is the "
         "LUNA creation time, L2637). INSR_W2 W2.10 (J22).")


VALUE_POLICIES["clinical_elective_access_entry"].register("event_datetime_basis", ["decided_to_admit_datetime", "created_datetime"],
         lambda F: F.when(F.col("decided_to_admit_datetime").isNotNull(), F.lit("decided_to_admit"))
                    .when(F.col("created_datetime").isNotNull(), F.lit("created")),
         "decided_to_admit or created; NULL iff event_datetime_derived is NULL. INSR_W2 W2.10.")


WRAP_POLICY = {'clinical_elective_access_entry'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_elective_access_entry_metadata': {'columns': {'activity_date': 'Primary-key component carried '
                                                                           'unchanged from the public '
                                                                           'parent; combine it with the '
                                                                           "companion's other key columns "
                                                                           'when joining.',
                                                          'activity_record_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                          'admission_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                          'bloodtrack_transaction_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                          'cancer_treatment_cycle_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                          'care_contact_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                          'care_participation_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                          'cds_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                          'chemotherapy_course_id': 'Native source '
                                                                                    'identifier carried '
                                                                                    'unchanged from the '
                                                                                    'public parent; joins '
                                                                                    'one lifecycle row to '
                                                                                    'that research row.',
                                                          'community_care_activity_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                          'community_care_contact_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                          'concept_map_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                          'cost_line_item_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                          'dcp_forms_activity_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                          'device_mapping_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
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
                                                                                           'parent; joins '
                                                                                           'one lifecycle '
                                                                                           'row to that '
                                                                                           'research row.',
                                                          'encounter_attribute_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                          'encounter_bounds_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                          'encounter_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                          'encounter_id_resolved': 'True when encounter_id '
                                                                                   'is null or resolves to '
                                                                                   'the governed encounter '
                                                                                   'spine; false records '
                                                                                   'unresolved linkage '
                                                                                   'without dropping the '
                                                                                   'row.',
                                                          'encounter_identifier_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                          'encounter_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                          'endobase_exam_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                          'endobase_exam_term_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
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
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                          'episode_encounter_reltn_id': 'Native source '
                                                                                        'identifier carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
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
                                                                                   'timestamp is more than '
                                                                                   '30 days after the '
                                                                                   "resolved person's death "
                                                                                   'timestamp; silver '
                                                                                   'records the doubt and '
                                                                                   'does not filter the row.',
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
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                          'implant_sequence': 'Primary-key component carried '
                                                                              'unchanged from the public '
                                                                              'parent; combine it with the '
                                                                              "companion's other key columns "
                                                                              'when joining.',
                                                          'indication_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                          'labour_delivery_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                          'load_batch_id': 'Deterministic batch token '
                                                                           "derived from the source row's "
                                                                           'bronze load timestamp.',
                                                          'loaded_at': 'Latest bronze load timestamp '
                                                                       'contributing to the public row; '
                                                                       "never the lifecycle pipeline's "
                                                                       'wall-clock time.',
                                                          'location_code': 'Primary-key component carried '
                                                                           'unchanged from the public '
                                                                           'parent; combine it with the '
                                                                           "companion's other key columns "
                                                                           'when joining.',
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
                                                          'pacs_report_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                          'person_address_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                          'person_attribute_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                          'person_death_evidence_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
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
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                          'person_relationship_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
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
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                          'practitioner_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                          'practitioner_identifier_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
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
                                                                                          'unchanged from '
                                                                                          'the public '
                                                                                          'parent; joins one '
                                                                                          'lifecycle row to '
                                                                                          'that research '
                                                                                          'row.',
                                                          'problem_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                          'procedure_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                          'pt_prot_reg_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                          'ptl_unique_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                                        'which the '
                                                                                        'normalised record '
                                                                                        'status applies; '
                                                                                        'null means no '
                                                                                        'source end is '
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
                                                          'request_thread_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                          'research_study_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
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
                                                                              'lifecycle row to that '
                                                                              'research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; combine it with the '
                                                                           "companion's other key columns "
                                                                           'when joining.',
                                                          'source_daily_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                          'source_identifier_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                          'source_object': 'Source-arm discriminator carried '
                                                                           'unchanged from the public '
                                                                           'parent; use it with arm-specific '
                                                                           'native identifiers when joining.',
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
                                                                                     'canonical silver '
                                                                                     'query.',
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
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                          'theatre_case_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                          'theatre_case_milestone_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                          'theatre_implant_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                          'transfusion_event_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                          'transfusion_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                          'treatment_cycle_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                          'treatment_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                          'waiting_list_oid': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                          'waiting_list_snapshot_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.'},
                                              'table': 'Internal quality and batch metadata for '
                                                       'clinical_elective_access_entry; same row grain as '
                                                       'the research table. Join keys: patient_event_key, '
                                                       'source_system_oid, waiting_list_oid.'},
 'clinical_elective_access_entry': {'columns': {'active_ind': 'Source closure state retained verbatim; false '
                                                              'does not mean source absence.',
                                                'actual_guaranteed_activity_datetime': 'B3 bronze field '
                                                                                       'ACTUAL_GUARANTEED_ACTIVITY_DT_TM_CLEAN; '
                                                                                       'source value '
                                                                                       'retained unless '
                                                                                       'documented as '
                                                                                       'derived.',
                                                'admin_category_rvid': 'B3 bronze field ADMIN_CATEGORY_RVID; '
                                                                       'source value retained unless '
                                                                       'documented as derived.',
                                                'admit_datetime': 'B3 bronze field ADMIT_DT_TM_CLEAN; source '
                                                                  'value retained unless documented as '
                                                                  'derived.',
                                                'admit_method_rvid': 'B3 bronze field ADMIT_METHOD_RVID; '
                                                                     'source value retained unless '
                                                                     'documented as derived.',
                                                'admit_offer_outcome_rvid': 'B3 bronze field '
                                                                            'ADMIT_OFFER_OUTCOME_RVID; '
                                                                            'source value retained unless '
                                                                            'documented as derived.',
                                                'business_unit': 'B3 bronze field BUSINESS_UNIT; source '
                                                                 'value retained unless documented as '
                                                                 'derived.',
                                                'clinical_priority_code': 'B3 bronze field FIELD_VALUE_VAR; '
                                                                          'source value retained unless '
                                                                          'documented as derived.',
                                                'clinical_priority_recorded_datetime': 'B3 bronze field '
                                                                                       'FIELD_VALUE_DATE_CLEAN; '
                                                                                       'source value '
                                                                                       'retained unless '
                                                                                       'documented as '
                                                                                       'derived.',
                                                'comments': 'Direct identifier published and IG-governed at '
                                                            'serve time',
                                                'confidentiality_code': 'Source confidentiality '
                                                                        'classification attached to the '
                                                                        'record for each elective access '
                                                                        'entry record. It is produced by the '
                                                                        'silver transformation and has no '
                                                                        'direct bronze-column lineage entry. '
                                                                        'Source code meanings and sentinel '
                                                                        'values are retained unless the '
                                                                        'pipeline explicitly maps them; null '
                                                                        'means no code was supplied.',
                                                'created_by_prid': 'B3 bronze field CREATED_BY_PRID; source '
                                                                   'value retained unless documented as '
                                                                   'derived.',
                                                'created_datetime': 'B3 bronze field CREATED_DT_TM; source '
                                                                    'value retained unless documented as '
                                                                    'derived.',
                                                'decided_to_admit_datetime': 'B3 bronze field '
                                                                             'DECIDED_TO_ADMIT_DT_TM_CLEAN; '
                                                                             'source value retained unless '
                                                                             'documented as derived.',
                                                'department': 'B3 bronze field DEPARTMENT; source value '
                                                              'retained unless documented as derived.',
                                                'division': 'B3 bronze field DIVISION; source value retained '
                                                            'unless documented as derived.',
                                                'division_rvid': 'B3 bronze field DIVISION_RVID; source '
                                                                 'value retained unless documented as '
                                                                 'derived.',
                                                'earliest_reasonable_offer_datetime': 'B3 bronze field '
                                                                                      'EARLIEST_REASONABLE_OFFER_DT_TM_CLEAN; '
                                                                                      'source value retained '
                                                                                      'unless documented as '
                                                                                      'derived.',
                                                'elective_admission_type_rvid': 'B3 bronze field '
                                                                                'ELECTIVE_ADMISSION_TYPE_RVID; '
                                                                                'source value retained '
                                                                                'unless documented as '
                                                                                'derived.',
                                                'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                                'available.',
                                                'encounter_type_rvid': 'B3 bronze field ENCOUNTER_TYPE_RVID; '
                                                                       'source value retained unless '
                                                                       'documented as derived.',
                                                'event_datetime': 'B3 bronze field CREATED_DT_TM; source '
                                                                  'value retained unless documented as '
                                                                  'derived.',
                                                'event_datetime_basis': 'decided_to_admit or created; NULL '
                                                                        'iff event_datetime_derived is NULL. '
                                                                        'INSR_W2 W2.10.',
                                                'event_datetime_derived': 'Clinical event time for the '
                                                                          'entry: decided_to_admit_datetime, '
                                                                          'else created_datetime '
                                                                          '(event_datetime is the LUNA '
                                                                          'creation time, L2637). INSR_W2 '
                                                                          'W2.10 (J22).',
                                                'event_datetime_local': 'event_datetime in Europe/London '
                                                                        'wall-clock time (GMT/BST). '
                                                                        'Millennium rows take the bronze '
                                                                        '*_LOCAL companions of the same '
                                                                        'source columns and fallback order; '
                                                                        'rows from local-clock sources '
                                                                        '(LUNA, PACS/DICOM, TFC LIMS, CCMDS, '
                                                                        'MSDS, BadgerNet, EndoBase, SLAM, '
                                                                        'iWeb, Datix and similar) carry '
                                                                        'event_datetime unchanged. '
                                                                        'event_datetime is a UTC instant for '
                                                                        'Millennium rows, so take local '
                                                                        'calendar dates and clock times from '
                                                                        'this column.',
                                                'event_end_datetime': 'B3 bronze field ADMIT_DT_TM_CLEAN; '
                                                                      'source value retained unless '
                                                                      'documented as derived.',
                                                'event_end_datetime_local': 'event_end_datetime in '
                                                                            'Europe/London wall-clock time '
                                                                            '(GMT/BST), by the same rule as '
                                                                            'event_datetime_local; null '
                                                                            'where event_end_datetime is '
                                                                            'null.',
                                                'fact_category': 'Contract category used to group the record '
                                                                 'with comparable clinical facts for each '
                                                                 'elective access entry record. It is '
                                                                 'produced by the silver transformation and '
                                                                 'has no direct bronze-column lineage entry. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                                'guaranteed_activity_datetime': 'B3 bronze field '
                                                                                'GUARANTEED_ACTIVITY_DT_TM_CLEAN; '
                                                                                'source value retained '
                                                                                'unless documented as '
                                                                                'derived.',
                                                'identifier_link_status': 'B3 bronze field '
                                                                          'IDENTIFIER_LINK_STATUS; source '
                                                                          'value retained unless documented '
                                                                          'as derived.',
                                                'intended_management_rvid': 'B3 bronze field '
                                                                            'INTENDED_MANAGEMENT_RVID; '
                                                                            'source value retained unless '
                                                                            'documented as derived.',
                                                'lead_clinician_prid': 'B3 bronze field LEAD_CLINICIAN_PRID; '
                                                                       'source value retained unless '
                                                                       'documented as derived.',
                                                'legacy_waiting_list_id': 'B3 bronze field '
                                                                          'LEGACY_WAITING_LIST_ID; source '
                                                                          'value retained unless documented '
                                                                          'as derived.',
                                                'linkage_fallback_conflict_ind': 'B3 bronze field '
                                                                                 'LINKAGE_FALLBACK_CONFLICT_IND; '
                                                                                 'source value retained '
                                                                                 'unless documented as '
                                                                                 'derived.',
                                                'linkage_historical_fallback_ind': 'B3 bronze field '
                                                                                   'LINKAGE_HISTORICAL_FALLBACK_IND; '
                                                                                   'source value retained '
                                                                                   'unless documented as '
                                                                                   'derived.',
                                                'loaded_at': 'map_elective_access_list.ADC_UPDT carried '
                                                             'unchanged. Procedure and attribute children '
                                                             'contribute clinical fields but their clocks '
                                                             'are not selected or aggregated here; this is '
                                                             'not the latest child load time or the current '
                                                             'Silver refresh time.',
                                                'modified_by_prid': 'B3 bronze field MODIFIED_BY_PRID; '
                                                                    'source value retained unless documented '
                                                                    'as derived.',
                                                'modified_datetime': 'B3 bronze field MODIFIED_DT_TM; source '
                                                                     'value retained unless documented as '
                                                                     'derived.',
                                                'nhs_number_valid_ind': 'B3 bronze field '
                                                                        'NHS_NUMBER_VALID_IND; source value '
                                                                        'retained unless documented as '
                                                                        'derived.',
                                                'pathway_oid': 'B3 bronze field PATHWAY_OID; source value '
                                                               'retained unless documented as derived.',
                                                'patient_event_key': 'Deterministic event key derived from '
                                                                     'source identifiers; retained as a '
                                                                     'stable join key.',
                                                'patient_oid': 'B3 bronze field PATIENT_OID; source value '
                                                               'retained unless documented as derived.',
                                                'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                             'resolved.',
                                                'person_link_method': 'B3 bronze field PERSON_LINK_METHOD; '
                                                                      'source value retained unless '
                                                                      'documented as derived.',
                                                'person_link_status': 'Cross-arm consensus status; conflicts '
                                                                      'never publish PERSON_ID.',
                                                'planned_datetime': 'B3 bronze field PLANNED_DT_TM_CLEAN; '
                                                                    'source value retained unless documented '
                                                                    'as derived.',
                                                'priority_rvid': 'B3 bronze field '
                                                                 'WAITING_LIST_PRIORITY_RVID; source value '
                                                                 'retained unless documented as derived.',
                                                'record_status': 'Retracted when '
                                                                 'map_elective_access_list.SOURCE_PRESENT_IND '
                                                                 'is false; otherwise superseded when '
                                                                 'ACTIVE_IND is false; otherwise active. '
                                                                 'Each missing flag defaults true, and '
                                                                 'retraction takes priority over '
                                                                 'supersession. This is a derived label, not '
                                                                 'the source closure flag.',
                                                'record_status_effective_from': 'map_elective_access_list.CREATED_DT_TM '
                                                                                'carried directly as the '
                                                                                'record-status start, '
                                                                                'without event-time '
                                                                                'clamping. No admission, '
                                                                                'procedure or '
                                                                                'child-attribute date '
                                                                                'replaces it.',
                                                'record_status_effective_to': 'map_elective_access_list.SOURCE_ABSENT_DETECTED_TS '
                                                                              'for a retracted row; '
                                                                              'otherwise MODIFIED_DT_TM for '
                                                                              'a superseded row; otherwise '
                                                                              'null. Retraction takes '
                                                                              'priority. A missing timestamp '
                                                                              'in the selected branch '
                                                                              'remains null: there is no '
                                                                              'fallback to the other branch '
                                                                              'or ADC_UPDT.',
                                                'referral_oid': 'B3 bronze field REFERRAL_OID; source value '
                                                                'retained unless documented as derived.',
                                                'removal_reason_rvid': 'B3 bronze field REMOVAL_REASON_RVID; '
                                                                       'source value retained unless '
                                                                       'documented as derived.',
                                                'site_rvid': 'B3 bronze field SITE_RVID; source value '
                                                             'retained unless documented as derived.',
                                                'source_code': 'B3 bronze field PROCEDURE_CODE; source value '
                                                               'retained unless documented as derived.',
                                                'source_coding_system': 'B3 bronze field PROCEDURE_CODE; '
                                                                        'source value retained unless '
                                                                        'documented as derived.',
                                                'source_display': 'B3 bronze field PROCEDURE_DESC; source '
                                                                  'value retained unless documented as '
                                                                  'derived.',
                                                'source_feed': 'Originating data feed responsible for the '
                                                               'record for each elective access entry '
                                                               'record. It is produced by the silver '
                                                               'transformation and has no direct '
                                                               'bronze-column lineage entry. Whitespace and '
                                                               'source sentinel text are retained unless the '
                                                               'pipeline explicitly normalizes them; null '
                                                               'means no value was supplied.',
                                                'source_system_oid': 'LUNA SOURCE_SYSTEM_OID as BIGINT; '
                                                                     'first component of the primary key.',
                                                'source_update_timestamp': 'map_elective_access_list.MODIFIED_DT_TM '
                                                                           'carried unchanged, not a maximum '
                                                                           'over the joined procedure or '
                                                                           'attribute children. No ingestion '
                                                                           'or Silver refresh timestamp is '
                                                                           'substituted.',
                                                'status_change_datetime': 'B3 bronze field '
                                                                          'WAITING_LIST_STATUS_CHANGE_DT_TM_CLEAN; '
                                                                          'source value retained unless '
                                                                          'documented as derived.',
                                                'status_reason': 'B3 bronze field '
                                                                 'WAITING_LIST_STATUS_REASON; source value '
                                                                 'retained unless documented as derived.',
                                                'status_rvid': 'B3 bronze field WAITING_LIST_STATUS_RVID; '
                                                               'source value retained unless documented as '
                                                               'derived.',
                                                'subject_id_system': 'B3 bronze field PATIENT_OID; source '
                                                                     'value retained unless documented as '
                                                                     'derived.',
                                                'subject_key': 'Deterministic SHA-256 over the strongest '
                                                               'source identifier. Not salted, not secret; a '
                                                               'stable join key across feeds where person_id '
                                                               'is unresolved.',
                                                'tci_created_datetime': 'B3 bronze field TCI_CREATED_DT_TM; '
                                                                        'source value retained unless '
                                                                        'documented as derived.',
                                                'tci_datetime': 'B3 bronze field TCI_DT_TM_CLEAN; source '
                                                                'value retained unless documented as '
                                                                'derived.',
                                                'tci_future_ind': 'B3 bronze field TCI_DT_TM_FUTURE_IND; '
                                                                  'source value retained unless documented '
                                                                  'as derived.',
                                                'tci_location_rvid': 'B3 bronze field TCI_LOCATION_RVID; '
                                                                     'source value retained unless '
                                                                     'documented as derived.',
                                                'treatment_function_rvid': 'B3 bronze field '
                                                                           'TREATMENT_FUNCTION_RVID; source '
                                                                           'value retained unless documented '
                                                                           'as derived.',
                                                'vip_ind': 'Indicator that the source record carries VIP '
                                                           'handling status for each elective access entry '
                                                           'record. It is produced by the silver '
                                                           'transformation and has no direct bronze-column '
                                                           'lineage entry. Null means the source did not '
                                                           'state the indicator and must not be interpreted '
                                                           'as false.',
                                                'waiting_list_code': 'B3 bronze field WAITING_LIST_CODE; '
                                                                     'source value retained unless '
                                                                     'documented as derived.',
                                                'waiting_list_id': 'B3 bronze field WAITING_LIST_ID; source '
                                                                   'value retained unless documented as '
                                                                   'derived.',
                                                'waiting_list_name': 'B3 bronze field WAITING_LIST_NAME; '
                                                                     'source value retained unless '
                                                                     'documented as derived.',
                                                'waiting_list_oid': 'LUNA WAITING_LIST_OID as BIGINT; second '
                                                                    'component of the primary key.',
                                                'withheld_identity_ind': 'Indicator that the source identity '
                                                                         'was deliberately withheld for each '
                                                                         'elective access entry record. It '
                                                                         'is produced by the silver '
                                                                         'transformation and has no direct '
                                                                         'bronze-column lineage entry. Null '
                                                                         'means the source did not state the '
                                                                         'indicator and must not be '
                                                                         'interpreted as false.'},
                                    'table': 'One LUNA elective-access entry with primary OPCS and '
                                             'clinical-priority folds.'}}

# INSR_R4_SILVER_V1 (module_list): generated by r4/release/gen_silver_components.py; edit the generator, not this.
ELECTIVE_ACCESS_ENTRY_PUBLIC_COLUMNS = list(ELECTIVE_ACCESS_ENTRY_PUBLIC_COLUMNS) + ['identity_status']  # decision 1: contract columns, additions last
