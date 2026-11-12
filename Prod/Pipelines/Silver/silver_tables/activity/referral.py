"""Silver activity.referral: 2 dataset(s).

- datasets: clinical_referral, _clinical_referral_metadata
- moved verbatim from: 4_silver_care_pathways:1478, 4_silver_care_pathways:1483, 4_silver_care_pathways:1487, 4_silver_care_pathways:1496, 4_silver_care_pathways:1501, 4_silver_care_pathways:1513

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n
from silver_tables._common.journey import _referral_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


REFERRAL_LIFECYCLE_FIELDS = [
    'identity_status',
    'load_batch_id',
]


REFERRAL_RETIRED_COLUMNS = [

]


REFERRAL_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_system_oid',
    'referral_oid',
    'identity_status',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_referral():
    # Assemble referral rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    return _referral_canonical().drop("_source_system", "_source_table", "_source_row_id")


@materialized_view(
    name=_n("journey_clinical.referral"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def referral():
    # Build the declared dataset: LUNA referrals (request side of threads). UBRN retained as
    # linkage evidence only.
    return _lifecycle_source_referral().drop(*REFERRAL_LIFECYCLE_FIELDS, *REFERRAL_RETIRED_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._referral_metadata'),
    refresh_policy="auto",
)
def referral_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_referral;
    # same row grain as the research table. Join keys: patient_event_key, source_system_oid,
    # referral_oid.
    return (_lifecycle_source_referral()).select(*REFERRAL_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'clinical_referral': ValuePolicy(table='clinical_referral', enabled=True),
}


WRAP_POLICY = {'clinical_referral'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_referral_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                             'activity_record_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                             'admission_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                             'allergy_instance_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                             'aria_dx_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                             'aria_pt_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                             'artifact_link_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                             'birth_row_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                             'bloodtrack_transaction_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                             'cancer_treatment_cycle_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                             'care_contact_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                             'care_participation_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                             'cds_id': 'Native source identifier carried unchanged from the '
                                                       'public parent; joins one lifecycle row to that '
                                                       'research row.',
                                             'chemotherapy_course_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                             'community_care_activity_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                             'community_care_contact_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                             'concept_map_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
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
                                             'dicom_path': 'Primary-key component carried unchanged from the '
                                                           "public parent; combine it with the companion's "
                                                           'other key columns when joining.',
                                             'doc_response_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'elective_access_procedure_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                             'encounter_attribute_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                             'encounter_bounds_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                             'encounter_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                             'encounter_id_resolved': 'True when encounter_id is null or '
                                                                      'resolves to the governed encounter '
                                                                      'spine; false records unresolved '
                                                                      'linkage without dropping the row.',
                                             'encounter_identifier_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                             'encounter_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                             'endobase_exam_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'endobase_exam_term_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                             'entity_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                             'entry_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                             'episode_encounter_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                             'episode_encounter_reltn_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                             'episode_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                             'episode_id_resolved': 'True when episode_id is null or '
                                                                    'resolves to the governed episode spine; '
                                                                    'false records unresolved linkage '
                                                                    'without dropping the row.',
                                             'episode_key': 'Deterministic v2 key carried unchanged from the '
                                                            'public parent; joins one lifecycle row to that '
                                                            'research row.',
                                             'event_after_death_30d': 'True when the event timestamp is more '
                                                                      'than 30 days after the resolved '
                                                                      "person's death timestamp; silver "
                                                                      'records the doubt and does not filter '
                                                                      'the row.',
                                             'event_before_birth': 'True when the event timestamp precedes '
                                                                   "the resolved person's birth timestamp; "
                                                                   'silver records the doubt and does not '
                                                                   'filter the row.',
                                             'event_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
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
                                             'genetic_result_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                             'genetic_test_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                             'homecare_request_item_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                             'identity_status': 'Subject identity resolution state recorded '
                                                                'by the silver canonical query.',
                                             'implant_event_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'implant_sequence': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
                                             'indication_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                             'is_illogical': 'Source RTT chronology flag indicating an '
                                                             'illogical activity sequence; retained for the '
                                                             'gold drop rule.',
                                             'journey_key': 'Deterministic v2 key carried unchanged from the '
                                                            'public parent; joins one lifecycle row to that '
                                                            'research row.',
                                             'journey_link_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'journey_participant_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                             'labour_delivery_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                             'load_batch_id': 'Deterministic batch token derived from the '
                                                              "source row's bronze load timestamp.",
                                             'loaded_at': 'Latest bronze load timestamp contributing to the '
                                                          "public row; never the lifecycle pipeline's "
                                                          'wall-clock time.',
                                             'location_code': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                             'location_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
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
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                             'organization_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                             'organization_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'pacs_examination_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                             'pacs_report_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                             'pathway_oid': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                             'patient_event_key': 'Deterministic SHA-256 event key carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that event row.',
                                             'patient_event_row_key': 'Deterministic row-level key carried '
                                                                      'unchanged from events_patient_event; '
                                                                      'joins one lifecycle row to one '
                                                                      'event-index row.',
                                             'period_business_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                             'period_oid': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                             'person_address_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                             'person_attribute_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                             'person_death_evidence_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                             'person_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                             'person_id_resolved': 'True when person_id is null or resolves '
                                                                   'to the governed person spine; false '
                                                                   'records unresolved linkage without '
                                                                   'dropping the row.',
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
                                             'pm_wait_list_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                             'practitioner_group_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                             'practitioner_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                             'practitioner_identifier_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                             'practitioner_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'practitioner_location_evidence_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                             'pregnancy_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                             'pregnancy_reconciliation_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                             'problem_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                             'procedure_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                             'pt_prot_reg_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
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
                                                           'carried unchanged from the public parent; a key '
                                                           'component because the research table is at '
                                                           'physical version grain and patient_event_key is '
                                                           "shared across an entry's versions.",
                                             'rtt_activity_oid': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'sch_event_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                             'score_type': 'Primary-key component carried unchanged from the '
                                                           "public parent; combine it with the companion's "
                                                           'other key columns when joining.',
                                             'sequence_nbr': 'Primary-key component carried unchanged from '
                                                             'the public parent; combine it with the '
                                                             "companion's other key columns when joining.",
                                             'service_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                             'service_key': 'Deterministic v2 key carried unchanged from the '
                                                            'public parent; joins one lifecycle row to that '
                                                            'research row.',
                                             'snapshot_date': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                             'source_daily_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                             'source_identifier_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                             'source_object': 'Source-arm discriminator carried unchanged '
                                                              'from the public parent; use it with '
                                                              'arm-specific native identifiers when joining.',
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
                                             'source_version_id': 'Native source version identifier carried '
                                                                  'unchanged from the public parent; CURRENT '
                                                                  'rows carry -1. A key component alongside '
                                                                  'row_source.',
                                             'surg_case_proc_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                             'susceptibility_result_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                             'theatre_attendance_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                             'theatre_case_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'theatre_case_milestone_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                             'theatre_implant_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                             'transfusion_event_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                             'transfusion_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                             'treatment_cycle_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                             'treatment_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                             'update_count': 'Primary-key component carried unchanged from '
                                                             'the public parent; combine it with the '
                                                             "companion's other key columns when joining.",
                                             'valid_from_datetime': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                             'value_set_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                             'waiting_list_oid': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'waiting_list_snapshot_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.'},
                                 'table': 'Internal quality and batch metadata for clinical_referral; same '
                                          'row grain as the research table. Join keys: patient_event_key, '
                                          'source_system_oid, referral_oid.'},
 'clinical_referral': {'columns': {'admin_category_code': 'Source administrative-category code.',
                                   'admin_category_display': 'Source administrative-category display.',
                                   'booking_type_code': 'Source booking-type code.',
                                   'booking_type_display': 'Source booking-type display.',
                                   'business_unit': 'Source business unit.',
                                   'confidentiality_code': 'Source confidentiality code when supplied.',
                                   'division': 'Name of the clinical division responsible for the referral '
                                               'within the organisational hierarchy.',
                                   'encounter_id': 'Nullable Millennium ENCNTR_ID; the LUNA referral feed '
                                                   'supplies no encounter.',
                                   'encounter_type_code': 'Source encounter-type code.',
                                   'encounter_type_display': 'Source encounter-type display.',
                                   'ers_lead_clinician_forename': 'e-RS lead clinician forename. Identifier, '
                                                                  'published and tagged. '
                                                                  'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                  'ERS_LEAD_CLINICIAN_FORENAME, INSR W1). '
                                                                  'INSR_W2 W2.20.',
                                   'ers_lead_clinician_surname': 'e-RS lead clinician surname. Identifier, '
                                                                 'published and tagged. '
                                                                 'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                 'ERS_LEAD_CLINICIAN_SURNAME, INSR W1). '
                                                                 'INSR_W2 W2.20.',
                                   'ers_local_patient_id': 'e-RS LocalPatientID (MRN as sent by e-RS). '
                                                           'Identifier, published and tagged. '
                                                           'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                           'ERS_LOCAL_PATIENT_ID, INSR W1). INSR_W2 W2.20.',
                                   'ers_nhs_number': 'e-RS NHSNumber. Identifier, published and tagged. '
                                                     'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze ERS_NHS_NUMBER, '
                                                     'INSR W1). INSR_W2 W2.20.',
                                   'ers_pathway_start': 'e-Referral pathway start date.',
                                   'ers_patient_name': 'e-RS patient name. Identifier, published and tagged. '
                                                       'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                       'ERS_PATIENT_NAME, INSR W1). INSR_W2 W2.20.',
                                   'ers_referral_comments': 'e-RS free-text referral comments. Identifier, '
                                                            'published and tagged. '
                                                            'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                            'ERS_REFERRAL_COMMENTS, INSR W1). INSR_W2 W2.20.',
                                   'ers_referral_reason': 'e-RS free-text referral reason. Identifier, '
                                                          'published and tagged. '
                                                          'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                          'ERS_REFERRAL_REASON, INSR W1). INSR_W2 W2.20.',
                                   'ers_referred_by_care_provider_forename': 'e-RS referring care provider '
                                                                             'forename. Identifier, '
                                                                             'published and tagged. '
                                                                             'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                             '(bronze '
                                                                             'ERS_REFERRED_BY_CARE_PROVIDER_FORENAME, '
                                                                             'INSR W1). INSR_W2 W2.20.',
                                   'ers_referred_by_care_provider_surname': 'e-RS referring care provider '
                                                                            'surname. Identifier, published '
                                                                            'and tagged. '
                                                                            'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                            '(bronze '
                                                                            'ERS_REFERRED_BY_CARE_PROVIDER_SURNAME, '
                                                                            'INSR W1). INSR_W2 W2.20.',
                                   'ers_referred_by_organisation': 'e-RS referring organisation. Identifier, '
                                                                   'published and tagged. '
                                                                   'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                   'ERS_REFERRED_BY_ORGANISATION, INSR W1). '
                                                                   'INSR_W2 W2.20.',
                                   'ers_referred_by_team_name': 'e-RS referring team name. Identifier, '
                                                                'published and tagged. '
                                                                'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                'ERS_REFERRED_BY_TEAM_NAME, INSR W1). '
                                                                'INSR_W2 W2.20.',
                                   'ers_referred_to_care_provider_forename': 'e-RS receiving care provider '
                                                                             'forename. Identifier, '
                                                                             'published and tagged. '
                                                                             'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                             '(bronze '
                                                                             'ERS_REFERRED_TO_CARE_PROVIDER_FORENAME, '
                                                                             'INSR W1). INSR_W2 W2.20.',
                                   'ers_referred_to_care_provider_surname': 'e-RS receiving care provider '
                                                                            'surname. Identifier, published '
                                                                            'and tagged. '
                                                                            'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                            '(bronze '
                                                                            'ERS_REFERRED_TO_CARE_PROVIDER_SURNAME, '
                                                                            'INSR W1). INSR_W2 W2.20.',
                                   'ers_referred_to_team_name': 'e-RS receiving team name. Identifier, '
                                                                'published and tagged. '
                                                                'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                'ERS_REFERRED_TO_TEAM_NAME, INSR W1). '
                                                                'INSR_W2 W2.20.',
                                   'ers_service_name': 'e-Referral service name.',
                                   'ers_specialty': 'e-Referral specialty recorded in the source referral '
                                                    'field `ERS_SPECIALTY`.',
                                   'ers_ubrn_received': 'e-Referral UBRN received date.',
                                   'event_datetime': 'Referral received timestamp.',
                                   'event_datetime_local': 'event_datetime in Europe/London wall-clock time '
                                                           '(GMT/BST). Millennium rows take the bronze '
                                                           '*_LOCAL companions of the same source columns '
                                                           'and fallback order; rows from local-clock '
                                                           'sources (LUNA, PACS/DICOM, TFC LIMS, CCMDS, '
                                                           'MSDS, BadgerNet, EndoBase, SLAM, iWeb, Datix and '
                                                           'similar) carry event_datetime unchanged. '
                                                           'event_datetime is a UTC instant for Millennium '
                                                           'rows, so take local calendar dates and clock '
                                                           'times from this column.',
                                   'event_end_datetime': 'Event end timestamp; not supplied by LUNA '
                                                         'referrals.',
                                   'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                               'wall-clock time (GMT/BST), by the same rule '
                                                               'as event_datetime_local; null where '
                                                               'event_end_datetime is null.',
                                   'fact_category': 'Whether this fact is clinical or administrative in the '
                                                    'v2 plane merge.',
                                   'loaded_at': 'map_referral.ADC_UPDT carried unchanged through the source '
                                                'and public projections. No parent or other source clock is '
                                                'joined into this value; it differs from SOURCE_ADC_UPDT '
                                                'used for source_update_timestamp and from the current '
                                                'Silver refresh time.',
                                   'local_patient_id': "LUNA LocalPatientID (Barts MRN) of the referral's "
                                                       'patient. Identifier, published and tagged. '
                                                       'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                       'LOCAL_PATIENT_ID, INSR W1). INSR_W2 W2.20.',
                                   'nhs_number': "LUNA NHSNumber of the referral's patient, as recorded. "
                                                 'Identifier, published and tagged. '
                                                 'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze NHS_NUMBER, INSR W1). '
                                                 'INSR_W2 W2.20.',
                                   'original_received_datetime': 'Original referral received timestamp.',
                                   'pathway_oid': 'Raw LUNA pathway object identifier.',
                                   'patient_event_key': 'Deterministic SHA-256 referral identity.',
                                   'person_id': 'Millennium PERSON_ID when resolved; native person foreign '
                                                'key.',
                                   'record_status': 'Superseded when map_referral.SOURCE_PRESENT_IND or '
                                                    'ACTIVE_IND is false; each missing flag defaults true. '
                                                    'Otherwise active. Source absence and inactivity both '
                                                    'map to superseded, not retracted; the clinical status '
                                                    'code does not set this row-history label.',
                                   'record_status_effective_from': 'Date and time the referral record was '
                                                                   'created in the source system.',
                                   'record_status_effective_to': 'For a superseded referral row, the first '
                                                                 'non-null '
                                                                 'map_referral.SOURCE_ABSENT_DETECTED_TS, '
                                                                 'MODIFIED_DATETIME or ADC_UPDT, in that '
                                                                 'order; otherwise null. The same '
                                                                 'source-presence/inactivity predicate sets '
                                                                 'record_status. No event-time clamp or '
                                                                 'clinical-event timestamp is substituted.',
                                   'referral_comments': 'LUNA free-text referral comments. Identifier, '
                                                        'published and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                        '(bronze REFERRAL_COMMENTS, INSR W1). INSR_W2 W2.20.',
                                   'referral_oid': 'LUNA REFERRAL_OID; component of the native primary key.',
                                   'referral_priority_code': 'Source referral-priority code.',
                                   'referral_priority_display': 'Source referral-priority display.',
                                   'referral_reason': 'LUNA free-text referral reason. Identifier, published '
                                                      'and tagged. INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                      'REFERRAL_REASON, INSR W1). INSR_W2 W2.20.',
                                   'referral_source_code': 'Source referral-source code.',
                                   'referral_source_display': 'Source referral-source display.',
                                   'referred_by_org_id': 'Raw referring organization identifier.',
                                   'referred_by_organisation': 'LUNA referring organisation text. '
                                                               'Identifier, published and tagged. '
                                                               'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                               'REFERRED_BY_ORGANISATION, INSR W1). INSR_W2 '
                                                               'W2.20.',
                                   'referred_by_prid': 'LUNA ReferredBy_PRID: referring professional id '
                                                       '(identifier). (bronze REFERRED_BY_PRID, INSR W1). '
                                                       'INSR_W2 W2.20.',
                                   'referring_facility_code': 'Source referring-facility code.',
                                   'referring_facility_display': 'Source referring-facility display.',
                                   'service_type_requested_code': 'Source requested-service-type code.',
                                   'service_type_requested_display': 'Source requested-service-type display.',
                                   'site_code': 'Source site code.',
                                   'site_display': 'Source site display.',
                                   'source_code': 'Verbatim treatment-function code.',
                                   'source_coding_system': 'Verbatim LUNA treatment-function coding system.',
                                   'source_display': 'Verbatim treatment-function display.',
                                   'source_feed': 'Registered feed owning the fact.',
                                   'source_system_oid': 'LUNA SOURCE_SYSTEM_OID; component of the native '
                                                        'primary key.',
                                   'source_update_timestamp': 'map_referral.SOURCE_ADC_UPDT carried '
                                                              'unchanged to the research row, distinct from '
                                                              'ADC_UPDT used for loaded_at. It is not the '
                                                              'clinical event timestamp or the current '
                                                              'Silver refresh time.',
                                   'status_change_datetime': 'Source referral status-change timestamp.',
                                   'status_change_reason_code': 'Source referral status-change reason code.',
                                   'status_change_reason_display': 'Source referral status-change reason '
                                                                   'display.',
                                   'status_code': 'Source referral-status code.',
                                   'status_display': 'Source referral-status display.',
                                   'subject_id_system': 'Identifier system used for subject_key.',
                                   'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                  'identifier. Not salted, not secret; a stable join key '
                                                  'across feeds where person_id is unresolved.',
                                   'suspected_cancer_site_code': 'Source suspected-cancer-site code.',
                                   'suspected_cancer_site_display': 'Source suspected-cancer-site display.',
                                   'treatment_function_code': 'Source treatment-function code.',
                                   'treatment_function_display': 'Source treatment-function display.',
                                   'ubrn': 'Referral linkage evidence only; never a join key.',
                                   'vip_ind': 'Source VIP indicator when supplied.',
                                   'waiting_list_oid': 'Raw LUNA waiting-list object identifier.',
                                   'withheld_identity_ind': 'Withheld-identity indicator when supplied.'},
                       'table': 'LUNA referrals (request side of threads). UBRN retained as linkage evidence '
                                'only.'}}

# INSR_R4_SILVER_V1 (module_drop): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import with_columns_last  # noqa: E402
REFERRAL_LIFECYCLE_FIELDS = [c for c in REFERRAL_LIFECYCLE_FIELDS if c not in ['identity_status']]  # decision 1: published
SPECS = with_columns_last(SPECS, 'clinical_referral', ('identity_status',))  # additions last
