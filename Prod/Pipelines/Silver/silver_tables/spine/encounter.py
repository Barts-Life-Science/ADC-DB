"""Silver spine.encounter: 2 dataset(s).

- datasets: spine_encounter, _spine_encounter_metadata
- moved verbatim from: 1_silver_spine_and_reference:4291, 1_silver_spine_and_reference:4360, 1_silver_spine_and_reference:4372

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n
from silver_tables.spine._shared import ENCOUNTER_PUBLIC_COLUMNS, _lifecycle_source_encounter


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


ENCOUNTER_LIFECYCLE_COLUMNS = [
    'encounter_key',
    'encounter_id',
    'load_batch_id',
]


@materialized_view(
    name=_n("journey_spine.encounter"),
    cluster_by=["person_id", "period_start"],
    refresh_policy="auto",
)
def encounter():
    # Build the declared dataset: One source encounter with evidence-based timestamps. Parent
    # remains null where bronze supplies no governed containment key.
    return _lifecycle_source_encounter().select(*ENCOUNTER_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_spine._encounter_metadata'),
    refresh_policy="auto",
)
def encounter_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for spine_encounter; same
    # row grain as the research table. Join keys: encounter_key, encounter_id.
    return (_lifecycle_source_encounter()).select(*ENCOUNTER_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_spine_encounter_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged from '
                                                            'the public parent; combine it with the '
                                                            "companion's other key columns when joining.",
                                           'activity_record_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                           'admission_key': 'Deterministic v2 key carried unchanged from the '
                                                            'public parent; joins one lifecycle row to that '
                                                            'research row.',
                                           'allergy_instance_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                           'aria_dx_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                           'aria_pt_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
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
                                                     'public parent; joins one lifecycle row to that '
                                                     'research row.',
                                           'chemotherapy_course_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
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
                                           'cost_line_item_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                           'dcp_forms_activity_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                           'device_mapping_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                           'diagnosis_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                           'dicom_path': 'Primary-key component carried unchanged from the '
                                                         "public parent; combine it with the companion's "
                                                         'other key columns when joining.',
                                           'doc_response_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                           'elective_access_procedure_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                           'encounter_attribute_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
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
                                           'episode_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                           'episode_id_resolved': 'True when episode_id is null or resolves '
                                                                  'to the governed episode spine; false '
                                                                  'records unresolved linkage without '
                                                                  'dropping the row.',
                                           'episode_key': 'Deterministic v2 key carried unchanged from the '
                                                          'public parent; joins one lifecycle row to that '
                                                          'research row.',
                                           'event_after_death_30d': 'True when the event timestamp is more '
                                                                    'than 30 days after the resolved '
                                                                    "person's death timestamp; silver "
                                                                    'records the doubt and does not filter '
                                                                    'the row.',
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
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                           'journey_participant_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                           'labour_delivery_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                           'load_batch_id': 'Deterministic batch token derived from the '
                                                            "source row's bronze load timestamp.",
                                           'loaded_at': 'Latest bronze load timestamp contributing to the '
                                                        "public row; never the lifecycle pipeline's "
                                                        'wall-clock time.',
                                           'location_code': 'Primary-key component carried unchanged from '
                                                            'the public parent; combine it with the '
                                                            "companion's other key columns when joining.",
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
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
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
                                                                'unchanged from the public parent; joins one '
                                                                'lifecycle row to that event row.',
                                           'patient_event_row_key': 'Deterministic row-level key carried '
                                                                    'unchanged from events_patient_event; '
                                                                    'joins one lifecycle row to one '
                                                                    'event-index row.',
                                           'period_business_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                           'period_oid': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
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
                                           'person_relationship_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
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
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                           'practitioner_location_evidence_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                           'pregnancy_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                           'pregnancy_reconciliation_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                           'problem_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                           'procedure_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                           'pt_prot_reg_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                           'ptl_unique_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                           'record_status': 'active, superseded or retracted, normalised '
                                                            "from the source's own status fields; superseded "
                                                            'rows are earlier versions the source kept.',
                                           'record_status_effective_from': 'Timestamp from which the '
                                                                           'normalised record status '
                                                                           'applies, when the source '
                                                                           'supplies one.',
                                           'record_status_effective_to': 'Timestamp until which the '
                                                                         'normalised record status applies; '
                                                                         'null means no source end is known.',
                                           'referral_oid': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                           'report_version_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
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
                                           'service_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
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
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
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
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                               'table': 'Internal quality and batch metadata for spine_encounter; same row '
                                        'grain as the research table. Join keys: encounter_key, '
                                        'encounter_id.'},
 'spine_encounter': {'columns': {'admission_source_code': 'Source admission source code.',
                                 'admission_source_display': 'Best available label for ADMIT_SRC_CD.',
                                 'arrival_confidence': 'Source-derived confidence for period_start.',
                                 'arrival_method': 'Method selecting period_start.',
                                 'attendance_evidence': 'Productised attendance evidence classification.',
                                 'attendance_witness_count': 'Number of attendance witnesses in the source '
                                                             'product.',
                                 'class_actcode': 'HL7 v3 ActCode for the encounter class (the vocabulary '
                                                  'FHIR Encounter.class uses): IMP, AMB, EMER or PRENC. From '
                                                  'encounter_level for '
                                                  'spell/outpatient_attendance/emergency_visit/preadmission '
                                                  '(the type class is the stronger signal); from the source '
                                                  'class display for recurring_contact and other; NULL for '
                                                  'results_only, waiting_list_placeholder and unmapped '
                                                  'classes (Wait List has no ActCode). Local codes are never '
                                                  'relabelled; class_code stays the Cerner code.',
                                 'class_actcode_display': 'HL7 ActCode display for class_actcode.',
                                 'class_actcode_method': 'How class_actcode was assigned: '
                                                         'encounter_level_crosswalk, source_class_crosswalk '
                                                         'or no_class (class_actcode NULL). Never NULL.',
                                 'class_code': 'Source (local Cerner) encounter class code; see '
                                               'class_actcode for the HL7 ActCode.',
                                 'class_display': 'Source encounter class display.',
                                 'confidentiality_code': 'Source confidentiality code.',
                                 'current_location_key': "Deterministic key of the encounter's current nurse "
                                                         'unit (joins reference_location.location_key); null '
                                                         'when map_encounter.LOC_NURSE_UNIT_CD is null or 0 '
                                                         '(no unit recorded).',
                                 'departure_confidence': 'Source-derived confidence for period_end.',
                                 'departure_method': 'Method selecting period_end.',
                                 'departure_rejected_reason': 'Why bronze refused the winning departure '
                                                              'candidate, leaving period_end NULL: '
                                                              'BEFORE_ARRIVAL (it preceded the arrival), '
                                                              'ADMINISTRATIVE_CLOSE (a nightly '
                                                              'inactivity-timeout close) or '
                                                              'FIXED_WINDOW_CLOSE (the 60-day outpatient '
                                                              'close job). NULL when no candidate was '
                                                              'refused. '
                                                              'map_encounter.DEPARTURE_REJECTED_REASON.',
                                 'discharge_datetime': 'Source actual discharge timestamp.',
                                 'discharge_destination_code': 'Source discharge destination code.',
                                 'discharge_destination_display': 'Best available label for '
                                                                  'DISCH_TO_LOCTN_CD.',
                                 'encounter_close_state': 'Whether the encounter is known to be closed, in '
                                                          'precedence order: departed (period_end is set); '
                                                          'source_closed_no_observed_departure (an '
                                                          'administrative or fixed-window job closed it, or '
                                                          'the source status is Discharged with no departure '
                                                          'stamp; departure time unknown); source_cancelled '
                                                          '(source status Cancelled, Cancelled Pending '
                                                          'Arrival or Rejected Pending Arrival: the '
                                                          'encounter never ran); source_not_arrived (source '
                                                          'status Pending Arrival: the patient never '
                                                          'arrived); departure_evidence_invalid (a departure '
                                                          'stamp precedes arrival, closure unknown); '
                                                          'open_ward_interval (the latest nurse-unit '
                                                          'interval has no end); no_departure_recorded '
                                                          '(source status still Active or Preadmit and no '
                                                          'departure evidence; open or abandoned, judge with '
                                                          'last_activity_datetime). Never NULL; no reference '
                                                          'to the current date.',
                                 'encounter_id': 'Deterministic encounter primary key.',
                                 'encounter_key': 'Source encounter primary key.',
                                 'encounter_level': 'Rules-light source-classified encounter level.',
                                 'inpatient_admit_datetime': 'Inpatient admission timestamp.',
                                 'last_activity_datetime': 'Latest recorded activity on the encounter (UTC '
                                                           'instant): greatest of the last current clinical '
                                                           'event, last active order and last nurse-unit '
                                                           'arrival. NULL when none exists. Use it to judge '
                                                           'whether an encounter with no departure is still '
                                                           'live.',
                                 'length_of_stay_minutes': 'Source-productised encounter duration.',
                                 'loaded_at': 'map_encounter.ADC_UPDT carried unchanged from the '
                                              'contributing bronze encounter row. This is ingestion '
                                              'provenance, not the Silver refresh time or encounter '
                                              'arrival/departure; a missing source timestamp remains null.',
                                 'organization_id': 'Source encounter organization reference.',
                                 'parent_encounter_id': 'Governed containment parent when supplied by source '
                                                        'evidence.',
                                 'parentage_status': 'Provenance state for encounter containment.',
                                 'period_end': 'Best observed encounter end.',
                                 'period_end_local': 'period_end in Europe/London wall-clock time '
                                                     '(map_encounter.DEPARTURE_DT_TM_BEST_LOCAL). Silver '
                                                     'timestamps are UTC instants; take local calendar dates '
                                                     'and clock times from this column.',
                                 'period_start': 'Best observed encounter start.',
                                 'period_start_local': 'period_start in Europe/London wall-clock time '
                                                       '(map_encounter.ARRIVAL_DT_TM_BEST_LOCAL). Silver '
                                                       'timestamps are UTC instants; take local calendar '
                                                       'dates and clock times from this column.',
                                 'person_id': 'Resolved Millennium person identifier.',
                                 'raw_arrival_datetime': 'Raw ARRIVE_DT_TM retained without asserting '
                                                         'observability.',
                                 'raw_departure_datetime': 'Raw DEPART_DT_TM retained without asserting '
                                                           'clinical meaning.',
                                 'reason_for_visit': 'Verbatim source reason for visit.',
                                 'record_status': 'Derived encounter-row status: superseded when '
                                                  'map_encounter.END_EFFECTIVE_DT_TM is earlier than '
                                                  '2100-01-01; otherwise active, including a null or '
                                                  'open-ended source timestamp. This classification does not '
                                                  'use the current wall clock or ACTIVE_IND and is distinct '
                                                  'from encounter workflow/discharge status.',
                                 'record_status_effective_from': 'map_encounter.ACTIVE_STATUS_DT_TM carried '
                                                                 'unchanged as the source active-status '
                                                                 'timestamp. No arrival, registration or '
                                                                 'ingestion-clock fallback is applied; a '
                                                                 'missing source value remains null.',
                                 'record_status_effective_to': 'map_encounter.END_EFFECTIVE_DT_TM when it is '
                                                               'earlier than 2100-01-01; otherwise null, '
                                                               'including missing values and open-ended '
                                                               'sentinels. This is the row-version end '
                                                               'boundary, not the encounter departure or '
                                                               'discharge time.',
                                 'registration_datetime': 'Registration timestamp retained as source '
                                                          'evidence.',
                                 'responsible_service_code': 'Source medical service code.',
                                 'responsible_service_display': 'Best available label for MED_SERVICE_CD.',
                                 'scheduled_end': 'Scheduled departure timestamp.',
                                 'scheduled_start': 'Scheduled arrival timestamp.',
                                 'service_provider_organization_key': 'Source organization primarily '
                                                                      'responsible for the encounter.',
                                 'source_update_timestamp': 'map_encounter.UPDT_DT_TM carried unchanged from '
                                                            'the source encounter row. It is separate from '
                                                            'the bronze ADC_UPDT ingestion clock and is not '
                                                            'the Silver refresh time; a missing source '
                                                            'timestamp remains null.',
                                 'specialty_code': 'Source specialty-unit code.',
                                 'specialty_display': 'Source specialty-unit display.',
                                 'status_code': 'Native encounter status code.',
                                 'status_display': 'Native encounter status display.',
                                 'subject_id_system': 'Identifier system used for subject_key.',
                                 'subject_key': 'Always-populated subject join key.',
                                 'type_class_code': 'Source encounter type-class code.',
                                 'type_class_display': 'Source encounter type-class display.',
                                 'type_code': 'Source encounter type code.',
                                 'type_display': 'Source encounter type display.',
                                 'vip_ind': 'Source VIP indicator.',
                                 'workflow_complete_datetime': 'Administrative workflow completion '
                                                               'timestamp.'},
                     'table': 'One source encounter with evidence-based timestamps. Parent remains null '
                              'where bronze supplies no governed containment key.'}}
