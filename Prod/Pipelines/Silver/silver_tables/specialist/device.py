"""Silver specialist.device: 3 dataset(s).

- datasets: _clinical_qc_device, clinical_device, _clinical_device_metadata
- moved verbatim from: 3_silver_specialist_care:6231, 3_silver_specialist_care:6344, 3_silver_specialist_care:6362

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, spark
from silver_tables.specialist._shared import DEVICE_LIFECYCLE_COLUMNS, DEVICE_PUBLIC_COLUMNS


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical /  qc device
@materialized_view(
    name=_n("journey_clinical._qc_device"),
    refresh_policy="auto",
)
def _qc_device():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for device.
    source = spark.read.table(_n("journey_clinical._registry_device_stage")).where(
        F.col("_stage_kind") == "device"
    )
    return _cross_qc_primitive(
        source,
        "device",
        {"device_code": "_device_code_json"},
    )


@materialized_view(
    name=_n("journey_clinical.device"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def device():
    # Build the declared dataset: One implanted-device registry record from the frozen
    # MediConnect source, retaining native identifiers and device details.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_device")),
        "device",
        {"device_code": "_device_code_json"},
        DEVICE_PUBLIC_COLUMNS,
        DEVICE_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._device_metadata'),
    refresh_policy="auto",
)
def device_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_device; same
    # row grain as the research table. Join keys: patient_event_key, mc_device_id.
    return (spark.read.table(_n("journey_clinical._qc_device"))).select(*DEVICE_LIFECYCLE_COLUMNS)


CROSS_QC = {
    'device': CrossQc(model='device', dates=('event_after_death_30d', 'event_before_birth'), coded_axes={'device_code': ('device', False)}, extra_public_columns=('device_mapping_rule_id', 'device_mapping_llm_decision'), embedding_lanes=(('device', None, 'bronze_mediconnect_device'),), promote_lifecycle=['identity_status']),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {
    'clinical_device': ValuePolicy(table='clinical_device', enabled=True),
}


WRAP_POLICY = {'clinical_device'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_device_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged from '
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
                               'table': 'Internal quality and batch metadata for clinical_device; same row '
                                        'grain as the research table. Join keys: patient_event_key, '
                                        'mc_device_id.'},
 '_clinical_qc_device': {'columns': {}, 'table': 'Private JSON bridge and Gold cross-rule flags for device.'},
 'clinical_device': {'columns': {'comment_text': 'Source device comment.',
                                 'confidentiality_code': 'Security classification when supplied.',
                                 'device_map_candidate_count': 'Number of candidate targets observed; a '
                                                               'mapped parent is unambiguous when this is 1.',
                                 'device_map_candidate_set_id': 'Stable identifier of the retained candidate '
                                                                'set.',
                                 'device_map_competing_count': 'Candidates within 0.05 cosine of the '
                                                               'selected candidate.',
                                 'device_map_component_count': 'Number of source-derived components.',
                                 'device_map_component_index': 'One-based source-derived component index.',
                                 'device_map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                          'measured one.',
                                 'device_map_cosine': 'Measured cosine between the normalised source and '
                                                      'selected target; null when scoring did not run.',
                                 'device_map_method': 'Mapping method: source_native, rule, exact_synonym or '
                                                      'embedding_proposed.',
                                 'device_map_rollup_levels': 'Maximum hierarchy distance to a selected '
                                                             'common ancestor.',
                                 'device_map_rule_id': 'Stable rule or lane identifier that produced the '
                                                       'mapping.',
                                 'device_map_scoring_model': 'Embedding model that produced map_cosine.',
                                 'device_map_status': 'Explicit mapping outcome from the S3b status '
                                                      'vocabulary.',
                                 'device_map_status_reason': 'Reason for the mapping outcome or abstention.',
                                 'device_map_version': 'Version of the source, terminology release or model '
                                                       'used by the mapping.',
                                 'device_mapping_llm_decision': 'Verifier verdict of the device-type mapping '
                                                                'row. (bronze DEVICE_MAPPING_LLM_DECISION, '
                                                                'INSR W1). INSR_W2 W2.20.',
                                 'device_mapping_rule_id': 'Rule id of the device-type mapping row '
                                                           '(INSR_W1_MEDICONNECT_V1). (bronze '
                                                           'DEVICE_MAPPING_RULE_ID, INSR W1). INSR_W2 W2.20.',
                                 'device_mapping_status': 'Device terminology mapping status.',
                                 'device_omop_concept_id': 'OMOP concept identifier selected with the device '
                                                           'mapping.',
                                 'device_omop_source_concept_id': 'Non-standard OMOP source concept '
                                                                  'identifier, kept separate from a standard '
                                                                  'target.',
                                 'device_role': 'GENERATOR | LEAD | MONITOR | OTHER',
                                 'device_snomed_code': 'SNOMED CT code selected for the device; null when no '
                                                       'unambiguous mapping is available.',
                                 'device_snomed_display': 'SNOMED CT preferred display selected for the '
                                                          'device.',
                                 'device_source_code': 'Source code for the device, retained without '
                                                       'de-identification.',
                                 'device_source_display': 'Source display for the device.',
                                 'device_source_system': 'Canonical coding-system URI for the source device.',
                                 'device_target_code': 'Code of the selected target in its own vocabulary.',
                                 'device_target_display': 'Display of the selected target in its own '
                                                          'vocabulary.',
                                 'device_target_system': 'Coding-system URI of the selected target.',
                                 'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when available.',
                                 'event_datetime': 'Sentinel-cleaned implantation timestamp.',
                                 'event_datetime_local': 'event_datetime in Europe/London wall-clock time '
                                                         '(GMT/BST). Millennium rows take the bronze *_LOCAL '
                                                         'companions of the same source columns and fallback '
                                                         'order; rows from local-clock sources (LUNA, '
                                                         'PACS/DICOM, TFC LIMS, CCMDS, MSDS, BadgerNet, '
                                                         'EndoBase, SLAM, iWeb, Datix and similar) carry '
                                                         'event_datetime unchanged. event_datetime is a UTC '
                                                         'instant for Millennium rows, so take local '
                                                         'calendar dates and clock times from this column.',
                                 'event_end_datetime': 'Device lifecycle end timestamp when available.',
                                 'event_end_datetime_local': 'event_end_datetime in Europe/London wall-clock '
                                                             'time (GMT/BST), by the same rule as '
                                                             'event_datetime_local; null where '
                                                             'event_end_datetime is null.',
                                 'explanted_ind': 'Inferred explant indicator; not independently confirmed.',
                                 'implant_date_quality': 'Implant-date quality class.',
                                 'implanted_datetime': 'Source implantation timestamp.',
                                 'lead_chamber': 'Cardiac chamber for a lead, parsed from the structured '
                                                 'source LOCATION1 text and null for non-lead records.',
                                 'lead_location': 'Anatomical implant location of a lead, parsed from the '
                                                  'structured source LOCATION1 text and null for non-lead '
                                                  'records.',
                                 'loaded_at': 'mediconnect_device.ADC_UPDT carried unchanged. The '
                                              'device-type lookup contributes mapping fields but no clocks, '
                                              'and registry rows in the shared internal stage are excluded '
                                              'before device QC/public projections. This is not a maximum '
                                              'with registry or lookup clocks, implant time or the current '
                                              'Silver refresh time.',
                                 'manufacturer': 'Raw manufacturer name for the device or lead exactly as '
                                                 'recorded in the source registry.',
                                 'manufacturer_parent': 'Parent or successor corporate group for the '
                                                        'recorded manufacturer, derived from the raw '
                                                        'MANUFACTURER value and null where no parent '
                                                        'applies.',
                                 'mc_device_id': 'MediConnect MC_DEVICE_ID; primary key of this table.',
                                 'model_code': 'Device model code.',
                                 'model_name': 'Device model name.',
                                 'patient_event_key': 'Deterministic event key derived from source '
                                                      'identifiers; retained as a stable join key.',
                                 'person_id': 'Native Millennium PERSON_ID as BIGINT when resolved.',
                                 'pocket_site': 'Device pocket site.',
                                 'record_status': 'Superseded when mediconnect_device.SOURCE_PRESENT_IND is '
                                                  'false, otherwise active; missing presence defaults true. '
                                                  'No retracted status is emitted. STATUS, EXPLANTED_IND and '
                                                  'mapping status do not set this derived label, and '
                                                  'registry rows in the shared internal stage are filtered '
                                                  'out before device publication.',
                                 'record_status_effective_from': 'Validated implant date derived from '
                                                                 'IMPLANTED_DATE, excluding sentinel, future '
                                                                 'and missing values flagged by '
                                                                 'IMPLANTED_DATE_QUALITY.',
                                 'record_status_effective_to': 'For a source-absent mediconnect_device row, '
                                                               'first non-null SOURCE_ABSENT_DETECTED_TS '
                                                               'then ADC_UPDT; otherwise null. This is not '
                                                               'an explant timestamp. Registry-family '
                                                               'absence clocks and device-type mapping '
                                                               'clocks are not used.',
                                 'serial_number': 'Device serial number.',
                                 'source_code': 'Device type with type/model display fallback for unknown '
                                                'type zero.',
                                 'source_coding_system': 'MediConnect device-type coding system.',
                                 'source_display': 'Device type or model display.',
                                 'source_update_timestamp': 'Always null as a TIMESTAMP in the current '
                                                            'MediConnect device projection. Neither ADC_UPDT '
                                                            'nor registry DATE_LAST_CHANGED is substituted. '
                                                            'Registry rows are filtered out of the shared '
                                                            'internal stage before device publication; '
                                                            'device-type mappings select no clock.',
                                 'status_display': 'Source device status display.',
                                 'subject_id_system': 'Identifier system used for subject_key.',
                                 'subject_key': 'Deterministic SHA-256 over the strongest source identifier. '
                                                'Not salted, not secret; a stable join key across feeds '
                                                'where person_id is unresolved.',
                                 'vip_ind': 'VIP indicator when supplied.',
                                 'withheld_identity_ind': 'Identity-withholding status is not populated by '
                                                          'the current source projections; null does not '
                                                          'mean identity was not withheld.'},
                     'table': 'One implanted-device registry record from the frozen MediConnect source, '
                              'retaining native identifiers and device details.'}}
