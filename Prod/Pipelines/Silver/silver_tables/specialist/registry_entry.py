"""Silver specialist.registry_entry: 3 dataset(s).

- datasets: _clinical_qc_registry_entry, clinical_registry_entry, _clinical_registry_entry_metadata
- moved verbatim from: 3_silver_specialist_care:122, 3_silver_specialist_care:163, 3_silver_specialist_care:181, 3_silver_specialist_care:66, 3_silver_specialist_care:91

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, spark


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical /  qc registry entry
@materialized_view(
    name=_n("journey_clinical._qc_registry_entry"),
    refresh_policy="auto",
)
def _qc_registry_entry():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # registry_entry.
    source = spark.read.table(_n("journey_clinical._registry_device_stage")).where(
        F.col("_stage_kind") == "registry"
    )
    return _cross_qc_primitive(
        source,
        "registry_entry",
        {"registry_payload": "_registry_payload_json"},
    )


# Clinical / registry entry

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
REGISTRY_ENTRY_PUBLIC_COLUMNS = [
    'patient_event_key',
    'source_object',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'event_datetime_local',
    'event_end_datetime_local',
    # TZ_SILVER_LOCAL_V1: Europe/London companions (bronze *_LOCAL for Millennium; local-clock sources unchanged)
    'registry_family',
    'registry_type',
    'entry_id',
    'parent_registry_entry_key',
    'linkage_status',
    'mrn',
    'nhs_number',
    'date_of_death',
    'source_edit_datetime',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]


REGISTRY_ENTRY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'entry_id',
    'identity_status',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


@materialized_view(
    name=_n("journey_clinical.registry_entry"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def registry_entry():
    # Build the declared dataset: Parameterized iWeb cardiac-registry entries across ten source
    # families.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_registry_entry")),
        "registry_entry",
        {},
        REGISTRY_ENTRY_PUBLIC_COLUMNS,
        REGISTRY_ENTRY_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._registry_entry_metadata'),
    refresh_policy="auto",
)
def registry_entry_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_registry_entry; same row grain as the research table. Join keys:
    # patient_event_key, source_object, entry_id.
    return (spark.read.table(_n("journey_clinical._qc_registry_entry"))).select(*REGISTRY_ENTRY_LIFECYCLE_COLUMNS)


CROSS_QC = {
    'registry_entry': CrossQc(model='registry_entry', dates=('event_after_death_30d', 'event_before_birth'), promote_lifecycle=['identity_status']),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_qc_registry_entry': {'columns': {},
                                 'table': 'Private JSON bridge and Gold cross-rule flags for '
                                          'registry_entry.'},
 '_clinical_registry_entry_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
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
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'aria_pt_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'artifact_link_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                   'birth_row_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'bloodtrack_transaction_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                   'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
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
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                   'community_care_activity_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                   'community_care_contact_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                   'concept_map_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                   'cost_line_item_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                   'dcp_forms_activity_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                   'device_mapping_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                   'diagnosis_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'dicom_path': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
                                                   'doc_response_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                   'elective_access_procedure_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                   'encounter_attribute_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                   'encounter_bounds_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                   'encounter_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'encounter_id_resolved': 'True when encounter_id is null '
                                                                            'or resolves to the governed '
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
                                                   'endobase_exam_term_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                   'entity_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                   'entry_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                                   'episode_encounter_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                   'episode_encounter_reltn_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                   'episode_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'episode_id_resolved': 'True when episode_id is null or '
                                                                          'resolves to the governed episode '
                                                                          'spine; false records unresolved '
                                                                          'linkage without dropping the row.',
                                                   'episode_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                                   'event_after_death_30d': 'True when the event timestamp '
                                                                            'is more than 30 days after the '
                                                                            "resolved person's death "
                                                                            'timestamp; silver records the '
                                                                            'doubt and does not filter the '
                                                                            'row.',
                                                   'event_before_birth': 'True when the event timestamp '
                                                                         "precedes the resolved person's "
                                                                         'birth timestamp; silver records '
                                                                         'the doubt and does not filter the '
                                                                         'row.',
                                                   'event_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                                   'extract_cd': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
                                                   'fhx_activity_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                   'gene_tested_row_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
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
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                   'is_illogical': 'Source RTT chronology flag indicating an '
                                                                   'illogical activity sequence; retained '
                                                                   'for the gold drop rule.',
                                                   'journey_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                                   'journey_link_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                   'journey_participant_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                   'labour_delivery_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                   'load_batch_id': 'Deterministic batch token derived from '
                                                                    "the source row's bronze load timestamp.",
                                                   'loaded_at': 'Latest bronze load timestamp contributing '
                                                                'to the public row; never the lifecycle '
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
                                                   'mc_device_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'microbiology_isolate_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                   'order_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                                   'organization_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                   'organization_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                   'pacs_examination_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                   'pacs_report_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                   'pathway_oid': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                   'patient_event_key': 'Deterministic SHA-256 event key '
                                                                        'carried unchanged from the public '
                                                                        'parent; joins one lifecycle row to '
                                                                        'that event row.',
                                                   'patient_event_row_key': 'Deterministic row-level key '
                                                                            'carried unchanged from '
                                                                            'events_patient_event; joins one '
                                                                            'lifecycle row to one '
                                                                            'event-index row.',
                                                   'period_business_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                   'period_oid': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'person_address_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                   'person_attribute_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                   'person_death_evidence_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                   'person_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                   'person_id_resolved': 'True when person_id is null or '
                                                                         'resolves to the governed person '
                                                                         'spine; false records unresolved '
                                                                         'linkage without dropping the row.',
                                                   'person_identifier_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                   'person_relationship_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
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
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                   'practitioner_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                   'practitioner_location_evidence_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                   'pregnancy_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'pregnancy_reconciliation_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                   'problem_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'procedure_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'pt_prot_reg_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
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
                                                   'referral_oid': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
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
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'score_type': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
                                                   'sequence_nbr': 'Primary-key component carried unchanged '
                                                                   'from the public parent; combine it with '
                                                                   "the companion's other key columns when "
                                                                   'joining.',
                                                   'service_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'service_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                                   'snapshot_date': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                                   'source_daily_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                   'source_identifier_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                   'source_object': 'Source-arm discriminator carried '
                                                                    'unchanged from the public parent; use '
                                                                    'it with arm-specific native identifiers '
                                                                    'when joining.',
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
                                                                              'timestamp carried through the '
                                                                              'canonical silver query.',
                                                   'source_version_id': 'Native source version identifier '
                                                                        'carried unchanged from the public '
                                                                        'parent; CURRENT rows carry -1. A '
                                                                        'key component alongside row_source.',
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
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                   'theatre_implant_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                   'transfusion_event_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                   'transfusion_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
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
                                                   'waiting_list_snapshot_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.'},
                                       'table': 'Internal quality and batch metadata for '
                                                'clinical_registry_entry; same row grain as the research '
                                                'table. Join keys: patient_event_key, source_object, '
                                                'entry_id.'},
 'clinical_registry_entry': {'columns': {'confidentiality_code': 'Security classification when supplied.',
                                         'date_of_death': 'Family-specific demographic date of death.',
                                         'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                         'available.',
                                         'entry_id': 'Native iWeb ENTRY_ID as BIGINT; primary key within '
                                                     'source_object.',
                                         'event_datetime': 'Family-specific clinical timestamp; child rows '
                                                           'inherit the parent timestamp.',
                                         'event_datetime_local': 'event_datetime in Europe/London wall-clock '
                                                                 'time (GMT/BST). Millennium rows take the '
                                                                 'bronze *_LOCAL companions of the same '
                                                                 'source columns and fallback order; rows '
                                                                 'from local-clock sources (LUNA, '
                                                                 'PACS/DICOM, TFC LIMS, CCMDS, MSDS, '
                                                                 'BadgerNet, EndoBase, SLAM, iWeb, Datix and '
                                                                 'similar) carry event_datetime unchanged. '
                                                                 'event_datetime is a UTC instant for '
                                                                 'Millennium rows, so take local calendar '
                                                                 'dates and clock times from this column.',
                                         'event_end_datetime': 'Family-specific clinical end timestamp.',
                                         'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                     'wall-clock time (GMT/BST), by the same '
                                                                     'rule as event_datetime_local; null '
                                                                     'where event_end_datetime is null.',
                                         'linkage_status': 'Bronze person-link status.',
                                         'loaded_at': 'ADC_UPDT of the contributing iWeb row, carried '
                                                      'unchanged per family. Surgery-procedure and '
                                                      'coronary-lesion parent joins select parent identity '
                                                      'and clinical event time, not a parent load clock. The '
                                                      'family union takes no cross-family maximum; device '
                                                      'rows are excluded before registry QC/public '
                                                      'projections.',
                                         'mrn': 'Source MRN where supplied.',
                                         'nhs_number': 'Source NHS number where supplied.',
                                         'parent_registry_entry_key': 'Deterministic SHA-256 key for the '
                                                                      'parent registry entry.',
                                         'patient_event_key': 'Deterministic event key derived from source '
                                                              'identifiers; retained as a stable join key.',
                                         'person_id': 'Native Millennium PERSON_ID as BIGINT when resolved.',
                                         'record_status': "Superseded when the contributing iWeb row's "
                                                          'SOURCE_PRESENT_IND is false, otherwise active; '
                                                          'missing presence defaults true. This rule applies '
                                                          'to all ten configured registry families and emits '
                                                          'no retracted status. Device rows in the shared '
                                                          'internal stage are filtered out before registry '
                                                          'publication.',
                                         'record_status_effective_from': 'Always null as a TIMESTAMP for all '
                                                                         'ten configured iWeb registry '
                                                                         "families. Neither the row's "
                                                                         'clinical event date, a joined '
                                                                         'parent procedure/episode event '
                                                                         'date, DATE_LAST_CHANGED nor a '
                                                                         'MediConnect implant date fills '
                                                                         'this history start.',
                                         'record_status_effective_to': 'For nine iWeb families, '
                                                                       'SOURCE_ABSENT_DETECTED_TS only when '
                                                                       'source presence is false, otherwise '
                                                                       'null. Coronary-procedure rows always '
                                                                       'have a null history end, including '
                                                                       'source-absent rows, because that '
                                                                       "family's configured absence-time "
                                                                       'expression is null. No ADC_UPDT or '
                                                                       'parent-date fallback is applied.',
                                         'registry_family': 'Parameterized registry family.',
                                         'registry_payload': 'Full verbatim source row as key/value pairs.',
                                         'registry_type': 'Registry subtype for composite-key MDT rows.',
                                         'source_edit_datetime': 'Source edit timestamp; never treated as '
                                                                 'clinical time.',
                                         'source_object': 'iWeb registry family identifying the source '
                                                          'object; part of the primary key with entry_id.',
                                         'source_update_timestamp': 'Timestamp of the last change made to '
                                                                    'the record in the source registry '
                                                                    'system.',
                                         'subject_id_system': 'Identifier system used for subject_key.',
                                         'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                        'identifier. Not salted, not secret; a stable join '
                                                        'key across feeds where person_id is unresolved.',
                                         'vip_ind': 'VIP indicator when supplied.',
                                         'withheld_identity_ind': 'Identity-withholding status is not '
                                                                  'populated by the current source '
                                                                  'projections; null does not mean identity '
                                                                  'was not withheld.'},
                             'table': 'Parameterized iWeb cardiac-registry entries across ten source '
                                      'families.'}}
