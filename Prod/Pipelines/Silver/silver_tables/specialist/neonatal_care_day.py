"""Silver specialist.neonatal_care_day: 3 dataset(s).

- datasets: _clinical_neonatal_care_day_stage, clinical_neonatal_care_day, _clinical_neonatal_care_day_metadata
- moved verbatim from: 3_silver_specialist_care:7028, 3_silver_specialist_care:7064, 3_silver_specialist_care:7097, 3_silver_specialist_care:7105, 3_silver_specialist_care:7151, 3_silver_specialist_care:7170, 3_silver_specialist_care:7175, 3_silver_specialist_care:7187

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n, spark
from silver_tables._common.journey import _neonatal_care_day_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# contract v2: retain the full canonical shape for internal reuse.
NEONATAL_CARE_DAY_SOURCE_COLUMNS = [
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
    "entity_id",
    "activity_date",
    "ward_location",
    "unit_function",
    "critical_care_start",
    "critical_care_discharge",
    "episode_link_status",
    "neonatal_episode_key",
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
NEONATAL_CARE_DAY_PUBLIC_COLUMNS = [
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
    'entity_id',
    'activity_date',
    'ward_location',
    'unit_function',
    'critical_care_start',
    'critical_care_discharge',
    'episode_link_status',
    'neonatal_episode_key',
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


NEONATAL_CARE_DAY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'entity_id',
    'activity_date',
    'identity_status',
    'load_batch_id',
]


NEONATAL_CARE_DAY_STAGE_COLUMNS = NEONATAL_CARE_DAY_SOURCE_COLUMNS + [
    "activity_codes_json", "high_cost_drugs_json",
]


# Clinical /  neonatal care day stage

# ==== journey_clinical._neonatal_care_day_stage (internal) ====
@materialized_view(
    name=_n("journey_clinical._neonatal_care_day_stage"),
    private=True,
    refresh_policy="auto",
)
def _neonatal_care_day_stage():
    # Build the declared dataset: Internal neonatal care-day rows carrying JSON arrays for the
    # public parent and code children.
    return _neonatal_care_day_canonical().select(*NEONATAL_CARE_DAY_STAGE_COLUMNS)


# Clinical / neonatal care day

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_neonatal_care_day():
    # Assemble neonatal care day rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    return spark.read.table(_n("journey_clinical._neonatal_care_day_stage")).select(*NEONATAL_CARE_DAY_SOURCE_COLUMNS)


@materialized_view(
    name=_n("journey_clinical.neonatal_care_day"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def neonatal_care_day():
    # Build the declared dataset: One neonatal critical-care activity day for an admitted
    # BadgerNet episode, retaining activity and high-cost-drug evidence.
    return _lifecycle_source_neonatal_care_day().select(*NEONATAL_CARE_DAY_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._neonatal_care_day_metadata'),
    refresh_policy="auto",
)
def neonatal_care_day_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_neonatal_care_day; same row grain as the research table. Join keys:
    # patient_event_key, entity_id, activity_date.
    return (_lifecycle_source_neonatal_care_day()).select(*NEONATAL_CARE_DAY_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_neonatal_care_day_metadata': {'columns': {'activity_date': 'Primary-key component carried '
                                                                       'unchanged from the public parent; '
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                      'activity_record_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'birth_row_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'bloodtrack_transaction_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                      'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                      'care_contact_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'care_participation_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
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
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                      'community_care_contact_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
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
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
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
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                      'entry_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                      'episode_encounter_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                      'episode_encounter_reltn_id': 'Native source '
                                                                                    'identifier carried '
                                                                                    'unchanged from the '
                                                                                    'public parent; joins '
                                                                                    'one lifecycle row to '
                                                                                    'that research row.',
                                                      'episode_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                      'episode_id_resolved': 'True when episode_id is null '
                                                                             'or resolves to the governed '
                                                                             'episode spine; false records '
                                                                             'unresolved linkage without '
                                                                             'dropping the row.',
                                                      'episode_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                      'event_after_death_30d': 'True when the event '
                                                                               'timestamp is more than 30 '
                                                                               'days after the resolved '
                                                                               "person's death timestamp; "
                                                                               'silver records the doubt and '
                                                                               'does not filter the row.',
                                                      'event_before_birth': 'True when the event timestamp '
                                                                            "precedes the resolved person's "
                                                                            'birth timestamp; silver records '
                                                                            'the doubt and does not filter '
                                                                            'the row.',
                                                      'event_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
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
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                      'genetic_result_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'genetic_test_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'homecare_request_item_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                      'labour_delivery_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'mc_device_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                      'microbiology_isolate_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                      'order_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
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
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                      'patient_event_key': 'Deterministic SHA-256 event key '
                                                                           'carried unchanged from the '
                                                                           'public parent; joins one '
                                                                           'lifecycle row to that event row.',
                                                      'patient_event_row_key': 'Deterministic row-level key '
                                                                               'carried unchanged from '
                                                                               'events_patient_event; joins '
                                                                               'one lifecycle row to one '
                                                                               'event-index row.',
                                                      'period_business_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
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
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'pm_wait_list_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'practitioner_group_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                      'practitioner_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'practitioner_identifier_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                      'practitioner_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'practitioner_location_evidence_key': 'Deterministic '
                                                                                            'v2 key carried '
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
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                      'source_daily_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
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
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
                                                      'source_system_oid': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'source_unit': 'Primary-key component carried '
                                                                     'unchanged from the public parent; '
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
                                                      'source_update_timestamp': 'Native source-system '
                                                                                 'update timestamp carried '
                                                                                 'through the canonical '
                                                                                 'silver query.',
                                                      'source_version_id': 'Native source version identifier '
                                                                           'carried unchanged from the '
                                                                           'public parent; CURRENT rows '
                                                                           'carry -1. A key component '
                                                                           'alongside row_source.',
                                                      'surg_case_proc_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                      'susceptibility_result_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                      'theatre_attendance_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                      'theatre_case_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                      'theatre_case_milestone_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                      'theatre_implant_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                      'transfusion_event_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                      'transfusion_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                      'treatment_cycle_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                                                   'one lifecycle row to '
                                                                                   'that research row.'},
                                          'table': 'Internal quality and batch metadata for '
                                                   'clinical_neonatal_care_day; same row grain as the '
                                                   'research table. Join keys: patient_event_key, entity_id, '
                                                   'activity_date.'},
 '_clinical_neonatal_care_day_stage': {'columns': {},
                                       'table': 'Internal neonatal care-day rows carrying JSON arrays for '
                                                'the public parent and code children.'},
 'clinical_neonatal_care_day': {'columns': {'activity_date': 'BadgerNet ActivityDate; second component of '
                                                             'the primary key.',
                                            'confidentiality_code': 'Source confidentiality classification '
                                                                    'attached to the record for each '
                                                                    'neonatal care day record. It is '
                                                                    'produced by the silver transformation '
                                                                    'and has no direct bronze-column lineage '
                                                                    'entry. Source code meanings and '
                                                                    'sentinel values are retained unless the '
                                                                    'pipeline explicitly maps them; null '
                                                                    'means no code was supplied.',
                                            'critical_care_discharge': 'Date or time value for critical care '
                                                                       'discharge on the neonatal care day '
                                                                       'record. It is derived from bronze '
                                                                       'field '
                                                                       '`CriticalCareDischargeDate_CLEAN` in '
                                                                       '`4_prod.bronze.map_neonatal_critical_care`. '
                                                                       'Source precision and timezone '
                                                                       'handling follow the pipeline '
                                                                       'expression; null means the time was '
                                                                       'unavailable.',
                                            'critical_care_start': 'Date or time value for critical care '
                                                                   'start on the neonatal care day record. '
                                                                   'It is derived from bronze field '
                                                                   '`CriticalCareStartDate_CLEAN` in '
                                                                   '`4_prod.bronze.map_neonatal_critical_care`. '
                                                                   'Source precision and timezone handling '
                                                                   'follow the pipeline expression; null '
                                                                   'means the time was unavailable.',
                                            'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                            'available.',
                                            'entity_id': 'BadgerNet EntityID; first component of the primary '
                                                         'key.',
                                            'episode_link_status': 'Processing or clinical status of episode '
                                                                   'link for the neonatal care day record. '
                                                                   'It is carried from bronze field '
                                                                   '`EPISODE_LINK_STATUS` in '
                                                                   '`4_prod.bronze.map_neonatal_critical_care`. '
                                                                   'Whitespace and source sentinel text are '
                                                                   'retained unless the pipeline explicitly '
                                                                   'normalizes them; null means no value was '
                                                                   'supplied.',
                                            'event_datetime': 'Date of the critical care activity record.',
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
                                                                  'clinical or administrative event ended '
                                                                  'for each neonatal care day record. It is '
                                                                  'produced by the silver transformation and '
                                                                  'has no direct bronze-column lineage '
                                                                  'entry. Source precision and timezone '
                                                                  'handling follow the pipeline expression; '
                                                                  'null means the time was unavailable.',
                                            'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                        'wall-clock time (GMT/BST), by the '
                                                                        'same rule as event_datetime_local; '
                                                                        'null where event_end_datetime is '
                                                                        'null.',
                                            'fact_category': 'Contract category used to group the record '
                                                             'with comparable clinical facts for each '
                                                             'neonatal care day record. It is produced by '
                                                             'the silver transformation and has no direct '
                                                             'bronze-column lineage entry. Whitespace and '
                                                             'source sentinel text are retained unless the '
                                                             'pipeline explicitly normalizes them; null '
                                                             'means no value was supplied.',
                                            'loaded_at': 'map_neonatal_critical_care.ADC_UPDT carried '
                                                         'unchanged through the care-day stage and public '
                                                         'projection. This is bronze ingestion provenance, '
                                                         'not ActivityDate, critical-care start/discharge or '
                                                         'Silver refresh time; a missing source timestamp '
                                                         'remains null.',
                                            'neonatal_episode_key': 'Deterministic SHA-256 key for the '
                                                                    'parent neonatal episode.',
                                            'patient_event_key': 'Deterministic event key derived from '
                                                                 'source identifiers; retained as a stable '
                                                                 'join key.',
                                            'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                         'resolved.',
                                            'record_status': 'Derived BadgerNet care-day row status: '
                                                             'retracted when '
                                                             'map_neonatal_critical_care.SOURCE_PRESENT_IND '
                                                             'is false, otherwise active; a missing flag '
                                                             'defaults true. The projection does not emit '
                                                             'superseded and this status does not describe '
                                                             "the baby's clinical condition or care level.",
                                            'record_status_effective_from': 'map_neonatal_critical_care.ActivityDate '
                                                                            'carried unchanged as the '
                                                                            'history start. This is the '
                                                                            'care-day date/time used as a '
                                                                            'proxy, not an independently '
                                                                            'observed status-change '
                                                                            'timestamp; no '
                                                                            'clinical-care-start or '
                                                                            'ingestion-time fallback is '
                                                                            'applied.',
                                            'record_status_effective_to': 'map_neonatal_critical_care.ADC_UPDT '
                                                                          'only when the care-day row is '
                                                                          'retracted; otherwise null. This '
                                                                          'is an ingestion-time end proxy, '
                                                                          'not ActivityDate or the '
                                                                          'critical-care discharge '
                                                                          'timestamp; a missing ADC_UPDT '
                                                                          'remains null.',
                                            'source_code': 'Neonatal Critical Care Activity Code 1 - coded '
                                                           'activity performed on this day.',
                                            'source_coding_system': 'Coding system or source namespace in '
                                                                    'which the source code is defined for '
                                                                    'each neonatal care day record. It is '
                                                                    'produced by the silver transformation '
                                                                    'and has no direct bronze-column lineage '
                                                                    'entry. Source code meanings and '
                                                                    'sentinel values are retained unless the '
                                                                    'pipeline explicitly maps them; null '
                                                                    'means no code was supplied.',
                                            'source_display': 'Neonatal Critical Care Activity Code 1 - '
                                                              'coded activity performed on this day.',
                                            'source_feed': 'Originating data feed responsible for the record '
                                                           'for each neonatal care day record. It is '
                                                           'produced by the silver transformation and has no '
                                                           'direct bronze-column lineage entry. Whitespace '
                                                           'and source sentinel text are retained unless the '
                                                           'pipeline explicitly normalizes them; null means '
                                                           'no value was supplied.',
                                            'source_update_timestamp': 'map_neonatal_critical_care.ADC_UPDT '
                                                                       'carried unchanged. It is the same '
                                                                       'contributing bronze load clock '
                                                                       'published as loaded_at, not an '
                                                                       'independent native clinical-system '
                                                                       'modification timestamp or Silver '
                                                                       'refresh time.',
                                            'subject_id_system': 'Identifier namespace used to interpret the '
                                                                 'subject identifier on this record for each '
                                                                 'neonatal care day record. It is derived '
                                                                 'from bronze field `BABY_PERSON_ID` in '
                                                                 '`4_prod.bronze.map_neonatal_critical_care`. '
                                                                 'Its uniqueness is limited to the '
                                                                 'originating source namespace; null means '
                                                                 'no identifier was supplied.',
                                            'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                           'identifier. Not salted, not secret; a stable '
                                                           'join key across feeds where person_id is '
                                                           'unresolved.',
                                            'unit_function': 'Functional type of the critical care unit '
                                                             '(e.g. 13 = NICU, 14 = LNU).',
                                            'vip_ind': 'Indicator that the source record carries VIP '
                                                       'handling status for each neonatal care day record. '
                                                       'It is produced by the silver transformation and has '
                                                       'no direct bronze-column lineage entry. Null means '
                                                       'the source did not state the indicator and must not '
                                                       'be interpreted as false.',
                                            'ward_location': 'Ward location code (e.g. nnu = neonatal unit, '
                                                             'tc = transitional care).',
                                            'withheld_identity_ind': 'Indicator that the source identity was '
                                                                     'deliberately withheld for each '
                                                                     'neonatal care day record. It is '
                                                                     'produced by the silver transformation '
                                                                     'and has no direct bronze-column '
                                                                     'lineage entry. Null means the source '
                                                                     'did not state the indicator and must '
                                                                     'not be interpreted as false.'},
                                'table': 'One neonatal critical-care activity day for an admitted BadgerNet '
                                         'episode, retaining activity and high-cost-drug evidence.'}}

# INSR_R4_SILVER_V1 (module_list): generated by r4/release/gen_silver_components.py; edit the generator, not this.
NEONATAL_CARE_DAY_PUBLIC_COLUMNS = list(NEONATAL_CARE_DAY_PUBLIC_COLUMNS) + ['identity_status']  # decision 1: contract columns, additions last
