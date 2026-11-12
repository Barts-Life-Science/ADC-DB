"""Silver activity.waiting_list_entry: 2 dataset(s).

- datasets: clinical_waiting_list_entry, _clinical_waiting_list_entry_metadata
- moved verbatim from: 4_silver_care_pathways:1957, 4_silver_care_pathways:1962, 4_silver_care_pathways:1966, 4_silver_care_pathways:1983, 4_silver_care_pathways:1990, 4_silver_care_pathways:2003, silver_value_policy.py:281

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n
from silver_tables._common.journey import _waiting_list_entry_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# W2.10 (J38 L3465): WL entry waiting start: source, else referral > decision to admit > requested (fill order P7_L3465b).
# Each pair is (published column, basis label); the tuple order is the precedence.
_WAIT = (("event_datetime", "source_waiting_start"), ("referral_datetime", "referral"),
         ("admit_decision_datetime", "decision_to_admit"), ("requested_datetime", "requested"))


WAITING_LIST_ENTRY_LIFECYCLE_FIELDS = [
    'identity_status',
    'load_batch_id',
]


WAITING_LIST_ENTRY_RETIRED_COLUMNS = [

]


WAITING_LIST_ENTRY_LIFECYCLE_COLUMNS = [
    # The companion must join one-to-one onto the research table. This table is
    # at physical version grain (CURRENT + HIST) and patient_event_key is
    # entry-grain, deliberately shared across an entry's versions, so it is not
    # a row key. pm_wait_list_id does not separate versions either: the pair
    # leaves 36,464,339 duplicate key groups, and joining on it fans gold out
    # to 5,531,116,187 rows. row_source with source_version_id is the physical
    # version grain -- 233,876,687 groups over 233,876,687 rows.
    'patient_event_key',
    'pm_wait_list_id',
    'row_source',
    'source_version_id',
    'identity_status',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_waiting_list_entry():
    # Assemble waiting list entry rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    return _waiting_list_entry_canonical().drop(
        "_source_system", "_source_table", "_source_row_id"
    )


@materialized_view(
    name=_n("journey_clinical.waiting_list_entry"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def waiting_list_entry():
    # Build the declared dataset: Waiting-list entries at physical version grain (CURRENT +
    # HIST). patient_event_id is entry-grain and shared across versions; VERSION_DT_TM is
    # descriptive fallback only, never an effective interval.
    return _lifecycle_source_waiting_list_entry().drop(*WAITING_LIST_ENTRY_LIFECYCLE_FIELDS, *WAITING_LIST_ENTRY_RETIRED_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._waiting_list_entry_metadata'),
    refresh_policy="auto",
)
def waiting_list_entry_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_waiting_list_entry; same row grain as the research table. Join keys:
    # patient_event_key, row_source, source_version_id.
    return (_lifecycle_source_waiting_list_entry()).select(*WAITING_LIST_ENTRY_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'clinical_waiting_list_entry': ValuePolicy(table='clinical_waiting_list_entry', enabled=True, zero_as_null=('sch_event_id', 'location_code')),
}


VALUE_POLICIES["clinical_waiting_list_entry"].register("waiting_start_derived", [c for c, _ in _WAIT],
         lambda F: F.coalesce(*[F.col(c) for c, _ in _WAIT]),
         "Waiting start: the source WAITING_START_DT_TM, else referral, else decision to admit, else requested "
         "(precedence by measured fill, P7_L3465b). The source column is unchanged. INSR_W2 W2.10 (J22, J38).")


VALUE_POLICIES["clinical_waiting_list_entry"].register("waiting_start_basis", [c for c, _ in _WAIT],
         lambda F: F.coalesce(*[F.when(F.col(c).isNotNull(), F.lit(b)) for c, b in _WAIT]),
         "Which column supplied waiting_start_derived; NULL iff it is NULL. INSR_W2 W2.10.")


WRAP_POLICY = {'clinical_waiting_list_entry'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_waiting_list_entry_metadata': {'columns': {'activity_date': 'Primary-key component carried '
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
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                       'birth_row_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                       'bloodtrack_transaction_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                       'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
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
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                       'chemotherapy_course_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                       'community_care_activity_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                       'community_care_contact_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                       'concept_map_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                       'cost_line_item_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                       'dcp_forms_activity_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
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
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
                                                       'doc_response_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                       'elective_access_procedure_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                       'encounter_attribute_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                                                'linkage without dropping '
                                                                                'the row.',
                                                       'encounter_identifier_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                       'encounter_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                       'endobase_exam_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                       'episode_encounter_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
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
                                                       'episode_id_resolved': 'True when episode_id is null '
                                                                              'or resolves to the governed '
                                                                              'episode spine; false records '
                                                                              'unresolved linkage without '
                                                                              'dropping the row.',
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
                                                       'event_before_birth': 'True when the event timestamp '
                                                                             "precedes the resolved person's "
                                                                             'birth timestamp; silver '
                                                                             'records the doubt and does not '
                                                                             'filter the row.',
                                                       'event_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                       'extract_cd': 'Primary-key component carried '
                                                                     'unchanged from the public parent; '
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
                                                       'fhx_activity_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                       'gene_tested_row_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                       'genetic_result_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                       'genetic_test_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                       'homecare_request_item_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                       'identity_status': 'Subject identity resolution state '
                                                                          'recorded by the silver canonical '
                                                                          'query.',
                                                       'implant_event_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                       'sequence; retained for the gold drop '
                                                                       'rule.',
                                                       'journey_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                       'journey_link_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                       'journey_participant_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                       'order_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                       'organization_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                       'organization_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                       'pacs_examination_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
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
                                                                            'lifecycle row to that event '
                                                                            'row.',
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
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                       'person_address_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                       'person_attribute_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                       'person_death_evidence_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                       'person_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                       'person_id_resolved': 'True when person_id is null or '
                                                                             'resolves to the governed '
                                                                             'person spine; false records '
                                                                             'unresolved linkage without '
                                                                             'dropping the row.',
                                                       'person_identifier_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                       'person_relationship_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                       'pharmacy_issue_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                       'practitioner_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                                       'the normalised '
                                                                                       'record status '
                                                                                       'applies, when the '
                                                                                       'source supplies one.',
                                                       'record_status_effective_to': 'Timestamp until which '
                                                                                     'the normalised record '
                                                                                     'status applies; null '
                                                                                     'means no source end is '
                                                                                     'known.',
                                                       'referral_oid': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                       'report_version_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                       'request_thread_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                       'research_study_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                       'row_source': 'Physical bronze row source, CURRENT or '
                                                                     'HIST, carried unchanged from the '
                                                                     'public parent; a key component because '
                                                                     'the research table is at physical '
                                                                     'version grain and patient_event_key is '
                                                                     "shared across an entry's versions.",
                                                       'rtt_activity_oid': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                       'sch_event_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                       'score_type': 'Primary-key component carried '
                                                                     'unchanged from the public parent; '
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
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
                                                       'source_system_oid': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                       'source_unit': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                       'source_update_timestamp': 'Native source-system '
                                                                                  'update timestamp carried '
                                                                                  'through the canonical '
                                                                                  'silver query.',
                                                       'source_version_id': 'Native source version '
                                                                            'identifier carried unchanged '
                                                                            'from the public parent; CURRENT '
                                                                            'rows carry -1. A key component '
                                                                            'alongside row_source.',
                                                       'surg_case_proc_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                       'susceptibility_result_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                       'theatre_attendance_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                       'theatre_case_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                       'theatre_case_milestone_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                       'theatre_implant_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                       'transfusion_event_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
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
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                       'waiting_list_snapshot_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.'},
                                           'table': 'Internal quality and batch metadata for '
                                                    'clinical_waiting_list_entry; same row grain as the '
                                                    'research table. Join keys: patient_event_key, '
                                                    'row_source, source_version_id.'},
 'clinical_waiting_list_entry': {'columns': {'active_status_display': 'Source active-status display.',
                                             'adjusted_waiting_start_datetime': 'Adjusted waiting-start '
                                                                                'timestamp.',
                                             'admit_booking_display': 'Source admission-booking display.',
                                             'admit_category_display': 'Source admission-category display.',
                                             'admit_decision_datetime': 'Source decision-to-admit timestamp.',
                                             'admit_guaranteed_datetime': 'Source guaranteed-admission '
                                                                          'timestamp.',
                                             'admit_offer_outcome_display': 'Source admission-offer outcome '
                                                                            'display.',
                                             'admit_type_display': 'Source admission-type display.',
                                             'anesthetic_display': 'Source anesthetic display.',
                                             'attendance_display': 'Source attendance display.',
                                             'confidentiality_code': 'Source confidentiality code when '
                                                                     'supplied.',
                                             'encounter_id': 'Millennium ENCNTR_ID when supplied; native '
                                                             'encounter foreign key.',
                                             'event_datetime': 'Recorded waiting-period start timestamp '
                                                               'carried from `WAITING_START_DT_TM`.',
                                             'event_datetime_local': 'event_datetime in Europe/London '
                                                                     'wall-clock time (GMT/BST). Millennium '
                                                                     'rows take the bronze *_LOCAL '
                                                                     'companions of the same source columns '
                                                                     'and fallback order; rows from '
                                                                     'local-clock sources (LUNA, PACS/DICOM, '
                                                                     'TFC LIMS, CCMDS, MSDS, BadgerNet, '
                                                                     'EndoBase, SLAM, iWeb, Datix and '
                                                                     'similar) carry event_datetime '
                                                                     'unchanged. event_datetime is a UTC '
                                                                     'instant for Millennium rows, so take '
                                                                     'local calendar dates and clock times '
                                                                     'from this column.',
                                             'event_end_datetime': 'Recorded waiting-period end timestamp '
                                                                   'carried from `WAITING_END_DT_TM`.',
                                             'event_end_datetime_local': 'event_end_datetime in '
                                                                         'Europe/London wall-clock time '
                                                                         '(GMT/BST), by the same rule as '
                                                                         'event_datetime_local; null where '
                                                                         'event_end_datetime is null.',
                                             'facility_display': 'Source facility display.',
                                             'fact_category': 'Whether this fact is clinical or '
                                                              'administrative in the v2 plane merge.',
                                             'from_ed_ind': 'Raw source indicator for origin in the '
                                                            'emergency department.',
                                             'hist_action': 'Source history action when the row is '
                                                            'historical.',
                                             'is_current': 'Whether this is the current physical version.',
                                             'last_dna_datetime': 'Source last did-not-attend timestamp.',
                                             'loaded_at': 'map_waiting_list.ADC_UPDT carried unchanged for '
                                                          'the physical CURRENT or HIST version. This is '
                                                          'ingestion provenance, not waiting-start/end, '
                                                          'descriptive VERSION_DT_TM or Silver refresh time; '
                                                          'a missing source timestamp remains null.',
                                             'location_code': 'Millennium nurse-unit code.',
                                             'location_display': 'Source nurse-unit display.',
                                             'management_display': 'Source management display.',
                                             'original_request_received_datetime': 'Original '
                                                                                   'request-received '
                                                                                   'timestamp.',
                                             'patient_event_key': 'Deterministic SHA-256 waiting-list entry '
                                                                  'identity shared by physical versions.',
                                             'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                             'planned_procedure_code': 'Source planned-procedure code.',
                                             'planned_procedure_display': 'Source planned-procedure display.',
                                             'pm_wait_list_id': 'Native waiting-list entry identifier.',
                                             'previous_provisional_admit_datetime': 'Previous '
                                                                                    'provisional-admission '
                                                                                    'timestamp.',
                                             'provisional_admit_datetime': 'Source provisional-admission '
                                                                           'timestamp.',
                                             'reason_for_change_display': 'Source reason-for-change display.',
                                             'reason_for_removal_display': 'Source reason-for-removal '
                                                                           'display.',
                                             'recommend_datetime': 'Source recommendation timestamp.',
                                             'record_status': 'Derived map_waiting_list version status: '
                                                              'superseded when SOURCE_PRESENT_IND cast to '
                                                              'Boolean is false or IS_CURRENT is false; '
                                                              'otherwise active. Null presence defaults to '
                                                              'true and null IS_CURRENT defaults to false. '
                                                              'Source absence therefore maps to superseded, '
                                                              'not retracted; this is distinct from the '
                                                              'clinical waiting-list status code.',
                                             'record_status_effective_from': 'map_waiting_list.BEG_EFFECTIVE_DT_TM '
                                                                             'carried unchanged for the '
                                                                             'physical CURRENT or HIST '
                                                                             'version. VERSION_DT_TM and '
                                                                             'waiting/request dates are not '
                                                                             'used as fallbacks; a missing '
                                                                             'value remains null.',
                                             'record_status_effective_to': 'For a superseded waiting-list '
                                                                           'version, first non-null '
                                                                           'map_waiting_list.SOURCE_ABSENT_DETECTED_TS, '
                                                                           'END_EFFECTIVE_DT_TM, then '
                                                                           'ADC_UPDT. Null for active '
                                                                           'versions or when all three are '
                                                                           'missing. This expression does '
                                                                           'not filter far-future '
                                                                           'END_EFFECTIVE_DT_TM sentinels '
                                                                           'and can use ingestion time as '
                                                                           'its last fallback.',
                                             'referral_datetime': 'Source referral timestamp.',
                                             'referral_source_display': 'Source referral-source display.',
                                             'referral_type_display': 'Source referral-type display.',
                                             'removal_datetime': 'Source removal timestamp.',
                                             'requested_datetime': 'Source requested timestamp.',
                                             'row_source': 'Physical bronze row source.',
                                             'sch_event_id': 'Raw scheduling event linkage evidence.',
                                             'scheduled_datetime': 'Source scheduled timestamp.',
                                             'service_type_requested_display': 'Source '
                                                                               'requested-service-type '
                                                                               'display.',
                                             'source_code': 'Verbatim planned-procedure code.',
                                             'source_coding_system': 'Verbatim Millennium planned-procedure '
                                                                     'coding system.',
                                             'source_display': 'Verbatim planned-procedure display.',
                                             'source_feed': 'Registered feed owning the fact.',
                                             'source_update_timestamp': 'map_waiting_list.SOURCE_ADC_UPDT '
                                                                        'carried unchanged for the physical '
                                                                        'CURRENT or HIST version. It is '
                                                                        'separate from ADC_UPDT ingestion '
                                                                        'provenance and is not '
                                                                        'VERSION_DT_TM, the clinical status '
                                                                        'date or Silver refresh time; a '
                                                                        'missing timestamp remains null.',
                                             'source_version_id': 'Native source version identifier; CURRENT '
                                                                  'rows carry -1.',
                                             'stand_by_display': 'Source stand-by display.',
                                             'status_code': 'Source waiting-list status code.',
                                             'status_datetime': 'Source status timestamp.',
                                             'status_display': 'Source waiting-list status display.',
                                             'status_end_datetime': 'Source status-end timestamp.',
                                             'sub_status_display': 'Source waiting-list sub-status display.',
                                             'subject_id_system': 'Identifier system used for subject_key.',
                                             'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                            'identifier. Not salted, not secret; a stable '
                                                            'join key across feeds where person_id is '
                                                            'unresolved.',
                                             'suspended_days': 'Source count of suspended days.',
                                             'updt_cnt': 'Native source update counter.',
                                             'urgency_display': 'Source urgency display.',
                                             'version_datetime': 'Descriptive fallback version timestamp '
                                                                 'only; it defines no effective interval.',
                                             'vip_ind': 'Source VIP indicator when supplied.',
                                             'waiting_start_basis': 'Which column supplied '
                                                                    'waiting_start_derived; NULL iff it is '
                                                                    'NULL. INSR_W2 W2.10.',
                                             'waiting_start_derived': 'Waiting start: the source '
                                                                      'WAITING_START_DT_TM, else referral, '
                                                                      'else decision to admit, else '
                                                                      'requested (precedence by measured '
                                                                      'fill, P7_L3465b). The source column '
                                                                      'is unchanged. INSR_W2 W2.10 (J22, '
                                                                      'J38).',
                                             'withheld_identity_ind': 'Withheld-identity indicator when '
                                                                      'supplied.'},
                                 'table': 'Waiting-list entries at physical version grain (CURRENT + HIST). '
                                          'patient_event_id is entry-grain and shared across versions; '
                                          'VERSION_DT_TM is descriptive fallback only, never an effective '
                                          'interval.'}}

# INSR_R4_SILVER_V1 (module_drop): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import with_columns_last  # noqa: E402
WAITING_LIST_ENTRY_LIFECYCLE_FIELDS = [c for c in WAITING_LIST_ENTRY_LIFECYCLE_FIELDS if c not in ['identity_status']]  # decision 1: published
SPECS = with_columns_last(SPECS, 'clinical_waiting_list_entry', ('identity_status',))  # additions last
