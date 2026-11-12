"""Silver spine.value_set: 2 dataset(s).

- datasets: reference_value_set, _reference_value_set_metadata
- moved verbatim from: 1_silver_spine_and_reference:5368, 1_silver_spine_and_reference:5383, 1_silver_spine_and_reference:5387, 1_silver_spine_and_reference:5391, 1_silver_spine_and_reference:5397, 1_silver_spine_and_reference:5420, 1_silver_spine_and_reference:5433

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Reference / value set
SRC_VALUE_SET_RELEASE = "3_lookup.omop.value_set_release"


VALUE_SET_LIFECYCLE_FIELDS = [
    'load_batch_id',
]


VALUE_SET_RETIRED_COLUMNS = [

]


VALUE_SET_LIFECYCLE_COLUMNS = [
    'value_set_key',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_value_set():
    # Assemble value set rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    r = read_source(SRC_VALUE_SET_RELEASE)
    # contract v2: name the deterministic value-set membership identity value_set_key
    return r.select(
        stable_id(
            "value-set-member", r.canonical_url, r.value_set_version,
            r.member_system, r.member_code,
        ).alias("value_set_key"),
        r.canonical_url.alias("canonical_url"),
        r.value_set_version.alias("version"),
        r.member_system.alias("member_system"),
        r.member_code.alias("member_code"),
        r.member_display.alias("member_display"),
        r.membership_status.alias("status"),
        F.lit(SRC_VALUE_SET_RELEASE).alias("source_table"),
        F.concat_ws("|", r.canonical_url, r.member_system, r.member_code)
         .alias("source_row_id"),
        r.release_id.alias("load_batch_id"),
        r.updated_at.alias("loaded_at"),
    )


@materialized_view(
    name=_n("journey_reference.value_set"),
    refresh_policy="auto",
)
def value_set():
    # Build the declared dataset: Pinned value-set membership from the governed
    # value_set_release lookup asset; v3-ServiceDeliveryLocationRoleType is a recorded v1
    # exclusion.
    return _lifecycle_source_value_set().drop(*VALUE_SET_LIFECYCLE_FIELDS, *VALUE_SET_RETIRED_COLUMNS)


@materialized_view(
    name=_n('journey_reference._value_set_metadata'),
    refresh_policy="auto",
)
def value_set_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for reference_value_set;
    # same row grain as the research table. Join keys: value_set_key.
    return (_lifecycle_source_value_set()).select(*VALUE_SET_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_reference_value_set_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
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
                                   'table': 'Internal quality and batch metadata for reference_value_set; '
                                            'same row grain as the research table. Join keys: '
                                            'value_set_key.'},
 'reference_value_set': {'columns': {'canonical_url': 'Canonical value-set URL.',
                                     'loaded_at': '3_lookup.omop.value_set_release.updated_at carried '
                                                  'unchanged for the value-set membership row. This is the '
                                                  "lookup's update timestamp, not a bronze ADC_UPDT clock, "
                                                  'release identifier or Silver refresh time; a missing '
                                                  'value remains null.',
                                     'member_code': 'Source-system code representing member for the value '
                                                    'set record. It is produced by the silver transformation '
                                                    'and has no direct bronze-column lineage entry. Source '
                                                    'code meanings and sentinel values are retained unless '
                                                    'the pipeline explicitly maps them; null means no code '
                                                    'was supplied.',
                                     'member_display': 'Human-readable member label for the value set '
                                                       'record. It is produced by the silver transformation '
                                                       'and has no direct bronze-column lineage entry. '
                                                       'Whitespace and source sentinel text are retained '
                                                       'unless the pipeline explicitly normalizes them; null '
                                                       'means no value was supplied.',
                                     'member_system': 'Member coding system.',
                                     'source_row_id': 'Stable source membership identity.',
                                     'source_table': 'Governed bronze value-set source.',
                                     'status': 'Value-set membership status.',
                                     'value_set_key': 'Deterministic SHA-256 value-set membership key.',
                                     'version': 'Pinned value-set version.'},
                         'table': 'Pinned value-set membership from the governed value_set_release lookup '
                                  'asset; v3-ServiceDeliveryLocationRoleType is a recorded v1 exclusion.'}}
