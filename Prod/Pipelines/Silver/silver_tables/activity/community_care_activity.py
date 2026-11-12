"""Silver activity.community_care_activity: 2 dataset(s).

- datasets: clinical_community_care_activity, _clinical_community_care_activity_metadata
- moved verbatim from: 4_silver_care_pathways:2399, 4_silver_care_pathways:2463, 4_silver_care_pathways:2524, 4_silver_care_pathways:2593, 4_silver_care_pathways:2598, 4_silver_care_pathways:2610

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n
from silver_tables._common.journey import _community_care_activity_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / community care activity

# contract v2: retain the full canonical shape for internal reuse.
COMMUNITY_ACTIVITY_SOURCE_COLUMNS = [
    "patient_event_key",
    "community_care_activity_key",
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
    "care_activity_date",
    "care_activity_date_quality_status",
    "community_contact_key",
    "contact_match_status",
    "contact_candidate_count",
    "same_date_contact_candidate_count",
    "community_database_id",
    "care_activity_id",
    "community_patient_key",
    "service_id",
    "service_name",
    "care_professional_local_id",
    "duration_minutes",
    "duration_quality_status",
    "source_clinical_term",
    "source_clinical_term_key",
    "observation_type_id",
    "code_category_id",
    "observation_value_raw",
    "observation_value_numeric",
    "unit_source_value",
    "normalized_ucum_code",
    "unit_concept_id",
    "unit_concept_name",
    "unit_mapping_status",
    "unit_mapping_method",
    "snomed_candidate_count",
    "snomed_candidate_concept_id",
    "snomed_candidate_code",
    "snomed_candidate_name",
    "snomed_candidate_domain",
    "snomed_candidate_class",
    "snomed_candidate_method",
    "snomed_candidate_status",
    "person_match_status",
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
COMMUNITY_ACTIVITY_PUBLIC_COLUMNS = [
    'patient_event_key',
    'community_care_activity_key',
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
    'care_activity_date',
    'care_activity_date_quality_status',
    'community_contact_key',
    'contact_match_status',
    'contact_candidate_count',
    'same_date_contact_candidate_count',
    'community_database_id',
    'care_activity_id',
    'community_patient_key',
    'service_id',
    'service_name',
    'care_professional_local_id',
    'duration_minutes',
    'duration_quality_status',
    'source_clinical_term',
    'source_clinical_term_key',
    'observation_type_id',
    'code_category_id',
    'observation_value_raw',
    'observation_value_numeric',
    'unit_source_value',
    'normalized_ucum_code',
    'unit_concept_id',
    'unit_concept_name',
    'unit_mapping_status',
    'unit_mapping_method',
    'snomed_candidate_count',
    'snomed_candidate_concept_id',
    'snomed_candidate_code',
    'snomed_candidate_name',
    'snomed_candidate_domain',
    'snomed_candidate_class',
    'snomed_candidate_method',
    'snomed_candidate_status',
    'person_match_status',
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


COMMUNITY_ACTIVITY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'community_care_activity_key',
    'identity_status',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_community_care_activity():
    # Assemble community care activity rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _community_care_activity_canonical().select(*COMMUNITY_ACTIVITY_SOURCE_COLUMNS)


@materialized_view(
    name=_n("journey_clinical.community_care_activity"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def community_care_activity():
    # Build the declared dataset: One admitted CSDS community-care activity with candidate
    # terminology provenance.
    return _lifecycle_source_community_care_activity().select(*COMMUNITY_ACTIVITY_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._community_care_activity_metadata'),
    refresh_policy="auto",
)
def community_care_activity_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_community_care_activity; same row grain as the research table. Join keys:
    # patient_event_key, community_care_activity_key.
    return (_lifecycle_source_community_care_activity()).select(*COMMUNITY_ACTIVITY_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'clinical_community_care_activity': ValuePolicy(table='clinical_community_care_activity', enabled=True),
}


# W2.06 (L2306 sibling): CSDS community activity dated 1 January before July 2007 is a year-only default.
VALUE_POLICIES["clinical_community_care_activity"].register("january_default_suspected", ["event_datetime"],
         lambda F: F.coalesce((F.month("event_datetime") == 1) & (F.dayofmonth("event_datetime") == 1)
                              & (F.col("event_datetime") < F.lit("2007-07-01").cast("timestamp")), F.lit(False)),
         "True for activity dated 1 January before 2007-07, where the date heaps at more than 5x the other January "
         "days: the source recorded the year only. INSR_W2.")


WRAP_POLICY = {'clinical_community_care_activity'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_community_care_activity_metadata': {'columns': {'activity_date': 'Primary-key component carried '
                                                                             'unchanged from the public '
                                                                             'parent; combine it with the '
                                                                             "companion's other key columns "
                                                                             'when joining.',
                                                            'activity_record_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                            'admission_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                            'allergy_instance_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                            'aria_dx_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                            'aria_pt_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                            'artifact_link_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                            'birth_row_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                            'bloodtrack_transaction_key': 'Deterministic v2 '
                                                                                          'key carried '
                                                                                          'unchanged from '
                                                                                          'the public '
                                                                                          'parent; joins one '
                                                                                          'lifecycle row to '
                                                                                          'that research '
                                                                                          'row.',
                                                            'cancer_treatment_cycle_key': 'Deterministic v2 '
                                                                                          'key carried '
                                                                                          'unchanged from '
                                                                                          'the public '
                                                                                          'parent; joins one '
                                                                                          'lifecycle row to '
                                                                                          'that research '
                                                                                          'row.',
                                                            'care_contact_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                            'care_participation_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                            'cds_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                            'chemotherapy_course_id': 'Native source '
                                                                                      'identifier carried '
                                                                                      'unchanged from the '
                                                                                      'public parent; joins '
                                                                                      'one lifecycle row to '
                                                                                      'that research row.',
                                                            'community_care_activity_key': 'Deterministic v2 '
                                                                                           'key carried '
                                                                                           'unchanged from '
                                                                                           'the public '
                                                                                           'parent; joins '
                                                                                           'one lifecycle '
                                                                                           'row to that '
                                                                                           'research row.',
                                                            'community_care_contact_key': 'Deterministic v2 '
                                                                                          'key carried '
                                                                                          'unchanged from '
                                                                                          'the public '
                                                                                          'parent; joins one '
                                                                                          'lifecycle row to '
                                                                                          'that research '
                                                                                          'row.',
                                                            'concept_map_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                            'cost_line_item_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                            'dcp_forms_activity_id': 'Native source '
                                                                                     'identifier carried '
                                                                                     'unchanged from the '
                                                                                     'public parent; joins '
                                                                                     'one lifecycle row to '
                                                                                     'that research row.',
                                                            'device_mapping_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                            'diagnosis_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                            'dicom_path': 'Primary-key component carried '
                                                                          'unchanged from the public parent; '
                                                                          "combine it with the companion's "
                                                                          'other key columns when joining.',
                                                            'doc_response_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                            'elective_access_procedure_key': 'Deterministic '
                                                                                             'v2 key carried '
                                                                                             'unchanged from '
                                                                                             'the public '
                                                                                             'parent; joins '
                                                                                             'one lifecycle '
                                                                                             'row to that '
                                                                                             'research row.',
                                                            'encounter_attribute_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                            'encounter_bounds_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                            'encounter_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                            'encounter_id_resolved': 'True when encounter_id '
                                                                                     'is null or resolves to '
                                                                                     'the governed encounter '
                                                                                     'spine; false records '
                                                                                     'unresolved linkage '
                                                                                     'without dropping the '
                                                                                     'row.',
                                                            'encounter_identifier_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                            'encounter_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                            'endobase_exam_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                            'endobase_exam_term_id': 'Native source '
                                                                                     'identifier carried '
                                                                                     'unchanged from the '
                                                                                     'public parent; joins '
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
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                            'episode_encounter_reltn_id': 'Native source '
                                                                                          'identifier '
                                                                                          'carried unchanged '
                                                                                          'from the public '
                                                                                          'parent; joins one '
                                                                                          'lifecycle row to '
                                                                                          'that research '
                                                                                          'row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                            'event_after_death_30d': 'True when the event '
                                                                                     'timestamp is more than '
                                                                                     '30 days after the '
                                                                                     "resolved person's "
                                                                                     'death timestamp; '
                                                                                     'silver records the '
                                                                                     'doubt and does not '
                                                                                     'filter the row.',
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
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                            'gene_tested_row_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                            'genetic_result_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                            'genetic_test_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                            'homecare_request_item_id': 'Native source '
                                                                                        'identifier carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                            'identity_status': 'Subject identity resolution '
                                                                               'state recorded by the silver '
                                                                               'canonical query.',
                                                            'implant_event_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                            'implant_sequence': 'Primary-key component '
                                                                                'carried unchanged from the '
                                                                                'public parent; combine it '
                                                                                "with the companion's other "
                                                                                'key columns when joining.',
                                                            'indication_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                            'is_illogical': 'Source RTT chronology flag '
                                                                            'indicating an illogical '
                                                                            'activity sequence; retained for '
                                                                            'the gold drop rule.',
                                                            'journey_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                            'journey_link_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                            'journey_participant_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                            'labour_delivery_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                            'location_stay_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                            'mc_device_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                            'organization_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                            'pacs_examination_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                            'pacs_report_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                            'pathway_oid': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                            'patient_event_key': 'Deterministic SHA-256 '
                                                                                 'event key carried '
                                                                                 'unchanged from the public '
                                                                                 'parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'event row.',
                                                            'patient_event_row_key': 'Deterministic '
                                                                                     'row-level key carried '
                                                                                     'unchanged from '
                                                                                     'events_patient_event; '
                                                                                     'joins one lifecycle '
                                                                                     'row to one event-index '
                                                                                     'row.',
                                                            'period_business_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                            'period_oid': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                            'person_address_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                            'person_attribute_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                            'person_death_evidence_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                            'person_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                            'person_id_resolved': 'True when person_id is '
                                                                                  'null or resolves to the '
                                                                                  'governed person spine; '
                                                                                  'false records unresolved '
                                                                                  'linkage without dropping '
                                                                                  'the row.',
                                                            'person_identifier_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                            'person_relationship_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                            'pharmacy_issue_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                            'pm_wait_list_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                            'practitioner_group_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                            'practitioner_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                            'practitioner_identifier_key': 'Deterministic v2 '
                                                                                           'key carried '
                                                                                           'unchanged from '
                                                                                           'the public '
                                                                                           'parent; joins '
                                                                                           'one lifecycle '
                                                                                           'row to that '
                                                                                           'research row.',
                                                            'practitioner_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                            'practitioner_location_evidence_key': 'Deterministic '
                                                                                                  'v2 key '
                                                                                                  'carried '
                                                                                                  'unchanged '
                                                                                                  'from the '
                                                                                                  'public '
                                                                                                  'parent; '
                                                                                                  'joins one '
                                                                                                  'lifecycle '
                                                                                                  'row to '
                                                                                                  'that '
                                                                                                  'research '
                                                                                                  'row.',
                                                            'pregnancy_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                            'pregnancy_reconciliation_key': 'Deterministic '
                                                                                            'v2 key carried '
                                                                                            'unchanged from '
                                                                                            'the public '
                                                                                            'parent; joins '
                                                                                            'one lifecycle '
                                                                                            'row to that '
                                                                                            'research row.',
                                                            'problem_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                            'procedure_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                            'pt_prot_reg_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                            'ptl_unique_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                            'record_status': 'active, superseded or '
                                                                             'retracted, normalised from the '
                                                                             "source's own status fields; "
                                                                             'superseded rows are earlier '
                                                                             'versions the source kept.',
                                                            'record_status_effective_from': 'Timestamp from '
                                                                                            'which the '
                                                                                            'normalised '
                                                                                            'record status '
                                                                                            'applies, when '
                                                                                            'the source '
                                                                                            'supplies one.',
                                                            'record_status_effective_to': 'Timestamp until '
                                                                                          'which the '
                                                                                          'normalised record '
                                                                                          'status applies; '
                                                                                          'null means no '
                                                                                          'source end is '
                                                                                          'known.',
                                                            'referral_oid': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                            'report_version_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                            'request_thread_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                            'research_study_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                            'row_source': 'Physical bronze row source, '
                                                                          'CURRENT or HIST, carried '
                                                                          'unchanged from the public parent; '
                                                                          'a key component because the '
                                                                          'research table is at physical '
                                                                          'version grain and '
                                                                          'patient_event_key is shared '
                                                                          "across an entry's versions.",
                                                            'rtt_activity_oid': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                            'sch_event_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                            'score_type': 'Primary-key component carried '
                                                                          'unchanged from the public parent; '
                                                                          "combine it with the companion's "
                                                                          'other key columns when joining.',
                                                            'sequence_nbr': 'Primary-key component carried '
                                                                            'unchanged from the public '
                                                                            'parent; combine it with the '
                                                                            "companion's other key columns "
                                                                            'when joining.',
                                                            'service_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                            'service_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                            'snapshot_date': 'Primary-key component carried '
                                                                             'unchanged from the public '
                                                                             'parent; combine it with the '
                                                                             "companion's other key columns "
                                                                             'when joining.',
                                                            'source_daily_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                            'source_identifier_id': 'Native source '
                                                                                    'identifier carried '
                                                                                    'unchanged from the '
                                                                                    'public parent; joins '
                                                                                    'one lifecycle row to '
                                                                                    'that research row.',
                                                            'source_object': 'Source-arm discriminator '
                                                                             'carried unchanged from the '
                                                                             'public parent; use it with '
                                                                             'arm-specific native '
                                                                             'identifiers when joining.',
                                                            'source_site': 'Primary-key component carried '
                                                                           'unchanged from the public '
                                                                           'parent; combine it with the '
                                                                           "companion's other key columns "
                                                                           'when joining.',
                                                            'source_system_oid': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                            'source_unit': 'Primary-key component carried '
                                                                           'unchanged from the public '
                                                                           'parent; combine it with the '
                                                                           "companion's other key columns "
                                                                           'when joining.',
                                                            'source_update_timestamp': 'Native source-system '
                                                                                       'update timestamp '
                                                                                       'carried through the '
                                                                                       'canonical silver '
                                                                                       'query.',
                                                            'source_version_id': 'Native source version '
                                                                                 'identifier carried '
                                                                                 'unchanged from the public '
                                                                                 'parent; CURRENT rows carry '
                                                                                 '-1. A key component '
                                                                                 'alongside row_source.',
                                                            'surg_case_proc_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                            'susceptibility_result_id': 'Native source '
                                                                                        'identifier carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                            'theatre_attendance_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                            'theatre_case_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                            'theatre_case_milestone_key': 'Deterministic v2 '
                                                                                          'key carried '
                                                                                          'unchanged from '
                                                                                          'the public '
                                                                                          'parent; joins one '
                                                                                          'lifecycle row to '
                                                                                          'that research '
                                                                                          'row.',
                                                            'theatre_implant_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                            'transfusion_event_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                            'transfusion_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                            'treatment_cycle_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                            'treatment_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                            'update_count': 'Primary-key component carried '
                                                                            'unchanged from the public '
                                                                            'parent; combine it with the '
                                                                            "companion's other key columns "
                                                                            'when joining.',
                                                            'valid_from_datetime': 'Primary-key component '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; '
                                                                                   'combine it with the '
                                                                                   "companion's other key "
                                                                                   'columns when joining.',
                                                            'value_set_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                            'waiting_list_oid': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                            'waiting_list_snapshot_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research '
                                                                                         'row.'},
                                                'table': 'Internal quality and batch metadata for '
                                                         'clinical_community_care_activity; same row grain '
                                                         'as the research table. Join keys: '
                                                         'patient_event_key, community_care_activity_key.'},
 'clinical_community_care_activity': {'columns': {'care_activity_date': 'Date on which the care activity was '
                                                                        'recorded as taking place.',
                                                  'care_activity_date_quality_status': 'Derived data-quality '
                                                                                       'flag describing the '
                                                                                       'state of the source '
                                                                                       'care activity date, '
                                                                                       'such as whether it '
                                                                                       'is absent or falls '
                                                                                       'within an acceptable '
                                                                                       'range.',
                                                  'care_activity_id': 'Source CSDS care activity identifier '
                                                                      '(CAI) for the activity row, unique '
                                                                      'only within its source database.',
                                                  'care_professional_local_id': 'CSDS Care Professional '
                                                                                'Local Identifier; not a '
                                                                                'Millennium personnel ID.',
                                                  'code_category_id': 'Numeric code category identifier '
                                                                      'carried unchanged from the source '
                                                                      'CSDS CYP202 care activity '
                                                                      'CodeCategoryId field.',
                                                  'community_care_activity_key': 'Source-preserved community '
                                                                                 'care activity key; primary '
                                                                                 'key of this table.',
                                                  'community_contact_key': 'Deterministic SHA-256 key for '
                                                                           'the parent community care '
                                                                           'contact.',
                                                  'community_database_id': 'Source community system database '
                                                                           'identifier (CDB) from the CSDS '
                                                                           'CYP202 care activity extract, '
                                                                           'used with care_activity_id to '
                                                                           'scope source records.',
                                                  'community_patient_key': 'Content-addressed key '
                                                                           'identifying the community '
                                                                           'patient, derived from the source '
                                                                           'patient number and registering '
                                                                           'source database identifier in '
                                                                           'the CSDS patient identifier '
                                                                           'lookup.',
                                                  'confidentiality_code': 'Source confidentiality '
                                                                          'classification attached to the '
                                                                          'record for each community care '
                                                                          'activity record. It is produced '
                                                                          'by the silver transformation and '
                                                                          'has no direct bronze-column '
                                                                          'lineage entry. Source code '
                                                                          'meanings and sentinel values are '
                                                                          'retained unless the pipeline '
                                                                          'explicitly maps them; null means '
                                                                          'no code was supplied.',
                                                  'contact_candidate_count': 'Number of candidate care '
                                                                             'contact rows matched to this '
                                                                             "activity's care contact "
                                                                             'identifier during contact '
                                                                             'linkage.',
                                                  'contact_match_status': 'UNIQUE_ID_DATE_MATCH, '
                                                                          'UNIQUE_ID_DATE_MISMATCH, '
                                                                          'DUPLICATE_ID_RESOLVED_BY_DATE, '
                                                                          'AMBIGUOUS_SAME_DATE, '
                                                                          'DUPLICATE_ID_NO_DATE_MATCH or '
                                                                          'NO_CONTACT.',
                                                  'duration_minutes': 'Recorded duration of the clinical '
                                                                      'contact for this activity, in '
                                                                      'minutes.',
                                                  'duration_quality_status': 'Derived data-quality flag '
                                                                             'describing the state of the '
                                                                             'recorded clinical contact '
                                                                             'duration, such as whether it '
                                                                             'is absent or within an '
                                                                             'acceptable range.',
                                                  'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT '
                                                                  'when available.',
                                                  'event_datetime': 'Date on which the care activity was '
                                                                    'recorded as taking place.',
                                                  'event_datetime_local': 'event_datetime in Europe/London '
                                                                          'wall-clock time (GMT/BST). '
                                                                          'Millennium rows take the bronze '
                                                                          '*_LOCAL companions of the same '
                                                                          'source columns and fallback '
                                                                          'order; rows from local-clock '
                                                                          'sources (LUNA, PACS/DICOM, TFC '
                                                                          'LIMS, CCMDS, MSDS, BadgerNet, '
                                                                          'EndoBase, SLAM, iWeb, Datix and '
                                                                          'similar) carry event_datetime '
                                                                          'unchanged. event_datetime is a '
                                                                          'UTC instant for Millennium rows, '
                                                                          'so take local calendar dates and '
                                                                          'clock times from this column.',
                                                  'event_end_datetime': 'Date and time when the represented '
                                                                        'clinical or administrative event '
                                                                        'ended for each community care '
                                                                        'activity record. It is produced by '
                                                                        'the silver transformation and has '
                                                                        'no direct bronze-column lineage '
                                                                        'entry. Source precision and '
                                                                        'timezone handling follow the '
                                                                        'pipeline expression; null means the '
                                                                        'time was unavailable.',
                                                  'event_end_datetime_local': 'event_end_datetime in '
                                                                              'Europe/London wall-clock time '
                                                                              '(GMT/BST), by the same rule '
                                                                              'as event_datetime_local; null '
                                                                              'where event_end_datetime is '
                                                                              'null.',
                                                  'fact_category': 'Contract category used to group the '
                                                                   'record with comparable clinical facts '
                                                                   'for each community care activity record. '
                                                                   'It is produced by the silver '
                                                                   'transformation and has no direct '
                                                                   'bronze-column lineage entry. Whitespace '
                                                                   'and source sentinel text are retained '
                                                                   'unless the pipeline explicitly '
                                                                   'normalizes them; null means no value was '
                                                                   'supplied.',
                                                  'january_default_suspected': 'True for activity dated 1 '
                                                                               'January before 2007-07, '
                                                                               'where the date heaps at more '
                                                                               'than 5x the other January '
                                                                               'days: the source recorded '
                                                                               'the year only. INSR_W2.',
                                                  'loaded_at': 'Always null as a TIMESTAMP in the current '
                                                               'community-care activity projection. No '
                                                               'contributing load clock is supplied or taken '
                                                               'from the parent contact; neither '
                                                               'care_activity_date nor Silver refresh time '
                                                               'is substituted.',
                                                  'normalized_ucum_code': 'Exact or approved-alias '
                                                                          'normalized UCUM code.',
                                                  'observation_type_id': 'Numeric observation type '
                                                                         'identifier carried unchanged from '
                                                                         'the source CSDS CYP202 care '
                                                                         'activity ObservationType field.',
                                                  'observation_value_numeric': 'Numeric interpretation of '
                                                                               'the source observation value '
                                                                               'where the recorded ObsValue '
                                                                               'could be parsed as a number.',
                                                  'observation_value_raw': 'Observation value as recorded in '
                                                                           'the source CSDS CYP202 care '
                                                                           'activity ObsValue field, '
                                                                           'retained unaltered before '
                                                                           'numeric parsing.',
                                                  'patient_event_key': 'Deterministic event key derived from '
                                                                       'source identifiers; retained as a '
                                                                       'stable join key.',
                                                  'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                               'resolved.',
                                                  'person_match_status': 'Outcome status of the linkage from '
                                                                         'the community patient identifier '
                                                                         'to a Millennium person record.',
                                                  'record_status': 'Retracted when '
                                                                   'map_community_care_activity.SOURCE_PRESENT_IND '
                                                                   'is false, otherwise active; missing '
                                                                   'presence defaults true. No superseded '
                                                                   'status is emitted. This is source-row '
                                                                   'presence, not the clinical finding, '
                                                                   'contact-match status or code-eligibility '
                                                                   'decision.',
                                                  'record_status_effective_from': 'Date on which the care '
                                                                                  'activity was recorded as '
                                                                                  'taking place.',
                                                  'record_status_effective_to': 'map_community_care_activity.SOURCE_ABSENT_DETECTED_TS '
                                                                                'only when '
                                                                                'SOURCE_PRESENT_IND is '
                                                                                'false; otherwise null. A '
                                                                                'missing absence timestamp '
                                                                                'stays null, with no '
                                                                                'ingestion-time fallback. '
                                                                                'This is the row-absence end '
                                                                                'proxy, not a clinical '
                                                                                'activity or contact end.',
                                                  'same_date_contact_candidate_count': 'Number of candidate '
                                                                                       'care contact rows '
                                                                                       'whose contact date '
                                                                                       'equals the care '
                                                                                       'activity date, used '
                                                                                       'to resolve duplicate '
                                                                                       'contact identifiers.',
                                                  'service_id': 'Identifier of the registering community '
                                                                'service or organisation from the CSDS '
                                                                'patient identifier lookup.',
                                                  'service_name': 'Name of the registering community service '
                                                                  'or organisation associated with the '
                                                                  'activity.',
                                                  'snomed_candidate_class': 'OMOP concept class of the '
                                                                            'SNOMED CT candidate concept.',
                                                  'snomed_candidate_code': 'SNOMED CT code of the candidate '
                                                                           'concept matched to the source '
                                                                           'clinical term.',
                                                  'snomed_candidate_concept_id': 'Unreviewed deterministic '
                                                                                 'exact-name/synonym '
                                                                                 'candidate; not an approved '
                                                                                 'resolved mapping.',
                                                  'snomed_candidate_count': 'Number of distinct SNOMED '
                                                                            'candidate concepts matched for '
                                                                            'the source clinical term on '
                                                                            'this activity row.',
                                                  'snomed_candidate_domain': 'OMOP domain of the SNOMED CT '
                                                                             'candidate concept.',
                                                  'snomed_candidate_method': 'Indicator of the deterministic '
                                                                             'matching technique used to '
                                                                             'select the SNOMED candidate, '
                                                                             'such as exact concept name or '
                                                                             'exact synonym match.',
                                                  'snomed_candidate_name': 'Concept name of the SNOMED CT '
                                                                           'candidate matched to the source '
                                                                           'clinical term.',
                                                  'snomed_candidate_status': 'CANDIDATE_UNREVIEWED, '
                                                                             'AMBIGUOUS, UNMAPPED, '
                                                                             'NO_SOURCE_TERM or DISABLED.',
                                                  'source_clinical_term': 'Original clinical term text '
                                                                          'recorded against the activity in '
                                                                          'the source legacy code field, '
                                                                          'used as input to candidate SNOMED '
                                                                          'enrichment.',
                                                  'source_clinical_term_key': 'Normalised form of the source '
                                                                              'clinical term used as the '
                                                                              'deterministic join key for '
                                                                              'SNOMED candidate matching.',
                                                  'source_code': 'CSDS care activity type code recorded for '
                                                                 'the activity in the source extract.',
                                                  'source_coding_system': 'CSDS care activity type code '
                                                                          'recorded for the activity in the '
                                                                          'source extract.',
                                                  'source_display': 'Description corresponding to the CSDS '
                                                                    'care activity type code.',
                                                  'source_feed': 'Originating data feed responsible for the '
                                                                 'record for each community care activity '
                                                                 'record. It is produced by the silver '
                                                                 'transformation and has no direct '
                                                                 'bronze-column lineage entry. Whitespace '
                                                                 'and source sentinel text are retained '
                                                                 'unless the pipeline explicitly normalizes '
                                                                 'them; null means no value was supplied.',
                                                  'source_update_timestamp': 'Always null as a TIMESTAMP in '
                                                                             'the current community-care '
                                                                             'activity projection. No native '
                                                                             'application-update or '
                                                                             'ingestion clock is supplied '
                                                                             'for this field; neither the '
                                                                             'activity date nor Silver '
                                                                             'refresh time is substituted.',
                                                  'subject_id_system': 'Content-addressed key identifying '
                                                                       'the community patient, derived from '
                                                                       'the source patient number and '
                                                                       'registering source database '
                                                                       'identifier in the CSDS patient '
                                                                       'identifier lookup.',
                                                  'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                 'source identifier. Not salted, not secret; '
                                                                 'a stable join key across feeds where '
                                                                 'person_id is unresolved.',
                                                  'unit_concept_id': 'OMOP standard concept identifier for '
                                                                     'the mapped unit of measure.',
                                                  'unit_concept_name': 'OMOP standard concept name for the '
                                                                       'mapped unit of measure.',
                                                  'unit_mapping_method': 'Indicator of the technique used to '
                                                                         'derive the unit mapping, such as '
                                                                         'direct code match or alias '
                                                                         'dictionary lookup.',
                                                  'unit_mapping_status': 'Status flag describing the outcome '
                                                                         'of mapping the source unit text to '
                                                                         'an OMOP unit concept.',
                                                  'unit_source_value': 'Unit of measure text as recorded in '
                                                                       'the source CSDS CYP202 care activity '
                                                                       'unit field, before normalization.',
                                                  'vip_ind': 'Indicator that the source record carries VIP '
                                                             'handling status for each community care '
                                                             'activity record. It is produced by the silver '
                                                             'transformation and has no direct bronze-column '
                                                             'lineage entry. Null means the source did not '
                                                             'state the indicator and must not be '
                                                             'interpreted as false.',
                                                  'withheld_identity_ind': 'Indicator that the source '
                                                                           'identity was deliberately '
                                                                           'withheld for each community care '
                                                                           'activity record. It is produced '
                                                                           'by the silver transformation and '
                                                                           'has no direct bronze-column '
                                                                           'lineage entry. Null means the '
                                                                           'source did not state the '
                                                                           'indicator and must not be '
                                                                           'interpreted as false.'},
                                      'table': 'One admitted CSDS community-care activity with candidate '
                                               'terminology provenance.'}}

# INSR_R4_SILVER_V1 (module_list): generated by r4/release/gen_silver_components.py; edit the generator, not this.
COMMUNITY_ACTIVITY_PUBLIC_COLUMNS = list(COMMUNITY_ACTIVITY_PUBLIC_COLUMNS) + ['identity_status']  # decision 1: contract columns, additions last
