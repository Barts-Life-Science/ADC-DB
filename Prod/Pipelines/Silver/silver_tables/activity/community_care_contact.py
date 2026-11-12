"""Silver activity.community_care_contact: 2 dataset(s).

- datasets: clinical_community_care_contact, _clinical_community_care_contact_metadata
- moved verbatim from: 4_silver_care_pathways:2199, 4_silver_care_pathways:2253, 4_silver_care_pathways:2304, 4_silver_care_pathways:2363, 4_silver_care_pathways:2368, 4_silver_care_pathways:2379, silver_value_policy.py:413

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n
from silver_tables._common.journey import _community_care_contact_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# W2.11 (L2513, P9_L2513): A3A = 99.9% of unresolved commissioner codes (77.9% of all community contacts, ledger
# W2_11_a3a); the ODS check (lanes/ods_a3a_check.py -> lanes/ods_a3a.json, 2026-10-30) confirmed it is the
# 3-character prefix of exactly one active sub-ICB location (A3A8R, RO98) and named its parent ICB (QMF, via RE5).
_A3A = {"alias": "A3A", "ods_code": "A3A8R", "parent_icb_ods_code": "QMF"}   # from lanes/ods_a3a.json


# Clinical / community care contact

# contract v2: retain the full canonical shape for internal reuse.
COMMUNITY_CONTACT_SOURCE_COLUMNS = [
    "patient_event_key",
    "community_care_contact_key",
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
    "care_contact_date",
    "community_database_id",
    "care_contact_id",
    "community_patient_key",
    "service_request_id",
    "service_id",
    "service_name",
    "team_id",
    "care_contact_id_variant_count",
    "person_match_status",
    "duration_minutes",
    "duration_quality_status",
    "earliest_reasonable_offer_date",
    "earliest_clinically_appropriate_date",
    "commissioner_ods_code",
    "commissioner_organization_id",
    "commissioner_organization_name",
    "consultation_mechanism_code",
    "consultation_mechanism_display",
    "location_type_code",
    "location_type_display",
    "service_team_type_code",
    "service_team_type_display",
    "service_team_type_mapping_status",
    "source_consultation_term",
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
COMMUNITY_CONTACT_PUBLIC_COLUMNS = [
    'patient_event_key',
    'community_care_contact_key',
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
    'care_contact_date',
    'community_database_id',
    'care_contact_id',
    'community_patient_key',
    'service_request_id',
    'service_id',
    'service_name',
    'team_id',
    'care_contact_id_variant_count',
    'person_match_status',
    'duration_minutes',
    'duration_quality_status',
    'earliest_reasonable_offer_date',
    'earliest_clinically_appropriate_date',
    'commissioner_ods_code',
    'commissioner_organization_id',
    'commissioner_organization_name',
    'consultation_mechanism_code',
    'consultation_mechanism_display',
    'location_type_code',
    'location_type_display',
    'service_team_type_code',
    'service_team_type_display',
    'service_team_type_mapping_status',
    'source_consultation_term',
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


COMMUNITY_CONTACT_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'community_care_contact_key',
    'identity_status',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_community_care_contact():
    # Assemble community care contact rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _community_care_contact_canonical().select(*COMMUNITY_CONTACT_SOURCE_COLUMNS)


@materialized_view(
    name=_n("journey_clinical.community_care_contact"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def community_care_contact():
    # Build the declared dataset: One CSDS community-care contact with sentinel-safe event time.
    return _lifecycle_source_community_care_contact().select(*COMMUNITY_CONTACT_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._community_care_contact_metadata'),
    refresh_policy="auto",
)
def community_care_contact_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_community_care_contact; same row grain as the research table. Join keys:
    # patient_event_key, community_care_contact_key.
    return (_lifecycle_source_community_care_contact()).select(*COMMUNITY_CONTACT_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'clinical_community_care_contact': ValuePolicy(table='clinical_community_care_contact', enabled=True),
}


VALUE_POLICIES["clinical_community_care_contact"].register("commissioner_ods_code_resolved", ["commissioner_ods_code"],
         lambda F: F.when(F.upper(F.trim(F.col("commissioner_ods_code"))) == _A3A["alias"], F.lit(_A3A["ods_code"]))
                    .otherwise(F.col("commissioner_ods_code")),
         "Commissioner ODS code with the 3-character form A3A resolved to its sub-ICB location (ODS ORD API, "
         "RO98/RO319); other codes unchanged. INSR_W2 W2.11 (L2513).")


VALUE_POLICIES["clinical_community_care_contact"].register("commissioner_parent_icb_ods_code", ["commissioner_ods_code"],
         lambda F: F.when(F.upper(F.trim(F.col("commissioner_ods_code"))) == _A3A["alias"],
                          F.lit(_A3A["parent_icb_ods_code"])),
         "Parent ICB of the sub-ICB location resolved from A3A (ODS RE5 relationship); NULL for other codes. "
         "INSR_W2 W2.11.")


VALUE_POLICIES["clinical_community_care_contact"].register("commissioner_resolution_method", ["commissioner_ods_code"],
         lambda F: F.when(F.upper(F.trim(F.col("commissioner_ods_code"))) == _A3A["alias"],
                          F.lit("ods:sub_icb_location_prefix:A3A"))
                    .when(F.col("commissioner_ods_code").isNotNull(), F.lit("submitted")),
         "submitted, or ods:sub_icb_location_prefix:A3A when resolved from the 3-character form. INSR_W2 W2.11.")


WRAP_POLICY = {'clinical_community_care_contact'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_community_care_contact_metadata': {'columns': {'activity_date': 'Primary-key component carried '
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
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
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
                                                           'birth_row_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                           'bloodtrack_transaction_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                           'cancer_treatment_cycle_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                           'care_contact_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                           'care_participation_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
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
                                                                                          'parent; joins one '
                                                                                          'lifecycle row to '
                                                                                          'that research '
                                                                                          'row.',
                                                           'community_care_contact_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                           'concept_map_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                           'cost_line_item_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                           'dcp_forms_activity_id': 'Native source '
                                                                                    'identifier carried '
                                                                                    'unchanged from the '
                                                                                    'public parent; joins '
                                                                                    'one lifecycle row to '
                                                                                    'that research row.',
                                                           'device_mapping_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                           'diagnosis_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                           'dicom_path': 'Primary-key component carried '
                                                                         'unchanged from the public parent; '
                                                                         "combine it with the companion's "
                                                                         'other key columns when joining.',
                                                           'doc_response_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
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
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                           'encounter_bounds_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                           'encounter_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                           'encounter_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
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
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                           'episode_encounter_reltn_id': 'Native source '
                                                                                         'identifier carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
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
                                                                                    'does not filter the '
                                                                                    'row.',
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
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
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
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                           'location_stay_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                           'mc_device_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                           'organization_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                           'pacs_examination_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                                                'the public parent; joins '
                                                                                'one lifecycle row to that '
                                                                                'event row.',
                                                           'patient_event_row_key': 'Deterministic row-level '
                                                                                    'key carried unchanged '
                                                                                    'from '
                                                                                    'events_patient_event; '
                                                                                    'joins one lifecycle row '
                                                                                    'to one event-index row.',
                                                           'period_business_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                           'person_death_evidence_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
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
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                           'person_relationship_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
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
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                           'practitioner_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                           'practitioner_identifier_key': 'Deterministic v2 '
                                                                                          'key carried '
                                                                                          'unchanged from '
                                                                                          'the public '
                                                                                          'parent; joins one '
                                                                                          'lifecycle row to '
                                                                                          'that research '
                                                                                          'row.',
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
                                                                                                 'row to '
                                                                                                 'that '
                                                                                                 'research '
                                                                                                 'row.',
                                                           'pregnancy_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                           'pregnancy_reconciliation_key': 'Deterministic v2 '
                                                                                           'key carried '
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
                                                           'procedure_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                           'pt_prot_reg_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
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
                                                           'referral_oid': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                           'row_source': 'Physical bronze row source, '
                                                                         'CURRENT or HIST, carried unchanged '
                                                                         'from the public parent; a key '
                                                                         'component because the research '
                                                                         'table is at physical version grain '
                                                                         'and patient_event_key is shared '
                                                                         "across an entry's versions.",
                                                           'rtt_activity_oid': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                           'sch_event_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                           'source_identifier_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                           'source_object': 'Source-arm discriminator '
                                                                            'carried unchanged from the '
                                                                            'public parent; use it with '
                                                                            'arm-specific native identifiers '
                                                                            'when joining.',
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
                                                                                       'public parent; joins '
                                                                                       'one lifecycle row to '
                                                                                       'that research row.',
                                                           'theatre_attendance_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                           'theatre_case_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                           'theatre_case_milestone_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                           'theatre_implant_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                           'transfusion_event_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
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
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                           'waiting_list_oid': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                           'waiting_list_snapshot_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.'},
                                               'table': 'Internal quality and batch metadata for '
                                                        'clinical_community_care_contact; same row grain as '
                                                        'the research table. Join keys: patient_event_key, '
                                                        'community_care_contact_key.'},
 'clinical_community_care_contact': {'columns': {'care_contact_date': 'Date of the care contact as submitted '
                                                                      'in the CSDS CYP201 record.',
                                                 'care_contact_id': 'Source CYP201 Care Contact Identifier; '
                                                                    'not unique by itself.',
                                                 'care_contact_id_variant_count': 'Rows sharing '
                                                                                  'source_database_id and '
                                                                                  'care_contact_id.',
                                                 'commissioner_ods_code': 'Submitted commissioner ODS code.',
                                                 'commissioner_ods_code_resolved': 'Commissioner ODS code '
                                                                                   'with the 3-character '
                                                                                   'form A3A resolved to its '
                                                                                   'sub-ICB location (ODS '
                                                                                   'ORD API, RO98/RO319); '
                                                                                   'other codes unchanged. '
                                                                                   'INSR_W2 W2.11 (L2513).',
                                                 'commissioner_organization_id': 'Internal organisation '
                                                                                 'identifier for the '
                                                                                 'commissioner resolved by '
                                                                                 'matching the submitted ODS '
                                                                                 'code to organisation alias '
                                                                                 'records.',
                                                 'commissioner_organization_name': 'Name of the commissioner '
                                                                                   'organisation resolved '
                                                                                   'from the organisation '
                                                                                   'reference data for the '
                                                                                   'submitted commissioner '
                                                                                   'ODS code.',
                                                 'commissioner_parent_icb_ods_code': 'Parent ICB of the '
                                                                                     'sub-ICB location '
                                                                                     'resolved from A3A (ODS '
                                                                                     'RE5 relationship); '
                                                                                     'NULL for other codes. '
                                                                                     'INSR_W2 W2.11.',
                                                 'commissioner_resolution_method': 'submitted, or '
                                                                                   'ods:sub_icb_location_prefix:A3A '
                                                                                   'when resolved from the '
                                                                                   '3-character form. '
                                                                                   'INSR_W2 W2.11.',
                                                 'community_care_contact_key': 'Source-preserved community '
                                                                               'care contact key; primary '
                                                                               'key of this table.',
                                                 'community_database_id': 'Source community system database '
                                                                          'identifier (CDB) from the CSDS '
                                                                          'CYP201 care contact extract, '
                                                                          'forming part of the record grain.',
                                                 'community_patient_key': 'Pseudonymised patient key derived '
                                                                          'from the source community system '
                                                                          'patient number and its '
                                                                          'registering database identifier.',
                                                 'confidentiality_code': 'Source confidentiality '
                                                                         'classification attached to the '
                                                                         'record for each community care '
                                                                         'contact record. It is produced by '
                                                                         'the silver transformation and has '
                                                                         'no direct bronze-column lineage '
                                                                         'entry. Source code meanings and '
                                                                         'sentinel values are retained '
                                                                         'unless the pipeline explicitly '
                                                                         'maps them; null means no code was '
                                                                         'supplied.',
                                                 'consultation_mechanism_code': 'Consultation mechanism '
                                                                                '(medium of contact) code '
                                                                                'standardised by the '
                                                                                'pipeline from the submitted '
                                                                                'source value.',
                                                 'consultation_mechanism_display': 'Description '
                                                                                   'corresponding to the '
                                                                                   'standardised '
                                                                                   'consultation mechanism '
                                                                                   'code.',
                                                 'duration_minutes': 'Clinical contact duration in minutes '
                                                                     'as submitted in the CSDS CYP201 '
                                                                     'record.',
                                                 'duration_quality_status': 'Derived data-quality status '
                                                                            'describing the completeness or '
                                                                            'plausibility of the submitted '
                                                                            'clinical contact duration.',
                                                 'earliest_clinically_appropriate_date': 'Earliest '
                                                                                         'clinically '
                                                                                         'appropriate date '
                                                                                         'submitted with the '
                                                                                         'CSDS CYP201 care '
                                                                                         'contact record.',
                                                 'earliest_reasonable_offer_date': 'Earliest reasonable '
                                                                                   'offer date submitted '
                                                                                   'with the CSDS CYP201 '
                                                                                   'care contact record.',
                                                 'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                                 'available.',
                                                 'event_datetime': 'Date of the care contact as submitted in '
                                                                   'the CSDS CYP201 record.',
                                                 'event_datetime_local': 'event_datetime in Europe/London '
                                                                         'wall-clock time (GMT/BST). '
                                                                         'Millennium rows take the bronze '
                                                                         '*_LOCAL companions of the same '
                                                                         'source columns and fallback order; '
                                                                         'rows from local-clock sources '
                                                                         '(LUNA, PACS/DICOM, TFC LIMS, '
                                                                         'CCMDS, MSDS, BadgerNet, EndoBase, '
                                                                         'SLAM, iWeb, Datix and similar) '
                                                                         'carry event_datetime unchanged. '
                                                                         'event_datetime is a UTC instant '
                                                                         'for Millennium rows, so take local '
                                                                         'calendar dates and clock times '
                                                                         'from this column.',
                                                 'event_end_datetime': 'Date and time when the represented '
                                                                       'clinical or administrative event '
                                                                       'ended for each community care '
                                                                       'contact record. It is produced by '
                                                                       'the silver transformation and has no '
                                                                       'direct bronze-column lineage entry. '
                                                                       'Source precision and timezone '
                                                                       'handling follow the pipeline '
                                                                       'expression; null means the time was '
                                                                       'unavailable.',
                                                 'event_end_datetime_local': 'event_end_datetime in '
                                                                             'Europe/London wall-clock time '
                                                                             '(GMT/BST), by the same rule as '
                                                                             'event_datetime_local; null '
                                                                             'where event_end_datetime is '
                                                                             'null.',
                                                 'fact_category': 'Contract category used to group the '
                                                                  'record with comparable clinical facts for '
                                                                  'each community care contact record. It is '
                                                                  'produced by the silver transformation and '
                                                                  'has no direct bronze-column lineage '
                                                                  'entry. Whitespace and source sentinel '
                                                                  'text are retained unless the pipeline '
                                                                  'explicitly normalizes them; null means no '
                                                                  'value was supplied.',
                                                 'loaded_at': 'Always null as a TIMESTAMP in the current '
                                                              'community-care contact projection. No '
                                                              'contributing load timestamp is supplied; '
                                                              'neither the contact date nor Silver refresh '
                                                              'time is substituted.',
                                                 'location_type_code': 'CSDS activity location type (site '
                                                                       'type) code submitted on the CYP201 '
                                                                       'care contact, derived from the '
                                                                       'source location code field.',
                                                 'location_type_display': 'Descriptive label for the CSDS '
                                                                          'activity location type code '
                                                                          'recorded on the care contact.',
                                                 'patient_event_key': 'Deterministic event key derived from '
                                                                      'source identifiers; retained as a '
                                                                      'stable join key.',
                                                 'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                              'resolved.',
                                                 'person_match_status': 'Status flag indicating the outcome '
                                                                        'of matching the community patient '
                                                                        'record to a curated person record.',
                                                 'record_status': 'Retracted when '
                                                                  'map_community_care_contact.SOURCE_PRESENT_IND '
                                                                  'is false, otherwise active; missing '
                                                                  'presence defaults true. No superseded '
                                                                  'status is emitted. This describes '
                                                                  'source-row presence, not attendance, '
                                                                  'consultation outcome or identity-match '
                                                                  'status.',
                                                 'record_status_effective_from': 'First non-null '
                                                                                 'care_contact_datetime_local '
                                                                                 'cast to TIMESTAMP then '
                                                                                 'care_contact_date cast to '
                                                                                 'TIMESTAMP from '
                                                                                 'map_community_care_contact. '
                                                                                 'This contact-time proxy is '
                                                                                 'not an independent status '
                                                                                 'transition. Unlike '
                                                                                 'event_datetime, this '
                                                                                 'history start is not '
                                                                                 'clamped to the [1950,2100) '
                                                                                 'range, and has no '
                                                                                 'ingestion-time fallback.',
                                                 'record_status_effective_to': 'map_community_care_contact.SOURCE_ABSENT_DETECTED_TS '
                                                                               'only when SOURCE_PRESENT_IND '
                                                                               'is false; otherwise null. A '
                                                                               'missing absence timestamp '
                                                                               'stays null, with no '
                                                                               'ingestion-time fallback. '
                                                                               'This is the row-absence end '
                                                                               'proxy, not the clinical '
                                                                               'contact end.',
                                                 'service_id': 'Identifier of the registering service or '
                                                               'organisation recorded in the source '
                                                               'community system patient lookup.',
                                                 'service_name': 'Name of the registering service or '
                                                                 'organisation recorded in the source '
                                                                 'community system patient lookup.',
                                                 'service_request_id': 'Source CYP201 Service Request '
                                                                       'Identifier.',
                                                 'service_team_type_code': 'CSDS team type code identifying '
                                                                           'the type of service team '
                                                                           'delivering the care contact.',
                                                 'service_team_type_display': 'Descriptive label for the '
                                                                              'CSDS service team type code '
                                                                              'recorded on the care contact.',
                                                 'service_team_type_mapping_status': 'Derived status flag '
                                                                                     'indicating whether the '
                                                                                     'submitted CSDS team '
                                                                                     'type value was '
                                                                                     'successfully resolved '
                                                                                     'to a recognised '
                                                                                     'service team type '
                                                                                     'reference code.',
                                                 'source_code': 'Coded consultation type submitted with the '
                                                                'CSDS CYP201 care contact record.',
                                                 'source_coding_system': 'Coded consultation type submitted '
                                                                         'with the CSDS CYP201 care contact '
                                                                         'record.',
                                                 'source_consultation_term': 'Original consultation '
                                                                             'mechanism term as recorded in '
                                                                             'the source system before '
                                                                             'normalisation.',
                                                 'source_display': 'Description corresponding to the coded '
                                                                   'consultation type submitted with the '
                                                                   'care contact record.',
                                                 'source_feed': 'Originating data feed responsible for the '
                                                                'record for each community care contact '
                                                                'record. It is produced by the silver '
                                                                'transformation and has no direct '
                                                                'bronze-column lineage entry. Whitespace and '
                                                                'source sentinel text are retained unless '
                                                                'the pipeline explicitly normalizes them; '
                                                                'null means no value was supplied.',
                                                 'source_update_timestamp': 'Always null as a TIMESTAMP in '
                                                                            'the current community-care '
                                                                            'contact projection. No native '
                                                                            'application-update or ingestion '
                                                                            'clock is supplied for this '
                                                                            'field; neither the contact date '
                                                                            'nor Silver refresh time is '
                                                                            'substituted.',
                                                 'subject_id_system': 'Pseudonymised patient key derived '
                                                                      'from the source community system '
                                                                      'patient number and its registering '
                                                                      'database identifier.',
                                                 'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                'source identifier. Not salted, not secret; '
                                                                'a stable join key across feeds where '
                                                                'person_id is unresolved.',
                                                 'team_id': 'Local team identifier submitted with the CSDS '
                                                            'CYP201 care contact record.',
                                                 'vip_ind': 'Indicator that the source record carries VIP '
                                                            'handling status for each community care contact '
                                                            'record. It is produced by the silver '
                                                            'transformation and has no direct bronze-column '
                                                            'lineage entry. Null means the source did not '
                                                            'state the indicator and must not be interpreted '
                                                            'as false.',
                                                 'withheld_identity_ind': 'Indicator that the source '
                                                                          'identity was deliberately '
                                                                          'withheld for each community care '
                                                                          'contact record. It is produced by '
                                                                          'the silver transformation and has '
                                                                          'no direct bronze-column lineage '
                                                                          'entry. Null means the source did '
                                                                          'not state the indicator and must '
                                                                          'not be interpreted as false.'},
                                     'table': 'One CSDS community-care contact with sentinel-safe event '
                                              'time.'}}

# INSR_R4_SILVER_V1 (module_list): generated by r4/release/gen_silver_components.py; edit the generator, not this.
COMMUNITY_CONTACT_PUBLIC_COLUMNS = list(COMMUNITY_CONTACT_PUBLIC_COLUMNS) + ['identity_status']  # decision 1: contract columns, additions last
