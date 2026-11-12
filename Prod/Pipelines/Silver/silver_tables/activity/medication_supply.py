"""Silver activity.medication_supply: 2 dataset(s).

- datasets: clinical_medication_supply, _clinical_medication_supply_metadata
- moved verbatim from: 4_silver_care_pathways:3762, 4_silver_care_pathways:3838, 4_silver_care_pathways:3911, 4_silver_care_pathways:3982, 4_silver_care_pathways:3987, 4_silver_care_pathways:3999

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n
from silver_tables._common.journey import _medication_supply_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / medication supply

# contract v2: retain the full canonical shape for internal reuse.
MEDICATION_SUPPLY_SOURCE_COLUMNS = [
    "patient_event_key",
    "homecare_request_item_id",
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
    "request_key",
    "item_seq",
    "request_status_code",
    "request_status_display",
    "item_status_code",
    "item_status_display",
    "item_type",
    "item_description",
    "pack_description",
    "quantity_requested",
    "quantity_original",
    "quantity_delivered",
    "order_unit",
    "label_directions",
    "nfd_reason",
    "supply_start_date",
    "supply_interval",
    "supply_period",
    "request_item_count",
    "request_complete_item_count",
    "request_released_date",
    "location_name",
    "cost_centre_name",
    "indication",
    "clinic",
    "dmd_vtm_concept_id",
    "dmd_vtm_code",
    "dmd_vtm_name",
    "drug_mapping_method",
    "care_site_cd",
    "care_site_match_method",
    "lnkpid",
    "name_key",
    "mrn_candidates",
    "nhs_candidates",
    "person_match_method",
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
    "drug_mapping_rule_id",  # INSR_W2 W2.20
    "drug_mapping_model",  # INSR_W2 W2.20
    "drug_mapping_score",  # INSR_W2 W2.20
    "homecare_contact_name",  # INSR_W2 W2.20
    "homecare_contact_addr1",  # INSR_W2 W2.20
    "homecare_contact_addr2",  # INSR_W2 W2.20
    "homecare_contact_addr3",  # INSR_W2 W2.20
    "homecare_contact_addr4",  # INSR_W2 W2.20
    "homecare_contact_postcode",  # INSR_W2 W2.20
    "homecare_contact_telephone",  # INSR_W2 W2.20
]


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
MEDICATION_SUPPLY_PUBLIC_COLUMNS = [
    'patient_event_key',
    'homecare_request_item_id',
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
    'request_key',
    'item_seq',
    'request_status_code',
    'request_status_display',
    'item_status_code',
    'item_status_display',
    'item_type',
    'item_description',
    'pack_description',
    'quantity_requested',
    'quantity_original',
    'quantity_delivered',
    'order_unit',
    'label_directions',
    'nfd_reason',
    'supply_start_date',
    'supply_interval',
    'supply_period',
    'request_item_count',
    'request_complete_item_count',
    'request_released_date',
    'location_name',
    'cost_centre_name',
    'indication',
    'clinic',
    'dmd_vtm_concept_id',
    'dmd_vtm_code',
    'dmd_vtm_name',
    'drug_mapping_method',
    'care_site_cd',
    'care_site_match_method',
    'lnkpid',
    'name_key',
    'mrn_candidates',
    'nhs_candidates',
    'person_match_method',
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
    "drug_mapping_rule_id",  # INSR_W2 W2.20
    "drug_mapping_model",  # INSR_W2 W2.20
    "drug_mapping_score",  # INSR_W2 W2.20
    "homecare_contact_name",  # INSR_W2 W2.20
    "homecare_contact_addr1",  # INSR_W2 W2.20
    "homecare_contact_addr2",  # INSR_W2 W2.20
    "homecare_contact_addr3",  # INSR_W2 W2.20
    "homecare_contact_addr4",  # INSR_W2 W2.20
    "homecare_contact_postcode",  # INSR_W2 W2.20
    "homecare_contact_telephone",  # INSR_W2 W2.20
]


MEDICATION_SUPPLY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'homecare_request_item_id',
    'identity_status',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_medication_supply():
    # Assemble medication supply rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    return _medication_supply_canonical().select(*MEDICATION_SUPPLY_SOURCE_COLUMNS)


@materialized_view(
    name=_n("journey_clinical.medication_supply"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def medication_supply():
    # Build the declared dataset: One JAC homecare supply-request item; supply, never
    # administration.
    return _lifecycle_source_medication_supply().select(*MEDICATION_SUPPLY_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._medication_supply_metadata'),
    refresh_policy="auto",
)
def medication_supply_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_medication_supply; same row grain as the research table. Join keys:
    # patient_event_key, homecare_request_item_id.
    return (_lifecycle_source_medication_supply()).select(*MEDICATION_SUPPLY_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'clinical_medication_supply': ValuePolicy(table='clinical_medication_supply', enabled=True),
}


WRAP_POLICY = {'clinical_medication_supply'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_medication_supply_metadata': {'columns': {'activity_date': 'Primary-key component carried '
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
                                                   'clinical_medication_supply; same row grain as the '
                                                   'research table. Join keys: patient_event_key, '
                                                   'homecare_request_item_id.'},
 'clinical_medication_supply': {'columns': {'care_site_cd': 'map_care_site key from a unique exact '
                                                            'location-name match.',
                                            'care_site_match_method': 'NAME_EXACT, UNMAPPED, or UNAVAILABLE.',
                                            'clinic': 'Clinic recorded on the request.',
                                            'confidentiality_code': 'Source confidentiality classification '
                                                                    'attached to the record for each '
                                                                    'medication supply record. It is '
                                                                    'produced by the silver transformation '
                                                                    'and has no direct bronze-column lineage '
                                                                    'entry. Source code meanings and '
                                                                    'sentinel values are retained unless the '
                                                                    'pipeline explicitly maps them; null '
                                                                    'means no code was supplied.',
                                            'cost_centre_name': 'Raw JAC homecare cost-centre name.',
                                            'dmd_vtm_code': 'Matched dm+d VTM SNOMED CT identifier.',
                                            'dmd_vtm_concept_id': 'Valid dm+d VTM OMOP concept_id from exact '
                                                                  'item-description matching.',
                                            'dmd_vtm_name': 'Matched dm+d VTM preferred name.',
                                            'drug_mapping_method': 'VTM_NAME, VTM_SYNONYM, UNMAPPED, '
                                                                   'DISABLED, or NULL for itemless headers.',
                                            'drug_mapping_model': 'Verifier model of an '
                                                                  'EMBEDDING_LLM_VERIFIED match. (bronze '
                                                                  'DRUG_MAPPING_MODEL, INSR W1). INSR_W2 '
                                                                  'W2.20.',
                                            'drug_mapping_rule_id': 'Rule id of an EMBEDDING_LLM_VERIFIED '
                                                                    'match. (bronze DRUG_MAPPING_RULE_ID, '
                                                                    'INSR W1). INSR_W2 W2.20.',
                                            'drug_mapping_score': 'Cosine similarity of an '
                                                                  'EMBEDDING_LLM_VERIFIED match. (bronze '
                                                                  'DRUG_MAPPING_SCORE, INSR W1). INSR_W2 '
                                                                  'W2.20.',
                                            'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                            'available.',
                                            'event_datetime': 'Source request-created date.',
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
                                            'event_end_datetime': 'Source request-completed date.',
                                            'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                        'wall-clock time (GMT/BST), by the '
                                                                        'same rule as event_datetime_local; '
                                                                        'null where event_end_datetime is '
                                                                        'null.',
                                            'fact_category': 'Contract category used to group the record '
                                                             'with comparable clinical facts for each '
                                                             'medication supply record. It is produced by '
                                                             'the silver transformation and has no direct '
                                                             'bronze-column lineage entry. Whitespace and '
                                                             'source sentinel text are retained unless the '
                                                             'pipeline explicitly normalizes them; null '
                                                             'means no value was supplied.',
                                            'homecare_contact_addr1': 'Homecare delivery address line 1. '
                                                                      'Identifier, published and tagged. '
                                                                      'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                      'HOMECARE_CONTACT_ADDR1, INSR W1). '
                                                                      'INSR_W2 W2.20.',
                                            'homecare_contact_addr2': 'Homecare delivery address line 2. '
                                                                      'Identifier, published and tagged. '
                                                                      'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                      'HOMECARE_CONTACT_ADDR2, INSR W1). '
                                                                      'INSR_W2 W2.20.',
                                            'homecare_contact_addr3': 'Homecare delivery address line 3. '
                                                                      'Identifier, published and tagged. '
                                                                      'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                      'HOMECARE_CONTACT_ADDR3, INSR W1). '
                                                                      'INSR_W2 W2.20.',
                                            'homecare_contact_addr4': 'Homecare delivery address line 4. '
                                                                      'Identifier, published and tagged. '
                                                                      'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                      'HOMECARE_CONTACT_ADDR4, INSR W1). '
                                                                      'INSR_W2 W2.20.',
                                            'homecare_contact_name': 'Homecare delivery contact name. '
                                                                     'Identifier, published and tagged. '
                                                                     'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                     'HOMECARE_CONTACT_NAME, INSR W1). '
                                                                     'INSR_W2 W2.20.',
                                            'homecare_contact_postcode': 'Homecare delivery postcode. '
                                                                         'Identifier, published and tagged. '
                                                                         'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                         '(bronze HOMECARE_CONTACT_POSTCODE, '
                                                                         'INSR W1). INSR_W2 W2.20.',
                                            'homecare_contact_telephone': 'Homecare delivery contact '
                                                                          'telephone. Identifier, published '
                                                                          'and tagged. '
                                                                          'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                          '(bronze '
                                                                          'HOMECARE_CONTACT_TELEPHONE, INSR '
                                                                          'W1). INSR_W2 W2.20.',
                                            'homecare_request_item_id': 'JAC HOMECARE_REQUEST_ITEM_ID; '
                                                                        'primary key of this table.',
                                            'indication': 'Clinical indication recorded on the request.',
                                            'item_description': 'Source medicine or supply description.',
                                            'item_seq': 'Item sequence within the request; NULL for an '
                                                        'itemless header.',
                                            'item_status_code': 'Raw JAC item status code.',
                                            'item_status_display': 'COMPLETE, INCOMPLETE, or provisional '
                                                                   'CANCELLED_OR_ERROR.',
                                            'item_type': 'Raw JAC homecare item type.',
                                            'label_directions': 'Source dispensing label directions.',
                                            'lnkpid': 'Direct identifier published and IG-governed at serve '
                                                      'time',
                                            'loaded_at': 'map_homecare_request.ADC_UPDT carried unchanged '
                                                         'through usable-code filtering and source/public '
                                                         'projections. No parent clock is joined or '
                                                         'aggregated. This input load clock is distinct from '
                                                         'the preferred item/request mirror clock in '
                                                         'source_update_timestamp, supply dates and Silver '
                                                         'refresh time.',
                                            'location_name': 'Raw JAC homecare location name.',
                                            'mrn_candidates': 'Distinct active Cerner PERSON_ID candidates '
                                                              'for the normalised MRN.',
                                            'name_key': 'Direct identifier published and IG-governed at '
                                                        'serve time',
                                            'nfd_reason': 'Source reason the item was not fulfilled.',
                                            'nhs_candidates': 'Distinct active Cerner PERSON_ID candidates '
                                                              'for the checksum-valid NHS number.',
                                            'order_unit': 'Source order unit.',
                                            'pack_description': 'Source pack description.',
                                            'patient_event_key': 'Deterministic event key derived from '
                                                                 'source identifiers; retained as a stable '
                                                                 'join key.',
                                            'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                         'resolved.',
                                            'person_match_method': 'MRN_NHS_AGREE, MRN_ONLY, NHS_ONLY, '
                                                                   'CONFLICT, AMBIGUOUS, or NONE.',
                                            'person_match_status': 'MATCHED or the reason no PERSON_ID was '
                                                                   'assigned.',
                                            'quantity_delivered': 'Delivered quantity from the item.',
                                            'quantity_original': 'Original requested quantity from the item.',
                                            'quantity_requested': 'Requested quantity from the item.',
                                            'record_status': 'Retracted when '
                                                             'map_homecare_request.SOURCE_PRESENT_IND is '
                                                             'false, otherwise active; missing presence '
                                                             'defaults true. No superseded status is '
                                                             'emitted. Request/item status, request '
                                                             'completion and delivered quantity do not '
                                                             'determine this label; usable-code filtering '
                                                             'separately controls publication eligibility.',
                                            'record_status_effective_from': 'Source request-created date.',
                                            'record_status_effective_to': 'map_homecare_request.ADC_UPDT '
                                                                          'only when SOURCE_PRESENT_IND is '
                                                                          'false; otherwise null. This is '
                                                                          'the input-load-time end proxy, '
                                                                          'not REQUEST_COMPLETED or a '
                                                                          'clinical supply end; a missing '
                                                                          'ADC_UPDT remains null.',
                                            'request_complete_item_count': 'Header-reported completed item '
                                                                           'count.',
                                            'request_item_count': 'Header-reported total item count.',
                                            'request_key': 'JAC homecare request identifier from lnkrb.',
                                            'request_released_date': 'Source request-released date.',
                                            'request_status_code': 'Raw JAC header status code.',
                                            'request_status_display': 'COMPLETE, RELEASED, IN_PROGRESS, or '
                                                                      'provisional CANCELLED_OR_ERROR.',
                                            'source_code': 'Matched dm+d VTM SNOMED CT identifier.',
                                            'source_coding_system': 'Matched dm+d VTM SNOMED CT identifier.',
                                            'source_display': 'Matched dm+d VTM preferred name.',
                                            'source_feed': 'Originating data feed responsible for the record '
                                                           'for each medication supply record. It is '
                                                           'produced by the silver transformation and has no '
                                                           'direct bronze-column lineage entry. Whitespace '
                                                           'and source sentinel text are retained unless the '
                                                           'pipeline explicitly normalizes them; null means '
                                                           'no value was supplied.',
                                            'source_update_timestamp': 'First non-null '
                                                                       'map_homecare_request.ITEM_SOURCE_RECORD_UPDATED_DT '
                                                                       'then SOURCE_RECORD_UPDATED_DT. These '
                                                                       'are upstream mirror timestamps, with '
                                                                       'the item clock preferred even when '
                                                                       'the request clock is later; the '
                                                                       'expression is coalesce, not '
                                                                       'greatest, and has no ADC_UPDT or '
                                                                       'Silver refresh-time fallback.',
                                            'subject_id_system': 'JAC internal patient link key; not a '
                                                                 'direct patient identifier.',
                                            'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                           'identifier. Not salted, not secret; a stable '
                                                           'join key across feeds where person_id is '
                                                           'unresolved.',
                                            'supply_interval': 'Source supply interval.',
                                            'supply_period': 'Source supply period.',
                                            'supply_start_date': 'Requested supply start date.',
                                            'vip_ind': 'Indicator that the source record carries VIP '
                                                       'handling status for each medication supply record. '
                                                       'It is produced by the silver transformation and has '
                                                       'no direct bronze-column lineage entry. Null means '
                                                       'the source did not state the indicator and must not '
                                                       'be interpreted as false.',
                                            'withheld_identity_ind': 'Indicator that the source identity was '
                                                                     'deliberately withheld for each '
                                                                     'medication supply record. It is '
                                                                     'produced by the silver transformation '
                                                                     'and has no direct bronze-column '
                                                                     'lineage entry. Null means the source '
                                                                     'did not state the indicator and must '
                                                                     'not be interpreted as false.'},
                                'table': 'One JAC homecare supply-request item; supply, never '
                                         'administration.'}}

# INSR_R4_SILVER_V1 (module_list): generated by r4/release/gen_silver_components.py; edit the generator, not this.
MEDICATION_SUPPLY_PUBLIC_COLUMNS = list(MEDICATION_SUPPLY_PUBLIC_COLUMNS) + ['identity_status']  # decision 1: contract columns, additions last
