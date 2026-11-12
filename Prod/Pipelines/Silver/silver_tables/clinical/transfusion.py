"""Silver clinical.transfusion: 2 dataset(s).

- datasets: clinical_transfusion, _clinical_transfusion_metadata
- moved verbatim from: 2_silver_clinical_events:7125, 2_silver_clinical_events:7190, 2_silver_clinical_events:7252, 2_silver_clinical_events:7305, 2_silver_clinical_events:7310, 2_silver_clinical_events:7322

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n
from silver_tables._common.journey import _transfusion_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / transfusion

# contract v2: retain the full canonical shape for internal reuse.
TRANSFUSION_SOURCE_COLUMNS = [
    "patient_event_key",
    "transfusion_key",
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
    "begin_datetime",
    "end_datetime",
    "transfusion_status",
    "elapsed_minutes",
    "unit_number",
    "blood_product_group",
    "blood_unit_group",
    "patient_blood_group",
    "quantity_value",
    "quantity_raw",
    "begin_location",
    "end_location",
    "unit_is_irradiated",
    "unit_is_cmv_neg",
    "requires_irradiated",
    "requires_cmv_neg",
    "ambiguity_ind",
    "product_concept_id",
    "product_concept_name",
    "unit_group_concept_id",
    "patient_group_concept_id",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
    "end_quantity_unit_inferred",  # INSR_W2 W2.20
    "end_quantity_ml",  # INSR_W2 W2.20
    "quantity_unit_rule",  # INSR_W2 W2.20
    "isbt_product_concept_id",  # INSR_W2 W2.20
    "isbt_product_concept_name",  # INSR_W2 W2.20
    "isbt_mapping_method",  # INSR_W2 W2.20
    "isbt_mapping_rule_id",  # INSR_W2 W2.20
    "isbt_mapping_model",  # INSR_W2 W2.20
    "isbt_mapping_score",  # INSR_W2 W2.20
    "product_mapping_method",  # INSR_W2 W2.20
    "product_mapping_rule_id",  # INSR_W2 W2.20
    "unit_group_mapping_method",  # INSR_W2 W2.20
    "unit_group_mapping_rule_id",  # INSR_W2 W2.20
    "patient_group_mapping_method",  # INSR_W2 W2.20
    "patient_group_mapping_rule_id",  # INSR_W2 W2.20
    "product_mapping_source",  # INSR_W2 W2.20
    "source_mrn_norm",  # INSR_W2 W2.20
]


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
TRANSFUSION_PUBLIC_COLUMNS = [
    'patient_event_key',
    'transfusion_key',
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
    'begin_datetime',
    'end_datetime',
    'transfusion_status',
    'elapsed_minutes',
    'unit_number',
    'blood_product_group',
    'blood_unit_group',
    'patient_blood_group',
    'quantity_value',
    'quantity_raw',
    'begin_location',
    'end_location',
    'unit_is_irradiated',
    'unit_is_cmv_neg',
    'requires_irradiated',
    'requires_cmv_neg',
    'ambiguity_ind',
    'product_concept_id',
    'product_concept_name',
    'unit_group_concept_id',
    'patient_group_concept_id',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
    "end_quantity_unit_inferred",  # INSR_W2 W2.20
    "end_quantity_ml",  # INSR_W2 W2.20
    "quantity_unit_rule",  # INSR_W2 W2.20
    "isbt_product_concept_id",  # INSR_W2 W2.20
    "isbt_product_concept_name",  # INSR_W2 W2.20
    "isbt_mapping_method",  # INSR_W2 W2.20
    "isbt_mapping_rule_id",  # INSR_W2 W2.20
    "isbt_mapping_model",  # INSR_W2 W2.20
    "isbt_mapping_score",  # INSR_W2 W2.20
    "product_mapping_method",  # INSR_W2 W2.20
    "product_mapping_rule_id",  # INSR_W2 W2.20
    "unit_group_mapping_method",  # INSR_W2 W2.20
    "unit_group_mapping_rule_id",  # INSR_W2 W2.20
    "patient_group_mapping_method",  # INSR_W2 W2.20
    "patient_group_mapping_rule_id",  # INSR_W2 W2.20
    "product_mapping_source",  # INSR_W2 W2.20
    "source_mrn_norm",  # INSR_W2 W2.20
]


TRANSFUSION_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'transfusion_key',
    'identity_status',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_transfusion():
    # Assemble transfusion rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return _transfusion_canonical().select(*TRANSFUSION_SOURCE_COLUMNS)


@materialized_view(
    name=_n("journey_clinical.transfusion"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def transfusion():
    # Build the declared dataset: One paired BloodTrack unit-recipient transfusion episode,
    # retaining available timing and source-event linkage.
    return _lifecycle_source_transfusion().select(*TRANSFUSION_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._transfusion_metadata'),
    refresh_policy="auto",
)
def transfusion_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_transfusion;
    # same row grain as the research table. Join keys: patient_event_key, transfusion_key.
    return (_lifecycle_source_transfusion()).select(*TRANSFUSION_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'clinical_transfusion': ValuePolicy(table='clinical_transfusion', enabled=True),
}


WRAP_POLICY = {'clinical_transfusion'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_transfusion_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
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
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
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
                                                'cost_line_item_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                'dcp_forms_activity_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                'device_mapping_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                'encounter_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                'endobase_exam_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                'endobase_exam_term_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                'entity_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
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
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                'episode_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                'episode_id_resolved': 'True when episode_id is null or '
                                                                       'resolves to the governed episode '
                                                                       'spine; false records unresolved '
                                                                       'linkage without dropping the row.',
                                                'episode_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                                'event_after_death_30d': 'True when the event timestamp is '
                                                                         'more than 30 days after the '
                                                                         "resolved person's death timestamp; "
                                                                         'silver records the doubt and does '
                                                                         'not filter the row.',
                                                'event_before_birth': 'True when the event timestamp '
                                                                      "precedes the resolved person's birth "
                                                                      'timestamp; silver records the doubt '
                                                                      'and does not filter the row.',
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
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                'genetic_test_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                'homecare_request_item_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                'is_illogical': 'Source RTT chronology flag indicating an '
                                                                'illogical activity sequence; retained for '
                                                                'the gold drop rule.',
                                                'journey_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                                'journey_link_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                                'journey_participant_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                'pacs_report_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                'pathway_oid': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                                'patient_event_key': 'Deterministic SHA-256 event key '
                                                                     'carried unchanged from the public '
                                                                     'parent; joins one lifecycle row to '
                                                                     'that event row.',
                                                'patient_event_row_key': 'Deterministic row-level key '
                                                                         'carried unchanged from '
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
                                                'person_address_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                'person_attribute_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                'person_death_evidence_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                'person_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                                'person_id_resolved': 'True when person_id is null or '
                                                                      'resolves to the governed person '
                                                                      'spine; false records unresolved '
                                                                      'linkage without dropping the row.',
                                                'person_identifier_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                'person_relationship_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                'pharmacy_issue_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
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
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                'pregnancy_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                'pregnancy_reconciliation_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                'problem_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                'procedure_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                'pt_prot_reg_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                'ptl_unique_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                                                              'applies; null means no source '
                                                                              'end is known.',
                                                'referral_oid': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
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
                                                'row_source': 'Physical bronze row source, CURRENT or HIST, '
                                                              'carried unchanged from the public parent; a '
                                                              'key component because the research table is '
                                                              'at physical version grain and '
                                                              "patient_event_key is shared across an entry's "
                                                              'versions.',
                                                'rtt_activity_oid': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                'sch_event_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                'score_type': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                                'sequence_nbr': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                                'service_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                'service_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                                'snapshot_date': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
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
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
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
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                'susceptibility_result_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                'treatment_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                'update_count': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                                'valid_from_datetime': 'Primary-key component carried '
                                                                       'unchanged from the public parent; '
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                'value_set_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                'waiting_list_oid': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                'waiting_list_snapshot_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.'},
                                    'table': 'Internal quality and batch metadata for clinical_transfusion; '
                                             'same row grain as the research table. Join keys: '
                                             'patient_event_key, transfusion_key.'},
 'clinical_transfusion': {'columns': {'ambiguity_ind': 'Pairing ambiguity indicator.',
                                      'begin_datetime': 'Paired transfusion begin timestamp.',
                                      'begin_location': 'Begin workflow location.',
                                      'blood_product_group': 'Normalized blood product group.',
                                      'blood_unit_group': 'Blood group recorded on the unit.',
                                      'confidentiality_code': 'Security classification when supplied.',
                                      'elapsed_minutes': 'Elapsed transfusion minutes when calculable.',
                                      'encounter_id': 'Nullable Millennium ENCNTR_ID; BloodTrack transfusion '
                                                      'rows do not supply encounter context.',
                                      'end_datetime': 'Paired transfusion end timestamp.',
                                      'end_location': 'End workflow location.',
                                      'end_quantity_ml': 'END_QUANTITY_VALUE when the inferred unit is ML. '
                                                         '(bronze END_QUANTITY_ML, INSR W1). INSR_W2 W2.20.',
                                      'end_quantity_unit_inferred': 'BT_QTY_UNIT_V1 inferred unit of '
                                                                    'END_QUANTITY_VALUE. (bronze '
                                                                    'END_QUANTITY_UNIT_INFERRED, INSR W1). '
                                                                    'INSR_W2 W2.20.',
                                      'event_datetime': 'Transfusion begin timestamp falling back to end.',
                                      'event_datetime_local': 'event_datetime in Europe/London wall-clock '
                                                              'time (GMT/BST). Millennium rows take the '
                                                              'bronze *_LOCAL companions of the same source '
                                                              'columns and fallback order; rows from '
                                                              'local-clock sources (LUNA, PACS/DICOM, TFC '
                                                              'LIMS, CCMDS, MSDS, BadgerNet, EndoBase, SLAM, '
                                                              'iWeb, Datix and similar) carry event_datetime '
                                                              'unchanged. event_datetime is a UTC instant '
                                                              'for Millennium rows, so take local calendar '
                                                              'dates and clock times from this column.',
                                      'event_end_datetime': 'Transfusion end timestamp.',
                                      'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                  'wall-clock time (GMT/BST), by the same '
                                                                  'rule as event_datetime_local; null where '
                                                                  'event_end_datetime is null.',
                                      'isbt_mapping_method': 'EXACT_NAME, EMBEDDING_LLM_VERIFIED or '
                                                             'GROUP_RULE. (bronze ISBT_MAPPING_METHOD, INSR '
                                                             'W1). INSR_W2 W2.20.',
                                      'isbt_mapping_model': 'Verifier model of the ISBT mapping. (bronze '
                                                            'ISBT_MAPPING_MODEL, INSR W1). INSR_W2 W2.20.',
                                      'isbt_mapping_rule_id': 'Rule id of the ISBT mapping. (bronze '
                                                              'ISBT_MAPPING_RULE_ID, INSR W1). INSR_W2 '
                                                              'W2.20.',
                                      'isbt_mapping_score': 'Similarity score of the ISBT mapping. (bronze '
                                                            'ISBT_MAPPING_SCORE, INSR W1). INSR_W2 W2.20.',
                                      'isbt_product_concept_id': 'SNOMED blood-product concept for the ISBT '
                                                                 '128 PRODUCT_CODE. (bronze '
                                                                 'ISBT_PRODUCT_CONCEPT_ID, INSR W1). INSR_W2 '
                                                                 'W2.20.',
                                      'isbt_product_concept_name': 'Name of ISBT_PRODUCT_CONCEPT_ID. (bronze '
                                                                   'ISBT_PRODUCT_CONCEPT_NAME, INSR W1). '
                                                                   'INSR_W2 W2.20.',
                                      'loaded_at': 'Timestamp when the pipeline wrote this row to the bronze '
                                                   'table.',
                                      'patient_blood_group': 'Patient blood group at transfusion.',
                                      'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                           'cross-feed join key.',
                                      'patient_group_concept_id': 'Mapped patient blood-group concept '
                                                                  'identifier.',
                                      'patient_group_mapping_method': 'PATIENT_GROUP map provenance '
                                                                      '(method). (bronze '
                                                                      'PATIENT_GROUP_MAPPING_METHOD, INSR '
                                                                      'W1). INSR_W2 W2.20.',
                                      'patient_group_mapping_rule_id': 'PATIENT_GROUP map provenance '
                                                                       '(rule_id). (bronze '
                                                                       'PATIENT_GROUP_MAPPING_RULE_ID, INSR '
                                                                       'W1). INSR_W2 W2.20.',
                                      'person_id': 'Millennium PERSON_ID when resolved; native person '
                                                   'foreign key.',
                                      'product_concept_id': 'Mapped SNOMED device concept identifier.',
                                      'product_concept_name': 'Mapped product concept display.',
                                      'product_mapping_method': 'PRODUCT map provenance (method). (bronze '
                                                                'PRODUCT_MAPPING_METHOD, INSR W1). INSR_W2 '
                                                                'W2.20.',
                                      'product_mapping_rule_id': 'PRODUCT map provenance (rule_id). (bronze '
                                                                 'PRODUCT_MAPPING_RULE_ID, INSR W1). INSR_W2 '
                                                                 'W2.20.',
                                      'product_mapping_source': 'Map table the product concept came from. '
                                                                '(bronze PRODUCT_MAPPING_SOURCE, INSR W1). '
                                                                'INSR_W2 W2.20.',
                                      'quantity_raw': 'Verbatim quantity text.',
                                      'quantity_unit_rule': 'Rule id of the unit inference (BT_QTY_UNIT_V1). '
                                                            '(bronze QUANTITY_UNIT_RULE, INSR W1). INSR_W2 '
                                                            'W2.20.',
                                      'quantity_value': 'Parsed transfused quantity.',
                                      'record_status': 'Superseded when '
                                                       'map_bloodtrack_transfusion.IS_CURRENT_IN_SOURCE is '
                                                       'false, otherwise active; a missing flag defaults '
                                                       'true. No retracted status is emitted. '
                                                       'TRANSFUSION_STATUS and ambiguity indicators are '
                                                       'separate, while usable-code filtering controls '
                                                       'publication eligibility.',
                                      'record_status_effective_from': 'First non-null '
                                                                      'map_bloodtrack_transfusion.BEGIN_TS '
                                                                      "then END_TS, also used as this row's "
                                                                      'clinical event time. This is a '
                                                                      'begin/end-time proxy, not an '
                                                                      'independently observed status '
                                                                      'transition; no event-time clamp is '
                                                                      'applied.',
                                      'record_status_effective_to': 'map_bloodtrack_transfusion.PIPELINE_LOADED_AT '
                                                                    'when IS_CURRENT_IN_SOURCE is false; '
                                                                    'otherwise null. A missing processing '
                                                                    'timestamp remains null; neither '
                                                                    'BEGIN_TS nor END_TS is used as a '
                                                                    'fallback.',
                                      'requires_cmv_neg': 'Patient requires CMV-negative product.',
                                      'requires_irradiated': 'Patient requires irradiated product.',
                                      'source_code': 'ISBT product code with product display fallback.',
                                      'source_coding_system': 'BloodTrack product coding system.',
                                      'source_display': 'Blood product description.',
                                      'source_mrn_norm': 'Digits-only normalisation of SOURCE_PATIENT_NUMBER '
                                                         '(the BloodTrack MRN) used for MRN linkage and '
                                                         'TRANSFUSION_KEY. Identifier, published and tagged. '
                                                         'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                         'SOURCE_MRN_NORM, INSR W1). INSR_W2 W2.20.',
                                      'source_update_timestamp': 'Always null as a TIMESTAMP in the current '
                                                                 'transfusion projection. PIPELINE_LOADED_AT '
                                                                 'is published separately as loaded_at and '
                                                                 'is not copied into this field; no native '
                                                                 'update, clinical-event or Silver refresh '
                                                                 'timestamp is substituted.',
                                      'subject_id_system': 'Identifier system used for subject_key.',
                                      'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                     'identifier. Not salted, not secret; a stable join key '
                                                     'across feeds where person_id is unresolved.',
                                      'transfusion_key': 'BloodTrack TRANSFUSION_KEY; native primary key of '
                                                         'this table.',
                                      'transfusion_status': 'Pairing and clock-quality outcome.',
                                      'unit_group_concept_id': 'Mapped unit blood-group concept identifier.',
                                      'unit_group_mapping_method': 'UNIT_GROUP map provenance (method). '
                                                                   '(bronze UNIT_GROUP_MAPPING_METHOD, INSR '
                                                                   'W1). INSR_W2 W2.20.',
                                      'unit_group_mapping_rule_id': 'UNIT_GROUP map provenance (rule_id). '
                                                                    '(bronze UNIT_GROUP_MAPPING_RULE_ID, '
                                                                    'INSR W1). INSR_W2 W2.20.',
                                      'unit_is_cmv_neg': 'Unit CMV-negative flag.',
                                      'unit_is_irradiated': 'Unit irradiation flag.',
                                      'unit_number': 'Blood unit number.',
                                      'vip_ind': 'VIP indicator when supplied.',
                                      'withheld_identity_ind': 'Identity-withholding status is not populated '
                                                               'by the current source projections; null does '
                                                               'not mean identity was not withheld.'},
                          'table': 'One paired BloodTrack unit-recipient transfusion episode, retaining '
                                   'available timing and source-event linkage.'}}

# INSR_R4_SILVER_V1 (module_list): generated by r4/release/gen_silver_components.py; edit the generator, not this.
TRANSFUSION_PUBLIC_COLUMNS = list(TRANSFUSION_PUBLIC_COLUMNS) + ['identity_status']  # decision 1: contract columns, additions last
