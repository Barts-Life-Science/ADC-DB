"""Silver clinical.family_history: 3 dataset(s).

- datasets: _clinical_qc_family_history, clinical_family_history, _clinical_family_history_metadata
- moved verbatim from: 2_silver_clinical_events:126, 2_silver_clinical_events:148, 2_silver_clinical_events:184, 2_silver_clinical_events:227, 2_silver_clinical_events:254, 2_silver_clinical_events:8290, 2_silver_clinical_events:8292

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n, spark
from silver_tables._common.journey import (
    S3_TABLE_AXIS_SPECS,
    _family_history_canonical,
    _s3_replace_public_variant,
)
from silver_tables.clinical._shared import _s3_component_metadata


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical /  qc family history
@materialized_view(
    name=_n("journey_clinical._qc_family_history"),
    refresh_policy="auto",
)
def _qc_family_history():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # family_history.
    return _cross_qc_primitive(
        _family_history_canonical(),
        "family_history",
        {"condition_code": "_qc_condition_code_json"},
    )


# Clinical / family history

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
FAMILY_HISTORY_PUBLIC_COLUMNS = [
    'patient_event_key',
    'fhx_activity_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'event_datetime_local',
    'event_end_datetime_local',
    # TZ_SILVER_LOCAL_V1: Europe/London companions (bronze *_LOCAL for Millennium; local-clock sources unchanged)
    'source_coding_system',
    'source_code',
    'source_display',
    'condition_code',
    'relationship_code',
    'relationship_display',
    'relationship_type_code',
    'relationship_type_display',
    'onset_age',
    'onset_age_unit',
    'severity_code',
    'severity_display',
    'source_lifecycle_status',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'asserter_practitioner_id',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]


FAMILY_HISTORY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'fhx_activity_id',
    'identity_status',
    'event_after_death_30d',
    'load_batch_id',
]


@materialized_view(
    name=_n("journey_clinical.family_history"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def family_history():
    # Build the declared dataset: One family-history assertion with the Journey Model standard
    # event block and CodeableConcept mappings.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_family_history")),
        "family_history",
        {"condition_code": "_qc_condition_code_json"},
        FAMILY_HISTORY_PUBLIC_COLUMNS,
        FAMILY_HISTORY_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._family_history_metadata'),
    refresh_policy="auto",
)
def family_history_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_family_history; same row grain as the research table. Join keys:
    # patient_event_key, fhx_activity_id.
    return _s3_component_metadata("journey_clinical.family_history", "journey_clinical._qc_family_history", FAMILY_HISTORY_LIFECYCLE_COLUMNS)


# Finalise the public column lists used by this notebook.
if "source_patient_event_key" not in FAMILY_HISTORY_PUBLIC_COLUMNS:
    FAMILY_HISTORY_PUBLIC_COLUMNS.insert(FAMILY_HISTORY_PUBLIC_COLUMNS.index("patient_event_key") + 1, "source_patient_event_key")


for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_family_history"].items():
    _s3_replace_public_variant(FAMILY_HISTORY_PUBLIC_COLUMNS, old_name, axes)


CROSS_QC = {
    'family_history': CrossQc(model='family_history', dates=('event_after_death_30d',), coded_axes={'condition_code': ('condition', False)}, promote_lifecycle=['identity_status']),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {
    'clinical_family_history': ValuePolicy(table='clinical_family_history', enabled=True, zero_as_null=('encounter_id',), zero_value_as_null=('onset_age',)),
}


WRAP_POLICY = {'clinical_family_history'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_family_history_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
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
                                                'clinical_family_history; same row grain as the research '
                                                'table. Join keys: patient_event_key, fhx_activity_id.'},
 '_clinical_qc_family_history': {'columns': {},
                                 'table': 'Private JSON bridge and Gold cross-rule flags for '
                                          'family_history.'},
 'clinical_family_history': {'columns': {'asserter_practitioner_id': 'Millennium personnel PERSON_ID when '
                                                                     'supplied.',
                                         'condition_map_candidate_count': 'Number of candidate targets '
                                                                          'observed; a mapped parent is '
                                                                          'unambiguous when this is 1.',
                                         'condition_map_candidate_set_id': 'Stable identifier of the '
                                                                           'retained candidate set.',
                                         'condition_map_competing_count': 'Candidates within 0.05 cosine of '
                                                                          'the selected candidate.',
                                         'condition_map_component_count': 'Number of source-derived '
                                                                          'components.',
                                         'condition_map_component_index': 'One-based source-derived '
                                                                          'component index.',
                                         'condition_map_confidence': 'Mapping confidence in [0,1] where the '
                                                                     'lane measured one.',
                                         'condition_map_cosine': 'Measured cosine between the normalised '
                                                                 'source and selected target; null when '
                                                                 'scoring did not run.',
                                         'condition_map_method': 'Mapping method: source_native, rule, '
                                                                 'exact_synonym or embedding_proposed.',
                                         'condition_map_rollup_levels': 'Maximum hierarchy distance to a '
                                                                        'selected common ancestor.',
                                         'condition_map_rule_id': 'Stable rule or lane identifier that '
                                                                  'produced the mapping.',
                                         'condition_map_scoring_model': 'Embedding model that produced '
                                                                        'map_cosine.',
                                         'condition_map_status': 'Explicit mapping outcome from the S3b '
                                                                 'status vocabulary.',
                                         'condition_map_status_reason': 'Reason for the mapping outcome or '
                                                                        'abstention.',
                                         'condition_map_version': 'Version of the source, terminology '
                                                                  'release or model used by the mapping.',
                                         'condition_omop_concept_id': 'OMOP concept identifier selected with '
                                                                      'the family-history condition mapping.',
                                         'condition_omop_source_concept_id': 'Non-standard OMOP source '
                                                                             'concept identifier, kept '
                                                                             'separate from a standard '
                                                                             'target.',
                                         'condition_snomed_code': 'SNOMED CT code selected for the '
                                                                  'family-history condition; null when no '
                                                                  'unambiguous mapping is available.',
                                         'condition_snomed_display': 'SNOMED CT preferred display selected '
                                                                     'for the family-history condition.',
                                         'condition_source_code': 'Source code for the family-history '
                                                                  'condition, retained without '
                                                                  'de-identification.',
                                         'condition_source_display': 'Source display for the family-history '
                                                                     'condition.',
                                         'condition_source_system': 'Canonical coding-system URI for the '
                                                                    'source family-history condition.',
                                         'condition_target_code': 'Code of the selected target in its own '
                                                                  'vocabulary.',
                                         'condition_target_display': 'Display of the selected target in its '
                                                                     'own vocabulary.',
                                         'condition_target_system': 'Coding-system URI of the selected '
                                                                    'target.',
                                         'confidentiality_code': 'Security classification when supplied.',
                                         'encounter_id': 'Millennium ORIGINATING_ENCNTR_ID; joins to '
                                                         'spine_encounter.encounter_id.',
                                         'event_datetime': 'Source family-history effective start.',
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
                                         'event_end_datetime': 'Source effective end when the assertion is '
                                                               'no longer current.',
                                         'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                     'wall-clock time (GMT/BST), by the same '
                                                                     'rule as event_datetime_local; null '
                                                                     'where event_end_datetime is null.',
                                         'fhx_activity_id': 'Millennium FHX_ACTIVITY_ID; primary key of this '
                                                            'table.',
                                         'loaded_at': 'map_family_history.ADC_UPDT carried unchanged through '
                                                      'usable-code filtering and QC/public projections, '
                                                      'identical to source_update_timestamp. QC '
                                                      'identity/encounter joins add no load clocks; neither '
                                                      'clinical event time nor the current Silver refresh '
                                                      'time replaces it.',
                                         'onset_age': 'Source recorded age at onset.',
                                         'onset_age_unit': 'Unit for recorded onset age.',
                                         'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                              'cross-feed join key.',
                                         'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                         'record_status': 'Superseded when '
                                                          'map_family_history.END_EFFECTIVE_DT_TM is '
                                                          'non-null and earlier than 2100-01-01; otherwise '
                                                          'active. No retracted status is emitted. The '
                                                          'separately retained LIFE_CYCLE_STATUS does not '
                                                          'determine this derived label.',
                                         'record_status_effective_from': 'Source-supplied timestamp from '
                                                                         'which the normalized record status '
                                                                         'applies for each family history '
                                                                         'record. It is derived from bronze '
                                                                         'field `FHX_ACTIVE_STATUS_DT_TM` in '
                                                                         '`4_prod.bronze.map_family_history`. '
                                                                         'Source precision and timezone '
                                                                         'handling follow the pipeline '
                                                                         'expression; null means the time '
                                                                         'was unavailable.',
                                         'record_status_effective_to': 'map_family_history.END_EFFECTIVE_DT_TM '
                                                                       'only when non-null and earlier than '
                                                                       '2100-01-01; otherwise null. This is '
                                                                       'the same predicate that sets '
                                                                       'superseded status. No 1950 lower '
                                                                       'clamp, event-end substitution or '
                                                                       'ingestion-time fallback is applied.',
                                         'relationship_code': 'Source family relationship code.',
                                         'relationship_display': 'Source family relationship display.',
                                         'relationship_type_code': 'Source relationship record type code.',
                                         'relationship_type_display': 'Source relationship record type '
                                                                      'display.',
                                         'severity_code': 'Source severity code.',
                                         'severity_display': 'Source severity display.',
                                         'source_code': 'Source condition code',
                                         'source_coding_system': 'Verbatim source code system.',
                                         'source_display': 'Verbatim source condition display.',
                                         'source_lifecycle_status': 'Verbatim source life-cycle status.',
                                         'source_update_timestamp': 'map_family_history.ADC_UPDT carried '
                                                                    'unchanged, identical to loaded_at. No '
                                                                    'independent native application-update '
                                                                    'clock is projected for this field, and '
                                                                    'the current Silver refresh time is not '
                                                                    'substituted.',
                                         'subject_id_system': 'Identifier system used for subject_key.',
                                         'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                        'identifier. Not salted, not secret; a stable join '
                                                        'key across feeds where person_id is unresolved.',
                                         'vip_ind': 'VIP indicator when supplied.',
                                         'withheld_identity_ind': 'Withheld-identity indicator when '
                                                                  'supplied.'},
                             'table': 'One family-history assertion with the Journey Model standard event '
                                      'block and CodeableConcept mappings.'}}
