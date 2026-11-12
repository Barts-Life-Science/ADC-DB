"""Silver clinical.allergy_intolerance: 3 dataset(s).

- datasets: _clinical_qc_allergy_intolerance, clinical_allergy_intolerance, _clinical_allergy_intolerance_metadata
- moved verbatim from: 2_silver_clinical_events:6976, 2_silver_clinical_events:6998, 2_silver_clinical_events:7037, 2_silver_clinical_events:7087, 2_silver_clinical_events:7105, 2_silver_clinical_events:8341

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _decode_display, _n, spark
from silver_tables._common.journey import S3_TABLE_AXIS_SPECS, _allergy_canonical, _s3_replace_public_variant


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical /  qc allergy intolerance
@materialized_view(
    name=_n("journey_clinical._qc_allergy_intolerance"),
    refresh_policy="auto",
)
def _qc_allergy_intolerance():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # allergy_intolerance.
    return _cross_qc_primitive(
        _allergy_canonical(),
        "allergy_intolerance",
        {"substance_code": "_qc_substance_code_json"},
    )


# Clinical / allergy intolerance

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
ALLERGY_PUBLIC_COLUMNS = [
    'patient_event_key',
    'allergy_instance_id',
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
    'substance_code',
    'substance_type_code',
    'substance_type_display',
    'reaction_class_code',
    'reaction_class_display',
    'reaction_status_code',
    'reaction_status_display',
    'severity_code',
    'severity_display',
    'absence_assertion_ind',
    'onset_precision_display',
    'source_of_info_display',
    'verified_status_flag',
    'reviewed_datetime',
    'cancel_reason_display',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]


ALLERGY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'allergy_instance_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


@materialized_view(
    name=_n("journey_clinical.allergy_intolerance"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def allergy_intolerance():
    # Build the declared dataset: One Millennium allergy or intolerance assertion, with recorded
    # substance, reaction and source-history information.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_allergy_intolerance")),
        "allergy_intolerance",
        {"substance_code": "_qc_substance_code_json"},
        ALLERGY_PUBLIC_COLUMNS,
        ALLERGY_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._allergy_intolerance_metadata'),
    refresh_policy="auto",
)
def allergy_intolerance_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_allergy_intolerance; same row grain as the research table. Join keys:
    # patient_event_key, allergy_instance_id.
    return (spark.read.table(_n("journey_clinical._qc_allergy_intolerance"))).select(*ALLERGY_LIFECYCLE_COLUMNS)


for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_allergy_intolerance"].items():
    _s3_replace_public_variant(ALLERGY_PUBLIC_COLUMNS, old_name, axes)


def _finish_allergy_intolerance(df):
    """allergy_intolerance: per-row finish step, moved verbatim from _cross_qc_finish (silver_journey_shared.py:1277)."""
    # INSR_W2 W2.04 (L2411): severity code set 12022 decoded where bronze carried no SEVERITY_DESC.
    df = _decode_display(df, "severity_code", "severity_display", 12022)
    return df


CROSS_QC = {
    'allergy_intolerance': CrossQc(model='allergy_intolerance', fk=('encounter_id', 'person_id'), dates=('event_after_death_30d', 'event_before_birth'), coded_axes={'substance_code': ('substance', False)}, extra_public_columns=('substance_omop_map_rule_id', 'substance_omop_map_version'), promote_lifecycle=['identity_status', 'person_id_resolved', 'encounter_id_resolved'], finish_steps=(_finish_allergy_intolerance,)),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {
    'clinical_allergy_intolerance': ValuePolicy(table='clinical_allergy_intolerance', enabled=True, zero_as_null=('severity_code',)),
}


WRAP_POLICY = {'clinical_allergy_intolerance'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_allergy_intolerance_metadata': {'columns': {'activity_date': 'Primary-key component carried '
                                                                         'unchanged from the public parent; '
                                                                         "combine it with the companion's "
                                                                         'other key columns when joining.',
                                                        'activity_record_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
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
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                        'birth_row_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                        'bloodtrack_transaction_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                        'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                        'care_contact_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                        'care_participation_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                        'cds_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                        'chemotherapy_course_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                        'community_care_activity_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                        'community_care_contact_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                        'concept_map_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                        'encounter_attribute_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                        'encounter_bounds_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
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
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                        'encounter_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                        'endobase_exam_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                        'endobase_exam_term_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                        'entity_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
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
                                                                              'precedes the resolved '
                                                                              "person's birth timestamp; "
                                                                              'silver records the doubt and '
                                                                              'does not filter the row.',
                                                        'event_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                        'extract_cd': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                        'fhx_activity_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                                        'sequence; retained for the gold '
                                                                        'drop rule.',
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
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                        'labour_delivery_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
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
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                        'mc_device_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                        'microbiology_isolate_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                        'order_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                        'organization_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                        'patient_event_key': 'Deterministic SHA-256 event '
                                                                             'key carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that event '
                                                                             'row.',
                                                        'patient_event_row_key': 'Deterministic row-level '
                                                                                 'key carried unchanged from '
                                                                                 'events_patient_event; '
                                                                                 'joins one lifecycle row to '
                                                                                 'one event-index row.',
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
                                                        'person_attribute_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                        'person_death_evidence_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                        'person_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                        'person_id_resolved': 'True when person_id is null '
                                                                              'or resolves to the governed '
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
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                        'pharmacy_issue_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                        'pm_wait_list_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                        'practitioner_group_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                        'practitioner_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                        'practitioner_identifier_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                        'practitioner_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                        'practitioner_location_evidence_key': 'Deterministic '
                                                                                              'v2 key '
                                                                                              'carried '
                                                                                              'unchanged '
                                                                                              'from the '
                                                                                              'public '
                                                                                              'parent; joins '
                                                                                              'one lifecycle '
                                                                                              'row to that '
                                                                                              'research row.',
                                                        'pregnancy_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                        'pregnancy_reconciliation_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
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
                                                        'record_status_effective_from': 'Timestamp from '
                                                                                        'which the '
                                                                                        'normalised record '
                                                                                        'status applies, '
                                                                                        'when the source '
                                                                                        'supplies one.',
                                                        'record_status_effective_to': 'Timestamp until which '
                                                                                      'the normalised record '
                                                                                      'status applies; null '
                                                                                      'means no source end '
                                                                                      'is known.',
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
                                                        'row_source': 'Physical bronze row source, CURRENT '
                                                                      'or HIST, carried unchanged from the '
                                                                      'public parent; a key component '
                                                                      'because the research table is at '
                                                                      'physical version grain and '
                                                                      'patient_event_key is shared across an '
                                                                      "entry's versions.",
                                                        'rtt_activity_oid': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                        'source_system_oid': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                        'source_unit': 'Primary-key component carried '
                                                                       'unchanged from the public parent; '
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                        'source_update_timestamp': 'Native source-system '
                                                                                   'update timestamp carried '
                                                                                   'through the canonical '
                                                                                   'silver query.',
                                                        'source_version_id': 'Native source version '
                                                                             'identifier carried unchanged '
                                                                             'from the public parent; '
                                                                             'CURRENT rows carry -1. A key '
                                                                             'component alongside '
                                                                             'row_source.',
                                                        'surg_case_proc_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                        'susceptibility_result_id': 'Native source '
                                                                                    'identifier carried '
                                                                                    'unchanged from the '
                                                                                    'public parent; joins '
                                                                                    'one lifecycle row to '
                                                                                    'that research row.',
                                                        'theatre_attendance_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                        'theatre_case_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                        'theatre_case_milestone_key': 'Deterministic v2 key '
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                        'treatment_cycle_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                        'treatment_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                        'update_count': 'Primary-key component carried '
                                                                        'unchanged from the public parent; '
                                                                        "combine it with the companion's "
                                                                        'other key columns when joining.',
                                                        'valid_from_datetime': 'Primary-key component '
                                                                               'carried unchanged from the '
                                                                               'public parent; combine it '
                                                                               "with the companion's other "
                                                                               'key columns when joining.',
                                                        'value_set_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                        'waiting_list_oid': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                        'waiting_list_snapshot_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.'},
                                            'table': 'Internal quality and batch metadata for '
                                                     'clinical_allergy_intolerance; same row grain as the '
                                                     'research table. Join keys: patient_event_key, '
                                                     'allergy_instance_id.'},
 '_clinical_qc_allergy_intolerance': {'columns': {},
                                      'table': 'Private JSON bridge and Gold cross-rule flags for '
                                               'allergy_intolerance.'},
 'clinical_allergy_intolerance': {'columns': {'absence_assertion_ind': 'True when the row asserts ABSENCE of '
                                                                       'allergy (e.g. no known allergies) '
                                                                       'rather than a positive assertion.',
                                              'allergy_instance_id': 'Source-system identifier for the '
                                                                     'allergy instance associated with each '
                                                                     'allergy intolerance record. It is '
                                                                     'carried from bronze field '
                                                                     '`ALLERGY_INSTANCE_ID` in '
                                                                     '`4_prod.bronze.map_allergy`. Its '
                                                                     'uniqueness is limited to the '
                                                                     'originating source namespace; null '
                                                                     'means no identifier was supplied.',
                                              'cancel_reason_display': 'Cancel reason when cancelled.',
                                              'confidentiality_code': 'Security classification when '
                                                                      'supplied.',
                                              'encounter_id': 'Recording encounter reference when supplied.',
                                              'event_datetime': 'Allergy onset timestamp from '
                                                                '`ONSET_DT_TM_CLEAN`, falling back to '
                                                                '`CREATED_DT_TM_CLEAN` when onset is absent; '
                                                                'null when both cleaned timestamps are '
                                                                'absent.',
                                              'event_datetime_local': 'event_datetime in Europe/London '
                                                                      'wall-clock time (GMT/BST). Millennium '
                                                                      'rows take the bronze *_LOCAL '
                                                                      'companions of the same source columns '
                                                                      'and fallback order; rows from '
                                                                      'local-clock sources (LUNA, '
                                                                      'PACS/DICOM, TFC LIMS, CCMDS, MSDS, '
                                                                      'BadgerNet, EndoBase, SLAM, iWeb, '
                                                                      'Datix and similar) carry '
                                                                      'event_datetime unchanged. '
                                                                      'event_datetime is a UTC instant for '
                                                                      'Millennium rows, so take local '
                                                                      'calendar dates and clock times from '
                                                                      'this column.',
                                              'event_end_datetime': 'Reaction-status end when resolved or '
                                                                    'cancelled.',
                                              'event_end_datetime_local': 'event_end_datetime in '
                                                                          'Europe/London wall-clock time '
                                                                          '(GMT/BST), by the same rule as '
                                                                          'event_datetime_local; null where '
                                                                          'event_end_datetime is null.',
                                              'loaded_at': 'map_allergy.ADC_UPDT carried unchanged through '
                                                           'usable-code filtering and quality/public '
                                                           'projections. This is the contributing bronze '
                                                           'load clock, distinct from PIPELINE_UPDT_DT_TM '
                                                           'published as source_update_timestamp; it is not '
                                                           'allergy onset, status-change time or Silver '
                                                           'refresh time.',
                                              'onset_precision_display': 'Source onset precision.',
                                              'patient_event_key': 'Stable silver-v2 key identifying the '
                                                                   'patient event associated with each '
                                                                   'allergy intolerance record. It is '
                                                                   'derived from bronze field '
                                                                   '`ALLERGY_INSTANCE_ID` in '
                                                                   '`4_prod.bronze.map_allergy`. This is a '
                                                                   'contract identifier rather than a '
                                                                   'display value; null means the source '
                                                                   'identifiers were insufficient to derive '
                                                                   'the key.',
                                              'person_id': 'Resolved Millennium person identifier when '
                                                           'available.',
                                              'reaction_class_code': 'Source reaction class code.',
                                              'reaction_class_display': 'Reaction class '
                                                                        '(Allergy/Intolerance/Side '
                                                                        'Effect/...).',
                                              'reaction_status_code': 'Source reaction status code.',
                                              'reaction_status_display': 'Reaction status '
                                                                         '(Active/Cancelled/Resolved/Proposed).',
                                              'record_status': 'Retracted when '
                                                               "upper(coalesce(REACTION_STATUS_DESC,'')) "
                                                               'equals CANCELLED, without trimming; '
                                                               'otherwise superseded when '
                                                               'END_EFFECTIVE_DT_TM is before 2100-01-01 or '
                                                               'ACTIVE_IND cast to LONG equals zero; '
                                                               'otherwise active. A null end defaults to '
                                                               'not-ended and a null activity flag defaults '
                                                               'to 1. Cancellation takes priority, and other '
                                                               'nonzero activity values do not trigger '
                                                               'supersession.',
                                              'record_status_effective_from': 'First non-null '
                                                                              'map_allergy.REACTION_STATUS_DT_TM_CLEAN, '
                                                                              'ACTIVE_STATUS_DT_TM, then '
                                                                              'BEG_EFFECTIVE_DT_TM. This '
                                                                              'source-status/validity '
                                                                              'timestamp is distinct from '
                                                                              'the onset/created fallbacks '
                                                                              'used for event_datetime; null '
                                                                              'when all three are missing.',
                                              'record_status_effective_to': 'CANCEL_DT_TM_CLEAN for '
                                                                            'cancelled allergy rows, even if '
                                                                            'null; otherwise '
                                                                            'END_EFFECTIVE_DT_TM for '
                                                                            'ended/inactive rows; otherwise '
                                                                            'null. There is no fallback '
                                                                            'between those branches. '
                                                                            'Supersession caused by '
                                                                            'ACTIVE_IND=0 can retain a '
                                                                            'far-future end value because '
                                                                            'the selected end is not '
                                                                            'range-clamped.',
                                              'reviewed_datetime': 'Sentinel-cleaned last review timestamp.',
                                              'severity_code': 'Source severity code.',
                                              'severity_display': 'Source allergy severity description '
                                                                  'carried from `SEVERITY_DESC`, for example '
                                                                  'mild, moderate or severe.',
                                              'source_code': 'Substance code (SNOMED then source identifier '
                                                             'then nomenclature id)',
                                              'source_coding_system': 'Verbatim substance code system.',
                                              'source_display': 'Verbatim substance display text.',
                                              'source_of_info_display': 'Allergy information-source '
                                                                        'description from '
                                                                        '`SOURCE_OF_INFO_DESC`, falling back '
                                                                        'to the recorded free text in '
                                                                        '`SOURCE_OF_INFO_FT` when the '
                                                                        'description is null.',
                                              'source_update_timestamp': 'Bronze pipeline processing '
                                                                         'timestamp carried from '
                                                                         '`PIPELINE_UPDT_DT_TM` in '
                                                                         '`4_prod.bronze.map_allergy`. It '
                                                                         'records when bronze processed the '
                                                                         'source row, not a native '
                                                                         'clinical-system update timestamp, '
                                                                         'clinical event time, or the '
                                                                         'current Silver refresh time.',
                                              'subject_id_system': 'Identifier system used for subject_key.',
                                              'subject_key': 'Always-populated deterministic subject key.',
                                              'substance_map_candidate_count': 'Number of candidate targets '
                                                                               'observed; a mapped parent is '
                                                                               'unambiguous when this is 1.',
                                              'substance_map_candidate_set_id': 'Stable identifier of the '
                                                                                'retained candidate set.',
                                              'substance_map_competing_count': 'Candidates within 0.05 '
                                                                               'cosine of the selected '
                                                                               'candidate.',
                                              'substance_map_component_count': 'Number of source-derived '
                                                                               'components.',
                                              'substance_map_component_index': 'One-based source-derived '
                                                                               'component index.',
                                              'substance_map_confidence': 'Mapping confidence in [0,1] where '
                                                                          'the lane measured one.',
                                              'substance_map_cosine': 'Measured cosine between the '
                                                                      'normalised source and selected '
                                                                      'target; null when scoring did not '
                                                                      'run.',
                                              'substance_map_method': 'Mapping method: source_native, rule, '
                                                                      'exact_synonym or embedding_proposed.',
                                              'substance_map_rollup_levels': 'Maximum hierarchy distance to '
                                                                             'a selected common ancestor.',
                                              'substance_map_rule_id': 'Stable rule or lane identifier that '
                                                                       'produced the mapping.',
                                              'substance_map_scoring_model': 'Embedding model that produced '
                                                                             'map_cosine.',
                                              'substance_map_status': 'Explicit mapping outcome from the S3b '
                                                                      'status vocabulary.',
                                              'substance_map_status_reason': 'Reason for the mapping outcome '
                                                                             'or abstention.',
                                              'substance_map_version': 'Version of the source, terminology '
                                                                       'release or model used by the '
                                                                       'mapping.',
                                              'substance_omop_concept_id': 'OMOP concept identifier selected '
                                                                           'with the allergy substance '
                                                                           'mapping.',
                                              'substance_omop_map_rule_id': 'Rule that filled '
                                                                            'substance_omop_concept_id / '
                                                                            'substance_omop_source_concept_id '
                                                                            "from the OMOP 'Maps to' lookup "
                                                                            '(omop:maps_to:<vocabulary>); '
                                                                            'NULL when those ids came with '
                                                                            "the row's own mapping. INSR_W2 "
                                                                            'W2.03.',
                                              'substance_omop_map_version': 'Vocabulary release '
                                                                            '(<vocabulary>|<version>) of the '
                                                                            'lookup that filled the '
                                                                            'substance OMOP ids; NULL when '
                                                                            'the lane filled nothing. '
                                                                            'INSR_W2 W2.03.',
                                              'substance_omop_source_concept_id': 'Non-standard OMOP source '
                                                                                  'concept identifier, kept '
                                                                                  'separate from a standard '
                                                                                  'target.',
                                              'substance_snomed_code': 'SNOMED CT code selected for the '
                                                                       'allergy substance; null when no '
                                                                       'unambiguous mapping is available.',
                                              'substance_snomed_display': 'SNOMED CT preferred display '
                                                                          'selected for the allergy '
                                                                          'substance.',
                                              'substance_source_code': 'Source code for the allergy '
                                                                       'substance, retained without '
                                                                       'de-identification.',
                                              'substance_source_display': 'Source display for the allergy '
                                                                          'substance.',
                                              'substance_source_system': 'Canonical coding-system URI for '
                                                                         'the source allergy substance.',
                                              'substance_target_code': 'Code of the selected target in its '
                                                                       'own vocabulary.',
                                              'substance_target_display': 'Display of the selected target in '
                                                                          'its own vocabulary.',
                                              'substance_target_system': 'Coding-system URI of the selected '
                                                                         'target.',
                                              'substance_type_code': 'Source substance type code.',
                                              'substance_type_display': 'Substance type '
                                                                        '(Drug/Food/Environment/...).',
                                              'verified_status_flag': 'Pharmacy-verified flag verbatim.',
                                              'vip_ind': 'VIP indicator when supplied.',
                                              'withheld_identity_ind': 'Withheld-identity indicator when '
                                                                       'supplied.'},
                                  'table': 'One Millennium allergy or intolerance assertion, with recorded '
                                           'substance, reaction and source-history information.'}}
