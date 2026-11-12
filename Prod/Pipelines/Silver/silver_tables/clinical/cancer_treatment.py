"""Silver clinical.cancer_treatment: 3 dataset(s).

- datasets: _clinical_qc_cancer_treatment, clinical_cancer_treatment, _clinical_cancer_treatment_metadata
- moved verbatim from: 2_silver_clinical_events:7482, 2_silver_clinical_events:7504, 2_silver_clinical_events:7549, 2_silver_clinical_events:7603, 2_silver_clinical_events:7621, 2_silver_clinical_events:8344

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n, spark
from silver_tables._common.journey import (
    S3_TABLE_AXIS_SPECS,
    _cancer_treatment_canonical,
    _s3_replace_public_variant,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical /  qc cancer treatment
@materialized_view(
    name=_n("journey_clinical._qc_cancer_treatment"),
    refresh_policy="auto",
)
def _qc_cancer_treatment():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # cancer_treatment.
    return _cross_qc_primitive(
        _cancer_treatment_canonical(),
        "cancer_treatment",
        {"drug_code": "_qc_drug_code_json"},
    )


# Clinical / cancer treatment

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
CANCER_TREATMENT_PUBLIC_COLUMNS = [
    'patient_event_key',
    'treatment_key',
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
    'drug_code',
    'treatment_plan',
    'regimen_name',
    'indication',
    'record_type',
    'dose_value',
    'dose_total',
    'dose_unit_code',
    'route_code',
    'start_date',
    'end_date',
    'final_treatment_date',
    'course_finished',
    'planned_cycles',
    'default_cycles',
    'chemo_radiation',
    'procurement_opcs_code',
    'delivery_opcs_code',
    'drug_similarity',
    'iqemo_course_id',
    'aria_rx_key',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]


CANCER_TREATMENT_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'treatment_key',
    'identity_status',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


@materialized_view(
    name=_n("journey_clinical.cancer_treatment"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def cancer_treatment():
    # Build the declared dataset: One SACT constituent-drug treatment fact, separate from
    # Millennium medication administration.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_cancer_treatment")),
        "cancer_treatment",
        {"drug_code": "_qc_drug_code_json"},
        CANCER_TREATMENT_PUBLIC_COLUMNS,
        CANCER_TREATMENT_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._cancer_treatment_metadata'),
    refresh_policy="auto",
)
def cancer_treatment_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_cancer_treatment; same row grain as the research table. Join keys:
    # patient_event_key, treatment_key.
    return (spark.read.table(_n("journey_clinical._qc_cancer_treatment"))).select(*CANCER_TREATMENT_LIFECYCLE_COLUMNS)


for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_cancer_treatment"].items():
    _s3_replace_public_variant(CANCER_TREATMENT_PUBLIC_COLUMNS, old_name, axes)


CROSS_QC = {
    'cancer_treatment': CrossQc(model='cancer_treatment', dates=('event_after_death_30d', 'event_before_birth'), coded_axes={'drug_code': ('drug', False)}, promote_lifecycle=['identity_status']),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_cancer_treatment_metadata': {'columns': {'activity_date': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                     'activity_record_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                     'birth_row_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                     'bloodtrack_transaction_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                     'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
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
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                     'community_care_activity_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                     'community_care_contact_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
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
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
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
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
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
                                                     'entry_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                     'episode_encounter_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                     'episode_encounter_reltn_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                     'episode_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                     'episode_id_resolved': 'True when episode_id is null or '
                                                                            'resolves to the governed '
                                                                            'episode spine; false records '
                                                                            'unresolved linkage without '
                                                                            'dropping the row.',
                                                     'episode_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                                     'event_after_death_30d': 'True when the event timestamp '
                                                                              'is more than 30 days after '
                                                                              "the resolved person's death "
                                                                              'timestamp; silver records the '
                                                                              'doubt and does not filter the '
                                                                              'row.',
                                                     'event_before_birth': 'True when the event timestamp '
                                                                           "precedes the resolved person's "
                                                                           'birth timestamp; silver records '
                                                                           'the doubt and does not filter '
                                                                           'the row.',
                                                     'event_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
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
                                                     'labour_delivery_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                     'load_batch_id': 'Deterministic batch token derived '
                                                                      "from the source row's bronze load "
                                                                      'timestamp.',
                                                     'loaded_at': 'Latest bronze load timestamp contributing '
                                                                  'to the public row; never the lifecycle '
                                                                  "pipeline's wall-clock time.",
                                                     'location_code': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                     'location_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                     'location_stay_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                     'mc_device_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                     'microbiology_isolate_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                     'order_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                     'patient_event_key': 'Deterministic SHA-256 event key '
                                                                          'carried unchanged from the public '
                                                                          'parent; joins one lifecycle row '
                                                                          'to that event row.',
                                                     'patient_event_row_key': 'Deterministic row-level key '
                                                                              'carried unchanged from '
                                                                              'events_patient_event; joins '
                                                                              'one lifecycle row to one '
                                                                              'event-index row.',
                                                     'period_business_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
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
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
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
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                     'practitioner_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                     'practitioner_location_evidence_key': 'Deterministic v2 '
                                                                                           'key carried '
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
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
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
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
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
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                     'source_daily_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                     'source_identifier_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                     'source_object': 'Source-arm discriminator carried '
                                                                      'unchanged from the public parent; use '
                                                                      'it with arm-specific native '
                                                                      'identifiers when joining.',
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
                                                                                'timestamp carried through '
                                                                                'the canonical silver query.',
                                                     'source_version_id': 'Native source version identifier '
                                                                          'carried unchanged from the public '
                                                                          'parent; CURRENT rows carry -1. A '
                                                                          'key component alongside '
                                                                          'row_source.',
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
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                     'theatre_implant_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                     'transfusion_event_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                     'transfusion_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                     'treatment_cycle_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                                  'one lifecycle row to that '
                                                                                  'research row.'},
                                         'table': 'Internal quality and batch metadata for '
                                                  'clinical_cancer_treatment; same row grain as the research '
                                                  'table. Join keys: patient_event_key, treatment_key.'},
 '_clinical_qc_cancer_treatment': {'columns': {},
                                   'table': 'Private JSON bridge and Gold cross-rule flags for '
                                            'cancer_treatment.'},
 'clinical_cancer_treatment': {'columns': {'aria_rx_key': 'ARIA prescription key.',
                                           'chemo_radiation': 'Concurrent chemo-radiation indicator.',
                                           'confidentiality_code': 'Security classification when supplied.',
                                           'course_finished': 'Source indication that the treatment course '
                                                              'has finished, from `CourseFinished` cast to '
                                                              'boolean.',
                                           'default_cycles': 'Default regimen cycle count.',
                                           'delivery_opcs_code': 'OPCS delivery code.',
                                           'dose_total': 'Total planned or delivered dose.',
                                           'dose_unit_code': 'ARIA coded value for the dosage unit used for '
                                                             'administration of the agent.',
                                           'dose_value': 'Constituent dose value.',
                                           'drug_map_candidate_count': 'Number of candidate targets '
                                                                       'observed; a mapped parent is '
                                                                       'unambiguous when this is 1.',
                                           'drug_map_candidate_set_id': 'Stable identifier of the retained '
                                                                        'candidate set.',
                                           'drug_map_competing_count': 'Candidates within 0.05 cosine of the '
                                                                       'selected candidate.',
                                           'drug_map_component_count': 'Number of source-derived components.',
                                           'drug_map_component_index': 'One-based source-derived component '
                                                                       'index.',
                                           'drug_map_confidence': 'Mapping confidence in [0,1] where the '
                                                                  'lane measured one.',
                                           'drug_map_cosine': 'Measured cosine between the normalised source '
                                                              'and selected target; null when scoring did '
                                                              'not run.',
                                           'drug_map_method': 'Mapping method: source_native, rule, '
                                                              'exact_synonym or embedding_proposed.',
                                           'drug_map_rollup_levels': 'Maximum hierarchy distance to a '
                                                                     'selected common ancestor.',
                                           'drug_map_rule_id': 'Stable rule or lane identifier that produced '
                                                               'the mapping.',
                                           'drug_map_scoring_model': 'Embedding model that produced '
                                                                     'map_cosine.',
                                           'drug_map_status': 'Explicit mapping outcome from the S3b status '
                                                              'vocabulary.',
                                           'drug_map_status_reason': 'Reason for the mapping outcome or '
                                                                     'abstention.',
                                           'drug_map_version': 'Version of the source, terminology release '
                                                               'or model used by the mapping.',
                                           'drug_omop_concept_id': 'OMOP concept identifier selected with '
                                                                   'the cancer treatment drug mapping.',
                                           'drug_omop_source_concept_id': 'Non-standard OMOP source concept '
                                                                          'identifier, kept separate from a '
                                                                          'standard target.',
                                           'drug_similarity': 'Drug-link similarity score.',
                                           'drug_snomed_code': 'SNOMED CT code selected for the cancer '
                                                               'treatment drug; null when no unambiguous '
                                                               'mapping is available.',
                                           'drug_snomed_display': 'SNOMED CT preferred display selected for '
                                                                  'the cancer treatment drug.',
                                           'drug_source_code': 'Source code for the cancer treatment drug, '
                                                               'retained without de-identification.',
                                           'drug_source_display': 'Source display for the cancer treatment '
                                                                  'drug.',
                                           'drug_source_system': 'Canonical coding-system URI for the source '
                                                                 'cancer treatment drug.',
                                           'drug_target_code': 'Code of the selected target in its own '
                                                               'vocabulary.',
                                           'drug_target_display': 'Display of the selected target in its own '
                                                                  'vocabulary.',
                                           'drug_target_system': 'Coding-system URI of the selected target.',
                                           'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                           'available.',
                                           'end_date': 'Source end date.',
                                           'event_datetime': 'Treatment start timestamp.',
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
                                           'event_end_datetime': 'Treatment end or final-treatment '
                                                                 'timestamp.',
                                           'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                       'wall-clock time (GMT/BST), by the '
                                                                       'same rule as event_datetime_local; '
                                                                       'null where event_end_datetime is '
                                                                       'null.',
                                           'final_treatment_date': 'Date of the final treatment administered '
                                                                   'within the iQemo chemotherapy course.',
                                           'indication': 'Clinical indication text held against the iQemo '
                                                         'regimen definition for which the regimen is used.',
                                           'iqemo_course_id': 'iQemo course identifier.',
                                           'loaded_at': 'map_cancer_treatment.ADC_UPDT carried unchanged '
                                                        'through usable-code filtering and QC/public '
                                                        'projections. No joined parent or QC clock is added. '
                                                        'This differs from SRC_ADC_UPDT used for '
                                                        'source_update_timestamp and from the current Silver '
                                                        'refresh time.',
                                           'patient_event_key': 'Deterministic event key derived from source '
                                                                'identifiers; retained as a stable join key.',
                                           'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                        'resolved.',
                                           'planned_cycles': 'Planned cycle count.',
                                           'procurement_opcs_code': 'OPCS procurement code.',
                                           'record_status': 'Superseded when '
                                                            'map_cancer_treatment.SOURCE_PRESENT_IND cast to '
                                                            'BOOLEAN is false, otherwise active; missing '
                                                            'presence defaults true. No retracted status is '
                                                            'emitted. CourseFinished, record_type and '
                                                            'treatment end dates do not set this row-history '
                                                            'label.',
                                           'record_status_effective_from': 'Start date of the treatment '
                                                                           'record, taken from the ARIA '
                                                                           'administration start date or the '
                                                                           'iQemo chemotherapy course start '
                                                                           'date.',
                                           'record_status_effective_to': 'map_cancer_treatment.ADC_UPDT when '
                                                                         'source presence is false; '
                                                                         'otherwise null. Neither EndDate '
                                                                         'nor FinalTreatmentDate is '
                                                                         'substituted, and a missing '
                                                                         'ADC_UPDT remains null.',
                                           'record_type': 'ARIA/iQemo linkage class.',
                                           'regimen_name': 'Treatment regimen name.',
                                           'route_code': 'Administration route code.',
                                           'source_code': 'Normalized drug token with agent-name display '
                                                          'fallback.',
                                           'source_coding_system': 'SACT drug-token coding system.',
                                           'source_display': 'Source agent name.',
                                           'source_update_timestamp': 'Timestamp of the last update to the '
                                                                      'underlying ARIA or iQemo source '
                                                                      'record as recorded in the raw layer.',
                                           'start_date': 'Source start date.',
                                           'subject_id_system': 'Identifier system used for subject_key.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'treatment_key': 'SACT TREATMENT_KEY; primary key of this table.',
                                           'treatment_plan': 'Source treatment plan.',
                                           'vip_ind': 'VIP indicator when supplied.',
                                           'withheld_identity_ind': 'Identity-withholding status is not '
                                                                    'populated by the current source '
                                                                    'projections; null does not mean '
                                                                    'identity was not withheld.'},
                               'table': 'One SACT constituent-drug treatment fact, separate from Millennium '
                                        'medication administration.'}}
