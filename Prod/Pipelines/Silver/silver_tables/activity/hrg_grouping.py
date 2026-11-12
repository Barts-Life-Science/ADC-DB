"""Silver activity.hrg_grouping: 3 dataset(s).

- datasets: _clinical_hrg_grouping_stage, clinical_hrg_grouping, _clinical_hrg_grouping_metadata
- moved verbatim from: 4_silver_care_pathways:2625, 4_silver_care_pathways:2710, 4_silver_care_pathways:2792, 4_silver_care_pathways:2800, 4_silver_care_pathways:2895, 4_silver_care_pathways:2914, 4_silver_care_pathways:2919, 4_silver_care_pathways:2931

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n, spark
from silver_tables._common.journey import _hrg_grouping_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# contract v2: retain the full canonical shape for internal reuse.
HRG_GROUPING_SOURCE_COLUMNS = [
    "patient_event_key",
    "cds_id",
    "source_object",
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
    "cds_record_id",
    "person_link_method",
    "admission_datetime",
    "discharge_datetime",
    "episode_start_datetime",
    "episode_end_datetime",
    "hosp_prov_spell_num",
    "provider_org_cd",
    "episode_order",
    "episode_duration_days",
    "main_specialty_cd",
    "treatment_function_cd",
    "admission_method_cd",
    "admission_source_cd",
    "admission_source_desc",
    "discharge_method_cd",
    "discharge_dest_cd",
    "discharge_dest_desc",
    "patient_class_cd",
    "patient_class_desc",
    "source_age",
    "source_sex_cd",
    "neonatal_care_level_cd",
    "critical_care_days",
    "rehab_days",
    "fce_hrg_cd",
    "fce_hrg_desc",
    "fce_grouping_method_flag",
    "fce_dominant_proc_cd",
    "fce_dominant_proc_desc",
    "fce_pbc_cd",
    "fce_calc_episode_duration",
    "fce_reporting_episode_duration",
    "dominant_episode_flag",
    "spell_hrg_cd",
    "spell_hrg_desc",
    "spell_grouping_method_flag",
    "spell_dominant_proc_cd",
    "spell_dominant_proc_desc",
    "spell_primary_diag_cd",
    "spell_primary_diag_desc",
    "spell_secondary_diag_cd",
    "spell_secondary_diag_desc",
    "spell_episode_count",
    "spell_los",
    "spell_reporting_los",
    "spell_critical_care_days",
    "spell_ssc_cd",
    "spell_best_practice_cd",
    "first_attend_cd",
    "first_attend_desc",
    "grouping_method_flag",
    "dominant_proc_cd",
    "dominant_proc_desc",
    "grouper_errors",
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
HRG_GROUPING_PUBLIC_COLUMNS = [
    'patient_event_key',
    'cds_id',
    'source_object',
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
    'cds_record_id',
    'person_link_method',
    'admission_datetime',
    'discharge_datetime',
    'episode_start_datetime',
    'episode_end_datetime',
    'hosp_prov_spell_num',
    'provider_org_cd',
    'episode_order',
    'episode_duration_days',
    'main_specialty_cd',
    'treatment_function_cd',
    'admission_method_cd',
    'admission_source_cd',
    'admission_source_desc',
    'discharge_method_cd',
    'discharge_dest_cd',
    'discharge_dest_desc',
    'patient_class_cd',
    'patient_class_desc',
    'source_age',
    'source_sex_cd',
    'neonatal_care_level_cd',
    'critical_care_days',
    'rehab_days',
    'fce_hrg_cd',
    'fce_hrg_desc',
    'fce_grouping_method_flag',
    'fce_dominant_proc_cd',
    'fce_dominant_proc_desc',
    'fce_pbc_cd',
    'fce_calc_episode_duration',
    'fce_reporting_episode_duration',
    'dominant_episode_flag',
    'spell_hrg_cd',
    'spell_hrg_desc',
    'spell_grouping_method_flag',
    'spell_dominant_proc_cd',
    'spell_dominant_proc_desc',
    'spell_primary_diag_cd',
    'spell_primary_diag_desc',
    'spell_secondary_diag_cd',
    'spell_secondary_diag_desc',
    'spell_episode_count',
    'spell_los',
    'spell_reporting_los',
    'spell_critical_care_days',
    'spell_ssc_cd',
    'spell_best_practice_cd',
    'first_attend_cd',
    'first_attend_desc',
    'grouping_method_flag',
    'dominant_proc_cd',
    'dominant_proc_desc',
    'grouper_errors',
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


HRG_GROUPING_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'cds_id',
    'identity_status',
    'load_batch_id',
]


HRG_GROUPING_STAGE_COLUMNS = HRG_GROUPING_SOURCE_COLUMNS + [
    "icd_diagnosis_codes_json", "opcs_procedure_codes_json",
]


# Clinical /  hrg grouping stage

# ==== journey_clinical._hrg_grouping_stage (internal) ====
@materialized_view(
    name=_n("journey_clinical._hrg_grouping_stage"),
    private=True,
    refresh_policy="auto",
)
def _hrg_grouping_stage():
    # Build the declared dataset: Internal HRG rows carrying JSON code arrays for the public
    # parent and code children.
    return _hrg_grouping_canonical().select(*HRG_GROUPING_STAGE_COLUMNS)


# Clinical / hrg grouping

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_hrg_grouping():
    # Assemble hrg grouping rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return spark.read.table(_n("journey_clinical._hrg_grouping_stage")).select(*HRG_GROUPING_SOURCE_COLUMNS)


@materialized_view(
    name=_n("journey_clinical.hrg_grouping"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def hrg_grouping():
    # Build the declared dataset: SLAM APC and outpatient HRG grouping results with sentinel-
    # safe event time.
    return _lifecycle_source_hrg_grouping().select(*HRG_GROUPING_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._hrg_grouping_metadata'),
    refresh_policy="auto",
)
def hrg_grouping_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_hrg_grouping;
    # same row grain as the research table. Join keys: patient_event_key, source_object, cds_id.
    return (_lifecycle_source_hrg_grouping()).select(*HRG_GROUPING_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_hrg_grouping_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
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
                                                 'artifact_link_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                 'birth_row_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                 'cds_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                                 'chemotherapy_course_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                 'community_care_activity_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
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
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                 'dicom_path': 'Primary-key component carried unchanged from '
                                                               'the public parent; combine it with the '
                                                               "companion's other key columns when joining.",
                                                 'doc_response_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                 'elective_access_procedure_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                 'encounter_id_resolved': 'True when encounter_id is null or '
                                                                          'resolves to the governed '
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
                                                 'endobase_exam_term_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                 'entity_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                 'entry_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
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
                                                                          "resolved person's death "
                                                                          'timestamp; silver records the '
                                                                          'doubt and does not filter the '
                                                                          'row.',
                                                 'event_before_birth': 'True when the event timestamp '
                                                                       "precedes the resolved person's birth "
                                                                       'timestamp; silver records the doubt '
                                                                       'and does not filter the row.',
                                                 'event_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
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
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                 'implant_sequence': 'Primary-key component carried '
                                                                     'unchanged from the public parent; '
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
                                                 'indication_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
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
                                                 'load_batch_id': 'Deterministic batch token derived from '
                                                                  "the source row's bronze load timestamp.",
                                                 'loaded_at': 'Latest bronze load timestamp contributing to '
                                                              'the public row; never the lifecycle '
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
                                                 'mc_device_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                 'microbiology_isolate_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                 'order_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                 'practitioner_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                 'practitioner_identifier_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                 'practitioner_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                 'practitioner_location_evidence_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                 'pregnancy_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                 'pregnancy_reconciliation_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                 'problem_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                                 'procedure_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                 'pt_prot_reg_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
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
                                                 'referral_oid': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                                               'patient_event_key is shared across an '
                                                               "entry's versions.",
                                                 'rtt_activity_oid': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                 'sch_event_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                 'score_type': 'Primary-key component carried unchanged from '
                                                               'the public parent; combine it with the '
                                                               "companion's other key columns when joining.",
                                                 'sequence_nbr': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
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
                                                 'source_object': 'Source-arm discriminator carried '
                                                                  'unchanged from the public parent; use it '
                                                                  'with arm-specific native identifiers when '
                                                                  'joining.',
                                                 'source_site': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                                 'source_system_oid': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                 'source_unit': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                 'waiting_list_snapshot_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.'},
                                     'table': 'Internal quality and batch metadata for '
                                              'clinical_hrg_grouping; same row grain as the research table. '
                                              'Join keys: patient_event_key, source_object, cds_id.'},
 '_clinical_hrg_grouping_stage': {'columns': {},
                                  'table': 'Internal HRG rows carrying JSON code arrays for the public '
                                           'parent and code children.'},
 'clinical_hrg_grouping': {'columns': {'admission_datetime': 'Date and time the patient was admitted for the '
                                                             'hospital provider spell.',
                                       'admission_method_cd': 'NHS admission method code for the hospital '
                                                              'provider spell.',
                                       'admission_source_cd': 'NHS source of admission code for the hospital '
                                                              'provider spell.',
                                       'admission_source_desc': 'Description of the source of admission '
                                                                'code, mapped from the CDS admission source '
                                                                'lookup.',
                                       'cds_id': 'Native CDS_APC_ID or CDS_OPA_ID; primary key within '
                                                 'source_object.',
                                       'cds_record_id': 'Trimmed CDS admitted-patient-care episode '
                                                        'identifier; unique key; joins back to raw.',
                                       'confidentiality_code': 'Source confidentiality classification '
                                                               'attached to the record for each HRG grouping '
                                                               'record. It is produced by the silver '
                                                               'transformation and has no direct '
                                                               'bronze-column lineage entry. Source code '
                                                               'meanings and sentinel values are retained '
                                                               'unless the pipeline explicitly maps them; '
                                                               'null means no code was supplied.',
                                       'critical_care_days': 'Number of critical care days for the episode '
                                                             'as submitted to the grouper.',
                                       'discharge_datetime': 'Date and time the patient was discharged from '
                                                             'the hospital provider spell.',
                                       'discharge_dest_cd': 'NHS discharge destination code for the hospital '
                                                            'provider spell.',
                                       'discharge_dest_desc': 'Description of the discharge destination '
                                                              'code, mapped from the CDS discharge '
                                                              'destination lookup.',
                                       'discharge_method_cd': 'NHS discharge method code for the hospital '
                                                              'provider spell.',
                                       'dominant_episode_flag': 'Grouper flag indicating whether this '
                                                                'episode is the dominant episode used to '
                                                                'derive the spell HRG.',
                                       'dominant_proc_cd': 'OPCS-4 code of the dominant procedure determined '
                                                           'by the grouper for the attendance.',
                                       'dominant_proc_desc': 'Description of DOMINANT_PROC_CD obtained from '
                                                             'the lookup 3_lookup.dwh.opcs_410.',
                                       'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                       'available.',
                                       'episode_duration_days': 'Length of the episode in days as submitted '
                                                                'to the grouper.',
                                       'episode_end_datetime': 'Date and time the finished consultant '
                                                               'episode ended.',
                                       'episode_order': 'Sequence number of this episode within the hospital '
                                                        'provider spell.',
                                       'episode_start_datetime': 'Date and time the finished consultant '
                                                                 'episode started.',
                                       'event_datetime': 'Date and time the finished consultant episode '
                                                         'started.',
                                       'event_datetime_local': 'event_datetime in Europe/London wall-clock '
                                                               'time (GMT/BST). Millennium rows take the '
                                                               'bronze *_LOCAL companions of the same source '
                                                               'columns and fallback order; rows from '
                                                               'local-clock sources (LUNA, PACS/DICOM, TFC '
                                                               'LIMS, CCMDS, MSDS, BadgerNet, EndoBase, '
                                                               'SLAM, iWeb, Datix and similar) carry '
                                                               'event_datetime unchanged. event_datetime is '
                                                               'a UTC instant for Millennium rows, so take '
                                                               'local calendar dates and clock times from '
                                                               'this column.',
                                       'event_end_datetime': 'Date and time the finished consultant episode '
                                                             'ended.',
                                       'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                   'wall-clock time (GMT/BST), by the same '
                                                                   'rule as event_datetime_local; null where '
                                                                   'event_end_datetime is null.',
                                       'fact_category': 'Contract category used to group the record with '
                                                        'comparable clinical facts for each HRG grouping '
                                                        'record. It is produced by the silver transformation '
                                                        'and has no direct bronze-column lineage entry. '
                                                        'Whitespace and source sentinel text are retained '
                                                        'unless the pipeline explicitly normalizes them; '
                                                        'null means no value was supplied.',
                                       'fce_calc_episode_duration': 'Grouper-calculated episode duration in '
                                                                    'days for this finished consultant '
                                                                    'episode.',
                                       'fce_dominant_proc_cd': 'OPCS-4 procedure code identified by the '
                                                               'HRG4+ grouper as the dominant procedure for '
                                                               'this episode; null where no procedure drove '
                                                               'the grouping.',
                                       'fce_dominant_proc_desc': 'Description of the episode dominant '
                                                                 'procedure code, mapped from the OPCS-4.10 '
                                                                 'reference lookup.',
                                       'fce_grouping_method_flag': 'Grouper-returned flag indicating the '
                                                                   'method by which the HRG4+ code was '
                                                                   'derived for this finished consultant '
                                                                   'episode.',
                                       'fce_hrg_cd': 'HRG4+ code assigned to this finished consultant '
                                                     'episode.',
                                       'fce_hrg_desc': 'Description of the episode-level HRG4+ code, mapped '
                                                       'from the HRG v4 reference lookup.',
                                       'fce_pbc_cd': 'Programme Budgeting Category code assigned by the '
                                                     'HRG4+ grouper to this episode.',
                                       'fce_reporting_episode_duration': 'Episode duration in days as used '
                                                                         'for HRG reporting purposes by the '
                                                                         'grouper.',
                                       'first_attend_cd': 'CDS first attendance code indicating whether the '
                                                          'attendance was a first or follow-up contact and '
                                                          'its modality.',
                                       'first_attend_desc': 'Description of FIRST_ATTEND_CD obtained from '
                                                            'the lookup 3_lookup.dwh.cds_first_attend.',
                                       'grouper_errors': 'Grouper error/quality messages.',
                                       'grouping_method_flag': 'HRG4+ grouper flag indicating the method by '
                                                               'which the HRG was derived for the '
                                                               'attendance.',
                                       'hosp_prov_spell_num': 'Hospital provider spell number grouping '
                                                              'episodes into spells.',
                                       'loaded_at': 'ADC_UPDT carried unchanged from the contributing '
                                                    'map_slam_apc_hrg or map_slam_op_hrg row through the '
                                                    'common HRG stage. This is per-row bronze ingestion '
                                                    'provenance, not episode/attendance time or Silver '
                                                    'refresh time; missing values remain null.',
                                       'main_specialty_cd': 'NHS main specialty code recorded for the '
                                                            'episode.',
                                       'neonatal_care_level_cd': 'Coded level of neonatal care recorded for '
                                                                 'the episode as submitted to the grouper.',
                                       'patient_class_cd': 'CDS patient classification code for the episode, '
                                                           'such as ordinary or day case admission.',
                                       'patient_class_desc': 'Description of the patient classification '
                                                             'code, mapped from the CDS patient class '
                                                             'lookup.',
                                       'patient_event_key': 'Deterministic event key derived from source '
                                                            'identifiers; retained as a stable join key.',
                                       'person_id': 'Native Millennium PERSON_ID as BIGINT when resolved.',
                                       'person_link_method': 'How PERSON_ID was resolved: MRN_ALIAS, or null '
                                                             'when unlinked.',
                                       'provider_org_cd': 'NHS organisation (provider) code identifying the '
                                                          'submitting healthcare provider for the episode.',
                                       'record_status': 'For both SLAM APC and outpatient HRG arms, '
                                                        "retracted when the arm's SOURCE_PRESENT_IND is "
                                                        'false, otherwise active; a missing presence flag '
                                                        'defaults true. Neither arm emits superseded. This '
                                                        'is a source-row presence status, not an HRG grouper '
                                                        'result or clinical episode state.',
                                       'record_status_effective_from': 'map_slam_apc_hrg.EPISODE_START_DT_TM '
                                                                       'for the APC arm, or '
                                                                       'map_slam_op_hrg.ATTENDANCE_DT_TM for '
                                                                       'the outpatient arm, cast to '
                                                                       'TIMESTAMP. Unlike event_datetime, '
                                                                       'this history field is not '
                                                                       'range-clamped to [1950,2100). It is '
                                                                       'an event-time proxy rather than an '
                                                                       'independently observed row-status '
                                                                       'transition; missing values remain '
                                                                       'null.',
                                       'record_status_effective_to': "The relevant SLAM arm's "
                                                                     'SOURCE_ABSENT_DETECTED_TS only when '
                                                                     'that row is retracted; otherwise null. '
                                                                     'A missing absence-detection time '
                                                                     'remains null: episode end, discharge '
                                                                     'and ADC_UPDT are not substituted.',
                                       'rehab_days': 'Number of rehabilitation days for the episode as '
                                                     'submitted to the grouper.',
                                       'source_age': 'NON-CANONICAL grouper input age as submitted to SLAM; '
                                                     'demographics of record are in map_person.',
                                       'source_code': 'HRG4+ code assigned to this finished consultant '
                                                      'episode.',
                                       'source_coding_system': 'HRG4+ code assigned to this finished '
                                                               'consultant episode.',
                                       'source_display': 'Description of the episode-level HRG4+ code, '
                                                         'mapped from the HRG v4 reference lookup.',
                                       'source_feed': 'Originating data feed responsible for the record for '
                                                      'each HRG grouping record. It is produced by the '
                                                      'silver transformation and has no direct bronze-column '
                                                      'lineage entry. Whitespace and source sentinel text '
                                                      'are retained unless the pipeline explicitly '
                                                      'normalizes them; null means no value was supplied.',
                                       'source_object': 'SLAM source arm (slam_apc_hrg or slam_op_hrg); part '
                                                        'of the primary key with cds_id.',
                                       'source_sex_cd': 'NON-CANONICAL grouper input sex code as submitted '
                                                        'to SLAM; demographics of record are in map_person.',
                                       'source_update_timestamp': 'SOURCE_RECORD_UPDATED_DT carried '
                                                                  'unchanged from the contributing '
                                                                  'map_slam_apc_hrg or map_slam_op_hrg row. '
                                                                  'It is separate from ADC_UPDT ingestion '
                                                                  'provenance; the union does not aggregate '
                                                                  'clocks across arms and Silver refresh '
                                                                  'time is not substituted.',
                                       'spell_best_practice_cd': 'Best practice tariff code assigned by the '
                                                                 'HRG4+ grouper at hospital provider spell '
                                                                 'level, null where no best practice '
                                                                 'classification applies.',
                                       'spell_critical_care_days': 'Number of critical care days recorded '
                                                                   'for the hospital provider spell as '
                                                                   'submitted to the grouper.',
                                       'spell_dominant_proc_cd': 'OPCS-4 procedure code identified by the '
                                                                 'HRG4+ grouper as the dominant procedure '
                                                                 'for the hospital provider spell.',
                                       'spell_dominant_proc_desc': 'Description of the spell dominant '
                                                                   'procedure code, mapped from the '
                                                                   'OPCS-4.10 reference lookup.',
                                       'spell_episode_count': 'Number of finished consultant episodes '
                                                              'counted by the grouper within the hospital '
                                                              'provider spell.',
                                       'spell_grouping_method_flag': 'Grouper-returned flag indicating the '
                                                                     'method by which the spell-level HRG4+ '
                                                                     'code was derived.',
                                       'spell_hrg_cd': 'HRG4+ code assigned at hospital-spell level '
                                                       '(repeated on every episode of the spell).',
                                       'spell_hrg_desc': 'Description of the spell-level HRG4+ code, mapped '
                                                         'from the HRG v4 reference lookup.',
                                       'spell_los': 'Grouper-calculated length of stay in days for the '
                                                    'hospital provider spell.',
                                       'spell_primary_diag_cd': 'ICD-10 primary diagnosis code used by the '
                                                                'grouper at hospital provider spell level.',
                                       'spell_primary_diag_desc': 'Description of the spell primary '
                                                                  'diagnosis code, mapped from the ICD '
                                                                  'diagnosis reference lookup.',
                                       'spell_reporting_los': 'Spell length of stay in days as used for HRG '
                                                              'reporting purposes by the grouper.',
                                       'spell_secondary_diag_cd': 'ICD-10 secondary diagnosis code used by '
                                                                  'the grouper at hospital provider spell '
                                                                  'level.',
                                       'spell_secondary_diag_desc': 'Description of the spell secondary '
                                                                    'diagnosis code, mapped from the ICD '
                                                                    'diagnosis reference lookup.',
                                       'spell_ssc_cd': 'Specialised service code assigned by the HRG4+ '
                                                       'grouper at hospital provider spell level.',
                                       'subject_id_system': 'Millennium PERSON_ID resolved from source MRN '
                                                            'via mill_person_alias type 10 (deterministic '
                                                            'tiebreak); null when unresolved — source MRN is '
                                                            'retained only in raw.',
                                       'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                      'identifier. Not salted, not secret; a stable join key '
                                                      'across feeds where person_id is unresolved.',
                                       'treatment_function_cd': 'NHS Treatment Function Code (national '
                                                                'standard).',
                                       'vip_ind': 'Indicator that the source record carries VIP handling '
                                                  'status for each HRG grouping record. It is produced by '
                                                  'the silver transformation and has no direct bronze-column '
                                                  'lineage entry. Null means the source did not state the '
                                                  'indicator and must not be interpreted as false.',
                                       'withheld_identity_ind': 'Indicator that the source identity was '
                                                                'deliberately withheld for each HRG grouping '
                                                                'record. It is produced by the silver '
                                                                'transformation and has no direct '
                                                                'bronze-column lineage entry. Null means the '
                                                                'source did not state the indicator and must '
                                                                'not be interpreted as false.'},
                           'table': 'SLAM APC and outpatient HRG grouping results with sentinel-safe event '
                                    'time.'}}

# INSR_R4_SILVER_V1 (module_list): generated by r4/release/gen_silver_components.py; edit the generator, not this.
HRG_GROUPING_PUBLIC_COLUMNS = list(HRG_GROUPING_PUBLIC_COLUMNS) + ['identity_status']  # decision 1: contract columns, additions last
