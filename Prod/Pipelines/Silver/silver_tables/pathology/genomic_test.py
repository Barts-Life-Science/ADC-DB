"""Silver pathology.genomic_test: 2 dataset(s).

- datasets: clinical_genomic_test, _clinical_genomic_test_metadata
- moved verbatim from: 2_silver_clinical_events:2440, 2_silver_clinical_events:2502, 2_silver_clinical_events:2561, 2_silver_clinical_events:2628, 2_silver_clinical_events:2633, 2_silver_clinical_events:2648

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n
from silver_tables._common.journey import _genomic_test_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


GENOMIC_TEST_SOURCE_COLUMNS = [
    "patient_event_key",
    "genetic_test_id",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "encounter_link_method",  # PATH_ENCOUNTER_V1
    "event_datetime",
    "event_end_datetime",
    "event_datetime_local",
    "event_end_datetime_local",
    "source_coding_system",
    "source_code",
    "source_display",
    "assay_code",
    "assay_name",
    "method",
    "analysis_context",
    "overall_result_status",
    "panel_code",
    "panel_version",
    "panel_version_inferred",
    "parser_profile_id",
    "report_version_key",
    "pathology_report_key",
    "specimen_key",
    "accession_identifier",
    "test_snomed_code",
    "test_loinc_code",
    "test_omop_concept_id",
    "assay_component",  # GEN4
    "is_primary_component",  # GEN4
    "nhs_test_directory_code",  # GEN4
    "test_code_method",  # GEN4
    "sample_type_site_raw",  # GEN4
    "sample_site_snomed_code",  # GEN4
    "block_raw",  # GEN4
    "tumour_content_raw",  # GEN4
    "tumour_content_pct",  # GEN4
    "clinical_details_raw",  # GEN4
    "indication_snomed_code",  # GEN4
    "interpretation_text",  # GEN4
    "predicted_response",  # GEN4
    "predicted_response_drug_class",  # GEN4
    "panel_genes_not_assessed",  # GEN4  # GEN4_SILVER_V1
    "is_current",
    "research_qi_only",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
GENOMIC_TEST_PUBLIC_COLUMNS = [
    'patient_event_key',
    'genetic_test_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'encounter_link_method',  # PATH_ENCOUNTER_V1
    'event_datetime',
    'event_end_datetime',
    'event_datetime_local',
    'event_end_datetime_local',
    'source_coding_system',
    'source_code',
    'source_display',
    'assay_code',
    'assay_name',
    'method',
    'analysis_context',
    'overall_result_status',
    'panel_code',
    'panel_version',
    'panel_version_inferred',
    'parser_profile_id',
    'report_version_key',
    'pathology_report_key',
    'specimen_key',
    'accession_identifier',
    'test_snomed_code',
    'test_loinc_code',
    'test_omop_concept_id',
    'assay_component',  # GEN4
    'is_primary_component',  # GEN4
    'nhs_test_directory_code',  # GEN4
    'test_code_method',  # GEN4
    'sample_type_site_raw',  # GEN4
    'sample_site_snomed_code',  # GEN4
    'block_raw',  # GEN4
    'tumour_content_raw',  # GEN4
    'tumour_content_pct',  # GEN4
    'clinical_details_raw',  # GEN4
    'indication_snomed_code',  # GEN4
    'interpretation_text',  # GEN4
    'predicted_response',  # GEN4
    'predicted_response_drug_class',  # GEN4
    'panel_genes_not_assessed',  # GEN4
    'is_current',
    'research_qi_only',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]


GENOMIC_TEST_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'genetic_test_id',
    'identity_status',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_genomic_test():
    # Assemble genomic test rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return _genomic_test_canonical().select(*GENOMIC_TEST_SOURCE_COLUMNS)


@materialized_view(
    name=_n("journey_clinical.genomic_test"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def genomic_test():
    # Build the declared dataset: One molecular/cytogenetic assay per report version (all
    # versions published; record_status supersedes on is_current). Vocab and panel columns are
    # all-NULL in the current build and lanes are pre-wired.
    return _lifecycle_source_genomic_test().select(*GENOMIC_TEST_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._genomic_test_metadata'),
    refresh_policy="auto",
)
def genomic_test_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_genomic_test;
    # same row grain as the research table. Join keys: patient_event_key, genetic_test_id.
    return (_lifecycle_source_genomic_test()).select(*GENOMIC_TEST_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_genomic_test_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
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
                                              'clinical_genomic_test; same row grain as the research table. '
                                              'Join keys: patient_event_key, genetic_test_id.'},
 'clinical_genomic_test': {'columns': {'accession_identifier': 'Alias-resolved accession identifier.',
                                       'analysis_context': 'Somatic or germline analysis context.',
                                       'assay_code': 'Source assay or report code.',
                                       'assay_component': 'GEN4 assay within the report: mngs_dna, mngs_rna, '
                                                          'tngs, flt3, npm1, bcr_abl1_qual. A myeloid NGS '
                                                          'report has a DNA and (when run) an RNA panel row; '
                                                          'an FLT3 report also an NPM1 row.',
                                       'assay_name': 'Resolved assay name.',
                                       'block_raw': 'Histology block identifier as reported (an identifier: '
                                                    'governed by ig tags, published in full).',
                                       'clinical_details_raw': 'Clinical details/information section of the '
                                                               'genetics report (free text).',
                                       'confidentiality_code': 'Source confidentiality classification '
                                                               'attached to the record for each genomic test '
                                                               'record. It is produced by the silver '
                                                               'transformation and has no direct '
                                                               'bronze-column lineage entry. Source code '
                                                               'meanings and sentinel values are retained '
                                                               'unless the pipeline explicitly maps them; '
                                                               'null means no code was supplied.',
                                       'encounter_id': "Millennium ENCNTR_ID as BIGINT: the row's own "
                                                       'Millennium source encounter, else the single '
                                                       'encounter shared by every Millennium source of its '
                                                       'accession; see encounter_link_method.',
                                       'encounter_link_method': 'How encounter_id was set '
                                                                '(PATH_ENCOUNTER_V1). source_accession: the '
                                                                "row's own Millennium source names the "
                                                                'encounter. accession_single: every current '
                                                                'Millennium source of the alias-resolved '
                                                                'accession names this one encounter (how TFC '
                                                                'LIMS rows inherit one once their accession '
                                                                'is merged with the Millennium order). '
                                                                'accession_multiple: those sources name '
                                                                'different encounters, so encounter_id is '
                                                                'NULL. none: the accession has no Millennium '
                                                                'source, so encounter_id is NULL. '
                                                                'PATH_ENCOUNTER_V2 (genomic only): '
                                                                'encounter_window_single / '
                                                                'encounter_window_same_day link a Barts-site '
                                                                "LIMS accession to the patient's encounter "
                                                                'containing the sample date; '
                                                                'encounter_window_multiple and '
                                                                'none_external_referral leave it NULL.',
                                       'event_datetime': 'Report issue time with accession fallback.',
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
                                       'event_end_datetime': 'Event end time.',
                                       'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                   'wall-clock time (GMT/BST), by the same '
                                                                   'rule as event_datetime_local; null where '
                                                                   'event_end_datetime is null.',
                                       'genetic_test_id': 'Pathology genetic_test_id; primary key of this '
                                                          'table.',
                                       'indication_snomed_code': 'SNOMED disorder for the stated indication, '
                                                                 'from deterministic term rules over '
                                                                 'clinical_details_raw.',
                                       'interpretation_text': "The laboratory's clinical interpretation "
                                                              'narrative (free text, up to 4,000 '
                                                              'characters).',
                                       'is_current': 'Whether the source report version is current.',
                                       'is_primary_component': "True for the report's primary assay, which "
                                                               'keeps the pre-GEN4 genetic_test_id; '
                                                               'secondary assays (mngs_rna, npm1) have their '
                                                               'own ids.',
                                       'loaded_at': 'map_pathology_genetic_test.ADC_UPDT carried unchanged '
                                                    'through its joins, usable-code filtering and '
                                                    'source/public projections. Alias resolution and '
                                                    'accession identity add no load clocks; neither joined '
                                                    'report/parent timestamps nor the separately aggregated '
                                                    'accession evidence clock replace this value. It is '
                                                    'identical to source_update_timestamp, not the clinical '
                                                    'event time or Silver refresh time.',
                                       'method': 'Reported assay method.',
                                       'nhs_test_directory_code': 'NHS National Genomic Test Directory '
                                                                  "code(s) for the assay ('|'-separated), "
                                                                  'rule-mapped. © NHS England reference '
                                                                  'codes.',
                                       'overall_result_status': 'Source assay-level result status.',
                                       'panel_code': 'Governed panel code.',
                                       'panel_genes_not_assessed': 'True when the report states genes '
                                                                   'outside its result table were not '
                                                                   'assessed or reported.',
                                       'panel_version': 'Explicit or inferred panel version.',
                                       'panel_version_inferred': 'Whether panel version was inferred.',
                                       'parser_profile_id': 'Deterministic parser profile.',
                                       'pathology_report_key': 'Deterministic pathology-report series key.',
                                       'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                            'cross-feed join key.',
                                       'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                       'predicted_response': 'likely_responsive or unlikely_responsive, only '
                                                             'when the report itself states it.',
                                       'predicted_response_drug_class': 'Drug class named in that statement '
                                                                        '(e.g. EGFR TKI).',
                                       'record_status': 'Superseded when '
                                                        'map_pathology_genetic_test.is_current is false, '
                                                        'otherwise active; missing is_current defaults true. '
                                                        "No retracted status is emitted. The assay's "
                                                        'overall_result_status is separate, and joined '
                                                        'report/identity fields do not set this label.',
                                       'record_status_effective_from': 'Always null as a TIMESTAMP in the '
                                                                       'current genomic-test projection. No '
                                                                       'source-status start is supplied '
                                                                       "here; the clinical event's "
                                                                       'report/sample/request date fallbacks '
                                                                       'and ADC_UPDT are not used for this '
                                                                       'field.',
                                       'record_status_effective_to': 'Always null as a TIMESTAMP in the '
                                                                     'current genomic-test projection, '
                                                                     'including when its status or '
                                                                     'source-current flag changes. No '
                                                                     'report, clinical-event or ingestion '
                                                                     'timestamp is substituted as a status '
                                                                     'end.',
                                       'report_version_key': 'Deterministic report-version key.',
                                       'research_qi_only': 'Research/QI release flag.',
                                       'sample_site_snomed_code': 'SNOMED body structure for the sample '
                                                                  'site, from deterministic term rules over '
                                                                  'sample_type_site_raw.',
                                       'sample_type_site_raw': 'Sample type/site exactly as written in the '
                                                               'report.',
                                       'source_code': 'Source assay code.',
                                       'source_coding_system': 'Assay coding namespace.',
                                       'source_display': 'Source assay display.',
                                       'source_feed': 'Originating data feed responsible for the record for '
                                                      'each genomic test record. It is produced by the '
                                                      'silver transformation and has no direct bronze-column '
                                                      'lineage entry. Whitespace and source sentinel text '
                                                      'are retained unless the pipeline explicitly '
                                                      'normalizes them; null means no value was supplied.',
                                       'source_update_timestamp': 'map_pathology_genetic_test.ADC_UPDT '
                                                                  'carried unchanged from the contributing '
                                                                  'row, identical to loaded_at. No '
                                                                  'independent native application-update '
                                                                  'clock is projected here, and Silver '
                                                                  'refresh time is not substituted.',
                                       'specimen_key': 'Deterministic specimen key.',
                                       'subject_id_system': 'Identifier namespace selected for '
                                                            '`subject_key`: Millennium person identifier '
                                                            'first, then MRN, then NHS number; nosubject '
                                                            'denotes the per-source-record fallback when '
                                                            'none is present.',
                                       'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                      'identifier. Not salted, not secret; a stable join key '
                                                      'across feeds where person_id is unresolved.',
                                       'test_code_method': 'How '
                                                           'test_loinc_code/test_snomed_code/test_omop_concept_id '
                                                           'were set: assay_rule:<rule_id> '
                                                           '(pathology_genetic_assay_concept).',
                                       'test_loinc_code': 'Approved LOINC assay code.',
                                       'test_omop_concept_id': 'Standard OMOP assay concept.',
                                       'test_snomed_code': 'Approved SNOMED assay code.',
                                       'tumour_content_pct': 'Tumour content percent when reported without a '
                                                             "qualifier; NULL for '<10%' style values.",
                                       'tumour_content_raw': "Tumour content as reported (e.g. '25%', "
                                                             "'<10%').",
                                       'vip_ind': 'Indicator that the source record carries VIP handling '
                                                  'status for each genomic test record. It is produced by '
                                                  'the silver transformation and has no direct bronze-column '
                                                  'lineage entry. Null means the source did not state the '
                                                  'indicator and must not be interpreted as false.',
                                       'withheld_identity_ind': 'Withheld identity indicator.'},
                           'table': 'One molecular/cytogenetic assay per report version (all versions '
                                    'published; record_status supersedes on is_current). Vocab and panel '
                                    'columns are all-NULL in the current build and lanes are pre-wired.'}}

# INSR_R4_SILVER_V1 (module_list): generated by r4/release/gen_silver_components.py; edit the generator, not this.
GENOMIC_TEST_PUBLIC_COLUMNS = list(GENOMIC_TEST_PUBLIC_COLUMNS) + ['identity_status']  # decision 1: contract columns, additions last
