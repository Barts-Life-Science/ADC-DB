"""Silver pathology.genomic_result: 2 dataset(s).

- datasets: clinical_genomic_result, _clinical_genomic_result_metadata
- moved verbatim from: 2_silver_clinical_events:2667, 2_silver_clinical_events:2756, 2_silver_clinical_events:2842, 2_silver_clinical_events:2936, 2_silver_clinical_events:2941, 2_silver_clinical_events:2955

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n
from silver_tables._common.journey import _genomic_result_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / genomic result

# contract v2: retain the full canonical shape for internal reuse.
GENOMIC_RESULT_SOURCE_COLUMNS = [
    "patient_event_key",
    "genetic_result_id",
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
    "genomic_test_key",
    "report_version_key",
    "hgnc_id",
    "reported_gene_symbol",
    "normalized_gene_symbol",
    "partner_hgnc_id",
    "partner_gene_symbol",
    "alteration_type",
    "detection_status",
    "hgvs_c_raw",
    "hgvs_c_parsed",
    "hgvs_p_raw",
    "hgvs_p_parsed",
    "transcript",
    "hgvs_validation_status",
    "genome_build",
    "chromosome",
    "position_start",
    "position_end",
    "vaf_raw",
    "vaf",
    "zygosity",
    "reported_classification",
    "reported_tier",
    "copy_number",
    "ratio_raw",
    "iscn_raw",
    "clinvar_concept_id",
    "omop_genomic_concept_id",
    "cosmic_id",  # GEN4
    "itd_length_bp",  # GEN4
    "reported_pathogenicity",  # GEN4
    "pathogenicity_loinc_code",  # SILVER_RELEASE_20261025 item 7
    "pathogenicity_omop_concept_id",  # SILVER_RELEASE_20261025 item 7
    "pathogenicity_display",  # SILVER_RELEASE_20261025 item 7
    "pathogenicity_question_loinc_code",  # SILVER_RELEASE_20261025 item 7
    "pathogenicity_question_omop_concept_id",  # SILVER_RELEASE_20261025 item 7
    "tier_system",  # SILVER_RELEASE_20261025 item 7
    "tier_code",  # SILVER_RELEASE_20261025 item 7
    "tier_display",  # SILVER_RELEASE_20261025 item 7
    "resolved_transcript",  # GEN4
    "transcript_source",  # GEN4
    "variant_key",  # GEN4
    "ref_allele",  # GEN4
    "alt_allele",  # GEN4
    "coordinate_method",  # GEN4
    "clinvar_variation_id",  # GEN4
    "omop_genomic_concept_level",  # GEN4
    "concept_method",  # GEN4
    "evidence_text",
    "evidence_start",
    "evidence_end",
    "parser_profile_id",
    "parser_version",
    "review_status",
    "lifecycle_status",
    "is_current",
    "research_qi_only",
    "specimen_key",
    "accession_identifier",
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
GENOMIC_RESULT_PUBLIC_COLUMNS = [
    'patient_event_key',
    'genetic_result_id',
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
    'genomic_test_key',
    'report_version_key',
    'hgnc_id',
    'reported_gene_symbol',
    'normalized_gene_symbol',
    'partner_hgnc_id',
    'partner_gene_symbol',
    'alteration_type',
    'detection_status',
    'hgvs_c_raw',
    'hgvs_c_parsed',
    'hgvs_p_raw',
    'hgvs_p_parsed',
    'transcript',
    'hgvs_validation_status',
    'genome_build',
    'chromosome',
    'position_start',
    'position_end',
    'vaf_raw',
    'vaf',
    'zygosity',
    'reported_classification',
    'reported_tier',
    'copy_number',
    'ratio_raw',
    'iscn_raw',
    'clinvar_concept_id',
    'omop_genomic_concept_id',
    'cosmic_id',  # GEN4
    'itd_length_bp',  # GEN4
    'reported_pathogenicity',  # GEN4
    'pathogenicity_loinc_code',  # SILVER_RELEASE_20261025 item 7
    'pathogenicity_omop_concept_id',  # SILVER_RELEASE_20261025 item 7
    'pathogenicity_display',  # SILVER_RELEASE_20261025 item 7
    'pathogenicity_question_loinc_code',  # SILVER_RELEASE_20261025 item 7
    'pathogenicity_question_omop_concept_id',  # SILVER_RELEASE_20261025 item 7
    'tier_system',  # SILVER_RELEASE_20261025 item 7
    'tier_code',  # SILVER_RELEASE_20261025 item 7
    'tier_display',  # SILVER_RELEASE_20261025 item 7
    'resolved_transcript',  # GEN4
    'transcript_source',  # GEN4
    'variant_key',  # GEN4
    'ref_allele',  # GEN4
    'alt_allele',  # GEN4
    'coordinate_method',  # GEN4
    'clinvar_variation_id',  # GEN4
    'omop_genomic_concept_level',  # GEN4
    'concept_method',  # GEN4
    'evidence_text',
    'evidence_start',
    'evidence_end',
    'parser_profile_id',
    'parser_version',
    'review_status',
    'lifecycle_status',
    'is_current',
    'research_qi_only',
    'specimen_key',
    'accession_identifier',
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


GENOMIC_RESULT_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'genetic_result_id',
    'identity_status',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_genomic_result():
    # Assemble genomic result rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return _genomic_result_canonical().select(*GENOMIC_RESULT_SOURCE_COLUMNS)


@materialized_view(
    name=_n("journey_clinical.genomic_result"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def genomic_result():
    # Build the declared dataset: One reportable molecular/cytogenetic finding. Schema-first:
    # zero rows in both catalogs as of 2026-08-18 (empty_by_design — no detected assays
    # upstream).
    return _lifecycle_source_genomic_result().select(*GENOMIC_RESULT_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_clinical._genomic_result_metadata'),
    refresh_policy="auto",
)
def genomic_result_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_genomic_result; same row grain as the research table. Join keys:
    # patient_event_key, genetic_result_id.
    return (_lifecycle_source_genomic_result()).select(*GENOMIC_RESULT_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_genomic_result_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
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
                                                'clinical_genomic_result; same row grain as the research '
                                                'table. Join keys: patient_event_key, genetic_result_id.'},
 'clinical_genomic_result': {'columns': {'accession_identifier': 'Alias-resolved accession identifier.',
                                         'alt_allele': 'GRCh38 alternate allele (VCF).',
                                         'alteration_type': 'Reported alteration type.',
                                         'chromosome': 'Explicitly reported chromosome.',
                                         'clinvar_concept_id': 'ClinVar concept identifier.',
                                         'clinvar_variation_id': 'ClinVar VariationID matched on variant_key '
                                                                 '(ClinVar GRCh38 release loaded in '
                                                                 '3_lookup).',
                                         'concept_method': 'Exact match key used for '
                                                           'omop_genomic_concept_id, prefixed by level.',
                                         'confidentiality_code': 'Source confidentiality classification '
                                                                 'attached to the record for each genomic '
                                                                 'result record. It is produced by the '
                                                                 'silver transformation and has no direct '
                                                                 'bronze-column lineage entry. Source code '
                                                                 'meanings and sentinel values are retained '
                                                                 'unless the pipeline explicitly maps them; '
                                                                 'null means no code was supplied.',
                                         'coordinate_method': 'How coordinates were derived: '
                                                              'transcript_model, transcript_model_projected '
                                                              '(stated older version with identical CDS), '
                                                              '*_position_only (interval only), or why none '
                                                              'were (ref_mismatch, '
                                                              'stated_transcript_unavailable, '
                                                              'version_projection_position_not_coding, '
                                                              'no_transcript_model, unsupported_hgvs, '
                                                              'no_parsed_hgvs_c). Positions always describe '
                                                              'ref_allele.',
                                         'copy_number': 'Reported copy number.',
                                         'cosmic_id': 'COSMIC identifier exactly as reported by the '
                                                      'laboratory; not enriched from COSMIC.',
                                         'detection_status': 'Finding detection status.',
                                         'encounter_id': "Millennium ENCNTR_ID as BIGINT: the row's own "
                                                         'Millennium source encounter, else the single '
                                                         'encounter shared by every Millennium source of its '
                                                         'accession; see encounter_link_method.',
                                         'encounter_link_method': 'How encounter_id was set '
                                                                  '(PATH_ENCOUNTER_V1). source_accession: '
                                                                  "the row's own Millennium source names the "
                                                                  'encounter. accession_single: every '
                                                                  'current Millennium source of the '
                                                                  'alias-resolved accession names this one '
                                                                  'encounter (how TFC LIMS rows inherit one '
                                                                  'once their accession is merged with the '
                                                                  'Millennium order). accession_multiple: '
                                                                  'those sources name different encounters, '
                                                                  'so encounter_id is NULL. none: the '
                                                                  'accession has no Millennium source, so '
                                                                  'encounter_id is NULL. PATH_ENCOUNTER_V2 '
                                                                  '(genomic only): encounter_window_single / '
                                                                  'encounter_window_same_day link a '
                                                                  'Barts-site LIMS accession to the '
                                                                  "patient's encounter containing the sample "
                                                                  'date; encounter_window_multiple and '
                                                                  'none_external_referral leave it NULL.',
                                         'event_datetime': 'Report issue time with accession fallback.',
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
                                         'event_end_datetime': 'Event end time.',
                                         'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                     'wall-clock time (GMT/BST), by the same '
                                                                     'rule as event_datetime_local; null '
                                                                     'where event_end_datetime is null.',
                                         'evidence_end': 'Evidence end offset.',
                                         'evidence_start': 'Evidence start offset.',
                                         'evidence_text': 'Minimal narrative evidence span.',
                                         'genetic_result_id': 'Pathology genetic_result_id; primary key of '
                                                              'this table.',
                                         'genome_build': 'Explicit genome build.',
                                         'genomic_test_key': 'Deterministic SHA-256 key of the parent '
                                                             'genomic test; equals '
                                                             'clinical_genomic_test.patient_event_key.',
                                         'hgnc_id': 'Primary HGNC identifier.',
                                         'hgvs_c_parsed': 'Parsed coding HGVS.',
                                         'hgvs_c_raw': 'Raw coding HGVS.',
                                         'hgvs_p_parsed': 'Parsed protein HGVS.',
                                         'hgvs_p_raw': 'Raw protein HGVS.',
                                         'hgvs_validation_status': 'HGVS validation state.',
                                         'is_current': 'Whether the source report version is current.',
                                         'iscn_raw': 'Raw ISCN notation.',
                                         'itd_length_bp': 'Reported FLT3/NPM1 insertion length in base '
                                                          'pairs.',
                                         'lifecycle_status': 'Inherited report lifecycle.',
                                         'loaded_at': 'map_pathology_genetic_result.ADC_UPDT carried '
                                                      'unchanged through its joins, usable-code filtering '
                                                      'and source/public projections. Alias resolution and '
                                                      'accession identity add no load clocks; neither joined '
                                                      'report/parent timestamps nor the separately '
                                                      'aggregated accession evidence clock replace this '
                                                      'value. It is identical to source_update_timestamp, '
                                                      'not the clinical event time or Silver refresh time.',
                                         'normalized_gene_symbol': 'Approved HGNC symbol.',
                                         'omop_genomic_concept_id': 'OMOP Genomic concept identifier.',
                                         'omop_genomic_concept_level': 'Level omop_genomic_concept_id was '
                                                                       'matched at: dna, rna, protein, '
                                                                       'fusion or gene.',
                                         'parser_profile_id': 'Deterministic parser profile identifier.',
                                         'parser_version': 'Parser implementation version.',
                                         'partner_gene_symbol': 'Fusion/rearrangement partner symbol.',
                                         'partner_hgnc_id': 'Partner HGNC identifier.',
                                         'pathogenicity_display': 'LOINC answer display of '
                                                                  'pathogenicity_loinc_code.',
                                         'pathogenicity_loinc_code': 'LOINC answer code for '
                                                                     'reported_pathogenicity, answering '
                                                                     'LOINC 53037-8 (Genetic variation '
                                                                     'clinical significance [Imp]): LA6668-3 '
                                                                     'Pathogenic, LA26332-9 Likely '
                                                                     'pathogenic, LA26333-7 Uncertain '
                                                                     'significance, LA26334-5 Likely benign, '
                                                                     'LA6675-8 Benign. Deterministic code '
                                                                     'table in silver; NULL when the '
                                                                     'laboratory reported no pathogenicity '
                                                                     'class.',
                                         'pathogenicity_omop_concept_id': 'Standard OMOP concept_id '
                                                                          '(vocabulary LOINC, class Answer, '
                                                                          'domain Meas Value) of '
                                                                          'pathogenicity_loinc_code: '
                                                                          '45884094 Pathogenic, 21498358 '
                                                                          'Likely pathogenic, 21498841 '
                                                                          'Uncertain significance, 21498359 '
                                                                          'Likely benign, 45880925 Benign.',
                                         'pathogenicity_question_loinc_code': 'LOINC question the coded '
                                                                              'pathogenicity answers: '
                                                                              '53037-8 Genetic variation '
                                                                              'clinical significance [Imp]. '
                                                                              'Set exactly when '
                                                                              'pathogenicity_loinc_code is '
                                                                              'set.',
                                         'pathogenicity_question_omop_concept_id': 'Standard OMOP concept_id '
                                                                                   'of the question, 3039641 '
                                                                                   '(LOINC 53037-8). Set '
                                                                                   'exactly when '
                                                                                   'pathogenicity_loinc_code '
                                                                                   'is set.',
                                         'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                              'cross-feed join key.',
                                         'person_id': 'Resolved Millennium PERSON_ID as BIGINT through the '
                                                      'shared pathology identity projection.',
                                         'position_end': 'Genomic end coordinate.',
                                         'position_start': 'Genomic start coordinate.',
                                         'ratio_raw': 'Raw molecular ratio.',
                                         'record_status': 'Retracted when '
                                                          'map_pathology_genetic_result.lifecycle_status is '
                                                          'exactly cancelled or entered_in_error; otherwise '
                                                          'superseded when is_current is false (null '
                                                          'defaults true), otherwise active. Comparisons are '
                                                          'case-sensitive without trimming; a null '
                                                          'lifecycle_status does not select retraction. '
                                                          'Retraction takes priority over supersession; '
                                                          'clinical findings and joined identity/link fields '
                                                          'are separate.',
                                         'record_status_effective_from': 'Always null as a TIMESTAMP in the '
                                                                         'current genomic-result projection. '
                                                                         'No source-status start is supplied '
                                                                         "here; the clinical event's "
                                                                         'report/sample/request date '
                                                                         'fallbacks and ADC_UPDT are not '
                                                                         'used for this field.',
                                         'record_status_effective_to': 'Always null as a TIMESTAMP in the '
                                                                       'current genomic-result projection, '
                                                                       'including when its status or '
                                                                       'source-current flag changes. No '
                                                                       'report, clinical-event or ingestion '
                                                                       'timestamp is substituted as a status '
                                                                       'end.',
                                         'ref_allele': 'GRCh38 reference allele (VCF).',
                                         'report_version_key': 'Deterministic report-version key.',
                                         'reported_classification': 'Classification or tier reported by the '
                                                                    'laboratory.',
                                         'reported_gene_symbol': 'Gene symbol as reported.',
                                         'reported_pathogenicity': 'Laboratory pathogenicity class: '
                                                                   'pathogenic, likely_pathogenic, '
                                                                   'uncertain_significance, likely_benign, '
                                                                   'benign.',
                                         'reported_tier': 'Reported clinical/actionability tier.',
                                         'research_qi_only': 'Research/QI release flag.',
                                         'resolved_transcript': 'RefSeq transcript the c. HGVS refers to: as '
                                                                "reported, else the report's panel list, "
                                                                "else the gene's MANE Select (see "
                                                                'transcript_source).',
                                         'review_status': 'Source finding review status.',
                                         'source_code': 'SNV, indel, fusion, CNV, rearrangement, repeat, '
                                                        'karyotype, or other.',
                                         'source_coding_system': 'Gene-symbol or alteration coding '
                                                                 'namespace.',
                                         'source_display': 'SNV, indel, fusion, CNV, rearrangement, repeat, '
                                                           'karyotype, or other.',
                                         'source_feed': 'Originating data feed responsible for the record '
                                                        'for each genomic result record. It is produced by '
                                                        'the silver transformation and has no direct '
                                                        'bronze-column lineage entry. Whitespace and source '
                                                        'sentinel text are retained unless the pipeline '
                                                        'explicitly normalizes them; null means no value was '
                                                        'supplied.',
                                         'source_update_timestamp': 'map_pathology_genetic_result.ADC_UPDT '
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
                                                        'identifier. Not salted, not secret; a stable join '
                                                        'key across feeds where person_id is unresolved.',
                                         'tier_code': 'AMP/ASCO/CAP somatic variant tier from reported_tier: '
                                                      'I, II, III or IV; NULL otherwise.',
                                         'tier_display': 'Meaning of tier_code: Tier I strong, Tier II '
                                                         'potential, Tier III unknown clinical significance; '
                                                         'Tier IV benign or likely benign.',
                                         'tier_system': 'Coding system of tier_code: '
                                                        'urn:barts:genomics:amp-asco-cap-2017-somatic-tier, '
                                                        'the AMP/ASCO/CAP 2017 somatic variant tiers (no '
                                                        'standard vocabulary concept exists for them in '
                                                        '3_lookup.omop). Set when reported_tier is I, II, '
                                                        'III or IV.',
                                         'transcript': 'Reported transcript accession.',
                                         'transcript_source': 'reported, report_panel_list or mane_select.',
                                         'vaf': 'Parsed variant allele fraction.',
                                         'vaf_raw': 'Raw variant allele frequency.',
                                         'variant_key': 'GRCh38 VCF key chrN:pos:ref:alt (left-normalised), '
                                                        'derived in-estate from resolved_transcript + '
                                                        'hgvs_c_parsed.',
                                         'vip_ind': 'Indicator that the source record carries VIP handling '
                                                    'status for each genomic result record. It is produced '
                                                    'by the silver transformation and has no direct '
                                                    'bronze-column lineage entry. Null means the source did '
                                                    'not state the indicator and must not be interpreted as '
                                                    'false.',
                                         'withheld_identity_ind': 'Withheld identity indicator.',
                                         'zygosity': 'Value describing zygosity for the genomic result '
                                                     'record. It is carried from bronze field `zygosity` in '
                                                     '`4_prod.bronze.map_pathology_genetic_result`. '
                                                     'Whitespace and source sentinel text are retained '
                                                     'unless the pipeline explicitly normalizes them; null '
                                                     'means no value was supplied.'},
                             'table': 'One reportable molecular/cytogenetic finding. Schema-first: zero rows '
                                      'in both catalogs as of 2026-08-18 (empty_by_design — no detected '
                                      'assays upstream).'}}

# INSR_R4_SILVER_V1 (module_list): generated by r4/release/gen_silver_components.py; edit the generator, not this.
GENOMIC_RESULT_PUBLIC_COLUMNS = list(GENOMIC_RESULT_PUBLIC_COLUMNS) + ['identity_status']  # decision 1: contract columns, additions last
