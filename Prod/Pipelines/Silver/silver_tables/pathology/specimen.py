"""Silver pathology.specimen: 3 dataset(s).

- datasets: _clinical_qc_specimen, clinical_specimen, _clinical_specimen_metadata
- moved verbatim from: 2_silver_clinical_events:1724, 2_silver_clinical_events:1848, 2_silver_clinical_events:1866

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, spark
from silver_tables._common.journey import _pathology_specimen_canonical
from silver_tables.pathology._shared import SPECIMEN_LIFECYCLE_COLUMNS, SPECIMEN_PUBLIC_COLUMNS


from silver_core.table_spec import (CrossQc, CrossQcBinding, SourceInputs, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical /  qc specimen
@materialized_view(
    name=_n("journey_clinical._qc_specimen"),
    refresh_policy="auto",
)
def _qc_specimen():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for specimen.
    return _cross_qc_primitive(
        _pathology_specimen_canonical(carry_source_namespace=True),  # SRC_LINEAGE_V1: see SPECIMEN_TYPE_SOURCE
        "specimen",
        {"specimen_type": "_qc_specimen_type_json"},
    )


@materialized_view(
    name=_n("journey_clinical.specimen"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def specimen():
    # Build the declared dataset: One canonical pathology accession with collection and request
    # evidence.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_specimen")),
        "specimen",
        {"specimen_type": "_qc_specimen_type_json"},
        SPECIMEN_PUBLIC_COLUMNS,
        SPECIMEN_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._specimen_metadata'),
    refresh_policy="auto",
)
def specimen_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_specimen;
    # same row grain as the research table. Join keys: patient_event_key.
    return (spark.read.table(_n("journey_clinical._qc_specimen"))).select(*SPECIMEN_LIFECYCLE_COLUMNS)


# SRC_LINEAGE_V1 (Phase 0a, deviation D1): the specimen_type bridge's source coding system is the bronze namespace
# (e.g. urn:barts:pathology:specimen-type:tfc:lims6), falling back to the generic URI, while the row's scalar
# source_coding_system is always the generic URI. The arm carries the namespace as _specimen_type_source_namespace
# and the axis takes its system from it with the bridge's own fallback; code and display are the row's.
SPECIMEN_TYPE_SOURCE = SourceInputs("_specimen_type_source_namespace", "source_code", "source_display",
                                    system_default="urn:barts:pathology:specimen-type")


CROSS_QC = {
    'specimen': CrossQc(model='specimen', fk=('person_id',), dates=('event_after_death_30d', 'event_before_birth'), coded_axes={'specimen_type': ('specimen_type', False)}, promote_lifecycle=['identity_status', 'person_id_resolved'], source_inputs={'specimen_type': SPECIMEN_TYPE_SOURCE}),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {
    'clinical_specimen': ValuePolicy(table='clinical_specimen', enabled=True),
}


# W2.13 (L3665, F14c): every mapped specimen-type row carries a method and a rule id. Rows the bronze lookup mapped
# without saying how get rule / lookup.specimen_type_snomed_map; values already set (a lane's) are kept; unmapped
# rows stay NULL.
VALUE_POLICIES["clinical_specimen"].value_rule("specimen_type_map_method", ["specimen_type_map_method", "specimen_type_target_code"],
           lambda F: F.when(F.col("specimen_type_target_code").isNotNull(),
                            F.coalesce(F.col("specimen_type_map_method"), F.lit("rule"))).otherwise(F.col("specimen_type_map_method")))


VALUE_POLICIES["clinical_specimen"].value_rule("specimen_type_map_rule_id", ["specimen_type_map_rule_id", "specimen_type_target_code"],
           lambda F: F.when(F.col("specimen_type_target_code").isNotNull(),
                            F.coalesce(F.col("specimen_type_map_rule_id"), F.lit("lookup.specimen_type_snomed_map")))
                      .otherwise(F.col("specimen_type_map_rule_id")))


WRAP_POLICY = {'clinical_specimen'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_qc_specimen': {'columns': {},
                           'table': 'Private JSON bridge and Gold cross-rule flags for specimen.'},
 '_clinical_specimen_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                             'activity_record_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                             'admission_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                             'allergy_instance_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                             'aria_dx_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                             'aria_pt_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                             'artifact_link_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                             'birth_row_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                             'bloodtrack_transaction_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                             'cancer_treatment_cycle_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                             'care_contact_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                             'care_participation_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                             'cds_id': 'Native source identifier carried unchanged from the '
                                                       'public parent; joins one lifecycle row to that '
                                                       'research row.',
                                             'chemotherapy_course_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                             'community_care_activity_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                             'community_care_contact_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                             'concept_map_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                             'cost_line_item_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                             'dcp_forms_activity_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                             'device_mapping_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                             'diagnosis_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                             'dicom_path': 'Primary-key component carried unchanged from the '
                                                           "public parent; combine it with the companion's "
                                                           'other key columns when joining.',
                                             'doc_response_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'elective_access_procedure_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                             'encounter_attribute_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                             'encounter_bounds_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                             'encounter_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                             'encounter_id_resolved': 'True when encounter_id is null or '
                                                                      'resolves to the governed encounter '
                                                                      'spine; false records unresolved '
                                                                      'linkage without dropping the row.',
                                             'encounter_identifier_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                             'encounter_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                             'endobase_exam_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'endobase_exam_term_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                             'entity_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                             'entry_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                             'episode_encounter_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                             'episode_encounter_reltn_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                             'episode_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                             'episode_id_resolved': 'True when episode_id is null or '
                                                                    'resolves to the governed episode spine; '
                                                                    'false records unresolved linkage '
                                                                    'without dropping the row.',
                                             'episode_key': 'Deterministic v2 key carried unchanged from the '
                                                            'public parent; joins one lifecycle row to that '
                                                            'research row.',
                                             'event_after_death_30d': 'True when the event timestamp is more '
                                                                      'than 30 days after the resolved '
                                                                      "person's death timestamp; silver "
                                                                      'records the doubt and does not filter '
                                                                      'the row.',
                                             'event_before_birth': 'True when the event timestamp precedes '
                                                                   "the resolved person's birth timestamp; "
                                                                   'silver records the doubt and does not '
                                                                   'filter the row.',
                                             'event_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                             'extract_cd': 'Primary-key component carried unchanged from the '
                                                           "public parent; combine it with the companion's "
                                                           'other key columns when joining.',
                                             'fhx_activity_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                             'gene_tested_row_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                             'genetic_result_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                             'genetic_test_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                             'homecare_request_item_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                             'identity_status': 'Subject identity resolution state recorded '
                                                                'by the silver canonical query.',
                                             'implant_event_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'implant_sequence': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
                                             'indication_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                             'is_illogical': 'Source RTT chronology flag indicating an '
                                                             'illogical activity sequence; retained for the '
                                                             'gold drop rule.',
                                             'journey_key': 'Deterministic v2 key carried unchanged from the '
                                                            'public parent; joins one lifecycle row to that '
                                                            'research row.',
                                             'journey_link_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'journey_participant_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                             'labour_delivery_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                             'load_batch_id': 'Deterministic batch token derived from the '
                                                              "source row's bronze load timestamp.",
                                             'loaded_at': 'Latest bronze load timestamp contributing to the '
                                                          "public row; never the lifecycle pipeline's "
                                                          'wall-clock time.',
                                             'location_code': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                             'location_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
                                             'location_stay_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                             'mc_device_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                             'microbiology_isolate_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                             'order_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                             'organization_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                             'organization_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'pacs_examination_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                             'pacs_report_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                             'pathway_oid': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                             'patient_event_key': 'Deterministic SHA-256 event key carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that event row.',
                                             'patient_event_row_key': 'Deterministic row-level key carried '
                                                                      'unchanged from events_patient_event; '
                                                                      'joins one lifecycle row to one '
                                                                      'event-index row.',
                                             'period_business_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                             'period_oid': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                             'person_address_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                             'person_attribute_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                             'person_death_evidence_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                             'person_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                             'person_id_resolved': 'True when person_id is null or resolves '
                                                                   'to the governed person spine; false '
                                                                   'records unresolved linkage without '
                                                                   'dropping the row.',
                                             'person_identifier_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                             'person_relationship_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                             'pharmacy_issue_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                             'pm_wait_list_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                             'practitioner_group_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                             'practitioner_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                             'practitioner_identifier_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                             'practitioner_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'practitioner_location_evidence_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                             'pregnancy_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                             'pregnancy_reconciliation_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                             'problem_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                             'procedure_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                             'pt_prot_reg_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                             'ptl_unique_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                             'record_status': 'active, superseded or retracted, normalised '
                                                              "from the source's own status fields; "
                                                              'superseded rows are earlier versions the '
                                                              'source kept.',
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
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                             'request_thread_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                             'research_study_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                             'row_source': 'Physical bronze row source, CURRENT or HIST, '
                                                           'carried unchanged from the public parent; a key '
                                                           'component because the research table is at '
                                                           'physical version grain and patient_event_key is '
                                                           "shared across an entry's versions.",
                                             'rtt_activity_oid': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'sch_event_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                             'score_type': 'Primary-key component carried unchanged from the '
                                                           "public parent; combine it with the companion's "
                                                           'other key columns when joining.',
                                             'sequence_nbr': 'Primary-key component carried unchanged from '
                                                             'the public parent; combine it with the '
                                                             "companion's other key columns when joining.",
                                             'service_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                             'service_key': 'Deterministic v2 key carried unchanged from the '
                                                            'public parent; joins one lifecycle row to that '
                                                            'research row.',
                                             'snapshot_date': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                             'source_daily_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                             'source_identifier_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                             'source_object': 'Source-arm discriminator carried unchanged '
                                                              'from the public parent; use it with '
                                                              'arm-specific native identifiers when joining.',
                                             'source_site': 'Primary-key component carried unchanged from '
                                                            'the public parent; combine it with the '
                                                            "companion's other key columns when joining.",
                                             'source_system_oid': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                             'source_unit': 'Primary-key component carried unchanged from '
                                                            'the public parent; combine it with the '
                                                            "companion's other key columns when joining.",
                                             'source_update_timestamp': 'Native source-system update '
                                                                        'timestamp carried through the '
                                                                        'canonical silver query.',
                                             'source_version_id': 'Native source version identifier carried '
                                                                  'unchanged from the public parent; CURRENT '
                                                                  'rows carry -1. A key component alongside '
                                                                  'row_source.',
                                             'surg_case_proc_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                             'susceptibility_result_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                             'theatre_attendance_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                             'theatre_case_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'theatre_case_milestone_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                             'theatre_implant_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                             'transfusion_event_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                             'transfusion_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                             'treatment_cycle_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                             'treatment_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                             'update_count': 'Primary-key component carried unchanged from '
                                                             'the public parent; combine it with the '
                                                             "companion's other key columns when joining.",
                                             'valid_from_datetime': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                             'value_set_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                             'waiting_list_oid': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                             'waiting_list_snapshot_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.'},
                                 'table': 'Internal quality and batch metadata for clinical_specimen; same '
                                          'row grain as the research table. Join keys: patient_event_key.'},
 'clinical_specimen': {'columns': {'accession_identifier': 'Canonical accession identifier '
                                                           '(pathology_accession_id); never LabNo-derived.',
                                   'body_site_code': 'Source collection body-site code.',
                                   'body_site_snomed_code': 'Source-provided SNOMED body-site code.',
                                   'canonical_accession_status': 'Canonical accession-link state.',
                                   'clinical_details': 'Source clinical details. Identifiable free text; '
                                                       'ig_risk 4',
                                   'conditions': 'Source request conditions. Identifiable free text; ig_risk '
                                                 '4',
                                   'confidentiality_code': 'Source confidentiality code.',
                                   'discipline': 'Pathology discipline from bronze map_pathology_accession: '
                                                 'lab series first, then the majority member WkgCode or '
                                                 'Cerner activity type; the rule is bronze '
                                                 'discipline_rule_id.',
                                   'encounter_id': "Millennium ENCNTR_ID as BIGINT: the row's own Millennium "
                                                   'source encounter, else the single encounter shared by '
                                                   'every Millennium source of its accession; see '
                                                   'encounter_link_method.',
                                   'encounter_link_method': 'How encounter_id was set (PATH_ENCOUNTER_V1). '
                                                            "source_accession: the row's own Millennium "
                                                            'source names the encounter. accession_single: '
                                                            'every current Millennium source of the '
                                                            'alias-resolved accession names this one '
                                                            'encounter (how TFC LIMS rows inherit one once '
                                                            'their accession is merged with the Millennium '
                                                            'order). accession_multiple: those sources name '
                                                            'different encounters, so encounter_id is NULL. '
                                                            'none: the accession has no Millennium source, '
                                                            'so encounter_id is NULL.',
                                   'event_datetime': 'Collection or receipt timestamp.',
                                   'event_datetime_local': 'event_datetime in Europe/London wall-clock time '
                                                           '(GMT/BST). Millennium rows take the bronze '
                                                           '*_LOCAL companions of the same source columns '
                                                           'and fallback order; rows from local-clock '
                                                           'sources (LUNA, PACS/DICOM, TFC LIMS, CCMDS, '
                                                           'MSDS, BadgerNet, EndoBase, SLAM, iWeb, Datix and '
                                                           'similar) carry event_datetime unchanged. '
                                                           'event_datetime is a UTC instant for Millennium '
                                                           'rows, so take local calendar dates and clock '
                                                           'times from this column.',
                                   'event_end_datetime': 'Event end when supplied.',
                                   'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                               'wall-clock time (GMT/BST), by the same rule '
                                                               'as event_datetime_local; null where '
                                                               'event_end_datetime is null.',
                                   'lab_series': 'Source laboratory series.',
                                   'loaded_at': 'ADC_UPDT of the surviving map_pathology_accession row, '
                                                'carried unchanged through identity and QC/public '
                                                'projections. The separately aggregated maximum '
                                                'evidence_loaded_at from alias-resolved '
                                                'map_pathology_accession_source rows is not used here. This '
                                                "is the accession row's load clock, not created_at, specimen "
                                                'collection time or Silver refresh time.',
                                   'normalized_lab_no': 'Matching evidence only — reused lab numbers reach 3',
                                   'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                        'cross-feed join key.',
                                   'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                   'person_resolution_status': 'Person-projection eligibility state.',
                                   'reason': 'Source request reason. Identifiable free text; ig_risk 4',
                                   'record_status': 'Always active in the current specimen projection of '
                                                    'surviving pathology accessions. The input is restricted '
                                                    'to accessions whose ID equals the alias-resolved '
                                                    'survivor ID; canonical_accession_status and '
                                                    'lifecycle_status do not change this literal label.',
                                   'record_status_effective_from': 'Time this immutable accession identifier '
                                                                   'was first assigned.',
                                   'record_status_effective_to': 'Always null as a TIMESTAMP in the current '
                                                                 'specimen projection. Sample, request and '
                                                                 'report dates and accession lifecycle '
                                                                 'fields are not used as status ends.',
                                   'report_datetime': 'Clamped source report timestamp.',
                                   'request_datetime': 'Clamped source request timestamp.',
                                   'research_qi_only': 'Registry doctrine flag; true on 100% of rows today — '
                                                       'describe-only',
                                   'resolved_accession_key': 'Alias-resolved accession key retained as '
                                                             'derivation evidence.',
                                   'sample_datetime': 'Clamped source sample timestamp.',
                                   'source_code': 'Source specimen type code.',
                                   'source_coding_system': 'Source specimen type system.',
                                   'source_display': 'Source specimen-type display (Cerner code value '
                                                     'description), else the code.',
                                   'source_feed': 'Registered owning feed.',
                                   'source_history_row_count': 'accession_source rows behind this accession.',
                                   'source_update_timestamp': 'Time this immutable accession identifier was '
                                                              'first assigned.',
                                   'specimen_type_code': 'Native specimen-type code.',
                                   'specimen_type_map_candidate_count': 'Number of candidate targets '
                                                                        'observed; a mapped parent is '
                                                                        'unambiguous when this is 1.',
                                   'specimen_type_map_candidate_set_id': 'Stable identifier of the retained '
                                                                         'candidate set.',
                                   'specimen_type_map_competing_count': 'Candidates within 0.05 cosine of '
                                                                        'the selected candidate.',
                                   'specimen_type_map_component_count': 'Number of source-derived '
                                                                        'components.',
                                   'specimen_type_map_component_index': 'One-based source-derived component '
                                                                        'index.',
                                   'specimen_type_map_confidence': 'Mapping confidence in [0,1] where the '
                                                                   'lane measured one.',
                                   'specimen_type_map_cosine': 'Measured cosine between the normalised '
                                                               'source and selected target; null when '
                                                               'scoring did not run.',
                                   'specimen_type_map_method': 'Mapping method: source_native, rule, '
                                                               'exact_synonym or embedding_proposed.',
                                   'specimen_type_map_rollup_levels': 'Maximum hierarchy distance to a '
                                                                      'selected common ancestor.',
                                   'specimen_type_map_rule_id': 'Stable rule or lane identifier that '
                                                                'produced the mapping.',
                                   'specimen_type_map_scoring_model': 'Embedding model that produced '
                                                                      'map_cosine.',
                                   'specimen_type_map_status': 'Explicit mapping outcome from the S3b status '
                                                               'vocabulary.',
                                   'specimen_type_map_status_reason': 'Reason for the mapping outcome or '
                                                                      'abstention.',
                                   'specimen_type_map_version': 'Version of the source, terminology release '
                                                                'or model used by the mapping.',
                                   'specimen_type_omop_concept_id': 'OMOP concept identifier selected with '
                                                                    'the specimen type mapping.',
                                   'specimen_type_omop_source_concept_id': 'Non-standard OMOP source concept '
                                                                           'identifier, kept separate from a '
                                                                           'standard target.',
                                   'specimen_type_snomed_code': 'SNOMED CT code selected for the specimen '
                                                                'type; null when no unambiguous mapping is '
                                                                'available.',
                                   'specimen_type_snomed_display': 'SNOMED CT preferred display selected for '
                                                                   'the specimen type.',
                                   'specimen_type_source_code': 'Source code for the specimen type, retained '
                                                                'without de-identification.',
                                   'specimen_type_source_display': 'Source display for the specimen type.',
                                   'specimen_type_source_system': 'Canonical coding-system URI for the '
                                                                  'source specimen type.',
                                   'specimen_type_target_code': 'Code of the selected target in its own '
                                                                'vocabulary.',
                                   'specimen_type_target_display': 'Display of the selected target in its '
                                                                   'own vocabulary.',
                                   'specimen_type_target_system': 'Coding-system URI of the selected target.',
                                   'subject_id_system': 'Identifier system used for subject_key.',
                                   'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                  'identifier. Not salted, not secret; a stable join key '
                                                  'across feeds where person_id is unresolved.',
                                   'tlcs_requested': 'Requested TLC context. Identifiable free text; ig_risk '
                                                     '4',
                                   'urgent_flag': 'Source urgent-request indicator code (Y',
                                   'vip_ind': 'Indicator that the source record carries VIP handling status '
                                              'for each specimen record. It is produced by the silver '
                                              'transformation and has no direct bronze-column lineage entry. '
                                              'Null means the source did not state the indicator and must '
                                              'not be interpreted as false.',
                                   'withheld_identity_ind': 'Withheld identity indicator.'},
                       'table': 'One canonical pathology accession with collection and request evidence.'}}
