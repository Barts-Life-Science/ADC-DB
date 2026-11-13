"""Silver clinical.presenting_complaint: 3 dataset(s).

- datasets: _clinical_qc_presenting_complaint, clinical_presenting_complaint, _clinical_presenting_complaint_metadata
- moved verbatim from: 2_silver_clinical_events:275, 2_silver_clinical_events:284, 2_silver_clinical_events:305, 2_silver_clinical_events:312, 2_silver_clinical_events:323

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.cross_qc import _s3_source_axis_from_columns
from silver_core.journey_core import _n, axis_columns, spark
from silver_tables._common.journey import _presenting_complaint_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / presenting complaint

# ==== journey_clinical.presenting_complaint (S3b, P2) ====
PRESENTING_COMPLAINT_PUBLIC_COLUMNS = [
    "patient_event_key", "source_patient_event_key", "encounter_key", "encounter_id", "subject_key",
    "subject_id_system", "person_id", "source_object", "source_event_id", "statement_sequence",
    "event_datetime", "event_datetime_local", "encounter_class", "source_coding_system", "source_code", "source_display",
    "complaint_text_normalised", "complaint_group_display", *axis_columns("complaint"),
    "record_status", "record_status_effective_from", "record_status_effective_to", "confidentiality_code",
    "vip_ind", "withheld_identity_ind", "source_update_timestamp", "loaded_at",
]


PRESENTING_COMPLAINT_LIFECYCLE_COLUMNS = [
    "patient_event_key", "source_patient_event_key", "identity_status", "person_id_resolved",
    "encounter_id_resolved", "event_before_birth", "event_after_death_30d", "load_batch_id",
]


@materialized_view(name=_n("journey_clinical._qc_presenting_complaint"), private=True, refresh_policy="auto")
def _qc_presenting_complaint():
    # Build the declared dataset: Private source-grain stage for presenting complaint and reason
    # for visit.
    return _cross_qc_primitive(_presenting_complaint_canonical(), "presenting_complaint", {})


@materialized_view(name=_n("journey_clinical.presenting_complaint"),
                   cluster_by=["person_id", "event_datetime"], refresh_policy="auto")
def presenting_complaint():
    # Build the declared dataset: One presenting-complaint or reason-for-visit component per
    # encounter and source statement.
    return _cross_qc_public(spark.read.table(_n("journey_clinical._qc_presenting_complaint")),
                            "presenting_complaint", {}, PRESENTING_COMPLAINT_PUBLIC_COLUMNS,
                            PRESENTING_COMPLAINT_LIFECYCLE_COLUMNS)


@materialized_view(name=_n("journey_clinical._presenting_complaint_metadata"),
                   refresh_policy="auto")
def presenting_complaint_lifecycle():
    # Build the declared dataset: Internal quality metadata at presenting-complaint component
    # grain.
    public = spark.read.table(_n("journey_clinical.presenting_complaint")).select(
        "patient_event_key", "source_patient_event_key")
    qc = spark.read.table(_n("journey_clinical._qc_presenting_complaint")).alias("q")
    return (public.alias("p").join(qc, F.col("p.source_patient_event_key") == F.col("q.patient_event_key"))
            .select(F.col("p.patient_event_key").alias("patient_event_key"),
                    F.col("p.source_patient_event_key").alias("source_patient_event_key"),
                    *[F.col("q." + c).alias(c) for c in PRESENTING_COMPLAINT_LIFECYCLE_COLUMNS
                      if c not in ("patient_event_key", "source_patient_event_key")]))


def _prepare_presenting_complaint(df):
    """presenting_complaint (SRC_LINEAGE_V1): the complaint lane's source fields from the row's scalar source columns,
    as bronze derives them (mapping_chain _s3_mapping_steps, presenting_complaint: _s3_direct_axis on
    source_coding_system/source_code/source_display). The model has no JSON bridge, so before this step the fields
    came only from the bronze mapping STRUCT."""
    return _s3_source_axis_from_columns(
        df, "complaint", F.col("source_coding_system"), F.col("source_code"), F.col("source_display"))


CROSS_QC = {
    'presenting_complaint': CrossQc(model='presenting_complaint', fk=('encounter_id', 'person_id'), dates=('event_after_death_30d', 'event_before_birth'), extra_public_columns=('source_text',), promote_lifecycle=['identity_status', 'person_id_resolved', 'encounter_id_resolved'], prepare_steps=(_prepare_presenting_complaint,)),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {
    'clinical_presenting_complaint': ValuePolicy(table='clinical_presenting_complaint', enabled=True),
}


WRAP_POLICY = {'clinical_presenting_complaint'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_presenting_complaint_metadata': {'columns': {'activity_date': 'Primary-key component carried '
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
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                         'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                         'care_contact_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                         'care_participation_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                         'cds_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                         'chemotherapy_course_id': 'Native source identifier '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                         'community_care_activity_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                         'community_care_contact_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                         'concept_map_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                         'cost_line_item_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                         'dcp_forms_activity_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                         'doc_response_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                         'elective_access_procedure_key': 'Deterministic v2 '
                                                                                          'key carried '
                                                                                          'unchanged from '
                                                                                          'the public '
                                                                                          'parent; joins one '
                                                                                          'lifecycle row to '
                                                                                          'that research '
                                                                                          'row.',
                                                         'encounter_attribute_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
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
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
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
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
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
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                                                  'timestamp is more than 30 '
                                                                                  'days after the resolved '
                                                                                  "person's death timestamp; "
                                                                                  'silver records the doubt '
                                                                                  'and does not filter the '
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
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                         'gene_tested_row_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                         'genetic_result_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                         'genetic_test_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                         'journey_participant_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
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
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                         'organization_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                         'pacs_examination_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                         'pacs_report_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                         'pathway_oid': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                         'patient_event_key': 'Deterministic SHA-256 event '
                                                                              'key carried unchanged from '
                                                                              'the public parent; joins one '
                                                                              'lifecycle row to that event '
                                                                              'row.',
                                                         'patient_event_row_key': 'Deterministic row-level '
                                                                                  'key carried unchanged '
                                                                                  'from '
                                                                                  'events_patient_event; '
                                                                                  'joins one lifecycle row '
                                                                                  'to one event-index row.',
                                                         'period_business_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
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
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
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
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                         'person_relationship_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                         'pharmacy_issue_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                         'pm_wait_list_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                         'practitioner_group_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                         'practitioner_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                         'practitioner_identifier_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
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
                                                                                               'row to that '
                                                                                               'research '
                                                                                               'row.',
                                                         'pregnancy_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                         'pregnancy_reconciliation_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                         'problem_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                         'procedure_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                         'pt_prot_reg_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                         'ptl_unique_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                         'record_status': 'active, superseded or retracted, '
                                                                          "normalised from the source's own "
                                                                          'status fields; superseded rows '
                                                                          'are earlier versions the source '
                                                                          'kept.',
                                                         'record_status_effective_from': 'Timestamp from '
                                                                                         'which the '
                                                                                         'normalised record '
                                                                                         'status applies, '
                                                                                         'when the source '
                                                                                         'supplies one.',
                                                         'record_status_effective_to': 'Timestamp until '
                                                                                       'which the normalised '
                                                                                       'record status '
                                                                                       'applies; null means '
                                                                                       'no source end is '
                                                                                       'known.',
                                                         'referral_oid': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                         'report_version_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
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
                                                                       'patient_event_key is shared across '
                                                                       "an entry's versions.",
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
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
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
                                                         'source_daily_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                         'source_unit': 'Primary-key component carried '
                                                                        'unchanged from the public parent; '
                                                                        "combine it with the companion's "
                                                                        'other key columns when joining.',
                                                         'source_update_timestamp': 'Native source-system '
                                                                                    'update timestamp '
                                                                                    'carried through the '
                                                                                    'canonical silver query.',
                                                         'source_version_id': 'Native source version '
                                                                              'identifier carried unchanged '
                                                                              'from the public parent; '
                                                                              'CURRENT rows carry -1. A key '
                                                                              'component alongside '
                                                                              'row_source.',
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
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                         'theatre_case_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                         'theatre_case_milestone_key': 'Deterministic v2 key '
                                                                                       'carried unchanged '
                                                                                       'from the public '
                                                                                       'parent; joins one '
                                                                                       'lifecycle row to '
                                                                                       'that research row.',
                                                         'theatre_implant_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                         'transfusion_event_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
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
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.'},
                                             'table': 'Internal quality metadata at presenting-complaint '
                                                      'component grain.'},
 '_clinical_qc_presenting_complaint': {'columns': {},
                                       'table': 'Private source-grain stage for presenting complaint and '
                                                'reason for visit.'},
 'clinical_presenting_complaint': {'columns': {'complaint_group_display': 'S3b presenting-complaint field '
                                                                          'retained at encounter, '
                                                                          'source-statement and component '
                                                                          'grain.',
                                               'complaint_map_candidate_count': 'Number of candidate targets '
                                                                                'observed; a mapped parent '
                                                                                'is unambiguous when this is '
                                                                                '1.',
                                               'complaint_map_candidate_set_id': 'Stable identifier of the '
                                                                                 'retained candidate set.',
                                               'complaint_map_competing_count': 'Candidates within 0.05 '
                                                                                'cosine of the selected '
                                                                                'candidate.',
                                               'complaint_map_component_count': 'Number of source-derived '
                                                                                'components.',
                                               'complaint_map_component_index': 'One-based source-derived '
                                                                                'component index; later '
                                                                                'components receive a stable '
                                                                                'component event key.',
                                               'complaint_map_confidence': 'Mapping confidence in [0,1] '
                                                                           'where the lane measured one.',
                                               'complaint_map_cosine': 'Cosine similarity between the '
                                                                       'normalised source text and selected '
                                                                       'SNOMED target, measured by '
                                                                       'complaint_map_scoring_model.',
                                               'complaint_map_method': 'Mapping method: source_native, rule, '
                                                                       'exact_synonym or embedding_proposed.',
                                               'complaint_map_rollup_levels': 'Maximum hierarchy distance to '
                                                                              'a selected common ancestor.',
                                               'complaint_map_rule_id': 'Stable rule or lane identifier that '
                                                                        'produced the mapping.',
                                               'complaint_map_scoring_model': 'Embedding model that produced '
                                                                              'map_cosine.',
                                               'complaint_map_status': 'Explicit mapping outcome from the '
                                                                       'S3b status vocabulary.',
                                               'complaint_map_status_reason': 'Reason for the mapping '
                                                                              'outcome or abstention.',
                                               'complaint_map_version': 'Version of the source, terminology '
                                                                        'release or model used by the '
                                                                        'mapping.',
                                               'complaint_omop_concept_id': 'OMOP concept identifier '
                                                                            'selected with the presenting '
                                                                            'complaint mapping.',
                                               'complaint_omop_source_concept_id': 'Non-standard OMOP source '
                                                                                   'concept identifier, kept '
                                                                                   'separate from a standard '
                                                                                   'target.',
                                               'complaint_snomed_code': 'SNOMED CT code selected for the '
                                                                        'presenting complaint; null when no '
                                                                        'unambiguous mapping is available.',
                                               'complaint_snomed_display': 'SNOMED CT preferred display '
                                                                           'selected for the presenting '
                                                                           'complaint.',
                                               'complaint_source_code': 'Source code for the presenting '
                                                                        'complaint, retained without '
                                                                        'de-identification.',
                                               'complaint_source_display': 'Source display for the '
                                                                           'presenting complaint.',
                                               'complaint_source_system': 'Canonical coding-system URI for '
                                                                          'the source presenting complaint.',
                                               'complaint_target_code': 'Code of the selected target in its '
                                                                        'own vocabulary.',
                                               'complaint_target_display': 'Display of the selected target '
                                                                           'in its own vocabulary.',
                                               'complaint_target_system': 'Coding-system URI of the selected '
                                                                          'target.',
                                               'complaint_text_normalised': 'S3b presenting-complaint field '
                                                                            'retained at encounter, '
                                                                            'source-statement and component '
                                                                            'grain.',
                                               'confidentiality_code': 'S3b presenting-complaint field '
                                                                       'retained at encounter, '
                                                                       'source-statement and component '
                                                                       'grain.',
                                               'encounter_class': 'S3b presenting-complaint field retained '
                                                                  'at encounter, source-statement and '
                                                                  'component grain.',
                                               'encounter_id': 'S3b presenting-complaint field retained at '
                                                               'encounter, source-statement and component '
                                                               'grain.',
                                               'encounter_key': 'S3b presenting-complaint field retained at '
                                                                'encounter, source-statement and component '
                                                                'grain.',
                                               'event_datetime': 'S3b presenting-complaint field retained at '
                                                                 'encounter, source-statement and component '
                                                                 'grain.',
                                               'event_datetime_local': 'event_datetime in Europe/London '
                                                                       'wall-clock time (GMT/BST). '
                                                                       'Millennium rows take the bronze '
                                                                       '*_LOCAL companions of the same '
                                                                       'source columns and fallback order; '
                                                                       'rows from local-clock sources (LUNA, '
                                                                       'PACS/DICOM, TFC LIMS, CCMDS, MSDS, '
                                                                       'BadgerNet, EndoBase, SLAM, iWeb, '
                                                                       'Datix and similar) carry '
                                                                       'event_datetime unchanged. '
                                                                       'event_datetime is a UTC instant for '
                                                                       'Millennium rows, so take local '
                                                                       'calendar dates and clock times from '
                                                                       'this column.',
                                               'loaded_at': 'S3b presenting-complaint field retained at '
                                                            'encounter, source-statement and component '
                                                            'grain.',
                                               'patient_event_key': 'S3b presenting-complaint field retained '
                                                                    'at encounter, source-statement and '
                                                                    'component grain.',
                                               'person_id': 'S3b presenting-complaint field retained at '
                                                            'encounter, source-statement and component '
                                                            'grain.',
                                               'record_status': 'S3b presenting-complaint field retained at '
                                                                'encounter, source-statement and component '
                                                                'grain.',
                                               'record_status_effective_from': 'S3b presenting-complaint '
                                                                               'field retained at encounter, '
                                                                               'source-statement and '
                                                                               'component grain.',
                                               'record_status_effective_to': 'S3b presenting-complaint field '
                                                                             'retained at encounter, '
                                                                             'source-statement and component '
                                                                             'grain.',
                                               'source_code': 'S3b presenting-complaint field retained at '
                                                              'encounter, source-statement and component '
                                                              'grain.',
                                               'source_coding_system': 'S3b presenting-complaint field '
                                                                       'retained at encounter, '
                                                                       'source-statement and component '
                                                                       'grain.',
                                               'source_display': 'S3b presenting-complaint field retained at '
                                                                 'encounter, source-statement and component '
                                                                 'grain.',
                                               'source_event_id': 'S3b presenting-complaint field retained '
                                                                  'at encounter, source-statement and '
                                                                  'component grain.',
                                               'source_object': 'S3b presenting-complaint field retained at '
                                                                'encounter, source-statement and component '
                                                                'grain.',
                                               'source_patient_event_key': 'S3b presenting-complaint field '
                                                                           'retained at encounter, '
                                                                           'source-statement and component '
                                                                           'grain.',
                                               'source_text': 'Free text the patient or registration clerk '
                                                              'recorded (registration reason for visit, ED '
                                                              'free-text complaint). These arms carry no '
                                                              'code, so source_code is NULL on them; the '
                                                              'exact ECDS pick-list mapping applies only to '
                                                              'coded rows. INSR_W2 W2.04 (L2982).',
                                               'source_update_timestamp': 'S3b presenting-complaint field '
                                                                          'retained at encounter, '
                                                                          'source-statement and component '
                                                                          'grain.',
                                               'statement_sequence': 'S3b presenting-complaint field '
                                                                     'retained at encounter, '
                                                                     'source-statement and component grain.',
                                               'subject_id_system': 'S3b presenting-complaint field retained '
                                                                    'at encounter, source-statement and '
                                                                    'component grain.',
                                               'subject_key': 'S3b presenting-complaint field retained at '
                                                              'encounter, source-statement and component '
                                                              'grain.',
                                               'vip_ind': 'S3b presenting-complaint field retained at '
                                                          'encounter, source-statement and component grain.',
                                               'withheld_identity_ind': 'S3b presenting-complaint field '
                                                                        'retained at encounter, '
                                                                        'source-statement and component '
                                                                        'grain.'},
                                   'table': 'One presenting-complaint or reason-for-visit component per '
                                            'encounter and source statement.'}}
