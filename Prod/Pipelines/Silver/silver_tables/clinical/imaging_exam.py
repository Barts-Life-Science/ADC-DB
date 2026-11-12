"""Silver clinical.imaging_exam: 3 dataset(s).

- datasets: _clinical_qc_imaging_exam, clinical_imaging_exam, _clinical_imaging_exam_metadata
- moved verbatim from: 2_silver_clinical_events:6817, 2_silver_clinical_events:6888, 2_silver_clinical_events:6906, silver_journey_shared.py:963, silver_journey_shared.py:973

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, spark
from silver_tables._common.clinical_events import (
    IMAGING_EXAM_LIFECYCLE_COLUMNS,
    IMAGING_EXAM_PUBLIC_COLUMNS,
    IMAGING_EXAM_SOURCE_COLUMNS,
)
from silver_tables._common.journey import (
    SDI_DICOM_MODALITIES,
    SRC_PACS_EXAMINATION,
    _imaging_exam_canonical,
    _mill_radiology_exam_canonical,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


SDI_PACS_NON_ACQUISITION = ["OT", "SR", "PR", "KO", "DOC"]


def _sdi_imaging_modality(df):
    """SDI_DICOM_MODALITY_V1: a Millennium exam linked to one PACS examination (PACS_EXAMINATION_ID is unique) takes that
    examination's acquisition modality; otherwise the bronze NHSI-category inference stands."""
    m = F.upper(F.trim(F.col("MODALITY")))
    px = (read_source(SRC_PACS_EXAMINATION)
          .where(m.isin(*SDI_DICOM_MODALITIES) & ~m.isin(*SDI_PACS_NON_ACQUISITION))
          .select(F.col("PACS_EXAMINATION_ID").cast("bigint").alias("_sdi_px_id"), m.alias("_sdi_px_modality")))
    df = df.join(px, (F.col("source_object") == F.lit("millennium"))
                 & (F.col("linked_pacs_examination_id") == F.col("_sdi_px_id")), "left")
    use = F.col("_sdi_px_modality").isNotNull()
    return (df.withColumn("modality_dicom_code", F.when(use, F.col("_sdi_px_modality")).otherwise(F.col("modality_dicom_code")))
              .withColumn("modality_dicom_method", F.when(use, F.lit("pacs_link")).otherwise(F.col("modality_dicom_method")))
              .drop("_sdi_px_id", "_sdi_px_modality"))


# Clinical /  qc imaging exam
@materialized_view(
    name=_n("journey_clinical._qc_imaging_exam"),
    refresh_policy="auto",
)
def _qc_imaging_exam():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # imaging_exam.
    source = (
        _imaging_exam_canonical(integration=True).select(*IMAGING_EXAM_SOURCE_COLUMNS)
        .unionByName(
            _mill_radiology_exam_canonical(integration=True).select(*IMAGING_EXAM_SOURCE_COLUMNS)
        )
    )
    return _cross_qc_primitive(
        source,
        "imaging_exam",
        {"exam_code": "_qc_exam_code_json"},
    )


@materialized_view(
    name=_n("journey_clinical.imaging_exam"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def imaging_exam():
    # Build the declared dataset: One PACS examination/study from the governed bronze landing
    # with report linkage.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_imaging_exam")),
        "imaging_exam",
        {"exam_code": "_qc_exam_code_json"},
        IMAGING_EXAM_PUBLIC_COLUMNS,
        IMAGING_EXAM_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._imaging_exam_metadata'),
    refresh_policy="auto",
)
def imaging_exam_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_imaging_exam;
    # same row grain as the research table. Join keys: patient_event_key, source_object,
    # event_id, pacs_examination_id.
    return (spark.read.table(_n("journey_clinical._qc_imaging_exam"))).select(*IMAGING_EXAM_LIFECYCLE_COLUMNS)


def _finish_imaging_exam(df):
    """imaging_exam: per-row finish step, moved verbatim from _cross_qc_finish (silver_journey_shared.py:1271)."""
    # SDI_DICOM_MODALITY_V1: keyed on the row's linked PACS examination
    df = _sdi_imaging_modality(df)
    return df


CROSS_QC = {
    'imaging_exam': CrossQc(model='imaging_exam', fk=('encounter_id', 'person_id'), dates=('event_after_death_30d', 'event_before_birth'), coded_axes={'exam_code': ('exam', False)}, promote_lifecycle=['identity_status', 'person_id_resolved', 'encounter_id_resolved'], finish_steps=(_finish_imaging_exam,)),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_imaging_exam_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
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
                                              'clinical_imaging_exam; same row grain as the research table. '
                                              'Join keys: patient_event_key, source_object, event_id, '
                                              'pacs_examination_id.'},
 '_clinical_qc_imaging_exam': {'columns': {},
                               'table': 'Private JSON bridge and Gold cross-rule flags for imaging_exam.'},
 'clinical_imaging_exam': {'columns': {'accession_candidate_count': 'PACS: published examinations sharing '
                                                                    'the accession. Millennium: present PACS '
                                                                    'examinations sharing the parsed '
                                                                    'accession before code and identity '
                                                                    'filtering.',
                                       'accession_evidence_level': 'pacs_request (PACS request accession), '
                                                                   'cerner_reference_pacs_linked (Cerner '
                                                                   'reference resolved to one PACS '
                                                                   'examination), cerner_reference_parsed '
                                                                   '(Cerner-only parsed reference), or NULL '
                                                                   'when no accession.',
                                       'accession_format': 'Bronze accession-rule format class (SECTRA_16, '
                                                           'SITE_16, RNH_14, OTHER_SITE_14, NUMERIC_16, '
                                                           'LEGACY_7_8, LEGACY_6, NUMERIC_OTHER, OTHER or '
                                                           'UNRECOGNISED).',
                                       'accession_identifier': 'Imaging accession identifier, unchanged: '
                                                               'PACS MILL_LINK_REF, Millennium '
                                                               'REFERENCE_NBR. For the Barts/Sectra '
                                                               'extraction accession use '
                                                               'sectra_accession_number.',
                                       'accession_identity_status': 'Row-level accession identity evidence. '
                                                                    'PACS: ACCESSION_IDENTITY_STATUS over '
                                                                    "the accession's PACS examinations. "
                                                                    'Millennium: CONFLICTING_PERSONS when '
                                                                    'the PACS link was refused for identity '
                                                                    'conflict, otherwise NULL. The cross-arm '
                                                                    'accession status is on '
                                                                    'clinical_imaging_accession_member.',
                                       'accession_parse_method': 'PACS: ACCESSION_PARSE_METHOD. Millennium: '
                                                                 'REF_PARSE_METHOD. Provenance of '
                                                                 'sectra_accession_number.',
                                       'accession_rule_version': 'Version of the shared bronze accession '
                                                                 'rules that produced '
                                                                 'sectra_accession_number.',
                                       'body_site_code': 'Source body-site value as recorded (PACS '
                                                         'BODY_PART); not a SNOMED code.',
                                       'body_site_snomed_code': 'SNOMED CT body structure derived from the '
                                                                'mapped examination concept '
                                                                '(exam_omop_concept_id) through the OMOP '
                                                                'procedure-site relationships: Has dir proc '
                                                                'site, else Has proc site, else Has indir '
                                                                'proc site; the lowest concept id when '
                                                                'several share that rank.',
                                       'body_site_snomed_display': 'SNOMED CT preferred name for '
                                                                   'body_site_snomed_code.',
                                       'body_site_snomed_method': 'omop_has_dir_proc_site | '
                                                                  'omop_has_proc_site | '
                                                                  'omop_has_indir_proc_site, with a '
                                                                  '_multiple suffix when the examination '
                                                                  'names several sites at that rank (only '
                                                                  'one is published). NULL when the '
                                                                  'examination has no mapped concept or no '
                                                                  'site relationship.',
                                       'confidentiality_code': 'Source confidentiality classification '
                                                               'attached to the record for each imaging exam '
                                                               'record. It is produced by the silver '
                                                               'transformation and has no direct '
                                                               'bronze-column lineage entry. Source code '
                                                               'meanings and sentinel values are retained '
                                                               'unless the pipeline explicitly maps them; '
                                                               'null means no code was supplied.',
                                       'encounter_id': 'Millennium ENCNTR_ID when supplied; native encounter '
                                                       'foreign key.',
                                       'event_datetime': 'Imaging study start timestamp.',
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
                                       'event_end_datetime': 'Imaging study end timestamp.',
                                       'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                   'wall-clock time (GMT/BST), by the same '
                                                                   'rule as event_datetime_local; null where '
                                                                   'event_end_datetime is null.',
                                       'event_id': 'Millennium radiology EVENT_ID; native key for the '
                                                   'millennium arm.',
                                       'exam_map_candidate_count': 'Number of candidate targets observed; a '
                                                                   'mapped parent is unambiguous when this '
                                                                   'is 1.',
                                       'exam_map_candidate_set_id': 'Stable identifier of the retained '
                                                                    'candidate set.',
                                       'exam_map_competing_count': 'Candidates within 0.05 cosine of the '
                                                                   'selected candidate.',
                                       'exam_map_component_count': 'Number of source-derived components.',
                                       'exam_map_component_index': 'One-based source-derived component '
                                                                   'index.',
                                       'exam_map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                              'measured one.',
                                       'exam_map_cosine': 'Measured cosine between the normalised source and '
                                                          'selected target; null when scoring did not run.',
                                       'exam_map_method': 'Mapping method: source_native, rule, '
                                                          'exact_synonym or embedding_proposed.',
                                       'exam_map_rollup_levels': 'Maximum hierarchy distance to a selected '
                                                                 'common ancestor.',
                                       'exam_map_rule_id': 'Stable rule or lane identifier that produced the '
                                                           'mapping.',
                                       'exam_map_scoring_model': 'Embedding model that produced map_cosine.',
                                       'exam_map_status': 'Explicit mapping outcome from the S3b status '
                                                          'vocabulary.',
                                       'exam_map_status_reason': 'Reason for the mapping outcome or '
                                                                 'abstention.',
                                       'exam_map_version': 'Version of the source, terminology release or '
                                                           'model used by the mapping.',
                                       'exam_omop_concept_id': 'OMOP concept identifier selected with the '
                                                               'imaging examination mapping.',
                                       'exam_omop_source_concept_id': 'Non-standard OMOP source concept '
                                                                      'identifier, kept separate from a '
                                                                      'standard target.',
                                       'exam_snomed_code': 'SNOMED CT code selected for the imaging '
                                                           'examination; null when no unambiguous mapping is '
                                                           'available.',
                                       'exam_snomed_display': 'SNOMED CT preferred display selected for the '
                                                              'imaging examination.',
                                       'exam_source_code': 'Source code for the imaging examination, '
                                                           'retained without de-identification.',
                                       'exam_source_display': 'Source display for the imaging examination.',
                                       'exam_source_system': 'Canonical coding-system URI for the source '
                                                             'imaging examination.',
                                       'exam_target_code': 'Code of the selected target in its own '
                                                           'vocabulary.',
                                       'exam_target_display': 'Display of the selected target in its own '
                                                              'vocabulary.',
                                       'exam_target_system': 'Coding-system URI of the selected target.',
                                       'linked_pacs_examination_id': 'PACS examination this row resolves to. '
                                                                     'PACS rows: their own '
                                                                     'PACS_EXAMINATION_ID. Millennium rows: '
                                                                     'map_radiology_event.PACS_EXAMINATION_ID, '
                                                                     'set only for a unique accession+code '
                                                                     'or unique accession-only match.',
                                       'linked_report_event_id': 'Millennium: RADIOLOGY_REPORT_EVENT_ID, the '
                                                                 'latest identity-compatible report document '
                                                                 'event. A convenience pointer; other '
                                                                 'reports stay relational. NULL for PACS '
                                                                 'rows.',
                                       'loaded_at': 'PACS: map_pacs_examination.ADC_UPDT. Millennium: '
                                                    'map_radiology_event.PIPELINE_UPDT_DT_TM, not its '
                                                    'ADC_UPDT. These are arm-specific load/processing '
                                                    'clocks, carried unchanged through the union and '
                                                    'QC/public projections; no report or cross-arm clock '
                                                    'maximum is taken and the current Silver refresh time is '
                                                    'not substituted.',
                                       'measured_series_count': 'SERIES_COUNT_MEASURED from the raw series '
                                                                'table. The series feed has a separate clock '
                                                                'and ends 2024-11-02; NULL after that.',
                                       'modality_code': 'Local modality label as recorded: PACS MODALITY, or '
                                                        'the Millennium NHS Imaging category '
                                                        '(NHSI_MODALITY_CATEGORY). Not a DICOM code; see '
                                                        'modality_dicom_code.',
                                       'modality_dicom_code': 'DICOM Modality (0008,0060) defined term. '
                                                              'PACS: MODALITY when it is a DICOM term. '
                                                              "Millennium: the linked PACS examination's "
                                                              'acquisition modality, else the bronze '
                                                              'NHSI-category inference '
                                                              '(map_radiology_event.MODALITY_DICOM). NULL '
                                                              'when no evidence names a single modality.',
                                       'modality_dicom_method': 'How modality_dicom_code was set: pacs (PACS '
                                                                'MODALITY is a DICOM term), pacs_non_dicom '
                                                                '(PACS MODALITY is a site label, e.g. '
                                                                'Pano/XDEXA; no code), pacs_link (Millennium '
                                                                'exam, linked PACS examination), '
                                                                'dicom_passthrough / nhsi_category_map / '
                                                                'nhsi_category_mixed / unmapped (bronze '
                                                                'NHSI-category inference). NULL when no '
                                                                'modality was recorded.',
                                       'native_image_count': 'Native Sectra IMAGE_COUNT (sparsely populated; '
                                                             'PACS feed clock). Not evidence of present '
                                                             'retrievability.',
                                       'native_series_count': 'Native Sectra SERIES_COUNT (sparsely '
                                                              'populated).',
                                       'organization_key': 'Nullable deterministic organization key; no '
                                                           'organization expression is present in the '
                                                           'imaging source cells.',
                                       'pacs_examination_id': 'PACS_EXAMINATION_ID; native key for the pacs '
                                                              'arm.',
                                       'pacs_link_method': 'NATIVE_PACS_EXAMINATION for PACS rows; the '
                                                           'bronze PACS_LINK_METHOD for Millennium rows. '
                                                           'Ambiguous and conflicting candidates are never '
                                                           'resolved by choice.',
                                       'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                            'cross-feed join key.',
                                       'performed_datetime': 'Valid performed time. PACS: '
                                                             'PERFORMED_DT_TM_CLEAN (examination time, else '
                                                             'the documented arrival fallback; future and '
                                                             '2099 sentinel values are NULL). Millennium: '
                                                             'PERFORMED_DT_TM_CLEAN. Never a scheduled, '
                                                             'request, report or load time. event_datetime '
                                                             'keeps its existing meaning.',
                                       'performed_datetime_source': 'Which clean clock performed_datetime '
                                                                    'came from (EXAMINATION_DT_TM, '
                                                                    'ARRIVAL_DT_TM or PERFORMED_DT_TM).',
                                       'performed_evidence': 'PACS: bronze PERFORMED_EVIDENCE set (STATUS, '
                                                             'REPORT, IMAGES, SERIES). Millennium: '
                                                             'RESULT_STATUS for an examination event whose '
                                                             'status is not cancelled, in error or not done.',
                                       'performed_evidence_verified_ind': 'True only for verified performed '
                                                                          'evidence. PACS status 40 '
                                                                          '(booking) and dangling-only '
                                                                          'report links are not evidence. '
                                                                          'Millennium: examination event '
                                                                          'with a performed result status.',
                                       'performer_practitioner_id': 'Nullable Millennium personnel PERSON_ID '
                                                                    'for the performer.',
                                       'person_id': 'Millennium PERSON_ID when resolved; native person '
                                                    'foreign key.',
                                       'record_status': 'PACS rows are retracted when '
                                                        'map_pacs_examination.SOURCE_PRESENT_IND is false, '
                                                        'otherwise active; missing presence does not '
                                                        'retract. Millennium rows are superseded when '
                                                        'map_radiology_event.IN_ERROR_IND is true, otherwise '
                                                        'active; missing error defaults false. The arms are '
                                                        'unioned without reconciling their labels; a '
                                                        'Millennium validity end does not itself supersede a '
                                                        'row.',
                                       'record_status_effective_from': 'PACS: '
                                                                       'map_pacs_examination.SRC_ADC_UPDT. '
                                                                       'Millennium: '
                                                                       'map_radiology_event.VALID_FROM_DT_TM_CLEAN. '
                                                                       'Each arm carries its value through '
                                                                       'the union and QC/public projections; '
                                                                       'no cross-arm maximum or '
                                                                       'clinical-event timestamp is '
                                                                       'substituted.',
                                       'record_status_effective_to': 'PACS: map_pacs_examination.ADC_UPDT '
                                                                     'only when source presence is false, '
                                                                     'otherwise null. Millennium: UPDT_DT_TM '
                                                                     'when IN_ERROR_IND is true, otherwise '
                                                                     'VALID_UNTIL_DT_TM_CLEAN directly, '
                                                                     'including a finite or sentinel end on '
                                                                     'an active row. No sentinel filtering '
                                                                     'or fallback between Millennium '
                                                                     'branches is applied.',
                                       'report_available_ind': 'PACS: REPORT_AVAILABLE_IND (at least one '
                                                               'resolved report; a dangling report id never '
                                                               'sets it). Millennium: a report document '
                                                               'event is linked through REF_EXAM_KEY.',
                                       'report_count': 'PACS: REPORT_COUNT_RESOLVED. Millennium: '
                                                       'identity-compatible report document events sharing '
                                                       'REF_EXAM_KEY.',
                                       'report_link_status': 'PACS: RESOLVED, DANGLING_ONLY or NONE. '
                                                             'Millennium: REPORT_LINK_STATUS (SELF, SIBLING, '
                                                             'SIBLING_LATEST_OF_MULTIPLE, AMBIGUOUS or '
                                                             'NONE). All links are in '
                                                             'clinical_imaging_report_link.',
                                       'report_patient_event_key': 'Deterministic SHA-256 key of the linked '
                                                                   'report event.',
                                       'report_reference_count': 'PACS: REPORT_REF_COUNT, including dangling '
                                                                 'report ids. NULL for Millennium rows.',
                                       'request_clinical_history': 'PACS request clinical information '
                                                                   '(CLINICAL_ANAMNESIS), verbatim and '
                                                                   'request-wide. Identifiable free text '
                                                                   '(ig_risk 4).',
                                       'request_clinical_history_anonymised': 'Approved-lane anonymised '
                                                                              'clinical history; same rule '
                                                                              'as '
                                                                              'request_clinical_question_anonymised.',
                                       'request_clinical_history_exam_segment': 'The clinical-history '
                                                                                'section headed by this '
                                                                                "examination's code; set "
                                                                                'only when its segment '
                                                                                'status is EXAM_SEGMENT. '
                                                                                'Identifiable free text '
                                                                                '(ig_risk 4).',
                                       'request_clinical_history_exam_segment_anonymised': 'Approved-lane '
                                                                                           'anonymised '
                                                                                           'examination-specific '
                                                                                           'history segment; '
                                                                                           'same rule.',
                                       'request_clinical_history_segment_status': 'Same vocabulary as '
                                                                                  'request_clinical_question_segment_status.',
                                       'request_clinical_question': 'PACS request clinical question, '
                                                                    'verbatim and request-wide. Identifiable '
                                                                    'free text (ig_risk 4); never copied '
                                                                    'into accession summaries or anonymised '
                                                                    'serving projections.',
                                       'request_clinical_question_anonymised': 'Approved-lane anonymised '
                                                                               'clinical question. NULL '
                                                                               'unless the registered '
                                                                               'request-text feed reports '
                                                                               'anonymized for the current '
                                                                               'source text hash. Never '
                                                                               'falls back to raw text or a '
                                                                               "report's anonymised text.",
                                       'request_clinical_question_exam_segment': 'The clinical-question '
                                                                                 'section headed by this '
                                                                                 "examination's code; set "
                                                                                 'only when its segment '
                                                                                 'status is EXAM_SEGMENT. '
                                                                                 'Identifiable free text '
                                                                                 '(ig_risk 4).',
                                       'request_clinical_question_exam_segment_anonymised': 'Approved-lane '
                                                                                            'anonymised '
                                                                                            'examination-specific '
                                                                                            'question '
                                                                                            'segment; same '
                                                                                            'rule.',
                                       'request_clinical_question_segment_status': 'EXAM_SEGMENT, '
                                                                                   'NOT_SEGMENTED (text is '
                                                                                   'request-wide), '
                                                                                   'CODE_NOT_IN_SEGMENTS, '
                                                                                   'DUPLICATE_SEGMENT_CODE, '
                                                                                   'DUPLICATE_EXAM_CODE_ON_REQUEST, '
                                                                                   'NO_EXAM_CODE or NO_TEXT.',
                                       'request_text_anonymisation_status': 'not_registered (feed not yet '
                                                                            'deployed), current (anonymized '
                                                                            'for the current text), stale '
                                                                            '(text changed since redaction), '
                                                                            'pending, failed, '
                                                                            'unresolved_person, or no_text. '
                                                                            'Millennium rows are NULL.',
                                       'requester_practitioner_id': 'Nullable Millennium personnel PERSON_ID '
                                                                    'for the requester.',
                                       'sectra_accession_number': 'Barts/Sectra extraction accession. PACS: '
                                                                  'trimmed REQUEST_ID_STRING '
                                                                  '(map_pacs_examination.SECTRA_ACCESSION_NBR). '
                                                                  'Millennium: REFERENCE_NBR parsed by the '
                                                                  'shared bronze accession rules. Scopes an '
                                                                  'extraction request: one accession can '
                                                                  'hold several examinations and study UIDs. '
                                                                  'Leading zeros preserved. Distinct from '
                                                                  'accession_identifier, which keeps its '
                                                                  'existing meaning. Identifier.',
                                       'source_code': 'Source exam code',
                                       'source_coding_system': 'Source exam coding system.',
                                       'source_display': 'Source exam display.',
                                       'source_event_class_code': 'Millennium EVENT_CLASS_CD (234 '
                                                                  'examination, 224 report document, 231 '
                                                                  'section); NULL for PACS rows.',
                                       'source_feed': 'Registered PACS or Millennium radiology source feed.',
                                       'source_object': 'Native source arm: millennium or pacs.',
                                       'source_status_code': 'Native status. PACS: EXAMINATION_STATUS_CD. '
                                                             'Millennium: RESULT_STATUS_CD.',
                                       'source_update_timestamp': 'PACS: map_pacs_examination.SRC_ADC_UPDT. '
                                                                  'Millennium: '
                                                                  'map_radiology_event.UPDT_DT_TM. These '
                                                                  'distinct clocks are carried per arm '
                                                                  'through the union, not combined as a '
                                                                  'maximum. Neither joined report clocks nor '
                                                                  'the current Silver refresh time replaces '
                                                                  'them.',
                                       'status_code': 'Imaging study status.',
                                       'study_instance_uid': 'DICOM study instance UID.',
                                       'subject_id_system': 'Identifier system used for subject_key.',
                                       'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                      'identifier. Not salted, not secret; a stable join key '
                                                      'across feeds where person_id is unresolved.',
                                       'vip_ind': 'Indicator that the source record carries VIP handling '
                                                  'status for each imaging exam record. It is produced by '
                                                  'the silver transformation and has no direct bronze-column '
                                                  'lineage entry. Null means the source did not state the '
                                                  'indicator and must not be interpreted as false.',
                                       'withheld_identity_ind': 'Identity-withholding status is not '
                                                                'populated by the current source '
                                                                'projections; null does not mean identity '
                                                                'was not withheld.'},
                           'table': 'One PACS examination/study from the governed bronze landing with report '
                                    'linkage.'}}
