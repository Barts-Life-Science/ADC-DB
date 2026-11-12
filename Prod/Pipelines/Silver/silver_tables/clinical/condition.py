"""Silver clinical.condition: 5 dataset(s).

- datasets: _clinical_qc_condition, clinical_condition, _clinical_condition_metadata, _clinical_qc_condition_stage, clinical_condition_stage
- moved verbatim from: 2_silver_clinical_events:449, 2_silver_clinical_events:469, 2_silver_clinical_events:522, 2_silver_clinical_events:596, 2_silver_clinical_events:614, 2_silver_clinical_events:7754, 2_silver_clinical_events:7776, 2_silver_clinical_events:7884

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.cross_qc import _s3_direct_axis
from silver_core.journey_core import _decode_display, _n, spark
from silver_tables._common.journey import (
    S3_TABLE_AXIS_SPECS,
    _condition_stage_canonical,
    _s3_replace_public_variant,
)
from silver_tables.clinical._shared import CONDITION_STAGE_LIFECYCLE_COLUMNS, _s3_component_metadata


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical /  qc condition
@materialized_view(
    name=_n("journey_clinical._qc_condition"),
    refresh_policy="auto",
)
def _qc_condition():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for condition.
    source = (
        spark.read.table(_n("journey_clinical._problem_revision_grouped"))
        .withColumn("revision_history_count", F.col("_revision_history_count"))
    )
    return _cross_qc_primitive(
        source,
        "condition",
        {"condition_code": "_condition_code_json"},
    )


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
CONDITION_PUBLIC_COLUMNS = [  # VDB_SILVER_COND_LOCAL_V1: + Europe/London companions
    'patient_event_key',
    'source_object',
    'diagnosis_id',
    'problem_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'encounter_slice_id',  # ENCOUNTER_SLICE_V1
    'event_datetime',
    'event_end_datetime',
    'event_datetime_local',
    'event_end_datetime_local',
    'abatement_datetime_local',
    'source_coding_system',
    'source_code',
    'source_display',
    'condition_code',
    'category_code',
    'category_display',
    # DIAG_PRIORITY_V1: diagnosis ranking.
    'diag_priority',
    'clinical_diag_priority',
    'ranking_code',
    'ranking_display',
    'clinical_status_code',
    'clinical_status_display',
    'verification_status_code',
    'verification_status_display',
    'onset_datetime',
    'abatement_datetime',
    'body_site_code',
    'body_site_display',
    'severity_code',
    'severity_display',
    'laterality_code',
    'laterality_display',
    'asserted_datetime',
    'asserter_practitioner_id',
    'recorder_practitioner_id',
    'revision_history_count',
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


CONDITION_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'diagnosis_id',
    'problem_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


@materialized_view(
    name=_n("journey_clinical.condition"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def condition():
    # Build the declared dataset: Diagnosis and problem assertions from registered bronze feeds;
    # no cross-feed deduplication or active-only filtering.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_condition")),
        "condition",
        {"condition_code": "_condition_code_json"},
        CONDITION_PUBLIC_COLUMNS,
        CONDITION_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._condition_metadata'),
    refresh_policy="auto",
)
def condition_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_condition;
    # same row grain as the research table. Join keys: patient_event_key, source_object,
    # diagnosis_id, problem_id.
    return _s3_component_metadata("journey_clinical.condition", "journey_clinical._qc_condition", CONDITION_LIFECYCLE_COLUMNS)


# Clinical /  qc condition stage
@materialized_view(
    name=_n("journey_clinical._qc_condition_stage"),
    refresh_policy="auto",
)
def _qc_condition_stage():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # condition_stage.
    return _cross_qc_primitive(
        _condition_stage_canonical(),
        "condition_stage",
        {"stage_code": "_qc_stage_code_json"},
    )


# Clinical / condition stage

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
CONDITION_STAGE_PUBLIC_COLUMNS = [
    'patient_event_key',
    'aria_pt_id',
    'aria_dx_id',
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
    'stage_code',
    'stage_of_disease',
    'stage_criteria',
    'dx_type',
    'dx_confirmed',
    'dx_method',
    'history_ind',
    'current_entry_ind',
    'cause_of_death_ind',
    'onset_datetime',
    'resolution_datetime',
    'clinical_description',
    'dx_comment',
    'person_link_status',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]


@materialized_view(
    name=_n("journey_clinical.condition_stage"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def condition_stage():
    # Build the declared dataset: One diagnosis-and-staging assertion from the frozen ARIA
    # source, retaining its composite identifiers and staging details.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_condition_stage")),
        "condition_stage",
        {"stage_code": "_qc_stage_code_json"},
        CONDITION_STAGE_PUBLIC_COLUMNS,
        CONDITION_STAGE_LIFECYCLE_COLUMNS,
    )


if "source_patient_event_key" not in CONDITION_PUBLIC_COLUMNS:
    CONDITION_PUBLIC_COLUMNS.insert(CONDITION_PUBLIC_COLUMNS.index("patient_event_key") + 1, "source_patient_event_key")


for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_condition"].items():
    _s3_replace_public_variant(CONDITION_PUBLIC_COLUMNS, old_name, axes)


for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_condition_stage"].items():
    _s3_replace_public_variant(CONDITION_STAGE_PUBLIC_COLUMNS, old_name, axes)


def _finish_condition(df):
    """condition: per-row finish step, moved verbatim from _cross_qc_finish (silver_journey_shared.py:1273)."""
    # INSR_W2 W2.04 (L2380): the diagnosis arm carried ACTIVE_STATUS_CD (code set 48) with no display. Code values
    # are unique across code sets, so the problem arm's LIFE_CYCLE_STATUS_CD values never match set 48.
    df = _decode_display(df, "clinical_status_code", "clinical_status_display", 48)
    return df


def _prepare_condition_stage(df):
    """condition_stage: per-row prepare step, moved verbatim from _cross_qc_prepare (silver_journey_shared.py:1100)."""
    df = _s3_direct_axis(
        df, "stage_group", F.lit("urn:barts:aria:stage-of-disease"),
        F.lower(F.trim(F.col("stage_of_disease"))), F.col("stage_of_disease"))
    return df


CROSS_QC = {
    'condition': CrossQc(model='condition', fk=('encounter_id', 'person_id'), dates=('event_after_death_30d', 'event_before_birth'), coded_axes={'condition_code': ('condition', False)}, extra_public_columns=('clinical_status_effective_datetime', 'onset_precision_flag', 'onset_precision_code', 'condition_omop_map_rule_id', 'condition_omop_map_version'), promote_lifecycle=['identity_status', 'person_id_resolved', 'encounter_id_resolved'], finish_steps=(_finish_condition,)),
    'condition_stage': CrossQc(model='condition_stage', dates=('event_after_death_30d', 'event_before_birth'), coded_axes={'stage_code': ('stage_diagnosis', False)}, extra_public_columns=('stage_source_kind', 'stage_source_id', 'stage_prefix', 't_stage', 'n_stage', 'm_stage', 'stage_text', 'stage_rule_id', 'source_row_hash'), promote_lifecycle=['identity_status'], prepare_steps=(_prepare_condition_stage,)),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {
    'clinical_condition': ValuePolicy(table='clinical_condition', enabled=True, zero_as_null=('asserter_practitioner_id', 'encounter_slice_id', 'recorder_practitioner_id'), system_user_columns={'recorder_practitioner_id': 'recorder'}),
    'clinical_condition_stage': ValuePolicy(table='clinical_condition_stage', enabled=True),
}


WRAP_POLICY = {'clinical_condition', 'clinical_condition_stage'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_condition_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged from '
                                                               'the public parent; combine it with the '
                                                               "companion's other key columns when joining.",
                                              'activity_record_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                              'admission_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                              'allergy_instance_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
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
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                              'community_care_contact_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                              'concept_map_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                              'dicom_path': 'Primary-key component carried unchanged from '
                                                            'the public parent; combine it with the '
                                                            "companion's other key columns when joining.",
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
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                              'encounter_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                              'endobase_exam_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                              'endobase_exam_term_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                              'entity_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
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
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                              'episode_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                              'episode_id_resolved': 'True when episode_id is null or '
                                                                     'resolves to the governed episode '
                                                                     'spine; false records unresolved '
                                                                     'linkage without dropping the row.',
                                              'episode_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
                                              'event_after_death_30d': 'True when the event timestamp is '
                                                                       'more than 30 days after the resolved '
                                                                       "person's death timestamp; silver "
                                                                       'records the doubt and does not '
                                                                       'filter the row.',
                                              'event_before_birth': 'True when the event timestamp precedes '
                                                                    "the resolved person's birth timestamp; "
                                                                    'silver records the doubt and does not '
                                                                    'filter the row.',
                                              'event_id': 'Native source identifier carried unchanged from '
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                              'extract_cd': 'Primary-key component carried unchanged from '
                                                            'the public parent; combine it with the '
                                                            "companion's other key columns when joining.",
                                              'fhx_activity_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                              'gene_tested_row_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                              'genetic_result_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                              'genetic_test_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                              'homecare_request_item_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                              'identity_status': 'Subject identity resolution state recorded '
                                                                 'by the silver canonical query.',
                                              'implant_event_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
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
                                              'journey_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
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
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
                                              'organization_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                              'organization_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                              'pacs_examination_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                              'pacs_report_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                              'pathway_oid': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
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
                                              'person_attribute_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                              'person_death_evidence_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                              'practitioner_group_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                              'practitioner_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                              'practitioner_identifier_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                              'practitioner_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                              'practitioner_location_evidence_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
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
                                              'rtt_activity_oid': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                              'sch_event_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                              'score_type': 'Primary-key component carried unchanged from '
                                                            'the public parent; combine it with the '
                                                            "companion's other key columns when joining.",
                                              'sequence_nbr': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                              'service_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                              'service_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
                                              'snapshot_date': 'Primary-key component carried unchanged from '
                                                               'the public parent; combine it with the '
                                                               "companion's other key columns when joining.",
                                              'source_daily_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                              'source_unit': 'Primary-key component carried unchanged from '
                                                             'the public parent; combine it with the '
                                                             "companion's other key columns when joining.",
                                              'source_update_timestamp': 'Native source-system update '
                                                                         'timestamp carried through the '
                                                                         'canonical silver query.',
                                              'source_version_id': 'Native source version identifier carried '
                                                                   'unchanged from the public parent; '
                                                                   'CURRENT rows carry -1. A key component '
                                                                   'alongside row_source.',
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
                                              'transfusion_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                              'treatment_cycle_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                              'treatment_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                              'update_count': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                              'valid_from_datetime': 'Primary-key component carried '
                                                                     'unchanged from the public parent; '
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
                                              'value_set_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                              'waiting_list_oid': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                              'waiting_list_snapshot_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.'},
                                  'table': 'Internal quality and batch metadata for clinical_condition; same '
                                           'row grain as the research table. Join keys: patient_event_key, '
                                           'source_object, diagnosis_id, problem_id.'},
 '_clinical_qc_condition': {'columns': {},
                            'table': 'Private JSON bridge and Gold cross-rule flags for condition.'},
 '_clinical_qc_condition_stage': {'columns': {},
                                  'table': 'Private JSON bridge and Gold cross-rule flags for '
                                           'condition_stage.'},
 'clinical_condition': {'columns': {'abatement_datetime': 'Source abatement or effective-end timestamp.',
                                    'abatement_datetime_local': 'abatement_datetime in Europe/London '
                                                                'wall-clock time (END_EFFECTIVE_DT_TM_LOCAL '
                                                                'under the same before-2100 rule); null on '
                                                                'the maternity arm.',
                                    'asserted_datetime': 'Source assertion timestamp.',
                                    'asserter_practitioner_id': 'Millennium personnel PERSON_ID as BIGINT '
                                                                'when available.',
                                    'body_site_code': 'Body-site code where supplied.',
                                    'body_site_display': 'Body-site display where supplied.',
                                    'category_code': 'Source condition category code.',
                                    'category_display': 'Source condition category display.',
                                    'clinical_diag_priority': 'Millennium DIAGNOSIS.CLINICAL_DIAG_PRIORITY, '
                                                              "verbatim: the clinician's ordering of "
                                                              'diagnoses recorded at the point of care, 1 = '
                                                              'first listed; 0 = not ranked. Set mainly on '
                                                              'clinician-entered Discharge, Working and '
                                                              'Admitting diagnoses; null on coded Final and '
                                                              'Principal diagnoses and on the problem and '
                                                              'maternity arms.',
                                    'clinical_status_code': 'Source clinical-status code.',
                                    'clinical_status_display': 'Source clinical-status display.',
                                    'clinical_status_effective_datetime': 'When the problem took its current '
                                                                          'life-cycle status (map_problem '
                                                                          'LIFE_CYCLE_DT_TM, UTC instant); '
                                                                          'NULL on diagnosis and maternity '
                                                                          'rows. INSR_W2 W2.19 (L931).',
                                    'condition_map_candidate_count': 'Number of candidate targets observed; '
                                                                     'a mapped parent is unambiguous when '
                                                                     'this is 1.',
                                    'condition_map_candidate_set_id': 'Stable identifier of the retained '
                                                                      'candidate set.',
                                    'condition_map_competing_count': 'Candidates within 0.05 cosine of the '
                                                                     'selected candidate.',
                                    'condition_map_component_count': 'Number of source-derived components.',
                                    'condition_map_component_index': 'One-based source-derived component '
                                                                     'index.',
                                    'condition_map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                                'measured one.',
                                    'condition_map_cosine': 'Measured cosine between the normalised source '
                                                            'and selected target; null when scoring did not '
                                                            'run.',
                                    'condition_map_method': 'Mapping method: source_native, rule, '
                                                            'exact_synonym or embedding_proposed.',
                                    'condition_map_rollup_levels': 'Maximum hierarchy distance to a selected '
                                                                   'common ancestor.',
                                    'condition_map_rule_id': 'Stable rule or lane identifier that produced '
                                                             'the mapping.',
                                    'condition_map_scoring_model': 'Embedding model that produced '
                                                                   'map_cosine.',
                                    'condition_map_status': 'Explicit mapping outcome from the S3b status '
                                                            'vocabulary.',
                                    'condition_map_status_reason': 'Reason for the mapping outcome or '
                                                                   'abstention.',
                                    'condition_map_version': 'Version of the source, terminology release or '
                                                             'model used by the mapping.',
                                    'condition_omop_concept_id': 'OMOP concept identifier selected with the '
                                                                 'condition mapping.',
                                    'condition_omop_map_rule_id': 'Rule that filled '
                                                                  'condition_omop_concept_id / '
                                                                  'condition_omop_source_concept_id from the '
                                                                  "OMOP 'Maps to' lookup "
                                                                  '(omop:maps_to:<vocabulary>); NULL when '
                                                                  "those ids came with the row's own "
                                                                  'mapping. INSR_W2 W2.03.',
                                    'condition_omop_map_version': 'Vocabulary release '
                                                                  '(<vocabulary>|<version>) of the lookup '
                                                                  'that filled the condition OMOP ids; NULL '
                                                                  'when the lane filled nothing. INSR_W2 '
                                                                  'W2.03.',
                                    'condition_omop_source_concept_id': 'Non-standard OMOP source concept '
                                                                        'identifier, kept separate from a '
                                                                        'standard target.',
                                    'condition_snomed_code': 'SNOMED CT code selected for the condition; '
                                                             'null when no unambiguous mapping is available.',
                                    'condition_snomed_display': 'SNOMED CT preferred display selected for '
                                                                'the condition.',
                                    'condition_source_code': 'Source code for the condition, retained '
                                                             'without de-identification.',
                                    'condition_source_display': 'Source display for the condition.',
                                    'condition_source_system': 'Canonical coding-system URI for the source '
                                                               'condition.',
                                    'condition_target_code': 'Code of the selected target in its own '
                                                             'vocabulary.',
                                    'condition_target_display': 'Display of the selected target in its own '
                                                                'vocabulary.',
                                    'condition_target_system': 'Coding-system URI of the selected target.',
                                    'confidentiality_code': 'Source confidentiality code.',
                                    'diag_priority': 'Millennium DIAGNOSIS.DIAG_PRIORITY, verbatim: the '
                                                     "clinical coder's sequence number for the diagnosis "
                                                     'within its encounter, 1 = primary diagnosis; 0 = not '
                                                     'ranked. Set on coded Final and Principal ICD-10 '
                                                     'diagnoses; null on the problem and maternity arms. '
                                                     'About 13% of coded encounters have more than one '
                                                     'diagnosis at 1 (the same code repeated, or one primary '
                                                     'per consultant episode). Rows are diagnosis_id x map '
                                                     'component, so a primary diagnosis that maps to several '
                                                     'concepts appears on several rows: count primaries with '
                                                     'COUNT(DISTINCT diagnosis_id) WHERE diag_priority = 1.',
                                    'diagnosis_id': 'Millennium DIAGNOSIS_ID; primary key for the diagnosis '
                                                    'arm.',
                                    'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                    'encounter_slice_id': 'Millennium ENCNTR_SLICE_ID as BIGINT: the '
                                                          'encounter slice (consultant episode) the '
                                                          'diagnosis was coded against. Clinical coders '
                                                          'record one ranked diagnosis list per slice, so '
                                                          'diag_priority is ranked within it. Diagnosis arm '
                                                          'only; null on the problem and maternity arms.',
                                    'event_datetime': 'Primary assertion timestamp.',
                                    'event_datetime_local': 'event_datetime in Europe/London wall-clock '
                                                            'time, over the bronze *_LOCAL companions of the '
                                                            'same fallback chain (diagnosis: DIAG_DT_TM, '
                                                            'ASSERTED_DT_TM, BEG_EFFECTIVE_DT_TM; problem: '
                                                            'ASSERTED_DT_TM, ONSET_DT_TM, '
                                                            'BEG_EFFECTIVE_DT_TM, earliest_problem_date). '
                                                            "The maternity arm's DIAGDATE_CLEAN is already "
                                                            'local wall-clock and is carried unchanged. '
                                                            'Silver timestamps are UTC instants; take local '
                                                            'calendar dates and clock times from this '
                                                            'column.',
                                    'event_end_datetime': 'Source effective end where supplied.',
                                    'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                'wall-clock time (END_EFFECTIVE_DT_TM_LOCAL '
                                                                'under the same before-2100 rule); null on '
                                                                'the maternity arm.',
                                    'laterality_code': 'Source laterality code.',
                                    'laterality_display': 'Source laterality display.',
                                    'loaded_at': 'Diagnosis and maternity arms carry their own ADC_UPDT. The '
                                                 'problem arm takes greatest(map_problem.ADC_UPDT, maximum '
                                                 'map_problem_history.ADC_UPDT for that PROBLEM_ID), across '
                                                 'all retained revisions rather than only the latest. '
                                                 "Problem-history UPDT_DT_TM is not added to the parent's "
                                                 'source_update_timestamp, which remains its own UPDT_DT_TM; '
                                                 'diagnosis uses its own UPDT_DT_TM and maternity uses '
                                                 'RECORD_UPDATED_DT. The maternity identity join adds no '
                                                 'pregnancy-spine clock, and the union adds no cross-arm '
                                                 'timestamp maximum.',
                                    'onset_datetime': 'Source onset timestamp.',
                                    'onset_precision_code': 'Millennium ONSET_DT_CD (code value qualifying '
                                                            'the onset date; 0, the no-code value, is NULL; '
                                                            'set on about 0.5% of active problems). NULL on '
                                                            'diagnosis and maternity rows. INSR_W2 (L4261).',
                                    'onset_precision_flag': 'Millennium ONSET_DT_FLAG as recorded: the '
                                                            'precision indicator for onset_datetime (0 = '
                                                            'full date and time; a non-zero value means '
                                                            'reduced precision, so the day or month of '
                                                            'onset_datetime may be a placeholder). Set on '
                                                            'every dated problem onset; NULL on diagnosis '
                                                            'and maternity rows. INSR_W2 (L4261).',
                                    'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                         'cross-feed join key.',
                                    'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                    'problem_id': 'Millennium PROBLEM_ID; primary key for the problem arm.',
                                    'ranking_code': 'Millennium RANKING_CD, verbatim (code set 12034: 3310 '
                                                    "Primary, 3311 Secondary, 684907 Tertiary); '0' = not "
                                                    'ranked. Rarely set on diagnoses (about 68,000 of 46.6 '
                                                    'million rows); set on about 119,000 problems. Null on '
                                                    'the maternity arm.',
                                    'ranking_display': 'Display for ranking_code from bronze '
                                                       'map_problem.ranking_desc. Null on the diagnosis arm, '
                                                       'because bronze map_diagnosis carries no ranking '
                                                       'description, and on the maternity arm.',
                                    'record_status': 'Always active. Inactive, deleted and end-dated '
                                                     '(superseded) diagnosis and problem rows are not '
                                                     'brought into silver (ACTIVE_ROWS_V1, '
                                                     'silver_active_rows.py); the maternity-diagnosis arm is '
                                                     'always active.',
                                    'record_status_effective_from': 'ACTIVE_STATUS_DT_TM from map_diagnosis '
                                                                    'or map_problem on the corresponding '
                                                                    'Millennium arm; DIAGDATE_CLEAN from '
                                                                    'map_maternity_diagnosis on the '
                                                                    'maternity arm. Values are carried '
                                                                    'unchanged without a fallback. The '
                                                                    'maternity value is a diagnosis-date '
                                                                    'proxy rather than an independently '
                                                                    'observed row-status change.',
                                    'record_status_effective_to': 'Always null: end-dated diagnosis and '
                                                                  'problem rows are not brought into silver '
                                                                  '(ACTIVE_ROWS_V1).',
                                    'recorder_practitioner_id': 'Millennium personnel PERSON_ID as BIGINT '
                                                                'when available.',
                                    'recorder_system_ind': 'True when the source recorder_practitioner_id '
                                                           'was 1, the Cerner system user (J5); the id is '
                                                           'then published as NULL. NULL when the source id '
                                                           'was NULL. INSR_W2.',
                                    'revision_history_count': 'Number of retained problem revision rows.',
                                    'severity_code': 'Source severity code.',
                                    'severity_display': 'Source severity display.',
                                    'source_code': 'Source condition code',
                                    'source_coding_system': 'Verbatim source coding system.',
                                    'source_display': 'Verbatim source condition display.',
                                    'source_feed': 'Registered source feed owning the assertion.',
                                    'source_object': 'Source arm: diagnosis, problem, or maternity.',
                                    'source_update_timestamp': 'Source application update datetime.',
                                    'subject_id_system': 'Identifier system used for subject_key.',
                                    'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                   'identifier. Not salted, not secret; a stable join key '
                                                   'across feeds where person_id is unresolved.',
                                    'verification_status_code': 'Source verification-status code.',
                                    'verification_status_display': 'Source verification-status display.',
                                    'vip_ind': 'Source VIP indicator.',
                                    'withheld_identity_ind': 'Identity-withholding status is not populated '
                                                             'by the current source projections; null does '
                                                             'not mean identity was not withheld.'},
                        'table': 'Diagnosis and problem assertions from registered bronze feeds; no '
                                 'cross-feed deduplication or active-only filtering.'},
 'clinical_condition_stage': {'columns': {'aria_dx_id': 'ARIA diagnosis identifier; second component of the '
                                                        'ARIA-arm primary key. NULL on Millennium TNM rows.',
                                          'aria_pt_id': 'ARIA patient identifier; first component of the '
                                                        'ARIA-arm primary key. NULL on Millennium TNM rows.',
                                          'cause_of_death_ind': 'ARIA rows: true when CS_OF_DTH_IND is Y '
                                                                'after uppercasing; null source values and '
                                                                'other values become false. NULL on '
                                                                'Millennium TNM rows.',
                                          'clinical_description': 'Clinical diagnosis description.',
                                          'confidentiality_code': 'Security classification when supplied.',
                                          'current_entry_ind': 'ARIA rows: true when CUR_ENTRY_IND is Y '
                                                               'after uppercasing; null source values and '
                                                               'other values become false. NULL on '
                                                               'Millennium TNM rows.',
                                          'dx_comment': 'Free-text comment recorded against the diagnosis '
                                                        'entry.',
                                          'dx_confirmed': 'Diagnosis confirmation state.',
                                          'dx_method': 'Coded value recording the method by which the '
                                                       'diagnosis was made.',
                                          'dx_type': 'Code indicating the type or category of the diagnosis '
                                                     'entry.',
                                          'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                          'available.',
                                          'event_datetime': 'ARIA rows: sentinel-cleaned diagnosis onset. '
                                                            'Millennium TNM rows: STAGE_DT_TM (Uro TNM event '
                                                            'end time, or pathology report issue time), a '
                                                            'UTC instant.',
                                          'event_datetime_local': 'event_datetime in Europe/London '
                                                                  'wall-clock time (GMT/BST). ARIA is a '
                                                                  'local-clock source and carries '
                                                                  'event_datetime unchanged; Millennium TNM '
                                                                  'rows convert the UTC STAGE_DT_TM.',
                                          'event_end_datetime': 'Sentinel-cleaned resolution timestamp.',
                                          'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                      'wall-clock time (GMT/BST), by the '
                                                                      'same rule as event_datetime_local; '
                                                                      'null where event_end_datetime is '
                                                                      'null.',
                                          'history_ind': 'ARIA HX_OF_IND = Y. NULL on Millennium TNM rows.',
                                          'loaded_at': "The arm's bronze ADC_UPDT "
                                                       '(map_aria_diagnosis_staging or '
                                                       'map_cancer_stage_mill), carried unchanged.',
                                          'm_stage': 'M category as recorded. NULL on ARIA rows (bronze '
                                                     'map_cancer_stage_mill.M_STAGE, INSR W1.14). INSR_W2 '
                                                     'W2.20.',
                                          'n_stage': 'N category as recorded. NULL on ARIA rows (bronze '
                                                     'map_cancer_stage_mill.N_STAGE, INSR W1.14). INSR_W2 '
                                                     'W2.20.',
                                          'onset_datetime': 'Source onset timestamp.',
                                          'patient_event_key': 'Deterministic event key derived from source '
                                                               'identifiers; retained as a stable join key.',
                                          'person_id': 'Native Millennium PERSON_ID as BIGINT when resolved.',
                                          'person_link_status': 'Bronze person-link status.',
                                          'record_status': 'ARIA rows: superseded when '
                                                           "coalesce(CUR_ENTRY_IND,'Y') differs from the "
                                                           'exact value Y or SOURCE_PRESENT_IND is false '
                                                           '(null defaults true); otherwise active. '
                                                           'Millennium TNM rows: superseded when '
                                                           'map_cancer_stage_mill.SOURCE_PRESENT_IND is 0, '
                                                           'otherwise active. No retracted status is '
                                                           'emitted.',
                                          'record_status_effective_from': 'ARIA rows: '
                                                                          'map_aria_diagnosis_staging.EVLV_TSTAMP_CLEAN '
                                                                          'carried unchanged; a missing '
                                                                          'value remains null. NULL on '
                                                                          'Millennium TNM rows (no history '
                                                                          'start in the source).',
                                          'record_status_effective_to': "The arm's bronze ADC_UPDT only when "
                                                                        'the row is superseded; otherwise '
                                                                        'null. An ingestion-time proxy, not '
                                                                        'an observed status-transition time.',
                                          'resolution_datetime': 'Source resolution timestamp.',
                                          'source_code': 'ARIA: ICD diagnosis code with diagnosis-name '
                                                         'fallback. Millennium TNM rows: the stated T, N and '
                                                         'M categories as recorded, prefix on T (e.g. pT2a '
                                                         'N0 M0); a Uro TNM event carries one category.',
                                          'source_coding_system': 'http://hl7.org/fhir/sid/icd-10 on ARIA '
                                                                  'rows; urn:barts:tnm-as-recorded on '
                                                                  'Millennium TNM rows (stage_source_kind '
                                                                  'set).',
                                          'source_display': 'ARIA: diagnosis name or description. Millennium '
                                                            'TNM rows: same text as source_code.',
                                          'source_row_hash': 'Payload hash. NULL on ARIA rows (bronze '
                                                             'map_cancer_stage_mill.ROW_HASH, INSR W1.14). '
                                                             'INSR_W2 W2.20.',
                                          'source_update_timestamp': 'ARIA rows: bronze PIPELINE_UPDT_DT_TM '
                                                                     'of map_aria_diagnosis_staging. '
                                                                     'Millennium TNM rows: '
                                                                     'map_cancer_stage_mill.ADC_UPDT (last '
                                                                     'write by cancer_pipeline). A '
                                                                     'processing clock, not a clinical time.',
                                          'stage_criteria': 'Staging criteria description.',
                                          'stage_diagnosis_map_candidate_count': 'Number of candidate '
                                                                                 'targets observed; a mapped '
                                                                                 'parent is unambiguous when '
                                                                                 'this is 1.',
                                          'stage_diagnosis_map_candidate_set_id': 'Stable identifier of the '
                                                                                  'retained candidate set.',
                                          'stage_diagnosis_map_competing_count': 'Candidates within 0.05 '
                                                                                 'cosine of the selected '
                                                                                 'candidate.',
                                          'stage_diagnosis_map_component_count': 'Number of source-derived '
                                                                                 'components.',
                                          'stage_diagnosis_map_component_index': 'One-based source-derived '
                                                                                 'component index.',
                                          'stage_diagnosis_map_confidence': 'Mapping confidence in [0,1] '
                                                                            'where the lane measured one.',
                                          'stage_diagnosis_map_cosine': 'Measured cosine between the '
                                                                        'normalised source and selected '
                                                                        'target; null when scoring did not '
                                                                        'run.',
                                          'stage_diagnosis_map_method': 'Mapping method: source_native, '
                                                                        'rule, exact_synonym or '
                                                                        'embedding_proposed.',
                                          'stage_diagnosis_map_rollup_levels': 'Maximum hierarchy distance '
                                                                               'to a selected common '
                                                                               'ancestor.',
                                          'stage_diagnosis_map_rule_id': 'Stable rule or lane identifier '
                                                                         'that produced the mapping.',
                                          'stage_diagnosis_map_scoring_model': 'Embedding model that '
                                                                               'produced map_cosine.',
                                          'stage_diagnosis_map_status': 'Explicit mapping outcome from the '
                                                                        'S3b status vocabulary.',
                                          'stage_diagnosis_map_status_reason': 'Reason for the mapping '
                                                                               'outcome or abstention.',
                                          'stage_diagnosis_map_version': 'Version of the source, terminology '
                                                                         'release or model used by the '
                                                                         'mapping.',
                                          'stage_diagnosis_omop_concept_id': 'OMOP concept identifier '
                                                                             'selected with the staging '
                                                                             'diagnosis mapping.',
                                          'stage_diagnosis_omop_source_concept_id': 'Non-standard OMOP '
                                                                                    'source concept '
                                                                                    'identifier, kept '
                                                                                    'separate from a '
                                                                                    'standard target.',
                                          'stage_diagnosis_snomed_code': 'SNOMED CT code selected for the '
                                                                         'staging diagnosis; null when no '
                                                                         'unambiguous mapping is available.',
                                          'stage_diagnosis_snomed_display': 'SNOMED CT preferred display '
                                                                            'selected for the staging '
                                                                            'diagnosis.',
                                          'stage_diagnosis_source_code': 'Source code for the staging '
                                                                         'diagnosis, retained without '
                                                                         'de-identification.',
                                          'stage_diagnosis_source_display': 'Source display for the staging '
                                                                            'diagnosis.',
                                          'stage_diagnosis_source_system': 'Canonical coding-system URI for '
                                                                           'the source staging diagnosis.',
                                          'stage_diagnosis_target_code': 'Code of the selected target in its '
                                                                         'own vocabulary.',
                                          'stage_diagnosis_target_display': 'Display of the selected target '
                                                                            'in its own vocabulary.',
                                          'stage_diagnosis_target_system': 'Coding-system URI of the '
                                                                           'selected target.',
                                          'stage_group_map_candidate_count': 'Number of candidate targets '
                                                                             'observed; a mapped parent is '
                                                                             'unambiguous when this is 1.',
                                          'stage_group_map_candidate_set_id': 'Stable identifier of the '
                                                                              'retained candidate set.',
                                          'stage_group_map_competing_count': 'Candidates within 0.05 cosine '
                                                                             'of the selected candidate.',
                                          'stage_group_map_component_count': 'Number of source-derived '
                                                                             'components.',
                                          'stage_group_map_component_index': 'One-based source-derived '
                                                                             'component index.',
                                          'stage_group_map_confidence': 'Mapping confidence in [0,1] where '
                                                                        'the lane measured one.',
                                          'stage_group_map_cosine': 'Measured cosine between the normalised '
                                                                    'source and selected target; null when '
                                                                    'scoring did not run.',
                                          'stage_group_map_method': 'Mapping method: source_native, rule, '
                                                                    'exact_synonym or embedding_proposed.',
                                          'stage_group_map_rollup_levels': 'Maximum hierarchy distance to a '
                                                                           'selected common ancestor.',
                                          'stage_group_map_rule_id': 'Stable rule or lane identifier that '
                                                                     'produced the mapping.',
                                          'stage_group_map_scoring_model': 'Embedding model that produced '
                                                                           'map_cosine.',
                                          'stage_group_map_status': 'Explicit mapping outcome from the S3b '
                                                                    'status vocabulary.',
                                          'stage_group_map_status_reason': 'Reason for the mapping outcome '
                                                                           'or abstention.',
                                          'stage_group_map_version': 'Version of the source, terminology '
                                                                     'release or model used by the mapping.',
                                          'stage_group_omop_concept_id': 'OMOP concept identifier selected '
                                                                         'with the stage group mapping.',
                                          'stage_group_omop_source_concept_id': 'Non-standard OMOP source '
                                                                                'concept identifier, kept '
                                                                                'separate from a standard '
                                                                                'target.',
                                          'stage_group_snomed_code': 'SNOMED CT code selected for the stage '
                                                                     'group; null when no unambiguous '
                                                                     'mapping is available.',
                                          'stage_group_snomed_display': 'SNOMED CT preferred display '
                                                                        'selected for the stage group.',
                                          'stage_group_source_code': 'Source code for the stage group, '
                                                                     'retained without de-identification.',
                                          'stage_group_source_display': 'Source display for the stage group.',
                                          'stage_group_source_system': 'Canonical coding-system URI for the '
                                                                       'source stage group.',
                                          'stage_group_target_code': 'Code of the selected target in its own '
                                                                     'vocabulary.',
                                          'stage_group_target_display': 'Display of the selected target in '
                                                                        'its own vocabulary.',
                                          'stage_group_target_system': 'Coding-system URI of the selected '
                                                                       'target.',
                                          'stage_of_disease': 'Source disease stage.',
                                          'stage_prefix': 'p, c, yp or yc when stated; NULL otherwise. NULL '
                                                          'on ARIA rows (bronze '
                                                          'map_cancer_stage_mill.STAGE_PREFIX, INSR W1.14). '
                                                          'INSR_W2 W2.20.',
                                          'stage_rule_id': 'URO_TNM_EVENTCD_V1 or PTNM_REGEX_V1. NULL on '
                                                           'ARIA rows (bronze map_cancer_stage_mill.RULE_ID, '
                                                           'INSR W1.14). INSR_W2 W2.20.',
                                          'stage_source_id': 'Clinical EVENT_ID, or pathology '
                                                             'report_version_id. NULL on ARIA rows (bronze '
                                                             'map_cancer_stage_mill.SOURCE_ID, INSR W1.14). '
                                                             'INSR_W2 W2.20.',
                                          'stage_source_kind': 'URO_TNM_EVENT or PATH_REPORT_PTNM. NULL on '
                                                               'ARIA rows (bronze '
                                                               'map_cancer_stage_mill.SOURCE_KIND, INSR '
                                                               'W1.14). INSR_W2 W2.20.',
                                          'stage_text': 'The matched statement or the event result text. '
                                                        'NULL on ARIA rows (bronze '
                                                        'map_cancer_stage_mill.STAGE_TEXT, INSR W1.14). '
                                                        'INSR_W2 W2.20.',
                                          'subject_id_system': 'Identifier system used for subject_key.',
                                          'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                         'identifier. Not salted, not secret; a stable join '
                                                         'key across feeds where person_id is unresolved.',
                                          't_stage': 'T category as recorded (e.g. T2a, Tis, Tx). NULL on '
                                                     'ARIA rows (bronze map_cancer_stage_mill.T_STAGE, INSR '
                                                     'W1.14). INSR_W2 W2.20.',
                                          'vip_ind': 'VIP indicator when supplied.',
                                          'withheld_identity_ind': 'Identity-withholding status is not '
                                                                   'populated by the current source '
                                                                   'projections; null does not mean identity '
                                                                   'was not withheld.'},
                              'table': 'One staging assertion: an ARIA diagnosis-and-staging record (frozen '
                                       'source) or a Millennium TNM statement (Uro TNM event or pathology '
                                       'report pTNM, INSR W1.14), with its source identifiers and staging '
                                       'details.'}}
