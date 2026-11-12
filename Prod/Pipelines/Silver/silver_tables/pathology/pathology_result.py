"""Silver pathology.pathology_result: 4 dataset(s).

- datasets: _clinical_pathology_result_primitive, _clinical_qc_pathology_result, clinical_pathology_result, _clinical_pathology_result_metadata
- moved verbatim from: 2_silver_clinical_events:2031, 2_silver_clinical_events:2086, 2_silver_clinical_events:2103, 2_silver_clinical_events:2105, 2_silver_clinical_events:2152, 2_silver_clinical_events:2228, 2_silver_clinical_events:2246, 2_silver_clinical_events:8311

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.cross_qc import _s3_direct_axis
from silver_core.journey_core import _n, read_source, spark
from silver_tables._common.journey import (
    S3_TABLE_AXIS_SPECS,
    SRC_PATHOLOGY,
    _pathology_result_canonical,
    _s3_replace_public_variant,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# contract v2: retain the full canonical shape for internal reuse.
RESULT_SOURCE_COLUMNS = [
    "patient_event_key",
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
    "result_code", '_r1_path_test_mapping', '_r1_path_test_map_key', '_r1_path_test_components_json', '_r1_path_result_mapping', '_r1_path_result_map_key', '_r1_path_result_components_json',
    "pathology_report_key",
    "specimen_key",
    "equivalence_group_key",
    "representation_role",
    "preferred_result_ind",
    "person_projection_status",
    "value_number",
    "value_text",
    "value_datetime",
    "value_concept_id",
    "value_concept_display",
    "operator_concept_id",
    "unit_source_value",
    "ucum_code",
    "unit_concept_id",
    "test_mapping_match_type",  # SDI_PATH_V1
    "test_mapping_rule_id",  # SDI_PATH_V1
    "canonical_ucum_code",  # SDI_PATH_V1
    "value_canonical",  # SDI_PATH_V1
    "canonical_conversion",  # SDI_PATH_V1
    "non_clinical_test_ind",  # BBR_FOLDIN_V1 BBR-9b
    "reference_range_low",
    "reference_range_high",
    "interpretation_code",
    "polarity",
    "finding_axis",
    "result_status",
    "body_site_code",
    "clinician_code",
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
    "_result_snomed_code",
    "_result_snomed_display",
    "_result_omop_code",
    "_result_confidence_tier",
]


RESULT_PRIMITIVE_COLUMNS = [
    "_result_code_json" if c == "result_code" else c for c in RESULT_SOURCE_COLUMNS
]


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
RESULT_PUBLIC_COLUMNS = [
    'patient_event_key',
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
    'result_code',
    'pathology_report_key',
    'specimen_key',
    'equivalence_group_key',
    'representation_role',
    'preferred_result_ind',
    'person_projection_status',
    'value_number',
    'value_text',
    'value_datetime',
    'value_concept_id',
    'value_concept_display',
    'operator_concept_id',
    'unit_source_value',
    'ucum_code',
    'unit_concept_id',
    'test_mapping_match_type',  # SDI_PATH_V1
    'test_mapping_rule_id',  # SDI_PATH_V1
    'canonical_ucum_code',  # SDI_PATH_V1
    'value_canonical',  # SDI_PATH_V1
    'canonical_conversion',  # SDI_PATH_V1
    'non_clinical_test_ind',  # BBR_FOLDIN_V1 BBR-9b
    'reference_range_low',
    'reference_range_high',
    'interpretation_code',
    'polarity',
    'finding_axis',
    'result_status',
    'body_site_code',
    'clinician_code',
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


RESULT_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


# Clinical /  pathology result primitive
SRC_PATHOLOGY_RESULT_EQUIVALENCE = "4_prod.bronze.map_pathology_result_equivalence"


@materialized_view(
    name=_n("journey_clinical._pathology_result_primitive"),
    private=True,
    refresh_policy="auto",
)
def _pathology_result_primitive():
    # Build the declared dataset: Internal repointed result rows; CodeableConcept JSON crosses
    # the incremental boundary — a VARIANT output column on a join-shaped flow crashes Enzyme
    # planning (GROUP_EXPRESSION_TYPE_IS_NOT_ORDERABLE) and forces complete recompute every
    # update.
    raw = read_source(SRC_PATHOLOGY).alias("s")
    # PATH_ENCOUNTER_V1: read the materialized parent link, which also carries the accession's encounter.
    link = spark.read.table(_n("journey_events._pathology_parent_link")).select(
        F.col("source_parent_key").alias("_source_parent_key"),
        F.col("pathology_accession_id").alias("_link_accession_id"),
        F.col("report_series_id").alias("_link_series_id"),
        F.col("accession_encounter_id").alias("_link_accession_encounter_id"),
        F.col("accession_encounter_conflict").alias("_link_accession_encounter_conflict"),
    ).alias("l")
    eq = read_source(SRC_PATHOLOGY_RESULT_EQUIVALENCE).select(
        F.col("source_record_key").alias("_eq_key"),
        F.col("canonical_result_id").alias("_eq_group"),
        F.col("representation_role").alias("_eq_role"),
        F.col("preferred_result_ind").alias("_eq_preferred"),
        F.col("person_projection_status").alias("_eq_person_projection"),
    ).alias("e")
    joined = (
        raw.join(link, raw.source_parent_key == F.col("l._source_parent_key"), "left")
        .join(eq, raw.source_record_key == F.col("e._eq_key"), "left")
        .select("s.*", "l._link_accession_id", "l._link_series_id",
                "l._link_accession_encounter_id", "l._link_accession_encounter_conflict",
                "e._eq_group", "e._eq_role", "e._eq_preferred", "e._eq_person_projection")
    )
    return _pathology_result_canonical(
        joined, include_parent_links=True
    ).select(*RESULT_PRIMITIVE_COLUMNS)


# Clinical /  qc pathology result
@materialized_view(
    name=_n("journey_clinical._qc_pathology_result"),
    refresh_policy="auto",
)
def _qc_pathology_result():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # pathology_result.
    return _cross_qc_primitive(
        spark.read.table(_n("journey_clinical._pathology_result_primitive")),
        "pathology_result",
        {"result_code": "_result_code_json"},
    )


@materialized_view(
    name=_n("journey_clinical.pathology_result"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def pathology_result():
    # Build the declared dataset: One pathology result with typed values, units, ranges and
    # retained mapping evidence.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_pathology_result")),
        "pathology_result",
        {"result_code": "_result_code_json"},
        RESULT_PUBLIC_COLUMNS,
        RESULT_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._pathology_result_metadata'),
    refresh_policy="auto",
)
def pathology_result_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_pathology_result; same row grain as the research table. Join keys:
    # patient_event_key.
    return (spark.read.table(_n("journey_clinical._qc_pathology_result"))).select(*RESULT_LIFECYCLE_COLUMNS)


for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_pathology_result"].items():
    _s3_replace_public_variant(RESULT_PUBLIC_COLUMNS, old_name, axes)


def _prepare_pathology_result(df):
    """pathology_result: per-row prepare step, moved verbatim from _cross_qc_prepare (silver_journey_shared.py:1082)."""
    present = F.col("_result_snomed_code").isNotNull() | F.col("_result_omop_code").isNotNull()
    result_embedding = present & F.lower(F.col("_result_confidence_tier")).rlike("auto")
    df = _s3_direct_axis(
        df, "result", F.concat(F.lit("urn:barts:pathology:result-value:"), F.coalesce(F.col("source_code"), F.lit("unknown"))),
        F.lower(F.trim(F.coalesce(F.col("value_text"), F.col("value_concept_display")))),
        F.coalesce(F.col("value_text"), F.col("value_concept_display")),
        snomed_code=F.col("_result_snomed_code"), snomed_display=F.col("_result_snomed_display"),
        omop_concept_id=F.expr("try_cast(_result_omop_code as bigint)"),
        method=F.when(result_embedding, F.lit("embedding_proposed")).when(present, F.lit("rule")),
        rule=F.when(result_embedding, F.lit("embedding:bronze_pathology_result_auto:tier"))
              .when(present, F.lit("bronze.map_pathology:result_concept")),
        candidates=F.when(present, F.lit(1)).otherwise(F.lit(0)))
    return df


CROSS_QC = {
    'pathology_result': CrossQc(model='pathology_result', fk=('encounter_id', 'person_id'), dates=('event_after_death_30d', 'event_before_birth'), coded_axes={'result_code': ('test', False)}, embedding_lanes=(('test', 'auto[_ -]?high', 'bronze_pathology_test_auto_high'), ('test', 'auto[_ -]?low', 'bronze_pathology_test_auto_low'), ('result', 'auto|embed|vector', 'bronze_pathology_result_auto')), promote_lifecycle=['identity_status', 'person_id_resolved', 'encounter_id_resolved'], prepare_steps=(_prepare_pathology_result,)),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_pathology_result_metadata': {'columns': {'activity_date': 'Primary-key component carried '
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
                                                  'clinical_pathology_result; same row grain as the research '
                                                  'table. Join keys: patient_event_key.'},
 '_clinical_pathology_result_primitive': {'columns': {},
                                          'table': 'Internal repointed result rows; CodeableConcept JSON '
                                                   'crosses the incremental boundary — a VARIANT output '
                                                   'column on a join-shaped flow crashes Enzyme planning '
                                                   '(GROUP_EXPRESSION_TYPE_IS_NOT_ORDERABLE) and forces '
                                                   'complete recompute every update.'},
 '_clinical_qc_pathology_result': {'columns': {},
                                   'table': 'Private JSON bridge and Gold cross-rule flags for '
                                            'pathology_result.'},
 'clinical_pathology_result': {'columns': {'body_site_code': 'Source body-site code.',
                                           'canonical_conversion': 'map_pathology.canonical_conversion: '
                                                                   'g/dL->g/L (x10) | %->mmol/mol (IFCC = '
                                                                   '(NGSP - 2.15) x 10.929, 1 dp) | '
                                                                   'ug/L->ng/L (x1000, troponin T only) | '
                                                                   'unit_label_conflict (troponin I labelled '
                                                                   'ug/L: TFC TROI has held ng/L-scale '
                                                                   'values since 2014, so neither the label '
                                                                   'nor a conversion is trusted; '
                                                                   'value_canonical and canonical_ucum_code '
                                                                   'are NULL) | egfr_unit_implied / '
                                                                   'inr_unit_implied (unit named by the test '
                                                                   'concept; value unchanged) | identity '
                                                                   '(ucum_code kept) | NULL (no numeric '
                                                                   'value, or no unit).',
                                           'canonical_ucum_code': 'map_pathology.canonical_ucum_code: UCUM '
                                                                  'unit of value_canonical '
                                                                  '(SDI_PATH_UNIT_V1): g/L for g/dL results, '
                                                                  'mmol/mol (IFCC) for HbA1c in %, ng/L for '
                                                                  'troponin T in ug/L, mL/min/{1.73_m2} for '
                                                                  'eGFR, {INR} for INR, else ucum_code. NULL '
                                                                  'when value_as_number is NULL, no unit is '
                                                                  'known, or the unit label contradicts the '
                                                                  'values (unit_label_conflict).',
                                           'clinician_code': 'Source clinician code.',
                                           'confidentiality_code': 'Source confidentiality classification '
                                                                   'attached to the record for each '
                                                                   'pathology result record. It is produced '
                                                                   'by the silver transformation and has no '
                                                                   'direct bronze-column lineage entry. '
                                                                   'Source code meanings and sentinel values '
                                                                   'are retained unless the pipeline '
                                                                   'explicitly maps them; null means no code '
                                                                   'was supplied.',
                                           'encounter_id': "Millennium ENCNTR_ID as BIGINT: the row's own "
                                                           'Millennium source encounter, else the single '
                                                           'encounter shared by every Millennium source of '
                                                           'its accession; see encounter_link_method.',
                                           'encounter_link_method': 'How encounter_id was set '
                                                                    '(PATH_ENCOUNTER_V1). source_accession: '
                                                                    "the row's own Millennium source names "
                                                                    'the encounter. accession_single: every '
                                                                    'current Millennium source of the '
                                                                    'alias-resolved accession names this one '
                                                                    'encounter (how TFC LIMS rows inherit '
                                                                    'one once their accession is merged with '
                                                                    'the Millennium order). '
                                                                    'accession_multiple: those sources name '
                                                                    'different encounters, so encounter_id '
                                                                    'is NULL. none: the accession has no '
                                                                    'Millennium source, so encounter_id is '
                                                                    'NULL.',
                                           'equivalence_group_key': 'Derived cross-arm result-equivalence '
                                                                    'key.',
                                           'event_datetime': 'Date and time when the represented clinical or '
                                                             'administrative event occurred for each '
                                                             'pathology result record. It is derived from '
                                                             'bronze field `measurement_datetime` in '
                                                             '`4_prod.bronze.map_pathology`. Source '
                                                             'precision and timezone handling follow the '
                                                             'pipeline expression; null means the time was '
                                                             'unavailable.',
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
                                           'event_end_datetime': 'Measurement end timestamp.',
                                           'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                       'wall-clock time (GMT/BST), by the '
                                                                       'same rule as event_datetime_local; '
                                                                       'null where event_end_datetime is '
                                                                       'null.',
                                           'finding_axis': 'Source finding/result axis.',
                                           'interpretation_code': 'Source interpretation or normalcy.',
                                           'loaded_at': 'map_pathology.ADC_UPDT carried unchanged through '
                                                        'parent-link and result-equivalence joins, '
                                                        'usable-code filtering and QC/public projections. '
                                                        'Those joins select linkage/equivalence fields, not '
                                                        'additional clocks; parent-link loaded_at is not '
                                                        'propagated. This differs from source_adc_updt used '
                                                        'for source_update_timestamp and from Silver refresh '
                                                        'time.',
                                           'non_clinical_test_ind': 'True for a LIMS housekeeping row, not a '
                                                                    'clinical test: SDI_PATH_PLACEHOLDER_V1 '
                                                                    'placeholder codes, internal / '
                                                                    'unused-code results (INTERC, UNUC ...), '
                                                                    'analyser and report bookkeeping, eGFR '
                                                                    'input and factor rows, and ZZZ-prefixed '
                                                                    'tests. Gold drops these rows. Serum '
                                                                    'indices are kept. BBR_FOLDIN_V1 BBR-9b.',
                                           'operator_concept_id': 'Result comparison operator concept.',
                                           'pathology_report_key': 'Deterministic pathology-report series '
                                                                   'key.',
                                           'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                'stable cross-feed join key.',
                                           'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                           'person_projection_status': 'Equivalence-layer person projection '
                                                                       'state.',
                                           'polarity': 'Source result polarity or growth grade.',
                                           'preferred_result_ind': 'Whether this is the preferred '
                                                                   'representation.',
                                           'record_status': 'Superseded when map_pathology.valid_until_dt_tm '
                                                            'is non-null and earlier than 2100-01-01; '
                                                            'otherwise active. No retracted status is '
                                                            'emitted. Result-status text, equivalence '
                                                            'preference and parent linkage do not determine '
                                                            'this label; the usable-code filter controls '
                                                            'eligibility separately.',
                                           'record_status_effective_from': 'Timestamp from which the '
                                                                           'normalised record status '
                                                                           'applies, when the source '
                                                                           'supplies one.',
                                           'record_status_effective_to': 'map_pathology.valid_until_dt_tm '
                                                                         'only when non-null and earlier '
                                                                         'than 2100-01-01; otherwise null. '
                                                                         'The predicate is identical to the '
                                                                         'superseded-status test. Unlike '
                                                                         'event timestamps, this history end '
                                                                         'has no 1950 lower clamp; neither '
                                                                         'event_end_dt_tm nor ingestion time '
                                                                         'is substituted.',
                                           'reference_range_high': 'Reference-range upper bound.',
                                           'reference_range_low': 'Reference-range lower bound.',
                                           'representation_role': 'Result representation role within the '
                                                                  'equivalence group.',
                                           'result_map_candidate_count': 'Number of candidate targets '
                                                                         'observed; a mapped parent is '
                                                                         'unambiguous when this is 1.',
                                           'result_map_candidate_set_id': 'Stable identifier of the retained '
                                                                          'candidate set.',
                                           'result_map_competing_count': 'Candidates within 0.05 cosine of '
                                                                         'the selected candidate.',
                                           'result_map_component_count': 'Number of source-derived '
                                                                         'components.',
                                           'result_map_component_index': 'One-based source-derived component '
                                                                         'index.',
                                           'result_map_confidence': 'Mapping confidence in [0,1] where the '
                                                                    'lane measured one.',
                                           'result_map_cosine': 'Measured cosine between the normalised '
                                                                'source and selected target; null when '
                                                                'scoring did not run.',
                                           'result_map_method': 'Mapping method: source_native, rule, '
                                                                'exact_synonym or embedding_proposed.',
                                           'result_map_rollup_levels': 'Maximum hierarchy distance to a '
                                                                       'selected common ancestor.',
                                           'result_map_rule_id': 'Stable rule or lane identifier that '
                                                                 'produced the mapping.',
                                           'result_map_scoring_model': 'Embedding model that produced '
                                                                       'map_cosine.',
                                           'result_map_status': 'Explicit mapping outcome from the S3b '
                                                                'status vocabulary.',
                                           'result_map_status_reason': 'Reason for the mapping outcome or '
                                                                       'abstention.',
                                           'result_map_version': 'Version of the source, terminology release '
                                                                 'or model used by the mapping.',
                                           'result_omop_concept_id': 'OMOP concept identifier selected with '
                                                                     'the pathology result value mapping.',
                                           'result_omop_source_concept_id': 'Non-standard OMOP source '
                                                                            'concept identifier, kept '
                                                                            'separate from a standard '
                                                                            'target.',
                                           'result_snomed_code': 'SNOMED CT code selected for the pathology '
                                                                 'result value; null when no unambiguous '
                                                                 'mapping is available.',
                                           'result_snomed_display': 'SNOMED CT preferred display selected '
                                                                    'for the pathology result value.',
                                           'result_source_code': 'Source code for the pathology result '
                                                                 'value, retained without de-identification.',
                                           'result_source_display': 'Source display for the pathology result '
                                                                    'value.',
                                           'result_source_system': 'Canonical coding-system URI for the '
                                                                   'source pathology result value.',
                                           'result_status': 'Source result status.',
                                           'result_target_code': 'Code of the selected target in its own '
                                                                 'vocabulary.',
                                           'result_target_display': 'Display of the selected target in its '
                                                                    'own vocabulary.',
                                           'result_target_system': 'Coding-system URI of the selected '
                                                                   'target.',
                                           'source_code': 'Source test code.',
                                           'source_coding_system': 'Source test coding system.',
                                           'source_display': 'Source test display.',
                                           'source_feed': 'Registered owning feed.',
                                           'source_update_timestamp': 'Greatest contributing source update '
                                                                      'timestamp; never changed by '
                                                                      'mapping-only backfills.',
                                           'specimen_key': 'Deterministic specimen key.',
                                           'subject_id_system': 'Identifier system used for subject_key.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'test_map_candidate_count': 'Number of candidate targets '
                                                                       'observed; a mapped parent is '
                                                                       'unambiguous when this is 1.',
                                           'test_map_candidate_set_id': 'Stable identifier of the retained '
                                                                        'candidate set.',
                                           'test_map_competing_count': 'Candidates within 0.05 cosine of the '
                                                                       'selected candidate.',
                                           'test_map_component_count': 'Number of source-derived components.',
                                           'test_map_component_index': 'One-based source-derived component '
                                                                       'index.',
                                           'test_map_confidence': 'Mapping confidence in [0,1] where the '
                                                                  'lane measured one.',
                                           'test_map_cosine': 'Measured cosine between the normalised source '
                                                              'and selected target; null when scoring did '
                                                              'not run.',
                                           'test_map_method': 'Mapping method: source_native, rule, '
                                                              'exact_synonym or embedding_proposed.',
                                           'test_map_rollup_levels': 'Maximum hierarchy distance to a '
                                                                     'selected common ancestor.',
                                           'test_map_rule_id': 'Stable rule or lane identifier that produced '
                                                               'the mapping.',
                                           'test_map_scoring_model': 'Embedding model that produced '
                                                                     'map_cosine.',
                                           'test_map_status': 'Explicit mapping outcome from the S3b status '
                                                              'vocabulary.',
                                           'test_map_status_reason': 'Reason for the mapping outcome or '
                                                                     'abstention.',
                                           'test_map_version': 'Version of the source, terminology release '
                                                               'or model used by the mapping.',
                                           'test_mapping_match_type': 'map_pathology.test_mapping_match_type: '
                                                                      'How measurement_concept_id was set: '
                                                                      'exact_context (the map key of this '
                                                                      'row code and description), '
                                                                      'native_event_cd / native_nlmc (native '
                                                                      'code map), safe_code (every observed '
                                                                      'description of the code maps to one '
                                                                      'concept), cross_arm_display '
                                                                      '(SDI_PATH_XARM_V1: a '
                                                                      'Millennium-linked row takes the '
                                                                      'concept the TFC LIMS arm map gives '
                                                                      'the same analyte display, and that '
                                                                      'arm tier), placeholder_code '
                                                                      '(SDI_PATH_PLACEHOLDER_V1: an '
                                                                      'unused/internal placeholder test '
                                                                      'code, unmapped by design), or '
                                                                      'unmapped.',
                                           'test_mapping_rule_id': 'map_pathology.test_mapping_rule_id: Rule '
                                                                   'that set a cross_arm_display or '
                                                                   'placeholder_code test mapping '
                                                                   '(SDI_PATH_XARM_V1 | '
                                                                   'SDI_PATH_PLACEHOLDER_V1); NULL when the '
                                                                   'row map key, a native map or the '
                                                                   'safe-code rule set it, or nothing did.',
                                           'test_omop_concept_id': 'OMOP concept identifier selected with '
                                                                   'the pathology test mapping.',
                                           'test_omop_source_concept_id': 'Non-standard OMOP source concept '
                                                                          'identifier, kept separate from a '
                                                                          'standard target.',
                                           'test_snomed_code': 'SNOMED CT code selected for the pathology '
                                                               'test; null when no unambiguous mapping is '
                                                               'available.',
                                           'test_snomed_display': 'SNOMED CT preferred display selected for '
                                                                  'the pathology test.',
                                           'test_source_code': 'Source code for the pathology test, retained '
                                                               'without de-identification.',
                                           'test_source_display': 'Source display for the pathology test.',
                                           'test_source_system': 'Canonical coding-system URI for the source '
                                                                 'pathology test.',
                                           'test_target_code': 'Code of the selected target in its own '
                                                               'vocabulary.',
                                           'test_target_display': 'Display of the selected target in its own '
                                                                  'vocabulary.',
                                           'test_target_system': 'Coding-system URI of the selected target.',
                                           'ucum_code': 'UCUM unit code.',
                                           'unit_concept_id': 'OMOP unit concept identifier.',
                                           'unit_source_value': 'Verbatim source unit.',
                                           'value_canonical': 'map_pathology.value_canonical: '
                                                              'value_as_number restated in '
                                                              'canonical_ucum_code (see '
                                                              'canonical_conversion); equal to '
                                                              'value_as_number unless converted. NULL when '
                                                              'canonical_conversion is NULL or '
                                                              'unit_label_conflict.',
                                           'value_concept_display': 'Coded result concept display.',
                                           'value_concept_id': 'Coded result concept identifier.',
                                           'value_datetime': 'Datetime result value.',
                                           'value_number': 'Numeric result value.',
                                           'value_text': 'Verbatim result value.',
                                           'vip_ind': 'Indicator that the source record carries VIP handling '
                                                      'status for each pathology result record. It is '
                                                      'produced by the silver transformation and has no '
                                                      'direct bronze-column lineage entry. Null means the '
                                                      'source did not state the indicator and must not be '
                                                      'interpreted as false.',
                                           'withheld_identity_ind': 'Withheld identity indicator.'},
                               'table': 'One pathology result with typed values, units, ranges and retained '
                                        'mapping evidence.'}}
