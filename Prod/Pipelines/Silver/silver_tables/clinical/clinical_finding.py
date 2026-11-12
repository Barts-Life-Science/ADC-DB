"""Silver clinical.clinical_finding: 3 dataset(s).

- datasets: _clinical_qc_clinical_finding, clinical_clinical_finding, _clinical_clinical_finding_metadata
- moved verbatim from: 2_silver_clinical_events:6465, 2_silver_clinical_events:6510, 2_silver_clinical_events:6554, 2_silver_clinical_events:6567, 2_silver_clinical_events:6600, 2_silver_clinical_events:6672, 2_silver_clinical_events:6690, 2_silver_clinical_events:8335

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.cross_qc import _r1_attach_silver_arm, _s3_direct_axis
from silver_core.journey_core import _n, codeable_concept, coding_obj, spark
from silver_core.value_policy import drop_time_only, time_part
from silver_tables._common.journey import (
    S3_TABLE_AXIS_SPECS,
    _coded_finding_typed,
    _date_finding_typed,
    _nomen_finding_typed,
    _s3_replace_public_variant,
    _text_finding_typed,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Assertions and generic event routing

# ==== Assertions and generic event routing ====

# contract v2: retain the full canonical shape for internal reuse.
CLINICAL_FINDING_SOURCE_COLUMNS = [
    "patient_event_key",
    "event_id",
    "sequence_nbr",
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
    "finding_code", '_r1_finding_mapping', '_r1_finding_map_key', '_r1_finding_components_json', '_r1_finding_value_mapping', '_r1_finding_value_map_key', '_r1_finding_value_components_json',
    "finding_kind",
    "value_text",
    "value_datetime",
    "value_code",
    "value_display",
    "normalcy_code",
    "normalcy_display",
    "result_status_code",
    "result_status_display",
    "order_id",
    "parent_event_id",
    "performer_practitioner_id",
    "verifier_practitioner_id",
    "organization_id",
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
CLINICAL_FINDING_PUBLIC_COLUMNS = [
    'patient_event_key',
    'event_id',
    'sequence_nbr',
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
    'finding_code',
    'finding_kind',
    'value_text',
    'value_datetime',
    'value_code',
    'value_display',
    'normalcy_code',
    'normalcy_display',
    'result_status_code',
    'result_status_display',
    'order_id',
    'parent_event_id',
    'performer_practitioner_id',
    'verifier_practitioner_id',
    'organization_id',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
    'registry_field_id',
    'event_datetime_status',
]


CLINICAL_FINDING_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'event_id',
    'sequence_nbr',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


def _registry_finding_canonical():
    # Assemble normalized registry finding rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = spark.read.table(_n("journey_clinical._registry_observations")).where("route='finding'")
    s = _r1_attach_silver_arm(s, "_clinical_registry_observations", "clinical_finding")
    return s.select(
        "patient_event_key", F.lit(None).cast("bigint").alias("event_id"), F.lit(1).cast("bigint").alias("sequence_nbr"),
        F.concat(F.lit("registry:"), F.col("registry_product")).alias("source_object"), "subject_key", "subject_id_system", "person_id",
        F.when(F.col("person_id").isNotNull(), F.lit("resolved")).otherwise(F.lit("unresolved")).alias("identity_status"),
        "encounter_id", "event_datetime", "event_end_datetime", "event_datetime_local", "event_end_datetime_local", F.lit("urn:barts:registry-field").alias("source_coding_system"),
        F.col("registry_field_id").alias("source_code"), F.col("question_display").alias("source_display"),
        codeable_concept(coding_obj(F.lit("urn:barts:registry-field"), F.col("registry_field_id"), F.col("question_display"), True)).alias("finding_code"), s["_r1_finding_mapping"].alias("_r1_finding_mapping"), s["_r1_finding_map_key"].alias("_r1_finding_map_key"), s["_r1_finding_components_json"].alias("_r1_finding_components_json"), s["_r1_finding_value_mapping"].alias("_r1_finding_value_mapping"), s["_r1_finding_value_map_key"].alias("_r1_finding_value_map_key"), s["_r1_finding_value_components_json"].alias("_r1_finding_value_components_json"),
        F.lit("registry_categorical").alias("finding_kind"), F.col("answer_text").alias("value_text"),
        F.lit(None).cast("timestamp").alias("value_datetime"), F.lit(None).cast("string").alias("value_code"),
        F.col("answer_text").alias("value_display"), F.lit(None).cast("string").alias("normalcy_code"),
        F.lit(None).cast("string").alias("normalcy_display"), F.lit(None).cast("string").alias("result_status_code"),
        F.lit(None).cast("string").alias("result_status_display"), F.lit(None).cast("bigint").alias("order_id"),
        F.lit(None).cast("bigint").alias("parent_event_id"), F.lit(None).cast("bigint").alias("performer_practitioner_id"),
        F.lit(None).cast("bigint").alias("verifier_practitioner_id"), F.lit(None).cast("bigint").alias("organization_id"),
        "record_status", "record_status_effective_from", "record_status_effective_to",
        F.lit(None).cast("string").alias("confidentiality_code"), F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"), F.lit("iweb_registry").alias("source_feed"),
        F.date_format("loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"), "source_update_timestamp", "loaded_at",
        "registry_field_id", "event_datetime_status",
    )


# Clinical /  qc clinical finding
@materialized_view(
    name=_n("journey_clinical._qc_clinical_finding"),
    refresh_policy="auto",
)
def _qc_clinical_finding():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # clinical_finding.
    source = (
        _coded_finding_typed().select(*CLINICAL_FINDING_SOURCE_COLUMNS)
        .unionByName(_nomen_finding_typed().select(*CLINICAL_FINDING_SOURCE_COLUMNS))
        .unionByName(_date_finding_typed().select(*CLINICAL_FINDING_SOURCE_COLUMNS))
        .unionByName(_text_finding_typed().select(*CLINICAL_FINDING_SOURCE_COLUMNS))
        .unionByName(_registry_finding_canonical(), allowMissingColumns=True)
    )
    return _cross_qc_primitive(
        source,
        "clinical_finding",
        {"finding_code": "_qc_finding_code_json"},
    )


@materialized_view(
    name=_n("journey_clinical.clinical_finding"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def clinical_finding():
    # Build the declared dataset: Governed residual clinical observations from exhaustive
    # generic-event routes.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_clinical_finding")),
        "clinical_finding",
        {"finding_code": "_qc_finding_code_json"},
        CLINICAL_FINDING_PUBLIC_COLUMNS,
        CLINICAL_FINDING_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._clinical_finding_metadata'),
    refresh_policy="auto",
)
def clinical_finding_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_clinical_finding; same row grain as the research table. Join keys:
    # patient_event_key, source_object, event_id, sequence_nbr.
    return (spark.read.table(_n("journey_clinical._qc_clinical_finding"))).select(*CLINICAL_FINDING_LIFECYCLE_COLUMNS)


for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_clinical_finding"].items():
    _s3_replace_public_variant(CLINICAL_FINDING_PUBLIC_COLUMNS, old_name, axes)


def _prepare_clinical_finding(df):
    """clinical_finding: per-row prepare step, moved verbatim from _cross_qc_prepare (silver_journey_shared.py:1095)."""
    df = _s3_direct_axis(
        df, "value", F.lit("urn:cerner:nomenclature:answer-text"),
        F.lower(F.trim(F.coalesce(F.col("value_display"), F.col("value_code"), F.col("value_text")))),
        F.coalesce(F.col("value_display"), F.col("value_text")))
    return df


def _finish_clinical_finding(df):
    """clinical_finding: per-row finish step, moved verbatim from _cross_qc_finish (silver_journey_shared.py:1280)."""
    # INSR_W2 W2.04 (F14d, L2433): a cosine measured against no accepted target describes nothing published.
    for axis in ("finding", "value"):
        df = df.withColumn(f"{axis}_map_cosine",
                           F.when(F.col(f"{axis}_target_code").isNotNull(), F.col(f"{axis}_map_cosine")))
    return df


CROSS_QC = {
    'clinical_finding': CrossQc(model='clinical_finding', fk=('encounter_id', 'person_id'), dates=('event_after_death_30d', 'event_before_birth'), coded_axes={'finding_code': ('finding', False)}, promote_lifecycle=['identity_status', 'person_id_resolved', 'encounter_id_resolved'], finish_steps=(_finish_clinical_finding,), prepare_steps=(_prepare_clinical_finding,)),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {
    'clinical_clinical_finding': ValuePolicy(table='clinical_clinical_finding', enabled=True, zero_as_null=('order_id', 'organization_id', 'performer_practitioner_id', 'verifier_practitioner_id'), system_user_columns={'performer_practitioner_id': 'performer', 'verifier_practitioner_id': 'verifier'}),
}


VALUE_POLICIES["clinical_clinical_finding"].register("value_time", ["value_datetime"],
         lambda F: time_part(F.col("value_datetime")),
         "Time of day (HH:mm:ss) when the source recorded a time with no date (Millennium placeholder date "
         "2000-01-01); value_datetime is then NULL. INSR_W2 W2.07 (J4, L2051).")


VALUE_POLICIES["clinical_clinical_finding"].value_rule("value_datetime", ["value_datetime"],
           lambda F: drop_time_only(F.col("value_datetime")))


WRAP_POLICY = {'clinical_clinical_finding'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_clinical_finding_metadata': {'columns': {'activity_date': 'Primary-key component carried '
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
                                                  'clinical_clinical_finding; same row grain as the research '
                                                  'table. Join keys: patient_event_key, source_object, '
                                                  'event_id, sequence_nbr.'},
 '_clinical_qc_clinical_finding': {'columns': {},
                                   'table': 'Private JSON bridge and Gold cross-rule flags for '
                                            'clinical_finding.'},
 'clinical_clinical_finding': {'columns': {'confidentiality_code': 'Source confidentiality code.',
                                           'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                           'spine_encounter.encounter_id.',
                                           'event_datetime': 'Clinically relevant finding timestamp.',
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
                                           'event_datetime_status': 'registry_dated or registry_undated for '
                                                                    'registry observations; null on native '
                                                                    'rows.',
                                           'event_end_datetime': 'Source effective end timestamp.',
                                           'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                       'wall-clock time (GMT/BST), by the '
                                                                       'same rule as event_datetime_local; '
                                                                       'null where event_end_datetime is '
                                                                       'null.',
                                           'event_id': 'Millennium EVENT_ID; native key for coded, '
                                                       'nomenclature, date, and text event arms.',
                                           'finding_kind': 'Generic result representation carried by the '
                                                           'source feed.',
                                           'finding_map_candidate_count': 'Number of candidate targets '
                                                                          'observed; a mapped parent is '
                                                                          'unambiguous when this is 1.',
                                           'finding_map_candidate_set_id': 'Stable identifier of the '
                                                                           'retained candidate set.',
                                           'finding_map_competing_count': 'Candidates within 0.05 cosine of '
                                                                          'the selected candidate.',
                                           'finding_map_component_count': 'Number of source-derived '
                                                                          'components.',
                                           'finding_map_component_index': 'One-based source-derived '
                                                                          'component index.',
                                           'finding_map_confidence': 'Mapping confidence in [0,1] where the '
                                                                     'lane measured one.',
                                           'finding_map_cosine': 'Measured cosine between the normalised '
                                                                 'source and selected target; null when '
                                                                 'scoring did not run.',
                                           'finding_map_method': 'Mapping method: source_native, rule, '
                                                                 'exact_synonym or embedding_proposed.',
                                           'finding_map_rollup_levels': 'Maximum hierarchy distance to a '
                                                                        'selected common ancestor.',
                                           'finding_map_rule_id': 'Stable rule or lane identifier that '
                                                                  'produced the mapping.',
                                           'finding_map_scoring_model': 'Embedding model that produced '
                                                                        'map_cosine.',
                                           'finding_map_status': 'Explicit mapping outcome from the S3b '
                                                                 'status vocabulary.',
                                           'finding_map_status_reason': 'Reason for the mapping outcome or '
                                                                        'abstention.',
                                           'finding_map_version': 'Version of the source, terminology '
                                                                  'release or model used by the mapping.',
                                           'finding_omop_concept_id': 'OMOP concept identifier selected with '
                                                                      'the clinical finding mapping.',
                                           'finding_omop_source_concept_id': 'Non-standard OMOP source '
                                                                             'concept identifier, kept '
                                                                             'separate from a standard '
                                                                             'target.',
                                           'finding_snomed_code': 'SNOMED CT code selected for the clinical '
                                                                  'finding; null when no unambiguous mapping '
                                                                  'is available.',
                                           'finding_snomed_display': 'SNOMED CT preferred display selected '
                                                                     'for the clinical finding.',
                                           'finding_source_code': 'Source code for the clinical finding, '
                                                                  'retained without de-identification.',
                                           'finding_source_display': 'Source display for the clinical '
                                                                     'finding.',
                                           'finding_source_system': 'Canonical coding-system URI for the '
                                                                    'source clinical finding.',
                                           'finding_target_code': 'Code of the selected target in its own '
                                                                  'vocabulary.',
                                           'finding_target_display': 'Display of the selected target in its '
                                                                     'own vocabulary.',
                                           'finding_target_system': 'Coding-system URI of the selected '
                                                                    'target.',
                                           'loaded_at': 'Own ADC_UPDT from map_coded_events, '
                                                        'map_nomen_events, map_date_events or '
                                                        'map_text_events, carried per arm through '
                                                        'routing/usable-code filters, the union and '
                                                        'QC/public projections. The additional result, '
                                                        'clinical-event, lookup and long-text clocks used by '
                                                        'source_update_timestamp are not added here; no '
                                                        'current Silver refresh timestamp is substituted.',
                                           'normalcy_code': 'Source normalcy or interpretation code.',
                                           'normalcy_display': 'Source normalcy or interpretation display.',
                                           'order_id': 'Source order identifier when supplied.',
                                           'organization_id': 'Millennium ORGANIZATION_ID for the source '
                                                              'organization.',
                                           'parent_event_id': 'Source parent-event identifier.',
                                           'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                'stable cross-feed join key.',
                                           'performer_practitioner_id': 'Millennium personnel PERSON_ID for '
                                                                        'the performer.',
                                           'performer_system_ind': 'True when the source '
                                                                   'performer_practitioner_id was 1, the '
                                                                   'Cerner system user (J5); the id is then '
                                                                   'published as NULL. NULL when the source '
                                                                   'id was NULL. INSR_W2.',
                                           'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                           'record_status': 'Coded-event rows are superseded only when the '
                                                            'least CR_VALID_UNTIL_DT_TM and '
                                                            'CE_VALID_UNTIL_DT_TM is non-null and before '
                                                            '2100-01-01; otherwise active, never retracted. '
                                                            'Nomenclature, date and text rows are retracted '
                                                            'for SOURCE_DELETED_IND cast to BOOLEAN true '
                                                            '(null defaults false); otherwise superseded for '
                                                            'the least result/event validity end being '
                                                            'non-null and before 2100-01-01 or '
                                                            'AUTHENTIC_FLAG cast to LONG zero (null defaults '
                                                            'one); otherwise active. Retraction takes '
                                                            'priority, and clinical result-status codes are '
                                                            'separate.',
                                           'record_status_effective_from': 'First non-null '
                                                                           'result-valid-from, '
                                                                           'event-valid-from, then clinical '
                                                                           'event time. Coded/nomenclature '
                                                                           'use CR_VALID_FROM_DT_TM and '
                                                                           'CE_VALID_FROM_DT_TM; date uses '
                                                                           'DATE_RESULT_VALID_FROM_DT_TM and '
                                                                           'CLINICAL_EVENT_VALID_FROM_DT_TM; '
                                                                           'text substitutes '
                                                                           'STRING_RESULT_VALID_FROM_DT_TM. '
                                                                           'Coded event time falls back '
                                                                           'through PERFORMED_DT_TM, '
                                                                           'EVENT_END_DT_TM, '
                                                                           'EVENT_START_DT_TM. Nomenclature '
                                                                           'uses CLINICAL_EVENT_DT_TM, '
                                                                           'EVENT_END_DT_TM, '
                                                                           'PERFORMED_DT_TM, '
                                                                           'EVENT_START_DT_TM. Date/text use '
                                                                           'RESULT_DT_TM, PERFORMED_DT_TM, '
                                                                           'EVENT_END_DT_TM, '
                                                                           'EVENT_START_DT_TM. No '
                                                                           'status-bound clamp or '
                                                                           'ingestion-time fallback is '
                                                                           'added.',
                                           'record_status_effective_to': 'Coded rows use the least '
                                                                         'CR_VALID_UNTIL_DT_TM and '
                                                                         'CE_VALID_UNTIL_DT_TM only when '
                                                                         'superseded, otherwise null. '
                                                                         'Nomenclature, date and text rows '
                                                                         'use their computed '
                                                                         'source_update_timestamp when '
                                                                         'retracted; otherwise their least '
                                                                         'result/event validity end when '
                                                                         'superseded, otherwise null. '
                                                                         'Nomenclature uses CR/CE ends, date '
                                                                         'DATE_RESULT/CLINICAL_EVENT ends '
                                                                         'and text '
                                                                         'STRING_RESULT/CLINICAL_EVENT ends. '
                                                                         'An AUTHENTIC_FLAG-only '
                                                                         'supersession can therefore retain '
                                                                         'a null or future/sentinel end; no '
                                                                         'extra end clamp or fallback is '
                                                                         'applied.',
                                           'registry_field_id': 'Governed SHA-256 registry product and field '
                                                                'identifier for registry observations; null '
                                                                'on native rows.',
                                           'result_status_code': 'Source result-status code.',
                                           'result_status_display': 'Source result-status display.',
                                           'sequence_nbr': 'Sequence number that makes the coded result '
                                                           'primary key unique when a single result spans '
                                                           'more than one row.',
                                           'source_code': 'Verbatim source event code.',
                                           'source_coding_system': 'Verbatim source event coding system.',
                                           'source_display': 'Verbatim source event display.',
                                           'source_feed': 'Registered generic-event feed.',
                                           'source_object': 'Native source arm: coded_event, nomen_event, '
                                                            'date_event, or text_event.',
                                           'source_update_timestamp': 'Per-arm greatest contributing clock, '
                                                                      'including own ADC_UPDT: coded adds '
                                                                      'SOURCE_ADC_UPDT, CR_UPDT_DT_TM and '
                                                                      'CE_UPDT_DT_TM; nomenclature adds '
                                                                      'SOURCE_CHANGE_TS and '
                                                                      'NOMENCLATURE_ADC_UPDT; date adds '
                                                                      'DATE_RESULT_EFFECTIVE_UPDT_DT_TM, '
                                                                      'CLINICAL_EVENT_ADC_UPDT and '
                                                                      'LOOKUP_ADC_UPDT; text adds '
                                                                      'STRING_RESULT_EFFECTIVE_UPDT_DT_TM, '
                                                                      'CLINICAL_EVENT_ADC_UPDT, '
                                                                      'LONG_TEXT_ADC_UPDT and '
                                                                      'LOOKUP_ADC_UPDT. Null inputs are '
                                                                      'ignored and all null gives null. '
                                                                      'These are not uniformly native '
                                                                      'application-update clocks, and the '
                                                                      'union takes no cross-arm maximum.',
                                           'subject_id_system': 'Identifier system used for subject_key.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'value_code': 'Source coded result value.',
                                           'value_datetime': 'Date or date-time result value.',
                                           'value_display': 'Source coded result display.',
                                           'value_map_candidate_count': 'Number of candidate targets '
                                                                        'observed; a mapped parent is '
                                                                        'unambiguous when this is 1.',
                                           'value_map_candidate_set_id': 'Stable identifier of the retained '
                                                                         'candidate set.',
                                           'value_map_competing_count': 'Candidates within 0.05 cosine of '
                                                                        'the selected candidate.',
                                           'value_map_component_count': 'Number of source-derived '
                                                                        'components.',
                                           'value_map_component_index': 'One-based source-derived component '
                                                                        'index.',
                                           'value_map_confidence': 'Mapping confidence in [0,1] where the '
                                                                   'lane measured one.',
                                           'value_map_cosine': 'Measured cosine between the normalised '
                                                               'source and selected target; null when '
                                                               'scoring did not run.',
                                           'value_map_method': 'Mapping method: source_native, rule, '
                                                               'exact_synonym or embedding_proposed.',
                                           'value_map_rollup_levels': 'Maximum hierarchy distance to a '
                                                                      'selected common ancestor.',
                                           'value_map_rule_id': 'Stable rule or lane identifier that '
                                                                'produced the mapping.',
                                           'value_map_scoring_model': 'Embedding model that produced '
                                                                      'map_cosine.',
                                           'value_map_status': 'Explicit mapping outcome from the S3b status '
                                                               'vocabulary.',
                                           'value_map_status_reason': 'Reason for the mapping outcome or '
                                                                      'abstention.',
                                           'value_map_version': 'Version of the source, terminology release '
                                                                'or model used by the mapping.',
                                           'value_omop_concept_id': 'OMOP concept identifier selected with '
                                                                    'the coded result value mapping.',
                                           'value_omop_source_concept_id': 'Non-standard OMOP source concept '
                                                                           'identifier, kept separate from a '
                                                                           'standard target.',
                                           'value_snomed_code': 'SNOMED CT code selected for the coded '
                                                                'result value; null when no unambiguous '
                                                                'mapping is available.',
                                           'value_snomed_display': 'SNOMED CT preferred display selected for '
                                                                   'the coded result value.',
                                           'value_source_code': 'Source code for the coded result value, '
                                                                'retained without de-identification.',
                                           'value_source_display': 'Source display for the coded result '
                                                                   'value.',
                                           'value_source_system': 'Canonical coding-system URI for the '
                                                                  'source coded result value.',
                                           'value_target_code': 'Code of the selected target in its own '
                                                                'vocabulary.',
                                           'value_target_display': 'Display of the selected target in its '
                                                                   'own vocabulary.',
                                           'value_target_system': 'Coding-system URI of the selected target.',
                                           'value_text': 'Verbatim text result or descriptor.',
                                           'value_time': 'Time of day (HH:mm:ss) when the source recorded a '
                                                         'time with no date (Millennium placeholder date '
                                                         '2000-01-01); value_datetime is then NULL. INSR_W2 '
                                                         'W2.07 (J4, L2051).',
                                           'verifier_practitioner_id': 'Millennium personnel PERSON_ID for '
                                                                       'the verifier.',
                                           'verifier_system_ind': 'True when the source '
                                                                  'verifier_practitioner_id was 1, the '
                                                                  'Cerner system user (J5); the id is then '
                                                                  'published as NULL. NULL when the source '
                                                                  'id was NULL. INSR_W2.',
                                           'vip_ind': 'Source VIP indicator.',
                                           'withheld_identity_ind': 'Identity-withholding status is not '
                                                                    'populated by the current source '
                                                                    'projections; null does not mean '
                                                                    'identity was not withheld.'},
                               'table': 'Governed residual clinical observations from exhaustive '
                                        'generic-event routes.'}}
