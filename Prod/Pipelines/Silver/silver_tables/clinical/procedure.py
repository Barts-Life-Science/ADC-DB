"""Silver clinical.procedure: 4 dataset(s).

- datasets: _clinical_procedure_primitive, _clinical_qc_procedure, clinical_procedure, _clinical_procedure_metadata
- moved verbatim from: 2_silver_clinical_events:1071, 2_silver_clinical_events:1089, 2_silver_clinical_events:1171, 2_silver_clinical_events:1189, 2_silver_clinical_events:8300, 2_silver_clinical_events:8302, 2_silver_clinical_events:883, 2_silver_clinical_events:894

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.cross_qc import _s3_direct_axis
from silver_core.journey_core import _n, spark
from silver_tables._common.journey import (
    S3_TABLE_AXIS_SPECS,
    _cc_procedure_canonical,
    _endobase_procedure_canonical,
    _implant_procedure_canonical,
    _procedure_canonical,
    _s3_replace_public_variant,
    _theatre_procedure_canonical,
)
from silver_tables.clinical._shared import _s3_component_metadata


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# contract v2: retain the full canonical shape for internal reuse.
PROCEDURE_SOURCE_COLUMNS = [
    "patient_event_key",
    "source_object",
    "procedure_id",
    "endobase_exam_id",
    "implant_event_id",
    "implant_sequence",
    "surg_case_proc_id",
    "source_row_hash",
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
    "procedure_code", '_r1_procedure_mapping', '_r1_procedure_map_key', '_r1_procedure_components_json', '_r1_procedure_device_mapping', '_r1_procedure_device_map_key', '_r1_procedure_device_components_json',
    "status_code",
    "status_display",
    "performed_start",
    "performed_end",
    "body_site_code",
    "body_site_display",
    "laterality_code",
    "laterality_display",
    # OPCS_QUALIFIER_BLOCK_V1 / IMPLANT_OPCS_LINK_V1: provenance of body site and laterality, OPCS role and operation anchor.
    "body_site_method",
    "laterality_method",
    "opcs_code_role",
    "opcs_anchor_procedure_id",
    "performer_practitioner_id",
    "procedure_location_code",
    "procedure_location_display",
    "procedure_note",
    "implant_description",
    "device_code",
    "device_display",
    "manufacturer",
    "serial_number",
    "batch_number",
    "udi_di",
    "udi_standard",
    "quantity",
    "implant_attribute_count",
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
    # Internal implant mapping evidence needed by the S3 device axis. These
    # cross the primitive/QC boundary and are removed by PROCEDURE_PUBLIC_COLUMNS.
    "_device_snomed_code",
    "_device_snomed_display",
    "_device_mapping_layer",
    "_device_mapping_confidence",
]


PROCEDURE_PRIMITIVE_COLUMNS = [
    {
        "procedure_code": "_procedure_code_json",
        "implant_attribute_history": "_implant_attribute_json",
        "implant_attribute_count": "_implant_attribute_count",
    }.get(name, name)
    for name in PROCEDURE_SOURCE_COLUMNS
]


# P9_L2786: the endobase end is coalesce(TRUE_END_TS, END_TS); a modify clock months after the exam is not an end.
_ENDOBASE_LOAD_CLOCK = ("(source_object = 'endobase_exam' AND to_date(performed_end) = to_date(source_update_timestamp) "
                        "AND trunc(performed_end, 'MM') = trunc(source_update_timestamp, 'MM') "
                        "AND datediff(performed_end, performed_start) > 30)")


def _with_no_opcs_anatomy(df):
    """Arms without OPCS evidence carry the four anatomy provenance columns as NULL."""
    return (df.withColumn("body_site_method", F.lit(None).cast("string"))
              .withColumn("laterality_method", F.lit(None).cast("string"))
              .withColumn("opcs_code_role", F.lit(None).cast("string"))
              .withColumn("opcs_anchor_procedure_id", F.lit(None).cast("bigint")))


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
PROCEDURE_PUBLIC_COLUMNS = [
    'patient_event_key',
    'source_object',
    'procedure_id',
    'endobase_exam_id',
    'implant_event_id',
    'implant_sequence',
    'surg_case_proc_id',
    'source_row_hash',
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
    'procedure_code',
    'status_code',
    'status_display',
    'performed_start',
    'performed_end',
    'body_site_code',
    'body_site_display',
    'laterality_code',
    'laterality_display',
    'body_site_method',
    'laterality_method',
    'opcs_code_role',
    'opcs_anchor_procedure_id',
    'performer_practitioner_id',
    'procedure_location_code',
    'procedure_location_display',
    'procedure_note',
    'implant_description',
    'device_code',
    'device_display',
    'manufacturer',
    'serial_number',
    'batch_number',
    'udi_di',
    'udi_standard',
    'quantity',
    'implant_attribute_count',
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


PROCEDURE_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'endobase_exam_id',
    'implant_event_id',
    'implant_sequence',
    'procedure_id',
    'surg_case_proc_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


# Clinical /  procedure primitive
def _procedure_primitive_query():
    # Combine procedure sources with grouped implant attributes, retaining JSON across the join-
    # shaped internal stage.
    # OPCS_QUALIFIER_BLOCK_V1 / IMPLANT_OPCS_LINK_V1: Millennium OPCS rows take the site and side of their coded
    # operation and implant rows those of the operation they link to. A source LATERALITY_CD still wins.
    attributes = spark.read.table(_n("journey_clinical._implant_attribute_grouped")).alias("a")
    blocks = spark.read.table(_n("journey_clinical._opcs_procedure_block")).select(
        F.col("procedure_id").alias("_block_procedure_id"),
        F.col("opcs_code_role").alias("_block_role"),
        F.col("anchor_procedure_id").alias("_block_anchor_id"),
        F.col("body_site_code").alias("_block_site_code"),
        F.col("body_site_display").alias("_block_site_display"),
        F.col("body_site_method").alias("_block_site_method"),
        F.col("laterality_code").alias("_block_side_code"),
        F.col("laterality_display").alias("_block_side_display"),
        F.col("laterality_method").alias("_block_side_method"),
    )
    links = spark.read.table(_n("journey_clinical._implant_opcs_link"))
    source_side = F.col("laterality_display").isNotNull()
    procedure_rows = (
        _procedure_canonical()
        .join(blocks, F.col("procedure_id") == F.col("_block_procedure_id"), "left")
        .withColumn("laterality_method",
                    F.when(source_side, F.lit("source_laterality_cd")).otherwise(F.col("_block_side_method")))
        .withColumn("laterality_code", F.when(source_side, F.col("laterality_code")).otherwise(F.col("_block_side_code")))
        .withColumn("laterality_display",
                    F.when(source_side, F.col("laterality_display")).otherwise(F.col("_block_side_display")))
        .withColumn("body_site_code", F.col("_block_site_code"))
        .withColumn("body_site_display", F.col("_block_site_display"))
        .withColumn("body_site_method", F.col("_block_site_method"))
        .withColumn("opcs_code_role", F.col("_block_role"))
        .withColumn("opcs_anchor_procedure_id", F.col("_block_anchor_id"))
        .withColumn("_implant_attribute_json", F.lit(None).cast("string"))
        .withColumn("_implant_attribute_count", F.lit(0).cast("long"))
    )
    # contract v2: join implant attributes to procedure rows by patient_event_key
    implant_rows = (
        _implant_procedure_canonical()
        .join(
            attributes,
            F.col("patient_event_key") == F.col("a._implant_attribute_event_key"),
            "left",
        )
        .join(links, F.col("patient_event_key") == F.col("_link_event_key"), "left")
        .withColumn("_implant_attribute_json", F.col("a._implant_attribute_json"))
        .withColumn(
            "_implant_attribute_count",
            F.coalesce(F.col("a._implant_attribute_count"), F.lit(0).cast("long")),
        )
        .withColumn(
            "loaded_at", F.greatest(F.col("loaded_at"), F.col("a._evidence_loaded_at"))
        )
        .withColumn("body_site_code", F.col("_link_body_site_code"))
        .withColumn("body_site_display", F.col("_link_body_site_display"))
        .withColumn("body_site_method", F.col("_link_body_site_method"))
        .withColumn("laterality_code", F.col("_link_laterality_code"))
        .withColumn("laterality_display", F.col("_link_laterality_display"))
        .withColumn("laterality_method", F.col("_link_laterality_method"))
        .withColumn("opcs_code_role", F.lit(None).cast("string"))
        .withColumn("opcs_anchor_procedure_id", F.col("_link_procedure_id"))
    )
    theatre_rows = _with_no_opcs_anatomy(
        _theatre_procedure_canonical()
        .withColumn("_implant_attribute_json", F.lit(None).cast("string"))
        .withColumn("_implant_attribute_count", F.lit(0).cast("long"))
    )
    cc_rows = _with_no_opcs_anatomy(
        _cc_procedure_canonical()
        .withColumn("_implant_attribute_json", F.lit(None).cast("string"))
        .withColumn("_implant_attribute_count", F.lit(0).cast("long"))
    )
    endobase_rows = _with_no_opcs_anatomy(
        _endobase_procedure_canonical()
        .withColumn("_implant_attribute_json", F.lit(None).cast("string"))
        .withColumn("_implant_attribute_count", F.lit(0).cast("long"))
    )
    # INSR_W2 W2.20: arm-specific W1 carries, typed NULL on the arms that do not have them.
    carry = ['quantity_parse_rule', 'quantity_parse_status', 'procedure_date_basis']

    def _carry(df):
        """Add the carried columns as NULL strings where an arm has none."""
        for c in carry:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast("string"))
        return df
    procedure_rows, implant_rows, theatre_rows, cc_rows, endobase_rows = (
        _carry(procedure_rows), _carry(implant_rows), _carry(theatre_rows), _carry(cc_rows), _carry(endobase_rows))
    PROCEDURE_PRIMITIVE_COLUMNS_W2 = list(PROCEDURE_PRIMITIVE_COLUMNS) + carry
    return (
        procedure_rows.select(*PROCEDURE_PRIMITIVE_COLUMNS_W2)
        .unionByName(implant_rows.select(*PROCEDURE_PRIMITIVE_COLUMNS_W2))
        .unionByName(theatre_rows.select(*PROCEDURE_PRIMITIVE_COLUMNS_W2))
        .unionByName(cc_rows.select(*PROCEDURE_PRIMITIVE_COLUMNS_W2))
        .unionByName(endobase_rows.select(*PROCEDURE_PRIMITIVE_COLUMNS_W2))
    )


@materialized_view(
    name=_n("journey_clinical._procedure_primitive"),
    private=True,
    refresh_policy="auto",
)
def _procedure_primitive():
    # Build the declared dataset: Internal orderable procedure union; CodeableConcept JSON
    # crosses the incremental boundary.
    return _procedure_primitive_query()


# Clinical /  qc procedure
@materialized_view(
    name=_n("journey_clinical._qc_procedure"),
    refresh_policy="auto",
)
def _qc_procedure():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for procedure.
    source = (
        spark.read.table(_n("journey_clinical._procedure_primitive"))
        .withColumn("implant_attribute_count", F.col("_implant_attribute_count"))
    )
    return _cross_qc_primitive(
        source,
        "procedure",
        {"procedure_code": "_procedure_code_json"},
    )


@materialized_view(
    name=_n("journey_clinical.procedure"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def procedure():
    # Build the declared dataset: Performed procedure and implant-placement evidence from
    # registered bronze feeds; source facts remain separate.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_procedure")),
        "procedure",
        {"procedure_code": "_procedure_code_json"},
        PROCEDURE_PUBLIC_COLUMNS,
        PROCEDURE_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._procedure_metadata'),
    refresh_policy="auto",
)
def procedure_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_procedure;
    # same row grain as the research table. Join keys: patient_event_key, source_object,
    # endobase_exam_id, implant_event_id, implant_sequence, procedure_id, surg_case_proc_id.
    return _s3_component_metadata("journey_clinical.procedure", "journey_clinical._qc_procedure", PROCEDURE_LIFECYCLE_COLUMNS)


if "source_patient_event_key" not in PROCEDURE_PUBLIC_COLUMNS:
    PROCEDURE_PUBLIC_COLUMNS.insert(PROCEDURE_PUBLIC_COLUMNS.index("patient_event_key") + 1, "source_patient_event_key")


for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_procedure"].items():
    _s3_replace_public_variant(PROCEDURE_PUBLIC_COLUMNS, old_name, axes)


def _prepare_procedure(df):
    """procedure: per-row prepare step, moved verbatim from _cross_qc_prepare (silver_journey_shared.py:1115)."""
    present = F.col("_device_snomed_code").isNotNull()
    device_embedding = present & (F.col("_device_mapping_layer") == "LAYER5_GMDN_EMBEDDING")
    df = _s3_direct_axis(
        # Task 1.6 proved SNOMED_DEVICE_CONCEPT_ID is an OMOP concept_id in this
        # source despite its name. Preserve that identifier in the OMOP slot and
        # let the governed SNOMED crosswalk recover concept_code/display below.
        df, "device", F.lit("urn:barts:implant:device"), F.col("device_code"), F.col("device_display"),
        omop_concept_id=F.expr("try_cast(_device_snomed_code as bigint)"),
        method=F.when(device_embedding, F.lit("embedding_proposed")).when(present, F.lit("rule")),
        confidence=F.when(present, F.col("_device_mapping_confidence")),
        rule=F.when(device_embedding, F.lit("embedding:bronze_implant_layer5:tier"))
              .when(present, F.concat(F.lit("bronze.map_implant_details:"),
                                     F.coalesce(F.col("_device_mapping_layer"), F.lit("mapped")))),
        candidates=F.when(present, F.lit(1)).otherwise(F.lit(0)))
    return df


CROSS_QC = {
    'procedure': CrossQc(model='procedure', fk=('encounter_id', 'person_id'), dates=('event_after_death_30d', 'event_before_birth'), coded_axes={'procedure_code': ('procedure', False), 'device_code': ('device', False)}, extra_public_columns=('quantity_parse_rule', 'quantity_parse_status', 'procedure_date_basis'), embedding_lanes=(('device', 'layer5|gmdn|embed|vector', 'bronze_implant_layer5'),), promote_lifecycle=['identity_status', 'person_id_resolved', 'encounter_id_resolved'], prepare_steps=(_prepare_procedure,)),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {
    'clinical_procedure': ValuePolicy(table='clinical_procedure', enabled=True, zero_as_null=('performer_practitioner_id',)),
}


VALUE_POLICIES["clinical_procedure"].register("performed_end_basis",
         ["source_object", "performed_start", "performed_end", "source_update_timestamp"],
         lambda F: F.when(F.expr(_ENDOBASE_LOAD_CLOCK), F.lit("load_clock_suppressed"))
                    .when(F.col("performed_end").isNotNull(), F.lit("source_end")),
         "source_end, or load_clock_suppressed where the Endobase end equals the record's update day and month and "
         "follows the start by more than 30 days (a modify clock, P9_L2786); performed_end is then NULL. INSR_W2.")


for _col in ("performed_end", "event_end_datetime", "event_end_datetime_local"):
    VALUE_POLICIES["clinical_procedure"].value_rule(_col, ["performed_end_basis", _col],
               # NULL basis (no performed_end) keeps the column: != on NULL is NULL (found by the lean fixture).
               lambda F, c=_col: F.when(F.coalesce(F.col("performed_end_basis"), F.lit("")) != "load_clock_suppressed",
                                        F.col(c)))


WRAP_POLICY = {'clinical_procedure'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_procedure_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged from '
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
                                  'table': 'Internal quality and batch metadata for clinical_procedure; same '
                                           'row grain as the research table. Join keys: patient_event_key, '
                                           'source_object, endobase_exam_id, implant_event_id, '
                                           'implant_sequence, procedure_id, surg_case_proc_id.'},
 '_clinical_procedure_primitive': {'columns': {},
                                   'table': 'Internal orderable procedure union; CodeableConcept JSON '
                                            'crosses the incremental boundary.'},
 '_clinical_qc_procedure': {'columns': {},
                            'table': 'Private JSON bridge and Gold cross-rule flags for procedure.'},
 'clinical_procedure': {'columns': {'batch_number': 'Implant batch number.',
                                    'body_site_code': 'SNOMED body-structure code. Millennium OPCS-4 rows: '
                                                      'the first OPCS site code of the coded operation, '
                                                      "mapped through OMOP 'Maps to'. Implant rows: the site "
                                                      'agreed by the operations the implant links to. See '
                                                      'body_site_method.',
                                    'body_site_display': 'SNOMED body-structure name for body_site_code.',
                                    'body_site_method': 'How body_site_code was derived. '
                                                        'opcs_qualifier_block_v1: first site code of the '
                                                        'OPCS-4 coded operation (same encounter and coding '
                                                        'episode, ordered by PROC_PRIORITY then '
                                                        'PROCEDURE_ID; sites coded after a harvest (Y54-Y69) '
                                                        'code describe the donor site and are not used); '
                                                        'suffix _multiple when the operation has several '
                                                        'site codes, _unmapped when the site code has no '
                                                        'SNOMED map (code NULL). '
                                                        'implant_opcs_link_v1_same_day / _adjacent_day: site '
                                                        'agreed by every candidate operation; _conflict when '
                                                        'they disagree (code NULL). NULL: no site evidence.',
                                    'confidentiality_code': 'Source confidentiality code.',
                                    'device_display': 'Device concept display.',
                                    'device_map_candidate_count': 'Number of candidate targets observed; a '
                                                                  'mapped parent is unambiguous when this is '
                                                                  '1.',
                                    'device_map_candidate_set_id': 'Stable identifier of the retained '
                                                                   'candidate set.',
                                    'device_map_competing_count': 'Candidates within 0.05 cosine of the '
                                                                  'selected candidate.',
                                    'device_map_component_count': 'Number of source-derived components.',
                                    'device_map_component_index': 'One-based source-derived component index.',
                                    'device_map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                             'measured one.',
                                    'device_map_cosine': 'Measured cosine between the normalised source and '
                                                         'selected target; null when scoring did not run.',
                                    'device_map_method': 'Mapping method: source_native, rule, exact_synonym '
                                                         'or embedding_proposed.',
                                    'device_map_rollup_levels': 'Maximum hierarchy distance to a selected '
                                                                'common ancestor.',
                                    'device_map_rule_id': 'Stable rule or lane identifier that produced the '
                                                          'mapping.',
                                    'device_map_scoring_model': 'Embedding model that produced map_cosine.',
                                    'device_map_status': 'Explicit mapping outcome from the S3b status '
                                                         'vocabulary.',
                                    'device_map_status_reason': 'Reason for the mapping outcome or '
                                                                'abstention.',
                                    'device_map_version': 'Version of the source, terminology release or '
                                                          'model used by the mapping.',
                                    'device_omop_concept_id': 'OMOP concept identifier selected with the '
                                                              'implanted device mapping.',
                                    'device_omop_source_concept_id': 'Non-standard OMOP source concept '
                                                                     'identifier, kept separate from a '
                                                                     'standard target.',
                                    'device_snomed_code': 'SNOMED CT code selected for the implanted device; '
                                                          'null when no unambiguous mapping is available.',
                                    'device_snomed_display': 'SNOMED CT preferred display selected for the '
                                                             'implanted device.',
                                    'device_source_code': 'Source code for the implanted device, retained '
                                                          'without de-identification.',
                                    'device_source_display': 'Source display for the implanted device.',
                                    'device_source_system': 'Canonical coding-system URI for the source '
                                                            'implanted device.',
                                    'device_target_code': 'Code of the selected target in its own '
                                                          'vocabulary.',
                                    'device_target_display': 'Display of the selected target in its own '
                                                             'vocabulary.',
                                    'device_target_system': 'Coding-system URI of the selected target.',
                                    'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                    'endobase_exam_id': 'Endobase ENDOBASE_EXAM_ID; primary key for the '
                                                        'endobase_exam arm.',
                                    'event_datetime': 'Primary performed timestamp.',
                                    'event_datetime_local': 'event_datetime in Europe/London wall-clock time '
                                                            '(GMT/BST). Millennium rows take the bronze '
                                                            '*_LOCAL companions of the same source columns '
                                                            'and fallback order; rows from local-clock '
                                                            'sources (LUNA, PACS/DICOM, TFC LIMS, CCMDS, '
                                                            'MSDS, BadgerNet, EndoBase, SLAM, iWeb, Datix '
                                                            'and similar) carry event_datetime unchanged. '
                                                            'event_datetime is a UTC instant for Millennium '
                                                            'rows, so take local calendar dates and clock '
                                                            'times from this column.',
                                    'event_end_datetime': 'Procedure end timestamp.',
                                    'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                'wall-clock time (GMT/BST), by the same rule '
                                                                'as event_datetime_local; null where '
                                                                'event_end_datetime is null.',
                                    'implant_attribute_count': 'Number of retained implant attribute rows.',
                                    'implant_description': 'Implant description for implant-placement facts.',
                                    'implant_event_id': 'Millennium implant EVENT_ID; first component of the '
                                                        'implant-arm primary key.',
                                    'implant_sequence': 'Millennium IMPLANT_SEQUENCE; second component of '
                                                        'the implant-arm primary key.',
                                    'laterality_code': 'Millennium LATERALITY_CD where the source records '
                                                       'one; otherwise the OPCS-4 Z94.x code of the coded '
                                                       'operation (Z94.1 bilateral, Z94.2 right, Z94.3 left, '
                                                       'Z94.4 unilateral side not stated). See '
                                                       'laterality_method.',
                                    'laterality_display': 'Laterality display: the Millennium laterality '
                                                          'description or the Z94.x meaning.',
                                    'laterality_method': 'How laterality was derived. source_laterality_cd: '
                                                         'Millennium LATERALITY_CD. opcs_qualifier_block_v1: '
                                                         'the Z94.x code of the OPCS-4 coded operation (a '
                                                         'Z94 coded after a harvest (Y54-Y69) code describes '
                                                         'the donor site and is not used); _conflict when it '
                                                         'carries different Z94 codes (laterality NULL). '
                                                         'implant_opcs_link_v1_same_day / _adjacent_day: '
                                                         'side agreed by every candidate operation with a '
                                                         'side; _conflict when they disagree. NULL: no side '
                                                         'evidence.',
                                    'loaded_at': 'Millennium, NCCMDS and EndoBase arms use their own '
                                                 'ADC_UPDT. Theatre uses greatest of procedure ADC_UPDT and '
                                                 'joined case ADC_UPDT. Implant first takes '
                                                 'coalesce(SOURCE_MAX_ADC_UPDT, ADC_UPDT, '
                                                 'BASE_EVENT_ADC_UPDT), in that order, then greatest of this '
                                                 'result and maximum ADC_UPDT from matching '
                                                 'map_implant_detail_events grouped by patient_event_key. '
                                                 'Attribute matching uses the base clinical-event ID or '
                                                 'implant-form parent-event ID. The ordered implant fallback '
                                                 'is not itself a maximum; the five-arm union and QC add no '
                                                 'further clocks or Silver refresh timestamp.',
                                    'manufacturer': 'Implant manufacturer name',
                                    'opcs_anchor_procedure_id': 'PROCEDURE_ID of the OPCS-4 operation this '
                                                                'row belongs to: the row itself for an '
                                                                'operation; the preceding operation in '
                                                                'coding order for a qualifier; NULL for a '
                                                                'qualifier coded before any operation. '
                                                                'Implant rows: the linked operation when '
                                                                'exactly one candidate operation exists '
                                                                '(IMPLANT_OPCS_LINK_V1).',
                                    'opcs_code_role': 'Role of this OPCS-4 code in its coded operation '
                                                      '(OPCS_QUALIFIER_BLOCK_V1): operation, site '
                                                      '(body-structure code), laterality (Z94.x) or method '
                                                      '(Y chapter). NULL for non-OPCS rows. Count procedures '
                                                      "with opcs_code_role = 'operation'; the other roles "
                                                      'qualify the operation named in '
                                                      'opcs_anchor_procedure_id.',
                                    'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                         'cross-feed join key.',
                                    'performed_end': 'Procedure performed end.',
                                    'performed_end_basis': 'source_end, or load_clock_suppressed where the '
                                                           "Endobase end equals the record's update day and "
                                                           'month and follows the start by more than 30 days '
                                                           '(a modify clock, P9_L2786); performed_end is '
                                                           'then NULL. INSR_W2.',
                                    'performed_start': 'Procedure performed start.',
                                    'performer_practitioner_id': 'Millennium personnel PERSON_ID as BIGINT '
                                                                 'when available.',
                                    'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                    'procedure_date_basis': 'Basis of PROCEDURE_DT_TM_EFFECTIVE: proc_dt_tm, '
                                                            'proc_start_dt_tm, proc_end_dt_tm or '
                                                            'encounter_arrival. (bronze '
                                                            'PROCEDURE_DATE_BASIS, INSR W1). INSR_W2 W2.20.',
                                    'procedure_id': 'Millennium PROCEDURE_ID; primary key for the '
                                                    'millennium_procedure arm.',
                                    'procedure_location_code': 'Source procedure-location code retained '
                                                               'without asserting a location-dimension FK.',
                                    'procedure_location_display': 'Source procedure-location display.',
                                    'procedure_map_candidate_count': 'Number of candidate targets observed; '
                                                                     'a mapped parent is unambiguous when '
                                                                     'this is 1.',
                                    'procedure_map_candidate_set_id': 'Stable identifier of the retained '
                                                                      'candidate set.',
                                    'procedure_map_competing_count': 'Candidates within 0.05 cosine of the '
                                                                     'selected candidate.',
                                    'procedure_map_component_count': 'Number of source-derived components.',
                                    'procedure_map_component_index': 'One-based source-derived component '
                                                                     'index.',
                                    'procedure_map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                                'measured one.',
                                    'procedure_map_cosine': 'Measured cosine between the normalised source '
                                                            'and selected target; null when scoring did not '
                                                            'run.',
                                    'procedure_map_method': 'Mapping method: source_native, rule, '
                                                            'exact_synonym or embedding_proposed.',
                                    'procedure_map_rollup_levels': 'Maximum hierarchy distance to a selected '
                                                                   'common ancestor.',
                                    'procedure_map_rule_id': 'Stable rule or lane identifier that produced '
                                                             'the mapping.',
                                    'procedure_map_scoring_model': 'Embedding model that produced '
                                                                   'map_cosine.',
                                    'procedure_map_status': 'Explicit mapping outcome from the S3b status '
                                                            'vocabulary.',
                                    'procedure_map_status_reason': 'Reason for the mapping outcome or '
                                                                   'abstention.',
                                    'procedure_map_version': 'Version of the source, terminology release or '
                                                             'model used by the mapping.',
                                    'procedure_note': 'Source procedure note.',
                                    'procedure_omop_concept_id': 'OMOP concept identifier selected with the '
                                                                 'procedure mapping.',
                                    'procedure_omop_source_concept_id': 'Non-standard OMOP source concept '
                                                                        'identifier, kept separate from a '
                                                                        'standard target.',
                                    'procedure_snomed_code': 'SNOMED CT code selected for the procedure; '
                                                             'null when no unambiguous mapping is available.',
                                    'procedure_snomed_display': 'SNOMED CT preferred display selected for '
                                                                'the procedure.',
                                    'procedure_source_code': 'Source code for the procedure, retained '
                                                             'without de-identification.',
                                    'procedure_source_display': 'Source display for the procedure.',
                                    'procedure_source_system': 'Canonical coding-system URI for the source '
                                                               'procedure.',
                                    'procedure_target_code': 'Code of the selected target in its own '
                                                             'vocabulary.',
                                    'procedure_target_display': 'Display of the selected target in its own '
                                                                'vocabulary.',
                                    'procedure_target_system': 'Coding-system URI of the selected target.',
                                    'quantity': 'Parsed implant quantity.',
                                    'quantity_parse_rule': 'Rule set behind '
                                                           'QUANTITY_NUMERIC/QUANTITY_PARSE_STATUS '
                                                           '(IMPLANT_QTY_V2). (bronze QUANTITY_PARSE_RULE, '
                                                           'INSR W1). INSR_W2 W2.20.',
                                    'quantity_parse_status': 'Status indicator showing whether the recorded '
                                                             'implant quantity text was successfully '
                                                             'converted to a numeric value. (bronze '
                                                             'QUANTITY_PARSE_STATUS, INSR W1). INSR_W2 '
                                                             'W2.20.',
                                    'record_status': 'Arm-specific label: Millennium procedures are '
                                                     'superseded when lower(coalesce(active_status_desc, '
                                                     'empty string)) contains the substring inactive, '
                                                     'otherwise active. Implants are always active. Theatre '
                                                     'procedures are superseded for source presence false '
                                                     '(null defaults true) or ACTIVE_IND cast to LONG zero '
                                                     '(null defaults one), otherwise active. EndoBase exams '
                                                     'are superseded for source presence false (null '
                                                     'defaults true), otherwise active. NCCMDS procedures '
                                                     'instead map source absence to retracted, otherwise '
                                                     'active. Clinical completion/cancellation fields remain '
                                                     'separate.',
                                    'record_status_effective_from': 'Millennium: ACTIVE_STATUS_DT_TM. '
                                                                    'Implant: VALID_FROM_DT_TM. Theatre: '
                                                                    'first non-null procedure '
                                                                    'PROC_START_DT_TM then parent case '
                                                                    'SCHED_START_DT_TM, not the wider '
                                                                    'performed-time fallback. NCCMDS: '
                                                                    'OPCS_Proc_Dt_CLEAN. EndoBase: first '
                                                                    'non-null PERFORMED_TS_CLEAN, '
                                                                    'EXAM_TS_CLEAN, TRUE_START_TS_CLEAN then '
                                                                    'START_TS_CLEAN. These arm-specific '
                                                                    'values are carried unchanged; no '
                                                                    'ingestion-time or cross-arm fallback is '
                                                                    'added.',
                                    'record_status_effective_to': 'Millennium: ACTIVE_STATUS_DT_TM only when '
                                                                  'inactive. Implant: always null. Theatre: '
                                                                  'first non-null procedure '
                                                                  'SOURCE_ABSENT_DETECTED_TS then procedure '
                                                                  'ADC_UPDT only when inactive; the parent '
                                                                  'case clock does not contribute. NCCMDS: '
                                                                  'own ADC_UPDT only when retracted. '
                                                                  'EndoBase: own ADC_UPDT only when '
                                                                  'source-absent/superseded. All other rows '
                                                                  'have null ends; clinical procedure/exam '
                                                                  'end timestamps are not substituted.',
                                    'serial_number': 'Implant serial number.',
                                    'source_code': 'Source procedure code or implant label',
                                    'source_coding_system': 'Verbatim source coding system.',
                                    'source_display': 'Verbatim source procedure display.',
                                    'source_feed': 'Registered source feed owning the fact.',
                                    'source_object': 'Source arm: millennium_procedure, implant, theatre, '
                                                     'endobase_exam, or nccmds.',
                                    'source_row_hash': 'Native ROW_HASH for the nccmds arm; NULL for other '
                                                       'arms.',
                                    'source_update_timestamp': 'Source-arm-specific timestamp; interpret '
                                                               'with source_object. Cerner procedures carry '
                                                               'map_procedure.UPDT_DT_TM; implants carry '
                                                               'map_implant_details.CLINSIG_UPDT_DT_TM; '
                                                               'theatre carries the greatest SOURCE_ADC_UPDT '
                                                               'from its case and case-procedure rows. '
                                                               'EndoBase exams and NCCMDS procedures carry '
                                                               'their bronze PIPELINE_UPDT_DT_TM. This mixes '
                                                               'source and processing clocks and is not a '
                                                               'uniform clinical event time or native-system '
                                                               'update time.',
                                    'status_code': 'Source-derived FHIR procedure status.',
                                    'status_display': 'Source status display.',
                                    'subject_id_system': 'Identifier system used for subject_key.',
                                    'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                   'identifier. Not salted, not secret; a stable join key '
                                                   'across feeds where person_id is unresolved.',
                                    'surg_case_proc_id': 'SurgiNet SURG_CASE_PROC_ID; primary key for the '
                                                         'theatre arm.',
                                    'udi_di': 'Unique device identifier device identifier.',
                                    'udi_standard': 'UDI issuing standard.',
                                    'vip_ind': 'Source VIP indicator.',
                                    'withheld_identity_ind': 'Identity-withholding status is not populated '
                                                             'by the current source projections; null does '
                                                             'not mean identity was not withheld.'},
                        'table': 'Performed procedure and implant-placement evidence from registered bronze '
                                 'feeds; source facts remain separate.'}}
