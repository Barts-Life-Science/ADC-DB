"""Silver clinical.medication_order: 4 dataset(s).

- datasets: _clinical_medication_order_primitive, _clinical_qc_medication_order, clinical_medication_order, _clinical_medication_order_metadata
- moved verbatim from: 2_silver_clinical_events:5362, 2_silver_clinical_events:5497, 2_silver_clinical_events:5713, 2_silver_clinical_events:5740, 2_silver_clinical_events:5800, 2_silver_clinical_events:5871, 2_silver_clinical_events:5891, 2_silver_clinical_events:8325

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import (
    OMOP_URI,
    SNOMED_URI,
    _n,
    _present,
    axis_columns,
    codeable_concept_json,
    coding_obj,
    read_source,
    spark,
    stable_id,
    subject_key_with_system,
)
from silver_tables._common.journey import (
    S3_TABLE_AXIS_SPECS,
    SRC_MEDICATION_ORDER,
    _medication_order_canonical,
    _s3_replace_public_variant,
)
from silver_tables.clinical._shared import _s3_component_metadata


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


def _bbr_inherit_template_mapping(df):
    """BBR-13 (Ben ruling 2026-11-03, option a): a continuous-order instance whose medication axis carries no code
    takes its parent's (template order's) mapped medication fields through an equi-join on template_order_id; the
    rule id gains ':inherited_from_template'. Today this fills 0 rows (instances are mapped as their parents): it is a
    deterministic backstop. Only the parent's first component is read, so a row never multiplies."""
    # The instance keeps its own component index/count: it receives one component, not the parent's set (review
    # fix 11-03: a copied count of 2 on a single row would claim a sibling row that does not exist).
    mapped = [c for c in axis_columns("medication")[3:]
              if c not in ("medication_map_component_index", "medication_map_component_count")]
    parents = (df.where(F.coalesce(F.col("medication_map_component_index"), F.lit(1)) == 1)
               .where(F.col("medication_snomed_code").isNotNull() | F.col("medication_omop_concept_id").isNotNull())
               .select(F.col("order_id").alias("_bbr_parent_id"), *[F.col(c).alias("_bbr_p_" + c) for c in mapped]))
    df = df.join(parents, (F.col("order_role") == "instance") & (F.col("template_order_id") == F.col("_bbr_parent_id")),
                 "left")
    # Materialised: the writes below fill medication_snomed_code, which the decision reads.
    df = df.withColumn("_bbr_inherit", F.col("_bbr_parent_id").isNotNull() & F.col("medication_snomed_code").isNull()
                       & F.col("medication_omop_concept_id").isNull())
    inherit = F.col("_bbr_inherit")
    for column in mapped:
        value = F.col("_bbr_p_" + column)
        if column == "medication_map_rule_id":
            value = F.concat(F.coalesce(value, F.lit("")), F.lit(":inherited_from_template"))
        df = df.withColumn(column, F.when(inherit, value).otherwise(F.col(column)))
    return df.drop("_bbr_parent_id", "_bbr_inherit", *["_bbr_p_" + c for c in mapped])


# Clinical /  medication order primitive
def _medication_order_primitive_query():
    # Join orders to grouped actions, ingredients and details for the internal medication-order
    # stage.
    o = read_source(SRC_MEDICATION_ORDER).alias("o")
    actions = spark.read.table(_n("journey_clinical._med_order_action_grouped")).alias("a")
    ingredients = spark.read.table(_n("journey_clinical._med_order_ingredient_grouped")).alias("i")
    details = spark.read.table(_n("journey_clinical._med_order_detail_grouped")).alias("d")
    joined = (
        o.join(actions, F.col("o.ORDER_ID") == F.col("a.ORDER_ID"), "left")
         .join(ingredients, F.col("o.ORDER_ID") == F.col("i.ORDER_ID"), "left")
         .join(details, F.col("o.ORDER_ID") == F.col("d.ORDER_ID"), "left")
    )
    event_id = stable_id("medication_order:mill", F.col("o.ORDER_ID"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("o.PERSON_ID"))], SRC_MEDICATION_ORDER, F.col("o.ORDER_ID")
    )
    retracted = F.coalesce(F.col("o.SOURCE_PRESENT_IND"), F.lit(True)) == F.lit(False)
    superseded = (F.coalesce(F.col("o.INACTIVE_ORDER_FLAG"), F.lit(0)) == 1) | (
        F.coalesce(F.col("o.ACTIVE_IND"), F.lit(1)) == 0
    )
    loaded_at = F.greatest(F.col("o.ADC_UPDT"), F.col("a.action_loaded_at"),
                           F.col("i.ingredient_loaded_at"), F.col("d.detail_loaded_at"))
    source_update = F.greatest(
        F.col("o.SOURCE_ADC_UPDT"), F.col("o.ADC_UPDT"),
        F.col("a.action_source_update_timestamp"),
        F.col("i.ingredient_source_update_timestamp"),
        F.col("d.detail_source_update_timestamp"),
    )
    # contract v2: publish native medication ORDER_ID and native foreign keys while retaining the event SHA as patient_event_key
    return joined.select(
        event_id.alias("patient_event_key"),
        F.col("o.ORDER_ID").cast("bigint").alias("order_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        F.col("o.PERSON_ID").cast("bigint").alias("person_id"),
        F.when(_present(F.col("o.PERSON_ID")), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.col("o.ENCNTR_ID").cast("bigint").alias("encounter_id"),
        F.coalesce(F.col("o.ORIG_ORDER_DT_TM"), F.col("o.CURRENT_START_DT_TM"))
         .alias("event_datetime"),
        F.coalesce(F.col("o.PROJECTED_STOP_DT_TM"), F.col("o.SOFT_STOP_DT_TM"),
                   F.col("o.DISCONTINUE_EFFECTIVE_DT_TM")).alias("event_end_datetime"),
        F.coalesce(F.col("o.ORIG_ORDER_DT_TM_LOCAL"), F.col("o.CURRENT_START_DT_TM_LOCAL")).alias("event_datetime_local"),
        F.coalesce(F.col("o.PROJECTED_STOP_DT_TM_LOCAL"), F.col("o.SOFT_STOP_DT_TM_LOCAL"), F.col("o.DISCONTINUE_EFFECTIVE_DT_TM_LOCAL")).alias("event_end_datetime_local"),
        F.lit("urn:cerner:synonym_id").alias("source_coding_system"),
        F.col("o.SYNONYM_ID").cast("string").alias("source_code"),
        F.coalesce(F.col("o.ORDER_MNEMONIC"), F.col("o.ORDERED_AS_MNEMONIC"),
                   F.col("o.HNA_ORDER_MNEMONIC")).alias("source_display"),
        codeable_concept_json(
            coding_obj(F.lit("urn:cerner:synonym_id"), F.col("o.SYNONYM_ID"),
                       F.coalesce(F.col("o.ORDER_MNEMONIC"), F.col("o.ORDERED_AS_MNEMONIC"),
                                  F.col("o.HNA_ORDER_MNEMONIC")), True),
            coding_obj(F.lit(SNOMED_URI), F.col("o.SNOMED_CODE"),
                       F.coalesce(F.col("o.OMOP_STANDARD_CONCEPT_NAME"), F.col("o.OMOP_CONCEPT_NAME")),
                       False, "bronze.map_medication_order", None),
            coding_obj(F.lit(OMOP_URI), F.col("o.OMOP_STANDARD_CONCEPT_ID"),
                       F.col("o.OMOP_STANDARD_CONCEPT_NAME"), False,
                       "bronze.map_medication_order", None),
        ).alias("_medication_code_json"), F.col("o.MED_ORDER_MAPPING").alias("_r1_med_order_mapping"), F.col("o.MED_ORDER_MAP_KEY").alias("_r1_med_order_map_key"), F.col("o.MED_ORDER_MAP_COMPONENTS_JSON").alias("_r1_med_order_components_json"),
        F.col("o.ORDER_STATUS_CD").cast("string").alias("order_status_code"),
        F.col("o.ORDER_STATUS_DESCRIPTION").alias("order_status_display"),
        F.col("o.DEPT_STATUS_CD").cast("string").alias("department_status_code"),
        F.col("o.DEPT_STATUS_DESCRIPTION").alias("department_status_display"),
        F.col("o.ACTIVE_STATUS_CD").cast("string").alias("active_status_code"),
        F.col("o.ACTIVE_STATUS_DESCRIPTION").alias("active_status_display"),
        # BBR_FOLDIN_V1 (BBR-14): FHIR MedicationRequest.category from ORIG_ORD_AS_FLAG; a recorded home medication is a
        # statement of what the patient takes (intent 'plan', reported_ind true), Ben decision BBR-D4. NULL flag -> NULL.
        F.when(F.col("o.ORIG_ORD_AS_FLAG").isNull(), F.lit(None).cast("string"))
         .when(F.col("o.ORIG_ORD_AS_FLAG") == 2, F.lit("plan")).otherwise(F.lit("order")).alias("intent_code"),
        F.when(F.col("o.ORIG_ORD_AS_FLAG") == 0, F.lit('inpatient'))
         .when(F.col("o.ORIG_ORD_AS_FLAG") == 1, F.lit('discharge'))
         .when(F.col("o.ORIG_ORD_AS_FLAG") == 2, F.lit('community'))
         .when(F.col("o.ORIG_ORD_AS_FLAG") == 3, F.lit('inpatient'))
         .when(F.col("o.ORIG_ORD_AS_FLAG") == 4, F.lit('inpatient'))
         .when(F.col("o.ORIG_ORD_AS_FLAG") == 5, F.lit('inpatient'))
         .alias("category_code"),
        F.when(F.col("o.ORIG_ORD_AS_FLAG") == 0, F.lit('Normal order'))
         .when(F.col("o.ORIG_ORD_AS_FLAG") == 1, F.lit('Prescription/Discharge order'))
         .when(F.col("o.ORIG_ORD_AS_FLAG") == 2, F.lit('Recorded/Home medication'))
         .when(F.col("o.ORIG_ORD_AS_FLAG") == 3, F.lit("Patient's own medication"))
         .when(F.col("o.ORIG_ORD_AS_FLAG") == 4, F.lit('Pharmacy charge only'))
         .when(F.col("o.ORIG_ORD_AS_FLAG") == 5, F.lit('Satellite'))
         .alias("category_display"),
        F.concat(F.lit("ORIG_ORD_AS_FLAG="), F.col("o.ORIG_ORD_AS_FLAG").cast("string")).alias("category_basis"),
        (F.col("o.ORIG_ORD_AS_FLAG") == 2).alias("reported_ind"),
        # BBR_FOLDIN_V1 (BBR-13): continuous-order role; content_order_id is the order whose attribute, detail and
        # ingredient rows describe this row (the parent for an instance), the by-reference inheritance of BBR-D2.
        F.col("o.TEMPLATE_ORDER_ID").cast("bigint").alias("template_order_id"),
        F.when(F.col("o.TEMPLATE_ORDER_FLAG") == 0, F.lit('order'))
         .when(F.col("o.TEMPLATE_ORDER_FLAG") == 1, F.lit('continuous_parent'))
         .when(F.col("o.TEMPLATE_ORDER_FLAG") == 4, F.lit('instance'))
         .alias("order_role"),
        F.concat(F.lit("TEMPLATE_ORDER_FLAG="), F.col("o.TEMPLATE_ORDER_FLAG").cast("string")).alias("order_role_basis"),
        F.coalesce(F.when(F.col("o.TEMPLATE_ORDER_FLAG") == 4, F.col("o.TEMPLATE_ORDER_ID").cast("bigint")),
                   F.col("o.ORDER_ID").cast("bigint")).alias("content_order_id"),
        F.col("o.MED_ORDER_TYPE_CD").cast("string").alias("medication_order_type_code"),
        F.col("o.MED_ORDER_TYPE_DESCRIPTION").alias("medication_order_type_display"),
        F.col("o.ORIG_ORDER_DT_TM").alias("authored_datetime"),
        F.col("o.CURRENT_START_DT_TM").alias("effective_start_datetime"),
        F.col("o.PROJECTED_STOP_DT_TM").alias("projected_stop_datetime"),
        F.col("o.DISCONTINUE_EFFECTIVE_DT_TM").alias("discontinued_datetime"),
        F.col("o.FREQUENCY_ID").cast("string").alias("frequency_id"),
        (F.col("o.PRN_IND") == 1).alias("prn_ind"), (F.col("o.IV_IND") == 1).alias("iv_ind"),
        (F.col("o.SUSPEND_IND") == 1).alias("suspend_ind"),
        (F.col("o.RESUME_IND") == 1).alias("resume_ind"),
        (F.col("o.DISCONTINUE_IND") == 1).alias("discontinue_ind"),
        F.col("o.LAST_UPDATE_PROVIDER_ID").cast("bigint").alias("requester_practitioner_id"),
        F.col("o.ORGANIZATION_ID").cast("bigint").alias("organization_id"),
        F.col("o.CLINICAL_DISPLAY_LINE").alias("clinical_display_line"),
        F.col("o.ORDER_DETAIL_DISPLAY_LINE").alias("order_detail_display_line"),
        F.col("a.status_history_json").alias("_status_history_json"),
        F.coalesce(F.col("a.status_history_count"), F.lit(0)).cast("long")
         .alias("status_history_count"),
        F.col("i.ingredients_json").alias("_ingredients_json"),
        F.coalesce(F.col("i.ingredient_count"), F.lit(0)).cast("long").alias("ingredient_count"),
        F.col("d.order_details_json").alias("_order_details_json"),
        F.coalesce(F.col("d.order_detail_count"), F.lit(0)).cast("long")
         .alias("order_detail_count"),
        F.when(retracted, F.lit("retracted")).when(superseded, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        F.col("o.ORIG_ORDER_DT_TM").alias("record_status_effective_from"),
        F.when(retracted, F.col("o.SOURCE_ABSENT_DETECTED_TS"))
         .when(superseded, F.coalesce(F.col("o.DISCONTINUE_EFFECTIVE_DT_TM"),
                                     F.col("o.STATUS_DT_TM"))).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("medication_order").alias("source_feed"),
        F.date_format(loaded_at, "yyyyMMddHHmmss").alias("load_batch_id"),
        source_update.alias("source_update_timestamp"),
        loaded_at.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"),
        F.lit(SRC_MEDICATION_ORDER).alias("_source_table"),
        F.col("o.ORDER_ID").cast("string").alias("_source_row_id"),
    )


@materialized_view(
    name=_n("journey_clinical._medication_order_primitive"),
    refresh_policy="auto",
)
def _medication_order_primitive():
    # Build the declared dataset: Internal primitive medication-order join with deterministic
    # JSON boundaries.
    return _medication_order_primitive_query()


# Clinical /  qc medication order
@materialized_view(
    name=_n("journey_clinical._qc_medication_order"),
    refresh_policy="auto",
)
def _qc_medication_order():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # medication_order.
    return _cross_qc_primitive(
        _medication_order_canonical(),
        "medication_order",
        {
            "ingredients": "_qc_ingredients_json",
            "medication_code": "_qc_medication_code_json",
            "order_details": "_qc_order_details_json",
            "status_history": "_qc_status_history_json",
        },
    )


# Clinical / medication order

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
MEDICATION_ORDER_PUBLIC_COLUMNS = [
    'patient_event_key',
    'order_id',
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
    'medication_code',
    'order_status_code',
    'order_status_display',
    'department_status_code',
    'department_status_display',
    'active_status_code',
    'active_status_display',
    'intent_code',
    'category_code',  # BBR_FOLDIN_V1
    'category_display',  # BBR_FOLDIN_V1
    'category_basis',  # BBR_FOLDIN_V1
    'reported_ind',  # BBR_FOLDIN_V1
    'template_order_id',  # BBR_FOLDIN_V1
    'order_role',  # BBR_FOLDIN_V1
    'order_role_basis',  # BBR_FOLDIN_V1
    'content_order_id',  # BBR_FOLDIN_V1
    'medication_order_type_code',
    'medication_order_type_display',
    'authored_datetime',
    'effective_start_datetime',
    'projected_stop_datetime',
    'discontinued_datetime',
    'frequency_id',
    'prn_ind',
    'iv_ind',
    'suspend_ind',
    'resume_ind',
    'discontinue_ind',
    'requester_practitioner_id',
    'organization_id',
    'clinical_display_line',
    'order_detail_display_line',
    'status_history_count',
    'ingredient_count',
    'order_detail_count',
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


MEDICATION_ORDER_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'order_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


@materialized_view(
    name=_n("journey_clinical.medication_order"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def medication_order():
    # Build the declared dataset: One Millennium medication order with current state and ordered
    # action, ingredient, and detail evidence.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_medication_order")),
        "medication_order",
        {
            "medication_code": "_qc_medication_code_json",
        },
        MEDICATION_ORDER_PUBLIC_COLUMNS,
        MEDICATION_ORDER_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._medication_order_metadata'),
    refresh_policy="auto",
)
def medication_order_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_medication_order; same row grain as the research table. Join keys:
    # patient_event_key, order_id.
    return _s3_component_metadata("journey_clinical.medication_order", "journey_clinical._qc_medication_order", MEDICATION_ORDER_LIFECYCLE_COLUMNS)


if "source_patient_event_key" not in MEDICATION_ORDER_PUBLIC_COLUMNS:
    MEDICATION_ORDER_PUBLIC_COLUMNS.insert(MEDICATION_ORDER_PUBLIC_COLUMNS.index("patient_event_key") + 1, "source_patient_event_key")


for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_medication_order"].items():
    _s3_replace_public_variant(MEDICATION_ORDER_PUBLIC_COLUMNS, old_name, axes)


def _finish_medication_order(df):
    """medication_order: per-row finish step, moved verbatim from _cross_qc_finish (silver_journey_shared.py:1287)."""
    # BBR_FOLDIN_V1 (BBR-13): uncoded instances inherit the parent mapping
    df = _bbr_inherit_template_mapping(df)
    return df


CROSS_QC = {
    'medication_order': CrossQc(model='medication_order', fk=('encounter_id', 'person_id'), dates=('event_after_death_30d', 'event_before_birth'), coded_axes={'medication_code': ('medication', False)}, shared_quantities=('dose_value', 'dose_unit', 'volume_value', 'volume_unit', 'duration_value', 'duration_unit'), promote_lifecycle=['identity_status', 'person_id_resolved', 'encounter_id_resolved'], finish_steps=(_finish_medication_order,)),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {
    'clinical_medication_order': ValuePolicy(table='clinical_medication_order', enabled=True, zero_as_null=('frequency_id', 'requester_practitioner_id'), system_user_columns={'requester_practitioner_id': 'requester'}),
}


WRAP_POLICY = {'clinical_medication_order'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_medication_order_metadata': {'columns': {'activity_date': 'Primary-key component carried '
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
                                                  'clinical_medication_order; same row grain as the research '
                                                  'table. Join keys: patient_event_key, order_id.'},
 '_clinical_medication_order_primitive': {'columns': {},
                                          'table': 'Internal primitive medication-order join with '
                                                   'deterministic JSON boundaries.'},
 '_clinical_qc_medication_order': {'columns': {},
                                   'table': 'Private JSON bridge and Gold cross-rule flags for '
                                            'medication_order.'},
 'clinical_medication_order': {'columns': {'active_status_code': 'Source active-status code.',
                                           'active_status_display': 'Source active-status display.',
                                           'authored_datetime': 'Original order time.',
                                           'category_basis': 'Rule basis of category_code: '
                                                             "'ORIG_ORD_AS_FLAG=<flag>'. BBR_FOLDIN_V1 "
                                                             'BBR-14.',
                                           'category_code': 'FHIR MedicationRequest.category from Cerner '
                                                            'ORIG_ORD_AS_FLAG: inpatient (0 normal, 3 '
                                                            "patient's own, 4 pharmacy charge only, 5 "
                                                            'satellite), discharge (1 prescription/discharge '
                                                            'order), community (2 recorded/home medication); '
                                                            'NULL when the flag is absent. BBR_FOLDIN_V1 '
                                                            'BBR-14.',
                                           'category_display': 'Cerner label of ORIG_ORD_AS_FLAG, verbatim. '
                                                               'BBR_FOLDIN_V1 BBR-14.',
                                           'clinical_display_line': 'Source clinical display line.',
                                           'confidentiality_code': 'Source confidentiality classification '
                                                                   'attached to the record for each '
                                                                   'medication order record. It is produced '
                                                                   'by the silver transformation and has no '
                                                                   'direct bronze-column lineage entry. '
                                                                   'Source code meanings and sentinel values '
                                                                   'are retained unless the pipeline '
                                                                   'explicitly maps them; null means no code '
                                                                   'was supplied.',
                                           'content_order_id': 'The order whose attribute, detail and '
                                                               'ingredient rows describe this row: '
                                                               'template_order_id on an instance, else '
                                                               'order_id. Join '
                                                               'clinical_medication_order_attribute / '
                                                               '_detail / _ingredient on it (instances carry '
                                                               'none of their own; Ben decision BBR-D2, by '
                                                               'reference). BBR_FOLDIN_V1 BBR-13.',
                                           'department_status_code': 'Department workflow status code.',
                                           'department_status_display': 'Department workflow status display.',
                                           'discontinue_ind': 'Source discontinued indicator.',
                                           'discontinued_datetime': 'Discontinue effective time.',
                                           'effective_start_datetime': 'Current effective start.',
                                           'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                           'spine_encounter.encounter_id.',
                                           'event_datetime': 'Order authored or start time.',
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
                                           'event_end_datetime': 'Projected stop or discontinue time.',
                                           'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                       'wall-clock time (GMT/BST), by the '
                                                                       'same rule as event_datetime_local; '
                                                                       'null where event_end_datetime is '
                                                                       'null.',
                                           'frequency_id': 'Source frequency identifier.',
                                           'ingredient_count': 'Number of retained ingredient rows.',
                                           'intent_code': 'Medication request intent (FHIR): plan for a '
                                                          'recorded/home medication (ORIG_ORD_AS_FLAG 2, see '
                                                          'reported_ind), order for every other order; NULL '
                                                          'when the flag is absent. BBR_FOLDIN_V1 BBR-14 '
                                                          "(was the constant 'order').",
                                           'iv_ind': 'S10 medication_order; retained as a source-faithful '
                                                     'bronze input or Journey standard-block field.',
                                           'loaded_at': 'Greatest of map_medication_order.ADC_UPDT and the '
                                                        'per-ORDER_ID maximum ADC_UPDT from '
                                                        'map_medication_order_action, '
                                                        'map_medication_order_ingredient and '
                                                        'map_medication_order_detail. All contributing child '
                                                        'rows participate, not only the latest action; JSON '
                                                        'decoding and quality/public projections preserve '
                                                        'this combined ingestion clock.',
                                           'medication_map_candidate_count': 'Number of candidate targets '
                                                                             'observed; a mapped parent is '
                                                                             'unambiguous when this is 1.',
                                           'medication_map_candidate_set_id': 'Stable identifier of the '
                                                                              'retained candidate set.',
                                           'medication_map_competing_count': 'Candidates within 0.05 cosine '
                                                                             'of the selected candidate.',
                                           'medication_map_component_count': 'Number of source-derived '
                                                                             'components.',
                                           'medication_map_component_index': 'One-based source-derived '
                                                                             'component index.',
                                           'medication_map_confidence': 'Mapping confidence in [0,1] where '
                                                                        'the lane measured one.',
                                           'medication_map_cosine': 'Measured cosine between the normalised '
                                                                    'source and selected target; null when '
                                                                    'scoring did not run.',
                                           'medication_map_method': 'Mapping method: source_native, rule, '
                                                                    'exact_synonym or embedding_proposed.',
                                           'medication_map_rollup_levels': 'Maximum hierarchy distance to a '
                                                                           'selected common ancestor.',
                                           'medication_map_rule_id': 'Stable rule or lane identifier that '
                                                                     'produced the mapping.',
                                           'medication_map_scoring_model': 'Embedding model that produced '
                                                                           'map_cosine.',
                                           'medication_map_status': 'Explicit mapping outcome from the S3b '
                                                                    'status vocabulary.',
                                           'medication_map_status_reason': 'Reason for the mapping outcome '
                                                                           'or abstention.',
                                           'medication_map_version': 'Version of the source, terminology '
                                                                     'release or model used by the mapping.',
                                           'medication_omop_concept_id': 'OMOP concept identifier selected '
                                                                         'with the ordered medication '
                                                                         'mapping.',
                                           'medication_omop_source_concept_id': 'Non-standard OMOP source '
                                                                                'concept identifier, kept '
                                                                                'separate from a standard '
                                                                                'target.',
                                           'medication_order_type_code': 'Source medication-order type code.',
                                           'medication_order_type_display': 'Source medication-order type '
                                                                            'display.',
                                           'medication_snomed_code': 'SNOMED CT code selected for the '
                                                                     'ordered medication; null when no '
                                                                     'unambiguous mapping is available.',
                                           'medication_snomed_display': 'SNOMED CT preferred display '
                                                                        'selected for the ordered '
                                                                        'medication.',
                                           'medication_source_code': 'Source code for the ordered '
                                                                     'medication, retained without '
                                                                     'de-identification.',
                                           'medication_source_display': 'Source display for the ordered '
                                                                        'medication.',
                                           'medication_source_system': 'Canonical coding-system URI for the '
                                                                       'source ordered medication.',
                                           'medication_target_code': 'Code of the selected target in its own '
                                                                     'vocabulary.',
                                           'medication_target_display': 'Display of the selected target in '
                                                                        'its own vocabulary.',
                                           'medication_target_system': 'Coding-system URI of the selected '
                                                                       'target.',
                                           'order_detail_count': 'Number of retained detail rows.',
                                           'order_detail_display_line': 'Source order-detail display line.',
                                           'order_id': 'Millennium ORDER_ID; primary key of this table.',
                                           'order_role': 'Continuous-order role from TEMPLATE_ORDER_FLAG: '
                                                         'order (0, one-off), continuous_parent (1, the '
                                                         'prescription), instance (4, one scheduled dose of '
                                                         'the parent; administered doses are in '
                                                         'medication_admin). Count prescriptions with '
                                                         "order_role <> 'instance'. BBR_FOLDIN_V1 BBR-13.",
                                           'order_role_basis': 'Rule basis of order_role: '
                                                               "'TEMPLATE_ORDER_FLAG=<flag>'. BBR_FOLDIN_V1 "
                                                               'BBR-13.',
                                           'order_status_code': 'Current source order status code.',
                                           'order_status_display': 'Current source order status display.',
                                           'organization_id': 'Millennium ORGANIZATION_ID for the source '
                                                              'organization.',
                                           'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                'stable cross-feed join key.',
                                           'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                           'prn_ind': 'True when source `PRN_IND` equals 1, indicating '
                                                      'as-needed medication; other non-null values become '
                                                      'false and null remains null.',
                                           'projected_stop_datetime': 'Projected stop time.',
                                           'record_status': 'Retracted when SOURCE_PRESENT_IND is false '
                                                            '(null defaults true); otherwise superseded when '
                                                            'INACTIVE_ORDER_FLAG equals 1 (null defaults 0) '
                                                            'or ACTIVE_IND equals 0 (null defaults 1); '
                                                            'otherwise active. Retraction takes priority. '
                                                            'Clinical order status, discontinue flags and '
                                                            'child-source presence do not directly determine '
                                                            'this label.',
                                           'record_status_effective_from': 'map_medication_order.ORIG_ORDER_DT_TM '
                                                                           'carried unchanged as the history '
                                                                           'start. Unlike event_datetime, it '
                                                                           'has no CURRENT_START_DT_TM '
                                                                           'fallback; this authored-order '
                                                                           'time is not an independently '
                                                                           'observed status-transition '
                                                                           'timestamp.',
                                           'record_status_effective_to': 'SOURCE_ABSENT_DETECTED_TS for '
                                                                         'retracted orders, even if null; '
                                                                         'otherwise first non-null '
                                                                         'DISCONTINUE_EFFECTIVE_DT_TM then '
                                                                         'STATUS_DT_TM for superseded '
                                                                         'orders; otherwise null. There is '
                                                                         'no fallback between branches, no '
                                                                         'projected/soft-stop or ingestion '
                                                                         'fallback, and no sentinel range '
                                                                         'clamp.',
                                           'reported_ind': 'True for a recorded/home medication '
                                                           '(ORIG_ORD_AS_FLAG 2): a statement of what the '
                                                           'patient takes, not an order to dispense (FHIR '
                                                           'reportedBoolean). BBR_FOLDIN_V1 BBR-14.',
                                           'requester_practitioner_id': 'Millennium personnel PERSON_ID for '
                                                                        'the requesting practitioner.',
                                           'requester_system_ind': 'True when the source '
                                                                   'requester_practitioner_id was 1, the '
                                                                   'Cerner system user (J5); the id is then '
                                                                   'published as NULL. NULL when the source '
                                                                   'id was NULL. INSR_W2.',
                                           'resume_ind': 'Source resumed indicator.',
                                           'source_code': 'Source medication synonym identifier.',
                                           'source_coding_system': 'Source medication coding system.',
                                           'source_display': 'Source medication display.',
                                           'source_feed': 'Registered source feed.',
                                           'source_update_timestamp': "Greatest of the order's "
                                                                      'SOURCE_ADC_UPDT and ADC_UPDT plus '
                                                                      'each action, ingredient and detail '
                                                                      "group's maximum of "
                                                                      'greatest(SOURCE_ADC_UPDT, '
                                                                      'ORDER_SOURCE_ADC_UPDT, ADC_UPDT), '
                                                                      'grouped by ORDER_ID across all '
                                                                      'contributing rows. This mixes source '
                                                                      'and ingestion clocks; it is not '
                                                                      'solely a native/raw-source update '
                                                                      'timestamp or Silver refresh time.',
                                           'status_history_count': 'Number of retained action rows.',
                                           'subject_id_system': 'Subject-key identifier system.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'suspend_ind': 'Source suspended indicator.',
                                           'template_order_id': 'Cerner TEMPLATE_ORDER_ID: on an instance '
                                                                'row the parent continuous order (order_id '
                                                                'of its parent row); NULL when the order has '
                                                                'no template. BBR_FOLDIN_V1 BBR-13.',
                                           'vip_ind': 'Indicator that the source record carries VIP handling '
                                                      'status for each medication order record. It is '
                                                      'produced by the silver transformation and has no '
                                                      'direct bronze-column lineage entry. Null means the '
                                                      'source did not state the indicator and must not be '
                                                      'interpreted as false.',
                                           'withheld_identity_ind': 'Withheld identity indicator.'},
                               'table': 'One Millennium medication order with current state and ordered '
                                        'action, ingredient, and detail evidence.'}}

# INSR_R4_SILVER_V1 (parent): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import ParentInputs, parent_inputs_spec  # noqa: E402
R4_PARENT_INPUTS = ParentInputs(parent='clinical_medication_order', dataset='_clinical_medication_order_parent_inputs', keys=('order_id',), inputs=('identity_status', 'person_id', 'record_status'), admit_sql="coalesce(((identity_status = 'resolved') AND (record_status = 'active')), false)", name=_n('journey_clinical._medication_order_parent_inputs'), parent_name=_n('journey_clinical.medication_order'))
SPECS = list(SPECS) + [parent_inputs_spec(R4_PARENT_INPUTS)]
