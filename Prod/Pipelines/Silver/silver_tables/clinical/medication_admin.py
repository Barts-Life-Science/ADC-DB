"""Silver clinical.medication_admin: 4 dataset(s).

- datasets: _clinical_medication_admin_primitive, _clinical_qc_medication_admin, clinical_medication_admin, _clinical_medication_admin_metadata
- moved verbatim from: 2_silver_clinical_events:5139, 2_silver_clinical_events:5153, 2_silver_clinical_events:5344, 2_silver_clinical_events:5515, 2_silver_clinical_events:5541, 2_silver_clinical_events:5602, 2_silver_clinical_events:5674, 2_silver_clinical_events:5694

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import (
    _n,
    _present,
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
    SRC_MED_ADMIN,
    _medication_admin_canonical,
    _s3_replace_public_variant,
)
from silver_tables.clinical._shared import _s3_component_metadata
from silver_core.value_policy import _zero_measure_rule


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical /  medication admin primitive
def _med_admin_status_entry(sequence, status_kind, status_code, status_display,
                            effective_datetime, practitioner_id):
    # Construct one consistently typed medication-administration status-history entry.
    return F.struct(
        F.lit(sequence).cast("long").alias("sequence"),
        status_kind.cast("string").alias("status_kind"),
        status_code.cast("string").alias("status_code"),
        status_display.cast("string").alias("status_display"),
        effective_datetime.cast("timestamp").alias("effective_datetime"),
        practitioner_id.cast("string").alias("practitioner_id"),
        F.lit(None).cast("long").alias("source_action_sequence"),
        F.lit(True).alias("source_present_ind"),
    )


def _medication_admin_primitive_query():
    # Enrich administration rows with grouped ingredients and linked order evidence for the
    # internal stage.
    a = read_source(SRC_MED_ADMIN).alias("a")
    ingredients = spark.read.table(_n("journey_clinical._med_admin_ingredient_grouped")).alias("i")
    orders = read_source(SRC_MEDICATION_ORDER).select(
        F.col("ORDER_ID").alias("_linked_order_id")
    ).alias("o")
    joined = (
        a.join(ingredients, F.col("a.EVENT_ID") == F.col("i.EVENT_ID"), "left")
         .join(orders, F.col("a.ORDER_ID") == F.col("o._linked_order_id"), "left")
    )
    event_id = stable_id("medication_admin:mill", F.col("a.EVENT_ID"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("a.PERSON_ID"))], SRC_MED_ADMIN, F.col("a.EVENT_ID")
    )
    performer = F.coalesce(F.col("a.PERFORMED_PRSNL_ID"), F.col("a.PRSNL_ID"))
    verifier = F.coalesce(F.col("a.MAE_VERIFIED_PRSNL_ID"), F.col("a.CE_VERIFIED_PRSNL_ID"))
    status_entries = F.filter(F.array(
        _med_admin_status_entry(
            10, F.lit("scheduled"),
            F.when(F.col("a.SCHEDULED_DT_TM").isNotNull(), F.lit("scheduled")),
            F.when(F.col("a.SCHEDULED_DT_TM").isNotNull(), F.lit("Scheduled")),
            F.col("a.SCHEDULED_DT_TM"), F.lit(None).cast("string"),
        ),
        _med_admin_status_entry(
            20, F.lit("administration_event"), F.col("a.EVENT_TYPE_CD"),
            F.col("a.EVENT_TYPE_DISPLAY"),
            F.coalesce(F.col("a.PERFORMED_DT_TM"), F.col("a.ADMIN_START_DT_TM")),
            F.when(performer.isNotNull(), stable_id("practitioner:mill", performer)),
        ),
        _med_admin_status_entry(
            30, F.lit("result"), F.col("a.RESULT_STATUS_CD"),
            F.col("a.RESULT_STATUS_DISPLAY"),
            F.coalesce(F.col("a.ADMIN_END_DT_TM"), F.col("a.ADMIN_START_DT_TM")),
            F.when(verifier.isNotNull(), stable_id("practitioner:mill", verifier)),
        ),
        _med_admin_status_entry(
            40, F.lit("verified"),
            F.when(F.coalesce(F.col("a.VERIFICATION_DT_TM"),
                              F.col("a.VERIFIED_DT_TM")).isNotNull(), F.lit("verified")),
            F.when(F.coalesce(F.col("a.VERIFICATION_DT_TM"),
                              F.col("a.VERIFIED_DT_TM")).isNotNull(), F.lit("Verified")),
            F.coalesce(F.col("a.VERIFICATION_DT_TM"), F.col("a.VERIFIED_DT_TM")),
            F.when(verifier.isNotNull(), stable_id("practitioner:mill", verifier)),
        ),
        _med_admin_status_entry(
            50, F.lit("order_status"), F.col("a.ORDER_STATUS_CD"),
            F.col("a.ORDER_STATUS_DISPLAY"), F.col("a.ORDER_STATUS_DT_TM"),
            F.lit(None).cast("string"),
        ),
        _med_admin_status_entry(
            60, F.lit("suspended"),
            F.when((F.col("a.SUSPEND_IND") == 1) | F.col("a.SUSPEND_EFFECTIVE_DT_TM").isNotNull(),
                   F.lit("suspended")),
            F.when((F.col("a.SUSPEND_IND") == 1) | F.col("a.SUSPEND_EFFECTIVE_DT_TM").isNotNull(),
                   F.lit("Suspended")),
            F.col("a.SUSPEND_EFFECTIVE_DT_TM"), F.lit(None).cast("string"),
        ),
        _med_admin_status_entry(
            70, F.lit("resumed"),
            F.when((F.col("a.RESUME_IND") == 1) | F.col("a.RESUME_EFFECTIVE_DT_TM").isNotNull(),
                   F.lit("resumed")),
            F.when((F.col("a.RESUME_IND") == 1) | F.col("a.RESUME_EFFECTIVE_DT_TM").isNotNull(),
                   F.lit("Resumed")),
            F.col("a.RESUME_EFFECTIVE_DT_TM"), F.lit(None).cast("string"),
        ),
        _med_admin_status_entry(
            80, F.lit("discontinued"),
            F.when((F.col("a.DISCONTINUE_IND") == 1)
                   | F.col("a.DISCONTINUE_EFFECTIVE_DT_TM").isNotNull(), F.lit("discontinued")),
            F.when((F.col("a.DISCONTINUE_IND") == 1)
                   | F.col("a.DISCONTINUE_EFFECTIVE_DT_TM").isNotNull(), F.lit("Discontinued")),
            F.col("a.DISCONTINUE_EFFECTIVE_DT_TM"), F.lit(None).cast("string"),
        ),
    ), lambda x: x["status_code"].isNotNull() | x["effective_datetime"].isNotNull())
    ended = F.col("a.CE_VALID_UNTIL_DT_TM").isNotNull() & (
        F.col("a.CE_VALID_UNTIL_DT_TM") < F.lit("2100-01-01").cast("timestamp")
    )
    superseded = ended | (F.coalesce(F.col("a.MR_IS_CURRENT_IND"), F.lit(True)) == F.lit(False))
    loaded_at = F.greatest(F.col("a.ADC_UPDT"), F.col("i.ingredient_loaded_at"))
    source_update = F.greatest(
        F.col("a.MAE_ADC_UPDT"), F.col("a.MR_ADC_UPDT"), F.col("a.ORDERS_ADC_UPDT"),
        F.col("a.OI_ADC_UPDT"), F.col("a.SYNONYM_ADC_UPDT"), F.col("a.LOOKUP_ADC_UPDT"),
        F.col("a.ADC_UPDT"), F.col("i.ingredient_source_update_timestamp"),
    )
    with_status = joined.withColumn("_status_history_array", status_entries)
    # contract v2: publish the administration EVENT_ID and native relationship identifiers while retaining SHA relationship keys explicitly
    return with_status.select(
        event_id.alias("patient_event_key"),
        F.col("a.EVENT_ID").cast("bigint").alias("event_id"),
        F.when(F.col("o._linked_order_id").isNotNull(), F.col("a.ORDER_ID").cast("bigint")).alias("order_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        F.col("a.PERSON_ID").cast("bigint").alias("person_id"),
        F.when(_present(F.col("a.PERSON_ID")), F.lit("resolved"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.col("a.ENCNTR_ID").cast("bigint").alias("encounter_id"),
        F.coalesce(F.col("a.ADMIN_START_DT_TM"), F.col("a.PERFORMED_DT_TM"),
                   F.col("a.SCHEDULED_DT_TM")).alias("event_datetime"),
        F.col("a.ADMIN_END_DT_TM").alias("event_end_datetime"),
        F.coalesce(F.col("a.ADMIN_START_DT_TM_LOCAL"), F.col("a.PERFORMED_DT_TM_LOCAL"), F.col("a.SCHEDULED_DT_TM_LOCAL")).alias("event_datetime_local"),
        F.col("a.ADMIN_END_DT_TM_LOCAL").alias("event_end_datetime_local"),
        F.lit("urn:cerner:order_synonym_id").alias("source_coding_system"),
        F.col("a.ORDER_SYNONYM_ID").cast("string").alias("source_code"),
        F.coalesce(F.col("a.ORDER_MNEMONIC"), F.col("a.ORDERED_AS_MNEMONIC"),
                   F.col("a.HNA_ORDER_MNEMONIC")).alias("source_display"),
        codeable_concept_json(
            coding_obj(F.lit("urn:cerner:order_synonym_id"), F.col("a.ORDER_SYNONYM_ID"),
                       F.coalesce(F.col("a.ORDER_MNEMONIC"), F.col("a.ORDERED_AS_MNEMONIC")), True),
            coding_obj(F.lit("http://www.nlm.nih.gov/research/umls/rxnorm"), F.col("a.RXNORM_CUI"),
                       F.col("a.RXNORM_STR"), False, "bronze.map_med_admin", None),
            coding_obj(F.lit("http://snomed.info/sct"),
                       F.coalesce(F.col("a.SNOMED_VALIDATED_CODE"), F.col("a.SNOMED_CODE"),
                                  F.col("a.LOOKUP_SNOMED_CODE")),
                       F.coalesce(F.col("a.SNOMED_VALIDATED_STR"), F.col("a.SNOMED_STR"),
                                  F.col("a.LOOKUP_SNOMED_FROM_OMOP")),
                       False, "bronze.map_med_admin", None),
            coding_obj(F.lit("urn:omop:concept_id"),
                       F.coalesce(F.col("a.OMOP_STANDARD_CONCEPT_ID"), F.col("a.OMOP_CONCEPT_ID")),
                       F.coalesce(F.col("a.OMOP_STANDARD_CONCEPT_NAME"), F.col("a.OMOP_CONCEPT_NAME")),
                       False, "bronze.map_med_admin", None),
        ).alias("_medication_code_json"), F.col("a.MED_ADMIN_MAPPING").alias("_r1_med_admin_mapping"), F.col("a.MED_ADMIN_MAP_KEY").alias("_r1_med_admin_map_key"), F.col("a.MED_ADMIN_MAP_COMPONENTS_JSON").alias("_r1_med_admin_components_json"),
        F.when(F.col("o._linked_order_id").isNotNull(),
               stable_id("medication_order:mill", F.col("a.ORDER_ID"))).alias("medication_order_key"),
        F.col("a.RESULT_STATUS_CD").cast("string").alias("administration_status_code"),
        F.col("a.RESULT_STATUS_DISPLAY").alias("administration_status_display"),
        F.col("a.EVENT_TYPE_CD").cast("string").alias("source_event_type_code"),
        F.col("a.EVENT_TYPE_DISPLAY").alias("source_event_type_display"),
        F.to_json(F.col("_status_history_array")).alias("_status_history_json"),
        F.size(F.col("_status_history_array")).cast("long").alias("status_history_count"),
        F.coalesce(F.col("a.ADMIN_DOSAGE"), F.col("a.DOSE_VALUE_EFFECTIVE"))
         .cast("decimal(38,10)").alias("dose_value"),
        F.coalesce(F.col("a.ADMIN_DOSAGE_UNIT_DISPLAY"), F.col("a.DOSE_UNIT_NORMALIZED"))
         .alias("dose_unit"),
        F.col("a.INITIAL_DOSAGE").cast("decimal(38,10)").alias("initial_dose_value"),
        F.col("a.INITIAL_DOSAGE_UNIT_DISPLAY").alias("initial_dose_unit"),
        F.col("a.DOSE_IN_MG").cast("decimal(38,10)").alias("dose_in_mg"),
        F.col("a.DOSE_IN_ML").cast("decimal(38,10)").alias("dose_in_ml"),
        F.col("a.DOSE_STANDARDIZATION_STATUS").alias("dose_standardization_status"),
        F.col("a.ADMIN_ROUTE_CD").cast("string").alias("route_code"),
        F.col("a.ADMIN_ROUTE_DISPLAY").alias("route_display"),
        F.col("a.ADMIN_SITE_CD").cast("string").alias("site_code"),
        F.col("a.ADMIN_SITE_DISPLAY").alias("site_display"),
        F.col("a.INFUSED_VOLUME").cast("decimal(38,10)").alias("infused_volume"),
        F.col("a.INFUSED_VOLUME_UNIT_DISPLAY").alias("infused_volume_unit"),
        F.col("a.INFUSION_RATE").cast("decimal(38,10)").alias("infusion_rate"),
        F.col("a.INFUSION_UNIT_DISPLAY").alias("infusion_rate_unit"),
        F.col("i.ingredients_json").alias("_ingredients_json"),
        F.coalesce(F.col("i.ingredient_count"), F.lit(0)).cast("long").alias("ingredient_count"),
        F.coalesce(F.col("i.all_ingredients_zero_ind"), F.lit(False)).alias("all_ingredients_zero_ind"),  # INSR_W2 W2.05
        performer.cast("bigint").alias("performer_practitioner_id"),
        verifier.cast("bigint").alias("verifier_practitioner_id"),
        F.col("a.NURSE_UNIT_CD").cast("string").alias("location_code"),
        F.when(F.col("a.NURSE_UNIT_CD").isNotNull(),
               stable_id("location:mill:nurse_unit", F.col("a.NURSE_UNIT_CD"))).alias("location_key"),
        F.col("a.ORGANIZATION_ID").cast("bigint").alias("organization_id"),
        F.col("a.SCHEDULED_DT_TM").alias("scheduled_datetime"),
        F.coalesce(F.col("a.PERFORMED_DT_TM"), F.col("a.ADMIN_START_DT_TM"))
         .alias("performed_datetime"),
        F.coalesce(F.col("a.VERIFICATION_DT_TM"), F.col("a.VERIFIED_DT_TM"))
         .alias("verified_datetime"),
        F.col("a.ORDER_STATUS_CD").cast("string").alias("order_status_code"),
        F.col("a.ORDER_STATUS_DISPLAY").alias("order_status_display"),
        (F.col("a.PRN_IND") == 1).alias("prn_ind"), (F.col("a.IV_IND") == 1).alias("iv_ind"),
        F.when(superseded, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        F.coalesce(F.col("a.CE_VALID_FROM_DT_TM"), F.col("a.MR_VALID_FROM_DT_TM"),
                   F.col("a.ADMIN_START_DT_TM")).alias("record_status_effective_from"),
        F.when(superseded, F.coalesce(F.col("a.CE_VALID_UNTIL_DT_TM"),
                                     F.col("a.MR_VALID_UNTIL_DT_TM")))
         .alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("med_admin").alias("source_feed"),
        F.date_format(loaded_at, "yyyyMMddHHmmss").alias("load_batch_id"),
        source_update.alias("source_update_timestamp"),
        loaded_at.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_MED_ADMIN).alias("_source_table"),
        F.col("a.EVENT_ID").cast("string").alias("_source_row_id"),
        F.coalesce(F.col("a.SNOMED_VALIDATED_CODE"), F.col("a.SNOMED_CODE"),
                   F.col("a.LOOKUP_SNOMED_CODE")).cast("string").alias("_snomed_code"),
        F.coalesce(F.col("a.SNOMED_VALIDATED_STR"), F.col("a.SNOMED_STR"),
                   F.col("a.LOOKUP_SNOMED_FROM_OMOP")).alias("_snomed_display"),
        F.coalesce(F.col("a.OMOP_STANDARD_CONCEPT_ID"), F.col("a.OMOP_CONCEPT_ID"))
         .cast("string").alias("_omop_code"),
        F.coalesce(F.col("a.OMOP_STANDARD_CONCEPT_NAME"), F.col("a.OMOP_CONCEPT_NAME"))
         .alias("_omop_display"),
        F.col("a.RXNORM_CUI").cast("string").alias("_rxnorm_code"),
        F.col("a.RXNORM_STR").alias("_rxnorm_display"),
    )


@materialized_view(
    name=_n("journey_clinical._medication_admin_primitive"),
    refresh_policy="auto",
)
def _medication_admin_primitive():
    # Build the declared dataset: Internal primitive medication-administration join with
    # deterministic JSON boundaries.
    return _medication_admin_primitive_query()


# Clinical /  qc medication admin
@materialized_view(
    name=_n("journey_clinical._qc_medication_admin"),
    refresh_policy="auto",
)
def _qc_medication_admin():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for
    # medication_admin.
    return _cross_qc_primitive(
        _medication_admin_canonical(),
        "medication_admin",
        {
            "ingredients": "_qc_ingredients_json",
            "medication_code": "_qc_medication_code_json",
            "status_history": "_qc_status_history_json",
        },
    )


# Clinical / medication admin

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
MEDICATION_ADMIN_PUBLIC_COLUMNS = [
    'patient_event_key',
    'event_id',
    'order_id',
    'location_key',
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
    'medication_order_key',
    'administration_status_code',
    'administration_status_display',
    'source_event_type_code',
    'source_event_type_display',
    'status_history_count',
    'dose_value',
    'dose_unit',
    'initial_dose_value',
    'initial_dose_unit',
    'dose_in_mg',
    'dose_in_ml',
    'dose_standardization_status',
    'route_code',
    'route_display',
    'site_code',
    'site_display',
    'infused_volume',
    'infused_volume_unit',
    'infusion_rate',
    'infusion_rate_unit',
    'ingredient_count',
    'performer_practitioner_id',
    'verifier_practitioner_id',
    'location_code',
    'organization_id',
    'scheduled_datetime',
    'performed_datetime',
    'verified_datetime',
    'order_status_code',
    'order_status_display',
    'prn_ind',
    'iv_ind',
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


MEDICATION_ADMIN_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'event_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


@materialized_view(
    name=_n("journey_clinical.medication_admin"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def medication_admin():
    # Build the declared dataset: One Millennium medication-administration event with current
    # state and ordered history.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_medication_admin")),
        "medication_admin",
        {
            "medication_code": "_qc_medication_code_json",
        },
        MEDICATION_ADMIN_PUBLIC_COLUMNS,
        MEDICATION_ADMIN_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._medication_admin_metadata'),
    refresh_policy="auto",
)
def medication_admin_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_medication_admin; same row grain as the research table. Join keys:
    # patient_event_key, event_id.
    return _s3_component_metadata("journey_clinical.medication_admin", "journey_clinical._qc_medication_admin", MEDICATION_ADMIN_LIFECYCLE_COLUMNS)


if "source_patient_event_key" not in MEDICATION_ADMIN_PUBLIC_COLUMNS:
    MEDICATION_ADMIN_PUBLIC_COLUMNS.insert(MEDICATION_ADMIN_PUBLIC_COLUMNS.index("patient_event_key") + 1, "source_patient_event_key")


for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_medication_admin"].items():
    _s3_replace_public_variant(MEDICATION_ADMIN_PUBLIC_COLUMNS, old_name, axes)


CROSS_QC = {
    'medication_admin': CrossQc(model='medication_admin', fk=('encounter_id', 'person_id'), dates=('event_after_death_30d', 'event_before_birth'), coded_axes={'medication_code': ('medication', False)}, shared_quantities=('dose_value', 'dose_unit', 'volume_value', 'volume_unit', 'rate_value', 'rate_unit'), extra_public_columns=('all_ingredients_zero_ind',), promote_lifecycle=['identity_status', 'person_id_resolved', 'encounter_id_resolved']),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {
    'clinical_medication_admin': ValuePolicy(table='clinical_medication_admin', enabled=True, zero_as_null=('verifier_practitioner_id',)),
}


for _value, _unit in [("dose_value", "dose_unit"), ("initial_dose_value", "initial_dose_unit"),
                      ("infused_volume", "infused_volume_unit"), ("infusion_rate", "infusion_rate_unit")]:
    VALUE_POLICIES["clinical_medication_admin"].value_rule(_value, [_value, _unit],
                                                           _zero_measure_rule(_value, _unit))


# Master W2.05 "initial_dose_value -> NULL" (F8: correct the initial_dose_value pass rule). Measured on a 1% prod
# sample (2026-10-30): every non-NULL initial_dose_value is 0 (547,324 of 547,324), always with a unit, so the
# zero-with-no-unit rule above never fires on it. A 0 initial dose records nothing: NULL it whatever the unit.
VALUE_POLICIES["clinical_medication_admin"].value_rule("initial_dose_value", ["initial_dose_value"],
           lambda F: F.when(F.col("initial_dose_value") != 0, F.col("initial_dose_value")))


# Master W2.05 "infusion zeros on non-infusion rows -> NULL": a 0 infused volume or rate on an administration that
# is not IV (iv_ind false or NULL) records nothing (1% sample: 527,384 such rows, 39,566 of them with a unit).
# IV rows keep a recorded 0.
for _value in ("infused_volume", "infusion_rate"):
    VALUE_POLICIES["clinical_medication_admin"].value_rule(_value, [_value, "iv_ind"],
               lambda F, _v=_value: F.when(~((F.col(_v) == 0) & ~F.coalesce(F.col("iv_ind"), F.lit(False))), F.col(_v)))


WRAP_POLICY = {'clinical_medication_admin'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_medication_admin_metadata': {'columns': {'activity_date': 'Primary-key component carried '
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
                                                  'clinical_medication_admin; same row grain as the research '
                                                  'table. Join keys: patient_event_key, event_id.'},
 '_clinical_medication_admin_primitive': {'columns': {},
                                          'table': 'Internal primitive medication-administration join with '
                                                   'deterministic JSON boundaries.'},
 '_clinical_qc_medication_admin': {'columns': {},
                                   'table': 'Private JSON bridge and Gold cross-rule flags for '
                                            'medication_admin.'},
 'clinical_medication_admin': {'columns': {'administration_status_code': 'Current source result status code.',
                                           'administration_status_display': 'Current source result status '
                                                                            'display.',
                                           'all_ingredients_zero_ind': 'True when every ingredient of this '
                                                                       'administration recorded a dose of 0 '
                                                                       '(J24): the ingredient doses are then '
                                                                       'published as NULL in ingredients and '
                                                                       'clinical_medication_admin_ingredient. '
                                                                       'INSR_W2 W2.05.',
                                           'confidentiality_code': 'Source confidentiality classification '
                                                                   'attached to the record for each '
                                                                   'medication admin record. It is produced '
                                                                   'by the silver transformation and has no '
                                                                   'direct bronze-column lineage entry. '
                                                                   'Source code meanings and sentinel values '
                                                                   'are retained unless the pipeline '
                                                                   'explicitly maps them; null means no code '
                                                                   'was supplied.',
                                           'dose_in_mg': 'Bronze-standardized milligram dose.',
                                           'dose_in_ml': 'Bronze-standardized millilitre dose.',
                                           'dose_standardization_status': 'Bronze dose-standardization '
                                                                          'status.',
                                           'dose_unit': 'Administered dose unit.',
                                           'dose_value': 'Effective administered dose.',
                                           'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                           'spine_encounter.encounter_id.',
                                           'event_datetime': 'Administration start or performed time.',
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
                                           'event_end_datetime': 'Administration end time.',
                                           'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                       'wall-clock time (GMT/BST), by the '
                                                                       'same rule as event_datetime_local; '
                                                                       'null where event_end_datetime is '
                                                                       'null.',
                                           'event_id': 'Millennium medication-administration EVENT_ID; '
                                                       'primary key of this table.',
                                           'infused_volume': 'The volume at any one point in time that '
                                                             'remains in the IV Bag.',
                                           'infused_volume_unit': 'Text description of the unit of measure '
                                                                  'for infused volume.',
                                           'infusion_rate': 'For continuously administered medications, IV '
                                                            'or IVP, the infusion rate and unit is used to '
                                                            'capture the flow rate of the medication into '
                                                            'the patient.',
                                           'infusion_rate_unit': 'Text description of the unit of measure '
                                                                 'for volume or quantity of the medication.',
                                           'ingredient_count': 'Number of retained ingredient rows.',
                                           'initial_dose_unit': 'Initially documented dose unit.',
                                           'initial_dose_value': 'Initially documented dose.',
                                           'iv_ind': 'Indicator showing whether the order is an intravenous '
                                                     '(IV) medication order.',
                                           'loaded_at': 'Greatest of map_med_admin.ADC_UPDT and the maximum '
                                                        'map_med_admin_ingredient.ADC_UPDT for the same '
                                                        'EVENT_ID. The order-ID existence join contributes '
                                                        'no separate load timestamp. This combines '
                                                        'administration and ingredient ingestion provenance, '
                                                        'not administration event time or Silver refresh '
                                                        'time; null only when all contributing load clocks '
                                                        'are null.',
                                           'location_code': 'Millennium NURSE_UNIT_CD for the administration '
                                                            'location.',
                                           'location_key': 'Deterministic SHA-256 nurse-unit key retained '
                                                           'beside location_code.',
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
                                                                         'with the administered medication '
                                                                         'mapping.',
                                           'medication_omop_source_concept_id': 'Non-standard OMOP source '
                                                                                'concept identifier, kept '
                                                                                'separate from a standard '
                                                                                'target.',
                                           'medication_order_key': 'Deterministic SHA-256 key of the linked '
                                                                   'medication order.',
                                           'medication_snomed_code': 'SNOMED CT code selected for the '
                                                                     'administered medication; null when no '
                                                                     'unambiguous mapping is available.',
                                           'medication_snomed_display': 'SNOMED CT preferred display '
                                                                        'selected for the administered '
                                                                        'medication.',
                                           'medication_source_code': 'Source code for the administered '
                                                                     'medication, retained without '
                                                                     'de-identification.',
                                           'medication_source_display': 'Source display for the administered '
                                                                        'medication.',
                                           'medication_source_system': 'Canonical coding-system URI for the '
                                                                       'source administered medication.',
                                           'medication_target_code': 'Code of the selected target in its own '
                                                                     'vocabulary.',
                                           'medication_target_display': 'Display of the selected target in '
                                                                        'its own vocabulary.',
                                           'medication_target_system': 'Coding-system URI of the selected '
                                                                       'target.',
                                           'order_id': 'Millennium medication ORDER_ID when the linked order '
                                                       'exists.',
                                           'order_status_code': 'Linked source order status code.',
                                           'order_status_display': 'Linked source order status display.',
                                           'organization_id': 'Millennium ORGANIZATION_ID for the source '
                                                              'organization.',
                                           'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                'stable cross-feed join key.',
                                           'performed_datetime': 'Performed administration time.',
                                           'performer_practitioner_id': 'Millennium personnel PERSON_ID for '
                                                                        'the administration performer.',
                                           'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                           'prn_ind': 'True when source `PRN_IND` equals 1, indicating '
                                                      'as-needed medication; other non-null values become '
                                                      'false and null remains null.',
                                           'record_status': 'Derived administration-row status: superseded '
                                                            'when map_med_admin.CE_VALID_UNTIL_DT_TM is '
                                                            'before 2100-01-01 or MR_IS_CURRENT_IND is '
                                                            'false; otherwise active. Null MR_IS_CURRENT_IND '
                                                            'defaults true and a null clinical-event end '
                                                            'alone does not supersede the row. This '
                                                            'projection does not emit retracted or derive '
                                                            'status from the administration result/workflow '
                                                            'code.',
                                           'record_status_effective_from': 'First non-null '
                                                                           'map_med_admin.CE_VALID_FROM_DT_TM, '
                                                                           'MR_VALID_FROM_DT_TM, then '
                                                                           'ADMIN_START_DT_TM. This is a '
                                                                           'source validity start with an '
                                                                           'administration-time fallback, '
                                                                           'not a uniform independent '
                                                                           'status-change timestamp; null '
                                                                           'when all three are missing.',
                                           'record_status_effective_to': 'For a superseded administration, '
                                                                         'first non-null '
                                                                         'map_med_admin.CE_VALID_UNTIL_DT_TM '
                                                                         'then MR_VALID_UNTIL_DT_TM; null '
                                                                         'for active rows or when both are '
                                                                         'missing. The end selection does '
                                                                         'not remove far-future sentinels, '
                                                                         'so supersession caused by '
                                                                         'MR_IS_CURRENT_IND=false can retain '
                                                                         'an open-ended clinical-event '
                                                                         'timestamp.',
                                           'route_code': 'Administration route code.',
                                           'route_display': 'Administration route display.',
                                           'scheduled_datetime': 'Scheduled administration time.',
                                           'site_code': 'Administration site code.',
                                           'site_display': 'Administration site display.',
                                           'source_code': 'Source order-synonym identifier.',
                                           'source_coding_system': 'Source medication coding system.',
                                           'source_display': 'Source medication display.',
                                           'source_event_type_code': 'Source medication event type code.',
                                           'source_event_type_display': 'Source medication event type '
                                                                        'display.',
                                           'source_feed': 'Registered source feed.',
                                           'source_update_timestamp': 'Greatest of '
                                                                      'map_med_admin.MAE_ADC_UPDT, '
                                                                      'MR_ADC_UPDT, ORDERS_ADC_UPDT, '
                                                                      'OI_ADC_UPDT, SYNONYM_ADC_UPDT, '
                                                                      'LOOKUP_ADC_UPDT and ADC_UPDT, plus '
                                                                      'the per-EVENT_ID maximum of '
                                                                      'greatest(UPDT_DT_TM, ADC_UPDT) from '
                                                                      'map_med_admin_ingredient. This '
                                                                      'combines contributing '
                                                                      'source/lookup/ingestion clocks, not a '
                                                                      'single application update time or '
                                                                      'Silver refresh time; null only when '
                                                                      'all contributing clocks are null.',
                                           'status_history_count': 'Number of retained status milestones.',
                                           'subject_id_system': 'Subject-key identifier system.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'verified_datetime': 'Date and time at which the medication '
                                                                'administration event was verified.',
                                           'verifier_practitioner_id': 'Millennium personnel PERSON_ID for '
                                                                       'the administration verifier.',
                                           'vip_ind': 'Indicator that the source record carries VIP handling '
                                                      'status for each medication admin record. It is '
                                                      'produced by the silver transformation and has no '
                                                      'direct bronze-column lineage entry. Null means the '
                                                      'source did not state the indicator and must not be '
                                                      'interpreted as false.',
                                           'withheld_identity_ind': 'Withheld identity indicator.'},
                               'table': 'One Millennium medication-administration event with current state '
                                        'and ordered history.'}}

# INSR_R4_SILVER_V1 (parent): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import ParentInputs, parent_inputs_spec  # noqa: E402
R4_PARENT_INPUTS = ParentInputs(parent='clinical_medication_admin', dataset='_clinical_medication_admin_parent_inputs', keys=('event_id',), inputs=('identity_status', 'person_id'), admit_sql="coalesce(((identity_status = 'resolved')), false)", name=_n('journey_clinical._medication_admin_parent_inputs'), parent_name=_n('journey_clinical.medication_admin'))
SPECS = list(SPECS) + [parent_inputs_spec(R4_PARENT_INPUTS)]
