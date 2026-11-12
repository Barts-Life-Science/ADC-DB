"""Silver clinical._shared: 0 dataset(s).

- shared helpers (no datasets)
- moved verbatim from: 2_silver_clinical_events:245, 2_silver_clinical_events:347, 2_silver_clinical_events:4254, 2_silver_clinical_events:4302, 2_silver_clinical_events:4305, 2_silver_clinical_events:4343, 2_silver_clinical_events:4358, 2_silver_clinical_events:4548

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, axis_columns, read_source, spark
from silver_tables._common.clinical_events import SRC_FORM_ITEM
from silver_tables._common.journey import S3_TABLE_AXIS_SPECS, _s3_replace_public_variant


# STM_SPECIMEN_TYPE_V1/silver-specimen: specimen-type keys in the adapter shape; source_code = "<bronze namespace>|<code>".

# Measured cosines for the legacy pathology and device tier lanes. Promoted from
# 8_dev.lookup.tier_lane_rescore on 2026-09-26; a prod pipeline must never read 8_dev.
# TIER_RESCORE_PROD_SOURCE_PATCH_APPLIED v1
SRC_S3C_RXNORM_MULTUM_MAP = "3_lookup.omop.rxnorm_multum_snomed_map"


def _numeric_route_expr(s):
    # Classify numeric events as scores, vital signs or residual findings using the existing
    # label rules.
    label = F.lower(F.coalesce(
        s.EVENT_LABEL, s.EVENT_CD_DISPLAY, s.OMOP_MANUAL_CONCEPT_NAME, F.lit("")
    ))
    score = label.rlike("(news|ews|score|scale|glasgow|gcs|braden|waterlow|morse|frailty|risk)")
    vital = label.rlike(
        r"^(systolic blood pressure|diastolic blood pressure|mean arterial pressure[\s\S]*|"
        r"respiratory rate|heart rate[\s\S]*|peripheral pulse rate|pulse rate|spo2|"
        r"oxygen saturation[\s\S]*|temperature [\s\S]*|weight|height|body mass index|bmi|"
        "capillary refill time actual)$"
    )
    return F.when(score, F.lit("clinical_score")).when(vital, F.lit("vital_sign")).otherwise(F.lit("excluded"))


def _s3_component_metadata(public_relation, qc_relation, columns):
    # Join each published component back to its source event's quality metadata using
    # source_patient_event_key.
    public = spark.read.table(_n(public_relation)).select("patient_event_key", "source_patient_event_key")
    qc = spark.read.table(_n(qc_relation)).alias("q")
    return (public.alias("p").join(qc, F.col("p.source_patient_event_key") == F.col("q.patient_event_key"))
            .select(F.col("p.patient_event_key").alias("patient_event_key"),
                    *[F.col("q." + c).alias(c) for c in columns if c != "patient_event_key"]))


# Clinical /  problem revision grouped

# ==== Typed clinical facts — condition and procedure ====
SRC_PROBLEM_HISTORY = "4_prod.bronze.map_problem_history"


# Clinical /  implant attribute grouped
SRC_IMPLANT_EVENTS = "4_prod.bronze.map_implant_detail_events"


OPCS_LATERALITY_DISPLAY = {
    "Z94.1": "Bilateral", "Z94.2": "Right", "Z94.3": "Left", "Z94.4": "Unilateral (side not stated)",
    "Z94.8": "Other specified laterality", "Z94.9": "Unspecified laterality",
}


def _opcs_laterality_display(code):
    """Display text for a Z94.x laterality code."""
    return F.create_map(*[F.lit(x) for pair in OPCS_LATERALITY_DISPLAY.items() for x in pair])[code]


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
VITAL_PUBLIC_COLUMNS = [
    'patient_event_key',
    'event_id',
    'doc_response_key',
    'source_dcp_forms_activity_id',
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
    'vital_code',
    'value_number',
    'value_text',
    'unit_source_value',
    'unit_concept_id',
    'reference_range_low',
    'reference_range_high',
    'method_code',
    'method_display',
    'body_site_code',
    'body_site_display',
    'interpretation_code',
    'interpretation_display',
    'result_status_code',
    'result_status_display',
    'performer_practitioner_id',
    'source_form_key',
    'promotion_rule_id',
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


for _s3b_axis in ("unit", "method", "body_site", "interpretation"):
    VITAL_PUBLIC_COLUMNS.extend(axis_columns(_s3b_axis))


VITAL_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'doc_response_key',
    'event_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


# Form promotion source
def _form_promotion_source(target_fact_type):
    # Join form items to active promotion rules for the requested vital-sign or score fact type.
    i = read_source(SRC_FORM_ITEM).alias("i")
    c = read_source(SRC_FORM_PROMOTION_CONFIG).where(
        F.col("active_ind") & (F.col("target_fact_type") == target_fact_type)
    ).alias("c")
    return i.join(c, i.DCP_INPUT_REF_ID == c.dcp_input_ref_id, "inner")


# Clinical /  promoted vital sign primitive
SRC_FORM_PROMOTION_CONFIG = "3_lookup.omop.form_promotion_config"


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
SCORE_PUBLIC_COLUMNS = [
    'patient_event_key',
    'event_id',
    'doc_response_key',
    'source_dcp_forms_activity_id',
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
    'score_code',
    'score_name',
    'value_number',
    'value_text',
    'unit_source_value',
    'unit_concept_id',
    'component_count',
    'interpretation_code',
    'interpretation_display',
    'result_status_code',
    'result_status_display',
    'performer_practitioner_id',
    'source_form_key',
    'promotion_rule_id',
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


SCORE_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'doc_response_key',
    'event_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


# Clinical /  med admin ingredient grouped
SRC_MED_ADMIN_INGREDIENT = "4_prod.bronze.map_med_admin_ingredient"


# Clinical /  med order ingredient grouped
SRC_MEDICATION_ORDER_INGREDIENT = "4_prod.bronze.map_medication_order_ingredient"


# Clinical /  med order detail grouped
SRC_MEDICATION_ORDER_DETAIL = "4_prod.bronze.map_medication_order_detail"


CONDITION_STAGE_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'aria_pt_id',
    'aria_dx_id',
    'identity_status',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]


for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_vital_sign"].items():
    _s3_replace_public_variant(VITAL_PUBLIC_COLUMNS, old_name, axes)


for old_name, axes in S3_TABLE_AXIS_SPECS["clinical_clinical_score"].items():
    _s3_replace_public_variant(SCORE_PUBLIC_COLUMNS, old_name, axes)
