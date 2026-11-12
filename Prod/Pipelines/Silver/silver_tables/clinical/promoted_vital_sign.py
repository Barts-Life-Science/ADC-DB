"""Silver clinical.promoted_vital_sign: 1 dataset(s).

- datasets: _clinical_promoted_vital_sign_primitive
- moved verbatim from: 2_silver_clinical_events:4360, 2_silver_clinical_events:4429, silver_journey_shared.py:4520

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import (
    _n,
    _present,
    codeable_concept,
    coding_obj,
    stable_id,
    subject_key_with_system,
)
from silver_tables._common.clinical_events import SRC_FORM_ITEM
from silver_tables._common.journey import VITAL_STAGE_COLUMNS
from silver_tables.clinical._shared import _form_promotion_source


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# INSR R1: the producer has no lane columns; _r1_attach_silver_arm adds them when _promoted_vital_from_stage reads.
PROMOTED_VITAL_STAGE_COLUMNS = [c for c in VITAL_STAGE_COLUMNS if c != "vital_code" and not c.startswith("_r1_")]


def _promoted_vital_canonical():
    # Assemble normalized promoted vital rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = _form_promotion_source("vital_sign")
    event_id = stable_id("form_promotion:vital_sign", F.col("i.DOC_RESPONSE_KEY"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("i.PERSON_ID_LONG"))],
        SRC_FORM_ITEM, F.col("i.DOC_RESPONSE_KEY"),
    )
    deleted = F.coalesce(F.col("i.CANONICAL_SOURCE_DELETED_IND"), F.lit(False))
    absent = ~F.coalesce(F.col("i.SOURCE_PRESENT_IND"), F.lit(False))
    inactive = F.coalesce(F.col("i.ACTIVE_IND"), F.lit(0)) == 0
    # contract v2: publish PowerForm response identity and native foreign keys for the promoted vital arm
    return s.select(
        event_id.alias("patient_event_key"),
        F.lit(None).cast("bigint").alias("event_id"),
        F.col("i.DOC_RESPONSE_KEY").cast("string").alias("doc_response_key"),
        F.col("i.DCP_FORMS_ACTIVITY_ID").cast("bigint").alias("source_dcp_forms_activity_id"),
        F.lit("form_promotion").alias("source_object"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        F.col("i.PERSON_ID_LONG").cast("bigint").alias("person_id"),
        F.when(_present(F.col("i.PERSON_ID_LONG")), F.lit("resolved")).otherwise(F.lit("unresolved"))
         .alias("identity_status"),
        F.col("i.ENCNTR_ID_LONG").cast("bigint").alias("encounter_id"),
        F.coalesce(F.col("i.RESPONSE_DT_TM"), F.col("i.PERFORMED_DT_TM"),
                   F.col("i.DOCUMENTATION_DT_TM")).alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        F.coalesce(F.col("i.RESPONSE_DT_TM_LOCAL"), F.col("i.PERFORMED_DT_TM_LOCAL"), F.col("i.DOCUMENTATION_DT_TM_LOCAL")).alias("event_datetime_local"),
        F.lit(None).cast("timestamp").alias("event_end_datetime_local"),
        F.col("c.source_coding_system").alias("source_coding_system"),
        F.col("c.source_code").alias("source_code"), F.col("c.source_display").alias("source_display"),
        codeable_concept(
            coding_obj(F.col("c.source_coding_system"), F.col("c.source_code"),
                       F.col("c.source_display"), True)
        ).alias("vital_code"),
        F.coalesce(F.col("i.CANONICAL_NUMERIC_RESULT"), F.col("i.NUMERIC_RESPONSE_NBR"))
         .cast("decimal(38,10)").alias("value_number"),
        F.coalesce(F.col("i.CANONICAL_TEXT_RESULT"), F.col("i.RESPONSE_TEXT_TXT"))
         .alias("value_text"),
        F.lit(None).cast("string").alias("unit_source_value"),
        F.col("i.UNIT_CONCEPT_ID").cast("string").alias("unit_concept_id"),
        F.lit(None).cast("decimal(38,10)").alias("reference_range_low"),
        F.lit(None).cast("decimal(38,10)").alias("reference_range_high"),
        F.lit(None).cast("string").alias("method_code"), F.lit(None).cast("string").alias("method_display"),
        F.lit(None).cast("string").alias("body_site_code"), F.lit(None).cast("string").alias("body_site_display"),
        F.lit(None).cast("string").alias("interpretation_code"),
        F.lit(None).cast("string").alias("interpretation_display"),
        F.col("i.FORM_STATUS_CD").cast("string").alias("result_status_code"),
        F.col("i.STATUS").alias("result_status_display"),
        F.col("i.PERFORMED_PRSNL_ID_LONG").cast("bigint").alias("performer_practitioner_id"),
        stable_id("form:mill:powerform", F.col("i.DCP_FORMS_ACTIVITY_ID")).alias("source_form_key"),
        F.col("c.promotion_rule_id"),
        F.when(deleted | absent, F.lit("retracted")).when(inactive, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        F.col("i.FIRST_DOCUMENTED_DT_TM").alias("record_status_effective_from"),
        F.when(deleted | absent | inactive, F.col("i.LAST_DOCUMENTED_DT_TM"))
         .alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"), F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("form_promotion").alias("source_feed"),
        F.date_format(F.col("i.ADC_UPDT"), "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("i.ADC_UPDT").alias("source_update_timestamp"),
        F.col("i.ADC_UPDT").alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_FORM_ITEM).alias("_source_table"),
        F.col("i.DOC_RESPONSE_KEY").alias("_source_row_id"),
        F.col("i.QUESTION_CONCEPT_ID").cast("string").alias("_omop_code"),
        F.col("c.source_display").alias("_omop_display"),
    )


@materialized_view(
    name=_n("journey_clinical._promoted_vital_sign_primitive"),
    refresh_policy="auto",
)
def _promoted_vital_sign_primitive():
    # Build the declared dataset: Internal incrementally maintained governed form-promotion
    # vital route.
    return _promoted_vital_canonical().select(*PROMOTED_VITAL_STAGE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_promoted_vital_sign_primitive': {'columns': {},
                                             'table': 'Internal incrementally maintained governed '
                                                      'form-promotion vital route.'}}
