"""Silver clinical.numeric_clinical_score: 1 dataset(s).

- datasets: _clinical_numeric_clinical_score
- moved verbatim from: 2_silver_clinical_events:4610, silver_journey_shared.py:4607

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import (
    _n,
    _present,
    codeable_concept,
    coding_obj,
    read_source,
    stable_id,
    subject_key_with_system,
)
from silver_tables._common.journey import SCORE_STAGE_COLUMNS, SRC_NUMERIC_EVENTS
from silver_tables.clinical._shared import _numeric_route_expr


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


def _numeric_score_canonical():
    # Assemble normalized numeric score rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_NUMERIC_EVENTS)
    s = s.where(_numeric_route_expr(s) == "clinical_score")
    event_id = stable_id("numeric_event:mill", s.EVENT_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_NUMERIC_EVENTS, s.EVENT_ID
    )
    deleted = F.coalesce(s.SOURCE_DELETED_IND, F.lit(False))
    ended = s.CLINICAL_EVENT_VALID_UNTIL_DT_TM.isNotNull() & (
        s.CLINICAL_EVENT_VALID_UNTIL_DT_TM < F.lit("2100-01-01").cast("timestamp")
    )
    component = F.struct(
        F.lit(1).cast("long").alias("sequence"), s.EVENT_ID.cast("string").alias("component_id"),
        F.lit("urn:cerner:event_cd").alias("coding_system"),
        s.EVENT_CD.cast("string").alias("coding_code"),
        F.coalesce(s.EVENT_LABEL, s.EVENT_CD_DISPLAY).alias("coding_display"),
        s.NUMERIC_RESULT.cast("decimal(38,10)").alias("value_number"),
        s.RESULT_TEXT_EFFECTIVE.alias("value_text"),
        F.coalesce(s.UNIT_OF_MEASURE_DISPLAY, s.RESULT_UNITS_DISPLAY).alias("unit"),
    )
    source_update = F.greatest(
        s.STRING_RESULT_UPDT_DT_TM, s.CLINICAL_EVENT_UPDT_DT_TM, s.ADC_UPDT
    )
    # contract v2: publish native numeric-event identifiers and native foreign keys for the score numeric arm
    return s.select(
        event_id.alias("patient_event_key"),
        s.EVENT_ID.cast("bigint").alias("event_id"),
        F.lit(None).cast("string").alias("doc_response_key"),
        F.lit(None).cast("bigint").alias("source_dcp_forms_activity_id"),
        F.lit("numeric_event").alias("source_object"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(_present(s.PERSON_ID), F.lit("resolved")).otherwise(F.lit("unresolved"))
         .alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        F.coalesce(s.PERFORMED_DT_TM, s.EVENT_START_DT_TM).alias("event_datetime"),
        s.EVENT_END_DT_TM.alias("event_end_datetime"),
        F.coalesce(s.PERFORMED_DT_TM_LOCAL, s.EVENT_START_DT_TM_LOCAL).alias("event_datetime_local"),
        s.EVENT_END_DT_TM_LOCAL.alias("event_end_datetime_local"),
        F.lit("urn:cerner:event_cd").alias("source_coding_system"),
        s.EVENT_CD.cast("string").alias("source_code"),
        F.coalesce(s.EVENT_LABEL, s.EVENT_CD_DISPLAY).alias("source_display"),
        codeable_concept(
            coding_obj(F.lit("urn:cerner:event_cd"), s.EVENT_CD,
                       F.coalesce(s.EVENT_LABEL, s.EVENT_CD_DISPLAY), True),
            coding_obj(F.lit("urn:omop:concept_id"), s.OMOP_MANUAL_CONCEPT_ID,
                       s.OMOP_MANUAL_CONCEPT_NAME, False, "bronze.map_numeric_events", None),
        ).alias("score_code"), s["SCORE_MAPPING"].alias("_r1_score_mapping"), s["SCORE_MAP_KEY"].alias("_r1_score_map_key"), s["SCORE_MAP_COMPONENTS_JSON"].alias("_r1_score_components_json"),
        F.coalesce(s.OMOP_MANUAL_CONCEPT_NAME, s.EVENT_LABEL, s.EVENT_CD_DISPLAY).alias("score_name"),
        s.NUMERIC_RESULT.cast("decimal(38,10)").alias("value_number"),
        s.RESULT_TEXT_EFFECTIVE.alias("value_text"),
        F.coalesce(s.UNIT_OF_MEASURE_DISPLAY, s.RESULT_UNITS_DISPLAY).alias("unit_source_value"),
        s.OMOP_MANUAL_UNITS.cast("string").alias("unit_concept_id"),
        F.to_json(F.array(component)).alias("_components_json"),
        F.lit(1).cast("long").alias("component_count"),
        s.NORMALCY_CD.cast("string").alias("interpretation_code"),
        s.NORMALCY_DISPLAY.alias("interpretation_display"),
        s.RESULT_STATUS_CD.cast("string").alias("result_status_code"),
        s.RESULT_STATUS_DISPLAY.alias("result_status_display"),
        s.PERFORMED_PRSNL_ID.cast("bigint").alias("performer_practitioner_id"),
        F.lit(None).cast("string").alias("source_form_key"),
        F.lit(None).cast("string").alias("promotion_rule_id"),
        F.when(deleted, F.lit("retracted")).when(ended, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        s.CLINICAL_EVENT_VALID_FROM_DT_TM.alias("record_status_effective_from"),
        F.when(deleted | ended, s.CLINICAL_EVENT_VALID_UNTIL_DT_TM).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"), F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.lit("numeric_event").alias("source_feed"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        source_update.alias("source_update_timestamp"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_NUMERIC_EVENTS).alias("_source_table"),
        s.EVENT_ID.cast("string").alias("_source_row_id"),
        s.OMOP_MANUAL_CONCEPT_ID.cast("string").alias("_omop_code"),
        s.OMOP_MANUAL_CONCEPT_NAME.alias("_omop_display"),
    )


# Clinical /  numeric clinical score
@materialized_view(
    name=_n("journey_clinical._numeric_clinical_score"),
    private=True,
    refresh_policy="auto",
)
def _numeric_clinical_score():
    # Build the declared dataset: Internal incrementally maintained native numeric-event
    # clinical-score route.
    return _numeric_score_canonical().select(*SCORE_STAGE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_numeric_clinical_score': {'columns': {},
                                      'table': 'Internal incrementally maintained native numeric-event '
                                               'clinical-score route.'}}
