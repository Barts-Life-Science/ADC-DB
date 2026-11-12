"""Silver clinical.implant_attribute: 1 dataset(s).

- datasets: _clinical_implant_attribute_grouped
- moved verbatim from: 2_silver_clinical_events:635, 2_silver_clinical_events:679

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source
from silver_tables._common.journey import _implant_procedure_canonical
from silver_tables.clinical._shared import SRC_IMPLANT_EVENTS


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


def _implant_attribute_grouped_query():
    # Link implant events to their procedure or parent form, then group the implant attribute
    # evidence.
    e = read_source(SRC_IMPLANT_EVENTS)
    # contract v2: key implant attribute aggregation and joins by patient_event_key
    # INSR_W2 W2.08 (L3235): a parent-form attribute belongs to the implant with the same sequence on that form,
    # not to every implant on the form (the OR join fanned out to ~50 rows per implant). Two equi-joins, unioned.
    implants = _implant_procedure_canonical().select(
        "patient_event_key", "_base_clinical_event_id", "_implant_form_event_id", "implant_sequence"
    )
    direct = e.join(implants, e.CLINICAL_EVENT_ID == implants._base_clinical_event_id, "inner")
    by_form = e.join(implants, (e.PARENT_EVENT_ID == implants._implant_form_event_id)
                     & (F.expr("try_cast(COLLATING_SEQ AS BIGINT)") == implants.implant_sequence), "inner")
    joined = direct.unionByName(by_form).dropDuplicates(["patient_event_key", "CLINICAL_EVENT_ID"])
    e = joined  # the attribute struct and aggregate below read the joined rows
    attribute = F.struct(
        F.coalesce(e.PERFORMED_DT_TM, e.EVENT_START_DT_TM).cast("long").alias("sequence"),
        e.CLINICAL_EVENT_ID.cast("string").alias("clinical_event_id"),
        e.ATTRIBUTE_NAME.alias("attribute_name"),
        e.ATTRIBUTE_VALUE.alias("attribute_value"),
        e.EVENT_TAG.alias("event_tag"),
        e.EVENT_TITLE_TEXT.alias("event_title"),
        e.RESULT_VAL.alias("result_value"),
        e.VALUE_SOURCE.alias("value_source"),
        e.VALUE_CONFLICT_IND.alias("value_conflict_ind"),
        e.PERFORMED_DT_TM.alias("performed_datetime"),
        e.VALID_FROM_DT_TM.alias("valid_from"),
    )
    return (
        joined.groupBy("patient_event_key")
        .agg(
            F.to_json(F.sort_array(F.collect_list(attribute)))
            .alias("_implant_attribute_json"),
            F.count(F.lit(1)).cast("long").alias("_implant_attribute_count"),
            F.max(e.ADC_UPDT).alias("_evidence_loaded_at"),
        )
        .select(
            F.col("patient_event_key").alias("_implant_attribute_event_key"),
            F.col("_implant_attribute_json"),
            F.col("_implant_attribute_count"),
            F.col("_evidence_loaded_at"),
        )
    )


@materialized_view(
    name=_n("journey_clinical._implant_attribute_grouped"),
    private=True,
    refresh_policy="auto",
)
def _implant_attribute_grouped():
    # Build the declared dataset: Internal ordered implant-attribute aggregate keyed by implant
    # procedure event.
    return _implant_attribute_grouped_query()


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_implant_attribute_grouped': {'columns': {},
                                         'table': 'Internal ordered implant-attribute aggregate keyed by '
                                                  'implant procedure event.'}}
