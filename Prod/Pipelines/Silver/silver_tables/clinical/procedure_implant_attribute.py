"""Silver clinical.procedure_implant_attribute: 1 dataset(s).

- datasets: clinical_procedure_implant_attribute
- moved verbatim from: 2_silver_clinical_events:1340, 2_silver_clinical_events:1365

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


# Clinical / procedure implant attribute

# ==== journey_clinical.procedure_implant_attribute ====
PROCEDURE_IMPLANT_ATTRIBUTE_PUBLIC_COLUMNS = [
    "patient_event_key", "implant_event_id", "implant_sequence", "sequence",
    "clinical_event_id", "attribute_name", "attribute_value", "event_tag",
    "event_title", "result_value", "value_source", "value_conflict_ind",
    "performed_datetime", "valid_from", "loaded_at",
]


@materialized_view(
    name=_n("journey_clinical.procedure_implant_attribute"),
    refresh_policy="auto",
)
def procedure_implant_attribute():
    # Build the declared dataset: One source implant attribute per parent implant
    # clinical_procedure row.
    e = read_source(SRC_IMPLANT_EVENTS)
    implants = _implant_procedure_canonical().select(
        "patient_event_key", "implant_event_id", "implant_sequence",
        "_base_clinical_event_id", "_implant_form_event_id",
    )
    joined = e.join(
        implants,
        (e.CLINICAL_EVENT_ID == implants._base_clinical_event_id)
        | (e.PARENT_EVENT_ID == implants._implant_form_event_id),
        "inner",
    )
    return joined.select(
        implants.patient_event_key.alias("patient_event_key"),
        implants.implant_event_id.alias("implant_event_id"),
        implants.implant_sequence.alias("implant_sequence"),
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
        e.ADC_UPDT.alias("loaded_at"),
    ).select(*PROCEDURE_IMPLANT_ATTRIBUTE_PUBLIC_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_procedure_implant_attribute': {'columns': {'attribute_name': 'Source implant attribute name.',
                                                      'attribute_value': 'Source implant attribute value.',
                                                      'clinical_event_id': 'Source clinical-event identifier '
                                                                           'for the implant attribute.',
                                                      'event_tag': 'Source clinical-event tag.',
                                                      'event_title': 'Source clinical-event title.',
                                                      'implant_event_id': 'Millennium implant EVENT_ID; '
                                                                          'first native parent-key '
                                                                          'component.',
                                                      'implant_sequence': 'Millennium IMPLANT_SEQUENCE; '
                                                                          'second native parent-key '
                                                                          'component.',
                                                      'loaded_at': 'Bronze load timestamp of the implant '
                                                                   'attribute row.',
                                                      'patient_event_key': 'Deterministic key of the parent '
                                                                           'implant clinical_procedure row.',
                                                      'performed_datetime': 'Source performed timestamp.',
                                                      'result_value': 'Source result value.',
                                                      'sequence': 'Deterministic source ordering value '
                                                                  'derived from performed or event-start '
                                                                  'time.',
                                                      'valid_from': 'Source validity-start timestamp.',
                                                      'value_conflict_ind': 'Whether source evidence for the '
                                                                            'value conflicts.',
                                                      'value_source': 'Source route that supplied the '
                                                                      'retained value.'},
                                          'table': 'One source implant attribute per parent implant '
                                                   'clinical_procedure row.'}}
