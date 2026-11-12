"""Silver clinical.medication_order_attribute: 1 dataset(s).

- datasets: clinical_medication_order_attribute
- moved verbatim from: 2_silver_clinical_events:8370, 2_silver_clinical_events:8384, 2_silver_clinical_events:8385, 2_silver_clinical_events:8387, 2_silver_clinical_events:8402

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, spark


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# S3 mapping domain stages (S3_DOMAIN_STAGES_V1)

# ==== journey_clinical.medication_order_attribute ==== PMS_P1_T6_SILVER_V1
# (field_meaning, output prefix, kind): text -> _display; number -> _value (+ unit column when paired); datetime -> _datetime
ORDER_ATTRIBUTES = [
    ("WEIGHT", "weight", "number"), ("WEIGHTUNIT", "weight_unit", "text"),
    ("INDICATION", "indication", "text"), ("DCREASON", "dcreason", "text"), ("STOPTYPE", "stoptype", "text"),
    ("DISPENSECATEGORY", "dispensecategory", "text"), ("PRNREASON", "prnreason", "text"), ("PRNINSTRUCTIONS", "prninstructions", "text"),
    ("FREETXTDOSE", "freetxtdose", "text"),
    ("STRENGTHDOSE", "strengthdose", "number"), ("STRENGTHDOSEUNIT", "strengthdose_unit", "text"),
    ("VOLUMEDOSE", "volumedose", "number"), ("VOLUMEDOSEUNIT", "volumedose_unit", "text"),
    ("RXROUTE", "rxroute", "text"), ("DRUGFORM", "drugform", "text"),
    ("FREQSCHEDID", "freqschedid", "text"), ("FREQ", "freq", "text"),
    ("DURATION", "duration", "number"), ("DURATIONUNIT", "duration_unit", "text"), ("DAYSSUPPLY", "dayssupply", "number"),
    ("RATE", "rate", "number"), ("RATEUNIT", "rate_unit", "text"),
    ("INFUSEOVER", "infuseover", "number"), ("INFUSEOVERUNIT", "infuseover_unit", "text"),
    ("REQSTARTDTTM", "reqstart", "datetime"), ("STOPDTTM", "stop", "datetime"),
]


_SUFFIX = {"text": "_display", "number": "_value", "datetime": "_datetime"}


_VALUE = {"text": "value_text", "number": "value_number", "datetime": "value_datetime"}


def order_attribute_body(d):
    """Shared body: EAV medication_order_detail -> one row per order_id, latest action wins per meaning."""
    d = d.where(F.col("field_meaning").isin([m for m, _, _ in ORDER_ATTRIBUTES]))
    aggs = [F.expr(f"max_by({_VALUE[k]}, struct(action_sequence, detail_sequence)) FILTER (WHERE field_meaning = '{m}')").alias(p + _SUFFIX[k])
            for m, p, k in ORDER_ATTRIBUTES]
    return d.groupBy(F.col("order_id").cast("bigint").alias("order_id")).agg(
        *aggs, F.max("last_action_sequence").alias("last_action_sequence"), F.max("loaded_at").alias("loaded_at"))


@materialized_view(
    name=_n("journey_clinical.medication_order_attribute"),
    cluster_by=["order_id"], refresh_policy="auto",
)
def medication_order_attribute():
    # Build the declared dataset: one row per medication order with its latest detail attributes.
    return order_attribute_body(spark.read.table(_n("journey_clinical.medication_order_detail")))


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_medication_order_attribute': {'columns': {'dayssupply_value': 'Latest DAYSSUPPLY order detail '
                                                                         '(value_number) by action then '
                                                                         'detail sequence.',
                                                     'dcreason_display': 'Latest DCREASON order detail '
                                                                         '(value_text) by action then detail '
                                                                         'sequence.',
                                                     'dispensecategory_display': 'Latest DISPENSECATEGORY '
                                                                                 'order detail (value_text) '
                                                                                 'by action then detail '
                                                                                 'sequence.',
                                                     'drugform_display': 'Latest DRUGFORM order detail '
                                                                         '(value_text) by action then detail '
                                                                         'sequence.',
                                                     'duration_unit_display': 'Latest DURATIONUNIT order '
                                                                              'detail (value_text) by action '
                                                                              'then detail sequence.',
                                                     'duration_value': 'Latest DURATION order detail '
                                                                       '(value_number) by action then detail '
                                                                       'sequence.',
                                                     'freetxtdose_display': 'Latest FREETXTDOSE order detail '
                                                                            '(value_text) by action then '
                                                                            'detail sequence.',
                                                     'freq_display': 'Latest FREQ order detail (value_text) '
                                                                     'by action then detail sequence.',
                                                     'freqschedid_display': 'Latest FREQSCHEDID order detail '
                                                                            '(value_text) by action then '
                                                                            'detail sequence.',
                                                     'indication_display': 'Latest INDICATION order detail '
                                                                           '(value_text) by action then '
                                                                           'detail sequence.',
                                                     'infuseover_unit_display': 'Latest INFUSEOVERUNIT order '
                                                                                'detail (value_text) by '
                                                                                'action then detail '
                                                                                'sequence.',
                                                     'infuseover_value': 'Latest INFUSEOVER order detail '
                                                                         '(value_number) by action then '
                                                                         'detail sequence.',
                                                     'last_action_sequence': 'Highest order action sequence '
                                                                             'seen in the detail rows.',
                                                     'loaded_at': 'Latest detail loaded_at for the order.',
                                                     'order_id': 'Millennium ORDER_ID; one row per '
                                                                 'medication order.',
                                                     'prninstructions_display': 'Latest PRNINSTRUCTIONS '
                                                                                'order detail (value_text) '
                                                                                'by action then detail '
                                                                                'sequence.',
                                                     'prnreason_display': 'Latest PRNREASON order detail '
                                                                          '(value_text) by action then '
                                                                          'detail sequence.',
                                                     'rate_unit_display': 'Latest RATEUNIT order detail '
                                                                          '(value_text) by action then '
                                                                          'detail sequence.',
                                                     'rate_value': 'Latest RATE order detail (value_number) '
                                                                   'by action then detail sequence.',
                                                     'reqstart_datetime': 'Latest REQSTARTDTTM order detail '
                                                                          '(value_datetime) by action then '
                                                                          'detail sequence.',
                                                     'rxroute_display': 'Latest RXROUTE order detail '
                                                                        '(value_text) by action then detail '
                                                                        'sequence.',
                                                     'stop_datetime': 'Latest STOPDTTM order detail '
                                                                      '(value_datetime) by action then '
                                                                      'detail sequence.',
                                                     'stoptype_display': 'Latest STOPTYPE order detail '
                                                                         '(value_text) by action then detail '
                                                                         'sequence.',
                                                     'strengthdose_unit_display': 'Latest STRENGTHDOSEUNIT '
                                                                                  'order detail (value_text) '
                                                                                  'by action then detail '
                                                                                  'sequence.',
                                                     'strengthdose_value': 'Latest STRENGTHDOSE order detail '
                                                                           '(value_number) by action then '
                                                                           'detail sequence.',
                                                     'volumedose_unit_display': 'Latest VOLUMEDOSEUNIT order '
                                                                                'detail (value_text) by '
                                                                                'action then detail '
                                                                                'sequence.',
                                                     'volumedose_value': 'Latest VOLUMEDOSE order detail '
                                                                         '(value_number) by action then '
                                                                         'detail sequence.',
                                                     'weight_unit_display': 'Latest WEIGHTUNIT order detail '
                                                                            '(value_text) by action then '
                                                                            'detail sequence.',
                                                     'weight_value': 'Latest WEIGHT order detail '
                                                                     '(value_number) by action then detail '
                                                                     'sequence.'},
                                         'table': 'Order-grain pivot of medication_order_detail: latest '
                                                  'value per field meaning. Join key: order_id.'}}

# INSR_R4_SILVER_V1 (child): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import ParentInputs, with_parent_inputs  # noqa: E402
R4_PARENT_INPUTS = ParentInputs(parent='clinical_medication_order', dataset='_clinical_medication_order_parent_inputs', keys=('order_id',), inputs=('identity_status', 'person_id', 'record_status'), admit_sql="coalesce(((identity_status = 'resolved') AND (record_status = 'active')), false)", name=_n('journey_clinical._medication_order_parent_inputs'), parent_name=_n('journey_clinical.medication_order'))
SPECS = with_parent_inputs(SPECS, 'clinical_medication_order_attribute', R4_PARENT_INPUTS)
