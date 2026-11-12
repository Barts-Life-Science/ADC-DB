"""Silver activity.cost_line_item: 1 dataset(s).

- datasets: reference_cost_line_item
- moved verbatim from: 4_silver_care_pathways:3355, 4_silver_care_pathways:3376, 4_silver_care_pathways:3379, 4_silver_care_pathways:3384, 4_silver_care_pathways:3409

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Reference / cost line item
SRC_SLAM_COST_LINE_ITEM = "4_prod.bronze.map_slam_cost_line_item"


COST_LINE_ITEM_LIFECYCLE_FIELDS = [
]


COST_LINE_ITEM_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_cost_line_item():
    # Assemble cost line item rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    s = read_source(SRC_SLAM_COST_LINE_ITEM)
    # contract v2: rename deterministic row and parent identities as keys and align the native activity component with its parent type
    return s.select(
        stable_id("cost_line:slam", s.EXTRACT_CD, s.ACTIVITY_RECORD_ID, s.LINE_HASH)
         .alias("cost_line_item_key"),
        stable_id("costed_activity:slam", s.EXTRACT_CD, s.ACTIVITY_RECORD_ID)
         .alias("costed_activity_key"),
        s.EXTRACT_CD.alias("extract_cd"),
        s.ACTIVITY_RECORD_ID.cast("bigint").alias("activity_record_id"),
        s.LINE_HASH.alias("line_hash"), s.ACTIVITY_COST_ITEM_CD.alias("activity_cost_item_cd"),
        s.RESOURCE_COST_ITEM_CD.alias("resource_cost_item_cd"),
        s.ACTIVITY_COUNT.alias("activity_count"), s.UNBUNDLED_SUBTYPE_CD.alias("unbundled_subtype_cd"),
        s.UNBUNDLED_CURRENCY_CD.alias("unbundled_currency_cd"),
        s.UNBUNDLED_CURRENCY_DT_TM.alias("unbundled_currency_datetime"),
        s.TOTAL_COST.cast("decimal(38,6)").alias("total_cost"),
        s.TOTAL_O_COST.cast("decimal(38,6)").alias("total_o_cost"),
        s.SOURCE_DUPLICATE_COUNT.alias("source_duplicate_count"),
        F.when(~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True)), F.lit("retracted"))
         .otherwise(F.lit("active")).alias("record_status"),
        s.ADC_UPDT.alias("loaded_at"),
    )


@materialized_view(
    name=_n("journey_reference.cost_line_item"),
    refresh_policy="auto",
)
def cost_line_item():
    # Build the declared dataset: PLICS cost lines (frozen FY19/20-FY21/22 extracts); event
    # plane excluded by design.
    return _lifecycle_source_cost_line_item().drop(*COST_LINE_ITEM_LIFECYCLE_FIELDS, *COST_LINE_ITEM_RETIRED_COLUMNS)


VALUE_POLICIES = {
    'reference_cost_line_item': ValuePolicy(table='reference_cost_line_item', enabled=True),
}


# W2.11 (L5028): PLICS activity counts as decimals (the source mixes fractional counts into a text column).
# activity_count is published by the SLAM cost-line table (4_silver_care_pathways:3354-3400, public leaf
# reference_cost_line_item, checked in 4_prod.information_schema 2026-10-30); clinical_costed_activity has only
# cost_line_count.
VALUE_POLICIES["reference_cost_line_item"].register("activity_count_decimal", ["activity_count"],
         lambda F: F.expr("try_cast(activity_count AS DECIMAL(18,4))"),
         "activity_count as DECIMAL(18,4); NULL when the source text is not a number. INSR_W2 W2.11 (L5028).")


WRAP_POLICY = {'reference_cost_line_item'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_cost_line_item': {'columns': {'activity_cost_item_cd': 'National Cost Collection activity cost '
                                                                   'item code for the cost line, preserved '
                                                                   'as supplied by the source extract '
                                                                   'without mapping.',
                                          'activity_count': 'Source-reported activity count for the cost '
                                                            'line, retained as text as supplied by the '
                                                            'extract.',
                                          'activity_count_decimal': 'activity_count as DECIMAL(18,4); NULL '
                                                                    'when the source text is not a number. '
                                                                    'INSR_W2 W2.11 (L5028).',
                                          'activity_record_id': 'SLAM ACTIVITY_RECORD_ID as BIGINT; joins '
                                                                'with extract_cd to the parent native key.',
                                          'cost_line_item_key': 'Deterministic SHA-256 key for this SLAM '
                                                                'cost line.',
                                          'costed_activity_key': 'Deterministic SHA-256 key for the parent '
                                                                 'costed activity.',
                                          'extract_cd': 'Source costing extract discriminator; required in '
                                                        'every cost-line join.',
                                          'line_hash': 'Content-hash key derived from the source line '
                                                       'attributes, used to identify a distinct cost line '
                                                       'after collapsing exact duplicates.',
                                          'loaded_at': 'map_slam_cost_line_item.ADC_UPDT carried unchanged '
                                                       'for the cost-line row. The parent activity key is '
                                                       'derived without a parent join or added parent clock; '
                                                       'this is bronze load provenance, not the currency '
                                                       'date/time or Silver refresh time.',
                                          'record_status': 'Retracted when '
                                                           'map_slam_cost_line_item.SOURCE_PRESENT_IND is '
                                                           'false, otherwise active; a missing presence flag '
                                                           'defaults true. No superseded status is emitted, '
                                                           'and the value is a derived STRING label rather '
                                                           'than the original Boolean presence flag.',
                                          'resource_cost_item_cd': 'National Cost Collection resource cost '
                                                                   'item code for the cost line, preserved '
                                                                   'as supplied by the source extract '
                                                                   'without mapping.',
                                          'source_duplicate_count': 'Exact source rows represented by this '
                                                                    'collapsed line.',
                                          'total_cost': 'Total cost reported by the source extract for the '
                                                        'cost line, retained at exact decimal(36,18) '
                                                        'precision.',
                                          'total_o_cost': 'Component of the line total cost reported by the '
                                                          'source extract, retained at exact decimal(36,18) '
                                                          'precision.',
                                          'unbundled_currency_cd': 'Unbundled currency code recorded against '
                                                                   'the cost line by the source extract, '
                                                                   'null on ordinary bundled lines.',
                                          'unbundled_currency_datetime': 'Date and time recorded against the '
                                                                         'unbundled currency for the cost '
                                                                         'line, as reported by the source '
                                                                         'extract.',
                                          'unbundled_subtype_cd': 'Observed values: 1 high-cost drug, 2 '
                                                                  'device, 3 diagnostic/other, 0 default; '
                                                                  'null on ordinary lines.'},
                              'table': 'PLICS cost lines (frozen FY19/20-FY21/22 extracts); event plane '
                                       'excluded by design.'}}
