"""Silver forms.form_response_selection: 1 dataset(s).

- datasets: _clinical_form_response_selection
- moved verbatim from: 6_silver_forms:6957

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from pyspark.sql.window import Window
import pyspark.sql.functions as F
from silver_core.journey_core import spark
from silver_tables.forms._shared import FORM_MULTI_COLUMNS, _n


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


@materialized_view(
    name=_n("journey_clinical._form_response_selection"),
    private=True,
    refresh_policy="auto",
)
def _form_response_selection():
    # Build the declared dataset: Internal production-equivalent selection rank per configured
    # response candidate.
    response = spark.read.table(
        _n("journey_clinical._form_response_primitive")
    ).where(F.col("configured_column").isNotNull())
    has_value = (
        F.col("response_value_number").isNotNull()
        | F.col("response_value_text").isNotNull()
        | F.col("response_value_datetime").isNotNull()
        | F.col("response_value_time").isNotNull()  # INSR_W2 W2.07
        | F.col("value_concept_id").isNotNull()
    )
    order = Window.partitionBy(
        "dcp_forms_activity_id",
        "configured_column",
    ).orderBy(
        F.col("active_ind").desc_nulls_last(),
        has_value.desc(),
        F.col("sequence").desc_nulls_last(),
        F.col("response_id").desc_nulls_last(),
    )
    is_multi = F.lit(False)
    for slug, column in sorted(FORM_MULTI_COLUMNS):
        is_multi = is_multi | (
            (F.col("form_slug") == slug)
            & (F.col("configured_column") == column)
        )
    return (
        response.withColumn("selection_rank", F.row_number().over(order))
        .withColumn(
            "is_selected",
            F.when(
                is_multi,
                F.coalesce(F.col("active_ind"), F.lit(False)),
            ).otherwise(F.col("selection_rank") == 1),
        )
        .select(
            "dcp_forms_activity_id",
            "response_id",
            "configured_column",
            "selection_rank",
            "is_selected",
        )
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_form_response_selection': {'columns': {'configured_column': 'Instrument column this response can '
                                                                        'populate; NULL means residual or '
                                                                        'unconfigured.',
                                                   'dcp_forms_activity_id': 'Millennium '
                                                                            'DCP_FORMS_ACTIVITY_ID; native '
                                                                            'parent key joining '
                                                                            'clinical_form.',
                                                   'is_selected': 'True for rank 1 of a single-valued '
                                                                  'element or every active response of a '
                                                                  'multi-valued element; false for '
                                                                  'residuals.',
                                                   'response_id': 'Millennium DOC_RESPONSE_KEY; native '
                                                                  'identifier of the response row.',
                                                   'selection_rank': 'Production ranking within form '
                                                                     'instance and configured column: '
                                                                     'active, valued, latest sequence, then '
                                                                     'latest response ID.'},
                                       'table': 'Internal production-equivalent selection rank per '
                                                'configured response candidate.'}}
