"""Silver forms.form_wildcard_ambiguity: 1 dataset(s).

- datasets: _clinical_form_wildcard_ambiguity
- moved verbatim from: 6_silver_forms:6787, 6_silver_forms:7094

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import spark
from silver_tables.forms._shared import FORM_CONFIGS, _element_matches, _n


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


def _wildcard_column_expr(slug_col, label_col):
    """Return the configured column only for section-wildcard matches."""
    expression = F.lit(None).cast("string")
    for cfg in FORM_CONFIGS:
        for element in cfg["elements"]:
            for match in _element_matches(element):
                if not match.get("section"):
                    condition = (
                        (slug_col == cfg["slug"])
                        & (F.upper(F.trim(label_col)) == match["label"].strip().upper())
                    )
                    expression = F.when(
                        condition, F.lit(element["column"])
                    ).otherwise(expression)
    return expression


@materialized_view(
    name=_n("journey_clinical._form_wildcard_ambiguity"),
    private=True,
    refresh_policy="auto",
)
def _form_wildcard_ambiguity():
    # Build the declared dataset: Internal acceptance evidence. Any row means a section-wildcard
    # form element matched more than one source section and blocks cutover.
    response = spark.read.table(
        _n("journey_clinical._form_response_primitive")
    )
    wildcard_column = _wildcard_column_expr(
        F.col("form_slug"),
        F.col("element_label"),
    )
    matched = (
        response.withColumn("configured_column", wildcard_column)
        .where(F.col("configured_column").isNotNull())
        .withColumn(
            "element_label_norm",
            F.upper(F.trim(F.col("element_label"))),
        )
        .withColumn(
            "section_norm",
            F.upper(F.trim(F.col("section"))),
        )
    )
    return (
        matched.groupBy(
            "form_slug",
            "configured_column",
            "element_label_norm",
        )
        .agg(
            F.sort_array(F.collect_set("section_norm")).alias(
                "matched_sections"
            ),
            F.countDistinct("section_norm").cast("long").alias(
                "distinct_section_count"
            ),
        )
        .where(F.col("distinct_section_count") > 1)
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_form_wildcard_ambiguity': {'columns': {'configured_column': 'Configured wildcard-section '
                                                                        'instrument column.',
                                                   'distinct_section_count': 'Number of distinct source '
                                                                             'sections matched by the '
                                                                             'wildcard label.',
                                                   'element_label_norm': 'Upper-trimmed source element label '
                                                                         'used by the wildcard match.',
                                                   'form_slug': 'Configured instrument slug; NULL when the '
                                                                'form is outside the 30 reviewed '
                                                                'instruments.',
                                                   'matched_sections': 'Distinct upper-trimmed source '
                                                                       'sections seen for the wildcard '
                                                                       'label.'},
                                       'table': 'Internal acceptance evidence. Any row means a '
                                                'section-wildcard form element matched more than one source '
                                                'section and blocks cutover.'}}
