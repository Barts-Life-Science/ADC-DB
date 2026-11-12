"""Silver documents.document_sensitivity_label: 1 dataset(s).

- datasets: text_document_sensitivity_label
- moved verbatim from: 5_silver_documents:875, 5_silver_documents:886

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


# Text / document sensitivity label

# ==== journey_text.document_sensitivity_label ====
TEXT_DOCUMENT_SENSITIVITY_LABEL_PUBLIC_COLUMNS = [
    "patient_event_key", "sequence", "sensitivity_label", "loaded_at",
]


@materialized_view(
    name=_n("journey_text.document_sensitivity_label"),
    refresh_policy="auto",
)
def text_document_sensitivity_label():
    # Build the declared dataset: One ordered sensitivity label per clinical text document.
    source = spark.read.table(
        _n("journey_text._document_threaded")
    ).select("patient_event_key", "_sensitivity_labels_json", "loaded_at")
    return (
        source.select(
            "patient_event_key", "loaded_at",
            F.posexplode_outer(
                F.from_json(F.col("_sensitivity_labels_json"), "array<string>")
            ).alias("_position", "sensitivity_label"),
        )
        .where(F.col("sensitivity_label").isNotNull())
        .select(
            "patient_event_key",
            (F.col("_position") + 1).cast("long").alias("sequence"),
            F.col("sensitivity_label").cast("string").alias("sensitivity_label"),
            "loaded_at",
        )
        .select(*TEXT_DOCUMENT_SENSITIVITY_LABEL_PUBLIC_COLUMNS)
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'text_document_sensitivity_label': {'columns': {'loaded_at': 'Bronze load timestamp inherited from the '
                                                              'parent document row.',
                                                 'patient_event_key': 'Deterministic key of the parent '
                                                                      'text_document row.',
                                                 'sensitivity_label': 'Verbatim source sensitivity or '
                                                                      'security label.',
                                                 'sequence': 'One-based order of the source sensitivity '
                                                             'label.'},
                                     'table': 'One ordered sensitivity label per clinical text document.'}}
