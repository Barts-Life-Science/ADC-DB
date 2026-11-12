"""Silver documents.document_threaded: 1 dataset(s).

- datasets: _text_document_threaded
- moved verbatim from: 5_silver_documents:648

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, spark
from silver_tables._common.journey import DOCUMENT_THREADED_COLUMNS


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Text /  document threaded
@materialized_view(
    name=_n("journey_text._document_threaded"),
    private=True,
)
# contract v2: use renamed document and thread keys for deterministic latest-version selection
def _document_threaded():
    # Build the declared dataset: Internal document versions with the latest version selected
    # deterministically in-graph.
    versions = spark.read.table(_n("journey_text._document_primitive"))
    effective_from = F.coalesce(
        F.col("record_status_effective_from"),
        F.col("event_datetime"),
        F.col("source_update_timestamp"),
        F.col("loaded_at"),
    )
    head_key = F.struct(
        F.when(
            F.col("document_text").isNotNull()
            & (F.trim(F.col("document_text")) != ""),
            F.lit(1),
        ).otherwise(F.lit(0)).alias("has_data"),
        F.coalesce(effective_from.cast("double"), F.lit(-1.0e308))
        .alias("effective_from_epoch"),
        F.coalesce(F.col("version_ordinal"), F.lit(-1).cast("long"))
        .alias("version_ordinal"),
        F.coalesce(F.col("version_id"), F.lit("")).alias("version_id"),
    )
    ranked = versions.select(
        "patient_event_key",
        "document_thread_key",
        head_key.alias("head_key"),
    ).alias("r")
    heads = ranked.groupBy("document_thread_key").agg(
        F.max("head_key").alias("selected_head_key")
    ).alias("h")
    selected = ranked.join(
        heads,
        (F.col("r.document_thread_key") == F.col("h.document_thread_key"))
        & (F.col("r.head_key") == F.col("h.selected_head_key")),
        "inner",
    ).select(
        F.col("r.document_thread_key").alias("_head_thread_key"),
        F.col("r.patient_event_key").alias("_latest_patient_event_key"),
    )
    return (
        versions.join(
            selected,
            versions["document_thread_key"] == selected["_head_thread_key"],
            "left",
        )
        .withColumn(
            "is_latest_version",
            F.coalesce(
                F.col("patient_event_key") == F.col("_latest_patient_event_key"),
                F.lit(False),
            ),
        )
        .drop("_head_thread_key", "_latest_patient_event_key")
        .select(*DOCUMENT_THREADED_COLUMNS)
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_text_document_threaded': {'columns': {},
                             'table': 'Internal document versions with the latest version selected '
                                      'deterministically in-graph.'}}
