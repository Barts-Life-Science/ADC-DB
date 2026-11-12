"""Silver spine.pathology_parent_link: 1 dataset(s).

- datasets: _events_pathology_parent_link
- moved verbatim from: 2_silver_clinical_events:1622, 2_silver_clinical_events:1652

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, spark
from silver_tables._common.journey import SRC_PATHOLOGY_REPORT_VERSIONS, _alias_resolved_accession
from silver_tables.spine._shared import SRC_PATHOLOGY_ACCESSION_SOURCE


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


def _pathology_parent_link_canonical():
    # Assemble normalized pathology parent link rows for downstream dataset builders, preserving
    # the existing source and identity rules.
    src = _alias_resolved_accession(read_source(SRC_PATHOLOGY_ACCESSION_SOURCE))
    ver = read_source(SRC_PATHOLOGY_REPORT_VERSIONS).select(
        F.col("source_record_key").alias("source_parent_key"),
        F.col("report_series_id"),
    )
    # PATH_ENCOUNTER_V1: the resolved accession's encounter evidence rides on the link.
    acc = spark.read.table(_n("journey_events._pathology_accession_identity")).select(
        "pathology_accession_id", "accession_encounter_id", "accession_encounter_conflict",
    )
    return (
        src.select(
            "source_parent_key", "source_system",
            F.col("_resolved_accession_id").alias("pathology_accession_id"),
            F.col("encounter_id").alias("source_encounter_id"),
            F.col("ADC_UPDT").alias("loaded_at"),
        )
        .join(ver, "source_parent_key", "left")
        .join(acc, "pathology_accession_id", "left")
    )


# Events /  pathology parent link
@materialized_view(
    name=_n("journey_events._pathology_parent_link"),
    refresh_policy="auto",
)
def _pathology_parent_link():
    # Build the declared dataset: INTERNAL source-parent linkage: source_parent_key -> alias-
    # resolved accession, report series (1:1 parent->version->series, preflight-verified), and
    # parent encounter.
    return _pathology_parent_link_canonical()


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_events_pathology_parent_link': {'columns': {},
                                   'table': 'INTERNAL source-parent linkage: source_parent_key -> '
                                            'alias-resolved accession, report series (1:1 '
                                            'parent->version->series, preflight-verified), and parent '
                                            'encounter.'}}
