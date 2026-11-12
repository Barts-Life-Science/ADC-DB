"""Silver activity.rtt_pathway_encounter_type: 1 dataset(s).

- datasets: clinical_rtt_pathway_encounter_type
- moved verbatim from: 4_silver_care_pathways:1728, 4_silver_care_pathways:1743

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


# Clinical / rtt pathway encounter type

# ==== journey_clinical.rtt_pathway_encounter_type ====
RTT_PATHWAY_ENCOUNTER_TYPE_PUBLIC_COLUMNS = [
    "patient_event_key", "source_system_oid", "pathway_oid", "period_oid",
    "sequence", "encounter_type", "loaded_at",
]


@materialized_view(
    name=_n("journey_clinical.rtt_pathway_encounter_type"),
    refresh_policy="auto",
)
def rtt_pathway_encounter_type():
    # Build the declared dataset: One ordered encounter-type tag per LUNA RTT pathway period.
    source = spark.read.table(
        _n("journey_clinical._rtt_pathway_primitive")
    ).select(
        "patient_event_key", "source_system_oid", "pathway_oid", "period_oid",
        "_encounter_types_json", "loaded_at",
    )
    return (
        source.select(
            "patient_event_key", "source_system_oid", "pathway_oid", "period_oid",
            "loaded_at",
            F.posexplode_outer(
                F.from_json(F.col("_encounter_types_json"), "array<string>")
            ).alias("_position", "encounter_type"),
        )
        .where(F.col("encounter_type").isNotNull())
        .select(
            "patient_event_key", "source_system_oid", "pathway_oid", "period_oid",
            (F.col("_position") + 1).cast("long").alias("sequence"),
            F.col("encounter_type").cast("string").alias("encounter_type"),
            "loaded_at",
        )
        .select(*RTT_PATHWAY_ENCOUNTER_TYPE_PUBLIC_COLUMNS)
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_rtt_pathway_encounter_type': {'columns': {'encounter_type': 'Verbatim LUNA pathway encounter-type '
                                                                       'tag.',
                                                     'loaded_at': 'Bronze load timestamp inherited from the '
                                                                  'parent pathway row.',
                                                     'pathway_oid': 'LUNA PATHWAY_OID; second native '
                                                                    'pathway-period key component.',
                                                     'patient_event_key': 'Deterministic key of the parent '
                                                                          'clinical_rtt_pathway row.',
                                                     'period_oid': 'LUNA PERIOD_OID; third native '
                                                                   'pathway-period key component.',
                                                     'sequence': 'One-based order of the encounter-type tag '
                                                                 'in the source array.',
                                                     'source_system_oid': 'LUNA SOURCE_SYSTEM_OID; first '
                                                                          'native pathway-period key '
                                                                          'component.'},
                                         'table': 'One ordered encounter-type tag per LUNA RTT pathway '
                                                  'period.'}}
