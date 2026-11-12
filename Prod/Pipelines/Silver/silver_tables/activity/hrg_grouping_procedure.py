"""Silver activity.hrg_grouping_procedure: 1 dataset(s).

- datasets: clinical_hrg_grouping_procedure
- moved verbatim from: 4_silver_care_pathways:3008, 4_silver_care_pathways:3023

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


# Clinical / hrg grouping procedure

# ==== journey_clinical.hrg_grouping_procedure ====
HRG_GROUPING_PROCEDURE_PUBLIC_COLUMNS = [
    "patient_event_key", "source_object", "cds_id", "sequence",
    "opcs4_code", "display", "loaded_at",
]


@materialized_view(
    name=_n("journey_clinical.hrg_grouping_procedure"),
    refresh_policy="auto",
)
def hrg_grouping_procedure():
    # Build the declared dataset: One OPCS-4 procedure code per HRG grouping row in source
    # order.
    stage = spark.read.table(
        _n("journey_clinical._hrg_grouping_stage")
    ).select(
        "patient_event_key", "source_object", "cds_id",
        "opcs_procedure_codes_json", "loaded_at",
    )
    return (
        stage.select(
            "patient_event_key", "source_object", "cds_id", "loaded_at",
            F.posexplode_outer(
                F.from_json(F.col("opcs_procedure_codes_json"), "array<string>")
            ).alias("_position", "opcs4_code"),
        )
        .where(F.col("opcs4_code").isNotNull())
        .select(
            "patient_event_key", "source_object", "cds_id",
            (F.col("_position") + 1).cast("long").alias("sequence"),
            F.trim(F.col("opcs4_code")).alias("opcs4_code"),
            F.lit(None).cast("string").alias("display"),
            "loaded_at",
        )
        .select(*HRG_GROUPING_PROCEDURE_PUBLIC_COLUMNS)
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_hrg_grouping_procedure': {'columns': {'cds_id': 'Native CDS_APC_ID or CDS_OPA_ID within '
                                                           'source_object.',
                                                 'display': 'Reserved display text; populated in Session 3.',
                                                 'loaded_at': 'Bronze load timestamp inherited from the HRG '
                                                              'grouping row.',
                                                 'opcs4_code': 'Verbatim OPCS-4 procedure code from the HRG '
                                                               'source array.',
                                                 'patient_event_key': 'Deterministic key of the parent '
                                                                      'clinical_hrg_grouping row.',
                                                 'sequence': 'One-based source order of the OPCS-4 procedure '
                                                             'code.',
                                                 'source_object': 'SLAM source arm; part of the native '
                                                                  'parent key.'},
                                     'table': 'One OPCS-4 procedure code per HRG grouping row in source '
                                              'order.'}}
