"""Silver activity.hrg_grouping_diagnosis: 1 dataset(s).

- datasets: clinical_hrg_grouping_diagnosis
- moved verbatim from: 4_silver_care_pathways:2951, 4_silver_care_pathways:2966

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


# Clinical / hrg grouping diagnosis

# ==== journey_clinical.hrg_grouping_diagnosis ====
HRG_GROUPING_DIAGNOSIS_PUBLIC_COLUMNS = [
    "patient_event_key", "source_object", "cds_id", "sequence",
    "icd10_code", "display", "loaded_at",
]


@materialized_view(
    name=_n("journey_clinical.hrg_grouping_diagnosis"),
    refresh_policy="auto",
)
def hrg_grouping_diagnosis():
    # Build the declared dataset: One ICD-10 diagnosis code per HRG grouping row in source
    # order.
    stage = spark.read.table(
        _n("journey_clinical._hrg_grouping_stage")
    ).select(
        "patient_event_key", "source_object", "cds_id",
        "icd_diagnosis_codes_json", "loaded_at",
    )
    return (
        stage.select(
            "patient_event_key", "source_object", "cds_id", "loaded_at",
            F.posexplode_outer(
                F.from_json(F.col("icd_diagnosis_codes_json"), "array<string>")
            ).alias("_position", "icd10_code"),
        )
        .where(F.col("icd10_code").isNotNull())
        .select(
            "patient_event_key", "source_object", "cds_id",
            (F.col("_position") + 1).cast("long").alias("sequence"),
            F.trim(F.col("icd10_code")).alias("icd10_code"),
            F.lit(None).cast("string").alias("display"),
            "loaded_at",
        )
        .select(*HRG_GROUPING_DIAGNOSIS_PUBLIC_COLUMNS)
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_hrg_grouping_diagnosis': {'columns': {'cds_id': 'Native CDS_APC_ID or CDS_OPA_ID within '
                                                           'source_object.',
                                                 'display': 'Reserved display text; populated in Session 3.',
                                                 'icd10_code': 'Verbatim ICD-10 diagnosis code from the HRG '
                                                               'source array.',
                                                 'loaded_at': 'Bronze load timestamp inherited from the HRG '
                                                              'grouping row.',
                                                 'patient_event_key': 'Deterministic key of the parent '
                                                                      'clinical_hrg_grouping row.',
                                                 'sequence': 'One-based source order of the ICD-10 diagnosis '
                                                             'code.',
                                                 'source_object': 'SLAM source arm; part of the native '
                                                                  'parent key.'},
                                     'table': 'One ICD-10 diagnosis code per HRG grouping row in source '
                                              'order.'}}
