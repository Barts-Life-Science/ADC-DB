"""Silver specialist.neonatal_care_day_activity: 1 dataset(s).

- datasets: clinical_neonatal_care_day_activity
- moved verbatim from: 3_silver_specialist_care:7208, 3_silver_specialist_care:7222

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


# Clinical / neonatal care day activity

# ==== journey_clinical.neonatal_care_day_activity ====
NEONATAL_CARE_DAY_ACTIVITY_PUBLIC_COLUMNS = [
    "patient_event_key", "entity_id", "activity_date", "sequence",
    "activity_code", "loaded_at",
]


@materialized_view(
    name=_n("journey_clinical.neonatal_care_day_activity"),
    refresh_policy="auto",
)
def neonatal_care_day_activity():
    # Build the declared dataset: One BadgerNet critical-care activity code per neonatal care
    # day.
    stage = spark.read.table(
        _n("journey_clinical._neonatal_care_day_stage")
    ).select(
        "patient_event_key", "entity_id", "activity_date",
        "activity_codes_json", "loaded_at",
    )
    return (
        stage.select(
            "patient_event_key", "entity_id", "activity_date", "loaded_at",
            F.posexplode_outer(
                F.from_json(F.col("activity_codes_json"), "array<string>")
            ).alias("_position", "activity_code"),
        )
        .where(F.col("activity_code").isNotNull())
        .select(
            "patient_event_key", "entity_id", "activity_date",
            (F.col("_position") + 1).cast("long").alias("sequence"),
            F.trim(F.col("activity_code")).alias("activity_code"),
            "loaded_at",
        )
        .select(*NEONATAL_CARE_DAY_ACTIVITY_PUBLIC_COLUMNS)
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_neonatal_care_day_activity': {'columns': {'activity_code': 'Verbatim neonatal critical-care '
                                                                      'activity code.',
                                                     'activity_date': 'BadgerNet ActivityDate; second native '
                                                                      'parent-key component.',
                                                     'entity_id': 'BadgerNet EntityID; first native '
                                                                  'parent-key component.',
                                                     'loaded_at': 'Bronze load timestamp inherited from the '
                                                                  'care-day row.',
                                                     'patient_event_key': 'Deterministic key of the parent '
                                                                          'clinical_neonatal_care_day row.',
                                                     'sequence': 'One-based source position from CCAC1 '
                                                                 'through CCAC20.'},
                                         'table': 'One BadgerNet critical-care activity code per neonatal '
                                                  'care day.'}}
