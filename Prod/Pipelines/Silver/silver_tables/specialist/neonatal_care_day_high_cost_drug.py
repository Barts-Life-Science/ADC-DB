"""Silver specialist.neonatal_care_day_high_cost_drug: 1 dataset(s).

- datasets: clinical_neonatal_care_day_high_cost_drug
- moved verbatim from: 3_silver_specialist_care:7263, 3_silver_specialist_care:7277

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


# Clinical / neonatal care day high cost drug

# ==== journey_clinical.neonatal_care_day_high_cost_drug ====
NEONATAL_CARE_DAY_HIGH_COST_DRUG_PUBLIC_COLUMNS = [
    "patient_event_key", "entity_id", "activity_date", "sequence",
    "high_cost_drug_code", "loaded_at",
]


@materialized_view(
    name=_n("journey_clinical.neonatal_care_day_high_cost_drug"),
    refresh_policy="auto",
)
def neonatal_care_day_high_cost_drug():
    # Build the declared dataset: One BadgerNet high-cost-drug code per neonatal care day.
    stage = spark.read.table(
        _n("journey_clinical._neonatal_care_day_stage")
    ).select(
        "patient_event_key", "entity_id", "activity_date",
        "high_cost_drugs_json", "loaded_at",
    )
    return (
        stage.select(
            "patient_event_key", "entity_id", "activity_date", "loaded_at",
            F.posexplode_outer(
                F.from_json(F.col("high_cost_drugs_json"), "array<string>")
            ).alias("_position", "high_cost_drug_code"),
        )
        .where(F.col("high_cost_drug_code").isNotNull())
        .select(
            "patient_event_key", "entity_id", "activity_date",
            (F.col("_position") + 1).cast("long").alias("sequence"),
            F.trim(F.col("high_cost_drug_code")).alias("high_cost_drug_code"),
            "loaded_at",
        )
        .select(*NEONATAL_CARE_DAY_HIGH_COST_DRUG_PUBLIC_COLUMNS)
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_neonatal_care_day_high_cost_drug': {'columns': {'activity_date': 'BadgerNet ActivityDate; second '
                                                                            'native parent-key component.',
                                                           'entity_id': 'BadgerNet EntityID; first native '
                                                                        'parent-key component.',
                                                           'high_cost_drug_code': 'Verbatim neonatal '
                                                                                  'high-cost-drug code.',
                                                           'loaded_at': 'Bronze load timestamp inherited '
                                                                        'from the care-day row.',
                                                           'patient_event_key': 'Deterministic key of the '
                                                                                'parent '
                                                                                'clinical_neonatal_care_day '
                                                                                'row.',
                                                           'sequence': 'One-based source position from '
                                                                       'HCDRUG1 through HCDRUG20.'},
                                               'table': 'One BadgerNet high-cost-drug code per neonatal care '
                                                        'day.'}}
