"""Silver specialist.registry_device: 1 dataset(s).

- datasets: _clinical_registry_device_stage
- moved verbatim from: 3_silver_specialist_care:45

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n
from silver_tables._common.journey import _device_canonical, _registry_entry_canonical
from silver_tables.specialist.device import device


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical /  registry device stage
@materialized_view(
    name=_n("journey_clinical._registry_device_stage"),
    private=True,
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def _registry_device_stage():
    # Build the declared dataset: Incremental boundary for joined registry/device scalars and
    # deterministic VARIANT JSON.
    registry = _registry_entry_canonical().withColumn("_stage_kind", F.lit("registry"))
    device = _device_canonical().withColumn("_stage_kind", F.lit("device"))
    return registry.unionByName(device, allowMissingColumns=True)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_registry_device_stage': {'columns': {},
                                     'table': 'Incremental boundary for joined registry/device scalars and '
                                              'deterministic VARIANT JSON.'}}
