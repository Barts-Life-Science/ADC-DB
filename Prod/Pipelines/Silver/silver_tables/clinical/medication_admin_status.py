"""Silver clinical.medication_admin_status: 1 dataset(s).

- datasets: clinical_medication_admin_status
- moved verbatim from: 2_silver_clinical_events:6253, 2_silver_clinical_events:6272

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, _usable_code, spark


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / medication admin status

# ==== journey_clinical.medication_admin_status ====
MEDICATION_ADMIN_STATUS_PUBLIC_COLUMNS = [
    "event_id", "sequence", "status_kind", "status_code", "status_display",
    "effective_datetime", "practitioner_key", "source_action_sequence",
    "source_present_ind", "loaded_at",
]


@materialized_view(
    name=_n("journey_clinical.medication_admin_status"),
    refresh_policy="auto",
)
def medication_admin_status():
    # Contract v2 parent/child conservation: medication_admin publishes only the
    # administrations whose ORDER_SYNONYM_ID passes Gate B, so this child applies
    # the same gate. Without it the child carried 11,722 rows over 3,020 events
    # that no reader could reach through the parent.
    # Build the declared dataset: One status milestone per medication administration, retaining
    # source sequence and the parent event key.
    parents = spark.read.table(
        _n("journey_clinical._medication_admin_primitive")
    ).where(_usable_code(F.col("source_code"))).select(
        "event_id", "_status_history_json", "loaded_at"
    )
    status_schema = (
        "array<struct<sequence:bigint,status_kind:string,status_code:string,"
        "status_display:string,effective_datetime:timestamp,practitioner_id:string,"
        "source_action_sequence:bigint,source_present_ind:boolean>>"
    )
    exploded = parents.select(
        "event_id", "loaded_at",
        F.explode(F.from_json(F.col("_status_history_json"), status_schema)).alias("_status"),
    )
    return exploded.select(
        "event_id",
        F.col("_status.sequence").alias("sequence"),
        F.col("_status.status_kind").alias("status_kind"),
        F.col("_status.status_code").alias("status_code"),
        F.col("_status.status_display").alias("status_display"),
        F.col("_status.effective_datetime").alias("effective_datetime"),
        F.col("_status.practitioner_id").alias("practitioner_key"),
        F.col("_status.source_action_sequence").alias("source_action_sequence"),
        F.col("_status.source_present_ind").alias("source_present_ind"),
        "loaded_at",
    ).select(*MEDICATION_ADMIN_STATUS_PUBLIC_COLUMNS)


VALUE_POLICIES = {
    'clinical_medication_admin_status': ValuePolicy(table='clinical_medication_admin_status', enabled=True, zero_as_null=('status_code',)),
}


WRAP_POLICY = {'clinical_medication_admin_status'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_medication_admin_status': {'columns': {'effective_datetime': 'Effective timestamp of the status '
                                                                        'milestone.',
                                                  'event_id': 'Millennium EVENT_ID of the parent medication '
                                                              'administration.',
                                                  'loaded_at': 'Bronze load timestamp inherited from the '
                                                               'parent source row.',
                                                  'practitioner_key': 'Deterministic practitioner key when '
                                                                      'the milestone identifies personnel.',
                                                  'sequence': 'Deterministic status milestone sequence from '
                                                              'the parent expression.',
                                                  'source_action_sequence': 'Source action sequence when '
                                                                            'supplied; null for synthesized '
                                                                            'milestones.',
                                                  'source_present_ind': 'True for status rows synthesized '
                                                                        'from a retained administration row.',
                                                  'status_code': 'Source status code synthesized by the '
                                                                 'existing parent expression.',
                                                  'status_display': 'Source status display synthesized by '
                                                                    'the existing parent expression.',
                                                  'status_kind': 'Milestone kind such as scheduled, result, '
                                                                 'verified, or discontinued.'},
                                      'table': 'One status milestone per medication administration, '
                                               'retaining source sequence and the parent event key.'}}

# INSR_R4_SILVER_V1 (child): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import ParentInputs, with_parent_inputs  # noqa: E402
R4_PARENT_INPUTS = ParentInputs(parent='clinical_medication_admin', dataset='_clinical_medication_admin_parent_inputs', keys=('event_id',), inputs=('identity_status', 'person_id'), admit_sql="coalesce(((identity_status = 'resolved')), false)", name=_n('journey_clinical._medication_admin_parent_inputs'), parent_name=_n('journey_clinical.medication_admin'))
SPECS = with_parent_inputs(SPECS, 'clinical_medication_admin_status', R4_PARENT_INPUTS)
