"""Gold pathways.appointment_resource: 2 dataset(s).

- datasets: _qc_clinical_appointment_resource, clinical_appointment_resource
- moved verbatim from: 4_gold_care_pathways:4303, 4_gold_care_pathways:4333, 4_gold_care_pathways:4339, 4_gold_care_pathways:4354

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.appointment_resource ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_appointment applies the parent Gold drop rule.
CLINICAL_APPOINTMENT_RESOURCE_SELECT = [
    '`sch_event_id` AS `sch_event_id`',
    '`sequence` AS `sequence`',
    '`appointment_role_id` AS `appointment_role_id`',
    '`schedule_id` AS `schedule_id`',
    '`role_code` AS `role_code`',
    '`role_display` AS `role_display`',
    '`practitioner_key` AS `practitioner_key`',
    '`location_code` AS `location_code`',
    '`slot_start` AS `slot_start`',
    '`slot_end` AS `slot_end`',
    '`source_present_ind` AS `source_present_ind`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_APPOINTMENT_RESOURCE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_appointment_resource.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_appointment_resource"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_APPOINTMENT_RESOURCE_MANDATORY_RULES)
def _gold_qc_clinical_appointment_resource():
    """Task 3 Gold child of clinical_appointment; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.appointment")).select(
        'sch_event_id',
    ).dropDuplicates(['sch_event_id'])
    child = spark.read.table(_src("clinical_appointment_resource"))
    df = _with_parent_status(child, parent, ['sch_event_id']).selectExpr(*CLINICAL_APPOINTMENT_RESOURCE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.appointment_resource"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_appointment_resource():
    """Publish clinical_appointment_resource without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_appointment_resource")).drop("__gold_parent_present")


finalise()


COMMENTS = {'_qc_clinical_appointment_resource': {'columns': {'appointment_role_id': 'Source scheduling '
                                                                          'appointment-role identifier.',
                                                   'loaded_at': 'Bronze load timestamp of the resource row.',
                                                   'location_code': 'Source appointment location code.',
                                                   'practitioner_key': 'Deterministic practitioner key '
                                                                       'derived from ALLOCATED_PERSONNEL_ID.',
                                                   'role_code': 'Source scheduling role code.',
                                                   'role_display': 'Source scheduling role display, falling '
                                                                   'back to role meaning.',
                                                   'sch_event_id': 'Millennium SCH_EVENT_ID of the parent '
                                                                   'clinical_appointment row.',
                                                   'schedule_id': 'Source schedule identifier.',
                                                   'sequence': 'Resource sequence, falling back to '
                                                               'SCH_APPT_ID.',
                                                   'slot_end': 'Source allocated slot end.',
                                                   'slot_start': 'Source allocated slot start.',
                                                   'source_present_ind': 'Whether the source row remains '
                                                                         'present.'},
                                       'quantity_override': True,
                                       'table': 'Internal QC of clinical_appointment_resource; counts rows '
                                                'whose Gold parent is absent.'},
 'clinical_appointment_resource': {'columns': {},
                                   'quantity_override': False,
                                   'table': 'One scheduling resource or slot row per appointment.'}}
