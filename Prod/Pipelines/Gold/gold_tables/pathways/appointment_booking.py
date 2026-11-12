"""Gold pathways.appointment_booking: 2 dataset(s).

- datasets: _qc_clinical_appointment_booking, clinical_appointment_booking
- moved verbatim from: 4_gold_care_pathways:4234, 4_gold_care_pathways:4267, 4_gold_care_pathways:4273, 4_gold_care_pathways:4288

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.appointment_booking ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_appointment applies the parent Gold drop rule.
CLINICAL_APPOINTMENT_BOOKING_SELECT = [
    '`sch_event_id` AS `sch_event_id`',
    '`sequence` AS `sequence`',
    '`schedule_id` AS `schedule_id`',
    '`schedule_sequence` AS `schedule_sequence`',
    '`status_code` AS `status_code`',
    '`status_display` AS `status_display`',
    '`status_meaning` AS `status_meaning`',
    '`effective_start` AS `effective_start`',
    # INSR_W3 W3.07: Gold: NULL where an open-end marker (J17).
    "CASE WHEN (CAST(`effective_end` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`effective_end` AS DATE)) < 9999) THEN NULL ELSE `effective_end` END AS `effective_end`",
    '`location_code` AS `location_code`',
    '`location_display` AS `location_display`',
    '`source_present_ind` AS `source_present_ind`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_APPOINTMENT_BOOKING_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_appointment_booking.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_appointment_booking"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_APPOINTMENT_BOOKING_MANDATORY_RULES)
def _gold_qc_clinical_appointment_booking():
    """Task 3 Gold child of clinical_appointment; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.appointment")).select(
        'sch_event_id',
    ).dropDuplicates(['sch_event_id'])
    child = spark.read.table(_src("clinical_appointment_booking"))
    df = _with_parent_status(child, parent, ['sch_event_id']).selectExpr(*CLINICAL_APPOINTMENT_BOOKING_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.appointment_booking"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_appointment_booking():
    """Publish clinical_appointment_booking without exposing the internal parent-admission marker."""
    return _with_row_key(spark.read.table(_n("gold_qc._clinical_appointment_booking")).drop("__gold_parent_present"), "clinical_appointment_booking")  # INSR_W3 W3.09


def _public_steps_clinical_appointment_booking(df):
    """clinical_appointment_booking: the public body after its projection (GoldRules.public_select), moved from gold_clinical_appointment_booking; df is the projected twin frame."""
    return _with_row_key(df, 'clinical_appointment_booking')


RULES = {
    'clinical_appointment_booking': GoldRules(
        table='clinical_appointment_booking',
        select=CLINICAL_APPOINTMENT_BOOKING_SELECT,
        mandatory=CLINICAL_APPOINTMENT_BOOKING_MANDATORY_RULES,
        row_key_sql=("sha2(to_json(named_struct('sch_event_id', `sch_event_id`, 'sequence', `sequence`, 'schedule_id', "
         "`schedule_id`, 'schedule_sequence', `schedule_sequence`, 'status_code', `status_code`, "
         "'status_display', `status_display`, 'status_meaning', `status_meaning`, 'effective_start', "
         "unix_micros(`effective_start`), 'effective_end', unix_micros(`effective_end`), 'location_code', "
         "`location_code`, 'location_display', `location_display`, 'source_present_ind', "
         "`source_present_ind`, 'loaded_at', unix_micros(`loaded_at`)), map('ignoreNullFields', 'false')), "
         '256)'),
        row_key_columns=('sch_event_id, sequence, schedule_id, schedule_sequence, status_code, status_display, '
         'status_meaning, effective_start, effective_end, location_code, location_display, '
         'source_present_ind, loaded_at'),
        public_select={'all_but': ['__gold_parent_present']},
        public_steps=(_public_steps_clinical_appointment_booking,)),
}
_rules = QcBinding(RULES)
_with_row_key = _rules._with_row_key


finalise()


COMMENTS = {'_qc_clinical_appointment_booking': {'columns': {'effective_end': 'Source effective-end timestamp.',
                                                  'effective_start': 'Source effective-start timestamp.',
                                                  'loaded_at': 'Bronze load timestamp of the scheduling row.',
                                                  'location_code': 'Source scheduling location code.',
                                                  'location_display': 'Source location description, falling '
                                                                      'back to free text.',
                                                  'sch_event_id': 'Millennium SCH_EVENT_ID of the parent '
                                                                  'clinical_appointment row.',
                                                  'schedule_id': 'Source scheduling iteration identifier.',
                                                  'schedule_sequence': 'Source scheduling sequence.',
                                                  'sequence': 'Booking sequence, falling back to '
                                                              'SCHEDULE_ID.',
                                                  'source_present_ind': 'Whether the source row remains '
                                                                        'present.',
                                                  'status_code': 'Source scheduling-state code for this '
                                                                 'iteration.',
                                                  'status_display': 'Source scheduling-state display for '
                                                                    'this iteration.',
                                                  'status_meaning': 'Source scheduling-state meaning for '
                                                                    'this iteration.'},
                                      'quantity_override': True,
                                      'table': 'Internal QC of clinical_appointment_booking; counts rows '
                                               'whose Gold parent is absent.'},
 'clinical_appointment_booking': {'columns': {'row_key': 'Row key: SHA-256 of the JSON of (sch_event_id, '
                                                         'sequence, schedule_id, schedule_sequence, '
                                                         'status_code, status_display, status_meaning, '
                                                         'effective_start, effective_end, location_code, '
                                                         'location_display, source_present_ind, loaded_at), '
                                                         'NULLs written explicitly. Unique per row; stable '
                                                         'while those values are unchanged. INSR_W3 W3.09.'},
                                  'quantity_override': False,
                                  'table': 'One booking or location iteration per scheduling appointment.'}}
