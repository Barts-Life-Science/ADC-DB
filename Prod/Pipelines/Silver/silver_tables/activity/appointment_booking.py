"""Silver activity.appointment_booking: 1 dataset(s).

- datasets: clinical_appointment_booking
- moved verbatim from: 4_silver_care_pathways:1275, 4_silver_care_pathways:1298

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, spark
from silver_tables.activity._shared import SRC_APPOINTMENT_SCHEDULE


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / appointment booking

# ==== journey_clinical.appointment_booking ====
APPOINTMENT_BOOKING_PUBLIC_COLUMNS = [
    "sch_event_id", "sequence", "schedule_id", "schedule_sequence",
    "status_code", "status_display", "status_meaning", "effective_start",
    "effective_end", "location_code", "location_display",
    "source_present_ind", "loaded_at",
]


@materialized_view(
    name=_n("journey_clinical.appointment_booking"),
    refresh_policy="auto",
)
def appointment_booking():
    # Contract v2 parent/child conservation: publish only bookings whose
    # appointment the parent published. map_appointment_schedule carries 4,563
    # SCH_EVENT_IDs that map_appointment does not hold at all, which left 5,630
    # booking rows unreachable through clinical_appointment. Taking the key set
    # from the parent's own output rather than re-deriving the parent's rule
    # means this cannot drift if that rule changes.
    # Build the declared dataset: One booking or location iteration per scheduling appointment.
    published = spark.read.table(_n("journey_clinical.appointment")).select(
        F.col("sch_event_id").cast("bigint").alias("published_sch_event_id")
    )
    s = read_source(SRC_APPOINTMENT_SCHEDULE)
    s = s.join(
        published,
        s.SCH_EVENT_ID.cast("bigint") == F.col("published_sch_event_id"),
        "left_semi",
    )
    return s.select(
        s.SCH_EVENT_ID.cast("bigint").alias("sch_event_id"),
        F.coalesce(s.SCHEDULE_SEQ, s.SCHEDULE_ID).cast("long").alias("sequence"),
        s.SCHEDULE_ID.cast("string").alias("schedule_id"),
        s.SCHEDULE_SEQ.cast("long").alias("schedule_sequence"),
        s.SCH_STATE_CD.cast("string").alias("status_code"),
        s.SCH_STATE_DESCRIPTION.alias("status_display"),
        s.SOURCE_STATE_MEANING.alias("status_meaning"),
        s.BEG_EFFECTIVE_DT_TM.alias("effective_start"),
        s.END_EFFECTIVE_DT_TM.alias("effective_end"),
        s.LOCATION_CD.cast("string").alias("location_code"),
        F.coalesce(s.LOCATION_DESCRIPTION, s.LOCATION_FREETEXT)
         .alias("location_display"),
        s.SOURCE_PRESENT_IND.cast("boolean").alias("source_present_ind"),
        s.ADC_UPDT.alias("loaded_at"),
    ).select(*APPOINTMENT_BOOKING_PUBLIC_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_appointment_booking': {'columns': {'effective_end': 'Source effective-end timestamp.',
                                              'effective_start': 'Source effective-start timestamp.',
                                              'loaded_at': 'Bronze load timestamp of the scheduling row.',
                                              'location_code': 'Source scheduling location code.',
                                              'location_display': 'Source location description, falling back '
                                                                  'to free text.',
                                              'sch_event_id': 'Millennium SCH_EVENT_ID of the parent '
                                                              'clinical_appointment row.',
                                              'schedule_id': 'Source scheduling iteration identifier.',
                                              'schedule_sequence': 'Source scheduling sequence.',
                                              'sequence': 'Booking sequence, falling back to SCHEDULE_ID.',
                                              'source_present_ind': 'Whether the source row remains present.',
                                              'status_code': 'Source scheduling-state code for this '
                                                             'iteration.',
                                              'status_display': 'Source scheduling-state display for this '
                                                                'iteration.',
                                              'status_meaning': 'Source scheduling-state meaning for this '
                                                                'iteration.'},
                                  'table': 'One booking or location iteration per scheduling appointment.'}}
