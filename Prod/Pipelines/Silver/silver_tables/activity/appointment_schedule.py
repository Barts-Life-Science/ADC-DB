"""Silver activity.appointment_schedule: 1 dataset(s).

- datasets: _clinical_appointment_schedule_grouped
- moved verbatim from: 4_silver_care_pathways:964, 4_silver_care_pathways:998

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source
from silver_tables.activity._shared import SRC_APPOINTMENT_SCHEDULE


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


def _appointment_schedule_grouped_query():
    # Group ordered schedule-state evidence per appointment for the appointment's history
    # payload.
    s = read_source(SRC_APPOINTMENT_SCHEDULE)
    entry = F.struct(
        F.coalesce(s.SCHEDULE_SEQ, s.SCHEDULE_ID).cast("long").alias("sequence"),
        s.SCHEDULE_ID.cast("string").alias("schedule_id"),
        s.SCHEDULE_SEQ.cast("long").alias("schedule_sequence"),
        s.SCH_STATE_CD.cast("string").alias("status_code"),
        s.SCH_STATE_DESCRIPTION.alias("status_display"),
        s.SOURCE_STATE_MEANING.alias("status_meaning"),
        s.BEG_EFFECTIVE_DT_TM.alias("effective_start"),
        s.END_EFFECTIVE_DT_TM.alias("effective_end"),
        s.LOCATION_CD.cast("string").alias("location_code"),
        F.coalesce(s.LOCATION_DESCRIPTION, s.LOCATION_FREETEXT).alias("location_display"),
        s.SOURCE_PRESENT_IND.cast("boolean").alias("source_present_ind"),
    )
    # SDI_APPT_LOCATION_V1: the display of the lowest LOCATION_CD (the code the appointment publishes), rows with a
    # display first, then the lowest display.
    s_disp = F.coalesce(s.LOCATION_DESCRIPTION, s.LOCATION_FREETEXT)
    s_disp = F.when(F.trim(s_disp) != "", s_disp)
    s_loc = F.min(F.when(s.LOCATION_CD.isNotNull(), F.struct(
        s.LOCATION_CD.alias("cd"), s_disp.isNull().alias("no_disp"), s_disp.alias("disp"))))
    return (
        s.groupBy("SCH_EVENT_ID")
        .agg(
            F.to_json(F.sort_array(F.collect_list(entry))).alias("_booking_json"),
            F.min("LOCATION_CD").alias("_schedule_location_cd"),
            s_loc.getField("disp").alias("_schedule_location_display"),  # SDI_APPT_LOCATION_V1
            F.max("SOURCE_ADC_UPDT").alias("_schedule_source_update"),
            F.max("ADC_UPDT").alias("_schedule_loaded_at"),
        )
    )


@materialized_view(
    name=_n("journey_clinical._appointment_schedule_grouped"),
    private=True,
    refresh_policy="auto",
)
def _appointment_schedule_grouped():
    # Build the declared dataset: Internal incremental appointment booking/location history
    # grouped as deterministic JSON.
    return _appointment_schedule_grouped_query()


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_appointment_schedule_grouped': {'columns': {},
                                            'table': 'Internal incremental appointment booking/location '
                                                     'history grouped as deterministic JSON.'}}
