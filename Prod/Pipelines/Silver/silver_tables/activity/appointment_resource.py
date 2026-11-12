"""Silver activity.appointment_resource: 2 dataset(s).

- datasets: _clinical_appointment_resource_grouped, clinical_appointment_resource
- moved verbatim from: 4_silver_care_pathways:1016, 4_silver_care_pathways:1070, 4_silver_care_pathways:1347, 4_silver_care_pathways:1368

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, spark, stable_id
from silver_tables._common.journey import SRC_APPOINTMENT_RESOURCE


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical /  appointment resource grouped
def _appointment_resource_grouped_query():
    # Group appointment resource and practitioner-role evidence for the appointment's resource
    # payload.
    r = read_source(SRC_APPOINTMENT_RESOURCE)
    entry = F.struct(
        F.coalesce(r.SCHEDULE_SEQ, r.SCH_APPT_ID).cast("long").alias("sequence"),
        r.SCH_APPT_ID.cast("string").alias("appointment_role_id"),
        r.SCHEDULE_ID.cast("string").alias("schedule_id"),
        r.SCH_ROLE_CD.cast("string").alias("role_code"),
        F.coalesce(r.SCH_ROLE_DESCRIPTION, r.ROLE_MEANING).alias("role_display"),
        F.when(r.ALLOCATED_PERSONNEL_ID.isNotNull(),
               stable_id("practitioner:mill", r.ALLOCATED_PERSONNEL_ID))
        .alias("practitioner_id"),
        r.APPT_LOCATION_CD.cast("string").alias("location_code"),
        r.BEG_DT_TM.alias("slot_start"), r.END_DT_TM.alias("slot_end"),
        r.SOURCE_PRESENT_IND.cast("boolean").alias("source_present_ind"),
    )
    # SDI_APPT_LOCATION_V1: the display of the lowest APPT_LOCATION_CD (the code the appointment publishes); the booked
    # primary non-patient resource (clinic session, or theatre) and the service resource from the latest SCHEDULE_SEQ,
    # lowest code on ties.
    r_disp = F.when(F.trim(r.APPT_LOCATION_DESCRIPTION) != "", r.APPT_LOCATION_DESCRIPTION)
    r_loc = F.min(F.when(r.APPT_LOCATION_CD.isNotNull(), F.struct(
        r.APPT_LOCATION_CD.alias("cd"), r_disp.isNull().alias("no_disp"), r_disp.alias("disp"))))
    r_seq = F.coalesce(r.SCHEDULE_SEQ.cast("long"), F.lit(-1))

    def _latest(keep, code, disp):
        return F.max(F.when(keep & (code > 0), F.struct(
            r_seq.alias("seq"), (-code.cast("long")).alias("neg_cd"), code.cast("long").alias("cd"), disp.alias("disp"))))

    r_clinic = _latest((F.coalesce(r.PRIMARY_ROLE_IND.cast("long"), F.lit(0)) == 1)
                       & (F.coalesce(F.upper(r.ROLE_MEANING), F.lit("")) != "PATIENT"),
                       r.RESOURCE_CD, r.RESOURCE_DESCRIPTION)
    r_service = _latest(F.lit(True), r.SERVICE_RESOURCE_CD, r.SERVICE_RESOURCE_DESCRIPTION)
    return (
        r.groupBy("SCH_EVENT_ID")
        .agg(
            F.to_json(F.sort_array(F.collect_list(entry))).alias("_resource_json"),
            F.max("BEG_DT_TM").alias("_slot_start"),
            F.max("END_DT_TM").alias("_slot_end"),
            F.max("BEG_DT_TM_LOCAL").alias("_slot_start_local"),
            F.max("END_DT_TM_LOCAL").alias("_slot_end_local"),
            # TZ_SILVER_LOCAL_V1: Europe/London companions (bronze *_LOCAL for Millennium; local-clock sources unchanged)
            F.min("ALLOCATED_PERSONNEL_ID").alias("_allocated_personnel_id"),
            F.min("APPT_LOCATION_CD").alias("_resource_location_cd"),
            r_loc.getField("disp").alias("_resource_location_display"),  # SDI_APPT_LOCATION_V1
            r_clinic.getField("cd").alias("_clinic_resource_cd"),
            r_clinic.getField("disp").alias("_clinic_resource_display"),
            r_service.getField("cd").alias("_service_resource_cd"),
            r_service.getField("disp").alias("_service_resource_display"),
            F.max("SOURCE_ADC_UPDT").alias("_resource_source_update"),
            F.max("ADC_UPDT").alias("_resource_loaded_at"),
        )
    )


@materialized_view(
    name=_n("journey_clinical._appointment_resource_grouped"),
    private=True,
    refresh_policy="auto",
)
def _appointment_resource_grouped():
    # Build the declared dataset: Internal incremental appointment resource/slot history grouped
    # as deterministic JSON.
    return _appointment_resource_grouped_query()


# Clinical / appointment resource

# ==== journey_clinical.appointment_resource ====
APPOINTMENT_RESOURCE_PUBLIC_COLUMNS = [
    "sch_event_id", "sequence", "appointment_role_id", "schedule_id",
    "role_code", "role_display", "practitioner_key", "location_code",
    "slot_start", "slot_end", "source_present_ind", "loaded_at",
]


@materialized_view(
    name=_n("journey_clinical.appointment_resource"),
    refresh_policy="auto",
)
def appointment_resource():
    # Contract v2 parent/child conservation, as for appointment_booking above.
    # map_appointment_resource carries 40,219 SCH_EVENT_IDs absent from
    # map_appointment, which left 53,626 resource rows unreachable through
    # clinical_appointment.
    # Build the declared dataset: One scheduling resource or slot row per appointment.
    published = spark.read.table(_n("journey_clinical.appointment")).select(
        F.col("sch_event_id").cast("bigint").alias("published_sch_event_id")
    )
    r = read_source(SRC_APPOINTMENT_RESOURCE)
    r = r.join(
        published,
        r.SCH_EVENT_ID.cast("bigint") == F.col("published_sch_event_id"),
        "left_semi",
    )
    return r.select(
        r.SCH_EVENT_ID.cast("bigint").alias("sch_event_id"),
        F.coalesce(r.SCHEDULE_SEQ, r.SCH_APPT_ID).cast("long").alias("sequence"),
        r.SCH_APPT_ID.cast("string").alias("appointment_role_id"),
        r.SCHEDULE_ID.cast("string").alias("schedule_id"),
        r.SCH_ROLE_CD.cast("string").alias("role_code"),
        F.coalesce(r.SCH_ROLE_DESCRIPTION, r.ROLE_MEANING).alias("role_display"),
        F.when(
            r.ALLOCATED_PERSONNEL_ID.isNotNull(),
            stable_id("practitioner:mill", r.ALLOCATED_PERSONNEL_ID),
        ).alias("practitioner_key"),
        r.APPT_LOCATION_CD.cast("string").alias("location_code"),
        r.BEG_DT_TM.alias("slot_start"),
        r.END_DT_TM.alias("slot_end"),
        r.SOURCE_PRESENT_IND.cast("boolean").alias("source_present_ind"),
        r.ADC_UPDT.alias("loaded_at"),
    ).select(*APPOINTMENT_RESOURCE_PUBLIC_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_appointment_resource_grouped': {'columns': {},
                                            'table': 'Internal incremental appointment resource/slot history '
                                                     'grouped as deterministic JSON.'},
 'clinical_appointment_resource': {'columns': {'appointment_role_id': 'Source scheduling appointment-role '
                                                                      'identifier.',
                                               'loaded_at': 'Bronze load timestamp of the resource row.',
                                               'location_code': 'Source appointment location code.',
                                               'practitioner_key': 'Deterministic practitioner key derived '
                                                                   'from ALLOCATED_PERSONNEL_ID.',
                                               'role_code': 'Source scheduling role code.',
                                               'role_display': 'Source scheduling role display, falling back '
                                                               'to role meaning.',
                                               'sch_event_id': 'Millennium SCH_EVENT_ID of the parent '
                                                               'clinical_appointment row.',
                                               'schedule_id': 'Source schedule identifier.',
                                               'sequence': 'Resource sequence, falling back to SCH_APPT_ID.',
                                               'slot_end': 'Source allocated slot end.',
                                               'slot_start': 'Source allocated slot start.',
                                               'source_present_ind': 'Whether the source row remains '
                                                                     'present.'},
                                   'table': 'One scheduling resource or slot row per appointment.'}}
