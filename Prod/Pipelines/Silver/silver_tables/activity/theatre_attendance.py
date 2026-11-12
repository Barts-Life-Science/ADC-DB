"""Silver activity.theatre_attendance: 1 dataset(s).

- datasets: reference_theatre_attendance
- moved verbatim from: 4_silver_care_pathways:4659, 4_silver_care_pathways:4683, 4_silver_care_pathways:4686, 4_silver_care_pathways:4691, 4_silver_care_pathways:4716

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id
from silver_tables.activity._shared import _theatre_parent_keys


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


SRC_THEATRE_ATTENDANCE = "4_prod.bronze.map_theatre_case_attendance"


THEATRE_ATTENDANCE_LIFECYCLE_FIELDS = [
]


THEATRE_ATTENDANCE_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_theatre_attendance():
    # Assemble theatre attendance rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    s = read_source(SRC_THEATRE_ATTENDANCE).alias("s")
    d = s.join(_theatre_parent_keys(), s.SURG_CASE_ID == F.col("_parent_surg_case_id"), "left")
    # contract v2: rename deterministic theatre identities as keys and publish native practitioner, person, and encounter ids
    return d.select(
        stable_id("theatre_attendance:surginet", s.CASE_ATTENDANCE_ID).alias("theatre_attendance_key"),
        s.SURG_CASE_ID.cast("string").alias("theatre_case_key"),
        F.when(F.col("_parent_surg_case_id").isNotNull(), F.lit("linked"))
         .otherwise(F.lit("orphan")).alias("case_link_status"),
        s.CASE_ATTENDEE_ID.cast("bigint").alias("attendee_practitioner_id"),
        s.CASE_ATTENDEE_ID.cast("string").alias("attendee_personnel_id"),
        s.ROLE_PERF_DESCRIPTION.alias("role_description"),
        s.SIGNING_ATTENDEE_IND.alias("signing_attendee_ind"),
        s.IN_DT_TM.alias("in_datetime"), s.IN_DT_TM_QUALITY.alias("in_datetime_quality"),
        s.OUT_DT_TM.alias("out_datetime"), s.OUT_DT_TM_QUALITY.alias("out_datetime_quality"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        s.SURG_AREA_DESCRIPTION.alias("surgical_area_desc"), s.ACTIVE_IND.alias("active_ind"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("surginet").alias("_source_system"), F.lit(SRC_THEATRE_ATTENDANCE).alias("_source_table"),
        s.CASE_ATTENDANCE_ID.cast("string").alias("_source_row_id"),
    )


@materialized_view(
    name=_n("journey_reference.theatre_attendance"),
    cluster_by=["person_id"], refresh_policy="auto",
)
def theatre_attendance():
    # Build the declared dataset: SurgiNet case attendance; parent-orphan rows remain visible
    # and flagged.
    return _lifecycle_source_theatre_attendance().drop(*THEATRE_ATTENDANCE_LIFECYCLE_FIELDS, *THEATRE_ATTENDANCE_RETIRED_COLUMNS)


VALUE_POLICIES = {
    'reference_theatre_attendance': ValuePolicy(table='reference_theatre_attendance', enabled=True, zero_as_null=('attendee_personnel_id', 'attendee_practitioner_id')),
}


WRAP_POLICY = {'reference_theatre_attendance'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_theatre_attendance': {'columns': {'_source_row_id': 'Journey care_participation; retained as a '
                                                                'source-faithful bronze input or Journey '
                                                                'standard-block field.',
                                              '_source_system': 'Value describing source system for the '
                                                                'theatre attendance record. It is produced '
                                                                'by the silver transformation and has no '
                                                                'direct bronze-column lineage entry. '
                                                                'Whitespace and source sentinel text are '
                                                                'retained unless the pipeline explicitly '
                                                                'normalizes them; null means no value was '
                                                                'supplied.',
                                              '_source_table': 'Value describing source table for the '
                                                               'theatre attendance record. It is produced by '
                                                               'the silver transformation and has no direct '
                                                               'bronze-column lineage entry. Whitespace and '
                                                               'source sentinel text are retained unless the '
                                                               'pipeline explicitly normalizes them; null '
                                                               'means no value was supplied.',
                                              'active_ind': 'Journey care_participation; retained as a '
                                                            'source-faithful bronze input or Journey '
                                                            'standard-block field.',
                                              'attendee_personnel_id': 'Journey care_participation '
                                                                       'practitioner FK; no practitioner '
                                                                       'attributes are re-landed.',
                                              'attendee_practitioner_id': 'Native Millennium personnel '
                                                                          'PERSON_ID as BIGINT for the '
                                                                          'attendee.',
                                              'case_link_status': 'S14 theatre feeder and Journey standard '
                                                                  'block; retained as a source-faithful '
                                                                  'bronze input or Journey standard-block '
                                                                  'field.',
                                              'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT.',
                                              'in_datetime': 'Journey care_participation; retained as a '
                                                             'source-faithful bronze input or Journey '
                                                             'standard-block field.',
                                              'in_datetime_quality': 'Journey care_participation; retained '
                                                                     'as a source-faithful bronze input or '
                                                                     'Journey standard-block field.',
                                              'loaded_at': 'map_theatre_case_attendance.ADC_UPDT carried '
                                                           'unchanged for the attendance row. The '
                                                           'parent-case existence join adds no timestamp; '
                                                           'this is bronze ingestion provenance, not '
                                                           'attendee in/out time or Silver refresh time.',
                                              'out_datetime': 'Journey care_participation; retained as a '
                                                              'source-faithful bronze input or Journey '
                                                              'standard-block field.',
                                              'out_datetime_quality': 'Journey care_participation; retained '
                                                                      'as a source-faithful bronze input or '
                                                                      'Journey standard-block field.',
                                              'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                              'role_description': 'Journey care_participation; retained as a '
                                                                  'source-faithful bronze input or Journey '
                                                                  'standard-block field.',
                                              'signing_attendee_ind': 'Journey care_participation; retained '
                                                                      'as a source-faithful bronze input or '
                                                                      'Journey standard-block field.',
                                              'surgical_area_desc': 'Journey care_participation; retained as '
                                                                    'a source-faithful bronze input or '
                                                                    'Journey standard-block field.',
                                              'theatre_attendance_key': 'Deterministic SHA-256 key for the '
                                                                        'theatre attendance row.',
                                              'theatre_case_key': 'Source-preserved theatre case key.'},
                                  'table': 'SurgiNet case attendance; parent-orphan rows remain visible and '
                                           'flagged.'}}
