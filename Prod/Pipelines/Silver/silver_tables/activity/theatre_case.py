"""Silver activity.theatre_case: 1 dataset(s).

- datasets: reference_theatre_case
- moved verbatim from: 4_silver_care_pathways:5146, 4_silver_care_pathways:5149, 4_silver_care_pathways:5153, 4_silver_care_pathways:5162, 4_silver_care_pathways:5213, silver_value_policy.py:313

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source
from silver_tables._common.journey import SRC_THEATRE_CASE, SRC_THEATRE_CASE_PROCEDURE


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


def _surgery_stop_minus_start_minutes(F):
    """Minutes from surgery_start to surgery_stop (fractional); NULL when either end is NULL."""
    return (F.unix_timestamp("surgery_stop") - F.unix_timestamp("surgery_start")) / 60


THEATRE_CASE_LIFECYCLE_FIELDS = [
]


THEATRE_CASE_RETIRED_COLUMNS = [

]


def _theatre_case_implant_expected():
    # Active procedures only; the max over the source strings prefers scheduled_procedure.
    p = read_source(SRC_THEATRE_CASE_PROCEDURE).where(F.coalesce(F.col("ACTIVE_IND"), F.lit(1)) == 1)
    src = (F.when(F.col("SCHED_IMPLANT_IND") == 1, F.lit("scheduled_procedure"))
           .when(F.col("CATALOG_IMPLANT_IND") == 1, F.lit("procedure_catalogue")))
    return p.groupBy(F.col("SURG_CASE_ID").alias("_ie_surg_case_id")).agg(
        F.max(src.isNotNull()).alias("_implant_expected_ind"), F.max(src).alias("_implant_expected_source"))


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_theatre_case():
    # Assemble one row per SurgiNet case with the case covariates and the implant-expected denominator.
    c = read_source(SRC_THEATRE_CASE).alias("c")
    d = c.join(_theatre_case_implant_expected(), c.SURG_CASE_ID == F.col("_ie_surg_case_id"), "left")
    return d.select(
        c.SURG_CASE_ID.cast("string").alias("theatre_case_key"),
        c.SURG_CASE_NBR_FORMATTED.alias("surgical_case_number"),
        c.PERSON_ID.cast("bigint").alias("person_id"),
        c.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        c.CASE_STATUS.alias("case_status"),
        c.ACTIVE_IND.alias("active_ind"),
        c.SOURCE_PRESENT_IND.alias("source_present_ind"),
        c.SCHED_START_DT_TM.alias("scheduled_start"),
        c.SCHED_START_DT_TM_LOCAL.alias("scheduled_start_local"),
        c.CHECKIN_DT_TM.alias("checkin_datetime"),
        c.SURG_START_DT_TM.alias("surgery_start"),
        c.SURG_START_DT_TM_LOCAL.alias("surgery_start_local"),
        c.SURG_START_DT_TM_QUALITY.alias("surgery_start_quality"),
        c.SURG_STOP_DT_TM.alias("surgery_stop"),
        c.SURGERY_DURATION_MINUTES.alias("surgery_duration_minutes"),
        c.SCHEDULED_DURATION_MINUTES.alias("scheduled_duration_minutes"),
        c.PERFORMED_MILESTONE_IND.alias("performed_milestone_ind"),
        c.ADD_ON_IND.alias("add_on_ind"),
        c.ASA_CLASS_CD.cast("string").alias("asa_class_code"),
        c.ASA_CLASS_DESCRIPTION.alias("asa_class_display"),
        c.WOUND_CLASS_CD.cast("string").alias("wound_class_code"),
        c.WOUND_CLASS_DESCRIPTION.alias("wound_class_display"),
        c.ANESTH_TYPE_CD.cast("string").alias("anaesthesia_type_code"),
        c.ANESTH_TYPE_DESCRIPTION.alias("anaesthesia_type_display"),
        c.CASE_LEVEL_CD.cast("string").alias("case_level_code"),
        c.CASE_LEVEL_DESCRIPTION.alias("case_level_display"),
        c.PAT_TYPE_CD.cast("string").alias("patient_type_code"),
        c.PAT_TYPE_DESCRIPTION.alias("patient_type_display"),
        c.SURG_SPECIALTY_ID.cast("bigint").alias("surgical_specialty_id"),
        c.SURGEON_PERSONNEL_ID.cast("bigint").alias("surgeon_practitioner_id"),
        c.ANAESTHETIST_PERSONNEL_ID.cast("bigint").alias("anaesthetist_practitioner_id"),
        c.INST_DESCRIPTION.alias("institution_display"),
        c.DEPT_DESCRIPTION.alias("department_display"),
        c.SURG_AREA_DESCRIPTION.alias("surgical_area_display"),
        c.SURG_OP_LOCATION_DESCRIPTION.alias("operating_location_display"),
        c.CANCEL_DT_TM.alias("cancel_datetime"),
        c.CANCEL_REASON_CD.cast("string").alias("cancel_reason_code"),
        c.CANCEL_REASON_DESCRIPTION.alias("cancel_reason_display"),
        F.col("_implant_expected_ind").alias("implant_expected_ind"),
        F.col("_implant_expected_source").alias("implant_expected_source"),
        c.ADC_UPDT.alias("loaded_at"),
        F.lit("surginet").alias("_source_system"),
        F.lit(SRC_THEATRE_CASE).alias("_source_table"),
        c.SURG_CASE_ID.cast("string").alias("_source_row_id"),
    )


@materialized_view(
    name=_n("journey_reference.theatre_case"),
    cluster_by=["person_id"], refresh_policy="auto",
)
def theatre_case():
    # Build the declared dataset: one SurgiNet surgical case.
    return _lifecycle_source_theatre_case().drop(*THEATRE_CASE_LIFECYCLE_FIELDS, *THEATRE_CASE_RETIRED_COLUMNS)


VALUE_POLICIES = {
    'reference_theatre_case': ValuePolicy(table='reference_theatre_case', enabled=True, zero_as_null=('anaesthetist_practitioner_id', 'surgeon_practitioner_id', 'surgical_specialty_id', 'asa_class_code', 'cancel_reason_code', 'case_level_code', 'patient_type_code', 'wound_class_code')),
}


# W2.10 (L1936, L1326): theatre procedure start and surgery duration.
VALUE_POLICIES["reference_theatre_case"].register("procedure_start_derived", ["surgery_start", "checkin_datetime", "scheduled_start", "case_status"],
         lambda F: F.coalesce(F.col("surgery_start"), F.col("checkin_datetime"),
                              F.when(F.col("case_status") == "PERFORMED", F.col("scheduled_start"))),
         "Surgery start, else theatre check-in, else (performed cases only) the scheduled start. INSR_W2 W2.10 (L1936).")


VALUE_POLICIES["reference_theatre_case"].register("time_source", ["surgery_start", "checkin_datetime", "scheduled_start", "case_status"],
         lambda F: F.when(F.col("surgery_start").isNotNull(), F.lit("surgery_start"))
                    .when(F.col("checkin_datetime").isNotNull(), F.lit("checkin"))
                    .when((F.col("case_status") == "PERFORMED") & F.col("scheduled_start").isNotNull(), F.lit("scheduled_start")),
         "Which time supplied procedure_start_derived. INSR_W2 W2.10.")


VALUE_POLICIES["reference_theatre_case"].register("duration_minutes_derived", ["surgery_duration_minutes", "surgery_start", "surgery_stop"],
         lambda F: F.when(F.col("surgery_duration_minutes").between(1, 1440), F.col("surgery_duration_minutes"))
                    .when(_surgery_stop_minus_start_minutes(F).between(1, 1440),
                          _surgery_stop_minus_start_minutes(F).cast("int")),
         "Surgery duration in minutes within 1-1440: the bronze value, else stop minus start; NULL otherwise "
         "(PMS Device L1326 outliers). INSR_W2 W2.10.")


VALUE_POLICIES["reference_theatre_case"].register("duration_source", ["surgery_duration_minutes", "surgery_start", "surgery_stop"],
         lambda F: F.when(F.col("surgery_duration_minutes").between(1, 1440), F.lit("bronze_duration"))
                    .when(_surgery_stop_minus_start_minutes(F).between(1, 1440), F.lit("stop_minus_start")),
         "bronze_duration or stop_minus_start; NULL iff duration_minutes_derived is NULL. INSR_W2 W2.10.")


WRAP_POLICY = {'reference_theatre_case'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_theatre_case': {'columns': {'_source_row_id': 'Bronze SURG_CASE_ID.',
                                        '_source_system': 'Constant surginet.',
                                        '_source_table': 'Bronze source table for the case row.',
                                        'active_ind': 'Millennium ACTIVE_IND for the case row.',
                                        'add_on_ind': 'SurgiNet add-on flag: case added to a list after '
                                                      'scheduling (emergency/urgent proxy).',
                                        'anaesthesia_type_code': 'Millennium code value for anaesthesia '
                                                                 'type.',
                                        'anaesthesia_type_display': 'Anaesthesia type description.',
                                        'anaesthetist_practitioner_id': 'Native Millennium personnel '
                                                                        'PERSON_ID of the anaesthetist.',
                                        'asa_class_code': 'Millennium code value for ASA physical status '
                                                          'class.',
                                        'asa_class_display': 'ASA physical status class description.',
                                        'cancel_datetime': 'Case cancellation time as a UTC instant '
                                                           '(Millennium); null when not cancelled.',
                                        'cancel_reason_code': 'Millennium code value for the cancellation '
                                                              'reason.',
                                        'cancel_reason_display': 'Cancellation reason description.',
                                        'case_level_code': 'Millennium code value for case level.',
                                        'case_level_display': 'Case level description.',
                                        'case_status': 'Case status derived in bronze: PERFORMED, CANCELLED '
                                                       'or SCHEDULED_ONLY.',
                                        'checkin_datetime': 'Patient check-in to theatres as a UTC instant '
                                                            '(Millennium).',
                                        'department_display': 'Theatre department description.',
                                        'duration_minutes_derived': 'Surgery duration in minutes within '
                                                                    '1-1440: the bronze value, else stop '
                                                                    'minus start; NULL otherwise (PMS Device '
                                                                    'L1326 outliers). INSR_W2 W2.10.',
                                        'duration_source': 'bronze_duration or stop_minus_start; NULL iff '
                                                           'duration_minutes_derived is NULL. INSR_W2 W2.10.',
                                        'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT.',
                                        'implant_expected_ind': 'True when any active procedure on the case '
                                                                'is implant-flagged at scheduling or in the '
                                                                'procedure catalogue; null when the case has '
                                                                'no procedure rows.',
                                        'implant_expected_source': 'scheduled_procedure when any procedure '
                                                                   'was implant-flagged at scheduling, else '
                                                                   'procedure_catalogue; null when not '
                                                                   'expected.',
                                        'institution_display': 'Institution (hospital site) description.',
                                        'loaded_at': 'map_theatre_case.ADC_UPDT carried unchanged; bronze '
                                                     'ingestion provenance, not case time.',
                                        'operating_location_display': 'Operating room description.',
                                        'patient_type_code': 'Millennium code value for patient type.',
                                        'patient_type_display': 'Patient type description (inpatient, day '
                                                                'case, ...).',
                                        'performed_milestone_ind': 'Whether a performed milestone was '
                                                                   'recorded for the case.',
                                        'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                        'procedure_start_derived': 'Surgery start, else theatre check-in, '
                                                                   'else (performed cases only) the '
                                                                   'scheduled start. INSR_W2 W2.10 (L1936).',
                                        'scheduled_duration_minutes': 'Scheduled case duration in minutes.',
                                        'scheduled_start': 'Scheduled case start as a UTC instant '
                                                           '(Millennium); use scheduled_start_local for '
                                                           'Europe/London wall-clock time.',
                                        'scheduled_start_local': 'scheduled_start in Europe/London '
                                                                 'wall-clock time (GMT/BST), from the bronze '
                                                                 '*_LOCAL companion.',
                                        'source_present_ind': 'Whether the case row was present in the '
                                                              'latest raw snapshot.',
                                        'surgeon_practitioner_id': 'Native Millennium personnel PERSON_ID of '
                                                                   'the primary surgeon.',
                                        'surgery_duration_minutes': 'Surgery stop minus start in minutes, as '
                                                                    'derived in bronze. NULL when either '
                                                                    'time is missing; implausible values '
                                                                    '(negative or over 24 hours) are kept as '
                                                                    'recorded.',
                                        'surgery_start': 'Surgery start as a UTC instant (Millennium); use '
                                                         'surgery_start_local for Europe/London wall-clock '
                                                         'time.',
                                        'surgery_start_local': 'surgery_start in Europe/London wall-clock '
                                                               'time (GMT/BST), from the bronze *_LOCAL '
                                                               'companion.',
                                        'surgery_start_quality': 'Bronze quality flag for surgery_start '
                                                                 '(e.g. milestone vs fallback).',
                                        'surgery_stop': 'Surgery stop as a UTC instant (Millennium).',
                                        'surgical_area_display': 'Surgical area description.',
                                        'surgical_case_number': 'Formatted SurgiNet case number as displayed '
                                                                'to staff.',
                                        'surgical_specialty_id': 'SurgiNet surgical specialty (prsnl_group) '
                                                                 'id.',
                                        'theatre_case_key': 'SurgiNet SURG_CASE_ID as string; the same value '
                                                            'children carry in theatre_case_key.',
                                        'time_source': 'Which time supplied procedure_start_derived. INSR_W2 '
                                                       'W2.10.',
                                        'wound_class_code': 'Millennium code value for wound class.',
                                        'wound_class_display': 'Wound class description (clean, '
                                                               'clean-contaminated, ...).'},
                            'table': 'One SurgiNet surgical case with case covariates (ASA, wound class, '
                                     'anaesthesia, urgency, times) and the implant-expected flag.'}}
