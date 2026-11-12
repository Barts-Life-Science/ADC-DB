"""Gold spine.theatre_case: 2 dataset(s).

- datasets: _qc_reference_theatre_case, reference_theatre_case
- moved verbatim from: 1_gold_spine_and_reference:4858, 1_gold_spine_and_reference:4904, 1_gold_spine_and_reference:4912, 1_gold_spine_and_reference:4981, 1_gold_spine_and_reference:4986, 1_gold_spine_and_reference:4990, 1_gold_spine_and_reference:5007

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# PMS_THEATRE_CASE_V1: gold twin of journey_reference.theatre_case.
REFERENCE_THEATRE_CASE_SELECT = [
    '`theatre_case_key` AS `theatre_case_key`',
    '`surgical_case_number` AS `surgical_case_number`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`case_status` AS `case_status`',
    '`active_ind` AS `active_ind`',
    '`source_present_ind` AS `source_present_ind`',
    '`scheduled_start` AS `scheduled_start`',
    '`scheduled_start_local` AS `scheduled_start_local`',
    '`checkin_datetime` AS `checkin_datetime`',
    '`surgery_start` AS `surgery_start`',
    '`surgery_start_local` AS `surgery_start_local`',
    '`surgery_start_quality` AS `surgery_start_quality`',
    '`surgery_stop` AS `surgery_stop`',
    '`surgery_duration_minutes` AS `surgery_duration_minutes`',
    '`scheduled_duration_minutes` AS `scheduled_duration_minutes`',
    '`performed_milestone_ind` AS `performed_milestone_ind`',
    '`add_on_ind` AS `add_on_ind`',
    '`asa_class_code` AS `asa_class_code`',
    '`asa_class_display` AS `asa_class_display`',
    '`wound_class_code` AS `wound_class_code`',
    '`wound_class_display` AS `wound_class_display`',
    '`anaesthesia_type_code` AS `anaesthesia_type_code`',
    '`anaesthesia_type_display` AS `anaesthesia_type_display`',
    '`case_level_code` AS `case_level_code`',
    '`case_level_display` AS `case_level_display`',
    '`patient_type_code` AS `patient_type_code`',
    '`patient_type_display` AS `patient_type_display`',
    '`surgical_specialty_id` AS `surgical_specialty_id`',
    '`surgeon_practitioner_id` AS `surgeon_practitioner_id`',
    '`anaesthetist_practitioner_id` AS `anaesthetist_practitioner_id`',
    '`institution_display` AS `institution_display`',
    '`department_display` AS `department_display`',
    '`surgical_area_display` AS `surgical_area_display`',
    '`operating_location_display` AS `operating_location_display`',
    '`cancel_datetime` AS `cancel_datetime`',
    '`cancel_reason_code` AS `cancel_reason_code`',
    '`cancel_reason_display` AS `cancel_reason_display`',
    '`implant_expected_ind` AS `implant_expected_ind`',
    '`implant_expected_source` AS `implant_expected_source`',
    '`loaded_at` AS `loaded_at`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
]


REFERENCE_THEATRE_CASE_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`procedure_start_derived` AS `procedure_start_derived`',
    '`time_source` AS `time_source`',
    '`duration_minutes_derived` AS `duration_minutes_derived`',
    '`duration_source` AS `duration_source`',
]


# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_THEATRE_CASE_ADVISORY_RULES = {
    # INSR_W4 W4.05 cross_field (estate 2026-10-29): SurgiNet duration differs from stop - start by more than an hour (L1326; duration is the source value).
    'gold.reference.theatre_case.table.insr_f6_duration_mismatch':
        'NOT COALESCE((abs(`surgery_duration_minutes` - (unix_timestamp(`surgery_stop`) - unix_timestamp(`surgery_start`)) / 60.0) > 60), FALSE)',
    # INSR_W4 W4.05 date_ordering (estate 2026-10-29): Surgery stop before start (L1326).
    'gold.reference.theatre_case.table.insr_f6_stop_before_start':
        'NOT COALESCE((`surgery_stop` < `surgery_start`), FALSE)',
    # A stop before start is a keying error in SurgiNet milestones; the times are kept as recorded
    # and the expectation metric reports how often it happens.
    "surgery_stop_not_before_start":
        "NOT COALESCE(`surgery_stop` < `surgery_start`, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
REFERENCE_THEATRE_CASE_PERSON_SCOPE_RULES = {
    "gold.reference.theatre_case.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
REFERENCE_THEATRE_CASE_ORPHAN_RULES = {
    "gold.reference.theatre_case.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_theatre_case"),
    refresh_policy="auto",
)
@dp.expect_all(REFERENCE_THEATRE_CASE_ADVISORY_RULES)
@dp.expect_all_or_drop(REFERENCE_THEATRE_CASE_PERSON_SCOPE_RULES)
@dp.expect_all(REFERENCE_THEATRE_CASE_ORPHAN_RULES)
def _gold_qc_reference_theatre_case():
    # encounter_id pointers the spine does not have are nulled; the case row is kept.
    df = _qc(
        "reference_theatre_case",
        REFERENCE_THEATRE_CASE_SELECT,
        fk_columns=["encounter_id"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_reference.theatre_case"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_theatre_case():
    df = spark.read.table(_n("gold_qc._reference_theatre_case")).selectExpr(
        '`theatre_case_key` AS `theatre_case_key`',
        '`surgical_case_number` AS `surgical_case_number`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`case_status` AS `case_status`',
        '`active_ind` AS `active_ind`',
        '`source_present_ind` AS `source_present_ind`',
        '`scheduled_start` AS `scheduled_start`',
        '`scheduled_start_local` AS `scheduled_start_local`',
        '`checkin_datetime` AS `checkin_datetime`',
        '`surgery_start` AS `surgery_start`',
        '`surgery_start_local` AS `surgery_start_local`',
        '`surgery_start_quality` AS `surgery_start_quality`',
        '`surgery_stop` AS `surgery_stop`',
        '`surgery_duration_minutes` AS `surgery_duration_minutes`',
        '`scheduled_duration_minutes` AS `scheduled_duration_minutes`',
        '`performed_milestone_ind` AS `performed_milestone_ind`',
        '`add_on_ind` AS `add_on_ind`',
        '`asa_class_code` AS `asa_class_code`',
        '`asa_class_display` AS `asa_class_display`',
        '`wound_class_code` AS `wound_class_code`',
        '`wound_class_display` AS `wound_class_display`',
        '`anaesthesia_type_code` AS `anaesthesia_type_code`',
        '`anaesthesia_type_display` AS `anaesthesia_type_display`',
        '`case_level_code` AS `case_level_code`',
        '`case_level_display` AS `case_level_display`',
        '`patient_type_code` AS `patient_type_code`',
        '`patient_type_display` AS `patient_type_display`',
        '`surgical_specialty_id` AS `surgical_specialty_id`',
        '`surgeon_practitioner_id` AS `surgeon_practitioner_id`',
        '`anaesthetist_practitioner_id` AS `anaesthetist_practitioner_id`',
        '`institution_display` AS `institution_display`',
        '`department_display` AS `department_display`',
        '`surgical_area_display` AS `surgical_area_display`',
        '`operating_location_display` AS `operating_location_display`',
        '`cancel_datetime` AS `cancel_datetime`',
        '`cancel_reason_code` AS `cancel_reason_code`',
        '`cancel_reason_display` AS `cancel_reason_display`',
        '`implant_expected_ind` AS `implant_expected_ind`',
        '`implant_expected_source` AS `implant_expected_source`',
        '`loaded_at` AS `loaded_at`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`procedure_start_derived` AS `procedure_start_derived`',  # INSR_W3 W3.12
        '`time_source` AS `time_source`',  # INSR_W3 W3.12
        '`duration_minutes_derived` AS `duration_minutes_derived`',  # INSR_W3 W3.12
        '`duration_source` AS `duration_source`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'reference_theatre_case': GoldRules(
        table='reference_theatre_case',
        select=REFERENCE_THEATRE_CASE_SELECT,
        advisory=REFERENCE_THEATRE_CASE_ADVISORY_RULES,
        person_scope=REFERENCE_THEATRE_CASE_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['theatre_case_key'], 'support_columns': ['loaded_at']},
        value_rules={'gold.reference.theatre_case.anaesthetist_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                      'column': 'anaesthetist_practitioner_id',
                                                                                      'companions': [],
                                                                                      'match': '`anaesthetist_practitioner_id` '
                                                                                               '= 0',
                                                                                      'table': 'reference_theatre_case'},
         'gold.reference.theatre_case.asa_class_code.insr_f7_zero_code': {'action': 'normalize',
                                                                          'column': 'asa_class_code',
                                                                          'companions': [],
                                                                          'match': 'TRIM(`asa_class_code`) '
                                                                                   "= '0' AND "
                                                                                   '`asa_class_display` IS '
                                                                                   'NULL',
                                                                          'table': 'reference_theatre_case'},
         'gold.reference.theatre_case.cancel_reason_code.insr_f7_zero_code': {'action': 'normalize',
                                                                              'column': 'cancel_reason_code',
                                                                              'companions': [],
                                                                              'match': 'TRIM(`cancel_reason_code`) '
                                                                                       "= '0' AND "
                                                                                       '`cancel_reason_display` '
                                                                                       'IS NULL',
                                                                              'table': 'reference_theatre_case'},
         'gold.reference.theatre_case.case_level_code.insr_f7_zero_code': {'action': 'normalize',
                                                                           'column': 'case_level_code',
                                                                           'companions': [],
                                                                           'match': 'TRIM(`case_level_code`) '
                                                                                    "= '0' AND "
                                                                                    '`case_level_display` '
                                                                                    'IS NULL',
                                                                           'table': 'reference_theatre_case'},
         'gold.reference.theatre_case.patient_type_code.insr_f7_zero_code': {'action': 'normalize',
                                                                             'column': 'patient_type_code',
                                                                             'companions': [],
                                                                             'match': 'TRIM(`patient_type_code`) '
                                                                                      "= '0' AND "
                                                                                      '`patient_type_display` '
                                                                                      'IS NULL',
                                                                             'table': 'reference_theatre_case'},
         'gold.reference.theatre_case.surgeon_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                 'column': 'surgeon_practitioner_id',
                                                                                 'companions': [],
                                                                                 'match': '`surgeon_practitioner_id` '
                                                                                          '= 0',
                                                                                 'table': 'reference_theatre_case'},
         'gold.reference.theatre_case.surgery_duration_minutes.insr_f6_duration_range': {'action': 'invalidate',
                                                                                         'column': 'surgery_duration_minutes',
                                                                                         'companions': [],
                                                                                         'match': '`surgery_duration_minutes` '
                                                                                                  '< 0 OR '
                                                                                                  '`surgery_duration_minutes` '
                                                                                                  '> 1440',
                                                                                         'table': 'reference_theatre_case'},
         'gold.reference.theatre_case.surgical_specialty_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                               'column': 'surgical_specialty_id',
                                                                               'companions': [],
                                                                               'match': '`surgical_specialty_id` '
                                                                                        '= 0',
                                                                               'table': 'reference_theatre_case'},
         'gold.reference.theatre_case.wound_class_code.insr_f7_zero_code': {'action': 'normalize',
                                                                            'column': 'wound_class_code',
                                                                            'companions': [],
                                                                            'match': 'TRIM(`wound_class_code`) '
                                                                                     "= '0' AND "
                                                                                     '`wound_class_display` '
                                                                                     'IS NULL',
                                                                            'table': 'reference_theatre_case'}}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_theatre_case': {'columns': {'_source_row_id': 'Bronze SURG_CASE_ID.',
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
                                            'cancel_reason_code': 'Millennium code value for the '
                                                                  'cancellation reason.',
                                            'cancel_reason_display': 'Cancellation reason description.',
                                            'case_level_code': 'Millennium code value for case level.',
                                            'case_level_display': 'Case level description.',
                                            'case_status': 'Case status derived in bronze: PERFORMED, '
                                                           'CANCELLED or SCHEDULED_ONLY.',
                                            'checkin_datetime': 'Patient check-in to theatres as a UTC '
                                                                'instant (Millennium).',
                                            'department_display': 'Theatre department description.',
                                            'duration_minutes_derived': 'Surgery duration in minutes within '
                                                                        '1-1440: the bronze value, else stop '
                                                                        'minus start; NULL otherwise (PMS '
                                                                        'Device L1326 outliers). INSR_W2 '
                                                                        'W2.10.',
                                            'duration_source': 'bronze_duration or stop_minus_start; NULL '
                                                               'iff duration_minutes_derived is NULL. '
                                                               'INSR_W2 W2.10.',
                                            'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT.',
                                            'implant_expected_ind': 'True when any active procedure on the '
                                                                    'case is implant-flagged at scheduling '
                                                                    'or in the procedure catalogue; null '
                                                                    'when the case has no procedure rows.',
                                            'implant_expected_source': 'scheduled_procedure when any '
                                                                       'procedure was implant-flagged at '
                                                                       'scheduling, else '
                                                                       'procedure_catalogue; null when not '
                                                                       'expected.',
                                            'institution_display': 'Institution (hospital site) description.',
                                            'loaded_at': 'map_theatre_case.ADC_UPDT carried unchanged; '
                                                         'bronze ingestion provenance, not case time.',
                                            'operating_location_display': 'Operating room description.',
                                            'patient_type_code': 'Millennium code value for patient type.',
                                            'patient_type_display': 'Patient type description (inpatient, '
                                                                    'day case, ...).',
                                            'performed_milestone_ind': 'Whether a performed milestone was '
                                                                       'recorded for the case.',
                                            'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                            'procedure_start_derived': 'Surgery start, else theatre '
                                                                       'check-in, else (performed cases '
                                                                       'only) the scheduled start. INSR_W2 '
                                                                       'W2.10 (L1936).',
                                            'scheduled_duration_minutes': 'Scheduled case duration in '
                                                                          'minutes.',
                                            'scheduled_start': 'Scheduled case start as a UTC instant '
                                                               '(Millennium); use scheduled_start_local for '
                                                               'Europe/London wall-clock time.',
                                            'scheduled_start_local': 'scheduled_start in Europe/London '
                                                                     'wall-clock time (GMT/BST), from the '
                                                                     'bronze *_LOCAL companion.',
                                            'source_present_ind': 'Whether the case row was present in the '
                                                                  'latest raw snapshot.',
                                            'surgeon_practitioner_id': 'Native Millennium personnel '
                                                                       'PERSON_ID of the primary surgeon.',
                                            'surgery_duration_minutes': 'Surgery stop minus start in '
                                                                        'minutes, as derived in bronze.',
                                            'surgery_start': 'Surgery start as a UTC instant (Millennium); '
                                                             'use surgery_start_local for Europe/London '
                                                             'wall-clock time.',
                                            'surgery_start_local': 'surgery_start in Europe/London '
                                                                   'wall-clock time (GMT/BST), from the '
                                                                   'bronze *_LOCAL companion.',
                                            'surgery_start_quality': 'Bronze quality flag for surgery_start '
                                                                     '(e.g. milestone vs fallback).',
                                            'surgery_stop': 'Surgery stop as a UTC instant (Millennium).',
                                            'surgical_area_display': 'Surgical area description.',
                                            'surgical_case_number': 'Formatted SurgiNet case number as '
                                                                    'displayed to staff.',
                                            'surgical_specialty_id': 'SurgiNet surgical specialty '
                                                                     '(prsnl_group) id.',
                                            'theatre_case_key': 'SurgiNet SURG_CASE_ID as string; the same '
                                                                'value children carry in theatre_case_key.',
                                            'time_source': 'Which time supplied procedure_start_derived. '
                                                           'INSR_W2 W2.10.',
                                            'wound_class_code': 'Millennium code value for wound class.',
                                            'wound_class_display': 'Wound class description (clean, '
                                                                   'clean-contaminated, ...).'},
                                'quantity_override': True,
                                'table': 'Internal quality-controlled twin of reference_theatre_case: the '
                                         'Gold repairs, nulls and expectations are applied here and the '
                                         'published Gold MV reads this object. Materialized rather than '
                                         'temporary so the published MV can refresh incrementally.'},
 'reference_theatre_case': {'columns': {'_source_row_id': 'Bronze SURG_CASE_ID.',
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
                                                                    'derived in bronze.',
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
                            'quantity_override': False,
                            'table': 'One SurgiNet surgical case with ASA, wound class, anaesthesia, '
                                     'urgency, times and the implant-expected flag. Gold QC twin of the '
                                     'silver product: encounter_id is nulled when the spine lacks it, 1 '
                                     'check is advisory.'}}
