"""Gold flow.tracking_attendance: 2 dataset(s).

- datasets: clinical_tracking_attendance, _gold_qc_clinical_tracking_attendance
- moved verbatim from: 7_gold_patient_flow:102, 7_gold_patient_flow:71, 7_gold_patient_flow:77, 7_gold_patient_flow:87

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== clinical_tracking_attendance ====
CLINICAL_TRACKING_ATTENDANCE_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_tracking_attendance.active_record": "COALESCE(record_status = 'active', FALSE)",
    # These patient-flow rows require a resolved person identity.
    "gold.clinical_tracking_attendance.resolved_identity": "COALESCE(identity_status = 'resolved', FALSE)",
}


CLINICAL_TRACKING_ATTENDANCE_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.tracking_attendance.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_tracking_attendance.key_present": "tracking_attendance_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_tracking_attendance.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_tracking_attendance",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_TRACKING_ATTENDANCE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_TRACKING_ATTENDANCE_ADVISORY_RULES)
def gold_clinical_tracking_attendance():
    """Publish clinical_tracking_attendance; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_tracking_attendance")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_tracking_attendance",
    refresh_policy="auto",
)
def _gold_qc_clinical_tracking_attendance():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_tracking_attendance", "tracking_attendance_key")


RULES = {
    'clinical_tracking_attendance': GoldRules(
        table='clinical_tracking_attendance',
        mandatory=CLINICAL_TRACKING_ATTENDANCE_MANDATORY_RULES,
        advisory=CLINICAL_TRACKING_ATTENDANCE_ADVISORY_RULES,
        value_rules={'gold.clinical.tracking_attendance.checkin_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                          'column': 'checkin_id',
                                                                          'companions': [],
                                                                          'match': '`checkin_id` = 0',
                                                                          'table': 'clinical_tracking_attendance'},
         'gold.clinical.tracking_attendance.checkout_disposition_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                                         'column': 'checkout_disposition_cd',
                                                                                         'companions': [],
                                                                                         'match': '`checkout_disposition_cd` '
                                                                                                  '= 0 AND '
                                                                                                  '`checkout_disposition_display` '
                                                                                                  'IS NULL',
                                                                                         'table': 'clinical_tracking_attendance'},
         'gold.clinical.tracking_attendance.checkout_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                           'column': 'checkout_id',
                                                                           'companions': [],
                                                                           'match': '`checkout_id` = 0',
                                                                           'table': 'clinical_tracking_attendance'},
         'gold.clinical.tracking_attendance.family_present_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                                   'column': 'family_present_cd',
                                                                                   'companions': [],
                                                                                   'match': '`family_present_cd` '
                                                                                            '= 0 AND '
                                                                                            '`family_present_display` '
                                                                                            'IS NULL',
                                                                                   'table': 'clinical_tracking_attendance'},
         'gold.clinical.tracking_attendance.primary_doc_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                              'column': 'primary_doc_id',
                                                                              'companions': [],
                                                                              'match': '`primary_doc_id` = '
                                                                                       '0',
                                                                              'table': 'clinical_tracking_attendance'},
         'gold.clinical.tracking_attendance.primary_nurse_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                'column': 'primary_nurse_id',
                                                                                'companions': [],
                                                                                'match': '`primary_nurse_id` '
                                                                                         '= 0',
                                                                                'table': 'clinical_tracking_attendance'},
         'gold.clinical.tracking_attendance.reactivate_user_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                  'column': 'reactivate_user_id',
                                                                                  'companions': [],
                                                                                  'match': '`reactivate_user_id` '
                                                                                           '= 0',
                                                                                  'table': 'clinical_tracking_attendance'},
         'gold.clinical.tracking_attendance.registration_status_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                      'column': 'registration_status_id',
                                                                                      'companions': [],
                                                                                      'match': '`registration_status_id` '
                                                                                               '= 0',
                                                                                      'table': 'clinical_tracking_attendance'},
         'gold.clinical.tracking_attendance.secondary_doc_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                'column': 'secondary_doc_id',
                                                                                'companions': [],
                                                                                'match': '`secondary_doc_id` '
                                                                                         '= 0',
                                                                                'table': 'clinical_tracking_attendance'},
         'gold.clinical.tracking_attendance.secondary_nurse_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                  'column': 'secondary_nurse_id',
                                                                                  'companions': [],
                                                                                  'match': '`secondary_nurse_id` '
                                                                                           '= 0',
                                                                                  'table': 'clinical_tracking_attendance'},
         'gold.clinical.tracking_attendance.specialty_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                            'column': 'specialty_id',
                                                                            'companions': [],
                                                                            'match': '`specialty_id` = 0',
                                                                            'table': 'clinical_tracking_attendance'},
         'gold.clinical.tracking_attendance.team_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                       'column': 'team_id',
                                                                       'companions': [],
                                                                       'match': '`team_id` = 0',
                                                                       'table': 'clinical_tracking_attendance'}}),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_clinical_tracking_attendance': {'columns': {'qc_checked_at': 'loaded_at of the admitted row (the '
                                                                        'QC evidence time).',
                                                       'qc_status': 'passed for every admitted row; advisory '
                                                                    'outcomes are in Lakeflow expectation '
                                                                    'metrics.',
                                                       'tracking_attendance_key': 'Key of the admitted '
                                                                                  'clinical_tracking_attendance '
                                                                                  'row.'},
                                           'quantity_override': False,
                                           'table': 'Compatibility evidence of admitted '
                                                    'clinical_tracking_attendance rows; advisory outcomes '
                                                    'are in Lakeflow expectation metrics.'},
 'clinical_tracking_attendance': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                              'attendance_open_ind': 'Boolean flag showing whether the '
                                                                     'tracking attendance is still open, '
                                                                     'derived from the absence of a '
                                                                     'check-out date and time on the '
                                                                     'underlying Millennium tracking '
                                                                     'check-in record.',
                                              'attendance_sequence': 'Integer ordinal position of the '
                                                                     'tracking attendance, assigned by '
                                                                     'ordering on check-in date and time '
                                                                     'with the tracking check-in identifier '
                                                                     'as tie-breaker.',
                                              'checkin_dt_tm': 'Date and time at which the tracked item was '
                                                               'checked in.',
                                              'checkin_id': 'Identifier of the personnel member who '
                                                            'performed the check-in.',
                                              'checkout_disposition_cd': 'Millennium code value identifying '
                                                                         'the disposition recorded at '
                                                                         'check-out from tracking.',
                                              'checkout_disposition_display': 'Display text for the checkout '
                                                                              'disposition recorded for the '
                                                                              'tracking attendance.',
                                              'checkout_dt_tm': 'Date and time at which the tracked item was '
                                                                'checked out.',
                                              'checkout_dt_tm_clean': 'Cleaned check-out date and time for '
                                                                      'the tracking attendance, derived from '
                                                                      'the Millennium tracking check-in '
                                                                      'check-out timestamp.',
                                              'checkout_id': 'Identifier of the personnel member who '
                                                             'performed the check-out.',
                                              'dwell_minutes': 'Duration of the tracking attendance in '
                                                               'minutes, calculated from check-in date and '
                                                               'time to cleaned check-out date and time.',
                                              'encounter_id': 'Millennium ENCNTR_ID of the encounter the row '
                                                              'belongs to (BIGINT); NULL when not recorded.',
                                              'family_present_cd': 'Code value indicating whether family was '
                                                                   'present for the tracked check-in.',
                                              'family_present_display': 'Display text indicating whether '
                                                                        'family was present during the '
                                                                        'tracking attendance.',
                                              'identity_status': 'resolved when person_id is present, '
                                                                 'unresolved when absent, not_applicable for '
                                                                 'tables without a patient.',
                                              'linkage_route': 'How the row was linked to its person or '
                                                               'encounter.',
                                              'load_batch_id': 'Load batch label derived from loaded_at '
                                                               '(yyyyMMddHHmmss); tdx-v21 when no load time '
                                                               'exists.',
                                              'loaded_at': 'When the row was last written to bronze '
                                                           '(ADC_UPDT), else its source update time; the '
                                                           'stable load clock.',
                                              'organization_id': 'Millennium organisation identifier added '
                                                                 'by the landing process and used to resolve '
                                                                 'Trust attribution.',
                                              'period_end': 'End timestamp of the attendance period, taken '
                                                            'from the cleaned tracking check-out date and '
                                                            'time.',
                                              'period_start': 'Start timestamp of the attendance period, '
                                                              'taken from the tracking check-in date and '
                                                              'time.',
                                              'person_id': 'Millennium PERSON_ID of the patient '
                                                           '(identifier); NULL when the source row names no '
                                                           'patient.',
                                              'pipeline_run_id': 'Bronze pipeline run that last wrote the '
                                                                 'row.',
                                              'primary_doc_id': 'Identifier of the primary doctor assigned '
                                                                'to the tracked check-in.',
                                              'primary_nurse_id': 'Identifier of the primary nurse assigned '
                                                                  'to the tracked check-in.',
                                              'reactivate_system_ind': 'True when the source '
                                                                       'reactivate_user_id was 1, the Cerner '
                                                                       'system user (J5); the id is then '
                                                                       'published as NULL. NULL when the '
                                                                       'source id was NULL. INSR_W2.',
                                              'reactivate_user_id': 'Identifier of the Millennium user who '
                                                                    'reactivated the tracking check-in.',
                                              'reactivation_dt_tm': 'Date and time at which the tracking '
                                                                    'check-in was reactivated.',
                                              'record_status': 'active, or superseded when the row stopped '
                                                               'appearing in the source snapshot '
                                                               '(source_present_ind false).',
                                              'record_status_effective_from': 'Start of the record status; '
                                                                              'not supplied by this source '
                                                                              '(always NULL).',
                                              'record_status_effective_to': 'When a superseded row was first '
                                                                            'detected absent from the source '
                                                                            '(source_absent_detected_ts); '
                                                                            'NULL while active.',
                                              'registration_status_id': 'Identifier of the registration '
                                                                        'status associated with the tracked '
                                                                        'check-in.',
                                              'row_hash': "SHA-256 of the row's payload columns, used by the "
                                                          'bronze MERGE to detect changes.',
                                              'secondary_doc_id': 'Identifier of the secondary doctor '
                                                                  'assigned to the tracked check-in.',
                                              'secondary_nurse_id': 'Identifier of the secondary nurse '
                                                                    'assigned to the tracked check-in.',
                                              'source_absent_detected_ts': 'When bronze first found the row '
                                                                           'absent from the source snapshot; '
                                                                           'NULL while present.',
                                              'source_adc_updt': 'When the source row was landed in raw (raw '
                                                                 'ADC_UPDT).',
                                              'source_present_ind': 'True while the row is present in the '
                                                                    'latest full source snapshot.',
                                              'source_row_id': 'Native source row identifier, as text.',
                                              'source_table': 'Bronze or raw source table the row came from.',
                                              'source_update_timestamp': 'Time the source row was landed in '
                                                                         'the raw layer (bronze '
                                                                         'SOURCE_ADC_UPDT), not when it '
                                                                         'changed in the source system; do '
                                                                         'not use it for recency.',
                                              'specialty_id': 'Identifier of the specialty assigned to the '
                                                              'tracked check-in.',
                                              'subject_id_system': 'System of subject_key: '
                                                                   'urn:cerner:person_id, or nosubject when '
                                                                   'the row names no patient.',
                                              'subject_key': 'Deterministic subject key: hash of the Cerner '
                                                             'person id, or of the source row when no person '
                                                             'is recorded.',
                                              'surg_case_id': 'Identifier of the surgical case associated '
                                                              'with the tracking attendance, carried through '
                                                              'from the silver tracking attendance data and '
                                                              'populated only for a minority of rows.',
                                              'team_id': 'Identifier of the care team assigned to the '
                                                         'tracked check-in.',
                                              'tracking_attendance_key': 'Deterministic SHA-256 key of the '
                                                                         'tracking attendance row (namespace '
                                                                         'plus native source key).',
                                              'tracking_checkin_id': 'Unique Millennium identifier for the '
                                                                     'tracking check-in record and primary '
                                                                     'key of the TRACKING_CHECKIN table.',
                                              'tracking_event_type_cd': 'Code value identifying the type of '
                                                                        'tracking event associated with the '
                                                                        'check-in.',
                                              'tracking_group_cd': 'Code value identifying the tracking '
                                                                   'group in which the check-in was '
                                                                   'recorded.',
                                              'tracking_group_display': 'Display text for the tracking group '
                                                                        'associated with the tracking '
                                                                        'attendance.',
                                              'tracking_id': 'Identifier of the parent tracking record to '
                                                             'which this check-in belongs.',
                                              'trauma_ind': 'Indicator flag recording whether the tracked '
                                                            'check-in is flagged as trauma.',
                                              'trust': 'NHS Trust attributed to the record by the landing '
                                                       'process via organisation-to-trust mapping.'},
                                  'quantity_override': False,
                                  'table': 'One row per FirstNet tracking check-in, keyed by Millennium '
                                           'TRACKING_CHECKIN_ID. Gold QC twin of silver '
                                           'clinical_tracking_attendance: mandatory rules drop rows, '
                                           'advisory rules are reported in Lakeflow expectation metrics.'}}
