"""Gold flow.appointment_action: 2 dataset(s).

- datasets: clinical_appointment_action, _gold_qc_clinical_appointment_action
- moved verbatim from: 7_gold_patient_flow:307, 7_gold_patient_flow:313, 7_gold_patient_flow:323, 7_gold_patient_flow:338

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== clinical_appointment_action ====
CLINICAL_APPOINTMENT_ACTION_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_appointment_action.active_record": "COALESCE(record_status = 'active', FALSE)",
    # These patient-flow rows require a resolved person identity.
    "gold.clinical_appointment_action.resolved_identity": "COALESCE(identity_status = 'resolved', FALSE)",
}


CLINICAL_APPOINTMENT_ACTION_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.appointment_action.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_appointment_action.key_present": "appointment_action_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_appointment_action.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_appointment_action",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_APPOINTMENT_ACTION_MANDATORY_RULES)
@dp.expect_all(CLINICAL_APPOINTMENT_ACTION_ADVISORY_RULES)
def gold_clinical_appointment_action():
    """Publish clinical_appointment_action; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_appointment_action")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_appointment_action",
    refresh_policy="auto",
)
def _gold_qc_clinical_appointment_action():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_appointment_action", "appointment_action_key")


RULES = {
    'clinical_appointment_action': GoldRules(
        table='clinical_appointment_action',
        mandatory=CLINICAL_APPOINTMENT_ACTION_MANDATORY_RULES,
        advisory=CLINICAL_APPOINTMENT_ACTION_ADVISORY_RULES,
        value_rules={'gold.clinical.appointment_action.organization_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                              'column': 'organization_id',
                                                                              'companions': [],
                                                                              'match': '`organization_id` '
                                                                                       '= 0',
                                                                              'table': 'clinical_appointment_action'},
         'gold.clinical.appointment_action.req_action_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                            'column': 'req_action_id',
                                                                            'companions': [],
                                                                            'match': '`req_action_id` = 0',
                                                                            'table': 'clinical_appointment_action'},
         'gold.clinical.appointment_action.resource_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                            'column': 'resource_cd',
                                                                            'companions': [],
                                                                            'match': '`resource_cd` = 0 '
                                                                                     'AND '
                                                                                     '`resource_display` '
                                                                                     'IS NULL',
                                                                            'table': 'clinical_appointment_action'},
         'gold.clinical.appointment_action.schedule_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                          'column': 'schedule_id',
                                                                          'companions': [],
                                                                          'match': '`schedule_id` = 0',
                                                                          'table': 'clinical_appointment_action'}}),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_clinical_appointment_action': {'columns': {'appointment_action_key': 'Key of the admitted '
                                                                                'clinical_appointment_action '
                                                                                'row.',
                                                      'qc_checked_at': 'loaded_at of the admitted row (the '
                                                                       'QC evidence time).',
                                                      'qc_status': 'passed for every admitted row; advisory '
                                                                   'outcomes are in Lakeflow expectation '
                                                                   'metrics.'},
                                          'quantity_override': False,
                                          'table': 'Compatibility evidence of admitted '
                                                   'clinical_appointment_action rows; advisory outcomes are '
                                                   'in Lakeflow expectation metrics.'},
 'clinical_appointment_action': {'columns': {'action_dt_tm': 'Date and time the scheduling action was taken.',
                                             'action_meaning': 'Meaning string corresponding to the '
                                                               'scheduling action code.',
                                             'action_prsnl_id': 'Identifier of the personnel member who '
                                                                'performed the scheduling action.',
                                             'action_source_cd': 'Code value identifying the source from '
                                                                 'which the scheduling action originated.',
                                             'action_source_display': 'Display text describing the source of '
                                                                      'the appointment action, resolved from '
                                                                      'a Millennium code value.',
                                             'action_system_ind': 'True when the source action_prsnl_id was '
                                                                  '1, the Cerner system user (J5); the id is '
                                                                  'then published as NULL. NULL when the '
                                                                  'source id was NULL. INSR_W2.',
                                             'active_ind': 'Indicator showing whether the scheduling event '
                                                           'action record is active.',
                                             'adc_updt': 'When bronze last wrote the row.',
                                             'appointment_action_key': 'Deterministic SHA-256 key of the '
                                                                       'appointment action row (namespace '
                                                                       'plus native source key).',
                                             'appointment_patient_event_id': 'Identifier of the scheduling '
                                                                             'appointment event that this '
                                                                             'appointment action relates to, '
                                                                             'sourced from the Millennium '
                                                                             'SCH_EVENT_ID field.',
                                             'appt_synonym_cd': 'Code value identifying the appointment '
                                                                'synonym associated with the scheduling '
                                                                'action.',
                                             'appt_synonym_display': 'Display text of the appointment type '
                                                                     'synonym associated with the '
                                                                     'appointment action, resolved from a '
                                                                     'Millennium code value.',
                                             'breach_offset_days': 'Number of days of breach offset recorded '
                                                                   'for this scheduling event action.',
                                             'contact_follow_up_dt_tm': 'Date and time recorded for a '
                                                                        'follow-up of a patient contact '
                                                                        'associated with the scheduling '
                                                                        'action.',
                                             'contact_outcome_cd': 'Code value identifying the recorded '
                                                                   'outcome of a patient contact associated '
                                                                   'with the scheduling action.',
                                             'contact_outcome_display': 'Display text of the contact outcome '
                                                                        'recorded for the appointment '
                                                                        'action, resolved from a Millennium '
                                                                        'code value.',
                                             'encounter_id': 'Millennium ENCNTR_ID of the encounter the row '
                                                             'belongs to (BIGINT); NULL when not recorded.',
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
                                             'organization_id': 'Identifier of the organization associated '
                                                                'with this scheduling event action, '
                                                                'populated with a resolved organization '
                                                                'during trust mapping.',
                                             'orig_action_id': 'Identifier of the original scheduling action '
                                                               'from which this action was derived. Original '
                                                               'action this action amends or undoes; 0 means '
                                                               'it is itself an original action (kept as 0 '
                                                               'by design).',
                                             'patient_deceased_ind': 'Indicator flagging whether the patient '
                                                                     'associated with the scheduling event '
                                                                     'was recorded as deceased at the time '
                                                                     'of the action.',
                                             'perform_dt_tm': 'Date and time the scheduling action was '
                                                              'performed.',
                                             'person_id': 'Millennium PERSON_ID of the patient (identifier); '
                                                          'NULL when the source row names no patient.',
                                             'pipeline_run_id': 'Bronze pipeline run that last wrote the '
                                                                'row.',
                                             'reason_meaning': 'Meaning string corresponding to the '
                                                               'scheduling action reason code.',
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
                                             'req_action_id': 'Identifier of the request action linked to '
                                                              'this scheduling event action.',
                                             'req_action_meaning': 'Cerner code meaning text corresponding '
                                                                   'to REQ_ACTION_CD for the requested '
                                                                   'action.',
                                             'resource_cd': 'Code value identifying the scheduling resource '
                                                            'associated with this scheduling event action.',
                                             'resource_display': 'Display text of the scheduling resource '
                                                                 'associated with the appointment action, '
                                                                 'resolved from a Millennium code value.',
                                             'row_hash': "SHA-256 of the row's payload columns, used by the "
                                                         'bronze MERGE to detect changes.',
                                             'sch_action_cd': 'Code value identifying the type of scheduling '
                                                              'action performed on the event.',
                                             'sch_action_display': 'Display text of the scheduling action '
                                                                   'performed on the appointment, resolved '
                                                                   'from a Millennium code value.',
                                             'sch_action_id': 'Internal surrogate identifier uniquely '
                                                              'identifying a scheduling event action record.',
                                             'sch_event_id': 'Identifier of the scheduling event '
                                                             '(appointment) to which this action applies.',
                                             'sch_reason_cd': 'Code value identifying the reason recorded '
                                                              'for the scheduling action.',
                                             'sch_reason_display': 'Display text of the scheduling reason '
                                                                   'recorded for the appointment action, '
                                                                   'resolved from a Millennium code value.',
                                             'schedule_id': 'Identifier of the schedule associated with the '
                                                            'scheduling event action.',
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
                                             'subject_id_system': 'System of subject_key: '
                                                                  'urn:cerner:person_id, or nosubject when '
                                                                  'the row names no patient.',
                                             'subject_key': 'Deterministic subject key: hash of the Cerner '
                                                            'person id, or of the source row when no person '
                                                            'is recorded.',
                                             'trust': 'Name of the NHS trust to which the record has been '
                                                      'attributed by the ingestion trust-resolution process.',
                                             'ver_status_meaning': 'Cerner code meaning text corresponding '
                                                                   'to VER_STATUS_CD for the verification '
                                                                   'status.'},
                                 'quantity_override': False,
                                 'table': 'One row per Millennium scheduling action on an appointment (book, '
                                          'confirm, check in, cancel, undo), keyed by SCH_ACTION_ID. Gold QC '
                                          'twin of silver clinical_appointment_action: mandatory rules drop '
                                          'rows, advisory rules are reported in Lakeflow expectation '
                                          'metrics.'}}
