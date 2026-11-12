"""Gold flow.tracking_location_stay: 2 dataset(s).

- datasets: clinical_tracking_location_stay, _gold_qc_clinical_tracking_location_stay
- moved verbatim from: 7_gold_patient_flow:116, 7_gold_patient_flow:122, 7_gold_patient_flow:132, 7_gold_patient_flow:150

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== clinical_tracking_location_stay ====
CLINICAL_TRACKING_LOCATION_STAY_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_tracking_location_stay.active_record": "COALESCE(record_status = 'active', FALSE)",
    # These patient-flow rows require a resolved person identity.
    "gold.clinical_tracking_location_stay.resolved_identity": "COALESCE(identity_status = 'resolved', FALSE)",
}


CLINICAL_TRACKING_LOCATION_STAY_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.tracking_location_stay.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_tracking_location_stay.key_present": "tracking_location_stay_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_tracking_location_stay.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_tracking_location_stay",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_TRACKING_LOCATION_STAY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_TRACKING_LOCATION_STAY_ADVISORY_RULES)
def gold_clinical_tracking_location_stay():
    """Publish clinical_tracking_location_stay; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_tracking_location_stay")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_tracking_location_stay",
    refresh_policy="auto",
)
def _gold_qc_clinical_tracking_location_stay():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_tracking_location_stay", "tracking_location_stay_key")


RULES = {
    'clinical_tracking_location_stay': GoldRules(
        table='clinical_tracking_location_stay',
        mandatory=CLINICAL_TRACKING_LOCATION_STAY_MANDATORY_RULES,
        advisory=CLINICAL_TRACKING_LOCATION_STAY_ADVISORY_RULES,
        value_rules={'gold.clinical.tracking_location_stay.loc_bed_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                               'column': 'loc_bed_cd',
                                                                               'companions': [],
                                                                               'match': '`loc_bed_cd` = 0 '
                                                                                        'AND '
                                                                                        '`loc_bed_display` '
                                                                                        'IS NULL',
                                                                               'table': 'clinical_tracking_location_stay'},
         'gold.clinical.tracking_location_stay.loc_room_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                                'column': 'loc_room_cd',
                                                                                'companions': [],
                                                                                'match': '`loc_room_cd` = '
                                                                                         '0 AND '
                                                                                         '`loc_room_display` '
                                                                                         'IS NULL',
                                                                                'table': 'clinical_tracking_location_stay'},
         'gold.clinical.tracking_location_stay.tracking_acuity_level_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                           'column': 'tracking_acuity_level_id',
                                                                                           'companions': [],
                                                                                           'match': '`tracking_acuity_level_id` '
                                                                                                    '= 0',
                                                                                           'table': 'clinical_tracking_location_stay'},
         'gold.clinical.tracking_location_stay.tracking_reason_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                                       'column': 'tracking_reason_cd',
                                                                                       'companions': [],
                                                                                       'match': '`tracking_reason_cd` '
                                                                                                '= 0 AND '
                                                                                                '`tracking_reason_display` '
                                                                                                'IS NULL',
                                                                                       'table': 'clinical_tracking_location_stay'},
         'gold.clinical.tracking_location_stay.unavail_tracking_event_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                            'column': 'unavail_tracking_event_id',
                                                                                            'companions': [],
                                                                                            'match': '`unavail_tracking_event_id` '
                                                                                                     '= 0',
                                                                                            'table': 'clinical_tracking_location_stay'}},
        tdx_substitutions=[('tracking_reason_comment', 'anon_tracking_reason_comment')]),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_clinical_tracking_location_stay': {'columns': {'qc_checked_at': 'loaded_at of the admitted row '
                                                                           '(the QC evidence time).',
                                                          'qc_status': 'passed for every admitted row; '
                                                                       'advisory outcomes are in Lakeflow '
                                                                       'expectation metrics.',
                                                          'tracking_location_stay_key': 'Key of the admitted '
                                                                                        'clinical_tracking_location_stay '
                                                                                        'row.'},
                                              'quantity_override': False,
                                              'table': 'Compatibility evidence of admitted '
                                                       'clinical_tracking_location_stay rows; advisory '
                                                       'outcomes are in Lakeflow expectation metrics.'},
 'clinical_tracking_location_stay': {'columns': {'active_events_ind': 'Indicator flag denoting whether '
                                                                      'active tracking events exist for this '
                                                                      'locator entry.',
                                                 'adc_updt': 'When bronze last wrote the row.',
                                                 'anon_context_fingerprint': 'Hash of the contextual inputs '
                                                                             'the redactor used for this '
                                                                             'row; the anonymised text is '
                                                                             'released only while it matches '
                                                                             'the current context.',
                                                 'anon_identity_fingerprint': 'Hash of the patient identity '
                                                                              'details the record-driven '
                                                                              'redactor used for this row.',
                                                 'anon_processed_at': 'When the redactor last processed this '
                                                                      "row's text.",
                                                 'anon_redaction_count': 'Number of redactions the redactor '
                                                                         'applied to the source text.',
                                                 'anon_redactor_version': 'Version of the redactor that '
                                                                          'produced the anonymised text.',
                                                 'anon_source_text_sha': 'SHA-256 digest of the text the '
                                                                         'redactor processed; when it no '
                                                                         'longer matches the current input '
                                                                         'digest the anonymised text is '
                                                                         'stale.',
                                                 'anon_status': 'Outcome of the anonymisation run for this '
                                                                "row's free text (anonymized when it "
                                                                'succeeded).',
                                                 'anon_tracking_reason_comment': 'Anonymised version of '
                                                                                 'tracking_reason_comment: '
                                                                                 'identifiers detected in '
                                                                                 'the free text are replaced '
                                                                                 'by placeholders; published '
                                                                                 'alongside the source '
                                                                                 'column, which gold '
                                                                                 'releases only when '
                                                                                 'anonymisation succeeded.',
                                                 'arrive_dt_tm': 'Date and time of arrival at the location '
                                                                 'recorded on this tracking locator entry.',
                                                 'depart_dt_tm': 'Date and time of departure from the '
                                                                 'location recorded on this tracking locator '
                                                                 'entry.',
                                                 'depart_dt_tm_clean': 'Cleaned date and time the tracked '
                                                                       'patient departed the tracking '
                                                                       'location, derived from the '
                                                                       'Millennium tracking locator '
                                                                       'departure timestamp.',
                                                 'encounter_id': 'Millennium ENCNTR_ID of the encounter the '
                                                                 'row belongs to (BIGINT); NULL when not '
                                                                 'recorded.',
                                                 'identity_status': 'resolved when person_id is present, '
                                                                    'unresolved when absent, not_applicable '
                                                                    'for tables without a patient.',
                                                 'linkage_route': 'How the row was linked to its person or '
                                                                  'encounter.',
                                                 'load_batch_id': 'Load batch label derived from loaded_at '
                                                                  '(yyyyMMddHHmmss); tdx-v21 when no load '
                                                                  'time exists.',
                                                 'loaded_at': 'When the row was last written to bronze '
                                                              '(ADC_UPDT), else its source update time; the '
                                                              'stable load clock.',
                                                 'loc_bed_cd': 'Millennium code value identifying the bed '
                                                               'location for this locator entry.',
                                                 'loc_bed_display': 'Display name of the bed associated with '
                                                                    'the tracked location stay, resolved '
                                                                    'from the Millennium code value lookup.',
                                                 'loc_nurse_unit_cd': 'Millennium code value identifying the '
                                                                      'nurse unit location for this locator '
                                                                      'entry.',
                                                 'loc_nurse_unit_display': 'Display name of the nurse unit '
                                                                           '(ward) associated with the '
                                                                           'tracked location stay, resolved '
                                                                           'from the Millennium code value '
                                                                           'lookup.',
                                                 'loc_room_cd': 'Millennium code value identifying the room '
                                                                'location for this locator entry.',
                                                 'loc_room_display': 'Display name of the room associated '
                                                                     'with the tracked location stay, '
                                                                     'resolved from the Millennium code '
                                                                     'value lookup.',
                                                 'location_cd': 'Millennium code value identifying the '
                                                                'tracked location for this locator entry.',
                                                 'location_display': 'Display name of the tracked location '
                                                                     'for the location stay, resolved from '
                                                                     'the Millennium code value lookup.',
                                                 'organization_id': 'Pipeline-added Millennium organisation '
                                                                    'identifier resolved during landing and '
                                                                    'used to map the row to a Trust.',
                                                 'period_end': 'End timestamp of the tracking location stay '
                                                               'period, taken from the cleaned departure '
                                                               'date and time.',
                                                 'period_start': 'Start timestamp of the tracking location '
                                                                 'stay period, taken from the arrival date '
                                                                 'and time at the tracking location.',
                                                 'person_id': 'Millennium PERSON_ID of the patient '
                                                              '(identifier); NULL when the source row names '
                                                              'no patient.',
                                                 'pipeline_run_id': 'Bronze pipeline run that last wrote the '
                                                                    'row.',
                                                 'rank_sequence': 'Numeric sequence indicating the rank or '
                                                                  'ordering of this locator entry.',
                                                 'record_status': 'active, or superseded when the row '
                                                                  'stopped appearing in the source snapshot '
                                                                  '(source_present_ind false).',
                                                 'record_status_effective_from': 'Start of the record '
                                                                                 'status; not supplied by '
                                                                                 'this source (always NULL).',
                                                 'record_status_effective_to': 'When a superseded row was '
                                                                               'first detected absent from '
                                                                               'the source '
                                                                               '(source_absent_detected_ts); '
                                                                               'NULL while active.',
                                                 'row_hash': "SHA-256 of the row's payload columns, used by "
                                                             'the bronze MERGE to detect changes.',
                                                 'scheduled_dt_tm': 'Date and time at which the tracked item '
                                                                    'was scheduled to be at this location.',
                                                 'source_absent_detected_ts': 'When bronze first found the '
                                                                              'row absent from the source '
                                                                              'snapshot; NULL while present.',
                                                 'source_adc_updt': 'When the source row was landed in raw '
                                                                    '(raw ADC_UPDT).',
                                                 'source_present_ind': 'True while the row is present in the '
                                                                       'latest full source snapshot.',
                                                 'source_row_id': 'Native source row identifier, as text.',
                                                 'source_table': 'Bronze or raw source table the row came '
                                                                 'from.',
                                                 'source_update_timestamp': 'Time the source row was landed '
                                                                            'in the raw layer (bronze '
                                                                            'SOURCE_ADC_UPDT), not when it '
                                                                            'changed in the source system; '
                                                                            'do not use it for recency.',
                                                 'stay_minutes': 'Duration of the tracking location stay in '
                                                                 'minutes, calculated from the arrival and '
                                                                 'cleaned departure timestamps.',
                                                 'stay_open_ind': 'Boolean flag indicating whether the '
                                                                  'tracking location stay is still open, '
                                                                  'derived from the absence or state of the '
                                                                  'departure timestamp.',
                                                 'subject_id_system': 'System of subject_key: '
                                                                      'urn:cerner:person_id, or nosubject '
                                                                      'when the row names no patient.',
                                                 'subject_key': 'Deterministic subject key: hash of the '
                                                                'Cerner person id, or of the source row when '
                                                                'no person is recorded.',
                                                 'surg_case_id': 'Identifier of the surgical case associated '
                                                                 'with this tracking location stay, sourced '
                                                                 'from the tracking location stay and '
                                                                 'tracking episode records.',
                                                 'tracking_acuity_level_id': 'Internal Millennium identifier '
                                                                             'of the tracking acuity level '
                                                                             'associated with this locator '
                                                                             'entry.',
                                                 'tracking_id': 'Internal Millennium identifier of the '
                                                                'tracking record to which this locator entry '
                                                                'belongs.',
                                                 'tracking_location_stay_key': 'Deterministic SHA-256 key of '
                                                                               'the tracking location stay '
                                                                               'row (namespace plus native '
                                                                               'source key).',
                                                 'tracking_locator_id': 'Internal Millennium identifier that '
                                                                        'uniquely identifies each tracking '
                                                                        'locator entry.',
                                                 'tracking_reason_cd': 'Millennium code value for the '
                                                                       'tracking reason recorded on this '
                                                                       'locator entry.',
                                                 'tracking_reason_comment': 'Anonymised free-text comment '
                                                                            'recording the reason associated '
                                                                            'with the tracking location '
                                                                            'stay.',
                                                 'tracking_reason_display': 'Display text of the reason '
                                                                            'recorded for the tracking '
                                                                            'location stay, resolved from '
                                                                            'the Millennium code value '
                                                                            'lookup.',
                                                 'trust': 'Pipeline-added NHS Trust attribution for the row, '
                                                          'derived during landing through organisation and '
                                                          'encounter mapping.',
                                                 'unavail_tracking_event_id': 'Internal Millennium '
                                                                              'identifier of the tracking '
                                                                              'event associated with '
                                                                              'unavailability for this '
                                                                              'locator entry.'},
                                     'quantity_override': False,
                                     'table': 'One row per FirstNet tracking location stay (arrival to '
                                              'departure at one tracked location), keyed by '
                                              'TRACKING_LOCATOR_ID. Gold QC twin of silver '
                                              'clinical_tracking_location_stay: mandatory rules drop rows, '
                                              'advisory rules are reported in Lakeflow expectation metrics. '
                                              'Coverage: Tracking base-location times are thin for '
                                              'April-June 2024 (0.24 of the neighbouring months at source); '
                                              'complete-through month per release: '
                                              '6_mgmt.data_quality.release_finding (finding_code '
                                              'F2_complete_through).'}}
