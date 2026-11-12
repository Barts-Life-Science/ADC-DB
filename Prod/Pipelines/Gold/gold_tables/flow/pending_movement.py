"""Gold flow.pending_movement: 2 dataset(s).

- datasets: clinical_pending_movement, _gold_qc_clinical_pending_movement
- moved verbatim from: 7_gold_patient_flow:259, 7_gold_patient_flow:265, 7_gold_patient_flow:275, 7_gold_patient_flow:293

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== clinical_pending_movement ====
CLINICAL_PENDING_MOVEMENT_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_pending_movement.active_record": "COALESCE(record_status = 'active', FALSE)",
    # These patient-flow rows require a resolved person identity.
    "gold.clinical_pending_movement.resolved_identity": "COALESCE(identity_status = 'resolved', FALSE)",
}


CLINICAL_PENDING_MOVEMENT_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.pending_movement.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_pending_movement.key_present": "pending_movement_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_pending_movement.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_pending_movement",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_PENDING_MOVEMENT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_PENDING_MOVEMENT_ADVISORY_RULES)
def gold_clinical_pending_movement():
    """Publish clinical_pending_movement; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_pending_movement")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_pending_movement",
    refresh_policy="auto",
)
def _gold_qc_clinical_pending_movement():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_pending_movement", "pending_movement_key")


RULES = {
    'clinical_pending_movement': GoldRules(
        table='clinical_pending_movement',
        mandatory=CLINICAL_PENDING_MOVEMENT_MANDATORY_RULES,
        advisory=CLINICAL_PENDING_MOVEMENT_ADVISORY_RULES,
        value_rules={'gold.clinical.pending_movement.accommodation_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                               'column': 'accommodation_cd',
                                                                               'companions': [],
                                                                               'match': '`accommodation_cd` '
                                                                                        '= 0 AND '
                                                                                        '`accommodation_display` '
                                                                                        'IS NULL',
                                                                               'table': 'clinical_pending_movement'},
         'gold.clinical.pending_movement.attenddoc_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                         'column': 'attenddoc_id',
                                                                         'companions': [],
                                                                         'match': '`attenddoc_id` = 0',
                                                                         'table': 'clinical_pending_movement'},
         'gold.clinical.pending_movement.disch_disposition_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                                   'column': 'disch_disposition_cd',
                                                                                   'companions': [],
                                                                                   'match': '`disch_disposition_cd` '
                                                                                            '= 0 AND '
                                                                                            '`disch_disposition_display` '
                                                                                            'IS NULL',
                                                                                   'table': 'clinical_pending_movement'},
         'gold.clinical.pending_movement.disch_to_loctn_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                                'column': 'disch_to_loctn_cd',
                                                                                'companions': [],
                                                                                'match': '`disch_to_loctn_cd` '
                                                                                         '= 0 AND '
                                                                                         '`disch_to_loctn_display` '
                                                                                         'IS NULL',
                                                                                'table': 'clinical_pending_movement'},
         'gold.clinical.pending_movement.isolation_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                           'column': 'isolation_cd',
                                                                           'companions': [],
                                                                           'match': '`isolation_cd` = 0 '
                                                                                    'AND '
                                                                                    '`isolation_display` '
                                                                                    'IS NULL',
                                                                           'table': 'clinical_pending_movement'},
         'gold.clinical.pending_movement.med_service_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                             'column': 'med_service_cd',
                                                                             'companions': [],
                                                                             'match': '`med_service_cd` = '
                                                                                      '0 AND '
                                                                                      '`med_service_display` '
                                                                                      'IS NULL',
                                                                             'table': 'clinical_pending_movement'},
         'gold.clinical.pending_movement.pend_bed_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                          'column': 'pend_bed_cd',
                                                                          'companions': [],
                                                                          'match': '`pend_bed_cd` = 0 AND '
                                                                                   '`pend_bed_display` IS '
                                                                                   'NULL',
                                                                          'table': 'clinical_pending_movement'},
         'gold.clinical.pending_movement.pend_building_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                               'column': 'pend_building_cd',
                                                                               'companions': [],
                                                                               'match': '`pend_building_cd` '
                                                                                        '= 0 AND '
                                                                                        '`pend_building_display` '
                                                                                        'IS NULL',
                                                                               'table': 'clinical_pending_movement'},
         'gold.clinical.pending_movement.pend_facility_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                               'column': 'pend_facility_cd',
                                                                               'companions': [],
                                                                               'match': '`pend_facility_cd` '
                                                                                        '= 0 AND '
                                                                                        '`pend_facility_display` '
                                                                                        'IS NULL',
                                                                               'table': 'clinical_pending_movement'},
         'gold.clinical.pending_movement.pend_nurse_unit_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                                 'column': 'pend_nurse_unit_cd',
                                                                                 'companions': [],
                                                                                 'match': '`pend_nurse_unit_cd` '
                                                                                          '= 0 AND '
                                                                                          '`pend_nurse_unit_display` '
                                                                                          'IS NULL',
                                                                                 'table': 'clinical_pending_movement'},
         'gold.clinical.pending_movement.pend_room_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                           'column': 'pend_room_cd',
                                                                           'companions': [],
                                                                           'match': '`pend_room_cd` = 0 '
                                                                                    'AND '
                                                                                    '`pend_room_display` '
                                                                                    'IS NULL',
                                                                           'table': 'clinical_pending_movement'},
         'gold.clinical.pending_movement.pending_priority_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                                  'column': 'pending_priority_cd',
                                                                                  'companions': [],
                                                                                  'match': '`pending_priority_cd` '
                                                                                           '= 0 AND '
                                                                                           '`pending_priority_display` '
                                                                                           'IS NULL',
                                                                                  'table': 'clinical_pending_movement'},
         'gold.clinical.pending_movement.pending_status_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                                'column': 'pending_status_cd',
                                                                                'companions': [],
                                                                                'match': '`pending_status_cd` '
                                                                                         '= 0 AND '
                                                                                         '`pending_status_display` '
                                                                                         'IS NULL',
                                                                                'table': 'clinical_pending_movement'},
         'gold.clinical.pending_movement.transaction_reason_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                                    'column': 'transaction_reason_cd',
                                                                                    'companions': [],
                                                                                    'match': '`transaction_reason_cd` '
                                                                                             '= 0 AND '
                                                                                             '`transaction_reason_display` '
                                                                                             'IS NULL',
                                                                                    'table': 'clinical_pending_movement'}},
        tdx_substitutions=[('transaction_reason', 'anon_transaction_reason')]),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_clinical_pending_movement': {'columns': {'pending_movement_key': 'Key of the admitted '
                                                                            'clinical_pending_movement row.',
                                                    'qc_checked_at': 'loaded_at of the admitted row (the QC '
                                                                     'evidence time).',
                                                    'qc_status': 'passed for every admitted row; advisory '
                                                                 'outcomes are in Lakeflow expectation '
                                                                 'metrics.'},
                                        'quantity_override': False,
                                        'table': 'Compatibility evidence of admitted '
                                                 'clinical_pending_movement rows; advisory outcomes are in '
                                                 'Lakeflow expectation metrics.'},
 'clinical_pending_movement': {'columns': {'accommodation_cd': 'Code value identifying the accommodation '
                                                               'type associated with the pending encounter '
                                                               'action.',
                                           'accommodation_display': 'Display text of the accommodation code '
                                                                    'for the pending patient movement, '
                                                                    'resolved from the Millennium code value '
                                                                    'lookup.',
                                           'accommodation_reason_cd': 'Code value identifying the reason for '
                                                                      'the accommodation associated with the '
                                                                      'pending encounter action.',
                                           'active_ind': 'Indicator showing whether the pending encounter '
                                                         'record is currently active in Cerner Millennium.',
                                           'adc_updt': 'When bronze last wrote the row.',
                                           'alc_decomp_dt_tm': 'Date and time associated with the alternate '
                                                               'level of care decompensation for the pending '
                                                               'encounter action.',
                                           'alc_reason_cd': 'Code value identifying the reason for the '
                                                            'alternate level of care designation.',
                                           'alt_lvl_care_cd': 'Code value identifying the alternate level of '
                                                              'care associated with the pending encounter '
                                                              'action.',
                                           'alt_lvl_care_dt_tm': 'Date and time associated with the '
                                                                 'alternate level of care designation for '
                                                                 'the pending encounter action.',
                                           'anon_context_fingerprint': 'Hash of the contextual inputs the '
                                                                       'redactor used for this row; the '
                                                                       'anonymised text is released only '
                                                                       'while it matches the current '
                                                                       'context.',
                                           'anon_identity_fingerprint': 'Hash of the patient identity '
                                                                        'details the record-driven redactor '
                                                                        'used for this row.',
                                           'anon_processed_at': "When the redactor last processed this row's "
                                                                'text.',
                                           'anon_redaction_count': 'Number of redactions the redactor '
                                                                   'applied to the source text.',
                                           'anon_redactor_version': 'Version of the redactor that produced '
                                                                    'the anonymised text.',
                                           'anon_source_text_sha': 'SHA-256 digest of the text the redactor '
                                                                   'processed; when it no longer matches the '
                                                                   'current input digest the anonymised text '
                                                                   'is stale.',
                                           'anon_status': "Outcome of the anonymisation run for this row's "
                                                          'free text (anonymized when it succeeded).',
                                           'anon_transaction_reason': 'Anonymised version of '
                                                                      'transaction_reason: identifiers '
                                                                      'detected in the free text are '
                                                                      'replaced by placeholders; published '
                                                                      'alongside the source column, which '
                                                                      'gold releases only when anonymisation '
                                                                      'succeeded.',
                                           'attenddoc_id': 'Internal personnel identifier of the attending '
                                                           'clinician associated with the pending encounter '
                                                           'action.',
                                           'beg_effective_dt_tm': 'Date and time from which the pending '
                                                                  'encounter record is effective.',
                                           'disch_disposition_cd': 'Code value identifying the discharge '
                                                                   'disposition recorded for the pending '
                                                                   'encounter action.',
                                           'disch_disposition_display': 'Display text of the discharge '
                                                                        'disposition code for the pending '
                                                                        'movement, resolved from the '
                                                                        'Millennium code value lookup.',
                                           'disch_to_loctn_cd': 'Code value identifying the location the '
                                                                'patient is to be discharged to for the '
                                                                'pending encounter action.',
                                           'disch_to_loctn_display': 'Display text of the discharge-to '
                                                                     'location code for the pending '
                                                                     'movement, resolved from the Millennium '
                                                                     'code value lookup.',
                                           'encntr_pending_id': 'Unique system-generated identifier for each '
                                                                'pending encounter action record in the '
                                                                'ENCNTR_PENDING table.',
                                           'encntr_type_cd': 'Code value identifying the encounter type '
                                                             'associated with the pending encounter action.',
                                           'encntr_type_display': 'Display text of the encounter type code '
                                                                  'associated with the pending movement, '
                                                                  'resolved from the Millennium code value '
                                                                  'lookup.',
                                           'encounter_id': 'Millennium ENCNTR_ID of the encounter the row '
                                                           'belongs to (BIGINT); NULL when not recorded.',
                                           'end_effective_dt_tm': 'Millennium END_EFFECTIVE_DT_TM of the '
                                                                  'pending movement; 2100-12-31 is the '
                                                                  'source marker for a request that is still '
                                                                  'open and is kept on purpose (J14). Use '
                                                                  'end_effective_dt_tm_clean for a '
                                                                  'NULL-for-open version.',
                                           'end_effective_dt_tm_clean': 'Cleaned end-effective date and time '
                                                                        'of the pending encounter movement '
                                                                        'record, derived from the Millennium '
                                                                        'encounter-pending end-effective '
                                                                        'timestamp marking when the record '
                                                                        'ceases to be valid.',
                                           'est_complete_dt_tm': 'Estimated date and time at which the '
                                                                 'pending encounter action is expected to be '
                                                                 'completed.',
                                           'identity_status': 'resolved when person_id is present, '
                                                              'unresolved when absent, not_applicable for '
                                                              'tables without a patient.',
                                           'isolation_cd': 'Code value identifying the isolation requirement '
                                                           'associated with the pending encounter action.',
                                           'isolation_display': 'Display text of the isolation code recorded '
                                                                'for the pending movement, resolved from the '
                                                                'Millennium code value lookup.',
                                           'load_batch_id': 'Load batch label derived from loaded_at '
                                                            '(yyyyMMddHHmmss); tdx-v21 when no load time '
                                                            'exists.',
                                           'loaded_at': 'When the row was last written to bronze (ADC_UPDT), '
                                                        'else its source update time; the stable load clock.',
                                           'med_service_cd': 'Code value identifying the medical service '
                                                             'associated with the pending encounter action.',
                                           'med_service_display': 'Display text of the medical service code '
                                                                  'for the pending movement, resolved from '
                                                                  'the Millennium code value lookup.',
                                           'organization_id': 'Internal Millennium organisation identifier '
                                                              '(facility or client organisation) attributed '
                                                              'to the pending encounter record during '
                                                              'loading, derived via '
                                                              'encounter-to-organisation mapping from the '
                                                              'linked encounter.',
                                           'pend_bed_cd': 'Location code value identifying the bed '
                                                          'associated with the pending encounter action.',
                                           'pend_bed_display': 'Display text of the bed location code for '
                                                               'the pending movement, resolved from the '
                                                               'Millennium code value lookup.',
                                           'pend_building_cd': 'Location code value identifying the building '
                                                               'associated with the pending encounter '
                                                               'action.',
                                           'pend_building_display': 'Display text of the building location '
                                                                    'code for the pending movement, resolved '
                                                                    'from the Millennium code value lookup.',
                                           'pend_facility_cd': 'Location code value identifying the facility '
                                                               'associated with the pending encounter '
                                                               'action.',
                                           'pend_facility_display': 'Display text of the facility location '
                                                                    'code for the pending movement, resolved '
                                                                    'from the Millennium code value lookup.',
                                           'pend_nurse_unit_cd': 'Location code value identifying the nurse '
                                                                 'unit associated with the pending encounter '
                                                                 'action.',
                                           'pend_nurse_unit_display': 'Display text of the nurse unit '
                                                                      'location code for the pending '
                                                                      'movement, resolved from the '
                                                                      'Millennium code value lookup.',
                                           'pend_room_cd': 'Location code value identifying the room '
                                                           'associated with the pending encounter action.',
                                           'pend_room_display': 'Display text of the room location code for '
                                                                'the pending movement, resolved from the '
                                                                'Millennium code value lookup.',
                                           'pending_dt_tm': 'Date and time associated with the pending '
                                                            'encounter action.',
                                           'pending_movement_key': 'Deterministic SHA-256 key of the pending '
                                                                   'movement row (namespace plus native '
                                                                   'source key).',
                                           'pending_priority_cd': 'Code value identifying the priority of '
                                                                  'the pending encounter action.',
                                           'pending_priority_display': 'Display text of the priority code '
                                                                       'assigned to the pending movement, '
                                                                       'resolved from the Millennium code '
                                                                       'value lookup.',
                                           'pending_prsnl_id': 'Identifier of the personnel record '
                                                               'associated with the pending encounter '
                                                               'action.',
                                           'pending_status_cd': 'Code value identifying the status of the '
                                                                'pending encounter action.',
                                           'pending_status_display': 'Display text of the status code of the '
                                                                     'pending movement, resolved from the '
                                                                     'Millennium code value lookup.',
                                           'pending_type_flag': 'Numeric flag indicating the type of pending '
                                                                'encounter action.',
                                           'person_id': 'Millennium PERSON_ID of the patient (identifier); '
                                                        'NULL when the source row names no patient.',
                                           'pipeline_run_id': 'Bronze pipeline run that last wrote the row.',
                                           'prev_est_depart_dt_tm': 'Previously recorded estimated departure '
                                                                    'date and time for the encounter.',
                                           'priority_seq': 'Sequence number indicating the priority ordering '
                                                           'of the pending encounter action.',
                                           'process_status_dt_tm': 'Date and time the process status of the '
                                                                   'pending encounter action was last set.',
                                           'process_status_flag': 'Numeric flag indicating the processing '
                                                                  'status of the pending encounter action.',
                                           'record_status': 'active, or superseded when the row stopped '
                                                            'appearing in the source snapshot '
                                                            '(source_present_ind false).',
                                           'record_status_effective_from': 'Start of the record status; not '
                                                                           'supplied by this source (always '
                                                                           'NULL).',
                                           'record_status_effective_to': 'When a superseded row was first '
                                                                         'detected absent from the source '
                                                                         '(source_absent_detected_ts); NULL '
                                                                         'while active.',
                                           'row_hash': "SHA-256 of the row's payload columns, used by the "
                                                       'bronze MERGE to detect changes.',
                                           'source_absent_detected_ts': 'When bronze first found the row '
                                                                        'absent from the source snapshot; '
                                                                        'NULL while present.',
                                           'source_adc_updt': 'When the source row was landed in raw (raw '
                                                              'ADC_UPDT).',
                                           'source_present_ind': 'True while the row is present in the '
                                                                 'latest full source snapshot.',
                                           'source_row_id': 'Native source row identifier, as text.',
                                           'source_table': 'Bronze or raw source table the row came from.',
                                           'source_update_timestamp': 'Time the source row was landed in the '
                                                                      'raw layer (bronze SOURCE_ADC_UPDT), '
                                                                      'not when it changed in the source '
                                                                      'system; do not use it for recency.',
                                           'specialty_unit_cd': 'Code value identifying the specialty unit '
                                                                'associated with the pending encounter '
                                                                'action.',
                                           'subject_id_system': 'System of subject_key: '
                                                                'urn:cerner:person_id, or nosubject when the '
                                                                'row names no patient.',
                                           'subject_key': 'Deterministic subject key: hash of the Cerner '
                                                          'person id, or of the source row when no person is '
                                                          'recorded.',
                                           'transaction_reason': 'Anonymised free-text or coded reason '
                                                                 'recorded for the pending movement '
                                                                 'transaction, carried forward from the '
                                                                 'upstream anon_transaction_reason field.',
                                           'transaction_reason_cd': 'Code value identifying the reason for '
                                                                    'the transaction associated with the '
                                                                    'pending encounter action.',
                                           'transaction_reason_display': 'Display text of the transaction '
                                                                         'reason code for the pending '
                                                                         'movement, resolved from the '
                                                                         'Millennium code value lookup.',
                                           'trust': 'Name of the NHS Trust to which the record is '
                                                    'attributed, derived during loading via '
                                                    'encounter-to-organisation mapping.'},
                               'quantity_override': False,
                               'table': 'One row per Millennium pending encounter movement (bed request or '
                                        'transfer awaiting completion). A request still open carries the '
                                        'source open-ended marker 2100-12-31 in end_effective_dt_tm, kept on '
                                        'purpose (J14); end_effective_dt_tm_clean holds NULL instead. The '
                                        'workflow narrowed after 2019 (see the coverage register). Coverage: '
                                        'Pending movements in 2025 are about 10% of 2019 at source: the '
                                        'workflow narrowed, the feed did not fail; complete-through month '
                                        'per release: 6_mgmt.data_quality.release_finding (finding_code '
                                        'F2_complete_through).'}}
