"""Gold flow.prearrival: 2 dataset(s).

- datasets: clinical_prearrival, _gold_qc_clinical_prearrival
- moved verbatim from: 7_gold_patient_flow:214, 7_gold_patient_flow:220, 7_gold_patient_flow:230, 7_gold_patient_flow:245

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== clinical_prearrival ====
CLINICAL_PREARRIVAL_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_prearrival.active_record": "COALESCE(record_status = 'active', FALSE)",
    # These patient-flow rows require a resolved person identity.
    "gold.clinical_prearrival.resolved_identity": "COALESCE(identity_status = 'resolved', FALSE)",
}


CLINICAL_PREARRIVAL_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.prearrival.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_prearrival.key_present": "prearrival_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_prearrival.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_prearrival",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_PREARRIVAL_MANDATORY_RULES)
@dp.expect_all(CLINICAL_PREARRIVAL_ADVISORY_RULES)
def gold_clinical_prearrival():
    """Publish clinical_prearrival; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_prearrival")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_prearrival",
    refresh_policy="auto",
)
def _gold_qc_clinical_prearrival():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_prearrival", "prearrival_key")


RULES = {
    'clinical_prearrival': GoldRules(
        table='clinical_prearrival',
        mandatory=CLINICAL_PREARRIVAL_MANDATORY_RULES,
        advisory=CLINICAL_PREARRIVAL_ADVISORY_RULES,
        tdx_value_repairs={'birth_dt_tm': "CAST(`birth_dt_tm` AS DATE) = DATE'1900-01-01'"},
        value_rules={'gold.clinical.prearrival.pcp_provider_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                      'column': 'pcp_provider_id',
                                                                      'companions': [],
                                                                      'match': '`pcp_provider_id` = 0',
                                                                      'table': 'clinical_prearrival'},
         'gold.clinical.prearrival.prearrival_type_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                           'column': 'prearrival_type_cd',
                                                                           'companions': [],
                                                                           'match': '`prearrival_type_cd` '
                                                                                    '= 0 AND '
                                                                                    '`prearrival_type_display` '
                                                                                    'IS NULL',
                                                                           'table': 'clinical_prearrival'},
         'gold.clinical.prearrival.referring_source_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                          'column': 'referring_source_id',
                                                                          'companions': [],
                                                                          'match': '`referring_source_id` '
                                                                                   '= 0',
                                                                          'table': 'clinical_prearrival'},
         'gold.clinical.prearrival.reg_prsnl_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                   'column': 'reg_prsnl_id',
                                                                   'companions': [],
                                                                   'match': '`reg_prsnl_id` = 0',
                                                                   'table': 'clinical_prearrival'},
         'gold.clinical.prearrival.sex_cd.insr_f7_zero_code': {'action': 'normalize',
                                                               'column': 'sex_cd',
                                                               'companions': [],
                                                               'match': '`sex_cd` = 0 AND `sex_display` IS '
                                                                        'NULL',
                                                               'table': 'clinical_prearrival'}},
        tdx_substitutions=[('chief_complaint', 'anon_chief_complaint')]),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_clinical_prearrival': {'columns': {'prearrival_key': 'Key of the admitted clinical_prearrival '
                                                                'row.',
                                              'qc_checked_at': 'loaded_at of the admitted row (the QC '
                                                               'evidence time).',
                                              'qc_status': 'passed for every admitted row; advisory outcomes '
                                                           'are in Lakeflow expectation metrics.'},
                                  'quantity_override': False,
                                  'table': 'Compatibility evidence of admitted clinical_prearrival rows; '
                                           'advisory outcomes are in Lakeflow expectation metrics.'},
 'clinical_prearrival': {'columns': {'active_ind': 'Indicator showing whether the pre-arrival record is '
                                                   'active.',
                                     'adc_updt': 'When bronze last wrote the row.',
                                     'age_txt': 'Free-text age of the expected patient as recorded at '
                                                'pre-arrival.',
                                     'anon_chief_complaint': 'Anonymised version of chief_complaint: '
                                                             'identifiers detected in the free text are '
                                                             'replaced by placeholders; published alongside '
                                                             'the source column, which gold releases only '
                                                             'when anonymisation succeeded.',
                                     'anon_context_fingerprint': 'Hash of the contextual inputs the redactor '
                                                                 'used for this row; the anonymised text is '
                                                                 'released only while it matches the current '
                                                                 'context.',
                                     'anon_identity_fingerprint': 'Hash of the patient identity details the '
                                                                  'record-driven redactor used for this row.',
                                     'anon_processed_at': "When the redactor last processed this row's text.",
                                     'anon_redaction_count': 'Number of redactions the redactor applied to '
                                                             'the source text.',
                                     'anon_redactor_version': 'Version of the redactor that produced the '
                                                              'anonymised text.',
                                     'anon_source_text_sha': 'SHA-256 digest of the text the redactor '
                                                             'processed; when it no longer matches the '
                                                             'current input digest the anonymised text is '
                                                             'stale.',
                                     'anon_status': "Outcome of the anonymisation run for this row's free "
                                                    'text (anonymized when it succeeded).',
                                     'attached_encntr_id': 'Identifier of the encounter to which the '
                                                           'pre-arrival record was later attached.',
                                     'attached_person_id': 'Identifier of the person to whom the pre-arrival '
                                                           'record was later attached.',
                                     'birth_dt_tm': 'Date and time of birth recorded for the expected '
                                                    'patient.',
                                     'chief_complaint': "Chief complaint recorded for the patient's "
                                                        'pre-arrival record, derived from the anonymised '
                                                        'chief complaint text in upstream pre-arrival '
                                                        'sources.',
                                     'create_dt_tm': 'Date and time the pre-arrival record was created.',
                                     'encounter_id': 'Millennium ENCNTR_ID of the encounter the row belongs '
                                                     'to (BIGINT); NULL when not recorded.',
                                     'estimated_arrive_dt_tm': 'Estimated date and time of the expected '
                                                               "patient's arrival.",
                                     'first_name': 'Free-text first name entered for the expected patient on '
                                                   'the pre-arrival record.',
                                     'identity_status': 'resolved when person_id is present, unresolved when '
                                                        'absent, not_applicable for tables without a '
                                                        'patient.',
                                     'last_name': 'Free-text last name entered for the expected patient on '
                                                  'the pre-arrival record.',
                                     'linkage_route': 'How the row was linked to its person or encounter.',
                                     'load_batch_id': 'Load batch label derived from loaded_at '
                                                      '(yyyyMMddHHmmss); tdx-v21 when no load time exists.',
                                     'loaded_at': 'When the row was last written to bronze (ADC_UPDT), else '
                                                  'its source update time; the stable load clock.',
                                     'organization_id': 'Pipeline-added internal organisation identifier '
                                                        '(facility or client organisation of the attached '
                                                        'encounter) used to map the record to a trust.',
                                     'pcp_provider_id': "Identifier of the expected patient's primary care "
                                                        'provider recorded on the pre-arrival record.',
                                     'pcp_provider_name': "Name of the expected patient's primary care "
                                                          'provider recorded on the pre-arrival record.',
                                     'person_id': 'Millennium PERSON_ID of the patient (identifier); NULL '
                                                  'when the source row names no patient.',
                                     'pipeline_run_id': 'Bronze pipeline run that last wrote the row.',
                                     'prearrival_key': 'Deterministic SHA-256 key of the prearrival row '
                                                       '(namespace plus native source key).',
                                     'prearrival_type_cd': 'Code value identifying the type of pre-arrival '
                                                           'record.',
                                     'prearrival_type_display': 'Display text describing the type of '
                                                                'pre-arrival notification, resolved from the '
                                                                'Millennium code value lookup.',
                                     'record_status': 'active, or superseded when the row stopped appearing '
                                                      'in the source snapshot (source_present_ind false).',
                                     'record_status_effective_from': 'Start of the record status; not '
                                                                     'supplied by this source (always NULL).',
                                     'record_status_effective_to': 'When a superseded row was first detected '
                                                                   'absent from the source '
                                                                   '(source_absent_detected_ts); NULL while '
                                                                   'active.',
                                     'referring_source_id': 'Identifier of the referring source associated '
                                                            'with the pre-arrival record.',
                                     'referring_source_name': 'Name of the referring source recorded on the '
                                                              'pre-arrival record.',
                                     'reg_prsnl_id': 'Identifier of the personnel member who registered the '
                                                     'pre-arrival record.',
                                     'reg_prsnl_name': 'Name of the personnel member who registered the '
                                                       'pre-arrival record.',
                                     'row_hash': "SHA-256 of the row's payload columns, used by the bronze "
                                                 'MERGE to detect changes.',
                                     'sex_cd': 'Code value for the recorded sex of the expected patient.',
                                     'sex_display': 'Display text for the sex recorded on the pre-arrival '
                                                    'record, resolved from the Millennium code value lookup.',
                                     'source_absent_detected_ts': 'When bronze first found the row absent '
                                                                  'from the source snapshot; NULL while '
                                                                  'present.',
                                     'source_adc_updt': 'When the source row was landed in raw (raw '
                                                        'ADC_UPDT).',
                                     'source_present_ind': 'True while the row is present in the latest full '
                                                           'source snapshot.',
                                     'source_row_id': 'Native source row identifier, as text.',
                                     'source_table': 'Bronze or raw source table the row came from.',
                                     'source_update_timestamp': 'Time the source row was landed in the raw '
                                                                'layer (bronze SOURCE_ADC_UPDT), not when it '
                                                                'changed in the source system; do not use it '
                                                                'for recency.',
                                     'subject_id_system': 'System of subject_key: urn:cerner:person_id, or '
                                                          'nosubject when the row names no patient.',
                                     'subject_key': 'Deterministic subject key: hash of the Cerner person '
                                                    'id, or of the source row when no person is recorded.',
                                     'tracking_group_cd': 'Code value identifying the tracking group to '
                                                          'which the pre-arrival record is assigned.',
                                     'tracking_group_display': 'Display text for the tracking group '
                                                               'associated with the pre-arrival record, '
                                                               'resolved from the Millennium code value '
                                                               'lookup.',
                                     'tracking_prearrival_id': 'Internal Millennium surrogate identifier '
                                                               'uniquely identifying each pre-arrival '
                                                               'tracking record.',
                                     'trust': 'Pipeline-derived NHS trust name attributed to the pre-arrival '
                                              'record via organisation-to-trust mapping, null where no '
                                              'mapping was resolved.'},
                         'quantity_override': False,
                         'table': 'One row per FirstNet pre-arrival notice (expected patient), keyed by '
                                  'TRACKING_PREARRIVAL_ID. Gold QC twin of silver clinical_prearrival: '
                                  'mandatory rules drop rows, advisory rules are reported in Lakeflow '
                                  'expectation metrics.'}}
