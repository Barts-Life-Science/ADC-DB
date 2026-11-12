"""Gold flow.location_attribute_history: 2 dataset(s).

- datasets: reference_location_attribute_history, _gold_qc_reference_location_attribute_history
- moved verbatim from: 7_gold_patient_flow:481, 7_gold_patient_flow:485, 7_gold_patient_flow:495, 7_gold_patient_flow:510

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== reference_location_attribute_history ====
REFERENCE_LOCATION_ATTRIBUTE_HISTORY_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.reference_location_attribute_history.active_record": "COALESCE(record_status = 'active', FALSE)",
}


REFERENCE_LOCATION_ATTRIBUTE_HISTORY_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.reference.location_attribute_history.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.reference_location_attribute_history.key_present": "location_attribute_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.reference_location_attribute_history.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.reference_location_attribute_history",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(REFERENCE_LOCATION_ATTRIBUTE_HISTORY_MANDATORY_RULES)
@dp.expect_all(REFERENCE_LOCATION_ATTRIBUTE_HISTORY_ADVISORY_RULES)
def gold_reference_location_attribute_history():
    """Publish reference_location_attribute_history; native rules count exclusions and advisory failures."""
    return _tdx_product("reference_location_attribute_history")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_reference_location_attribute_history",
    refresh_policy="auto",
)
def _gold_qc_reference_location_attribute_history():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("reference_location_attribute_history", "location_attribute_key")


RULES = {
    'reference_location_attribute_history': GoldRules(
        table='reference_location_attribute_history',
        mandatory=REFERENCE_LOCATION_ATTRIBUTE_HISTORY_MANDATORY_RULES,
        advisory=REFERENCE_LOCATION_ATTRIBUTE_HISTORY_ADVISORY_RULES,
        value_rules={'gold.reference.location_attribute_history.value_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                                  'column': 'value_cd',
                                                                                  'companions': [],
                                                                                  'match': '`value_cd` = 0 '
                                                                                           'AND '
                                                                                           '`value_display` '
                                                                                           'IS NULL',
                                                                                  'table': 'reference_location_attribute_history'},
         'gold.reference.location_attribute_history.value_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                'column': 'value_id',
                                                                                'companions': [],
                                                                                'match': '`value_id` = 0',
                                                                                'table': 'reference_location_attribute_history'}},
        tdx_substitutions=[('value_string', 'anon_value_string'), ('description', 'anon_description')]),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_reference_location_attribute_history': {'columns': {'location_attribute_key': 'Key of the '
                                                                                         'admitted '
                                                                                         'reference_location_attribute_history '
                                                                                         'row.',
                                                               'qc_checked_at': 'loaded_at of the admitted '
                                                                                'row (the QC evidence time).',
                                                               'qc_status': 'passed for every admitted row; '
                                                                            'advisory outcomes are in '
                                                                            'Lakeflow expectation metrics.'},
                                                   'quantity_override': False,
                                                   'table': 'Compatibility evidence of admitted '
                                                            'reference_location_attribute_history rows; '
                                                            'advisory outcomes are in Lakeflow expectation '
                                                            'metrics.'},
 'reference_location_attribute_history': {'columns': {'active_ind': 'Indicator showing whether the location '
                                                                    'attribute row is active.',
                                                      'adc_updt': 'When bronze last wrote the row.',
                                                      'anon_context_fingerprint': 'Hash of the contextual '
                                                                                  'inputs the redactor used '
                                                                                  'for this row; the '
                                                                                  'anonymised text is '
                                                                                  'released only while it '
                                                                                  'matches the current '
                                                                                  'context.',
                                                      'anon_description': 'Anonymised version of '
                                                                          'description: identifiers detected '
                                                                          'in the free text are replaced by '
                                                                          'placeholders; published alongside '
                                                                          'the source column, which gold '
                                                                          'releases only when anonymisation '
                                                                          'succeeded.',
                                                      'anon_identity_fingerprint': 'Hash of the patient '
                                                                                   'identity details the '
                                                                                   'record-driven redactor '
                                                                                   'used for this row.',
                                                      'anon_processed_at': 'When the redactor last processed '
                                                                           "this row's text.",
                                                      'anon_redaction_count': 'Number of redactions the '
                                                                              'redactor applied to the '
                                                                              'source text.',
                                                      'anon_redactor_version': 'Version of the redactor that '
                                                                               'produced the anonymised '
                                                                               'text.',
                                                      'anon_source_text_sha': 'SHA-256 digest of the text '
                                                                              'the redactor processed; when '
                                                                              'it no longer matches the '
                                                                              'current input digest the '
                                                                              'anonymised text is stale.',
                                                      'anon_status': 'Outcome of the anonymisation run for '
                                                                     "this row's free text (anonymized when "
                                                                     'it succeeded).',
                                                      'anon_value_string': 'Anonymised version of '
                                                                           'value_string: identifiers '
                                                                           'detected in the free text are '
                                                                           'replaced by placeholders; '
                                                                           'published alongside the source '
                                                                           'column, which gold releases only '
                                                                           'when anonymisation succeeded.',
                                                      'attrib_type_cd': 'Millennium code value identifying '
                                                                        'the type of attribute recorded '
                                                                        'against the location.',
                                                      'attrib_type_display': 'Display text of the location '
                                                                             'attribute type, resolved from '
                                                                             'the Millennium code value '
                                                                             'reference.',
                                                      'attribute_row_id': 'Internal surrogate identifier for '
                                                                          'a location attribute record, '
                                                                          'taken from the Millennium '
                                                                          'location attribute or location '
                                                                          'attribute history row identifier.',
                                                      'beg_effective_dt_tm': 'Date and time from which the '
                                                                             'location attribute row is '
                                                                             'effective.',
                                                      'description': 'Descriptive text for the location '
                                                                     'attribute history record, sourced from '
                                                                     'an anonymised upstream description '
                                                                     'field.',
                                                      'end_effective_dt_tm': 'Date and time until which the '
                                                                             'location attribute row is '
                                                                             'effective, defaulting to a '
                                                                             'far-future date for open '
                                                                             'records.',
                                                      'end_effective_dt_tm_clean': 'Cleaned date and time at '
                                                                                   'which the location '
                                                                                   'attribute record stopped '
                                                                                   'being effective, derived '
                                                                                   'from the Millennium end '
                                                                                   'effective date-time.',
                                                      'hist_action': 'Short code indicating the history '
                                                                     'action recorded for the location '
                                                                     'attribute, such as an addition or '
                                                                     'deletion.',
                                                      'identity_status': 'resolved when person_id is '
                                                                         'present, unresolved when absent, '
                                                                         'not_applicable for tables without '
                                                                         'a patient.',
                                                      'load_batch_id': 'Load batch label derived from '
                                                                       'loaded_at (yyyyMMddHHmmss); tdx-v21 '
                                                                       'when no load time exists.',
                                                      'loaded_at': 'When the row was last written to bronze '
                                                                   '(ADC_UPDT), else its source update time; '
                                                                   'the stable load clock.',
                                                      'location_attribute_key': 'Deterministic SHA-256 key '
                                                                                'of the location attribute '
                                                                                'row (namespace plus native '
                                                                                'source key).',
                                                      'location_cd': 'Millennium code value identifying the '
                                                                     'location to which the attribute '
                                                                     'applies.',
                                                      'location_display': 'Display name of the location to '
                                                                          'which the attribute applies, '
                                                                          'resolved from the Millennium code '
                                                                          'value reference.',
                                                      'pipeline_run_id': 'Bronze pipeline run that last '
                                                                         'wrote the row.',
                                                      'pm_loc_attrib_id': 'Internal surrogate identifier '
                                                                          'uniquely identifying each '
                                                                          'location attribute row in the '
                                                                          'Millennium PM_LOC_ATTRIB table.',
                                                      'record_status': 'active, or superseded when the row '
                                                                       'stopped appearing in the source '
                                                                       'snapshot (source_present_ind false).',
                                                      'record_status_effective_from': 'Start of the record '
                                                                                      'status; not supplied '
                                                                                      'by this source '
                                                                                      '(always NULL).',
                                                      'record_status_effective_to': 'When a superseded row '
                                                                                    'was first detected '
                                                                                    'absent from the source '
                                                                                    '(source_absent_detected_ts); '
                                                                                    'NULL while active.',
                                                      'row_hash': "SHA-256 of the row's payload columns, "
                                                                  'used by the bronze MERGE to detect '
                                                                  'changes.',
                                                      'source_absent_detected_ts': 'When bronze first found '
                                                                                   'the row absent from the '
                                                                                   'source snapshot; NULL '
                                                                                   'while present.',
                                                      'source_adc_updt': 'When the source row was landed in '
                                                                         'raw (raw ADC_UPDT).',
                                                      'source_present_ind': 'True while the row is present '
                                                                            'in the latest full source '
                                                                            'snapshot.',
                                                      'source_row_id': 'Native source row identifier, as '
                                                                       'text.',
                                                      'source_table': 'Bronze or raw source table the row '
                                                                      'came from.',
                                                      'source_update_timestamp': 'Time the attribute row was '
                                                                                 'landed in the raw layer '
                                                                                 '(bronze SOURCE_ADC_UPDT), '
                                                                                 'not when the attribute '
                                                                                 'changed in Millennium; do '
                                                                                 'not use it for recency.',
                                                      'value_cd': 'Millennium code value holding the '
                                                                  'attribute value when the attribute is '
                                                                  'code-typed.',
                                                      'value_display': 'Display text of the location '
                                                                       'attribute value, resolved from the '
                                                                       'Millennium code value reference '
                                                                       'where applicable.',
                                                      'value_dt_tm': 'Date/time attribute value used when '
                                                                     'the attribute is date/time-typed.',
                                                      'value_id': 'Identifier holding the attribute value '
                                                                  'when the attribute is ID-typed.',
                                                      'value_num': 'Numeric attribute value used when the '
                                                                   'attribute is number-typed.',
                                                      'value_string': 'String value recorded for the '
                                                                      'location attribute in this history '
                                                                      'record, sourced from an anonymised '
                                                                      'upstream value field.',
                                                      'value_type': 'Single-character indicator of which '
                                                                    'typed value column (code, ID, string, '
                                                                    'number or date/time) holds the '
                                                                    'attribute value.'},
                                          'quantity_override': False,
                                          'table': 'One row per recorded change of a Millennium location '
                                                   'attribute, keyed by the source attribute row id. Gold QC '
                                                   'twin of silver reference_location_attribute_history: '
                                                   'mandatory rules drop rows, advisory rules are reported '
                                                   'in Lakeflow expectation metrics.'}}
