"""Gold safety.safety_incident_factor: 2 dataset(s).

- datasets: clinical_safety_incident_factor, _gold_qc_clinical_safety_incident_factor
- moved verbatim from: 8_gold_patient_safety:158, 8_gold_patient_safety:162, 8_gold_patient_safety:172, 8_gold_patient_safety:190

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== clinical_safety_incident_factor ====
CLINICAL_SAFETY_INCIDENT_FACTOR_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_safety_incident_factor.active_record": "COALESCE(record_status = 'active', FALSE)",
}


CLINICAL_SAFETY_INCIDENT_FACTOR_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.safety_incident_factor.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_safety_incident_factor.key_present": "safety_incident_factor_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_safety_incident_factor.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_safety_incident_factor",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_SAFETY_INCIDENT_FACTOR_MANDATORY_RULES)
@dp.expect_all(CLINICAL_SAFETY_INCIDENT_FACTOR_ADVISORY_RULES)
def gold_clinical_safety_incident_factor():
    """Publish clinical_safety_incident_factor; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_safety_incident_factor")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_safety_incident_factor",
    refresh_policy="auto",
)
def _gold_qc_clinical_safety_incident_factor():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_safety_incident_factor", "safety_incident_factor_key")


RULES = {
    'clinical_safety_incident_factor': GoldRules(
        table='clinical_safety_incident_factor',
        mandatory=CLINICAL_SAFETY_INCIDENT_FACTOR_MANDATORY_RULES,
        advisory=CLINICAL_SAFETY_INCIDENT_FACTOR_ADVISORY_RULES,
        value_rules={'gold.clinical.safety_incident_factor.factor_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                           'column': 'factor_target_code',
                                                                                           'companions': ['factor_target_display'],
                                                                                           'match': '`factor_target_system` '
                                                                                                    '= '
                                                                                                    "'http://snomed.info/sct' "
                                                                                                    'AND '
                                                                                                    '`factor_target_code` '
                                                                                                    'IS '
                                                                                                    'NOT '
                                                                                                    'NULL '
                                                                                                    'AND '
                                                                                                    '(NOT '
                                                                                                    '`factor_target_code` '
                                                                                                    'RLIKE '
                                                                                                    "'^[1-9][0-9]{5,17}$' "
                                                                                                    'OR '
                                                                                                    'substring(`factor_target_code`, '
                                                                                                    'length(`factor_target_code`) '
                                                                                                    '- 2, '
                                                                                                    '2) '
                                                                                                    'NOT '
                                                                                                    'IN '
                                                                                                    "('00', "
                                                                                                    "'10') "
                                                                                                    'OR '
                                                                                                    '(substring(`factor_target_code`, '
                                                                                                    'length(`factor_target_code`) '
                                                                                                    '- 2, '
                                                                                                    '2) = '
                                                                                                    "'10' "
                                                                                                    'AND '
                                                                                                    'length(`factor_target_code`) '
                                                                                                    '< '
                                                                                                    '11))',
                                                                                           'table': 'clinical_safety_incident_factor'}}),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_clinical_safety_incident_factor': {'columns': {'qc_checked_at': 'loaded_at of the admitted row '
                                                                           '(the QC evidence time).',
                                                          'qc_status': 'passed for every admitted row; '
                                                                       'advisory outcomes are in Lakeflow '
                                                                       'expectation metrics.',
                                                          'safety_incident_factor_key': 'Key of the admitted '
                                                                                        'clinical_safety_incident_factor '
                                                                                        'row.'},
                                              'quantity_override': False,
                                              'table': 'Compatibility evidence of admitted '
                                                       'clinical_safety_incident_factor rows; advisory '
                                                       'outcomes are in Lakeflow expectation metrics.'},
 'clinical_safety_incident_factor': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                                 'device_batch_number': 'Batch or lot number of the medical '
                                                                        'device involved in the clinical '
                                                                        'safety incident factor, as recorded '
                                                                        'in the source.',
                                                 'device_equipment': 'Medical device or equipment recorded '
                                                                     'as involved in the safety incident '
                                                                     'factor.',
                                                 'device_involvement_factors': 'Recorded factors describing '
                                                                               'how the medical device was '
                                                                               'involved in the clinical '
                                                                               'safety incident.',
                                                 'device_manufacturer': 'Manufacturer of the medical device '
                                                                        'involved in the safety incident '
                                                                        'factor.',
                                                 'device_model': 'Model of the medical device involved in '
                                                                 'the safety incident factor.',
                                                 'device_serial_number': 'Serial number of the medical '
                                                                         'device involved in the clinical '
                                                                         'safety incident factor, as '
                                                                         'recorded in the source.',
                                                 'device_supplier': 'Supplier of the medical device recorded '
                                                                    'as a factor in the clinical safety '
                                                                    'incident, as captured in the source.',
                                                 'device_type': 'Type or category of the medical device '
                                                                'involved in the safety incident factor.',
                                                 'factor_attributes_json': 'JSON of the raw Datix factor '
                                                                           'attributes (drug, route, dose, '
                                                                           '…) as recorded.',
                                                 'factor_code': 'Code identifying a contributing factor '
                                                                'recorded against a clinical safety '
                                                                'incident, derived from the Datix equipment '
                                                                'or medication drug fields via the incident '
                                                                'factor mapping.',
                                                 'factor_display': 'Display text describing the contributing '
                                                                   'factor recorded against a clinical '
                                                                   'safety incident, derived from the Datix '
                                                                   'equipment or medication drug fields.',
                                                 'factor_map_confidence': 'Numeric confidence score for the '
                                                                          'mapping applied to the clinical '
                                                                          'safety incident factor, populated '
                                                                          'only where a mapping was made.',
                                                 'factor_map_method': 'Method used to map the clinical '
                                                                      'safety incident factor to a target '
                                                                      'concept or code.',
                                                 'factor_map_rule_id': 'Identifier of the mapping rule '
                                                                       'applied to map the clinical safety '
                                                                       'incident factor.',
                                                 'factor_map_version': 'Version of the mapping resource or '
                                                                       'rules used to map the clinical '
                                                                       'safety incident factor.',
                                                 'factor_ordinal': 'Sequence number of the factor within its '
                                                                   'clinical safety incident.',
                                                 'factor_row_id': 'Internal identifier for each incident '
                                                                  'factor row, derived from the Datix '
                                                                  'incident record or LFPSE incident '
                                                                  'identifier.',
                                                 'factor_source_record_id': 'Identifier of the source Datix '
                                                                            'incident or LFPSE record from '
                                                                            'which the incident factor row '
                                                                            'was derived.',
                                                 'factor_source_table': 'Name of the source table from which '
                                                                        'the clinical safety incident factor '
                                                                        'was derived.',
                                                 'factor_target_code': 'SNOMED code the Datix drug text maps '
                                                                       'to via '
                                                                       '3_lookup.omop.drug_name_lexicon; '
                                                                       'null when unmapped.',
                                                 'factor_target_display': 'Display of factor_target_code.',
                                                 'factor_target_system': 'Code system of the mapped '
                                                                         'medication factor (SNOMED CT dm+d '
                                                                         'VTM or Substance).',
                                                 'factor_type': 'Type or category of the contributing factor '
                                                                'recorded for the clinical safety incident.',
                                                 'identity_status': 'resolved when person_id is present, '
                                                                    'unresolved when absent, not_applicable '
                                                                    'for tables without a patient.',
                                                 'inc_id': 'Identifier of the Datix incident record that '
                                                           'this LFPSE row relates to.',
                                                 'lfpse_id': 'Numeric identifier of the associated Learn '
                                                             'From Patient Safety Events (LFPSE) record for '
                                                             'the incident factor, where one exists.',
                                                 'load_batch_id': 'Load batch label derived from loaded_at '
                                                                  '(yyyyMMddHHmmss); tdx-v21 when no load '
                                                                  'time exists.',
                                                 'loaded_at': 'When the row was last written to bronze '
                                                              '(ADC_UPDT), else its source update time; the '
                                                              'stable load clock.',
                                                 'pipeline_run_id': 'Bronze pipeline run that last wrote the '
                                                                    'row.',
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
                                                 'safety_incident_factor_key': 'Deterministic SHA-256 key of '
                                                                               'the safety incident factor '
                                                                               'row (namespace plus native '
                                                                               'source key).',
                                                 'source_absent_detected_ts': 'When bronze first found the '
                                                                              'row absent from the source '
                                                                              'snapshot; NULL while present.',
                                                 'source_adc_updt': 'When the source row was landed in raw '
                                                                    '(raw ADC_UPDT).',
                                                 'source_fields': 'Source field or fields from which the '
                                                                  'clinical safety incident factor was '
                                                                  'extracted.',
                                                 'source_present_ind': 'True while the row is present in the '
                                                                       'latest full source snapshot.',
                                                 'source_row_id': 'Native source row identifier, as text.',
                                                 'source_table': 'Bronze or raw source table the row came '
                                                                 'from.',
                                                 'source_update_timestamp': 'Time the source row was landed '
                                                                            'in the raw layer (bronze '
                                                                            'SOURCE_ADC_UPDT), not when it '
                                                                            'changed in the source system; '
                                                                            'do not use it for recency.'},
                                     'quantity_override': False,
                                     'table': 'One row per contributory factor (medication, device or '
                                              'process) recorded on a Datix incident. Gold QC twin of silver '
                                              'clinical_safety_incident_factor: mandatory rules drop rows, '
                                              'advisory rules are reported in Lakeflow expectation metrics. '
                                              'Coverage: 63% of medication factors map to a drug concept; '
                                              'device factors carry a manufacturer on about 12% and process '
                                              'factors a code on 4%; complete-through month per release: '
                                              '6_mgmt.data_quality.release_finding (finding_code '
                                              'F2_complete_through).'}}
