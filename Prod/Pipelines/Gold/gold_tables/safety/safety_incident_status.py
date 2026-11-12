"""Gold safety.safety_incident_status: 2 dataset(s).

- datasets: clinical_safety_incident_status, _gold_qc_clinical_safety_incident_status
- moved verbatim from: 8_gold_patient_safety:292, 8_gold_patient_safety:296, 8_gold_patient_safety:306, 8_gold_patient_safety:321

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== clinical_safety_incident_status ====
CLINICAL_SAFETY_INCIDENT_STATUS_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_safety_incident_status.active_record": "COALESCE(record_status = 'active', FALSE)",
}


CLINICAL_SAFETY_INCIDENT_STATUS_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.safety_incident_status.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_safety_incident_status.key_present": "safety_incident_status_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_safety_incident_status.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_safety_incident_status",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_SAFETY_INCIDENT_STATUS_MANDATORY_RULES)
@dp.expect_all(CLINICAL_SAFETY_INCIDENT_STATUS_ADVISORY_RULES)
def gold_clinical_safety_incident_status():
    """Publish clinical_safety_incident_status; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_safety_incident_status")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_safety_incident_status",
    refresh_policy="auto",
)
def _gold_qc_clinical_safety_incident_status():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_safety_incident_status", "safety_incident_status_key")


RULES = {
    'clinical_safety_incident_status': GoldRules(
        table='clinical_safety_incident_status',
        mandatory=CLINICAL_SAFETY_INCIDENT_STATUS_MANDATORY_RULES,
        advisory=CLINICAL_SAFETY_INCIDENT_STATUS_ADVISORY_RULES),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_clinical_safety_incident_status': {'columns': {'qc_checked_at': 'loaded_at of the admitted row '
                                                                           '(the QC evidence time).',
                                                          'qc_status': 'passed for every admitted row; '
                                                                       'advisory outcomes are in Lakeflow '
                                                                       'expectation metrics.',
                                                          'safety_incident_status_key': 'Key of the admitted '
                                                                                        'clinical_safety_incident_status '
                                                                                        'row.'},
                                              'quantity_override': False,
                                              'table': 'Compatibility evidence of admitted '
                                                       'clinical_safety_incident_status rows; advisory '
                                                       'outcomes are in Lakeflow expectation metrics.'},
 'clinical_safety_incident_status': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                                 'identity_status': 'resolved when person_id is present, '
                                                                    'unresolved when absent, not_applicable '
                                                                    'for tables without a patient.',
                                                 'inc_id': 'Identifier of the clinical safety incident to '
                                                           'which this status audit record relates, sourced '
                                                           'from the Datix incident record ID.',
                                                 'load_batch_id': 'Load batch label derived from loaded_at '
                                                                  '(yyyyMMddHHmmss); tdx-v21 when no load '
                                                                  'time exists.',
                                                 'loaded_at': 'When the row was last written to bronze '
                                                              '(ADC_UPDT), else its source update time; the '
                                                              'stable load clock.',
                                                 'login': 'Login of the user who recorded or changed the '
                                                          'incident status in Datix.',
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
                                                 'safety_incident_status_key': 'Deterministic SHA-256 key of '
                                                                               'the safety incident status '
                                                                               'row (namespace plus native '
                                                                               'source key).',
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
                                                 'status': 'Status value assigned to the safety incident at '
                                                           'this point in its audit history.',
                                                 'status_dt_tm': 'Date and time at which the incident status '
                                                                 'was recorded.',
                                                 'status_row_id': 'Derived identifier for an individual '
                                                                  'incident status audit row, built from the '
                                                                  'status date and user login.',
                                                 'status_sequence': 'Order of this status change within the '
                                                                    "incident's status history, derived from "
                                                                    'the status date and login.'},
                                     'quantity_override': False,
                                     'table': 'One row per status change of a Datix incident. Gold QC twin '
                                              'of silver clinical_safety_incident_status: mandatory rules '
                                              'drop rows, advisory rules are reported in Lakeflow '
                                              'expectation metrics.'}}
