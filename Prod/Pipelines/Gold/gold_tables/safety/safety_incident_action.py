"""Gold safety.safety_incident_action: 2 dataset(s).

- datasets: clinical_safety_incident_action, _gold_qc_clinical_safety_incident_action
- moved verbatim from: 8_gold_patient_safety:249, 8_gold_patient_safety:253, 8_gold_patient_safety:263, 8_gold_patient_safety:278

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== clinical_safety_incident_action ====
CLINICAL_SAFETY_INCIDENT_ACTION_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_safety_incident_action.active_record": "COALESCE(record_status = 'active', FALSE)",
}


CLINICAL_SAFETY_INCIDENT_ACTION_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.safety_incident_action.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_safety_incident_action.key_present": "safety_incident_action_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_safety_incident_action.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_safety_incident_action",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_SAFETY_INCIDENT_ACTION_MANDATORY_RULES)
@dp.expect_all(CLINICAL_SAFETY_INCIDENT_ACTION_ADVISORY_RULES)
def gold_clinical_safety_incident_action():
    """Publish clinical_safety_incident_action; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_safety_incident_action")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_safety_incident_action",
    refresh_policy="auto",
)
def _gold_qc_clinical_safety_incident_action():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_safety_incident_action", "safety_incident_action_key")


RULES = {
    'clinical_safety_incident_action': GoldRules(
        table='clinical_safety_incident_action',
        mandatory=CLINICAL_SAFETY_INCIDENT_ACTION_MANDATORY_RULES,
        advisory=CLINICAL_SAFETY_INCIDENT_ACTION_ADVISORY_RULES),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_clinical_safety_incident_action': {'columns': {'qc_checked_at': 'loaded_at of the admitted row '
                                                                           '(the QC evidence time).',
                                                          'qc_status': 'passed for every admitted row; '
                                                                       'advisory outcomes are in Lakeflow '
                                                                       'expectation metrics.',
                                                          'safety_incident_action_key': 'Key of the admitted '
                                                                                        'clinical_safety_incident_action '
                                                                                        'row.'},
                                              'quantity_override': False,
                                              'table': 'Compatibility evidence of admitted '
                                                       'clinical_safety_incident_action rows; advisory '
                                                       'outcomes are in Lakeflow expectation metrics.'},
 'clinical_safety_incident_action': {'columns': {'act_by_inits': 'Datix staff initials code of the user who '
                                                                 'completed the action.',
                                                 'act_chain_id': 'Integer identifier of the action chain '
                                                                 'that the action belongs to, where '
                                                                 'applicable.',
                                                 'act_clingroup': 'Code for the clinical group associated '
                                                                  'with the action.',
                                                 'act_cost': 'Cost estimate recorded for the action.',
                                                 'act_cost_max': 'Maximum cost estimate recorded for the '
                                                                 'action.',
                                                 'act_cost_min': 'Minimum cost estimate recorded for the '
                                                                 'action.',
                                                 'act_cost_type': 'Code indicating the type of cost recorded '
                                                                  'for the action.',
                                                 'act_ddone': 'Date on which the action was recorded as '
                                                              'done.',
                                                 'act_ddue': 'Date by which the action is due to be '
                                                             'completed.',
                                                 'act_directorate': 'Code for the directorate associated '
                                                                    'with the action.',
                                                 'act_dstart': 'Start date of the action.',
                                                 'act_from_inits': 'Datix staff initials code of the user '
                                                                   'who assigned the action.',
                                                 'act_locactual': 'Code for the specific location associated '
                                                                  'with the action.',
                                                 'act_loctype': 'Code for the type of location associated '
                                                                'with the action.',
                                                 'act_organisation': 'Code for the organisation associated '
                                                                     'with the action.',
                                                 'act_priority': 'Priority assigned to the action.',
                                                 'act_score': 'Integer score recorded against the action; '
                                                              'its scoring scheme is not documented in the '
                                                              'available metadata.',
                                                 'act_specialty': 'Code for the specialty associated with '
                                                                  'the action.',
                                                 'act_step_no': 'Step number of the action within its action '
                                                                'chain.',
                                                 'act_to_inits': 'Datix staff initials code of the user to '
                                                                 'whom the action is assigned.',
                                                 'act_type': 'Code indicating the type of action.',
                                                 'act_unit': 'Code for the site or unit associated with the '
                                                             'action.',
                                                 'action_id': 'Identifier of the safety incident action '
                                                              'record, sourced from the RECORDID of the '
                                                              'Datix action record.',
                                                 'adc_updt': 'When bronze last wrote the row.',
                                                 'identity_status': 'resolved when person_id is present, '
                                                                    'unresolved when absent, not_applicable '
                                                                    'for tables without a patient.',
                                                 'inc_id': 'Identifier of the safety incident to which the '
                                                           'action relates, sourced from the incident '
                                                           'identifier in the incident action mapping and '
                                                           'the case identifier on Datix action records.',
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
                                                 'safety_incident_action_key': 'Deterministic SHA-256 key of '
                                                                               'the safety incident action '
                                                                               'row (namespace plus native '
                                                                               'source key).',
                                                 'secgroup': 'Datix security group associated with the '
                                                             'action record.',
                                                 'seclevel': 'Datix security level applied to the action '
                                                             'record.',
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
                                                 'source_updt_dt_tm': 'Last-update time of the row in the '
                                                                      'source system (for Datix, '
                                                                      'updateddate); NULL when the source '
                                                                      'supplies none.',
                                                 'updateddate': 'Date and time the action record was last '
                                                                'updated in Datix.'},
                                     'quantity_override': False,
                                     'table': 'One row per action recorded against a Datix incident, keyed '
                                              'by RECORDID. Gold QC twin of silver '
                                              'clinical_safety_incident_action: mandatory rules drop rows, '
                                              'advisory rules are reported in Lakeflow expectation metrics.'}}
