"""Gold flow.bed_status_observation: 2 dataset(s).

- datasets: clinical_bed_status_observation, _gold_qc_clinical_bed_status_observation
- moved verbatim from: 7_gold_patient_flow:352, 7_gold_patient_flow:356, 7_gold_patient_flow:366, 7_gold_patient_flow:381

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== clinical_bed_status_observation ====
CLINICAL_BED_STATUS_OBSERVATION_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_bed_status_observation.active_record": "COALESCE(record_status = 'active', FALSE)",
}


CLINICAL_BED_STATUS_OBSERVATION_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.bed_status_observation.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_bed_status_observation.key_present": "bed_status_observation_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_bed_status_observation.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_bed_status_observation",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_BED_STATUS_OBSERVATION_MANDATORY_RULES)
@dp.expect_all(CLINICAL_BED_STATUS_OBSERVATION_ADVISORY_RULES)
def gold_clinical_bed_status_observation():
    """Publish clinical_bed_status_observation; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_bed_status_observation")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_bed_status_observation",
    refresh_policy="auto",
)
def _gold_qc_clinical_bed_status_observation():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_bed_status_observation", "bed_status_observation_key")


RULES = {
    'clinical_bed_status_observation': GoldRules(
        table='clinical_bed_status_observation',
        mandatory=CLINICAL_BED_STATUS_OBSERVATION_MANDATORY_RULES,
        advisory=CLINICAL_BED_STATUS_OBSERVATION_ADVISORY_RULES,
        value_rules={'gold.clinical.bed_status_observation.bed_status_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                                  'column': 'bed_status_cd',
                                                                                  'companions': [],
                                                                                  'match': '`bed_status_cd` '
                                                                                           '= 0 AND '
                                                                                           '`bed_status_display` '
                                                                                           'IS NULL',
                                                                                  'table': 'clinical_bed_status_observation'}}),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_clinical_bed_status_observation': {'columns': {'bed_status_observation_key': 'Key of the admitted '
                                                                                        'clinical_bed_status_observation '
                                                                                        'row.',
                                                          'qc_checked_at': 'loaded_at of the admitted row '
                                                                           '(the QC evidence time).',
                                                          'qc_status': 'passed for every admitted row; '
                                                                       'advisory outcomes are in Lakeflow '
                                                                       'expectation metrics.'},
                                              'quantity_override': False,
                                              'table': 'Compatibility evidence of admitted '
                                                       'clinical_bed_status_observation rows; advisory '
                                                       'outcomes are in Lakeflow expectation metrics.'},
 'clinical_bed_status_observation': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                                 'bed_active_ind': 'Indicator showing whether the source bed '
                                                                   'record is active, derived from the '
                                                                   'Millennium bed table active indicator.',
                                                 'bed_status_cd': 'Code value representing the status of '
                                                                  'this bed.',
                                                 'bed_status_display': 'Display text describing the status '
                                                                       'of the bed at the time of '
                                                                       'observation, derived from the source '
                                                                       'bed status observation and '
                                                                       'Millennium code value lookup.',
                                                 'bed_status_observation_key': 'Deterministic SHA-256 key of '
                                                                               'the bed status observation '
                                                                               'row (namespace plus native '
                                                                               'source key).',
                                                 'bed_updt_dt_tm': 'Date and time the source bed record was '
                                                                   'last updated, taken from the Millennium '
                                                                   'bed table update timestamp.',
                                                 'dup_bed_ind': 'Indicator flagging whether this bed is a '
                                                                'duplicate bed.',
                                                 'identity_status': 'resolved when person_id is present, '
                                                                    'unresolved when absent, not_applicable '
                                                                    'for tables without a patient.',
                                                 'load_batch_id': 'Load batch label derived from loaded_at '
                                                                  '(yyyyMMddHHmmss); tdx-v21 when no load '
                                                                  'time exists.',
                                                 'loaded_at': 'When the row was last written to bronze '
                                                              '(ADC_UPDT), else its source update time; the '
                                                              'stable load clock.',
                                                 'loc_room_cd': 'Code value identifying the parent room '
                                                                'location of this bed.',
                                                 'location_code': 'Code value identifying the location '
                                                                  'associated with the bed, sourced from the '
                                                                  'Millennium bed table location code.',
                                                 'nurse_unit_cd': 'Code identifying the nursing unit (ward) '
                                                                  'associated with the bed status '
                                                                  'observation.',
                                                 'nurse_unit_display': 'Display name of the nursing unit in '
                                                                       'which the observed bed is located, '
                                                                       'derived from source observation, '
                                                                       'location unit mapping and Millennium '
                                                                       'code value lookup.',
                                                 'observation_date': 'Date of the weekly bed-observation run '
                                                                     'that captured this status: the run '
                                                                     'date, or the observation_date job '
                                                                     'parameter when one was given.',
                                                 'observed_at': 'Date and time at which the bed status '
                                                                'observation was recorded, taken from the '
                                                                'observation timestamp in the source bed '
                                                                'status observation.',
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
                                                 'room_location_cd': 'Code identifying the room location '
                                                                     'associated with the bed status '
                                                                     'observation.',
                                                 'row_hash': "SHA-256 of the row's payload columns, used by "
                                                             'the bronze MERGE to detect changes.',
                                                 'source_absent_detected_ts': 'When bronze first found the '
                                                                              'row absent from the source '
                                                                              'snapshot; NULL while present.',
                                                 'source_adc_updt': 'When the source row was landed in raw '
                                                                    '(raw ADC_UPDT).',
                                                 'source_delta_version': 'Delta version of the raw bed table '
                                                                         'read by the weekly bed-observation '
                                                                         'run.',
                                                 'source_last_utc_ts': 'Most recent timestamp, in UTC, '
                                                                       'recorded on the source Millennium '
                                                                       'bed record, carried through '
                                                                       'unchanged from the source.',
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
                                     'table': 'One row per bed location per weekly observation date: the '
                                              "bed's status at the moment the weekly snapshot ran "
                                              '(Bronze_Pipeline_Parallel). No within-week history; join '
                                              'observation dates to clinical_bed_status_observation_coverage '
                                              'to see which weeks were captured.'}}
