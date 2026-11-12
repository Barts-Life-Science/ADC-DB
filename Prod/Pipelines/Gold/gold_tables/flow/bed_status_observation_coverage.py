"""Gold flow.bed_status_observation_coverage: 2 dataset(s).

- datasets: clinical_bed_status_observation_coverage, _gold_qc_clinical_bed_status_observation_coverage
- moved verbatim from: 7_gold_patient_flow:395, 7_gold_patient_flow:399, 7_gold_patient_flow:409, 7_gold_patient_flow:424

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== clinical_bed_status_observation_coverage ====
CLINICAL_BED_STATUS_OBSERVATION_COVERAGE_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_bed_status_observation_coverage.active_record": "COALESCE(record_status = 'active', FALSE)",
}


CLINICAL_BED_STATUS_OBSERVATION_COVERAGE_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.bed_status_observation_coverage.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_bed_status_observation_coverage.key_present": "bed_status_coverage_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_bed_status_observation_coverage.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_bed_status_observation_coverage",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_BED_STATUS_OBSERVATION_COVERAGE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_BED_STATUS_OBSERVATION_COVERAGE_ADVISORY_RULES)
def gold_clinical_bed_status_observation_coverage():
    """Publish clinical_bed_status_observation_coverage; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_bed_status_observation_coverage")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_bed_status_observation_coverage",
    refresh_policy="auto",
)
def _gold_qc_clinical_bed_status_observation_coverage():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_bed_status_observation_coverage", "bed_status_coverage_key")


RULES = {
    'clinical_bed_status_observation_coverage': GoldRules(
        table='clinical_bed_status_observation_coverage',
        mandatory=CLINICAL_BED_STATUS_OBSERVATION_COVERAGE_MANDATORY_RULES,
        advisory=CLINICAL_BED_STATUS_OBSERVATION_COVERAGE_ADVISORY_RULES),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_clinical_bed_status_observation_coverage': {'columns': {'bed_status_coverage_key': 'Key of the '
                                                                                              'admitted '
                                                                                              'clinical_bed_status_observation_coverage '
                                                                                              'row.',
                                                                   'qc_checked_at': 'loaded_at of the '
                                                                                    'admitted row (the QC '
                                                                                    'evidence time).',
                                                                   'qc_status': 'passed for every admitted '
                                                                                'row; advisory outcomes are '
                                                                                'in Lakeflow expectation '
                                                                                'metrics.'},
                                                       'quantity_override': False,
                                                       'table': 'Compatibility evidence of admitted '
                                                                'clinical_bed_status_observation_coverage '
                                                                'rows; advisory outcomes are in Lakeflow '
                                                                'expectation metrics.'},
 'clinical_bed_status_observation_coverage': {'columns': {'bed_status_coverage_key': 'Deterministic SHA-256 '
                                                                                     'key of the bed status '
                                                                                     'coverage row '
                                                                                     '(namespace plus native '
                                                                                     'source key).',
                                                          'beds_active': 'Count of beds considered active '
                                                                         'for the bed status observation '
                                                                         'run, sourced from the bed '
                                                                         'observation run record.',
                                                          'beds_observed': 'Count of beds for which a status '
                                                                           'observation was captured in the '
                                                                           'observation run.',
                                                          'identity_status': 'resolved when person_id is '
                                                                             'present, unresolved when '
                                                                             'absent, not_applicable for '
                                                                             'tables without a patient.',
                                                          'load_batch_id': 'Load batch label derived from '
                                                                           'loaded_at (yyyyMMddHHmmss); '
                                                                           'tdx-v21 when no load time '
                                                                           'exists.',
                                                          'loaded_at': 'When the row was last written to '
                                                                       'bronze (ADC_UPDT), else its source '
                                                                       'update time; the stable load clock.',
                                                          'observation_date': 'Calendar date to which the '
                                                                              'bed status observation run '
                                                                              'applies.',
                                                          'observed_at': 'Timestamp at which the bed status '
                                                                         'observation run was performed.',
                                                          'pipeline_run_id': 'Bronze pipeline run that last '
                                                                             'wrote the row.',
                                                          'reason': 'Explanation recorded with the '
                                                                    'observation run status, sourced from '
                                                                    'the bed observation run record.',
                                                          'record_status': 'active, or superseded when the '
                                                                           'row stopped appearing in the '
                                                                           'source snapshot '
                                                                           '(source_present_ind false).',
                                                          'record_status_effective_from': 'Start of the '
                                                                                          'record status; '
                                                                                          'not supplied by '
                                                                                          'this source '
                                                                                          '(always NULL).',
                                                          'record_status_effective_to': 'When a superseded '
                                                                                        'row was first '
                                                                                        'detected absent '
                                                                                        'from the source '
                                                                                        '(source_absent_detected_ts); '
                                                                                        'NULL while active.',
                                                          'source_delta_version': 'Delta version of the raw '
                                                                                  'bed table read by the '
                                                                                  'weekly bed-observation '
                                                                                  'run.',
                                                          'status': 'Outcome status of the bed status '
                                                                    'observation run, sourced from the bed '
                                                                    'observation run record.'},
                                              'quantity_override': False,
                                              'table': 'One row per weekly bed-observation run date, '
                                                       'recording whether the snapshot was taken. Gold QC '
                                                       'twin of silver '
                                                       'clinical_bed_status_observation_coverage: mandatory '
                                                       'rules drop rows, advisory rules are reported in '
                                                       'Lakeflow expectation metrics.'}}
