"""Gold flow.location_unit: 2 dataset(s).

- datasets: reference_location_unit, _gold_qc_reference_location_unit
- moved verbatim from: 7_gold_patient_flow:438, 7_gold_patient_flow:442, 7_gold_patient_flow:452, 7_gold_patient_flow:467

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== reference_location_unit ====
REFERENCE_LOCATION_UNIT_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.reference_location_unit.active_record": "COALESCE(record_status = 'active', FALSE)",
}


REFERENCE_LOCATION_UNIT_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.reference.location_unit.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.reference_location_unit.key_present": "location_unit_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.reference_location_unit.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.reference_location_unit",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(REFERENCE_LOCATION_UNIT_MANDATORY_RULES)
@dp.expect_all(REFERENCE_LOCATION_UNIT_ADVISORY_RULES)
def gold_reference_location_unit():
    """Publish reference_location_unit; native rules count exclusions and advisory failures."""
    return _tdx_product("reference_location_unit")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_reference_location_unit",
    refresh_policy="auto",
)
def _gold_qc_reference_location_unit():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("reference_location_unit", "location_unit_key")


RULES = {
    'reference_location_unit': GoldRules(
        table='reference_location_unit',
        mandatory=REFERENCE_LOCATION_UNIT_MANDATORY_RULES,
        advisory=REFERENCE_LOCATION_UNIT_ADVISORY_RULES,
        tdx_value_repairs={'end_effective_dt_tm': "CAST(`end_effective_dt_tm` AS DATE) >= DATE'2099-12-31' AND "
                                'YEAR(CAST(`end_effective_dt_tm` AS DATE)) < 9999'},
        value_rules={'gold.reference.location_unit.current_bed_status_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                                  'column': 'current_bed_status_cd',
                                                                                  'companions': [],
                                                                                  'match': '`current_bed_status_cd` '
                                                                                           '= 0 AND '
                                                                                           '`current_bed_status_display` '
                                                                                           'IS NULL',
                                                                                  'table': 'reference_location_unit'}}),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_reference_location_unit': {'columns': {'location_unit_key': 'Key of the admitted '
                                                                       'reference_location_unit row.',
                                                  'qc_checked_at': 'loaded_at of the admitted row (the QC '
                                                                   'evidence time).',
                                                  'qc_status': 'passed for every admitted row; advisory '
                                                               'outcomes are in Lakeflow expectation '
                                                               'metrics.'},
                                      'quantity_override': False,
                                      'table': 'Compatibility evidence of admitted reference_location_unit '
                                               'rows; advisory outcomes are in Lakeflow expectation '
                                               'metrics.'},
 'reference_location_unit': {'columns': {'active_ind': 'Indicator of whether this bed record is currently '
                                                       'active.',
                                         'adc_updt': 'When bronze last wrote the row.',
                                         'beg_effective_dt_tm': 'Date and time from which this bed record is '
                                                                'effective.',
                                         'building_cd': 'Millennium code value identifying the building to '
                                                        'which the location unit belongs, as sourced from '
                                                        'the Millennium location hierarchy.',
                                         'class_cd': 'Millennium code value for the class assigned to the '
                                                     'room.',
                                         'current_bed_status_cd': 'Code value indicating the current status '
                                                                  'of the bed location, as sourced from the '
                                                                  'Millennium bed table.',
                                         'current_bed_status_display': 'Display text of the current bed '
                                                                       'status for the location unit, taken '
                                                                       'from the Millennium code value '
                                                                       'lookup.',
                                         'dup_bed_ind': 'Indicator flagging whether this bed is a duplicate '
                                                        'bed.',
                                         'end_effective_dt_tm': 'Date and time at which this bed record '
                                                                'ceases to be effective.',
                                         'facility_cd': 'Millennium code value identifying the facility to '
                                                        'which the location unit belongs, as sourced from '
                                                        'the Millennium location hierarchy.',
                                         'fixed_bed_ind': 'Indicator showing whether the room is flagged as '
                                                          'having fixed beds.',
                                         'identity_status': 'resolved when person_id is present, unresolved '
                                                            'when absent, not_applicable for tables without '
                                                            'a patient.',
                                         'load_batch_id': 'Load batch label derived from loaded_at '
                                                          '(yyyyMMddHHmmss); tdx-v21 when no load time '
                                                          'exists.',
                                         'loaded_at': 'When the row was last written to bronze (ADC_UPDT), '
                                                      'else its source update time; the stable load clock.',
                                         'location_code': 'Millennium code value uniquely identifying the '
                                                          'location unit.',
                                         'location_display': 'Display name of the location unit, taken from '
                                                             'the Millennium code value lookup.',
                                         'location_level': 'Hierarchy level or type of the location unit, '
                                                           'derived from the Millennium code value CDF '
                                                           'meaning.',
                                         'location_unit_key': 'Deterministic SHA-256 key of the location '
                                                              'unit row (namespace plus native source key).',
                                         'nurse_unit_cd': 'Millennium code value identifying the nurse unit '
                                                          '(ward) associated with the location unit, as '
                                                          'sourced from the Millennium location hierarchy.',
                                         'nurse_unit_display': 'Display name of the nurse unit (ward) '
                                                               'associated with the location, taken from the '
                                                               'Millennium code value lookup.',
                                         'parent_location_cd': 'Millennium code value of the location '
                                                               'directly above this location unit in the '
                                                               'Millennium location hierarchy, such as the '
                                                               'room that contains a bed.',
                                         'parent_location_display': 'Display name of the parent location in '
                                                                    'the location hierarchy, taken from the '
                                                                    'Millennium code value lookup.',
                                         'pipeline_run_id': 'Bronze pipeline run that last wrote the row.',
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
                                                                      'absent from the source snapshot; NULL '
                                                                      'while present.',
                                         'source_adc_updt': 'When the source row was landed in raw (raw '
                                                            'ADC_UPDT).',
                                         'source_present_ind': 'True while the row is present in the latest '
                                                               'full source snapshot.',
                                         'source_row_id': 'Native source row identifier, as text.',
                                         'source_table': 'Bronze or raw source table the row came from.',
                                         'source_update_timestamp': 'Time the location row was landed in the '
                                                                    'raw layer (bronze SOURCE_ADC_UPDT), not '
                                                                    'when it changed in Millennium; do not '
                                                                    'use it for recency.'},
                             'quantity_override': False,
                             'table': 'One row per Millennium location (bed, room, unit or ward), keyed by '
                                      'LOCATION_CD. Gold QC twin of silver reference_location_unit: '
                                      'mandatory rules drop rows, advisory rules are reported in Lakeflow '
                                      'expectation metrics.'}}
