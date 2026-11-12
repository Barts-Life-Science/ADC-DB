"""Gold flow.tracking_episode: 2 dataset(s).

- datasets: clinical_tracking_episode, _gold_qc_clinical_tracking_episode
- moved verbatim from: 7_gold_patient_flow:26, 7_gold_patient_flow:32, 7_gold_patient_flow:42, 7_gold_patient_flow:57

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== clinical_tracking_episode ====
CLINICAL_TRACKING_EPISODE_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_tracking_episode.active_record": "COALESCE(record_status = 'active', FALSE)",
    # These patient-flow rows require a resolved person identity.
    "gold.clinical_tracking_episode.resolved_identity": "COALESCE(identity_status = 'resolved', FALSE)",
}


CLINICAL_TRACKING_EPISODE_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.tracking_episode.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_tracking_episode.key_present": "tracking_episode_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_tracking_episode.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_tracking_episode",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_TRACKING_EPISODE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_TRACKING_EPISODE_ADVISORY_RULES)
def gold_clinical_tracking_episode():
    """Publish clinical_tracking_episode; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_tracking_episode")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_tracking_episode",
    refresh_policy="auto",
)
def _gold_qc_clinical_tracking_episode():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_tracking_episode", "tracking_episode_key")


RULES = {
    'clinical_tracking_episode': GoldRules(
        table='clinical_tracking_episode',
        mandatory=CLINICAL_TRACKING_EPISODE_MANDATORY_RULES,
        advisory=CLINICAL_TRACKING_EPISODE_ADVISORY_RULES,
        value_rules={'gold.clinical.tracking_episode.base_loc_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                          'column': 'base_loc_cd',
                                                                          'companions': [],
                                                                          'match': '`base_loc_cd` = 0 AND '
                                                                                   '`base_loc_display` IS '
                                                                                   'NULL',
                                                                          'table': 'clinical_tracking_episode'},
         'gold.clinical.tracking_episode.cur_tracking_locator_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                    'column': 'cur_tracking_locator_id',
                                                                                    'companions': [],
                                                                                    'match': '`cur_tracking_locator_id` '
                                                                                             '= 0',
                                                                                    'table': 'clinical_tracking_episode'},
         'gold.clinical.tracking_episode.parent_entity_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                             'column': 'parent_entity_id',
                                                                             'companions': [],
                                                                             'match': '`parent_entity_id` '
                                                                                      '= 0',
                                                                             'table': 'clinical_tracking_episode'}}),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_clinical_tracking_episode': {'columns': {'qc_checked_at': 'loaded_at of the admitted row (the QC '
                                                                     'evidence time).',
                                                    'qc_status': 'passed for every admitted row; advisory '
                                                                 'outcomes are in Lakeflow expectation '
                                                                 'metrics.',
                                                    'tracking_episode_key': 'Key of the admitted '
                                                                            'clinical_tracking_episode row.'},
                                        'quantity_override': False,
                                        'table': 'Compatibility evidence of admitted '
                                                 'clinical_tracking_episode rows; advisory outcomes are in '
                                                 'Lakeflow expectation metrics.'},
 'clinical_tracking_episode': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                           'base_loc_cd': 'Millennium location code value for the base '
                                                          'location of the tracking item.',
                                           'base_loc_display': 'Display name of the base location associated '
                                                               'with the clinical tracking episode, resolved '
                                                               'from the Millennium code value lookup.',
                                           'base_loc_dt_tm': 'Date and time associated with the base '
                                                             'location assignment of the tracking item.',
                                           'cur_tracking_locator_id': 'Identifier of the current tracking '
                                                                      'locator record for the tracking item.',
                                           'encounter_id': 'Millennium ENCNTR_ID of the encounter the row '
                                                           'belongs to (BIGINT); NULL when not recorded.',
                                           'end_tracking_dt_tm': 'Date and time at which tracking of the '
                                                                 'item ended.',
                                           'end_tracking_dt_tm_clean': 'Cleaned date and time at which '
                                                                       'tracking of the item ended, derived '
                                                                       'from the Millennium tracking item '
                                                                       'end tracking timestamp.',
                                           'identity_status': 'resolved when person_id is present, '
                                                              'unresolved when absent, not_applicable for '
                                                              'tables without a patient.',
                                           'item_active_ind': 'Boolean indicator showing whether the '
                                                              'underlying Millennium tracking item record is '
                                                              'active.',
                                           'linkage_route': 'How the row was linked to its person or '
                                                            'encounter.',
                                           'load_batch_id': 'Load batch label derived from loaded_at '
                                                            '(yyyyMMddHHmmss); tdx-v21 when no load time '
                                                            'exists.',
                                           'loaded_at': 'When the row was last written to bronze (ADC_UPDT), '
                                                        'else its source update time; the stable load clock.',
                                           'organization_id': 'Millennium organisation surrogate identifier '
                                                              'resolved during ingestion, partly via '
                                                              'encounter data, and used to derive the Trust.',
                                           'parent_entity_id': 'Identifier of the parent entity record, in '
                                                               'the table named by PARENT_ENTITY_NAME, '
                                                               'associated with the tracking item.',
                                           'parent_entity_name': 'Name of the parent entity table referenced '
                                                                 'by PARENT_ENTITY_ID for the tracking item.',
                                           'person_id': 'Millennium person identifier for the person '
                                                        'associated with the tracking item, where the '
                                                        'tracked item references a person.',
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
                                           'start_tracking_dt_tm': 'Date and time at which tracking of the '
                                                                   'item started.',
                                           'subject_id_system': 'System of subject_key: '
                                                                'urn:cerner:person_id, or nosubject when the '
                                                                'row names no patient.',
                                           'subject_key': 'Deterministic subject key: hash of the Cerner '
                                                          'person id, or of the source row when no person is '
                                                          'recorded.',
                                           'surg_case_id': 'Internal surrogate identifier of the surgical '
                                                           'theatre case associated with the tracking '
                                                           'episode, derived from the Millennium tracking '
                                                           'item parent entity reference where it points to '
                                                           'a surgical case.',
                                           'tracking_episode_key': 'Deterministic SHA-256 key of the '
                                                                   'tracking episode row (namespace plus '
                                                                   'native source key).',
                                           'tracking_id': 'Millennium identifier for the tracking item '
                                                          'record, appearing to uniquely identify each row '
                                                          'in TRACKING_ITEM.',
                                           'tracking_open_ind': 'Boolean indicator showing whether the '
                                                                'tracking episode is still open, derived '
                                                                'from the tracking item end tracking date '
                                                                'and time.',
                                           'tracking_prearrival_id': 'Internal surrogate identifier of the '
                                                                     'tracking pre-arrival record linked to '
                                                                     'the episode, derived from the tracking '
                                                                     'item parent entity reference.',
                                           'tracking_status_flag': 'Numeric flag indicating the tracking '
                                                                   'status of the item.',
                                           'tracking_type_flag': 'Numeric flag indicating the type of '
                                                                 'tracking item, such as whether it relates '
                                                                 'to a person, encounter, inventory item or '
                                                                 'personnel member.',
                                           'trust': 'NHS trust name or code assigned at ingestion by mapping '
                                                    "the row's organisation to a trust, partly resolved via "
                                                    'encounter data.'},
                               'quantity_override': False,
                               'table': 'One row per FirstNet (emergency department) tracking episode, keyed '
                                        'by Millennium TRACKING_ID. Gold QC twin of silver '
                                        'clinical_tracking_episode: mandatory rules drop rows, advisory '
                                        'rules are reported in Lakeflow expectation metrics.'}}
