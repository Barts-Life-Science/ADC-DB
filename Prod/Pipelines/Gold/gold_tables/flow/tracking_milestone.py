"""Gold flow.tracking_milestone: 2 dataset(s).

- datasets: clinical_tracking_milestone, _gold_qc_clinical_tracking_milestone
- moved verbatim from: 7_gold_patient_flow:164, 7_gold_patient_flow:172, 7_gold_patient_flow:182, 7_gold_patient_flow:200

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== clinical_tracking_milestone ====
CLINICAL_TRACKING_MILESTONE_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.clinical_tracking_milestone.active_record": "COALESCE(record_status = 'active', FALSE)",
    # These patient-flow rows require a resolved person identity.
    "gold.clinical_tracking_milestone.resolved_identity": "COALESCE(identity_status = 'resolved', FALSE)",
    # An undated milestone was previously excluded from this product.
    "gold.clinical_tracking_milestone.dated_milestone": "milestone_datetime IS NOT NULL",
}


CLINICAL_TRACKING_MILESTONE_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.tracking_milestone.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.clinical_tracking_milestone.key_present": "tracking_milestone_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.clinical_tracking_milestone.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.clinical_tracking_milestone",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_TRACKING_MILESTONE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_TRACKING_MILESTONE_ADVISORY_RULES)
def gold_clinical_tracking_milestone():
    """Publish clinical_tracking_milestone; native rules count exclusions and advisory failures."""
    return _tdx_product("clinical_tracking_milestone")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_clinical_tracking_milestone",
    refresh_policy="auto",
)
def _gold_qc_clinical_tracking_milestone():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("clinical_tracking_milestone", "tracking_milestone_key")


RULES = {
    'clinical_tracking_milestone': GoldRules(
        table='clinical_tracking_milestone',
        mandatory=CLINICAL_TRACKING_MILESTONE_MANDATORY_RULES,
        advisory=CLINICAL_TRACKING_MILESTONE_ADVISORY_RULES,
        value_rules={'gold.clinical.tracking_milestone.complete_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                          'column': 'complete_id',
                                                                          'companions': [],
                                                                          'match': '`complete_id` = 0',
                                                                          'table': 'clinical_tracking_milestone'},
         'gold.clinical.tracking_milestone.event_status_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                                'column': 'event_status_cd',
                                                                                'companions': [],
                                                                                'match': '`event_status_cd` '
                                                                                         '= 0 AND '
                                                                                         '`event_status_display` '
                                                                                         'IS NULL',
                                                                                'table': 'clinical_tracking_milestone'},
         'gold.clinical.tracking_milestone.onset_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                       'column': 'onset_id',
                                                                       'companions': [],
                                                                       'match': '`onset_id` = 0',
                                                                       'table': 'clinical_tracking_milestone'},
         'gold.clinical.tracking_milestone.requested_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                           'column': 'requested_id',
                                                                           'companions': [],
                                                                           'match': '`requested_id` = 0',
                                                                           'table': 'clinical_tracking_milestone'},
         'gold.clinical.tracking_milestone.tracking_event_type_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                                       'column': 'tracking_event_type_cd',
                                                                                       'companions': [],
                                                                                       'match': '`tracking_event_type_cd` '
                                                                                                '= 0 AND '
                                                                                                '`tracking_event_type_display` '
                                                                                                'IS NULL',
                                                                                       'table': 'clinical_tracking_milestone'}}),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_clinical_tracking_milestone': {'columns': {'qc_checked_at': 'loaded_at of the admitted row (the '
                                                                       'QC evidence time).',
                                                      'qc_status': 'passed for every admitted row; advisory '
                                                                   'outcomes are in Lakeflow expectation '
                                                                   'metrics.',
                                                      'tracking_milestone_key': 'Key of the admitted '
                                                                                'clinical_tracking_milestone '
                                                                                'row.'},
                                          'quantity_override': False,
                                          'table': 'Compatibility evidence of admitted '
                                                   'clinical_tracking_milestone rows; advisory outcomes are '
                                                   'in Lakeflow expectation metrics.'},
 'clinical_tracking_milestone': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                             'clinical_event_cd': 'Code value identifying the clinical event '
                                                                  'associated with the tracking event.',
                                             'complete_dt_tm': 'Date and time the tracking event was '
                                                               'completed.',
                                             'complete_id': 'Identifier of the personnel associated with '
                                                            'completing the tracking event.',
                                             'critical_interval': 'Interval after which the tracking event '
                                                                  'is treated as critical.',
                                             'encounter_id': 'Millennium ENCNTR_ID of the encounter the row '
                                                             'belongs to (BIGINT); NULL when not recorded.',
                                             'event_status_cd': 'Code value representing the status of the '
                                                                'tracking event.',
                                             'event_status_display': 'Display text for the status of the '
                                                                     'tracking milestone event, resolved '
                                                                     'from the Millennium code value lookup.',
                                             'identity_status': 'resolved when person_id is present, '
                                                                'unresolved when absent, not_applicable for '
                                                                'tables without a patient.',
                                             'linkage_route': 'How the row was linked to its person or '
                                                              'encounter.',
                                             'load_batch_id': 'Load batch label derived from loaded_at '
                                                              '(yyyyMMddHHmmss); tdx-v21 when no load time '
                                                              'exists.',
                                             'loaded_at': 'When the row was last written to bronze '
                                                          '(ADC_UPDT), else its source update time; the '
                                                          'stable load clock.',
                                             'milestone_class': 'Classification of the clinical tracking '
                                                                'milestone, derived from the tracking '
                                                                'milestone mapping using the Millennium '
                                                                'track event display name.',
                                             'milestone_config_era': 'Tracking milestone configuration era: '
                                                                     'pre_2024_04 or post_2024_04 by '
                                                                     'milestone_datetime (the configuration '
                                                                     'changed on 2024-04-01, so step changes '
                                                                     'at that date are configuration, not '
                                                                     'activity). NULL when '
                                                                     'milestone_datetime is NULL. INSR_W2.',
                                             'milestone_datetime': 'Date and time of the tracking milestone, '
                                                                   "derived from the tracking event's "
                                                                   'complete, onset or requested date-time.',
                                             'milestone_phase': 'Phase of the clinical tracking pathway that '
                                                                'the milestone belongs to, derived from the '
                                                                'tracking milestone mapping using the '
                                                                'Millennium track event display name.',
                                             'milestone_rule_id': 'Identifier of the mapping rule used to '
                                                                  'assign the track event to this milestone, '
                                                                  'derived from the tracking milestone '
                                                                  'mapping.',
                                             'milestone_rule_version': 'Version identifier of the milestone '
                                                                       'rule applied to the tracking event, '
                                                                       'taken directly from the source '
                                                                       'Millennium tracking milestone '
                                                                       'record.',
                                             'onset_dt_tm': 'Date and time of onset (start) of the tracking '
                                                            'event.',
                                             'onset_id': 'Identifier of the personnel associated with the '
                                                         'onset of the tracking event.',
                                             'organization_id': 'Landing-added Millennium organisation '
                                                                'identifier resolved for the tracking event, '
                                                                'used for organisation-to-trust mapping.',
                                             'overdue_interval': 'Interval after which the tracking event is '
                                                                 'treated as overdue.',
                                             'person_id': 'Millennium PERSON_ID of the patient (identifier); '
                                                          'NULL when the source row names no patient.',
                                             'pipeline_run_id': 'Bronze pipeline run that last wrote the '
                                                                'row.',
                                             'record_status': 'active, or superseded when the row stopped '
                                                              'appearing in the source snapshot '
                                                              '(source_present_ind false).',
                                             'record_status_effective_from': 'Start of the record status; '
                                                                             'not supplied by this source '
                                                                             '(always NULL).',
                                             'record_status_effective_to': 'When a superseded row was first '
                                                                           'detected absent from the source '
                                                                           '(source_absent_detected_ts); '
                                                                           'NULL while active.',
                                             'requested_dt_tm': 'Date and time the tracking event was '
                                                                'requested.',
                                             'requested_id': 'Identifier of the personnel associated with '
                                                             'requesting the tracking event.',
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
                                             'source_update_timestamp': 'Time the source row was landed in '
                                                                        'the raw layer (bronze '
                                                                        'SOURCE_ADC_UPDT), not when it '
                                                                        'changed in the source system; do '
                                                                        'not use it for recency.',
                                             'subject_id_system': 'System of subject_key: '
                                                                  'urn:cerner:person_id, or nosubject when '
                                                                  'the row names no patient.',
                                             'subject_key': 'Deterministic subject key: hash of the Cerner '
                                                            'person id, or of the source row when no person '
                                                            'is recorded.',
                                             'surg_case_id': 'Internal surrogate identifier of the surgical '
                                                             'case associated with the clinical tracking '
                                                             'milestone, populated only for surgery-related '
                                                             'milestones.',
                                             'track_event_description': 'Description of the Millennium '
                                                                        'tracking event associated with the '
                                                                        'milestone.',
                                             'track_event_display': 'Display name of the Millennium tracking '
                                                                    'event associated with the milestone.',
                                             'track_event_id': 'Identifier of the track event definition '
                                                               'that this tracking event instantiates.',
                                             'tracking_event_id': 'Unique Millennium identifier for the '
                                                                  'tracking event row in the TRACKING_EVENT '
                                                                  'table.',
                                             'tracking_event_type_cd': 'Code value identifying the type of '
                                                                       'tracking event.',
                                             'tracking_event_type_display': 'Display text for the type of '
                                                                            'tracking event associated with '
                                                                            'the milestone, resolved from '
                                                                            'the Millennium code value '
                                                                            'lookup.',
                                             'tracking_group_cd': 'Code value identifying the tracking group '
                                                                  'associated with the tracking event.',
                                             'tracking_group_display': 'Display text for the tracking group '
                                                                       'to which the milestone belongs, '
                                                                       'resolved from the Millennium code '
                                                                       'value lookup.',
                                             'tracking_id': 'Identifier of the tracking record to which this '
                                                            'tracking event belongs.',
                                             'tracking_milestone_key': 'Deterministic SHA-256 key of the '
                                                                       'tracking milestone row (namespace '
                                                                       'plus native source key).',
                                             'trust': 'Landing-added NHS trust name or label resolved for '
                                                      'the row through encounter-to-organisation and '
                                                      'organisation-to-trust mapping.'},
                                 'quantity_override': False,
                                 'table': 'One row per FirstNet tracking event (milestone such as triage or '
                                          'seen by clinician), keyed by TRACKING_EVENT_ID. Gold QC twin of '
                                          'silver clinical_tracking_milestone: mandatory rules drop rows, '
                                          'advisory rules are reported in Lakeflow expectation metrics. '
                                          'Coverage: Tracking milestones per attendance triple from '
                                          'September 2024 (a configuration change; check-ins unchanged); '
                                          'complete-through month per release: '
                                          '6_mgmt.data_quality.release_finding (finding_code '
                                          'F2_complete_through).'}}
