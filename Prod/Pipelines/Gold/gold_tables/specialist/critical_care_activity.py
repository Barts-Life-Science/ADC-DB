"""Gold specialist.critical_care_activity: 2 dataset(s).

- datasets: _qc_clinical_critical_care_activity, clinical_critical_care_activity
- moved verbatim from: 3_gold_specialist_care:623, 3_gold_specialist_care:656, 3_gold_specialist_care:664, 3_gold_specialist_care:723, 3_gold_specialist_care:727, 3_gold_specialist_care:744

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.critical_care_activity ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CRITICAL_CARE_ACTIVITY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_row_hash` AS `source_row_hash`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`period_link_status` AS `period_link_status`',
    '`period_business_key` AS `period_business_key`',
    '`parent_period_id` AS `parent_period_id`',
    '`cds_apc_id` AS `cds_apc_id`',
    '`cc_type` AS `cc_type`',
    '`source_duplicate_count` AS `source_duplicate_count`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _clinical_critical_care_activity_metadata; source history stays on the main research table.
CLINICAL_CRITICAL_CARE_ACTIVITY_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 336,263 rows of 341,038 that
    # are current and attributable; identity_status = 'resolved' keeps the 331,191 rows of
    # 341,038 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_CRITICAL_CARE_ACTIVITY_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.critical_care_activity.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 9,847 of 341,038 at the profile.
    "gold.clinical.critical_care_activity.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 4,775 of 341,038 at the profile.
    "gold.clinical.critical_care_activity.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 7,892 of 341,038 rows (2.31%) when profiled on 2026-08-24.
    "gold.clinical.critical_care_activity.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_CRITICAL_CARE_ACTIVITY_PERSON_SCOPE_RULES = {
    "gold.clinical.critical_care_activity.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_critical_care_activity"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_CRITICAL_CARE_ACTIVITY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CRITICAL_CARE_ACTIVITY_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_CRITICAL_CARE_ACTIVITY_PERSON_SCOPE_RULES)
def _gold_qc_clinical_critical_care_activity():
    """Quality-controlled twin of journey_clinical.critical_care_activity."""
    df = _qc(
        "clinical_critical_care_activity",
        CLINICAL_CRITICAL_CARE_ACTIVITY_SELECT,
        date_flags=["event_before_birth"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.critical_care_activity"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_critical_care_activity():
    """Contract-v2 public twin of clinical_critical_care_activity; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_critical_care_activity")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_row_hash` AS `source_row_hash`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`period_link_status` AS `period_link_status`',
        '`period_business_key` AS `period_business_key`',
        '`parent_period_id` AS `parent_period_id`',
        '`cds_apc_id` AS `cds_apc_id`',
        '`cc_type` AS `cc_type`',
        '`source_duplicate_count` AS `source_duplicate_count`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'clinical_critical_care_activity': GoldRules(
        table='clinical_critical_care_activity',
        select=CLINICAL_CRITICAL_CARE_ACTIVITY_SELECT,
        mandatory=CLINICAL_CRITICAL_CARE_ACTIVITY_MANDATORY_RULES,
        advisory=CLINICAL_CRITICAL_CARE_ACTIVITY_ADVISORY_RULES,
        person_scope=CLINICAL_CRITICAL_CARE_ACTIVITY_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_before_birth',)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_critical_care_activity': {'columns': {'cc_type': 'Value describing cc type for the critical '
                                                                'care activity record. It is carried from '
                                                                'bronze field `CC_Type` in '
                                                                '`4_prod.bronze.map_critical_care_activity`. '
                                                                'Whitespace and source sentinel text are '
                                                                'retained unless the pipeline explicitly '
                                                                'normalizes them; null means no value was '
                                                                'supplied.',
                                                     'cds_apc_id': 'Source-system identifier for the CDS apc '
                                                                   'associated with each critical care '
                                                                   'activity record. It is carried from '
                                                                   'bronze field `CDS_APC_ID` in '
                                                                   '`4_prod.bronze.map_critical_care_activity`. '
                                                                   'Its uniqueness is limited to the '
                                                                   'originating source namespace; null means '
                                                                   'no identifier was supplied.',
                                                     'confidentiality_code': 'Source confidentiality '
                                                                             'classification attached to the '
                                                                             'record for each critical care '
                                                                             'activity record. It is '
                                                                             'produced by the silver '
                                                                             'transformation and has no '
                                                                             'direct bronze-column lineage '
                                                                             'entry. Source code meanings '
                                                                             'and sentinel values are '
                                                                             'retained unless the pipeline '
                                                                             'explicitly maps them; null '
                                                                             'means no code was supplied.',
                                                     'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT '
                                                                     'when available.',
                                                     'event_datetime': 'Date and time when the represented '
                                                                       'clinical or administrative event '
                                                                       'occurred for each critical care '
                                                                       'activity record. It is derived from '
                                                                       'bronze field `Activity_Date_CLEAN` '
                                                                       'in '
                                                                       '`4_prod.bronze.map_critical_care_activity`. '
                                                                       'Source precision and timezone '
                                                                       'handling follow the pipeline '
                                                                       'expression; null means the time was '
                                                                       'unavailable.',
                                                     'event_datetime_local': 'Start date/time of the '
                                                                             'critical-care activity event '
                                                                             'expressed in local time.',
                                                     'event_end_datetime': 'Date and time when the '
                                                                           'represented clinical or '
                                                                           'administrative event ended for '
                                                                           'each critical care activity '
                                                                           'record. It is produced by the '
                                                                           'silver transformation and has no '
                                                                           'direct bronze-column lineage '
                                                                           'entry. Source precision and '
                                                                           'timezone handling follow the '
                                                                           'pipeline expression; null means '
                                                                           'the time was unavailable.',
                                                     'event_end_datetime_local': 'End date/time of the '
                                                                                 'critical-care activity '
                                                                                 'event expressed in local '
                                                                                 'time.',
                                                     'fact_category': 'Contract category used to group the '
                                                                      'record with comparable clinical facts '
                                                                      'for each critical care activity '
                                                                      'record. It is produced by the silver '
                                                                      'transformation and has no direct '
                                                                      'bronze-column lineage entry. '
                                                                      'Whitespace and source sentinel text '
                                                                      'are retained unless the pipeline '
                                                                      'explicitly normalizes them; null '
                                                                      'means no value was supplied.',
                                                     'loaded_at': 'map_critical_care_activity.ADC_UPDT '
                                                                  'carried unchanged through code filtering '
                                                                  'and source/public projections. No '
                                                                  'parent-period timestamp is joined or '
                                                                  'added. It is the contributing bronze load '
                                                                  'clock, distinct from PIPELINE_UPDT_DT_TM '
                                                                  'published as source_update_timestamp, not '
                                                                  'Silver refresh time.',
                                                     'parent_period_id': 'Internal surrogate identifier '
                                                                         'uniquely identifying each critical '
                                                                         'care period record in this table.',
                                                     'patient_event_key': 'Deterministic event key derived '
                                                                          'from source identifiers; retained '
                                                                          'as a stable join key.',
                                                     'period_business_key': 'CCMDS parent period business '
                                                                            'key.',
                                                     'period_link_status': 'Processing or clinical status of '
                                                                           'period link for the critical '
                                                                           'care activity record. It is '
                                                                           'carried from bronze field '
                                                                           '`PERIOD_LINK_STATUS` in '
                                                                           '`4_prod.bronze.map_critical_care_activity`. '
                                                                           'Whitespace and source sentinel '
                                                                           'text are retained unless the '
                                                                           'pipeline explicitly normalizes '
                                                                           'them; null means no value was '
                                                                           'supplied.',
                                                     'person_id': 'Native Millennium PERSON_ID as BIGINT '
                                                                  'when resolved.',
                                                     'record_status': 'Retracted when '
                                                                      'map_critical_care_activity.SOURCE_PRESENT_IND '
                                                                      'is false, otherwise active; a missing '
                                                                      'presence flag defaults true. No '
                                                                      'superseded status is emitted. '
                                                                      'Parent-period link status, care type '
                                                                      'and activity code do not determine '
                                                                      'this row-status label.',
                                                     'record_status_effective_from': 'map_critical_care_activity.Activity_Date_CLEAN '
                                                                                     'carried unchanged. '
                                                                                     'This is an '
                                                                                     'activity-date proxy, '
                                                                                     'not an independently '
                                                                                     'observed source-status '
                                                                                     'transition; no '
                                                                                     'parent-period start or '
                                                                                     'ingestion-time '
                                                                                     'fallback is applied.',
                                                     'record_status_effective_to': 'map_critical_care_activity.ADC_UPDT '
                                                                                   'only when the activity '
                                                                                   'row is retracted; '
                                                                                   'otherwise null. This is '
                                                                                   'an ingestion-time end '
                                                                                   'proxy, not a '
                                                                                   'parent-period discharge '
                                                                                   'or clinical activity '
                                                                                   'end; a missing ADC_UPDT '
                                                                                   'remains null.',
                                                     'source_code': 'Code supplied by the originating source '
                                                                    'system for the represented concept for '
                                                                    'each critical care activity record. It '
                                                                    'is derived from bronze field '
                                                                    '`Activity_Code` in '
                                                                    '`4_prod.bronze.map_critical_care_activity`. '
                                                                    'Source code meanings and sentinel '
                                                                    'values are retained unless the pipeline '
                                                                    'explicitly maps them; null means no '
                                                                    'code was supplied.',
                                                     'source_coding_system': 'Coding system or source '
                                                                             'namespace in which the source '
                                                                             'code is defined for each '
                                                                             'critical care activity record. '
                                                                             'It is produced by the silver '
                                                                             'transformation and has no '
                                                                             'direct bronze-column lineage '
                                                                             'entry. Source code meanings '
                                                                             'and sentinel values are '
                                                                             'retained unless the pipeline '
                                                                             'explicitly maps them; null '
                                                                             'means no code was supplied.',
                                                     'source_display': 'Textual description of the meaning '
                                                                       'of the NHS Data Dictionary code '
                                                                       'value for the element.',
                                                     'source_duplicate_count': 'Number of source duplicate '
                                                                               'items associated with the '
                                                                               'critical care activity '
                                                                               'record. It is carried from '
                                                                               'bronze field '
                                                                               '`SOURCE_DUPLICATE_COUNT` in '
                                                                               '`4_prod.bronze.map_critical_care_activity`. '
                                                                               'The unit is stated in the '
                                                                               'description; null means the '
                                                                               'measure could not be derived '
                                                                               'and is not equivalent to '
                                                                               'zero.',
                                                     'source_feed': 'Originating data feed responsible for '
                                                                    'the record for each critical care '
                                                                    'activity record. It is produced by the '
                                                                    'silver transformation and has no direct '
                                                                    'bronze-column lineage entry. Whitespace '
                                                                    'and source sentinel text are retained '
                                                                    'unless the pipeline explicitly '
                                                                    'normalizes them; null means no value '
                                                                    'was supplied.',
                                                     'source_row_hash': 'CCMDS ROW_HASH supplied by the '
                                                                        'source; evidence for the exception '
                                                                        'where no durable native activity id '
                                                                        'exists.',
                                                     'source_update_timestamp': 'Bronze pipeline processing '
                                                                                'timestamp carried from '
                                                                                '`PIPELINE_UPDT_DT_TM` in '
                                                                                '`4_prod.bronze.map_critical_care_activity`. '
                                                                                'It records when bronze '
                                                                                'processed the source row, '
                                                                                'not a native '
                                                                                'clinical-system update '
                                                                                'timestamp, clinical event '
                                                                                'time, or the current Silver '
                                                                                'refresh time.',
                                                     'subject_id_system': 'Identifier namespace used to '
                                                                          'interpret the subject identifier '
                                                                          'on this record for each critical '
                                                                          'care activity record. It is '
                                                                          'derived from bronze field '
                                                                          '`PERSON_ID` in '
                                                                          '`4_prod.bronze.map_critical_care_activity`. '
                                                                          'Its uniqueness is limited to the '
                                                                          'originating source namespace; '
                                                                          'null means no identifier was '
                                                                          'supplied.',
                                                     'subject_key': 'Deterministic SHA-256 over the '
                                                                    'strongest source identifier. Not '
                                                                    'salted, not secret; a stable join key '
                                                                    'across feeds where person_id is '
                                                                    'unresolved.',
                                                     'vip_ind': 'Indicator that the source record carries '
                                                                'VIP handling status for each critical care '
                                                                'activity record. It is produced by the '
                                                                'silver transformation and has no direct '
                                                                'bronze-column lineage entry. Null means the '
                                                                'source did not state the indicator and must '
                                                                'not be interpreted as false.',
                                                     'withheld_identity_ind': 'Indicator that the source '
                                                                              'identity was deliberately '
                                                                              'withheld for each critical '
                                                                              'care activity record. It is '
                                                                              'produced by the silver '
                                                                              'transformation and has no '
                                                                              'direct bronze-column lineage '
                                                                              'entry. Null means the source '
                                                                              'did not state the indicator '
                                                                              'and must not be interpreted '
                                                                              'as false.'},
                                         'quantity_override': True,
                                         'table': 'Internal quality-controlled twin of '
                                                  'clinical_critical_care_activity: the Gold repairs, nulls '
                                                  'and expectations are applied here and the published Gold '
                                                  'MV reads this object. Materialized rather than temporary '
                                                  'so the published MV can refresh incrementally.'},
 'clinical_critical_care_activity': {'columns': {'cc_type': 'Value describing cc type for the critical care '
                                                            'activity record. It is carried from bronze '
                                                            'field `CC_Type` in '
                                                            '`4_prod.bronze.map_critical_care_activity`. '
                                                            'Whitespace and source sentinel text are '
                                                            'retained unless the pipeline explicitly '
                                                            'normalizes them; null means no value was '
                                                            'supplied.',
                                                 'cds_apc_id': 'Source-system identifier for the CDS apc '
                                                               'associated with each critical care activity '
                                                               'record. It is carried from bronze field '
                                                               '`CDS_APC_ID` in '
                                                               '`4_prod.bronze.map_critical_care_activity`. '
                                                               'Its uniqueness is limited to the originating '
                                                               'source namespace; null means no identifier '
                                                               'was supplied.',
                                                 'confidentiality_code': 'Source confidentiality '
                                                                         'classification attached to the '
                                                                         'record for each critical care '
                                                                         'activity record. It is produced by '
                                                                         'the silver transformation and has '
                                                                         'no direct bronze-column lineage '
                                                                         'entry. Source code meanings and '
                                                                         'sentinel values are retained '
                                                                         'unless the pipeline explicitly '
                                                                         'maps them; null means no code was '
                                                                         'supplied.',
                                                 'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                                 'available.',
                                                 'event_datetime': 'Date and time when the represented '
                                                                   'clinical or administrative event '
                                                                   'occurred for each critical care activity '
                                                                   'record. It is derived from bronze field '
                                                                   '`Activity_Date_CLEAN` in '
                                                                   '`4_prod.bronze.map_critical_care_activity`. '
                                                                   'Source precision and timezone handling '
                                                                   'follow the pipeline expression; null '
                                                                   'means the time was unavailable.',
                                                 'event_datetime_local': 'Start date/time of the '
                                                                         'critical-care activity event '
                                                                         'expressed in local time.',
                                                 'event_end_datetime': 'Date and time when the represented '
                                                                       'clinical or administrative event '
                                                                       'ended for each critical care '
                                                                       'activity record. It is produced by '
                                                                       'the silver transformation and has no '
                                                                       'direct bronze-column lineage entry. '
                                                                       'Source precision and timezone '
                                                                       'handling follow the pipeline '
                                                                       'expression; null means the time was '
                                                                       'unavailable.',
                                                 'event_end_datetime_local': 'End date/time of the '
                                                                             'critical-care activity event '
                                                                             'expressed in local time.',
                                                 'fact_category': 'Contract category used to group the '
                                                                  'record with comparable clinical facts for '
                                                                  'each critical care activity record. It is '
                                                                  'produced by the silver transformation and '
                                                                  'has no direct bronze-column lineage '
                                                                  'entry. Whitespace and source sentinel '
                                                                  'text are retained unless the pipeline '
                                                                  'explicitly normalizes them; null means no '
                                                                  'value was supplied.',
                                                 'loaded_at': 'map_critical_care_activity.ADC_UPDT carried '
                                                              'unchanged through code filtering and '
                                                              'source/public projections. No parent-period '
                                                              'timestamp is joined or added. It is the '
                                                              'contributing bronze load clock, distinct from '
                                                              'PIPELINE_UPDT_DT_TM published as '
                                                              'source_update_timestamp, not Silver refresh '
                                                              'time.',
                                                 'parent_period_id': 'Internal surrogate identifier uniquely '
                                                                     'identifying each critical care period '
                                                                     'record in this table.',
                                                 'patient_event_key': 'Deterministic event key derived from '
                                                                      'source identifiers; retained as a '
                                                                      'stable join key.',
                                                 'period_business_key': 'CCMDS parent period business key.',
                                                 'period_link_status': 'Processing or clinical status of '
                                                                       'period link for the critical care '
                                                                       'activity record. It is carried from '
                                                                       'bronze field `PERIOD_LINK_STATUS` in '
                                                                       '`4_prod.bronze.map_critical_care_activity`. '
                                                                       'Whitespace and source sentinel text '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly normalizes them; null '
                                                                       'means no value was supplied.',
                                                 'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                              'resolved.',
                                                 'record_status': 'Retracted when '
                                                                  'map_critical_care_activity.SOURCE_PRESENT_IND '
                                                                  'is false, otherwise active; a missing '
                                                                  'presence flag defaults true. No '
                                                                  'superseded status is emitted. '
                                                                  'Parent-period link status, care type and '
                                                                  'activity code do not determine this '
                                                                  'row-status label.',
                                                 'record_status_effective_from': 'map_critical_care_activity.Activity_Date_CLEAN '
                                                                                 'carried unchanged. This is '
                                                                                 'an activity-date proxy, '
                                                                                 'not an independently '
                                                                                 'observed source-status '
                                                                                 'transition; no '
                                                                                 'parent-period start or '
                                                                                 'ingestion-time fallback is '
                                                                                 'applied.',
                                                 'record_status_effective_to': 'map_critical_care_activity.ADC_UPDT '
                                                                               'only when the activity row '
                                                                               'is retracted; otherwise '
                                                                               'null. This is an '
                                                                               'ingestion-time end proxy, '
                                                                               'not a parent-period '
                                                                               'discharge or clinical '
                                                                               'activity end; a missing '
                                                                               'ADC_UPDT remains null.',
                                                 'source_code': 'Code supplied by the originating source '
                                                                'system for the represented concept for each '
                                                                'critical care activity record. It is '
                                                                'derived from bronze field `Activity_Code` '
                                                                'in '
                                                                '`4_prod.bronze.map_critical_care_activity`. '
                                                                'Source code meanings and sentinel values '
                                                                'are retained unless the pipeline explicitly '
                                                                'maps them; null means no code was supplied.',
                                                 'source_coding_system': 'Coding system or source namespace '
                                                                         'in which the source code is '
                                                                         'defined for each critical care '
                                                                         'activity record. It is produced by '
                                                                         'the silver transformation and has '
                                                                         'no direct bronze-column lineage '
                                                                         'entry. Source code meanings and '
                                                                         'sentinel values are retained '
                                                                         'unless the pipeline explicitly '
                                                                         'maps them; null means no code was '
                                                                         'supplied.',
                                                 'source_display': 'Textual description of the meaning of '
                                                                   'the NHS Data Dictionary code value for '
                                                                   'the element.',
                                                 'source_duplicate_count': 'Number of source duplicate items '
                                                                           'associated with the critical '
                                                                           'care activity record. It is '
                                                                           'carried from bronze field '
                                                                           '`SOURCE_DUPLICATE_COUNT` in '
                                                                           '`4_prod.bronze.map_critical_care_activity`. '
                                                                           'The unit is stated in the '
                                                                           'description; null means the '
                                                                           'measure could not be derived and '
                                                                           'is not equivalent to zero.',
                                                 'source_feed': 'Originating data feed responsible for the '
                                                                'record for each critical care activity '
                                                                'record. It is produced by the silver '
                                                                'transformation and has no direct '
                                                                'bronze-column lineage entry. Whitespace and '
                                                                'source sentinel text are retained unless '
                                                                'the pipeline explicitly normalizes them; '
                                                                'null means no value was supplied.',
                                                 'source_row_hash': 'CCMDS ROW_HASH supplied by the source; '
                                                                    'evidence for the exception where no '
                                                                    'durable native activity id exists.',
                                                 'source_update_timestamp': 'Bronze pipeline processing '
                                                                            'timestamp carried from '
                                                                            '`PIPELINE_UPDT_DT_TM` in '
                                                                            '`4_prod.bronze.map_critical_care_activity`. '
                                                                            'It records when bronze '
                                                                            'processed the source row, not a '
                                                                            'native clinical-system update '
                                                                            'timestamp, clinical event time, '
                                                                            'or the current Silver refresh '
                                                                            'time.',
                                                 'subject_id_system': 'Identifier namespace used to '
                                                                      'interpret the subject identifier on '
                                                                      'this record for each critical care '
                                                                      'activity record. It is derived from '
                                                                      'bronze field `PERSON_ID` in '
                                                                      '`4_prod.bronze.map_critical_care_activity`. '
                                                                      'Its uniqueness is limited to the '
                                                                      'originating source namespace; null '
                                                                      'means no identifier was supplied.',
                                                 'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                'source identifier. Not salted, not secret; '
                                                                'a stable join key across feeds where '
                                                                'person_id is unresolved.',
                                                 'vip_ind': 'Indicator that the source record carries VIP '
                                                            'handling status for each critical care activity '
                                                            'record. It is produced by the silver '
                                                            'transformation and has no direct bronze-column '
                                                            'lineage entry. Null means the source did not '
                                                            'state the indicator and must not be interpreted '
                                                            'as false.',
                                                 'withheld_identity_ind': 'Indicator that the source '
                                                                          'identity was deliberately '
                                                                          'withheld for each critical care '
                                                                          'activity record. It is produced '
                                                                          'by the silver transformation and '
                                                                          'has no direct bronze-column '
                                                                          'lineage entry. Null means the '
                                                                          'source did not state the '
                                                                          'indicator and must not be '
                                                                          'interpreted as false.'},
                                     'quantity_override': False,
                                     'table': 'One admitted CCMDS critical-care activity with parent-link '
                                              'evidence. Gold QC twin of the silver product: 1 columns are '
                                              'repaired or nulled, 1 rule(s) drop rows, 3 check(s) are '
                                              'advisory. Each rule states its reason in the pipeline '
                                              'notebook, and Lakeflow expectation metrics report what every '
                                              'rule matched on each update. Coverage: Critical-care activity '
                                              'rows step up about 5-fold from August 2025 after a CCMDS '
                                              'coding change (periods unchanged); 22% of rows have no parent '
                                              'period; complete-through month per release: '
                                              '6_mgmt.data_quality.release_finding (finding_code '
                                              'F2_complete_through).'}}
