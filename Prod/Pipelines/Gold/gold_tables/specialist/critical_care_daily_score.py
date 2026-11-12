"""Gold specialist.critical_care_daily_score: 2 dataset(s).

- datasets: _qc_clinical_critical_care_daily_score, clinical_critical_care_daily_score
- moved verbatim from: 3_gold_specialist_care:1002, 3_gold_specialist_care:1041, 3_gold_specialist_care:1050, 3_gold_specialist_care:1115, 3_gold_specialist_care:1119, 3_gold_specialist_care:1136

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.critical_care_daily_score ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CRITICAL_CARE_DAILY_SCORE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_unit` AS `source_unit`',
    '`source_site` AS `source_site`',
    '`source_daily_id` AS `source_daily_id`',
    '`score_type` AS `score_type`',
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
    'CASE WHEN `score_value` < 0 THEN NULL ELSE `score_value` END AS `score_value`',
    '`score_value_raw` AS `score_value_raw`',
    '`score_date` AS `score_date`',
    '`score_calc_date` AS `score_calc_date`',
    '`admission_key` AS `admission_key`',
    '`critical_care_admission_key` AS `critical_care_admission_key`',
    '`day_latest_ind` AS `day_latest_ind`',
    '`row_class` AS `row_class`',
    '`person_link_status` AS `person_link_status`',
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


# contract v2: QC/batch inputs come from internal _clinical_critical_care_daily_score_metadata; source history stays on the main research table.
CLINICAL_CRITICAL_CARE_DAILY_SCORE_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 3,989,576 rows of 3,989,576
    # that are current and attributable; identity_status = 'resolved' keeps the 3,798,086
    # rows of 3,989,576 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_CRITICAL_CARE_DAILY_SCORE_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.critical_care_daily_score.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 191,490 of 3,989,576 at the profile.
    "gold.clinical.critical_care_daily_score.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 3,989,576 at the profile.
    "gold.clinical.critical_care_daily_score.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 156 of 3,989,576 rows (0.00391%) when profiled on 2026-08-24.
    "gold.clinical.critical_care_daily_score.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_CRITICAL_CARE_DAILY_SCORE_PERSON_SCOPE_RULES = {
    "gold.clinical.critical_care_daily_score.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_critical_care_daily_score"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_CRITICAL_CARE_DAILY_SCORE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CRITICAL_CARE_DAILY_SCORE_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_CRITICAL_CARE_DAILY_SCORE_PERSON_SCOPE_RULES)
def _gold_qc_clinical_critical_care_daily_score():
    """Quality-controlled twin of journey_clinical.critical_care_daily_score."""
    df = _qc(
        "clinical_critical_care_daily_score",
        CLINICAL_CRITICAL_CARE_DAILY_SCORE_SELECT,
        date_flags=["event_after_death_30d"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.critical_care_daily_score"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_critical_care_daily_score():
    """Contract-v2 public twin of clinical_critical_care_daily_score; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_critical_care_daily_score")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_unit` AS `source_unit`',
        '`source_site` AS `source_site`',
        '`source_daily_id` AS `source_daily_id`',
        '`score_type` AS `score_type`',
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
        '`score_value` AS `score_value`',
        '`score_value_raw` AS `score_value_raw`',
        '`score_date` AS `score_date`',
        '`score_calc_date` AS `score_calc_date`',
        '`admission_key` AS `admission_key`',
        '`critical_care_admission_key` AS `critical_care_admission_key`',
        '`day_latest_ind` AS `day_latest_ind`',
        '`row_class` AS `row_class`',
        '`person_link_status` AS `person_link_status`',
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
    'clinical_critical_care_daily_score': GoldRules(
        table='clinical_critical_care_daily_score',
        select=CLINICAL_CRITICAL_CARE_DAILY_SCORE_SELECT,
        mandatory=CLINICAL_CRITICAL_CARE_DAILY_SCORE_MANDATORY_RULES,
        advisory=CLINICAL_CRITICAL_CARE_DAILY_SCORE_ADVISORY_RULES,
        person_scope=CLINICAL_CRITICAL_CARE_DAILY_SCORE_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d',)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_critical_care_daily_score': {'columns': {'admission_key': 'Stable silver-v2 key identifying '
                                                                         'the admission associated with each '
                                                                         'critical care daily score record. '
                                                                         'It is carried from bronze field '
                                                                         '`ADMISSION_KEY` in '
                                                                         '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                         'This is a contract identifier '
                                                                         'rather than a display value; null '
                                                                         'means the source identifiers were '
                                                                         'insufficient to derive the key.',
                                                        'confidentiality_code': 'Source confidentiality '
                                                                                'classification attached to '
                                                                                'the record for each '
                                                                                'critical care daily score '
                                                                                'record. It is produced by '
                                                                                'the silver transformation '
                                                                                'and has no direct '
                                                                                'bronze-column lineage '
                                                                                'entry. Source code meanings '
                                                                                'and sentinel values are '
                                                                                'retained unless the '
                                                                                'pipeline explicitly maps '
                                                                                'them; null means no code '
                                                                                'was supplied.',
                                                        'critical_care_admission_key': 'Deterministic '
                                                                                       'SHA-256 key for the '
                                                                                       'parent critical-care '
                                                                                       'admission.',
                                                        'day_latest_ind': 'Indicator of whether day latest '
                                                                          'applies to the critical care '
                                                                          'daily score record. It is carried '
                                                                          'from bronze field '
                                                                          '`DAY_LATEST_IND` in '
                                                                          '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                          'Null means the source did not '
                                                                          'state the indicator and must not '
                                                                          'be interpreted as false.',
                                                        'encounter_id': 'Native Millennium ENCNTR_ID as '
                                                                        'BIGINT when available.',
                                                        'event_datetime': 'Date and time when the '
                                                                          'represented clinical or '
                                                                          'administrative event occurred for '
                                                                          'each critical care daily score '
                                                                          'record. It is derived from bronze '
                                                                          'field `DATE_DAILY_CLEAN` in '
                                                                          '`4_prod.bronze.map_critical_care_daily_score`, '
                                                                          'selected as the provenance anchor '
                                                                          'from 2 recorded bronze source '
                                                                          'fields. Source precision and '
                                                                          'timezone handling follow the '
                                                                          'pipeline expression; null means '
                                                                          'the time was unavailable.',
                                                        'event_datetime_local': 'Local-time representation '
                                                                                'of the event start date and '
                                                                                'time for the critical-care '
                                                                                'daily score record.',
                                                        'event_end_datetime': 'Date and time when the '
                                                                              'represented clinical or '
                                                                              'administrative event ended '
                                                                              'for each critical care daily '
                                                                              'score record. It is produced '
                                                                              'by the silver transformation '
                                                                              'and has no direct '
                                                                              'bronze-column lineage entry. '
                                                                              'Source precision and timezone '
                                                                              'handling follow the pipeline '
                                                                              'expression; null means the '
                                                                              'time was unavailable.',
                                                        'event_end_datetime_local': 'Local-time '
                                                                                    'representation of the '
                                                                                    'event end date and time '
                                                                                    'for the critical-care '
                                                                                    'daily score record.',
                                                        'fact_category': 'Contract category used to group '
                                                                         'the record with comparable '
                                                                         'clinical facts for each critical '
                                                                         'care daily score record. It is '
                                                                         'produced by the silver '
                                                                         'transformation and has no direct '
                                                                         'bronze-column lineage entry. '
                                                                         'Whitespace and source sentinel '
                                                                         'text are retained unless the '
                                                                         'pipeline explicitly normalizes '
                                                                         'them; null means no value was '
                                                                         'supplied.',
                                                        'loaded_at': 'map_critical_care_daily_score.ADC_UPDT '
                                                                     'carried unchanged through code '
                                                                     'filtering and source/public '
                                                                     'projections. The parent admission key '
                                                                     'is derived without joining a parent '
                                                                     'clock; this is row-level bronze '
                                                                     'ingestion provenance, distinct from '
                                                                     'UPDATED_AT and Silver refresh time.',
                                                        'patient_event_key': 'Deterministic event key '
                                                                             'derived from source '
                                                                             'identifiers; retained as a '
                                                                             'stable join key.',
                                                        'person_id': 'Native Millennium PERSON_ID as BIGINT '
                                                                     'when resolved.',
                                                        'person_link_status': 'Processing or clinical status '
                                                                              'of person link for the '
                                                                              'critical care daily score '
                                                                              'record. It is carried from '
                                                                              'bronze field '
                                                                              '`PERSON_LINK_STATUS` in '
                                                                              '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                              'Whitespace and source '
                                                                              'sentinel text are retained '
                                                                              'unless the pipeline '
                                                                              'explicitly normalizes them; '
                                                                              'null means no value was '
                                                                              'supplied.',
                                                        'record_status': 'Retracted when '
                                                                         'map_critical_care_daily_score.SOURCE_PRESENT_IND '
                                                                         'is false, otherwise active; a '
                                                                         'missing presence flag defaults '
                                                                         'true. No superseded status is '
                                                                         'emitted. DAY_LATEST_IND, '
                                                                         'ROW_CLASS, score type and '
                                                                         'parent-admission state do not '
                                                                         'determine this label.',
                                                        'record_status_effective_from': 'First non-null '
                                                                                        'map_critical_care_daily_score.DATE_DAILY_CLEAN '
                                                                                        'then '
                                                                                        'DATE_SCORE_CALC_CLEAN, '
                                                                                        'matching the '
                                                                                        'event-time '
                                                                                        'expression. This '
                                                                                        'date/calculation-time '
                                                                                        'proxy is not an '
                                                                                        'independently '
                                                                                        'observed row-status '
                                                                                        'transition; no '
                                                                                        'UPDATED_AT or '
                                                                                        'ingestion fallback '
                                                                                        'is applied.',
                                                        'record_status_effective_to': 'map_critical_care_daily_score.ADC_UPDT '
                                                                                      'only when the score '
                                                                                      'row is retracted; '
                                                                                      'otherwise null. This '
                                                                                      'is an ingestion-time '
                                                                                      'end proxy, not the '
                                                                                      'score date, '
                                                                                      'calculation date or '
                                                                                      'admission discharge '
                                                                                      'time.',
                                                        'row_class': 'Value describing row class for the '
                                                                     'critical care daily score record. It '
                                                                     'is carried from bronze field '
                                                                     '`ROW_CLASS` in '
                                                                     '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                     'Whitespace and source sentinel text '
                                                                     'are retained unless the pipeline '
                                                                     'explicitly normalizes them; null means '
                                                                     'no value was supplied.',
                                                        'score_calc_date': 'Calendar date associated with '
                                                                           'score calc for the critical care '
                                                                           'daily score record. It is '
                                                                           'derived from bronze field '
                                                                           '`DATE_SCORE_CALC_CLEAN` in '
                                                                           '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                           'The value has no time-of-day '
                                                                           'component; null means the source '
                                                                           'date was unavailable.',
                                                        'score_date': 'Calendar date associated with score '
                                                                      'for the critical care daily score '
                                                                      'record. It is derived from bronze '
                                                                      'field `DATE_DAILY_CLEAN` in '
                                                                      '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                      'The value has no time-of-day '
                                                                      'component; null means the source date '
                                                                      'was unavailable.',
                                                        'score_type': 'Medicus SCORE_TYPE; primary-key '
                                                                      'component.',
                                                        'score_value': 'Numeric value recorded for score on '
                                                                       'the critical care daily score '
                                                                       'record. It is carried from bronze '
                                                                       'field `SCORE_VALUE` in '
                                                                       '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                       'Units and source sentinel values '
                                                                       'follow the named source field unless '
                                                                       'the pipeline explicitly converts '
                                                                       'them; null is not equivalent to '
                                                                       'zero.',
                                                        'score_value_raw': 'Value describing score value raw '
                                                                           'for the critical care daily '
                                                                           'score record. It is carried from '
                                                                           'bronze field `SCORE_VALUE_RAW` '
                                                                           'in '
                                                                           '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                           'Whitespace and source sentinel '
                                                                           'text are retained unless the '
                                                                           'pipeline explicitly normalizes '
                                                                           'them; null means no value was '
                                                                           'supplied.',
                                                        'source_code': 'Code supplied by the originating '
                                                                       'source system for the represented '
                                                                       'concept for each critical care daily '
                                                                       'score record. It is derived from '
                                                                       'bronze field `SCORE_TYPE` in '
                                                                       '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                       'Source code meanings and sentinel '
                                                                       'values are retained unless the '
                                                                       'pipeline explicitly maps them; null '
                                                                       'means no code was supplied.',
                                                        'source_coding_system': 'Coding system or source '
                                                                                'namespace in which the '
                                                                                'source code is defined for '
                                                                                'each critical care daily '
                                                                                'score record. It is '
                                                                                'produced by the silver '
                                                                                'transformation and has no '
                                                                                'direct bronze-column '
                                                                                'lineage entry. Source code '
                                                                                'meanings and sentinel '
                                                                                'values are retained unless '
                                                                                'the pipeline explicitly '
                                                                                'maps them; null means no '
                                                                                'code was supplied.',
                                                        'source_daily_id': 'Medicus SOURCE_DAILY_ID as '
                                                                           'BIGINT; primary-key component.',
                                                        'source_display': 'Human-readable label supplied by '
                                                                          'the source system for the source '
                                                                          'code for each critical care daily '
                                                                          'score record. It is derived from '
                                                                          'bronze field `SCORE_TYPE` in '
                                                                          '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                          'Whitespace and source sentinel '
                                                                          'text are retained unless the '
                                                                          'pipeline explicitly normalizes '
                                                                          'them; null means no value was '
                                                                          'supplied.',
                                                        'source_feed': 'Originating data feed responsible '
                                                                       'for the record for each critical '
                                                                       'care daily score record. It is '
                                                                       'produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. '
                                                                       'Whitespace and source sentinel text '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly normalizes them; null '
                                                                       'means no value was supplied.',
                                                        'source_site': 'Medicus SOURCE_SITE; primary-key '
                                                                       'component.',
                                                        'source_unit': 'Medicus SOURCE_UNIT; primary-key '
                                                                       'component.',
                                                        'source_update_timestamp': 'Native source-system '
                                                                                   'update timestamp carried '
                                                                                   'through the research '
                                                                                   'transformation for each '
                                                                                   'critical care daily '
                                                                                   'score record. It is '
                                                                                   'derived from bronze '
                                                                                   'field `UPDATED_AT` in '
                                                                                   '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                                   'Source precision and '
                                                                                   'timezone handling follow '
                                                                                   'the pipeline expression; '
                                                                                   'null means the time was '
                                                                                   'unavailable.',
                                                        'subject_id_system': 'Identifier namespace used to '
                                                                             'interpret the subject '
                                                                             'identifier on this record for '
                                                                             'each critical care daily score '
                                                                             'record. It is derived from '
                                                                             'bronze field `PERSON_ID` in '
                                                                             '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                             'Its uniqueness is limited to '
                                                                             'the originating source '
                                                                             'namespace; null means no '
                                                                             'identifier was supplied.',
                                                        'subject_key': 'Deterministic SHA-256 over the '
                                                                       'strongest source identifier. Not '
                                                                       'salted, not secret; a stable join '
                                                                       'key across feeds where person_id is '
                                                                       'unresolved.',
                                                        'vip_ind': 'Indicator that the source record carries '
                                                                   'VIP handling status for each critical '
                                                                   'care daily score record. It is produced '
                                                                   'by the silver transformation and has no '
                                                                   'direct bronze-column lineage entry. Null '
                                                                   'means the source did not state the '
                                                                   'indicator and must not be interpreted as '
                                                                   'false.',
                                                        'withheld_identity_ind': 'Indicator that the source '
                                                                                 'identity was deliberately '
                                                                                 'withheld for each critical '
                                                                                 'care daily score record. '
                                                                                 'It is produced by the '
                                                                                 'silver transformation and '
                                                                                 'has no direct '
                                                                                 'bronze-column lineage '
                                                                                 'entry. Null means the '
                                                                                 'source did not state the '
                                                                                 'indicator and must not be '
                                                                                 'interpreted as false.'},
                                            'quantity_override': True,
                                            'table': 'Internal quality-controlled twin of '
                                                     'clinical_critical_care_daily_score: the Gold repairs, '
                                                     'nulls and expectations are applied here and the '
                                                     'published Gold MV reads this object. Materialized '
                                                     'rather than temporary so the published MV can refresh '
                                                     'incrementally.'},
 'clinical_critical_care_daily_score': {'columns': {'admission_key': 'Stable silver-v2 key identifying the '
                                                                     'admission associated with each '
                                                                     'critical care daily score record. It '
                                                                     'is carried from bronze field '
                                                                     '`ADMISSION_KEY` in '
                                                                     '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                     'This is a contract identifier rather '
                                                                     'than a display value; null means the '
                                                                     'source identifiers were insufficient '
                                                                     'to derive the key.',
                                                    'confidentiality_code': 'Source confidentiality '
                                                                            'classification attached to the '
                                                                            'record for each critical care '
                                                                            'daily score record. It is '
                                                                            'produced by the silver '
                                                                            'transformation and has no '
                                                                            'direct bronze-column lineage '
                                                                            'entry. Source code meanings and '
                                                                            'sentinel values are retained '
                                                                            'unless the pipeline explicitly '
                                                                            'maps them; null means no code '
                                                                            'was supplied.',
                                                    'critical_care_admission_key': 'Deterministic SHA-256 '
                                                                                   'key for the parent '
                                                                                   'critical-care admission.',
                                                    'day_latest_ind': 'Indicator of whether day latest '
                                                                      'applies to the critical care daily '
                                                                      'score record. It is carried from '
                                                                      'bronze field `DAY_LATEST_IND` in '
                                                                      '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                      'Null means the source did not state '
                                                                      'the indicator and must not be '
                                                                      'interpreted as false.',
                                                    'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT '
                                                                    'when available.',
                                                    'event_datetime': 'Date and time when the represented '
                                                                      'clinical or administrative event '
                                                                      'occurred for each critical care daily '
                                                                      'score record. It is derived from '
                                                                      'bronze field `DATE_DAILY_CLEAN` in '
                                                                      '`4_prod.bronze.map_critical_care_daily_score`, '
                                                                      'selected as the provenance anchor '
                                                                      'from 2 recorded bronze source fields. '
                                                                      'Source precision and timezone '
                                                                      'handling follow the pipeline '
                                                                      'expression; null means the time was '
                                                                      'unavailable.',
                                                    'event_datetime_local': 'Local-time representation of '
                                                                            'the event start date and time '
                                                                            'for the critical-care daily '
                                                                            'score record.',
                                                    'event_end_datetime': 'Date and time when the '
                                                                          'represented clinical or '
                                                                          'administrative event ended for '
                                                                          'each critical care daily score '
                                                                          'record. It is produced by the '
                                                                          'silver transformation and has no '
                                                                          'direct bronze-column lineage '
                                                                          'entry. Source precision and '
                                                                          'timezone handling follow the '
                                                                          'pipeline expression; null means '
                                                                          'the time was unavailable.',
                                                    'event_end_datetime_local': 'Local-time representation '
                                                                                'of the event end date and '
                                                                                'time for the critical-care '
                                                                                'daily score record.',
                                                    'fact_category': 'Contract category used to group the '
                                                                     'record with comparable clinical facts '
                                                                     'for each critical care daily score '
                                                                     'record. It is produced by the silver '
                                                                     'transformation and has no direct '
                                                                     'bronze-column lineage entry. '
                                                                     'Whitespace and source sentinel text '
                                                                     'are retained unless the pipeline '
                                                                     'explicitly normalizes them; null means '
                                                                     'no value was supplied.',
                                                    'loaded_at': 'map_critical_care_daily_score.ADC_UPDT '
                                                                 'carried unchanged through code filtering '
                                                                 'and source/public projections. The parent '
                                                                 'admission key is derived without joining a '
                                                                 'parent clock; this is row-level bronze '
                                                                 'ingestion provenance, distinct from '
                                                                 'UPDATED_AT and Silver refresh time.',
                                                    'patient_event_key': 'Deterministic event key derived '
                                                                         'from source identifiers; retained '
                                                                         'as a stable join key.',
                                                    'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                                 'resolved.',
                                                    'person_link_status': 'Processing or clinical status of '
                                                                          'person link for the critical care '
                                                                          'daily score record. It is carried '
                                                                          'from bronze field '
                                                                          '`PERSON_LINK_STATUS` in '
                                                                          '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                          'Whitespace and source sentinel '
                                                                          'text are retained unless the '
                                                                          'pipeline explicitly normalizes '
                                                                          'them; null means no value was '
                                                                          'supplied.',
                                                    'record_status': 'Retracted when '
                                                                     'map_critical_care_daily_score.SOURCE_PRESENT_IND '
                                                                     'is false, otherwise active; a missing '
                                                                     'presence flag defaults true. No '
                                                                     'superseded status is emitted. '
                                                                     'DAY_LATEST_IND, ROW_CLASS, score type '
                                                                     'and parent-admission state do not '
                                                                     'determine this label.',
                                                    'record_status_effective_from': 'First non-null '
                                                                                    'map_critical_care_daily_score.DATE_DAILY_CLEAN '
                                                                                    'then '
                                                                                    'DATE_SCORE_CALC_CLEAN, '
                                                                                    'matching the event-time '
                                                                                    'expression. This '
                                                                                    'date/calculation-time '
                                                                                    'proxy is not an '
                                                                                    'independently observed '
                                                                                    'row-status transition; '
                                                                                    'no UPDATED_AT or '
                                                                                    'ingestion fallback is '
                                                                                    'applied.',
                                                    'record_status_effective_to': 'map_critical_care_daily_score.ADC_UPDT '
                                                                                  'only when the score row '
                                                                                  'is retracted; otherwise '
                                                                                  'null. This is an '
                                                                                  'ingestion-time end proxy, '
                                                                                  'not the score date, '
                                                                                  'calculation date or '
                                                                                  'admission discharge time.',
                                                    'row_class': 'Value describing row class for the '
                                                                 'critical care daily score record. It is '
                                                                 'carried from bronze field `ROW_CLASS` in '
                                                                 '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                                    'score_calc_date': 'Calendar date associated with score '
                                                                       'calc for the critical care daily '
                                                                       'score record. It is derived from '
                                                                       'bronze field `DATE_SCORE_CALC_CLEAN` '
                                                                       'in '
                                                                       '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                       'The value has no time-of-day '
                                                                       'component; null means the source '
                                                                       'date was unavailable.',
                                                    'score_date': 'Calendar date associated with score for '
                                                                  'the critical care daily score record. It '
                                                                  'is derived from bronze field '
                                                                  '`DATE_DAILY_CLEAN` in '
                                                                  '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                  'The value has no time-of-day component; '
                                                                  'null means the source date was '
                                                                  'unavailable.',
                                                    'score_type': 'Medicus SCORE_TYPE; primary-key '
                                                                  'component.',
                                                    'score_value': 'Numeric value recorded for score on the '
                                                                   'critical care daily score record. It is '
                                                                   'carried from bronze field `SCORE_VALUE` '
                                                                   'in '
                                                                   '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                   'Units and source sentinel values follow '
                                                                   'the named source field unless the '
                                                                   'pipeline explicitly converts them; null '
                                                                   'is not equivalent to zero.',
                                                    'score_value_raw': 'Value describing score value raw for '
                                                                       'the critical care daily score '
                                                                       'record. It is carried from bronze '
                                                                       'field `SCORE_VALUE_RAW` in '
                                                                       '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                       'Whitespace and source sentinel text '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly normalizes them; null '
                                                                       'means no value was supplied.',
                                                    'source_code': 'Code supplied by the originating source '
                                                                   'system for the represented concept for '
                                                                   'each critical care daily score record. '
                                                                   'It is derived from bronze field '
                                                                   '`SCORE_TYPE` in '
                                                                   '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                   'Source code meanings and sentinel values '
                                                                   'are retained unless the pipeline '
                                                                   'explicitly maps them; null means no code '
                                                                   'was supplied.',
                                                    'source_coding_system': 'Coding system or source '
                                                                            'namespace in which the source '
                                                                            'code is defined for each '
                                                                            'critical care daily score '
                                                                            'record. It is produced by the '
                                                                            'silver transformation and has '
                                                                            'no direct bronze-column lineage '
                                                                            'entry. Source code meanings and '
                                                                            'sentinel values are retained '
                                                                            'unless the pipeline explicitly '
                                                                            'maps them; null means no code '
                                                                            'was supplied.',
                                                    'source_daily_id': 'Medicus SOURCE_DAILY_ID as BIGINT; '
                                                                       'primary-key component.',
                                                    'source_display': 'Human-readable label supplied by the '
                                                                      'source system for the source code for '
                                                                      'each critical care daily score '
                                                                      'record. It is derived from bronze '
                                                                      'field `SCORE_TYPE` in '
                                                                      '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                      'Whitespace and source sentinel text '
                                                                      'are retained unless the pipeline '
                                                                      'explicitly normalizes them; null '
                                                                      'means no value was supplied.',
                                                    'source_feed': 'Originating data feed responsible for '
                                                                   'the record for each critical care daily '
                                                                   'score record. It is produced by the '
                                                                   'silver transformation and has no direct '
                                                                   'bronze-column lineage entry. Whitespace '
                                                                   'and source sentinel text are retained '
                                                                   'unless the pipeline explicitly '
                                                                   'normalizes them; null means no value was '
                                                                   'supplied.',
                                                    'source_site': 'Medicus SOURCE_SITE; primary-key '
                                                                   'component.',
                                                    'source_unit': 'Medicus SOURCE_UNIT; primary-key '
                                                                   'component.',
                                                    'source_update_timestamp': 'Native source-system update '
                                                                               'timestamp carried through '
                                                                               'the research transformation '
                                                                               'for each critical care daily '
                                                                               'score record. It is derived '
                                                                               'from bronze field '
                                                                               '`UPDATED_AT` in '
                                                                               '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                               'Source precision and '
                                                                               'timezone handling follow the '
                                                                               'pipeline expression; null '
                                                                               'means the time was '
                                                                               'unavailable.',
                                                    'subject_id_system': 'Identifier namespace used to '
                                                                         'interpret the subject identifier '
                                                                         'on this record for each critical '
                                                                         'care daily score record. It is '
                                                                         'derived from bronze field '
                                                                         '`PERSON_ID` in '
                                                                         '`4_prod.bronze.map_critical_care_daily_score`. '
                                                                         'Its uniqueness is limited to the '
                                                                         'originating source namespace; null '
                                                                         'means no identifier was supplied.',
                                                    'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                   'source identifier. Not salted, not '
                                                                   'secret; a stable join key across feeds '
                                                                   'where person_id is unresolved.',
                                                    'vip_ind': 'Indicator that the source record carries VIP '
                                                               'handling status for each critical care daily '
                                                               'score record. It is produced by the silver '
                                                               'transformation and has no direct '
                                                               'bronze-column lineage entry. Null means the '
                                                               'source did not state the indicator and must '
                                                               'not be interpreted as false.',
                                                    'withheld_identity_ind': 'Indicator that the source '
                                                                             'identity was deliberately '
                                                                             'withheld for each critical '
                                                                             'care daily score record. It is '
                                                                             'produced by the silver '
                                                                             'transformation and has no '
                                                                             'direct bronze-column lineage '
                                                                             'entry. Null means the source '
                                                                             'did not state the indicator '
                                                                             'and must not be interpreted as '
                                                                             'false.'},
                                        'quantity_override': False,
                                        'table': 'One admitted Medicus daily critical-care score row. Gold '
                                                 'QC twin of the silver product: 1 columns are repaired or '
                                                 'nulled, 1 rule(s) drop rows, 3 check(s) are advisory. Each '
                                                 'rule states its reason in the pipeline notebook, and '
                                                 'Lakeflow expectation metrics report what every rule '
                                                 'matched on each update. Coverage: Medicus critical-care '
                                                 'daily scores are mostly not calculated from 2022 (SOFA '
                                                 'filled on 96% of patient-days before, 12% after) while '
                                                 'patient-days are unchanged; complete-through month per '
                                                 'release: 6_mgmt.data_quality.release_finding (finding_code '
                                                 'F2_complete_through).'}}
